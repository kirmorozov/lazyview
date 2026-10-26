//! Scenarios that need a database server of their own: they restart it, purge its binlogs or
//! need it idle. Opt-in: `LAZYVIEW_TEST_EXCLUSIVE=<flavor>=<container>,...` names the Docker
//! container of each such flavor in `LAZYVIEW_TEST_DSNS` (never a server other tests share);
//! flavors without one are skipped with a note.

mod common;

use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use common::*;
use mysql_async::prelude::Queryable;

/// The Docker container of the flavor this run is on, if `LAZYVIEW_TEST_EXCLUSIVE` names one.
fn container(ctx: &Ctx) -> Option<String> {
    let raw = std::env::var("LAZYVIEW_TEST_EXCLUSIVE").ok()?;
    raw.split(',').find_map(|p| {
        let (flavor, c) = p.trim().split_once('=')?;
        (flavor.trim() == ctx.servers[0].flavor).then(|| c.trim().to_string())
    })
}

/// `docker restart` (a clean shutdown, then a start) and wait until the server answers again.
async fn restart(db: &Db, container: &str) -> Result<()> {
    let c = container.to_string();
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "-t", "30", &c])
            .output()
    })
    .await??;
    ensure!(
        out.status.success(),
        "docker restart {container}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let t = Instant::now();
    loop {
        if let Ok(mut c) = db.conn().await {
            if c.query_drop("SELECT 1").await.is_ok() {
                return Ok(());
            }
        }
        ensure!(
            t.elapsed() < Duration::from_secs(120),
            "{container} not back within 120 s"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

const DDL: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
    "INSERT INTO entities VALUES (10, 1, 'new'), (11, 1, 'paid'), (12, 2, 'new'), (13, 3, 'paid')",
];

fn spec() -> Vec<ViewSpec> {
    vec![ViewSpec::new(
        "x_counts",
        Kind::Incremental,
        "SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp",
    )]
}

async fn setup(ctx: &Ctx, tag: &str, ov: &[(&str, &str)]) -> Result<(Db, Engine)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    for s in DDL {
        exec(&mut c, s).await?;
    }
    ensure_lazyview_table(&mut c).await?;
    let specs = spec();
    add_views(&mut c, &specs).await?;
    let mut eng = ctx.start_engine(&db, ov)?;
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    wait_ready(&mut c, &mut eng, &names, t_ready()).await?;
    verify(ctx, &mut eng, &db, &specs, "initial").await?;
    Ok((db, eng))
}

/// A clean server restart while lazyview streams and saves its position: statements that land
/// in the shutdown window get ER_SERVER_SHUTDOWN (1053), which made lazyview exit ("fatal:
/// saving the binlog position ... Server shutdown in progress") on 3 of 5 restarts. It must
/// reconnect every time and stay exact. Positions are saved every 50 ms and a writer keeps
/// going, so the window is hit.
#[test]
fn exclusive_server_restarts_are_not_fatal() {
    scenario("x_restart", |ctx| async move {
        let Some(container) = container(&ctx) else {
            ctx.note("SKIP: needs LAZYVIEW_TEST_EXCLUSIVE=<flavor>=<container>");
            return Ok(());
        };
        let ov = [("LAZYVIEW_POSITION_PERSIST_MS", "50")];
        let (db, mut eng) = setup(&ctx, "x_restart", &ov).await?;
        let specs = spec();
        let mut next = 100;
        for round in 1..=5 {
            // Writes right before and during the shutdown keep flushes and position saves going.
            let writer = {
                let db = db.clone();
                let first = next;
                tokio::spawn(async move {
                    let mut c = match db.conn().await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    for id in first..first + 200 {
                        let sql = format!("INSERT INTO entities VALUES ({id}, {}, 'new')", id % 7);
                        if c.query_drop(sql).await.is_err() {
                            return;
                        }
                    }
                })
            };
            tokio::time::sleep(Duration::from_millis(150)).await;
            restart(&db, &container).await?;
            let _ = writer.await;
            next += 200;
            eng.check_alive()
                .with_context(|| format!("restart {round}: lazyview exited"))?;
            let mut c = db.conn().await?;
            exec(
                &mut c,
                &format!(
                    "UPDATE entities SET status = 'paid' WHERE id = {}",
                    next - 1
                ),
            )
            .await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after restart {round}"),
            )
            .await?;
        }
        eng.stop().await
    });
}

/// The binlog was purged past the saved position while lazyview was down: it exits (default
/// LAZYVIEW_ON_MISSING_BINLOG=fail), and `_lazyview_overview` said every view was `ready` while
/// they no longer follow the source. The state rows say why it stopped; a start that can run
/// (here with LAZYVIEW_ON_MISSING_BINLOG=rebuild) rebuilds the views and clears it.
#[test]
fn exclusive_purged_binlog_marks_the_views() {
    scenario("x_purge", |ctx| async move {
        if container(&ctx).is_none() {
            ctx.note("SKIP: needs LAZYVIEW_TEST_EXCLUSIVE=<flavor>=<container>");
            return Ok(());
        }
        let (db, eng) = setup(&ctx, "x_purge", &[]).await?;
        eng.stop().await?;
        let mut c = db.conn().await?;
        exec(&mut c, "INSERT INTO entities VALUES (20, 4, 'new')").await?;
        // Purge until the saved file is gone (MariaDB keeps a file a replica connection or crash
        // recovery still needs, for a moment after the FLUSH).
        let saved: String = c
            .query_first("SELECT CAST(binlog_file AS CHAR) FROM _lazyview_status")
            .await?
            .context("no saved position")?;
        let t = Instant::now();
        loop {
            exec(&mut c, "FLUSH BINARY LOGS").await?;
            let head = lazyview::binlog::current_position(&mut c).await?;
            exec(&mut c, &format!("PURGE BINARY LOGS TO '{}'", head.file)).await?;
            let files: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
            let gone = files
                .into_iter()
                .all(|r| r.get::<String, _>(0).as_deref() != Some(saved.as_str()));
            if gone {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(60),
                "{saved} could not be purged"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let eng = ctx.start_engine(&db, &[])?;
        match eng.wait_finished(Duration::from_secs(60)).await? {
            Ok(()) => bail!("lazyview did not stop at a purged position"),
            Err(e) => ensure!(e.contains("no longer available"), "unexpected stop: {e}"),
        }
        let rows: Vec<(String, String, Option<String>)> = c
            .query("SELECT CAST(name AS CHAR), CAST(state AS CHAR), CAST(last_error AS CHAR) FROM _lazyview_overview")
            .await?;
        ensure!(
            !rows.is_empty()
                && rows.iter().all(|(_, s, e)| s == "error"
                    && e.as_deref()
                        .is_some_and(|e| e.contains("no longer available"))),
            "views not marked after the stop: {rows:?}"
        );
        let specs = spec();
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_ON_MISSING_BINLOG", "rebuild")])?;
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        wait_ready(&mut c, &mut eng, &names, t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "rebuilt after the purge").await?;
        eng.stop().await
    });
}

/// On an idle source, a binlog rotation (FLUSH BINARY LOGS, a server restart) left the applied
/// and saved position in the previous file, so "applied >= the server's binlog end" never held
/// until the next write (10 min seen). Heartbeats between transactions move it on.
#[test]
fn exclusive_idle_rotation_moves_the_position() {
    scenario("x_idle", |ctx| async move {
        let Some(container) = container(&ctx) else {
            ctx.note("SKIP: needs LAZYVIEW_TEST_EXCLUSIVE=<flavor>=<container>");
            return Ok(());
        };
        let (db, mut eng) = setup(&ctx, "x_idle", &[]).await?;
        let mut c = db.conn().await?;
        for (what, rotate) in [("FLUSH BINARY LOGS", false), ("server restart", true)] {
            if rotate {
                restart(&db, &container).await?;
                c = db.conn().await?;
            } else {
                exec(&mut c, "FLUSH BINARY LOGS").await?;
            }
            // Nothing is written from here on: only the position moves.
            eng.wait_caught_up(&mut c, Duration::from_secs(30))
                .await
                .with_context(|| format!("idle after {what}"))?;
            let head = lazyview::binlog::current_position(&mut c).await?;
            let t = Instant::now();
            loop {
                let saved: Option<String> = c
                    .query_first("SELECT CAST(binlog_file AS CHAR) FROM _lazyview_status")
                    .await?;
                if saved.as_deref() == Some(head.file.as_str()) {
                    break;
                }
                ensure!(
                    t.elapsed() < Duration::from_secs(30),
                    "after {what}: saved position still in {saved:?}, the server writes {}",
                    head.file
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        verify(&ctx, &mut eng, &db, &spec(), "after the rotations").await?;
        eng.stop().await
    });
}

/// The engine's connections killed under it (KILL from an admin session, a proxy or a failover
/// dropping sessions): MariaDB answers the next statement with ER_CONNECTION_KILLED (1927),
/// which was fatal like 1053. lazyview must reconnect and stay exact.
#[test]
fn exclusive_killed_connections_are_not_fatal() {
    scenario("x_kill", |ctx| async move {
        if container(&ctx).is_none() {
            ctx.note("SKIP: needs LAZYVIEW_TEST_EXCLUSIVE=<flavor>=<container>");
            return Ok(());
        }
        let ov = [("LAZYVIEW_POSITION_PERSIST_MS", "50")];
        let (db, mut eng) = setup(&ctx, "x_kill", &ov).await?;
        let specs = spec();
        let mut c = db.conn().await?;
        for round in 1..=3 {
            // Every other session of the test user on this (exclusive) server: the engine's.
            let ids: Vec<u64> = c
                .query("SELECT ID FROM information_schema.PROCESSLIST WHERE USER = 'lv' AND ID <> CONNECTION_ID()")
                .await?;
            ensure!(!ids.is_empty(), "no engine sessions to kill");
            for id in ids {
                let _ = c.query_drop(format!("KILL {id}")).await;
            }
            exec(
                &mut c,
                &format!("INSERT INTO entities VALUES ({}, 5, 'new')", 200 + round),
            )
            .await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            eng.check_alive()
                .with_context(|| format!("kill {round}: lazyview exited"))?;
            verify(&ctx, &mut eng, &db, &specs, &format!("after kill {round}")).await?;
        }
        eng.stop().await
    });
}
