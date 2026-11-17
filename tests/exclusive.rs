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

/// The MySQL account the engine connects as: `LAZYVIEW_TEST_ENGINE_USER` (CI: `lvdoc:doc`) when
/// set, the schema DSN's admin user otherwise — the harness starts every engine that way.
fn engine_user(db: &Db) -> String {
    let raw = match std::env::var("LAZYVIEW_TEST_ENGINE_USER") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            let rest = db.dsn.split_once("://").map_or(db.dsn.as_str(), |(_, r)| r);
            rest.split_once('@').unwrap_or(("", "")).0.to_string()
        }
    };
    raw.split(':').next().unwrap_or_default().to_string()
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
        if let Ok(mut c) = connect_once(&db.dsn).await {
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
    setup_views(ctx, tag, DDL, &spec(), ov).await
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

/// The tables of the compression window below. `note` is read by no view, and its width is what
/// makes MariaDB compress the row events at all: records under `log_bin_compress_min_len` (256
/// bytes) are written uncompressed, so without it the window would prove nothing.
const WIDE_DDL: &[&str] = &[
    "CREATE TABLE items (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL, \
     note VARCHAR(1000) NULL)",
    "INSERT INTO items VALUES (10, 1, 'new', NULL), (11, 1, 'paid', NULL), (12, 2, 'new', NULL), \
     (13, 3, 'paid', NULL)",
];

/// The view of the compression window: no row has group 9 until the window's own transaction, so
/// a row for it would mean a compressed transaction had been applied.
fn wide_spec() -> Vec<ViewSpec> {
    vec![ViewSpec::new(
        "x_wide_counts",
        Kind::Incremental,
        "SELECT grp, COUNT(*) AS n FROM items GROUP BY grp",
    )]
}

/// Polls an engine's log file until a line contains `needle`, returning its text on the first hit
/// (for a mid-stream condition the test waits to see reported; `chaos.rs` has the same shape).
async fn wait_log_contains(
    log: &std::path::Path,
    needle: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(text) = std::fs::read_to_string(log) {
            if text.contains(needle) {
                return Ok(text);
            }
        }
        ensure!(
            Instant::now() <= deadline,
            "no {needle:?} line in the engine log {} within {timeout:?}",
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// MariaDB's own binlog compression, `log_bin_compress`, on a server this test has to itself.
///
/// This is the one compression window that cannot run beside another scenario: the variable has no
/// session scope (the GLOBAL is the only way to write a compressed event), and the events it writes
/// are the compressed types 165..171, which this reader can never decode — so an engine that
/// started before the window stalls on them for good, whatever else it was doing. That is what
/// "needs a database server of their own" means, so the scenario lives here and runs under the
/// `LAZYVIEW_TEST_EXCLUSIVE` opt-in. MySQL / Percona compress at session scope and their payloads
/// are readable, so their window stays in `tests/lifecycle.rs` (`lc_compressed`, which the MariaDB
/// family runs as the same transaction shapes, uncompressed).
///
/// What is proven: lazyview streams normally up to the window; on reaching one of those events it
/// reports the documented error with the documented fix (docs/server-setup.md: `log_bin_compress`
/// must be OFF); it stays alive and retries, with its position held before the unreadable events;
/// and the compressed transaction never reaches a view, so nothing is half-applied. It cannot pass
/// them: starting over while the GLOBAL is ON is refused at start-up
/// (`lifecycle_refuses_log_bin_compress`).
#[test]
fn exclusive_mariadb_compressed_window_is_unreadable() {
    scenario("x_lc_compress", |ctx| async move {
        if !ctx.server().is_mariadb() {
            ctx.skip(
                "only MariaDB writes binlog events this reader cannot decode; MySQL's transaction \
                 compression is session-scoped and readable (tests/lifecycle.rs)",
            );
            return Ok(());
        }
        if container(&ctx).is_none() {
            ctx.note("SKIP: needs LAZYVIEW_TEST_EXCLUSIVE=<flavor>=<container>");
            return Ok(());
        }
        let ov = [
            ("LAZYVIEW_POSITION_PERSIST_MS", "300"),
            ("LAZYVIEW_QUERY_CHUNK", "7"),
        ];
        let specs = wide_spec();
        let (db, mut eng) = setup_views(&ctx, "x_lc_compress", WIDE_DDL, &specs, &ov).await?;
        let mut c = db.conn().await?;
        // Ordinary writes first, so the window opens while the engine is streaming: the mid-stream
        // path (a start at the current position is the start-up refusal mentioned above).
        for id in 100..100 + scaled(40) {
            exec(
                &mut c,
                &format!("INSERT INTO items VALUES ({id}, {}, 'new', NULL)", id % 7),
            )
            .await?;
        }
        verify(&ctx, &mut eng, &db, &specs, "streaming before the window").await?;

        // The window: the GLOBAL every session on this server writes with, back OFF as soon as the
        // statements that have to land inside it are through.
        let mut globals = Globals::set(&mut c, &db.dsn, &[("log_bin_compress", "ON")]).await?;
        exec_all(
            &mut c,
            &[
                "START TRANSACTION".to_string(),
                // The marker's row image is 600 bytes of note: its Write_rows event is written
                // compressed.
                "INSERT INTO items (id, grp, status, note) VALUES (987654, 9, 'paid', \
                 REPEAT('m', 600))"
                    .to_string(),
                // Bulk wide images: every row event of this transaction is compressed too.
                "UPDATE items SET note = REPEAT('u', 600) WHERE id <> 987654".to_string(),
                "COMMIT".to_string(),
            ],
        )
        .await?;
        globals.restore(&mut c).await?;
        // Where the window's transaction ends on the server. The reader may take the plain events
        // that precede the compressed ones (MariaDB compresses per record, so the transaction's
        // GTID, BEGIN and Table_map are written uncompressed) — nothing past here may ever be
        // applied, and the position must stay before it.
        let window_end = server_position(&mut c).await?;

        // The window really was compressed: the stream that reads into it reports the unreadable
        // events with the documented fix hint...
        let log = wait_log_contains(
            &eng.log_path,
            "MariaDB compressed binlog event (type",
            Duration::from_secs(60),
        )
        .await
        .with_context(|| format!("engine log {}", eng.log_path.display()))?;
        ensure!(
            log.contains("set log_bin_compress=OFF"),
            "the reported error lacks the documented fix hint — log {}",
            eng.log_path.display()
        );
        // ...and it keeps retrying rather than dying. The GLOBAL is OFF again and the writer is
        // idle, so anything that moved the position would have been the reader passing the events
        // it cannot decode — that is what the position check below rules out.
        eng.check_alive()?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut mc = db.conn().await?;
        let saved: (String, u64) = mc
            .query_first("SELECT CAST(binlog_file AS CHAR), binlog_pos FROM _lazyview_status")
            .await?
            .context("no saved position after the window")?;
        ensure!(
            (saved.0.as_str(), saved.1) < (window_end.file.as_str(), window_end.pos),
            "the reader passed the unreadable events: it saved {}:{} while the window's \
             transaction ends at {}:{}",
            saved.0,
            saved.1,
            window_end.file,
            window_end.pos
        );
        ensure!(
            scalar_i64(&mut mc, "SELECT COUNT(*) FROM items WHERE id = 987654").await? == 1,
            "the compressed marker row was not written to items"
        );
        ensure!(
            scalar_i64(&mut mc, "SELECT COUNT(*) FROM x_wide_counts WHERE grp = 9").await? == 0,
            "a compressed transaction reached x_wide_counts although its events are unreadable"
        );
        // The stream cannot pass those events, so the engine ends crashed, not stopped.
        eng.crash().await
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
        // The sessions to kill are the engine's, and the engine runs as `engine_user` (CI:
        // lvdoc), not as the schema's admin user. They appear only while the engine is
        // connected (after a kill it takes a moment to come back), so poll for them.
        let user = engine_user(&db);
        for round in 1..=3 {
            let (dsn, u) = (db.dsn.clone(), user.clone());
            wait_until(
                "engine sessions in the PROCESSLIST",
                Duration::from_secs(30),
                || {
                    let (dsn, u) = (dsn.clone(), u.clone());
                    async move {
                        let mut p = connect(&dsn).await?;
                        let alive = !session_ids(&mut p, &format!("USER = {}", q(&u)))
                            .await?
                            .is_empty();
                        p.disconnect().await.ok();
                        Ok(alive)
                    }
                },
            )
            .await?;
            // Every other session of the engine user on this (exclusive) server: the engine's.
            let ids = session_ids(&mut c, &format!("USER = {}", q(&user))).await?;
            ensure!(!ids.is_empty(), "no engine sessions to kill");
            for id in ids {
                let _ = c.query_drop(format!("KILL {id}")).await;
            }
            exec(
                &mut c,
                &format!("INSERT INTO entities VALUES ({}, 5, 'new')", 200 + round),
            )
            .await?;
            // Keep the fixed sleep: nothing must happen in this window — the engine must
            // not exit after its sessions were killed; verify() below polls the recovery.
            tokio::time::sleep(Duration::from_secs(2)).await;
            eng.check_alive()
                .with_context(|| format!("kill {round}: lazyview exited"))?;
            verify(&ctx, &mut eng, &db, &specs, &format!("after kill {round}")).await?;
        }
        eng.stop().await
    });
}
