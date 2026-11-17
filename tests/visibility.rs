//! The visibility fences, one test per fence (the commit-order fence: #28; the fence per
//! flavor: F7 #219). Binlog events ship before the committing transaction ends, so the engine
//! must prove a change is visible before it reads the sources. Every flavor has a fence, and
//! the engine dispatches to it in this order (the engine's `fence`); which of them is a fence at
//! all is the flavor's answer, not a branch on product names:
//!
//! - the MySQL family with GTIDs: `WAIT_FOR_EXECUTED_GTID_SET` (`src/source/mysql.rs`);
//! - the flavors whose consistent-snapshot position is a committed-transaction watermark
//!   (`Flavor::snapshot_position_is_watermark`, `src/source/mod.rs`) — MariaDB: the binlog
//!   position of a consistent snapshot (`src/source/maria.rs`). The MySQL family answers `false`:
//!   what its `binlog_snapshot_%` variables report is the raw binlog write position, which has
//!   already passed transactions still in flight that its snapshot cannot read (#257), so waiting
//!   on it would approve a stale read;
//! - the MySQL family without GTIDs, and any flavor the snapshot ruled out: one row of our own,
//!   committed in binlog order (`src/source/mysql.rs`, `src/fence.rs`);
//! - no fence available: the `LAZYVIEW_VISIBILITY_DELAY_MS` fallback, not exact under load.
//!
//! Each test runs on the flavors where its fence applies; on the others it logs which fence
//! covers that flavor and returns, so the binary never prints SKIP on a MySQL-family server
//! and every CI job runs at least one fence test for real (CI sets `LAZYVIEW_TEST_STRICT`
//! with `visibility_`: a skip here is a failure). All exact tests share
//! `visibility_commit_fence_exact`'s load: ten single-row autocommit changes and one XA
//! transaction under `WIDEN`, checked against the oracle after every change.
//!
//! The tests change server globals, so they run one at a time and restore the globals
//! afterwards.

mod common;

use std::sync::Mutex;

use anyhow::{bail, ensure, Context, Result};
use common::*;
use lazyview::source::Flavor;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;

/// Serializes the tests in this binary: they set the same server globals.
static GLOBALS: Mutex<()> = Mutex::new(());

/// Widens the window between "the dump thread may send an event" and "InnoDB has committed it":
/// with `sync_binlog=0` the dump thread sees a commit group right after its flush, and the 200 ms
/// group sync delay runs before the commit stage. A 20 ms sleep then reads stale rows.
const WIDEN: [(&str, &str); 2] = [
    ("sync_binlog", "0"),
    ("binlog_group_commit_sync_delay", "200000"),
];

/// The MySQL-family `binlog_group_commit_sync_delay` (and `binlog_order_commits`) do not exist on
/// MariaDB: there the window is widened with `sync_binlog=0` alone, and the load's own group
/// commits (ten autocommit changes plus the XA) carry the flush-to-commit gap.
fn widen_for(f: &Fences) -> Vec<(&'static str, &'static str)> {
    if f.mariadb {
        vec![WIDEN[0]]
    } else {
        WIDEN.to_vec()
    }
}

const DDL: &[&str] = &[
    "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)",
    "INSERT INTO t VALUES (1, 0), (2, 1), (3, 2), (4, 3)",
];

/// Rows move in and out of the view as `v` changes parity.
fn view() -> ViewSpec {
    ViewSpec::new(
        "v_even",
        Kind::Incremental,
        "SELECT id, v FROM t WHERE v % 2 = 0",
    )
}

/// Opt-in for the tests here: they change GLOBAL variables, so no other suite may use the server
/// at the same time (CI sets it; it runs one test binary at a time).
const OPT_IN: &str = "LAZYVIEW_TEST_GLOBAL_BINLOG_COMMIT";
const OPT_IN_SKIP: &str = "changes GLOBAL sync_binlog, binlog_group_commit_sync_delay and binlog_order_commits; set LAZYVIEW_TEST_GLOBAL_BINLOG_COMMIT=1 to run it";

/// The fence this server has, mirroring the engine's dispatch (the engine's `fence`): the GTID
/// wait, the consistent snapshot, the commit-order row, or none (the delay fallback).
struct Fences {
    mariadb: bool,
    percona: bool,
    /// The MySQL family's `gtid_mode` is ON, so the engine's GTID wait covers every transaction.
    gtid_on: bool,
    /// A consistent-snapshot transaction gets a binlog position (MySQL Community reports none).
    snapshot: bool,
}

impl Fences {
    /// Reads the same facts the engine's dispatch decides on.
    async fn probe(conn: &mut Conn) -> Result<Fences> {
        let comment = scalar(conn, "SELECT @@version_comment")
            .await?
            .unwrap_or_default();
        let lower = comment.to_ascii_lowercase();
        let mariadb = lower.contains("mariadb");
        let percona = lower.contains("percona");
        // MariaDB has no gtid_mode variable, and the GTID wait is MySQL-family only anyway.
        let gtid_on =
            !mariadb && scalar(conn, "SELECT @@GLOBAL.gtid_mode").await?.as_deref() == Some("ON");
        let snapshot = snapshot_position(conn).await?.is_some();
        Ok(Fences {
            mariadb,
            percona,
            gtid_on,
            snapshot,
        })
    }

    /// The GTID wait: the MySQL family with GTIDs (Percona included, as `is_mysql_family` says).
    fn gtid_fence(&self) -> bool {
        !self.mariadb && self.gtid_on
    }

    /// The consistent-snapshot position: MariaDB's fence, and its answer alone
    /// (`Flavor::snapshot_position_is_watermark`): the MySQL family reports the raw binlog write
    /// position, which covers transactions still in flight, so the engine must not wait on it
    /// there (#257).
    fn snapshot_fence(&self) -> bool {
        self.mariadb && !self.gtid_fence() && self.snapshot
    }

    /// The commit-order row: the MySQL family with neither GTIDs nor a snapshot fence — Oracle
    /// Community and Percona Server, both through `src/source/mysql.rs`'s commit-order step — and
    /// MariaDB once its snapshot is ruled out.
    fn commit_fence(&self) -> bool {
        !self.mariadb && !self.gtid_fence() && !self.snapshot_fence()
    }

    /// The flavor this server is, named the way the engine's connect names it.
    fn flavor(&self) -> Flavor {
        if self.mariadb {
            Flavor::MariaDb
        } else if self.percona {
            Flavor::Percona
        } else {
            Flavor::MySql
        }
    }

    /// The test that runs for real on this flavor.
    fn covering_test(&self) -> &'static str {
        if self.gtid_fence() {
            "visibility_gtid_fence_exact"
        } else if self.snapshot_fence() {
            "visibility_snapshot_fence_exact"
        } else if self.commit_fence() {
            "visibility_commit_fence_exact"
        } else {
            "visibility_fence_per_flavor"
        }
    }
}

/// The binlog position a consistent-snapshot transaction includes, read the way the engine's
/// snapshot fence reads it (`maria::snapshot_position`): `None` when the server reports no
/// position at all (MySQL Community), which rules the fence out by itself.
async fn snapshot_position(conn: &mut Conn) -> Result<Option<(String, u64)>> {
    conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await?;
    // The commit must run whatever the status read did: an open snapshot would poison the session.
    let read: Result<Vec<(String, String)>> = conn
        .query("SHOW SESSION STATUS LIKE 'binlog_snapshot_%'")
        .await
        .context("reading the binlog snapshot position");
    conn.query_drop("COMMIT").await?;
    let mut file = None;
    let mut pos = None;
    for (k, v) in read? {
        match k.to_ascii_lowercase().as_str() {
            "binlog_snapshot_file" => file = Some(v),
            "binlog_snapshot_position" => pos = v.trim().parse::<u64>().ok(),
            _ => {}
        }
    }
    Ok(match (file, pos) {
        (Some(file), Some(pos)) if !file.is_empty() => Some((file, pos)),
        _ => None,
    })
}

/// The test's result, or the first error: the run's, then the globals restore's.
fn both<T>(run: Result<T>, restored: Result<()>) -> Result<T> {
    let v = run?;
    restored?;
    Ok(v)
}

/// The three WARN families that mean an exact fence gave up: the commit-order fallback (ruled
/// out, unavailable or failed) and the two 5 s timeout WARNs. On the exact path none may fire.
/// The waits that answered are only invisible when they were supposed to: on a GTID server the
/// GTID wait must have answered (no `gtid fence unavailable`, no NULL), on a snapshot server the
/// snapshot must have reported its position.
fn ensure_exact(log: &str, path: &std::path::Path, f: &Fences) -> Result<()> {
    for marker in [
        "commit fence",
        "binlog_order_commits is OFF",
        "not visible after 5 s",
    ] {
        ensure!(
            !log.contains(marker),
            "the fence fell back ({marker} in {})",
            path.display()
        );
    }
    if f.gtid_fence() {
        ensure!(
            !log.contains("gtid fence unavailable") && !log.contains("gtid fence returned NULL"),
            "the GTID wait did not answer, see {}",
            path.display()
        );
    }
    if f.snapshot_fence() {
        ensure!(
            !log.contains("no binlog snapshot position")
                && !log.contains("snapshot fence unavailable"),
            "the consistent snapshot did not report a position, see {}",
            path.display()
        );
    }
    Ok(())
}

/// Creates the source table and the view, starts an engine (batch window 0, so every change is
/// refreshed at once) and waits until the view is ready. `extra` adds engine settings.
async fn start(ctx: &Ctx, db: &Db, specs: &[ViewSpec]) -> Result<(Conn, Engine)> {
    start_with(ctx, db, specs, &[]).await
}

async fn start_with(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    extra: &[(&str, &str)],
) -> Result<(Conn, Engine)> {
    let mut c = db.conn().await?;
    for s in DDL {
        exec(&mut c, s).await?;
    }
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, specs).await?;
    let mut env = vec![("LAZYVIEW_BATCH_WINDOW_MS", "0")];
    env.extend_from_slice(extra);
    let mut eng = ctx.start_engine(db, &env)?;
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    wait_ready(&mut c, &mut eng, &names, t_ready()).await?;
    verify(ctx, &mut eng, db, specs, "initial").await?;
    Ok((c, eng))
}

/// Ten single-row autocommit changes, each checked against the oracle, then one XA transaction
/// (`xid` names it, unique per test). Returns the engine's log file.
async fn exact_run(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    xid: &str,
) -> Result<std::path::PathBuf> {
    exact_run_with(ctx, db, specs, xid, &[]).await
}

/// `exact_run` with extra engine settings.
async fn exact_run_with(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    xid: &str,
    extra: &[(&str, &str)],
) -> Result<std::path::PathBuf> {
    let (mut c, mut eng) = start_with(ctx, db, specs, extra).await?;
    for i in 0..10 {
        exec(
            &mut c,
            &format!("UPDATE t SET v = v + 1 WHERE id = {}", 1 + i % 4),
        )
        .await?;
        verify(ctx, &mut eng, db, specs, &format!("autocommit update {i}")).await?;
    }
    // XA COMMIT runs the same ordered commit stage as an autocommit statement.
    exec_all(
        &mut c,
        &[
            format!("XA START '{xid}'"),
            "UPDATE t SET v = v + 100 WHERE id = 2".to_string(),
            format!("XA END '{xid}'"),
            format!("XA PREPARE '{xid}'"),
            format!("XA COMMIT '{xid}'"),
        ],
    )
    .await?;
    verify(ctx, &mut eng, db, specs, "xa commit").await?;
    let log = eng.log_path.clone();
    eng.stop().await?;
    Ok(log)
}

/// The same changes with `binlog_order_commits=OFF`: counts the checks that find stale views.
/// A catch-up timeout is not a stale view and fails the test.
async fn off_run(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    what: &str,
) -> Result<(usize, std::path::PathBuf)> {
    let (mut c, mut eng) = start(ctx, db, specs).await?;
    let mut stale = 0usize;
    for i in 0..10 {
        exec(
            &mut c,
            &format!("UPDATE t SET v = v + 1 WHERE id = {}", 1 + i % 4),
        )
        .await?;
        match verify(ctx, &mut eng, db, specs, &format!("{what} update {i}")).await {
            Ok(()) => {}
            Err(e) if format!("{e:#}").contains("catch-up") => return Err(e),
            Err(e) => {
                stale += 1;
                ctx.log(format!("stale check {i}: {e:#}"));
            }
        }
    }
    let log = eng.log_path.clone();
    eng.stop().await?;
    Ok((stale, log))
}

#[test]
fn visibility_commit_fence_exact() {
    let _serial = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
    scenario("visibility_commit_fence_exact", |ctx| async move {
        if !opted_in(OPT_IN) {
            ctx.skip(OPT_IN_SKIP);
            return Ok(());
        }
        let db = ctx.new_db("vis_exact").await?;
        let mut admin = db.conn().await?;
        let f = Fences::probe(&mut admin).await?;
        if !f.commit_fence() {
            // The other fences' tests run here for real; this one needs no GTIDs and no
            // snapshot position, the shape only Oracle Community has.
            ctx.log(format!(
                "the commit-order fence does not apply here: {} runs on this flavor",
                f.covering_test()
            ));
            return Ok(());
        }
        let specs = vec![view()];
        let mut globals = Globals::set(&mut admin, &db.dsn, &WIDEN).await?;
        let r = exact_run(&ctx, &db, &specs, "vis28").await;
        let restored = globals.restore(&mut admin).await;
        let log_path = both(r, restored)?;
        // The fence path must have run: no fence WARN (order not enforced, or a failed write).
        let log = std::fs::read_to_string(&log_path)?;
        ensure!(
            !log.contains("commit fence"),
            "the commit fence fell back, see the engine log"
        );
        Ok(())
    });
}

#[test]
fn visibility_order_commits_off_falls_back() {
    let _serial = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
    scenario(
        "visibility_order_commits_off_falls_back",
        |ctx| async move {
            if !opted_in(OPT_IN) {
                ctx.skip(OPT_IN_SKIP);
                return Ok(());
            }
            let db = ctx.new_db("vis_off").await?;
            let mut admin = db.conn().await?;
            let f = Fences::probe(&mut admin).await?;
            if !f.commit_fence() {
                ctx.log(format!(
                    "the commit-order fence does not apply here: {} runs on this flavor",
                    f.covering_test()
                ));
                return Ok(());
            }
            let specs = vec![view()];
            let mut changes = WIDEN.to_vec();
            changes.push(("binlog_order_commits", "OFF"));
            let mut globals = Globals::set(&mut admin, &db.dsn, &changes).await?;
            let r = off_run(&ctx, &db, &specs, "OFF").await;
            let restored = globals.restore(&mut admin).await;
            let (stale, log_path) = both(r, restored)?;
            let log = std::fs::read_to_string(&log_path)?;
            ensure!(
                log.contains("binlog_order_commits is OFF"),
                "no WARN about binlog_order_commits=OFF in {}",
                log_path.display()
            );
            if stale == 0 {
                bail!("no stale check under OFF: the harness does not reproduce #28");
            }
            Ok(())
        },
    );
}

/// MariaDB's and Percona Server's fence: every change read from the binlog is inside the
/// snapshot position the engine waited on, so the view is never stale under the widened window.
#[test]
fn visibility_snapshot_fence_exact() {
    let _serial = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
    scenario("visibility_snapshot_fence_exact", |ctx| async move {
        if !opted_in(OPT_IN) {
            ctx.skip(OPT_IN_SKIP);
            return Ok(());
        }
        let db = ctx.new_db("vis_snap").await?;
        let mut admin = db.conn().await?;
        let f = Fences::probe(&mut admin).await?;
        if !f.snapshot_fence() {
            ctx.log(format!(
                "the consistent-snapshot fence does not apply here: {} runs on this flavor",
                f.covering_test()
            ));
            return Ok(());
        }
        let specs = vec![view()];
        let mut globals = Globals::set(&mut admin, &db.dsn, &widen_for(&f)).await?;
        let r = exact_run(&ctx, &db, &specs, "vis3").await;
        let restored = globals.restore(&mut admin).await;
        let log_path = both(r, restored)?;
        let log = std::fs::read_to_string(&log_path)?;
        ensure_exact(&log, &log_path, &f)?;
        Ok(())
    });
}

/// The same fence ruled out: `LAZYVIEW_TEST_FAIL_SNAPSHOT` fails every snapshot read, so the
/// engine must notice (the retry WARN and the fallback debug), wait its
/// `LAZYVIEW_VISIBILITY_DELAY_MS` and still deliver exact views in the end. The stale-under-
/// OFF proof of the fallback lives in `visibility_order_commits_off_falls_back`: with the
/// snapshot retries inside `retry_src` the read lands after the commit, so a stale check here
/// would not be reachable; what this test shows is that a ruled-out snapshot fence never
/// wedges the engine, only degrades it to the documented fallback.
#[test]
fn visibility_snapshot_fence_falls_back() {
    let _serial = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
    scenario("visibility_snapshot_fence_falls_back", |ctx| async move {
        if !opted_in(OPT_IN) {
            ctx.skip(OPT_IN_SKIP);
            return Ok(());
        }
        let db = ctx.new_db("vis_snap_off").await?;
        let mut admin = db.conn().await?;
        let f = Fences::probe(&mut admin).await?;
        if !f.snapshot_fence() {
            ctx.log(format!(
                "the consistent-snapshot fence does not apply here: {} runs on this flavor",
                f.covering_test()
            ));
            return Ok(());
        }
        let specs = vec![view()];
        let mut globals = Globals::set(&mut admin, &db.dsn, &widen_for(&f)).await?;
        // Every try of every flush fails (the engine makes five per read): the fence is
        // ruled out from the first flush to the last.
        let r = exact_run_with(
            &ctx,
            &db,
            &specs,
            "vis4",
            &[("LAZYVIEW_TEST_FAIL_SNAPSHOT", "1000000")],
        )
        .await;
        let restored = globals.restore(&mut admin).await;
        let log_path = both(r, restored)?;
        let log = std::fs::read_to_string(&log_path)?;
        ensure!(
            log.contains("injected snapshot fence failure"),
            "the snapshot fence was never ruled out, see {}",
            log_path.display()
        );
        ensure!(
            log.contains("snapshot fence unavailable"),
            "no debug about the ruled-out snapshot fence in {}",
            log_path.display()
        );
        Ok(())
    });
}

/// Oracle MySQL with GTIDs: the GTID wait is the fence, and it answers for every transaction
/// of the load (autocommit and XA) before the engine reads the sources.
#[test]
fn visibility_gtid_fence_exact() {
    let _serial = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
    scenario("visibility_gtid_fence_exact", |ctx| async move {
        if !opted_in(OPT_IN) {
            ctx.skip(OPT_IN_SKIP);
            return Ok(());
        }
        let db = ctx.new_db("vis_gtid").await?;
        let mut admin = db.conn().await?;
        let f = Fences::probe(&mut admin).await?;
        if !f.gtid_fence() {
            // Ruled out here means no GTIDs: the commit-order and snapshot tests cover those
            // flavors, and their fallback tests show what happens without an exact fence.
            ctx.log(format!(
                "the GTID wait does not apply here: {} runs on this flavor",
                f.covering_test()
            ));
            return Ok(());
        }
        let specs = vec![view()];
        let mut globals = Globals::set(&mut admin, &db.dsn, &widen_for(&f)).await?;
        let r = exact_run(&ctx, &db, &specs, "vis5").await;
        let restored = globals.restore(&mut admin).await;
        let log_path = both(r, restored)?;
        let log = std::fs::read_to_string(&log_path)?;
        ensure_exact(&log, &log_path, &f)?;
        Ok(())
    });
}

/// F7's per-flavor fence (#219): on every flavor, under the widened window, the engine's
/// dispatch must settle on this flavor's exact fence: the load is `visibility_commit_fence_exact`'s,
/// the views are exact at every check, and no fallback WARN appears (each flavor's own test
/// asserts more about the fence it owns; this one never skips, so every CI job runs at least
/// one fence test for real).
#[test]
fn visibility_fence_per_flavor() {
    let _serial = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
    scenario("visibility_fence_per_flavor", |ctx| async move {
        if !opted_in(OPT_IN) {
            ctx.skip(OPT_IN_SKIP);
            return Ok(());
        }
        let db = ctx.new_db("vis_flavor").await?;
        let mut admin = db.conn().await?;
        let f = Fences::probe(&mut admin).await?;
        // The decision is the flavor's (#257), so the server under test must be answered for what
        // it is: MariaDB's pair is a committed-transaction watermark, the MySQL family's (Oracle's
        // and Percona Server's) is the raw binlog write position, which covers transactions still
        // in flight and so cannot be the fence the engine waits on.
        let watermark = f.flavor().snapshot_position_is_watermark();
        ensure!(
            watermark == f.mariadb,
            "the flavor answer is wrong for {:?}: it says the consistent-snapshot position is{} \
             a committed-transaction watermark",
            f.flavor(),
            if watermark { "" } else { " not" }
        );
        ctx.log(format!(
            "expected fence here: {}",
            if f.gtid_fence() {
                "the GTID wait"
            } else if f.snapshot_fence() {
                "the consistent-snapshot position"
            } else if f.commit_fence() {
                "the commit-order row"
            } else {
                "none: only the delay fallback"
            }
        ));
        let specs = vec![view()];
        let mut globals = Globals::set(&mut admin, &db.dsn, &widen_for(&f)).await?;
        let r = exact_run(&ctx, &db, &specs, "vis6").await;
        let restored = globals.restore(&mut admin).await;
        let log_path = both(r, restored)?;
        let log = std::fs::read_to_string(&log_path)?;
        ensure_exact(&log, &log_path, &f)?;
        Ok(())
    });
}
