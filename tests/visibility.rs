//! Commit-order visibility fence (#28): Oracle MySQL without GTIDs. Readers must see every change
//! lazyview has read from the binlog before they read the sources. With `binlog_order_commits=ON`
//! the commit of lazyview's own fence write proves that; with `OFF` lazyview warns and falls back
//! to a fixed sleep, which this binary shows to be stale under the widened window below.
//!
//! The tests change server globals, so they run one at a time and restore the globals afterwards.
//! Only Oracle MySQL with `gtid_mode=OFF` runs them; every other flavor logs a SKIP line.

mod common;

use std::sync::Mutex;

use anyhow::{bail, ensure, Context, Result};
use common::*;
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

fn opted_in() -> bool {
    std::env::var(OPT_IN).map(|v| v == "1").unwrap_or(false)
}

/// `None`: Oracle MySQL with `gtid_mode=OFF`, where the commit fence applies. Otherwise the reason.
async fn fence_applies(conn: &mut Conn) -> Result<Option<String>> {
    let comment = scalar(conn, "SELECT @@version_comment")
        .await?
        .unwrap_or_default();
    let lower = comment.to_ascii_lowercase();
    if lower.contains("mariadb") || lower.contains("percona") {
        return Ok(Some(format!("not Oracle MySQL ({comment})")));
    }
    let gtid = scalar(conn, "SELECT @@GLOBAL.gtid_mode")
        .await?
        .unwrap_or_default();
    if gtid != "OFF" {
        return Ok(Some(format!(
            "gtid_mode is {gtid}, the commit fence needs OFF"
        )));
    }
    Ok(None)
}

/// A numeric value as a number (the server rejects a quoted string for an integer variable),
/// anything else as a string literal (`ON`, `OFF`).
fn lit(value: &str) -> String {
    if value.parse::<i64>().is_ok() {
        value.to_string()
    } else {
        q(value)
    }
}

/// Puts back `SET GLOBAL name = value` for each pair: every pair is tried, the errors are reported
/// together.
async fn apply(conn: &mut Conn, values: &[(String, String)]) -> Result<()> {
    let mut errs = Vec::new();
    for (name, value) in values {
        if let Err(e) = exec(conn, &format!("SET GLOBAL {name} = {}", lit(value))).await {
            errs.push(format!("{name}: {e:#}"));
        }
    }
    ensure!(errs.is_empty(), "SET GLOBAL failed: {}", errs.join("; "));
    Ok(())
}

/// Server globals saved before a test changed them. [`Globals::restore`] puts them back on the
/// normal path; `Drop` restores them from a thread of its own if the test ended early.
struct Globals {
    dsn: String,
    saved: Vec<(String, String)>,
    restored: bool,
}

impl Globals {
    /// Saves the current values of `changes`' names, then applies `changes`.
    async fn set(admin: &mut Conn, dsn: &str, changes: &[(&str, &str)]) -> Result<Globals> {
        let mut saved = Vec::new();
        for (name, _) in changes {
            let v = scalar(admin, &format!("SELECT @@GLOBAL.{name}"))
                .await?
                .with_context(|| format!("@@GLOBAL.{name} is NULL"))?;
            saved.push((name.to_string(), v));
        }
        let mut g = Globals {
            dsn: dsn.to_string(),
            saved,
            restored: false,
        };
        // In order, stopping at the first failure: a half-applied change set is put back from
        // `saved` and the test fails with the SET error.
        for (name, value) in changes {
            if let Err(e) = exec(admin, &format!("SET GLOBAL {name} = {}", lit(value))).await {
                let _ = g.restore(admin).await;
                return Err(e.context(format!("SET GLOBAL {name}")));
            }
        }
        Ok(g)
    }

    async fn restore(&mut self, admin: &mut Conn) -> Result<()> {
        apply(admin, &self.saved).await?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for Globals {
    fn drop(&mut self) {
        if self.restored {
            return;
        }
        let (dsn, values) = (self.dsn.clone(), self.saved.clone());
        // Drop cannot await: restore on a runtime of this thread (never nested in the test's).
        let res = std::thread::spawn(move || -> Result<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async {
                let mut c = connect(&dsn).await?;
                apply(&mut c, &values).await
            })
        })
        .join();
        match res {
            Ok(Ok(())) => eprintln!("globals restored on drop"),
            _ => eprintln!(
                "GLOBALS NOT RESTORED, run by hand: {}",
                self.saved
                    .iter()
                    .map(|(n, v)| format!("SET GLOBAL {n} = {};", lit(v)))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        }
    }
}

/// The test's result, or the first error: the run's, then the globals restore's.
fn both<T>(run: Result<T>, restored: Result<()>) -> Result<T> {
    let v = run?;
    restored?;
    Ok(v)
}

/// Creates the source table and the view, starts an engine (batch window 0, so every change is
/// refreshed at once) and waits until the view is ready.
async fn start(ctx: &Ctx, db: &Db, specs: &[ViewSpec]) -> Result<(Conn, Engine)> {
    let mut c = db.conn().await?;
    for s in DDL {
        exec(&mut c, s).await?;
    }
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, specs).await?;
    let mut eng = ctx.start_engine(db, &[("LAZYVIEW_BATCH_WINDOW_MS", "0")])?;
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    wait_ready(&mut c, &mut eng, &names, t_ready()).await?;
    verify(ctx, &mut eng, db, specs, "initial").await?;
    Ok((c, eng))
}

/// Ten single-row autocommit changes, each checked against the oracle, then one XA transaction.
/// Returns the engine's log file.
async fn exact_run(ctx: &Ctx, db: &Db, specs: &[ViewSpec]) -> Result<std::path::PathBuf> {
    let (mut c, mut eng) = start(ctx, db, specs).await?;
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
            "XA START 'vis28'",
            "UPDATE t SET v = v + 100 WHERE id = 2",
            "XA END 'vis28'",
            "XA PREPARE 'vis28'",
            "XA COMMIT 'vis28'",
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
async fn off_run(ctx: &Ctx, db: &Db, specs: &[ViewSpec]) -> Result<(usize, std::path::PathBuf)> {
    let (mut c, mut eng) = start(ctx, db, specs).await?;
    let mut stale = 0usize;
    for i in 0..10 {
        exec(
            &mut c,
            &format!("UPDATE t SET v = v + 1 WHERE id = {}", 1 + i % 4),
        )
        .await?;
        match verify(ctx, &mut eng, db, specs, &format!("OFF update {i}")).await {
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
        if !opted_in() {
            ctx.skip(OPT_IN_SKIP);
            return Ok(());
        }
        let db = ctx.new_db("vis_exact").await?;
        let mut admin = db.conn().await?;
        if let Some(why) = fence_applies(&mut admin).await? {
            ctx.skip(&why);
            return Ok(());
        }
        let specs = vec![view()];
        let mut globals = Globals::set(&mut admin, &db.dsn, &WIDEN).await?;
        let r = exact_run(&ctx, &db, &specs).await;
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
            if !opted_in() {
                ctx.skip(OPT_IN_SKIP);
                return Ok(());
            }
            let db = ctx.new_db("vis_off").await?;
            let mut admin = db.conn().await?;
            if let Some(why) = fence_applies(&mut admin).await? {
                ctx.skip(&why);
                return Ok(());
            }
            let specs = vec![view()];
            let mut changes = WIDEN.to_vec();
            changes.push(("binlog_order_commits", "OFF"));
            let mut globals = Globals::set(&mut admin, &db.dsn, &changes).await?;
            let r = off_run(&ctx, &db, &specs).await;
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
