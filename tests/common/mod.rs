//! Integration-test harness for lazyview.
//!
//! * Servers come from `LAZYVIEW_TEST_DSNS` (`name=mysql://...,name2=mysql://...`, e.g. the output
//!   of `scripts/test-servers.sh dsns`). If unset every scenario passes with a printed note.
//!   Multi-server scenarios ([`scenario_multi`]) skip visibly with too few servers, or fail when
//!   `CI` is set or `LAZYVIEW_TEST_REQUIRE_MULTI=1` (`=0` allows the skip in CI too).
//! * Every scenario runs against every listed flavor (in parallel, one tokio task per flavor);
//!   each run works in its own fresh schemas (`lvt_<tag>_<random>`), dropped on success and kept
//!   on failure for debugging.
//! * Engines run in-process, each on its own dedicated tokio runtime/thread so that a "crash"
//!   (dropping the runtime) really kills every task the engine spawned. Engine logs go to
//!   `tmp/out/test-logs/<schema>/engine-<n>.log` (kept on failure).
//! * Seeds: `LAZYVIEW_TEST_SEED` (default: random, printed on failure). Size: `LAZYVIEW_TEST_SCALE`
//!   (float, default 1). Per-run timeout: `LAZYVIEW_TEST_TIMEOUT_S` (default 900 × scale).
//! * `LAZYVIEW_TEST_ENGINE_USER=user:password` runs every engine as that account instead of the
//!   admin account of the DSN (setup and oracle queries stay on the admin account). CI sets it to
//!   `lvdoc:doc`, the account `docker/initdb` creates with exactly the grants of
//!   docs/server-setup.md, so every suite proves the documented privileges are enough.
//! * Engine log filter: `LAZYVIEW_TEST_LOG` (falls back to `RUST_LOG`, default `lazyview=debug,warn`).

#![allow(dead_code, unused_imports)]

pub mod oracle;
pub mod typed;
pub mod workload;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use lazyview::binlog::Position;
use lazyview::{Config, Metrics, Snapshot};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Row, Value};
use rand::RngExt;
use tokio::sync::{oneshot, watch};

pub use oracle::{check_views, check_views_targets, Kind, ViewSpec};

// ---------------------------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Server {
    /// Flavor label from `LAZYVIEW_TEST_DSNS`, e.g. `mysql84`.
    pub flavor: String,
    /// DSN as given (points at some existing schema; only host/credentials/options are reused).
    pub base_dsn: String,
}

impl Server {
    pub fn is_mariadb(&self) -> bool {
        self.flavor.to_ascii_lowercase().contains("maria")
    }
}

/// Servers listed in `LAZYVIEW_TEST_DSNS`, or `None` when unset/empty.
pub fn servers() -> Option<Vec<Server>> {
    let raw = std::env::var("LAZYVIEW_TEST_DSNS").ok()?;
    let mut out = Vec::new();
    for (i, part) in raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .enumerate()
    {
        let (flavor, dsn) = match part.split_once('=') {
            Some((n, d)) if !n.contains("://") => (n.trim().to_string(), d.trim().to_string()),
            _ => (format!("srv{i}"), part.to_string()),
        };
        out.push(Server {
            flavor,
            base_dsn: dsn,
        });
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// `LAZYVIEW_TEST_SCALE` (default 1.0).
pub fn scale() -> f64 {
    std::env::var("LAZYVIEW_TEST_SCALE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(1.0)
}

/// `n` scaled by `LAZYVIEW_TEST_SCALE`, at least 1.
pub fn scaled(n: usize) -> usize {
    ((n as f64 * scale()).round() as usize).max(1)
}

/// Base seed: `LAZYVIEW_TEST_SEED` or a random one (the same for every flavor of a run).
pub fn base_seed() -> u64 {
    static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SEED.get_or_init(|| match std::env::var("LAZYVIEW_TEST_SEED") {
        Ok(v) => v.trim().parse().expect("LAZYVIEW_TEST_SEED must be a u64"),
        Err(_) => rand::rng().random::<u64>() % 1_000_000_000_000,
    })
}

/// Applies the diagnostic view filters: `LAZYVIEW_TEST_SKIP_VIEWS` (comma separated names to
/// drop, e.g. to look past a known engine bug in one view) and `LAZYVIEW_TEST_ONLY_VIEWS` (keep
/// only these, for minimal reproductions). Prints what was filtered.
pub fn skip_views(specs: Vec<ViewSpec>) -> Vec<ViewSpec> {
    let list = |var: &str| -> Option<Vec<String>> {
        std::env::var(var).ok().map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
    };
    let skip = list("LAZYVIEW_TEST_SKIP_VIEWS").unwrap_or_default();
    let only = list("LAZYVIEW_TEST_ONLY_VIEWS");
    let (kept, gone): (Vec<ViewSpec>, Vec<ViewSpec>) = specs.into_iter().partition(|s| {
        !skip.contains(&s.name) && only.as_ref().is_none_or(|o| o.contains(&s.name))
    });
    if !gone.is_empty() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            println!(
                "NOTE: LAZYVIEW_TEST_SKIP_VIEWS / LAZYVIEW_TEST_ONLY_VIEWS filter out views {:?}",
                gone.iter().map(|s| s.name.clone()).collect::<Vec<_>>()
            )
        });
    }
    kept
}

/// Deterministic seed derivation (splitmix64 over the base seed and a label).
pub fn derive_seed(base: u64, label: &str) -> u64 {
    let mut h = base ^ 0x9e37_79b9_7f4a_7c15;
    for b in label.bytes() {
        h = splitmix(h ^ b as u64);
    }
    splitmix(h)
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn repo_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn logs_root() -> PathBuf {
    repo_dir().join("tmp/out/test-logs")
}

fn run_timeout() -> Duration {
    let secs = std::env::var("LAZYVIEW_TEST_TIMEOUT_S")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(900.0 * scale().max(1.0));
    Duration::from_secs_f64(secs)
}

/// Replaces the schema (path) of a `mysql://` DSN, keeping credentials, host and `?options`.
/// `dsn` with its credentials replaced by `user:password`.
pub fn dsn_with_user(dsn: &str, user_pass: &str) -> String {
    let (scheme, rest) = dsn.split_once("://").unwrap_or(("mysql", dsn));
    let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    format!("{scheme}://{user_pass}@{host}")
}

pub fn dsn_with_db(dsn: &str, db: &str) -> String {
    let (scheme, rest) = dsn.split_once("://").unwrap_or(("mysql", dsn));
    let (before_q, query) = match rest.find('?') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let authority = match before_q.find('/') {
        Some(i) => &before_q[..i],
        None => before_q,
    };
    format!("{scheme}://{authority}/{db}{query}")
}

// ---------------------------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------------------------

pub async fn connect(dsn: &str) -> Result<Conn> {
    let opts = Opts::from_url(dsn).with_context(|| format!("bad DSN {dsn}"))?;
    let mut last = None;
    for attempt in 0..5 {
        match Conn::new(opts.clone()).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
            }
        }
    }
    Err(anyhow!(last.unwrap())).with_context(|| format!("connect {}", redact(dsn)))
}

pub fn redact(dsn: &str) -> String {
    match (dsn.find("://"), dsn.find('@')) {
        (Some(s), Some(a)) if a > s => format!("{}://***{}", &dsn[..s], &dsn[a..]),
        _ => dsn.to_string(),
    }
}

/// Executes one (or several `;`-free) statement(s), discarding results.
pub async fn exec(conn: &mut Conn, sql: &str) -> Result<()> {
    conn.query_drop(sql)
        .await
        .with_context(|| format!("SQL failed: {}", clip(sql, 600)))
}

/// Executes statements in order.
pub async fn exec_all(conn: &mut Conn, stmts: &[String]) -> Result<()> {
    for s in stmts {
        exec(conn, s).await?;
    }
    Ok(())
}

/// Rows as text (NULL → None); binary values are hex-escaped.
pub async fn rows(conn: &mut Conn, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let rows: Vec<Row> = conn
        .query(sql)
        .await
        .with_context(|| format!("SQL failed: {}", clip(sql, 600)))?;
    Ok(rows
        .into_iter()
        .map(|r| r.unwrap().into_iter().map(|v| value_text(&v)).collect())
        .collect())
}

/// First column of the first row as text (`None` if no row or NULL).
pub async fn scalar(conn: &mut Conn, sql: &str) -> Result<Option<String>> {
    Ok(rows(conn, sql)
        .await?
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next().flatten()))
}

pub async fn scalar_i64(conn: &mut Conn, sql: &str) -> Result<i64> {
    let v = scalar(conn, sql)
        .await?
        .ok_or_else(|| anyhow!("no value for: {sql}"))?;
    v.parse::<i64>()
        .with_context(|| format!("not an integer: {v} (from {sql})"))
}

pub fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(match std::str::from_utf8(b) {
            Ok(s) => s.to_string(),
            Err(_) => format!("0x{}", hex::encode(b)),
        }),
        other => Some(other.as_sql(true)),
    }
}

/// Server error code of a mysql_async error inside an anyhow chain.
pub fn server_code(e: &anyhow::Error) -> Option<u16> {
    for cause in e.chain() {
        if let Some(mysql_async::Error::Server(s)) = cause.downcast_ref::<mysql_async::Error>() {
            return Some(s.code);
        }
    }
    None
}

/// Quotes a string literal (backslash-escape mode, the server default).
pub fn q(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\0' => out.push_str("\\0"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\x1a' => out.push_str("\\Z"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

pub fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

pub fn clip(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut end = n;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

/// The server's (major, minor) version, e.g. (10, 11) for `10.11.19-MariaDB-log`.
pub async fn server_version(conn: &mut Conn) -> Result<(u32, u32)> {
    let version = scalar(conn, "SELECT VERSION()").await?.unwrap_or_default();
    let mut parts = version
        .split(|ch: char| !ch.is_ascii_digit())
        .map(|p| p.parse::<u32>().unwrap_or(0));
    Ok((parts.next().unwrap_or(0), parts.next().unwrap_or(0)))
}

/// Server's current binlog end (`SHOW BINARY LOG STATUS`, fallback `SHOW MASTER STATUS`).
pub async fn server_position(conn: &mut Conn) -> Result<Position> {
    let mut res: Result<Vec<Row>> = conn
        .query("SHOW BINARY LOG STATUS")
        .await
        .map_err(Into::into);
    if res.is_err() {
        res = conn.query("SHOW MASTER STATUS").await.map_err(Into::into);
    }
    let rows = res.context("reading binlog position")?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("binary logging is disabled (no status row)"))?;
    let file: String = row.get("File").ok_or_else(|| anyhow!("no File column"))?;
    let pos: u64 = row
        .get("Position")
        .ok_or_else(|| anyhow!("no Position column"))?;
    Ok(Position { file, pos })
}

pub async fn table_exists(conn: &mut Conn, table: &str) -> Result<bool> {
    let n = scalar_i64(
        conn,
        &format!(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {}",
            q(table)
        ),
    )
    .await?;
    Ok(n > 0)
}

pub async fn table_type(conn: &mut Conn, table: &str) -> Result<Option<String>> {
    scalar(
        conn,
        &format!(
            "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {}",
            q(table)
        ),
    )
    .await
}

pub async fn count(conn: &mut Conn, table: &str) -> Result<i64> {
    scalar_i64(conn, &format!("SELECT COUNT(*) FROM {}", ident(table))).await
}

// ---------------------------------------------------------------------------------------------
// Databases (fresh schemas)
// ---------------------------------------------------------------------------------------------

/// A fresh schema on one server.
#[derive(Debug, Clone)]
pub struct Db {
    pub flavor: String,
    pub server_idx: usize,
    pub name: String,
    /// DSN pointing at this schema (used for the engine and test connections).
    pub dsn: String,
}

impl Db {
    pub async fn conn(&self) -> Result<Conn> {
        connect(&self.dsn).await
    }
}

/// Lowercase alnum/underscore tag, max `n` chars.
fn tag_clean(tag: &str, n: usize) -> String {
    tag.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(n)
        .collect()
}

async fn create_db(server: &Server, server_idx: usize, tag: &str) -> Result<Db> {
    let suffix: String = {
        let mut r = rand::rng();
        (0..6)
            .map(|_| (b'a' + r.random_range(0..26u8)) as char)
            .collect()
    };
    let name = format!("lvt_{}_{}", tag_clean(tag, 40), suffix);
    let mut c = connect(&server.base_dsn)
        .await
        .with_context(|| format!("[{}] admin connection", server.flavor))?;
    exec(
        &mut c,
        &format!(
            "CREATE DATABASE {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            ident(&name)
        ),
    )
    .await?;
    c.disconnect().await.ok();
    Ok(Db {
        flavor: server.flavor.clone(),
        server_idx,
        dsn: dsn_with_db(&server.base_dsn, &name),
        name,
    })
}

async fn drop_db(server: &Server, name: &str) -> Result<()> {
    let mut c = connect(&server.base_dsn).await?;
    exec(&mut c, &format!("DROP DATABASE IF EXISTS {}", ident(name))).await?;
    c.disconnect().await.ok();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------------------------

/// Per-run context: one primary server (the binlog source), optionally peers (targets).
pub struct Ctx {
    pub test: String,
    /// `servers[0]` is the source; the rest are peers (used as MV targets).
    pub servers: Vec<Server>,
    pub seed: u64,
    started: Instant,
    dbs: Mutex<Vec<Db>>,
    engine_seq: AtomicU32,
    /// Notes printed on failure (e.g. sub-seeds of the failing combination).
    notes: Mutex<Vec<String>>,
}

impl Ctx {
    pub fn flavor(&self) -> &str {
        &self.servers[0].flavor
    }

    pub fn server(&self) -> &Server {
        &self.servers[0]
    }

    pub fn label(&self) -> String {
        self.servers
            .iter()
            .map(|s| s.flavor.as_str())
            .collect::<Vec<_>>()
            .join("→")
    }

    /// Progress line `[test/flavor +1.2s] msg` (shown with --nocapture or on failure).
    pub fn log(&self, msg: impl AsRef<str>) {
        println!(
            "[{}/{} +{:.1}s] {}",
            self.test,
            self.label(),
            self.started.elapsed().as_secs_f64(),
            msg.as_ref()
        );
    }

    pub fn note(&self, msg: impl Into<String>) {
        self.notes.lock().unwrap().push(msg.into());
    }

    /// Reports that this scenario (or a part of it) does not run on this flavor: the line goes to
    /// the real stderr, past the test harness's capture, so it shows for passing tests too.
    pub fn skip(&self, why: &str) {
        use std::io::Write as _;
        let _ = writeln!(
            std::io::stderr(),
            "SKIP {} on {}: {why}",
            self.test,
            self.label()
        );
    }

    /// Seed for a sub-step, derived from the run seed.
    pub fn sub_seed(&self, label: &str) -> u64 {
        derive_seed(self.seed, label)
    }

    /// Creates a fresh schema on the source server.
    pub async fn new_db(&self, tag: &str) -> Result<Db> {
        self.new_db_on(0, tag).await
    }

    /// Creates a fresh schema on `servers[idx]`.
    pub async fn new_db_on(&self, idx: usize, tag: &str) -> Result<Db> {
        let db = create_db(&self.servers[idx], idx, tag).await?;
        self.log(format!("schema {} on {}", db.name, db.flavor));
        self.dbs.lock().unwrap().push(db.clone());
        Ok(db)
    }

    /// Starts an engine for `db` (the source schema) with test defaults plus `overrides`.
    pub fn start_engine(&self, db: &Db, overrides: &[(&str, &str)]) -> Result<Engine> {
        let n = self.engine_seq.fetch_add(1, Ordering::SeqCst);
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        let engine_dsn = match std::env::var("LAZYVIEW_TEST_ENGINE_USER") {
            Ok(u) if !u.is_empty() => dsn_with_user(&db.dsn, &u),
            _ => db.dsn.clone(),
        };
        env.insert("LAZYVIEW_DSN".into(), engine_dsn);
        env.insert("LAZYVIEW_SERVER_ID".into(), unique_server_id().to_string());
        env.insert("LAZYVIEW_INSTANCE_ID".into(), "test".into());
        env.insert("LAZYVIEW_BATCH_WINDOW_MS".into(), "50".into());
        env.insert("LAZYVIEW_HEARTBEAT_MS".into(), "1000".into());
        // Many engines run in one process: no HTTP endpoint unless a test asks for one.
        env.insert("LAZYVIEW_HEALTH_ADDR".into(), "off".into());
        // Full-mode / unknown-unit rebuilds immediately (the default cadence would leave them
        // stale at check time); efficiency_full_refresh_cadence tests the cadence itself.
        env.insert(
            "LAZYVIEW_FULL_REFRESH_MS".into(),
            test_full_refresh_ms().to_string(),
        );
        // The supervisor mode of a CI leg (`LAZYVIEW_SUPERVISOR=off`) reaches every engine unless
        // a test sets its own.
        if let Ok(v) = std::env::var("LAZYVIEW_SUPERVISOR") {
            env.insert("LAZYVIEW_SUPERVISOR".into(), v);
        }
        for (k, v) in overrides {
            env.insert(k.to_string(), v.to_string());
        }
        let cfg = Config::from_lookup(|k| env.get(k).cloned()).context("engine config")?;
        let dir = logs_root().join(&db.name);
        std::fs::create_dir_all(&dir).ok();
        let log_path = dir.join(format!("engine-{n}.log"));
        let label = format!("{}#{n}", db.flavor);
        let shown: Vec<String> = overrides.iter().map(|(k, v)| format!("{k}={v}")).collect();
        self.log(format!(
            "start engine {label} server_id={} {} log={}",
            cfg.server_id,
            shown.join(" "),
            log_path.display()
        ));
        Engine::spawn(cfg, label, log_path)
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
}

fn unique_server_id() -> u32 {
    // Random ids: unique across concurrently running engines (also across other test processes).
    rand::rng().random_range(1_000_000..4_000_000_000u32)
}

/// Runs `f` against every flavor in `LAZYVIEW_TEST_DSNS` (in parallel) and panics with a summary
/// if any flavor failed.
pub fn scenario<F, Fut>(test: &str, f: F)
where
    F: Fn(Arc<Ctx>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let Some(servers) = servers() else {
        println!("SKIP {test}: LAZYVIEW_TEST_DSNS is not set (e.g. export LAZYVIEW_TEST_DSNS=$(scripts/test-servers.sh dsns))");
        return;
    };
    let groups: Vec<Vec<Server>> = servers.into_iter().map(|s| vec![s]).collect();
    run_groups(test, groups, f);
}

/// Runs `f` once per source flavor with `n_peers` other flavors as peers (rotating through the
/// list: source i gets peers i+1, i+2, …). With fewer than `n_peers + 1` servers listed the
/// scenario is skipped (see [`skip_multi`]): a visible, counted `SKIP` line, or a failure when
/// skipping is not allowed (`CI=true` without `LAZYVIEW_TEST_REQUIRE_MULTI=0`, or
/// `LAZYVIEW_TEST_REQUIRE_MULTI=1`).
pub fn scenario_multi<F, Fut>(test: &str, n_peers: usize, f: F)
where
    F: Fn(Arc<Ctx>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let servers = servers().unwrap_or_default();
    if servers.len() < n_peers + 1 {
        skip_multi(test, n_peers + 1, servers.len());
        return;
    }
    let n = servers.len();
    let groups: Vec<Vec<Server>> = (0..n)
        .map(|i| {
            (0..=n_peers)
                .map(|k| servers[(i + k) % n].clone())
                .collect()
        })
        .collect();
    run_groups(test, groups, f);
}

/// Multi-server scenarios skipped so far in this test binary, with what they needed (see
/// [`skip_multi`]).
static MULTI_SKIPPED: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Number of multi-server scenarios skipped so far in this test binary.
pub fn multi_skipped() -> usize {
    MULTI_SKIPPED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .len()
}

/// Repeats the skipped multi-server scenarios after the test harness's own summary, which counts
/// them as passed. Runs at process exit (registered with `atexit` by the first skip).
extern "C" fn print_multi_skipped() {
    let skipped = MULTI_SKIPPED.lock().unwrap_or_else(|e| e.into_inner());
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr(),
        "SKIPPED {} multi-server scenario(s), counted as passed above: {}",
        skipped.len(),
        skipped.join(", ")
    );
}

/// Whether a multi-server scenario may be skipped for lack of servers: never with
/// `LAZYVIEW_TEST_REQUIRE_MULTI=1`, always with `LAZYVIEW_TEST_REQUIRE_MULTI=0`, otherwise only
/// outside CI (`CI` unset, empty, `0` or `false`).
pub fn multi_skip_allowed() -> bool {
    let truthy = |v: &str| !matches!(v.trim(), "" | "0" | "false" | "FALSE" | "no");
    match std::env::var("LAZYVIEW_TEST_REQUIRE_MULTI") {
        Ok(v) if !v.trim().is_empty() => !truthy(&v),
        _ => !std::env::var("CI").is_ok_and(|v| truthy(&v)),
    }
}

/// Records that the multi-server scenario `test` cannot run with `have` servers: the line goes
/// to the real stderr (past the test harness's capture, so it shows for passing tests too) and
/// is counted in [`MULTI_SKIPPED`], which is printed once more at exit; the scenario fails
/// instead when skipping is not allowed.
pub fn skip_multi(test: &str, need: usize, have: usize) {
    let msg = format!(
        "needs {need} servers in LAZYVIEW_TEST_DSNS, have {have} \
         (e.g. export LAZYVIEW_TEST_DSNS=$(scripts/test-servers.sh dsns))"
    );
    if !multi_skip_allowed() {
        panic!("{test}: {msg}; multi-server scenarios must run here (CI=true or LAZYVIEW_TEST_REQUIRE_MULTI=1)");
    }
    let n = {
        let mut skipped = MULTI_SKIPPED.lock().unwrap_or_else(|e| e.into_inner());
        skipped.push(format!("{test} (needs {need}, have {have})"));
        if skipped.len() == 1 {
            extern "C" {
                fn atexit(f: extern "C" fn()) -> std::os::raw::c_int;
            }
            // SAFETY: atexit only records the function pointer; the callback does not unwind.
            unsafe { atexit(print_multi_skipped) };
        }
        skipped.len()
    };
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr(),
        "SKIP {test}: {msg} [multi-server scenario {n} skipped in this binary; LAZYVIEW_TEST_REQUIRE_MULTI=1 fails instead]"
    );
}

fn run_groups<F, Fut>(test: &str, groups: Vec<Vec<Server>>, f: F)
where
    F: Fn(Arc<Ctx>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let seed = base_seed();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("test runtime");
    let f = Arc::new(f);
    let test_name = test.to_string();
    let failures: Vec<String> = rt.block_on(async move {
        let mut handles = Vec::new();
        for servers in groups {
            let ctx = Arc::new(Ctx {
                test: test_name.clone(),
                servers,
                seed: derive_seed(seed, &test_name),
                started: Instant::now(),
                dbs: Mutex::new(Vec::new()),
                engine_seq: AtomicU32::new(0),
                notes: Mutex::new(Vec::new()),
            });
            let f = f.clone();
            let label = ctx.label();
            let h = tokio::spawn(async move {
                let fut = f(ctx.clone());
                let res = match tokio::time::timeout(run_timeout(), fut).await {
                    Ok(r) => r,
                    Err(_) => Err(anyhow!("scenario timed out after {:?}", run_timeout())),
                };
                finish_run(&ctx, res).await
            });
            handles.push((label, h));
        }
        let mut failures = Vec::new();
        for (label, h) in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(msg)) => failures.push(msg),
                Err(e) => failures.push(format!(
                    "[{label}] scenario task panicked: {}",
                    join_panic_msg(e)
                )),
            }
        }
        failures
    });
    rt.shutdown_timeout(Duration::from_secs(5));
    if !failures.is_empty() {
        panic!(
            "{} failed on {} flavor run(s) (base seed {seed}; rerun with LAZYVIEW_TEST_SEED={seed}):\n\n{}",
            test,
            failures.len(),
            failures.join("\n\n")
        );
    }
}

async fn finish_run(ctx: &Ctx, res: Result<()>) -> std::result::Result<(), String> {
    let dbs = ctx.dbs.lock().unwrap().clone();
    match res {
        Ok(()) => {
            // Engines are stopped (or killed via Engine::drop) by now; give killed ones a moment.
            tokio::time::sleep(Duration::from_millis(100)).await;
            for db in &dbs {
                if let Err(e) = drop_db(&ctx.servers[db.server_idx], &db.name).await {
                    println!(
                        "[{}] warning: could not drop {}: {e:#}",
                        ctx.label(),
                        db.name
                    );
                }
                std::fs::remove_dir_all(logs_root().join(&db.name)).ok();
            }
            ctx.log(format!("OK in {:.1}s", ctx.elapsed().as_secs_f64()));
            Ok(())
        }
        Err(e) => {
            let mut msg = format!(
                "[{} on {}] FAILED after {:.1}s: {e:#}",
                ctx.test,
                ctx.label(),
                ctx.elapsed().as_secs_f64()
            );
            let notes = ctx.notes.lock().unwrap().clone();
            if !notes.is_empty() {
                let _ = write!(msg, "\n  notes: {}", notes.join("; "));
            }
            for db in &dbs {
                let _ = write!(
                    msg,
                    "\n  kept schema `{}` on {} ({}); engine logs: {}",
                    db.name,
                    db.flavor,
                    redact(&db.dsn),
                    logs_root().join(&db.name).display()
                );
            }
            println!("{msg}");
            Err(msg)
        }
    }
}

fn join_panic_msg(e: tokio::task::JoinError) -> String {
    if e.is_panic() {
        panic_msg(e.into_panic())
    } else {
        e.to_string()
    }
}

fn panic_msg(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic>".to_string()
    }
}

// ---------------------------------------------------------------------------------------------
// Engine handle
// ---------------------------------------------------------------------------------------------

/// An engine running `lazyview::engine::run` on its own runtime + thread.
///
/// Dropping the handle kills the engine (like `kill -9`: its futures are dropped, its runtime shut
/// down, open DB transactions are rolled back by the server).
pub struct Engine {
    pub label: String,
    pub metrics: Arc<Metrics>,
    pub cfg: Config,
    pub log_path: PathBuf,
    shutdown: watch::Sender<bool>,
    kill: Option<oneshot::Sender<()>>,
    done: oneshot::Receiver<std::result::Result<(), String>>,
    exit: Option<std::result::Result<(), String>>,
}

impl Engine {
    fn spawn(cfg: Config, label: String, log_path: PathBuf) -> Result<Engine> {
        let metrics = Arc::new(Metrics::default());
        let (sd_tx, sd_rx) = watch::channel(false);
        let (kill_tx, kill_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = oneshot::channel();
        let file = Arc::new(
            std::fs::File::create(&log_path)
                .with_context(|| format!("create {}", log_path.display()))?,
        );
        let filter = std::env::var("LAZYVIEW_TEST_LOG")
            .or_else(|_| std::env::var("RUST_LOG"))
            .unwrap_or_else(|_| "lazyview=debug,warn".into());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(file)
            .with_ansi(false)
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let m = metrics.clone();
        let run_cfg = cfg.clone();
        let thread_label = label.clone();
        std::thread::Builder::new()
            .name(format!("lv-engine-{label}"))
            .spawn(move || {
                let _guard = tracing::dispatcher::set_default(&dispatch);
                let d2 = dispatch.clone();
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name(format!("lv-{thread_label}"))
                    .on_thread_start(move || {
                        // Thread-local subscriber for the whole life of the worker thread.
                        std::mem::forget(tracing::dispatcher::set_default(&d2));
                    })
                    .build()
                    .expect("engine runtime");
                let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(async move {
                        tokio::select! {
                            r = lazyview::engine::run(run_cfg, m, sd_rx) => Some(r),
                            _ = kill_rx => None,
                        }
                    })
                }));
                rt.shutdown_timeout(Duration::from_secs(2));
                let out = match res {
                    Ok(Some(Ok(()))) => Ok(()),
                    Ok(Some(Err(e))) => Err(format!("engine returned error: {e:#}")),
                    Ok(None) => Err("engine killed".to_string()),
                    Err(p) => Err(format!("engine panicked: {}", panic_msg(p))),
                };
                let _ = done_tx.send(out);
            })
            .context("spawn engine thread")?;
        Ok(Engine {
            label,
            metrics,
            cfg,
            log_path,
            shutdown: sd_tx,
            kill: Some(kill_tx),
            done: done_rx,
            exit: None,
        })
    }

    /// `Some(result)` once the engine has exited.
    pub fn exited(&mut self) -> Option<&std::result::Result<(), String>> {
        if self.exit.is_none() {
            match self.done.try_recv() {
                Ok(r) => self.exit = Some(r),
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.exit = Some(Err("engine thread vanished".into()))
                }
            }
        }
        self.exit.as_ref()
    }

    /// Errors if the engine has exited (for any reason).
    pub fn check_alive(&mut self) -> Result<()> {
        let label = self.label.clone();
        let log = self.log_path.display().to_string();
        match self.exited() {
            None => Ok(()),
            Some(Ok(())) => bail!("engine {label} exited unexpectedly (Ok) — log {log}"),
            Some(Err(e)) => bail!("engine {label} died: {e} — log {log}"),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.metrics.snapshot()
    }

    pub fn applied(&self) -> Option<Position> {
        self.metrics.applied()
    }

    async fn wait_exit(&mut self, timeout: Duration) -> Result<std::result::Result<(), String>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(r) = self.exited() {
                return Ok(r.clone());
            }
            if Instant::now() > deadline {
                bail!(
                    "engine {} did not exit within {timeout:?} — log {}",
                    self.label,
                    self.log_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Graceful shutdown; errors if the engine failed or does not stop within 60 s.
    pub async fn stop(mut self) -> Result<()> {
        let _ = self.shutdown.send(true);
        match self.wait_exit(Duration::from_secs(60)).await? {
            Ok(()) => Ok(()),
            Err(e) => bail!(
                "engine {} stopped with error: {e} — log {}",
                self.label,
                self.log_path.display()
            ),
        }
    }

    /// Waits for the engine to exit on its own (`once` / `LAZYVIEW_SYNC=false`); returns its result.
    pub async fn wait_finished(
        mut self,
        timeout: Duration,
    ) -> Result<std::result::Result<(), String>> {
        self.wait_exit(timeout).await
    }

    /// Simulates a crash: drops the engine's futures and runtime at an arbitrary await point.
    pub async fn crash(mut self) -> Result<()> {
        if let Some(k) = self.kill.take() {
            let _ = k.send(());
        }
        match self.wait_exit(Duration::from_secs(30)).await? {
            Err(e) if e == "engine killed" => Ok(()),
            Err(e) => bail!(
                "engine {} had already failed before the crash: {e} — log {}",
                self.label,
                self.log_path.display()
            ),
            Ok(()) => bail!(
                "engine {} had already exited (Ok) before the crash — log {}",
                self.label,
                self.log_path.display()
            ),
        }
    }

    /// Waits until `applied() >= target`.
    pub async fn wait_applied(&mut self, target: &Position, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(a) = self.metrics.applied() {
                if &a >= target {
                    return Ok(());
                }
            }
            self.check_alive()?;
            if Instant::now() > deadline {
                bail!(
                    "engine {} not caught up within {timeout:?}: applied={:?} target={target} metrics={:?} — log {}",
                    self.label,
                    self.metrics.applied().map(|p| p.to_string()),
                    self.metrics.snapshot(),
                    self.log_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    /// Captures the source's binlog end and waits until the engine applied it, `rounds` times
    /// (MV writes advance the binlog; chained views need one extra round per chain level).
    pub async fn wait_caught_up_rounds(
        &mut self,
        src: &mut Conn,
        timeout: Duration,
        rounds: usize,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        for _ in 0..rounds.max(1) {
            let target = server_position(src).await?;
            let left = deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            self.wait_applied(&target, left).await?;
        }
        Ok(())
    }

    /// `wait_caught_up_rounds` with 3 rounds (enough for one level of chained views + margin).
    pub async fn wait_caught_up(&mut self, src: &mut Conn, timeout: Duration) -> Result<()> {
        self.wait_caught_up_rounds(src, timeout, 3).await
    }

    /// Update statistics of view `view` of `db`, as `GET /stats` reports them (src/stats.rs):
    /// folded at the end of every flush, so after a catch-up they include everything applied.
    /// `None` until a flush listed the view.
    pub fn view_stats(&self, db: &Db, view: &str) -> Option<ViewStats> {
        self.metrics.view_stats.view(&format!("{}.{view}", db.name))
    }

    /// [`Self::view_stats`], failing when the view is not listed.
    pub fn stats_of(&self, db: &Db, view: &str) -> Result<ViewStats> {
        self.view_stats(db, view)
            .with_context(|| format!("view `{view}` is not listed in /stats"))
    }
}

pub use lazyview::stats::ViewSnapshot as ViewStats;

/// A free local TCP address for an engine's `LAZYVIEW_HEALTH_ADDR`.
pub fn free_local_addr() -> Result<String> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    Ok(format!("127.0.0.1:{port}"))
}

/// `GET <path>` on an engine's health endpoint (`addr` as given in `LAZYVIEW_HEALTH_ADDR`):
/// status and body.
pub async fn http_get(addr: &str, path: &str) -> Result<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .context("connect timeout")??;
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .context("read timeout")??;
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .with_context(|| format!("bad HTTP response {text:?}"))?;
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .with_context(|| format!("bad status line in {head:?}"))?;
    Ok((status, body.to_string()))
}

/// `GET /stats` of an engine started with `LAZYVIEW_HEALTH_ADDR=addr`, as JSON.
pub async fn fetch_stats(addr: &str) -> Result<serde_json::Value> {
    let (status, body) = http_get(addr, "/stats").await?;
    if status != 200 {
        bail!("GET {addr}/stats: status {status}: {body}");
    }
    serde_json::from_str(&body).with_context(|| format!("GET {addr}/stats: not JSON: {body}"))
}

/// `LAZYVIEW_FULL_REFRESH_MS` given to test engines: `LAZYVIEW_TEST_FULL_REFRESH_MS` (default 0 =
/// rebuild full-mode / unknown-unit views on every flush, so checks are exact right after
/// catch-up). With a cadence > 0, [`verify`] checks incrementally maintained views right after
/// catch-up and polls the lazily rebuilt ones (Full, Chained, FK-child) for up to 2 × cadence.
pub fn test_full_refresh_ms() -> u64 {
    std::env::var("LAZYVIEW_TEST_FULL_REFRESH_MS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// Views rebuilt on the `LAZYVIEW_FULL_REFRESH_MS` cadence rather than per flush.
pub fn is_cadence_rebuilt(spec: &ViewSpec) -> bool {
    spec.kind != Kind::Incremental || workload::reads_fk_child(&spec.name)
}

/// Default timeouts (scaled).
pub fn t_ready() -> Duration {
    Duration::from_secs_f64(180.0 * scale().max(1.0))
}
pub fn t_catchup() -> Duration {
    Duration::from_secs_f64(180.0 * scale().max(1.0))
}

// ---------------------------------------------------------------------------------------------
// Metadata tables
// ---------------------------------------------------------------------------------------------

/// Creates `_lazyview` as the engine does (PLAN.md §1 + `name`, `targets`), if missing. Used to
/// pre-load definitions before the first engine start; the engine itself creates it otherwise.
pub async fn ensure_lazyview_table(conn: &mut Conn) -> Result<()> {
    exec(
        conn,
        "CREATE TABLE IF NOT EXISTS _lazyview (
           id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
           name       VARCHAR(64) NULL,
           definition MEDIUMTEXT NOT NULL,
           enabled    TINYINT(1) NOT NULL DEFAULT 1,
           targets    VARCHAR(255) NULL,
           created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
           updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           UNIQUE KEY uq_lazyview_name (name)
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    .await
}

/// Inserts a definition; returns its `_lazyview.id`.
pub async fn add_definition(
    conn: &mut Conn,
    definition: &str,
    targets: Option<&str>,
) -> Result<u64> {
    let t = targets.map(q).unwrap_or_else(|| "NULL".into());
    exec(
        conn,
        &format!(
            "INSERT INTO _lazyview (definition, targets) VALUES ({}, {t})",
            q(definition)
        ),
    )
    .await?;
    let id = scalar_i64(conn, "SELECT LAST_INSERT_ID()").await?;
    Ok(id as u64)
}

/// Inserts every spec's definition; returns name → id.
pub async fn add_views(conn: &mut Conn, specs: &[ViewSpec]) -> Result<HashMap<String, u64>> {
    let mut ids = HashMap::new();
    for s in specs {
        let id = add_definition(conn, &s.definition, s.targets.as_deref()).await?;
        ids.insert(s.name.clone(), id);
    }
    Ok(ids)
}

#[derive(Debug, Clone)]
pub struct StateRow {
    pub name: String,
    pub state: String,
    pub mode: Option<String>,
    pub last_error: Option<String>,
    pub definition_hash: Option<String>,
    pub refreshed_at: Option<String>,
    /// Target instance of this state row (`source` or a target name).
    pub target: Option<String>,
    /// `_lazyview.id` this row belongs to.
    pub definition_id: Option<u64>,
}

pub async fn view_states(conn: &mut Conn) -> Result<Vec<StateRow>> {
    if !table_exists(conn, "_lazyview_state").await? {
        return Ok(Vec::new());
    }
    let rs = rows(
        conn,
        "SELECT name, state, mode, last_error, definition_hash, CAST(refreshed_at AS CHAR), target, definition_id FROM _lazyview_state",
    )
    .await?;
    Ok(rs
        .into_iter()
        .map(|r| StateRow {
            name: r[0].clone().unwrap_or_default(),
            state: r[1].clone().unwrap_or_default(),
            mode: r[2].clone(),
            last_error: r[3].clone(),
            definition_hash: r[4].clone(),
            refreshed_at: r[5].clone(),
            target: r[6].clone(),
            definition_id: r[7].as_deref().and_then(|v| v.parse().ok()),
        })
        .collect())
}

/// State rows of one view (several if the engine keeps one per target).
pub async fn view_state(conn: &mut Conn, name: &str) -> Result<Vec<StateRow>> {
    Ok(view_states(conn)
        .await?
        .into_iter()
        .filter(|s| s.name == name)
        .collect())
}

/// Waits until every named view has state rows and all of them are `ready`. Fails fast if a
/// view stays in `error` for 5 s (with the engine's message) or if the engine dies.
pub async fn wait_ready(
    conn: &mut Conn,
    engine: &mut Engine,
    names: &[&str],
    timeout: Duration,
) -> Result<()> {
    wait_ready_grace(conn, engine, names, timeout, Some(Duration::from_secs(5))).await
}

/// Like [`wait_ready`] but tolerates `error` states until the timeout (for views expected to
/// recover, e.g. after a source table comes back or a definition is fixed).
pub async fn wait_ready_tolerant(
    conn: &mut Conn,
    engine: &mut Engine,
    names: &[&str],
    timeout: Duration,
) -> Result<()> {
    wait_ready_grace(conn, engine, names, timeout, None).await
}

async fn wait_ready_grace(
    conn: &mut Conn,
    engine: &mut Engine,
    names: &[&str],
    timeout: Duration,
    error_grace: Option<Duration>,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut error_since: HashMap<String, Instant> = HashMap::new();
    let mut no_row_since: HashMap<String, Instant> = HashMap::new();
    loop {
        engine.check_alive()?;
        let states = view_states(conn).await?;
        let mut pending = Vec::new();
        // The engine is up and has made other views ready (any view, not only these).
        let any_ready = states.iter().any(|s| s.state == "ready");
        for n in names {
            let mine: Vec<&StateRow> = states.iter().filter(|s| &s.name == n).collect();
            // A view the engine never registers while others are already ready: fail after 60 s
            // instead of the full timeout (look for errors in the engine log).
            if mine.is_empty() && any_ready && error_grace.is_some() {
                let since = *no_row_since
                    .entry(n.to_string())
                    .or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(60) {
                    bail!(
                        "view {n} has no _lazyview_state row 60 s after other views became ready (engine never registered its definition?) — log {}",
                        engine.log_path.display()
                    );
                }
            }
            if let Some(e) = mine.iter().find(|s| s.state == "error") {
                let since = *error_since
                    .entry(n.to_string())
                    .or_insert_with(Instant::now);
                if let Some(g) = error_grace {
                    if since.elapsed() >= g {
                        bail!(
                            "view {n} is in state 'error': {}",
                            e.last_error.clone().unwrap_or_default()
                        );
                    }
                }
            } else {
                error_since.remove(*n);
            }
            if mine.is_empty() || mine.iter().any(|s| s.state != "ready") {
                let detail: Vec<String> = mine
                    .iter()
                    .map(|s| match &s.last_error {
                        Some(e) if s.state == "error" => format!(
                            "{}:error({})",
                            s.target.as_deref().unwrap_or("?"),
                            clip(e, 200)
                        ),
                        _ => format!("{}:{}", s.target.as_deref().unwrap_or("?"), s.state),
                    })
                    .collect();
                pending.push(format!(
                    "{n}={}",
                    if detail.is_empty() {
                        "<no state row>".to_string()
                    } else {
                        detail.join("/")
                    }
                ));
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!(
                "views not ready within {timeout:?}: {} — log {}",
                pending.join(", "),
                engine.log_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until the view's state rows are all `ready` with a definition hash different from
/// `old_hash` (i.e. a changed definition has been built and swapped in).
pub async fn wait_rebuilt(
    conn: &mut Conn,
    engine: &mut Engine,
    name: &str,
    old_hash: &Option<String>,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        let st = view_state(conn, name).await?;
        if !st.is_empty()
            && st
                .iter()
                .all(|s| s.state == "ready" && &s.definition_hash != old_hash)
        {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!(
                "view {name} not rebuilt with its new definition within {timeout:?}; state: {st:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Current definition hash of a view (first state row).
pub async fn current_hash(conn: &mut Conn, name: &str) -> Result<Option<String>> {
    Ok(view_state(conn, name)
        .await?
        .into_iter()
        .next()
        .and_then(|s| s.definition_hash))
}

/// Waits until the state row of definition `def_id` (whatever name the engine gave it, e.g. `#5`
/// for an unparsable definition) reaches `state`; returns that row.
pub async fn wait_def_state(
    conn: &mut Conn,
    engine: &mut Engine,
    def_id: u64,
    state: &str,
    timeout: Duration,
) -> Result<StateRow> {
    let deadline = Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        let rows: Vec<StateRow> = view_states(conn)
            .await?
            .into_iter()
            .filter(|s| s.definition_id == Some(def_id))
            .collect();
        if let Some(s) = rows.iter().find(|s| s.state == state) {
            return Ok(s.clone());
        }
        if Instant::now() > deadline {
            bail!("definition #{def_id} did not reach state '{state}' within {timeout:?}; its state rows: {rows:?} — log {}", engine.log_path.display());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until a view reaches `state` (any of its rows); returns that row.
pub async fn wait_state(
    conn: &mut Conn,
    engine: &mut Engine,
    name: &str,
    state: &str,
    timeout: Duration,
) -> Result<StateRow> {
    let deadline = Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        if let Some(s) = view_state(conn, name)
            .await?
            .into_iter()
            .find(|s| s.state == state)
        {
            return Ok(s);
        }
        if Instant::now() > deadline {
            bail!(
                "view {name} did not reach state '{state}' within {timeout:?}; current: {:?}",
                view_state(conn, name).await?
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Asserts that every named view is `ready` right now.
pub async fn assert_ready(conn: &mut Conn, names: &[&str]) -> Result<()> {
    let states = view_states(conn).await?;
    for n in names {
        let mine: Vec<&StateRow> = states.iter().filter(|s| &s.name == n).collect();
        if mine.is_empty() {
            bail!("view {n} has no _lazyview_state row");
        }
        for s in mine {
            if s.state != "ready" {
                bail!(
                    "view {n} state is '{}' (last_error: {:?})",
                    s.state,
                    s.last_error
                );
            }
        }
    }
    Ok(())
}

/// Waits until `cond` returns true (polling every 50 ms).
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if cond().await? {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!("timed out after {timeout:?} waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Metrics helpers
// ---------------------------------------------------------------------------------------------

/// Human-readable non-zero counters of a snapshot/delta.
pub fn fmt_snapshot(s: &Snapshot) -> String {
    let dbg = format!("{s:?}");
    // Snapshot { a: 1, b: 0, ... } → keep non-zero fields.
    let inner = dbg
        .trim_start_matches("Snapshot")
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}');
    let parts: Vec<&str> = inner
        .split(',')
        .map(str::trim)
        .filter(|p| !p.ends_with(": 0") && !p.is_empty())
        .collect();
    format!("{{{}}}", parts.join(", "))
}

/// Metric delta over an async block.
pub async fn measure<F, Fut, T>(engine: &Engine, f: F) -> Result<(T, Snapshot)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let before = engine.snapshot();
    let out = f().await?;
    Ok((out, engine.snapshot().since(&before)))
}

// ---------------------------------------------------------------------------------------------
// Convenience: full verification
// ---------------------------------------------------------------------------------------------

/// Waits for catch-up, then compares every view materialized on the source against its oracle.
/// On mismatch, waits a bit more and re-checks to tell "wrong" from "applied() reported too
/// early" — both fail.
pub async fn verify(
    ctx: &Ctx,
    engine: &mut Engine,
    src: &Db,
    specs: &[ViewSpec],
    what: &str,
) -> Result<()> {
    verify_targets(ctx, engine, src, &HashMap::new(), specs, what).await
}

/// Like [`verify`], also comparing MVs on the given targets (target name → schema).
pub async fn verify_targets(
    ctx: &Ctx,
    engine: &mut Engine,
    src: &Db,
    targets: &HashMap<String, Db>,
    specs: &[ViewSpec],
    what: &str,
) -> Result<()> {
    let mut sc = src.conn().await?;
    engine
        .wait_caught_up(&mut sc, t_catchup())
        .await
        .with_context(|| format!("catch-up before check '{what}'"))?;
    engine.check_alive()?;
    let t = Instant::now();
    let cadence = test_full_refresh_ms();
    let res = if cadence == 0 {
        oracle::check_views_targets(src, targets, specs).await
    } else {
        // Strict for incremental views; cadence-rebuilt ones get up to 2 × cadence + 10 s.
        let (lazy, strict): (Vec<ViewSpec>, Vec<ViewSpec>) =
            specs.iter().cloned().partition(is_cadence_rebuilt);
        match oracle::check_views_targets(src, targets, &strict).await {
            Err(e) => Err(e),
            Ok(()) => {
                let deadline =
                    Instant::now() + Duration::from_millis(2 * cadence) + Duration::from_secs(10);
                loop {
                    match oracle::check_views_targets(src, targets, &lazy).await {
                        Ok(()) => break Ok(()),
                        Err(e) if Instant::now() > deadline => {
                            break Err(e.context(format!(
                            "cadence-rebuilt views not converged within 2 × {cadence} ms + 10 s"
                        )))
                        }
                        Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
                    }
                }
            }
        }
    };
    match res {
        Ok(()) => {
            ctx.log(format!(
                "check '{what}': {} views equal oracle ({} ms)",
                specs.len(),
                t.elapsed().as_millis()
            ));
            sc.disconnect().await.ok();
            Ok(())
        }
        Err(first) => {
            tokio::time::sleep(Duration::from_secs(3)).await;
            engine.wait_caught_up(&mut sc, t_catchup()).await.ok();
            let again = oracle::check_views_targets(src, targets, specs).await;
            let verdict = match &again {
                Ok(()) => "converged after an extra wait → applied() was reported before the changes were visible",
                Err(_) => "still mismatched after an extra 3 s wait",
            };
            Err(first.context(format!(
                "check '{what}' failed ({verdict}); metrics: {}",
                fmt_snapshot(&engine.snapshot())
            )))
        }
    }
}

/// Polls the oracle comparison (without relying on `applied()`) until it passes or `timeout`
/// expires; for situations where catch-up may legitimately stall (e.g. a target is down).
pub async fn eventually_equal(
    ctx: &Ctx,
    engine: &mut Engine,
    src: &Db,
    targets: &HashMap<String, Db>,
    specs: &[ViewSpec],
    timeout: Duration,
    what: &str,
) -> Result<()> {
    let start = Instant::now();
    loop {
        engine.check_alive()?;
        match oracle::check_views_targets(src, targets, specs).await {
            Ok(()) => {
                ctx.log(format!(
                    "check '{what}': {} views converged after {:.1}s",
                    specs.len(),
                    start.elapsed().as_secs_f64()
                ));
                return Ok(());
            }
            Err(e) if start.elapsed() > timeout => {
                return Err(e.context(format!(
                    "'{what}': not converged within {timeout:?}; metrics {}",
                    fmt_snapshot(&engine.snapshot())
                )))
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

/// Owned env overrides → the borrowed form `Ctx::start_engine` takes.
pub fn ov_refs(v: &[(String, String)]) -> Vec<(&str, &str)> {
    v.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect()
}

/// Env overrides configuring named targets (`LAZYVIEW_TARGETS` + `LAZYVIEW_TARGET_<NAME>_DSN`).
pub fn target_env(targets: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut v = vec![(
        "LAZYVIEW_TARGETS".to_string(),
        targets.iter().map(|t| t.0).collect::<Vec<_>>().join(","),
    )];
    for (name, dsn) in targets {
        v.push((
            format!(
                "LAZYVIEW_TARGET_{}_DSN",
                name.to_ascii_uppercase().replace('-', "_")
            ),
            dsn.to_string(),
        ));
    }
    v
}

/// Inserts the definitions and starts an engine, adding chained views (MV over MV) only once
/// their upstream views are ready — so that one engine ordering problem does not mask every
/// other check (`lifecycle_chained_definitions_together` covers the all-at-once case).
/// Returns the engine and name → `_lazyview.id`.
pub async fn start_with_views(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    overrides: &[(&str, &str)],
) -> Result<(Engine, HashMap<String, u64>)> {
    let mut c = db.conn().await?;
    ensure_lazyview_table(&mut c).await?;
    let (chained, base): (Vec<ViewSpec>, Vec<ViewSpec>) =
        specs.iter().cloned().partition(|s| s.kind == Kind::Chained);
    let mut ids = add_views(&mut c, &base).await?;
    let mut eng = ctx.start_engine(db, overrides)?;
    let base_names: Vec<&str> = base.iter().map(|s| s.name.as_str()).collect();
    wait_ready(&mut c, &mut eng, &base_names, t_ready()).await?;
    if !chained.is_empty() {
        ids.extend(add_views(&mut c, &chained).await?);
        let chained_names: Vec<&str> = chained.iter().map(|s| s.name.as_str()).collect();
        wait_ready(&mut c, &mut eng, &chained_names, t_ready()).await?;
    }
    c.disconnect().await.ok();
    Ok((eng, ids))
}
