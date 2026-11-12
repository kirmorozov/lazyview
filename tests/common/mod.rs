//! Integration-test harness for lazyview.
//!
//! The harness is configured entirely by environment variables: the server list
//! (`LAZYVIEW_TEST_DSNS`; unset means every scenario passes with a printed note), seeds and
//! scale, the per-run timeout, the engine account and log filter, and the opt-in switches for
//! the special suites. All of them are documented in one place, `docs/development.md`
//! ("Tests"); this header deliberately does not repeat that list — keep additions there.

#![allow(dead_code, unused_imports)]

pub mod oracle;
// The analyzer's test-only renamer (one implementation for unit and integration tests).
#[path = "../../src/analyzer/rename.rs"]
pub mod rename;
pub mod soakcheck;
pub mod soakwork;
pub mod typed;
pub mod workload;

/// The string literals of a Rust source (plain and raw strings, `\` line continuations): the
/// SQL statements tests write. The harness's own copy (the renamer above no longer ships it);
/// `src/analyzer/tests_rename.rs` carries the same copy for the unit tests.
pub fn rust_literals(src: &str) -> Vec<String> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'\'' => {
                // A char literal or a lifetime.
                i += if b.get(i + 2) == Some(&b'\'') {
                    3
                } else if b.get(i + 1) == Some(&b'\\') {
                    4
                } else {
                    1
                };
            }
            b'r' if matches!(b.get(i + 1), Some(b'"' | b'#'))
                && (i == 0 || !b[i - 1].is_ascii_alphanumeric()) =>
            {
                let hashes = b[i + 1..].iter().take_while(|&&c| c == b'#').count();
                let start = i + 2 + hashes;
                let close = format!("\"{}", "#".repeat(hashes));
                let Some(end) = src[start..].find(&close).map(|e| start + e) else {
                    break;
                };
                out.push(src[start..end].to_string());
                i = end + close.len();
            }
            b'"' => {
                let mut s = String::new();
                let mut j = i + 1;
                while j < b.len() && b[j] != b'"' {
                    if b[j] == b'\\' {
                        j += 1;
                        match b.get(j) {
                            Some(b'n') => s.push('\n'),
                            Some(b't') => s.push('\t'),
                            Some(b'\\') => s.push('\\'),
                            Some(b'"') => s.push('"'),
                            Some(b'\'') => s.push('\''),
                            Some(b'0') => s.push('\0'),
                            Some(b'\n') => {
                                while b.get(j + 1).is_some_and(|c| c.is_ascii_whitespace()) {
                                    j += 1;
                                }
                            }
                            Some(_) => {
                                s.push('\\');
                                let c = src[j..].chars().next().unwrap();
                                s.push(c);
                                j += c.len_utf8() - 1;
                            }
                            None => {}
                        }
                        j += 1;
                        continue;
                    }
                    let c = src[j..].chars().next().unwrap();
                    s.push(c);
                    j += c.len_utf8();
                }
                out.push(s);
                i = j + 1;
            }
            _ => i += 1,
        }
    }
    out
}

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

pub use engine::*;
pub use globals::*;
pub use meta::*;
pub use oracle::{check_views, check_views_targets, Kind, ViewSpec};
pub use renamed::*;
use renamed::{compare_renamed, observed, renamed_schemas};

mod engine;
mod globals;
mod meta;
mod renamed;

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
    let r = renamed_sql(conn, sql)?;
    conn.query_drop(r.sql.as_str())
        .await
        .with_context(|| format!("SQL failed: {}", r.shown(sql)))
}

/// Executes statements in order.
pub async fn exec_all<S: AsRef<str>>(conn: &mut Conn, stmts: &[S]) -> Result<()> {
    for s in stmts {
        exec(conn, s.as_ref()).await?;
    }
    Ok(())
}

/// Runs `stmts` in one transaction, committed, or rolled back when `rollback`.
pub async fn txn(conn: &mut Conn, stmts: &[String], rollback: bool) -> Result<()> {
    exec(conn, "START TRANSACTION").await?;
    exec_all(conn, stmts).await?;
    exec(conn, if rollback { "ROLLBACK" } else { "COMMIT" }).await
}

/// Rows as text (NULL → None); binary values are hex-escaped.
pub async fn rows(conn: &mut Conn, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let r = renamed_sql(conn, sql)?;
    let rows: Vec<Row> = conn
        .query(r.sql.as_str())
        .await
        .with_context(|| format!("SQL failed: {}", r.shown(sql)))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            row.unwrap()
                .into_iter()
                .map(|v| value_text(&v).map(|t| r.cell(t)))
                .collect()
        })
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

/// The ids of all *other* sessions (excluding this connection) matching `filter`, an SQL
/// condition over the shared columns `USER`, `DB`, `ID` (e.g. `DB = 'items'`). The session
/// list is read from `performance_schema.processlist` (utf8mb4) when the server has it and
/// falls back to `information_schema.PROCESSLIST` when that query fails (e.g. MariaDB with
/// performance_schema off). The information_schema view carries other sessions' query text
/// in utf8mb3 and fails outright on MySQL/Percona 8.0.22+ as soon as one concurrent query
/// contains a 4-byte character, which the utf8mb4 table does not.
pub async fn session_ids(conn: &mut Conn, filter: &str) -> Result<Vec<u64>> {
    let sql = |table: &str, id: &str| {
        format!("SELECT {id} FROM {table} WHERE {filter} AND {id} <> CONNECTION_ID()")
    };
    let rows = match rows(
        conn,
        &sql("performance_schema.processlist", "PROCESSLIST_ID"),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => rows(conn, &sql("information_schema.PROCESSLIST", "ID")).await?,
    };
    rows.into_iter()
        .map(|r| {
            r[0].clone()
                .ok_or_else(|| anyhow!("process list: NULL session id"))
                .and_then(|s| {
                    s.parse::<u64>()
                        .with_context(|| format!("process list: bad session id {s}"))
                })
        })
        .collect()
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

/// Whether the server writes the child rows of foreign-key cascades to the binlog itself: MySQL
/// 9.x / 26.x with `innodb_native_foreign_keys=OFF` (their default) cascade at the SQL layer and
/// log the cascaded rows before the parent's row event. Elsewhere (the variable does not exist)
/// the storage engine changes them without binlog events.
pub async fn cascades_logged(conn: &mut Conn) -> Result<bool> {
    let r = rows(conn, "SHOW VARIABLES LIKE 'innodb_native_foreign_keys'").await?;
    Ok(r.first()
        .and_then(|row| row.get(1).cloned().flatten())
        .is_some_and(|v| v.eq_ignore_ascii_case("OFF")))
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

async fn create_db(
    server: &Server,
    server_idx: usize,
    tag: &str,
    names: Option<&SharedNames>,
) -> Result<Db> {
    let suffix: String = {
        let mut r = rand::rng();
        (0..6)
            .map(|_| (b'a' + r.random_range(0..26u8)) as char)
            .collect()
    };
    // The tag comes from the test: it is a literal or a `format!` over `[a-z0-9_]` labels, so it
    // is embedded as-is. A tag that is not a usable identifier is a test bug and fails loudly in
    // the `CREATE DATABASE` below.
    let mut name = format!("lvt_{tag}_{suffix}");
    if let Some(n) = names {
        // The renamed run's schema: an opaque name under `lvt_` (the test reads it as
        // `Db::name`, so it is not mapped back), under which its connections are renamed.
        // Under the test-schema prefix: CI's least-privilege engine user is granted on `lvt\_%`
        // only, and leftover cleanup matches `lvt_*`. The rest stays opaque.
        name = format!(
            "lvt_{}{suffix}",
            n.lock().unwrap().fresh(rename::Role::Schema)
        );
        renamed_schemas()
            .lock()
            .unwrap()
            .insert(name.clone(), n.clone());
    }
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
    /// The renamed run of the monkey-wrench rename test: every statement this run's connections
    /// send is renamed (see [`compare_renamed`]).
    pub names: Option<SharedNames>,
    /// Engines started by this run: label, metrics, log file.
    engines: Mutex<Vec<(String, Arc<Metrics>, PathBuf)>>,
}

impl Ctx {
    pub fn flavor(&self) -> &str {
        &self.servers[0].flavor
    }

    pub fn server(&self) -> &Server {
        &self.servers[0]
    }

    pub fn label(&self) -> String {
        let l = self
            .servers
            .iter()
            .map(|s| s.flavor.as_str())
            .collect::<Vec<_>>()
            .join("→");
        if self.names.is_some() {
            format!("{l} renamed")
        } else {
            l
        }
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

    /// Creates a fresh schema on the source server whose connections send every statement
    /// renamed by `names` (the renamed side of a monkey-wrench rename comparison).
    pub async fn new_db_renamed(&self, tag: &str, names: &SharedNames) -> Result<Db> {
        let db = create_db(&self.servers[0], 0, tag, Some(names)).await?;
        self.log(format!("schema {} (renamed) on {}", db.name, db.flavor));
        self.dbs.lock().unwrap().push(db.clone());
        Ok(db)
    }

    /// Creates a fresh schema on `servers[idx]`.
    pub async fn new_db_on(&self, idx: usize, tag: &str) -> Result<Db> {
        let db = create_db(&self.servers[idx], idx, tag, self.names.as_ref()).await?;
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
        let e = Engine::spawn(cfg, label, log_path)?;
        self.engines
            .lock()
            .unwrap()
            .push((e.label.clone(), e.metrics.clone(), e.log_path.clone()));
        Ok(e)
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
            let new_ctx = {
                let test_name = test_name.clone();
                move |servers: Vec<Server>, names: Option<SharedNames>| {
                    Arc::new(Ctx {
                        seed: derive_seed(seed, &test_name),
                        test: test_name.clone(),
                        servers,
                        started: Instant::now(),
                        dbs: Mutex::new(Vec::new()),
                        engine_seq: AtomicU32::new(0),
                        notes: Mutex::new(Vec::new()),
                        names,
                        engines: Mutex::new(Vec::new()),
                    })
                }
            };
            let ctx = new_ctx(servers.clone(), None);
            let f = f.clone();
            let label = ctx.label();
            let renamed = rename_seed().map(|s| derive_seed(s, &test_name));
            let h = tokio::spawn(async move {
                let Some(rseed) = renamed else {
                    return run_ctx(&*f, &ctx, false).await.map(|_| ());
                };
                let plain = run_ctx(&*f, &ctx, true).await?;
                let mut n = rename::Names::new(rseed);
                n.vary_case = rename_case();
                n.special_tables = !servers[0].is_mariadb();
                let names = Arc::new(Mutex::new(n));
                let rctx = new_ctx(servers, Some(names.clone()));
                let renamed = run_ctx(&*f, &rctx, true).await?;
                let n = names.lock().unwrap().clone();
                compare_renamed(&rctx, &n, plain.unwrap(), renamed.unwrap())
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

/// Runs the scenario on `ctx` (with a timeout) and cleans up; with `observe`, also returns what
/// the monkey-wrench rename test compares between the plain and the renamed run.
async fn run_ctx<F, Fut>(
    f: &F,
    ctx: &Arc<Ctx>,
    observe: bool,
) -> std::result::Result<Option<Observed>, String>
where
    F: Fn(Arc<Ctx>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let res = match tokio::time::timeout(run_timeout(), f(ctx.clone())).await {
        Ok(r) => r,
        Err(_) => Err(anyhow!("scenario timed out after {:?}", run_timeout())),
    };
    let res = match res {
        Ok(()) if observe => observed(ctx).await.map(Some),
        Ok(()) => Ok(None),
        Err(e) => Err(e),
    };
    let out = match res {
        Ok(o) => finish_run(ctx, Ok(())).await.map(|_| o),
        Err(e) => finish_run(ctx, Err(e)).await.map(|_| None),
    };
    let dbs = ctx.dbs.lock().unwrap().clone();
    let mut reg = renamed_schemas().lock().unwrap();
    for db in &dbs {
        reg.remove(&db.name);
    }
    out
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
            if let Some(n) = &ctx.names {
                let _ = write!(msg, "\n  monkey-wrench {}", n.lock().unwrap().describe());
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
            // Poll the oracle comparison for up to 3 s instead of sleeping a fixed 3 s:
            // a late-converging oracle settles as soon as it matches.
            let _ = wait_until(
                "the oracle to match after the first mismatch",
                Duration::from_secs(3),
                || {
                    let (src, targets, specs) = (src, targets, specs);
                    async move {
                        Ok(oracle::check_views_targets(src, targets, specs)
                            .await
                            .is_ok())
                    }
                },
            )
            .await;
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

/// Creates a fresh schema for `tag`, runs `ddl` in it, adds `specs` to `_lazyview`, starts an
/// engine with the overrides `ov`, waits until every view is ready and verifies them against
/// their oracles ("initial"). Returns the schema and the running engine.
pub async fn setup_views(
    ctx: &Ctx,
    tag: &str,
    ddl: &[&str],
    specs: &[ViewSpec],
    ov: &[(&str, &str)],
) -> Result<(Db, Engine)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    exec_all(&mut c, ddl).await?;
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, specs).await?;
    let mut eng = ctx.start_engine(&db, ov)?;
    wait_ready(&mut c, &mut eng, &workload::names(specs), t_ready()).await?;
    verify(ctx, &mut eng, &db, specs, "initial").await?;
    Ok((db, eng))
}
