//! Orchestration: startup, background builds, the binlog loop, flushes and online swaps.
//!
//! Each view definition (`_lazyview` row) is a [`Slot`] with up to two versions:
//! * `live` — the table readers see, maintained incrementally from the binlog;
//! * `next` — a new version (new definition, targets, or a rebuild after DDL) being built in
//!   the background into shadow tables. Changes arriving meanwhile are buffered for it, applied
//!   once the build finishes, and then the shadow is swapped in atomically (`RENAME TABLE`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Value};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::accumulate::{feed, feed_cascade, Fed, UnitChunk, UnitSet, ViewDirty};
use crate::analyze::{self, Mode, ViewPlan};
use crate::binlog::{self, BinlogReader, Flavor, Item, Position};
use crate::catalog::{Catalog, TableInfo, TableRef};
use crate::config::{Config, MissingBinlog, StartMode};
use crate::coord;
use crate::datum::{Datum, Key};
use crate::lookback::Lookback;
use crate::meta::{self, StateUpdate, SOURCE_TARGET};
use crate::metrics::Metrics;
use crate::mv::{self, BuildTarget, Layout};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;

/// Runs the service until `shutdown` turns true (or, for `once` / `sync=false`, until done).
pub async fn run(
    cfg: Config,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    Metrics::set(&metrics.gauges.heartbeat_unix, crate::metrics::unix_now());
    let health = cfg
        .health_addr
        .map(|addr| tokio::spawn(crate::health::serve(addr, metrics.clone())));
    let result = run_engine(cfg, metrics, &mut shutdown).await;
    if let Some(h) = health {
        h.abort();
    }
    result
}

async fn run_engine(
    cfg: Config,
    metrics: Arc<Metrics>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let user = binlog::replica_opts(&cfg.dsn)
        .ok()
        .and_then(|o| o.user().map(str::to_string))
        .unwrap_or_else(|| "lazyview".into());
    let schema = binlog::replica_opts(&cfg.dsn)
        .ok()
        .and_then(|o| o.db_name().map(str::to_string))
        .unwrap_or_default();
    let r = async {
        let Some((mut engine, start)) = Engine::start(cfg, metrics, shutdown).await? else {
            return Ok(());
        };
        let Some(start) = start else {
            return Ok(());
        };
        engine.stream(start, shutdown).await
    }
    .await;
    r.map_err(|e| with_access_hint(e, &user, &schema))
}

/// Amazon RDS / Aurora purge binlogs aggressively by default; a short retention means a restart
/// after an outage cannot resume and must rebuild every view. Warn once at startup.
async fn managed_service_checks(conn: &mut Conn) {
    let basedir: Option<String> = conn.query_first("SELECT @@basedir").await.unwrap_or(None);
    let aurora: Option<String> = conn
        .query_first("SELECT @@aurora_version")
        .await
        .unwrap_or(None);
    let rds = basedir
        .as_deref()
        .map(|b| b.contains("rdsdbbin"))
        .unwrap_or(false)
        || aurora.is_some();
    if !rds {
        return;
    }
    let hours: Option<Option<String>> = conn
        .query_first(
            "SELECT value FROM mysql.rds_configuration WHERE name = 'binlog retention hours'",
        )
        .await
        .unwrap_or(None);
    match hours.flatten().and_then(|h| h.trim().parse::<u64>().ok()) {
        Some(h) if h >= 12 => {}
        other => warn!(
            "Amazon RDS/Aurora binlog retention is {}; after an outage longer than that lazyview cannot resume and \
             rebuilds all views. Run once: CALL mysql.rds_set_configuration('binlog retention hours', 24);",
            other.map(|h| format!("{h} hours")).unwrap_or_else(|| "not set (binlogs are purged within minutes)".into())
        ),
    }
    let gtid: Option<String> = conn
        .query_first("SELECT @@GLOBAL.gtid_mode")
        .await
        .unwrap_or(None);
    if aurora.is_none()
        && gtid
            .as_deref()
            .map(|g| !g.eq_ignore_ascii_case("ON"))
            .unwrap_or(false)
    {
        info!("tip: gtid_mode=ON and enforce_gtid_consistency=ON in the RDS parameter group give lazyview an exact commit-visibility fence");
    }
}

/// Connection-class failures (socket closed, reset, timeouts): retry after reconnecting.
fn is_conn_error(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|c| match c.downcast_ref::<mysql_async::Error>() {
            Some(mysql_async::Error::Io(_)) => true,
            Some(mysql_async::Error::Driver(d)) => {
                matches!(d, mysql_async::DriverError::ConnectionClosed)
            }
            _ => false,
        })
}

fn is_tls_error(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(
            c.downcast_ref::<mysql_async::Error>(),
            Some(mysql_async::Error::Io(mysql_async::IoError::Tls(_)))
        ) || c.to_string().contains("invalid peer certificate")
    })
}

fn server_code(e: &anyhow::Error) -> Option<u16> {
    e.chain()
        .find_map(|c| match c.downcast_ref::<mysql_async::Error>() {
            Some(mysql_async::Error::Server(se)) => Some(se.code),
            _ => None,
        })
}

/// Privilege errors get the exact GRANT statements appended.
fn with_access_hint(e: anyhow::Error, user: &str, schema: &str) -> anyhow::Error {
    match server_code(&e) {
        Some(1045) if !format!("{e:#}").contains("REPLICATION") => e.context(format!(
            "access denied for user '{user}': check LAZYVIEW_USER / LAZYVIEW_PASSWORD (or the DSN) and that the user may connect from this host"
        )),
        Some(1044 | 1045 | 1142 | 1143 | 1227 | 1370) => e.context(format!(
            "the lazyview database user lacks privileges; as an admin run:\n  \
             GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%';\n  \
             GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW ON `{schema}`.* TO '{user}'@'%';\n  \
             (plus SELECT on any other schema your views read)"
        )),
        Some(1049) => e.context(format!("database `{schema}` does not exist; create it first: CREATE DATABASE `{schema}`")),
        _ => e,
    }
}

/// Connects, waiting (with backoff) while the server is unreachable. Authentication and
/// unknown-database errors are fatal. `Ok(None)` when shutdown was requested.
async fn connect_wait(
    opts: &Opts,
    what: &str,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Conn>> {
    let mut backoff = Duration::from_secs(1);
    let mut waited = false;
    loop {
        match connect(opts).await {
            Ok(c) => {
                if waited {
                    info!(
                        "{what} at {}:{} is reachable",
                        opts.ip_or_hostname(),
                        opts.tcp_port()
                    );
                }
                return Ok(Some(c));
            }
            Err(e) if is_tls_error(&e) => {
                return Err(e.context(format!(
                    "TLS to {what} at {}:{} failed; for a private/managed CA set LAZYVIEW_TLS_CA=/path/to/ca.pem \
                     (the certificate must name this host), or LAZYVIEW_TLS=required to encrypt without verifying it",
                    opts.ip_or_hostname(),
                    opts.tcp_port()
                )))
            }
            Err(e) if matches!(server_code(&e), Some(1044 | 1045 | 1049 | 1227)) => {
                return Err(e.context(format!("connecting to {what}")))
            }
            Err(e) => {
                warn!(
                    "waiting for {what} at {}:{}: {}; retrying in {backoff:?}",
                    opts.ip_or_hostname(),
                    opts.tcp_port(),
                    e.root_cause()
                );
                waited = true;
            }
        }
        metrics.beat();
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

struct Target {
    opts: Opts,
    schema: String,
    conn: Option<Conn>,
    /// Unreachable: connection attempts fail fast until then (backoff), so a dead target never
    /// stalls the binlog loop for more than one connect timeout per period.
    retry_at: Option<Instant>,
    failures: u32,
}

/// Retry delay after `n` consecutive failures of one target (1 s doubling, at most 30 s).
fn target_backoff(n: u32) -> Duration {
    Duration::from_secs(1u64 << n.saturating_sub(1).min(5)).min(Duration::from_secs(30))
}

/// One connect attempt to a target (TCP + handshake), well inside the liveness window.
const TARGET_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

struct Version {
    plan: Arc<ViewPlan>,
    layout: Layout,
    name: String,
    /// Targets whose table is in place and maintained.
    targets: Vec<String>,
    dirty: ViewDirty,
    /// Source schema changed under it: kept readable but no longer maintained.
    frozen: bool,
    /// Replay position of the oldest change not yet in this table (buffered in `dirty`, or a
    /// rebuild pending or in flight). The saved and the reported position never pass it.
    since: Option<Position>,
    /// Targets whose writes failed: units still to apply there, retried with backoff while the
    /// other targets are maintained as usual.
    lag: BTreeMap<String, Lag>,
    /// Remembered images of lookup tables without lineage (see [`Lookback`]).
    recent: Lookback,
}

struct Lag {
    units: UnitSet,
    /// Global views: the single row must be recomputed on this target.
    all: bool,
    since: Position,
    failures: u32,
    retry_at: Instant,
}

/// A target the view could not be built on (unreachable, refused): retried by a partial build.
struct Down {
    failures: u32,
    retry_at: Instant,
}

struct Next {
    plan: Arc<ViewPlan>,
    name: String,
    targets: Vec<String>,
    dirty: ViewDirty,
    generation: u64,
    task: JoinHandle<()>,
    done: Option<Result<Built>>,
    /// Only adds these targets to the unchanged live version.
    partial: bool,
    hash: String,
    /// Targets already swapped (a swap interrupted on one target is resumed on the rest).
    swapped: HashSet<String>,
    /// Retry the swap no earlier than this (an MV reader held a metadata lock).
    not_before: Option<Instant>,
    /// Position the build started at: it reflects every change up to here.
    started: Position,
    /// Replay position of the oldest change buffered in `dirty`.
    since: Option<Position>,
    /// Images resolved against the shadow table (handed to the live version at the swap).
    recent: Lookback,
}

struct Slot {
    def_id: u64,
    name: String,
    definition: String,
    hash: String,
    targets: Vec<String>,
    live: Option<Version>,
    next: Option<Next>,
    error: Option<String>,
    /// (name, target) tables to drop once `next` is swapped in.
    retired: Vec<(String, String)>,
    /// When the last full rebuild of this view started (LAZYVIEW_FULL_REFRESH_MS cadence).
    last_full: Option<Instant>,
    /// Tables the definition reads (to retry a failed definition when they change/appear).
    wanted: Vec<TableRef>,
    /// Readers see a table that misses changes no dirty set covers (source DDL, upstream view
    /// swapped) since this position, until a build started after it is swapped in.
    stale_since: Option<Position>,
    /// Chained view waiting for its upstream lazyview table to be built first.
    waiting: Option<String>,
    /// Failed refreshes: the next attempt waits until then (exponential backoff).
    retry_at: Option<Instant>,
    /// Targets the view could not be built on.
    down: BTreeMap<String, Down>,
    /// Latest plan of the current definition (its mode stays visible in state `error`).
    last_plan: Option<Arc<ViewPlan>>,
    /// Consecutive failed refreshes of the live version.
    refresh_failures: u32,
    /// Re-plan once the build in flight is done (its source data was replaced meanwhile).
    replan_after: bool,
}

#[derive(Clone, Copy, Debug)]
struct RouteT {
    slot: usize,
    next: bool,
    kind: RouteKind,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RouteKind {
    /// Rows of this plan alias.
    Alias(usize),
    /// Full-refresh view reading the plan table `tables[i]`: a row marks it stale unless it is
    /// an UPDATE that leaves every column the view reads (`table_columns[i]`) unchanged.
    All(usize),
    /// Any row marks the version stale without decoding (columns read unknown).
    Any,
    /// Parent of a cascading FK into a table the view reads (plan cascade index).
    Cascade(usize),
}

type XaRow = (TableRef, Option<BinlogRow>, Option<BinlogRow>);

/// An XA transaction read from the binlog; its rows are applied at XA COMMIT.
struct XaTxn {
    /// Position before it (replaying from there re-reads its rows).
    start: Position,
    rows: Vec<XaRow>,
    /// Tables with rows that were not kept (no view read them at the time).
    other_tables: HashSet<TableRef>,
    prepared_at: Instant,
    /// Unresolved for longer than [`XA_PIN_MAX`]: no longer holds back the saved position.
    unpinned: bool,
}

/// An XA transaction left prepared longer than this stops holding back the saved position
/// (with a warning): its rows stay in memory, but a restart cannot re-read them anymore.
const XA_PIN_MAX: Duration = Duration::from_secs(3600);

/// A chained view reads a lazyview table that another definition will create but that is not
/// built yet: the view is planned once that upstream table is swapped in.
#[derive(Debug)]
struct WaitingFor(String);

impl std::fmt::Display for WaitingFor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "waiting for upstream view `{}` to be built", self.0)
    }
}

impl std::error::Error for WaitingFor {}

/// A finished build: its layout and the targets it could not be built on.
struct Built {
    layout: Layout,
    failed: Vec<(String, anyhow::Error)>,
}

struct BuildDone {
    def_id: u64,
    generation: u64,
    result: Result<Built>,
}

struct Engine {
    cfg: Config,
    metrics: Arc<Metrics>,
    src_opts: Opts,
    schema: String,
    flavor: Flavor,
    /// Source reads: lookups, refresh SELECTs, catalog.
    src: Conn,
    /// Metadata writes on the source.
    meta: Conn,
    targets: BTreeMap<String, Target>,
    catalog: Catalog,
    slots: Vec<Slot>,
    /// View name → id of every enabled definition (chained views wait for these).
    def_names: HashMap<String, u64>,
    /// Tables we have created and own: (name, target) → definition hash.
    owned: HashMap<(String, String), String>,
    routes: HashMap<TableRef, Arc<Vec<RouteT>>>,
    /// TABLE_MAP ids of the current transaction (re-sent for every transaction; cleared at
    /// each boundary so table id churn cannot grow it).
    table_ids: HashMap<u64, (TableRef, Option<Arc<Vec<RouteT>>>)>,
    /// (table, binlog column count) → column count of the table's current definition.
    shapes: HashMap<(TableRef, u64), Option<usize>>,
    /// XA transaction being read: (xid, position before it, rows of routed tables).
    xa_current: Option<(String, XaTxn)>,
    /// Prepared XA transactions awaiting XA COMMIT / ROLLBACK.
    xa_pending: HashMap<String, XaTxn>,
    defs_dirty: bool,
    /// Definitions to re-plan and rebuild: def_id → (due, freeze the live version).
    replan: HashMap<u64, (Instant, bool)>,
    /// Consecutive failures per definition (exponential retry backoff).
    failures: HashMap<u64, u32>,
    /// Re-read `_lazyview` once more after this (no exact visibility fence available).
    defs_recheck: Option<Instant>,
    last_src_ping: Instant,
    boundary: Position,
    persisted: Position,
    last_persist: Instant,
    in_txn: bool,
    /// The current transaction touched something besides our own metadata tables.
    txn_foreign: bool,
    persist_needed: bool,
    first_dirty: Option<Instant>,
    last_gtid: Option<String>,
    /// The server reports the binlog position of a consistent snapshot (see `fence`); `None`
    /// until tried.
    snapshot_fence: Option<bool>,
    generation: u64,
    build_tx: mpsc::UnboundedSender<BuildDone>,
    build_rx: Option<mpsc::UnboundedReceiver<BuildDone>>,
    build_slots: Arc<tokio::sync::Semaphore>,
    leader: coord::Leadership,
    ready_logged: bool,
    /// Live views refreshed since `refreshed_at` was last bumped.
    refreshed: HashSet<String>,
    last_touch: Instant,
    last_stats: (Instant, crate::metrics::Snapshot),
    last_meta_ping: Instant,
}

/// One connection attempt (TCP, TLS and handshake) is bounded well inside the liveness window:
/// an unreachable host would otherwise hold the loop for the OS's SYN retries (~130 s).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Binlog dump connections: connect, register as a replica and request the stream.
const BINLOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// While the binlog stream keeps delivering (a backlog), a due flush waits at most this long.
const CATCH_UP_BATCH: Duration = Duration::from_millis(100);

/// Opens a connection with the service's session settings.
async fn connect(opts: &Opts) -> Result<Conn> {
    let mut c = match tokio::time::timeout(CONNECT_TIMEOUT, Conn::new(opts.clone())).await {
        Ok(r) => r?,
        Err(_) => bail!(
            "connecting to {}:{}: timed out after {CONNECT_TIMEOUT:?}",
            opts.ip_or_hostname(),
            opts.tcp_port()
        ),
    };
    mv::init_session(&mut c).await?;
    Ok(c)
}

/// Identity of the server that writes the binlog: a saved position only means something on
/// that server. MySQL/Percona: `@@server_uuid`; MariaDB: server id and GTID domain.
async fn server_identity(conn: &mut Conn, flavor: Flavor) -> Result<String> {
    let sql = match flavor {
        Flavor::MySql => "SELECT CAST(@@server_uuid AS CHAR)",
        Flavor::MariaDb => "SELECT CAST(CONCAT(@@server_id, ':', @@gtid_domain_id) AS CHAR)",
    };
    let v: Option<Option<String>> = conn.query_first(sql).await?;
    Ok(v.flatten().unwrap_or_default())
}

fn schema_of(opts: &Opts, what: &str) -> Result<String> {
    opts.db_name().map(str::to_string).ok_or_else(|| {
        anyhow!("{what} DSN must name a database (mysql://user:pass@host:port/<schema>)")
    })
}

/// Whether an UPDATE changed any column marked in `cols`. Images that are not full rows
/// (`binlog_row_image` other than FULL) count as changed; so do partial JSON updates.
fn columns_changed(before: &BinlogRow, after: &BinlogRow, cols: &[bool]) -> bool {
    if before.len() != cols.len() || after.len() != cols.len() {
        return true;
    }
    cols.iter().enumerate().any(|(c, &read)| {
        read && (before.as_ref(c) != after.as_ref(c)
            || matches!(after.as_ref(c), Some(BinlogValue::JsonDiff(_))))
    })
}

fn mode_name(plan: &ViewPlan) -> (&'static str, Option<String>) {
    match &plan.mode {
        Mode::Spj => ("incremental", Some("spj".into())),
        Mode::Grouped => ("incremental", Some("grouped".into())),
        Mode::Global => ("incremental", Some("global".into())),
        Mode::Full(r) => ("full", Some(r.chars().take(255).collect())),
    }
}

impl Engine {
    /// `Ok(None)`: shutdown requested while waiting for the database or the lock.
    async fn start(
        cfg: Config,
        metrics: Arc<Metrics>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<(Engine, Option<Position>)>> {
        Metrics::set(&metrics.gauges.starting, 1);
        let src_opts = binlog::replica_opts(&cfg.dsn)?;
        let schema = schema_of(&src_opts, "LAZYVIEW_DSN")?;
        let Some(mut src) = connect_wait(&src_opts, "source database", &metrics, shutdown).await?
        else {
            return Ok(None);
        };
        let info = binlog::server_info(&mut src).await?;
        info!(
            "connected to {} at {}:{}, schema `{schema}`",
            info.version,
            src_opts.ip_or_hostname(),
            src_opts.tcp_port()
        );
        managed_service_checks(&mut src).await;
        let Some(mut meta_conn) =
            connect_wait(&src_opts, "source database", &metrics, shutdown).await?
        else {
            return Ok(None);
        };
        meta::ensure(&mut meta_conn, &schema).await?;
        let Some(leader) = coord::acquire(
            &mut meta_conn,
            &src_opts,
            &schema,
            &cfg.instance_id,
            &metrics,
            shutdown,
        )
        .await?
        else {
            return Ok(None);
        };

        let mut targets = BTreeMap::new();
        targets.insert(
            SOURCE_TARGET.to_string(),
            Target {
                opts: src_opts.clone(),
                schema: schema.clone(),
                conn: None,
                retry_at: None,
                failures: 0,
            },
        );
        for (name, dsn) in &cfg.targets {
            let opts = binlog::replica_opts(dsn)?;
            let ts = schema_of(&opts, &format!("target {name}"))?;
            targets.insert(
                name.clone(),
                Target {
                    opts,
                    schema: ts,
                    conn: None,
                    retry_at: None,
                    failures: 0,
                },
            );
        }

        let owned = meta::load_state(&mut meta_conn)
            .await?
            .into_iter()
            .filter(|(_, v)| v.managed)
            .map(|(k, v)| (k, v.hash))
            .collect();
        let (build_tx, build_rx) = mpsc::unbounded_channel();
        let cfg_build_concurrency = cfg.build_concurrency;
        let zero = Position {
            file: String::new(),
            pos: 0,
        };
        let mut e = Engine {
            cfg,
            metrics,
            src_opts,
            schema,
            flavor: info.flavor,
            src,
            meta: meta_conn,
            targets,
            catalog: Catalog::default(),
            slots: Vec::new(),
            def_names: HashMap::new(),
            owned,
            routes: HashMap::new(),
            table_ids: HashMap::new(),
            shapes: HashMap::new(),
            xa_current: None,
            xa_pending: HashMap::new(),
            defs_dirty: false,
            replan: HashMap::new(),
            failures: HashMap::new(),
            defs_recheck: None,
            last_src_ping: Instant::now(),
            boundary: zero.clone(),
            persisted: zero,
            last_persist: Instant::now(),
            in_txn: false,
            txn_foreign: false,
            persist_needed: false,
            first_dirty: None,
            last_gtid: None,
            snapshot_fence: None,
            generation: 0,
            build_tx,
            build_rx: Some(build_rx),
            build_slots: Arc::new(tokio::sync::Semaphore::new(cfg_build_concurrency)),
            leader,
            ready_logged: false,
            refreshed: HashSet::new(),
            last_touch: Instant::now(),
            last_stats: (Instant::now(), Default::default()),
            last_meta_ping: Instant::now(),
        };

        let defs = meta::load_definitions(&mut e.meta).await?;
        if !e.cfg.sync {
            // Validate definitions only (usable in CI: non-zero exit on any problem).
            let mut bad = 0;
            let mut names: HashMap<String, u64> = HashMap::new();
            for d in defs.iter().filter(|d| d.enabled) {
                match e
                    .parse_and_plan(&d.effective_definition(), &d.targets, None)
                    .await
                {
                    Ok(p) => {
                        if let Some(other) = names.insert(p.name.clone(), d.id) {
                            bad += 1;
                            error!(
                                "definition #{}: view `{}` is also defined by #{other}",
                                d.id, p.name
                            );
                        } else {
                            info!(
                                "definition #{}: `{}` ok — {} → {}",
                                d.id,
                                p.name,
                                p.mode,
                                d.targets.join(",")
                            );
                        }
                    }
                    Err(err) => {
                        bad += 1;
                        error!("definition #{}: {err:#}", d.id);
                    }
                }
            }
            if bad > 0 {
                bail!("{bad} invalid definition(s)");
            }
            info!(
                "LAZYVIEW_SYNC=false: metadata ready, {} definition(s) valid, not streaming",
                names.len()
            );
            return Ok(Some((e, None)));
        }

        let stored = meta::load_position(&mut e.meta, &e.cfg.instance_id).await?;
        let mut rebuild_all =
            matches!(e.cfg.start_mode, StartMode::Rebuild | StartMode::Once) || stored.is_none();
        let mut start = match &stored {
            Some(p) if !rebuild_all => p.clone(),
            _ => binlog::current_position(&mut e.src).await?,
        };
        // A position saved on another server (failover, restore, RESET MASTER) may name a file
        // that exists here too, with other contents.
        let identity = server_identity(&mut e.src, e.flavor)
            .await
            .unwrap_or_default();
        let saved_identity = meta::load_identity(&mut e.meta, &e.cfg.instance_id)
            .await?
            .filter(|s| !s.is_empty());
        let unusable = if rebuild_all {
            None
        } else if let Some(saved) =
            saved_identity.filter(|s| !identity.is_empty() && *s != identity)
        {
            Some(format!(
                "saved binlog position {start} was written by server {saved}, but the source is server {identity} (failover, restore or RESET MASTER?)"
            ))
        } else if !binlog::binlog_exists(&mut e.src, &start.file).await? {
            Some(format!(
                "saved binlog position {start} is no longer available on the server"
            ))
        } else {
            None
        };
        if let Some(why) = unusable {
            match e.cfg.on_missing_binlog {
                MissingBinlog::Fail => bail!(
                    "{why}; set LAZYVIEW_ON_MISSING_BINLOG=rebuild or LAZYVIEW_START_MODE=rebuild"
                ),
                MissingBinlog::Rebuild => {
                    warn!("{why}; rebuilding all views");
                    rebuild_all = true;
                    start = binlog::current_position(&mut e.src).await?;
                }
            }
        }
        if !identity.is_empty() {
            meta::save_identity(&mut e.meta, &e.cfg.instance_id, &identity).await?;
        }
        e.boundary = start.clone();
        e.load_slots(defs, !rebuild_all).await?;
        e.rebuild_routes();

        if e.slots.is_empty() {
            info!(
                "no views defined yet; add one with: INSERT INTO `{}`.`_lazyview` (definition) VALUES ('CREATE VIEW my_view AS SELECT ...')",
                e.schema
            );
        }
        if e.cfg.start_mode == StartMode::Once {
            e.wait_builds().await?;
            info!("LAZYVIEW_START_MODE=once: all views built");
            return Ok(Some((e, None)));
        }
        e.persist_position(true).await?;
        e.update_gauges();
        Ok(Some((e, Some(start))))
    }

    // ---------------------------------------------------------------- definitions & planning

    async fn table_info(&mut self, t: &TableRef) -> Result<Option<Arc<TableInfo>>> {
        self.catalog
            .get(&mut self.src, &t.0, &t.1, self.flavor == Flavor::MariaDb)
            .await
    }

    /// Parses and plans a definition against the current catalog. `own_def` excludes the slot
    /// itself from the chained-view check.
    async fn parse_and_plan(
        &mut self,
        definition: &str,
        targets: &[String],
        own_def: Option<u64>,
    ) -> Result<ViewPlan> {
        let parsed = analyze::parse_definition(definition)?;
        if let Some(sch) = &parsed.schema {
            if *sch != self.schema {
                bail!(
                    "schema-qualified view name `{sch}`.`{}` is not supported: views are created in the lazyview schema `{}` (and in each target's schema); write CREATE VIEW {} AS ...",
                    parsed.name, self.schema, parsed.name
                );
            }
        }
        for t in targets {
            if !self.targets.contains_key(t) {
                bail!("unknown target `{t}` (configure LAZYVIEW_TARGETS / LAZYVIEW_TARGET_<NAME>_DSN)");
            }
        }
        let tables = analyze::referenced_tables(&parsed.query, &self.schema);
        let mut infos = HashMap::new();
        for t in &tables {
            if t.0 == self.schema && t.1 == parsed.name {
                bail!("view `{}` cannot read itself", parsed.name);
            }
            // Chained views: an upstream MV must exist on the source to be watched.
            if let Some(up) = self
                .slots
                .iter()
                .find(|s| Some(s.def_id) != own_def && t.0 == self.schema && s.name == t.1)
            {
                if !up.targets.iter().any(|x| x == SOURCE_TARGET) {
                    bail!("view reads `{}`, which is materialized only on other instances; chained views need the upstream view on target `source`", t.1);
                }
            }
            match self.table_info(t).await? {
                Some(info) => {
                    infos.insert(t.clone(), info);
                }
                // Another definition materializes it but it is not built yet: plan once it is.
                None if t.0 == self.schema
                    && self
                        .def_names
                        .get(&t.1)
                        .is_some_and(|id| Some(*id) != own_def) =>
                {
                    return Err(WaitingFor(t.1.clone()).into())
                }
                None => bail!("table `{}`.`{}` does not exist", t.0, t.1),
            }
        }
        // Base tables of SQL views (the view is maintained by full rebuilds over them).
        let mut vq: Vec<(TableRef, usize)> = infos
            .values()
            .flat_map(|i| i.view_bases.iter().map(|b| (b.clone(), 0)))
            .collect();
        while let Some((t, depth)) = vq.pop() {
            if infos.contains_key(&t) || depth > 16 {
                continue;
            }
            if let Some(info) = self.table_info(&t).await? {
                vq.extend(info.view_bases.iter().map(|b| (b.clone(), depth + 1)));
                infos.insert(t, info);
            }
        }
        // Stored / loadable functions read tables lazyview cannot see.
        let calls = analyze::function_calls(&parsed.query);
        if !calls.is_empty() {
            let routines: Vec<(String, String)> = self
                .src
                .query("SELECT CAST(ROUTINE_SCHEMA AS CHAR), CAST(ROUTINE_NAME AS CHAR) FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION'")
                .await
                .unwrap_or_default();
            let udfs: Vec<String> = self
                .src
                .query("SELECT CAST(name AS CHAR) FROM mysql.func")
                .await
                .unwrap_or_default();
            for (sch, f) in &calls {
                let sch = sch.clone().unwrap_or_else(|| self.schema.clone());
                let stored = routines
                    .iter()
                    .any(|(rs, rn)| rs.eq_ignore_ascii_case(&sch) && rn.eq_ignore_ascii_case(f));
                let udf = udfs.iter().any(|u| u.eq_ignore_ascii_case(f));
                if stored || udf {
                    bail!(
                        "view calls {} function `{f}`: the tables it reads are invisible to lazyview, so the view could not be kept correct",
                        if udf { "loadable" } else { "stored" }
                    );
                }
            }
        }
        // Dependency cycles through other lazyview tables.
        let mut seen: HashSet<String> = HashSet::new();
        let mut stack: Vec<String> = tables
            .iter()
            .filter(|t| t.0 == self.schema)
            .map(|t| t.1.clone())
            .collect();
        while let Some(n) = stack.pop() {
            if !seen.insert(n.clone()) {
                continue;
            }
            if n == parsed.name {
                bail!(
                    "cyclic view dependency: `{}` (indirectly) reads itself",
                    parsed.name
                );
            }
            for o in self
                .slots
                .iter()
                .filter(|o| Some(o.def_id) != own_def && Self::slot_uses(o, &n))
            {
                for p in [
                    o.live.as_ref().map(|l| &l.plan),
                    o.next.as_ref().map(|x| &x.plan),
                ]
                .into_iter()
                .flatten()
                {
                    stack.extend(
                        p.tables
                            .iter()
                            .filter(|t| t.0 == self.schema)
                            .map(|t| t.1.clone()),
                    );
                }
            }
        }
        // Parents of cascading foreign keys (their changes cascade without binlog rows).
        let mut queue: Vec<(TableRef, usize)> = infos
            .values()
            .flat_map(|i| i.cascades.iter().map(|f| (f.parent.clone(), 0)))
            .collect();
        while let Some((t, depth)) = queue.pop() {
            if infos.contains_key(&t) || depth > 8 {
                continue;
            }
            if let Some(info) = self.table_info(&t).await? {
                queue.extend(info.cascades.iter().map(|f| (f.parent.clone(), depth + 1)));
                infos.insert(t, info);
            }
        }
        let plan = analyze::plan(&parsed, &self.schema, &infos)?;
        if plan.cascades.iter().any(|c| c.unit_from_parent.is_none()) && plan.is_incremental() {
            warn!(view = %plan.name, "reads a child of a cascading foreign key whose units cannot be derived from the parent; parent deletes/updates trigger an online rebuild");
        }
        Ok(plan)
    }

    async fn set_state(
        &mut self,
        name: &str,
        target: &str,
        def_id: u64,
        hash: &str,
        plan: Option<&ViewPlan>,
        state: &str,
        err: Option<&str>,
    ) {
        let (mode, detail) = plan.map(mode_name).unwrap_or(("unknown", None));
        let r = meta::set_state(
            &mut self.meta,
            StateUpdate {
                name,
                target,
                definition_id: def_id,
                hash,
                mode,
                detail: detail.as_deref(),
                state,
                error: err,
                // Ownership is recorded only once our table is in place under `name`.
                managed: state == "ready",
            },
        )
        .await;
        if let Err(e) = r {
            warn!("writing _lazyview_state for {name}@{target}: {e:#}");
        }
        if state == "ready" {
            self.owned
                .insert((name.to_string(), target.to_string()), hash.to_string());
        }
        if state != "error" {
            let _ = meta::delete_stale_state(&mut self.meta, def_id, name).await;
        }
    }

    /// The view name in a `CREATE VIEW` definition is the source of truth: `_lazyview.name` is
    /// filled in or updated to it (a renaming definition renames the view online). A bare SELECT
    /// takes its name from `name`, so the two always agree then.
    async fn check_name(&mut self, d: &meta::DefRow, plan: &ViewPlan) -> Result<()> {
        if d.name.as_deref() == Some(plan.name.as_str()) {
            return Ok(());
        }
        match meta::set_name(&mut self.meta, d.id, &plan.name).await {
            Ok(()) => Ok(()),
            Err(e) if server_code(&e) == Some(1062) => {
                let other = meta::definition_named(&mut self.meta, &plan.name)
                    .await
                    .ok()
                    .flatten();
                bail!(
                    "name `{}` already used by row #{}",
                    plan.name,
                    other.map(|i| i.to_string()).unwrap_or_else(|| "?".into())
                )
            }
            Err(e) => {
                warn!("recording _lazyview.name for #{}: {e:#}", d.id);
                Ok(())
            }
        }
    }

    /// Earliest time a routine full rebuild of this slot may start (rebuilds of full-mode views,
    /// or of incremental views whose affected rows are unknown, run at most once per interval).
    /// `None`: right away (no rebuild in this run yet, e.g. a resumed view).
    fn full_due(&self, s: &Slot) -> Option<Instant> {
        s.last_full.map(|t| t + self.cfg.full_refresh_interval)
    }

    fn idx_of(&self, def_id: u64) -> Option<usize> {
        self.slots.iter().position(|s| s.def_id == def_id)
    }

    /// Queues a re-plan + rebuild. `retry` applies exponential backoff (failed attempts);
    /// `freeze` stops maintaining the live version meanwhile (its plan no longer matches the
    /// source schema).
    fn schedule_replan(&mut self, def_id: u64, freeze: bool, retry: bool) {
        let at = self.boundary.clone();
        self.schedule_replan_at(def_id, freeze, retry, &at);
    }

    /// As [`Self::schedule_replan`]; unless it is a retry (the view is in error then), readers
    /// see a stale view from `since` on until a rebuild started after it is swapped in.
    fn schedule_replan_at(&mut self, def_id: u64, freeze: bool, retry: bool, since: &Position) {
        let delay = if retry {
            let n = self.failures.entry(def_id).or_insert(0);
            *n += 1;
            Duration::from_secs((1u64 << (*n).min(8)).min(300))
        } else {
            Duration::ZERO
        };
        let due = Instant::now() + delay;
        let e = self.replan.entry(def_id).or_insert((due, freeze));
        e.0 = e.0.min(due);
        e.1 |= freeze;
        if !retry {
            if let Some(s) = self.slots.iter_mut().find(|s| s.def_id == def_id) {
                if s.live.is_some() || s.next.is_some() {
                    s.stale_since.get_or_insert_with(|| since.clone());
                }
            }
        }
        self.first_dirty.get_or_insert_with(Instant::now);
    }

    /// Re-establishes the source-read connection after a connection-class error.
    async fn heal_src(&mut self) {
        match connect(&self.src_opts).await {
            Ok(c) => {
                self.src = c;
                info!("reconnected to source database");
            }
            Err(e) => warn!("source database still unreachable: {}", e.root_cause()),
        }
        self.metrics.beat();
    }

    /// Re-opens the metadata connection after a connection-class error. The leadership lock
    /// died with the old connection: it is taken again and the fencing epoch checked, and if
    /// another instance took over meanwhile this one stops (`Err`). `Ok(false)`: still
    /// unreachable, try again later.
    async fn heal_meta(&mut self) -> Result<bool> {
        let conn = connect(&self.src_opts).await;
        self.metrics.beat();
        let mut conn = match conn {
            Ok(c) => c,
            Err(e) => {
                warn!("metadata connection still unreachable: {}", e.root_cause());
                return Ok(false);
            }
        };
        match coord::reacquire(&mut conn, &self.leader, &self.cfg.instance_id).await {
            Ok(()) => {
                self.meta = conn;
                info!("metadata connection re-established; still the active instance");
                Ok(true)
            }
            Err(e) if is_conn_error(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// A failed metadata statement: connection-class errors heal the connection and are
    /// retried later; anything else (lost leadership, a changed epoch) is fatal.
    async fn meta_failed(&mut self, e: anyhow::Error, what: &str) -> Result<()> {
        if !is_conn_error(&e) {
            return Err(e.context(what.to_string()));
        }
        warn!("{what}: {}; reconnecting", e.root_cause());
        self.heal_meta().await.map(|_| ())
    }

    /// The whole view is in error (state `error` on every target, keeping the mode of its latest
    /// plan); it no longer holds back the saved position.
    async fn slot_error(&mut self, idx: usize, err: &anyhow::Error) {
        let msg = format!("{err:#}");
        error!(view = %self.slots[idx].name, "{msg}");
        let s = &mut self.slots[idx];
        s.error = Some(msg.clone());
        s.waiting = None;
        s.stale_since = None;
        let (name, id, hash, targets, plan) = (
            s.name.clone(),
            s.def_id,
            s.hash.clone(),
            s.targets.clone(),
            s.last_plan.clone(),
        );
        for t in targets {
            self.set_state(&name, &t, id, &hash, plan.as_deref(), "error", Some(&msg))
                .await;
        }
    }

    /// A chained view whose upstream lazyview table is not built yet: state `waiting` (not an
    /// error); it is planned when the upstream is swapped in (see [`Self::wake_waiting`]).
    async fn slot_waiting(&mut self, idx: usize, upstream: &str) {
        let s = &mut self.slots[idx];
        if s.waiting.as_deref() != Some(upstream) {
            info!(view = %s.name, "waiting for upstream view `{upstream}` to be built");
        }
        s.waiting = Some(upstream.to_string());
        s.error = None;
        s.stale_since = None;
        let msg = WaitingFor(upstream.to_string()).to_string();
        let (name, id, hash, targets) =
            (s.name.clone(), s.def_id, s.hash.clone(), s.targets.clone());
        for t in targets {
            self.set_state(&name, &t, id, &hash, None, "waiting", Some(&msg))
                .await;
        }
    }

    /// Plans views waiting for `upstream` (just swapped in on the source).
    fn wake_waiting(&mut self, upstream: &str) {
        let ids: Vec<u64> = self
            .slots
            .iter()
            .filter(|s| s.waiting.as_deref() == Some(upstream))
            .map(|s| s.def_id)
            .collect();
        for id in ids {
            self.schedule_replan(id, false, false);
        }
    }

    /// A definition that cannot be planned (yet).
    async fn plan_failed(&mut self, idx: usize, e: &anyhow::Error) {
        match e.downcast_ref::<WaitingFor>() {
            Some(w) => {
                let up = w.0.clone();
                self.slot_waiting(idx, &up).await
            }
            None => self.slot_error(idx, e).await,
        }
    }

    /// Indexes the view names of the enabled definitions.
    fn index_definitions(&mut self, defs: &[meta::DefRow]) {
        self.def_names = defs
            .iter()
            .filter(|d| d.enabled)
            .filter_map(|d| {
                analyze::parse_definition(&d.effective_definition())
                    .ok()
                    .map(|p| (p.name, d.id))
            })
            .collect();
    }

    /// Plans definition `d` for slot `idx`, name checks included. Failures and waits are
    /// recorded on the slot (`None`).
    async fn plan_slot(&mut self, idx: usize, d: &meta::DefRow) -> Option<Arc<ViewPlan>> {
        let id = d.id;
        let plan = match self
            .parse_and_plan(&d.effective_definition(), &d.targets, Some(id))
            .await
        {
            Ok(p) => Arc::new(p),
            Err(e) => {
                self.plan_failed(idx, &e).await;
                return None;
            }
        };
        self.slots[idx].last_plan = Some(plan.clone());
        let clash = match self.check_name(d, &plan).await {
            Err(e) => Some(e),
            Ok(()) if self.name_taken(idx, &plan.name) => Some(anyhow!(
                "another definition already materializes `{}`",
                plan.name
            )),
            Ok(()) => None,
        };
        if let Some(e) = clash {
            if self.slots[idx].live.is_none() {
                self.slots[idx].name = format!("#{id}");
            }
            self.slot_error(idx, &e).await;
            return None;
        }
        self.slots[idx].waiting = None;
        Some(plan)
    }

    /// Initial slot creation at startup. With `resume`, valid existing tables become live (per
    /// target; the others are built).
    async fn load_slots(&mut self, defs: Vec<meta::DefRow>, resume: bool) -> Result<()> {
        let defs: Vec<meta::DefRow> = defs.into_iter().filter(|d| d.enabled).collect();
        self.index_definitions(&defs);
        for d in &defs {
            let idx = self.new_slot(d);
            let Some(plan) = self.plan_slot(idx, d).await else {
                continue;
            };
            self.slots[idx].name = plan.name.clone();
            let mut build = d.targets.clone();
            if resume {
                match self.try_resume(idx, &plan).await {
                    Ok(rest) => build = rest,
                    Err(e) => warn!(view = %plan.name, "cannot resume, rebuilding: {e:#}"),
                }
            }
            if build.is_empty() {
                continue;
            }
            let partial = self.slots[idx].live.is_some();
            let name = plan.name.clone();
            self.start_next(idx, plan, name, build, partial).await;
        }
        // Chained views defined before an upstream that was resumed after them.
        let live: Vec<String> = self
            .slots
            .iter()
            .filter_map(|s| s.live.as_ref())
            .filter(|l| l.targets.iter().any(|t| t == SOURCE_TARGET))
            .map(|l| l.name.clone())
            .collect();
        for name in live {
            self.wake_waiting(&name);
        }
        // Tables of definitions deleted/disabled/renamed while lazyview was stopped.
        let used: HashSet<(String, String)> = self
            .slots
            .iter()
            .flat_map(|s| {
                let mut v: Vec<(String, String)> = s
                    .targets
                    .iter()
                    .map(|t| (s.name.clone(), t.clone()))
                    .collect();
                if let Some(l) = &s.live {
                    v.extend(l.targets.iter().map(|t| (l.name.clone(), t.clone())));
                }
                if let Some(n) = &s.next {
                    v.extend(n.targets.iter().map(|t| (n.name.clone(), t.clone())));
                }
                v.extend(s.retired.iter().cloned());
                v
            })
            .collect();
        let orphans: Vec<(String, String)> = self
            .owned
            .keys()
            .filter(|k| !used.contains(*k))
            .cloned()
            .collect();
        for (name, t) in orphans {
            info!(view = %name, target = %t, "definition no longer exists; removing its table");
            self.drop_owned(&name, &t).await;
        }
        // Tables marked as lazyview's in its own schema that no state row records (a crash
        // between creating or swapping a table and recording it).
        if self.cfg.drop_removed {
            let marked: Vec<String> = self
                .meta
                .exec(
                    "SELECT CAST(TABLE_NAME AS CHAR) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = ? AND TABLE_COMMENT LIKE ?",
                    (&self.schema, format!("{}%", mv::MARKER_PREFIX)),
                )
                .await
                .unwrap_or_default();
            for table in marked {
                let base = mv::base_name(&table).to_string();
                if used.contains(&(base, SOURCE_TARGET.to_string()))
                    || self
                        .owned
                        .contains_key(&(table.clone(), SOURCE_TARGET.to_string()))
                {
                    continue;
                }
                info!(table = %table, "removing a table lazyview created for a definition that no longer exists");
                if let Err(e) = mv::drop_table(&mut self.meta, &self.schema, &table).await {
                    warn!("dropping {table}: {e:#}");
                }
            }
        }
        Ok(())
    }

    fn new_slot(&mut self, d: &meta::DefRow) -> usize {
        let def = d.effective_definition();
        let parsed = analyze::parse_definition(&def).ok();
        self.slots.push(Slot {
            def_id: d.id,
            name: parsed
                .as_ref()
                .map(|p| p.name.clone())
                .unwrap_or_else(|| format!("#{}", d.id)),
            wanted: parsed
                .map(|p| analyze::referenced_tables(&p.query, &self.schema))
                .unwrap_or_default(),
            hash: analyze::definition_hash(&def),
            definition: def,
            targets: d.targets.clone(),
            live: None,
            next: None,
            error: None,
            retired: Vec::new(),
            last_full: None,
            stale_since: None,
            waiting: None,
            retry_at: None,
            down: BTreeMap::new(),
            last_plan: None,
            refresh_failures: 0,
            replan_after: false,
        });
        self.slots.len() - 1
    }

    /// Whether another definition uses `name` (current, live, building or still retiring).
    fn name_taken(&self, idx: usize, name: &str) -> bool {
        self.slots
            .iter()
            .enumerate()
            .any(|(i, s)| i != idx && Self::slot_uses(s, name))
    }

    fn slot_uses(s: &Slot, name: &str) -> bool {
        s.name == name
            || s.live.as_ref().map(|l| l.name == name).unwrap_or(false)
            || s.next.as_ref().map(|n| n.name == name).unwrap_or(false)
            || s.retired.iter().any(|(n, _)| n == name)
    }

    /// The write connection of target `t`, (re)connecting if needed. After a failed attempt the
    /// target fails fast until its backoff expires.
    async fn target_conn(&mut self, t: &str) -> Result<&mut Conn> {
        let metrics = self.metrics.clone();
        let target = self
            .targets
            .get_mut(t)
            .ok_or_else(|| anyhow!("unknown target {t}"))?;
        if target.conn.is_none() {
            if let Some(at) = target.retry_at.filter(|at| *at > Instant::now()) {
                bail!(
                    "target {t} is unreachable (next connection attempt in {:.1}s)",
                    (at - Instant::now()).as_secs_f64()
                );
            }
            let r = match tokio::time::timeout(TARGET_CONNECT_TIMEOUT, connect(&target.opts)).await
            {
                Ok(r) => r.with_context(|| format!("connecting to target {t}")),
                Err(_) => Err(anyhow!(
                    "connecting to target {t}: timed out after {TARGET_CONNECT_TIMEOUT:?}"
                )),
            };
            metrics.beat();
            match r {
                Ok(c) => {
                    target.conn = Some(c);
                    target.failures = 0;
                    target.retry_at = None;
                }
                Err(e) => {
                    target.failures += 1;
                    target.retry_at = Some(Instant::now() + target_backoff(target.failures));
                    return Err(e);
                }
            }
        }
        Ok(target.conn.as_mut().unwrap())
    }

    /// Existing tables are reused per target: a ready state with the same definition hash and a
    /// table whose columns match the plan's layout. Returns the targets that still need a build
    /// (unreachable ones included: they are built once they answer).
    async fn try_resume(&mut self, idx: usize, plan: &Arc<ViewPlan>) -> Result<Vec<String>> {
        let s = &self.slots[idx];
        let (name, targets, hash) = (plan.name.clone(), s.targets.clone(), s.hash.clone());
        let state = meta::load_state(&mut self.meta).await?;
        let candidates: Vec<String> = targets
            .iter()
            .filter(|t| {
                let key = (name.clone(), t.to_string());
                self.owned.get(&key) == Some(&hash)
                    && state.get(&key).map(|r| r.state == "ready").unwrap_or(false)
            })
            .cloned()
            .collect();
        if candidates.is_empty() {
            return Ok(targets);
        }
        let layout = mv::probe(&mut self.src, &self.schema, plan).await?;
        let mut resumed = Vec::new();
        for t in candidates {
            let ts = self.targets[&t].schema.clone();
            let cols = match self.target_conn(&t).await {
                Ok(conn) => mv::table_columns(conn, &ts, &name).await.ok(),
                Err(_) => None,
            };
            if cols.is_some_and(|c| mv::layout_matches(&layout, &c)) {
                resumed.push(t);
            }
        }
        if resumed.is_empty() {
            return Ok(targets);
        }
        let rest: Vec<String> = targets
            .iter()
            .filter(|t| !resumed.contains(t))
            .cloned()
            .collect();
        info!(view = %name, targets = %resumed.join(","), "resuming existing materialized table");
        self.slots[idx].live = Some(Version {
            plan: plan.clone(),
            layout,
            name,
            targets: resumed,
            dirty: ViewDirty {
                images: vec![HashSet::new(); plan.aliases.len()],
                ..Default::default()
            },
            frozen: false,
            since: None,
            lag: BTreeMap::new(),
            recent: Lookback::default(),
        });
        Ok(rest)
    }

    /// Drops the shadow table of `name` on target `t` (best effort).
    async fn drop_shadow(&mut self, t: &str, name: &str) {
        let ts = self.targets[t].schema.clone();
        if let Ok(c) = self.target_conn(t).await {
            let _ = mv::drop_table(c, &ts, &mv::shadow_name(name)).await;
        }
    }

    /// Starts building a new version in the background on `targets` (`partial`: adds them to
    /// the unchanged live version). Each target is prepared independently by the build: one
    /// that is unreachable, or holds a table lazyview does not own, is reported back and retried
    /// later while the view is built on the others.
    async fn start_next(
        &mut self,
        idx: usize,
        plan: Arc<ViewPlan>,
        name: String,
        targets: Vec<String>,
        partial: bool,
    ) {
        if let Some(n) = self.slots[idx].next.take() {
            n.task.abort();
            if n.name != name || n.targets != targets {
                for t in &n.targets {
                    self.drop_shadow(t, &n.name).await;
                }
            }
        }
        let def_id = self.slots[idx].def_id;
        let hash = self.slots[idx].hash.clone();
        // Side tables (`<name>__lv_new` / `__lv_old`) are only ever ours if a state row of this
        // definition exists for that name and target; the table itself if we created it.
        let state = meta::load_state(&mut self.meta).await.unwrap_or_default();
        let specs: Vec<BuildSpec> = targets
            .iter()
            .map(|t| {
                let key = (name.clone(), t.clone());
                let live_has = self.slots[idx]
                    .live
                    .as_ref()
                    .map(|l| l.name == name && l.targets.contains(t))
                    .unwrap_or(false);
                BuildSpec {
                    target: t.clone(),
                    opts: self.targets[t].opts.clone(),
                    schema: self.targets[t].schema.clone(),
                    marker: mv::marker(&name, def_id),
                    check_side: !state
                        .get(&key)
                        .map(|r| r.definition_id == def_id)
                        .unwrap_or(false),
                    check_table: !(self.owned.contains_key(&key) || live_has),
                }
            })
            .collect();
        self.generation += 1;
        let generation = self.generation;
        for t in &targets {
            self.set_state(&name, t, def_id, &hash, Some(&plan), "building", None)
                .await;
        }
        let task = tokio::spawn(build_task(
            self.src_opts.clone(),
            self.schema.clone(),
            specs,
            plan.clone(),
            name.clone(),
            self.cfg.write_batch_rows,
            self.metrics.clone(),
            self.build_tx.clone(),
            self.build_slots.clone(),
            def_id,
            generation,
        ));
        let routine = self.slots[idx]
            .live
            .as_ref()
            .map(|l| Arc::ptr_eq(&l.plan, &plan))
            .unwrap_or(false);
        if routine && !partial {
            debug!(view = %name, "rebuilding");
        } else {
            info!(view = %name, targets = %targets.join(","), mode = %plan.mode, tracing = %plan.tracing(), "building");
        }
        Metrics::add(&self.metrics.full_rebuilds, 1);
        self.slots[idx].last_full = Some(Instant::now());
        self.slots[idx].error = None;
        self.slots[idx].next = Some(Next {
            dirty: ViewDirty {
                images: vec![HashSet::new(); plan.aliases.len()],
                ..Default::default()
            },
            plan,
            name,
            targets,
            generation,
            task,
            done: None,
            partial,
            hash,
            swapped: HashSet::new(),
            not_before: None,
            started: self.boundary.clone(),
            since: None,
            recent: Lookback::default(),
        });
        self.rebuild_routes();
    }

    /// Re-reads `_lazyview` and reconciles slots: new, changed, renamed, retargeted, removed.
    async fn reload_definitions(&mut self) -> Result<()> {
        Metrics::add(&self.metrics.definition_reloads, 1);
        let defs = meta::load_definitions(&mut self.meta).await?;
        let enabled: HashMap<u64, meta::DefRow> = defs
            .into_iter()
            .filter(|d| d.enabled)
            .map(|d| (d.id, d))
            .collect();

        // Removed or disabled.
        let mut i = 0;
        while i < self.slots.len() {
            if enabled.contains_key(&self.slots[i].def_id) {
                i += 1;
                continue;
            }
            let s = self.slots.remove(i);
            info!(view = %s.name, "definition removed");
            self.retire_slot(s).await;
            self.table_ids.clear();
        }

        let defs: Vec<meta::DefRow> = enabled.values().cloned().collect();
        self.index_definitions(&defs);
        // Views waiting for an upstream whose definition is gone now fail (table missing).
        let orphaned: Vec<u64> = self
            .slots
            .iter()
            .filter(|s| {
                s.waiting
                    .as_ref()
                    .is_some_and(|w| !self.def_names.contains_key(w))
            })
            .map(|s| s.def_id)
            .collect();
        for id in orphaned {
            self.schedule_replan(id, false, false);
        }
        let mut ids: Vec<u64> = enabled.keys().copied().collect();
        ids.sort();
        for id in ids {
            let d = &enabled[&id];
            let hash = analyze::definition_hash(&d.effective_definition());
            let idx = match self.slots.iter().position(|s| s.def_id == id) {
                Some(idx) => {
                    let s = &self.slots[idx];
                    if s.hash == hash && s.targets == d.targets {
                        continue;
                    }
                    idx
                }
                None => self.new_slot(d),
            };
            let is_new = self.slots[idx].live.is_none() && self.slots[idx].next.is_none();
            let s = &mut self.slots[idx];
            s.definition = d.effective_definition();
            s.hash = hash;
            s.targets = d.targets.clone();
            s.last_plan = None;
            s.down.clear();
            if let Ok(p) = analyze::parse_definition(&d.effective_definition()) {
                s.wanted = analyze::referenced_tables(&p.query, &self.schema);
                if s.live.is_none() {
                    s.name = p.name;
                }
            }
            let Some(plan) = self.plan_slot(idx, d).await else {
                continue;
            };
            let name = plan.name.clone();
            // Everything currently live (or already retiring) that the new version does not
            // replace in place is dropped once the new version is live.
            let mut retired = std::mem::take(&mut self.slots[idx].retired);
            if let Some(l) = &self.slots[idx].live {
                retired.extend(l.targets.iter().map(|t| (l.name.clone(), t.clone())));
            }
            retired.retain(|(n, t)| !(n == &name && d.targets.contains(t)));
            retired.sort();
            retired.dedup();
            self.slots[idx].retired = retired;
            self.slots[idx].name = name.clone();
            if is_new {
                info!(view = %name, "new definition");
            } else {
                info!(view = %name, "definition changed; building the new version online (current version stays live)");
            }
            self.start_next(idx, plan, name, d.targets.clone(), false)
                .await;
        }
        self.rebuild_routes();
        Ok(())
    }

    async fn retire_slot(&mut self, s: Slot) {
        if let Some(n) = s.next {
            n.task.abort();
            for t in &n.targets {
                self.drop_shadow(t, &n.name).await;
            }
        }
        let mut tables: Vec<(String, String)> = s
            .targets
            .iter()
            .map(|t| (s.name.clone(), t.clone()))
            .collect();
        tables.extend(s.retired);
        if let Some(l) = &s.live {
            tables.extend(l.targets.iter().map(|t| (l.name.clone(), t.clone())));
        }
        tables.sort();
        tables.dedup();
        for (name, t) in tables {
            if self.slots.iter().any(|o| Self::slot_uses(o, &name)) {
                continue;
            }
            self.drop_owned(&name, &t).await;
        }
    }

    async fn drop_owned(&mut self, name: &str, target: &str) {
        let key = (name.to_string(), target.to_string());
        if self.owned.remove(&key).is_none() {
            let _ = meta::delete_state(&mut self.meta, name, target).await;
            return;
        }
        if self.cfg.drop_removed {
            if let Some(t) = self.targets.get(target) {
                let ts = t.schema.clone();
                match self.target_conn(target).await {
                    Ok(c) => {
                        if let Err(e) = mv::drop_table(c, &ts, name).await {
                            warn!("dropping {name}@{target}: {e:#}");
                        }
                    }
                    Err(e) => warn!("dropping {name}@{target}: {e:#}"),
                }
            }
        }
        let _ = meta::delete_state(&mut self.meta, name, target).await;
    }

    fn rebuild_routes(&mut self) {
        let mut routes: HashMap<TableRef, Vec<RouteT>> = HashMap::new();
        for (si, s) in self.slots.iter().enumerate() {
            let versions = [
                s.live
                    .as_ref()
                    .filter(|l| !l.frozen)
                    .map(|l| (false, &l.plan)),
                s.next.as_ref().map(|n| (true, &n.plan)),
            ];
            for (next, plan) in versions.into_iter().flatten() {
                if plan.is_incremental() || matches!(plan.mode, Mode::Global) {
                    for (ai, a) in plan.aliases.iter().enumerate() {
                        routes
                            .entry((a.table.schema.clone(), a.table.name.clone()))
                            .or_default()
                            .push(RouteT {
                                slot: si,
                                next,
                                kind: RouteKind::Alias(ai),
                            });
                    }
                } else {
                    for (ti, t) in plan.tables.iter().enumerate() {
                        let kind = match plan.table_columns.get(ti) {
                            Some(Some(_)) => RouteKind::All(ti),
                            _ => RouteKind::Any,
                        };
                        routes.entry(t.clone()).or_default().push(RouteT {
                            slot: si,
                            next,
                            kind,
                        });
                    }
                }
                for (ci, c) in plan.cascades.iter().enumerate() {
                    routes
                        .entry((c.parent.schema.clone(), c.parent.name.clone()))
                        .or_default()
                        .push(RouteT {
                            slot: si,
                            next,
                            kind: RouteKind::Cascade(ci),
                        });
                }
            }
        }
        self.routes = routes.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
        self.table_ids.clear();
    }

    /// Plan, change buffer and "oldest unapplied change" of the routed version.
    fn version_of(
        &mut self,
        r: RouteT,
    ) -> Option<(&ViewPlan, &mut ViewDirty, &mut Option<Position>)> {
        let s = self.slots.get_mut(r.slot)?;
        if r.next {
            s.next
                .as_mut()
                .map(|n| (&*n.plan, &mut n.dirty, &mut n.since))
        } else {
            s.live
                .as_mut()
                .map(|l| (&*l.plan, &mut l.dirty, &mut l.since))
        }
    }

    fn plan_of(&self, r: RouteT) -> Option<&Arc<ViewPlan>> {
        let s = self.slots.get(r.slot)?;
        if r.next {
            s.next.as_ref().map(|n| &n.plan)
        } else {
            s.live.as_ref().map(|l| &l.plan)
        }
    }

    /// Checks routed plans against the column count of a TABLE_MAP. A plan built for another
    /// shape cannot decode the rows: they are skipped for it when they predate the plan (its
    /// build already read them), and the view is re-planned when the plan is outdated (DDL
    /// that was not seen as a statement). Other views of the table are unaffected.
    async fn check_shapes(
        &mut self,
        key: &TableRef,
        routes: Arc<Vec<RouteT>>,
        columns: u64,
    ) -> Arc<Vec<RouteT>> {
        let expected = |plan: &ViewPlan, kind: RouteKind| match kind {
            RouteKind::Alias(a) => Some(plan.aliases[a].table.columns.len()),
            RouteKind::Cascade(c) => Some(plan.cascades[c].parent.columns.len()),
            RouteKind::All(ti) => plan.table_columns[ti].as_ref().map(Vec::len),
            RouteKind::Any => None,
        };
        let mismatch =
            |plan: &ViewPlan, kind| expected(plan, kind).is_some_and(|n| n as u64 != columns);
        if !routes.iter().any(|r| {
            self.plan_of(*r)
                .map(|p| mismatch(p, r.kind))
                .unwrap_or(false)
        }) {
            return routes;
        }
        let current = match self.shapes.get(&(key.clone(), columns)) {
            Some(n) => *n,
            None => {
                // One fresh read per (table, shape) until the next DDL clears the cache.
                self.catalog.invalidate(key);
                let n = self
                    .table_info(key)
                    .await
                    .ok()
                    .flatten()
                    .map(|i| i.columns.len());
                self.shapes.insert((key.clone(), columns), n);
                n
            }
        };
        let mut kept = Vec::with_capacity(routes.len());
        let mut outdated = Vec::new();
        for r in routes.iter() {
            let Some(plan) = self.plan_of(*r) else {
                continue;
            };
            if !mismatch(plan, r.kind) {
                kept.push(*r);
                continue;
            }
            if expected(plan, r.kind) != current {
                outdated.push(self.slots[r.slot].def_id);
            }
        }
        outdated.sort_unstable();
        outdated.dedup();
        for id in outdated {
            if !self.replan.contains_key(&id) {
                warn!(table = %key.1, "table definition differs from the view's plan; re-planning");
                self.schedule_replan(id, true, false);
            }
        }
        Arc::new(kept)
    }

    // ---------------------------------------------------------------- streaming

    async fn stream(
        &mut self,
        start: Position,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut build_rx = self.build_rx.take().expect("stream runs once");
        let mut backoff = Duration::from_millis(500);
        let mut pos = start;
        // Not even the server's heartbeats for this long: the connection is dead (half-open).
        let silence = (self.cfg.heartbeat * 3).max(Duration::from_secs(15));
        'reconnect: loop {
            self.metrics.beat();
            let connected = tokio::time::timeout(
                BINLOG_CONNECT_TIMEOUT,
                BinlogReader::connect(
                    self.src_opts.clone(),
                    self.cfg.server_id,
                    self.flavor,
                    &pos,
                    self.cfg.heartbeat,
                ),
            )
            .await
            .unwrap_or_else(|_| Err(anyhow!("timed out after {BINLOG_CONNECT_TIMEOUT:?}")));
            self.metrics.beat();
            let mut reader = match connected {
                Ok(r) => r,
                Err(e) => {
                    if Self::binlog_unusable(&e) {
                        self.on_missing_binlog(&e).await?;
                        pos = self.boundary.clone();
                        continue 'reconnect;
                    }
                    if binlog::is_duplicate_server_id(&e) && self.reroll_server_id() {
                        continue 'reconnect;
                    }
                    warn!("binlog connect failed: {e:#}; retrying in {backoff:?}");
                    // Short waits keep the liveness heartbeat going through a long outage.
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.changed() => if *shutdown.borrow() { break 'reconnect; },
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    continue 'reconnect;
                }
            };
            info!(position = %pos, "streaming binlog");
            Metrics::set(&self.metrics.gauges.streaming, 1);
            Metrics::set(&self.metrics.gauges.starting, 0);
            let mut last_event = Instant::now();
            let mut delivered = false;
            loop {
                Metrics::set(
                    &self.metrics.gauges.heartbeat_unix,
                    crate::metrics::unix_now(),
                );
                let deadline = self.flush_deadline();
                let sleep_until =
                    deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
                let mut idle = false;
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break 'reconnect;
                        }
                    }
                    Some(done) = build_rx.recv() => self.on_build_done(done),
                    item = reader.next() => {
                        last_event = Instant::now();
                        match item {
                            Ok(Some(item)) => {
                                if !delivered {
                                    delivered = true;
                                    backoff = Duration::from_millis(500);
                                }
                                Metrics::add(&self.metrics.events, 1);
                                if let Err(e) = self.on_item(&reader, item).await {
                                    error!("processing binlog event: {e:#}");
                                    return Err(e);
                                }
                            }
                            Ok(None) => {
                                warn!("binlog stream closed by server; reconnecting");
                                break;
                            }
                            Err(e) => {
                                if Self::binlog_unusable(&e) {
                                    self.on_missing_binlog(&e).await?;
                                } else if binlog::is_duplicate_server_id(&e) {
                                    self.reroll_server_id();
                                } else {
                                    warn!("binlog stream error: {e:#}; reconnecting");
                                }
                                break;
                            }
                        }
                    }
                    // Polled last: fires only while the stream has nothing to deliver.
                    _ = tokio::time::sleep_until(sleep_until.into()), if deadline.is_some() && !self.in_txn => idle = true,
                    _ = tokio::time::sleep_until((last_event + silence).into()) => {
                        warn!("no binlog event or heartbeat for {silence:?}; reconnecting");
                        break;
                    }
                }
                // A due flush runs once the stream is drained. While a backlog keeps delivering
                // (catch-up), it runs only every CATCH_UP_BATCH past its deadline (or at
                // LAZYVIEW_BATCH_MAX_KEYS), so a backlog is applied in batches rather than one
                // flush per transaction.
                let flush = if idle {
                    self.flush_due()
                } else {
                    self.flush_due_within(CATCH_UP_BATCH)
                };
                if !self.in_txn && flush {
                    self.flush().await?;
                }
                if !self.in_txn {
                    self.housekeeping().await?;
                }
            }
            Metrics::set(&self.metrics.gauges.streaming, 0);
            let _ = reader.close().await;
            // Changes of a partially read transaction are kept: re-applying them is harmless.
            // Its TABLE_MAPs and XA rows are read again from the boundary.
            self.in_txn = false;
            self.table_ids.clear();
            self.xa_current = None;
            self.txn_foreign = false;
            pos = self.boundary.clone();
            // A stream that fails right after connecting backs off like a failed connect.
            let pause = if delivered {
                Duration::from_millis(200)
            } else {
                let p = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                p
            };
            self.metrics.beat();
            tokio::select! {
                _ = tokio::time::sleep(pause) => {}
                _ = shutdown.changed() => if *shutdown.borrow() { break 'reconnect; },
            }
        }
        if !self.in_txn {
            let _ = self.flush().await;
        }
        for s in &self.slots {
            if let Some(n) = &s.next {
                n.task.abort();
            }
        }
        info!("stopped");
        Ok(())
    }

    /// ER 1236 from the dump thread (except a duplicate server id): the position cannot be
    /// read on this server (purged, past the end of a file, inside an event after a failover
    /// or RESET MASTER). Handled like a purged binlog (LAZYVIEW_ON_MISSING_BINLOG).
    fn binlog_unusable(e: &anyhow::Error) -> bool {
        binlog::is_missing_binlog(e)
            || (server_code(e) == Some(1236) && !binlog::is_duplicate_server_id(e))
    }

    /// Another replica uses our server id. A random LAZYVIEW_SERVER_ID is re-rolled (`true`:
    /// reconnect right away); a configured one can only be reported (`false`: keep retrying).
    fn reroll_server_id(&mut self) -> bool {
        if self.cfg.server_id_fixed {
            error!(
                server_id = self.cfg.server_id,
                "another replica connects with the same server id; set a unique LAZYVIEW_SERVER_ID"
            );
            return false;
        }
        let old = self.cfg.server_id;
        self.cfg.server_id = crate::config::random_server_id();
        info!(
            old,
            new = self.cfg.server_id,
            "server id collision with another replica; using a new random server id"
        );
        true
    }

    async fn on_missing_binlog(&mut self, e: &anyhow::Error) -> Result<()> {
        match self.cfg.on_missing_binlog {
            MissingBinlog::Fail => Err(anyhow!(
                "binlog position {} is no longer available: {e:#}",
                self.boundary
            )),
            MissingBinlog::Rebuild => {
                warn!(
                    "binlog position {} purged; rebuilding all views",
                    self.boundary
                );
                self.boundary = binlog::current_position(&mut self.src).await?;
                let ids: Vec<u64> = self.slots.iter().map(|s| s.def_id).collect();
                for id in ids {
                    self.schedule_replan(id, false, false);
                }
                Ok(())
            }
        }
    }

    async fn on_item(&mut self, reader: &BinlogReader, item: Item) -> Result<()> {
        match item {
            Item::Gtid { gtid } => {
                self.last_gtid = Some(gtid);
                self.in_txn = true;
            }
            Item::TableMap {
                table_id,
                schema,
                table,
                columns,
            } => {
                self.in_txn = true;
                let key = (schema, table);
                if !(key.0 == self.schema && (key.1 == meta::STATUS || key.1 == meta::STATE)) {
                    self.txn_foreign = true;
                }
                if key.0 == self.schema && key.1 == meta::DEFS {
                    self.table_ids.insert(table_id, (key, None));
                    return Ok(());
                }
                let route = match self.routes.get(&key).cloned() {
                    Some(r) => Some(self.check_shapes(&key, r, columns).await),
                    None => None,
                };
                self.table_ids.insert(table_id, (key, route));
            }
            Item::Rows {
                table_id, event, ..
            } => {
                self.in_txn = true;
                let Some((key, route)) = self.table_ids.get(&table_id).cloned() else {
                    return Ok(());
                };
                if key.0 == self.schema && key.1 == meta::DEFS {
                    self.defs_dirty = true;
                    self.first_dirty.get_or_insert_with(Instant::now);
                    return Ok(());
                }
                let Some(route) = route else {
                    // No view reads it now; one defined before the XA COMMIT may.
                    if let Some((_, xa)) = self.xa_current.as_mut() {
                        xa.other_tables.insert(key);
                    }
                    return Ok(());
                };
                let since = self.boundary.clone();
                self.mark_any(&route, &since);
                let need_rows =
                    self.xa_current.is_some() || route.iter().any(|r| r.kind != RouteKind::Any);
                if !need_rows {
                    return Ok(());
                }
                let rows = reader.rows(table_id, &event)?;
                self.feed_rows(&route, &rows, &since);
                if let Some((_, xa)) = self.xa_current.as_mut() {
                    xa.rows
                        .extend(rows.into_iter().map(|(b, a)| (key.clone(), b, a)));
                }
            }
            Item::XaStart { xid } => {
                self.in_txn = true;
                self.xa_current = Some((
                    xid,
                    XaTxn {
                        start: self.boundary.clone(),
                        rows: Vec::new(),
                        other_tables: HashSet::new(),
                        prepared_at: Instant::now(),
                        unpinned: false,
                    },
                ));
            }
            Item::XaPrepare { xid, pos } => {
                // Prepared rows become visible only at XA COMMIT: keep them to feed again then.
                self.end_txn(pos);
                if let Some((x, mut xa)) = self.xa_current.take() {
                    let key = if x == xid { x } else { xid };
                    xa.prepared_at = Instant::now();
                    self.xa_pending.insert(key, xa);
                }
                self.persist_needed |= std::mem::take(&mut self.txn_foreign);
                self.after_boundary();
            }
            Item::XaEnd { xid, commit, pos } => {
                let before = self.end_txn(pos);
                match self.xa_pending.remove(&xid) {
                    Some(xa) if commit => self.xa_committed(xa),
                    Some(_) => {}
                    None if commit => {
                        // Prepared before the position we resumed from (positions are never
                        // saved past an open XA PREPARE unless it stayed unresolved for longer
                        // than XA_PIN_MAX): its rows are unknown, rebuild everything.
                        warn!(%xid, "XA COMMIT for a transaction prepared before this run; rebuilding all views");
                        for s in self.slots.iter_mut() {
                            for (d, since) in s
                                .live
                                .iter_mut()
                                .map(|l| (&mut l.dirty, &mut l.since))
                                .chain(s.next.iter_mut().map(|n| (&mut n.dirty, &mut n.since)))
                            {
                                d.rebuild = true;
                                d.all = true;
                                since.get_or_insert_with(|| before.clone());
                            }
                        }
                        self.first_dirty.get_or_insert_with(Instant::now);
                    }
                    None => {}
                }
                self.persist_needed = true;
                self.after_boundary();
            }
            Item::Commit { pos } => {
                self.end_txn(pos);
                // One-phase XA commits like a normal transaction.
                self.xa_current = None;
                self.persist_needed |= std::mem::take(&mut self.txn_foreign);
                self.after_boundary();
            }
            Item::Statement { schema, query, pos } => {
                let before = self.end_txn(pos);
                self.txn_foreign = false;
                self.persist_needed = true;
                let tables = crate::ddl::statement_tables(&query, &schema);
                if !tables.is_empty() {
                    tracing::trace!(%query, "statement affects tables {tables:?}");
                    self.on_tables_changed(&tables, &before);
                }
                self.after_boundary();
            }
            Item::Heartbeat | Item::Other => {}
        }
        Ok(())
    }

    /// A transaction boundary at `pos`; returns the previous one. TABLE_MAP ids are re-sent
    /// for every transaction, so they are forgotten here.
    fn end_txn(&mut self, pos: Position) -> Position {
        self.in_txn = false;
        self.table_ids.clear();
        std::mem::replace(&mut self.boundary, pos)
    }

    /// XA COMMIT of a transaction read in this run: its rows become visible now.
    fn xa_committed(&mut self, xa: XaTxn) {
        let mut by_table: Vec<(TableRef, Vec<(Option<BinlogRow>, Option<BinlogRow>)>)> = Vec::new();
        for (t, b, a) in xa.rows {
            match by_table.last_mut() {
                Some((lt, v)) if *lt == t => v.push((b, a)),
                _ => by_table.push((t, vec![(b, a)])),
            }
        }
        for (t, rows) in by_table {
            if let Some(route) = self.routes.get(&t).cloned() {
                self.mark_any(&route, &xa.start);
                self.feed_rows(&route, &rows, &xa.start);
            }
        }
        // Views defined while it was prepared read tables whose rows were not kept.
        if xa.other_tables.is_empty() {
            return;
        }
        for s in self.slots.iter_mut() {
            for (p, d, since) in s
                .live
                .iter_mut()
                .map(|l| (&l.plan, &mut l.dirty, &mut l.since))
                .chain(
                    s.next
                        .iter_mut()
                        .map(|n| (&n.plan, &mut n.dirty, &mut n.since)),
                )
            {
                let reads = p.tables.iter().any(|t| xa.other_tables.contains(t))
                    || p.cascades.iter().any(|c| {
                        xa.other_tables
                            .contains(&(c.parent.schema.clone(), c.parent.name.clone()))
                    });
                if reads {
                    d.rebuild = true;
                    d.all = true;
                    since.get_or_insert_with(|| xa.start.clone());
                }
            }
        }
        self.first_dirty.get_or_insert_with(Instant::now);
    }

    fn update_gauges(&mut self) {
        let g = &self.metrics.gauges;
        let n = |f: &dyn Fn(&Slot) -> bool| self.slots.iter().filter(|s| f(s)).count() as u64;
        let (total, ready, errors) = (
            self.slots.len() as u64,
            n(&|s| s.live.is_some() && s.error.is_none()),
            n(&|s| s.error.is_some()),
        );
        Metrics::set(&g.views, total);
        Metrics::set(&g.views_ready, ready);
        Metrics::set(&g.views_building, n(&|s| s.next.is_some()));
        Metrics::set(&g.views_error, errors);
        let all_ready = total > 0
            && ready + errors == total
            && self
                .slots
                .iter()
                .all(|s| s.next.is_none() || s.live.is_some());
        if all_ready && !self.ready_logged {
            if errors > 0 {
                info!(
                    "ready: {ready} view(s) live, {errors} in error (see `{}`._lazyview_overview)",
                    self.schema
                );
            } else {
                info!("ready: {ready} view(s) live");
            }
        }
        self.ready_logged = all_ready;
    }

    /// Periodic one-line activity summary and metadata-connection keepalive (which also
    /// verifies we still hold the leadership lock).
    async fn housekeeping(&mut self) -> Result<()> {
        if self.last_stats.0.elapsed() >= Duration::from_secs(60) {
            let now = self.metrics.snapshot();
            let d = now.since(&self.last_stats.1);
            if d.events > 0 {
                debug!(
                    "last 60s: {} binlog events, {} rows ({} irrelevant), {} units refreshed ({} changed), +{}/-{} rows written, {} flushes",
                    d.events, d.rows_seen, d.rows_skipped, d.units_refreshed, d.units_changed, d.mv_rows_inserted, d.mv_rows_deleted, d.flushes
                );
            }
            self.last_stats = (Instant::now(), now);
        }
        if self.last_src_ping.elapsed() >= Duration::from_secs(30) {
            self.last_src_ping = Instant::now();
            if self.src.query_drop("DO 1").await.is_err() {
                self.heal_src().await;
            }
        }
        if self.last_meta_ping.elapsed() >= Duration::from_secs(60) {
            self.last_meta_ping = Instant::now();
            if let Err(e) = coord::verify(&mut self.meta, &self.leader).await {
                self.meta_failed(e, "checking the leadership lock").await?;
            }
        }
        Ok(())
    }

    /// Routes that need no row image: any row marks the version stale.
    fn mark_any(&mut self, route: &[RouteT], since: &Position) {
        for r in route.iter().filter(|r| r.kind == RouteKind::Any) {
            if let Some((plan, d, s)) = self.version_of(*r) {
                if plan.is_incremental() {
                    d.rebuild = true;
                } else {
                    d.all = true;
                }
                s.get_or_insert_with(|| since.clone());
            }
        }
        self.first_dirty.get_or_insert_with(Instant::now);
    }

    /// Hot path: one decoded row fans out to every routed (view, version, alias). `since` is
    /// where a replay re-reads the row (the start of its transaction).
    fn feed_rows(
        &mut self,
        route: &[RouteT],
        rows: &[(Option<BinlogRow>, Option<BinlogRow>)],
        since: &Position,
    ) {
        let (mut seen, mut skipped) = (0u64, 0u64);
        for (before, after) in rows {
            seen += 1;
            for r in route {
                let Some((plan, dirty, ver_since)) = self.version_of(*r) else {
                    continue;
                };
                let fed = match r.kind {
                    RouteKind::Alias(a) => feed(plan, a, dirty, before.as_ref(), after.as_ref()),
                    RouteKind::Cascade(c) => {
                        feed_cascade(plan, c, dirty, before.as_ref(), after.as_ref())
                    }
                    RouteKind::All(ti) => match (before, after, &plan.table_columns[ti]) {
                        (Some(b), Some(a), Some(cols)) if !columns_changed(b, a, cols) => {
                            Fed::Skipped
                        }
                        _ => {
                            dirty.all = true;
                            Fed::Recorded
                        }
                    },
                    RouteKind::Any => continue,
                };
                match fed {
                    Fed::Skipped => skipped += 1,
                    Fed::Recorded => {
                        ver_since.get_or_insert_with(|| since.clone());
                    }
                }
            }
        }
        self.first_dirty.get_or_insert_with(Instant::now);
        Metrics::add(&self.metrics.rows_seen, seen);
        Metrics::add(&self.metrics.rows_skipped, skipped);
    }

    /// Oldest replay position of a change not yet applied to a maintained table: a restart from
    /// here re-reads everything still pending. Views in error do not hold it back (they are
    /// rebuilt after a restart), neither do lagging targets once they are reported down. With
    /// `held`, also the oldest change whose images are remembered for later lookups (applied,
    /// but a restart must read them again to have them; see [`Lookback`]).
    fn pending_floor(&self, held: bool) -> Position {
        let mut floor = self.boundary.clone();
        let mut lower = |p: Option<&Position>| {
            if let Some(p) = p.filter(|p| !p.file.is_empty() && **p < floor) {
                floor = p.clone();
            }
        };
        for s in self.slots.iter().filter(|s| s.error.is_none()) {
            lower(s.stale_since.as_ref());
            if let Some(l) = s.live.as_ref().filter(|l| !l.frozen) {
                lower(l.since.as_ref());
                if held {
                    lower(l.recent.floor());
                }
                for lag in l.lag.values() {
                    lower(Some(&lag.since));
                }
            }
        }
        floor
    }

    /// Reports the position the views reflect: everything up to it is applied to every live
    /// table, except views in error and new versions still building (those hold it at their
    /// start, as do pending definition changes).
    fn after_boundary(&mut self) {
        // Remembered images are forgotten once the binlog is read past their window, unless
        // images still to be resolved need them.
        let read = self.boundary.clone();
        for l in self.slots.iter_mut().filter_map(|s| s.live.as_mut()) {
            if !l.recent.is_empty() && l.dirty.images.iter().all(HashSet::is_empty) {
                l.recent.prune(&read);
            }
        }
        if self.defs_dirty {
            return;
        }
        let mut floor = self.pending_floor(false);
        for s in self.slots.iter().filter(|s| s.error.is_none()) {
            if let Some(n) = &s.next {
                let routine = s
                    .live
                    .as_ref()
                    .is_some_and(|l| Arc::ptr_eq(&l.plan, &n.plan) && !l.frozen);
                if !routine && n.started < floor {
                    floor = n.started.clone();
                }
            }
        }
        if self.metrics.applied().is_none_or(|a| a < floor) {
            self.metrics.set_applied(floor);
        }
    }

    fn on_tables_changed(&mut self, tables: &[TableRef], since: &Position) {
        self.shapes.clear();
        for t in tables {
            if t.1 == "*" {
                self.catalog.clear();
            } else {
                self.catalog.invalidate(t);
            }
            if t.0 == self.schema && (t.1 == meta::DEFS || t.1 == "*") {
                self.defs_dirty = true;
            }
        }
        let hit = |x: &TableRef| {
            tables
                .iter()
                .any(|t| t.0 == x.0 && (t.1 == "*" || t.1 == x.1))
        };
        let mut todo: Vec<(u64, bool)> = Vec::new();
        for s in self.slots.iter() {
            if (s.error.is_some() || s.waiting.is_some())
                && s.next.is_none()
                && s.wanted.iter().any(hit)
            {
                todo.push((s.def_id, false));
                continue;
            }
            // Tables the view reads, and parents of cascading FKs into them (their column
            // positions are part of the plan too).
            let reads = |p: &ViewPlan| {
                p.tables.iter().any(hit)
                    || p.cascades
                        .iter()
                        .any(|c| hit(&(c.parent.schema.clone(), c.parent.name.clone())))
            };
            let affected = s.live.as_ref().map(|l| reads(&l.plan)).unwrap_or(false)
                || s.next.as_ref().map(|n| reads(&n.plan)).unwrap_or(false)
                || s.wanted.iter().any(hit);
            if affected {
                // A table that is another lazyview's materialized table was swapped (chained
                // view): rebuild without freezing; anything else is a schema change.
                let upstream = tables.iter().all(|t| {
                    t.0 == self.schema
                        && self.slots.iter().any(|o| {
                            o.def_id != s.def_id && Self::slot_uses(o, mv::base_name(&t.1))
                        })
                });
                todo.push((s.def_id, !upstream));
            }
        }
        for (id, freeze) in todo {
            self.schedule_replan_at(id, freeze, false, since);
        }
        if !self.replan.is_empty() || self.defs_dirty {
            self.first_dirty.get_or_insert_with(Instant::now);
        }
    }

    fn on_build_done(&mut self, done: BuildDone) {
        if let Some(s) = self.slots.iter_mut().find(|s| s.def_id == done.def_id) {
            if let Some(n) = s.next.as_mut() {
                if n.generation == done.generation {
                    n.done = Some(done.result);
                    self.first_dirty.get_or_insert_with(Instant::now);
                }
            }
        }
    }

    fn key_count(&self) -> usize {
        self.slots
            .iter()
            .filter_map(|s| s.live.as_ref())
            .filter(|l| !l.frozen)
            .map(|l| l.dirty.key_count())
            .sum()
    }

    fn flush_deadline(&self) -> Option<Instant> {
        let now = Instant::now();
        let mut due: Vec<Instant> = Vec::new();
        if self.defs_dirty {
            due.push(now);
        }
        due.extend(self.replan.values().map(|(t, _)| *t));
        due.extend(self.defs_recheck);
        for s in &self.slots {
            if let Some(n) = &s.next {
                if n.done.is_some() {
                    due.push(n.not_before.unwrap_or(now));
                }
            } else if s.live.as_ref().is_some_and(|l| !l.frozen)
                && s.waiting.is_none()
                && !self.replan.contains_key(&s.def_id)
            {
                // Targets the view could not be built on (retried as in `retry_down`).
                due.extend(
                    s.down
                        .iter()
                        .filter(|(t, _)| s.targets.contains(t))
                        .map(|(_, d)| d.retry_at),
                );
            }
            let Some(l) = s.live.as_ref().filter(|l| !l.frozen) else {
                continue;
            };
            // Stale full-mode views (and rebuilds for unknown units) wake the loop when their
            // next refresh slot opens.
            let rebuild = l.dirty.rebuild || (l.dirty.all && matches!(l.plan.mode, Mode::Full(_)));
            if rebuild && s.next.is_none() && !self.replan.contains_key(&s.def_id) {
                due.push(self.full_due(s).unwrap_or(now));
            }
            // Failed refreshes and lagging targets are retried after their backoff.
            if let Some(t) = s.retry_at.filter(|_| !l.dirty.is_empty()) {
                due.push(t);
            }
            due.extend(l.lag.values().map(|g| g.retry_at));
        }
        let urgent = due.into_iter().min();
        let batch = self.first_dirty.map(|t| t + self.cfg.batch_window);
        let persist = self
            .persist_needed
            .then(|| self.last_persist + self.cfg.position_persist_interval);
        [urgent, batch, persist].into_iter().flatten().min()
    }

    fn flush_due(&self) -> bool {
        self.flush_due_within(Duration::ZERO)
    }

    /// As [`Self::flush_due`], with the deadline passed by at least `late`.
    fn flush_due_within(&self, late: Duration) -> bool {
        match self.flush_deadline() {
            Some(d) => Instant::now() >= d + late || self.key_count() >= self.cfg.batch_max_keys,
            None => false,
        }
    }

    // ---------------------------------------------------------------- flush

    async fn flush(&mut self) -> Result<()> {
        let started = Instant::now();
        // Failures below re-arm it.
        self.first_dirty = None;
        let recheck_due = self
            .defs_recheck
            .map(|t| Instant::now() >= t)
            .unwrap_or(false);
        if self.defs_dirty || recheck_due {
            // The _lazyview change must be visible before we read the table.
            if self.fence().await {
                self.defs_dirty = false;
                self.defs_recheck = if recheck_due || self.exact_fence() {
                    None
                } else {
                    Some(
                        Instant::now()
                            + self.cfg.visibility_delay.max(Duration::from_millis(50)) * 10,
                    )
                };
                if let Err(e) = self.reload_definitions().await {
                    // Read again once the metadata connection is back.
                    self.defs_dirty = true;
                    self.meta_failed(e, "reading _lazyview").await?;
                }
            } else {
                self.first_dirty = Some(Instant::now());
            }
        }
        let now = Instant::now();
        let mut ready: Vec<(u64, bool)> = self
            .replan
            .iter()
            .filter(|(_, (t, _))| *t <= now)
            .map(|(id, (_, f))| (*id, *f))
            .collect();
        ready.sort();
        // Buffered changes for a long build must not grow without bound: fall back to a
        // rebuild after the swap instead.
        let cap = self.cfg.batch_max_keys.saturating_mul(10).max(100_000);
        for s in self.slots.iter_mut() {
            if let Some(n) = s.next.as_mut() {
                if n.dirty.key_count() > cap {
                    let len = n.dirty.images.len();
                    n.dirty = ViewDirty {
                        images: vec![HashSet::new(); len],
                        rebuild: true,
                        ..Default::default()
                    };
                }
            }
        }
        // Builds, refreshes and lookups below must see every transaction read so far.
        let has_reads = !ready.is_empty()
            || self.slots.iter().any(|s| {
                s.live
                    .as_ref()
                    .map(|l| !l.frozen && (!l.dirty.is_empty() || !l.lag.is_empty()))
                    .unwrap_or(false)
                    || s.next.as_ref().map(|n| n.done.is_some()).unwrap_or(false)
                    || (s.next.is_none() && s.down.values().any(|d| d.retry_at <= now))
            });
        if has_reads && !self.fence().await {
            // Not visible yet: everything stays buffered for the next flush.
            self.first_dirty = Some(Instant::now());
        } else {
            for (id, freeze) in ready {
                self.replan.remove(&id);
                if let Some(i) = self.idx_of(id) {
                    self.replan_slot(i, freeze).await;
                }
            }
            self.retry_down().await;
            let now = Instant::now();
            for i in 0..self.slots.len() {
                if self.slots[i]
                    .next
                    .as_ref()
                    .map(|n| n.done.is_some() && n.not_before.map(|t| t <= now).unwrap_or(true))
                    .unwrap_or(false)
                {
                    self.finish_next(i).await;
                }
                self.refresh_live(i).await;
            }
        }
        if let Err(e) = self.persist_position(false).await {
            self.meta_failed(e, "saving the binlog position").await?;
        }
        if !self.refreshed.is_empty() && self.last_touch.elapsed() >= Duration::from_secs(10) {
            let names: Vec<String> = self.refreshed.drain().collect();
            let _ = meta::touch_refreshed(&mut self.meta, &names).await;
            self.last_touch = Instant::now();
        }
        Metrics::add(&self.metrics.flushes, 1);
        self.update_gauges();
        self.after_boundary();
        tracing::trace!(elapsed = ?started.elapsed(), "flush");
        Ok(())
    }

    /// Waits until the transactions read from the binlog are visible to readers: binlog events
    /// are shipped before the engine commit completes.
    fn exact_fence(&self) -> bool {
        (self.flavor == Flavor::MySql && self.last_gtid.is_some())
            || self.snapshot_fence == Some(true)
    }

    /// `false`: the last transaction read is not visible yet (the GTID or snapshot wait timed
    /// out), so nothing may be read from the sources now.
    ///
    /// MySQL / Percona with GTIDs: `WAIT_FOR_EXECUTED_GTID_SET`. MariaDB and Percona Server
    /// commit in binlog order and report the binlog position a consistent snapshot includes
    /// (`Binlog_snapshot_file` / `_position`): once it reaches the position read, every
    /// transaction read is visible. Elsewhere: `LAZYVIEW_VISIBILITY_DELAY_MS`.
    async fn fence(&mut self) -> bool {
        if self.flavor == Flavor::MySql {
            if let Some(g) = self.last_gtid.clone() {
                let r: Result<Option<Option<i64>>, _> = self
                    .src
                    .exec_first("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 5)", (g.clone(),))
                    .await;
                self.metrics.beat();
                match r {
                    // 0: executed; 1: timed out.
                    Ok(Some(Some(0))) => return true,
                    Ok(Some(Some(_))) => {
                        warn!(gtid = %g, "transaction not visible after 5 s (WAIT_FOR_EXECUTED_GTID_SET timed out); retrying");
                        return false;
                    }
                    Ok(_) => debug!("gtid fence returned NULL"),
                    Err(e) => {
                        debug!("gtid fence unavailable: {e}");
                        if is_conn_error(&anyhow!(e)) {
                            self.heal_src().await;
                            return false;
                        }
                    }
                }
            }
        }
        if self.snapshot_fence != Some(false) && !self.boundary.file.is_empty() {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut pause = Duration::from_millis(1);
            loop {
                match self.visible_upto().await {
                    Ok(Some(p)) => {
                        self.snapshot_fence = Some(true);
                        if p >= self.boundary {
                            return true;
                        }
                        if Instant::now() >= deadline {
                            warn!(visible = %p, read = %self.boundary, "transaction not visible after 5 s (consistent snapshot behind the binlog position read); retrying");
                            return false;
                        }
                        self.metrics.beat();
                        tokio::time::sleep(pause).await;
                        pause = (pause * 2).min(Duration::from_millis(50));
                    }
                    Ok(None) => {
                        debug!("no binlog snapshot position; using LAZYVIEW_VISIBILITY_DELAY_MS");
                        self.snapshot_fence = Some(false);
                        break;
                    }
                    Err(e) => {
                        debug!("snapshot fence unavailable: {e:#}");
                        if is_conn_error(&e) {
                            self.heal_src().await;
                            return false;
                        }
                        break;
                    }
                }
            }
        }
        if !self.cfg.visibility_delay.is_zero() {
            tokio::time::sleep(self.cfg.visibility_delay).await;
        }
        true
    }

    /// The binlog position up to which every transaction is visible, from a consistent snapshot
    /// (`None`: the server does not report one, e.g. MySQL Community).
    async fn visible_upto(&mut self) -> Result<Option<Position>> {
        // The session reads with READ COMMITTED; the snapshot needs REPEATABLE READ.
        self.src
            .query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        self.src
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        let rows: Result<Vec<(String, String)>, _> = self
            .src
            .query("SHOW SESSION STATUS LIKE 'binlog_snapshot_%'")
            .await;
        self.src.query_drop("COMMIT").await?;
        let mut file = None;
        let mut pos = None;
        for (k, v) in rows? {
            match k.to_ascii_lowercase().as_str() {
                "binlog_snapshot_file" => file = Some(v),
                "binlog_snapshot_position" => pos = v.trim().parse::<u64>().ok(),
                _ => {}
            }
        }
        Ok(match (file, pos) {
            (Some(file), Some(pos)) if !file.is_empty() => Some(Position { file, pos }),
            _ => None,
        })
    }

    async fn replan_slot(&mut self, i: usize, freeze: bool) {
        let (def, targets, id) = {
            let s = &self.slots[i];
            (s.definition.clone(), s.targets.clone(), s.def_id)
        };
        if !freeze {
            // Upstream data replaced / retry of a failed build: if a build is already running,
            // let it finish and re-plan once more afterwards instead of restarting it.
            let since = self.slots[i].stale_since.clone();
            if let Some(n) = self.slots[i].next.as_mut() {
                n.dirty.rebuild = true;
                if let Some(p) = since {
                    n.since.get_or_insert(p);
                }
                self.slots[i].replan_after = true;
                return;
            }
        } else if let Some(l) = self.slots[i].live.as_mut() {
            l.frozen = true;
        }
        self.slots[i].replan_after = false;
        self.rebuild_routes();
        match self.parse_and_plan(&def, &targets, Some(id)).await {
            Ok(plan) => {
                let plan = Arc::new(plan);
                self.slots[i].last_plan = Some(plan.clone());
                self.slots[i].waiting = None;
                let name = plan.name.clone();
                if freeze {
                    info!(view = %name, "source schema changed; rebuilding online");
                }
                self.start_next(i, plan, name, targets, false).await;
            }
            Err(e) if e.downcast_ref::<WaitingFor>().is_some() => self.plan_failed(i, &e).await,
            Err(e) => {
                self.slot_error(i, &e).await;
                // Transient causes (connection loss, table briefly missing) heal on retry.
                self.schedule_replan(id, freeze, true);
            }
        }
    }

    /// The view could not be built or kept on target `t`: state `error` there, retried with
    /// backoff by a partial build while the other targets stay maintained.
    async fn target_down(
        &mut self,
        i: usize,
        name: &str,
        t: &str,
        plan: Option<&ViewPlan>,
        e: &anyhow::Error,
    ) {
        let s = &mut self.slots[i];
        let d = s.down.entry(t.to_string()).or_insert(Down {
            failures: 0,
            retry_at: Instant::now(),
        });
        d.failures += 1;
        let delay = target_backoff(d.failures);
        d.retry_at = Instant::now() + delay;
        let msg = format!("{e:#}");
        warn!(view = %name, target = %t, "{msg}; retrying in {delay:?}");
        let (id, hash) = (s.def_id, s.hash.clone());
        self.set_state(name, t, id, &hash, plan, "error", Some(&msg))
            .await;
    }

    /// Partial builds of targets whose retry is due (the view is live on the other targets).
    async fn retry_down(&mut self) {
        let now = Instant::now();
        for i in 0..self.slots.len() {
            let s = &self.slots[i];
            if s.next.is_some() || s.waiting.is_some() || self.replan.contains_key(&s.def_id) {
                continue;
            }
            let Some(l) = s.live.as_ref().filter(|l| !l.frozen) else {
                continue;
            };
            let due: Vec<String> = s
                .down
                .iter()
                .filter(|(t, d)| d.retry_at <= now && s.targets.contains(t))
                .map(|(t, _)| t.clone())
                .collect();
            if due.is_empty() {
                continue;
            }
            let (plan, name) = (l.plan.clone(), l.name.clone());
            self.start_next(i, plan, name, due, true).await;
        }
    }

    /// A build or swap of `next` failed: keep the live version serving and maintained, and
    /// retry the build with backoff (so a short target/DB outage never freezes a view forever).
    /// The retry is the re-plan: nothing else may start a rebuild of this view before it.
    async fn next_failed(&mut self, i: usize, n: Next, e: anyhow::Error) {
        for t in &n.targets {
            if !n.swapped.contains(t) {
                self.drop_shadow(t, &n.name).await;
            }
        }
        let def_id = self.slots[i].def_id;
        if let Some(l) = self.slots[i].live.as_mut() {
            // Changes buffered for the failed version also concern the live one.
            if Arc::ptr_eq(&l.plan, &n.plan) || matches!(l.plan.mode, Mode::Full(_)) {
                l.dirty.merge(n.dirty);
                if matches!(l.plan.mode, Mode::Full(_)) {
                    l.dirty.all = true;
                }
                if let Some(p) = n.since {
                    if l.since.as_ref().is_none_or(|s| p < *s) {
                        l.since = Some(p);
                    }
                }
            }
        }
        if n.partial {
            // Only some targets of an otherwise live view: those are retried on their own.
            let plan = n.plan.clone();
            for t in &n.targets {
                self.target_down(i, &n.name, t, Some(&plan), &e).await;
            }
            self.rebuild_routes();
            return;
        }
        let frozen = self.slots[i]
            .live
            .as_ref()
            .map(|l| l.frozen)
            .unwrap_or(false);
        self.rebuild_routes();
        self.slot_error(i, &e).await;
        self.schedule_replan(def_id, frozen, true);
    }

    /// Applies changes buffered during the build to the shadow tables, swaps them in.
    async fn finish_next(&mut self, i: usize) {
        let mut n = self.slots[i].next.take().unwrap();
        let Built { layout, failed } = match n.done.take().unwrap() {
            Ok(b) => b,
            Err(e) => {
                self.next_failed(i, n, e).await;
                return;
            }
        };
        // Targets the build could not use are retried later; the view goes live on the rest.
        let mut first_err = None;
        for (t, e) in failed {
            n.targets.retain(|x| *x != t);
            if n.partial || !n.targets.is_empty() || n.swapped.contains(&t) {
                let plan = n.plan.clone();
                self.target_down(i, &n.name, &t, Some(&plan), &e).await;
            }
            first_err.get_or_insert(e);
        }
        if n.targets.is_empty() {
            let e = first_err.unwrap_or_else(|| anyhow!("no target to build on"));
            self.next_failed(i, n, e).await;
            return;
        }
        let was_live_same_plan = self.slots[i]
            .live
            .as_ref()
            .map(|l| Arc::ptr_eq(&l.plan, &n.plan))
            .unwrap_or(false);
        let shadow = mv::shadow_name(&n.name);
        let mut dirty = n.dirty.take();
        let since = n.since.take();
        let full_again = matches!(n.plan.mode, Mode::Full(_)) && dirty.all;
        if !matches!(n.plan.mode, Mode::Full(_)) && n.swapped.is_empty() {
            let pending: Vec<String> = n.targets.clone();
            match self
                .refresh(
                    &n.plan,
                    &layout,
                    &mut dirty,
                    &pending,
                    &shadow,
                    &mut n.recent,
                    since.as_ref(),
                )
                .await
            {
                Ok(r) => {
                    let plan = n.plan.clone();
                    for (t, e) in r.failed {
                        n.targets.retain(|x| *x != t);
                        let e = e.context("applying changes buffered during build");
                        self.target_down(i, &n.name, &t, Some(&plan), &e).await;
                        first_err.get_or_insert(e);
                    }
                }
                Err(e) => {
                    if is_conn_error(&e) {
                        self.heal_src().await;
                    }
                    n.dirty.merge(dirty);
                    n.since = since;
                    self.next_failed(i, n, e.context("applying changes buffered during build"))
                        .await;
                    return;
                }
            }
        }
        for t in n.targets.clone() {
            if n.swapped.contains(&t) {
                continue;
            }
            let ts = self.targets[&t].schema.clone();
            // Someone created a table with this name meanwhile: never replace it.
            let live_has = self.slots[i]
                .live
                .as_ref()
                .map(|l| l.name == n.name && l.targets.contains(&t))
                .unwrap_or(false);
            if !live_has && !self.owned.contains_key(&(n.name.clone(), t.clone())) {
                let marker = mv::marker(&n.name, self.slots[i].def_id);
                let exists = match self.target_conn(&t).await {
                    Ok(c) => {
                        mv::table_kind(c, &ts, &n.name)
                            .await
                            .ok()
                            .flatten()
                            .is_some()
                            && mv::table_marker(c, &ts, &n.name).await.ok().flatten()
                                != Some(marker)
                    }
                    Err(_) => false,
                };
                if exists {
                    let e = anyhow!(
                        "refusing to replace `{ts}`.`{}` on target {t}: the table appeared during the build and is not managed by lazyview",
                        n.name
                    );
                    n.targets.retain(|x| *x != t);
                    let plan = n.plan.clone();
                    self.target_down(i, &n.name, &t, Some(&plan), &e).await;
                    first_err.get_or_insert(e);
                    continue;
                }
            }
            let timeout = self.cfg.target_timeout;
            let r = match self.target_conn(&t).await {
                Ok(c) => match tokio::time::timeout(timeout, mv::swap(c, &ts, &n.name)).await {
                    Ok(r) => r,
                    Err(_) => Err(anyhow!("timed out after {timeout:?}")),
                },
                Err(e) => Err(e),
            };
            match r {
                Ok(()) => {
                    n.swapped.insert(t.clone());
                }
                Err(e) if server_code(&e) == Some(1205) => {
                    // An open reader transaction on the old table holds its metadata lock:
                    // keep maintaining both versions and retry the swap shortly.
                    debug!(view = %n.name, target = %t, "swap waits for a metadata lock; retrying");
                    n.dirty.merge(dirty);
                    n.since = match (since, n.since.take()) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    n.done = Some(Ok(Built {
                        layout,
                        failed: Vec::new(),
                    }));
                    n.not_before = Some(Instant::now() + Duration::from_secs(5));
                    self.slots[i].next = Some(n);
                    self.first_dirty.get_or_insert_with(Instant::now);
                    return;
                }
                Err(e) => {
                    self.targets.get_mut(&t).unwrap().conn = None;
                    let e = e.context(format!("swapping in new version on target {t}"));
                    n.targets.retain(|x| *x != t);
                    let plan = n.plan.clone();
                    self.target_down(i, &n.name, &t, Some(&plan), &e).await;
                    first_err.get_or_insert(e);
                }
            }
        }
        if n.swapped.is_empty() {
            let e = first_err.unwrap_or_else(|| anyhow!("no target to swap in"));
            self.next_failed(i, n, e).await;
            return;
        }
        let def_id = self.slots[i].def_id;
        let swapped: Vec<String> = n
            .targets
            .iter()
            .filter(|t| n.swapped.contains(*t))
            .cloned()
            .collect();
        for t in &swapped {
            self.set_state(&n.name, t, def_id, &n.hash, Some(&n.plan), "ready", None)
                .await;
            self.slots[i].down.remove(t);
        }
        let s = &mut self.slots[i];
        s.error = None;
        if !n.partial {
            self.failures.remove(&def_id);
            let s = &mut self.slots[i];
            // A build that started after the view went stale covers what it missed.
            if s.stale_since.as_ref().is_some_and(|p| *p <= n.started) {
                s.stale_since = None;
            }
            let retired = std::mem::take(&mut s.retired);
            for (name, t) in retired {
                if (name == n.name && n.targets.contains(&t))
                    || self
                        .slots
                        .iter()
                        .enumerate()
                        .any(|(j, o)| j != i && Self::slot_uses(o, &name))
                {
                    continue;
                }
                self.drop_owned(&name, &t).await;
            }
        }
        if was_live_same_plan && !n.partial {
            debug!(view = %n.name, "rebuilt");
        } else {
            info!(view = %n.name, targets = %swapped.join(","), "live");
        }
        // Changes that arrived after the buffered ones were applied (swap retries).
        let mut live_dirty = ViewDirty {
            images: vec![HashSet::new(); n.plan.aliases.len()],
            ..Default::default()
        };
        live_dirty.merge(n.dirty);
        live_dirty.all |= full_again;
        live_dirty.rebuild |= dirty.rebuild;
        let since = match (since, n.since) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let replan_after = std::mem::take(&mut self.slots[i].replan_after);
        let recent = std::mem::take(&mut n.recent);
        match self.slots[i].live.as_mut().filter(|_| n.partial) {
            Some(l) => {
                // New targets of the unchanged live version (also fed every change meanwhile).
                for t in swapped {
                    if !l.targets.contains(&t) {
                        l.targets.push(t);
                    }
                }
                l.targets.sort();
                l.recent.extend(recent);
                if !live_dirty.is_empty() {
                    l.dirty.merge(live_dirty);
                    if let Some(p) = since {
                        if l.since.as_ref().is_none_or(|s| p < *s) {
                            l.since = Some(p);
                        }
                    }
                }
            }
            None => {
                let empty = live_dirty.is_empty();
                self.slots[i].live = Some(Version {
                    plan: n.plan,
                    layout,
                    name: n.name.clone(),
                    targets: swapped,
                    dirty: live_dirty,
                    frozen: false,
                    since: if empty { None } else { since },
                    lag: BTreeMap::new(),
                    recent,
                });
            }
        }
        if replan_after {
            // The upstream data changed during this build: build once more from a new plan.
            self.schedule_replan(def_id, false, false);
        }
        self.rebuild_routes();
        let on_source = self.slots[i]
            .live
            .as_ref()
            .is_some_and(|l| l.targets.iter().any(|t| t == SOURCE_TARGET));
        if on_source {
            self.wake_waiting(&n.name);
        }
    }

    async fn refresh_live(&mut self, i: usize) {
        self.retry_lag(i).await;
        let def_id = self.slots[i].def_id;
        let replan_pending = self.replan.contains_key(&def_id);
        let backing_off = self.slots[i].retry_at.is_some_and(|t| t > Instant::now());
        let now = Instant::now();
        let full_due = self.full_due(&self.slots[i]).is_none_or(|t| t <= now);
        let s = &mut self.slots[i];
        let Some(l) = s.live.as_mut() else {
            return;
        };
        if l.frozen || l.dirty.is_empty() || backing_off {
            return;
        }
        let mut dirty = l.dirty.take();
        let (plan, layout, name) = (l.plan.clone(), l.layout.clone(), l.name.clone());
        let lagging: Vec<String> = l.lag.keys().cloned().collect();
        let targets: Vec<String> = l
            .targets
            .iter()
            .filter(|t| !lagging.contains(t))
            .cloned()
            .collect();
        if let Mode::Full(_) = plan.mode {
            // Rebuild into a shadow; at most one rebuild runs and at most one is pending. Changes
            // during a running build mark it for one more rebuild; a pending re-plan (a retry
            // with backoff) rebuilds instead.
            let live_since = l.since.clone();
            if let Some(n) = s.next.as_mut() {
                n.dirty.all = true;
                if let Some(p) = live_since {
                    n.since.get_or_insert(p);
                }
            } else if full_due && !replan_pending {
                let all = l.targets.clone();
                self.start_next(i, plan, name, all, false).await;
            } else {
                // Not due yet: stay stale until the next refresh slot (see flush_deadline).
                l.dirty.all = true;
            }
            return;
        }
        let mut recent = std::mem::take(&mut l.recent);
        let since = l.since.clone();
        let r = self
            .refresh(
                &plan,
                &layout,
                &mut dirty,
                &targets,
                &name,
                &mut recent,
                since.as_ref(),
            )
            .await;
        if let Some(l) = self.slots[i].live.as_mut() {
            l.recent = recent;
        }
        match r {
            Err(e) => {
                if is_conn_error(&e) {
                    self.heal_src().await;
                }
                // Keep the changes for the next attempt (after a backoff); rebuild only if it
                // keeps failing.
                let s = &mut self.slots[i];
                if let Some(l) = s.live.as_mut() {
                    l.dirty.merge(dirty);
                }
                s.refresh_failures += 1;
                let n = s.refresh_failures;
                let delay = Duration::from_millis(250 << n.min(7)).min(Duration::from_secs(30));
                s.retry_at = Some(Instant::now() + delay);
                warn!(view = %name, attempt = n, "refresh failed: {e:#}; retrying in {delay:?}");
                if n == 3 && !replan_pending {
                    self.schedule_replan(def_id, false, true);
                }
            }
            Ok(r) => {
                let s = &mut self.slots[i];
                s.retry_at = None;
                s.refresh_failures = 0;
                if s.next.is_none() && !replan_pending {
                    self.failures.remove(&def_id);
                }
                self.refreshed.insert(name.clone());
                let s = &mut self.slots[i];
                let Some(l) = s.live.as_mut() else {
                    return;
                };
                let floor = l.since.clone().unwrap_or_else(|| self.boundary.clone());
                // Targets that missed this refresh (or earlier ones) get the units later.
                for (t, e) in &r.failed {
                    warn!(view = %name, target = %t, "write failed: {e:#}; retrying");
                    l.lag.entry(t.clone()).or_insert(Lag {
                        units: UnitSet::default(),
                        all: false,
                        since: floor.clone(),
                        failures: 1,
                        retry_at: Instant::now() + target_backoff(1),
                    });
                }
                for t in r.failed.iter().map(|(t, _)| t).chain(&lagging) {
                    if let Some(g) = l.lag.get_mut(t) {
                        g.units.extend(r.units.clone());
                        g.all |= r.all;
                    }
                }
                if dirty.rebuild {
                    let live_since = l.since.clone();
                    if let Some(n) = s.next.as_mut() {
                        n.dirty.rebuild = true;
                        if let Some(p) = live_since {
                            n.since.get_or_insert(p);
                        }
                    } else if full_due && !replan_pending {
                        debug!(view = %name, "affected rows unknown (cascading FK / overflow); rebuilding online");
                        let all = l.targets.clone();
                        self.start_next(i, plan, name, all, false).await;
                    } else {
                        l.dirty.rebuild = true;
                    }
                } else {
                    l.since = None;
                }
            }
        }
    }

    /// Retries targets whose writes failed; one that keeps failing is reported down (and
    /// rebuilt once it answers again).
    async fn retry_lag(&mut self, i: usize) {
        let now = Instant::now();
        let due: Vec<String> = match self.slots[i].live.as_ref().filter(|l| !l.frozen) {
            Some(l) => l
                .lag
                .iter()
                .filter(|(_, g)| g.retry_at <= now)
                .map(|(t, _)| t.clone())
                .collect(),
            None => return,
        };
        for t in due {
            let Some(l) = self.slots[i].live.as_mut() else {
                return;
            };
            let Some(mut g) = l.lag.remove(&t) else {
                continue;
            };
            let (plan, layout, name) = (l.plan.clone(), l.layout.clone(), l.name.clone());
            let mut d = ViewDirty {
                units: g.units.clone(),
                images: vec![HashSet::new(); plan.aliases.len()],
                all: g.all,
                ..Default::default()
            };
            let r = self
                .refresh(
                    &plan,
                    &layout,
                    &mut d,
                    std::slice::from_ref(&t),
                    &name,
                    &mut Lookback::default(),
                    None,
                )
                .await;
            let s = &mut self.slots[i];
            let Some(l) = s.live.as_mut() else {
                return;
            };
            match r {
                Ok(r) if r.failed.is_empty() => {
                    info!(view = %name, target = %t, "target caught up");
                    let (id, hash) = (s.def_id, s.hash.clone());
                    self.set_state(&name, &t, id, &hash, Some(&plan), "ready", None)
                        .await;
                }
                Ok(r) => {
                    g.failures += 1;
                    if g.failures >= LAG_MAX_FAILURES {
                        // Down: rebuilt there once it answers (see retry_down).
                        l.targets.retain(|x| *x != t);
                        let e = r.failed.into_iter().next().unwrap().1;
                        self.target_down(i, &name, &t, Some(&plan), &e).await;
                        self.rebuild_routes();
                    } else {
                        g.retry_at = Instant::now() + target_backoff(g.failures);
                        l.lag.insert(t, g);
                    }
                }
                Err(e) => {
                    // The sources could not be read: not this target's fault.
                    debug!(view = %name, target = %t, "retrying lagging target: {e:#}");
                    g.retry_at = Instant::now() + Duration::from_secs(1);
                    l.lag.insert(t, g);
                }
            }
        }
    }

    /// Refreshes the units in `dirty` on table `table` of every target in `targets`. Sources are
    /// read once per chunk and each target is written independently: one that fails is skipped
    /// from then on and reported. `dirty` is cleared (except a pending rebuild) unless reading
    /// the sources failed.
    async fn refresh(
        &mut self,
        plan: &ViewPlan,
        layout: &Layout,
        dirty: &mut ViewDirty,
        targets: &[String],
        table: &str,
        recent: &mut Lookback,
        since: Option<&Position>,
    ) -> Result<Refreshed> {
        let mut failed: Vec<(String, anyhow::Error)> = Vec::new();
        if matches!(plan.mode, Mode::Global) {
            if !dirty.all {
                return Ok(Refreshed::default());
            }
            let rows = mv::read_source(&mut self.src, plan, layout, None, &self.metrics).await?;
            Metrics::add(&self.metrics.units_refreshed, 1);
            for t in targets {
                if let Err(e) = self.apply_target(t, table, layout, None, &rows).await {
                    failed.push((t.clone(), e));
                }
            }
            dirty.all = false;
            return Ok(Refreshed {
                units: UnitSet::default(),
                all: true,
                failed,
            });
        }
        // `dirty` is only cleared once everything succeeded, so a failure can be retried.
        let mut units = dirty.units.clone();
        let mut rebuild = dirty.rebuild;
        if !self
            .lookups(
                plan,
                layout,
                &dirty.images,
                (targets, table),
                (recent, since),
                &mut units,
            )
            .await?
        {
            debug!(view = %plan.name, "too many changed rows to look up; rebuilding instead");
            rebuild = true;
        }
        Metrics::add(&self.metrics.units_refreshed, units.len() as u64);
        // Unit filters bind one placeholder per unit column.
        let per = self
            .cfg
            .query_chunk
            .min(MAX_FILTER_PARAMS / plan.unit_exprs.len().max(1))
            .max(1);
        for chunk in units.chunks(per) {
            self.metrics.beat();
            let rows =
                mv::read_source(&mut self.src, plan, layout, Some(&chunk), &self.metrics).await?;
            for t in targets {
                if failed.iter().any(|(f, _)| f == t) {
                    continue;
                }
                if let Err(e) = self
                    .apply_target(t, table, layout, Some(&chunk), &rows)
                    .await
                {
                    failed.push((t.clone(), e));
                }
            }
        }
        let n = dirty.images.len();
        *dirty = ViewDirty {
            images: vec![HashSet::new(); n],
            rebuild,
            ..Default::default()
        };
        Ok(Refreshed {
            units,
            all: false,
            failed,
        })
    }

    /// Writes the new rows of a chunk (all rows: `None`) to `table` on target `t`.
    async fn apply_target(
        &mut self,
        t: &str,
        table: &str,
        layout: &Layout,
        chunk: Option<&UnitChunk>,
        rows: &[Vec<Value>],
    ) -> Result<()> {
        let ts = self.targets[t].schema.clone();
        let metrics = self.metrics.clone();
        let (batch, timeout) = (self.cfg.write_batch_rows, self.cfg.target_timeout);
        let c = self
            .target_conn(t)
            .await
            .with_context(|| format!("target {t}"))?;
        let r = match tokio::time::timeout(
            timeout,
            mv::apply(c, &ts, table, layout, chunk, rows, batch, &metrics),
        )
        .await
        {
            Ok(r) => r.map(|_| ()),
            Err(_) => Err(anyhow!("timed out after {timeout:?}")),
        };
        if let Err(e) = r {
            // Drop the connection so the next attempt reconnects.
            self.targets.get_mut(t).unwrap().conn = None;
            return Err(e.context(format!("target {t}")));
        }
        Ok(())
    }

    /// Resolves the buffered row images to units.
    ///
    /// * A table with a lineage key: the materialized rows on `table` (held by `targets`) that
    ///   contain one of its changed rows, by an index lookup on the primary key (the old rows),
    ///   plus the current rows that contain them (the new rows).
    /// * Other lookup tables: their images replace the table and are joined with the current
    ///   state of the others. With one such table that is exact (a partner row that changed too
    ///   is traced through its own change). With several, an old row may only have joined old
    ///   rows of the others: `recent` (see [`Lookback`]) gives the combinations to look up,
    ///   with images remembered from earlier flushes; `since` is the replay position of the
    ///   oldest change in `images`.
    ///
    /// `false`: too many combinations or remembered images; the caller rebuilds the view instead.
    async fn lookups(
        &mut self,
        plan: &ViewPlan,
        layout: &Layout,
        images: &[HashSet<Key>],
        (targets, table): (&[String], &str),
        (recent, since): (&mut Lookback, Option<&Position>),
        units: &mut UnitSet,
    ) -> Result<bool> {
        let fresh = |a: usize| images.get(a).is_some_and(|s| !s.is_empty());
        for a in (0..plan.aliases.len()).filter(|&a| fresh(a)) {
            if plan.lineage_of(a).is_some() {
                self.lineage_lookup(plan, layout, a, &images[a], targets, table, units)
                    .await?;
            }
        }
        let plain: Vec<usize> = (0..plan.aliases.len())
            .filter(|&a| plan.aliases[a].direct_unit.is_none() && plan.lineage_of(a).is_none())
            .collect();
        if plain.len() < 2 {
            for &a in plain.iter().filter(|&&a| fresh(a)) {
                if !self.lookup(plan, &[(a, &images[a])], units).await? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let metrics = self.metrics.clone();
        let overflow = |what: &str| {
            debug!(view = %plan.name, "{what}; rebuilding instead");
            Metrics::add(&metrics.lookback_overflows, 1);
        };
        let Some(combos) = recent.combinations(&plain, images) else {
            overflow("too many lookup tables changed together");
            recent.clear();
            return Ok(false);
        };
        for set in &combos.sets {
            let parts: Vec<(usize, &HashSet<Key>)> = match set.as_slice() {
                [a] => vec![(*a, &images[*a])],
                _ => set.iter().map(|&a| (a, &combos.all[a])).collect(),
            };
            if !self.lookup(plan, &parts, units).await? {
                overflow("too many lookup combinations");
                recent.clear();
                return Ok(false);
            }
        }
        if plain.iter().any(|&a| fresh(a)) {
            // Every change visible to the lookups above is in the binlog before this position.
            let until = binlog::current_position(&mut self.src).await?;
            let from = since.unwrap_or(&self.boundary).clone();
            recent.remember(until, from, &plain, images);
        }
        recent.prune(&self.boundary);
        if recent.key_count() > self.cfg.batch_max_keys.saturating_mul(10) {
            overflow("too many remembered lookup images");
            recent.clear();
            return Ok(false);
        }
        Ok(true)
    }

    /// Lineage lookup of alias `alias` (see [`Self::lookups`]): `keys` are primary keys of its
    /// changed rows.
    #[allow(clippy::too_many_arguments)]
    async fn lineage_lookup(
        &mut self,
        plan: &ViewPlan,
        layout: &Layout,
        alias: usize,
        keys: &HashSet<Key>,
        targets: &[String],
        table: &str,
        units: &mut UnitSet,
    ) -> Result<()> {
        let k = plan
            .lineage_of(alias)
            .ok_or_else(|| anyhow!("internal: alias {alias} has no lineage key"))?;
        let mut set = UnitSet::default();
        for key in keys {
            set.insert(key.clone());
        }
        let per = self
            .cfg
            .query_chunk
            .min(MAX_FILTER_PARAMS / k.cols.len().max(1))
            .max(1);
        let mv_cols: Vec<String> = k.cols.iter().map(|c| analyze::quote_ident(c)).collect();
        let unit_cols = layout.unit_cols().join(", ");
        for chunk in set.chunks(per) {
            self.metrics.beat();
            // Old rows: the materialized rows that contain them.
            let (f, p) = chunk.filter(&mv_cols);
            let sql = |ts: &str| {
                format!(
                    "SELECT DISTINCT {unit_cols} FROM {}.{} WHERE {f}",
                    analyze::quote_ident(ts),
                    analyze::quote_ident(table)
                )
            };
            let rows = self.read_materialized(targets, sql, p).await?;
            for r in rows {
                units.insert(r.unwrap().into_iter().map(Datum::from_value).collect());
            }
            // New rows: the current rows that contain them.
            let (f, p) = chunk.filter(&k.exprs);
            let rows: Vec<mysql_async::Row> = self.src.exec(plan.key_lookup_sql(&f), p).await?;
            Metrics::add(&self.metrics.lookup_queries, 1);
            for r in rows {
                units.insert(r.unwrap().into_iter().map(Datum::from_value).collect());
            }
        }
        Ok(())
    }

    /// Runs a read of the materialized table (`sql` for a target schema) on the first target of
    /// `targets` that answers, `source` first (they hold the same rows).
    async fn read_materialized(
        &mut self,
        targets: &[String],
        sql: impl Fn(&str) -> String,
        params: Vec<Value>,
    ) -> Result<Vec<mysql_async::Row>> {
        let mut order: Vec<&String> = targets.iter().filter(|t| *t == SOURCE_TARGET).collect();
        order.extend(targets.iter().filter(|t| *t != SOURCE_TARGET));
        let mut last = None;
        for t in order {
            let ts = self.targets[t.as_str()].schema.clone();
            let timeout = self.cfg.target_timeout;
            let r = match self.target_conn(t).await {
                Ok(c) => {
                    match tokio::time::timeout(timeout, c.exec(sql(&ts), params.clone())).await {
                        Ok(r) => r.map_err(anyhow::Error::from),
                        Err(_) => Err(anyhow!("timed out after {timeout:?}")),
                    }
                }
                Err(e) => Err(e),
            };
            Metrics::add(&self.metrics.mv_queries, 1);
            match r {
                Ok(rows) => return Ok(rows),
                Err(e) if server_code(&e).is_some() && !is_conn_error(&e) => {
                    return Err(e.context(format!("reading the materialized table on target {t}")))
                }
                Err(e) => {
                    // Unreachable or timed out: the next target holds the same rows.
                    self.targets.get_mut(t.as_str()).unwrap().conn = None;
                    last = Some(e);
                }
            }
        }
        Err(last
            .unwrap_or_else(|| anyhow!("no target"))
            .context("reading the materialized table: no target answered"))
    }

    /// Looks up the units of the rows obtained by replacing every alias in `parts` by its images
    /// (`SELECT DISTINCT <units>` over the view's FROM/WHERE). `false`: too many statements for
    /// one combination.
    async fn lookup(
        &mut self,
        plan: &ViewPlan,
        parts: &[(usize, &HashSet<Key>)],
        units: &mut UnitSet,
    ) -> Result<bool> {
        struct Part<'a> {
            alias: usize,
            first: String,
            rest: String,
            keys: Vec<&'a Key>,
            per: usize,
        }
        let mut subs = Vec::new();
        for &(alias, images) in parts {
            let a = &plan.aliases[alias];
            let cols: Vec<&crate::catalog::Column> =
                a.image_cols.iter().map(|&c| &a.table.columns[c]).collect();
            if cols.is_empty() {
                // No membership column: every unit may be affected (e.g. a cross join).
                bail!(
                    "view `{}`: table `{}` contributes to every row; incremental lookup impossible",
                    plan.name,
                    a.table.name
                );
            }
            subs.push(Part {
                alias,
                first: cols
                    .iter()
                    .map(|c| format!("{} AS {}", c.param, analyze::quote_ident(&c.name)))
                    .collect::<Vec<_>>()
                    .join(", "),
                rest: cols
                    .iter()
                    .map(|c| c.param.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                keys: images.iter().collect(),
                per: (MAX_FILTER_PARAMS / (parts.len() * cols.len()))
                    .clamp(1, self.cfg.query_chunk),
            });
        }
        let parts = subs;
        let counts: Vec<usize> = parts.iter().map(|p| p.keys.len().div_ceil(p.per)).collect();
        if parts.len() > 1 && counts.iter().product::<usize>() > MAX_LOOKUP_COMBO_QUERIES {
            return Ok(false);
        }
        // Every combination of one chunk per alias (an odometer over the chunk indexes).
        let mut at = vec![0usize; parts.len()];
        loop {
            let chunks: Vec<&[&Key]> = parts
                .iter()
                .zip(&at)
                .map(|(p, &i)| &p.keys[i * p.per..((i + 1) * p.per).min(p.keys.len())])
                .collect();
            let subst: Vec<(usize, String)> = parts
                .iter()
                .zip(&chunks)
                .map(|(p, chunk)| {
                    let mut derived = format!("(SELECT {}", p.first);
                    for _ in 1..chunk.len() {
                        derived.push_str(" UNION ALL SELECT ");
                        derived.push_str(&p.rest);
                    }
                    derived.push(')');
                    (p.alias, derived)
                })
                .collect();
            let (sql, order) = match subst.as_slice() {
                [(a, derived)] => (plan.lookup_sql(*a, derived), vec![0]),
                _ => plan.lookup_sql_multi(&subst)?,
            };
            let params: Vec<Value> = order
                .iter()
                .flat_map(|&k| {
                    chunks[k]
                        .iter()
                        .flat_map(|key| key.iter().map(Datum::to_value))
                })
                .collect();
            let rows: Vec<mysql_async::Row> = self.src.exec(sql, params).await?;
            Metrics::add(&self.metrics.lookup_queries, 1);
            for r in rows {
                let key: Key = r.unwrap().into_iter().map(Datum::from_value).collect();
                units.insert(key);
            }
            let mut d = 0;
            loop {
                if d == at.len() {
                    return Ok(true);
                }
                at[d] += 1;
                if at[d] < counts[d] {
                    break;
                }
                at[d] = 0;
                d += 1;
            }
        }
    }

    /// Saves the position replay must restart from: never past a change still pending (see
    /// [`Self::pending_floor`]) or an open prepared XA transaction (its rows must be re-read
    /// after a restart to be applied at XA COMMIT), unless that one stayed unresolved for
    /// longer than [`XA_PIN_MAX`].
    async fn persist_position(&mut self, force: bool) -> Result<()> {
        if self.boundary.file.is_empty() {
            return Ok(());
        }
        let mut pos = self.pending_floor(true);
        for (xid, xa) in self.xa_pending.iter_mut() {
            if xa.start.file.is_empty() {
                continue;
            }
            if xa.prepared_at.elapsed() > XA_PIN_MAX {
                if !xa.unpinned {
                    xa.unpinned = true;
                    warn!(%xid, start = %xa.start, "XA transaction prepared over {XA_PIN_MAX:?} ago is still unresolved; the saved position no longer waits for it (if it commits after a restart, every view is rebuilt)");
                }
                continue;
            }
            pos = pos.min(xa.start.clone());
        }
        if let Some((_, xa)) = &self.xa_current {
            if !xa.start.file.is_empty() {
                pos = pos.min(xa.start.clone());
            }
        }
        if !force && (pos == self.persisted || !self.persist_needed) {
            // Held back where it was saved: look again one interval later (the flush deadline
            // is `last_persist` + interval; leaving it in the past would spin the loop).
            self.last_persist = Instant::now();
            return Ok(());
        }
        self.persist_needed = true;
        meta::save_position(
            &mut self.meta,
            &self.cfg.instance_id,
            &pos,
            self.leader.epoch,
        )
        .await?;
        // Held back: save again once the pending changes are applied.
        self.persist_needed = pos < self.boundary;
        self.persisted = pos;
        self.last_persist = Instant::now();
        Ok(())
    }

    /// `once` mode: waits for all builds and swaps them in.
    async fn wait_builds(&mut self) -> Result<()> {
        let mut rx = self.build_rx.take().expect("wait_builds runs once");
        while self
            .slots
            .iter()
            .any(|s| s.next.as_ref().map(|n| n.done.is_none()).unwrap_or(false))
        {
            match rx.recv().await {
                Some(done) => self.on_build_done(done),
                None => break,
            }
        }
        for i in 0..self.slots.len() {
            if self.slots[i].next.is_some() {
                self.finish_next(i).await;
            }
        }
        if self
            .slots
            .iter()
            .any(|s| s.error.is_some() || !s.down.is_empty())
        {
            bail!("some views failed to build; see _lazyview_state");
        }
        Ok(())
    }
}

/// Result of [`Engine::refresh`].
#[derive(Default)]
struct Refreshed {
    /// Units refreshed (for targets that missed them).
    units: UnitSet,
    /// Global view recomputed.
    all: bool,
    /// Targets whose writes failed.
    failed: Vec<(String, anyhow::Error)>,
}

/// Placeholders per statement (the server limit is 65535).
const MAX_FILTER_PARAMS: usize = 60_000;

/// Statements one combination of image sets may take (chunks of each multiplied).
const MAX_LOOKUP_COMBO_QUERIES: usize = 256;

/// A lagging target is reported down after this many failed retries.
const LAG_MAX_FAILURES: u32 = 5;

/// One target of a build, with the ownership checks it needs there.
struct BuildSpec {
    target: String,
    opts: Opts,
    schema: String,
    /// Ownership marker (table comment) of this view's tables: a table carrying it is ours
    /// even without a state row (a crash between the swap and the state update).
    marker: String,
    /// Refuse if `<name>__lv_new` / `__lv_old` exist (not created by lazyview for this view).
    check_side: bool,
    /// Refuse if a table `<name>` exists (one lazyview did not create).
    check_table: bool,
}

/// Connects to a build target, runs its ownership checks and creates the shadow table.
async fn prepare_target(spec: &BuildSpec, name: &str, layout: &Layout) -> Result<Conn> {
    let (t, ts) = (&spec.target, &spec.schema);
    let mut c = match tokio::time::timeout(TARGET_CONNECT_TIMEOUT * 3, connect(&spec.opts)).await {
        Ok(r) => r.with_context(|| format!("connecting to target {t}"))?,
        Err(_) => bail!("connecting to target {t}: timed out"),
    };
    let ours = |m: Option<String>| m.as_deref() == Some(spec.marker.as_str());
    if spec.check_side {
        for side in [mv::shadow_name(name), mv::old_name(name)] {
            if mv::table_kind(&mut c, ts, &side).await?.is_some()
                && !ours(mv::table_marker(&mut c, ts, &side).await?)
            {
                bail!("refusing to build `{name}`: table `{ts}`.`{side}` on target {t} exists and was not created by lazyview for this view");
            }
        }
    }
    if spec.check_table {
        if let Some(k) = mv::table_kind(&mut c, ts, name).await? {
            if !ours(mv::table_marker(&mut c, ts, name).await?) {
                bail!(
                    "refusing to replace existing {} `{ts}`.`{name}` on target {t} that lazyview does not manage",
                    k.to_lowercase()
                );
            }
        }
    }
    mv::create_shadow_marked(&mut c, ts, name, layout, &spec.marker)
        .await
        .with_context(|| format!("creating the shadow table on target {t}"))?;
    Ok(c)
}

#[allow(clippy::too_many_arguments)]
async fn build_task(
    src_opts: Opts,
    src_schema: String,
    targets: Vec<BuildSpec>,
    plan: Arc<ViewPlan>,
    name: String,
    batch_rows: usize,
    metrics: Arc<Metrics>,
    tx: mpsc::UnboundedSender<BuildDone>,
    slots: Arc<tokio::sync::Semaphore>,
    def_id: u64,
    generation: u64,
) {
    let result = async {
        let _permit = slots.acquire_owned().await?;
        let mut src = connect(&src_opts).await?;
        let layout = mv::probe(&mut src, &src_schema, &plan).await?;
        // Each target is prepared on its own: one that fails is reported, the others built.
        let mut conns = Vec::new();
        let mut failed = Vec::new();
        for spec in &targets {
            match prepare_target(spec, &name, &layout).await {
                Ok(c) => conns.push((c, spec.schema.clone())),
                Err(e) => failed.push((spec.target.clone(), e)),
            }
        }
        if !conns.is_empty() {
            let mut bt: Vec<BuildTarget> = conns
                .iter_mut()
                .map(|(c, s)| BuildTarget {
                    conn: c,
                    schema: s.clone(),
                })
                .collect();
            let n = mv::fill(
                &mut src, &plan, &layout, &mut bt, &name, batch_rows, &metrics,
            )
            .await?;
            debug!(view = %name, rows = n, "build finished");
        }
        for (c, _) in conns {
            let _ = c.disconnect().await;
        }
        let _ = src.disconnect().await;
        Ok::<_, anyhow::Error>(Built { layout, failed })
    }
    .await;
    match &result {
        Err(e) => warn!(view = %name, "build failed: {e:#}"),
        Ok(b) => {
            for (t, e) in &b.failed {
                warn!(view = %name, target = %t, "build failed on this target: {e:#}");
            }
        }
    }
    let _ = tx.send(BuildDone {
        def_id,
        generation,
        result,
    });
}
