//! Orchestration: startup, background builds, the binlog loop, flushes and online swaps.
//!
//! Each view definition (`_lazyview` row) is a [`Slot`] with up to two versions:
//! * `live` — the table readers see, maintained incrementally from the binlog;
//! * `next` — a new version (new definition, targets, or a rebuild after DDL) being built in
//!   the background into shadow tables. Changes arriving meanwhile are buffered for it, applied
//!   once the build finishes, and then the shadow is swapped in atomically (`RENAME TABLE`).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::accumulate::{
    self, extend_partial, feed, feed_cascade, feed_map, Fed, KeySet, UnitChunk, UnitSet, ViewDirty,
};
use crate::analyze::{self, Mode, ViewPlan};
use crate::binlog::{self, BinlogReader, Flavor, Item, Position};
use crate::cascade;
use crate::catalog::{Catalog, TableInfo, TableRef};
use crate::config::{Config, MissingBinlog, StartMode};
use crate::coord;
use crate::datum::{Datum, Key};
use crate::lookback::Lookback;
use crate::meta::{self, StateUpdate, SOURCE_TARGET};
use crate::metrics::Metrics;
use crate::mv::{self, BuildTarget, Layout};
use crate::plan_swap;
use crate::source::{self, mysql, Cursor, FenceState, SourceCaps};
use crate::supervisor::{self, Supervisor};
use crate::target;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;

/// The once-a-minute clocks of the `Engine::core_events` warnings (guard R1 c; one per warning).
static LAST_UNKNOWN: AtomicU64 = AtomicU64::new(0);

/// Run the service until `shutdown` turns true (or, for `once`, until done). With
/// `cfg.sync == false` this is the setup and migration run (`lazyview setup`): create and
/// migrate lazyview's own tables, validate every enabled definition, log its plan, and return.
pub async fn run(
    cfg: Config,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    Metrics::set(&metrics.gauges.heartbeat_unix, crate::metrics::unix_now());
    let token = cfg.health_token.clone();
    let health = cfg
        .health_addr
        .map(|addr| tokio::spawn(crate::health::serve(addr, metrics.clone(), token)));
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
    let replica = binlog::replica_opts(&cfg.dsn);
    let user = replica
        .as_ref()
        .ok()
        .and_then(|o| o.user().map(str::to_string))
        .unwrap_or_else(|| "lazyview".into());
    let schema = replica
        .as_ref()
        .ok()
        .and_then(|o| o.db_name().map(str::to_string))
        .unwrap_or_default();
    let r = async {
        let Some((mut engine, start)) = Engine::start(cfg, metrics, shutdown, replica).await?
        else {
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

/// Whether the server is Amazon RDS (`@@basedir` under `rdsdbbin`) or Aurora (`@@aurora_version`).
fn is_managed_service(basedir: Option<&str>, aurora: bool) -> bool {
    basedir.is_some_and(|b| b.contains("rdsdbbin")) || aurora
}

/// The startup warning for an RDS / Aurora binlog retention (`value` of `mysql.rds_configuration`'s
/// `binlog retention hours`), `None` when it is at least 12 hours.
fn retention_warning(hours: Option<&str>) -> Option<String> {
    match hours.and_then(|h| h.trim().parse::<u64>().ok()) {
        Some(h) if h >= 12 => None,
        other => Some(format!(
            "Amazon RDS/Aurora binlog retention is {}; after an outage longer than that lazyview cannot resume and \
             rebuilds all views. Run once: CALL mysql.rds_set_configuration('binlog retention hours', 24);",
            other
                .map(|h| format!("{h} hours"))
                .unwrap_or_else(|| "not set (binlogs are purged within minutes)".into())
        )),
    }
}

/// Whether to suggest GTIDs: RDS MySQL (not Aurora, where the parameter does not exist as such)
/// with `gtid_mode` known and not `ON`.
fn wants_gtid_tip(aurora: bool, gtid_mode: Option<&str>) -> bool {
    !aurora && gtid_mode.is_some_and(|g| !g.eq_ignore_ascii_case("ON"))
}

/// Startup warnings for the source's `binlog-do-db` / `binlog-ignore-db` filters: they name the
/// table's own schema, so an excluded schema is invisible in the binlog. lazyview's schema
/// (`schema`, the metadata connection's, where the metadata tables and the commit fence live)
/// must be binlogged, and so must the source DSN's default schema when it differs from it
/// (source reads). The filters are read once; an unreadable status is debug-logged, not warned.
async fn binlog_filter_checks(conn: &mut Conn, schema: &str, source_schema: Option<&str>) {
    let (do_db, ignore_db) = match binlog::binlog_filters(conn).await {
        Ok(f) => f,
        Err(e) => {
            debug!(
                "cannot read the binlog filters at startup: {}",
                e.root_cause()
            );
            return;
        }
    };
    let excluded = |s: &str| binlog::schema_filtered(&do_db, &ignore_db, s);
    if excluded(schema) {
        warn!(
            "binlog-do-db / binlog-ignore-db exclude schema {schema}: lazyview's metadata and \
             visibility-fence writes are not binlogged, so the fence cannot prove visibility; \
             remove the filter or include {schema}"
        );
    }
    if let Some(s) = source_schema.filter(|s| *s != schema) {
        if excluded(s) {
            warn!(
                "binlog-do-db / binlog-ignore-db exclude schema {s}: changes to tables in {s} \
                 never reach lazyview"
            );
        }
    }
}

/// Amazon RDS / Aurora purge binlogs aggressively by default; a short retention means a restart
/// after an outage cannot resume and must rebuild every view. Warn once at startup.
async fn managed_service_checks(conn: &mut Conn) {
    let basedir: Option<String> = conn.query_first("SELECT @@basedir").await.unwrap_or(None);
    let aurora: Option<String> = conn
        .query_first("SELECT @@aurora_version")
        .await
        .unwrap_or(None);
    if !is_managed_service(basedir.as_deref(), aurora.is_some()) {
        return;
    }
    let hours: Option<Option<String>> = conn
        .query_first(
            "SELECT value FROM mysql.rds_configuration WHERE name = 'binlog retention hours'",
        )
        .await
        .unwrap_or(None);
    if let Some(w) = retention_warning(hours.flatten().as_deref()) {
        warn!("{w}");
    }
    let gtid: Option<String> = conn
        .query_first("SELECT @@GLOBAL.gtid_mode")
        .await
        .unwrap_or(None);
    if wants_gtid_tip(aurora.is_some(), gtid.as_deref()) {
        info!("tip: gtid_mode=ON and enforce_gtid_consistency=ON in the RDS parameter group give lazyview an exact commit-visibility fence");
    }
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
    lock_wait_s: u64,
) -> Result<Option<Conn>> {
    let mut waited = false;
    loop {
        // A source connection: a source is always MySQL-family (`postgres://` is rejected for
        // `LAZYVIEW_DSN` in `crate::config`). The one helper retries transient connect failures
        // with Policy::CONNECT (jittered, `metrics.beat()` between tries); errors it does not
        // classify as transient come back here, where the fatal set
        // (`!is_transient && !is_tls_error`) still ends the wait.
        match supervisor::retrying(
            metrics,
            &format!("waiting for {what}"),
            supervisor::Policy::CONNECT,
            || connect_source(opts, lock_wait_s, metrics),
        )
        .await
        {
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
                    "waiting for {what} at {}:{}: {}; retrying in {:?}",
                    opts.ip_or_hostname(),
                    opts.tcp_port(),
                    e.root_cause(),
                    supervisor::Policy::CONNECT.cap
                );
                waited = true;
            }
        }
        // Still unreachable after a whole policy run: wait one cap, shutdown-aware. Beat
        // every second meanwhile (as `hold_for_builds` does): the cap equals the stall
        // limit, so an un-beaten wait of a whole cap would let `/livez` see a heartbeat as
        // old as the limit says the loop beats - and it does not (health_probe_transitions
        // samples the age while the database is unreachable). The wake every second is the
        // same cost the loop already pays between `retrying` rounds.
        let mut left = supervisor::Policy::CONNECT.cap;
        while !left.is_zero() {
            metrics.beat();
            let step = left.min(Duration::from_secs(1));
            tokio::select! {
                biased;
                _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
                _ = tokio::time::sleep(step) => {}
            }
            left -= step;
        }
    }
}

/// Tries of one idempotent read of the source connection before it is given up on
/// (see [`retry_src`]).
const SRC_TRIES: usize = 5;

/// Repeats one idempotent read of the source connection: [`SRC_TRIES`] tries, 200 ms × 2ⁿ
/// capped at 2 s, `metrics.beat()` between tries, and a re-opened connection before a retry
/// after a connection-class error (the reconnect half of [`Engine::heal_src`]: this early in a
/// start nothing is fenced yet, so there is no verdict to drop). `f` gets the 1-based try
/// number, so a read can fail its first tries (a test hook). A read that keeps failing is an
/// `Err`, never a default: a value a correctness guard depends on must stay an error.
/// R2 replaces this copy with the shared `retry_source`.
async fn retry_src<T, F>(
    metrics: &Metrics,
    what: &str,
    src: &mut Conn,
    opts: &Opts,
    lock_wait_s: u64,
    mut f: F,
) -> Result<T>
where
    F: for<'a> FnMut(usize, &'a mut Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
{
    let mut backoff = Duration::from_millis(200);
    let mut try_no = 0;
    loop {
        try_no += 1;
        let err = match f(try_no, src).await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        if try_no >= SRC_TRIES {
            return Err(err);
        }
        warn!(
            "{what} failed (try {try_no} of {SRC_TRIES}): {}; retrying in {backoff:?}",
            err.root_cause()
        );
        metrics.beat();
        if binlog::is_transient(&err) {
            match connect_source(opts, lock_wait_s, metrics).await {
                Ok(c) => *src = c,
                Err(e) => warn!(
                    "{what}: source database still unreachable: {}",
                    e.root_cause()
                ),
            }
        } else {
            tokio::time::sleep(backoff).await;
        }
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}

struct Target {
    /// The target family, from the DSN's scheme.
    kind: target::Kind,
    opts: target::Opts,
    schema: String,
    conn: Option<target::Conn>,
    /// Statement limits and upsert syntax of this server (read with the connection).
    write: Option<mv::WriteOpts>,
    /// Session sql_mode of the connection (the supervisor's pooled connections set it too).
    sql_mode: Option<String>,
    /// Unreachable: connection attempts fail fast until then (backoff), so a dead target never
    /// stalls the binlog loop for more than one connect timeout per period.
    retry_at: Option<Instant>,
    failures: u32,
}

/// One connect attempt to a target (TCP + handshake), well inside the liveness window.
const TARGET_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Retry delay after `n` consecutive failures of one target: the shared backoff of the one
/// helper (supervisor), the `Policy::CONNECT` shape (1 s doubling to its 30 s cap, jittered).
/// The `retry_at` parking of the target is the engine's own.
fn connect_retry_delay(n: u32) -> Duration {
    let p = supervisor::Policy::CONNECT;
    supervisor::backoff(n, p.base, p.cap, p.jitter)
}

/// Consecutive failed refreshes of a live view after which its state rows show the error (it is
/// retried with backoff all the same, never re-planned or rebuilt).
const REFRESH_ERROR_SHOWN_AFTER: u32 = 3;

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
    /// Targets answered but took every write of the last flush slower than the budget of the
    /// changes kept for targets that are behind (`mark_behind_targets`, #215): excluded from the
    /// refresh like a lagging one, and their units handed to a `lag` entry at the next refresh
    /// (`note_refresh`) or when the retry time in the value arrives (`retry_lag`).
    behind: BTreeMap<String, Instant>,
    /// Remembered images of lookup tables without lineage (see [`Lookback`]).
    recent: Lookback,
    /// Server binlog position right after the catalog read its plan was made from (see
    /// [`Engine::swap_in_place`]); `None` when unknown.
    catalog_head: Option<Position>,
    /// Tables whose definition the plan took from a later catalog read, at a source DDL that
    /// put it in place without a rebuild: (position right after that read, the DDL's position).
    table_heads: HashMap<TableRef, (Position, Position)>,
}

impl Version {
    /// A position right after every catalog read its plan's table definitions come from (the
    /// latest of them).
    fn plan_head(&self) -> Option<Position> {
        let head = self.catalog_head.clone()?;
        Some(
            self.table_heads
                .values()
                .map(|h| &h.0)
                .fold(head, |a, b| a.max(b.clone())),
        )
    }
}

struct Lag {
    units: UnitSet,
    /// Partial unit keys still to apply (per set of the plan's partial units).
    partial: Vec<UnitSet>,
    /// Global views: the single row must be recomputed on this target.
    all: bool,
    since: Position,
    failures: u32,
    retry_at: Instant,
    /// Reported down (`Down`): excluded from the refresh's targets and retried here, with its
    /// keys kept. Its kept keys live only in memory, so the saved position waits for its
    /// `since` (`pending_floor`) and a restart replays the outage rather than losing them.
    down: bool,
}

/// Lowers `floor` to `p` when that is a known position older than it.
fn lower_pos(floor: &mut Position, p: Option<&Position>) {
    if let Some(p) = p.filter(|p| !p.file.is_empty() && **p < *floor) {
        *floor = p.clone();
    }
}

/// Lowers `floor` to the oldest `since` among the changes kept for targets that are behind,
/// a target reported down included. The kept keys live only in memory, so the saved position
/// waits for them and a restart replays the outage from the binlog, rebuilding the same key
/// sets, instead of leaving that target silently stale.
fn lag_floor(floor: &mut Position, lag: &BTreeMap<String, Lag>) {
    for g in lag.values() {
        lower_pos(floor, Some(&g.since));
    }
}

/// A target the view could not be built on (unreachable, refused): retried by a partial build,
/// unless its changes are kept in `Lag` (see `retry_lag`).
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
    /// Targets whose swap gave up on its own bounded lock wait (`swap_lock_wait_s`, a reader
    /// held the table's metadata lock): the binlog loop never waits for or retries them —
    /// they are reported and left to the next rebuild (N-02).
    lock_wait: HashSet<String>,
    /// Position the build started at: it reflects every change up to here.
    started: Position,
    /// Replay position of the oldest change buffered in `dirty`.
    since: Option<Position>,
    /// Images resolved against the shadow table (handed to the live version at the swap).
    recent: Lookback,
    /// Rebuild of the unchanged live version (see `start_next`): state rows stay `ready`.
    routine: bool,
    /// The swap runs in the background (`task`); the live version pauses until it is done.
    swapping: bool,
    /// As [`Version::catalog_head`], for its plan.
    catalog_head: Option<Position>,
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
    /// The upstream it waits for is in error (or blocked itself): the message its state shows.
    blocked: Option<String>,
    /// Failed refreshes: the next attempt waits until then (exponential backoff).
    retry_at: Option<Instant>,
    /// Targets the view could not be built on.
    down: BTreeMap<String, Down>,
    /// Latest plan of the current definition (its mode stays visible in state `error`).
    last_plan: Option<Arc<ViewPlan>>,
    /// As [`Version::catalog_head`], for `last_plan`.
    last_plan_head: Option<Position>,
    /// Consecutive failed refreshes of the live version. Published every flush as
    /// `lazyview_view_refresh_failures` (see [`Self::publish_stats`] and src/stats.rs): a view
    /// whose refresh fails on every attempt stays `ready`, so this is its alert signal (O-02).
    refresh_failures: u32,
    /// Re-plan once the build in flight is done (its source data was replaced meanwhile).
    replan_after: bool,
    /// `_lazyview.rebuild` was set when the binlog was here: cleared once a build started at or
    /// after this position is live.
    rebuild_asked: Option<Position>,
    /// Hash of the definition whose planning timed out: not planned again until it changes.
    plan_timed_out: Option<String>,
    /// Refresh results and builds since the last hand-over to `/stats` (see `publish_stats`).
    stats: crate::stats::Flush,
    /// The source clock (`BinlogReader::event_ts`) of the newest change this view has applied:
    /// stamped once per flush that left the view with nothing buffered, never per event. `0`:
    /// the view has applied nothing since this process started (see [`SourceClock::lag`]).
    applied_ts: u64,
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
    /// Child of a cascade whose rows the cascade map keeps (plan cascade index).
    MapChild(usize),
}

type XaRow = (TableRef, Option<BinlogRow>, Option<BinlogRow>);

/// An XA transaction read from the binlog; its rows are applied at XA COMMIT.
struct XaTxn {
    /// The cursor before it (replaying from there re-reads its rows); `Cursor::is_some`
    /// is whether the transaction has a start at all.
    start: Cursor,
    rows: Vec<XaRow>,
    /// Estimated memory of `rows` (`KEY_BYTES` per row, see `buffer_bytes`). The rows of
    /// tables no view read are kept only while this stays under `Config::mem.xa_hold_bytes`.
    bytes: usize,
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

/// The view name differs only in letter case from another row's: `_lazyview.name` ignores
/// case, so the row cannot record it. The view keeps its own name (its state row is keyed
/// case-sensitively), not a `#<id>` placeholder.
#[derive(Debug)]
struct CaseClash {
    name: String,
    id: u64,
    other: String,
    other_id: u64,
}

impl std::fmt::Display for CaseClash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "name `{}` is already used by row #{} as `{}`: view names that differ only in letter case \
             are the same name to lazyview; rename this view, or remove this row with \
             DELETE FROM _lazyview WHERE id = {}",
            self.name, self.other_id, self.other, self.id
        )
    }
}

impl std::error::Error for CaseClash {}

/// Planning a definition took longer than `LAZYVIEW_PLAN_TIMEOUT_MS`: the definition is not
/// planned again until it changes (the abandoned planner may still be running).
#[derive(Debug)]
struct PlanTimedOut(Duration);

impl std::fmt::Display for PlanTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ms = self.0.as_millis();
        let took = if ms.is_multiple_of(1000) {
            format!("{} s", ms / 1000)
        } else {
            format!("{ms} ms")
        };
        write!(
            f,
            "planning took longer than {took}; simplify the definition or raise LAZYVIEW_PLAN_TIMEOUT_MS"
        )
    }
}

impl std::error::Error for PlanTimedOut {}

/// A state row as last written (see [`Engine::restore_state_rows`]).
#[derive(Debug, Clone)]
struct ShownState {
    definition_id: u64,
    hash: String,
    mode: String,
    detail: Option<String>,
    /// The route and access plan written with the state (NULL when no plan).
    route: Option<String>,
    access: Option<String>,
    state: String,
    error: Option<String>,
}

/// A finished build: its layout, the targets it could not be built on, those it already
/// swapped in itself (routine rebuilds of full-mode views), those whose swap gave up on its
/// bounded lock wait (they are left to the next rebuild; N-01, N-02) and the cascade cache it
/// seeded.
struct Built {
    layout: Layout,
    failed: Vec<(String, anyhow::Error)>,
    swapped: Vec<String>,
    lock_wait: Vec<String>,
    cache: cascade::Cache,
}

struct BuildDone {
    def_id: u64,
    generation: u64,
    result: Result<Built>,
}

/// Why one round of the binlog loop ended and the reader is about to reconnect (review A-02).
/// The reason decides the pause and the counter, so `/metrics` can tell the stall states apart:
/// a source that keeps cutting the stream is not a reader stuck on one event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamEnd {
    /// An event the decoder cannot map. Reading again from the saved boundary fails on the same
    /// bytes, so the flat 200 ms a network fault gets was an endless hot loop that left `/livez`
    /// green (A-02); this reason backs off like a failed connect instead.
    Undecodable,
    /// A source-class stream error: the connection or the dump thread went away, or an error the
    /// reader cannot prove is the decoder's.
    Transient,
    /// The server closed the dump (`Ok(None)`).
    Closed,
    /// Neither an event nor a heartbeat for `silence`: a half-open connection.
    Silence,
}

/// The wait before the next reconnect, by why the last round ended (review A-02). It runs once
/// per reconnect, never per event. `streak` counts the consecutive rounds that ended on the same
/// undecodable position, so a stuck event follows the shared connect shape — 1 s doubling to 30 s
/// with jitter (`supervisor::Policy::CONNECT`) — and the reader is reading the next event again
/// within a second of a fixed build replacing this one. Every other reason keeps the pause it had
/// before A-02: a round that delivered something reconnects after 200 ms, one that died at the
/// door waits a connect cap. The reader drops nothing on any path: a reconnect re-reads from the
/// saved boundary.
fn stream_pause(end: Option<StreamEnd>, delivered: bool, streak: u32) -> Duration {
    match end {
        Some(StreamEnd::Undecodable) => supervisor::backoff(
            streak.max(1),
            supervisor::Policy::CONNECT.base,
            supervisor::Policy::CONNECT.cap,
            supervisor::Policy::CONNECT.jitter,
        ),
        _ if delivered => Duration::from_millis(200),
        _ => supervisor::Policy::CONNECT.cap,
    }
}

struct Engine {
    cfg: Config,
    metrics: Arc<Metrics>,
    /// The source-read role: `Engine::src`, the supervised source reads, the per-build reads.
    query_opts: Opts,
    /// The metadata role: the catalog writes, the fencing epoch, the leadership lock.
    meta_opts: Opts,
    /// The binlog dump role: the one connection that reads the stream.
    binlog_opts: Opts,
    schema: String,
    flavor: Flavor,
    /// Source capabilities, detected from the flavor once at connect.
    caps: SourceCaps,
    /// The server's `VERSION()` (the source's planning rules read it, see `refuse_for_source`).
    server_version: String,
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
    table_ids: HashMap<
        u64,
        (
            TableRef,
            Option<Arc<Vec<RouteT>>>,
            Option<crate::dedup::Source>,
        ),
    >,
    /// (table, binlog column count) → column count of the table's current definition.
    shapes: HashMap<(TableRef, u64), Option<usize>>,
    /// XA transaction being read: (xid, position before it, rows of routed tables).
    xa_current: Option<(String, XaTxn)>,
    /// Prepared XA transactions awaiting XA COMMIT / ROLLBACK.
    xa_pending: HashMap<String, XaTxn>,
    defs_dirty: bool,
    /// A `_lazyview_state` row was deleted: write back those of current views.
    state_check: bool,
    /// Last state written per (name, target), to write a row deleted by hand back.
    shown: HashMap<(String, String), ShownState>,
    /// Definitions to re-plan and rebuild: def_id → (due, freeze the live version).
    replan: HashMap<u64, (Instant, bool)>,
    /// Consecutive failures per definition (exponential retry backoff).
    failures: HashMap<u64, u32>,
    /// Re-read `_lazyview` once more after this (no exact visibility fence available).
    defs_recheck: Option<Instant>,
    last_src_ping: Instant,
    /// Freshness: what the reader knows of the source's clock (review O-01, see [`SourceClock`]).
    clock: SourceClock,
    /// Last `SELECT UNIX_TIMESTAMP()` of the source (the offset refresh), and the last time the
    /// per-view statistics were handed to the aggregator outside a flush.
    last_clock: Instant,
    last_publish: Instant,
    boundary: Cursor,
    persisted: Cursor,
    last_persist: Instant,
    in_txn: bool,
    /// The current transaction touched something besides our own metadata tables.
    txn_foreign: bool,
    persist_needed: bool,
    first_dirty: Option<Instant>,
    /// A version was marked `Dedup::mark_shape_stale` while draining: executor (b) takes its
    /// rows over, which `rebuild_routes` wires up; the flush below runs it once.
    core_shape_routes: bool,
    last_gtid: Option<String>,
    /// The server reports the binlog position of a consistent snapshot (see `fence`); `None`
    /// until tried.
    snapshot_fence: Option<bool>,
    /// Whether this flavor's consistent-snapshot position is a committed-transaction watermark
    /// (`Flavor::snapshot_position_is_watermark`): answered once here, so the fence asks a cached
    /// value and the hot path stays as it was.
    snapshot_watermark: bool,
    /// Oracle MySQL without GTIDs: the server commits in binlog order, so a commit of our own
    /// proves visibility (see `fence::probe`). `None` until probed on this connection.
    commit_fence: Option<bool>,
    /// The boundary the last successful commit fence covered: no new event since, no new write.
    commit_fenced: Option<Position>,
    /// The commit fence's WARN (order not enforced, or the write failed) was logged.
    fence_warned: bool,
    generation: u64,
    build_tx: mpsc::UnboundedSender<BuildDone>,
    build_rx: Option<mpsc::UnboundedReceiver<BuildDone>>,
    build_slots: Arc<tokio::sync::Semaphore>,
    leader: coord::Leadership,
    /// What the leadership lock's own task publishes (review A-06): `Fenced` means another
    /// instance has the lock and this one is the replaced one. Checking it costs a `watch` borrow —
    /// an atomic read, no round-trip, no allocation, and nothing on the binlog event path.
    leader_state: watch::Receiver<LeaderState>,
    /// Armed while that state is not `Active` (lv-super ruling 1): the flush holds until then, and
    /// [`Self::flush_deadline`] will not let the loop wake before it, so a closed gate costs one
    /// wake per beat instead of a spin.
    leader_hold: Option<Instant>,
    /// The engine's end of the lock's beat task: dropped with the engine, which stops the task and
    /// closes the lock session, so a dropped engine releases the lock instead of making the next
    /// instance wait for the lease.
    lock_task: LockTask,
    ready_logged: bool,
    /// The warn for the changes kept for targets that are behind passing their budget was
    /// emitted: one warn and one count per crossing of the lag budget
    /// (`Config::mem.lag_hold_bytes`), not per housekeeping
    /// tick (re-armed when the estimate falls back within the budget).
    lag_budget_warned: bool,
    /// (slot, targets written) of every live refresh of the flush in progress (`note_refresh`
    /// fills it, `flush_draining` reads it once when the flush is through, #215).
    flush_wrote: Vec<(usize, Vec<String>)>,
    /// Live views refreshed since `refreshed_at` was last bumped.
    refreshed: HashSet<String>,
    last_touch: Instant,
    last_stats: (Instant, crate::metrics::Snapshot),
    /// Hands each flush's per-view counters to the statistics aggregator task (src/stats.rs);
    /// the task ends when this, its only sender, is dropped with the engine.
    stats_tx: crate::stats::FoldSender,
    /// Concurrent refreshes on connection pools; `None` with `LAZYVIEW_SUPERVISOR=off` (one
    /// sequential refresh on the connections above).
    sup: Option<Arc<Supervisor>>,
    /// Executor (a): the dedup core and its targets, one per live version on the Dedup route
    /// (src/exec.rs).
    dedup: crate::exec::Dedup,
    /// Rate limiting of the statement-format DML warning ([`Engine::note_statement_dml`]):
    /// per (schema, table) the time of the last line — `None` while no line has been logged yet,
    /// which is how the first event logs at once without any arithmetic on the clock — and the
    /// events folded into the next line. Bounded by the number of tables whose sessions twist
    /// `binlog_format`.
    stmt_dml_last: HashMap<(String, String), (Option<Instant>, u64)>,
}

/// Binlog dump connections: connect, register as a replica and request the stream.
const BINLOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Flushes taking longer are logged (debug) with their slowest steps.
const SLOW_FLUSH: Duration = Duration::from_secs(1);

/// Minimum time between WARN lines for the same table in [`Engine::note_statement_dml`]: a bulk
/// job in statement format is one event per statement, so one line per table per interval
/// carries the count of the rest (review C-03, condition 2).
const STMT_DML_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// While the binlog stream keeps delivering (a backlog), a due flush waits at most this long.
const CATCH_UP_BATCH: Duration = Duration::from_millis(100);

/// Cadence of the per-view statistics hand-over to the aggregator *outside* a flush (review
/// O-01): a view that stops advancing has nothing to flush, yet its `source_lag_seconds` has to
/// move, so housekeeping republishes on this cadence. One batch per interval, never per event.
const STATS_PUBLISH_INTERVAL: u64 = 15;

/// Binlog read ahead while a flush runs (see `Engine::flush_draining`): at most this many
/// events, and at most `Config::mem.drain_max_bytes`, are queued; beyond that the stream waits
/// (backpressure). The event bound is a constant; the byte bound is a value of `Config`, derived
/// at startup from the memory the process was given (the total and the rule in [`crate::mem`],
/// review R-01 / N-06), so the buffers of the engine cannot outgrow the container. A process
/// with no limit gets the constants the engine carried before: 32 MiB for this queue and 256 MiB
/// for each of the three holds below (`Config::mem.build_hold_bytes`, `.xa_hold_bytes`,
/// `.lag_hold_bytes`, documented where they act: `hold_for_builds`, the unrouted rows of a
/// prepared XA in `on_rows`, and `lag_over_budget`).
const DRAIN_MAX_EVENTS: usize = 100_000;
/// Queue slots kept once a drained queue is empty again (a larger queue is shrunk to this).
const DRAIN_KEEP_SLOTS: usize = 1024;
/// Estimated memory of one buffered integer id, and of any other key, row image or mark (hash
/// slot, boxed datums and a short value; wide images count low).
const INT_KEY_BYTES: usize = 16;
const KEY_BYTES: usize = 96;

/// Estimated memory of buffered unit keys: the ids and the keys, of a change buffer or of the
/// sets a target that is behind keeps.
fn units_bytes(units: &UnitSet, partial: &[UnitSet]) -> usize {
    let ints = units.ints.len() + partial.iter().map(|p| p.ints.len()).sum::<u64>();
    let keys = units.keys.len() + partial.iter().map(|p| p.keys.len()).sum::<usize>();
    ints as usize * INT_KEY_BYTES + keys * KEY_BYTES
}

/// Estimated memory of a change buffer, from the sizes of its sets (no walk over the keys).
fn buffer_bytes(d: &ViewDirty) -> usize {
    units_bytes(&d.units, &d.partial)
        + (d.images.iter().map(HashSet::len).sum::<usize>() + d.marks.len() + d.holds.len())
            * KEY_BYTES
}

/// A binlog read result as the loop handles it (`Ok(None)`: the server closed the stream).
type Read = Result<Option<Item>>;

/// What the binlog reader knows about the **source's** clock: the two timestamps the freshness
/// figure is computed from, plus how the engine came to know the source's clock (review O-01,
/// ruling 1: "N seconds behind the source" has to hold during a backlog, not only while the
/// source is idle). Both are the source's own clock — a common-header timestamp and the
/// source's `UNIX_TIMESTAMP()` — so no host clock and no clock skew enters the number, and an
/// idle source gives exactly 0.
///
/// Cost (owner rule): three integer stores, and only on the rows events of tables a view reads
/// in [`Engine::on_item`] (the header ts is a field the reader has already parsed), no
/// allocation, and nothing on every other event; `offset` costs one statement per (re)connect
/// and per housekeeping tick, never per event.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SourceClock {
    /// Header ts of the newest change to a table a view reads that this connection has seen
    /// (`BinlogReader::event_ts` at the routed [`Item::Rows`] in [`Engine::on_item`]); 0:
    /// nothing read yet. The engine's own pen (its meta and materialized tables) and the
    /// shape-only events are not a view's business and do not move it. A view that has applied
    /// this much is caught up with the source.
    fed_ts: u64,
    /// Header ts of the *first* change to a table a view reads that this connection has read:
    /// the oldest change the views may still be missing. A view that has applied nothing since
    /// the process started (`Slot::applied_ts` 0) is measured against it, so an engine restarted
    /// over a gap reports the length of the gap rather than the age of the epoch. 0: this
    /// connection has read nothing.
    gap_ts: u64,
    /// The last stamp was a heartbeat (the head-moving stamps are the rows of tables a view
    /// reads in [`Engine::on_item`]). Only a heartbeat whose payload position is the position
    /// the views have applied to calls the stream at the head: the server drains its socket to
    /// the client at once and then heartbeats — a backlog can still sit in the read buffer —
    /// so the position, not the event type, is what proves the stream caught up with the
    /// source's binlog.
    at_head: bool,
    /// When the server last said its dump was empty (a heartbeat arrived), on the source's
    /// clock — whether or not that heartbeat proved the stream at the head. A view that has
    /// applied nothing this connection (and so has no `gap_ts` to age against yet) is measured
    /// from here, so a stream proven behind before its first routed change reads a number
    /// growing from the last heartbeat, not the age of the epoch.
    head_ts: u64,
    /// The reader is connected and able to deliver: false while reconnecting, and while binlog
    /// reading is held by a build's byte budget (`hold_for_builds`) — a source that keeps
    /// writing while nothing is read is a growing lag, not a growing head.
    reading: bool,
    /// The queue a flush left draining still holds events the views have not been handed
    /// (`Drained`): read, so the source is ahead of the views, and not yet counted as fed.
    pending_read: bool,
    /// `source UNIX_TIMESTAMP() − local UNIX now`, asked at connect and at the housekeeping
    /// tick: [`Self::now`] is the source's clock between two of those questions.
    offset: i64,
}

impl SourceClock {
    /// The source's clock, right now: the local clock shifted by the measured offset (so a
    /// source whose clock differs from the host's by minutes is still measured against itself).
    fn now(&self) -> u64 {
        (crate::metrics::unix_now() as i64 + self.offset).max(0) as u64
    }

    /// The newest change the view has applied, on the source's clock: its own stamp; else the
    /// oldest change this connection has read (see [`Self::gap_ts`]); else the last heartbeat —
    /// a view that has read nothing this connection ages from when the server last said its
    /// dump was empty, never from the epoch.
    fn applied(&self, applied_ts: u64) -> u64 {
        if applied_ts != 0 {
            applied_ts
        } else if self.gap_ts != 0 {
            self.gap_ts
        } else {
            self.head_ts
        }
    }

    /// Seconds `applied_ts` is behind the source, saturating:
    ///
    /// * `0` while the stream is at its head (a heartbeat arrived after the last change a view
    ///   reads), the reader is reading, and the view has applied every change read — an idle
    ///   source, however long idle, is 0 seconds behind, which is what the old "position age"
    ///   gauge could not say;
    /// * otherwise the age of the newest applied change on the source's clock, so a backlog
    ///   after an outage, a reader held by a build's budget, a reconnecting stream and a view
    ///   whose refresh keeps failing all show a number that grows with the time the source has
    ///   been ahead.
    fn lag(&self, applied_ts: u64) -> u64 {
        let applied = self.applied(applied_ts);
        if self.at_head && self.reading && !self.pending_read && applied >= self.fed_ts {
            0
        } else {
            self.now().saturating_sub(applied)
        }
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;

    /// Ruling 1, "idle: 0": a stream at its head with a view that has applied everything reads
    /// 0 seconds behind the source — and stays 0 under a host/source clock skew, because the
    /// figure is source-clock minus source-clock (the offset never enters the zero branch).
    #[test]
    fn idle_is_zero_even_under_clock_skew() {
        let caught = SourceClock {
            fed_ts: 2000,
            gap_ts: 2000,
            head_ts: 0,
            at_head: true,
            reading: true,
            pending_read: false,
            offset: 3600, // the source's clock runs an hour ahead of the host's
        };
        assert_eq!(caught.lag(2000), 0, "caught up, at head: 0");
        assert_eq!(
            caught.lag(0),
            0,
            "nothing applied yet, measured against the gap head: 0"
        );
    }

    /// Ruling 1, "backlog: the real age": a view that has applied only up to `applied_ts` while
    /// the source has advanced shows the age of its newest applied change on the source clock.
    #[test]
    fn backlog_is_the_real_age() {
        let now = crate::metrics::unix_now();
        let applied = now - 30;
        let c = SourceClock {
            fed_ts: now,
            gap_ts: applied,
            head_ts: 0,
            at_head: true,
            reading: true,
            pending_read: false,
            offset: 0,
        };
        let lag = c.lag(applied);
        assert!(
            (29..=32).contains(&lag),
            "backlog lag {lag} is not ~30s of real age"
        );
    }

    /// Ruling 1, "restarted over a gap: about the gap's length": a view that has applied nothing
    /// since the process started (`applied_ts` 0) is measured against the oldest change this
    /// connection has read (`gap_ts`), so a backlog that opened while the engine was down reads
    /// as the gap's length rather than the age of the epoch.
    #[test]
    fn restarted_over_a_gap_reports_the_gap() {
        let now = crate::metrics::unix_now();
        let gap_start = now - 40;
        let c = SourceClock {
            fed_ts: now,
            gap_ts: gap_start,
            head_ts: 0,
            at_head: true,
            reading: true,
            pending_read: true, // the queue still holds read-not-applied changes
            offset: 0,
        };
        let lag = c.lag(0);
        assert!(
            (38..=44).contains(&lag),
            "gap lag {lag} is not ~40s (the length of the gap)"
        );
    }

    /// Ruling 1, "paused reader: grows": a view caught up on content reads 0 while the reader
    /// runs, but the same view shows a growing number the moment binlog reading is held — the
    /// source keeps writing changes nobody is reading.
    #[test]
    fn paused_reader_grows() {
        let now = crate::metrics::unix_now();
        let applied = now - 5;
        let caught = SourceClock {
            fed_ts: applied,
            gap_ts: applied,
            head_ts: 0,
            at_head: true,
            reading: true,
            pending_read: false,
            offset: 0,
        };
        assert_eq!(caught.lag(applied), 0, "caught up while reading: 0");
        let held = SourceClock {
            reading: false,
            ..caught
        };
        let lag = held.lag(applied);
        assert!(
            (4..=8).contains(&lag),
            "held-reader lag {lag} is not ~5s and growing"
        );
    }

    /// Ruling 1's epoch guard: a connection proven behind (a heartbeat whose position was not
    /// the applied one) before it read a single routed change has no `gap_ts` to age against —
    /// it ages from the last heartbeat, not from 1970.
    #[test]
    fn proven_behind_before_the_first_read_ages_from_the_heartbeat() {
        let now = crate::metrics::unix_now();
        let c = SourceClock {
            fed_ts: 0,
            gap_ts: 0,
            head_ts: now - 7,
            at_head: false,
            reading: true,
            pending_read: false,
            offset: 0,
        };
        let lag = c.lag(0);
        assert!(
            (5..=10).contains(&lag),
            "lag {lag} is not ~7s of age since the heartbeat (and never the age of the epoch)"
        );
        assert!(
            lag < 60,
            "lag {lag} reads the epoch, not the last heartbeat"
        );
    }
}

/// Binlog reads queued while a flush ran (see `Engine::flush_draining`), each with its
/// approximate memory, and their running total.
struct Drained {
    queue: VecDeque<(Read, usize)>,
    bytes: usize,
    /// Binlog file the oldest queued events were read from (None while the queue is empty).
    file: Option<String>,
    /// The queue's byte budget (`Config::mem.drain_max_bytes`), carried so the two bounds are
    /// read from the queue itself on the hot path.
    max_bytes: usize,
}

impl Drained {
    /// An empty queue bounded by `max_bytes` (see `Config::mem`).
    fn new(max_bytes: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            bytes: 0,
            file: None,
            max_bytes,
        }
    }

    /// Queues `r`, read from binlog file `file`. Heartbeats are dropped: they carry nothing, and
    /// one taken from the queue would move the position to the reader's, past the events queued
    /// behind it (see `Engine::on_item`); the next one read while idle does that instead.
    fn push(&mut self, r: Read, file: &str) {
        let b = match &r {
            Ok(Some(Item::Rows { event, .. })) => 64 + event.data().len(),
            Ok(Some(Item::Heartbeat)) => return,
            _ => 64,
        };
        if self.queue.is_empty() {
            self.file = Some(file.to_string());
        }
        self.bytes += b;
        self.queue.push_back((r, b));
    }

    /// Whether the queue holds no event the views are still to be handed (see
    /// [`SourceClock::pending_read`]).
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn pop(&mut self) -> Option<Read> {
        let (r, b) = self.queue.pop_front()?;
        self.bytes -= b;
        if self.queue.is_empty() {
            self.file = None;
            if self.queue.capacity() > DRAIN_KEEP_SLOTS {
                self.queue.shrink_to(DRAIN_KEEP_SLOTS);
            }
        }
        Some(r)
    }

    /// Below both bounds, and not ended by an error or the end of the stream.
    fn open(&self) -> bool {
        self.queue.len() < DRAIN_MAX_EVENTS
            && self.bytes < self.max_bytes
            && !matches!(self.queue.back(), Some((Ok(None) | Err(_), _)))
    }

    fn full(&self) -> bool {
        self.queue.len() >= DRAIN_MAX_EVENTS || self.bytes >= self.max_bytes
    }
}

/// Opens a connection with the service's session settings of the target family `kind` (at most
/// `lock_wait_s` for a table lock, see [`target::init_session`]).
async fn connect_target(
    opts: &target::Opts,
    kind: target::Kind,
    lock_wait_s: u64,
    metrics: &Metrics,
) -> Result<target::Conn> {
    // One dispatch per connection: the family is chosen there, once.
    kind.connect(opts, lock_wait_s, metrics).await
}

/// The Opts of one source role (`LAZYVIEW_TEST_{BINLOG,QUERY,META}_DSN` of `Config`): the
/// role's own DSN when the network test sets one, `LAZYVIEW_DSN` otherwise. The hooks default
/// to `Config::dsn` in `config::Config::from_lookup`, so an unset hook leaves every role on the
/// service's single source endpoint; the `unwrap_or` only covers a hand-built `Config`.
fn role_opts(role: Option<&str>, dsn: &str) -> Result<Opts> {
    binlog::replica_opts(role.unwrap_or(dsn))
}

/// Opens a *source* connection (always MySQL-family, §0.2) with the service's
/// session settings of the MySQL target family.
async fn connect_source(opts: &Opts, lock_wait_s: u64, metrics: &Metrics) -> Result<Conn> {
    let mut c =
        match tokio::time::timeout(supervisor::CONNECT_TIMEOUT, Conn::new(opts.clone())).await {
            Ok(r) => r?,
            Err(_) => bail!(
                "connecting to {}:{}: timed out after {:?}",
                opts.ip_or_hostname(),
                opts.tcp_port(),
                supervisor::CONNECT_TIMEOUT,
            ),
        };
    target::init_session(&mut c, target::Kind::Mysql, lock_wait_s, metrics).await?;
    Ok(c)
}

/// The session sql_mode of a connection set up by [`connect`] (for pooled connections).
async fn session_sql_mode(conn: &mut Conn) -> Result<String> {
    let mode: Option<String> = conn
        .query_first("SELECT CAST(@@SESSION.sql_mode AS CHAR)")
        .await?;
    Ok(mode.unwrap_or_default())
}

/// Identity of the server that writes the binlog: a saved position only means something on
/// that server. MySQL/Percona: `@@server_uuid`; MariaDB: server id and GTID domain.
/// `fail_first` (test hook `LAZYVIEW_TEST_FAIL_IDENTITY`) fails the first such `try_no` reads
/// of one start: a stand-in for a connection blip on this read.
async fn server_identity(
    conn: &mut Conn,
    flavor: Flavor,
    fail_first: Option<usize>,
    try_no: usize,
) -> Result<String> {
    if fail_first.is_some_and(|n| try_no <= n) {
        bail!("injected identity failure");
    }
    let sql = match flavor {
        Flavor::MySql | Flavor::Percona => "SELECT CAST(@@server_uuid AS CHAR)",
        Flavor::MariaDb => "SELECT CAST(CONCAT(@@server_id, ':', @@gtid_domain_id) AS CHAR)",
    };
    let v: Option<Option<String>> = conn.query_first(sql).await?;
    Ok(v.flatten().unwrap_or_default())
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

/// Target ids waiting in a buffer: its units and partial unit keys, or a global view's single
/// row.
fn pending_ids(d: &ViewDirty, plan: &ViewPlan) -> u64 {
    (d.units.len() + d.partial.iter().map(UnitSet::len).sum::<usize>()) as u64
        + u64::from(d.all && matches!(plan.mode, Mode::Global))
}

fn mode_name(plan: &ViewPlan) -> (&'static str, Option<String>) {
    match &plan.mode {
        Mode::Spj => ("incremental", Some("spj".into())),
        Mode::Grouped => ("incremental", Some("grouped".into())),
        Mode::Global => ("incremental", Some("global".into())),
        Mode::Full(r) => ("full", Some(r.chars().take(255).collect())),
    }
}

// ------------------------------------------------------------------------------------ leadership
//
// The lock that says which instance of this schema is the active one lives on a connection of its
// own and is beaten by the task below — not by the binlog loop, which a swap waiting its
// `lock_wait_timeout` or a catch-up batch can occupy for longer than the lease (review A-06,
// condition 2), and not per binlog event: the cost is one tiny statement per instance per
// `LAZYVIEW_LOCK_HEARTBEAT_MS` (~0.2 QPS at the default), whatever the event rate.

/// What this instance's own lock task knows about its leadership. Read through a
/// [`watch`]: a gate pays an atomic read and no round-trip, so checking it can never become work
/// on the event path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderState {
    /// The lock session answered its last beat and holds the named lock: this instance is the
    /// active one.
    Active,
    /// No beat has been answered for `2 x LAZYVIEW_LOCK_HEARTBEAT_MS`, so the lease this instance's
    /// lock lives on may be running out and another instance may take the lock at any moment
    /// (lv-super ruling 1, 2026-11-16). It closes the same gates as `Fenced` — no *new* write
    /// starts here — but it is not a verdict: the beat that answers, or the rejoin that confirms
    /// the same epoch, brings this back to `Active`, and the writes already dispatched are ended
    /// through the zombie fence at once rather than left to run to `LAZYVIEW_TARGET_TIMEOUT_MS`.
    Uncertain,
    /// Another session holds the lock, or the fencing epoch moved while our session was away:
    /// this instance is the replaced one. `why` names the epochs for the log and the fatal error.
    Fenced(String),
}

/// What one beat of the lock session said.
enum Beat {
    /// The session answered and still holds the lock.
    Held,
    /// The session answered: some other session holds the lock now. `why` says what the backend
    /// knows (the two epochs), for the WARN and the engine's fatal error.
    Taken(String),
}

/// The lock session as the beat task sees it: two calls, either of which can fail. Its own trait
/// so the task's state machine can be driven by a script in a unit test, with no server behind it.
trait LockBeat: Send {
    /// Keeps the session from going silent and reports whether it still holds the lock. `Err`
    /// means the session cannot be trusted: it is dropped and `rejoin` opens a new one.
    fn beat(&mut self) -> impl Future<Output = Result<Beat>> + Send;

    /// Takes the lock again on a re-opened session. `Err` with a transient cause means the lock
    /// server is simply away — try again on the next beat; any other `Err` means this instance
    /// was replaced.
    fn rejoin(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// [`coord::LockSession`] seen as the two calls the beat task makes.
struct LockSessionBeat {
    session: coord::LockSession,
    instance_id: String,
    /// For the fenced message: what this instance believed when it took the lock.
    epoch: u64,
    lock_name: String,
}

impl LockBeat for LockSessionBeat {
    async fn beat(&mut self) -> Result<Beat> {
        if self.session.beat().await? {
            Ok(Beat::Held)
        } else {
            Ok(Beat::Taken(format!(
                "another session holds the leadership lock `{}` while we hold epoch {}",
                self.lock_name, self.epoch
            )))
        }
    }

    async fn rejoin(&mut self) -> Result<()> {
        self.session.rejoin(&self.instance_id).await
    }
}

/// What the lock task asks of the refresh supervisor the moment the lease stops being provable
/// (lv-super ruling 1, 2026-11-16): end the target writes this instance has already started, at
/// once. The lease must not expire with a write of ours still running, or the replacement's
/// refresh of the same rows becomes the older of two writers.
///
/// A trait so the beat task's state machine can be driven by a script in a unit test with no target
/// behind it, for the same reason [`LockBeat`] exists.
trait WriteFence: Send + Sync + 'static {
    /// Ends the write ops in flight through the zombie fence (§4.3) and reports how many the
    /// servers confirmed gone. Bounded per op by `supervisor::FENCE_WAIT`, and called on a
    /// transition of the lock state — never per row, never per binlog event.
    fn stop_writes(&self) -> impl Future<Output = usize> + Send;
}

/// The writes this instance can have in flight, as the lock task sees them: the refresh supervisor's,
/// when it runs one. `LAZYVIEW_SUPERVISOR=off` issues no write ops, so there is nothing to end.
/// An enum over a `dyn WriteFence` because the trait's method returns a future, and a task that
/// beats on a bounded statement of its own has no business paying a vtable for it.
enum Writes {
    Supervised(Arc<Supervisor>),
    None,
}

impl WriteFence for Writes {
    async fn stop_writes(&self) -> usize {
        match self {
            Writes::Supervised(s) => s.fence_writes_in_flight().await,
            Writes::None => 0,
        }
    }
}

/// The same leadership state, seen by the refresh supervisor's write ops
/// ([`supervisor::LeaderGate`]): one `watch` borrow per write op, so the gate that keeps a replaced
/// — or no-longer-provable — instance off the targets costs an atomic read and nothing else.
struct LeaderWriteGate {
    state: watch::Receiver<LeaderState>,
}

impl supervisor::LeaderGate for LeaderWriteGate {
    fn check(&self, endpoint: &str) -> Result<()> {
        match &*self.state.borrow() {
            LeaderState::Active => Ok(()),
            LeaderState::Uncertain => Err(anyhow!(
                "{endpoint}: the leadership lock has not answered a beat; no new writes until it is confirmed again"
            )),
            LeaderState::Fenced(why) => Err(anyhow!("fenced: {why}")),
        }
    }
}

/// The engine's end of the lock task: dropping it — which happens with the engine — stops the
/// task, and the task taking its session with it closes that connection, so the named lock is
/// released at once instead of after the lease.
struct LockTask {
    stop: Option<watch::Sender<bool>>,
}

impl LockTask {
    /// Ends the beat task, which takes the lock session with it: the named lock is released now
    /// instead of when the lease ends. `Engine::stream` calls this as it ends; `Drop` is the same
    /// call for the paths that never get there (a fatal error, a panic, a dropped engine).
    fn stop(&mut self) {
        // Dropping the last sender is the stop signal: a `watch` answers `changed()` at once,
        // forever, on a closed channel — and `take` is how one drops it from here.
        drop(self.stop.take());
    }
}

impl Drop for LockTask {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Publishes the state unless it already says that, so a beat that changes nothing wakes nobody.
fn publish_leader(state: &watch::Sender<LeaderState>, next: LeaderState) {
    state.send_if_modified(|cur| {
        if *cur == next {
            false
        } else {
            *cur = next;
            true
        }
    });
}

/// The lock task's own verdict that this instance has been replaced: counted, logged at WARN with
/// both epochs, and published for the engine to stop with (review A-06 condition 3). The session
/// goes with the task, so the replacement never waits on a lock we no longer hold.
fn fence_leader(metrics: &Metrics, state: &watch::Sender<LeaderState>, why: String) {
    Metrics::add(&metrics.leader_fenced, 1);
    warn!("fenced: {why}; this instance is the replaced one and stops at its next check");
    publish_leader(state, LeaderState::Fenced(why));
}

/// (lv-super ruling 1, 2026-11-16) Two periods without an answered beat: this instance can no
/// longer prove it holds the lock, so it stops issuing writes — the same gates `Fenced` closes, and
/// for the same reason: whoever takes the lock must not find two writers of the same rows — and the
/// writes it has already started are ended through the zombie fence rather than left to run to
/// `LAZYVIEW_TARGET_TIMEOUT_MS`. One entry and one count per episode of uncertainty; the beat that
/// answers, or the rejoin that confirms the same epoch, publishes `Active` again.
///
/// The fence runs on a task of its own (at most one at a time): the beats must not wait for it,
/// because a beat task that goes silent is exactly what loses the lease.
fn hold_writes<F: WriteFence>(
    metrics: &Metrics,
    state: &watch::Sender<LeaderState>,
    fence: &Arc<F>,
    fencing: &mut Option<JoinHandle<usize>>,
    uncertain_after: Duration,
) {
    if !matches!(*state.borrow(), LeaderState::Active) {
        return;
    }
    Metrics::add(&metrics.leader_uncertain, 1);
    warn!("leadership lock: no beat answered for {uncertain_after:?}; holding every write gate until the lock is confirmed again");
    publish_leader(state, LeaderState::Uncertain);
    if fencing.as_ref().is_none_or(JoinHandle::is_finished) {
        let fence = fence.clone();
        *fencing = Some(tokio::spawn(async move { fence.stop_writes().await }));
    }
}

/// Ends the write ops this instance started before it goes: one of ours must not commit after the
/// replacement's, so here the fence *is* awaited (unlike the `Uncertain` entry, where the beats have
/// to keep running). Bounded per op by `supervisor::FENCE_WAIT`.
async fn quarantine_writes<F: WriteFence>(fencing: &mut Option<JoinHandle<usize>>, fence: &Arc<F>) {
    match fencing.take() {
        Some(h) => {
            let _ = h.await;
        }
        None => {
            fence.stop_writes().await;
        }
    }
}

/// Stops a fence still running when the instance ends for a reason that is not a takeover: the
/// process is leaving, and an unfinished KILL of a connection that dies with it is not worth the
/// wait. What could not be confirmed stays parked, as it does for a timed-out write.
fn stop_fencing(fencing: &mut Option<JoinHandle<usize>>) {
    if let Some(h) = fencing.take() {
        h.abort();
    }
}

/// The leadership lock's own task (review A-06): one bounded statement per `heartbeat` on the
/// lock's own session, which keeps the server from ever seeing that session go silent while this
/// instance lives — the lease exists to end a *lost* node's session, never a live one's — and
/// which notices a lock someone else has taken within one beat instead of at the binlog loop's
/// next 60 s check (a check that loop could also be too busy to run).
///
/// A beat that cannot be answered is not fatal: the session is dropped, a fresh one is opened and
/// the lock and the epoch confirmed again (`coord::LockSession::rejoin`), so a GC pause, a CPU
/// starvation or a proxy blip never ends a healthy instance with builds running. Only a rejoin
/// that finds the lock held elsewhere, or the epoch moved, says this instance was replaced.
///
/// What two periods of unanswered beats do say (ruling 1) is `Uncertain`: the gates close and the
/// writes in flight are fenced, because the lease — the server's clock, which we cannot read — may
/// already be running out. `Lease::new` demands a lease of at least `4 x heartbeat` so that all of
/// this, and one more beat to confirm the lock on the way back, ends before the lease does.
async fn run_lock_beat<D: LockBeat + Send + 'static, F: WriteFence>(
    mut d: D,
    heartbeat: Duration,
    metrics: Arc<Metrics>,
    state: watch::Sender<LeaderState>,
    fence: Arc<F>,
    mut shutdown: watch::Receiver<bool>,
    mut stop: watch::Receiver<bool>,
) {
    let uncertain_after = 2 * heartbeat;
    let mut last_ok = Instant::now();
    let mut fencing: Option<JoinHandle<usize>> = None;
    loop {
        match d.beat().await {
            Ok(Beat::Held) => {
                metrics.leader_beat_ok();
                last_ok = Instant::now();
                publish_leader(&state, LeaderState::Active);
            }
            Ok(Beat::Taken(why)) => {
                quarantine_writes(&mut fencing, &fence).await;
                fence_leader(&metrics, &state, why);
                return;
            }
            Err(e) => {
                Metrics::add(&metrics.leader_beat_failures, 1);
                warn!(
                    "leadership lock: beat failed ({}); rejoining",
                    e.root_cause()
                );
                match d.rejoin().await {
                    Ok(()) => {
                        metrics.leader_beat_ok();
                        last_ok = Instant::now();
                        publish_leader(&state, LeaderState::Active);
                        info!("leadership lock: session re-opened, the lock and epoch confirmed; still the active instance");
                    }
                    // The lock server is away: the lease is the server's own clock, so this
                    // instance keeps its lock there for now and the next beat tries again. The
                    // beat that failed already counted; a deferred rejoin is not a second one.
                    Err(e) if binlog::is_transient(&e) => {
                        warn!("leadership lock: rejoin deferred ({})", e.root_cause());
                    }
                    Err(e) => {
                        quarantine_writes(&mut fencing, &fence).await;
                        fence_leader(&metrics, &state, format!("{e:#}"));
                        return;
                    }
                }
            }
        }
        // The bound, checked once per pass: a beat that hung until its timeout answered nothing
        // either, and the time it hung for counts towards this. One answered beat resets it.
        if last_ok.elapsed() >= uncertain_after {
            hold_writes(&metrics, &state, &fence, &mut fencing, uncertain_after);
        }
        tokio::select! {
            _ = tokio::time::sleep(heartbeat) => {}
            _ = shutdown.changed() => if *shutdown.borrow() {
                stop_fencing(&mut fencing);
                return
            },
            // The engine went: release the lock rather than hold it until the lease does.
            _ = stop.changed() => {
                stop_fencing(&mut fencing);
                return
            },
        }
    }
}

impl Engine {
    /// `Ok(None)`: shutdown requested while waiting for the database or the lock.
    async fn start(
        cfg: Config,
        metrics: Arc<Metrics>,
        shutdown: &mut watch::Receiver<bool>,
        replica: Result<Opts>,
    ) -> Result<Option<(Engine, Option<Position>)>> {
        Metrics::set(&metrics.gauges.starting, 1);
        // B7: the one `replica_opts` parse (engine B7); the Result arrives from `run_engine`.
        let src_opts = replica?;
        // The three source roles each connect on their own DSN (the chaos plan's F4). Unset,
        // every one of them is LAZYVIEW_DSN, so a run without the hooks opens exactly what it
        // opened before: one endpoint, three connections to it.
        let query_opts = role_opts(cfg.test_query_dsn.as_deref(), &cfg.dsn)?;
        let meta_opts = role_opts(cfg.test_meta_dsn.as_deref(), &cfg.dsn)?;
        let binlog_opts = role_opts(cfg.test_binlog_dsn.as_deref(), &cfg.dsn)?;
        let schema = target::mysql::schema_of(&src_opts, "LAZYVIEW_DSN")?;
        let Some(mut src) = connect_wait(
            &query_opts,
            "source database",
            &metrics,
            shutdown,
            cfg.lock_wait_s,
        )
        .await?
        else {
            return Ok(None);
        };
        let info = binlog::server_info(&mut src).await?;
        info!(
            "connected to {} at {}:{}, schema `{schema}`",
            info.version,
            query_opts.ip_or_hostname(),
            query_opts.tcp_port()
        );
        managed_service_checks(&mut src).await;
        binlog_filter_checks(&mut src, &schema, query_opts.db_name()).await;
        let Some(mut meta_conn) = connect_wait(
            &meta_opts,
            "source database",
            &metrics,
            shutdown,
            cfg.lock_wait_s,
        )
        .await?
        else {
            return Ok(None);
        };
        // Two runs (owner 2026-11-16): the operations run (the default) never issues DDL on
        // lazyview's own tables — it checks them through information_schema with SELECT rights
        // and refuses to start with a `SetupRequired` error (exit code 2) naming `lazyview
        // setup`. The setup and migration run (`lazyview setup`, today's LAZYVIEW_SYNC=false)
        // owns all DDL on them. View tables in the targets are not lazyview's own tables: their
        // DDL stays in the operations run.
        if cfg.sync {
            meta::verify(&mut meta_conn, &schema).await?;
        } else {
            meta::ensure(&mut meta_conn, &schema).await?;
        }
        // The leadership state, published by the beat task below and read by every write gate
        // (ruling 1): the engine holds the receiver, the task the sender. Open before the
        // supervisor, which is gated by it, and before the lock session: an instance that never
        // gets the lock never publishes anything, and `Active` is what a fresh session means.
        let (leader_tx, leader_rx) = watch::channel(LeaderState::Active);
        // The refresh supervisor, and the two ends of its leadership gate: the write ops ask the
        // `watch` before they are issued, and the lock task ends the ops already running the moment
        // the lease stops being provable. `Arc` because both of them share it.
        let mut sup = Supervisor::new(
            &cfg.supervisor,
            &query_opts,
            &session_sql_mode(&mut src).await?,
            cfg.lock_wait_s,
            metrics.clone(),
        );
        if let Some(s) = sup.as_mut() {
            s.set_leader_gate(Arc::new(LeaderWriteGate {
                state: leader_rx.clone(),
            }));
        }
        let sup = sup.map(Arc::new);
        // The leadership lock goes on a session of its own (review A-06): `meta_conn` stays what
        // the catalog, the state rows and the position writes use, and the lock — with the lease
        // the server enforces on it — is kept alive by the task below, never by this connection or
        // by the binlog loop. Its endpoint is the metadata one unless a test names another: the
        // lock and the `_lazyview_status` epoch row it fences must stay on one server.
        let lock_opts = role_opts(
            cfg.test_lock_dsn
                .as_deref()
                .or(cfg.test_meta_dsn.as_deref()),
            &cfg.dsn,
        )?;
        let Some(session) = coord::LockSession::open(
            lock_opts,
            &schema,
            &cfg.instance_id,
            &metrics,
            coord::Lease::from_config(&cfg),
            shutdown,
        )
        .await?
        else {
            return Ok(None);
        };
        let leader = coord::Leadership {
            lock_name: session.leadership.lock_name.clone(),
            epoch: session.leadership.epoch,
        };
        info!(
            "leadership lock `{}` held on a session of its own: beat {} s, lease {} s",
            leader.lock_name,
            cfg.lock_heartbeat.as_secs_f64(),
            cfg.lock_lease.as_secs_f64()
        );
        // The beat task, on the runtime and not on the binlog loop, so a loop occupied past the
        // lease cannot starve the beats (condition 2). The engine owns the only stop sender: when
        // it goes, the task stops and closes the session, which releases the named lock at once.
        let (lock_stop, lock_stop_rx) = watch::channel(false);
        // The writes this instance can have in flight, for the lock task to end the moment the lease
        // stops being provable (ruling 1). `LAZYVIEW_SUPERVISOR=off` issues none.
        let fence = Arc::new(match &sup {
            Some(s) => Writes::Supervised(s.clone()),
            None => Writes::None,
        });
        tokio::spawn(run_lock_beat(
            LockSessionBeat {
                session,
                instance_id: cfg.instance_id.clone(),
                epoch: leader.epoch,
                lock_name: leader.lock_name.clone(),
            },
            cfg.lock_heartbeat,
            metrics.clone(),
            leader_tx,
            fence,
            shutdown.clone(),
            lock_stop_rx,
        ));

        let mut targets = BTreeMap::new();
        targets.insert(
            SOURCE_TARGET.to_string(),
            Target {
                kind: target::Kind::Mysql,
                opts: target::Opts::Mysql(src_opts.clone()),
                schema: schema.clone(),
                conn: None,
                write: None,
                sql_mode: None,
                retry_at: None,
                failures: 0,
            },
        );
        for (name, dsn) in &cfg.targets {
            let (kind, opts) = target::opts(dsn)?;
            let ts = target::schema_of(&opts, &format!("target {name}"))?;
            targets.insert(
                name.clone(),
                Target {
                    kind,
                    opts,
                    schema: ts,
                    conn: None,
                    write: None,
                    sql_mode: None,
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
        match cfg.supervisor.mode {
            crate::config::SupervisorMode::Off => {
                info!("refreshes run sequentially (LAZYVIEW_SUPERVISOR=off)")
            }
            mode => {
                if mode == crate::config::SupervisorMode::Adaptive {
                    info!("adaptive limiter not implemented yet; using fixed limits");
                }
                info!(
                    "refreshes run concurrently: {} source reads, {} writes per target",
                    cfg.supervisor.max_source, cfg.supervisor.max_target
                );
            }
        }
        let zero = Position {
            file: String::new(),
            pos: 0,
        };
        // The one total every byte budget was derived from, and where it came from (the line an
        // operator reads when comparing the container's limit with the buffers, review N-06).
        info!("{}", cfg.mem.summary());
        let dedup = crate::exec::Dedup::new(
            zero.clone(),
            cfg.batch_max_keys.max(1),
            cfg.query_chunk.max(1),
            // The core's own bound is a share of the same total; above it its caller stops
            // reading (dedup::over_bound), never allocates past it.
            cfg.mem.dedup_max_bytes,
        );
        // Detached: the task ends when the engine, owner of its only sender, is dropped.
        let (stats_tx, stats_task) = crate::stats::spawn_aggregator(metrics.view_stats.clone());
        drop(stats_task);
        let mut e = Engine {
            cfg,
            metrics,
            query_opts,
            meta_opts,
            binlog_opts,
            schema,
            flavor: info.flavor,
            caps: info.flavor.detect(),
            server_version: info.version.clone(),
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
            state_check: false,
            shown: HashMap::new(),
            replan: HashMap::new(),
            failures: HashMap::new(),
            defs_recheck: None,
            last_src_ping: Instant::now(),
            // Freshness (review O-01): before the first connect the engine is believed at the
            // head (the first heartbeat proves it either way) and ages from the start instant.
            clock: SourceClock {
                at_head: true,
                head_ts: crate::metrics::unix_now(),
                ..Default::default()
            },
            last_clock: Instant::now(),
            last_publish: Instant::now(),
            boundary: Cursor::none(info.flavor),
            persisted: Cursor::none(info.flavor),
            last_persist: Instant::now(),
            in_txn: false,
            txn_foreign: false,
            persist_needed: false,
            first_dirty: None,
            core_shape_routes: false,
            last_gtid: None,
            snapshot_fence: None,
            snapshot_watermark: info.flavor.snapshot_position_is_watermark(),
            commit_fence: None,
            commit_fenced: None,
            fence_warned: false,
            generation: 0,
            build_tx,
            build_rx: Some(build_rx),
            build_slots: Arc::new(tokio::sync::Semaphore::new(cfg_build_concurrency)),
            leader,
            leader_state: leader_rx,
            leader_hold: None,
            lock_task: LockTask {
                stop: Some(lock_stop),
            },
            ready_logged: false,
            lag_budget_warned: false,
            flush_wrote: Vec::new(),
            refreshed: HashSet::new(),
            last_touch: Instant::now(),
            last_stats: (Instant::now(), Default::default()),
            stats_tx,
            sup,
            dedup,
            stmt_dml_last: HashMap::new(),
        };

        if !e.cfg.sync {
            let defs = meta::load_definitions(&mut e.meta).await?;
            // Validate definitions only (usable in CI: non-zero exit on any problem).
            let mut bad = 0;
            let mut names: HashMap<String, u64> = HashMap::new();
            let stored = meta::load_state(&mut e.meta).await.unwrap_or_default();
            for d in defs.iter().filter(|d| d.enabled) {
                match e
                    .parse_and_plan(&d.effective_definition(), &d.targets, None, &HashMap::new())
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
                            let ap = crate::access::AccessPlan::of(&p);
                            info!("{ap}");
                            for t in &d.targets {
                                let key = (p.name.clone(), t.clone());
                                let fp = stored
                                    .get(&key)
                                    .and_then(|s| s.access.as_deref())
                                    .and_then(crate::access::AccessPlan::fingerprint_of);
                                match fp {
                                    Some(f) if f == ap.fingerprint => {
                                        info!(view = %p.name, target = %t, "access same")
                                    }
                                    Some(f) => info!(
                                        view = %p.name,
                                        target = %t,
                                        "access changed (stored {f}, now {})",
                                        ap.fingerprint
                                    ),
                                    None => {
                                        info!(view = %p.name, target = %t, "access new (no state)")
                                    }
                                }
                            }
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
                "setup run: metadata ready, {} definition(s) valid, not streaming \
                 (start the operations run with `lazyview run`)",
                names.len()
            );
            return Ok(Some((e, None)));
        }

        let stored = meta::load_position(&mut e.meta, &e.cfg.instance_id, e.flavor).await?;
        let mut rebuild_all =
            matches!(e.cfg.start_mode, StartMode::Rebuild | StartMode::Once) || stored.is_none();
        let mut start = match &stored {
            Some(p) if !rebuild_all => p.clone(),
            _ => Cursor::from_position(e.flavor, &binlog::current_position(&mut e.src).await?),
        };
        // A position saved on another server (failover, restore, RESET MASTER) may name a file
        // that exists here too, with other contents.
        // An unreadable identity must not open the resume guard: retry, then refuse to start on a
        // saved position we cannot attribute to this server.
        let id_flavor = e.flavor;
        let id_fail = e.cfg.test_fail_identity;
        let identity = match retry_src(
            &e.metrics,
            "reading the server identity",
            &mut e.src,
            &e.query_opts,
            e.cfg.lock_wait_s,
            |try_no, src| Box::pin(server_identity(src, id_flavor, id_fail, try_no)),
        )
        .await
        {
            Ok(id) => id,
            Err(_) if stored.is_none() => String::new(), // nothing to check against: fresh start
            Err(e) => return Err(e).context("cannot read the source server identity"),
        };
        let saved_identity = meta::load_identity(&mut e.meta, &e.cfg.instance_id)
            .await?
            .filter(|s| !s.is_empty());
        // `(why, gone)`: `gone` = the saved file verifiably no longer exists on the source.
        let unusable = if rebuild_all {
            None
        } else if let Some(saved) =
            saved_identity.filter(|s| !identity.is_empty() && *s != identity)
        {
            Some((
                format!(
                    "saved binlog position {start} was written by server {saved}, but the source is server {identity} (failover, restore or RESET MASTER?)"
                ),
                false,
            ))
        } else if !source::can_resume(&mut e.src, &start).await? {
            Some((
                format!("saved binlog position {start} is no longer available on the server"),
                true,
            ))
        } else {
            None
        };
        if let Some((why, gone)) = unusable {
            match e.cfg.on_missing_binlog {
                MissingBinlog::Fail => {
                    let msg = format!(
                        "{why}; set LAZYVIEW_ON_MISSING_BINLOG=rebuild or LAZYVIEW_START_MODE=rebuild"
                    );
                    // Only a purged file makes the views stale for good. A server mismatch can be
                    // undone (the right server back): the rows stay, and that start resumes.
                    if gone {
                        e.mark_unusable(&msg).await;
                    }
                    bail!(msg)
                }
                MissingBinlog::Rebuild => {
                    warn!("{why}; rebuilding all views");
                    rebuild_all = true;
                    start = Cursor::from_position(
                        e.flavor,
                        &binlog::current_position(&mut e.src).await?,
                    );
                }
            }
        }
        if !identity.is_empty() {
            meta::save_identity(&mut e.meta, &e.cfg.instance_id, &identity).await?;
        }
        e.boundary = start.clone();
        // Read the definitions only now that `start` is fixed: a definition committed while the engine
        // starts is then in this snapshot, in the stream from `start`, or both (reloading an unchanged
        // definition is a no-op). Read before the position, a commit in between was in neither and its
        // view was never built.
        let defs = meta::load_definitions(&mut e.meta).await?;
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
        Ok(Some((e, Some(start.to_position()))))
    }

    // ---------------------------------------------------------------- definitions & planning

    async fn table_info(&mut self, t: &TableRef) -> Result<Option<Arc<TableInfo>>> {
        self.catalog.get(&mut self.src, &t.0, &t.1, self.caps).await
    }

    /// Parses and plans a definition against the current catalog, except for the tables in
    /// `pinned` (their definitions as given). `own_def` excludes the slot itself from the
    /// chained-view check.
    async fn parse_and_plan(
        &mut self,
        definition: &str,
        targets: &[String],
        own_def: Option<u64>,
        pinned: &HashMap<TableRef, Arc<TableInfo>>,
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
                // Named like a table it reads that lazyview did not build: that is a name clash
                // (the documented refusal), not a view reading itself.
                let marker = own_def.map(|id| target::marker(&parsed.name, id));
                let ours = self
                    .owned
                    .contains_key(&(parsed.name.clone(), SOURCE_TARGET.to_string()))
                    || (marker.is_some()
                        && target::mysql::table_marker(&mut self.src, &self.schema, &parsed.name)
                            .await?
                            == marker);
                if !ours {
                    if let Some(kind) =
                        target::mysql::table_kind(&mut self.src, &self.schema, &parsed.name).await?
                    {
                        if targets.iter().any(|t| t == SOURCE_TARGET) {
                            bail!(
                                "refusing to replace existing {} `{}`.`{}` on target {SOURCE_TARGET} that lazyview does not manage (the view reads it: give the view another name)",
                                kind.to_lowercase(),
                                self.schema,
                                parsed.name
                            );
                        }
                        bail!(
                            "view `{}` is named like the {} `{}`.`{}` it reads: give the view another name",
                            parsed.name,
                            kind.to_lowercase(),
                            self.schema,
                            parsed.name
                        );
                    }
                }
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
            let info = match pinned.get(t) {
                Some(i) => Some(i.clone()),
                None => self.table_info(t).await?,
            };
            match info {
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
        let plan = self.plan_bounded(definition, &parsed, infos).await?;
        // Why each cascade the view rebuilds on cannot be traced (the analyzer's reason).
        let mut reasons: Vec<String> = plan
            .cascades
            .iter()
            .filter(|c| c.unit_from_parent.is_none() && !c.traced && !c.derived && c.map.is_none())
            .map(|c| {
                c.untraced.clone().unwrap_or_else(|| {
                    format!(
                        "`{}` cascades into `{}`.`{}`",
                        c.parent.name, c.child.0, c.child.1
                    )
                })
            })
            .collect();
        reasons.sort();
        reasons.dedup();
        let mapped: Vec<String> = plan
            .cascades
            .iter()
            .filter(|c| c.map.is_some())
            .map(|c| format!("`{}` -> `{}`", c.parent.name, c.child.1))
            .collect();
        if !mapped.is_empty() && plan.is_incremental() {
            debug!(view = %plan.name, routes = %mapped.join(", "), "follows cascades through the cascade map");
        }
        if !reasons.is_empty() && plan.is_incremental() {
            warn!(view = %plan.name, reasons = %reasons.join("; "), "reads a child of a cascading foreign key whose units cannot be derived from the parent; parent deletes/updates trigger an online rebuild");
        }
        Ok(plan)
    }

    /// Plans off the loop's thread, within `LAZYVIEW_PLAN_TIMEOUT_MS`: a definition that plans
    /// pathologically slowly must not stall every view (and, past
    /// `LAZYVIEW_LIVENESS_STALL_SECS`, make `/livez` restart the process, which plans it again).
    async fn plan_bounded(
        &mut self,
        definition: &str,
        parsed: &analyze::ParsedDef,
        infos: HashMap<TableRef, Arc<TableInfo>>,
    ) -> Result<ViewPlan> {
        let (parsed, schema) = (parsed.clone(), self.schema.clone());
        let delay = self
            .cfg
            .test_plan_delay
            .as_ref()
            .filter(|(marker, _)| definition.contains(marker.as_str()))
            .map(|(_, d)| *d);
        // A detached thread, not `spawn_blocking`: the runtime would wait for an abandoned
        // planner at shutdown.
        let (tx, rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("lazyview-plan".into())
            .stack_size(8 << 20)
            .spawn(move || {
                if let Some(d) = delay {
                    std::thread::sleep(d);
                }
                let _ = tx.send(analyze::plan(&parsed, &schema, &infos));
            })
            .context("starting the planner thread")?;
        self.metrics.beat();
        let r = tokio::time::timeout(self.cfg.plan_timeout, rx).await;
        self.metrics.beat();
        match r {
            Ok(Ok(plan)) => {
                let plan = plan?;
                self.refuse_for_source(&plan)?;
                Ok(plan)
            }
            Ok(Err(_)) => Err(anyhow!("planning failed: the planner panicked")),
            Err(_) => {
                Metrics::add(&self.metrics.plans_timed_out, 1);
                Err(PlanTimedOut(self.cfg.plan_timeout).into())
            }
        }
    }

    /// Refuses an incremental plan that the source cannot maintain from its row images
    /// ([`source::plan_refusal`], per flavor and server version). Full-refresh plans re-read the
    /// source and are never refused here.
    fn refuse_for_source(&self, plan: &ViewPlan) -> Result<()> {
        if matches!(plan.mode, Mode::Full(_)) {
            return Ok(());
        }
        for a in &plan.aliases {
            if let Some(why) =
                source::plan_refusal(self.flavor, &self.server_version, &a.table, &a.referenced)
            {
                bail!("{why}");
            }
        }
        Ok(())
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
        let ap = plan.map(crate::access::AccessPlan::of);
        let route_text = ap.as_ref().map(|a| a.route_name().to_string());
        let access_text = ap.as_ref().map(|a| a.to_string());
        self.shown.insert(
            (name.to_string(), target.to_string()),
            ShownState {
                definition_id: def_id,
                hash: hash.to_string(),
                mode: mode.to_string(),
                detail: detail.clone(),
                route: route_text.clone(),
                access: access_text.clone(),
                state: state.to_string(),
                error: err.map(str::to_string),
            },
        );
        // The leadership gate (ruling 1) on the one path that writes `_lazyview_state`: what the
        // views look like from here is recorded for the operator-facing reads, but the row itself
        // is the new leader's to write — it owns the same instance id, and a replaced instance that
        // kept writing them would make the state rows say what only this instance knows.
        if self.leader_holds_writes() {
            debug!("state row of {name}@{target} held: the leadership lock does not allow new writes right now");
            return;
        }
        let r = meta::set_state(
            &mut self.meta,
            StateUpdate {
                name,
                target,
                definition_id: def_id,
                hash,
                mode,
                detail: detail.as_deref(),
                route: ap.as_ref().map(|a| a.route_name()),
                access: access_text.as_deref(),
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
            if let Err(e) = meta::delete_stale_state(&mut self.meta, def_id, name).await {
                warn!("deleting stale state rows of {name}: {e:#}");
            }
        }
    }

    /// Shows (`Some`) or clears (`None`) the error of a live view whose refreshes keep failing, in
    /// `last_error` of its `ready` state rows (state and `refreshed_at` stay as they are).
    async fn show_refresh_error(&mut self, i: usize, err: Option<&str>) {
        let Some(l) = self.slots[i].live.as_ref() else {
            return;
        };
        let (name, targets) = (l.name.clone(), l.targets.clone());
        for t in &targets {
            if let Err(e) = meta::set_last_error(&mut self.meta, &name, t, err).await {
                warn!("writing _lazyview_state.last_error of {name}@{t}: {e:#}");
            }
            if let Some(st) = self.shown.get_mut(&(name.clone(), t.clone())) {
                if st.state == "ready" {
                    st.error = err.map(str::to_string);
                }
            }
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
                match other {
                    Some((other_id, other)) if other != plan.name => Err(CaseClash {
                        name: plan.name.clone(),
                        id: d.id,
                        other,
                        other_id,
                    }
                    .into()),
                    other => bail!(
                        "name `{}` already used by row #{}",
                        plan.name,
                        other
                            .map(|(i, _)| i.to_string())
                            .unwrap_or_else(|| "?".into())
                    ),
                }
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
        let at = self.boundary.to_position();
        self.schedule_replan_at(def_id, freeze, retry, &at);
    }

    /// As [`Self::schedule_replan`]; unless it is a retry (the view is in error then), readers
    /// see a stale view from `since` on until a rebuild started after it is swapped in.
    fn schedule_replan_at(&mut self, def_id: u64, freeze: bool, retry: bool, since: &Position) {
        let delay = if retry {
            let n = self.failures.entry(def_id).or_insert(0);
            *n += 1;
            // The one backoff maths; the definition is parsed once per plan (plan_bounded),
            // planning being a pure function of it.
            supervisor::backoff(*n, Duration::from_secs(1), Duration::from_secs(300), false)
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

    /// The source's binlog position now (right after a catalog read: every DDL that read saw is
    /// before it); `None` if it cannot be read.
    async fn catalog_head(&mut self) -> Option<Position> {
        binlog::current_position(&mut self.src).await.ok()
    }

    /// [`Version::catalog_head`] of `plan` in slot `idx`: that of the slot's latest plan or of
    /// its live version when it is one of those, else unknown.
    fn plan_head(&self, idx: usize, plan: &Arc<ViewPlan>) -> Option<Position> {
        let s = &self.slots[idx];
        if s.last_plan.as_ref().is_some_and(|p| Arc::ptr_eq(p, plan)) {
            return s.last_plan_head.clone();
        }
        s.live
            .as_ref()
            .filter(|l| Arc::ptr_eq(&l.plan, plan))
            .and_then(Version::plan_head)
    }

    /// Re-establishes the source-read connection after a connection-class error.
    async fn heal_src(&mut self) {
        match connect_source(&self.query_opts, self.cfg.lock_wait_s, &self.metrics).await {
            Ok(c) => {
                self.src = c;
                // The new connection probes the commit order again (and re-creates a dropped
                // fence table).
                self.commit_fence = None;
                self.commit_fenced = None;
                // and re-probes the consistent snapshot: a verdict made on the dead connection
                // must not survive the reconnect.
                self.snapshot_fence = None;
                info!("reconnected to source database");
            }
            Err(e) => warn!("source database still unreachable: {}", e.root_cause()),
        }
        self.metrics.beat();
    }

    /// Re-opens the metadata connection after a connection-class error. Nothing leadership-related
    /// is settled here any more (review A-06): the lock died with this connection only while it
    /// rode it, and it does not any more. The lock's own task holds and re-confirms it on its own
    /// session and fences this instance if another has taken it, and `meta::save_position` still
    /// refuses the position write of an instance whose epoch moved. `Ok(false)`: still
    /// unreachable, try again later.
    async fn heal_meta(&mut self) -> Result<bool> {
        let conn = connect_source(&self.meta_opts, self.cfg.lock_wait_s, &self.metrics).await;
        self.metrics.beat();
        match conn {
            Ok(c) => {
                self.meta = c;
                info!("metadata connection re-established");
                Ok(true)
            }
            Err(e) => {
                warn!("metadata connection still unreachable: {}", e.root_cause());
                Ok(false)
            }
        }
    }

    /// A failed metadata statement: connection-class errors heal the connection and are retried
    /// later; anything else (a changed epoch, a statement the server refuses) is fatal.
    async fn meta_failed(&mut self, e: anyhow::Error, what: &str) -> Result<()> {
        if !binlog::is_transient(&e) {
            return Err(e.context(what.to_string()));
        }
        // The leadership gate (ruling 1) covers the metadata connection too: while the lock cannot
        // be proven, nothing is re-issued through it after a reconnect — a verdict the server gives
        // us (a changed epoch, a refused statement) is still fatal above, and the new leader owns
        // these rows. Waiting for the next beat is what an uncertain one does instead.
        if self.leader_holds_writes() {
            warn!("{what}: {}; the leadership lock does not allow new writes, waiting for the next beat", e.root_cause());
            return Ok(());
        }
        warn!("{what}: {}; reconnecting", e.root_cause());
        self.heal_meta().await.map(|_| ())
    }

    /// lazyview exits because the views cannot be kept up to date from here (the saved binlog
    /// file is verifiably gone from the source): every state row says so (`error` with `why`), so
    /// whoever reads `_lazyview_overview` sees stale views instead of `ready` ones. Not
    /// resumable: the next start that can run rebuilds them (and writes their state again).
    async fn mark_unusable(&mut self, why: &str) {
        // The leadership gate (ruling 1): writing `error` over the state rows of this instance is
        // the new leader's job, so a gated instance — one that is replaced, or cannot prove its
        // lease — leaves the rows the replacement is about to own alone.
        if self.leader_holds_writes() {
            warn!("not recording the stop in _lazyview_state ({why}): the leadership lock does not allow this instance to write");
            return;
        }
        let reason = format!("stale: lazyview stopped: {why}");
        if let Err(e) = meta::mark_all_error(&mut self.meta, &reason).await {
            warn!("recording the stop in _lazyview_state: {e:#}");
        }
    }

    /// The whole view is in error (state `error` on every target, keeping the mode of its latest
    /// plan); it no longer holds back the saved position.
    async fn slot_error(&mut self, idx: usize, err: &anyhow::Error) {
        let msg = format!("{err:#}");
        error!(view = %self.slots[idx].name, "{msg}");
        let s = &mut self.slots[idx];
        s.error = Some(msg.clone());
        s.waiting = None;
        s.blocked = None;
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
        self.update_blocked(&name, 0).await;
    }

    /// The view is being built and a transient cause is delaying the swap of its new table (a
    /// reader holds the table's metadata lock, guard R1 on #254): state `building` with the
    /// reason in `last_error`, never `error` — that state tells an operator to act, and the
    /// stall alerts key on it. The retry that follows (`schedule_replan`) is what swaps it.
    async fn slot_building(&mut self, idx: usize, err: &anyhow::Error) {
        let msg = format!("{err:#}");
        warn!(view = %self.slots[idx].name, "{msg}");
        let (name, id, hash, targets, plan) = (
            self.slots[idx].name.clone(),
            self.slots[idx].def_id,
            self.slots[idx].hash.clone(),
            self.slots[idx].targets.clone(),
            self.slots[idx].last_plan.clone(),
        );
        for t in targets {
            self.set_state(
                &name,
                &t,
                id,
                &hash,
                plan.as_deref(),
                "building",
                Some(&msg),
            )
            .await;
        }
    }

    /// A chained view whose upstream lazyview table is not built yet: state `waiting` (not an
    /// error); it is planned when the upstream is swapped in (see [`Self::wake_waiting`]). While
    /// the upstream is in error (or blocked itself) the state is `error`, naming the upstream's
    /// error, and goes back to `waiting` when the upstream is built again.
    async fn slot_waiting(&mut self, idx: usize, upstream: &str) {
        let s = &mut self.slots[idx];
        if s.waiting.as_deref() != Some(upstream) {
            info!(view = %s.name, "waiting for upstream view `{upstream}` to be built");
        }
        s.waiting = Some(upstream.to_string());
        s.error = None;
        s.stale_since = None;
        s.blocked = None;
        self.show_waiting(idx, true, 0).await;
    }

    /// Writes the state of waiting view `idx`: `waiting`, or `error` while its upstream is in
    /// error. `force`: also when that did not change.
    async fn show_waiting(&mut self, idx: usize, force: bool, depth: usize) {
        let s = &self.slots[idx];
        let Some(upstream) = s.waiting.clone() else {
            return;
        };
        let blocked = self
            .slots
            .iter()
            .find(|o| o.def_id != s.def_id && o.name == upstream)
            .and_then(|o| o.error.as_deref().or(o.blocked.as_deref()))
            .map(|e| format!("upstream view `{upstream}` is in error: {e}"));
        if !force && blocked == s.blocked {
            return;
        }
        let (name, id, hash, targets) =
            (s.name.clone(), s.def_id, s.hash.clone(), s.targets.clone());
        if let Some(b) = &blocked {
            warn!(view = %name, "{b}");
        }
        self.slots[idx].blocked = blocked.clone();
        let (state, msg) = match blocked {
            Some(b) => ("error", b),
            None => ("waiting", WaitingFor(upstream).to_string()),
        };
        for t in targets {
            self.set_state(&name, &t, id, &hash, None, state, Some(&msg))
                .await;
        }
        self.update_blocked(&name, depth + 1).await;
    }

    /// Updates the views waiting for `upstream` after its error state changed (down the chain
    /// of views waiting for those).
    async fn update_blocked(&mut self, upstream: &str, depth: usize) {
        if depth > 16 {
            return;
        }
        let waiting: Vec<usize> = (0..self.slots.len())
            .filter(|&i| self.slots[i].waiting.as_deref() == Some(upstream))
            .collect();
        for i in waiting {
            Box::pin(self.show_waiting(i, false, depth)).await;
        }
    }

    /// Writes the state rows of current views that were deleted (by hand: lazyview deletes only
    /// those of removed tables and names) back as last written, so `_lazyview_overview` does not
    /// show NULL until a restart.
    async fn restore_state_rows(&mut self) -> Result<()> {
        // (ruling 1) A write path of the coordination store, so the same gate: rows deleted while
        // this instance cannot prove its lock are restored by whoever can.
        if self.leader_holds_writes() {
            return Ok(());
        }
        let present: HashSet<(String, String)> = meta::state_keys(&mut self.meta)
            .await?
            .into_iter()
            .collect();
        let missing: Vec<((String, String), ShownState)> = self
            .slots
            .iter()
            .flat_map(|s| s.targets.iter().map(|t| (s.name.clone(), t.clone())))
            .filter(|k| !present.contains(k))
            .filter_map(|k| self.shown.get(&k).cloned().map(|v| (k, v)))
            .collect();
        for ((name, target), v) in missing {
            info!(view = %name, %target, "state row was deleted; writing it again");
            meta::set_state(
                &mut self.meta,
                StateUpdate {
                    name: &name,
                    target: &target,
                    definition_id: v.definition_id,
                    hash: &v.hash,
                    mode: &v.mode,
                    detail: v.detail.as_deref(),
                    route: v.route.as_deref(),
                    access: v.access.as_deref(),
                    state: &v.state,
                    error: v.error.as_deref(),
                    managed: self.owned.contains_key(&(name.clone(), target.clone())),
                },
            )
            .await?;
        }
        Ok(())
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
        if e.downcast_ref::<PlanTimedOut>().is_some() {
            self.slots[idx].plan_timed_out = Some(self.slots[idx].hash.clone());
        }
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
            .parse_and_plan(
                &d.effective_definition(),
                &d.targets,
                Some(id),
                &HashMap::new(),
            )
            .await
        {
            Ok(p) => Arc::new(p),
            Err(e) => {
                self.plan_failed(idx, &e).await;
                return None;
            }
        };
        self.slots[idx].last_plan = Some(plan.clone());
        self.slots[idx].last_plan_head = self.catalog_head().await;
        let clash = match self.check_name(d, &plan).await {
            Err(e) => Some(e),
            Ok(()) if self.name_taken(idx, &plan.name) => Some(anyhow!(
                "another definition already materializes `{}`",
                plan.name
            )),
            Ok(()) => None,
        };
        if let Some(e) = clash {
            // The name belongs to another view (its state rows): this one shows as `#<id>`,
            // unless only the letter case differs.
            if self.slots[idx].live.is_none() && e.downcast_ref::<CaseClash>().is_none() {
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
            if d.rebuild {
                info!(view = %plan.name, "rebuild requested (_lazyview.rebuild = 1)");
                self.slots[idx].rebuild_asked = Some(self.boundary.to_position());
            } else if resume {
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
        // State rows without a table of definitions deleted or disabled while lazyview was
        // stopped (views in error, waiting or never built): nothing else removes them.
        if let Ok(rows) = meta::load_state(&mut self.meta).await {
            for ((name, t), r) in rows {
                if !r.managed && !used.contains(&(name.clone(), t.clone())) {
                    if let Err(e) = meta::delete_state(&mut self.meta, &name, &t).await {
                        warn!("deleting state row {name}@{t}: {e:#}");
                    }
                }
            }
        }
        // Tables marked as lazyview's in its own schema that no state row records (a crash
        // between creating or swapping a table and recording it).
        if self.cfg.drop_removed {
            let marked: Vec<String> = self
                .meta
                .exec(
                    "SELECT CAST(TABLE_NAME AS CHAR) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = ? AND TABLE_COMMENT LIKE ?",
                    (&self.schema, format!("{}%", target::MARKER_PREFIX)),
                )
                .await
                .unwrap_or_default();
            for table in marked {
                let base = target::base_name(&table).to_string();
                if used.contains(&(base, SOURCE_TARGET.to_string()))
                    || self
                        .owned
                        .contains_key(&(table.clone(), SOURCE_TARGET.to_string()))
                {
                    continue;
                }
                info!(table = %table, "removing a table lazyview created for a definition that no longer exists");
                if let Err(e) =
                    target::mysql::drop_table(&mut self.meta, &self.schema, &table, &self.metrics)
                        .await
                {
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
            // A definition that does not parse (any more) keeps the name recorded in
            // `_lazyview.name`, so its last good table stays (it is in error, not deleted).
            name: parsed
                .as_ref()
                .map(|p| p.name.clone())
                .or_else(|| d.name.clone())
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
            blocked: None,
            retry_at: None,
            down: BTreeMap::new(),
            last_plan: None,
            last_plan_head: None,
            refresh_failures: 0,
            replan_after: false,
            rebuild_asked: None,
            plan_timed_out: None,
            stats: Default::default(),
            applied_ts: 0,
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
    async fn target_conn(&mut self, t: &str) -> Result<&mut target::Conn> {
        let metrics = self.metrics.clone();
        // Every caller resolved `t` through the plan's alias resolution; the neighbours below
        // (and `prepare_refresh`, `apply_target`) already index the map directly.
        let target = self.targets.get_mut(t).unwrap();
        if target.conn.is_none() {
            if let Some(at) = target.retry_at.filter(|at| *at > Instant::now()) {
                bail!(
                    "target {t} is unreachable (next connection attempt in {:.1}s)",
                    (at - Instant::now()).as_secs_f64()
                );
            }
            let (rows, bytes) = (self.cfg.write_batch_rows, self.cfg.write_batch_bytes);
            let r = match tokio::time::timeout(TARGET_CONNECT_TIMEOUT, async {
                let mut c =
                    connect_target(&target.opts, target.kind, self.cfg.lock_wait_s, &metrics)
                        .await?;
                let w = match &mut c {
                    target::Conn::Mysql(c) => {
                        target::write_opts_with(c, target.kind, rows, bytes, &metrics).await?
                    }
                    target::Conn::Postgres(c) => c.write_opts(target.kind, rows, bytes).await?,
                };
                let mode = match &mut c {
                    target::Conn::Mysql(c) => session_sql_mode(c).await?,
                    // A PostgreSQL session has no sql_mode (its five SET statements are
                    // `target::postgres::session_setup`).
                    target::Conn::Postgres(_) => String::new(),
                };
                Ok::<_, anyhow::Error>((c, w, mode))
            })
            .await
            {
                Ok(r) => r.with_context(|| format!("connecting to target {t}")),
                Err(_) => Err(anyhow!(
                    "connecting to target {t}: timed out after {TARGET_CONNECT_TIMEOUT:?}"
                )),
            };
            metrics.beat();
            match r {
                Ok((c, w, mode)) => {
                    target.conn = Some(c);
                    target.write = Some(w);
                    target.sql_mode = Some(mode);
                    target.failures = 0;
                    target.retry_at = None;
                }
                Err(e) => {
                    target.failures += 1;
                    target.retry_at = Some(Instant::now() + connect_retry_delay(target.failures));
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
        let mut layout = mv::probe(&mut self.src, &self.schema, plan).await?;
        // The index budget of this view's tables: the tightest of the targets it has (T3.1).
        layout.key_budget = target::min_key_budget(
            targets
                .iter()
                .filter_map(|t| self.targets.get(t).map(|s| s.kind)),
        );
        let mut resumed = Vec::new();
        for t in candidates {
            let ts = self.targets[&t].schema.clone();
            let kind = self.targets[&t].kind;
            // Columns as the layout's, and the unique index rows are upserted by (tables
            // built before upserts lack `_lv_row`: rebuilt once).
            let fits = match self.target_conn(&t).await {
                Ok(conn) => match conn.table_columns(&ts, &name).await {
                    Ok(c) if target::layout_matches(kind, &layout, &c) => conn
                        .has_row_index(&ts, &name, &layout)
                        .await
                        .unwrap_or(false),
                    _ => false,
                },
                Err(_) => false,
            };
            if fits {
                resumed.push(t);
            }
        }
        if resumed.is_empty() {
            return Ok(targets);
        }
        // Seed the in-memory cascade cache on the source, and end the gap window at the position
        // that same consistent read includes: a parent replayed from before it is a gap (the seed
        // read saw its children already moved), one replayed from at or after it was seen by no
        // seed read and expands correctly from its own images. The position read is the one extra
        // statement of a resume; where the server reports no snapshot position the window keeps
        // the wider shape of the position read after the seed. (The Cascade arm of feed_rows
        // clears gap_until once the log passes it.)
        let (mut cache, seed_pos) = seed_cache_at_snapshot(&mut self.src, plan).await?;
        cache.gap_until = Some(match seed_pos {
            Some(p) => p,
            None => binlog::current_position(&mut self.src).await?,
        });
        let rest: Vec<String> = targets
            .iter()
            .filter(|t| !resumed.contains(t))
            .cloned()
            .collect();
        info!(view = %name, targets = %resumed.join(","), "resuming existing materialized table");
        // The state rows say `ready` already (as written by the run that built them).
        let (mode, detail) = mode_name(plan);
        let ap = crate::access::AccessPlan::of(plan);
        for t in &resumed {
            let shown = ShownState {
                definition_id: self.slots[idx].def_id,
                hash: hash.clone(),
                mode: mode.to_string(),
                detail: detail.clone(),
                route: Some(ap.route_name().to_string()),
                access: Some(ap.to_string()),
                state: "ready".to_string(),
                error: None,
            };
            self.shown.insert((name.clone(), t.clone()), shown);
        }
        let catalog_head = self.plan_head(idx, plan);
        self.slots[idx].live = Some(Version {
            plan: plan.clone(),
            layout,
            name,
            targets: resumed,
            dirty: ViewDirty {
                images: vec![KeySet::default(); plan.aliases.len()],
                cache,
                ..Default::default()
            },
            frozen: false,
            since: None,
            lag: BTreeMap::new(),
            behind: BTreeMap::new(),
            recent: Lookback::default(),
            catalog_head,
            table_heads: HashMap::new(),
        });
        Ok(rest)
    }

    /// Drops the shadow table of `name` on target `t` (best effort).
    async fn drop_shadow(&mut self, t: &str, name: &str) {
        let ts = self.targets[t].schema.clone();
        if let Ok(c) = self.target_conn(t).await {
            let _ = c
                .drop_table(&ts, &target::shadow_name(name), &Metrics::default())
                .await;
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
        let catalog_head = self.plan_head(idx, &plan);
        // A routine rebuild (full-mode refresh, unknown affected rows) of the unchanged live
        // version: its tables and state rows are ours, and its state stays `ready`.
        let routine = !partial
            && self.slots[idx].live.as_ref().is_some_and(|l| {
                Arc::ptr_eq(&l.plan, &plan) && l.name == name && l.targets == targets
            });
        // Side tables (`<name>__lv_new` / `__lv_old`) are only ever ours if a state row of this
        // definition exists for that name and target; the table itself if we created it.
        let state = if routine {
            HashMap::new()
        } else {
            meta::load_state(&mut self.meta).await.unwrap_or_default()
        };
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
                    kind: self.targets[t].kind,
                    schema: self.targets[t].schema.clone(),
                    marker: target::marker(&name, def_id),
                    check_side: !routine
                        && !state
                            .get(&key)
                            .map(|r| r.definition_id == def_id)
                            .unwrap_or(false),
                    check_table: !(self.owned.contains_key(&key) || live_has),
                }
            })
            .collect();
        self.generation += 1;
        let generation = self.generation;
        if !routine {
            for t in &targets {
                self.set_state(&name, t, def_id, &hash, Some(&plan), "building", None)
                    .await;
            }
        }
        // A full-mode view has nothing to apply to the shadow table: a rebuild, or the first
        // build of a view without a live version, swaps it in itself, keeping the DDL off the
        // binlog loop.
        let first = self.slots[idx].live.is_none() && !partial;
        let swap = (routine || first) && matches!(plan.mode, Mode::Full(_));
        // LAZYVIEW_TEST_BUILD_DELAY: a long build, for tests.
        let test_delay = self
            .cfg
            .test_build_delay
            .as_ref()
            .filter(|(marker, _)| self.slots[idx].definition.contains(marker.as_str()))
            .map(|(_, d)| *d);
        let task = tokio::spawn(build_task(
            self.query_opts.clone(),
            self.schema.clone(),
            specs,
            plan.clone(),
            name.clone(),
            (self.cfg.write_batch_rows, self.cfg.write_batch_bytes),
            self.cfg.lock_wait_s,
            self.cfg.swap_lock_wait_s,
            self.metrics.clone(),
            self.build_tx.clone(),
            self.build_slots.clone(),
            (def_id, generation),
            swap,
            test_delay,
        ));
        if routine {
            debug!(view = %name, "rebuilding");
        } else {
            info!(view = %name, targets = %targets.join(","), mode = %plan.mode, tracing = %plan.tracing(), "building");
            let ap = crate::access::AccessPlan::of(&plan);
            info!(view = %name, route = %ap.route_name(), fp = %ap.fingerprint, "access plan\n{ap}");
            for o in &ap.occurrences {
                if let crate::access::Path::Hop(h) = &o.path {
                    debug!(view = %name, alias = %o.alias, "hop {name}.{}: {}", o.alias, h.select);
                }
            }
        }
        Metrics::add(&self.metrics.full_rebuilds, 1);
        let reason = if partial {
            "build on a target that was down"
        } else if !routine {
            "new version: definition, targets or source schema changed, or rebuild requested"
        } else if matches!(plan.mode, Mode::Full(_)) {
            "full-mode view: the definition is not maintained incrementally"
        } else {
            "affected ids unknown (cascading foreign key, lookup overflow or untraced change)"
        };
        let replaces_live = self.slots[idx].live.is_some();
        self.slots[idx].stats.built(replaces_live, reason);
        self.slots[idx].last_full = Some(Instant::now());
        // Building again: views waiting for this one no longer show its error.
        let unblocked =
            self.slots[idx].error.take().is_some() | self.slots[idx].blocked.take().is_some();
        self.slots[idx].next = Some(Next {
            dirty: ViewDirty {
                images: vec![KeySet::default(); plan.aliases.len()],
                cache: if plan.cascades.iter().any(|c| c.map.is_some()) {
                    cascade::Cache::seeding()
                } else {
                    cascade::Cache::default()
                },
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
            lock_wait: HashSet::new(),
            started: self.boundary.to_position(),
            since: None,
            recent: Lookback::default(),
            routine,
            swapping: false,
            catalog_head,
        });
        self.rebuild_routes();
        if unblocked {
            let name = self.slots[idx].name.clone();
            self.update_blocked(&name, 0).await;
        }
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
                        if d.rebuild && s.rebuild_asked.is_none() {
                            info!(view = %s.name, "rebuild requested (_lazyview.rebuild = 1)");
                            self.slots[idx].rebuild_asked = Some(self.boundary.to_position());
                            // Also plans a definition again whose planning timed out.
                            self.slots[idx].plan_timed_out = None;
                            self.schedule_replan(id, false, false);
                        }
                        continue;
                    }
                    idx
                }
                None => self.new_slot(d),
            };
            let is_new = self.slots[idx].live.is_none() && self.slots[idx].next.is_none();
            let s = &mut self.slots[idx];
            if d.rebuild && s.rebuild_asked.is_none() {
                s.rebuild_asked = Some(self.boundary.to_position());
            }
            s.definition = d.effective_definition();
            s.hash = hash;
            s.targets = d.targets.clone();
            s.last_plan = None;
            s.last_plan_head = None;
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
            if let Err(e) = meta::delete_state(&mut self.meta, name, target).await {
                warn!("deleting state row {name}@{target}: {e:#}");
            }
            return;
        }
        if self.cfg.drop_removed {
            if let Some(t) = self.targets.get(target) {
                let ts = t.schema.clone();
                match self.target_conn(target).await {
                    Ok(c) => {
                        if let Err(e) = c.drop_table(&ts, name, &Metrics::default()).await {
                            warn!("dropping {name}@{target}: {e:#}");
                        }
                    }
                    Err(e) => warn!("dropping {name}@{target}: {e:#}"),
                }
            }
        }
        if let Err(e) = meta::delete_state(&mut self.meta, name, target).await {
            warn!("deleting state row {name}@{target}: {e:#}");
        }
    }

    fn rebuild_routes(&mut self) {
        // Executor (a): live versions on the Dedup route have a core target (src/exec.rs).
        let live: Vec<(u64, Arc<ViewPlan>)> = self
            .slots
            .iter()
            .filter_map(|s| {
                s.live
                    .as_ref()
                    .filter(|l| !l.frozen)
                    .map(|l| (s.def_id, l.plan.clone()))
            })
            .collect();
        let mut synced = self.dedup.sync(&live);
        for (id, why) in &synced.refused {
            let name = self
                .slots
                .iter()
                .find(|s| s.def_id == *id)
                .map(|s| s.name.clone())
                .unwrap_or_default();
            info!(
                view = %name,
                definition = id,
                "the dedup core refused the view's plan, it keeps the viewplan executor: {why}"
            );
        }
        // A refused definition whose target held the version's cascade cache comes back with it
        // (`sync`/`advance_replacements` took it out before `remove_target`): restore it into the
        // live version, so executor (b) continues with the real map - no rebuild (owner rule).
        self.restore_lost_caches(&mut synced);
        // A new or replaced core target runs with the live version's cascade cache (its rows
        // arrive via `Source`, its hop queries read the source with it): `replace_target`
        // keeps the OLD target's caches, so a re-plan (DDL, TRUNCATE, replan, every rebuild
        // and swap) needs the version's freshly built one moved in too. A cache that still
        // seeds keeps its target held (fed nothing) until its install/drain delivers it.
        self.move_core_caches(&synced);
        let mut routes: HashMap<TableRef, Vec<RouteT>> = HashMap::new();
        let mut stolen_cache: Vec<usize> = Vec::new();
        for (si, s) in self.slots.iter().enumerate() {
            let versions = [
                s.live
                    .as_ref()
                    .filter(|l| !l.frozen)
                    .map(|l| (false, &l.plan)),
                s.next.as_ref().map(|n| (true, &n.plan)),
            ];
            for (next, plan) in versions.into_iter().flatten() {
                let on_core = !next
                    && self.dedup.plan_matches(s.def_id, plan)
                    && !self.dedup.is_refused(s.def_id);
                if on_core {
                    // A live version on the core: its rows go to the core's consumer, not to a
                    // `RouteT` fan-out (a refused plan keeps its routes, draining entry or not).
                    continue;
                }
                if !next && !self.dedup.is_refused(s.def_id) && self.dedup.entry(s.def_id).is_some()
                {
                    // The executor takes the rows while the core still runs the old plan (a
                    // re-plan waits for the queue to drain): the version's cascade cache went
                    // into the target, so the view must rebuild (the rebuild reads the source
                    // and seeds the map afresh).
                    stolen_cache.push(si);
                }
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
                // (A traced cascade is found through the parent's alias route.)
                for (ci, c) in plan.cascades.iter().enumerate().filter(|(_, c)| !c.traced) {
                    routes
                        .entry((c.parent.schema.clone(), c.parent.name.clone()))
                        .or_default()
                        .push(RouteT {
                            slot: si,
                            next,
                            kind: RouteKind::Cascade(ci),
                        });
                }
                for (ci, c) in plan
                    .cascades
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.map.is_some())
                {
                    routes.entry(c.child.clone()).or_default().push(RouteT {
                        slot: si,
                        next,
                        kind: RouteKind::MapChild(ci),
                    });
                }
            }
        }
        for si in stolen_cache {
            if let Some(l) = self.slots[si].live.as_mut().filter(|l| !l.frozen) {
                l.dirty.rebuild = true;
                if l.since.is_none() {
                    let b = self.boundary.to_position();
                    l.since = Some(b);
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
            RouteKind::MapChild(c) => Some(plan.cascades[c].child_table.columns.len()),
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
        let current = self.current_columns(key, columns).await;
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

    /// New or replaced core targets (`synced`) run with the live version's cascade cache (their
    /// rows arrive via `Source`, their hop queries read the source with it): `replace_target`
    /// keeps the OLD target's caches, so the version's freshly built one moves in. A cache that
    /// still seeds holds its target (fed nothing) until its install/drain delivers it.
    /// B2 (owner rule: incremental views never fall back to a full rebuild): a refused
    /// definition's cascade cache comes back out of its removed core target
    /// (`Synced::lost_cache`, taken by `exec::Dedup::sync` / `advance_replacements` before
    /// `remove_target`) - restore it into the live version, so executor (b) keeps resolving
    /// cascaded and mapped child rows from the real map. Rows the core had already applied sit
    /// at or after `boundary` (a target that is behind holds it back, see `lag_floor`), so no
    /// replay floor is lowered for them; a version whose own cache is seeding keeps its
    /// in-flight seed (the build's `deliver_cache` moves the fresh one in).
    fn restore_lost_caches(&mut self, synced: &mut crate::exec::Synced) {
        for (id, cache) in synced.lost_cache.drain(..) {
            let Some(i) = self.idx_of(id) else {
                continue;
            };
            if let Some(l) = self.slots[i].live.as_mut().filter(|l| !l.frozen) {
                if !l.dirty.cache.is_seeding() {
                    l.dirty.cache = cache;
                }
            }
        }
    }

    fn move_core_caches(&mut self, synced: &crate::exec::Synced) {
        for id in self.dedup.waiting_cache() {
            let cache = self
                .slots
                .iter_mut()
                .find(|s| s.def_id == id)
                .and_then(|s| s.live.as_mut())
                .filter(|l| !l.frozen && !l.dirty.cache.is_seeding())
                .map(|l| std::mem::take(&mut l.dirty.cache));
            if let Some(cache) = cache {
                self.dedup.deliver_cache(id, cache);
            }
        }
        for id in synced.added.iter().chain(synced.replaced.iter()) {
            let slot = self.slots.iter_mut().find(|s| s.def_id == *id);
            let live = slot.and_then(|s| s.live.as_mut()).filter(|l| !l.frozen);
            if let Some(l) = live {
                // The core runs the new (patched) plan from here on: its source reads the live
                // column layout again, so (a) takes the view back (see `Dedup::plan_matches`) -
                // which means its `RouteT` must go (a route that outlives the mark would feed
                // the view twice), so `rebuild_routes` runs at this flush.
                if self.dedup.core_shape_stale(*id) {
                    self.core_shape_routes = true;
                }
                self.dedup.clear_shape_stale(*id);
                if l.dirty.cache.is_seeding() {
                    self.dedup.hold_for_cache(*id);
                } else if l.dirty.cache.routes.iter().any(|r| !r.rows.is_empty()) {
                    // Only a cache with rows goes over: a live version whose rows went to the
                    // core all along never fed its own map, and `replace_target` kept the old
                    // target's rows - taking an empty cache would erase them (the consumer then
                    // expands a parent's change to nothing).
                    let cache = std::mem::take(&mut l.dirty.cache);
                    self.dedup.set_cache(*id, cache);
                }
            }
        }
    }

    /// A build seeds a fresh cascade cache (`build_task`'s `seed_cache`, installed by
    /// [`Self::finish_next`]): a live version on the core must run its target with it, or the
    /// consumer expands cascades from a cache that no longer has the rows. The hold lifts as
    /// soon as the cache is in (the binlog loop feeds no rows in between).
    fn reseed_core_cache(&mut self, def_id: u64) {
        if !self.on_core(def_id) {
            return;
        }
        let seeding = self
            .slots
            .iter()
            .find(|s| s.def_id == def_id)
            .and_then(|s| s.live.as_ref())
            .map(|l| l.dirty.cache.is_seeding())
            .unwrap_or(true);
        if seeding {
            // The build has not finished seeding: the flush's `move_core_caches` delivers it
            // once it has (the target is fed nothing meanwhile).
            self.dedup.hold_for_cache(def_id);
            return;
        }
        let cache = self
            .slots
            .iter_mut()
            .find(|s| s.def_id == def_id)
            .and_then(|s| s.live.as_mut())
            .filter(|l| l.dirty.cache.routes.iter().any(|r| !r.rows.is_empty()))
            .map(|l| std::mem::take(&mut l.dirty.cache));
        if let Some(cache) = cache {
            self.dedup.hold_for_cache(def_id);
            self.dedup.deliver_cache(def_id, cache);
        }
    }

    /// The column count `key` really has now (the `current` of [`Self::check_shapes`], read
    /// once per (table, TABLE_MAP shape) until the next DDL clears the cache).
    async fn current_columns(&mut self, key: &TableRef, columns: u64) -> Option<usize> {
        match self.shapes.get(&(key.clone(), columns)) {
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
        }
    }

    // ---------------------------------------------------------------- streaming

    /// Test hook `LAZYVIEW_TEST_UNDECODABLE_ROWS=<n>` (review A-02): the first `n` rows events the
    /// reader delivers are handled as undecodable. A server gives no reason to fail to decode an
    /// event it has just written, so this is how the stuck reader is tested on real servers; the
    /// handling itself is the real one (`StreamEnd::Undecodable`), so the classification, the
    /// counters, the backoff ladder and the pinned position all run for real. Cost when unset,
    /// which is every production run: one `u32` load and a predictable branch per rows event; no
    /// allocation, no syscall. It keeps no state of its own — the count it compares against is
    /// the `events_undecodable` counter itself. Only the rows of tables a view reads count: the
    /// engine's own writes (state, definitions, the fence, the materialized views) come back on
    /// the same stream and would otherwise spend the budget at arbitrary points.
    fn test_undecodable(&self, item: &Item) -> bool {
        let n = self.cfg.test_undecodable_rows;
        if n == 0 {
            return false;
        }
        match item {
            Item::Rows { table_id, .. } => {
                let read = self
                    .table_ids
                    .get(table_id)
                    .is_some_and(|(_, route, src)| route.is_some() || src.is_some());
                read && Metrics::get(&self.metrics.events_undecodable) < u64::from(n)
            }
            _ => false,
        }
    }

    async fn stream(
        &mut self,
        start: Position,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut build_rx = self.build_rx.take().expect("stream runs once");
        let mut pos = start;
        // Not even the server's heartbeats for this long: the connection is dead (half-open).
        let silence = (self.cfg.heartbeat * 3).max(Duration::from_secs(15));
        // The event the reader is stuck on, and how many rounds in a row it has stopped there
        // (A-02): the position is what makes "log it once" and "grow the backoff" the same state.
        let mut stuck_at: Option<Position> = None;
        let mut stuck_streak: u32 = 0;
        'reconnect: loop {
            // The core's boundary is where reading starts: on the first connect the resume
            // position, on a reconnect the last boundary. Buffered ids then pin real positions,
            // never the empty one that `pending_floor` ignores.
            self.dedup.core.rewound(pos.clone());
            self.metrics.beat();
            // A connect is a repeat (the reader resumes from `boundary`, and delete-before-
            // insert tolerates re-applied events): the one helper with Policy::CONNECT, and no
            // nested retry inside BinlogReader::connect.
            let connected = supervisor::retrying(
                &self.metrics,
                "connecting the binlog stream",
                supervisor::Policy::CONNECT,
                || async {
                    tokio::time::timeout(
                        BINLOG_CONNECT_TIMEOUT,
                        BinlogReader::connect(
                            self.binlog_opts.clone(),
                            self.cfg.server_id,
                            self.flavor,
                            &pos,
                            self.cfg.heartbeat,
                        ),
                    )
                    .await
                    .unwrap_or_else(|_| Err(anyhow!("timed out after {BINLOG_CONNECT_TIMEOUT:?}")))
                },
            )
            .await;
            self.metrics.beat();
            let mut reader = match connected {
                Ok(r) => r,
                Err(e) => {
                    if Self::binlog_unusable(&e) {
                        self.on_missing_binlog(&e).await?;
                        pos = self.boundary.to_position();
                        continue 'reconnect;
                    }
                    if binlog::is_duplicate_server_id(&e) && self.reroll_server_id() {
                        continue 'reconnect;
                    }
                    warn!("binlog connect failed: {e:#}; retrying with the shared policy");
                    // Still unreachable after a whole policy run: wait one cap, shutdown-aware.
                    tokio::select! {
                        _ = tokio::time::sleep(supervisor::Policy::CONNECT.cap) => {}
                        _ = shutdown.changed() => if *shutdown.borrow() { break 'reconnect; },
                    }
                    continue 'reconnect;
                }
            };
            info!(position = %pos, "streaming binlog");
            // Freshness (review O-01, ruling 1): a connection knows nothing of the source's
            // clock until it asks, and has read nothing; the timestamps are per connection, so a
            // reconnect that re-reads from the boundary starts afresh instead of keeping a head
            // this connection never saw. It starts believed at the head (the first heartbeat
            // proves it either way within a heartbeat period) and ages from the connect
            // instant, never from the epoch.
            let head_ts = self.clock.now();
            self.clock = SourceClock {
                offset: self.clock.offset,
                reading: true,
                at_head: true,
                head_ts,
                ..Default::default()
            };
            self.refresh_source_clock().await;
            Metrics::set(&self.metrics.gauges.streaming, 1);
            Metrics::set(&self.metrics.gauges.starting, 0);
            let mut last_event = Instant::now();
            let mut delivered = false;
            // Why this round ends, once it breaks (A-02); `None` is a round that ended because the
            // process is shutting down or the source told the reader to start over.
            let mut end = None;
            // What the reader said when it ended the round on an undecodable event: the decoder's
            // own words, which tell the operator what to do about it (an unreadable compressed
            // event names the server switch to turn off). Carried to the one line below, which is
            // where a stream error is reported — logging it here too would print it every round.
            let mut end_detail: Option<String> = None;
            // Read while the last flush ran: processed first, in order (see `flush_draining`).
            let mut drained = Drained::new(self.cfg.mem.drain_max_bytes);
            loop {
                Metrics::set(
                    &self.metrics.gauges.heartbeat_unix,
                    crate::metrics::unix_now(),
                );
                let deadline = self.flush_deadline();
                let sleep_until =
                    deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
                let mut idle = false;
                let mut read = drained.pop();
                // Freshness: what the queue still holds is read but not yet handed to the views,
                // so the source is ahead of them until the flush applies it (see `SourceClock`).
                self.clock.pending_read = !drained.is_empty();
                if read.is_some() && *shutdown.borrow() {
                    break 'reconnect;
                }
                if read.is_none() {
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
                            read = Some(item);
                        }
                        // Polled last: fires only while the stream has nothing to deliver.
                        _ = tokio::time::sleep_until(sleep_until.into()), if deadline.is_some() && !self.in_txn => idle = true,
                        _ = tokio::time::sleep_until((last_event + silence).into()) => {
                            warn!("no binlog event or heartbeat for {silence:?}; reconnecting");
                            end = Some(StreamEnd::Silence);
                            break;
                        }
                    }
                }
                match read {
                    None => {}
                    Some(Ok(Some(item))) => {
                        if !delivered {
                            delivered = true;
                        }
                        Metrics::add(&self.metrics.events, 1);
                        if self.test_undecodable(&item) {
                            // Test hook: the same handling as a real mapping failure, so the
                            // stuck reader of A-02 is testable on a server.
                            end = Some(StreamEnd::Undecodable);
                            break;
                        }
                        if let Err(e) = self.on_item(&reader, item).await {
                            if source::is_undecodable(&e) {
                                // A-02, the owner's ruling: an event the decoder cannot map is a
                                // bug and the process exits rather than skip it, so this line is
                                // the whole record the operator needs — kind, table and position
                                // are in the message (`source::Undecodable`'s own words) — and the
                                // restart loop is what the alert of the observability plan reads.
                                Metrics::add(&self.metrics.events_undecodable, 1);
                                error!("binlog reader: {e:#}");
                            } else {
                                error!("processing binlog event: {e:#}");
                            }
                            return Err(e);
                        }
                    }
                    Some(Ok(None)) => {
                        warn!("binlog stream closed by server; reconnecting");
                        end = Some(StreamEnd::Closed);
                        break;
                    }
                    Some(Err(e)) => {
                        if Self::binlog_unusable(&e) {
                            self.on_missing_binlog(&e).await?;
                        } else if binlog::is_duplicate_server_id(&e) {
                            self.reroll_server_id();
                        } else if source::is_undecodable(&e) {
                            // A-02: not a network fault, so not the network's schedule. The
                            // reader stays on this event; the round-end report names the position
                            // once and the pause backs off like a failed connect. The liveness
                            // beat below stays: the process *is* alive, the counter is what shows
                            // the view is not advancing. The message itself goes to that report:
                            // a `warn!` here would repeat it every round of the backoff.
                            end = Some(StreamEnd::Undecodable);
                            end_detail = Some(format!("{e:#}"));
                        } else {
                            warn!("binlog stream error: {e:#}; reconnecting");
                            end = Some(StreamEnd::Transient);
                        }
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
                    self.flush_draining(&mut reader, &mut drained, &mut last_event)
                        .await?;
                    // Once per flush, never per event: building versions over their budget hold
                    // the reader until a build is applied.
                    if self.builds_over_budget().is_some()
                        && !self
                            .hold_for_builds(
                                &mut reader,
                                &mut drained,
                                &mut last_event,
                                &mut build_rx,
                                shutdown,
                            )
                            .await?
                    {
                        break 'reconnect;
                    }
                }
                if !self.in_txn {
                    self.housekeeping().await?;
                }
            }
            // Why the round ended (A-02): one counter per reason, so `/metrics` separates a
            // source that keeps cutting the stream from a reader stuck on one event; the stuck
            // position named once rather than once per retry; and the backoff for a reader that
            // would otherwise spin on one undecodable event. Counted per round, never per event.
            let streak = match end {
                Some(StreamEnd::Undecodable) => {
                    Metrics::add(&self.metrics.events_undecodable, 1);
                    Metrics::add(&self.metrics.stream_reconnect_undecodable, 1);
                    let at = reader.position().clone();
                    if stuck_at.as_ref() == Some(&at) {
                        stuck_streak = stuck_streak.saturating_add(1);
                    } else {
                        // Named once per position: another round at the same place says nothing
                        // new, and the counter is what grows. The decoder's own words ride along:
                        // the position alone does not tell an operator what to do, and what the
                        // refused event says (a server switch to turn off, a build to replace) is
                        // said once here instead of once per round of the backoff.
                        let total = Metrics::get(&self.metrics.stream_reconnect_undecodable);
                        let detail = end_detail
                            .as_deref()
                            .unwrap_or("the reader refused the event it read");
                        error!(
                            position = %at,
                            "binlog event cannot be decoded; the reader stays here and backs off \
                             like a failed connect ({total} reconnects at this position so far): \
                             {detail}"
                        );
                        stuck_at = Some(at);
                        stuck_streak = 1;
                    }
                    stuck_streak
                }
                Some(StreamEnd::Transient) => {
                    Metrics::add(&self.metrics.stream_reconnect_transient, 1);
                    0
                }
                Some(StreamEnd::Closed) => {
                    Metrics::add(&self.metrics.stream_reconnect_closed, 1);
                    0
                }
                Some(StreamEnd::Silence) => {
                    Metrics::add(&self.metrics.stream_reconnect_silence, 1);
                    0
                }
                None => 0,
            };
            if streak == 0 {
                // Any other round, or another event, starts the ladder again.
                stuck_at = None;
                stuck_streak = 0;
            }
            Metrics::set(&self.metrics.gauges.streaming, 0);
            // Nothing is being read until the next connection is up: a view keeps its last
            // applied timestamp, so its lag grows from here (review O-01) rather than sticking at
            // the last published value.
            self.clock.reading = false;
            self.clock.at_head = false;
            let _ = reader.close().await;
            // Changes of a partially read transaction are kept: re-applying them is harmless.
            // Its TABLE_MAPs and XA rows are read again from the boundary.
            self.in_txn = false;
            self.table_ids.clear();
            self.xa_current = None;
            self.txn_foreign = false;
            pos = self.boundary.to_position();
            // A stream that fails right after connecting backs off like a failed connect; a
            // reader stuck on one event follows the same ladder (A-02, see `stream_pause`).
            let pause = stream_pause(end, delivered, streak);
            self.metrics.beat();
            tokio::select! {
                _ = tokio::time::sleep(pause) => {}
                _ = shutdown.changed() => if *shutdown.borrow() { break 'reconnect; },
            }
        }
        if !self.in_txn {
            if let Err(e) = self.flush().await {
                debug!("final flush at shutdown: {e:#}");
            }
        }
        if let Some(sup) = &self.sup {
            sup.disconnect().await;
        }
        for s in &self.slots {
            if let Some(n) = &s.next {
                n.task.abort();
            }
        }
        // The loop is over: stop beating and close the lock session, so the named lock goes here
        // and now rather than making whoever starts the next instance wait for our lease.
        self.lock_task.stop();
        info!("stopped");
        Ok(())
    }

    /// [`Self::flush`] while the binlog keeps being read into `drained`, so the server keeps
    /// streaming during the flush: without reading, its socket fills and the stream resumes in
    /// small bursts on TCP's 200 ms timers, which can starve catch-up for minutes (CI run
    /// 39346546709). Reading stops at the queue's bounds (backpressure), after an error or the
    /// end of the stream, and after a ROTATE: queued rows decode with the table maps of their
    /// own file, which the reader keeps for one rotate. The loop processes the queue in order
    /// before it reads again, so positions, `pending_floor` and every other bookkeeping see the
    /// events exactly as if they had been read after the flush; rows are decoded in `on_item`
    /// then, as before (decode cost unchanged). A flush that starts while events are still
    /// queued reads only while the reader is in the file of the oldest of them: after a ROTATE it
    /// waits for the queue to drain, so no queued event is ever two rotates behind the reader.
    ///
    /// Cost per event that arrives while a flush runs: one more poll of the pending flush
    /// future (the biased `select!` polls it first on every wake), the size match and the
    /// length / byte / last-entry checks of [`Drained`], a binlog file-name compare (the rotate
    /// check), the push with an emptiness check (a copy of the file name when the queue was
    /// empty: once per drain) and, in the loop, the pop with its byte subtraction, an emptiness /
    /// capacity check and a `shutdown` watch read. The queue grows by amortized reallocation
    /// (doubling: up to the next power of two above [`DRAIN_MAX_EVENTS`] slots) and is shrunk to
    /// [`DRAIN_KEEP_SLOTS`] once empty. Events
    /// that arrive while no flush runs cost one pop of an empty queue and two `Option` tests.
    async fn flush_draining(
        &mut self,
        reader: &mut BinlogReader,
        drained: &mut Drained,
        last_event: &mut Instant,
    ) -> Result<()> {
        // The file the oldest queued events came from, else the reader's current one.
        let file = drained
            .file
            .clone()
            .unwrap_or_else(|| reader.position().file.clone());
        let (mut read_any, mut full) = (false, false);
        let metrics = self.metrics.clone();
        // Everything fed so far (the binlog read position) is what this flush applies: the
        // position the targets will have applied when it is through (#215, see below).
        let applied = self.boundary.to_position();
        let r = {
            let flush = self.flush();
            tokio::pin!(flush);
            loop {
                if !full && drained.full() {
                    full = true;
                    Metrics::add(&metrics.binlog_backpressure, 1);
                }
                let open = drained.open() && reader.position().file == file;
                tokio::select! {
                    biased;
                    r = &mut flush => {
                        if read_any {
                            Metrics::add(&metrics.flushes_overlapped, 1);
                        }
                        break r;
                    }
                    read = reader.next(), if open => {
                        *last_event = Instant::now();
                        // Freshness (review O-01): `read_any` — which marks the stream
                        // not-at-head and the queue as read-not-applied — counts only a real
                        // item. A heartbeat carries no change and is dropped from the queue;
                        // claiming it would knock a caught-up view off the zero branch for a
                        // heartbeat period. Cost: one discriminant test on the item the push
                        // below matches on anyway.
                        if !matches!(read, Ok(Some(Item::Heartbeat))) {
                            read_any = true;
                        }
                        drained.push(read, &file);
                    }
                }
            }
        };
        // A flush that outran its writes: while it ran the stream moved past what it applied by
        // more than the budget of the changes kept for targets that are behind — the targets
        // answered every write, yet demonstrably fall behind (a starved link; #215). Reported
        // and accounted as lagging, the same way a target that failed its writes is. Decided
        // from the applied position against the budget, never from error text. Cost (guard R3):
        // one position compare and one pass over the (view, target) pairs per flush; the event
        // path is untouched (the reader's position is read once, here, not per event).
        // The flush read past what the views were handed: those queued events are the source
        // being ahead of the views, so the stream is no longer at its head (the next heartbeat
        // after the queue drains says it is again) and the queued part counts as read-not-applied
        // until a flush applies it.
        if read_any {
            self.clock.pending_read = true;
            self.clock.at_head = false;
        }
        if r.is_ok() && read_any {
            self.mark_behind_targets(&applied, reader.position(), drained.bytes);
        }
        r
    }

    /// Flags the targets the flush just wrote as behind: the flush applied everything up to
    /// `applied`, while the stream was read up to `read` (events worth `drained_bytes` are
    /// queued behind that position). Past the `Config::mem.lag_hold_bytes` budget
    /// (`LAZYVIEW_TEST_LAG_HOLD_BYTES` in tests) every target that took this flush's writes is
    /// flagged in [`Version::behind`]: the next refresh skips it and hands it its changes
    /// through a `lag` entry (`note_refresh`), or the entry is made when the flag's retry time
    /// arrives (`retry_lag`) — either way the accounting from there is the one a target whose
    /// writes failed gets: kept keys, the budget warning, `retry_lag` catching it up and
    /// counting the catch-up. Nothing is rebuilt or dropped; a target that keeps up again
    /// simply never gets flagged (only a written target of a slow flush is).
    fn mark_behind_targets(&mut self, applied: &Position, read: &Position, drained_bytes: usize) {
        let budget = self.cfg.mem.lag_hold_bytes;
        // Bytes between the two positions: an offset difference in one binlog file; across a
        // rotation the size of the files between is unknown, so the queue alone decides.
        let behind = if read.file == applied.file {
            read.pos.saturating_sub(applied.pos) > budget as u64
        } else {
            drained_bytes > budget
        };
        if !behind {
            return;
        }
        let retry_at = Instant::now() + connect_retry_delay(1);
        for (i, targets) in &self.flush_wrote {
            let Some(l) = self.slots[*i].live.as_mut().filter(|l| !l.frozen) else {
                continue;
            };
            for t in targets {
                if l.lag.contains_key(t.as_str()) || l.behind.contains_key(t.as_str()) {
                    continue; // already behind through a failure or an earlier flush
                }
                l.behind.insert(t.clone(), retry_at);
                warn!(view = %l.name, target = %t,
                    "writes to this target are behind: the stream has moved {drained_bytes} \
                     bytes of changes past the position this flush applied, over the {budget} \
                     byte budget; the target is accounted as lagging until it catches up");
            }
        }
    }

    /// Holds the binlog reader while [`Self::builds_over_budget`]: a building version cannot take
    /// its buffered changes before its build is done, and dropping them would cost the view a
    /// rebuild after the swap (owner rule: never). Nothing is read meanwhile, so the server's
    /// socket fills and the stream waits; build results are taken, a due flush runs (applying a
    /// finished build's changes is what lifts the hold), and the loop keeps beating (`/livez`)
    /// and housekeeping. Called only after a flush, between transactions; counted once per hold
    /// in `binlog_backpressure`. `Ok(false)`: shutdown was requested meanwhile.
    async fn hold_for_builds(
        &mut self,
        reader: &mut BinlogReader,
        drained: &mut Drained,
        last_event: &mut Instant,
        build_rx: &mut mpsc::UnboundedReceiver<BuildDone>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        Metrics::add(&self.metrics.binlog_backpressure, 1);
        let held = self.builds_over_budget().unwrap_or(0);
        warn!(buffered_mib = held >> 20, "changes buffered for building views exceed their budget; binlog reading paused until a build is applied");
        let since = Instant::now();
        // Freshness (review O-01): while the reader is held, the source keeps writing changes
        // nobody has read, so the stream is not at its head and a view's lag grows from here —
        // which is the number the "paused reader" alert reads.
        self.clock.reading = false;
        while self.builds_over_budget().is_some() {
            let tick = Instant::now() + Duration::from_secs(1);
            let wake = self.flush_deadline().map_or(tick, |d| d.min(tick));
            tokio::select! {
                biased;
                _ = shutdown.changed() => if *shutdown.borrow() { return Ok(false); },
                Some(done) = build_rx.recv() => self.on_build_done(done),
                _ = tokio::time::sleep_until(wake.into()) => {}
            }
            self.metrics.beat();
            if self.flush_due() {
                self.flush_draining(reader, drained, last_event).await?;
            }
            self.housekeeping().await?;
        }
        self.clock.reading = true;
        info!(paused = ?since.elapsed(), "binlog reading resumed");
        // Not read meanwhile: the silence check starts afresh.
        *last_event = Instant::now();
        Ok(true)
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

    /// Whether the saved cursor (where the next start resumes) is verifiably no longer served
    /// by the source: a lost slot. A failed question counts as not lost.
    async fn saved_cursor_gone(&mut self) -> bool {
        let Ok(Some(saved)) =
            meta::load_position(&mut self.meta, &self.cfg.instance_id, self.flavor).await
        else {
            return false;
        };
        matches!(source::can_resume(&mut self.src, &saved).await, Ok(false))
    }

    /// The resume point the stream asked for is no longer served: a lost slot. On a source
    /// that drops them on its own ([`SourceCaps::cursor_purgeable`]) this is an expected event and
    /// `LAZYVIEW_ON_MISSING_BINLOG` decides what it costs; on one that does not, nothing policy-wise
    /// explains it and only a fail is honest.
    async fn on_missing_binlog(&mut self, e: &anyhow::Error) -> Result<()> {
        if !self.caps.cursor_purgeable {
            bail!(
                "resume point {} is no longer served by a source that does not lose them: {e:#}",
                self.boundary
            );
        }
        match self.cfg.on_missing_binlog {
            MissingBinlog::Fail => {
                let msg = format!(
                    "binlog position {} is no longer available: {e:#}",
                    self.boundary
                );
                // 1236 also carries dump errors that leave the position readable: only a saved
                // file that is verifiably gone marks the views; otherwise the next start resumes.
                if self.saved_cursor_gone().await {
                    self.mark_unusable(&msg).await;
                }
                Err(anyhow!(msg))
            }
            MissingBinlog::Rebuild => {
                warn!(
                    "binlog position {} purged; rebuilding all views",
                    self.boundary
                );
                self.boundary = Cursor::from_position(
                    self.flavor,
                    &binlog::current_position(&mut self.src).await?,
                );
                let ids: Vec<u64> = self.slots.iter().map(|s| s.def_id).collect();
                for id in ids {
                    self.schedule_replan(id, false, false);
                }
                Ok(())
            }
        }
    }

    async fn on_item(&mut self, reader: &BinlogReader, item: Item) -> Result<()> {
        // Freshness (review O-01): the head moves only on rows of a table a view reads (stamped
        // in the `Rows` arm below, where the routing lookup already happened): the views chase
        // changes to the tables they read, so everything else — the engine's own pen (its meta
        // tables, and the materialized tables it writes, which no view reads unless it is
        // chained and then routes) and the events that only carry shape or boundaries (query,
        // gtid, commit, XA) — is like the artificial events that open a stream and leaves the
        // head where the last change a view cares about put it. Stamping every item instead let
        // the engine's own per-flush `persist_position` put the head at "now": a view caught up
        // over a stale backlog read 0 (ruling 1 broken during a backlog), and an idle engine
        // flickered 1 s between its own write and the next heartbeat (acceptance criterion 1
        // broken). Only a heartbeat calls the stream at the head: the server sends one when its
        // dump has nothing left to send. Cost (owner rule): two integer stores, and only on
        // rows events of tables a view reads — strictly fewer than before, no new lookup.
        if matches!(item, Item::Heartbeat) {
            // The payload's position is where the server's dump stood; the stream is at its
            // head only if the views have applied to exactly there, outside a transaction (the
            // server drains its socket at once, so a backlog may still sit in the read buffer
            // while it heartbeats). Either way the heartbeat is the last moment the server was
            // known empty: a view with nothing read yet ages from here, not from the epoch.
            let head = reader.heartbeat_head();
            let applied = self.boundary.to_position();
            // The payload equality proves the head when the server's payload matches what the
            // views applied; the position equality does the same when the payload cannot be
            // trusted (some servers/flavors deliver the dump-empty heartbeat without a payload
            // position we can match): everything this connection has read is committed, the
            // queue is empty (the zero branch's `pending_read`), so nothing sits unread between
            // the applied position and the head the heartbeat just proved.
            self.clock.at_head = !self.in_txn
                && ((head != 0 && applied.pos == head && applied.file == reader.position().file)
                    || applied == *reader.position());
            self.clock.head_ts = self.clock.now();
        }
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
                // Our own tables: the commit fence's rows are not a foreign change, and no view
                // reads them.
                if !(key.0 == self.schema
                    && (key.1 == meta::STATUS || key.1 == meta::STATE || key.1 == mysql::table()))
                {
                    self.txn_foreign = true;
                }
                // The definitions table and the commit fence's table are ours: no view reads
                // them, both are remembered without a route.
                if key.0 == self.schema && (key.1 == meta::DEFS || key.1 == mysql::table()) {
                    self.table_ids.insert(table_id, (key, None, None));
                    return Ok(());
                }
                let route = match self.routes.get(&key).cloned() {
                    Some(r) => Some(self.check_shapes(&key, r, columns).await),
                    None => None,
                };
                // Core targets: this table as the core's consumer sees it; an occurrence
                // planned for another column count re-plans its view (as `check_shapes` does
                // for outdated plans on routes).
                let probe = self.dedup.core.source(&key, Some(columns as usize));
                let mut src = probe.clone();
                if let Some(s) = &probe {
                    let mut keep_a = false;
                    let mut delivered = false;
                    for t in self.dedup.core.mismatched(s) {
                        let Some(id) = self.dedup.def_of(t) else {
                            continue;
                        };
                        let p = self
                            .slots
                            .iter()
                            .find(|sl| sl.def_id == id)
                            .and_then(|sl| sl.live.as_ref())
                            .filter(|l| !l.frozen)
                            .map(|l| l.plan.clone());
                        let Some(p) = p else {
                            continue;
                        };
                        let want = p
                            .aliases
                            .iter()
                            .find(|a| a.table.schema == key.0 && a.table.name == key.1)
                            .map(|a| a.table.columns.len())
                            .or_else(|| {
                                p.cascades
                                    .iter()
                                    .find(|c| c.parent.schema == key.0 && c.parent.name == key.1)
                                    .map(|c| c.parent.columns.len())
                            })
                            .or_else(|| {
                                p.cascades
                                    .iter()
                                    .find(|c| c.child == key)
                                    .map(|c| c.child_table.columns.len())
                            })
                            .or_else(|| {
                                p.tables
                                    .iter()
                                    .position(|t| *t == key)
                                    .and_then(|ti| p.table_columns[ti].as_ref().map(Vec::len))
                            });
                        let current = self.current_columns(&key, columns).await;
                        if want.is_some_and(|n| Some(n) != current)
                            && !self.replan.contains_key(&id)
                        {
                            warn!(table = %key.1, "table definition differs from the view's plan; re-planning");
                            self.schedule_replan(id, true, false);
                        }
                        if self.dedup.replacing(id) {
                            let prefix = self.core_hold_prefix(id);
                            if prefix {
                                // A held equal-shape replace whose registered occurrences,
                                // cascades and maps all read a positional prefix of the live
                                // layout (columns added or dropped outside the reads): the old
                                // registered reads still land on the same columns of this map's
                                // rows, so (a) keeps the view - fed through a source with no
                                // column-count skip (`None`: any shape).
                                keep_a = true;
                            } else if self.in_txn {
                                // Mid-transaction the queue may hold this transaction's own
                                // items: only a fenced flush may apply them. The (b) cache
                                // takes the view over until the shape settles, and it is
                                // rebuilt from the source.
                                if self.dedup.mark_shape_stale(id) {
                                    self.core_shape_routes = true;
                                    if let Some(l) = self
                                        .slots
                                        .iter_mut()
                                        .find(|sl| sl.def_id == id)
                                        .and_then(|sl| sl.live.as_mut())
                                        .filter(|l| !l.frozen)
                                    {
                                        l.dirty.rebuild = true;
                                        if l.since.is_none() {
                                            let b = self.boundary.to_position();
                                            l.since = Some(b);
                                        }
                                    }
                                }
                            } else {
                                // The registered reads would index moved columns: this map's
                                // rows must not go to the core until the replace is delivered.
                                // The queue holds only rows fed before the DDL - applicable
                                // under the old plan - so run the jobs and deliver now (the
                                // drain loop feeds no rows meanwhile), then this map's rows
                                // flow to the core under the new registration. Nothing is
                                // lost, so no rebuild.
                                self.flush_dedup().await;
                                let mut synced = self.dedup.advance_replacements();
                                self.restore_lost_caches(&mut synced);
                                self.move_core_caches(&synced);
                                delivered = !synced.replaced.is_empty();
                                if self.dedup.replacing(id) {
                                    // Jobs in retry hold keep the queue: the rows would skip
                                    // both writers - (b) takes the view over and it is rebuilt
                                    // from the source.
                                    if self.dedup.mark_shape_stale(id) {
                                        self.core_shape_routes = true;
                                        if let Some(l) = self
                                            .slots
                                            .iter_mut()
                                            .find(|sl| sl.def_id == id)
                                            .and_then(|sl| sl.live.as_mut())
                                            .filter(|l| !l.frozen)
                                        {
                                            l.dirty.rebuild = true;
                                            if l.since.is_none() {
                                                let b = self.boundary.to_position();
                                                l.since = Some(b);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if keep_a {
                        src = self.dedup.core.source(&key, None);
                    } else if delivered {
                        // The replace landed: this map's count is the registered one now.
                        src = self.dedup.core.source(&key, Some(columns as usize));
                    }
                }
                self.table_ids.insert(table_id, (key, route, src));
            }
            Item::Rows {
                table_id,
                kind,
                event,
            } => {
                self.in_txn = true;
                let Some((key, route, src)) = self.table_ids.get(&table_id).cloned() else {
                    return Ok(());
                };
                if key.0 == self.schema && key.1 == meta::DEFS {
                    self.defs_dirty = true;
                    self.first_dirty.get_or_insert_with(Instant::now);
                    return Ok(());
                }
                if key.0 == self.schema && key.1 == meta::STATE {
                    // A state row deleted by hand is written back (see `restore_state_rows`).
                    if kind == binlog::RowsKind::Delete {
                        self.state_check = true;
                        self.first_dirty.get_or_insert_with(Instant::now);
                    }
                    return Ok(());
                }
                let own =
                    key.0 == self.schema && (key.1 == meta::STATUS || key.1 == mysql::table());
                // The rows of a table no view reads are kept only while one prepared
                // transaction's budget of them lasts (`Config::mem.xa_hold_bytes`, a share of
                // the total of [`crate::mem`], 256 MiB where there is no container limit): past
                // it they are dropped, and only a view defined while the transaction was
                // prepared and reads such a table is then rebuilt. The rows of a table a view
                // does read are always kept: they are the transaction's own changes, and
                // dropping them would cost every live view a rebuild.
                let keep_unrouted = !own
                    && self
                        .xa_current
                        .as_ref()
                        .is_some_and(|(_, xa)| xa.bytes <= self.cfg.mem.xa_hold_bytes);
                if route.is_none() && src.is_none() {
                    // No view reads it now; one defined while this transaction is prepared may.
                    // Keep its rows so `xa_committed` can feed that view instead of rebuilding it
                    // (owner rule), up to one prepared transaction's budget of kept rows.
                    if let Some((_, xa)) = self.xa_current.as_mut() {
                        if keep_unrouted {
                            let rows = reader.rows(table_id, &event)?;
                            xa.bytes += rows.len() * KEY_BYTES;
                            xa.rows
                                .extend(rows.into_iter().map(|(b, a)| (key.clone(), b, a)));
                        } else {
                            xa.other_tables.insert(key);
                        }
                    }
                    return Ok(());
                }
                // Freshness (review O-01, ruling 1): rows of a table a view reads — as a route
                // or as the core's consumer, the two `None` arms above returned every other
                // table. This is the newest change handed to the views and (once) the oldest
                // this connection has read; the non-zero branch of `lag` ages both against the
                // source's clock, and only a heartbeat may call the stream at the head again.
                let ts = u64::from(event.header().timestamp());
                if ts != 0 {
                    if self.clock.fed_ts == 0 {
                        self.clock.gap_ts = ts;
                    }
                    self.clock.fed_ts = ts;
                }
                self.clock.at_head = false;
                let since = self.boundary.to_position();
                if let Some(route) = &route {
                    self.mark_any(route, &since);
                }
                let need_rows = self.xa_current.is_some()
                    || src.is_some()
                    || route
                        .as_ref()
                        .is_some_and(|rt| rt.iter().any(|r| r.kind != RouteKind::Any));
                if !need_rows {
                    return Ok(());
                }
                let rows = reader.rows(table_id, &event)?;
                if let Some(route) = &route {
                    self.feed_rows(route, &rows, &since);
                }
                if let Some(s) = &src {
                    self.feed_core(s, &rows, None, route.is_none());
                }
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
                        bytes: 0,
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
                    Some(xa) if commit => self.xa_committed(&xid, xa),
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
                self.note_statement_dml(&schema, &query, &pos);
                let before = self.end_txn(pos);
                self.txn_foreign = false;
                self.persist_needed = true;
                let effects = crate::ddl::statement_effects(&query, &schema);
                let tables = effects.tables;
                if !tables.is_empty() {
                    tracing::trace!(%query, "statement affects tables {tables:?}");
                    self.on_tables_changed(&tables, effects.rows_kept, &before)
                        .await;
                    if first_keyword(&query).eq_ignore_ascii_case("TRUNCATE") {
                        self.on_view_table_lost(&tables, &before, "truncated");
                    } else if !effects.dropped.is_empty() {
                        // Only removed tables: `DROP INDEX … ON t` names `t` but leaves it.
                        self.on_view_table_lost(&effects.dropped, &before, "dropped");
                    }
                }
                self.after_boundary();
            }
            Item::Heartbeat => {
                // The server sends heartbeats only while it has nothing else to send. Between
                // transactions, one moves the boundary to the reader's position: on an idle
                // source the applied and saved position would stay in the previous file after a
                // binlog rotation (also a server restart). Pending changes still hold the saved
                // position back (`pending_floor`). Heartbeats are never queued (`Drained::push`),
                // so the reader has read nothing past this one.
                let p = reader.position();
                if !self.in_txn
                    && self.xa_current.is_none()
                    && Cursor::from_position(self.flavor, p) > self.boundary
                {
                    self.boundary = Cursor::from_position(self.flavor, p);
                    self.dedup.boundary(p, Instant::now());
                    self.persist_needed = true;
                    self.after_boundary();
                }
            }
            Item::Incident { pos } => {
                warn!(at = %pos, "INCIDENT event in the binlog: the source reports possibly lost changes");
            }
            Item::Other => {}
        }
        Ok(())
    }

    /// Review C-03: with `binlog_format=ROW` a QUERY event carries DDL and administrative
    /// statements only; a DML statement here was logged in statement format, because the
    /// executing session twisted its `binlog_format` (not our problem that server or user does
    /// twisting of session — owner). Its rows are not in the stream, so the views reading the
    /// table stay wrong and `ready` until they are rebuilt: the WARN is what tells the operator.
    /// One line per table per [`STMT_DML_LOG_INTERVAL`] with the count of the rest, and never
    /// the statement text — in statement format it carries the row values (F2 S-08). Row events
    /// never reach the caller's arm, so they pay nothing for this check; the rare statement
    /// event pays a keyword scan and a map entry.
    fn note_statement_dml(&mut self, schema: &str, query: &str, pos: &Position) {
        let kw = first_keyword(query);
        if !["INSERT", "REPLACE", "UPDATE", "DELETE"]
            .iter()
            .any(|k| kw.eq_ignore_ascii_case(k))
        {
            return;
        }
        let kind = kw.to_ascii_uppercase();
        let table = statement_target_table(query).unwrap_or_else(|| "*".to_owned());
        Metrics::add(&self.metrics.statement_events_ignored, 1);
        let now = Instant::now();
        let e = self
            .stmt_dml_last
            .entry((schema.to_owned(), table))
            .or_insert((None, 0));
        let Some(count) = stmt_dml_gate(e, now) else {
            return; // folded into the next line for this table
        };
        warn!(
            kind = %kind,
            schema = %schema,
            at = %pos,
            count,
            "DML logged in statement format: its rows never reach the stream, the views \
             reading the table stay stale until they are rebuilt"
        );
    }

    /// A transaction boundary at `pos`; returns the previous one. TABLE_MAP ids are re-sent
    /// for every transaction, so they are forgotten here.
    fn end_txn(&mut self, pos: Position) -> Position {
        self.in_txn = false;
        self.table_ids.clear();
        self.dedup.boundary(&pos, Instant::now());
        std::mem::replace(&mut self.boundary, Cursor::from_position(self.flavor, &pos))
            .to_position()
    }

    /// XA COMMIT of a transaction read in this run: its rows become visible now.
    fn xa_committed(&mut self, xid: &str, xa: XaTxn) {
        let mut by_table: Vec<(TableRef, Vec<(Option<BinlogRow>, Option<BinlogRow>)>)> = Vec::new();
        for (t, b, a) in xa.rows {
            match by_table.last_mut() {
                Some((lt, v)) if *lt == t => v.push((b, a)),
                _ => by_table.push((t, vec![(b, a)])),
            }
        }
        for (t, rows) in by_table {
            if let Some(route) = self.routes.get(&t).cloned() {
                self.mark_any(&route, &xa.start.to_position());
                self.feed_rows(&route, &rows, &xa.start.to_position());
            }
            if let Some(s) = self.dedup.core.source(&t, None) {
                self.feed_core(
                    &s,
                    &rows,
                    Some(&xa.start.to_position()),
                    !self.routes.contains_key(&t),
                );
            }
        }
        // Only the tables whose rows were dropped (past `Config::mem.xa_hold_bytes`) still cost a view a
        // rebuild: say so, it is the one case this run could not trace the change.
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
                    warn!(view = %s.name, xid, "a view defined while this XA transaction was prepared reads a table whose rows were dropped; rebuilding it");
                    d.rebuild = true;
                    d.all = true;
                    since.get_or_insert_with(|| xa.start.to_position());
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
            n(&|s| s.error.is_some() || s.blocked.is_some()),
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

    /// Asks the source its own time (`SELECT UNIX_TIMESTAMP()`), once, and keeps the difference
    /// to the local clock (review O-01, ruling 1): a view's lag is then source clock minus
    /// source clock, so an engine whose host clock is off by minutes still reports 0 for a view
    /// that is caught up, and a view that is behind shows the real distance. One statement per
    /// (re)connect and per housekeeping tick — never per event, and no new write anywhere. A
    /// failed question keeps the last offset: the figures ride the previous measurement until
    /// the next tick, which is what an idle source (lag 0 by construction) and a brief source
    /// hiccup both allow.
    async fn refresh_source_clock(&mut self) {
        match self
            .src
            .query_first::<i64, _>("SELECT UNIX_TIMESTAMP()")
            .await
        {
            Ok(Some(src_now)) if src_now > 0 => {
                self.clock.offset = src_now - crate::metrics::unix_now() as i64;
                self.last_clock = Instant::now();
            }
            Ok(_) => {
                debug!("SELECT UNIX_TIMESTAMP() gave no answer; keeping the source clock offset")
            }
            Err(e) => {
                debug!("asking the source its clock failed: {e:#}; keeping the source clock offset")
            }
        }
    }

    /// Periodic one-line activity summary and the keepalives of the connections the loop owns.
    /// Leadership is not checked here any more (review A-06): the lock's own task beats its own
    /// session and this reads, for free, what that task published.
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
        // Freshness (review O-01): re-measure the source's clock on this cadence (one statement
        // per tick, next to the ping above, never per event), and hand the per-view counters to
        // the aggregator even when no flush has run — a view that stops advancing is exactly the
        // case that has nothing to flush, and its lag has to move (ruling 1: a held reader and a
        // stuck view show a growing number, not a stale-but-green one).
        if self.last_clock.elapsed() >= Duration::from_secs(30) {
            self.refresh_source_clock().await;
        }
        if self.last_publish.elapsed() >= Duration::from_secs(STATS_PUBLISH_INTERVAL) {
            self.last_publish = Instant::now();
            self.publish_stats();
        }
        // The leadership lock is no longer checked from here (review A-06): the lock's own task
        // beats its own session and publishes what it found, and this is the cheap read of that.
        // Once per housekeeping tick, never a round-trip on the metadata connection this loop
        // shares with the catalog and the position writes — and a replaced instance now learns it
        // within one beat instead of one minute, whether or not the metadata connection answers.
        if let LeaderState::Fenced(why) = &*self.leader_state.borrow() {
            bail!("fenced: {why}");
        }
        // The changes kept for targets that are behind are bounded: past the budget the engine
        // only reports (`lag_over_budget`) — nothing is dropped and nothing is rebuilt, the
        // saved position waits for the kept keys instead (`pending_floor`).
        match self.lag_over_budget() {
            Some(held) if !self.lag_budget_warned => self.report_lag_over_budget(held),
            Some(_) => {}
            None => self.lag_budget_warned = false,
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
        // A live version whose cascade cache went into a core target it does not run (a re-plan
        // `sync` holds back, or a refusal still draining, see `rebuild_routes`): its kept child
        // rows are gone, so a cascaded or mapped child row cannot be resolved from the parent -
        // such a change marks the view for a rebuild instead (the rebuild reads the source and
        // seeds the map afresh) rather than being applied blind.
        let blind: Vec<bool> = route
            .iter()
            .map(|r| {
                matches!(r.kind, RouteKind::Cascade(_) | RouteKind::MapChild(_))
                    && self.cache_with_core(r.slot, r.next)
            })
            .collect();
        for (before, after) in rows {
            seen += 1;
            for (ri, r) in route.iter().enumerate() {
                let Some((plan, dirty, ver_since)) = self.version_of(*r) else {
                    continue;
                };
                let fed = match r.kind {
                    RouteKind::Alias(a) => feed(plan, a, dirty, before.as_ref(), after.as_ref()),
                    RouteKind::Cascade(c) => {
                        if let Some(gap_until) = dirty.cache.gap_until.clone() {
                            let gap = since < &gap_until;
                            dirty.cache.gap = gap;
                            if !gap {
                                dirty.cache.gap_until = None;
                            }
                        }
                        if !blind[ri] {
                            feed_cascade(plan, c, dirty, before.as_ref(), after.as_ref())
                        } else {
                            dirty.rebuild = true;
                            ver_since.get_or_insert_with(|| since.clone());
                            Fed::Recorded
                        }
                    }
                    RouteKind::MapChild(c) => {
                        if !blind[ri] {
                            feed_map(plan, c, dirty, before.as_ref(), after.as_ref())
                        } else {
                            dirty.rebuild = true;
                            ver_since.get_or_insert_with(|| since.clone());
                            Fed::Recorded
                        }
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
                dirty.tally.row(fed == Fed::Skipped);
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

    /// Hot path, executor (a): the rows of a source table to the dedup core, whose consumer replaces the
    /// `RouteT` fan-out of live Dedup-route versions (same per-row work, no image kept). `origin`: the replay
    /// position of rows kept until now (an XA transaction's, at its XA COMMIT); `count`: no `RouteT` counted
    /// them in `rows_seen`.
    fn feed_core(
        &mut self,
        s: &crate::dedup::Source,
        rows: &[(Option<BinlogRow>, Option<BinlogRow>)],
        origin: Option<&Position>,
        count: bool,
    ) {
        for (b, a) in rows {
            match origin {
                Some(o) => self.dedup.core.row_at(o, s, b.as_ref(), a.as_ref()),
                None => self.dedup.core.row(s, b.as_ref(), a.as_ref()),
            }
        }
        if count {
            Metrics::add(&self.metrics.rows_seen, rows.len() as u64);
        }
        self.dedup.note_rows(rows.len());
        self.first_dirty.get_or_insert_with(Instant::now);
        self.core_events();
    }

    /// Exactly one authority writes a view: executor (a) while its live version runs with the
    /// plan its core target was registered under - including while a replace of equal shape
    /// waits for the queue to drain (`Dedup::plan_matches`: the running target reads the same
    /// way, unless its registered source reads another column layout, which drops the rows it
    /// skips). A refused plan, and a refused target still draining, leave the view to executor
    /// (b), which has their `RouteT`s.
    /// (b) hot path: `cache_with_core` asks this once per row event for each cascaded/mapped
    /// route, so the def's slot goes through the existing def→slot index (`idx_of`).
    fn on_core(&self, def_id: u64) -> bool {
        let Some(i) = self.idx_of(def_id) else {
            return false;
        };
        match self.slots[i].live.as_ref().filter(|l| !l.frozen) {
            Some(l) => !self.dedup.is_refused(def_id) && self.dedup.plan_matches(def_id, &l.plan),
            None => false,
        }
    }

    /// Column equality (`catalog::Column` carries no `PartialEq`): every field, so a
    /// same-name-same-place column with another type or nullability is not "the same".
    fn same_column(x: &crate::catalog::Column, y: &crate::catalog::Column) -> bool {
        x.name == y.name
            && x.kind == y.kind
            && x.param == y.param
            && x.exact == y.exact
            && x.invisible == y.invisible
            && x.data_type == y.data_type
            && x.column_type == y.column_type
            && x.charset == y.charset
            && x.collation == y.collation
            && x.octet_len == y.octet_len
            && x.nullable == y.nullable
            && match (&x.generated, &y.generated) {
                (None, None) => true,
                (Some(a), Some(b)) => a.expr == b.expr && a.stored == b.stored && a.deps == b.deps,
                _ => false,
            }
    }

    /// A held equal-shape replace (`Dedup::replacing`) may keep executor (a) while the table
    /// already logs another column count: true when every shape the core registered for
    /// `def_id` - each alias table, cascade parent and map child - is a positional prefix of
    /// the live plan's shape of the same table (an ALTER added columns after the reads, or
    /// dropped unread ones), so the registered positional reads still name the same columns
    /// of the new layout and the rows may be fed without the consumer's column-count skip.
    /// Anything else (a column moved or dropped inside a read) returns false: the caller
    /// marks the version shape-stale, executor (b) takes the rows and the view is rebuilt.
    fn core_hold_prefix(&self, def_id: u64) -> bool {
        let (Some(e), Some(l)) = (
            self.dedup.entry(def_id),
            self.slots
                .iter()
                .find(|s| s.def_id == def_id)
                .and_then(|s| s.live.as_ref())
                .filter(|l| !l.frozen),
        ) else {
            return false;
        };
        if e.plan.table_columns != l.plan.table_columns
            || e.plan.aliases.len() != l.plan.aliases.len()
            || e.plan.cascades.len() != l.plan.cascades.len()
        {
            return false;
        }
        let prefix = |a: &[crate::catalog::Column], b: &[crate::catalog::Column]| -> bool {
            a.len() <= b.len() && a.iter().zip(b).all(|(x, y)| Self::same_column(x, y))
        };
        e.plan
            .aliases
            .iter()
            .zip(&l.plan.aliases)
            .all(|(x, y)| prefix(&x.table.columns, &y.table.columns))
            && e.plan.cascades.iter().zip(&l.plan.cascades).all(|(x, y)| {
                x.parent.schema == y.parent.schema
                    && x.parent.name == y.parent.name
                    && prefix(&x.parent.columns, &y.parent.columns)
                    && x.child_table.schema == y.child_table.schema
                    && x.child_table.name == y.child_table.name
                    && prefix(&x.child_table.columns, &y.child_table.columns)
            })
    }

    /// `true`: the live version of `slot` does not run the core target that holds its cascade
    /// cache (a refusal still drains the target), so executor (b) must not resolve cascaded or
    /// mapped child rows from a cache it no longer has.
    fn cache_with_core(&self, slot: usize, next: bool) -> bool {
        if next {
            return false;
        }
        let Some(s) = self.slots.get(slot) else {
            return false;
        };
        match s.live.as_ref().filter(|l| !l.frozen) {
            Some(_) => self.dedup.entry(s.def_id).is_some() && !self.on_core(s.def_id),
            None => false,
        }
    }

    /// What the dedup core reported: changes with unknown ids take the documented safety net of their
    /// version (a rebuild, warned at most once a minute, guard R1 c). A cascade-cache gap cannot
    /// be reported yet: no cascade plan reaches the core (D-6T-cascades), the host's
    /// `CoreEvent::CascadeGap` handling lives in `tmp/out/lvs-step6/6t/cg-core-partial.patch`
    /// until cascade-gap-core moves map cascades onto the core.
    fn core_events(&mut self) {
        for ev in self.dedup.core.events() {
            // lazyview does not stage XA in the core (F7); other events have no host action.
            if let crate::dedup::CoreEvent::Unknown { target, why } = ev {
                let Some(i) = self.dedup.def_of(target).and_then(|id| self.idx_of(id)) else {
                    continue;
                };
                if !self.on_core(self.slots[i].def_id) {
                    // (b) is the authority: its own apply path reports unknown rows.
                    continue;
                }
                let Some(name) = self
                    .slots
                    .get(i)
                    .filter(|s| s.live.as_ref().is_some_and(|l| !l.frozen))
                    .map(|s| s.name.clone())
                else {
                    continue;
                };
                if let Some(l) = self.slots[i].live.as_mut().filter(|l| !l.frozen) {
                    l.dirty.rebuild = true;
                    if l.since.is_none() {
                        let b = self.boundary.to_position();
                        l.since = Some(b);
                    }
                }
                crate::accumulate::warn_throttled(
                    &LAST_UNKNOWN,
                    || warn!(view = %name, why, "ids unknown, rebuilding the view"),
                );
            }
        }
    }

    /// Oldest replay position of a change not yet applied to a maintained table: a restart from
    /// here re-reads everything still pending. Views in error do not hold it back (they are
    /// rebuilt after a restart). A target that is behind does, whether or not it was reported
    /// down: the keys kept for it live only in memory, so the saved position waits for them and
    /// a restart replays the outage into them (see [`lag_floor`]). With `held`, also the oldest
    /// change whose images are remembered for later lookups (applied, but a restart must read
    /// them again to have them; see [`Lookback`]).
    fn pending_floor(&self, held: bool) -> Position {
        let mut floor = self.boundary.to_position();
        for s in self.slots.iter().filter(|s| s.error.is_none()) {
            lower_pos(&mut floor, s.stale_since.as_ref());
            if let Some(l) = s.live.as_ref().filter(|l| !l.frozen) {
                lower_pos(&mut floor, l.since.as_ref());
                let pin = self.dedup.pinned(s.def_id);
                lower_pos(&mut floor, pin.as_ref());
                if held {
                    lower_pos(&mut floor, l.recent.floor());
                }
                lag_floor(&mut floor, &l.lag);
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
        let read = self.boundary.to_position();
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

    async fn on_tables_changed(&mut self, tables: &[TableRef], rows_kept: bool, since: &Position) {
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
                            o.def_id != s.def_id && Self::slot_uses(o, target::base_name(&t.1))
                        })
                });
                todo.push((s.def_id, !upstream));
            }
        }
        for (id, freeze) in todo {
            // A schema change that keeps every row: the same reads swap in place. Anything else
            // (a table swapped in or rows removed) rebuilds online.
            if freeze && rows_kept && self.swap_in_place(id, since, tables).await {
                continue;
            }
            let from = self.stale_from(id, since, tables);
            self.schedule_replan_at(id, freeze, false, &from);
        }
        if !self.replan.is_empty() || self.defs_dirty {
            self.first_dirty.get_or_insert_with(Instant::now);
        }
    }

    /// A source DDL at `at` changed `tables`, some of which the live version of definition `id`
    /// reads: re-plan it and, when the new plan reads the same columns, keys and SQL (see
    /// [`plan_swap::same_reads`]), put it in place of the live plan. Only the column positions
    /// the rows are decoded with change: no rebuild, the table and its rows stay; events after the
    /// DDL decode with the new positions, those before it were decoded with the old ones.
    ///
    /// The tables the DDL did not change keep the live plan's definitions; those it changed are
    /// read again. That holds only while the live plan describes their rows before the DDL: its
    /// definition of each was read when the binlog ended at or before `at` (its head; after a
    /// restart or a catch-up the plan may come from a later catalog). The new read's head likewise
    /// bounds the DDLs it may include: a later DDL of those tables before it fails this check and
    /// rebuilds the view, stale since this swap (see [`Self::stale_from`]). DDL path only.
    async fn swap_in_place(&mut self, id: u64, at: &Position, tables: &[TableRef]) -> bool {
        let Some(i) = self.idx_of(id) else {
            return false;
        };
        let s = &self.slots[i];
        let Some(l) = s
            .live
            .as_ref()
            .filter(|l| !l.frozen && !matches!(l.plan.mode, Mode::Full(_)))
        else {
            return false;
        };
        if s.next.is_some()
            || s.error.is_some()
            || s.waiting.is_some()
            || self.replan.contains_key(&id)
        {
            return false;
        }
        let hit = |x: &TableRef| {
            tables
                .iter()
                .any(|t| t.0 == x.0 && (t.1 == "*" || t.1 == x.1))
        };
        // Definitions the live plan decodes rows with, per table.
        let mut held: HashMap<TableRef, Arc<TableInfo>> = HashMap::new();
        for info in l
            .plan
            .aliases
            .iter()
            .map(|a| &a.table)
            .chain(l.plan.cascades.iter().map(|c| &c.parent))
        {
            held.entry((info.schema.clone(), info.name.clone()))
                .or_insert_with(|| info.clone());
        }
        let touched: Vec<TableRef> = held.keys().filter(|t| hit(t)).cloned().collect();
        let predates = |t: &TableRef| {
            l.table_heads
                .get(t)
                .map(|h| &h.0)
                .or(l.catalog_head.as_ref())
                .is_some_and(|h| h <= at)
        };
        if touched.is_empty() || !touched.iter().all(predates) {
            return false;
        }
        held.retain(|t, _| !touched.contains(t));
        let (def, targets) = (s.definition.clone(), s.targets.clone());
        // A core-owned version's queued rows predate this DDL: drain them here, between
        // transactions, while (a) is still the authority and `l.plan` is not swapped yet (a job
        // of a version the executor has taken over only drains the queue and answers empty, so
        // draining after the swap would lose those rows). With the queue empty, `sync` below
        // replaces the target in place instead of holding it: a held equal-shape replace leaves
        // `on_core` false and executor (b) takes the view over with a rebuild.
        if !self.in_txn && self.on_core(id) {
            self.flush_dedup().await;
        }
        // The catalog read below must see the DDL.
        if !self.fence().await {
            return false;
        }
        let plan = match self.parse_and_plan(&def, &targets, Some(id), &held).await {
            Ok(p) => p,
            Err(e) => {
                debug!(view = %self.slots[i].name, "re-plan after a source DDL failed: {e:#}");
                return false;
            }
        };
        let Some(head) = self.catalog_head().await else {
            return false;
        };
        let Some(l) = self.slots[i].live.as_mut() else {
            return false;
        };
        if let Err(why) = plan_swap::same_reads(&l.plan, &plan) {
            info!(view = %l.name, "source schema changed ({why}); rebuilding online");
            return false;
        }
        let plan = Arc::new(plan_swap::adopt(&l.plan, &plan));
        l.plan = plan.clone();
        for t in touched {
            l.table_heads.insert(t, (head.clone(), at.clone()));
        }
        info!(
            view = %l.name,
            "source schema changed; the view reads the same columns: new column positions, no rebuild"
        );
        let plan_head = l.plan_head();
        let s = &mut self.slots[i];
        s.last_plan = Some(plan);
        s.last_plan_head = plan_head;
        self.rebuild_routes();
        true
    }

    /// Where a view a source DDL at `at` of `tables` rebuilds is stale from: `at`, or the DDL
    /// that put one of those tables' definitions in place when that definition may already
    /// include this DDL (rows since then may have been decoded with this DDL's layout).
    fn stale_from(&self, id: u64, at: &Position, tables: &[TableRef]) -> Position {
        let hit = |x: &TableRef| {
            tables
                .iter()
                .any(|t| t.0 == x.0 && (t.1 == "*" || t.1 == x.1))
        };
        let mut from = at.clone();
        if let Some(l) = self.idx_of(id).and_then(|i| self.slots[i].live.as_ref()) {
            for (t, (read, swapped)) in &l.table_heads {
                if hit(t) && read > at && *swapped < from {
                    from = swapped.clone();
                }
            }
        }
        from
    }

    /// A view's own table on the source was emptied (`TRUNCATE`) or removed (`DROP TABLE`) by
    /// hand (lazyview does neither to a live table): rebuild the view online, as for a source
    /// schema change. A dropped table is created again by the swap.
    fn on_view_table_lost(&mut self, tables: &[TableRef], since: &Position, how: &str) {
        let hit: Vec<(u64, String)> = self
            .slots
            .iter()
            .filter_map(|s| {
                let l = s.live.as_ref()?;
                let on_source = l.targets.iter().any(|t| t == SOURCE_TARGET);
                tables
                    .iter()
                    .any(|t| on_source && t.0 == self.schema && t.1 == l.name)
                    .then(|| (s.def_id, l.name.clone()))
            })
            .collect();
        for (id, name) in hit {
            warn!(view = %name, "the view's table was {how} outside lazyview; rebuilding it online");
            self.schedule_replan_at(id, false, false, since);
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

    /// The estimated bytes buffered for building versions, when they exceed their budget
    /// (`Config::mem.build_hold_bytes`; `LAZYVIEW_TEST_BUILD_HOLD_BYTES` in tests). One pass over the
    /// slots: evaluated once per flush, never per event.
    fn builds_over_budget(&self) -> Option<usize> {
        let budget = self.cfg.mem.build_hold_bytes;
        let held: usize = self
            .slots
            .iter()
            .filter_map(|s| s.next.as_ref())
            .map(|n| buffer_bytes(&n.dirty))
            .sum();
        (held > budget).then_some(held)
    }

    /// The estimated bytes kept for targets that are behind, when they exceed their budget
    /// (`Config::mem.lag_hold_bytes`; `LAZYVIEW_TEST_LAG_HOLD_BYTES` in tests). One pass over the slots;
    /// called from `housekeeping`, never per event and never per flush. The budget is a
    /// reporting threshold only: passing it drops nothing and rebuilds nothing (see
    /// [`Self::report_lag_over_budget`]).
    fn lag_over_budget(&self) -> Option<usize> {
        let budget = self.cfg.mem.lag_hold_bytes;
        let held: usize = self
            .slots
            .iter()
            .filter_map(|s| s.live.as_ref())
            .flat_map(|l| l.lag.values())
            .map(|g| units_bytes(&g.units, &g.partial))
            .sum();
        (held > budget).then_some(held)
    }

    /// The budget of the changes kept for targets that are behind was passed: report only, take
    /// no action. A target that is down has nothing draining its keys while it is unreachable,
    /// but they are sets, bounded by the distinct ids the view can have, so it keeps them until
    /// it catches up; the saved position waits for them ([`lag_floor`]), so a restart replays
    /// the outage instead of losing the changes silently. Names the target that has been behind
    /// longest: it is the one keeping keys the longest, usually the unreachable one. Called
    /// once per crossing (the `lag_budget_warned` latch), not per housekeeping tick.
    fn report_lag_over_budget(&mut self, held: usize) {
        self.lag_budget_warned = true;
        Metrics::add(&self.metrics.lag_over_budget, 1);
        let budget = self.cfg.mem.lag_hold_bytes;
        let mut best: Option<(String, String)> = None;
        let mut oldest: Option<Position> = None;
        for s in self.slots.iter() {
            let Some(l) = s.live.as_ref() else { continue };
            for (t, g) in &l.lag {
                if oldest.as_ref().is_none_or(|p| g.since < *p) {
                    oldest = Some(g.since.clone());
                    best = Some((l.name.clone(), t.clone()));
                }
            }
        }
        if let Some((view, target)) = best {
            warn!(view = %view, target = %target, buffered_mib = held >> 20,
                "the changes kept for this target exceed their {budget} byte budget; they are kept until it catches up, and the saved position waits for them");
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
                    due.push(now);
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
            due.extend(l.behind.values().copied());
        }
        // Core targets (guard R3, lead 2026-11-13): the flush trigger is the running row count
        // in exec::Dedup that `feed_core` bumps, plus the retry hold `drained` cached at the
        // last flush: O(1) per binlog event, never a pass over the core or the slots. Fed rows
        // ride the batch deadline below (`feed_core` marks the version dirty); only rows left
        // by an interrupted flush, with no batch window armed, wake the flush themselves.
        if self.dedup.rows_pending() && self.first_dirty.is_none() {
            due.push(now);
        }
        if let Some(t) = self.dedup.trigger(now) {
            due.push(t);
        }
        let urgent = due.into_iter().min();
        let batch = self.first_dirty.map(|t| t + self.cfg.batch_window);
        let persist = self
            .persist_needed
            .then(|| self.last_persist + self.cfg.position_persist_interval);
        // A closed leadership gate holds the flush: the loop wakes at the end of the hold rather
        // than on every due change, and since `flush_due_within` reads this deadline, neither the
        // batch window nor the persist interval can re-issue a gated flush ahead of it.
        let next = [urgent, batch, persist].into_iter().flatten().min();
        match (next, self.leader_hold) {
            (Some(d), Some(hold)) => Some(d.max(hold)),
            (None, hold) => hold,
            (d, None) => d,
        }
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

    /// The leadership gates (review A-06 condition 3, lv-super ruling 1): one `watch` borrow — an
    /// atomic read, no allocation, no round trip — asked before any *new* work is issued and never
    /// per binlog event: the flush entry, the position save, the state-row writes, and a failed
    /// metadata statement. `true` means nothing new may start here: this instance is either the
    /// replaced one (`Fenced`, which ends the loop with the fatal error naming both epochs) or the
    /// one that cannot prove its lease right now (`Uncertain`, which an answered beat restores).
    ///
    /// The refresh supervisor's write ops are gated by the same state, through
    /// [`supervisor::LeaderGate`]; `leader_hold` is what keeps the loop off a closed gate.
    fn leader_holds_writes(&self) -> bool {
        !matches!(*self.leader_state.borrow(), LeaderState::Active)
    }

    async fn flush(&mut self) -> Result<()> {
        // The leadership gate (review A-06 condition 3, lv-super ruling 1), at the one point where
        // every write of this instance is issued: while the lock cannot be proven — the replaced
        // instance, or the one whose beats have stopped being answered — no refresh, no state row
        // and no position save starts. The dirty changes stay as they are, so the flush that
        // follows the next beat applies them; `leader_hold` is what keeps the loop asleep until
        // then, and the supervisor's own gate is the backstop for an op issued after this check.
        if self.leader_holds_writes() {
            self.leader_hold = Some(Instant::now() + self.cfg.lock_heartbeat);
            debug!("flush held: the leadership lock does not allow new writes right now");
            return Ok(());
        }
        self.leader_hold = None;
        let started = Instant::now();
        // (re)filled by `note_refresh` as this flush's refreshes complete (#215).
        self.flush_wrote.clear();
        // (duration, step, slot) of the per-view steps, for the slow-flush log.
        let mut slow: Vec<(Duration, &str, usize)> = Vec::new();
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
        if self.state_check {
            // The DELETE must be visible before we look for the rows.
            if self.fence().await {
                self.state_check = false;
                if let Err(e) = self.restore_state_rows().await {
                    self.state_check = true;
                    self.meta_failed(e, "reading _lazyview_state").await?;
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
        // Changes buffered for building versions stay until their build is applied (never a
        // rebuild); past their byte budget the reader waits instead (see `hold_for_builds`).
        // Builds, refreshes and lookups below must see every transaction read so far.
        let has_reads = !ready.is_empty()
            || self.dedup.core.pending_any()
            || self.slots.iter().any(|s| {
                s.live
                    .as_ref()
                    .map(|l| {
                        !l.frozen
                            && (!l.dirty.is_empty() || !l.lag.is_empty() || !l.behind.is_empty())
                    })
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
            let _now = Instant::now();
            // Supervised: the live views refresh together (below), each after its swap.
            let together = self.sup.is_some();
            for i in 0..self.slots.len() {
                let t0 = Instant::now();
                if self.slots[i]
                    .next
                    .as_ref()
                    .map(|n| n.done.is_some())
                    .unwrap_or(false)
                {
                    self.finish_next(i).await;
                    slow.push((t0.elapsed(), "swap", i));
                }
                if !together {
                    let t1 = Instant::now();
                    self.refresh_live(i).await;
                    slow.push((t1.elapsed(), "refresh", i));
                }
            }
            if together && !self.slots.is_empty() {
                let t1 = Instant::now();
                self.refresh_all_live().await;
                slow.push((t1.elapsed(), "refresh (all views, from)", 0));
            }
            let t1 = Instant::now();
            self.flush_dedup().await;
            self.dedup.drained();
            // The jobs just ran: drain their reports too (`feed_core` drains the rest on the
            // next event; a gap noted by the last queued job would else wait for a row that
            // may never come).
            self.core_events();
            // Replans `sync` held back (items in flight): the queue has drained now.
            let mut synced = self.dedup.advance_replacements();
            self.restore_lost_caches(&mut synced);
            self.move_core_caches(&synced);
            if !synced.removed.is_empty() || std::mem::take(&mut self.core_shape_routes) {
                // A refused version's target is finally gone, or a held equal-shape replace was
                // marked shape-stale: the rows come back to (are taken over by) the `RouteT`
                // path, which `rebuild_routes` rewires (its `sync` delivers the replace too).
                self.rebuild_routes();
            }
            slow.push((t1.elapsed(), "refresh (dedup core)", 0));
        }
        if let Err(e) = self.persist_position(false).await {
            self.meta_failed(e, "saving the binlog position").await?;
        }
        if !self.refreshed.is_empty() && self.last_touch.elapsed() >= Duration::from_secs(10) {
            let names: Vec<String> = self.refreshed.drain().collect();
            if let Err(e) = meta::touch_refreshed(&mut self.meta, &names).await {
                warn!("recording refresh times: {e:#}");
            }
            self.last_touch = Instant::now();
        }
        self.publish_stats();
        Metrics::add(&self.metrics.flushes, 1);
        self.update_gauges();
        self.after_boundary();
        let elapsed = started.elapsed();
        if elapsed >= SLOW_FLUSH {
            slow.sort_by_key(|x| std::cmp::Reverse(x.0));
            let top: Vec<String> = slow
                .iter()
                .take(5)
                .filter_map(|(d, what, i)| {
                    let s = self.slots.get(*i)?;
                    Some(format!("{what} {} {:.0}ms", s.name, d.as_secs_f64() * 1e3))
                })
                .collect();
            debug!(elapsed = ?elapsed, "slow flush: {}", top.join(", "));
        }
        tracing::trace!(elapsed = ?elapsed, "flush");
        Ok(())
    }

    /// Hands every view's counters since the last flush to the statistics aggregator: one
    /// non-blocking channel send per flush, no lock (see src/stats.rs).
    fn publish_stats(&mut self) {
        let schema = &self.schema;
        let views = self
            .slots
            .iter_mut()
            .map(|s| {
                let plan = s
                    .live
                    .as_ref()
                    .map(|l| &l.plan)
                    .or(s.next.as_ref().map(|n| &n.plan))
                    .or(s.last_plan.as_ref());
                let mode = plan.map(|p| mode_name(p).0).unwrap_or("unknown");
                let (tally, pending, cached) = match s.live.as_mut() {
                    Some(l) => {
                        let mut t = std::mem::take(&mut l.dirty.tally);
                        // Rows the core's consumer skipped are not counted by `feed_rows`
                        // (that is executor (b)'s path): lift them into the engine's counter
                        // as this tally takes them.
                        let core_t = self.dedup.tally(s.def_id);
                        crate::metrics::Metrics::add(
                            &self.metrics.rows_skipped,
                            core_t.rows_skipped,
                        );
                        t.add(&core_t);
                        (
                            t,
                            pending_ids(&l.dirty, &l.plan) + self.dedup.pending(s.def_id),
                            l.dirty
                                .cache
                                .routes
                                .iter()
                                .map(|r| r.rows.len() as u64)
                                .sum(),
                        )
                    }
                    None => Default::default(),
                };
                crate::metrics::Metrics::add(&self.metrics.cascade_rows, tally.cascade_rows);
                // Freshness (review O-01): a view left with nothing buffered has applied every
                // change it was handed, so its stamp is the head it was fed to — one `u64` store
                // per flush per view, never per event. A view with work kept, a build in flight,
                // an error, or one waiting on an upstream or a blocked upstream keeps its older
                // stamp, and its lag then grows with the time the source has been ahead.
                let settled = pending == 0
                    && s.error.is_none()
                    && s.blocked.is_none()
                    && s.waiting.is_none()
                    && s.next.is_none()
                    && s.live.as_ref().is_some_and(|l| {
                        !l.frozen && l.dirty.is_empty() && l.lag.is_empty() && l.behind.is_empty()
                    });
                if settled && self.clock.fed_ts > s.applied_ts {
                    s.applied_ts = self.clock.fed_ts;
                }
                let source_lag_seconds = self.clock.lag(s.applied_ts);
                crate::stats::Fold {
                    view: format!("{schema}.{}", s.name),
                    mode,
                    tally,
                    flush: std::mem::take(&mut s.stats),
                    pending,
                    cascade_cached_rows: cached,
                    // The streak the alerting reads (O-02): one u32 read per view per flush,
                    // no new per-event work (`Slot::refresh_failures` moves only on a refresh
                    // outcome; its zeroing is the success path's `std::mem::take`).
                    refresh_failures: u64::from(s.refresh_failures),
                    source_lag_seconds,
                }
            })
            .collect();
        self.stats_tx.send(views);
    }

    /// Waits until the transactions read from the binlog are visible to readers: binlog events
    /// are shipped before the engine commit completes.
    fn exact_fence(&self) -> bool {
        (self.flavor.is_mysql_family() && self.last_gtid.is_some())
            || self.snapshot_fence == Some(true)
            || self.commit_fence == Some(true)
    }

    /// `false`: the last transaction read is not visible yet (a wait timed out), so nothing
    /// may be read from the sources now.
    ///
    /// The mechanisms are the flavor's (`mysql::gtid_wait`, `maria::snapshot_position`,
    /// `mysql::commit_order`); the engine keeps only the caches (`snapshot_fence`,
    /// `snapshot_watermark`, `commit_fence`, `commit_fenced`), the retry policy (`retry_src`) and
    /// the `LAZYVIEW_VISIBILITY_DELAY_MS` fallback. Which mechanism is a fence at all is the
    /// flavor's answer too: `snapshot_watermark` says whether waiting on the consistent-snapshot
    /// position proves anything (`Flavor::snapshot_position_is_watermark`).
    async fn fence(&mut self) -> bool {
        // MySQL / Percona with GTIDs: `WAIT_FOR_EXECUTED_GTID_SET` (see `mysql::gtid_wait`).
        if self.flavor.is_mysql_family() {
            if let Some(g) = self.last_gtid.clone() {
                let r = mysql::gtid_wait(&mut self.src, &g).await;
                self.metrics.beat();
                match r {
                    // 0: executed; 1: timed out.
                    Ok(Some(Some(0))) => return true,
                    Ok(Some(Some(_))) => {
                        warn!("transaction not visible after 5 s (WAIT_FOR_EXECUTED_GTID_SET timed out); retrying");
                        return false;
                    }
                    Ok(_) => debug!("gtid fence returned NULL"),
                    Err(e) => {
                        debug!("gtid fence unavailable: {e}");
                        if binlog::is_transient(&e) {
                            self.heal_src().await;
                            return false;
                        }
                    }
                }
            }
        }
        // The consistent-snapshot wait (`maria::snapshot_position` behind `visible_upto`) is a
        // fence only where the reported position is a committed-transaction watermark
        // (`snapshot_watermark`, the flavor's answer): MariaDB's is. The MySQL family reports its
        // raw binlog write position, which has already passed transactions still in flight that
        // the snapshot itself cannot read (Percona Server 8.4: the position advanced past an
        // in-flight update whose row the snapshot still showed as old), so waiting on it would
        // approve a stale read. Those flavors take the GTID wait above and the commit-order row
        // below; a server that reports no position at all (MySQL Community) rules itself out.
        if self.snapshot_watermark && self.snapshot_fence != Some(false) && self.boundary.is_some()
        {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut pause = Duration::from_millis(1);
            loop {
                match self.visible_upto().await {
                    Ok(Some(p)) => {
                        self.snapshot_fence = Some(true);
                        if p >= self.boundary.to_position() {
                            return true;
                        }
                        if Instant::now() >= deadline {
                            warn!("transaction not visible after 5 s (consistent snapshot behind the binlog position read); retrying");
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
                        if binlog::is_transient(&e) {
                            self.heal_src().await;
                            return false;
                        }
                        break;
                    }
                }
            }
        }
        // The commit-order fence (`mysql::commit_order`, through `src/fence.rs` which only that
        // file calls): one row of our own, committed after every transaction read. It runs
        // whenever the consistent-snapshot wait is not the fence in force: the flavor says its
        // position is no watermark (the MySQL family), or the server ruled itself out by
        // reporting none. The engine keeps only the caches and the
        // `LAZYVIEW_VISIBILITY_DELAY_MS` fallback.
        if (self.snapshot_fence == Some(false) || !self.snapshot_watermark)
            && self.boundary.is_some()
        {
            let mut state = FenceState {
                snapshot: self.snapshot_fence,
                commit_order: self.commit_fence,
                committed: self.commit_fenced.clone(),
            };
            let fallback = self.fallback_wait();
            let mut report = crate::source::FenceReport::default();
            match mysql::commit_order(
                &mut self.src,
                &self.boundary.to_position(),
                &self.cfg.instance_id,
                &fallback,
                &mut state,
                &mut report,
                &mut || self.metrics.beat(),
            )
            .await
            {
                Ok(mysql::CommitStep::Written) => {
                    self.keep_fence_state(state);
                    if let Some(w) = report.fence_warn {
                        self.warn_fence(w);
                    }
                    return true;
                }
                Ok(mysql::CommitStep::RuledOut) => {
                    self.keep_fence_state(state);
                    if let Some(w) = report.fence_warn {
                        self.warn_fence(w);
                    }
                }
                Err(e) => {
                    self.keep_fence_state(state);
                    if binlog::is_transient(&e) {
                        self.heal_src().await;
                    }
                    return false;
                }
            }
        }
        if !self.cfg.visibility_delay.is_zero() {
            tokio::time::sleep(self.cfg.visibility_delay).await;
        }
        true
    }

    /// The binlog position up to which every transaction is visible, from a consistent
    /// snapshot (`None`: the server does not report one, e.g. MySQL Community).
    async fn visible_upto(&mut self) -> Result<Option<Position>> {
        // A transient failure here used to end the flush's exact fence: repeat the whole read
        // (the boundary shape of `retry_src`), and let the caller fall back to the visibility
        // delay only once the tries are spent.
        let fail = self.cfg.test_fail_snapshot;
        retry_src(
            &self.metrics,
            "reading the binlog snapshot position",
            &mut self.src,
            &self.query_opts,
            self.cfg.lock_wait_s,
            |try_no, src| Box::pin(crate::source::maria::snapshot_position(src, fail, try_no)),
        )
        .await
    }

    /// Writes the commit-order caches back after one wait: the engine owns
    /// them, the source only reads and updates them for one wait.
    fn keep_fence_state(&mut self, state: FenceState) {
        self.snapshot_fence = state.snapshot;
        self.commit_fence = state.commit_order;
        self.commit_fenced = state.committed;
    }

    /// The fallback wait as the WARN names it: the configured `LAZYVIEW_VISIBILITY_DELAY_MS`.
    fn fallback_wait(&self) -> String {
        match self.cfg.visibility_delay.as_millis() {
            0 => "waits LAZYVIEW_VISIBILITY_DELAY_MS (0 ms: no wait) instead".to_string(),
            ms => format!("waits LAZYVIEW_VISIBILITY_DELAY_MS ({ms} ms) instead"),
        }
    }

    /// Logs a commit-fence WARN once per engine (one engine per process).
    fn warn_fence(&mut self, what: impl std::fmt::Display) {
        if !std::mem::replace(&mut self.fence_warned, true) {
            warn!("{what}");
        }
    }

    async fn replan_slot(&mut self, i: usize, freeze: bool) {
        let (def, targets, id) = {
            let s = &self.slots[i];
            (s.definition.clone(), s.targets.clone(), s.def_id)
        };
        // Planning it timed out: not again until the definition changes.
        if self.slots[i].plan_timed_out.as_ref() == Some(&self.slots[i].hash) {
            debug!(view = %self.slots[i].name, "not re-planned: planning timed out before");
            return;
        }
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
        match self
            .parse_and_plan(&def, &targets, Some(id), &HashMap::new())
            .await
        {
            Ok(plan) => {
                let plan = Arc::new(plan);
                self.slots[i].last_plan = Some(plan.clone());
                self.slots[i].last_plan_head = self.catalog_head().await;
                self.slots[i].waiting = None;
                let name = plan.name.clone();
                if freeze {
                    info!(view = %name, "source schema changed; rebuilding online");
                }
                self.start_next(i, plan, name, targets, false).await;
            }
            Err(e)
                if e.downcast_ref::<WaitingFor>().is_some()
                    || e.downcast_ref::<PlanTimedOut>().is_some() =>
            {
                self.plan_failed(i, &e).await
            }
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
        let delay = connect_retry_delay(d.failures);
        d.retry_at = Instant::now() + delay;
        let msg = format!("{e:#}");
        warn!(view = %name, target = %t, "{msg}; retrying in {delay:?}");
        let (id, hash) = (s.def_id, s.hash.clone());
        // A swap that only spent its own bounded wait on the table's metadata lock is transient
        // contention, not a build failure (guard R1 on #254, lv-super's note): the row keeps the
        // state a view has while it is being built and names the reason in `last_error`, until
        // it swaps or gives up for good. `error` tells an operator to act (and the stall alerts
        // key on it), so it stays for the failures that are real. The retry below is what gets
        // this view swapped: the next build, or the re-arm in `finish_next`.
        let state = if target::is_lock_wait_timeout(e) {
            "building"
        } else {
            "error"
        };
        self.set_state(name, t, id, &hash, plan, state, Some(&msg))
            .await;
    }

    /// Retries the targets a view could not be built on at all (their table is built there by a
    /// partial build). A target that was maintained and went down keeps its keys in `Lag` and is
    /// caught up by `retry_lag` instead.
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
                .filter(|(t, d)| {
                    d.retry_at <= now && s.targets.contains(t) && !l.lag.contains_key(t.as_str())
                })
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
        // A swap that only spent its bounded wait on a table's metadata lock is transient (guard
        // R1 on #254): the row says `building` with the reason, not `error`.
        if target::is_lock_wait_timeout(&e) {
            self.slot_building(i, &e).await;
        } else {
            self.slot_error(i, &e).await;
        }
        self.schedule_replan(def_id, frozen, true);
    }

    /// The lock wait of the swap the binlog loop does itself (seconds; the shortest wait the
    /// servers allow), one try, no retry. The swap's real bounded wait is the operator's
    /// `swap_lock_wait_s`, spent in the swap task off the loop: the shared reader is never
    /// paused for one target's metadata lock (N-01, N-02).
    const LOOP_SWAP_LOCK_WAIT_S: u64 = 1;

    /// Applies changes buffered during the build to the shadow tables, swaps them in.
    async fn finish_next(&mut self, i: usize) {
        let mut n = self.slots[i].next.take().unwrap();
        let Built {
            layout,
            failed,
            swapped,
            lock_wait,
            cache,
        } = match n.done.take().unwrap() {
            Ok(b) => b,
            Err(e) => {
                self.next_failed(i, n, e).await;
                return;
            }
        };
        let log = n.dirty.cache.install(cache);
        accumulate::drain(&n.plan, &mut n.dirty, log);
        n.swapped.extend(swapped);
        n.lock_wait.extend(lock_wait);
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
        let shadow = target::shadow_name(&n.name);
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
                    if binlog::is_transient(&e) {
                        self.heal_src().await;
                    }
                    n.dirty.merge(dirty);
                    n.since = since;
                    self.next_failed(i, n, e.context("applying changes buffered during build"))
                        .await;
                    return;
                }
            }
            // EVERY swap is done off the binlog loop (N-02): the DDL takes seconds on some
            // servers, and a reader holding a table's metadata lock must never pause the shared
            // reader. Any target the swap task has not reported yet gets the task, whatever kind
            // of build this is (a routine rebuild, a first build, a re-plan that replaces a live
            // version, a partial add); `swapping` makes it exactly one task per version. A live
            // version pauses meanwhile: the changes are buffered for this version and applied
            // right after the swap (below), so readers never see older data after it.
            let swap_todo = n
                .targets
                .iter()
                .any(|t| !n.swapped.contains(t) && !n.lock_wait.contains(t));
            if swap_todo && !n.swapping && !n.targets.is_empty() {
                let marker = target::marker(&n.name, self.slots[i].def_id);
                let specs = n
                    .targets
                    .iter()
                    .map(|t| {
                        let target = &self.targets[t];
                        // The ownership check of the swap on the loop (below), right before it.
                        let live_has = self.slots[i]
                            .live
                            .as_ref()
                            .is_some_and(|l| l.name == n.name && l.targets.contains(t));
                        let check = (!live_has
                            && !self.owned.contains_key(&(n.name.clone(), t.clone())))
                        .then(|| marker.clone());
                        (
                            t.clone(),
                            target.opts.clone(),
                            target.schema.clone(),
                            target.kind,
                            check,
                        )
                    })
                    .collect();
                n.task = tokio::spawn(swap_task(
                    specs,
                    n.name.clone(),
                    layout,
                    self.build_tx.clone(),
                    (self.slots[i].def_id, n.generation),
                    self.cfg.lock_wait_s,
                    self.cfg.swap_lock_wait_s,
                    self.metrics.clone(),
                ));
                n.swapping = true;
                if !dirty.is_empty() {
                    n.since = since;
                }
                n.dirty.merge(dirty);
                self.slots[i].next = Some(n);
                return;
            }
        }
        for t in n.targets.clone() {
            if n.swapped.contains(&t) {
                continue;
            }
            // The swap waits for the table lock off the binlog loop (N-02): a target whose
            // off-loop swap (the build task or the swap task) already timed out on the lock
            // is not tried again here — it goes straight to "left to the next rebuild".
            let timed_out = n.lock_wait.contains(&t);
            let r = if timed_out {
                // The swap task already spent the operator's bounded wait on this table's lock
                // (N-02): the loop does not wait for it again. A decoded kind, so the call sites
                // and `target_down` see a transient lock wait, not a build failure.
                Err(anyhow!(target::SwapLockWait {
                    per_try_s: self.cfg.swap_lock_wait_s,
                    tries: crate::supervisor::Policy::SWAP.attempts + 1,
                }))
            } else {
                let ts = self.targets[&t].schema.clone();
                let kind = self.targets[&t].kind;
                // Someone created a table with this name meanwhile: never replace it.
                let live_has = self.slots[i]
                    .live
                    .as_ref()
                    .map(|l| l.name == n.name && l.targets.contains(&t))
                    .unwrap_or(false);
                if !live_has && !self.owned.contains_key(&(n.name.clone(), t.clone())) {
                    let marker = target::marker(&n.name, self.slots[i].def_id);
                    let exists = match self.target_conn(&t).await {
                        Ok(c) => foreign_table(c, &ts, &n.name, &marker).await,
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
                // The tail of a swap the swap task already tried (or gave up on): the shared
                // reader never waits for a lock it cannot get, so this single try uses the
                // shortest wait the server allows and no retry (N-02). The operator's bounded
                // wait (`swap_lock_wait_s`) is spent in the swap task, off the loop.
                let (swap_wait, session_wait, m) = (
                    Self::LOOP_SWAP_LOCK_WAIT_S,
                    self.cfg.lock_wait_s,
                    self.metrics.clone(),
                );
                match self.target_conn(&t).await {
                    Ok(c) => {
                        match tokio::time::timeout(
                            timeout,
                            target::swap(c, kind, &ts, &n.name, swap_wait, session_wait, true, &m),
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(_) => Err(anyhow!("timed out after {timeout:?}")),
                        }
                    }
                    Err(e) => Err(e),
                }
            };
            match r {
                Ok(()) => {
                    n.swapped.insert(t.clone());
                }
                Err(e) if timed_out || target::is_lock_wait_timeout(&e) => {
                    // A reader's open transaction holds the table's metadata lock and the
                    // swap spent its own bounded wait on it (N-01). The binlog loop never
                    // waits for that lock and never retries this swap (N-02): the view is
                    // reported at the default level and the swap is left to the next rebuild.
                    n.lock_wait.insert(t.clone());
                    if self.slots[i].live.is_some() && !n.partial {
                        warn!(
                            view = %n.name, target = %t,
                            "the swap could not get the table lock within {} s per try ({} tries); \
                             the view keeps its current table: the swap is left to the next rebuild",
                            self.cfg.swap_lock_wait_s,
                            crate::supervisor::Policy::SWAP.attempts + 1,
                        );
                        // The live version keeps serving; the changes buffered for this
                        // version go back to it (a full-mode view is dirty again: the
                        // LAZYVIEW_FULL_REFRESH_MS cadence starts the next rebuild, and that
                        // one swaps once the reader is gone). Same merge as `next_failed`.
                        if let Some(l) = self.slots[i].live.as_mut() {
                            if Arc::ptr_eq(&l.plan, &n.plan) || matches!(l.plan.mode, Mode::Full(_))
                            {
                                l.dirty.merge(dirty);
                                if matches!(l.plan.mode, Mode::Full(_)) {
                                    l.dirty.all = true;
                                } else {
                                    // Guard R1 (finding 3): "left to the next rebuild" must be a
                                    // promise the engine keeps for an INCREMENTAL view too. It
                                    // re-arms the abandoned swap as the routine online rebuild
                                    // this path already has (the one for unknown affected rows),
                                    // at the `LAZYVIEW_FULL_REFRESH_MS` cadence: the loop wakes
                                    // for it (`flush_deadline`) and `refresh_live` starts it, and
                                    // that build swaps off the loop once the reader is gone.
                                    l.dirty.rebuild = true;
                                }
                                if let Some(p) = since {
                                    if l.since.as_ref().is_none_or(|s| p < *s) {
                                        l.since = Some(p);
                                    }
                                }
                            }
                        }
                        // The same for the other shape of this branch: a version that REPLACES
                        // the live one (a re-plan: definition, targets or source schema changed)
                        // is not covered by the live version's own work, so the re-plan that
                        // produced it is retried with the build backoff. `next_failed` gives a
                        // failed build of such a version exactly this re-arm.
                        if !self.slots[i]
                            .live
                            .as_ref()
                            .is_some_and(|l| Arc::ptr_eq(&l.plan, &n.plan))
                        {
                            let def_id = self.slots[i].def_id;
                            self.schedule_replan(def_id, false, true);
                        }
                        for t in &n.targets {
                            if !n.swapped.contains(t) {
                                self.drop_shadow(t, &n.name).await;
                            }
                        }
                        return; // `n` is dropped: the live version stays as it was.
                    }
                    // A first build (no live version yet) or a partial add: there is no
                    // serving table to keep, so the target fails as for any other swap error
                    // — but the connection is healthy (a lock timeout is not a connection
                    // fault) and the retry runs as a new build task, never on the loop.
                    warn!(view = %n.name, target = %t, "the swap gave up on the table lock: {e:#}");
                    let e = e.context(format!("swapping in new version on target {t}"));
                    n.targets.retain(|x| *x != t);
                    let plan = n.plan.clone();
                    self.target_down(i, &n.name, &t, Some(&plan), &e).await;
                    first_err.get_or_insert(e);
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
            if n.routine {
                // The state row says `ready` already; only `refreshed_at` moves.
                self.refreshed.insert(n.name.clone());
            } else {
                self.set_state(&n.name, t, def_id, &n.hash, Some(&n.plan), "ready", None)
                    .await;
            }
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
            // So does a rebuild asked for with `_lazyview.rebuild = 1`: the request is done.
            if s.rebuild_asked.as_ref().is_some_and(|p| *p <= n.started) {
                s.rebuild_asked = None;
                if let Err(e) = meta::clear_rebuild(&mut self.meta, def_id).await {
                    warn!(view = %n.name, "clearing _lazyview.rebuild: {e:#}");
                }
            }
            let s = &mut self.slots[i];
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
            images: vec![KeySet::default(); n.plan.aliases.len()],
            ..Default::default()
        };
        live_dirty.cache = std::mem::take(&mut n.dirty.cache);
        live_dirty.merge(n.dirty);
        live_dirty.all |= full_again;
        // Changes not applied to the shadow (a swap that was retried, or ran in the background)
        // apply to the new live version.
        live_dirty.merge(dirty);
        let since = match (since, n.since) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let replan_after = std::mem::take(&mut self.slots[i].replan_after);
        let recent = std::mem::take(&mut n.recent);
        // Ids the live version's buffer receives from the build (and of those already pending).
        let mut carried = (pending_ids(&live_dirty, &n.plan), 0);
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
                    let before = pending_ids(&l.dirty, &l.plan);
                    l.dirty.merge(live_dirty);
                    carried.1 = before + carried.0 - pending_ids(&l.dirty, &l.plan);
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
                    behind: BTreeMap::new(),
                    recent,
                    catalog_head: n.catalog_head,
                    table_heads: HashMap::new(),
                });
            }
        }
        self.slots[i].stats.carried(carried.0, carried.1);
        // The build seeded a cascade cache for this version: a target on the core runs with it.
        self.reseed_core_cache(def_id);
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
        let Some(mut st) = self.live_begin(i).await else {
            return;
        };
        let r = self
            .refresh(
                &st.plan,
                &st.layout,
                &mut st.dirty,
                &st.targets,
                &st.name,
                &mut st.recent,
                st.since.as_ref(),
            )
            .await;
        self.live_end(i, st, r).await;
    }

    /// What [`Self::refresh_live`] does before the refresh: lagging targets, full-mode views
    /// (rebuilt instead), and the changes to apply. None: nothing to refresh now.
    async fn live_begin(&mut self, i: usize) -> Option<LiveRefresh> {
        // A routine rebuild is being swapped in: changes wait for it (see `finish_next`).
        if self.slots[i].next.as_ref().is_some_and(|n| n.swapping) {
            return None;
        }
        self.retry_lag(i).await;
        let def_id = self.slots[i].def_id;
        let replan_pending = self.replan.contains_key(&def_id);
        let backing_off = self.slots[i].retry_at.is_some_and(|t| t > Instant::now());
        let now = Instant::now();
        let full_due = self.full_due(&self.slots[i]).is_none_or(|t| t <= now);
        let s = &mut self.slots[i];
        let l = s.live.as_mut()?;
        if l.frozen || l.dirty.is_empty() || backing_off {
            return None;
        }
        let dirty = l.dirty.take();
        let (plan, layout, name) = (l.plan.clone(), l.layout.clone(), l.name.clone());
        let lagging: Vec<String> = l.lag.keys().chain(l.behind.keys()).cloned().collect();
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
            return None;
        }
        let recent = std::mem::take(&mut l.recent);
        let since = l.since.clone();
        Some(LiveRefresh {
            plan,
            layout,
            name,
            dirty,
            targets,
            lagging,
            recent,
            since,
            def_id,
            replan_pending,
            full_due,
        })
    }

    /// After a successful refresh of slot `i`'s live version (from `live_end`; executor (a) too): targets whose
    /// writes failed start lagging at `floor` and get the refreshed units later, lagging targets get them too,
    /// and the view's statistics count the refresh. A target flagged behind (see
    /// [`Self::mark_behind_targets`]) gets its lag entry here, as one of the `lagging`. The
    /// targets this refresh wrote are recorded in [`Engine::flush_wrote`] for the behind check.
    fn note_refresh(
        &mut self,
        i: usize,
        name: &str,
        r: Refreshed,
        lagging: &[String],
        floor: Position,
        targets: &[String],
    ) {
        let s = &mut self.slots[i];
        let Some(l) = s.live.as_mut() else {
            return;
        };
        self.flush_wrote.push((i, targets.to_vec()));
        // Targets that missed this refresh (or earlier ones) get the units later.
        for (t, e) in &r.failed {
            warn!(view = %name, target = %t, "write failed: {e:#}; retrying");
            l.behind.remove(t.as_str());
            l.lag.entry(t.clone()).or_insert(Lag {
                units: UnitSet::default(),
                partial: Vec::new(),
                all: false,
                since: floor.clone(),
                failures: 1,
                retry_at: Instant::now() + connect_retry_delay(1),
                down: false,
            });
        }
        // A target flagged behind (`mark_behind_targets`) that this refresh applied to none of:
        // what it is owed is exactly what was fed since the flag, i.e. what this refresh
        // carried. It becomes the lag entry the failed path above gives (#215): from here the
        // accounting is the one a down target gets — kept keys over the budget reported, the
        // saved position waiting, `retry_lag` catching it up and counting the catch-up.
        for t in lagging {
            if l.behind.remove(t.as_str()).is_some() && !l.lag.contains_key(t.as_str()) {
                l.lag.insert(
                    t.clone(),
                    Lag {
                        units: UnitSet::default(),
                        partial: Vec::new(),
                        all: false,
                        since: floor.clone(),
                        failures: 0,
                        retry_at: Instant::now() + connect_retry_delay(1),
                        down: false,
                    },
                );
            }
        }
        for t in r.failed.iter().map(|(t, _)| t).chain(lagging) {
            if let Some(g) = l.lag.get_mut(t) {
                g.units.extend(r.units.clone());
                extend_partial(&mut g.partial, r.partial.clone());
                g.all |= r.all;
            }
        }
        s.stats.refreshed(r.units, r.partial, r.all, &r.work);
    }

    /// What [`Self::refresh_live`] does with the result `r` of the refresh `st`.
    async fn live_end(&mut self, i: usize, st: LiveRefresh, r: Result<Refreshed>) {
        let LiveRefresh {
            plan,
            layout: _,
            name,
            dirty,
            targets,
            lagging,
            recent,
            since: _,
            def_id,
            replan_pending,
            full_due,
        } = st;
        if let Some(l) = self.slots[i].live.as_mut() {
            l.recent = recent;
        }
        match r {
            Err(e) => {
                if binlog::is_transient(&e) {
                    self.heal_src().await;
                }
                // Keep the changes for the next attempt, after a backoff, however often it fails: a
                // failed refresh never re-plans or rebuilds the view (owner rule). From the third
                // failure in a row the state rows show the error (`show_refresh_error`).
                let s = &mut self.slots[i];
                if let Some(l) = s.live.as_mut() {
                    l.dirty.merge(dirty);
                }
                s.refresh_failures += 1;
                let n = s.refresh_failures;
                // The shared backoff, READS shape with a 30 s cap and jitter (the multi-instance
                // case: instances must not retry in lockstep).
                let delay = supervisor::backoff(
                    n,
                    supervisor::Policy::READS.base,
                    Duration::from_secs(30),
                    true,
                );
                s.retry_at = Some(Instant::now() + delay);
                Metrics::add(&self.metrics.refresh_errors, 1);
                warn!(view = %name, attempt = n, "refresh failed: {e:#}; retrying in {delay:?}");
                if n == REFRESH_ERROR_SHOWN_AFTER {
                    let msg = format!(
                        "refresh failed {n} times in a row, retrying with backoff (the table is stale \
                         meanwhile; rebuild = 1 in _lazyview re-plans the view): {e:#}"
                    );
                    self.show_refresh_error(i, Some(&msg)).await;
                }
            }
            Ok(r) => {
                let s = &mut self.slots[i];
                s.retry_at = None;
                let shown = std::mem::take(&mut s.refresh_failures) >= REFRESH_ERROR_SHOWN_AFTER;
                if s.next.is_none() && !replan_pending {
                    self.failures.remove(&def_id);
                }
                if shown {
                    self.show_refresh_error(i, None).await;
                }
                self.refreshed.insert(name.clone());
                let s = &mut self.slots[i];
                let Some(l) = s.live.as_mut() else {
                    return;
                };
                let floor = l
                    .since
                    .clone()
                    .unwrap_or_else(|| self.boundary.to_position());
                self.note_refresh(i, &name, r, &lagging, floor, &targets);
                let s = &mut self.slots[i];
                let Some(l) = s.live.as_mut() else {
                    return;
                };
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
        // Behind flags whose retry time came without a refresh handing them their units (a
        // one-target view: the skipped refresh has no target left to read the materialized
        // table from) become lag entries on their own (#215): the changes buffered now are
        // what the target is owed, so the entry seeds itself from them and the pass below
        // catches it up. A flagged target that owes nothing (buffer empty) just drops the
        // flag — it never missed a change, so there is no catch-up to count either.
        if let Some(l) = self.slots[i].live.as_mut().filter(|l| !l.frozen) {
            let flagged: Vec<String> = l
                .behind
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(t, _)| t.clone())
                .collect();
            for t in flagged {
                l.behind.remove(&t);
                if l.dirty.units.is_empty() && l.dirty.partial.is_empty() && !l.dirty.all {
                    continue;
                }
                let seed = Lag {
                    units: l.dirty.units.clone(),
                    partial: l.dirty.partial.clone(),
                    all: l.dirty.all,
                    since: l
                        .since
                        .clone()
                        .unwrap_or_else(|| self.boundary.to_position()),
                    failures: 0,
                    retry_at: now,
                    down: false,
                };
                l.lag.entry(t).or_insert(seed);
            }
        }
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
                partial: g.partial.clone(),
                images: vec![KeySet::default(); plan.aliases.len()],
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
                    Metrics::add(&self.metrics.target_catchups, 1);
                    let (id, hash) = (s.def_id, s.hash.clone());
                    self.set_state(&name, &t, id, &hash, Some(&plan), "ready", None)
                        .await;
                    // A target reported down was taken out of the version's targets; it is
                    // maintained again from now on, and its `Down` entry is gone with its keys.
                    let was_down = g.down;
                    if let Some(l) = self.slots[i].live.as_mut() {
                        if was_down && !l.targets.contains(&t) {
                            l.targets.push(t.clone());
                            l.targets.sort();
                        }
                    }
                    if was_down {
                        self.slots[i].down.remove(&t);
                        self.rebuild_routes();
                    }
                }
                Ok(r) => {
                    g.failures += 1;
                    // Reported down after LAG_MAX_FAILURES: excluded from the refresh's targets,
                    // its state shows the error, and it is retried here at the capped backoff.
                    // Its keys stay (owner rule: a traceable change is never answered with a
                    // rebuild), so `retry_lag` catches it up when it answers; `retry_down` only
                    // builds targets that were never built on at all.
                    let e = r.failed.into_iter().next().unwrap().1;
                    if g.failures >= LAG_MAX_FAILURES && !g.down {
                        g.down = true;
                        l.targets.retain(|x| *x != t);
                        self.target_down(i, &name, &t, Some(&plan), &e).await;
                        self.rebuild_routes();
                    }
                    g.retry_at = Instant::now() + connect_retry_delay(g.failures);
                    // The keys go back into the lag map last: `l` borrows the slot, and
                    // `target_down` and `rebuild_routes` above need `self` to itself.
                    if let Some(l) = self.slots[i].live.as_mut() {
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
        plan: &Arc<ViewPlan>,
        layout: &Layout,
        dirty: &mut ViewDirty,
        targets: &[String],
        table: &str,
        recent: &mut Lookback,
        since: Option<&Position>,
    ) -> Result<Refreshed> {
        if self.sup.is_some() {
            let mut p = self
                .prepare_refresh(plan, layout, dirty, targets, table, recent, since)
                .await?;
            let out = run_pending(self.sup.as_ref().expect("supervised"), &mut p).await;
            self.metrics.beat();
            return finish_refresh(dirty, p, out);
        }
        let mut failed: Vec<(String, anyhow::Error)> = Vec::new();
        let mut work = crate::stats::Work::default();
        if matches!(plan.mode, Mode::Global) {
            if !dirty.all {
                return Ok(Refreshed::default());
            }
            let fresh = self.caps.fresh_snapshot;
            let rows =
                mv::read_source(&mut self.src, plan, layout, None, &self.metrics, fresh).await?;
            work.source_queries += 1;
            // Converted once for the chunk and shared by every target of it (`Prepared` is
            // immutable, `diff` only reads it), as the supervisor's chunk op does.
            let new = mv::prepare(layout, rows);
            Metrics::add(&self.metrics.units_refreshed, 1);
            for t in targets {
                match self.apply_target(t, table, layout, None, &new).await {
                    Ok(a) => work.rows.applied(&a),
                    Err(e) => failed.push((t.clone(), e)),
                }
            }
            dirty.all = false;
            return Ok(Refreshed {
                units: UnitSet::default(),
                partial: Vec::new(),
                all: true,
                failed,
                work,
            });
        }
        if dirty.all {
            // A unit-based view the gap window left untraceable (`accumulate::mark_all`, #167):
            // the children a parent delete took with it are gone from the source and the cache,
            // so no lookup can name the units they left. The generic remedy, as the Global
            // branch above: read the whole view once and apply it in place, which deletes the
            // vanished rows. One read per window, at most once per view per window; a refresh,
            // not a rebuild, and the other buffered units are subsumed by it, so the buffer
            // clears with it (a failed target is retried as a whole refresh by `retry_lag`).
            let fresh = self.caps.fresh_snapshot;
            let rows =
                mv::read_source(&mut self.src, plan, layout, None, &self.metrics, fresh).await?;
            work.source_queries += 1;
            // Converted once for the chunk and shared by every target of it (`Prepared` is
            // immutable, `diff` only reads it), as the supervisor's chunk op does.
            let new = mv::prepare(layout, rows);
            Metrics::add(&self.metrics.units_refreshed, 1);
            for t in targets {
                match self.apply_target(t, table, layout, None, &new).await {
                    Ok(a) => work.rows.applied(&a),
                    Err(e) => failed.push((t.clone(), e)),
                }
            }
            let n = dirty.images.len();
            let rebuild = dirty.rebuild;
            *dirty = ViewDirty {
                images: vec![KeySet::default(); n],
                rebuild,
                ..Default::default()
            };
            return Ok(Refreshed {
                units: UnitSet::default(),
                partial: Vec::new(),
                all: true,
                failed,
                work,
            });
        }
        // `dirty` is only cleared once everything succeeded, so a failure can be retried.
        let mut units = dirty.units.clone();
        let rebuild = dirty.rebuild;
        let resolve = (units.len(), Metrics::get(&self.metrics.lookup_queries));
        self.lookups(
            plan,
            layout,
            &dirty.images,
            (targets, table),
            (recent, since),
            &mut units,
        )
        .await?;
        self.presence_lookups(
            plan,
            layout,
            (&dirty.marks, &dirty.holds),
            (targets, table),
            &mut units,
        )
        .await?;
        work.ids_resolved = (units.len() - resolve.0) as u64;
        work.lookup_queries = Metrics::get(&self.metrics.lookup_queries) - resolve.1;
        let partial = dirty.partial.clone();
        Metrics::add(
            &self.metrics.units_refreshed,
            (units.len() + partial.iter().map(UnitSet::len).sum::<usize>()) as u64,
        );
        // Unit filters bind one placeholder per unit column.
        let per = |n: usize| {
            self.cfg
                .query_chunk
                .min(MAX_FILTER_PARAMS / n.max(1))
                .max(1)
        };
        let mut chunks = units.chunks(per(plan.unit_exprs.len()));
        // Partial units: each chunk recomputes every unit matching its keys.
        for (at, keys) in plan.partial.sets.iter().zip(&partial) {
            chunks.extend(
                keys.chunks(per(at.len()))
                    .into_iter()
                    .map(|c| UnitChunk::Partial(at.clone(), Box::new(c))),
            );
        }
        for chunk in chunks {
            self.metrics.beat();
            let rows = mv::read_source(
                &mut self.src,
                plan,
                layout,
                Some(&chunk),
                &self.metrics,
                self.caps.fresh_snapshot,
            )
            .await?;
            work.source_queries += 1;
            // One conversion per chunk, shared by the chunk's targets (see `mv::apply_prepared`).
            let new = mv::prepare(layout, rows);
            for t in targets {
                if failed.iter().any(|(f, _)| f == t) {
                    continue;
                }
                match self
                    .apply_target(t, table, layout, Some(&chunk), &new)
                    .await
                {
                    Ok(a) => work.rows.applied(&a),
                    Err(e) => failed.push((t.clone(), e)),
                }
            }
        }
        let n = dirty.images.len();
        *dirty = ViewDirty {
            images: vec![KeySet::default(); n],
            rebuild,
            ..Default::default()
        };
        Ok(Refreshed {
            units,
            partial,
            all: false,
            failed,
            work,
        })
    }

    /// The part of a supervised [`Self::refresh`] that needs the engine: the lookups (on the
    /// engine's own connections) and the job for the supervisor's pools (see
    /// `src/supervisor.rs`), run by [`run_pending`] and completed by [`finish_refresh`]. A
    /// target without a working control connection (write limits, sql_mode) fails right away.
    async fn prepare_refresh(
        &mut self,
        plan: &Arc<ViewPlan>,
        layout: &Layout,
        dirty: &mut ViewDirty,
        targets: &[String],
        table: &str,
        recent: &mut Lookback,
        since: Option<&Position>,
    ) -> Result<PendingRefresh> {
        let mut p = PendingRefresh {
            global: matches!(plan.mode, Mode::Global),
            rebuild: dirty.rebuild,
            ..Default::default()
        };
        if p.global {
            if !dirty.all {
                p.idle = true;
                return Ok(p);
            }
            Metrics::add(&self.metrics.units_refreshed, 1);
        } else if dirty.all {
            // The supervised form of the whole-view refresh of a unit-based view (see
            // `refresh`): the read of the whole view and the in-place apply subsume every
            // buffered unit, so no lookups run and the job has no unit filter (`units`
            // stays None: all rows).
            p.whole = true;
            Metrics::add(&self.metrics.units_refreshed, 1);
        } else {
            // `dirty` is only cleared once everything succeeded, so a failure can be retried.
            p.rebuild |= dirty.rebuild;
            let mut units = dirty.units.clone();
            let resolve = (units.len(), Metrics::get(&self.metrics.lookup_queries));
            self.lookups(
                plan,
                layout,
                &dirty.images,
                (targets, table),
                (recent, since),
                &mut units,
            )
            .await?;
            self.presence_lookups(
                plan,
                layout,
                (&dirty.marks, &dirty.holds),
                (targets, table),
                &mut units,
            )
            .await?;
            p.work.ids_resolved = (units.len() - resolve.0) as u64;
            p.work.lookup_queries = Metrics::get(&self.metrics.lookup_queries) - resolve.1;
            p.partial = dirty.partial.clone();
            Metrics::add(
                &self.metrics.units_refreshed,
                (units.len() + p.partial.iter().map(UnitSet::len).sum::<usize>()) as u64,
            );
            p.units = Some(units);
        }
        let mut jobs = Vec::new();
        for t in targets {
            if let Err(e) = self.target_conn(t).await {
                p.failed.push((t.clone(), e.context(format!("target {t}"))));
                continue;
            }
            let target = &self.targets[t.as_str()];
            let (Some(write), Some(mode)) = (target.write, target.sql_mode.clone()) else {
                p.failed.push((
                    t.clone(),
                    anyhow!("internal: target {t} has no write settings"),
                ));
                continue;
            };
            let (opts, schema) = (target.opts.clone(), target.schema.clone());
            let endpoint = self
                .sup
                .as_ref()
                .expect("supervised refresh")
                .write_endpoint(t, target.kind, &opts, &mode, self.cfg.lock_wait_s);
            jobs.push(supervisor::TargetJob {
                name: t.clone(),
                kind: target.kind,
                schema,
                write,
                endpoint,
            });
        }
        if !jobs.is_empty() {
            p.job = Some(supervisor::Job {
                plan: plan.clone(),
                layout: Arc::new(layout.clone()),
                table: table.to_string(),
                targets: jobs,
                // Unit filters bind one placeholder per unit column.
                max_chunk: self
                    .cfg
                    .query_chunk
                    .min(MAX_FILTER_PARAMS / plan.unit_exprs.len().max(1))
                    .max(1),
                timeout: self.cfg.target_timeout,
                fresh: self.caps.fresh_snapshot,
            });
        }
        self.metrics.beat();
        Ok(p)
    }

    /// The live refreshes of every slot at once (supervised): each view's lookups in turn, then
    /// every view's chunk reads and writes together on the shared pools, then each result.
    async fn refresh_all_live(&mut self) {
        let mut started = Vec::new();
        for i in 0..self.slots.len() {
            let Some(mut st) = self.live_begin(i).await else {
                continue;
            };
            let p = self
                .prepare_refresh(
                    &st.plan,
                    &st.layout,
                    &mut st.dirty,
                    &st.targets,
                    &st.name,
                    &mut st.recent,
                    st.since.as_ref(),
                )
                .await;
            started.push((i, st, p));
        }
        if started.is_empty() {
            return;
        }
        let sup = self.sup.as_ref().expect("supervised refresh");
        let outs = futures_util::future::join_all(started.iter_mut().map(|(_, _, p)| async move {
            match p {
                Ok(p) => Some(run_pending(sup, p).await),
                Err(_) => None,
            }
        }))
        .await;
        self.metrics.beat();
        for ((i, mut st, p), out) in started.into_iter().zip(outs) {
            let r = match (p, out) {
                (Ok(p), Some(out)) => finish_refresh(&mut st.dirty, p, out),
                (Err(e), _) => Err(e),
                (Ok(_), None) => unreachable!("every prepared refresh ran"),
            };
            self.live_end(i, st, r).await;
        }
    }

    /// Executor (a): the dedup core's due jobs until none is due (a failed job's lane holds it
    /// back, so the loop ends). One job at a time; each job's refresh runs its chunks
    /// concurrently when supervised.
    async fn flush_dedup(&mut self) {
        loop {
            let jobs = self.dedup.core.due(Instant::now());
            if jobs.is_empty() {
                return;
            }
            for job in jobs {
                self.run_dedup_job(job).await;
            }
        }
    }

    /// One job: its hop items answered on the source (the flush's fence covers `Chunk::high`),
    /// then its ids refreshed on every target database of the version through `refresh`, as
    /// executor (b) refreshes units.
    async fn run_dedup_job(
        &mut self,
        job: crate::dedup::Job<Position, crate::dedup::NoItem, crate::dedup::NoJob>,
    ) {
        let chunk = match job.kind {
            crate::dedup::JobKind::Flush(c) => c,
            crate::dedup::JobKind::Executor(n) => match n {},
        };
        let fail_early = |s: &mut Self, job_id| {
            s.dedup.core.complete(
                job_id,
                crate::dedup::JobResult::failed(Some(Instant::now() + Duration::from_millis(100))),
            );
        };
        let Some(def_id) = self.dedup.def_of(job.target) else {
            fail_early(self, job.id);
            return;
        };
        let Some(i) = self.idx_of(def_id) else {
            fail_early(self, job.id);
            return;
        };
        // One authority per view (see `on_core`): a refused plan never took the rows at all, and
        // a refused target still draining keeps its entry meanwhile. The core's items are then
        // its own bookkeeping: its hop queries still run (the queue must drain before the entry
        // is replaced), nothing is applied. A frozen or swapping version is NOT (b)'s turn: its
        // items fail early and retry after the swap, never drained unapplied.
        let taken = self.dedup.entry(def_id).is_some_and(|_| {
            self.dedup.is_refused(def_id)
                || self
                    .slots
                    .get(i)
                    .and_then(|s| s.live.as_ref())
                    .is_some_and(|l| !l.frozen && !self.dedup.plan_matches(def_id, &l.plan))
        });
        if taken && self.slots[i].live.is_none() {
            self.dedup
                .core
                .complete(job.id, crate::dedup::JobResult::ok());
            return;
        }
        // The version must still be live and not mid-swap (executor (a) never re-plans, F9 b).
        if !taken
            && (!self.slots[i].live.as_ref().is_some_and(|l| !l.frozen)
                || self.slots[i].next.as_ref().is_some_and(|n| n.swapping))
        {
            fail_early(self, job.id);
            return;
        }
        let (plan, layout, name) = {
            let l = self.slots[i].live.as_ref().expect("live, checked above");
            (l.plan.clone(), l.layout.clone(), self.slots[i].name.clone())
        };
        let floor = chunk.origin.clone();
        // The chunk's items name the plan the core registered (a held re-plan keeps the old
        // one): `to_dirty` and the hop lookups use it; the refresh uses the live version.
        let Some(eplan) = self.dedup.entry(def_id).map(|e| e.plan.clone()) else {
            fail_early(self, job.id);
            return;
        };
        let (mut dirty, links) = crate::exec::Dedup::to_dirty(&eplan, chunk);
        let (mut answers, mut queries, mut resolved) = (Vec::new(), 0u64, 0u64);
        if !links.is_empty() {
            match self
                .dedup
                .hop_answers(
                    &mut self.src,
                    &links,
                    self.cfg.query_chunk,
                    self.flavor == Flavor::MariaDb,
                )
                .await
            {
                Ok((a, q)) => {
                    resolved = self.dedup.add_answers(&mut dirty, &a);
                    queries = q;
                    Metrics::add(&self.metrics.lookup_queries, queries);
                    answers = a;
                }
                Err(e) => {
                    if binlog::is_transient(&e) {
                        self.heal_src().await;
                    }
                    warn!(view = %name, "hop queries failed: {e:#}");
                    let delay = self.dedup.backoff(def_id, true);
                    self.dedup.core.complete(
                        job.id,
                        crate::dedup::JobResult::failed(Some(Instant::now() + delay)),
                    );
                    return;
                }
            }
        }
        if taken {
            // Executor (b) wrote these rows with the version's own plan: the core kept only
            // its hop answers.
            self.dedup.core.complete(
                job.id,
                crate::dedup::JobResult {
                    hop_answers: answers,
                    resolved,
                    ..crate::dedup::JobResult::ok()
                },
            );
            return;
        }
        let (lagging, targets) = {
            let l = self.slots[i].live.as_ref().expect("live, checked above");
            let lagging: Vec<String> = l.lag.keys().cloned().collect();
            let targets: Vec<String> = l
                .targets
                .iter()
                .filter(|t| !lagging.contains(t))
                .cloned()
                .collect();
            (lagging, targets)
        };
        let r = self
            .refresh(
                &plan,
                &layout,
                &mut dirty,
                &targets,
                &name,
                &mut Lookback::default(),
                None,
            )
            .await;
        match r {
            Ok(mut r) => {
                r.work.ids_resolved += resolved;
                r.work.lookup_queries += queries;
                self.note_refresh(i, &name, r, &lagging, floor, &targets);
                self.refreshed.insert(name.clone());
                self.dedup.backoff(def_id, false);
                let shown = std::mem::take(&mut self.slots[i].refresh_failures)
                    >= REFRESH_ERROR_SHOWN_AFTER;
                if shown {
                    self.show_refresh_error(i, None).await;
                }
                self.dedup.core.complete(
                    job.id,
                    crate::dedup::JobResult {
                        hop_answers: answers,
                        resolved,
                        ..crate::dedup::JobResult::ok()
                    },
                );
            }
            Err(e) => {
                // Never `schedule_replan`, never a rebuild (F9 b: executor (a) retries failed
                // jobs with backoff). The shared failure counter still shows the error in
                // `_lazyview_state` after `REFRESH_ERROR_SHOWN_AFTER` attempts, as (b) does.
                if binlog::is_transient(&e) {
                    self.heal_src().await;
                }
                let delay = self.dedup.backoff(def_id, true);
                let s = &mut self.slots[i];
                s.refresh_failures += 1;
                let n = s.refresh_failures;
                Metrics::add(&self.metrics.refresh_errors, 1);
                warn!(view = %name, attempt = n, "refresh failed: {e:#}; retrying in {delay:?}");
                if n == REFRESH_ERROR_SHOWN_AFTER {
                    let msg = format!(
                        "refresh failed {n} times in a row, retrying with backoff (the table is \
                         stale meanwhile; rebuild = 1 in _lazyview re-plans the view): {e:#}"
                    );
                    self.show_refresh_error(i, Some(&msg)).await;
                }
                self.dedup.core.complete(
                    job.id,
                    crate::dedup::JobResult::failed(Some(Instant::now() + delay)),
                );
            }
        }
    }

    /// Writes the new rows of a chunk (all rows: `None`) to `table` on target `t`. `new` is the
    /// chunk's rows converted once by [`mv::prepare`], shared by every target of the chunk.
    async fn apply_target(
        &mut self,
        t: &str,
        table: &str,
        layout: &Layout,
        chunk: Option<&UnitChunk>,
        new: &mv::Prepared,
    ) -> Result<mv::Applied> {
        let ts = self.targets[t].schema.clone();
        let metrics = self.metrics.clone();
        let timeout = self.cfg.target_timeout;
        self.target_conn(t)
            .await
            .with_context(|| format!("target {t}"))?;
        let target = self.targets.get_mut(t).unwrap();
        let (Some(c), Some(w)) = (target.conn.as_mut(), target.write) else {
            bail!("internal: target {t} has no connection");
        };
        let r = match tokio::time::timeout(
            timeout,
            mv::apply_prepared_on(c, &ts, table, layout, chunk, new, &w, &metrics),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(anyhow!("timed out after {timeout:?}")),
        };
        r.map_err(|e| {
            // Drop the connection so the next attempt reconnects.
            self.targets.get_mut(t).unwrap().conn = None;
            e.context(format!("target {t}"))
        })
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
    /// However many tables, combinations or statements that takes, they run one after the other
    /// (owner rule: a lookup over its limits is chunked, never a rebuild).
    async fn lookups(
        &mut self,
        plan: &ViewPlan,
        layout: &Layout,
        images: &[KeySet],
        (targets, table): (&[String], &str),
        (recent, since): (&mut Lookback, Option<&Position>),
        units: &mut UnitSet,
    ) -> Result<()> {
        let fresh = |a: usize| images.get(a).is_some_and(|s| !s.is_empty());
        for a in (0..plan.aliases.len()).filter(|&a| fresh(a)) {
            if plan.lineage_of(a).is_some() {
                self.lineage_lookup(plan, layout, a, &images[a], targets, table, units)
                    .await?;
            }
        }
        // (Unit, lineage and partial-unit aliases have no images to combine.)
        let plain: Vec<usize> = (0..plan.aliases.len())
            .filter(|&a| {
                plan.aliases[a].direct_unit.is_none()
                    && plan.lineage_of(a).is_none()
                    && plan.partial_of(a).is_none()
            })
            .collect();
        if plain.len() < 2 {
            for &a in plain.iter().filter(|&&a| fresh(a)) {
                self.lookup(plan, &[(a, &images[a])], units).await?;
            }
            return Ok(());
        }
        let combos = recent.combinations(&plain, images);
        for set in combos.sets() {
            let parts: Vec<(usize, &KeySet)> = match set.as_slice() {
                [a] => vec![(*a, &images[*a])],
                _ => set.iter().map(|&a| (a, &combos.all[a])).collect(),
            };
            self.lookup(plan, &parts, units).await?;
        }
        if plain.iter().any(|&a| fresh(a)) {
            // Every change visible to the lookups above is in the binlog before this position.
            let until = binlog::current_position(&mut self.src).await?;
            let from = since
                .cloned()
                .unwrap_or_else(|| self.boundary.to_position());
            recent.remember(until, from, &plain, images);
        }
        recent.prune(&self.boundary.to_position());
        Ok(())
    }

    /// Units of pinned cascade children whose parent key changed (see `ViewDirty::marks`):
    /// the materialized units a presence column marks for the parent's old key, and the current
    /// units holding rows with a new key (a key update into a pinned value).
    async fn presence_lookups(
        &mut self,
        plan: &ViewPlan,
        layout: &Layout,
        (marks, holds): (&[(usize, Datum)], &[(usize, Datum)]),
        (targets, table): (&[String], &str),
        units: &mut UnitSet,
    ) -> Result<()> {
        for (k, v) in marks {
            let rows = self
                .read_materialized(
                    targets,
                    table,
                    layout,
                    target::MvFilter::Presence {
                        p: &plan.presence[*k],
                        value: v,
                    },
                )
                .await?;
            for r in rows {
                units.insert(r.into_iter().map(Datum::from_value).collect());
            }
        }
        for (k, v) in holds {
            let fresh = self.caps.fresh_snapshot;
            let sql = plan.key_lookup_sql(&plan.presence[*k].holds(v));
            let rows: Vec<mysql_async::Row> =
                mv::exec_source(&mut self.src, sql, Vec::new(), fresh).await?;
            Metrics::add(&self.metrics.lookup_queries, 1);
            for r in rows {
                units.insert(r.unwrap().into_iter().map(Datum::from_value).collect());
            }
        }
        Ok(())
    }

    /// Lineage lookup of alias `alias` (see [`Self::lookups`]): `keys` are primary keys of its
    /// changed rows.
    async fn lineage_lookup(
        &mut self,
        plan: &ViewPlan,
        layout: &Layout,
        alias: usize,
        keys: &KeySet,
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
        for chunk in set.chunks(per) {
            self.metrics.beat();
            // Old rows: the materialized rows that contain them.
            let rows = self
                .read_materialized(
                    targets,
                    table,
                    layout,
                    target::MvFilter::Keys {
                        chunk: &chunk,
                        cols: &k.cols,
                    },
                )
                .await?;
            for r in rows {
                units.insert(r.into_iter().map(Datum::from_value).collect());
            }
            // New rows: the current rows that contain them.
            let (f, p) = chunk.filter(&k.exprs);
            let fresh = self.caps.fresh_snapshot;
            let rows: Vec<mysql_async::Row> =
                mv::exec_source(&mut self.src, plan.key_lookup_sql(&f), p, fresh).await?;
            Metrics::add(&self.metrics.lookup_queries, 1);
            for r in rows {
                units.insert(r.unwrap().into_iter().map(Datum::from_value).collect());
            }
        }
        Ok(())
    }

    /// Runs a read of the materialized table (see [`target::Conn::read_materialized`]) on the
    /// first target of `targets` that answers, `source` first (they hold the same rows). Each
    /// family renders the statement for its own target: the same read, no MySQL-only path
    /// (M-02). One statement per chunk of the lookup, never per row.
    async fn read_materialized(
        &mut self,
        targets: &[String],
        table: &str,
        layout: &Layout,
        filter: target::MvFilter<'_>,
    ) -> Result<Vec<Vec<Value>>> {
        let mut order: Vec<&String> = targets.iter().filter(|t| *t == SOURCE_TARGET).collect();
        order.extend(targets.iter().filter(|t| *t != SOURCE_TARGET));
        let mut last = None;
        for t in order {
            let ts = self.targets[t.as_str()].schema.clone();
            let timeout = self.cfg.target_timeout;
            let r = match self.target_conn(t).await {
                Ok(c) => {
                    match tokio::time::timeout(
                        timeout,
                        c.read_materialized(&ts, table, layout, filter),
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(_) => Err(anyhow!("timed out after {timeout:?}")),
                    }
                }
                Err(e) => Err(e),
            };
            Metrics::add(&self.metrics.mv_queries, 1);
            match r {
                Ok(rows) => return Ok(rows),
                Err(e) if server_code(&e).is_some() && !binlog::is_transient(&e) => {
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
    /// (`SELECT DISTINCT <units>` over the view's FROM/WHERE), one statement per combination of
    /// one chunk of images per alias.
    async fn lookup(
        &mut self,
        plan: &ViewPlan,
        parts: &[(usize, &KeySet)],
        units: &mut UnitSet,
    ) -> Result<()> {
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
            // A bound string, without the `COLLATE` it carries when the view's own definition has
            // one ([`without_collate`]): a `COLLATE` clause is EXPLICIT, so the view's own meets
            // the parameter's in an error ("Illegal mix of collations", 1267). Without it the
            // view's `COLLATE` decides, exactly as it does in the definition. Losing the
            // collation costs nothing here: the lookup only says which units MAY be affected, and
            // a charset's default collation is its coarsest, so it can over-match (a few units
            // refreshed that the changed row cannot move) but never miss one; each unit it finds
            // is then refreshed by reading the real rows under the real collations.
            let bound: Vec<&str> = if plan.has_explicit_collate {
                cols.iter().map(|c| without_collate(&c.param)).collect()
            } else {
                cols.iter().map(|c| c.param.as_str()).collect()
            };
            subs.push(Part {
                alias,
                first: cols
                    .iter()
                    .zip(&bound)
                    .map(|(c, p)| format!("{p} AS {}", analyze::quote_ident(&c.name)))
                    .collect::<Vec<_>>()
                    .join(", "),
                rest: bound.join(", "),
                keys: images.iter().collect(),
                per: (MAX_FILTER_PARAMS / (parts.len() * cols.len()))
                    .clamp(1, self.cfg.query_chunk),
            });
        }
        let parts = subs;
        let counts: Vec<usize> = parts.iter().map(|p| p.keys.len().div_ceil(p.per)).collect();
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
            let fresh = self.caps.fresh_snapshot;
            let rows: Vec<mysql_async::Row> =
                mv::exec_source(&mut self.src, sql, params, fresh).await?;
            Metrics::add(&self.metrics.lookup_queries, 1);
            for r in rows {
                let key: Key = r.unwrap().into_iter().map(Datum::from_value).collect();
                units.insert(key);
            }
            let mut d = 0;
            loop {
                if d == at.len() {
                    return Ok(());
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
    /// longer than [`XA_PIN_MAX`]. The boundary is a real resume point (`Cursor::is_some`), so
    /// it needs no guard here; [`meta::load_position`] is the one place that can hand back no
    /// cursor at all (nothing saved yet, or a row of an older build).
    async fn persist_position(&mut self, force: bool) -> Result<()> {
        // The leadership gate (ruling 1): the saved position is a row of the coordination store,
        // and an instance whose lock it cannot prove does not own it — the replacement writes it
        // under the same instance id. Held back rather than failed: `persist_needed` stays armed,
        // so the save follows the next beat. `meta::save_position`'s `WHERE epoch = ?` stays as the
        // backstop for a save that began while the beat was still believed.
        if self.leader_holds_writes() {
            debug!("position save held: the leadership lock does not allow new writes right now");
            return Ok(());
        }
        let mut pos = self.pending_floor(true);
        for (xid, xa) in self.xa_pending.iter_mut() {
            if !xa.start.is_some() {
                continue;
            }
            if xa.prepared_at.elapsed() > XA_PIN_MAX {
                if !xa.unpinned {
                    xa.unpinned = true;
                    warn!(%xid, start = %xa.start, "XA transaction prepared over {XA_PIN_MAX:?} ago is still unresolved; the saved position no longer waits for it (if it commits after a restart, every view is rebuilt)");
                }
                continue;
            }
            pos = pos.min(xa.start.to_position());
        }
        if let Some((_, xa)) = &self.xa_current {
            if xa.start.is_some() {
                pos = pos.min(xa.start.to_position());
            }
        }
        if !force && (pos == self.persisted.to_position() || !self.persist_needed) {
            // Held back where it was saved: look again one interval later (the flush deadline
            // is `last_persist` + interval; leaving it in the past would spin the loop).
            self.last_persist = Instant::now();
            return Ok(());
        }
        self.persist_needed = true;
        meta::save_position(
            &mut self.meta,
            &self.cfg.instance_id,
            &Cursor::from_position(self.flavor, &pos),
            self.leader.epoch,
        )
        .await?;
        // Held back: save again once the pending changes are applied.
        self.persist_needed = pos < self.boundary.to_position();
        self.persisted = Cursor::from_position(self.flavor, &pos);
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
        // First builds swap in the background (see `finish_next`): wait for those swaps too.
        let swapping = |s: &Slot| s.next.as_ref().is_some_and(|n| n.swapping);
        while self
            .slots
            .iter()
            .any(|s| swapping(s) && s.next.as_ref().is_some_and(|n| n.done.is_none()))
        {
            match rx.recv().await {
                Some(done) => self.on_build_done(done),
                None => break,
            }
        }
        for i in 0..self.slots.len() {
            if swapping(&self.slots[i]) {
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
    /// Partial unit keys refreshed, per set (likewise).
    partial: Vec<UnitSet>,
    /// Global view recomputed.
    all: bool,
    /// Targets whose writes failed.
    failed: Vec<(String, anyhow::Error)>,
    /// Queries and rows written, for the view's statistics.
    work: crate::stats::Work,
}

/// A live refresh between [`Engine::live_begin`] and [`Engine::live_end`].
struct LiveRefresh {
    plan: Arc<ViewPlan>,
    layout: Layout,
    name: String,
    /// The changes taken from the live version (merged back if the refresh fails).
    dirty: ViewDirty,
    targets: Vec<String>,
    lagging: Vec<String>,
    recent: Lookback,
    since: Option<Position>,
    def_id: u64,
    replan_pending: bool,
    full_due: bool,
}

/// A supervised refresh with its lookups done (see [`Engine::prepare_refresh`]).
#[derive(Default)]
struct PendingRefresh {
    /// Chunk reads and writes for the supervisor; None when no target can be written.
    job: Option<supervisor::Job>,
    /// The units (None: the whole view, a global aggregate).
    units: Option<UnitSet>,
    /// Partial unit keys, per set of the plan's partial units.
    partial: Vec<UnitSet>,
    global: bool,
    /// A unit-based view refreshed as a whole (gap window, `accumulate::mark_all`): the job
    /// reads and applies the whole view; the run clears the buffer (see `finish_refresh`).
    whole: bool,
    /// A global view without changes: nothing to do.
    idle: bool,
    rebuild: bool,
    /// Targets that failed before the run.
    failed: Vec<(String, anyhow::Error)>,
    /// Lookups done so far (the run adds its reads and writes).
    work: crate::stats::Work,
}

/// Runs the chunk ops of a prepared refresh on the supervisor's pools.
async fn run_pending(sup: &Supervisor, p: &mut PendingRefresh) -> Result<supervisor::Outcome> {
    match p.job.take() {
        Some(job) if !p.idle => sup.run(job, p.units.as_ref(), &p.partial).await,
        _ => Ok(supervisor::Outcome::default()),
    }
}

/// A bound string param without its trailing ` COLLATE <co>`: only for a param of the shape
/// `CAST(CAST(? AS BINARY) AS CHAR CHARACTER SET <cs>) COLLATE <co>` (a string column,
/// [`crate::catalog::TEXT_PARAM_PREFIX`]), which is the one that carries a collation of its own; a
/// param that has none, and one of any other shape, is returned as it is. See the lookup that
/// uses it.
fn without_collate(param: &str) -> &str {
    match param
        .strip_prefix(crate::catalog::TEXT_PARAM_PREFIX)
        .and_then(|_| param.rfind(" COLLATE "))
    {
        Some(i) => &param[..i],
        None => param,
    }
}

/// The result of a supervised refresh, clearing `dirty` (except a pending rebuild) once it
/// succeeded (see [`Engine::refresh`]).
fn finish_refresh(
    dirty: &mut ViewDirty,
    p: PendingRefresh,
    out: Result<supervisor::Outcome>,
) -> Result<Refreshed> {
    if p.idle {
        return Ok(Refreshed::default());
    }
    let mut failed = p.failed;
    let out = out?;
    failed.extend(out.failed);
    let mut work = p.work;
    work.source_queries += out.chunks as u64;
    work.rows = out.rows;
    if p.global {
        dirty.all = false;
        return Ok(Refreshed {
            units: UnitSet::default(),
            partial: Vec::new(),
            all: true,
            failed,
            work,
        });
    }
    if p.whole {
        // The whole-view read and apply of the run covered every buffered unit, so the
        // buffer clears with it (a failed target is retried as a whole refresh by
        // `retry_lag`); a pending rebuild survives, as on the unit path.
        let n = dirty.images.len();
        *dirty = ViewDirty {
            images: vec![KeySet::default(); n],
            rebuild: p.rebuild,
            ..Default::default()
        };
        return Ok(Refreshed {
            units: UnitSet::default(),
            partial: Vec::new(),
            all: true,
            failed,
            work,
        });
    }
    let n = dirty.images.len();
    *dirty = ViewDirty {
        images: vec![KeySet::default(); n],
        rebuild: p.rebuild,
        ..Default::default()
    };
    Ok(Refreshed {
        units: p.units.unwrap_or_default(),
        partial: p.partial,
        all: false,
        failed,
        work,
    })
}

/// Placeholders per statement (the server limit is 65535).
const MAX_FILTER_PARAMS: usize = 60_000;

/// A lagging target is reported down after this many failed retries.
const LAG_MAX_FAILURES: u32 = 5;

/// One target of a build, with the ownership checks it needs there.
struct BuildSpec {
    target: String,
    opts: target::Opts,
    kind: target::Kind,
    schema: String,
    /// Ownership marker (table comment) of this view's tables: a table carrying it is ours
    /// even without a state row (a crash between the swap and the state update).
    marker: String,
    /// Refuse if `<name>__lv_new` / `__lv_old` exist (not created by lazyview for this view).
    check_side: bool,
    /// Refuse if a table `<name>` exists (one lazyview did not create).
    check_table: bool,
}

/// The first keyword of a statement, after leading comments (`/*! … */` executable comments
/// excluded: their content counts).
fn first_keyword(query: &str) -> &str {
    let mut s = query.trim_start();
    loop {
        if let Some(r) = s
            .strip_prefix("/*")
            .filter(|r| !r.starts_with('!') && !r.starts_with("M!"))
        {
            match r.find("*/") {
                Some(e) => s = r[e + 2..].trim_start(),
                None => return "",
            }
        } else if s.starts_with("--") || s.starts_with('#') {
            match s.find('\n') {
                Some(e) => s = s[e + 1..].trim_start(),
                None => return "",
            }
        } else {
            break;
        }
    }
    let end = s
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(s.len());
    &s[..end]
}

/// The table a DML statement names: the first name after the keyword, past `INTO` / `FROM`.
/// Best effort and never logged — it is only the rate-limit key of
/// [`Engine::note_statement_dml`], so an approximate read is enough; `None` says the limiter
/// should key on the schema alone.
fn statement_target_table(query: &str) -> Option<String> {
    // Past the leading comments, as `first_keyword` does.
    let mut s = query.trim_start();
    loop {
        if let Some(r) = s
            .strip_prefix("/*")
            .filter(|r| !r.starts_with('!') && !r.starts_with("M!"))
        {
            let e = r.find("*/")?;
            s = r[e + 2..].trim_start();
        } else if s.starts_with("--") || s.starts_with('#') {
            s = {
                let e = s.find('\n')?;
                s[e + 1..].trim_start()
            };
        } else {
            break;
        }
    }
    let mut words = s.split_whitespace();
    words.next()?; // the DML keyword, checked by the caller
    let mut w = words.next()?;
    if w.eq_ignore_ascii_case("INTO") || w.eq_ignore_ascii_case("FROM") {
        w = words.next()?;
    }
    // `t(`, `t;`, `t,` end the name; backticks and quotes are kept off, qualifiers are not read.
    let name = w
        .split(['(', ';', ','])
        .next()
        .unwrap_or("")
        .trim_matches(|c| c == '`' || c == '"' || c == '\'');
    (!name.is_empty()).then(|| name.to_owned())
}

/// The rate limit of [`Engine::note_statement_dml`] for one (schema, table) entry: `Some(count)`
/// when a line is due, `count` being the events it reports (this one plus those folded since the
/// last line, which the entry forgets as it reports them), and `None` when the event is folded
/// into the next line for that table. `None` for the time of the last line means "none logged
/// yet", so the first event always logs and no arithmetic on the clock is needed to say so:
/// `now - STMT_DML_LOG_INTERVAL` panics when the result would fall before the monotonic clock's
/// origin — on Linux the boot, so a host up for less than the interval panicked on its first
/// statement-format DML (review C-03). Split out of the method so the unit test can drive it
/// without a live engine.
fn stmt_dml_gate(slot: &mut (Option<Instant>, u64), now: Instant) -> Option<u64> {
    if let Some(last) = slot.0 {
        if now.duration_since(last) < STMT_DML_LOG_INTERVAL {
            slot.1 += 1; // folded into the next line for this table
            return None;
        }
    }
    let count = slot.1 + 1;
    slot.0 = Some(now);
    slot.1 = 0;
    Some(count)
}

/// Swaps the shadow tables of a routine rebuild or a first build in, off the binlog loop (see
/// `finish_next`); `Some(marker)`: first check that no table lazyview did not create appeared
/// under the view's name. A target where that fails is left to the engine, which checks again
/// and retries it as for any build.
async fn swap_task(
    targets: Vec<(String, target::Opts, String, target::Kind, Option<String>)>,
    name: String,
    layout: Layout,
    tx: mpsc::UnboundedSender<BuildDone>,
    (def_id, generation): (u64, u64),
    lock_wait_s: u64,
    swap_lock_wait_s: u64,
    metrics: Arc<Metrics>,
) {
    let mut swapped = Vec::new();
    let mut lock_wait = Vec::new();
    for (t, opts, schema, kind, check) in targets {
        let r = async {
            let mut c = match tokio::time::timeout(
                TARGET_CONNECT_TIMEOUT * 3,
                connect_target(&opts, kind, lock_wait_s, &metrics),
            )
            .await
            {
                Ok(r) => r?,
                Err(_) => bail!("connecting to target {t}: timed out"),
            };
            if let Some(marker) = &check {
                if foreign_table(&mut c, &schema, &name, marker).await {
                    let _ = c.disconnect().await;
                    bail!("a table `{name}` appeared during the build");
                }
            }
            // A reader holding the old table's metadata lock: the swap waits for it only its
            // own operator setting, `swap_lock_wait_s`, a bounded number of tries
            // (Policy::SWAP, N-01); the "already swapped" check makes a repeat a no-op (no
            // second loop here). A lock-wait failure is remembered so the binlog loop never
            // retries this target (N-02): it is left to the next rebuild.
            let r = target::swap(
                &mut c,
                kind,
                &schema,
                &name,
                swap_lock_wait_s,
                lock_wait_s,
                false,
                &metrics,
            )
            .await;
            let _ = c.disconnect().await;
            r
        }
        .await;
        match r {
            Ok(()) => swapped.push(t),
            Err(e) if target::is_lock_wait_timeout(&e) => {
                debug!(view = %name, target = %t, "the swap's lock wait timed out; left to the next rebuild");
                lock_wait.push(t);
            }
            Err(e) => debug!(view = %name, target = %t, "swap left to the engine: {e:#}"),
        }
    }
    let _ = tx.send(BuildDone {
        def_id,
        generation,
        result: Ok(Built {
            layout,
            failed: Vec::new(),
            swapped,
            lock_wait,
            cache: cascade::Cache::default(),
        }),
    });
}

/// Whether a table `name` exists that lazyview did not create for this view (`marker`, see
/// [`target::marker`]): a swap must never replace it.
async fn foreign_table(c: &mut target::Conn, schema: &str, name: &str, marker: &str) -> bool {
    c.table_kind(schema, name).await.ok().flatten().is_some()
        && c.table_marker(schema, name).await.ok().flatten().as_deref() != Some(marker)
}

/// Connects to a build target, runs its ownership checks and creates the shadow table.
async fn prepare_target(
    spec: &BuildSpec,
    name: &str,
    layout: &Layout,
    (batch_rows, batch_bytes): (usize, usize),
    metrics: &Metrics,
) -> Result<(target::Conn, mv::WriteOpts)> {
    let (t, ts) = (&spec.target, &spec.schema);
    let mut c = match tokio::time::timeout(
        TARGET_CONNECT_TIMEOUT * 3,
        connect_target(
            &spec.opts,
            spec.kind,
            crate::config::DEFAULT_LOCK_WAIT_S,
            metrics,
        ),
    )
    .await
    {
        Ok(r) => r.with_context(|| format!("connecting to target {t}"))?,
        Err(_) => bail!("connecting to target {t}: timed out"),
    };
    // The batch limits come from the family's own connection (engine.rs:2232's pattern): the
    // MySQL `max_packet/2` cap or PostgreSQL's rows-and-bytes-only batching (T1.5).
    let w = match &mut c {
        target::Conn::Mysql(mc) => {
            target::write_opts_with(mc, spec.kind, batch_rows, batch_bytes, metrics).await
        }
        target::Conn::Postgres(pc) => pc.write_opts(spec.kind, batch_rows, batch_bytes).await,
    }
    .with_context(|| format!("target {t}"))?;
    let ours = |m: Option<String>| m.as_deref() == Some(spec.marker.as_str());
    if spec.check_side {
        for side in [target::shadow_name(name), target::old_name(name)] {
            if c.table_kind(ts, &side).await?.is_some() && !ours(c.table_marker(ts, &side).await?) {
                bail!("refusing to build `{name}`: table `{ts}`.`{side}` on target {t} exists and was not created by lazyview for this view");
            }
        }
    }
    if spec.check_table {
        if let Some(k) = c.table_kind(ts, name).await? {
            if !ours(c.table_marker(ts, name).await?) {
                bail!(
                    "refusing to replace existing {} `{ts}`.`{name}` on target {t} that lazyview does not manage",
                    k.to_lowercase()
                );
            }
        }
    }
    target::create_shadow_marked(&mut c, spec.kind, ts, name, layout, &spec.marker, metrics)
        .await
        .with_context(|| format!("creating the shadow table on target {t}"))?;
    Ok((c, w))
}

/// Seeds this build's in-memory cascade cache from the source before the table is filled: a
/// cascade after this read is in the binlog this version replays, so none falls between the
/// two reads.
/// The startup seed of the cascade cache together with the binlog position its own consistent
/// read includes (issue #167): the seed queries and the position read are ONE transaction, so the
/// parent changes that read never saw are exactly the ones from before that position, and a change
/// committed while the seed was reading replays from its own images instead of counting as a gap.
/// `None` is the server reporting no snapshot position (`source::maria::snapshot_position`), where the caller
/// keeps the wider window of the position read after the seed.
async fn seed_cache_at_snapshot(
    conn: &mut Conn,
    plan: &ViewPlan,
) -> Result<(cascade::Cache, Option<Position>)> {
    // The session reads with READ COMMITTED; the snapshot needs REPEATABLE READ.
    conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await?;
    // The commit must run whatever the seed or the position read did: an open snapshot would
    // poison the session for every later read the engine makes on it.
    let cache = match seed_cache(conn, plan).await {
        Ok(cache) => cache,
        Err(e) => {
            let _ = conn.query_drop("COMMIT").await;
            return Err(e);
        }
    };
    let pos = match crate::source::maria::snapshot_status(conn).await {
        Ok(pos) => pos,
        Err(e) => {
            let _ = conn.query_drop("COMMIT").await;
            return Err(e);
        }
    };
    conn.query_drop("COMMIT")
        .await
        .context("committing the cascade seed transaction")?;
    Ok((cache, pos))
}

async fn seed_cache(conn: &mut Conn, plan: &ViewPlan) -> Result<cascade::Cache> {
    let mut cache = cascade::Cache::default();
    for (ci, c) in plan.cascades.iter().enumerate() {
        let Some(sql) = cascade::seed_sql(c) else {
            continue;
        };
        let mut result = conn
            .exec_iter(sql, ())
            .await
            .context("seeding the cascade cache")?;
        while let Some(row) = result.next().await? {
            let vals = row.unwrap();
            let (key, kept) = cascade::seed_row(c, vals);
            cache.put(ci, key, kept);
        }
    }
    Ok(cache)
}

async fn build_task(
    src_opts: Opts,
    src_schema: String,
    targets: Vec<BuildSpec>,
    plan: Arc<ViewPlan>,
    name: String,
    batch: (usize, usize),
    lock_wait_s: u64,
    swap_lock_wait_s: u64,
    metrics: Arc<Metrics>,
    tx: mpsc::UnboundedSender<BuildDone>,
    slots: Arc<tokio::sync::Semaphore>,
    (def_id, generation): (u64, u64),
    swap: bool,
    test_delay: Option<Duration>,
) {
    let result = async {
        let _permit = slots.acquire_owned().await?;
        if let Some(d) = test_delay {
            tokio::time::sleep(d).await;
        }
        let mut src = connect_source(&src_opts, lock_wait_s, &metrics).await?;
        let mut layout = mv::probe(&mut src, &src_schema, &plan).await?;
        // The index budget of this view's tables: the tightest of the targets it is built into
        // (T3.1); a MySQL-only view keeps the 3072 bytes every build used before PostgreSQL targets.
        layout.key_budget = target::min_key_budget(targets.iter().map(|s| s.kind));
        // The cascade cache is read on the source before the table is filled, so that no
        // cascade falls between the two reads: the binlog this version replays holds the rest.
        let cache = seed_cache(&mut src, &plan).await?;
        // Each target is prepared on its own: one that fails is reported, the others built.
        let mut conns = Vec::new();
        let mut failed = Vec::new();
        for spec in &targets {
            match prepare_target(spec, &name, &layout, batch, &metrics).await {
                Ok((c, w)) => conns.push((c, spec.schema.clone(), w)),
                Err(e) => failed.push((spec.target.clone(), e)),
            }
        }
        if !conns.is_empty() {
            let mut bt: Vec<BuildTarget> = Vec::new();
            for (c, s, w) in conns.iter_mut() {
                bt.push(BuildTarget {
                    conn: c,
                    schema: s.clone(),
                    write: *w,
                });
            }
            let n = mv::fill(&mut src, &plan, &layout, &mut bt, &name, batch.0, &metrics).await?;
            debug!(view = %name, rows = n, "build finished");
        }
        // `swap`: swap the shadow tables in right away. A target where that fails is left to
        // the engine, which swaps (and retries) the rest as for any build.
        let mut swapped = Vec::new();
        let mut lock_wait = Vec::new();
        for ((c, ts, _), spec) in conns.iter_mut().zip(
            targets
                .iter()
                .filter(|s| !failed.iter().any(|(t, _)| *t == s.target)),
        ) {
            if !swap {
                break;
            }
            if spec.check_table && foreign_table(c, ts, &name, &spec.marker).await {
                debug!(view = %name, target = %spec.target, "swap left to the engine: a table appeared during the build");
                continue;
            }
            match target::swap(c, spec.kind, ts, &name, swap_lock_wait_s, lock_wait_s, false, &metrics).await {
                Ok(()) => swapped.push(spec.target.clone()),
                Err(e) if target::is_lock_wait_timeout(&e) => {
                    // The swap's own lock wait ran out (off the binlog loop, bounded):
                    // remember the target so the loop does not retry it (N-02); the engine
                    // reports it and leaves the swap to the next rebuild.
                    debug!(view = %name, target = %spec.target, "the swap's lock wait timed out; left to the next rebuild");
                    lock_wait.push(spec.target.clone());
                }
                Err(e) => {
                    debug!(view = %name, target = %spec.target, "swap left to the engine: {e:#}")
                }
            }
        }
        for (c, _, _) in conns {
            let _ = c.disconnect().await;
        }
        let _ = src.disconnect().await;
        Ok::<_, anyhow::Error>(Built {
            layout,
            failed,
            swapped,
            lock_wait,
            cache,
        })
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

#[cfg(test)]
mod role_dsn_tests {
    use super::*;

    /// The three source roles the network tests aim a fault at (chaos plan F4): unset ⇒ the
    /// service's own endpoint (a run without the hooks opens nothing new), set ⇒ that role's
    /// endpoint alone (a Toxiproxy listener in front of the same server).
    #[test]
    fn each_source_role_connects_where_its_hook_points() {
        let dsn = "mysql://lv:p@db:3306/app";
        for role in [None, Some(dsn)] {
            let o = role_opts(role, dsn).unwrap();
            assert_eq!((o.ip_or_hostname(), o.tcp_port()), ("db", 3306));
            assert_eq!(
                o.user(),
                Some("lv"),
                "the role keeps the service's credentials"
            );
        }
        let o = role_opts(Some("mysql://lv:p@127.0.0.1:3307/app"), dsn).unwrap();
        assert_eq!(
            (o.ip_or_hostname(), o.tcp_port()),
            ("127.0.0.1", 3307),
            "only the role whose hook is set moves"
        );
        // A role's DSN goes through the MySQL-only replica parser, as LAZYVIEW_DSN's does:
        // `Config` refuses a `postgres://` one earlier, with its own message.
        assert!(role_opts(Some("not a dsn"), dsn).is_err());
        assert!(role_opts(Some("postgres://a@b/d"), dsn).is_err());
    }

    /// The lock's own endpoint (review A-06): unset, it is the metadata server's, because the lock
    /// and the `_lazyview_status` epoch row it fences must stay together; set, the network test
    /// aims its fault at the lock's session alone.
    #[test]
    fn the_lock_role_follows_the_metadata_role_until_it_is_named() {
        let dsn = "mysql://lv:p@db:3306/app";
        let meta = "mysql://lv:p@127.0.0.1:3311/app";
        let lock = "mysql://lv:p@127.0.0.1:3312/app";
        let of = |test_lock: Option<&str>, test_meta: Option<&str>| {
            role_opts(test_lock.or(test_meta), dsn).unwrap()
        };
        // Nothing set: the lock sits where LAZYVIEW_DSN sits, like every other role.
        let o = of(None, None);
        assert_eq!((o.ip_or_hostname(), o.tcp_port()), ("db", 3306));
        // The metadata hook alone moves the lock too — the epoch row moved, so the lock follows.
        let o = of(None, Some(meta));
        assert_eq!((o.ip_or_hostname(), o.tcp_port()), ("127.0.0.1", 3311));
        // The lock's own hook wins, and moves nothing else.
        let o = of(Some(lock), Some(meta));
        assert_eq!((o.ip_or_hostname(), o.tcp_port()), ("127.0.0.1", 3312));
    }
}

#[cfg(test)]
mod lock_task_tests {
    use super::*;
    use crate::supervisor::LeaderGate;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One scripted answer of the lock session's beat.
    #[derive(Debug, Clone, Copy)]
    enum Script {
        /// Answered, and the lock is still ours.
        Held,
        /// Answered, and another session holds the lock now.
        Taken,
        /// No answer within the beat's bound: the session is gone and `rejoin` is due.
        Lost,
    }

    /// What a rejoin after a lost session answers.
    #[derive(Debug, Clone)]
    enum Rejoin {
        /// The lock server cannot be reached (a transient error): the beat is retried, the
        /// instance keeps its lock for now.
        Away,
        /// The lock is held elsewhere or the epoch moved: this instance is the replaced one.
        Replaced(&'static str),
        /// The lock and the same epoch came back: still the leader.
        Held,
    }

    /// The [`LockBeat`] seam driven by a script: the lock task's whole state machine, with no
    /// server behind it.
    struct Scripted {
        beats: VecDeque<Script>,
        rejoins: VecDeque<Rejoin>,
        /// Closed when the script is through — the way the engine's drop ends the task.
        stop: Option<watch::Sender<bool>>,
        beats_done: Arc<AtomicUsize>,
        rejoins_done: Arc<AtomicUsize>,
    }

    impl LockBeat for Scripted {
        async fn beat(&mut self) -> Result<Beat> {
            self.beats_done.fetch_add(1, Ordering::Relaxed);
            match self.beats.pop_front() {
                None => {
                    drop(self.stop.take());
                    Ok(Beat::Held)
                }
                Some(Script::Held) => Ok(Beat::Held),
                Some(Script::Taken) => Ok(Beat::Taken(
                    "another session holds the leadership lock `lazyview:7` while we hold epoch 2"
                        .to_string(),
                )),
                Some(Script::Lost) => Err(anyhow!(
                    "the lock session did not answer a beat within 20 ms"
                )),
            }
        }

        async fn rejoin(&mut self) -> Result<()> {
            self.rejoins_done.fetch_add(1, Ordering::Relaxed);
            match self.rejoins.pop_front() {
                None => Ok(()),
                Some(Rejoin::Away) => Err(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "the lock server is away",
                ))),
                Some(Rejoin::Replaced(why)) => Err(anyhow!(why)),
                Some(Rejoin::Held) => Ok(()),
            }
        }
    }

    /// The [`WriteFence`] seam: counts the times the lock task asked for the writes in flight to be
    /// ended, and says that no server confirmed any of them gone (which is what a fence that has to
    /// park its ids looks like to the caller).
    struct ScriptedFence {
        stops: Arc<AtomicUsize>,
    }

    impl WriteFence for ScriptedFence {
        async fn stop_writes(&self) -> usize {
            self.stops.fetch_add(1, Ordering::Relaxed) + 1
        }
    }

    struct Run {
        state: LeaderState,
        metrics: Arc<Metrics>,
        beats: usize,
        rejoins: usize,
        /// Times the lock task asked for the in-flight writes to be ended.
        fences: usize,
        /// Every state the task published, in order (the final one is `state`).
        published: Vec<LeaderState>,
    }

    /// Runs the lock task over a script and reports what it left behind. The task ends by itself
    /// when it fences, and with the script otherwise (the driver closes the stop channel), so a
    /// run that needs neither would hang and fails on the bound below instead.
    fn drive(beats: &[Script], rejoins: &[Rejoin]) -> Run {
        let (stop_tx, stop_rx) = watch::channel(false);
        let (beats_done, rejoins_done) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let d = Scripted {
            beats: beats.iter().copied().collect(),
            rejoins: rejoins.iter().cloned().collect(),
            stop: Some(stop_tx),
            beats_done: beats_done.clone(),
            rejoins_done: rejoins_done.clone(),
        };
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = watch::channel(LeaderState::Active);
        let (_sd, sd_rx) = watch::channel(false);
        let stops = Arc::new(AtomicUsize::new(0));
        let published = Arc::new(std::sync::Mutex::new(Vec::new()));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let ended = rt
            .block_on(async {
                // The history of what the task published: the last value of a `watch` says nothing
                // about the states the gates stood closed in between.
                let rec = {
                    let published = published.clone();
                    let mut rec = tx.subscribe();
                    tokio::spawn(async move {
                        while rec.changed().await.is_ok() {
                            published
                                .lock()
                                .unwrap()
                                .push(rec.borrow_and_update().clone());
                        }
                    })
                };
                let r = tokio::time::timeout(
                    Duration::from_secs(5),
                    run_lock_beat(
                        d,
                        Duration::from_millis(5),
                        metrics.clone(),
                        tx,
                        Arc::new(ScriptedFence {
                            stops: stops.clone(),
                        }),
                        sd_rx,
                        stop_rx,
                    ),
                )
                .await;
                // The recorder ends with the sender; give it its last look before the runtime
                // goes, so a state published on the way out is not missed.
                let _ = tokio::time::timeout(Duration::from_millis(100), rec).await;
                r
            })
            .is_ok();
        assert!(
            ended,
            "the lock task must end: fenced, or with the script and its stop"
        );
        let state = (*rx.borrow()).clone();
        let states = std::mem::take(&mut *published.lock().unwrap());
        Run {
            state,
            metrics,
            beats: beats_done.load(Ordering::Relaxed),
            rejoins: rejoins_done.load(Ordering::Relaxed),
            fences: stops.load(Ordering::Relaxed),
            published: states,
        }
    }

    /// A healthy instance: every beat answered, the state stays `Active`, the beat clock moves,
    /// and nothing is counted as a failure.
    #[test]
    fn a_beat_answers_and_we_stay_the_leader() {
        let r = drive(&[Script::Held, Script::Held, Script::Held], &[]);
        assert_eq!(r.state, LeaderState::Active);
        assert_eq!(
            r.beats, 4,
            "the script's three beats, plus the one that finds it through and ends the task"
        );
        assert_eq!(r.rejoins, 0, "no rejoin without a lost session");
        assert_eq!(Metrics::get(&r.metrics.leader_beat_failures), 0);
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 0);
        assert!(
            Metrics::get(&r.metrics.gauges.leader_last_beat_ok) > 0,
            "the beat marks the lock's own clock"
        );
    }

    /// The beat answers and the lock is someone else's: fenced at once, counted, and the reason
    /// names the lock and our epoch for the log and the engine's fatal error.
    #[test]
    fn a_beat_that_says_the_lock_is_taken_elsewhere_fences_us() {
        let r = drive(&[Script::Held, Script::Taken], &[]);
        let why = match &r.state {
            LeaderState::Fenced(why) => why.clone(),
            other => panic!("expected Fenced, got {other:?}"),
        };
        assert!(
            why.contains("lazyview:7") && why.contains("epoch 2"),
            "{why}"
        );
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 1);
        assert_eq!(r.beats, 2, "the task stops beating once it has fenced us");
        assert_eq!(
            r.rejoins, 0,
            "an answered beat that says `taken` needs no rejoin"
        );
    }

    /// A session that dies and comes back with the same epoch (a GC pause, a proxy blip): the
    /// instance keeps leading, with the failure counted and no fence — the case condition 2 is
    /// about, and the reason the beats are not run from the binlog loop.
    #[test]
    fn a_lost_session_rejoined_on_the_same_epoch_keeps_us_active() {
        let r = drive(&[Script::Held, Script::Lost, Script::Held], &[Rejoin::Held]);
        assert_eq!(r.state, LeaderState::Active);
        assert_eq!(Metrics::get(&r.metrics.leader_beat_failures), 1);
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 0);
        assert_eq!(r.rejoins, 1);
        assert_eq!(
            r.beats, 4,
            "the third beat answers: the same epoch, still the leader"
        );
    }

    /// A rejoin that finds another instance holding the lock (or the epoch moved while we were
    /// away): this one is the replaced one, and it is fenced on the spot.
    #[test]
    fn a_rejoin_that_finds_the_lock_gone_elsewhere_fences_us() {
        let r = drive(
            &[Script::Held, Script::Lost],
            &[Rejoin::Replaced(
                "another lazyview instance took over while disconnected (epoch Some(3), ours 2); stopping",
            )],
        );
        let why = match &r.state {
            LeaderState::Fenced(why) => why.clone(),
            other => panic!("expected Fenced, got {other:?}"),
        };
        assert!(why.contains("epoch Some(3), ours 2"), "{why}");
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 1);
        assert_eq!(r.rejoins, 1);
    }

    /// A lock server that is simply away defers the rejoin, it does not fence anyone: our session
    /// and its lease are the server's business, and a healthy instance with builds running must
    /// never be ended by a network fault.
    #[test]
    fn a_lock_server_that_is_away_only_defers_the_rejoin() {
        let r = drive(
            &[Script::Held, Script::Lost, Script::Lost, Script::Held],
            &[Rejoin::Away, Rejoin::Away, Rejoin::Held],
        );
        assert_eq!(
            r.state,
            LeaderState::Active,
            "an unreachable lock server is not a takeover"
        );
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 0);
        assert_eq!(
            Metrics::get(&r.metrics.leader_beat_failures),
            2,
            "one count per beat that went unanswered, a deferred rejoin is not a second one"
        );
        assert_eq!(r.rejoins, 2);
    }

    /// Ruling 1: two periods without an answered beat close the write gates — `Uncertain`, which is
    /// not a verdict — and the target writes this instance has already started are ended at once
    /// rather than left to run out `LAZYVIEW_TARGET_TIMEOUT_MS`: the lease must not expire with one
    /// of ours still running. The beat that answers opens the gates again, and an instance that
    /// never lost the lock is never fenced.
    #[test]
    fn two_periods_without_an_answer_close_the_gates_and_fence_the_writes() {
        let r = drive(
            &[Script::Held, Script::Lost, Script::Lost, Script::Held],
            &[Rejoin::Away, Rejoin::Away],
        );
        assert_eq!(
            r.published,
            vec![LeaderState::Uncertain, LeaderState::Active],
            "closed by two unanswered beats, opened by the beat that answered"
        );
        assert_eq!(r.state, LeaderState::Active, "the lock was ours throughout");
        assert_eq!(
            Metrics::get(&r.metrics.leader_uncertain),
            1,
            "one entry per episode of uncertainty, not one per beat"
        );
        assert_eq!(
            r.fences, 1,
            "the writes in flight were ended as the gates closed, not when the beat came back"
        );
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 0);
        assert_eq!(r.rejoins, 2, "one per beat that could not be answered");
    }

    /// A single unanswered beat is not two: one blip and a rejoin on the same epoch never closes a
    /// gate — the false alarm that would stall a healthy instance's refreshes (condition 2's worry,
    /// now for the gates too).
    #[test]
    fn one_unanswered_beat_closes_nothing() {
        let r = drive(&[Script::Held, Script::Lost, Script::Held], &[Rejoin::Held]);
        assert_eq!(r.published, Vec::new(), "never left `Active`");
        assert_eq!(r.state, LeaderState::Active);
        assert_eq!(Metrics::get(&r.metrics.leader_uncertain), 0);
        assert_eq!(r.fences, 0, "nothing was in flight to end");
    }

    /// Ruling 1, the way back: the gates reopen on a rejoin that confirms the same epoch — the same
    /// proof `Active` has always required, given after the gates were closed.
    #[test]
    fn a_rejoin_that_confirms_the_epoch_reopens_the_gates() {
        let r = drive(
            &[Script::Held, Script::Lost, Script::Lost, Script::Lost],
            &[Rejoin::Away, Rejoin::Away, Rejoin::Held],
        );
        assert_eq!(
            r.published,
            vec![LeaderState::Uncertain, LeaderState::Active],
            "the confirmed rejoin, not the next beat, is what reopened them"
        );
        assert_eq!(Metrics::get(&r.metrics.leader_fenced), 0);
        assert_eq!(Metrics::get(&r.metrics.leader_beat_failures), 3);
    }

    /// A replaced instance leaves no write of ours behind to commit after the replacement's: the
    /// in-flight writes are fenced before the fatal state is published, on both ways out — the beat
    /// that answers "someone else holds it", and the rejoin that finds the epoch moved.
    #[test]
    fn a_fenced_instance_ends_the_writes_it_started_before_it_exits() {
        let r = drive(&[Script::Held, Script::Taken], &[]);
        assert!(matches!(r.state, LeaderState::Fenced(_)), "{:?}", r.state);
        assert_eq!(
            r.fences, 1,
            "quarantined through the zombie fence before the exit was published"
        );
        assert_eq!(r.published.len(), 1, "one publish: the fenced state itself");

        let r = drive(
            &[Script::Held, Script::Lost],
            &[Rejoin::Replaced(
                "another lazyview instance took over while disconnected (epoch Some(3), ours 2); stopping",
            )],
        );
        assert!(matches!(r.state, LeaderState::Fenced(_)), "{:?}", r.state);
        assert_eq!(r.fences, 1, "the same, on the rejoin's verdict");
    }

    /// The gate the refresh supervisor asks before issuing a write op: `Active` lets it through, and
    /// both states that are not `Active` refuse it — `Uncertain` with the reason an operator needs
    /// (the beat, not a takeover), `Fenced` with the epochs.
    #[test]
    fn the_write_gate_refuses_every_state_but_active() {
        let (tx, rx) = watch::channel(LeaderState::Active);
        let gate = LeaderWriteGate { state: rx };
        gate.check("items").expect("the active instance writes");
        tx.send_replace(LeaderState::Uncertain);
        let e = gate.check("items").unwrap_err().to_string();
        assert!(
            e.contains("items") && e.contains("leadership lock") && e.contains("confirmed again"),
            "{e}"
        );
        tx.send_replace(LeaderState::Fenced("epoch 3, ours 2".to_string()));
        let e = gate.check("items").unwrap_err().to_string();
        assert!(e.contains("epoch 3, ours 2"), "{e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bound string loses its `COLLATE`; a param that has none, and a param that is not a bound
    /// string, come back as they were (the lookup binds strings without a collation only where the
    /// view definition has one of its own).
    #[test]
    fn a_lookup_param_loses_only_its_collate() {
        assert_eq!(
            without_collate(
                "CAST(CAST(? AS BINARY) AS CHAR CHARACTER SET latin1) COLLATE latin1_bin",
            ),
            "CAST(CAST(? AS BINARY) AS CHAR CHARACTER SET latin1)",
            "the collation of the view definition decides"
        );
        assert_eq!(
            without_collate("CAST(CAST(? AS BINARY) AS CHAR CHARACTER SET latin1)"),
            "CAST(CAST(? AS BINARY) AS CHAR CHARACTER SET latin1)",
            "a param without one is as it was"
        );
        assert_eq!(without_collate("?"), "?");
        assert_eq!(
            without_collate("CAST(? AS DECIMAL(10,2)) COLLATE utf8mb4_bin"),
            "CAST(? AS DECIMAL(10,2)) COLLATE utf8mb4_bin",
            "not a bound string: unchanged"
        );
    }

    /// The drain queue keeps its byte total and the file of its oldest events in step with
    /// pushes and pops, stops at its bounds and at the end of the stream, and gives back its
    /// capacity once empty.
    #[test]
    fn drained_queue_accounting() {
        let mut d = Drained::new(crate::mem::MemoryBudget::fallback().drain_max_bytes);
        assert!(d.open() && !d.full() && d.file.is_none() && d.pop().is_none());
        d.push(Ok(Some(Item::Heartbeat)), "binlog.000001");
        assert!(
            d.file.is_none() && d.bytes == 0 && d.pop().is_none(),
            "heartbeats are not queued"
        );
        d.push(Ok(Some(Item::Other)), "binlog.000001");
        d.push(Ok(Some(Item::Other)), "binlog.000002");
        assert_eq!(
            d.file.as_deref(),
            Some("binlog.000001"),
            "the oldest event's file"
        );
        assert_eq!(d.bytes, 128);
        assert!(matches!(d.pop(), Some(Ok(Some(Item::Other)))));
        assert_eq!((d.bytes, d.file.as_deref()), (64, Some("binlog.000001")));
        assert!(d.pop().is_some());
        assert_eq!((d.bytes, d.file.as_deref()), (0, None));
        d.push(Ok(None), "binlog.000002");
        assert!(!d.open(), "nothing is read past the end of the stream");
        d.pop();
        for _ in 0..DRAIN_MAX_EVENTS {
            d.push(Ok(Some(Item::Other)), "binlog.000003");
        }
        assert!(d.full() && !d.open());
        while d.pop().is_some() {}
        assert!(d.queue.capacity() <= DRAIN_KEEP_SLOTS && d.bytes == 0);
        // The byte bound acts on its own, and it is the value the queue was built with: past
        // 200 bytes this queue is full with four events (a test makes a budget any size).
        let mut d = Drained::new(200);
        for _ in 0..3 {
            d.push(Ok(Some(Item::Other)), "binlog.000004");
        }
        assert!(d.open() && !d.full(), "192 of 200 bytes");
        d.push(Ok(Some(Item::Other)), "binlog.000004");
        assert!(d.full() && !d.open(), "256 bytes is over the budget");
    }

    /// The memory estimate of a change buffer counts every kind of entry at its size.
    #[test]
    fn buffer_bytes_by_kind() {
        let mut d = ViewDirty::for_aliases(1);
        assert_eq!(buffer_bytes(&d), 0);
        d.units.insert_int(1);
        d.units.insert_int(2);
        d.units.insert(Key::from([Datum::Bytes(b"k".to_vec())]));
        d.images[0].insert(Key::from([Datum::Int(7), Datum::Null]));
        d.partial.push(UnitSet::default());
        d.partial[0].insert_int(3);
        d.marks.push((0, Datum::Int(1)));
        assert_eq!(buffer_bytes(&d), 3 * INT_KEY_BYTES + 3 * KEY_BYTES);
        // With no container limit the budgets are the constants the engine carried before the
        // total existed (the property `crate::mem` exists to keep).
        let m = crate::mem::MemoryBudget::fallback();
        assert_eq!(m.drain_max_bytes, 32 << 20);
        assert_eq!(m.build_hold_bytes, 256 << 20);
        assert_eq!(m.xa_hold_bytes, 256 << 20);
        assert_eq!(m.lag_hold_bytes, 256 << 20);
        assert_eq!(m.dedup_max_bytes, 256 << 20);
    }

    fn lag(since: (&str, u64), down: bool) -> Lag {
        Lag {
            units: UnitSet::default(),
            partial: Vec::new(),
            all: false,
            since: Position {
                file: since.0.to_string(),
                pos: since.1,
            },
            failures: 0,
            retry_at: Instant::now(),
            down,
        }
    }

    /// The saved position waits for a lagging target reported down (`pending_floor` keeps
    /// every `lag_floor` entry): its kept keys live only in memory, so a restart must replay
    /// the outage into them instead of leaving the target silently stale.
    #[test]
    fn the_saved_position_waits_for_a_down_target() {
        let mut floor = Position {
            file: "binlog.000009".to_string(),
            pos: 500,
        };
        let lag: BTreeMap<String, Lag> = [
            ("t2", lag(("binlog.000003", 42), false)),
            ("t1", lag(("binlog.000002", 7), true)),
        ]
        .into_iter()
        .map(|(t, g)| (t.to_string(), g))
        .collect();
        assert!(lag["t1"].down);
        lag_floor(&mut floor, &lag);
        assert_eq!(
            floor,
            Position {
                file: "binlog.000002".to_string(),
                pos: 7
            },
            "the down target's `since` holds the saved position back"
        );
    }

    fn server(code: u16) -> anyhow::Error {
        anyhow::Error::new(mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: "test".into(),
            state: "HY000".into(),
        }))
        .context("saving the binlog position")
    }

    /// A clean server restart answers 1053 to statements in its shutdown window: reconnect, do
    /// not exit. Other server errors (lost leadership, SQL errors) stay fatal.
    #[test]
    fn server_going_away_is_a_connection_error() {
        // The one classifier (binlog::is_transient): the going-away codes and the lock-wait /
        // deadlock codes are transient; privilege, SQL and unknown-table errors stay fatal.
        for code in [1053, 1927, 3169, 4031, 2006, 2013, 1205, 1213] {
            assert!(binlog::is_transient(&server(code)), "{code}");
        }
        for code in [1045, 1062, 1146, 1236] {
            assert!(!binlog::is_transient(&server(code)), "{code}");
        }
        let closed = anyhow::Error::new(mysql_async::Error::Driver(
            mysql_async::DriverError::ConnectionClosed,
        ));
        assert!(binlog::is_transient(&closed));
    }

    #[test]
    fn statement_keywords() {
        assert_eq!(first_keyword("  TRUNCATE t"), "TRUNCATE");
        assert_eq!(first_keyword("/* x */ drop table t"), "drop");
        assert_eq!(first_keyword("-- c\nDROP TABLE t"), "DROP");
        assert_eq!(first_keyword("/*!40000 DROP */"), "");
    }

    /// The statement-format DML warning is rate limited per (schema, table): the first event for
    /// a table logs at once — an entry that has never logged has no time to compare against,
    /// which is what keeps the clock off the path where subtracting the interval from `now` would
    /// panic below the clock's origin, a host up for less than the interval (review C-03) — and
    /// an event inside the interval folds into the line that follows it.
    #[test]
    fn the_first_statement_dml_line_logs_at_once() {
        let t0 = Instant::now();
        let mut slot = (None, 0u64); // a table never warned about
        assert_eq!(
            stmt_dml_gate(&mut slot, t0),
            Some(1),
            "the first event logs at once, whatever the host's uptime"
        );
        assert_eq!(slot, (Some(t0), 0), "the line stamps its table");
        assert_eq!(
            stmt_dml_gate(&mut slot, t0 + Duration::from_secs(59)),
            None,
            "an event inside the interval folds instead of logging"
        );
        assert_eq!(
            slot,
            (Some(t0), 1),
            "the folded event waits for the next line"
        );
        assert_eq!(
            stmt_dml_gate(&mut slot, t0 + STMT_DML_LOG_INTERVAL),
            Some(2),
            "the line that follows carries the folded event too"
        );
        assert_eq!(slot, (Some(t0 + STMT_DML_LOG_INTERVAL), 0));
        assert_eq!(
            stmt_dml_gate(&mut slot, t0 + STMT_DML_LOG_INTERVAL * 2),
            Some(1),
            "a quiet interval reports one event"
        );
    }
}

#[cfg(test)]
mod managed_service_tests {
    use super::*;

    #[test]
    fn rds_and_aurora_are_recognised() {
        assert!(is_managed_service(Some("/rdsdbbin/mysql-8.4.8.R1/"), false));
        assert!(is_managed_service(Some("/usr/"), true));
        assert!(is_managed_service(None, true));
        assert!(!is_managed_service(Some("/usr/"), false));
        assert!(!is_managed_service(None, false));
    }

    #[test]
    fn retention_below_twelve_hours_warns() {
        assert!(retention_warning(Some("12")).is_none());
        assert!(retention_warning(Some(" 24 ")).is_none());
        assert!(retention_warning(Some("11")).unwrap().contains("11 hours"));
        assert!(retention_warning(Some("0")).unwrap().contains("0 hours"));
        for unset in [None, Some(""), Some("NULL"), Some("x")] {
            assert!(
                retention_warning(unset).unwrap().contains("not set"),
                "{unset:?}"
            );
        }
    }

    #[test]
    fn gtid_tip_only_for_known_off_modes_off_aurora() {
        assert!(wants_gtid_tip(false, Some("OFF")));
        assert!(wants_gtid_tip(false, Some("OFF_PERMISSIVE")));
        assert!(wants_gtid_tip(false, Some("ON_PERMISSIVE")));
        assert!(!wants_gtid_tip(false, Some("ON")));
        assert!(!wants_gtid_tip(false, Some("on")));
        assert!(!wants_gtid_tip(false, None));
        assert!(!wants_gtid_tip(true, Some("OFF")));
    }

    /// A-02: the wait before a reconnect is the network's schedule for a network fault and the
    /// connect ladder for a reader stuck on one undecodable event — never the flat 200 ms, which
    /// is what made the stuck reader an endless hot loop with green probes. The bounds are the
    /// ladder's because [`supervisor::backoff`] jitters (0.5 .. 1.5 of the step).
    #[test]
    fn stream_pause_backs_off_for_an_undecodable_event() {
        for streak in 1..=6u32 {
            let step = (Duration::from_secs(1) * (1u32 << (streak - 1)))
                .min(supervisor::Policy::CONNECT.cap);
            let p = stream_pause(Some(StreamEnd::Undecodable), true, streak);
            assert!(
                p >= step.mul_f64(0.49) && p <= step.mul_f64(1.51),
                "streak {streak}: {p:?} is not the {step:?} connect step with jitter"
            );
            assert!(p > Duration::from_millis(200), "streak {streak}: {p:?}");
        }
        // The first round of a streak already backs off: `streak` counts the round that just ended.
        assert!(
            stream_pause(Some(StreamEnd::Undecodable), false, 0) >= Duration::from_millis(500),
            "a stuck reader never reconnects on the network's short timer"
        );
        // Every other reason keeps exactly the pause it had before A-02: 200 ms once something was
        // delivered this connection, a connect cap when the round died at the door.
        for end in [
            Some(StreamEnd::Transient),
            Some(StreamEnd::Closed),
            Some(StreamEnd::Silence),
            None,
        ] {
            assert_eq!(
                stream_pause(end, true, 3),
                Duration::from_millis(200),
                "{end:?}"
            );
            assert_eq!(
                stream_pause(end, false, 3),
                supervisor::Policy::CONNECT.cap,
                "{end:?}"
            );
        }
    }
}
