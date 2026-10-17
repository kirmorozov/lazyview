//! lazyview throughput benchmark: how many source row changes per second the engine applies
//! while keeping its views fresh, how fresh they are, and what it costs.
//!
//! ```text
//! cargo run --release --example throughput -- --dsn mysql://lv:lv@127.0.0.1:33084/bench \
//!     --scenario join_direct --seconds 30 --writers 8 --batch 50
//! cargo run --release --example throughput -- --list
//! cargo run --release --example throughput -- --report tmp/out/bench/results.jsonl
//! ```
//!
//! One run = one scenario on one server:
//! 1. fresh database `bench_<scenario>` (dropped afterwards), schema + seed data, view
//!    definitions in `_lazyview`, an oracle (plain SQL VIEW `_oracle_<name>`) per view;
//! 2. the engine is started in-process on its own tokio runtime (threads `lv-eng*`); the time
//!    until every view is `ready` is the initial build time;
//! 3. a short idle freshness probe, then the load: N writer connections in a *separate child
//!    process* (this binary with `--role writer`) commit transactions of B row changes each, as
//!    fast as possible or capped by `--rate`; a probe commits marked changes and polls the view;
//!    a sampler records server binlog position, applied position, committed rows and RSS;
//! 4. drain (until the engine applied everything), final catch-up, oracle comparison;
//! 5. optional (`--catchup S`): engine stopped, S seconds of backlog written, engine restarted in
//!    `resume` mode, time to apply the backlog = engine capacity; oracle comparison again;
//! 6. summary table on stdout + one JSON line appended to `--out`.
//!
//! See `--help` for what every reported number means.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write as _};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use lazyview::binlog::Position;
use lazyview::{Config, Metrics};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, Value};
use rand::rngs::Xoshiro256PlusPlus;
use rand::{RngExt, SeedableRng};
use serde_json::{json, Value as J};
use tokio::sync::watch;

const HELP: &str = r#"lazyview throughput benchmark

USAGE
  throughput --dsn mysql://lv:lv@127.0.0.1:33084/bench --scenario NAME [options]
  throughput --list                       scenarios
  throughput --report FILE.jsonl          table of all runs in a results file
  throughput --dsn URL --cleanup          drop leftover <prefix>_<scenario> databases of every scenario

OPTIONS (defaults in brackets)
  --dsn URL            server + credentials; the path is the database-name prefix [bench]
                       -> the run uses database <prefix>_<scenario> (must start with "bench")
  --db NAME            explicit database name (must start with "bench")
  --scenario NAME      see --list
  --seconds S          load duration [30]
  --writers N          concurrent writer connections (separate process) [8]
  --batch B            row changes per writer transaction [50]
  --rate R             cap on total source row changes/s, 0 = as fast as possible [0]
  --window-ms MS       LAZYVIEW_BATCH_WINDOW_MS [200]
  --orders N --customers N --items N --sessions N --products N   seed sizes
                       [100000 / 20000 / 300000 / 50000 / 1000]
  --hot N              hot_keys: number of hot orders (plus their items) [100]
  --views N            many_views: number of views [20]
  --probe-ms MS        freshness probe period [200]; --no-probe disables the probe
  --probe-poll-ms MS   probe poll interval [5]
  --probe-timeout S    give up on one probe after S seconds [120]
  --idle-probe S       idle freshness probe before the load [2]
  --sample-ms MS       position/lag sampler interval [100]
  --engine-threads N   engine tokio worker threads, 0 = tokio default (= cores, like the binary) [0]
  --env K=V            extra engine setting (repeatable), e.g. --env LAZYVIEW_QUERY_CHUNK=2000
  --drain-timeout S    max wait for the engine to catch up after the load [600]
  --label TEXT         free-form tag stored with the result
  --out FILE           JSON lines file to append to [tmp/out/bench/results.jsonl]
  --log-dir DIR        engine logs [tmp/out/bench/logs]; --log-filter F [lazyview=info,warn]
  --seed N             workload RNG seed [42]
  --keep-db            do not drop the database afterwards
  --catchup S          backlog catch-up phase after the live phase: stop the engine, write for S
                       seconds, restart it (resume) and time the drain; 0 = off [0]
  --catchup-writers N --catchup-batch B   writers / rows per txn of the backlog [= live values]
  --build-info TEXT    build provenance stored with the result (tmp/bench.sh: git commit + tree hash)

WHAT IS MEASURED
  source.rows_s / txns_s   committed row changes (server-reported affected rows) and
                           transactions per second, summed over all writers, during the load.
  engine.applied_rows_s    source row changes the engine had applied per second during the load:
                           the applied binlog position (Metrics::applied) is mapped to "rows
                           committed when the server binlog was at that position" by
                           interpolating the sampler's (server position, committed rows) series.
  engine.lag_s / rows_behind  at each sample: now - time the server binlog reached the applied
                           position / committed rows not yet applied. lag_slope = least-squares
                           slope of lag_s over the 2nd half of the load (s per s).
  engine.verdict           "keeping_up" unless lag_slope > 0.05 s/s, final lag > max(1 s, 2 flush
                           cycles) and drain_s > 1 flush cycle ("falling_behind": the engine is the
                           bottleneck; an engine that keeps up shows a sawtooth lag of about one
                           cycle and clears the final backlog with the flush in progress). "inconclusive" when the
                           2nd half of the load spans < 5 flush cycles (flush_cycle_s = window +
                           flush p50): lag is a sawtooth with the flush cycle, so short runs cannot
                           tell growth from the cycle. "not_drained": no catch-up within --drain-timeout.
  engine.max_sustainable_rows_s  falling_behind: busy_rows_s = row changes / (load + drain time),
                           the rate the engine got through while continuously busy (its saturated
                           throughput; applied_rows_s_2nd_half is also reported but is coarse when
                           flushes take seconds); keeping_up: the source rate, which is then only a
                           lower bound (bound = "source") - raise --writers/--batch to find the limit.
  engine.drain_s / drain_rows_s  time (and rows/s) to apply the backlog after writers stopped.
  freshness                probe: UPDATE of a reserved row with a unique marker (autocommit),
                           then poll the MV (indexed lookup) until the marker is visible;
                           latency = visible - commit acknowledged. p50/p95/p99/max over the load
                           phase (and over the idle phase before it).
  resources.engine_cpu_pct CPU of this process minus the harness threads (main thread, lvb-*:
                           sampler, probe, pipe reader), from /proc/self/stat and
                           /proc/self/task/*/stat, over load+drain, in % of one core. Writers run in a
                           child process (measured separately: writer_cpu_pct) so they do not
                           pollute it. cpu_us_per_change = engine CPU time / row changes.
                           mysqld_cpu_pct: the server process (writers + engine queries).
  resources.rss_mb_*       VmRSS of this process (harness is small): before the engine started,
                           after the build, peak during load+drain; VmHWM at the end.
  efficiency               Metrics deltas from load start to final catch-up, per row change
                           (writer rows + probe rows): dedup_ratio = rows_seen / units_refreshed
                           (rows_seen counts each decoded row of a watched table once; units are
                           summed over views, so with V views reading a table "no dedup" is 1/V;
                           a customer row fanning out to 5 orders is 0.2), binlog_events,
                           queries per 1k changes (source / lookup / mv), mv_rows_written_per_change
                           = (mv_rows_inserted + mv_rows_deleted) / changes, rows_skipped (updates
                           touching no referenced column, per view route), units_changed_frac.
  efficiency.flush_ms      duration of every engine flush (refresh of all dirty units + position
                           save; from the engine's trace event), p50/p95/max, and busy_pct = time
                           spent flushing / wall time (~100%: flush work, i.e. server round trips,
                           is the bottleneck; > 100% means flushes overlapped, which an engine with
                           background/parallel flushes can do). metrics_delta holds every counter
                           of lazyview::Metrics, including ones added after this harness was written.
  server.commit_ms_p50     latency of a 1-row autocommit UPDATE right before the load: the cost of
                           one durable commit (the engine commits once per unit chunk per view).
  params.engine_env        effective engine settings. LAZYVIEW_FULL_REFRESH_MS defaults to 0 here (as in
                           the integration tests): full-mode views rebuild as soon as they are dirty.
                           applied() does not wait for full-mode rebuilds (they run in the background,
                           even with a cadence of 0), so for a full-mode view the oracle check waits up
                           to cadence + max(60 s, 5 x initial build) for the rebuild (full_refresh_wait_s
                           per view) and the
                           catch-up phase also reports consistent_s / rows_s_consistent (drain + that
                           wait): for full-mode views only those are meaningful.
  correctness              after catch-up every MV must equal its oracle view (COUNT + SUM(CRC32)
                           over all columns, row diff on mismatch); valid=false otherwise.
  catchup.rows_s           (--catchup) engine CAPACITY: backlog row changes / time from binlog stream
                           (re)connect until applied >= server position after the backlog. The engine is
                           saturated by construction, so this does not depend on how fast the source
                           commits (the live figures here usually do). Also: startup_s (excluded),
                           engine CPU/RSS, efficiency figures for the drain, its own oracle check.
                           engine.capacity_rows_s repeats it next to the live verdict. A saturated live
                           engine behaves like a catching-up one (each flush takes what accumulated
                           during the previous one), so capacity approximates the max live rate.
  host                     uptime/loadavg before and after, host CPU busy % (/proc/stat) during load.
  valid                    false if any oracle check failed or the engine did not catch up.
"#;

/// Probe rows: orders 1..=8, their items (item k belongs to order ((k-1) % orders) + 1) and
/// customers 1..=8 are never touched by writers.
const PROBE_IDS: u64 = 8;
const STATUSES: [&str; 4] = ["new", "paid", "shipped", "cancelled"];
const MARKER_BASE: u64 = 1_000_000_000;

// =============================================================================================
// Arguments
// =============================================================================================

#[derive(Clone, Debug, PartialEq)]
enum Role {
    Bench,
    Writer,
    List,
    Report(PathBuf),
    Cleanup,
    Help,
}

#[derive(Clone, Debug)]
struct Args {
    role: Role,
    dsn: String,
    db: Option<String>,
    writer_dsn: Option<String>,
    scenario: String,
    seconds: f64,
    writers: usize,
    batch: usize,
    rate: f64,
    window_ms: u64,
    orders: u64,
    customers: u64,
    items: u64,
    sessions: u64,
    products: u64,
    hot: u64,
    views: usize,
    probe: bool,
    probe_ms: u64,
    probe_poll_ms: u64,
    probe_timeout_s: f64,
    idle_probe_s: f64,
    sample_ms: u64,
    engine_threads: usize,
    env: Vec<(String, String)>,
    drain_timeout_s: f64,
    label: String,
    out: PathBuf,
    log_dir: PathBuf,
    log_filter: String,
    seed: u64,
    keep_db: bool,
    /// Backlog catch-up phase: seconds of writes while the engine is stopped (0 = off).
    catchup: f64,
    catchup_writers: Option<usize>,
    catchup_batch: Option<usize>,
    /// Free-form build provenance (git commit + tree hash), stored with the result.
    build_info: String,
}

fn repo_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

impl Default for Args {
    fn default() -> Self {
        Args {
            role: Role::Bench,
            dsn: String::new(),
            db: None,
            writer_dsn: None,
            scenario: String::new(),
            seconds: 30.0,
            writers: 8,
            batch: 50,
            rate: 0.0,
            window_ms: 200,
            orders: 100_000,
            customers: 20_000,
            items: 300_000,
            sessions: 50_000,
            products: 1_000,
            hot: 100,
            views: 20,
            probe: true,
            probe_ms: 200,
            probe_poll_ms: 5,
            probe_timeout_s: 120.0,
            idle_probe_s: 2.0,
            sample_ms: 100,
            engine_threads: 0,
            env: Vec::new(),
            drain_timeout_s: 600.0,
            label: String::new(),
            out: repo_dir().join("tmp/out/bench/results.jsonl"),
            log_dir: repo_dir().join("tmp/out/bench/logs"),
            log_filter: "lazyview=info,warn".into(),
            seed: 42,
            keep_db: false,
            catchup: 0.0,
            catchup_writers: None,
            catchup_batch: None,
            build_info: String::new(),
        }
    }
}

fn parse_args(raw: &[String]) -> Result<Args> {
    let mut a = Args::default();
    let mut i = 0;
    while i < raw.len() {
        let arg = raw[i].clone();
        let (key, inline) = match arg.split_once('=') {
            Some((k, v)) if arg.starts_with("--") => (k.to_string(), Some(v.to_string())),
            _ => (arg.clone(), None),
        };
        let mut take = || -> Result<String> {
            if let Some(v) = &inline {
                return Ok(v.clone());
            }
            i += 1;
            raw.get(i)
                .cloned()
                .ok_or_else(|| anyhow!("{key} needs a value"))
        };
        macro_rules! num {
            ($t:ty) => {{
                let v = take()?;
                v.trim()
                    .parse::<$t>()
                    .with_context(|| format!("{key}: not a number: {v}"))?
            }};
        }
        match key.as_str() {
            "-h" | "--help" => a.role = Role::Help,
            "--list" => a.role = Role::List,
            "--cleanup" => a.role = Role::Cleanup,
            "--report" => a.role = Role::Report(PathBuf::from(take()?)),
            "--role" => {
                a.role = match take()?.as_str() {
                    "writer" => Role::Writer,
                    "bench" => Role::Bench,
                    r => bail!("unknown role {r}"),
                }
            }
            "--dsn" => a.dsn = take()?,
            "--db" => a.db = Some(take()?),
            "--writer-dsn" => a.writer_dsn = Some(take()?),
            "--scenario" => a.scenario = take()?,
            "--seconds" => a.seconds = num!(f64),
            "--writers" => a.writers = num!(usize),
            "--batch" => a.batch = num!(usize),
            "--rate" => a.rate = num!(f64),
            "--window-ms" => a.window_ms = num!(u64),
            "--orders" => a.orders = num!(u64),
            "--customers" => a.customers = num!(u64),
            "--items" => a.items = num!(u64),
            "--sessions" => a.sessions = num!(u64),
            "--products" => a.products = num!(u64),
            "--hot" => a.hot = num!(u64),
            "--views" => a.views = num!(usize),
            "--no-probe" => a.probe = false,
            "--probe-ms" => a.probe_ms = num!(u64),
            "--probe-poll-ms" => a.probe_poll_ms = num!(u64),
            "--probe-timeout" => a.probe_timeout_s = num!(f64),
            "--idle-probe" => a.idle_probe_s = num!(f64),
            "--sample-ms" => a.sample_ms = num!(u64),
            "--engine-threads" => a.engine_threads = num!(usize),
            "--env" => {
                let v = take()?;
                let (k, val) = v
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--env expects K=V, got {v}"))?;
                a.env.push((k.trim().to_string(), val.to_string()));
            }
            "--drain-timeout" => a.drain_timeout_s = num!(f64),
            "--label" => a.label = take()?,
            "--out" => a.out = PathBuf::from(take()?),
            "--log-dir" => a.log_dir = PathBuf::from(take()?),
            "--log-filter" => a.log_filter = take()?,
            "--seed" => a.seed = num!(u64),
            "--keep-db" => a.keep_db = true,
            "--catchup" => a.catchup = num!(f64),
            "--catchup-writers" => a.catchup_writers = Some(num!(usize)),
            "--catchup-batch" => a.catchup_batch = Some(num!(usize)),
            "--build-info" => a.build_info = take()?,
            other => bail!("unknown argument {other} (see --help)"),
        }
        i += 1;
    }
    if matches!(a.role, Role::Bench | Role::Writer) {
        if a.dsn.is_empty() && a.writer_dsn.is_none() {
            bail!("--dsn is required (see --help)");
        }
        if scenario(&a.scenario).is_none() {
            bail!(
                "unknown --scenario '{}'; one of: {}",
                a.scenario,
                SCENARIOS
                    .iter()
                    .map(|s| s.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if a.writers == 0
            || a.batch == 0
            || a.catchup_writers == Some(0)
            || a.catchup_batch == Some(0)
        {
            bail!("--writers and --batch must be > 0");
        }
        if a.orders <= PROBE_IDS + a.hot
            || a.customers <= PROBE_IDS
            || a.items < a.orders
            || a.customers > a.orders
        {
            bail!("sizes: need orders > {} + hot, customers > {PROBE_IDS}, customers <= orders <= items", PROBE_IDS);
        }
    }
    Ok(a)
}

// =============================================================================================
// Scenarios: views, workload mix, probe
// =============================================================================================

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    /// orders: status + amount + rev (status moves rows in/out of filters)
    UpdOrder,
    /// orders: a column no view reads (note)
    UpdOrderNote,
    InsOrder,
    /// order_items: qty + rev
    UpdItem,
    InsItem,
    DelItem,
    /// customers: name + tier + rev
    UpdCustomer,
    /// customers: a column no view reads (visits)
    UpdCustomerVisits,
    /// events_log insert (no view reads it)
    InsEvent,
    /// sessions update (no view reads it)
    UpdSession,
    /// orders amount + rev within the hot set
    HotOrder,
    /// items of hot orders: qty + rev
    HotItem,
}

struct Scenario {
    name: &'static str,
    about: &'static str,
    mix: &'static [(u32, Op)],
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "spj_single",
        about: "single-table filtered projection of orders; updates spread uniformly over all ids (15% touch an unreferenced column)",
        mix: &[(80, Op::UpdOrder), (15, Op::UpdOrderNote), (5, Op::InsOrder)],
    },
    Scenario {
        name: "join_direct",
        about: "orders JOIN order_items JOIN customers, unit = order id; item and order changes map to units straight from the binlog image",
        mix: &[(55, Op::UpdItem), (25, Op::UpdOrder), (12, Op::InsItem), (8, Op::DelItem)],
    },
    Scenario {
        name: "hot_keys",
        about: "all writes hit a small hot set of orders and their items (dedup heavy): SPJ over orders + per-order aggregate over items",
        mix: &[(50, Op::HotOrder), (50, Op::HotItem)],
    },
    Scenario {
        name: "lookup_fanout",
        about: "customer (dimension) updates fanning out via lookup queries to their orders (orders/customers per customer)",
        mix: &[(90, Op::UpdCustomer), (10, Op::UpdCustomerVisits)],
    },
    Scenario {
        name: "grouped_agg",
        about: "GROUP BY with COUNT/SUM/MAX over order_items: per order (fine groups) and per product (coarse groups)",
        mix: &[(70, Op::UpdItem), (15, Op::InsItem), (15, Op::DelItem)],
    },
    Scenario {
        name: "many_views",
        about: "N overlapping views (SPJ, joins, lookups, aggregates) over the same tables; mixed workload",
        mix: &[(35, Op::UpdItem), (25, Op::UpdOrder), (10, Op::UpdCustomer), (15, Op::InsItem), (10, Op::DelItem), (5, Op::InsOrder)],
    },
    Scenario {
        name: "unwatched",
        about: "all writes go to tables no view reads (events_log inserts, sessions updates): binlog decode/skip ceiling",
        mix: &[(50, Op::InsEvent), (50, Op::UpdSession)],
    },
    Scenario {
        name: "full_mode",
        about: "SELECT DISTINCT view (not incrementally maintainable): every dirty batch is a full rebuild + swap",
        mix: &[(100, Op::UpdOrder)],
    },
];

fn scenario(name: &str) -> Option<&'static Scenario> {
    SCENARIOS.iter().find(|s| s.name == name)
}

#[derive(Clone, Debug)]
struct ViewDef {
    name: String,
    select: String,
}

fn vd(name: &str, select: &str) -> ViewDef {
    ViewDef {
        name: name.to_string(),
        select: select.split_whitespace().collect::<Vec<_>>().join(" "),
    }
}

/// A freshness probe: `update` (with `{p}` = probe row id, `{m}` = marker) is committed, then
/// `SELECT COUNT(*) FROM view WHERE check` must reach the expected count (1, or the number of
/// ids returned by `ids_sql`, substituted as `{ids}`).
#[derive(Clone, Debug)]
struct ProbeSpec {
    update: String,
    view: String,
    check: String,
    ids_sql: Option<String>,
    min_poll_ms: u64,
}

fn probe(update: &str, view: &str, check: &str) -> ProbeSpec {
    ProbeSpec {
        update: update.into(),
        view: view.into(),
        check: check.into(),
        ids_sql: None,
        min_poll_ms: 0,
    }
}

const ORDERS_SPJ: &str = "SELECT id, customer_id, status, amount, rev FROM orders";
const ITEMS_BY_ORDER: &str = "SELECT order_id, COUNT(*) AS n_items, SUM(qty) AS qty, SUM(qty * price) AS total, MAX(rev) AS max_rev FROM order_items GROUP BY order_id";
const PROBE_ORDER_REV: &str = "UPDATE orders SET rev = {m} WHERE id = {p}";
const PROBE_ITEM_REV: &str = "UPDATE order_items SET rev = {m} WHERE id = {p}";

fn scenario_views(name: &str, a: &Args) -> (Vec<ViewDef>, ProbeSpec) {
    match name {
        "spj_single" => (
            vec![vd("spj_orders", "SELECT id, customer_id, status, amount, rev FROM orders WHERE status <> 'cancelled'")],
            probe(PROBE_ORDER_REV, "spj_orders", "id = {p} AND rev = {m}"),
        ),
        "join_direct" => (
            vec![vd(
                "jd_lines",
                "SELECT o.id AS order_id, i.id AS item_id, o.status, o.rev AS order_rev, c.name AS customer,
                        i.product_id, i.qty, i.price, i.rev AS item_rev
                 FROM orders o JOIN order_items i ON i.order_id = o.id JOIN customers c ON c.id = o.customer_id",
            )],
            probe(PROBE_ITEM_REV, "jd_lines", "order_id = {p} AND item_id = {p} AND item_rev = {m}"),
        ),
        "hot_keys" => (
            vec![vd("hk_orders", ORDERS_SPJ), vd("hk_totals", ITEMS_BY_ORDER)],
            probe(PROBE_ORDER_REV, "hk_orders", "id = {p} AND rev = {m}"),
        ),
        "lookup_fanout" => (
            vec![vd(
                "lf_orders",
                "SELECT o.id, o.status, o.amount, c.id AS customer_id, c.name AS customer, c.tier, c.rev AS c_rev
                 FROM orders o JOIN customers c ON c.id = o.customer_id",
            )],
            ProbeSpec {
                ids_sql: Some("SELECT id FROM orders WHERE customer_id = {p}".into()),
                ..probe("UPDATE customers SET rev = {m} WHERE id = {p}", "lf_orders", "id IN ({ids}) AND c_rev = {m}")
            },
        ),
        "grouped_agg" => (
            vec![
                vd("ga_by_order", ITEMS_BY_ORDER),
                vd("ga_by_product", "SELECT product_id, COUNT(*) AS n_items, SUM(qty) AS qty, SUM(qty * price) AS revenue FROM order_items GROUP BY product_id"),
            ],
            probe(PROBE_ITEM_REV, "ga_by_order", "order_id = {p} AND max_rev = {m}"),
        ),
        "many_views" => {
            let views = many_views(a.views.max(1), a);
            let first = views[0].name.clone();
            (views, probe(PROBE_ORDER_REV, &first, "id = {p} AND rev = {m}"))
        }
        "unwatched" => (
            vec![vd("uw_orders", ORDERS_SPJ), vd("uw_totals", "SELECT order_id, COUNT(*) AS n_items, SUM(qty * price) AS total FROM order_items GROUP BY order_id")],
            probe(PROBE_ORDER_REV, "uw_orders", "id = {p} AND rev = {m}"),
        ),
        "full_mode" => (
            vec![vd("fm_customer_status", "SELECT DISTINCT customer_id, status FROM orders")],
            ProbeSpec {
                // The full-mode MV has no unit index: poll less often (each poll scans it).
                min_poll_ms: 25,
                ..probe("UPDATE orders SET status = 'p{m}' WHERE id = {p}", "fm_customer_status", "customer_id = {p} AND status = 'p{m}'")
            },
        ),
        _ => unreachable!("validated"),
    }
}

/// `n` overlapping views over orders / order_items / customers, cycling through 10 shapes.
fn many_views(n: usize, a: &Args) -> Vec<ViewDef> {
    (0..n)
        .map(|k| {
            let (t, v) = (k % 10, k / 10);
            let s = STATUSES[(k + v) % 4];
            let (suffix, sel) = match t {
                0 => (
                    "orders",
                    if v == 0 { ORDERS_SPJ.to_string() } else { format!("{ORDERS_SPJ} WHERE id % {} <> 0", v + 1) },
                ),
                1 => ("by_status", format!("SELECT id, customer_id, amount FROM orders WHERE status = '{s}'")),
                2 => (
                    "cust_tier",
                    format!(
                        "SELECT o.id, o.status, c.name AS customer, c.tier FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tier = {}",
                        (k + v) % 4
                    ),
                ),
                3 => (
                    "lines",
                    format!(
                        "SELECT o.id AS order_id, i.id AS item_id, i.qty, i.price, c.name AS customer FROM orders o JOIN order_items i ON i.order_id = o.id JOIN customers c ON c.id = o.customer_id WHERE o.status = '{s}'"
                    ),
                ),
                4 => (
                    "order_totals",
                    format!(
                        "SELECT order_id, COUNT(*) AS n_items, SUM(qty * price) AS total FROM order_items{} GROUP BY order_id",
                        if v == 0 { String::new() } else { format!(" WHERE qty > {v}") }
                    ),
                ),
                5 => (
                    "cust_totals",
                    format!(
                        "SELECT customer_id, COUNT(*) AS n_orders, SUM(amount) AS total FROM orders{} GROUP BY customer_id",
                        if v == 0 { String::new() } else { " WHERE status <> 'cancelled'".to_string() }
                    ),
                ),
                6 => (
                    "big_items",
                    format!("SELECT order_id, SUM(qty) AS qty, MAX(rev) AS max_rev FROM order_items WHERE qty >= {} GROUP BY order_id", 2 + v),
                ),
                7 => {
                    let lo = (v * 250 + k * 37) % 750;
                    ("amount_band", format!("SELECT id, amount, rev FROM orders WHERE amount BETWEEN {lo} AND {}", lo + 250))
                }
                8 => (
                    "cust_status",
                    if v == 0 {
                        "SELECT customer_id, status, COUNT(*) AS n FROM orders GROUP BY customer_id, status".to_string()
                    } else {
                        "SELECT customer_id, status, SUM(amount) AS total FROM orders GROUP BY customer_id, status".to_string()
                    },
                ),
                _ => (
                    "product_lines",
                    format!("SELECT id, order_id, product_id, qty FROM order_items WHERE product_id <= {}", (a.products * (v as u64 + 1) / 5).max(1)),
                ),
            };
            vd(&format!("mv{k:02}_{suffix}"), &sel)
        })
        .collect()
}

// =============================================================================================
// Schema + seed
// =============================================================================================

const SCHEMA: &[&str] = &[
    "CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, tier TINYINT NOT NULL,
       rev BIGINT NOT NULL DEFAULT 0, visits INT NOT NULL DEFAULT 0) ENGINE=InnoDB",
    "CREATE TABLE orders (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, customer_id INT NOT NULL,
       status VARCHAR(16) NOT NULL, amount DECIMAL(12,2) NOT NULL, rev BIGINT NOT NULL DEFAULT 0,
       note VARCHAR(64) NULL, KEY (customer_id), KEY (status)) ENGINE=InnoDB",
    "CREATE TABLE order_items (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id BIGINT NOT NULL,
       product_id INT NOT NULL, qty INT NOT NULL, price DECIMAL(10,2) NOT NULL, rev BIGINT NOT NULL DEFAULT 0,
       KEY (order_id), KEY (product_id)) ENGINE=InnoDB",
    "CREATE TABLE sessions (id INT NOT NULL PRIMARY KEY, user_id INT NOT NULL, hits BIGINT NOT NULL DEFAULT 0,
       data VARCHAR(200) NULL) ENGINE=InnoDB",
    "CREATE TABLE events_log (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, kind VARCHAR(16) NOT NULL,
       ref_id BIGINT NOT NULL, payload VARCHAR(255) NOT NULL,
       created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)) ENGINE=InnoDB",
];

async fn seed(conn: &mut Conn, a: &Args) -> Result<()> {
    for ddl in SCHEMA {
        exec(conn, ddl).await?;
    }
    // The engine starts streaming after the seed: keep the bulk rows out of the binlog (less IO)
    // when the user may do so (SYSTEM_VARIABLES_ADMIN / SUPER); otherwise they are logged.
    let unlogged = conn.query_drop("SET SESSION sql_log_bin = 0").await.is_ok();
    let t = Instant::now();
    let max = a.orders.max(a.items).max(a.customers).max(a.sessions);
    exec(
        conn,
        "CREATE TABLE _bench_seq (n INT UNSIGNED NOT NULL PRIMARY KEY) ENGINE=InnoDB",
    )
    .await?;
    exec(conn, "INSERT INTO _bench_seq VALUES (1)").await?;
    let mut have = 1u64;
    while have < max {
        exec(conn, &format!("INSERT INTO _bench_seq SELECT n + {have} FROM _bench_seq WHERE n + {have} <= {max}")).await?;
        have = (have * 2).min(max);
    }
    let (n, c) = (a.orders, a.customers);
    let tables: [(&str, u64, String); 4] = [
        ("customers", c, "INSERT INTO customers (id, name, tier, rev) SELECT n, CONCAT('cust', n), n % 4, 0 FROM _bench_seq".into()),
        (
            "orders",
            n,
            format!(
                "INSERT INTO orders (id, customer_id, status, amount, rev) SELECT n, ((n - 1) % {c}) + 1,
                   CASE WHEN n <= {PROBE_IDS} THEN 'new' ELSE ELT(1 + (n * 7) % 4, 'new', 'paid', 'shipped', 'cancelled') END,
                   (n % 1000) + 0.5, 0 FROM _bench_seq"
            ),
        ),
        (
            "order_items",
            a.items,
            format!(
                "INSERT INTO order_items (id, order_id, product_id, qty, price, rev) SELECT n, ((n - 1) % {n}) + 1,
                   ((n * 7) % {}) + 1, 1 + n % 5, (n % 50) + 0.25, 0 FROM _bench_seq",
                a.products.max(1)
            ),
        ),
        ("sessions", a.sessions, format!("INSERT INTO sessions (id, user_id, hits, data) SELECT n, (n % {c}) + 1, 0, 'x' FROM _bench_seq")),
    ];
    let mut steps = vec![
        format!("binlog {}", if unlogged { "off" } else { "on" }),
        format!("seq {:.1}s", t.elapsed().as_secs_f64()),
    ];
    for (name, rows, sql) in &tables {
        let ts = Instant::now();
        let mut lo = 1;
        while lo <= *rows {
            let hi = (lo + 49_999).min(*rows);
            exec(conn, &format!("{sql} WHERE n BETWEEN {lo} AND {hi}")).await?;
            lo = hi + 1;
        }
        steps.push(format!("{name} {:.1}s", ts.elapsed().as_secs_f64()));
    }
    exec(conn, "DROP TABLE _bench_seq").await?;
    if unlogged {
        exec(conn, "SET SESSION sql_log_bin = 1").await?;
    }
    exec(
        conn,
        "ANALYZE TABLE customers, orders, order_items, sessions",
    )
    .await?;
    eprintln!("[throughput] seed steps: {}", steps.join(", "));
    Ok(())
}

// =============================================================================================
// Workload generation (shared by the writer child)
// =============================================================================================

struct Shape {
    orders: u64,
    customers: u64,
    items: u64,
    sessions: u64,
    products: u64,
    hot: u64,
    /// (writer index, writers): each writer updates its own slice of the hot set, so writers do
    /// not serialize on each other's row locks (the engine still sees `hot` keys).
    part: (u64, u64),
}

impl Shape {
    fn of(a: &Args) -> Shape {
        Shape {
            orders: a.orders,
            customers: a.customers,
            items: a.items,
            sessions: a.sessions,
            products: a.products.max(1),
            hot: a.hot.max(1),
            part: (0, 1),
        }
    }
    fn for_writer(a: &Args, idx: usize) -> Shape {
        let mut s = Shape::of(a);
        if s.hot >= a.writers as u64 {
            s.part = (idx as u64, a.writers as u64);
        }
        s
    }
    fn order(&self, r: &mut Xoshiro256PlusPlus) -> u64 {
        r.random_range(PROBE_IDS + 1..=self.orders)
    }
    fn item(&self, r: &mut Xoshiro256PlusPlus) -> u64 {
        loop {
            let k = r.random_range(1..=self.items);
            if (k - 1) % self.orders >= PROBE_IDS {
                return k;
            }
        }
    }
    fn customer(&self, r: &mut Xoshiro256PlusPlus) -> u64 {
        r.random_range(PROBE_IDS + 1..=self.customers)
    }
    fn hot_order(&self, r: &mut Xoshiro256PlusPlus) -> u64 {
        let (w, n) = self.part;
        let slots = (self.hot - w).div_ceil(n);
        PROBE_IDS + 1 + w + n * r.random_range(0..slots)
    }
    fn hot_item(&self, r: &mut Xoshiro256PlusPlus) -> u64 {
        let o = self.hot_order(r);
        let per = (self.items - o) / self.orders + 1;
        o + r.random_range(0..per) * self.orders
    }
}

/// One row change: (lock-order rank, id, SQL). Sorting a transaction's ops by (rank, id) gives
/// every writer the same lock order (no deadlocks between writers).
fn gen_op(op: Op, r: &mut Xoshiro256PlusPlus, sh: &Shape) -> (u8, u64, String) {
    let amount = |r: &mut Xoshiro256PlusPlus| {
        format!(
            "{}.{:02}",
            r.random_range(1..1000u32),
            r.random_range(0..100u32)
        )
    };
    match op {
        Op::UpdOrder => {
            let id = sh.order(r);
            let s = STATUSES[r.random_range(0..4usize)];
            (1, id, format!("UPDATE orders SET status='{s}', amount={}, rev=rev+1 WHERE id={id}", amount(r)))
        }
        Op::HotOrder => {
            let id = sh.hot_order(r);
            (1, id, format!("UPDATE orders SET amount={}, rev=rev+1 WHERE id={id}", amount(r)))
        }
        Op::UpdOrderNote => {
            let id = sh.order(r);
            (1, id, format!("UPDATE orders SET note='n{}' WHERE id={id}", r.random_range(0..u32::MAX)))
        }
        Op::InsOrder => {
            let c = sh.customer(r);
            let s = STATUSES[r.random_range(0..4usize)];
            (9, 0, format!("INSERT INTO orders (customer_id, status, amount, rev) VALUES ({c}, '{s}', {}, 0)", amount(r)))
        }
        Op::UpdItem | Op::HotItem => {
            let id = if op == Op::HotItem { sh.hot_item(r) } else { sh.item(r) };
            (2, id, format!("UPDATE order_items SET qty={}, rev=rev+1 WHERE id={id}", r.random_range(1..20u32)))
        }
        Op::InsItem => {
            let o = sh.order(r);
            (
                9,
                0,
                format!(
                    "INSERT INTO order_items (order_id, product_id, qty, price, rev) VALUES ({o}, {}, {}, {}.{:02}, 0)",
                    r.random_range(1..=sh.products),
                    r.random_range(1..10u32),
                    r.random_range(1..100u32),
                    r.random_range(0..100u32)
                ),
            )
        }
        Op::DelItem => {
            let id = sh.item(r);
            (2, id, format!("DELETE FROM order_items WHERE id={id}"))
        }
        Op::UpdCustomer => {
            let id = sh.customer(r);
            (0, id, format!("UPDATE customers SET name='cust{id}x{}', tier={}, rev=rev+1 WHERE id={id}", r.random_range(0..1_000_000u32), r.random_range(0..4u32)))
        }
        Op::UpdCustomerVisits => {
            let id = sh.customer(r);
            (0, id, format!("UPDATE customers SET visits=visits+1 WHERE id={id}"))
        }
        Op::InsEvent => (
            9,
            0,
            format!(
                "INSERT INTO events_log (kind, ref_id, payload) VALUES ('{}', {}, 'payload-{:016x}-{:016x}-{:016x}')",
                ["click", "view", "login", "logout"][r.random_range(0..4usize)],
                r.random_range(1..=sh.orders),
                r.random::<u64>(),
                r.random::<u64>(),
                r.random::<u64>()
            ),
        ),
        Op::UpdSession => {
            let id = r.random_range(1..=sh.sessions);
            (3, id, format!("UPDATE sessions SET hits=hits+1, data='d{:016x}' WHERE id={id}", r.random::<u64>()))
        }
    }
}

fn pick_op(mix: &[(u32, Op)], r: &mut Xoshiro256PlusPlus) -> Op {
    let total: u32 = mix.iter().map(|m| m.0).sum();
    let mut x = r.random_range(0..total);
    for (w, op) in mix {
        if x < *w {
            return *op;
        }
        x -= w;
    }
    mix[0].1
}

fn gen_txn(mix: &[(u32, Op)], batch: usize, r: &mut Xoshiro256PlusPlus, sh: &Shape) -> String {
    let mut ops: Vec<(u8, u64, String)> =
        (0..batch).map(|_| gen_op(pick_op(mix, r), r, sh)).collect();
    ops.sort_by_key(|o| (o.0, o.1));
    let mut sql = String::with_capacity(batch * 80 + 32);
    sql.push_str("START TRANSACTION");
    for (_, _, s) in ops {
        sql.push(';');
        sql.push_str(&s);
    }
    sql.push_str(";COMMIT");
    sql
}

/// Runs a multi-statement text query, returning the sum of affected rows of all statements.
async fn exec_multi(conn: &mut Conn, sql: &str) -> std::result::Result<u64, mysql_async::Error> {
    let mut res = conn.query_iter(sql).await?;
    let mut total = res.affected_rows();
    loop {
        let _: Vec<Row> = res.collect().await?;
        if res.is_empty() {
            break;
        }
        total += res.affected_rows();
    }
    Ok(total)
}

fn server_err_code(e: &mysql_async::Error) -> Option<u16> {
    match e {
        mysql_async::Error::Server(s) => Some(s.code),
        _ => None,
    }
}

// =============================================================================================
// Writer child process (`--role writer`)
// =============================================================================================
//
// Protocol on stdout: `READY` once connected; after `GO` on stdin: `P <ms> <rows> <txns>
// <retries> <errors>` every 10 ms, finally `DONE <ms> <rows> <txns> <retries> <errors>
// <cpu_ticks> <p50_us> <p95_us> <p99_us> <max_us>`. `STOP` or EOF on stdin stops early.

#[derive(Default)]
struct Counters {
    rows: AtomicU64,
    txns: AtomicU64,
    retries: AtomicU64,
    errors: AtomicU64,
}

fn out_line(s: &str) {
    let mut o = std::io::stdout().lock();
    let _ = writeln!(o, "{s}");
    let _ = o.flush();
}

fn writer_main(a: Args) -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let dsn = a
            .writer_dsn
            .clone()
            .ok_or_else(|| anyhow!("--writer-dsn missing"))?;
        let opts = Opts::from_url(&dsn)?;
        let mut conns = Vec::new();
        for _ in 0..a.writers {
            conns.push(Conn::new(opts.clone()).await.context("writer connect")?);
        }
        let (go_tx, mut go_rx) = watch::channel(false);
        let stop = Arc::new(AtomicBool::new(false));
        {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let stdin = std::io::stdin();
                for line in stdin.lock().lines() {
                    match line.as_deref().map(str::trim) {
                        Ok("GO") => {
                            let _ = go_tx.send(true);
                        }
                        Ok("STOP") | Err(_) => break,
                        _ => {}
                    }
                }
                stop.store(true, Ordering::SeqCst);
                let _ = go_tx.send(true);
            });
        }
        out_line("READY");
        while !*go_rx.borrow() {
            go_rx.changed().await.ok();
        }
        let scn = scenario(&a.scenario).unwrap();
        let start = Instant::now();
        let deadline = start + Duration::from_secs_f64(a.seconds);
        let counters = Arc::new(Counters::default());
        let reporter = {
            let (c, stop) = (counters.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    out_line(&format!(
                        "P {} {} {} {} {}",
                        start.elapsed().as_millis(),
                        c.rows.load(Ordering::Relaxed),
                        c.txns.load(Ordering::Relaxed),
                        c.retries.load(Ordering::Relaxed),
                        c.errors.load(Ordering::Relaxed)
                    ));
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        };
        let interval = if a.rate > 0.0 {
            Some(Duration::from_secs_f64(
                a.batch as f64 * a.writers as f64 / a.rate,
            ))
        } else {
            None
        };
        let mut tasks = Vec::new();
        for (idx, mut conn) in conns.into_iter().enumerate() {
            let (c, stop, a) = (counters.clone(), stop.clone(), a.clone());
            tasks.push(tokio::spawn(async move {
                let sh = Shape::for_writer(&a, idx);
                let mut rng = Xoshiro256PlusPlus::seed_from_u64(
                    a.seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (idx as u64 + 1),
                );
                let mut lat: Vec<u32> = Vec::with_capacity(1 << 16);
                let mut next = interval.map(|iv| start + iv.mul_f64(idx as f64 / a.writers as f64));
                let mut consecutive_errors = 0;
                loop {
                    if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                        break;
                    }
                    if let (Some(n), Some(iv)) = (next.as_mut(), interval) {
                        if *n > Instant::now() {
                            tokio::time::sleep_until((*n).into()).await;
                        }
                        *n += iv;
                        if Instant::now() >= deadline {
                            break;
                        }
                    }
                    let sql = gen_txn(scn.mix, a.batch, &mut rng, &sh);
                    let t = Instant::now();
                    match exec_multi(&mut conn, &sql).await {
                        Ok(n) => {
                            c.rows.fetch_add(n, Ordering::Relaxed);
                            c.txns.fetch_add(1, Ordering::Relaxed);
                            lat.push(t.elapsed().as_micros().min(u32::MAX as u128) as u32);
                            consecutive_errors = 0;
                        }
                        Err(e) => {
                            let _ = conn.query_drop("ROLLBACK").await;
                            if matches!(server_err_code(&e), Some(1213 | 1205)) {
                                c.retries.fetch_add(1, Ordering::Relaxed);
                            } else {
                                c.errors.fetch_add(1, Ordering::Relaxed);
                                consecutive_errors += 1;
                                eprintln!("writer {idx}: {e}");
                                if consecutive_errors > 20 {
                                    eprintln!("writer {idx}: too many errors, giving up");
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                }
                let _ = conn.disconnect().await;
                lat
            }));
        }
        let mut lat: Vec<u32> = Vec::new();
        for t in tasks {
            lat.extend(t.await.unwrap_or_default());
        }
        let ms = start.elapsed().as_millis();
        stop.store(true, Ordering::SeqCst);
        let _ = reporter.await;
        lat.sort_unstable();
        let q = |p: f64| {
            if lat.is_empty() {
                0
            } else {
                lat[((p * lat.len() as f64).ceil() as usize).clamp(1, lat.len()) - 1]
            }
        };
        let cpu = proc_stat_ticks("/proc/self/stat").map(|x| x.1).unwrap_or(0);
        out_line(&format!(
            "DONE {ms} {} {} {} {} {cpu} {} {} {} {}",
            counters.rows.load(Ordering::SeqCst),
            counters.txns.load(Ordering::SeqCst),
            counters.retries.load(Ordering::SeqCst),
            counters.errors.load(Ordering::SeqCst),
            q(0.5),
            q(0.95),
            q(0.99),
            lat.last().copied().unwrap_or(0)
        ));
        Ok(())
    })
}

/// Parent side of the writer child.
#[derive(Default)]
struct WriterState {
    rows: AtomicU64,
    txns: AtomicU64,
    retries: AtomicU64,
    errors: AtomicU64,
    ready: AtomicBool,
    done: Mutex<Option<Vec<u64>>>,
    exited: AtomicBool,
}

struct WriterProc {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    state: Arc<WriterState>,
}

impl WriterProc {
    /// Re-runs this binary as `--role writer` with the parent's arguments; `overrides` are
    /// appended (later arguments win), e.g. a different `--seconds` for the catch-up backlog.
    fn spawn(db_dsn: &str, overrides: &[String]) -> Result<WriterProc> {
        let exe = std::env::current_exe()?;
        let mut args: Vec<String> = std::env::args().skip(1).collect();
        args.extend(overrides.iter().cloned());
        args.extend([
            "--role".into(),
            "writer".into(),
            "--writer-dsn".into(),
            db_dsn.into(),
        ]);
        let mut child = Command::new(exe)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawn writer process")?;
        let stdout = child.stdout.take().unwrap();
        let state = Arc::new(WriterState::default());
        let st = state.clone();
        std::thread::Builder::new()
            .name("lvb-pipe".into())
            .spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    let mut it = line.split_whitespace();
                    match it.next() {
                        Some("READY") => st.ready.store(true, Ordering::SeqCst),
                        Some(tag @ ("P" | "DONE")) => {
                            let v: Vec<u64> = it.filter_map(|x| x.parse().ok()).collect();
                            if v.len() >= 5 {
                                st.rows.store(v[1], Ordering::SeqCst);
                                st.txns.store(v[2], Ordering::SeqCst);
                                st.retries.store(v[3], Ordering::SeqCst);
                                st.errors.store(v[4], Ordering::SeqCst);
                            }
                            if tag == "DONE" {
                                *st.done.lock().unwrap() = Some(v);
                            }
                        }
                        _ => {}
                    }
                }
                st.exited.store(true, Ordering::SeqCst);
            })?;
        let stdin = child.stdin.take();
        Ok(WriterProc {
            child,
            stdin,
            state,
        })
    }

    async fn wait_ready(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !self.state.ready.load(Ordering::SeqCst) {
            if self.state.exited.load(Ordering::SeqCst) || Instant::now() > deadline {
                bail!("writer process did not become ready");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    fn send(&mut self, s: &str) {
        if let Some(i) = self.stdin.as_mut() {
            let _ = writeln!(i, "{s}");
            let _ = i.flush();
        }
    }
}

impl Drop for WriterProc {
    fn drop(&mut self) {
        self.stdin.take();
        if self.child.try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(500));
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

// =============================================================================================
// /proc helpers
// =============================================================================================

fn clk_tck() -> f64 {
    static T: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *T.get_or_init(|| {
        Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
            .unwrap_or(100.0)
    })
}

/// (comm, utime + stime ticks) of a /proc stat file.
fn proc_stat_ticks(path: &str) -> Option<(String, u64)> {
    let s = std::fs::read_to_string(path).ok()?;
    let (open, close) = (s.find('(')?, s.rfind(')')?);
    let comm = s[open + 1..close].to_string();
    let rest: Vec<&str> = s[close + 1..].split_whitespace().collect();
    let ut: u64 = rest.get(11)?.parse().ok()?;
    let st: u64 = rest.get(12)?.parse().ok()?;
    Some((comm, ut + st))
}

/// CPU ticks of the whole process and of the harness threads (main thread + `lvb-*`).
fn cpu_split() -> (u64, u64) {
    let total = proc_stat_ticks("/proc/self/stat").map(|x| x.1).unwrap_or(0);
    let pid = std::process::id().to_string();
    let mut harness = 0;
    if let Ok(rd) = std::fs::read_dir("/proc/self/task") {
        for e in rd.flatten() {
            let tid = e.file_name().to_string_lossy().to_string();
            if let Some((comm, t)) = proc_stat_ticks(&format!("/proc/self/task/{tid}/stat")) {
                if tid == pid || comm.starts_with("lvb-") {
                    harness += t;
                }
            }
        }
    }
    (total, harness)
}

/// (VmRSS, VmHWM) in kB.
fn rss_kb() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        s.lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

/// (busy, total) jiffies of all CPUs from /proc/stat.
fn sys_cpu() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let v: Vec<u64> = s
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .skip(1)
        .take(8)
        .filter_map(|x| x.parse().ok())
        .collect();
    if v.len() < 5 {
        return (0, 0);
    }
    let total: u64 = v.iter().sum();
    (total - v[3] - v[4], total)
}

fn loadavg() -> Vec<f64> {
    std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_default()
        .split_whitespace()
        .take(3)
        .filter_map(|x| x.parse().ok())
        .collect()
}

fn uptime_line() -> String {
    Command::new("uptime")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

fn cpus_online() -> usize {
    std::fs::read_to_string("/proc/cpuinfo")
        .map(|s| s.lines().filter(|l| l.starts_with("processor")).count())
        .unwrap_or(0)
}

fn ncpu() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[derive(Clone, Copy)]
struct CpuMark {
    at: Instant,
    total: u64,
    harness: u64,
    mysqld: Option<u64>,
    sys: (u64, u64),
}

fn cpu_mark(mysqld_pid: Option<u32>) -> CpuMark {
    let (total, harness) = cpu_split();
    CpuMark {
        at: Instant::now(),
        total,
        harness,
        mysqld: mysqld_pid.and_then(|p| proc_stat_ticks(&format!("/proc/{p}/stat")).map(|x| x.1)),
        sys: sys_cpu(),
    }
}

struct CpuDelta {
    wall: f64,
    engine_s: f64,
    harness_s: f64,
    mysqld_s: Option<f64>,
    sys_busy_pct: Option<f64>,
}

fn cpu_delta(a: &CpuMark, b: &CpuMark) -> CpuDelta {
    let tck = clk_tck();
    let wall = b.at.duration_since(a.at).as_secs_f64().max(1e-9);
    let harness = b.harness.saturating_sub(a.harness);
    let total = b.total.saturating_sub(a.total);
    let (busy, all) = (
        b.sys.0.saturating_sub(a.sys.0),
        b.sys.1.saturating_sub(a.sys.1),
    );
    CpuDelta {
        wall,
        engine_s: total.saturating_sub(harness) as f64 / tck,
        harness_s: harness as f64 / tck,
        mysqld_s: match (a.mysqld, b.mysqld) {
            (Some(x), Some(y)) => Some(y.saturating_sub(x) as f64 / tck),
            _ => None,
        },
        sys_busy_pct: (all > 0).then(|| busy as f64 * 100.0 / all as f64),
    }
}

// =============================================================================================
// SQL helpers
// =============================================================================================

async fn exec(conn: &mut Conn, sql: &str) -> Result<()> {
    conn.query_drop(sql)
        .await
        .with_context(|| format!("SQL failed: {}", clip(sql, 300)))
}

fn clip(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut e = n;
    while !s.is_char_boundary(e) {
        e -= 1;
    }
    format!("{}…", &s[..e])
}

fn vtext(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        o => Some(o.as_sql(true)),
    }
}

async fn rows(conn: &mut Conn, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let rs: Vec<Row> = conn
        .query(sql)
        .await
        .with_context(|| format!("SQL failed: {}", clip(sql, 300)))?;
    Ok(rs
        .into_iter()
        .map(|r| r.unwrap().iter().map(vtext).collect())
        .collect())
}

async fn scalar(conn: &mut Conn, sql: &str) -> Result<Option<String>> {
    Ok(rows(conn, sql)
        .await?
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next().flatten()))
}

async fn scalar_u64(conn: &mut Conn, sql: &str) -> Result<u64> {
    let v = scalar(conn, sql)
        .await?
        .ok_or_else(|| anyhow!("no value: {sql}"))?;
    v.parse().with_context(|| format!("not a number: {v}"))
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Replaces the database of a `mysql://` DSN.
fn dsn_with_db(dsn: &str, db: &str) -> String {
    let (scheme, rest) = dsn.split_once("://").unwrap_or(("mysql", dsn));
    let (before_q, query) = match rest.find('?') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let authority = before_q.split('/').next().unwrap_or(before_q);
    format!("{scheme}://{authority}/{db}{query}")
}

async fn connect(opts: Opts) -> Result<Conn> {
    let mut last = None;
    for attempt in 0..5u64 {
        match Conn::new(opts.clone()).await {
            Ok(c) => return Ok(c),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
    }
    Err(anyhow!(last.unwrap()).context("connect"))
}

/// Binlog coordinates as one monotonically increasing byte offset (sum of the sizes of the
/// earlier files + position), robust against rotation during a run.
#[derive(Default)]
struct BinlogMap {
    bases: HashMap<String, u64>,
}

impl BinlogMap {
    async fn refresh(&mut self, conn: &mut Conn) -> Result<()> {
        let list = rows(conn, "SHOW BINARY LOGS").await?;
        let mut prev: Option<(String, u64)> = None;
        for r in list {
            let name = r.first().cloned().flatten().unwrap_or_default();
            let size: u64 = r
                .get(1)
                .cloned()
                .flatten()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let base = match (self.bases.get(&name), &prev) {
                (Some(b), _) => *b,
                (None, Some((p, ps))) => self.bases.get(p).copied().unwrap_or(0) + ps,
                (None, None) => 0,
            };
            self.bases.insert(name.clone(), base);
            prev = Some((name, size));
        }
        Ok(())
    }

    async fn abs(&mut self, conn: &mut Conn, p: &Position) -> Result<Option<u64>> {
        if !self.bases.contains_key(&p.file) {
            self.refresh(conn).await?;
        }
        Ok(self.bases.get(&p.file).map(|b| b + p.pos))
    }
}

// =============================================================================================
// Engine handle (own runtime + thread, like the integration-test harness)
// =============================================================================================

struct EngineHandle {
    metrics: Arc<Metrics>,
    shutdown: watch::Sender<bool>,
    done: std::sync::mpsc::Receiver<std::result::Result<(), String>>,
    exit: Option<std::result::Result<(), String>>,
}

impl EngineHandle {
    fn spawn(cfg: Config, threads: usize) -> Result<EngineHandle> {
        let metrics = Arc::new(Metrics::default());
        let (sd_tx, sd_rx) = watch::channel(false);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let m = metrics.clone();
        std::thread::Builder::new()
            .name("lv-eng-main".into())
            .spawn(move || {
                let mut b = tokio::runtime::Builder::new_multi_thread();
                if threads > 0 {
                    b.worker_threads(threads);
                }
                let rt = b
                    .enable_all()
                    .thread_name("lv-eng")
                    .build()
                    .expect("engine runtime");
                let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(lazyview::engine::run(cfg, m, sd_rx))
                }));
                rt.shutdown_timeout(Duration::from_secs(5));
                let _ = done_tx.send(match res {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(format!("engine error: {e:#}")),
                    Err(_) => Err("engine panicked".into()),
                });
            })?;
        Ok(EngineHandle {
            metrics,
            shutdown: sd_tx,
            done: done_rx,
            exit: None,
        })
    }

    fn check_alive(&mut self) -> Result<()> {
        if self.exit.is_none() {
            match self.done.try_recv() {
                Ok(r) => self.exit = Some(r),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(_) => self.exit = Some(Err("engine thread vanished".into())),
            }
        }
        match &self.exit {
            None => Ok(()),
            Some(Ok(())) => bail!("engine exited unexpectedly"),
            Some(Err(e)) => bail!("engine died: {e}"),
        }
    }

    async fn wait_applied(&mut self, target: &Position, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .metrics
                .applied()
                .map(|a| &a >= target)
                .unwrap_or(false)
            {
                return Ok(());
            }
            self.check_alive()?;
            if Instant::now() > deadline {
                bail!(
                    "engine not caught up within {timeout:?}: applied={:?} target={target}",
                    self.metrics.applied().map(|p| p.to_string())
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Captures the server's binlog end and waits until it is applied, `rounds` times.
    async fn catch_up(&mut self, conn: &mut Conn, timeout: Duration, rounds: usize) -> Result<()> {
        let deadline = Instant::now() + timeout;
        for _ in 0..rounds {
            let target = lazyview::binlog::current_position(conn).await?;
            self.wait_applied(&target, deadline.saturating_duration_since(Instant::now()))
                .await?;
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        let _ = self.shutdown.send(true);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match self.check_alive() {
                Ok(()) => {}
                Err(_) if matches!(self.exit, Some(Ok(()))) => return Ok(()),
                Err(e) => return Err(e),
            }
            if Instant::now() > deadline {
                bail!("engine did not stop within 60 s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for EngineHandle {
    /// Error paths: ask the engine to stop (a dropped sender alone would not).
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

// =============================================================================================
// Sampler, timeline analysis
// =============================================================================================

#[derive(Clone, Copy, Debug)]
struct Sample {
    /// Seconds since load start.
    t: f64,
    server: u64,
    applied: Option<u64>,
    rows: u64,
    rss_kb: u64,
}

async fn sampler(
    mut conn: Conn,
    metrics: Arc<Metrics>,
    ws: Arc<WriterState>,
    t0: Instant,
    every: Duration,
    stop: Arc<AtomicBool>,
    out: Arc<Mutex<Vec<Sample>>>,
) -> Result<()> {
    let mut map = BinlogMap::default();
    map.refresh(&mut conn).await?;
    let mut next = Instant::now();
    loop {
        let pos = lazyview::binlog::current_position(&mut conn).await?;
        let rows = ws.rows.load(Ordering::SeqCst);
        let t = t0.elapsed().as_secs_f64();
        let applied = metrics.applied();
        let server = map.abs(&mut conn, &pos).await?.unwrap_or(0);
        let applied = match applied {
            Some(p) => map.abs(&mut conn, &p).await?,
            None => None,
        };
        out.lock().unwrap().push(Sample {
            t,
            server,
            applied,
            rows,
            rss_kb: rss_kb().0,
        });
        if stop.load(Ordering::SeqCst) {
            break;
        }
        next += every;
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        } else {
            next = now;
        }
    }
    conn.disconnect().await.ok();
    Ok(())
}

struct Tl<'a> {
    s: &'a [Sample],
}

impl Tl<'_> {
    /// Interpolates `f` at the moment the server binlog reached byte offset `abs`.
    fn at_pos(&self, abs: u64, f: impl Fn(&Sample) -> f64) -> f64 {
        let i = self.s.partition_point(|x| x.server < abs);
        if i == 0 {
            return f(&self.s[0]);
        }
        if i >= self.s.len() {
            return f(self.s.last().unwrap());
        }
        let (a, b) = (&self.s[i - 1], &self.s[i]);
        let span = b.server.saturating_sub(a.server) as f64;
        let frac = if span > 0.0 {
            (abs - a.server) as f64 / span
        } else {
            1.0
        };
        f(a) + frac * (f(b) - f(a))
    }

    /// Source rows the engine had applied at sample `i`.
    fn applied_rows(&self, i: usize) -> f64 {
        let x = &self.s[i];
        match x.applied {
            None => 0.0,
            Some(ap) if ap >= x.server => x.rows as f64,
            Some(ap) => self.at_pos(ap, |y| y.rows as f64).min(x.rows as f64),
        }
    }

    /// (lag seconds, rows behind) at sample `i`.
    fn lag(&self, i: usize) -> (f64, f64) {
        let x = &self.s[i];
        match x.applied {
            None => (x.t - self.s[0].t, x.rows as f64),
            Some(ap) if ap >= x.server => (0.0, 0.0),
            Some(ap) => (
                (x.t - self.at_pos(ap, |y| y.t)).max(0.0),
                (x.rows as f64 - self.applied_rows(i)).max(0.0),
            ),
        }
    }

    /// Index of the last sample with t <= `t` (or 0).
    fn idx(&self, t: f64) -> usize {
        self.s.partition_point(|x| x.t <= t).saturating_sub(1)
    }
}

fn slope(points: &[(f64, f64)]) -> f64 {
    let n = points.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    let mx = points.iter().map(|p| p.0).sum::<f64>() / n;
    let my = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|p| (p.0 - mx).powi(2)).sum();
    let sxy: f64 = points.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    if sxx == 0.0 {
        0.0
    } else {
        sxy / sxx
    }
}

// =============================================================================================
// Freshness probe
// =============================================================================================

#[derive(Default, Debug, Clone)]
struct ProbeResult {
    lat_ms: Vec<f64>,
    timeouts: u64,
    changes: u64,
}

struct Prober {
    spec: ProbeSpec,
    w: Conn,
    r: Conn,
    /// Per probe row id: (`{ids}` substitution, expected count).
    targets: Vec<(u64, String, u64)>,
    marker: u64,
    turn: usize,
}

impl Prober {
    async fn new(opts: &Opts, spec: ProbeSpec) -> Result<Prober> {
        let mut w = connect(opts.clone()).await?;
        let r = connect(opts.clone()).await?;
        let mut targets = Vec::new();
        for p in 1..=PROBE_IDS {
            match &spec.ids_sql {
                Some(sql) => {
                    let ids: Vec<String> = rows(&mut w, &sql.replace("{p}", &p.to_string()))
                        .await?
                        .into_iter()
                        .filter_map(|r| r.into_iter().next().flatten())
                        .collect();
                    if ids.is_empty() {
                        bail!("probe {p}: no rows for {sql}");
                    }
                    targets.push((p, ids.join(","), ids.len() as u64));
                }
                None => targets.push((p, String::new(), 1)),
            }
        }
        Ok(Prober {
            spec,
            w,
            r,
            targets,
            marker: MARKER_BASE,
            turn: 0,
        })
    }

    async fn run(
        &mut self,
        period: Duration,
        poll: Duration,
        timeout: Duration,
        until: impl Fn() -> bool,
    ) -> Result<ProbeResult> {
        let poll = poll.max(Duration::from_millis(self.spec.min_poll_ms));
        let mut res = ProbeResult::default();
        while !until() {
            let (p, ids, expect) = self.targets[self.turn % self.targets.len()].clone();
            self.turn += 1;
            self.marker += 1;
            let m = self.marker.to_string();
            let sub = |s: &str| {
                s.replace("{p}", &p.to_string())
                    .replace("{m}", &m)
                    .replace("{ids}", &ids)
            };
            let check = format!(
                "SELECT COUNT(*) FROM {} WHERE {}",
                ident(&self.spec.view),
                sub(&self.spec.check)
            );
            let start = Instant::now();
            exec(&mut self.w, &sub(&self.spec.update)).await?;
            let t0 = Instant::now();
            res.changes += 1;
            loop {
                if scalar_u64(&mut self.r, &check).await? >= expect {
                    res.lat_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
                    break;
                }
                if t0.elapsed() > timeout {
                    res.timeouts += 1;
                    break;
                }
                tokio::time::sleep(poll).await;
            }
            let next = start + period;
            while Instant::now() < next && !until() {
                tokio::time::sleep((next - Instant::now()).min(Duration::from_millis(20))).await;
            }
        }
        Ok(res)
    }
}

/// Latency of single-row autocommit UPDATEs on a scratch table no view reads (ms): the cost of
/// one durable commit on this server right now (fsync of redo log + binlog).
async fn commit_latency(conn: &mut Conn, n: usize) -> Result<Vec<f64>> {
    exec(conn, "CREATE TABLE IF NOT EXISTS _bench_commit (id INT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB").await?;
    exec(conn, "REPLACE INTO _bench_commit VALUES (1, 0)").await?;
    let mut out = Vec::new();
    for _ in 0..n {
        let t = Instant::now();
        exec(conn, "UPDATE _bench_commit SET v = v + 1 WHERE id = 1").await?;
        out.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(out)
}

fn pct(v: &[f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1]
}

fn probe_json(p: &ProbeResult) -> J {
    let mean = if p.lat_ms.is_empty() {
        f64::NAN
    } else {
        p.lat_ms.iter().sum::<f64>() / p.lat_ms.len() as f64
    };
    json!({
        "n": p.lat_ms.len(), "timeouts": p.timeouts,
        "p50_ms": rnd(pct(&p.lat_ms, 0.5), 1), "p95_ms": rnd(pct(&p.lat_ms, 0.95), 1),
        "p99_ms": rnd(pct(&p.lat_ms, 0.99), 1), "max_ms": rnd(pct(&p.lat_ms, 1.0), 1), "mean_ms": rnd(mean, 1),
    })
}

/// JSON number rounded to `d` decimals (NaN/inf → null).
fn rnd(x: f64, d: i32) -> J {
    if !x.is_finite() {
        return J::Null;
    }
    let f = 10f64.powi(d);
    json!((x * f).round() / f)
}

// =============================================================================================
// Correctness: MV vs oracle
// =============================================================================================

async fn columns_of(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
    Ok(rows(
        conn,
        &format!("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"),
    )
    .await?
    .into_iter()
    .filter_map(|r| r.into_iter().next().flatten())
    .collect())
}

async fn checksum(conn: &mut Conn, table: &str, cols: &[String]) -> Result<(u64, String)> {
    let expr = cols
        .iter()
        .map(|c| format!("QUOTE({})", ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let r = rows(
        conn,
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS('|', {expr}))), 0) FROM {}",
            ident(table)
        ),
    )
    .await?;
    let r = r
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no checksum row"))?;
    Ok((
        r[0].as_deref().unwrap_or("0").parse()?,
        r[1].clone().unwrap_or_default(),
    ))
}

/// Up to 5 rows only in the MV and only in the oracle.
async fn row_diff(conn: &mut Conn, mv: &str, oracle: &str, cols: &[String]) -> Result<String> {
    let list = cols.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ");
    let mut a = rows(conn, &format!("SELECT {list} FROM {}", ident(mv))).await?;
    let mut b = rows(conn, &format!("SELECT {list} FROM {}", ident(oracle))).await?;
    a.sort();
    b.sort();
    let (mut i, mut j, mut only_a, mut only_b) = (0, 0, Vec::new(), Vec::new());
    while i < a.len() || j < b.len() {
        match (a.get(i), b.get(j)) {
            (Some(x), Some(y)) if x == y => {
                i += 1;
                j += 1;
            }
            (Some(x), Some(y)) if x < y => {
                only_a.push(x.clone());
                i += 1;
            }
            (Some(_), Some(y)) => {
                only_b.push(y.clone());
                j += 1;
            }
            (Some(x), None) => {
                only_a.push(x.clone());
                i += 1;
            }
            (None, Some(y)) => {
                only_b.push(y.clone());
                j += 1;
            }
            (None, None) => break,
        }
    }
    let fmt = |v: &[Vec<Option<String>>]| {
        v.iter()
            .take(5)
            .map(|r| {
                format!(
                    "({})",
                    r.iter()
                        .map(|c| c.clone().unwrap_or("NULL".into()))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    Ok(format!(
        "{} rows only in MV: {}; {} rows only in oracle: {}",
        only_a.len(),
        fmt(&only_a),
        only_b.len(),
        fmt(&only_b)
    ))
}

/// Compares every MV with its oracle view (call after catch-up): COUNT + SUM(CRC32) over all
/// columns; on a mismatch waits, catches up again and reports a row diff. (all equal, per view).
async fn verify(
    c: &mut Conn,
    eng: &mut EngineHandle,
    views: &[ViewDef],
    modes: &HashMap<String, String>,
    full_refresh: Duration,
    full_slack: Duration,
) -> Result<(bool, Vec<J>)> {
    let mut checks = Vec::new();
    let mut all_ok = true;
    for v in views {
        let oracle = format!("_oracle_{}", v.name);
        let cols = columns_of(c, &oracle).await?;
        let mut mv = checksum(c, &v.name, &cols).await?;
        let or = checksum(c, &oracle, &cols).await?;
        let mut detail = None;
        let mut converge_s = None;
        if mv != or && modes.get(&v.name).map(String::as_str) == Some("full") {
            // Documented staleness: a full-mode view is rebuilt (shadow + swap) at most once per
            // LAZYVIEW_FULL_REFRESH_MS, and applied() does not wait for that rebuild - even with a
            // cadence of 0 the rebuild runs in the background after the position moved on. Wait
            // for it (cadence + full_slack: max(60 s, 5 x the initial build time)) and report how
            // long it took; a view that does not converge by then fails.
            let t = Instant::now();
            let deadline = t + full_refresh + full_slack;
            while mv != or && Instant::now() < deadline {
                eng.check_alive()?;
                tokio::time::sleep(Duration::from_millis(250)).await;
                if Metrics::get(&eng.metrics.gauges.views_building) == 0 {
                    mv = checksum(c, &v.name, &cols).await?;
                }
            }
            converge_s = Some(rnd(t.elapsed().as_secs_f64(), 1));
        }
        if mv != or {
            tokio::time::sleep(Duration::from_secs(3)).await;
            eng.catch_up(c, Duration::from_secs(60), 2).await.ok();
            mv = checksum(c, &v.name, &cols).await?;
            if mv != or {
                detail = Some(row_diff(c, &v.name, &oracle, &cols).await?);
            } else {
                detail = Some("converged only after an extra wait (applied() reported before the change was visible)".to_string());
            }
            all_ok = false;
        }
        checks.push(json!({"name": v.name, "rows": mv.0, "oracle_rows": or.0, "ok": detail.is_none(), "detail": detail, "full_refresh_wait_s": converge_s}));
    }
    Ok((all_ok, checks))
}

/// Efficiency figures from Metrics deltas `d` over `changes` source row changes; `flushes` are
/// the flush durations (ms) inside a window of `wall` seconds.
fn efficiency_json(
    d: &dyn Fn(&str) -> u64,
    keys: &[&'static str],
    changes: f64,
    probe_changes: u64,
    flushes: &[f64],
    wall: f64,
) -> J {
    let per1k = |k: &str| rnd(d(k) as f64 * 1000.0 / changes, 2);
    let ratio = |x: u64, y: u64| {
        if y == 0 {
            J::Null
        } else {
            rnd(x as f64 / y as f64, 3)
        }
    };
    let delta: serde_json::Map<String, J> =
        keys.iter().map(|k| (k.to_string(), json!(d(k)))).collect();
    json!({
        "changes": changes as u64, "probe_changes": probe_changes,
        "binlog_events": d("events"),
        "rows_seen": d("rows_seen"), "rows_skipped": d("rows_skipped"),
        "units_refreshed": d("units_refreshed"), "units_changed": d("units_changed"),
        "dedup_ratio": ratio(d("rows_seen"), d("units_refreshed")),
        "units_refreshed_per_change": ratio(d("units_refreshed"), changes as u64),
        "units_changed_frac": ratio(d("units_changed"), d("units_refreshed")),
        "source_queries_per_1k": per1k("source_queries"), "lookup_queries_per_1k": per1k("lookup_queries"),
        "mv_queries_per_1k": per1k("mv_queries"),
        "mv_rows_written_per_change": rnd((d("mv_rows_inserted") + d("mv_rows_deleted")) as f64 / changes, 3),
        "flushes": d("flushes"), "units_per_flush": ratio(d("units_refreshed"), d("flushes")),
        "flush_ms": if flushes.is_empty() { J::Null } else { json!({
            "n": flushes.len(), "p50": rnd(pct(flushes, 0.5), 1), "p95": rnd(pct(flushes, 0.95), 1), "max": rnd(pct(flushes, 1.0), 1),
            "busy_pct": rnd(flushes.iter().sum::<f64>() / 10.0 / wall.max(1e-9), 1),
        }) },
        "full_rebuilds": d("full_rebuilds"), "metrics_delta": delta,
    })
}

fn flushes_between(from: Instant, to: Instant) -> Vec<f64> {
    FLUSHES
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, _)| *at >= from && *at <= to)
        .map(|x| x.1)
        .collect()
}

// =============================================================================================
// Benchmark run
// =============================================================================================

struct ServerInfo {
    version: String,
    flavor: String,
    settings: J,
    mysqld_pid: Option<u32>,
}

async fn server_info(conn: &mut Conn) -> Result<ServerInfo> {
    let version = scalar(conn, "SELECT VERSION()").await?.unwrap_or_default();
    let comment = scalar(conn, "SELECT @@version_comment")
        .await?
        .unwrap_or_default();
    let flavor = if version.to_lowercase().contains("mariadb") {
        "mariadb"
    } else if comment.to_lowercase().contains("percona") {
        "percona"
    } else {
        "mysql"
    };
    let mut settings = serde_json::Map::new();
    for v in [
        "gtid_mode",
        "sync_binlog",
        "innodb_flush_log_at_trx_commit",
        "innodb_buffer_pool_size",
        "binlog_format",
        "binlog_row_image",
        "max_binlog_size",
    ] {
        let val = scalar(conn, &format!("SELECT @@{v}")).await.ok().flatten();
        settings.insert(v.into(), val.map(J::from).unwrap_or(J::Null));
    }
    let mysqld_pid = scalar(conn, "SELECT @@pid_file")
        .await
        .ok()
        .flatten()
        .and_then(|f| std::fs::read_to_string(f).ok())
        .and_then(|s| s.trim().parse().ok())
        .filter(|p: &u32| std::path::Path::new(&format!("/proc/{p}/stat")).exists());
    Ok(ServerInfo {
        version,
        flavor: flavor.into(),
        settings: J::Object(settings),
        mysqld_pid,
    })
}

async fn wait_ready(
    conn: &mut Conn,
    eng: &mut EngineHandle,
    names: &[String],
    timeout: Duration,
) -> Result<HashMap<String, String>> {
    let deadline = Instant::now() + timeout;
    let mut error_since: Option<Instant> = None;
    loop {
        eng.check_alive()?;
        let st = rows(
            conn,
            "SELECT name, state, mode, last_error FROM _lazyview_state WHERE target = 'source'",
        )
        .await
        .unwrap_or_default();
        let mut modes = HashMap::new();
        let mut pending = Vec::new();
        let mut errors = Vec::new();
        for n in names {
            match st.iter().find(|r| r[0].as_deref() == Some(n.as_str())) {
                Some(r) if r[1].as_deref() == Some("ready") => {
                    modes.insert(n.clone(), r[2].clone().unwrap_or_default());
                }
                Some(r) if r[1].as_deref() == Some("error") => {
                    errors.push(format!("{n}: {}", r[3].clone().unwrap_or_default()))
                }
                Some(r) => pending.push(format!("{n}={}", r[1].clone().unwrap_or_default())),
                None => pending.push(format!("{n}=<none>")),
            }
        }
        if pending.is_empty() && errors.is_empty() {
            return Ok(modes);
        }
        if !errors.is_empty() {
            let since = *error_since.get_or_insert_with(Instant::now);
            if since.elapsed() > Duration::from_secs(5) {
                bail!("view(s) in error: {}", errors.join("; "));
            }
        } else {
            error_since = None;
        }
        if Instant::now() > deadline {
            bail!("views not ready within {timeout:?}: {}", pending.join(", "));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // civil-from-days (H. Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn eprog(t0: Instant, msg: impl AsRef<str>) {
    eprintln!(
        "[throughput +{:6.1}s] {}",
        t0.elapsed().as_secs_f64(),
        msg.as_ref()
    );
}

async fn bench(a: Args) -> Result<bool> {
    let run_t0 = Instant::now();
    let scn = scenario(&a.scenario).unwrap();
    let base_opts = Opts::from_url(&a.dsn).context("bad --dsn")?;
    let prefix = base_opts
        .db_name()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "bench".into());
    let db =
        a.db.clone()
            .unwrap_or_else(|| format!("{prefix}_{}", a.scenario));
    if !db.starts_with("bench") || db.contains('`') {
        bail!("database name `{db}` must start with \"bench\" (it is dropped and recreated)");
    }
    let server_opts: Opts = OptsBuilder::from_opts(base_opts.clone())
        .db_name(None::<String>)
        .into();
    let db_dsn = dsn_with_db(&a.dsn, &db);
    let db_opts = Opts::from_url(&db_dsn)?;
    let hostport = format!("{}:{}", base_opts.ip_or_hostname(), base_opts.tcp_port());

    let mut admin = connect(server_opts.clone()).await?;
    let info = server_info(&mut admin).await?;
    eprog(
        run_t0,
        format!(
            "{} on {} {} ({hostport}), database {db}",
            scn.name, info.flavor, info.version
        ),
    );
    exec(
        &mut admin,
        &format!("DROP DATABASE IF EXISTS {}", ident(&db)),
    )
    .await?;
    exec(
        &mut admin,
        &format!(
            "CREATE DATABASE {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            ident(&db)
        ),
    )
    .await?;

    let result = run_in_db(&a, scn, &info, &db, &db_dsn, &db_opts, &hostport, run_t0).await;

    if !a.keep_db {
        if let Err(e) = exec(
            &mut admin,
            &format!("DROP DATABASE IF EXISTS {}", ident(&db)),
        )
        .await
        {
            eprintln!("warning: could not drop {db}: {e:#}");
        }
    }
    admin.disconnect().await.ok();
    let report = result?;
    let valid = report["valid"].as_bool().unwrap_or(false);
    print_summary(&report);
    if let Some(dir) = a.out.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&a.out)
        .with_context(|| format!("open {}", a.out.display()))?;
    writeln!(f, "{}", serde_json::to_string(&report)?)?;
    eprog(run_t0, format!("appended result to {}", a.out.display()));
    Ok(valid)
}

#[allow(clippy::too_many_arguments)]
async fn run_in_db(
    a: &Args,
    scn: &Scenario,
    info: &ServerInfo,
    db: &str,
    db_dsn: &str,
    db_opts: &Opts,
    hostport: &str,
    run_t0: Instant,
) -> Result<J> {
    let mut c = connect(db_opts.clone()).await?;
    let (views, probe_spec) = scenario_views(scn.name, a);

    // ---- setup: schema, seed, definitions, oracles
    let t = Instant::now();
    seed(&mut c, a).await?;
    let seed_s = t.elapsed().as_secs_f64();
    eprog(
        run_t0,
        format!(
            "seeded {} orders / {} customers / {} items / {} sessions in {seed_s:.1}s",
            a.orders, a.customers, a.items, a.sessions
        ),
    );
    lazyview::meta::ensure(&mut c, db).await?;
    for v in &views {
        exec(
            &mut c,
            &format!(
                "CREATE OR REPLACE VIEW {} AS {}",
                ident(&format!("_oracle_{}", v.name)),
                v.select
            ),
        )
        .await?;
        let def = format!("CREATE VIEW {} AS {}", v.name, v.select);
        exec(
            &mut c,
            &format!(
                "INSERT INTO _lazyview (definition) VALUES ('{}')",
                def.replace('\\', "\\\\").replace('\'', "\\'")
            ),
        )
        .await?;
    }

    // ---- engine start + initial build
    std::fs::create_dir_all(&a.log_dir).ok();
    let log_path = a
        .log_dir
        .join(format!("{db}-{}.log", now_iso().replace(':', "")));
    init_logging(&log_path, &a.log_filter)?;
    let mut env: HashMap<String, String> = HashMap::new();
    env.insert("LAZYVIEW_DSN".into(), db_dsn.to_string());
    env.insert("LAZYVIEW_HEALTH_ADDR".into(), "off".into());
    env.insert("LAZYVIEW_BATCH_WINDOW_MS".into(), a.window_ms.to_string());
    env.insert("LAZYVIEW_INSTANCE_ID".into(), "bench".into());
    // Full-mode views rebuild as soon as they are dirty (the integration tests do the same):
    // with the 60 s default cadence applied() moves past changes a full-mode view has not
    // absorbed yet, and full_mode would measure the cadence instead of the cost of rebuilds.
    env.insert("LAZYVIEW_FULL_REFRESH_MS".into(), "0".into());
    for (k, v) in &a.env {
        env.insert(k.clone(), v.clone());
    }
    let cfg = Config::from_lookup(|k| env.get(k).cloned()).context("engine config")?;
    let rss_baseline = rss_kb().0;
    let m_build0 = cpu_mark(info.mysqld_pid);
    let t = Instant::now();
    let mut eng = EngineHandle::spawn(cfg, a.engine_threads)?;
    let names: Vec<String> = views.iter().map(|v| v.name.clone()).collect();
    let modes = wait_ready(&mut c, &mut eng, &names, Duration::from_secs(900)).await?;
    let build_s = t.elapsed().as_secs_f64();
    let build_cpu = cpu_delta(&m_build0, &cpu_mark(info.mysqld_pid));
    let mut view_rows = Vec::new();
    let mut mv_rows = 0u64;
    for v in &views {
        let n = scalar_u64(&mut c, &format!("SELECT COUNT(*) FROM {}", ident(&v.name))).await?;
        mv_rows += n;
        view_rows.push(json!({"name": v.name, "mode": modes.get(&v.name), "rows": n}));
    }
    eprog(
        run_t0,
        format!(
            "{} view(s) ready in {build_s:.2}s ({mv_rows} MV rows, {:.0} rows/s); engine log {}",
            views.len(),
            mv_rows as f64 / build_s,
            log_path.display()
        ),
    );
    eng.catch_up(&mut c, Duration::from_secs(120), 1)
        .await
        .context("initial catch-up")?;
    let rss_after_build = rss_kb().0;

    // ---- idle probe
    let mut prober = if a.probe {
        Some(Prober::new(db_opts, probe_spec.clone()).await?)
    } else {
        None
    };
    let period = Duration::from_millis(a.probe_ms.max(1));
    let poll = Duration::from_millis(a.probe_poll_ms.max(1));
    let ptimeout = Duration::from_secs_f64(a.probe_timeout_s);
    let idle = match prober.as_mut() {
        Some(p) if a.idle_probe_s > 0.0 => {
            let until = Instant::now() + Duration::from_secs_f64(a.idle_probe_s);
            Some(
                p.run(period, poll, ptimeout, || Instant::now() >= until)
                    .await?,
            )
        }
        _ => None,
    };
    let commit_ms = commit_latency(&mut c, 10).await?;
    eng.catch_up(&mut c, Duration::from_secs(120), 1).await?;

    // ---- load
    let mut wp = WriterProc::spawn(db_dsn, &[])?;
    wp.wait_ready().await?;
    let host_before = json!({"uptime": uptime_line(), "loadavg": loadavg()});
    let counters0: HashMap<&'static str, u64> = eng.metrics.counters().into_iter().collect();
    let m0 = cpu_mark(info.mysqld_pid);
    let t0 = Instant::now();
    let samples: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::new()));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler_task = tokio::spawn(sampler(
        connect(db_opts.clone()).await?,
        eng.metrics.clone(),
        wp.state.clone(),
        t0,
        Duration::from_millis(a.sample_ms.max(10)),
        sampler_stop.clone(),
        samples.clone(),
    ));
    let load_over = Arc::new(AtomicBool::new(false));
    let probe_task = prober.take().map(|mut p| {
        let stop = load_over.clone();
        tokio::spawn(async move {
            let r = p
                .run(period, poll, ptimeout, || stop.load(Ordering::SeqCst))
                .await;
            (p, r)
        })
    });
    wp.send("GO");
    eprog(
        run_t0,
        format!(
            "load: {} writers x {} rows/txn, {}, {:.0}s, window {} ms",
            a.writers,
            a.batch,
            if a.rate > 0.0 {
                format!("capped at {} rows/s", a.rate)
            } else {
                "unthrottled".into()
            },
            a.seconds,
            a.window_ms
        ),
    );
    let mut last_prog = Instant::now();
    let load_deadline = t0 + Duration::from_secs_f64(a.seconds + 60.0);
    loop {
        if wp.state.done.lock().unwrap().is_some() || wp.state.exited.load(Ordering::SeqCst) {
            break;
        }
        if let Err(e) = eng.check_alive() {
            wp.send("STOP");
            return Err(e);
        }
        if Instant::now() > load_deadline {
            wp.send("STOP");
            bail!("writer process did not finish");
        }
        if last_prog.elapsed() >= Duration::from_secs(5) {
            last_prog = Instant::now();
            let s = samples.lock().unwrap().clone();
            if s.len() > 2 {
                let tl = Tl { s: &s };
                let i = s.len() - 1;
                let j = tl.idx(s[i].t - 5.0);
                let dt = (s[i].t - s[j].t).max(1e-9);
                let (lag, behind) = tl.lag(i);
                eprog(
                    run_t0,
                    format!(
                        "t={:5.1}s source {:8.0} rows/s  applied {:8.0} rows/s  lag {:6.2}s  behind {:8.0} rows",
                        s[i].t,
                        (s[i].rows - s[j].rows) as f64 / dt,
                        (tl.applied_rows(i) - tl.applied_rows(j)) / dt,
                        lag,
                        behind
                    ),
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let load_s = t0.elapsed().as_secs_f64();
    load_over.store(true, Ordering::SeqCst);
    let m_load = cpu_mark(info.mysqld_pid);
    let host_after_load = json!({"uptime": uptime_line(), "loadavg": loadavg()});
    let end_pos = lazyview::binlog::current_position(&mut c).await?;
    let done = wp
        .state
        .done
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| anyhow!("writer process exited without DONE (see its stderr)"))?;
    // DONE <ms> <rows> <txns> <retries> <errors> <cpu_ticks> <p50> <p95> <p99> <max>
    let writer_s = done[0] as f64 / 1000.0;
    let (w_rows, w_txns, w_retries, w_errors, w_cpu) =
        (done[1], done[2], done[3], done[4], done[5]);
    eprog(
        run_t0,
        format!("writers done: {w_rows} rows, {w_txns} txns in {writer_s:.1}s; draining"),
    );

    // ---- drain
    let td = Instant::now();
    let drain_res = eng
        .wait_applied(&end_pos, Duration::from_secs_f64(a.drain_timeout_s))
        .await;
    let drain_s = td.elapsed().as_secs_f64();
    let drained = drain_res.is_ok();
    if let Err(e) = &drain_res {
        eprog(run_t0, format!("drain failed: {e:#}"));
    }
    let (probe_load, prober) = match probe_task {
        Some(h) => {
            let (p, r) = h.await?;
            (Some(r?), Some(p))
        }
        None => (None, None),
    };
    drop(prober);
    let caught_up = drained
        && eng
            .catch_up(&mut c, Duration::from_secs_f64(a.drain_timeout_s), 2)
            .await
            .is_ok();
    let m1 = cpu_mark(info.mysqld_pid);
    sampler_stop.store(true, Ordering::SeqCst);
    sampler_task.await??;
    let counters1: HashMap<&'static str, u64> = eng.metrics.counters().into_iter().collect();
    let (_, rss_hwm) = rss_kb();
    drop(wp);

    // ---- correctness
    let full_refresh = Duration::from_millis(
        env.get("LAZYVIEW_FULL_REFRESH_MS")
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(60_000),
    );
    let full_slack = Duration::from_secs_f64((build_s * 5.0).max(60.0));
    let (live_ok, checks) = if caught_up {
        verify(&mut c, &mut eng, &views, &modes, full_refresh, full_slack).await?
    } else {
        (false, Vec::new())
    };
    eprog(
        run_t0,
        format!(
            "live phase correctness: {}",
            if live_ok { "OK" } else { "FAILED" }
        ),
    );

    // ---- optional backlog catch-up phase (engine stopped while writers build a backlog)
    let catchup = if a.catchup > 0.0 && caught_up {
        match catchup_phase(
            a,
            info,
            db_dsn,
            &env,
            &views,
            &modes,
            full_refresh,
            full_slack,
            &mut c,
            &mut eng,
            run_t0,
        )
        .await
        {
            Ok(j) => Some(j),
            Err(e) => {
                eprog(run_t0, format!("catch-up phase failed: {e:#}"));
                Some(json!({"ok": false, "error": format!("{e:#}")}))
            }
        }
    } else {
        None
    };
    let all_ok = live_ok && catchup.as_ref().map(|j| j["ok"] == true).unwrap_or(true);
    eng.stop().await.ok();
    c.disconnect().await.ok();

    // ---- analysis
    let s = samples.lock().unwrap().clone();
    let tl = Tl { s: &s };
    let i_end = tl.idx(load_s);
    let i_mid = tl.idx(load_s / 2.0);
    let applied_end = tl.applied_rows(i_end);
    let (lag_end, behind_end) = tl.lag(i_end);
    let lag_pts: Vec<(f64, f64)> = (i_mid..=i_end).map(|i| (s[i].t, tl.lag(i).0)).collect();
    let behind_pts: Vec<(f64, f64)> = (i_mid..=i_end).map(|i| (s[i].t, tl.lag(i).1)).collect();
    let lag_slope = slope(&lag_pts);
    let lag_max = (0..s.len()).map(|i| tl.lag(i).0).fold(0.0, f64::max);
    let dt2 = (s[i_end].t - s[i_mid].t).max(1e-9);
    let applied_2nd = (applied_end - tl.applied_rows(i_mid)) / dt2;
    let source_rows_s = w_rows as f64 / writer_s.max(1e-9);
    let applied_rows_s = applied_end / s[i_end].t.max(1e-9);
    // Lag is a sawtooth with the flush cycle (the window opens at the first change after a flush,
    // then the flush runs; applied() moves at flush end); a slope over fewer than ~5 cycles says
    // nothing about unbounded growth.
    let flushes = flushes_between(m0.at, m1.at);
    let flush_p50_s = if flushes.is_empty() {
        0.0
    } else {
        pct(&flushes, 0.5) / 1000.0
    };
    let cycle_s = a.window_ms as f64 / 1000.0 + flush_p50_s;
    let conclusive = load_s / 2.0 >= 5.0 * cycle_s;
    // Falling behind = the backlog grows: lag rising over the 2nd half, above two flush cycles at
    // the end (one cycle of lag is normal), and the final backlog takes longer than a cycle to
    // drain (an engine that keeps up clears it with the flush in progress).
    let saturated = lag_slope > 0.05 && lag_end > (2.0 * cycle_s).max(1.0) && drain_s > cycle_s;
    let verdict = if !drained {
        "not_drained"
    } else if saturated && !conclusive {
        "inconclusive"
    } else if saturated {
        "falling_behind"
    } else {
        "keeping_up"
    };
    // While saturated the engine is busy from (about) load start until drained.
    let busy_rows_s = (w_rows + probe_load.as_ref().map(|p| p.changes).unwrap_or(0)) as f64
        / (load_s + drain_s).max(1e-9);
    let (max_sust, bound) = match verdict {
        "falling_behind" | "not_drained" => (busy_rows_s, "engine"),
        "inconclusive" => (source_rows_s, "inconclusive"),
        _ => (source_rows_s, "source"),
    };
    let binlog_bytes = s
        .last()
        .map(|x| x.server)
        .unwrap_or(0)
        .saturating_sub(s.first().map(|x| x.server).unwrap_or(0));
    let mut timeline = Vec::new();
    let mut prev = 0usize;
    let t_last = s.last().map(|x| x.t).unwrap_or(0.0);
    let mut sec = 1.0;
    while sec <= t_last + 1e-9 {
        let i = tl.idx(sec);
        if i == prev {
            // No new sample in this second (the sampler's server query stalled): no row.
            sec += 1.0;
            continue;
        }
        let dt = (s[i].t - s[prev].t).max(1e-9);
        let (lag, behind) = tl.lag(i);
        timeline.push(json!([
            rnd(s[i].t, 1),
            rnd((s[i].rows - s[prev].rows) as f64 / dt, 0),
            rnd((tl.applied_rows(i) - tl.applied_rows(prev)) / dt, 0),
            rnd(lag, 3),
            rnd(behind, 0)
        ]));
        prev = i;
        sec += 1.0;
    }

    let probe_changes = probe_load.as_ref().map(|p| p.changes).unwrap_or(0);
    let changes = (w_rows + probe_changes).max(1) as f64;
    let d = |k: &str| {
        counters1
            .get(k)
            .copied()
            .unwrap_or(0)
            .saturating_sub(counters0.get(k).copied().unwrap_or(0))
    };
    let keys: Vec<&'static str> = counters1.keys().copied().collect();

    let cpu_ld = cpu_delta(&m0, &m1);
    let cpu_l = cpu_delta(&m0, &m_load);
    let tck = clk_tck();
    let rss_peak = s.iter().map(|x| x.rss_kb).max().unwrap_or(0);
    let mb = |kb: u64| rnd(kb as f64 / 1024.0, 1);
    let lat_us = |i: usize| rnd(done.get(i).copied().unwrap_or(0) as f64 / 1000.0, 2);

    let capacity = catchup.as_ref().and_then(|j| {
        j["rows_s_consistent"]
            .as_f64()
            .or_else(|| j["rows_s"].as_f64())
    });
    let mut engine_env: Vec<String> = env
        .iter()
        .filter(|(k, _)| k.as_str() != "LAZYVIEW_DSN")
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    engine_env.sort();
    let report = json!({
        "ts": now_iso(),
        "label": a.label,
        "build": a.build_info,
        "scenario": scn.name,
        "about": scn.about,
        "valid": all_ok,
        "server": {
            "addr": hostport, "flavor": info.flavor, "version": info.version, "settings": info.settings,
            "commit_ms_p50": rnd(pct(&commit_ms, 0.5), 2), "commit_ms_max": rnd(pct(&commit_ms, 1.0), 2),
        },
        "engine_log": log_path.display().to_string(),
        "params": {
            "seconds": a.seconds, "writers": a.writers, "batch": a.batch, "rate": a.rate, "window_ms": a.window_ms,
            "catchup_s": a.catchup,
            "sizes": {"orders": a.orders, "customers": a.customers, "items": a.items, "sessions": a.sessions, "products": a.products},
            "hot": a.hot, "views": views.len(), "probe_ms": a.probe_ms, "probe_poll_ms": a.probe_poll_ms.max(probe_spec.min_poll_ms),
            "engine_threads": a.engine_threads, "env": a.env.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>(), "seed": a.seed,
            "engine_env": engine_env,
            "mix": scn.mix.iter().map(|(w, op)| format!("{w}% {op:?}")).collect::<Vec<_>>(),
        },
        "host": {
            "cores": ncpu(), "cpus_online": cpus_online(), "before": host_before, "after_load": host_after_load,
            "busy_pct_load": cpu_l.sys_busy_pct.map(|x| rnd(x, 1)),
        },
        "setup": {
            "seed_s": rnd(seed_s, 2), "build_s": rnd(build_s, 3), "mv_rows": mv_rows,
            "build_rows_s": rnd(mv_rows as f64 / build_s.max(1e-9), 0),
            "build_engine_cpu_s": rnd(build_cpu.engine_s, 2), "views": view_rows,
        },
        "source": {
            "rows": w_rows, "txns": w_txns, "seconds": rnd(writer_s, 2),
            "rows_s": rnd(source_rows_s, 0), "txns_s": rnd(w_txns as f64 / writer_s.max(1e-9), 1),
            "retries": w_retries, "errors": w_errors,
            "txn_latency_ms": {"p50": lat_us(6), "p95": lat_us(7), "p99": lat_us(8), "max": lat_us(9)},
            "writer_cpu_pct": rnd(w_cpu as f64 / tck / writer_s.max(1e-9) * 100.0, 1),
            "binlog_mb_s": rnd(binlog_bytes as f64 / 1048576.0 / t_last.max(1e-9), 2),
        },
        "engine": {
            "applied_rows_s": rnd(applied_rows_s, 0), "applied_rows_s_2nd_half": rnd(applied_2nd, 0),
            "busy_rows_s": rnd(busy_rows_s, 0),
            "lag_end_s": rnd(lag_end, 3), "lag_max_s": rnd(lag_max, 3), "rows_behind_end": rnd(behind_end, 0),
            "lag_slope_s_per_s": rnd(lag_slope, 4), "rows_behind_slope_per_s": rnd(slope(&behind_pts), 1),
            "verdict": verdict, "max_sustainable_rows_s": rnd(max_sust, 0), "bound": bound,
            "flush_cycle_s": rnd(cycle_s, 3), "verdict_conclusive": conclusive,
            "capacity_rows_s": capacity.map(|x| rnd(x, 0)),
            "drain_s": rnd(drain_s, 3), "drain_rows_s": if drain_s > 0.5 { rnd(behind_end / drain_s, 0) } else { J::Null },
            "caught_up": caught_up,
        },
        "freshness": {
            "idle": idle.as_ref().map(probe_json), "load": probe_load.as_ref().map(probe_json),
            "probe": format!("{} -> {} WHERE {}", probe_spec.update, probe_spec.view, probe_spec.check),
        },
        "resources": {
            "engine_cpu_pct": rnd(cpu_ld.engine_s / cpu_ld.wall * 100.0, 1),
            "engine_cpu_pct_load": rnd(cpu_l.engine_s / cpu_l.wall * 100.0, 1),
            "engine_cpu_s": rnd(cpu_ld.engine_s, 2),
            "cpu_us_per_change": rnd(cpu_ld.engine_s * 1e6 / changes, 2),
            "harness_cpu_pct": rnd(cpu_ld.harness_s / cpu_ld.wall * 100.0, 1),
            "mysqld_cpu_pct": cpu_ld.mysqld_s.map(|x| rnd(x / cpu_ld.wall * 100.0, 1)),
            "rss_mb_baseline": mb(rss_baseline), "rss_mb_after_build": mb(rss_after_build),
            "rss_mb_peak": mb(rss_peak), "rss_mb_hwm": mb(rss_hwm),
            "engine_rss_mb": mb(rss_peak.max(rss_after_build).saturating_sub(rss_baseline)),
            "window_s": rnd(cpu_ld.wall, 2),
        },
        "efficiency": efficiency_json(&d, &keys, changes, probe_changes, &flushes, cpu_ld.wall),
        "correctness": {"ok": live_ok, "views": checks},
        "catchup": catchup,
        "timeline_cols": ["t_s", "source_rows_s", "applied_rows_s", "lag_s", "rows_behind"],
        "timeline": timeline,
    });
    Ok(report)
}

/// Backlog catch-up: stops the engine (it saves its position), lets writers build a backlog
/// for `--catchup` seconds, restarts the engine (`resume`) and measures how fast it applies the
/// backlog while saturated by construction - the engine's capacity independent of how fast the
/// source can commit. The window starts when the binlog stream is connected (startup excluded,
/// reported separately) and ends when `applied >= ` the server position after the backlog.
#[allow(clippy::too_many_arguments)]
async fn catchup_phase(
    a: &Args,
    info: &ServerInfo,
    db_dsn: &str,
    env: &HashMap<String, String>,
    views: &[ViewDef],
    modes: &HashMap<String, String>,
    full_refresh: Duration,
    full_slack: Duration,
    c: &mut Conn,
    eng: &mut EngineHandle,
    run_t0: Instant,
) -> Result<J> {
    let writers = a.catchup_writers.unwrap_or(a.writers);
    let batch = a.catchup_batch.unwrap_or(a.batch);
    eng.stop().await.context("stop engine before the backlog")?;
    let resume = rows(
        c,
        "SELECT binlog_file, binlog_pos FROM _lazyview_status WHERE instance_id = 'bench'",
    )
    .await?;
    let resume = resume.into_iter().next().and_then(|r| {
        Some(Position {
            file: r.first()?.clone()?,
            pos: r.get(1)?.clone()?.parse().ok()?,
        })
    });
    let start_pos = lazyview::binlog::current_position(c).await?;
    let mut map = BinlogMap::default();
    map.refresh(c).await?;
    let start_abs = map.abs(c, &start_pos).await?.unwrap_or(0);
    let resume_abs = match &resume {
        Some(p) => map.abs(c, p).await?,
        None => None,
    };

    // ---- backlog
    let overrides: Vec<String> = [
        "--seconds".to_string(),
        a.catchup.to_string(),
        "--rate".into(),
        "0".into(),
        "--writers".into(),
        writers.to_string(),
        "--batch".into(),
        batch.to_string(),
        "--seed".into(),
        (a.seed + 1).to_string(),
    ]
    .into();
    let mut wp = WriterProc::spawn(db_dsn, &overrides)?;
    wp.wait_ready().await?;
    eprog(run_t0, format!("catch-up: engine stopped (saved position {}), writing a backlog: {writers} writers x {batch} rows/txn for {:.0}s", resume.as_ref().map(|p| p.to_string()).unwrap_or("?".into()), a.catchup));
    wp.send("GO");
    let deadline = Instant::now() + Duration::from_secs_f64(a.catchup + 120.0);
    while wp.state.done.lock().unwrap().is_none() {
        if wp.state.exited.load(Ordering::SeqCst) || Instant::now() > deadline {
            wp.send("STOP");
            bail!("backlog writer process did not finish");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let done = wp.state.done.lock().unwrap().clone().unwrap_or_default();
    drop(wp);
    let (w_ms, w_rows, w_txns) = (done[0], done[1], done[2]);
    let end_pos = lazyview::binlog::current_position(c).await?;
    let end_abs = map.abs(c, &end_pos).await?.unwrap_or(start_abs);
    let backlog_bytes = end_abs.saturating_sub(start_abs).max(1);
    eprog(run_t0, format!("catch-up: backlog of {w_rows} row changes ({w_txns} txns, {:.1} MB binlog) in {:.1}s; restarting the engine", backlog_bytes as f64 / 1048576.0, w_ms as f64 / 1000.0));

    // ---- restart + drain
    let host_before = json!({"uptime": uptime_line(), "loadavg": loadavg()});
    let cfg = Config::from_lookup(|k| env.get(k).cloned()).context("engine config")?;
    let m0 = cpu_mark(info.mysqld_pid);
    let t_spawn = Instant::now();
    let mut e2 = EngineHandle::spawn(cfg, a.engine_threads)?;
    let mut t_stream: Option<Instant> = None;
    let mut tl: Vec<(f64, u64)> = Vec::new();
    let mut rss_peak = 0;
    let mut last_sample: Option<Instant> = None;
    let deadline = Instant::now() + Duration::from_secs_f64(a.drain_timeout_s);
    loop {
        e2.check_alive()?;
        if t_stream.is_none() && Metrics::get(&e2.metrics.gauges.streaming) == 1 {
            t_stream = Some(Instant::now());
        }
        let ap = e2.metrics.applied();
        if ap.as_ref().map(|p| *p >= end_pos).unwrap_or(false) {
            break;
        }
        if last_sample
            .map(|t| t.elapsed() >= Duration::from_millis(100))
            .unwrap_or(true)
        {
            last_sample = Some(Instant::now());
            let abs = match &ap {
                Some(p) => map.abs(c, p).await?.unwrap_or(start_abs),
                None => start_abs,
            };
            tl.push((
                t_spawn.elapsed().as_secs_f64(),
                abs.clamp(start_abs, end_abs),
            ));
            rss_peak = rss_peak.max(rss_kb().0);
        }
        if Instant::now() > deadline {
            bail!(
                "catch-up not finished within {:.0}s: applied {:?}, target {end_pos}",
                a.drain_timeout_s,
                ap.map(|p| p.to_string())
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let t_done = Instant::now();
    let m1 = cpu_mark(info.mysqld_pid);
    tl.push((t_spawn.elapsed().as_secs_f64(), end_abs));
    rss_peak = rss_peak.max(rss_kb().0);
    let host_after = json!({"uptime": uptime_line(), "loadavg": loadavg()});
    let t_stream = t_stream.unwrap_or(t_spawn);
    let startup_s = t_stream.duration_since(t_spawn).as_secs_f64();
    let drain_s = t_done.duration_since(t_stream).as_secs_f64().max(1e-6);
    let counters: HashMap<&'static str, u64> = e2.metrics.counters().into_iter().collect();
    *eng = e2;
    eng.catch_up(c, Duration::from_secs(120), 2)
        .await
        .context("catch-up final position")?;
    let (ok, checks) = verify(c, eng, views, modes, full_refresh, full_slack).await?;
    let rows_s = w_rows as f64 / drain_s;
    // Full-mode views converge after applied() (background rebuild): time until all were equal.
    let full_wait = checks
        .iter()
        .filter_map(|x| x["full_refresh_wait_s"].as_f64())
        .fold(0.0, f64::max);
    let consistent_s = drain_s + full_wait;
    eprog(
        run_t0,
        format!(
            "catch-up: {w_rows} row changes applied in {drain_s:.2}s = {rows_s:.0} rows/s (+{startup_s:.2}s startup){}; correctness {}",
            if full_wait > 0.0 { format!(", full-mode views consistent after {consistent_s:.2}s = {:.0} rows/s", w_rows as f64 / consistent_s) } else { String::new() },
            if ok { "OK" } else { "FAILED" }
        ),
    );

    // Per-second rows/s, assuming rows are spread evenly over the backlog's binlog bytes.
    let per_byte = w_rows as f64 / backlog_bytes as f64;
    let mut timeline = Vec::new();
    let (mut prev_t, mut prev_abs) = (startup_s, start_abs);
    let mut sec = startup_s + 1.0;
    let t_end = tl.last().map(|x| x.0).unwrap_or(0.0);
    while sec < t_end + 1.0 {
        let i = tl.partition_point(|x| x.0 <= sec).saturating_sub(1);
        let (t, abs) = tl[i];
        if t > prev_t {
            timeline.push(json!([
                rnd(t - startup_s, 1),
                rnd((abs - prev_abs) as f64 * per_byte / (t - prev_t), 0)
            ]));
            (prev_t, prev_abs) = (t, abs);
        }
        sec += 1.0;
    }
    let d = |k: &str| counters.get(k).copied().unwrap_or(0);
    let keys: Vec<&'static str> = counters.keys().copied().collect();
    let cpu = cpu_delta(&m0, &m1);
    let flushes = flushes_between(m0.at, m1.at);
    let changes = w_rows.max(1) as f64;
    Ok(json!({
        "ok": ok,
        "seconds": a.catchup, "writers": writers, "batch": batch,
        "backlog_rows": w_rows, "backlog_txns": w_txns, "backlog_write_s": rnd(w_ms as f64 / 1000.0, 2),
        "backlog_source_rows_s": rnd(w_rows as f64 * 1000.0 / (w_ms.max(1) as f64), 0),
        "backlog_binlog_mb": rnd(backlog_bytes as f64 / 1048576.0, 2),
        "resume_from": resume.as_ref().map(|p| p.to_string()), "backlog_start": start_pos.to_string(), "backlog_end": end_pos.to_string(),
        "replayed_extra_bytes": resume_abs.map(|r| start_abs.saturating_sub(r)),
        "startup_s": rnd(startup_s, 3), "drain_s": rnd(drain_s, 3),
        "rows_s": rnd(rows_s, 0), "rows_s_incl_startup": rnd(w_rows as f64 / (drain_s + startup_s), 0),
        "consistent_s": rnd(consistent_s, 3), "rows_s_consistent": rnd(w_rows as f64 / consistent_s, 0),
        "binlog_mb_s": rnd(backlog_bytes as f64 / 1048576.0 / drain_s, 2),
        "resources": {
            "engine_cpu_pct": rnd(cpu.engine_s / cpu.wall * 100.0, 1), "engine_cpu_s": rnd(cpu.engine_s, 2),
            "cpu_us_per_change": rnd(cpu.engine_s * 1e6 / changes, 2),
            "mysqld_cpu_pct": cpu.mysqld_s.map(|x| rnd(x / cpu.wall * 100.0, 1)),
            "host_busy_pct": cpu.sys_busy_pct.map(|x| rnd(x, 1)), "rss_mb_peak": rnd(rss_peak as f64 / 1024.0, 1),
        },
        "host": {"before": host_before, "after": host_after},
        "efficiency": efficiency_json(&d, &keys, changes, 0, &flushes, cpu.wall),
        "timeline_cols": ["t_s since stream start", "applied_rows_s"],
        "timeline": timeline,
        "correctness": {"ok": ok, "views": checks},
    }))
}

/// Engine log to a file, plus [`FlushLayer`] (flush durations from the engine's
/// `trace!(elapsed, "flush")` event; nothing else is enabled at trace level).
fn init_logging(path: &std::path::Path, filter: &str) -> Result<()> {
    use tracing_subscriber::prelude::*;
    let file = std::fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
    let env = tracing_subscriber::EnvFilter::new(
        std::env::var("RUST_LOG").unwrap_or_else(|_| filter.to_string()),
    );
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(Arc::new(file))
        .with_ansi(false)
        .with_filter(env);
    let flush = FlushLayer.with_filter(
        tracing_subscriber::filter::Targets::new()
            .with_target("lazyview::engine", tracing::Level::TRACE),
    );
    tracing_subscriber::registry()
        .with(fmt)
        .with(flush)
        .try_init()
        .map_err(|e| anyhow!("logging: {e}"))
}

/// (when, duration ms) of every engine flush.
static FLUSHES: Mutex<Vec<(Instant, f64)>> = Mutex::new(Vec::new());

struct FlushLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for FlushLayer {
    fn on_event(&self, ev: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        #[derive(Default)]
        struct V {
            flush: bool,
            ms: Option<f64>,
        }
        impl tracing::field::Visit for V {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                match f.name() {
                    "message" => self.flush = format!("{v:?}") == "flush",
                    "elapsed" => self.ms = duration_ms(&format!("{v:?}")),
                    _ => {}
                }
            }
        }
        let mut v = V::default();
        ev.record(&mut v);
        if let (true, Some(ms)) = (v.flush, v.ms) {
            FLUSHES.lock().unwrap().push((Instant::now(), ms));
        }
    }
}

/// Parses `Duration`'s Debug form (`1.5s`, `12.3ms`, `950µs`, `80ns`) into milliseconds.
fn duration_ms(s: &str) -> Option<f64> {
    let split = s.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
    let (num, unit) = s.split_at(split);
    let x: f64 = num.parse().ok()?;
    Some(match unit {
        "s" => x * 1000.0,
        "ms" => x,
        "µs" | "us" => x / 1000.0,
        "ns" => x / 1e6,
        _ => return None,
    })
}

// =============================================================================================
// Output
// =============================================================================================

fn f(v: &J, d: usize) -> String {
    match v.as_f64() {
        Some(x) => format!("{x:.d$}"),
        None => "-".into(),
    }
}

fn thousands(v: &J) -> String {
    let Some(x) = v.as_f64() else {
        return "-".into();
    };
    let s = format!("{:.0}", x.abs());
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    if x < 0.0 {
        format!("-{out}")
    } else {
        out
    }
}

fn print_summary(r: &J) {
    let p = &r["params"];
    let mut o = String::new();
    let _ = writeln!(
        o,
        "\n=== lazyview throughput: {} on {} {} | window {} ms | {} writers x {} rows/txn | {} | {} s {}===",
        r["scenario"].as_str().unwrap_or(""),
        r["server"]["flavor"].as_str().unwrap_or(""),
        r["server"]["version"].as_str().unwrap_or(""),
        p["window_ms"],
        p["writers"],
        p["batch"],
        if p["rate"].as_f64().unwrap_or(0.0) > 0.0 { format!("rate cap {} rows/s", p["rate"]) } else { "unthrottled".into() },
        p["seconds"],
        if r["label"].as_str().unwrap_or("").is_empty() { String::new() } else { format!("[{}] ", r["label"].as_str().unwrap()) }
    );
    let _ = writeln!(o, "  {}", r["about"].as_str().unwrap_or(""));
    let st = &r["setup"];
    let views: Vec<String> = st["views"]
        .as_array()
        .map(|v| {
            v.iter()
                .map(|x| {
                    format!(
                        "{}({}, {} rows)",
                        x["name"].as_str().unwrap_or(""),
                        x["mode"].as_str().unwrap_or("?"),
                        x["rows"]
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let shown = if views.len() > 4 {
        format!("{} ... ({} views)", views[..3].join(", "), views.len())
    } else {
        views.join(", ")
    };
    let _ = writeln!(
        o,
        "  setup        seed {} s | build {} s for {} MV rows = {} rows/s (engine cpu {} s) | {}",
        f(&st["seed_s"], 1),
        f(&st["build_s"], 2),
        thousands(&st["mv_rows"]),
        thousands(&st["build_rows_s"]),
        f(&st["build_engine_cpu_s"], 1),
        shown
    );
    let s = &r["source"];
    let _ = writeln!(
        o,
        "  source       {} row changes, {} txns in {} s = {} rows/s, {} txn/s | txn p50 {} ms p99 {} ms | retries {} errors {} | binlog {} MB/s",
        thousands(&s["rows"]), thousands(&s["txns"]), f(&s["seconds"], 1), thousands(&s["rows_s"]), f(&s["txns_s"], 0),
        f(&s["txn_latency_ms"]["p50"], 1), f(&s["txn_latency_ms"]["p99"], 1), s["retries"], s["errors"], f(&s["binlog_mb_s"], 1)
    );
    let e = &r["engine"];
    let _ = writeln!(
        o,
        "  engine       applied {} rows/s (2nd half {}) | lag end {} s, max {} s, slope {} s/s, behind {} rows | {} | drain {} s",
        thousands(&e["applied_rows_s"]), thousands(&e["applied_rows_s_2nd_half"]), f(&e["lag_end_s"], 2), f(&e["lag_max_s"], 2),
        f(&e["lag_slope_s_per_s"], 3), thousands(&e["rows_behind_end"]), e["verdict"].as_str().unwrap_or("").to_uppercase(), f(&e["drain_s"], 2)
    );
    let _ = writeln!(
        o,
        "  => live: max sustainable {} {} rows/s ({}){}",
        if e["bound"] == "engine" { "~" } else { ">=" },
        thousands(&e["max_sustainable_rows_s"]),
        match e["bound"].as_str().unwrap_or("") {
            "engine" => "engine-bound: lag grew".to_string(),
            "source" => "source-bound: engine kept up".to_string(),
            "inconclusive" => format!(
                "lag grew, but the run is too short for the flush cycle of {} s: inconclusive",
                f(&e["flush_cycle_s"], 1)
            ),
            b => b.to_string(),
        },
        if e["capacity_rows_s"].is_null() {
            String::new()
        } else {
            format!(
                " | engine capacity (backlog catch-up) {} rows/s",
                thousands(&e["capacity_rows_s"])
            )
        }
    );
    let fr = &r["freshness"];
    let pl = |x: &J| {
        if x.is_null() {
            "-".to_string()
        } else {
            format!(
                "n={} p50 {} p95 {} p99 {} max {} ms (timeouts {})",
                x["n"],
                f(&x["p50_ms"], 0),
                f(&x["p95_ms"], 0),
                f(&x["p99_ms"], 0),
                f(&x["max_ms"], 0),
                x["timeouts"]
            )
        }
    };
    let _ = writeln!(
        o,
        "  freshness    load: {} | idle: {}",
        pl(&fr["load"]),
        pl(&fr["idle"])
    );
    let rs = &r["resources"];
    let h = &r["host"];
    let _ = writeln!(
        o,
        "  cpu/mem      engine {}% of a core ({}% during load), {} us/change | rss {} MB (engine +{} MB) | writers {}% | mysqld {}% | harness {}%",
        f(&rs["engine_cpu_pct"], 0), f(&rs["engine_cpu_pct_load"], 0), f(&rs["cpu_us_per_change"], 1), f(&rs["rss_mb_peak"], 0),
        f(&rs["engine_rss_mb"], 0), f(&s["writer_cpu_pct"], 0), f(&rs["mysqld_cpu_pct"], 0), f(&rs["harness_cpu_pct"], 1)
    );
    let _ = writeln!(
        o,
        "  host         {} cpus ({} usable), busy {}% during load, load avg before {} after {} | server commit (1-row autocommit) p50 {} ms max {} ms",
        h["cpus_online"], h["cores"], f(&h["busy_pct_load"], 0), h["before"]["loadavg"], h["after_load"]["loadavg"],
        f(&r["server"]["commit_ms_p50"], 1), f(&r["server"]["commit_ms_max"], 1)
    );
    let ef = &r["efficiency"];
    let _ = writeln!(
        o,
        "  efficiency   rows seen {} (skipped {}) | units refreshed {} ({} changed frac) | dedup {} rows/unit | per 1k changes: source q {}, lookup q {}, mv q {} | MV rows written/change {} | flushes {} ({} units/flush) | full rebuilds {}",
        thousands(&ef["rows_seen"]), thousands(&ef["rows_skipped"]), thousands(&ef["units_refreshed"]), f(&ef["units_changed_frac"], 2),
        f(&ef["dedup_ratio"], 2), f(&ef["source_queries_per_1k"], 1), f(&ef["lookup_queries_per_1k"], 1), f(&ef["mv_queries_per_1k"], 1),
        f(&ef["mv_rows_written_per_change"], 2), ef["flushes"], f(&ef["units_per_flush"], 0), ef["full_rebuilds"]
    );
    let fm = &ef["flush_ms"];
    if !fm.is_null() {
        let _ = writeln!(o, "  flushes      n={} duration p50 {} ms p95 {} ms max {} ms | engine flushing {}% of the time", fm["n"], f(&fm["p50"], 0), f(&fm["p95"], 0), f(&fm["max"], 0), f(&fm["busy_pct"], 0));
    }
    let cu = &r["catchup"];
    if !cu.is_null() {
        if !cu["error"].is_null() {
            let _ = writeln!(
                o,
                "  catch-up     FAILED: {}",
                cu["error"].as_str().unwrap_or("")
            );
        } else {
            let ce = &cu["efficiency"];
            let _ = writeln!(
                o,
                "  catch-up     backlog {} row changes ({} txns, {} MB binlog, written at {} rows/s by {}x{}) applied in {} s = {} rows/s ({} MB/s binlog){}, startup {} s | engine {}% of a core, {} us/change, rss {} MB | mysqld {}% | host busy {}%",
                thousands(&cu["backlog_rows"]), thousands(&cu["backlog_txns"]), f(&cu["backlog_binlog_mb"], 1), thousands(&cu["backlog_source_rows_s"]), cu["writers"], cu["batch"],
                f(&cu["drain_s"], 2), thousands(&cu["rows_s"]), f(&cu["binlog_mb_s"], 2),
                if cu["consistent_s"].as_f64().unwrap_or(0.0) > cu["drain_s"].as_f64().unwrap_or(0.0) + 0.001 {
                    format!("; full-mode views consistent after {} s = {} rows/s", f(&cu["consistent_s"], 2), thousands(&cu["rows_s_consistent"]))
                } else {
                    String::new()
                },
                f(&cu["startup_s"], 2),
                f(&cu["resources"]["engine_cpu_pct"], 0), f(&cu["resources"]["cpu_us_per_change"], 1), f(&cu["resources"]["rss_mb_peak"], 0),
                f(&cu["resources"]["mysqld_cpu_pct"], 0), f(&cu["resources"]["host_busy_pct"], 0)
            );
            let fm = &ce["flush_ms"];
            let _ = writeln!(
                o,
                "               dedup {} rows/unit | per 1k changes: source q {}, lookup q {}, mv q {} | MV rows written/change {} | flushes {} ({} units/flush, p50 {} ms, max {} ms, busy {}%) | full rebuilds {} | correctness {}",
                f(&ce["dedup_ratio"], 2), f(&ce["source_queries_per_1k"], 1), f(&ce["lookup_queries_per_1k"], 1), f(&ce["mv_queries_per_1k"], 1),
                f(&ce["mv_rows_written_per_change"], 2), ce["flushes"], f(&ce["units_per_flush"], 0), f(&fm["p50"], 0), f(&fm["max"], 0), f(&fm["busy_pct"], 0),
                ce["full_rebuilds"], if cu["ok"] == true { "OK" } else { "FAILED" }
            );
        }
    }
    let cr = &r["correctness"];
    let bad: Vec<String> = cr["views"]
        .as_array()
        .map(|v| {
            v.iter()
                .filter(|x| x["ok"] != true)
                .map(|x| {
                    format!(
                        "{}: {}",
                        x["name"].as_str().unwrap_or(""),
                        x["detail"].as_str().unwrap_or("")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let nviews = cr["views"].as_array().map(|v| v.len()).unwrap_or(0);
    if cr["ok"] == true {
        let _ = writeln!(
            o,
            "  correctness  OK: {nviews} view(s) equal their oracle after the live phase"
        );
    } else {
        let _ = writeln!(
            o,
            "  correctness  FAILED (run INVALID): {}",
            if bad.is_empty() {
                "engine did not catch up".to_string()
            } else {
                bad.join(" | ")
            }
        );
    }
    if let Some(tlv) = r["timeline"].as_array() {
        let _ = writeln!(
            o,
            "  timeline     t(s)   source rows/s  applied rows/s   lag s   rows behind"
        );
        let step = (tlv.len() / 30).max(1);
        for (i, row) in tlv.iter().enumerate() {
            if i % step == 0 || i + 1 == tlv.len() {
                let _ = writeln!(
                    o,
                    "             {:>6} {:>15} {:>15} {:>7} {:>13}",
                    f(&row[0], 0),
                    thousands(&row[1]),
                    thousands(&row[2]),
                    f(&row[3], 2),
                    thousands(&row[4])
                );
            }
        }
    }
    println!("{o}");
}

fn report(path: &PathBuf) -> Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    println!(
        "{:<20} {:<24} {:<14} {:>5} {:>7} {:>6} {:>8} {:>8} {:>14} {:>9} {:>9} {:>6} {:>6} {:>6} {:>5} {:>6} {:>6} {:>6} {:>6} {:>6} {:>5} {:<4}",
        "ts", "label (server)", "scenario", "win", "W x B", "rate", "src r/s", "eng r/s", "verdict", "maxsust", "capacity", "lag s", "p50ms", "p99ms", "cpu%", "us/chg", "dedup", "srcq/k", "mvq/k", "wamp", "load", "ok"
    );
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let r: J = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let p = &r["params"];
        let e = &r["engine"];
        let fl = &r["freshness"]["load"];
        println!(
            "{:<20} {:<24} {:<14} {:>5} {:>7} {:>6} {:>8} {:>8} {:>14} {:>9} {:>9} {:>6} {:>6} {:>6} {:>5} {:>6} {:>6} {:>6} {:>6} {:>6} {:>5} {:<4}",
            r["ts"].as_str().unwrap_or(""),
            clip(
                &match r["label"].as_str().unwrap_or("") {
                    "" => format!("{} {}", r["server"]["flavor"].as_str().unwrap_or(""), r["server"]["version"].as_str().unwrap_or("")),
                    l => l.to_string(),
                },
                24
            ),
            r["scenario"].as_str().unwrap_or(""),
            p["window_ms"],
            format!("{}x{}", p["writers"], p["batch"]),
            if p["rate"].as_f64().unwrap_or(0.0) > 0.0 { thousands(&p["rate"]) } else { "-".into() },
            thousands(&r["source"]["rows_s"]),
            thousands(&e["applied_rows_s"]),
            e["verdict"].as_str().unwrap_or(""),
            format!("{}{}", if e["bound"] == "engine" { "~" } else { ">=" }, thousands(&e["max_sustainable_rows_s"])),
            thousands(&e["capacity_rows_s"]),
            f(&e["lag_end_s"], 2),
            f(&fl["p50_ms"], 0),
            f(&fl["p99_ms"], 0),
            f(&r["resources"]["engine_cpu_pct"], 0),
            f(&r["resources"]["cpu_us_per_change"], 1),
            f(&r["efficiency"]["dedup_ratio"], 1),
            f(&r["efficiency"]["source_queries_per_1k"], 1),
            f(&r["efficiency"]["mv_queries_per_1k"], 1),
            f(&r["efficiency"]["mv_rows_written_per_change"], 2),
            f(&r["host"]["before"]["loadavg"][0], 1),
            if r["valid"] == true { "yes" } else { "NO" }
        );
    }
    Ok(())
}

/// Drops `<prefix>_<scenario>` of every scenario (only names starting with "bench").
async fn cleanup(a: &Args) -> Result<()> {
    let base = Opts::from_url(&a.dsn).context("bad --dsn")?;
    let prefix = base
        .db_name()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "bench".into());
    let mut c = connect(OptsBuilder::from_opts(base).db_name(None::<String>).into()).await?;
    for s in SCENARIOS {
        let db = format!("{prefix}_{}", s.name);
        if db.starts_with("bench") && !db.contains('`') {
            let existed = scalar(
                &mut c,
                &format!(
                    "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{db}'"
                ),
            )
            .await?;
            if existed.as_deref() != Some("0") {
                exec(&mut c, &format!("DROP DATABASE IF EXISTS {}", ident(&db))).await?;
                eprintln!("dropped leftover database {db}");
            }
        }
    }
    c.disconnect().await.ok();
    Ok(())
}

fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let a = match parse_args(&raw) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(2);
        }
    };
    let code = match a.role.clone() {
        Role::Help => {
            print!("{HELP}");
            0
        }
        Role::List => {
            for s in SCENARIOS {
                println!("{:<14} {}", s.name, s.about);
            }
            0
        }
        Role::Report(p) => match report(&p) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("error: {e:#}");
                1
            }
        },
        Role::Cleanup => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            match rt.block_on(cleanup(&a)) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("error: {e:#}");
                    1
                }
            }
        }
        Role::Writer => match writer_main(a) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("writer error: {e:#}");
                1
            }
        },
        Role::Bench => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("lvb-h")
                .enable_all()
                .build()
                .expect("harness runtime");
            let r = rt.block_on(bench(a));
            rt.shutdown_timeout(Duration::from_secs(2));
            match r {
                Ok(true) => 0,
                Ok(false) => {
                    eprintln!("run INVALID (correctness check failed or engine did not catch up)");
                    3
                }
                Err(e) => {
                    eprintln!("error: {e:#}");
                    1
                }
            }
        }
    };
    std::process::exit(code);
}
