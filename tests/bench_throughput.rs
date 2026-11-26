//! Steady-state throughput per view shape and flavor — PR3 of the throughput and recovery plan
//! (`tmp/out/lvs-perf/plan/breakdown.md` §1, PR3).
//!
//! The real `lazyview` binary runs as its own process (`tests/common/engineproc.rs`, as a
//! deployment runs it), load comes from the seeded writers of `tests/common/benchwork.rs`, and
//! every rate is a delta of the engine's own `/metrics` counters over one measured window. CPU and
//! RSS come from `/proc` in the harness, so the engine pays nothing for them (no per-event cost, no
//! hot-path allocation; the copy set is untouched).
//!
//! One shape per engine run in its own schema: the engine's counters are process-global, so two
//! shapes in one instance would report each other's rows (`tests/efficiency.rs:1-3` precedent).
//! Per shape the run is: build every view to `ready` (warm-up), write a fixed row budget, wait for
//! the applied position to reach the source position (the drain), and take the window between them;
//! the whole sequence repeats `LAZYVIEW_TEST_BENCH_REPS` times and the medians are printed. After
//! every window every view is compared with its oracle *by the server* (`COUNT(*)` plus a
//! checksum), so the harness never holds a view in memory, and `full_rebuilds` must not have moved
//! for a shape whose views are incremental (owner rule: an incremental view never falls back to a
//! rebuild — a refresh is allowed, which is why the `setop` shape's `UNION ALL` view is exempt).
//!
//! This PR records numbers and asserts only what must be true for a run to mean anything
//! (events applied, at least one row per flush, no backpressure, no rebuild, views exact). Budgets
//! come from the measured distribution in PR5, per the plan.
//!
//! Knobs (all optional, all printed in the run header): `LAZYVIEW_TEST_BENCH_ROWS` (row budget per
//! window), `_REPS` (windows per shape), `_WRITERS` (concurrent connections), `_SHAPES` (comma list
//! filter), `_FULL_MS` (`LAZYVIEW_FULL_REFRESH_MS` for the engine: `0`, the default, refreshes a
//! full-mode view as soon as it is dirty — raise it to measure the cadence-bound rate the plan's
//! item 19 wants), `_PIN` (a `taskset` cpu list, e.g. `2-3`: pins this process, so the engine and
//! the writers inherit it; off by default because a shared host has no reserved cores), `_JSON`
//! (write the rows as JSON to that path, for PR4's CI artifact).
//!
//! The second test is the same table with the views on a PostgreSQL target and needs
//! `LAZYVIEW_TEST_PG_DSNS`; without it the leg prints one `MEASURED` line saying so and measures
//! nothing (a note, not a `SKIP`: the source-leg table is the proof of this binary).

mod common;

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Result};
use mysql_async::Conn;

use common::benchwork::*;
use common::engineproc::{self, EngineProc};
use common::oracle::ViewSpec;
use common::pg::PgDb;
use common::*;

const T_BOOT: Duration = Duration::from_secs(60);
const T_STREAM: Duration = Duration::from_secs(90);
const T_READY: Duration = Duration::from_secs(120);
const T_CATCHUP: Duration = Duration::from_secs(180);
/// One `/proc` sample per this interval: the sampler's honesty is the selftest's job
/// (`harness_selftest`'s `…sampler_vs_time`).
const SAMPLE_EVERY: Duration = Duration::from_millis(50);

/// Which side the materialized views live on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Leg {
    /// MySQL-family source, views on the source (the schema the engine and the tables share).
    Source,
    /// MySQL-family source, views on a PostgreSQL target (`tests/targets.rs` uses the same shape).
    PgTarget,
}

#[test]
fn bench_throughput_shapes() {
    run(Leg::Source, "bench_throughput");
}

/// The same shapes with the views on a PostgreSQL target. Without `LAZYVIEW_TEST_PG_DSNS` there is
/// no such target to measure, and the leg says so on the log instead of skipping: the numbers of
/// this binary are the source-leg table, which this run also produces.
#[test]
fn bench_throughput_pg_target() {
    if pg::pg_servers().is_none() {
        engineproc::measured(
            "bench pg-target leg: no PostgreSQL target in LAZYVIEW_TEST_PG_DSNS, nothing measured here \
             (the source leg is the table)",
        );
        return;
    }
    run(Leg::PgTarget, "bench_throughput_pg_target");
}

fn run(leg: Leg, test: &str) {
    // The two legs measure rates: run them one at a time even under `--test-threads=2`, or they
    // would take CPU from each other's engines and the medians would mean nothing.
    let _serial = bench_lock();
    scenario(test, move |ctx| async move { run_leg(ctx, leg).await });
}

/// One benchmark at a time in this process (see `run`).
fn bench_lock() -> MutexGuard<'static, ()> {
    static L: OnceLock<Mutex<()>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Everything the run reads from the environment, with the plan's defaults.
struct Config {
    rows: usize,
    reps: usize,
    min_window: Duration,
    writers: usize,
    only: Option<Vec<String>>,
    full_ms: u64,
    pin: Option<String>,
    json: Option<String>,
}

fn num(var: &str, default: usize, lo: usize, hi: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map_or(default, |v| v.clamp(lo, hi))
}

impl Config {
    fn from_env() -> Self {
        Config {
            rows: num("LAZYVIEW_TEST_BENCH_ROWS", 6_000, 500, 200_000),
            reps: num("LAZYVIEW_TEST_BENCH_REPS", 5, 1, 9),
            writers: num("LAZYVIEW_TEST_BENCH_WRITERS", 2, 1, 8),
            min_window: Duration::from_secs(
                num("LAZYVIEW_TEST_BENCH_MIN_WINDOW_S", 5, 1, 120) as u64
            ),
            only: std::env::var("LAZYVIEW_TEST_BENCH_SHAPES")
                .ok()
                .map(|v| v.split(',').map(|s| s.trim().to_lowercase()).collect()),
            full_ms: num("LAZYVIEW_TEST_BENCH_FULL_MS", 0, 0, 600_000) as u64,
            pin: std::env::var("LAZYVIEW_TEST_BENCH_PIN")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
            json: std::env::var("LAZYVIEW_TEST_BENCH_JSON")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
        }
    }

    fn wants(&self, shape: &str) -> bool {
        self.only
            .as_ref()
            .is_none_or(|w| w.iter().any(|s| s == shape))
    }
}

/// One measured window of one shape.
#[derive(Clone)]
struct Row {
    shape: String,
    flavor: String,
    leg: &'static str,
    rep: usize,
    seed: u64,
    budget_rows: usize,
    window_s: f64,
    write_s: f64,
    events_s: f64,
    seen_s: f64,
    applied_s: f64,
    flushes_s: f64,
    rows_per_flush: f64,
    cpu_cores: f64,
    rss_med_mib: f64,
    rss_peak_mib: f64,
    full_rebuilds: u64,
    backpressure: u64,
    lag_over_budget: u64,
    cascade_rows: u64,
}

async fn run_leg(ctx: Arc<Ctx>, leg: Leg) -> Result<()> {
    let cfg = Config::from_env();
    let pg_server = match leg {
        Leg::Source => None,
        Leg::PgTarget => pg::pg_servers().and_then(|v| v.into_iter().next()),
    };
    header(&ctx, &cfg, leg);
    let targets = match leg {
        Leg::Source => None,
        Leg::PgTarget => Some("pg"),
    };
    let mut rows: Vec<Row> = Vec::new();
    for shape in shapes(targets).into_iter().filter(|s| cfg.wants(s.name)) {
        rows.extend(run_shape(&ctx, &cfg, leg, pg_server.as_ref(), shape).await?);
    }
    ensure!(
        !rows.is_empty(),
        "no shape ran (LAZYVIEW_TEST_BENCH_SHAPES?)"
    );
    print_table(&rows, leg);
    if let Some(path) = &cfg.json {
        write_json(path, &ctx, &cfg, leg, &rows)?;
    }
    Ok(())
}

/// The run header: the comparability controls of the plan's §1 (seed, pinning, load, governor) in
/// one line, so a reader of the table knows what the numbers were taken on.
fn header(ctx: &Ctx, cfg: &Config, leg: Leg) {
    let load = std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_default()
        .split_whitespace()
        .take(3)
        .collect::<Vec<_>>()
        .join(" ");
    let gov = std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let pin = match &cfg.pin {
        Some(spec) => match pin_self(spec) {
            Ok(()) => format!("taskset {spec}"),
            Err(e) => format!("off (taskset {spec} failed: {e})"),
        },
        None => "off".to_string(),
    };
    let bin = engineproc::engine_binary()
        .map_or_else(|e| format!("none ({e})"), |p| p.display().to_string());
    let ncpu = std::thread::available_parallelism().map_or(0, |n| n.get());
    let sha = git_sha();
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let elapsed = ctx.elapsed();
    let line = format!(
        "bench start test={} leg={:?} engine_bin={} seed={} rows={} reps={} writers={} full_ms={} \
         min_window_s={} pin={pin} ncpu={} loadavg=[{load}] governor={gov} sha={} epoch={} elapsed={:?}",
        ctx.test,
        leg,
        bin,
        ctx.seed,
        cfg.rows,
        cfg.reps,
        cfg.writers,
        cfg.full_ms,
        cfg.min_window.as_secs(),
        ncpu,
        sha,
        epoch,
        elapsed,
    );
    ctx.log(&line);
    ctx.note(line.clone());
    engineproc::measured(line);
}

/// Pins this process (the engine and the writers are children, so they inherit the mask).
fn pin_self(spec: &str) -> Result<()> {
    let out = std::process::Command::new("taskset")
        .args(["-pc", spec, &std::process::id().to_string()])
        .output()?;
    ensure!(
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).trim().to_string()
    );
    Ok(())
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args([
            "-C",
            &repo_dir().display().to_string(),
            "rev-parse",
            "--short",
            "HEAD",
        ])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".into(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        )
}

async fn run_shape(
    ctx: &Arc<Ctx>,
    cfg: &Config,
    leg: Leg,
    pg_server: Option<&pg::Server>,
    shape: Shape,
) -> Result<Vec<Row>> {
    let db = ctx.new_db(&format!("bt_{}", shape.name)).await?;
    let mut c = db.conn().await?;
    create_tables(&mut c).await?;
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, &shape.base).await?;
    let pgdb = match (leg, pg_server) {
        (Leg::PgTarget, Some(s)) => Some(pg::new_pg_db(s, &format!("bt_{}", shape.name)).await?),
        _ => None,
    };
    let mut env = match &pgdb {
        Some(p) => target_env(&[("pg", &p.dsn)]),
        None => Vec::new(),
    };
    env.push(("LAZYVIEW_FULL_REFRESH_MS".into(), cfg.full_ms.to_string()));
    let mut eng = EngineProc::start_with(ctx, &db, &ov_refs(&env), &[]).await?;
    let t_boot = eng.wait_startup(T_BOOT).await?;
    let t_stream = eng.wait_streaming(T_STREAM).await?;
    let base: Vec<String> = shape.base.iter().map(|v| v.name.clone()).collect();
    let chained: Vec<String> = shape.chained.iter().map(|v| v.name.clone()).collect();
    wait_states(&db, &base, T_READY, &eng).await?;
    // The chained view's SELECT reads the upstream MV, so its oracle can only be created once
    // that table exists.
    for spec in shape.base.iter().chain(shape.chained.iter()) {
        create_oracle(&mut c, spec).await?;
    }
    if !shape.chained.is_empty() {
        add_views(&mut c, &shape.chained).await?;
        wait_states(&db, &chained, T_READY, &eng).await?;
    }
    let specs: Vec<&ViewSpec> = shape.base.iter().chain(shape.chained.iter()).collect();
    let seed = ctx.sub_seed(&format!("shape-{}", shape.name));
    ctx.log(format!(
        "bench shape={} ({}) boot={t_boot:?} streaming={t_stream:?} views={} seed={seed}",
        shape.name,
        shape.what,
        specs.len(),
    ));

    // A chained view reads the engine's own writes to its upstream table, which reach the binlog only
    // after the source position the writers left: a round per level of the chain plus the shared
    // helper's margin (#378). The chain is at most `chained.len()` levels deep.
    let rounds = if shape.chained.is_empty() {
        1
    } else {
        2 + shape.chained.len()
    };

    // Warm-up: the first window measures a cold engine (builds just finished, caches empty), so it
    // is written and drained without being measured.
    write_burst(&db, cfg, &shape, 0, seed, cfg.min_window / 2).await?;
    drain(&mut eng, &mut c, T_CATCHUP, rounds).await?;

    let mut out = Vec::new();
    for rep in 1..=cfg.reps {
        let before = eng.scrape().await?;
        eng.start_sampler(SAMPLE_EVERY);
        let t0 = Instant::now();
        let budget = write_burst(&db, cfg, &shape, rep * 64, seed, cfg.min_window).await?;
        let write_s = t0.elapsed();
        drain(&mut eng, &mut c, T_CATCHUP, rounds).await?;
        let after = eng.scrape().await?;
        let usage = eng.stop_sampler()?;
        let window_s = t0.elapsed().as_secs_f64().max(0.001);

        let events = after.delta(&before, "events");
        let seen = after.delta(&before, "rows_seen");
        let applied = after.delta(&before, "mv_rows_upserted")
            + after.delta(&before, "mv_rows_inserted")
            + after.delta(&before, "mv_rows_deleted");
        let flushes = after.delta(&before, "flushes");
        let rebuilds = after.delta(&before, "full_rebuilds");
        let row = Row {
            shape: shape.name.to_string(),
            flavor: db.flavor.clone(),
            leg: if leg == Leg::Source { "source" } else { "pg" },
            rep,
            seed,
            budget_rows: budget,
            window_s,
            write_s: write_s.as_secs_f64(),
            events_s: events as f64 / window_s,
            seen_s: seen as f64 / window_s,
            applied_s: applied as f64 / window_s,
            flushes_s: flushes as f64 / window_s,
            rows_per_flush: if flushes > 0 {
                applied as f64 / flushes as f64
            } else {
                0.0
            },
            cpu_cores: usage.cpu_seconds / window_s,
            rss_med_mib: usage.rss_med_kib as f64 / 1024.0,
            rss_peak_mib: usage.rss_peak_kib as f64 / 1024.0,
            full_rebuilds: rebuilds,
            backpressure: after.delta(&before, "binlog_backpressure"),
            lag_over_budget: after.delta(&before, "lag_over_budget"),
            cascade_rows: after.delta(&before, "cascade_rows"),
        };
        check_window(&shape, &row, events, applied, flushes, &eng)?;
        compare_views(&mut c, &db, &pgdb, leg, &specs).await?;
        engineproc::measured(format!("bench row {row}"));
        ctx.note(format!("{} rep {rep}/{}: {row}", shape.name, cfg.reps));
        out.push(row);
    }
    eng.stop()?;
    Ok(out)
}

/// What must be true for a window to have measured anything: the sanity floor of the plan's PR3
/// acceptance (`events_s > 0`, `rows_per_flush >= 1`) plus the two side-signals that must stay at
/// zero in a clean run and the owner's rule that an incremental view never falls back to a rebuild.
fn check_window(
    shape: &Shape,
    r: &Row,
    events: u64,
    applied: u64,
    flushes: u64,
    eng: &EngineProc,
) -> Result<()> {
    let ctx = format!(
        "shape {} rep {} (budget {} rows, {:.2}s window); engine log tail:\n{}",
        shape.name,
        r.rep,
        r.budget_rows,
        r.window_s,
        eng.log_tail(25),
    );
    ensure!(events > 0, "no binlog event was applied: {ctx}");
    ensure!(applied > 0, "no materialized row was written: {ctx}");
    ensure!(flushes > 0, "no flush happened: {ctx}");
    ensure!(
        r.rows_per_flush >= 1.0,
        "rows_per_flush {} < 1: {ctx}",
        r.rows_per_flush
    );
    ensure!(
        r.backpressure == 0,
        "binlog_backpressure moved ({}): {ctx}",
        r.backpressure
    );
    ensure!(
        r.lag_over_budget == 0,
        "lag_over_budget moved ({}): {ctx}",
        r.lag_over_budget
    );
    if shape.incremental {
        ensure!(
            r.full_rebuilds == 0,
            "an incremental view was rebuilt as a whole ({} full_rebuilds): {ctx}",
            r.full_rebuilds
        );
    }
    Ok(())
}

/// Every view equals its oracle, computed by the servers: on the source leg the row checksum, on
/// the PostgreSQL leg the row count (the exactness of a PostgreSQL target, column by column, is
/// what `tests/targets.rs` proves; the bench keeps its own memory flat and compares aggregates).
async fn compare_views(
    c: &mut Conn,
    db: &Db,
    pgdb: &Option<PgDb>,
    leg: Leg,
    specs: &[&ViewSpec],
) -> Result<()> {
    for spec in specs {
        match (leg, pgdb) {
            (Leg::Source, _) => compare_with_oracle(c, &db.name, spec).await?,
            (Leg::PgTarget, Some(p)) => {
                let want = scalar_i64(
                    c,
                    &format!("SELECT COUNT(*) FROM {}", ident(&oracle_name(&spec.name))),
                )
                .await?;
                let got = pg_count(p, &spec.name).await?;
                ensure!(
                    got == want,
                    "{}: the PostgreSQL target holds {got} rows, the source oracle {want}",
                    spec.name
                );
            }
            (Leg::PgTarget, None) => bail!("{}: no PostgreSQL target on this leg", spec.name),
        }
    }
    Ok(())
}

async fn pg_count(pgdb: &PgDb, view: &str) -> Result<i64> {
    let client = pgdb.client().await?;
    let q = format!("SELECT count(*) FROM \"{}\".\"{}\"", pgdb.schema, view);
    let row = client
        .query_one(&q, &[])
        .await
        .map_err(|e| anyhow!("{q}: {e}"))?;
    row.try_get::<usize, i64>(0)
        .map_err(|e| anyhow!("{q}: {e}"))
}

/// The writers of one window: `writers` connections, each running its own seeded plan, repeating
/// phases of `rows` rows until the window has lasted `min_window`. No pacing sleep: the engine's
/// own backpressure is the throttle, which is what a throughput number is for. The floor is what
/// makes a rate a rate — a window of a few poll intervals measures the harness, not the engine.
async fn write_burst(
    db: &Db,
    cfg: &Config,
    shape: &Shape,
    phase_base: usize,
    seed: u64,
    min_window: Duration,
) -> Result<usize> {
    let t0 = Instant::now();
    let mut total = 0usize;
    let mut phase = phase_base;
    loop {
        total += one_phase(db, cfg, shape, phase, seed).await?;
        if t0.elapsed() >= min_window || total >= cfg.rows * 20 {
            return Ok(total);
        }
        phase += 1;
    }
}

async fn one_phase(db: &Db, cfg: &Config, shape: &Shape, phase: usize, seed: u64) -> Result<usize> {
    let per = (cfg.rows / cfg.writers).max(1);
    let mut handles = Vec::new();
    for w in 0..cfg.writers {
        let dsn = db.dsn.clone();
        let plan = write_plan(seed, phase, w, per, shape.cascade_deletes);
        handles.push(tokio::spawn(async move {
            let mut c = connect(&dsn).await?;
            for s in &plan.stmts {
                exec(&mut c, s).await?;
            }
            c.disconnect().await.ok();
            Ok::<usize, anyhow::Error>(plan.row_ops)
        }));
    }
    let mut total = 0usize;
    for h in handles {
        total += h.await.map_err(|e| anyhow!("writer task: {e}"))??;
    }
    Ok(total)
}

/// Catch-up: the engine says `ready` (the plan's `start → caught up` boundary) and has applied the
/// source position the writes left behind, `rounds` times: a chained view reads the engine's own
/// writes to its upstream table, which reach the binlog only after the first position, so the
/// shared wait takes the position again per chain level (#378).
async fn drain(eng: &mut EngineProc, c: &mut Conn, timeout: Duration, rounds: usize) -> Result<()> {
    eng.wait_ready(timeout).await?;
    eng.wait_caught_up_rounds(c, timeout, rounds).await
}

/// Polls `_lazyview_state` until every named view has rows and all of them are `ready` (the
/// harness's `wait_ready` needs the in-process engine, so the process leg polls the state itself).
async fn wait_states(db: &Db, names: &[String], timeout: Duration, eng: &EngineProc) -> Result<()> {
    ensure!(!names.is_empty(), "no view to wait for");
    let mut c = db.conn().await?;
    let deadline = Instant::now() + timeout;
    loop {
        let all = view_states(&mut c).await?;
        let why: Vec<String> = names
            .iter()
            .filter_map(|n| {
                let rows: Vec<_> = all.iter().filter(|s| &s.name == n).collect();
                if rows.is_empty() {
                    Some(format!("{n}: no state row"))
                } else if rows.iter().all(|s| s.state == "ready") {
                    None
                } else {
                    Some(format!(
                        "{n}: {}",
                        rows.iter()
                            .map(|s| format!(
                                "{}{}{}",
                                s.state,
                                s.target
                                    .as_deref()
                                    .map_or(String::new(), |t| format!("@{t}")),
                                s.last_error
                                    .clone()
                                    .map_or(String::new(), |e| format!(" ({e})"))
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                }
            })
            .collect();
        if why.is_empty() {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!(
                "views not ready in {timeout:?}: {}; engine log tail:\n{}",
                why.join(" | "),
                eng.log_tail(25),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One line per window, the columns of the plan's §1.
impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shape={} flavor={} leg={} rep={} seed={} budget={} window_s={:.2} write_s={:.2} \
             events_s={:.0} seen_s={:.0} rows_s={:.0} flushes_s={:.1} rows_per_flush={:.1} \
             cpu_cores={:.2} rss_med_mib={:.0} rss_peak_mib={:.0} full_rebuilds={} bp={} \
             lag_over_budget={} cascade_rows={}",
            self.shape,
            self.flavor,
            self.leg,
            self.rep,
            self.seed,
            self.budget_rows,
            self.window_s,
            self.write_s,
            self.events_s,
            self.seen_s,
            self.applied_s,
            self.flushes_s,
            self.rows_per_flush,
            self.cpu_cores,
            self.rss_med_mib,
            self.rss_peak_mib,
            self.full_rebuilds,
            self.backpressure,
            self.lag_over_budget,
            self.cascade_rows,
        )
    }
}

/// The table: one line per shape with the median of each column and the min/max the plan keeps.
fn print_table(rows: &[Row], leg: Leg) {
    let mut shapes: Vec<String> = rows.iter().map(|r| r.shape.clone()).collect();
    shapes.sort();
    shapes.dedup();
    for shape in shapes {
        let of: Vec<&Row> = rows.iter().filter(|r| r.shape == shape).collect();
        let cols = [
            ("events_s", col(&of, |r| r.events_s)),
            ("rows_s", col(&of, |r| r.applied_s)),
            ("seen_s", col(&of, |r| r.seen_s)),
            ("flushes_s", col(&of, |r| r.flushes_s)),
            ("rows_per_flush", col(&of, |r| r.rows_per_flush)),
            ("cpu_cores", col(&of, |r| r.cpu_cores)),
            ("rss_med_mib", col(&of, |r| r.rss_med_mib)),
            ("rss_peak_mib", col(&of, |r| r.rss_peak_mib)),
        ];
        let body = cols
            .iter()
            .map(|(n, v)| format!("{n}={} [{:.0}..{:.0}]", median(v), min(v), max(v)))
            .collect::<Vec<_>>()
            .join(" ");
        let line = format!(
            "TABLE {:<9} flavor={} leg={:?} n={} rebuilds={} cascades={} {}",
            shape,
            of[0].flavor,
            leg,
            of.len(),
            of.iter().map(|r| r.full_rebuilds).sum::<u64>(),
            of.iter().map(|r| r.cascade_rows).sum::<u64>(),
            body,
        );
        engineproc::measured(line.clone());
        eprintln!("{line}");
    }
}

fn col(rows: &[&Row], f: impl Fn(&Row) -> f64) -> Vec<f64> {
    rows.iter().map(|r| f(r)).collect()
}

/// The number the plan reports: the middle of the measured windows (the mean of the two middle
/// ones when there is an even number), with min and max kept beside it.
fn median(v: &[f64]) -> f64 {
    let mut v = v.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    match v.len() {
        0 => 0.0,
        n if n % 2 == 1 => v[n / 2],
        n => (v[n / 2 - 1] + v[n / 2]) / 2.0,
    }
}

fn min(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn max(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
}

/// PR4's CI artifact: the same rows as JSON, with the header's comparability controls.
fn write_json(path: &str, ctx: &Ctx, cfg: &Config, leg: Leg, rows: &[Row]) -> Result<()> {
    let v = serde_json::json!({
        "test": ctx.test,
        "leg": format!("{leg:?}"),
        "sha": git_sha(),
        "epoch": SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()),
        "seed": ctx.seed,
        "rows_per_window": cfg.rows,
        "writers": cfg.writers,
        "full_refresh_ms": cfg.full_ms,
        "pin": cfg.pin,
        "windows": rows.iter().map(|r| serde_json::json!({
            "shape": r.shape, "flavor": r.flavor, "leg": r.leg, "rep": r.rep, "seed": r.seed,
            "budget_rows": r.budget_rows, "window_s": r.window_s, "write_s": r.write_s,
            "events_s": r.events_s, "seen_s": r.seen_s, "rows_s": r.applied_s,
            "flushes_s": r.flushes_s, "rows_per_flush": r.rows_per_flush,
            "cpu_cores": r.cpu_cores, "rss_med_mib": r.rss_med_mib, "rss_peak_mib": r.rss_peak_mib,
            "full_rebuilds": r.full_rebuilds, "binlog_backpressure": r.backpressure,
            "lag_over_budget": r.lag_over_budget, "cascade_rows": r.cascade_rows,
        })).collect::<Vec<_>>(),
    });
    std::fs::write(path, serde_json::to_string_pretty(&v)?)
        .map_err(|e| anyhow!("write {path}: {e}"))?;
    engineproc::measured(format!("bench json: {path} ({} windows)", rows.len()));
    Ok(())
}
