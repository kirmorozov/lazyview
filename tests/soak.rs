//! The soak harness's tests: the neutral workload of `tests/common/soakwork.rs` (PR S1), the
//! checks, samplers and one-page report of `tests/common/soakcheck.rs` (PR S2), and the two modes
//! of `tests/common/soakchaos.rs` (PR S3): a 30-minute smoke and the 24–72 hour long run
//! (§5), both behind `LAZYVIEW_TEST_SOAK`. What lives here proves the generator and the view set
//! before anything runs for hours, and then runs them for hours.

mod common;

use anyhow::{ensure, Context, Result};
use common::oracle::fingerprint;
use common::soakchaos::{plan, run_event, soak_chaos_per_h, ChaosRun};
use common::soakcheck::{
    quiesce_and_check, run_dir, scan_warns, set_writers, soak_check_s, Check, CheckRow, Report,
    Window,
};
use common::soakwork::{
    run_paced, run_soak_batches, setup_soak, soak_rate, soak_views, soak_writers, SoakIds,
    SoakWorkload, SOAK_TABLES,
};
use common::workload::Sizes;
use common::*;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// The neutral set plans, builds and equals its oracle, and the generator is deterministic: the
/// same seed in two independent schemas writes the same rows into every table. Guards F8–F10
/// (the soak has its own neutral schema and mix, a seeded generator) without a long run.
#[test]
fn soak_workload_is_deterministic() {
    scenario("soak_det", |ctx| async move {
        let seed = ctx.sub_seed("seed");
        let wseed = ctx.sub_seed("wl");
        let specs = soak_views();
        ensure!(
            specs.len() >= 20,
            "the neutral set must cover every access path, got {}",
            specs.len()
        );

        // Two independent schemas, the same seed: setup twice, 200 batches each.
        let a = ctx.new_db("soak_det_a").await?;
        let b = ctx.new_db("soak_det_b").await?;
        let ids_a = setup_soak(&a, seed, Sizes::tiny()).await?;
        let ids_b = setup_soak(&b, seed, Sizes::tiny()).await?;
        let mut ca = a.conn().await?;
        let mut wa = SoakWorkload::new(wseed, ids_a);
        let n_a = run_soak_batches(&mut ca, &mut wa, 200).await?;
        let mut cb = b.conn().await?;
        let mut wb = SoakWorkload::new(wseed, ids_b);
        let n_b = run_soak_batches(&mut cb, &mut wb, 200).await?;
        ctx.log(format!(
            "{n_a} / {n_b} txns (same seed); ops: {}",
            wa.stats_line()
        ));
        ensure!(
            n_a == n_b,
            "the two runs wrote {n_a} and {n_b} transactions"
        );

        // The fingerprint of every table must agree between the two runs (and be non-trivial).
        let mut rows = 0;
        for t in SOAK_TABLES {
            let (na, ha) = fingerprint(&mut ca, t).await?;
            let (nb, hb) = fingerprint(&mut cb, t).await?;
            ctx.log(format!("fingerprint {t}: {na}/{ha} vs {nb}/{hb}"));
            ensure!(
                (na, ha) == (nb, hb),
                "table {t} differs between the two runs with the same seed: {na}/{ha} vs {nb}/{hb}"
            );
            rows += na;
        }
        ensure!(rows > 0, "the seed data is empty: {rows} rows in total");

        // The whole set (every access path, the two noisy definitions, the chained pair) builds
        // and equals its oracle in one check.
        let (mut eng, _) = start_with_views(&ctx, &a, &specs, &[]).await?;
        verify(&ctx, &mut eng, &a, &specs, "after 200 batches").await?;
        eng.stop().await?;
        ca.disconnect().await.ok();
        cb.disconnect().await.ok();
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// The two modes (S3.4)
// ---------------------------------------------------------------------------------------------
/// §5's smoke: 30 minutes on one flavor, the same code path as the long run, a check every 60 s
/// and a chaos event every ~6 minutes — enough for all five kinds of §3 to land, which is the
/// point of running the smoke in CI (S3.4: both modes share one function body).
const SMOKE_MINUTES: u32 = 30;
const SMOKE_CHAOS_PER_H: u32 = 10;

/// `LAZYVIEW_TEST_SOAK_HOURS` (D8: 48 h), the long mode's length.
fn soak_hours() -> u32 {
    std::env::var("LAZYVIEW_TEST_SOAK_HOURS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| (1..=72).contains(v))
        .unwrap_or(48)
}

/// `LAZYVIEW_TEST_SOAK=smoke` runs the smoke, `=long` the long run; anything else skips both with
/// a counted note (the opt-in shape of `reg_saved_position_purged_while_down`).
#[test]
fn soak_smoke_30m() {
    soak_mode("smoke");
}

/// The 24–72 hour run of §5, on the harness's own servers (`LAZYVIEW_TEST_SOAK_CONTAINERS`):
/// lv-super starts it before v0.2.0 and before any pilot, through `scripts/soak.sh long`.
#[test]
fn soak_long() {
    soak_mode("long");
}

/// One body, two parameter sets (S3.4): CI's nightly smoke exercises the long run's code.
fn soak_mode(mode: &'static str) {
    let smoke = mode == "smoke";
    let (minutes, per_hour, sizes, check_s) = if smoke {
        (
            SMOKE_MINUTES,
            SMOKE_CHAOS_PER_H,
            Sizes::tiny(),
            60.min(soak_check_s()),
        )
    } else {
        let hours = soak_hours();
        (
            hours.saturating_mul(60),
            soak_chaos_per_h(),
            Sizes::scaled(),
            soak_check_s(),
        )
    };
    // F12: `run_timeout()` (`mod.rs:165`) would stop the scenario after 900 s × scale, so the run
    // sets its own timeout — its own duration plus 30 minutes of margin — *before* `scenario`.
    std::env::set_var(
        "LAZYVIEW_TEST_TIMEOUT_S",
        (u64::from(minutes) * 60 + 1800).to_string(),
    );
    let tag = format!("soak_{mode}");
    scenario(&tag, move |ctx| async move {
        let switch = std::env::var("LAZYVIEW_TEST_SOAK")
            .ok()
            .map(|v| v.trim().to_string());
        if switch.as_deref() != Some(mode) {
            ctx.skip(&format!(
                "set LAZYVIEW_TEST_SOAK={mode} to run the {}",
                if smoke {
                    "30-minute smoke"
                } else {
                    "long soak"
                }
            ));
            return Ok(());
        }
        let dir = run_dir();
        let seed = ctx.sub_seed("soak");
        let mut report = Report::new(mode, seed, &ctx.server().base_dsn, ctx.flavor());
        let mut logs: Vec<PathBuf> = Vec::new();
        ctx.log(format!(
            "soak {mode}: {minutes} min, seed {seed:#x}, {} views, {} writers at TPS {}, check \
             every {check_s}s, {} chaos events/h → {}",
            soak_views().len(),
            soak_writers(),
            soak_rate(),
            per_hour,
            dir.display()
        ));
        let out = soak_run(
            &ctx,
            seed,
            minutes,
            per_hour,
            check_s,
            sizes,
            &mut report,
            &mut logs,
        )
        .await;
        // The report is the evidence and `finish_run` (`mod.rs:856`) drops the schemas and the
        // engine log dir afterwards: write it on every path, including this one (F12).
        write_report(&ctx, &dir, &mut report, &logs);
        out?;
        let failed: Vec<String> = report
            .failed()
            .iter()
            .map(|c| format!("{}: {}", c.name, c.detail))
            .collect();
        ensure!(
            failed.is_empty(),
            "{} failed check(s): {}",
            failed.len(),
            failed.join(" | ")
        );
        ctx.log(format!(
            "soak {mode}: {} — {} txns, {} checks, {} chaos events",
            report.done_line(),
            report.txns,
            report.checks.len(),
            report.events.len()
        ));
        Ok(())
    });
}

/// One soak run: the schema and the views, the paced writers, the seeded schedule of §3 and the
/// checks of §4 on their own interval, then the counters of the report (§4) — the caller writes
/// the report itself, on every path.
#[allow(clippy::too_many_arguments)]
async fn soak_run(
    ctx: &Ctx,
    seed: u64,
    minutes: u32,
    per_hour: u32,
    check_s: u64,
    sizes: Sizes,
    report: &mut Report,
    logs: &mut Vec<PathBuf>,
) -> Result<()> {
    let specs = soak_views();
    ensure!(
        specs.len() >= 20,
        "the neutral set must cover every access path, got {}",
        specs.len()
    );
    // One server per `scenario` run (`mod.rs:689`), so every view is maintained on the source;
    // `soakwork::soak_views_with_peer` is the shape of a `scenario_multi` run with a peer.
    let targets: HashMap<String, Db> = HashMap::new();
    let db = ctx.new_db("soak").await?;
    let ids = setup_soak(&db, seed, sizes).await?;
    let ov: Vec<(&str, &str)> = Vec::new();
    // The definitions go into `_lazyview` and the engine starts with them (`mod.rs:1138`, the
    // shape `soak_workload_is_deterministic` uses), with `LAZYVIEW_HEALTH_ADDR` on a fresh port
    // (`engine.rs:251`) so `/metrics` answers for the lag and memory samples (S2.6).
    let addr = free_local_addr()?;
    let (mut eng, _) =
        start_with_views(ctx, &db, &specs, &[("LAZYVIEW_HEALTH_ADDR", addr.as_str())]).await?;
    logs.push(eng.log_path.clone());

    let n_writers = soak_writers();
    let per_writer = ((usize::try_from(soak_rate()).unwrap_or(1)) / n_writers).max(1) as u32;
    let mut writers = start_writers(&db, &ids, seed, n_writers, per_writer);
    let began = Instant::now();
    let deadline = began + Duration::from_secs(u64::from(minutes) * 60);
    let events = plan(seed, minutes, per_hour);
    let mut next_check = began + Duration::from_secs(check_s);
    let mut window = Window::now(&eng);
    let mut totals: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut done = 0usize;
    let out = loop {
        if Instant::now() >= deadline {
            break Ok(());
        }
        // Whichever is due first: the next event of the seeded schedule, or the next check.
        if let Some((at, kind)) = events
            .get(done)
            .copied()
            .filter(|(at, _)| began + *at <= Instant::now())
        {
            done += 1;
            // §3: the event runs while the paced writers keep going; the exactness check that ends
            // it pauses them, and a paced writer exits when its watch turns (`soakwork.rs:1561`) —
            // so they are joined after the event and started again.
            let mut run = ChaosRun {
                ctx,
                src: &db,
                targets: &targets,
                specs: &specs,
                writers: &writers.0,
                ov: &ov,
                logs,
            };
            let (row, next) = run_event(&mut run, eng, kind, at).await?;
            eng = next;
            // An event that stopped the engine handed back a new one, and `Snapshot::since`
            // subtracts (`metrics.rs:40`): a window opened on the old engine's counters would
            // underflow. Every counter table starts at the engine it measures.
            window = Window::now(&eng);
            fold(report, &row, began);
            report.events.push(Check {
                name: kind.name(),
                pass: row.check.pass,
                detail: row.check.detail,
            });
            report.txns += join_writers(&mut writers.1).await?;
            writers = start_writers(&db, &ids, seed, n_writers, per_writer);
            continue;
        }
        let soon = events
            .get(done)
            .map_or(next_check, |(at, _)| began + *at)
            .min(next_check);
        if soon > Instant::now() {
            tokio::time::sleep(soon - Instant::now()).await;
        }
        if Instant::now() < next_check {
            continue; // an event became due while waiting
        }
        next_check = Instant::now() + Duration::from_secs(check_s);
        let (d, w) = window.roll(&eng);
        window = w;
        for (name, value) in &d {
            if *value > 0 {
                *totals.entry(*name).or_default() += *value;
            }
        }
        set_writers(&writers.0, true);
        report.txns += join_writers(&mut writers.1).await?;
        // The scheduled check of §4, in its own order (S2.3). A failed check ends the run: the
        // report names it, and a schema that disagrees with its oracle is not worth 40 more hours.
        let row = quiesce_and_check(
            ctx,
            &mut eng,
            &db,
            &targets,
            &specs,
            &writers.0,
            &format!("scheduled @+{}s", began.elapsed().as_secs()),
        )
        .await?;
        fold(report, &row, began);
        writers = start_writers(&db, &ids, seed, n_writers, per_writer);
    };
    // The load never outlives the run, and its transactions are the report's.
    set_writers(&writers.0, true);
    match join_writers(&mut writers.1).await {
        Ok(n) => report.txns += n,
        Err(e) if out.is_ok() => return Err(e),
        Err(_) => {}
    }
    let (d, _) = window.roll(&eng);
    for (name, value) in &d {
        if *value > 0 {
            *totals.entry(*name).or_default() += *value;
        }
    }
    report.counters = totals.into_iter().collect();
    eng.stop().await?;
    out
}

/// The numbers of one check into the report (§4's rows): its PASS/FAIL row, the lag and RSS
/// samples, and the per-view table (the last check's, which is the state the run ended in).
fn fold(report: &mut Report, row: &CheckRow, began: Instant) {
    report.lag_secs.push(row.lag.settle_s);
    report
        .rss
        .push((began.elapsed().as_secs_f64(), row.mem.rss_kib));
    report.views = row.views.clone();
}

/// (Re)starts the paced writers of S1.6: one task per writer, each with its own stop watch, so a
/// check can pause the load and the run start it again; each writer owns its share of the TPS.
fn start_writers(
    db: &Db,
    ids: &Arc<SoakIds>,
    seed: u64,
    n: usize,
    per_writer: u32,
) -> (
    Vec<watch::Sender<bool>>,
    Vec<tokio::task::JoinHandle<Result<usize>>>,
) {
    let mut senders = Vec::with_capacity(n);
    let mut handles = Vec::with_capacity(n);
    for w in 0..n {
        let (tx, rx) = watch::channel(false);
        let db = db.clone();
        let ids = ids.clone();
        handles.push(tokio::spawn(async move {
            run_paced(
                &db,
                ids,
                derive_seed(seed, &format!("writer{w}")),
                1,
                per_writer,
                rx,
            )
            .await
        }));
        senders.push(tx);
    }
    (senders, handles)
}

/// Joins the writer tasks and adds up their transactions (they exit when their watch turns).
async fn join_writers(handles: &mut Vec<tokio::task::JoinHandle<Result<usize>>>) -> Result<u64> {
    let mut total = 0u64;
    for h in handles.drain(..) {
        let n = h
            .await
            .context("a paced writer task was cancelled")?
            .context("a paced writer failed")?;
        total += n as u64;
    }
    Ok(total)
}

/// Writes `report.md` / `report.json` and copies the engine logs next to them: `finish_run`
/// removes the log dir, so without the copy the WARN evidence of a passing run is gone (the
/// breakdown's note for lv-super). Best-effort: a run's verdict must not depend on the copy.
fn write_report(ctx: &Ctx, dir: &Path, report: &mut Report, logs: &[PathBuf]) {
    if let Err(e) = std::fs::create_dir_all(dir) {
        ctx.log(format!("report: cannot create {}: {e}", dir.display()));
        return;
    }
    for p in logs {
        if let Some(n) = p.file_name() {
            if let Err(e) = std::fs::copy(p, dir.join(n)) {
                ctx.log(format!("report: cannot copy {}: {e}", p.display()));
            }
        }
    }
    if let Ok(w) = scan_warns(logs) {
        report.warns = w;
    }
    match report.write(dir) {
        Ok(()) => ctx.log(format!(
            "report: {}/report.md — {}",
            dir.display(),
            report.done_line()
        )),
        Err(e) => ctx.log(format!("report: {e:#}")),
    }
    if let Some(f) = report.failed().first() {
        bail_note(ctx, f);
    }
}

/// Names the first failed row on the run's log (the suite's summary line shows only the last).
fn bail_note(ctx: &Ctx, f: &Check) {
    ctx.log(format!("FAILED {} {}", f.name, f.detail));
}
