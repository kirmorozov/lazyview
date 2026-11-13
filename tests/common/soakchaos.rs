//! The seeded chaos schedule and the chaos events: §3 of `tmp/out/lvs-soak/breakdown.md`
//! (PR S3). What breaks the engine, a target and the source on purpose, when, and what the run
//! asserts after each event. Every event runs while the paced writers keep going and ends with
//! the scheduled check of [`soakcheck::quiesce_and_check`] (S2.3) plus the event's own
//! assertions, exactly as the table of §3 lists them.
//!
//! Two rules of the breakdown shape this file:
//!
//! * **F5/D5 is the binding rule.** An incremental view that does a whole-view refresh is a
//!   failure, and the `XA … prepared before this run` line is one too. The engine's
//!   `full_rebuilds` counts the *documented* cases — with the harness default
//!   `LAZYVIEW_TEST_FULL_REFRESH_MS=0` every flush rebuilds the `Full`/`Chained` views — so on a
//!   view set that contains them it is reported per event and never asserted to be 0; the
//!   per-view rule (F5) is what an event fails on.
//! * **Counters are per engine start** (`Engine::spawn` makes a fresh `Metrics`, `engine.rs:24`).
//!   An event that stops or crashes the engine does its work on the engine that saw it, reads
//!   its numbers there, and hands the run a fresh engine; every start's log file is recorded in
//!   the run's `Vec`, because `finish_run` removes the log dir (F12) and the logs are the
//!   evidence.

use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use lazyview::binlog::Position;
use lazyview::Snapshot;
use mysql_async::prelude::Queryable;

use super::soakcheck::{
    counter, counters, quiesce_and_check, view_rows, Check, CheckRow, Lag, MemRow,
};
use super::workload::names;
use super::*;

// ---------------------------------------------------------------------------------------------
// The schedule (S3.2)
// ---------------------------------------------------------------------------------------------

/// The event kinds of §3. `BinlogPurge` is deliberately **not** here: purging a shared server's
/// binlogs is opt-in and lv-devops-owned (`servers.md`: "Nobody changes GLOBAL settings or
/// purges binlogs"), so it stays where it is (`reg_saved_position_purged_while_down`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chaos {
    EngineCrash,
    EngineStop,
    TargetDown,
    SourcePause,
    SourceDdl,
}

/// Every kind, in the order `plan` guarantees them when the window allows one event per kind.
const KINDS: [Chaos; 5] = [
    Chaos::EngineCrash,
    Chaos::EngineStop,
    Chaos::TargetDown,
    Chaos::SourcePause,
    Chaos::SourceDdl,
];

impl Chaos {
    /// The report's name of this kind (`Check::name`, S2.2).
    pub fn name(self) -> &'static str {
        match self {
            Chaos::EngineCrash => "crash",
            Chaos::EngineStop => "stop",
            Chaos::TargetDown => "target_down",
            Chaos::SourcePause => "source_pause",
            Chaos::SourceDdl => "source_ddl",
        }
    }
}

/// `LAZYVIEW_TEST_SOAK_CHAOS_PER_H` (D8: 4 events per hour in the long run), in the style of
/// `mod.rs:89 scale()`.
pub fn soak_chaos_per_h() -> u32 {
    std::env::var("LAZYVIEW_TEST_SOAK_CHAOS_PER_H")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4)
}

/// Draws the run's chaos: `per_hour` events per hour over `minutes`, sorted by time, at least
/// one of every kind when the window allows it (with fewer events than kinds they are still all
/// different). Times and kinds come from [`derive_seed`] (`mod.rs:142`), so the same seed replays
/// the same chaos — the like-for-like diff of §6 needs the schedule to be part of the seed.
pub fn plan(seed: u64, minutes: u32, per_hour: u32) -> Vec<(Duration, Chaos)> {
    let minutes = u64::from(minutes.max(1));
    let window = (minutes * 60).max(2);
    let wanted = (((u64::from(per_hour.max(1))) * minutes) / 60).clamp(1, 512) as usize;
    let first = derive_seed(seed, "chaos-first") % KINDS.len() as u64;
    let mut taken: Vec<u64> = Vec::with_capacity(wanted);
    let mut out: Vec<(Duration, Chaos)> = Vec::with_capacity(wanted);
    for i in 0..wanted {
        let draw = derive_seed(seed, &format!("chaos{i}"));
        // The first `KINDS.len()` events are one of each kind (in a seeded order); the rest are
        // drawn independently.
        let kind = if i < KINDS.len() {
            KINDS[((i as u64 + first) as usize) % KINDS.len()]
        } else {
            KINDS[(derive_seed(draw, "kind") % KINDS.len() as u64) as usize]
        };
        // one event per second at most: a collision moves to the next free second
        let mut at = draw % window;
        while taken.contains(&at) {
            at = (at + 1) % window;
        }
        taken.push(at);
        out.push((Duration::from_secs(at), kind));
    }
    out.sort_by_key(|(at, _)| *at);
    out
}

// ---------------------------------------------------------------------------------------------
// The harness's own containers (S3.2, F7)
// ---------------------------------------------------------------------------------------------

/// The only containers the soak may stop and start (F7): never another project's
/// (`lazywf-test-*`, `m2cdc-*`, `lvtmp-eav-*`, `lvtmp-fence-1` stay untouched).
pub const OWNED_PREFIX: &str = "lvtmp-soak-";

/// `LAZYVIEW_TEST_SOAK_CONTAINERS=<source>=<c>,<target>=<c>`: the harness's own two containers
/// of D1, in the shape of `exclusive.rs:15 container()`. `None` when the switch is unset — the
/// shape the smoke runs in, where no container may be touched.
pub fn soak_containers() -> Option<(String, String)> {
    let raw = std::env::var("LAZYVIEW_TEST_SOAK_CONTAINERS").ok()?;
    let mut source = None;
    let mut target = None;
    for part in raw.split(',') {
        let (key, value) = part.trim().split_once('=')?;
        match key.trim() {
            "source" => source = Some(value.trim().to_string()),
            "target" => target = Some(value.trim().to_string()),
            _ => {}
        }
    }
    Some((source?, target?))
}

/// `docker stop` / `docker start` of one of the harness's own containers, modelled on
/// `exclusive.rs:37 restart()` (`stop`/`start` instead of `restart`). The name guard (F7) and
/// the subcommand guard run **before** `docker` is asked, so a wrong name never reaches the
/// daemon.
pub fn docker(args: &[&str]) -> Result<()> {
    let (sub, name) = match args.split_last() {
        Some((name, head)) if !head.is_empty() => (head[0], *name),
        _ => bail!("docker {args:?}: expected `stop [-t <s>] <container>` or `start <container>`"),
    };
    ensure!(
        sub == "stop" || sub == "start",
        "docker {sub}: the soak only stops and starts containers (restart is `exclusive.rs`'s, on \
         a server of its own)"
    );
    ensure!(
        name.starts_with(OWNED_PREFIX),
        "refusing to {sub} `{name}`: the soak may touch only its own `{OWNED_PREFIX}*` containers \
         (F7), and the servers of every other test are shared"
    );
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .with_context(|| format!("docker {} {name}", args.join(" ")))?;
    ensure!(
        out.status.success(),
        "docker {} {name}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

/// [`docker`] off the runtime's threads (the `spawn_blocking` of `exclusive.rs:39`): a `stop`
/// waits out the server's own shutdown.
pub async fn docker_blocking(args: Vec<String>) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        docker(&args.iter().map(String::as_str).collect::<Vec<_>>())
    })
    .await
    .context("the docker task panicked")?
}

// ---------------------------------------------------------------------------------------------
// The events (S3.3)
// ---------------------------------------------------------------------------------------------

/// What an event may use: the run's schema and views, its writers (so the event's check can pause
/// them), the overrides every engine of the run starts with, and the log files collected so far.
pub struct ChaosRun<'a> {
    pub ctx: &'a Ctx,
    pub src: &'a Db,
    pub targets: &'a HashMap<String, Db>,
    pub specs: &'a [ViewSpec],
    pub writers: &'a [watch::Sender<bool>],
    pub ov: &'a [(&'a str, &'a str)],
    pub logs: &'a mut Vec<PathBuf>,
}

impl ChaosRun<'_> {
    /// Starts an engine of the run with the run's overrides plus a fresh health endpoint
    /// (`engine.rs:251 free_local_addr()`, one per start) and records its log file: one file per
    /// start, so every restart is covered by the WARN accounting (S2.7).
    pub fn start_engine(&mut self) -> Result<Engine> {
        self.start_engine_with(&[])
    }

    /// [`Self::start_engine`] with extra overrides — the target-down event needs the budget of
    /// `reg_down_target_keeps_its_keys` (`LAZYVIEW_TEST_LAG_HOLD_BYTES=1`, no supervisor), which
    /// the run itself keeps off (the notes for lv-super: the default budget is the more
    /// realistic rehearsal).
    pub fn start_engine_with(&mut self, extra: &[(&str, &str)]) -> Result<Engine> {
        let addr = free_local_addr()?;
        let mut ov: Vec<(&str, &str)> = self.ov.to_vec();
        ov.extend_from_slice(extra);
        ov.push(("LAZYVIEW_HEALTH_ADDR", addr.as_str()));
        let eng = self.ctx.start_engine(self.src, &ov)?;
        self.logs.push(eng.log_path.clone());
        Ok(eng)
    }
}

/// Runs one chaos event of §3 while the writers keep going and ends it with
/// [`quiesce_and_check`]: the exactness check of the scheduled one, then the event's own
/// assertions. Returns the event's row (its `check` is the report row: `pass = Some(false)`
/// names what the event found) and the engine the run continues with — a new one whenever the
/// event stopped the old one.
///
/// `Err` is for an event that could not be performed at all (the harness's fault: a statement the
/// schema refused, an engine that died while it should have been alive); a view that disagrees
/// with its oracle is evidence, recorded in the row, and the run goes on.
pub async fn run_event(
    run: &mut ChaosRun<'_>,
    eng: Engine,
    kind: Chaos,
    at: Duration,
) -> Result<(CheckRow, Engine)> {
    let what = format!("{}@+{}s", kind.name(), at.as_secs());
    let ctx = run.ctx;
    ctx.log(format!("chaos {what}"));
    let mut eng = eng;
    let mut problems: Vec<String> = Vec::new();
    let mut extra: Vec<String> = Vec::new();

    match kind {
        // The `reg_crash_with_stale_full_view` set (`regressions/lifecycle.rs:310`): drop the
        // engine like `kill -9`, start it again, and it must be alive, streaming, and resumed
        // from the position it saved in `_lazyview_status`.
        Chaos::EngineCrash => {
            let before = saved_position(run.src).await?;
            eng.crash().await?;
            eng = run.start_engine()?;
            wait_streaming(&mut eng).await?;
            let mut c = run.src.conn().await?;
            wait_ready(&mut c, &mut eng, &names(run.specs), t_ready()).await?;
            c.disconnect().await.ok();
            // Resumed, not restarted from scratch: the applied position passes what was saved
            // before the crash, and the engine is streaming again.
            let applied = eng.applied();
            let passed = wait_until(
                "applied() to pass the position saved before the crash",
                t_catchup(),
                || async { Ok(eng.applied().is_some_and(|p| p >= before)) },
            )
            .await
            .is_ok();
            if !passed {
                problems.push(format!(
                    "the engine did not resume past the saved position {before}"
                ));
            }
            extra.push(format!(
                "saved {} -> applied {}",
                before,
                applied.map_or_else(|| "-".to_string(), |p| p.to_string())
            ));
            // D5: a prepared XA surviving the crash is a failure, never an allowlisted line.
            if let Some(line) = grep_log(&eng.log_path, "prepared before this run")? {
                problems.push(format!("the restarted engine found a prepared XA: {line}"));
            }
        }
        // The clean-stop gap of `regressions/catchup.rs:209` (each statement its own
        // transaction while the engine is down), then the same catch-up the pilot sees.
        Chaos::EngineStop => {
            eng.stop().await?;
            let mut c = run.src.conn().await?;
            exec(
                &mut c,
                "UPDATE entities SET weight = COALESCE(weight, 0) + 1 WHERE grp > 0",
            )
            .await
            .context("stop event: the first statement while the engine is down")?;
            exec(&mut c, "UPDATE items SET qty = qty + 1 WHERE grp > 0")
                .await
                .context("stop event: the second statement while the engine is down")?;
            c.disconnect().await.ok();
            eng = run.start_engine()?;
            let mut c = run.src.conn().await?;
            wait_ready(&mut c, &mut eng, &names(run.specs), t_ready()).await?;
            // Three rounds: one per chained level plus margin (`engine.rs:205`).
            eng.wait_caught_up_rounds(&mut c, t_catchup(), 3)
                .await
                .with_context(|| format!("{what}: the catch-up after the restart"))?;
            let d = counters(&eng, &Snapshot::default());
            extra.push(format!(
                "2 statements while down, caught up in 3 rounds, full_rebuilds {} \
                 (the Full/Chained views rebuild by design, F5)",
                counter(&d, "full_rebuilds")
            ));
            c.disconnect().await.ok();
        }
        // The `reg_down_target_keeps_its_keys` set (`regressions/availability.rs:262`): a target
        // that refuses writes keeps its keys past the budget and catches up from them, and is
        // never rebuilt. It needs the engine's own numbers, so the event runs on an engine of its
        // own with the reg's overrides (`regressions/availability.rs:3946`'s budget of one byte),
        // and the run continues on a fresh engine with the run's own defaults.
        Chaos::TargetDown => {
            let blocked = run
                .specs
                .iter()
                .find(|s| s.kind == Kind::Incremental)
                .map(|s| s.name.clone())
                .context("the neutral set has no incremental view to block")?;
            eng.stop().await?;
            let mut ev = run.start_engine_with(&[
                ("LAZYVIEW_SUPERVISOR", "off"),
                ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "1"),
            ])?;
            let mut c = run.src.conn().await?;
            wait_ready(&mut c, &mut ev, &names(run.specs), t_ready()).await?;
            let before = ev.snapshot();
            let mut block = run.src.conn().await?;
            let down = async {
                install_block(&mut block, &blocked).await?;
                // A target that refuses writes is only reported down when there are changes to
                // apply to it: five failed retries (`regressions/availability.rs:289`), which the
                // reg drives with its own inserts (`:283`) because nothing else writes. The event
                // pauses the paced writers for its check, so it writes them itself: two rows to
                // the table the blocked view reads, one more while the target is down.
                held_rows(run.src, 2).await?;
                wait_down(&mut block, &blocked, &mut ev).await?;
                held_rows(run.src, 1).await
            }
            .await;
            // The trigger comes down on every path: a soak must never leave a view blocked.
            let unblock = exec(&mut block, "DROP TRIGGER lv_soak_block").await;
            let problems_here = match (down, unblock) {
                (Ok(()), Ok(())) => {
                    wait_ready_tolerant(&mut c, &mut ev, &[blocked.as_str()], t_ready())
                        .await
                        .err()
                        .map(|e| format!("the blocked target never answered: {e:#}"))
                }
                (Err(e), _) => Some(format!("{e:#}")),
                (Ok(()), Err(e)) => Some(format!("the block could not be lifted: {e:#}")),
            };
            if let Some(p) = problems_here {
                problems.push(p);
            }
            // The F4 assertion set, on the engine that saw the outage.
            let d = counters(&ev, &before);
            let (catchups, over, rebuilds) = (
                counter(&d, "target_catchups"),
                counter(&d, "lag_over_budget"),
                counter(&d, "full_rebuilds"),
            );
            if catchups < 1 {
                problems.push(format!(
                    "the target did not catch up from the changes kept for it (target_catchups \
                     {catchups})"
                ));
            }
            if over != 1 {
                problems.push(format!(
                    "the kept changes did not pass the budget exactly once (lag_over_budget {over})"
                ));
            }
            // F5 on that engine: the blocked view is incremental, so a rebuild there is the
            // failure. `/stats` lists only what an engine has flushed itself (`engine.rs:230`), and
            // this engine lived for one outage, so the event reads its own view rather than the
            // whole table: the strict "flushed but not listed in /stats" rule of §4 stays with the
            // checks, which run on the run's engine after real traffic.
            let refreshers: Vec<String> = match ev.view_stats(run.src, &blocked) {
                None => {
                    problems.push(format!(
                        "{blocked} was never flushed on the engine that saw the outage"
                    ));
                    Vec::new()
                }
                Some(vs) if vs.whole_view_refreshes > 0 => {
                    vec![format!("{blocked}@source = {}", vs.whole_view_refreshes)]
                }
                Some(_) => Vec::new(),
            };
            if !refreshers.is_empty() {
                problems.push(format!(
                    "an incremental view was rebuilt instead of kept: {}",
                    refreshers.join(", ")
                ));
            }
            extra.push(format!(
                "blocked {blocked}: target_catchups {catchups}, lag_over_budget {over}, \
                 full_rebuilds {rebuilds} (F5: reported)"
            ));
            ev.stop().await?;
            eng = run.start_engine()?;
            let mut c = run.src.conn().await?;
            wait_ready(&mut c, &mut eng, &names(run.specs), t_ready()).await?;
            c.disconnect().await.ok();
        }
        // The container-level outage: the engine must reconnect and stay alive (the 1053-not-fatal
        // rule of `exclusive.rs:88`, the `CONN_GONE_CODES` of `src/engine.rs:176`), and the
        // writers retry through it (`workload.rs:1556 run_txn`). Long mode only: the smoke has no
        // containers of its own (D6), so it reports the event as skipped.
        Chaos::SourcePause => match soak_containers() {
            None => {
                extra.push(
                    "no LAZYVIEW_TEST_SOAK_CONTAINERS: the source stays up (D6: the smoke has no \
                     container of its own)"
                        .to_string(),
                );
            }
            Some((source, _)) => {
                docker_blocking(vec![
                    "stop".to_string(),
                    "-t".to_string(),
                    "5".to_string(),
                    source.clone(),
                ])
                .await?;
                docker_blocking(vec!["start".to_string(), source.clone()]).await?;
                wait_server_up(run.src).await?;
                eng.check_alive()?;
                // The stream reconnects on its own; the gauge says how long it was gone.
                wait_streaming(&mut eng).await?;
                let mut c = run.src.conn().await?;
                wait_ready_tolerant(&mut c, &mut eng, &names(run.specs), t_ready()).await?;
                c.disconnect().await.ok();
                extra.push(format!(
                    "stopped and started {source} (-t 5), the stream reconnected"
                ));
            }
        },
        // The shapes of `tests/ddl.rs:74`–`82`. What no view reads must not rebuild anything;
        // the widened column is a documented rebuild (F5) and is reported, not failed.
        Chaos::SourceDdl => {
            let before = eng.snapshot();
            let mut c = run.src.conn().await?;
            exec(&mut c, "ALTER TABLE items ADD COLUMN spare INT NULL").await?;
            exec(&mut c, "ALTER TABLE items DROP COLUMN spare").await?;
            exec(&mut c, "ANALYZE TABLE items").await?;
            let after_quiet = counters(&eng, &before);
            exec(
                &mut c,
                "ALTER TABLE entities MODIFY COLUMN kind VARCHAR(32) NULL",
            )
            .await?;
            exec(&mut c, "DROP INDEX k_kind ON entities").await.ok();
            exec(&mut c, "CREATE INDEX k_kind ON entities(kind)")
                .await
                .ok();
            wait_ready_tolerant(&mut c, &mut eng, &names(run.specs), t_ready()).await?;
            c.disconnect().await.ok();
            let all = counters(&eng, &before);
            let d = eng.snapshot().since(&before);
            if counter(&all, "plans_timed_out") != 0 {
                problems.push(format!(
                    "a definition took longer than LAZYVIEW_PLAN_TIMEOUT_MS to plan \
                     (plans_timed_out {})",
                    counter(&all, "plans_timed_out")
                ));
            }
            if d.definition_reloads > 0 {
                extra.push("a definition was reloaded".to_string());
            }
            extra.push(format!(
                "adds/drops/ANALYZE: full_rebuilds {} (must be 0 outside the documented cases, \
                 F5), widened `kind` and the index: full_rebuilds {} total, definition_reloads {}",
                counter(&after_quiet, "full_rebuilds"),
                counter(&all, "full_rebuilds"),
                d.definition_reloads
            ));
        }
    }

    // Every view must have been flushed on the engine that is about to be checked: `/stats` lists
    // only what an engine has flushed itself (`engine.rs:230`), and an event that restarted the
    // engine hands back one that has seen almost nothing. The writers keep going through an event
    // (§3), so this is a few seconds of traffic; a view that never appears is named here and the
    // check's own rule (`soakcheck.rs:585`) fails it.
    if let Err(e) = wait_flushed(&eng, run.src, run.specs).await {
        problems.push(format!("{e:#}"));
    }

    // Every event ends with the exactness check of §3 (§3's rule: "every event is followed by an
    // exactness check").
    let mut row = match quiesce_and_check(
        ctx,
        &mut eng,
        run.src,
        run.targets,
        run.specs,
        run.writers,
        &what,
    )
    .await
    {
        Ok(row) => row,
        Err(e) => CheckRow {
            check: Check::failed("check", format!("{what}: {e:#}")),
            views_checked: 0,
            oracle_rechecks: 0,
            counters: counters(&eng, &Snapshot::default()),
            whole_view_refreshes: 0,
            refreshers: Vec::new(),
            lag: Lag::default(),
            mem: MemRow::default(),
            warns: BTreeMap::new(),
            unexpected_warns: Vec::new(),
            views: Vec::new(),
        },
    };
    if !problems.is_empty() {
        let shown = format!("{} — {}", row.check.detail, problems.join("; "));
        row.check = Check::failed("check", shown);
    }
    if !extra.is_empty() {
        row.check.detail = format!("{}; {}", row.check.detail, extra.join("; "));
    }
    if let Some(p) = problems.first() {
        ctx.log(format!("chaos {what}: {p}"));
    }
    Ok((row, eng))
}

// ---------------------------------------------------------------------------------------------
// The event helpers
// ---------------------------------------------------------------------------------------------

/// Blocks one view's own table with a `BEFORE INSERT … SIGNAL` trigger — the shape of
/// `reg_down_target_keeps_its_keys` (`regressions/availability.rs:275`), which is what makes the
/// engine keep the changes for a target that is behind. [`wait_down`] reports it down; the caller
/// drops the trigger on every path.
async fn install_block(block: &mut Conn, view: &str) -> Result<()> {
    exec(
        block,
        &format!(
            "CREATE TRIGGER lv_soak_block BEFORE INSERT ON {view} FOR EACH ROW \
             SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'lazyview soak: the target refuses writes'"
        ),
    )
    .await
    .with_context(|| format!("the block of {view} could not be installed"))
}

/// Waits until the engine reports `view` down: the failed-retry loop of
/// `regressions/availability.rs:290`, the same 150 s window (five retries at the doubling backoff
/// of `engine.rs:359`, capped at 30 s).
async fn wait_down(block: &mut Conn, view: &str, eng: &mut Engine) -> Result<()> {
    let t = Instant::now();
    loop {
        eng.check_alive()?;
        let rows = view_state(block, view).await?;
        if rows.iter().any(|r| r.state == "error") {
            return Ok(());
        }
        ensure!(
            t.elapsed() < Duration::from_secs(150),
            "{view} was never reported down: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The ids of the event's own rows, above anything the paced writers reach: they count up from the
/// seed data (`soakwork.rs:507` `SoakIds`), while the soak writes at `LAZYVIEW_TEST_SOAK_TPS` for
/// at most 72 h (§5), which is orders of magnitude below `i64::from(i32::MAX)`.
const BLOCK_ID_BASE: i64 = 2_000_000_000;

/// Writes `n` rows the blocked view reads, so the target has something to refuse: the inserts of
/// `regressions/availability.rs:283` and `:302`, in `entities`, which `w_entities_active` selects
/// from (`soakwork.rs:206`). `state = 'new'` puts them in that view and `ON DUPLICATE KEY UPDATE`
/// keeps the ids reusable, so a second `target_down` event of the same run is the same shape.
async fn held_rows(db: &Db, n: u64) -> Result<()> {
    let mut c = db.conn().await?;
    for k in 1..=i64::try_from(n).unwrap_or(i64::MAX) {
        let id = BLOCK_ID_BASE + k;
        exec(
            &mut c,
            &format!(
                "INSERT INTO entities (id, grp, kind, amount, state, weight, created_at) \
                 VALUES ({id}, 7, 'held', 3.50, 'new', 1.0, '2024-01-05 10:00:00.000') \
                 ON DUPLICATE KEY UPDATE weight = COALESCE(weight, 0) + 1"
            ),
        )
        .await
        .with_context(|| format!("the row kept for the blocked target (id {id})"))?;
    }
    c.disconnect().await.ok();
    Ok(())
}

/// The highest position any row of `_lazyview_status` holds (the engine saves it every 50 ms with
/// the harness defaults, `regressions/lifecycle.rs:310`).
async fn saved_position(db: &Db) -> Result<Position> {
    let mut c = db.conn().await?;
    let rows = rows(
        &mut c,
        "SELECT CAST(binlog_file AS CHAR), CAST(binlog_pos AS CHAR) FROM _lazyview_status",
    )
    .await?;
    c.disconnect().await.ok();
    let mut best: Option<Position> = None;
    for r in rows {
        let (Some(file), Some(pos)) = (r[0].clone(), r[1].clone()) else {
            continue;
        };
        if file.is_empty() {
            continue;
        }
        let p = Position {
            file,
            pos: pos.parse().unwrap_or(0),
        };
        if best.as_ref().is_none_or(|b| p > *b) {
            best = Some(p);
        }
    }
    best.context("nothing is saved in _lazyview_status: the engine never streamed")
}

/// The first log line containing `needle` (`None` when the log has none): the XA check of D5.
fn grep_log(path: &PathBuf, needle: &str) -> Result<Option<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read engine log {}", path.display())),
    };
    Ok(text
        .lines()
        .find(|l| l.contains(needle))
        .map(|l| l.chars().take(200).collect()))
}

/// Waits until this engine has flushed every one of `specs` at least once, so `/stats` carries a
/// row for each (`Engine::view_stats`, `engine.rs:230`: a view is listed only after a flush listed
/// it). The writers keep going through an event (§3), which is what makes them flush; the names
/// that stay missing are the message, and the check's own rule (`soakcheck.rs:585`) fails them.
async fn wait_flushed(eng: &Engine, src: &Db, specs: &[ViewSpec]) -> Result<()> {
    let missing = || -> Vec<String> {
        specs
            .iter()
            .filter(|s| eng.view_stats(src, &s.name).is_none())
            .map(|s| s.name.clone())
            .collect()
    };
    let out: Vec<String> = wait_until("every view flushed on this engine", t_catchup(), || async {
        Ok(missing().is_empty())
    })
    .await
    .map(|_| Vec::new())
    .unwrap_or_else(|_| missing());
    ensure!(
        out.is_empty(),
        "never flushed on this engine: {}",
        out.join(", ")
    );
    Ok(())
}

/// Waits until the engine is connected to the binlog stream again (`streaming == 1`,
/// `src/metrics.rs:97`).
async fn wait_streaming(eng: &mut Engine) -> Result<()> {
    let m = eng.metrics.clone();
    wait_until("the engine streaming again", t_catchup(), || async {
        Ok(m.gauges.streaming.load(Ordering::Relaxed) == 1)
    })
    .await
}

/// Waits until the source answers again after a `docker start` (the loop of
/// `exclusive.rs:50`).
async fn wait_server_up(db: &Db) -> Result<()> {
    let t = Instant::now();
    loop {
        if let Ok(mut c) = db.conn().await {
            if c.query_drop("SELECT 1").await.is_ok() {
                c.disconnect().await.ok();
                return Ok(());
            }
            c.disconnect().await.ok();
        }
        ensure!(
            t.elapsed() < Duration::from_secs(180),
            "the source did not answer within 180 s of `docker start`"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
