//! The soak harness's checks, samplers and one-page report: §4 of
//! `tmp/out/lvs-soak/breakdown.md` (PR S2). One scheduled check is the sequence the breakdown
//! gives — quiesce the writers, catch up, compare every (view, target) with its oracle, apply the
//! per-view rebuild rule, read the counter budget, sample lag and memory, scan the engine logs —
//! and the run's evidence is the `report.md` / `report.json` pair it writes.
//!
//! Everything here is a **caller** of what the harness already has (`check_views_targets`,
//! `Engine::wait_caught_up_rounds`, `Engine::view_stats`, `http_get`): no new checking logic, and
//! nothing that depends on which route a view got (§6), so the same code runs unchanged once the
//! dedup core serves the views. The report is written **before** the test returns, because
//! `finish_run` drops the `lvt_*` schemas and the engine log dir (F12): the run directory is what
//! survives the run.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

// ---------------------------------------------------------------------------------------------
// Knobs and the run directory (S2.2)
// ---------------------------------------------------------------------------------------------

/// `LAZYVIEW_TEST_SOAK_CHECK_S` (default 300 = the long run's interval; §5's smoke uses 60), in
/// the style of `mod.rs:89 scale()`.
pub fn soak_check_s() -> u64 {
    std::env::var("LAZYVIEW_TEST_SOAK_CHECK_S")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(300)
}

/// `LAZYVIEW_TEST_SOAK_RSS_SLOPE_MIB_H` (D7: 64 MiB/h over the second half of the run), in
/// [`slope_kib_per_hour`]'s KiB/hour units.
pub fn soak_rss_slope_mib_h() -> f64 {
    std::env::var("LAZYVIEW_TEST_SOAK_RSS_SLOPE_MIB_H")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(64.0)
}

/// The artifact directory of one run: `tmp/out/lvs-soak/runs/<utc>/` (§4). The report,
/// `samples.csv` and the copied engine logs all go here — the schema is gone after the run.
pub fn run_dir() -> PathBuf {
    repo_dir().join("tmp/out/lvs-soak/runs").join(utc_stamp())
}

/// `2026-11-12-113500` from [`SystemTime`] (`chrono` is not a dependency): a `date -u` stamp that
/// is also a legal CI artifact name — no `:` and no space.
pub fn utc_stamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let day = secs.rem_euclid(86_400);
    format!(
        "{y:04}-{m:02}-{d:02}-{:02}{:02}{:02}",
        day / 3600,
        (day / 60) % 60,
        day % 60
    )
}

/// Days since 1970-01-01 → `(year, month, day)` (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The engine version for the report header: the crate version, i.e. what `lazyview --version`
/// prints (`lazyview_build_info` carries the same string in `/metrics`).
pub fn engine_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

// ---------------------------------------------------------------------------------------------
// The report (S2.2)
// ---------------------------------------------------------------------------------------------

/// One row of the report. `pass = None` is a report-only row: the budgets of D7 are reported for
/// the first two long runs and gate only afterwards.
#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub pass: Option<bool>,
    pub detail: String,
}

impl Check {
    pub fn ok(name: &'static str, detail: impl Into<String>) -> Check {
        Check {
            name,
            pass: Some(true),
            detail: detail.into(),
        }
    }

    pub fn failed(name: &'static str, detail: impl Into<String>) -> Check {
        Check {
            name,
            pass: Some(false),
            detail: detail.into(),
        }
    }

    /// A row that only reports (a budget, a Full/Chained view's refreshes — F5, D7).
    pub fn reported(name: &'static str, detail: impl Into<String>) -> Check {
        Check {
            name,
            pass: None,
            detail: detail.into(),
        }
    }

    fn mark(&self) -> &'static str {
        match self.pass {
            Some(true) => "PASS",
            Some(false) => "FAIL",
            None => "note",
        }
    }
}

/// One `(view, target)` row of `_lazyview_state` (the columns `src/meta.rs:308` writes) paired
/// with that view's `/stats` numbers. This table *is* the report's per-view section: the exactness
/// rule keys on `mode` (F5), the route is printed for the like-for-like diff after 6T (§6).
#[derive(Debug, Clone, Default)]
pub struct ViewRow {
    pub name: String,
    pub target: String,
    /// `_lazyview_state.mode`: `incremental`, `full`, …
    pub mode: String,
    /// `_lazyview_state.route`: `dedup | viewplan | recompute | full`.
    pub route: String,
    pub whole_view_refreshes: u64,
    /// `last_whole_view_refresh` of `/stats` (`src/stats.rs:337`): the reason and unix time of the
    /// last whole-view refresh. The F5 rule reads it (a refresh the engine explains with a
    /// documented reason no chaos event owns is not a failure), and the report keeps it: it is
    /// in-memory per engine start, so it is unrecoverable once the run is over.
    pub last_whole_view_refresh: Option<(String, u64)>,
    pub ids_pending: u64,
    pub cascade_cached_rows: u64,
}

/// One lag sample (§4): the source's binlog end, how long `applied()` took to reach it, and the
/// binlog depth that was outstanding (`common::file_seq`).
#[derive(Debug, Clone, Default)]
pub struct Lag {
    /// Seconds between reading the source's binlog end and `applied()` reaching it.
    pub settle_s: f64,
    /// The position the sample waited for.
    pub target: String,
    /// The applied position afterwards.
    pub applied: String,
    pub file_seq_diff: u64,
    pub pos_diff: u64,
}

/// One memory sample (§4): `VmRSS` plus the key-buffer gauges the checks watch.
#[derive(Debug, Clone, Default)]
pub struct MemRow {
    pub rss_kib: u64,
    /// `/metrics` gauge `lazyview_inflight_rows` (`src/metrics.rs:105`).
    pub inflight_rows: u64,
    /// Sum of the per-view `ids_pending` (`src/stats.rs:308`).
    pub ids_pending: u64,
    /// Sum of the per-view `cascade_cached_rows` (`src/stats.rs:321`).
    pub cascade_cached_rows: u64,
    /// `(view, ids_pending, cascade_cached_rows)` per spec.
    pub per_view: Vec<(String, u64, u64)>,
    /// Views with no `/stats` entry yet: their numbers are missing, not zero.
    pub unlisted: Vec<String>,
    /// `lazyview_applied_position_age_seconds`, when the health endpoint answered.
    pub applied_position_age_s: Option<f64>,
    /// `lazyview_binlog_disconnected_seconds`, when the health endpoint answered.
    pub binlog_disconnected_s: Option<f64>,
}

/// What one scheduled check measured — the numbers the report's check row carries (§4).
#[derive(Debug, Clone)]
pub struct CheckRow {
    /// The report row of this check. Its `name` is `"check"`; the `what` of the call is the first
    /// words of its detail (chaos events name themselves, S3.3).
    pub check: Check,
    pub views_checked: usize,
    /// Extra oracle sweeps a first mismatch needed before it settled: verify's re-check idea
    /// separating "wrong" from "`applied()` reported too early" (`mod.rs:1013`–`1040`).
    pub oracle_rechecks: usize,
    /// The budget table over the run so far (for the window between two checks keep a [`Window`]).
    pub counters: Vec<(&'static str, u64)>,
    pub whole_view_refreshes: u64,
    /// Incremental views that did a whole-view refresh (F5/D5: a failure, never allowlisted).
    pub refreshers: Vec<String>,
    pub lag: Lag,
    pub mem: MemRow,
    pub warns: BTreeMap<String, u64>,
    pub unexpected_warns: Vec<String>,
    pub views: Vec<ViewRow>,
}

/// The one-page report of a soak run (§4). Written at the end **and on every failure path** before
/// the test returns, because `finish_run` drops the schemas and the log dir (F12).
#[derive(Debug, Clone, Default)]
pub struct Report {
    pub mode: String,
    pub flavor: String,
    pub version: String,
    pub seed: u64,
    /// Redacted with [`redact`] when built: the report is an artifact that gets uploaded.
    pub dsn: String,
    pub started_utc: String,
    pub checks: Vec<Check>,
    /// The chaos events with their times and outcomes (S3.3 fills this in).
    pub events: Vec<Check>,
    pub views: Vec<ViewRow>,
    /// Settle seconds of every check → p50/p95/max.
    pub lag_secs: Vec<f64>,
    /// `(seconds since the run started, VmRSS KiB)`; the slope check uses the second half.
    pub rss: Vec<(f64, u64)>,
    pub warns: BTreeMap<String, u64>,
    pub counters: Vec<(&'static str, u64)>,
    pub txns: u64,
    pub ops: BTreeMap<&'static str, u64>,
    /// Retries of XA branches the server had rolled back (`workload.rs` `run_xa_txn`, Q1), set
    /// by the run from `workload::XA_ROLLED_BACK_RETRIES` before writing the report.
    pub xa_rolled_back_retries: u64,
}

impl Report {
    /// How many WARN lines are the cascade cache's gap of issue #167 ([`CASCADE_GAP_WARN`]) —
    /// the chaos-expected line Q2 allowlisted, counted from the scanned buckets so the report
    /// says whether the run met it (a *divergence* is a failed oracle check, never this number).
    pub fn cascade_gap_warns(&self) -> u64 {
        self.warns
            .iter()
            .filter(|(k, _)| k.contains(CASCADE_GAP_WARN))
            .map(|(_, n)| *n)
            .sum()
    }

    /// A report header for one run: the DSN goes in redacted, the version is the crate's.
    pub fn new(mode: &str, seed: u64, dsn: &str, flavor: &str) -> Report {
        Report {
            mode: mode.to_string(),
            flavor: flavor.to_string(),
            version: engine_version(),
            seed,
            dsn: redact(dsn),
            started_utc: utc_stamp(),
            ..Default::default()
        }
    }

    /// The rows that failed (`Some(false)`); empty means the run passed.
    pub fn failed(&self) -> Vec<&Check> {
        self.checks
            .iter()
            .filter(|c| c.pass == Some(false))
            .collect()
    }

    /// `ALL_OK` / `FAILED <check>` — the one-line `DONE` style of `tmp/lvs-suite.sh`.
    pub fn done_line(&self) -> String {
        match self.failed().first() {
            None => "ALL_OK".to_string(),
            Some(c) => format!("FAILED {}", c.name),
        }
    }

    /// Writes the one-page `report.md` and the machine-readable `report.json` into `dir` (created).
    pub fn write(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        std::fs::write(dir.join("report.md"), self.markdown())
            .with_context(|| format!("write {}/report.md", dir.display()))?;
        let json =
            serde_json::to_string_pretty(&self.value()).context("serialize report.json")? + "\n";
        std::fs::write(dir.join("report.json"), json)
            .with_context(|| format!("write {}/report.json", dir.display()))?;
        Ok(())
    }

    fn markdown(&self) -> String {
        let (p50, p95, max) = percentiles(&self.lag_secs);
        let mut s = format!(
            "# soak {} on {} — {}\n\n",
            self.mode, self.flavor, self.started_utc
        );
        let _ = writeln!(s, "- engine: {}", self.version);
        let _ = writeln!(
            s,
            "- seed: {} · mode: {} · servers: {}",
            self.seed, self.mode, self.dsn
        );
        let _ = writeln!(
            s,
            "- transactions: {} · ops: {}",
            self.txns,
            fmt_counts(self.ops.iter().map(|(k, v)| (*k, *v)))
        );
        let _ = writeln!(
            s,
            "- lag s over {} checks: p50={p50:.2} p95={p95:.2} max={max:.2}",
            self.lag_secs.len()
        );
        let _ = writeln!(
            s,
            "- RSS KiB: start={} end={} slope={:.0} KiB/h over {} samples (budget {} KiB/h)",
            self.rss.first().map_or(0, |x| x.1),
            self.rss.last().map_or(0, |x| x.1),
            slope_kib_per_hour(&self.rss),
            self.rss.len(),
            soak_rss_slope_mib_h() * 1024.0
        );
        let _ = writeln!(
            s,
            "- counters: {}",
            fmt_counts(self.counters.iter().copied())
        );
        let _ = writeln!(
            s,
            "- xa branch retries: {} · cascade-gap WARN lines: {} (issue #167; an oracle \
             divergence is a failed check, not this count)",
            self.xa_rolled_back_retries,
            self.cascade_gap_warns(),
        );
        s.push_str("\n## checks\n\n| | check | detail |\n|---|---|---|\n");
        for c in &self.checks {
            let _ = writeln!(s, "| {} | {} | {} |", c.mark(), c.name, one_line(&c.detail));
        }
        s.push_str("\n## chaos events\n\n| | event | detail |\n|---|---|---|\n");
        for c in &self.events {
            let _ = writeln!(s, "| {} | {} | {} |", c.mark(), c.name, one_line(&c.detail));
        }
        s.push_str(
            "\n## views\n\n| view | target | mode | route | whole_view_refreshes | ids_pending \
             | cascade_cached_rows |\n|---|---|---|---|---|---|---|\n",
        );
        for v in &self.views {
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {} | {} | {} |",
                v.name,
                v.target,
                v.mode,
                v.route,
                v.whole_view_refreshes,
                v.ids_pending,
                v.cascade_cached_rows
            );
        }
        s.push_str("\n## WARN buckets\n\n| count | message (first 60 chars) |\n|---|---|\n");
        for (k, n) in &self.warns {
            let _ = writeln!(s, "| {n} | {} |", one_line(k));
        }
        s
    }

    fn value(&self) -> serde_json::Value {
        let (p50, p95, max) = percentiles(&self.lag_secs);
        serde_json::json!({
            "mode": self.mode,
            "flavor": self.flavor,
            "version": self.version,
            "seed": self.seed,
            "dsn": self.dsn,
            "started_utc": self.started_utc,
            "txns": self.txns,
            "ops": self.ops,
            "counters": self.counters.iter().cloned().collect::<BTreeMap<_, _>>(),
            "lag": { "n": self.lag_secs.len(), "p50": p50, "p95": p95, "max": max },
            "rss": {
                "start_kib": self.rss.first().map_or(0, |x| x.1),
                "end_kib": self.rss.last().map_or(0, |x| x.1),
                "slope_kib_per_hour": slope_kib_per_hour(&self.rss),
                "samples": self.rss.len(),
            },
            "warns": self.warns,
            "xa_rolled_back_retries": self.xa_rolled_back_retries,
            "cascade_gap_warns": self.cascade_gap_warns(),
            "checks": self.checks.iter().map(|c| serde_json::json!({
                "name": c.name, "pass": c.pass, "detail": c.detail,
            })).collect::<Vec<_>>(),
            "events": self.events.iter().map(|c| serde_json::json!({
                "name": c.name, "pass": c.pass, "detail": c.detail,
            })).collect::<Vec<_>>(),
            "views": self.views.iter().map(|v| serde_json::json!({
                "name": v.name, "target": v.target, "mode": v.mode, "route": v.route,
                "whole_view_refreshes": v.whole_view_refreshes,
                "last_whole_view_refresh": v.last_whole_view_refresh,
                "ids_pending": v.ids_pending,
                "cascade_cached_rows": v.cascade_cached_rows,
            })).collect::<Vec<_>>(),
        })
    }
}

fn fmt_counts<'a>(pairs: impl IntoIterator<Item = (&'a str, u64)>) -> String {
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A check detail as one markdown-table cell.
fn one_line(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

// ---------------------------------------------------------------------------------------------
// The scheduled check (S2.3)
// ---------------------------------------------------------------------------------------------

/// Flips every writer's stop watch (the `run_until_stopped` shape, `workload.rs:1635`); the caller
/// owns the writer tasks and joins them around a check.
pub fn set_writers(writers: &[watch::Sender<bool>], stop: bool) {
    for w in writers {
        let _ = w.send(stop);
    }
}

/// The whole-view refresh reasons of `/stats` (`last_whole_view_refresh.reason`, the four
/// strings of `src/engine.rs:2512-2520`) that the docs explain for an **incremental** view and no
/// chaos event of the soak can own, so the F5 rule of [`check_once`] tolerates them beyond the
/// ledger:
///
/// * `affected ids unknown (cascading foreign key, lookup overflow or untraced change)` — a change
///   a cascading FK made inside the storage engine, an id lookup that overflowed, or a change the
///   analyzer cannot trace: the rows the view must recompute are unknown, so the whole table is
///   rebuilt ("the same limit applies to incremental views that need a rebuild because the affected
///   rows are unknown: a cascading foreign key", `docs/configuration.md:128`, and the cascade
///   section of `docs/views.md`). The paced writers delete parent rows at any time, so no event
///   owns these.
/// * `build on a target that was down` — a target that came back is rebuilt as a whole
///   (`docs/operations.md:248`, "a target rebuilt after it was down"); the `TargetDown` event has
///   restarted the engine by then, and a restart resets the per-view counters and empties the
///   ledger, so that event cannot have approved it.
///
/// The engine's other two strings are not accepted here on their own, so the rule is not loosened
/// beyond what `docs/views.md:433-436` documents: `full-mode view: the definition is not
/// maintained incrementally` cannot happen to a view whose `mode` is `incremental` at all, and
/// `new version: definition, targets or source schema changed, or rebuild requested` is a rebuild
/// *some event caused* — it passes only through [`WAVE_REASONS`], where an event actually owns it,
/// or through the bar the event recorded in the ledger. A refresh no event owns and no reason here
/// explains still FAILS (D5), and `tests/soak_widen.rs` fails if the engine renames one of the
/// strings below.
pub const UNOWNED_REASONS: [&str; 2] = [
    "affected ids unknown (cascading foreign key, lookup overflow or untraced change)",
    "build on a target that was down",
];

/// The one reason of `src/engine.rs:2510-2516` that explains a whole-view refresh the run's own
/// `SourceDdl` event causes, so [`Ledger::in_wave`] accepts it — for a view that event *owns*
/// (its plan, or a cascading-FK parent of one, is in the DDL's affected set) and only while that
/// event's rebuild wave is open:
///
/// * `new version: definition, targets or source schema changed, or rebuild requested` — the
///   engine bumps the view's version when the source schema it planned against changed, and the
///   version bump rebuilds the whole table. `docs/views.md:433` documents this for `ALTER TABLE`
///   of a source table, including the case that matters here: "…and so does a view whose plan may
///   already include later changes of the table (**after a restart or while catching up across
///   several DDLs**)", which is exactly the event's three statements on `items` and `entities`
///   arriving in one burst behind a restart earlier in the run. `docs/views.md:436` adds the
///   chained view whose upstream table was swapped in the same wave.
///
/// Unlike [`UNOWNED_REASONS`] these are never tolerated on their own: outside the event's owned
/// set, or after its wave closed, the same string is the failure the CI soak smoke of 2026-11-16
/// named (`w_chain_a@source = 1`, last reason `new version: …`), and it stays a failure.
pub const WAVE_REASONS: [&str; 1] =
    ["new version: definition, targets or source schema changed, or rebuild requested"];

/// `20261116`-style seconds since the epoch, the clock `last_whole_view_refresh.at` is written in
/// (`src/stats.rs:337`, `src/engine.rs:2536`), so a wave window and a refresh time are comparable.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The rebuild wave of one `SourceDdl` event: the views its DDL owns, and the wall-clock window in
/// which that event's rebuilds can still land. Published by `soakchaos::run_event` when the burst
/// fires (bounded by the ledger's own budget) and narrowed when that ledger has seen the wave end,
/// so a *scheduled* check that runs beside a live event does not hold a view to a bar the ledger has
/// not read yet — the f1 class, where `w_chain_a`'s dependent rebuild landed 40 s after the burst,
/// while the event was still reading its own numbers.
#[derive(Debug, Clone)]
pub struct DdlWave {
    /// The `view@target` keys of the views the event's DDL may rebuild: `soakchaos::plan_stale_of`
    /// (the `occ` tables **and the cascading-FK parents** of each plan, `src/engine.rs:3800-3807`,
    /// plus chains). A view outside this set is not the event's, whatever its reason says.
    pub owned: HashSet<String>,
    /// The unix seconds the burst fired at.
    pub since: u64,
    /// The unix seconds until which the event's wave may still land.
    pub until: u64,
}

/// The approved-rebuild ledger of the per-view F5 rule (S3.3/Q3): what the chaos events of a run
/// have accounted for on the current engine, so a check can tell a rebuild an event owns from one
/// nothing explains. Emptied whenever the run starts a fresh engine, whose `/stats` counters start
/// at zero (`ChaosRun::start_engine_with`).
#[derive(Debug, Default)]
pub struct Ledger {
    /// `view@target` → the cumulative `whole_view_refreshes` an event has accounted for. A check
    /// tolerates a count up to this bar and no higher.
    bars: HashMap<String, u64>,
    /// The wave of the `SourceDdl` event in flight, if one is.
    wave: Option<DdlWave>,
}

impl Ledger {
    /// The bar a check holds `key` to: the count the events of the run have accounted for.
    pub fn bar(&self, key: &str) -> u64 {
        self.bars.get(key).copied().unwrap_or(0)
    }

    /// An event accounted for `count` whole-view refreshes of `key`: the bar never goes down.
    pub fn approve(&mut self, key: &str, count: u64) {
        let e = self.bars.entry(key.to_string()).or_default();
        *e = (*e).max(count);
    }

    /// A fresh engine: its counters start at zero, so every approval earned against the old ones
    /// is meaningless, and so is any wave the old engine was still landing.
    pub fn clear(&mut self) {
        self.bars.clear();
        self.wave = None;
    }

    /// The `SourceDdl` event's burst has fired at `since`: from now, a documented whole-view
    /// refresh of a view in `owned` is attributable to it, until `until` (the caller's bound: the
    /// ledger's own budget) or until [`Self::close_wave`] confirms the wave ended, whichever is
    /// sooner.
    pub fn open_wave(&mut self, owned: HashSet<String>, since: u64, until: u64) {
        self.wave = Some(DdlWave {
            owned,
            since,
            until,
        });
    }

    /// The event's ledger has re-read its bars until the wave stopped moving: from now, a refresh
    /// beyond a bar is the event's for `tail` more seconds (the race between the ledger's last
    /// read and the check that follows it) and nobody's after that.
    pub fn close_wave(&mut self, now: u64, tail: u64) {
        if let Some(w) = self.wave.as_mut() {
            w.until = w.until.min(now + tail);
        }
    }

    /// Does the event in flight own a whole-view refresh of `key` that happened at `at`, as read
    /// at `now`? The refresh must post-date the burst (`at >= since`: a reason left over from an
    /// earlier cause is not this event's) and be read while the window is open. A closed wave
    /// owns nothing past its tail.
    pub fn in_wave(&self, key: &str, at: u64, now: u64) -> bool {
        match self.wave.as_ref() {
            Some(w) => w.owned.contains(key) && at >= w.since && now <= w.until && now >= w.since,
            None => false,
        }
    }

    /// The F5 verdict for one incremental view: does its counter pass? `Ok` says why (an empty
    /// `String` is the ordinary at-or-below-the-bar pass); `Err` names the refresh for the report.
    /// A beyond-bar refresh passes only where the engine explains it with a reason the docs
    /// document — one no event can own ([`UNOWNED_REASONS`]), or one the event in flight owns
    /// ([`WAVE_REASONS`], and then the ledger adopts it: the bar rises to the count the event
    /// owns, so the same refresh is not judged again by the next check).
    pub fn judge(
        &mut self,
        key: &str,
        count: u64,
        last: Option<&(String, u64)>,
        now: u64,
    ) -> std::result::Result<String, String> {
        let bar = self.bar(key);
        if count <= bar {
            return Ok(String::new());
        }
        let counted = format!("{key} = {count}");
        match last {
            Some((reason, _)) if UNOWNED_REASONS.contains(&reason.as_str()) => {
                Ok(format!("{counted} ({reason})"))
            }
            Some((reason, at))
                if WAVE_REASONS.contains(&reason.as_str()) && self.in_wave(key, *at, now) =>
            {
                self.approve(key, count);
                Ok(format!("{counted} ({reason}, the source-ddl wave)"))
            }
            other => Err(match other {
                Some((reason, _)) => format!("{counted} (last reason: {reason})"),
                None => counted,
            }),
        }
    }
}

/// One check of §4, in the order the breakdown lists them: pause the writers (and let the rows they
/// had in flight be written), `wait_caught_up_rounds(_, 3)` (`engine.rs:205`), `check_views_targets`
/// (`oracle.rs:362`), the per-view rebuild rule (F5), the counter budget, the lag sample, the memory
/// sample, the engine-log scan — then the writers resume. They resume on the failure path too: a
/// failed check must never leave the load stopped.
///
/// A check fails on an oracle mismatch, on an incremental view with a whole-view refresh (D5: the
/// only cases that ever pass are a count an event recorded in the ledger, a refresh the engine
/// explains with one of [`UNOWNED_REASONS`], and a refresh of a view the `SourceDdl` event in
/// flight owns while its wave is open — [`Ledger::judge`]), on a WARN outside [`EXPECTED_WARNS`],
/// on an `ERROR`/panic line, and on any hard-budget counter rising. `full_rebuilds`,
/// `target_catchups` and `lag_over_budget` are not checked here: they have the documented cases of
/// F5 and are what a target-down event must produce (S3.3 asserts those itself).
///
/// `ledger` is what the chaos events of the run have accounted for on this engine (S3.3/Q3,
/// [`Ledger`]): the bars a source-ddl event approved, plus the wave of the event in flight. A view
/// above its bar and outside both of those tolerations fails like an unapproved one; a fresh
/// engine empties the ledger, whose bars were earned against counters that no longer exist.
///
/// The counter table is measured against [`Snapshot::default()`], i.e. since the engine started;
/// between two consecutive checks the caller keeps a [`Window`] and calls [`counters`].
#[allow(clippy::too_many_arguments)] // the check's whole context, like `soak_run` (S3.4)
pub async fn quiesce_and_check(
    ctx: &Ctx,
    eng: &mut Engine,
    src: &Db,
    targets: &HashMap<String, Db>,
    specs: &[ViewSpec],
    writers: &[watch::Sender<bool>],
    ledger: &mut Ledger,
    what: &str,
) -> Result<CheckRow> {
    set_writers(writers, true);
    let out = check_once(ctx, eng, src, targets, specs, ledger, what).await;
    set_writers(writers, false);
    out
}

async fn check_once(
    ctx: &Ctx,
    eng: &mut Engine,
    src: &Db,
    targets: &HashMap<String, Db>,
    specs: &[ViewSpec],
    ledger: &mut Ledger,
    what: &str,
) -> Result<CheckRow> {
    let t = Instant::now();
    // (1) quiesce: the writers are stopped; wait out the rows already in flight. Soft: a writer
    // inside a transaction finishes it, and `inflight_rows` is the engine's own gauge for that.
    let _ = wait_until(
        "in-flight rows to drain",
        Duration::from_secs(30),
        || async { Ok(eng.metrics.gauges.inflight_rows.load(Ordering::Relaxed) == 0) },
    )
    .await;
    let mut sc = src.conn().await?;
    // (2) catch-up: three rounds, one per chained level plus margin (`engine.rs:205`).
    eng.wait_caught_up_rounds(&mut sc, t_catchup(), 3)
        .await
        .with_context(|| format!("'{what}': catch-up before the oracle sweep"))?;
    eng.check_alive()?;
    // (3) every (view, target) against its oracle, both reads in one consistent snapshot (F1).
    // verify's re-check idea (`mod.rs:1013`–`1040`): poll before failing, so "applied() reported
    // too early" is told apart from "wrong"; both are recorded, only the second one fails.
    let mut rechecks = 0usize;
    if let Err(first) = check_views_targets(src, targets, specs).await {
        rechecks = 1;
        let again = wait_until("the oracle to match", Duration::from_secs(3), || async {
            Ok(check_views_targets(src, targets, specs).await.is_ok())
        })
        .await;
        if let Err(late) = again {
            return Err(first).with_context(|| {
                format!("'{what}': the oracle still disagreed on the re-check ({late:#})")
            });
        }
    }
    // (4) the per-view rebuild rule (F5): `_lazyview_state.mode` × `whole_view_refreshes`, and the
    // reason of the last refresh (`last_whole_view_refresh`, `src/stats.rs:337`). The per-view
    // counter is cumulative since the engine started, so a chaos event that legitimately caused a
    // rebuild raises that view's bar in the ledger to the count the event explained — every refresh
    // *beyond* what an event accounted for, and everything on a view no event touched, still fails
    // and is named (D5). Beyond the bar a refresh passes only where [`Ledger::judge`] says so: a
    // reason no event of this run can own ([`UNOWNED_REASONS`]), or a reason the `SourceDdl` event
    // in flight owns for this view while its wave is open ([`WAVE_REASONS`]). A `new version: …`
    // no event owns, or a moved counter with no refresh behind it to read, is named and fails.
    let views = view_rows(eng, src, specs).await?;
    let now = unix_now();
    let mut refreshers: Vec<String> = Vec::new();
    let mut explained: Vec<String> = Vec::new();
    for v in views.iter() {
        if !v.mode.eq_ignore_ascii_case("incremental") {
            continue; // the Full/Chained views rebuild by design (F5)
        }
        let key = format!("{}@{}", v.name, v.target);
        match ledger.judge(
            &key,
            v.whole_view_refreshes,
            v.last_whole_view_refresh.as_ref(),
            now,
        ) {
            Ok(note) => {
                if !note.is_empty() {
                    explained.push(note);
                }
            }
            Err(named) => refreshers.push(named),
        }
    }
    let whole_view_refreshes: u64 = views.iter().map(|v| v.whole_view_refreshes).sum();
    // (5) the counter budget (§4).
    let counters = counters(eng, &Snapshot::default());
    // (6) lag, (7) memory, (8) the log scan (this engine's current log; the soak keeps every
    // start's path in a Vec and scans them all, S3.4).
    let lag = lag_sample(&mut sc, eng).await?;
    let addr = eng
        .cfg
        .health_addr
        .map(|a| a.to_string())
        .unwrap_or_default();
    let mem = mem_sample(eng, src, specs, &addr).await?;
    let warns = scan_warns(std::slice::from_ref(&eng.log_path))?;
    let unexpected_warns: Vec<String> = unexpected_warns(&warns).into_iter().cloned().collect();
    sc.disconnect().await.ok();

    let mut problems: Vec<String> = Vec::new();
    problems.extend(
        refreshers
            .iter()
            .map(|v| format!("incremental view did a whole-view refresh: {v}")),
    );
    problems.extend(budget_problems(&counters));
    problems.extend(
        unexpected_warns
            .iter()
            .map(|w| format!("unexpected WARN: {w}")),
    );
    let explained_note = if explained.is_empty() {
        String::new()
    } else {
        format!(
            ", beyond the ledger but explained by its documented reason: {}",
            explained.join(", ")
        )
    };
    let detail = format!(
        "{what}: {} view rows, {} recheck(s), full_rebuilds {}, whole_view_refreshes {}{}, \
         lag {:.2}s, RSS {} KiB, inflight {}, {} WARN buckets ({} unexpected), {} ms",
        views.len(),
        rechecks,
        counter(&counters, "full_rebuilds"),
        whole_view_refreshes,
        explained_note,
        lag.settle_s,
        mem.rss_kib,
        mem.inflight_rows,
        warns.len(),
        unexpected_warns.len(),
        t.elapsed().as_millis(),
    );
    let shown = if problems.is_empty() {
        detail.clone()
    } else {
        format!("{detail} — {}", problems.join("; "))
    };
    ctx.log(format!("check '{what}': {shown}"));
    let check = if problems.is_empty() {
        Check::ok("check", detail)
    } else {
        Check::failed("check", shown)
    };
    let row = CheckRow {
        check,
        views_checked: views.len(),
        oracle_rechecks: rechecks,
        counters,
        whole_view_refreshes,
        refreshers,
        lag,
        mem,
        warns,
        unexpected_warns,
        views,
    };
    if problems.is_empty() {
        Ok(row)
    } else {
        Err(anyhow!("'{what}': {}", problems.join("; ")))
    }
}

/// The value of one counter in a [`counters`] table (0 when the table does not carry it).
pub fn counter(counters: &[(&'static str, u64)], name: &str) -> u64 {
    counters
        .iter()
        .find(|(n, _)| *n == name)
        .map_or(0, |(_, v)| *v)
}

/// Every `(view, target)` row of `_lazyview_state` (the columns of `src/meta.rs:308`) paired with
/// the view's `/stats` numbers. A view that was flushed but is not listed in `/stats` is a STOP
/// condition of the breakdown, so it is named together with `batches_dropped` — the number that
/// makes `/stats` counters low (`docs/operations.md:181`).
pub async fn view_rows(eng: &Engine, src: &Db, specs: &[ViewSpec]) -> Result<Vec<ViewRow>> {
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    let mut c = src.conn().await?;
    let state = rows(
        &mut c,
        "SELECT CAST(name AS CHAR), CAST(target AS CHAR), COALESCE(CAST(mode AS CHAR), ''), \
         COALESCE(CAST(route AS CHAR), '') FROM _lazyview_state ORDER BY name, target",
    )
    .await?;
    c.disconnect().await.ok();
    let dropped = eng.metrics.view_stats.snapshot().batches_dropped;
    let mut out = Vec::new();
    for r in state {
        let (Some(name), Some(target)) = (r[0].clone(), r[1].clone()) else {
            continue;
        };
        if !names.contains(&name.as_str()) {
            continue;
        }
        let Some(vs) = eng.view_stats(src, &name) else {
            bail!("view `{name}`@{target} was flushed but is not listed in /stats (batches_dropped {dropped})");
        };
        out.push(ViewRow {
            name,
            target,
            mode: r[2].clone().unwrap_or_default(),
            route: r[3].clone().unwrap_or_default(),
            whole_view_refreshes: vs.whole_view_refreshes,
            last_whole_view_refresh: vs.last_whole_view_refresh.clone(),
            ids_pending: vs.ids_pending,
            cascade_cached_rows: vs.cascade_cached_rows,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// The counter window (S2.4)
// ---------------------------------------------------------------------------------------------

/// The counter window of one check: the snapshot taken when the previous check finished, so the
/// budget table covers exactly that window (§4). `Window::now` opens one, `Window::roll` reads the
/// delta and reopens it.
#[derive(Debug, Clone)]
pub struct Window(pub Snapshot);

impl Window {
    /// A window opening at the engine's counters right now.
    pub fn now(eng: &Engine) -> Window {
        Window(eng.snapshot())
    }

    /// The budget table since this window opened, and the same window reopened at today's values.
    pub fn roll(&self, eng: &Engine) -> (Vec<(&'static str, u64)>, Window) {
        (counters(eng, &self.0), Window::now(eng))
    }
}

/// The counters of the budget table of §4 (`src/metrics.rs:47`–`86`), as `self - since` pairs.
pub fn counters(eng: &Engine, since: &Snapshot) -> Vec<(&'static str, u64)> {
    let d = eng.snapshot().since(since);
    vec![
        ("full_rebuilds", d.full_rebuilds),
        ("refresh_errors", d.refresh_errors),
        ("plans_timed_out", d.plans_timed_out),
        ("binlog_backpressure", d.binlog_backpressure),
        ("lag_over_budget", d.lag_over_budget),
        ("target_catchups", d.target_catchups),
        ("supervisor_zombie_kills", d.supervisor_zombie_kills),
    ]
}

/// The counters that must never rise in a soak. `full_rebuilds` has the documented cases of F5
/// (first builds, `Full`/`Chained` refreshes, the XA line), and `target_catchups` / `lag_over_budget`
/// are what a target-down event has to produce (`regressions.rs:3937`), so their own event asserts
/// them; these four have no documented case at all.
pub const HARD_BUDGET: &[&str] = &[
    "refresh_errors",
    "plans_timed_out",
    "binlog_backpressure",
    "supervisor_zombie_kills",
];

/// The hard-budget violations in a [`counters`] table (empty = nothing rose).
pub fn budget_problems(counters: &[(&'static str, u64)]) -> Vec<String> {
    HARD_BUDGET
        .iter()
        .filter_map(|n| {
            counters
                .iter()
                .find(|(c, _)| c == n)
                .filter(|(_, v)| *v > 0)
                .map(|(n, v)| format!("counter {n} rose to {v}"))
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The lag sampler (S2.5)
// ---------------------------------------------------------------------------------------------

/// One lag sample (§4): `server_position` (`mod.rs:355`), then `wait_applied` (`engine.rs:181`),
/// with the binlog depth of what was outstanding as the secondary number (`common::file_seq`).
pub async fn lag_sample(src: &mut Conn, eng: &mut Engine) -> Result<Lag> {
    let target = server_position(src).await?;
    let t = Instant::now();
    eng.wait_applied(&target, t_catchup())
        .await
        .context("lag sample: engine did not apply the source's binlog end")?;
    let settle_s = t.elapsed().as_secs_f64();
    let applied = eng.applied().unwrap_or_else(|| target.clone());
    Ok(Lag {
        settle_s,
        target: target.to_string(),
        applied: applied.to_string(),
        file_seq_diff: file_seq(&target.file).saturating_sub(file_seq(&applied.file)),
        pos_diff: target.pos.saturating_sub(applied.pos),
    })
}

/// p50 / p95 / max, nearest rank (an empty slice is all zeroes).
pub fn percentiles(values: &[f64]) -> (f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut s = values.to_vec();
    s.sort_by(f64::total_cmp);
    let n = s.len();
    let rank = |q: f64| {
        let i = (q * n as f64).ceil() as usize;
        s[i.clamp(1, n) - 1]
    };
    (rank(0.5), rank(0.95), s[n - 1])
}

/// Appends lag rows to `samples.csv` in the run directory (header on the first row), so a run that
/// is still going — or one that dies — has its samples on disk (§4).
pub fn append_samples(dir: &Path, rows: &[Lag]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join("samples.csv");
    let fresh = !path.exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    let mut chunk = String::new();
    if fresh {
        writeln!(chunk, "settle_s,target,applied,file_seq_diff,pos_diff")?;
    }
    for l in rows {
        writeln!(
            chunk,
            "{},{},{},{},{}",
            l.settle_s, l.target, l.applied, l.file_seq_diff, l.pos_diff
        )?;
    }
    f.write_all(chunk.as_bytes())?;
    f.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The memory sampler (S2.6)
// ---------------------------------------------------------------------------------------------

/// `VmRSS` of this process from `/proc/self/status`, in KiB. In-process the driver **and** the
/// engine are in this one number (F2); the driver's own footprint is small and bounded, so a growth
/// trend is still an engine-leak signal (D2).
pub fn vm_rss_kib() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status").context("read /proc/self/status")?;
    parse_vm_rss(&status).context("no `VmRSS:` line in /proc/self/status")
}

/// The `VmRSS:` value of a `/proc/self/status` text, in KiB (pure, so the selftest covers it).
fn parse_vm_rss(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|v| v.parse::<u64>().ok())
}

/// The least-squares slope of `(seconds, KiB)` samples, in KiB/hour (§4's no-growth rule; D7's
/// budget is [`soak_rss_slope_mib_h`] × 1024). Fewer than two samples, or samples at one instant,
/// cannot show a trend.
pub fn slope_kib_per_hour(points: &[(f64, u64)]) -> f64 {
    if points.len() < 2 {
        return 0.0;
    }
    let n = points.len() as f64;
    let sx: f64 = points.iter().map(|(x, _)| *x).sum();
    let sy: f64 = points.iter().map(|(_, y)| *y as f64).sum();
    let sxx: f64 = points.iter().map(|(x, _)| x * x).sum();
    let sxy: f64 = points.iter().map(|(x, y)| x * *y as f64).sum();
    let den = n * sxx - sx * sx;
    if den == 0.0 {
        return 0.0;
    }
    (n * sxy - sx * sy) / den * 3600.0
}

/// One memory sample (§4): `VmRSS`, the `inflight_rows` gauge, the per-view `ids_pending` and
/// `cascade_cached_rows` from `stats_of` (`engine.rs:242`), and the two `/metrics` ages when the
/// engine was started with `LAZYVIEW_HEALTH_ADDR` (`addr` empty = it was not, so no fetch).
pub async fn mem_sample(eng: &Engine, db: &Db, specs: &[ViewSpec], addr: &str) -> Result<MemRow> {
    let mut row = MemRow {
        rss_kib: vm_rss_kib()?,
        inflight_rows: eng.metrics.gauges.inflight_rows.load(Ordering::Relaxed),
        ..Default::default()
    };
    for s in specs {
        match eng.view_stats(db, &s.name) {
            Some(v) => {
                row.ids_pending += v.ids_pending;
                row.cascade_cached_rows += v.cascade_cached_rows;
                row.per_view
                    .push((s.name.clone(), v.ids_pending, v.cascade_cached_rows));
            }
            None => row.unlisted.push(s.name.clone()),
        }
    }
    if !addr.is_empty() {
        let (status, body) = http_get(addr, "/metrics").await?;
        if status == 200 {
            row.applied_position_age_s =
                metric_value(&body, "lazyview_applied_position_age_seconds");
            row.binlog_disconnected_s = metric_value(&body, "lazyview_binlog_disconnected_seconds");
        }
    }
    Ok(row)
}

/// The value of one Prometheus line of a `/metrics` body (`name{labels} value` or `name value`).
pub fn metric_value(body: &str, name: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?;
        if !(rest.is_empty() || rest.starts_with(' ') || rest.starts_with('{')) {
            return None;
        }
        rest.split_whitespace().next_back()?.parse::<f64>().ok()
    })
}

// ---------------------------------------------------------------------------------------------
// The WARN accounting (S2.7)
// ---------------------------------------------------------------------------------------------

/// How much of a `WARN` message forms its bucket (§4: the first 60 characters).
pub const WARN_BUCKET: usize = 60;

/// The WARN pattern of the cascade cache's gap line (`src/accumulate.rs:913`): after a restart
/// the engine replays a binlog window the in-memory cascade cache never saw, and says so at most
/// once a minute per view. Chaos-expected (issue #167); the oracle sweep of every check still
/// fails a view whose rows actually diverge, and that is named.
pub const CASCADE_GAP_WARN: &str = "the cascade cache missed binlog events";

/// The WARN patterns a soak run expects — the chaos-own cases of §4. A bucket whose message
/// contains one of them is expected; every other bucket fails the check.
pub const EXPECTED_WARNS: &[&str] = &[
    // A target is down or behind: every failed refresh / write is reported and retried with
    // backoff (`src/engine.rs:4291`, `:4840`).
    "; retrying in ",
    // The binlog stream dropped and reconnected — the source-pause event's own line
    // (`src/engine.rs:2796`, `:2815`, `:2824`).
    "reconnecting",
    // The kept-changes budget was crossed while a target was behind (`src/engine.rs:3782`, the
    // `reg_down_target_keeps_its_keys` warning of F4).
    "exceed their",
    // Chaos-expected: an engine restart replays a window the in-memory cascade cache cannot see
    // (`src/accumulate.rs:913`, issue #167) — counted in the report as `cascade_gap_warns`.
    CASCADE_GAP_WARN,
];

/// The `WARN` buckets of the given engine logs (§4): every [`Engine`]'s `log_path` (`engine.rs:15`,
/// one file per start), so a soak that restarted engines scans every file it collected. A missing
/// file is skipped (`finish_run` cleans the log dir, F12). Any `ERROR` or `panicked at` line fails.
pub fn scan_warns(paths: &[PathBuf]) -> Result<BTreeMap<String, u64>> {
    let mut out: BTreeMap<String, u64> = BTreeMap::new();
    for p in paths {
        let text = match std::fs::read_to_string(p) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("read engine log {}", p.display())),
        };
        for (k, n) in scan_warn_text(&text, &p.display().to_string())? {
            *out.entry(k).or_default() += n;
        }
    }
    Ok(out)
}

/// [`scan_warns`] over one log's text (`where_` names the log in an error).
pub fn scan_warn_text(text: &str, where_: &str) -> Result<BTreeMap<String, u64>> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        if line.contains("panicked at") {
            bail!("{where_}: the engine panicked: {line}");
        }
        let (level, msg) = classify(line);
        if level == Some("ERROR") {
            bail!("{where_}: ERROR line in the engine log: {line}");
        }
        if level == Some("WARN") {
            *out.entry(clip(&msg, WARN_BUCKET)).or_default() += 1;
        }
    }
    Ok(out)
}

/// The buckets outside [`EXPECTED_WARNS`]: an empty vector is the WARN rule of §4 passing.
pub fn unexpected_warns(warns: &BTreeMap<String, u64>) -> Vec<&String> {
    warns
        .keys()
        .filter(|k| !EXPECTED_WARNS.iter().any(|p| k.contains(p)))
        .collect()
}

/// The levels `tracing_subscriber::fmt` writes.
const LEVELS: [&str; 5] = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];

/// Splits one engine log line into `(level, message)`: the level is the token in the prefix before
/// the target's `": "`, the message is what follows it with the leading `field=value` tokens
/// dropped — §4 buckets by the *message*'s first 60 chars, so a per-view field must not split one
/// message into many buckets, and a message that mentions `ERROR 1062` must not read as a failure.
fn classify(line: &str) -> (Option<&'static str>, String) {
    let (head, body) = match line.split_once(": ") {
        Some((h, b)) => (h, b),
        None => (line, ""),
    };
    let level = LEVELS.iter().copied().find(|l| word(head, l));
    let msg = body
        .split_whitespace()
        .skip_while(|t| t.contains('='))
        .collect::<Vec<_>>()
        .join(" ");
    (level, msg)
}

/// Whether `needle` occurs in `hay` as a whole word (not inside `WARNING` or `my_WARN_thing`).
fn word(hay: &str, needle: &str) -> bool {
    let b = hay.as_bytes();
    let edge = |i: usize| i >= b.len() || !(b[i].is_ascii_alphanumeric() || b[i] == b'_');
    hay.match_indices(needle)
        .any(|(i, _)| (i == 0 || edge(i - 1)) && edge(i + needle.len()))
}
