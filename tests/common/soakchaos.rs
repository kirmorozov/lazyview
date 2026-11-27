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

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use lazyview::binlog::Position;
use lazyview::Snapshot;
use mysql_async::prelude::Queryable;

use super::soakcheck::{
    counter, counters, quiesce_and_check, unix_now, view_rows, Attribution, Check, CheckRow, Lag,
    Ledger, MemRow,
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
    /// A random toxic on one source role's link (T7): the event's engine reaches every source
    /// role through its own proxy and the toxic comes from the bounded table below. Drawn only
    /// in the long mode with `LAZYVIEW_TEST_SOAK_TOXI` — the smoke never draws it (D6).
    NetworkFault,
}

/// Every kind, in the order `plan` guarantees them when the window allows one event per kind.
/// `NetworkFault` is last so the kinds a run may draw (D6: never the network kind in the smoke)
/// are a prefix of this table.
const KINDS: [Chaos; 6] = [
    Chaos::EngineCrash,
    Chaos::EngineStop,
    Chaos::TargetDown,
    Chaos::SourcePause,
    Chaos::SourceDdl,
    Chaos::NetworkFault,
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
            Chaos::NetworkFault => "network_fault",
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
pub fn plan(seed: u64, minutes: u32, per_hour: u32, network: bool) -> Vec<(Duration, Chaos)> {
    // D6: the smoke has no proxy and no container control, so it draws only the five kinds of
    // §3; the network kind joins the draw in the long mode when the run opted in (T7). It is
    // the last entry of `KINDS`, so the permitted set is always a prefix.
    let kinds: &[Chaos] = if network {
        &KINDS
    } else {
        &KINDS[..KINDS.len() - 1]
    };
    let minutes = u64::from(minutes.max(1));
    let window = (minutes * 60).max(2);
    let wanted = (((u64::from(per_hour.max(1))) * minutes) / 60).clamp(1, 512) as usize;
    let first = derive_seed(seed, "chaos-first") % kinds.len() as u64;
    let mut taken: Vec<u64> = Vec::with_capacity(wanted);
    let mut out: Vec<(Duration, Chaos)> = Vec::with_capacity(wanted);
    for i in 0..wanted {
        let draw = derive_seed(seed, &format!("chaos{i}"));
        // The first `kinds.len()` events are one of each kind (in a seeded order); the rest are
        // drawn independently.
        let kind = if i < kinds.len() {
            kinds[((i as u64 + first) as usize) % kinds.len()]
        } else {
            kinds[(derive_seed(draw, "kind") % kinds.len() as u64) as usize]
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
// The random toxic of `Chaos::NetworkFault` (T7, §5)
// ---------------------------------------------------------------------------------------------

/// The three source roles the T3 hooks route (`LAZYVIEW_TEST_{BINLOG,QUERY,META}_DSN`): the
/// event puts one proxy per role and aims the drawn toxic at one of them.
const ROLES: [&str; 3] = ["binlog", "query", "meta"];

/// The fixed toxic table (type × direction): the six types of F2 **plus `slow_close`**, which
/// decision Q6 admits to the random mix (a half-open connection is this plan's fault class);
/// `packet_loss` is Q6's rejection (a % loss on a shared runner is an unreproducible finding).
/// Every parameter is drawn inside its row's bounded range, and the event lifts the fault
/// before its `quiesce_and_check`, so nothing here can wedge a shared server past the engine's
/// own bounds (F5) — in particular no row uses `timeout: 0`, the never-closing black hole (F2).
const TOXI_ROWS: u64 = 11;

/// The event's toxic, drawn from `derive_seed(seed, "toxi<i>")` alone: the same seed replays
/// the same sequence (the acceptance's two-run comparison). Returns the report label, the
/// index into [`ROLES`] and the toxic to arm (named so the API read-back can identify it).
fn draw_toxic(seed: u64, i: usize) -> (String, usize, toxi::Toxic) {
    let d = derive_seed(seed, &format!("toxi{i}"));
    let p = derive_seed(d, "toxic-param");
    let q = derive_seed(p, "toxic-param-2");
    let role = (derive_seed(d, "toxic-role") % ROLES.len() as u64) as usize;
    let (t, what) = match (d % TOXI_ROWS) as usize {
        0 | 1 => {
            let (l, j) = (40 + p % 161, q % 41);
            let dir = if d.is_multiple_of(2) {
                toxi::Dir::Downstream
            } else {
                toxi::Dir::Upstream
            };
            (
                toxi::Toxic::latency(l, j, dir),
                format!("latency {l}±{j}ms {}", dir.as_str()),
            )
        }
        2 | 3 => {
            let r = 1024 + p % 7168;
            let dir = if d.is_multiple_of(2) {
                toxi::Dir::Downstream
            } else {
                toxi::Dir::Upstream
            };
            (
                toxi::Toxic::bandwidth(r, dir),
                format!("bandwidth {r}KB/s {}", dir.as_str()),
            )
        }
        4 | 5 => {
            let (a, v, dl) = (200 + p % 1801, q % 101, 10 + q % 191);
            let dir = if d.is_multiple_of(2) {
                toxi::Dir::Downstream
            } else {
                toxi::Dir::Upstream
            };
            (
                toxi::Toxic::slicer(a, v, dl, dir),
                format!("slicer {a}±{v}B every {dl}us {}", dir.as_str()),
            )
        }
        6 => {
            let t = p % 2001;
            (
                toxi::Toxic::reset_peer(t),
                format!("reset_peer after {t}ms"),
            )
        }
        7 => {
            let t = 1000 + p % 2001;
            (toxi::Toxic::timeout(t), format!("timeout {t}ms (never 0)"))
        }
        8 | 9 => {
            // ≥ 64 KiB: the handshake and the login survive, the cut lands mid-result-set.
            let b = 65536 + p % 196608;
            let dir = if d.is_multiple_of(2) {
                toxi::Dir::Downstream
            } else {
                toxi::Dir::Upstream
            };
            (
                toxi::Toxic::limit_data(b, dir),
                format!("limit_data {b}B {}", dir.as_str()),
            )
        }
        _ => {
            let dl = 500 + p % 2501;
            (slow_close(dl), format!("slow_close {dl}ms"))
        }
    };
    (what, role, t.named("soak_fault"))
}

/// `slow_close{delay}` (F2): the one toxic of the mix the `toxi.rs` scenario builders do not
/// offer (D7 kept it out of the deterministic scenarios), so the soak builds it from the
/// public fields.
fn slow_close(delay_ms: u64) -> toxi::Toxic {
    let mut attributes = serde_json::Map::new();
    attributes.insert("delay".to_string(), delay_ms.into());
    toxi::Toxic {
        name: format!("slow_close_{delay_ms}"),
        kind: "slow_close".to_string(),
        stream: toxi::Dir::Downstream.as_str().to_string(),
        toxicity: 1.0,
        attributes,
    }
}

/// The host:port a proxy dials for this schema's server: the DSN's own authority with a
/// loopback host rewritten to the name the toxiproxy container uses for the host (the chaos
/// profile's `host.docker.internal` extra-host; `LAZYVIEW_TEST_TOXI_HOST` overrides). Set
/// `LAZYVIEW_TEST_TOXI_UPSTREAM=<host:port>` (the same hook as [`toxi::upstream`]) to dial it
/// verbatim: a compose-mode shared server publishes on the host loopback only, so from the
/// daemon its docker DNS name and container port are the only reachable upstream, not the
/// published host port.
fn link_upstream(dsn: &str) -> Result<String> {
    if let Some(u) = std::env::var("LAZYVIEW_TEST_TOXI_UPSTREAM")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        return Ok(u);
    }
    let rest = dsn
        .split_once("://")
        .with_context(|| format!("not a DSN: {dsn}"))?
        .1;
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    let hostport = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let (host, port) = hostport
        .split_once(':')
        .with_context(|| format!("DSN authority without a port: {hostport}"))?;
    let host = match host {
        "127.0.0.1" | "localhost" | "::1" => std::env::var("LAZYVIEW_TEST_TOXI_HOST")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "host.docker.internal".to_string()),
        h => h.to_string(),
    };
    Ok(format!("{host}:{port}"))
}

/// The DSN the harness hands the engine (`mod.rs:801 start_engine`): `db.dsn`, switched to
/// `LAZYVIEW_TEST_ENGINE_USER` when set. The proxied role DSNs derive from it so the engine's
/// least-privilege user crosses the proxies too (Q4).
fn engine_dsn(db: &Db) -> String {
    match std::env::var("LAZYVIEW_TEST_ENGINE_USER") {
        Ok(u) if !u.trim().is_empty() => dsn_with_user(&db.dsn, u.trim()),
        _ => db.dsn.clone(),
    }
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
/// them), the overrides every engine of the run starts with, the log files collected so far, and
/// the run's approved-rebuild ledger (`soakcheck::Ledger`: `view@target` → the
/// cumulative `whole_view_refreshes` an event has accounted for, which the per-view F5 rule then
/// tolerates, plus the wave of the `SourceDdl` event in flight; an event that explains a rebuild
/// approves it, everything else fails).
pub struct ChaosRun<'a> {
    pub ctx: &'a Ctx,
    pub src: &'a Db,
    pub targets: &'a HashMap<String, Db>,
    pub specs: &'a [ViewSpec],
    pub writers: &'a [watch::Sender<bool>],
    pub ov: &'a [(&'a str, &'a str)],
    pub logs: &'a mut Vec<PathBuf>,
    pub approved: &'a mut Ledger,
    /// What the stop/crash events of the run have accounted for in the counter budget
    /// (`soakcheck::Attribution`, #266): the window each one owns, so a check can tell the fence
    /// of an orphaned write from a fence nothing explains.
    pub attribution: &'a mut Attribution,
    /// The run's base seed: `NetworkFault` draws its toxic from this and [`Self::toxi_i`].
    pub seed: u64,
    /// The event's index in the seeded schedule (`toxi{i}` labels, T7).
    pub toxi_i: usize,
    /// Statements the `EngineCrash` event runs **while the engine is dead**, between the kill and
    /// the restart: the #167 window batch of
    /// [`super::soakwork::SoakWorkload::cascade_in_window`], the one the `EngineStop` event already
    /// runs while stopped. The dead engine cannot apply them, so the saved position stays behind
    /// every statement and the resume is *known* to replay a mapped-cascade parent change instead
    /// of whatever the paced writers happened to emit before the kill landed (#319). Non-empty also
    /// anchors the kill on the engine rather than the clock: the paced writers are quiesced first
    /// (`soakcheck::set_writers`, the same call `quiesce_and_check` makes around every event's
    /// check), the engine is waited out to a caught-up point (`applied()` past what the source
    /// holds, the same observable `wait_caught_up` uses), and the writers are turned back on once the
    /// restarted engine has reported itself streaming (`wait_streaming`) and before `wait_ready`, which
    /// like `wait_flushed` needs them producing. The pause is what makes the anchor reachable at all: `run_paced` paces
    /// against the wall clock, so a loaded engine is never at the head for three rounds and the wait
    /// alone would fail a run at `LAZYVIEW_TEST_SOAK_TPS=400`. Empty — the soak's shape — is the
    /// plain kill-9 at the scheduled instant, which is what made a pinned replay a lottery on host
    /// throughput.
    pub crash_window: &'a [String],
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
        // Per-view `/stats` counters are per engine start (`engine.rs:24` a fresh `Metrics`):
        // approvals were earned against the old engine's cumulative numbers, which the new
        // engine's zeros make meaningless — the ledger empties with the engine. The budget
        // attribution forgets which values it has reported for the same reason (#266); the windows
        // of the events stay, because the engine this call starts is the one the stop event's
        // orphan is waiting in.
        self.approved.clear();
        self.attribution.engine_restarted();
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
    // The wall time the event ran in, for the budget attribution below (#266).
    let began_unix = unix_now();
    let mut eng = eng;
    let mut problems: Vec<String> = Vec::new();
    let mut extra: Vec<String> = Vec::new();

    match kind {
        // The `reg_crash_with_stale_full_view` set (`regressions/lifecycle.rs:310`): drop the
        // engine like `kill -9`, start it again, and it must be alive, streaming, and resumed
        // from the position it saved in `_lazyview_status`.
        Chaos::EngineCrash => {
            // #319: a run that names its `crash_window` does not kill the engine at a wall-clock
            // instant. The kill waits for an engine-observable point — the engine has applied
            // everything the source holds, the same test `wait_caught_up` makes — so the position
            // the cut falls on is the same one every run of the scenario, and what the resume
            // replays is the batch below rather than a sample of the writers' pace.
            //
            // The writers have to be stopped for that wait, and that is the load-bearing part.
            // `run_paced` paces against `Instant::now()` (`soakwork.rs:1610-1621`: one row per
            // `1e9 / tps` ns, and after a pause it bursts to catch the schedule back up), so a writer
            // under load is *never* behind the source by less than a pacing period for long enough for
            // three rounds to pass: on the amplifier this scenario reproduces on — `LAZYVIEW_TEST_SOAK_TPS=400`
            // over 4 writers, a 2.5 ms period — the wait could not be satisfied at any throughput, and
            // 8 of 10 `mysql97` runs died on `the engine never reached a caught-up point to crash at`,
            // which is a fact about this harness and not about the engine. Quiescing first is also what
            // makes the anchor mean the same thing twice: the crash then always lands on an engine that
            // has applied everything the writers left, with the batch below as the only thing in the gap.
            //
            // Stopping and restarting them this way is the harness's own idiom, not a new one:
            // `quiesce_and_check` does exactly this around every event's oracle sweep
            // (`soakcheck.rs:701-703`, `set_writers(true)` / `(false)`), and `wait_flushed` further down
            // this function depends on them running again, so they are turned back on as soon as the
            // engine is up — the resumed engine therefore replays the gap while live traffic keeps
            // arriving, which is the condition the scenario is about.
            let anchored = !run.crash_window.is_empty();
            if anchored {
                crate::common::soakcheck::set_writers(run.writers, true);
                let mut c = run.src.conn().await?;
                eng.wait_caught_up_rounds(&mut c, t_catchup(), 3)
                    .await
                    .with_context(|| {
                        format!("{what}: the engine never reached a caught-up point to crash at")
                    })?;
                c.disconnect().await.ok();
                ctx.log(format!(
                    "{what}: writers paused, engine caught up — crashing here, {n_window} window \
                     statements to replay after the restart",
                    n_window = run.crash_window.len()
                ));
            }
            let before = saved_position(run.src).await?;
            eng.crash().await?;
            // The window batch, run while the engine is dead (each statement its own transaction,
            // the clean-stop shape of `regressions/catchup.rs:209`, so the seed read can land
            // between any two of them): the saved position is behind every statement, so the seed
            // read at resume has already moved their children and the parent events replay against
            // that cache — the #167 shape, by construction and not as a lottery on how many
            // statements the writers had reached (`cascade_in_window`, PR 8, is the same batch the
            // `EngineStop` event runs while the engine is stopped).
            if !run.crash_window.is_empty() {
                let mut c = run.src.conn().await?;
                for stmt in run.crash_window {
                    exec(&mut c, stmt)
                        .await
                        .with_context(|| format!("{what}: the window cascade: {stmt}"))?;
                }
                c.disconnect().await.ok();
            }
            eng = run.start_engine()?;
            wait_streaming(&mut eng).await?;
            // The writers come back only once the engine reports itself streaming. Releasing them at
            // the restart instead lets their catch-up burst — `run_paced` chases its own schedule, so a
            // pause of t at TPS 400 is ~400·t rows written as fast as the connection allows — land on an
            // engine that is still connecting and snapshotting. That is the most likely cause of the one
            // TPS-400 red this branch owns: `fix5-m97-tps400/a2-135857Z` died on `timed out after 180s
            // waiting for the engine streaming again`, with the pause logged 13 s earlier. Below
            // `wait_streaming` and above `wait_ready`, because `wait_ready` here and `wait_flushed` at
            // the end of the event both need live writers producing.
            if anchored {
                crate::common::soakcheck::set_writers(run.writers, false);
            }
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
            // PR 8 (`167-chaos-window`): the cascade pair of the plan — an `ON UPDATE CASCADE`
            // key move and an `ON DELETE CASCADE` parent delete (and the `entities` delete that
            // cascades to `entity_refs`) inside the window, so every resume rehearses the #167
            // shape: the parent events replay against the cache the seed read left behind.
            for stmt in super::soakwork::SoakWorkload::cascade_in_window(run.seed, at.as_secs()) {
                exec(&mut c, &stmt)
                    .await
                    .with_context(|| format!("stop event: the window cascade: {stmt}"))?;
            }
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
            // The window's budget (the #167 remedy, the ruling lv-super kept since
            // `tmp/out/lvs-cascade-gap/decisions.md`): the cascade views the window writes are
            // never rebuilt and refresh as a whole at most once — the counters are per engine
            // start, and this is the only start since the window, so they read per window
            // directly. `full_rebuilds == 0` for an incremental view is the F5 rule the event's
            // closing `quiesce_and_check` already holds; here the refresh is approved so that
            // check tolerates the one refresh the window owns.
            let cascade = super::soakwork::soak_cascade_views();
            for v in view_rows(&eng, run.src, run.specs).await? {
                if cascade.contains(&v.name.as_str()) {
                    ensure!(
                        v.whole_view_refreshes <= 1,
                        "{what}: the cascade view {} refreshed its whole view {} times in one \
                         engine-restart window (budget 1)",
                        v.name,
                        v.whole_view_refreshes
                    );
                    run.approved.approve(&format!("{}@{}", v.name, v.target), 1);
                }
            }
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
        // The shapes of `tests/ddl.rs:74`–`82`. What no plan references must not rebuild anything,
        // and the widened column is the documented rebuild of F5 ("source column type changed") —
        // both asserted per view, not just reported (Q3): the approved set is what the engine
        // itself would replan — table granularity, [`plan_stale_of`], never view names — a rebuild
        // inside it is approved and reported, a rebuild of any *other* incremental view is named
        // and fails, and whether the plan-`kind`-reading views rebuilt is reported, not assumed.
        Chaos::SourceDdl => {
            let before = eng.snapshot();
            // The expectation and the rebuild baseline, both before any DDL of this event.
            let stale = plan_stale_of(run, &["items", "entities"]).await?;
            let kind_reading = kind_readers_of(run).await?;
            let base: HashMap<String, u64> = view_rows(&eng, run.src, run.specs)
                .await?
                .into_iter()
                .map(|v| (format!("{}@{}", v.name, v.target), v.whole_view_refreshes))
                .collect();
            // The burst is what a whole-view refresh of one of these views is credited to from now
            // on (`soakcheck::Ledger::in_wave`), so a scheduled check that runs beside this event
            // holds an owned view to its bar + the event's own wave and not to a bar the ledger has
            // not re-read yet. It closes when the loop below has seen the wave end.
            let burst = unix_now();
            run.approved.open_wave(
                stale.clone(),
                burst,
                burst + LEDGER_READS as u64 + LEDGER_QUIET_S + WAVE_TAIL_S,
            );
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
            // The per-view rule (F5) on the deltas of this event: `/stats` numbers are cumulative
            // since the engine started, so the event compares against its own baseline, approves
            // the plan-reading incremental views up to what it observed (the number the later
            // scheduled checks hold them to), and fails any other incremental view that moved.
            let mut rebuilt: Vec<String> = Vec::new();
            let mut stray: Vec<String> = Vec::new();
            let mut kind_total = 0usize;
            let mut kind_built = 0usize;
            let mut approved_rows = 0usize;
            for v in view_rows(&eng, run.src, run.specs).await? {
                let key = format!("{}@{}", v.name, v.target);
                if !v.mode.eq_ignore_ascii_case("incremental") {
                    continue; // the Full/Chained views rebuild by design (F5)
                }
                let delta = v
                    .whole_view_refreshes
                    .saturating_sub(base.get(&key).copied().unwrap_or(0));
                if kind_reading.contains(&key) {
                    kind_total += 1;
                }
                if stale.contains(&key) {
                    approved_rows += 1;
                    run.approved.approve(&key, v.whole_view_refreshes);
                    if delta > 0 {
                        rebuilt.push(format!("{key} +{delta}"));
                        if kind_reading.contains(&key) {
                            kind_built += 1;
                        }
                    }
                } else if delta > 0 {
                    stray.push(format!(
                        "{key} +{delta} (its plan references neither `items` nor `entities`)"
                    ));
                }
            }
            if !stray.is_empty() {
                problems.push(format!(
                    "whole-view refreshes outside the plan-derived set: {}",
                    stray.join(", ")
                ));
            }
            // A rebuild of F5 can land while this event reads its numbers: in the f1 run the wave
            // arrived 1–11 s after `wait_ready` had returned, both of the event's reads had
            // reported no movement, and every refresh after them was held to that stale bar; in the
            // mariadb1011 smoke of 2026-11-16 `w_chain_a`'s dependent rebuild landed ~40 s after
            // the burst, past a ledger that had closed on five equal (all-zero) reads. So the ledger
            // does not close on quiet alone: it re-reads the bars of the views this event may
            // rebuild once a second and closes only after the wave has had its floor of time
            // ([`LEDGER_MIN_WAVE_S`] since the burst) and the engine has then reported no whole-view
            // refresh of an owned view for [`LEDGER_QUIET_S`] s, on [`LEDGER_STABLE_READS`]
            // consecutive such reads (at most [`LEDGER_READS`] reads). The last read is the bar the
            // event's check and the scheduled ones hold them to, and the window in which a check
            // may still credit this event with a refresh closes with it, plus its tail.
            let mut prev: Option<HashMap<String, u64>> = None;
            let mut same = 0usize;
            for _ in 0..LEDGER_READS {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = unix_now();
                let mut bar: HashMap<String, u64> = HashMap::new();
                let mut quiet = now.saturating_sub(burst) >= LEDGER_MIN_WAVE_S;
                for v in view_rows(&eng, run.src, run.specs).await? {
                    let key = format!("{}@{}", v.name, v.target);
                    if stale.contains(&key) && v.mode.eq_ignore_ascii_case("incremental") {
                        bar.insert(key, v.whole_view_refreshes);
                        // A refresh of an owned view that is still recent: the wave is landing.
                        if let Some((_, at)) = v.last_whole_view_refresh.as_ref() {
                            quiet &= now.saturating_sub(*at) >= LEDGER_QUIET_S;
                        }
                    }
                }
                same = if quiet && prev.as_ref() == Some(&bar) {
                    same + 1
                } else {
                    0
                };
                for (key, count) in &bar {
                    run.approved.approve(key, *count);
                }
                prev = Some(bar);
                if same >= LEDGER_STABLE_READS {
                    break;
                }
            }
            run.approved.close_wave(unix_now(), WAVE_TAIL_S);
            // Which happened is reported, not assumed: the documented rebuild of the plans that
            // read `kind`, or no rebuild at all — the latter is legal (a widening the kept rows
            // survive), and those views are then still held to their oracle by the exactness check
            // that ends every event (§3), which sweeps the whole set including them.
            let widened = if kind_built == 0 {
                format!(
                    "widened `kind`: no rebuild among the {kind_total} plan-`kind`-reading \
                     incremental views (kept; the event's check swept their oracle — reason: \
                     source column type changed (F5))"
                )
            } else {
                format!(
                    "widened `kind`: rebuilt {kind_built}/{kind_total} plan-`kind`-reading \
                     incremental views (reason: source column type changed (F5), approved)"
                )
            };
            extra.push(format!(
                "adds/drops/ANALYZE: full_rebuilds {}, widened `kind` and the index: \
                 full_rebuilds {} total, definition_reloads {}; {widened}; approved {approved_rows} \
                 incremental plans referencing `items`/`entities` (table granularity, \
                 `on_tables_changed`)",
                counter(&after_quiet, "full_rebuilds"),
                counter(&all, "full_rebuilds"),
                d.definition_reloads,
            ));
        }
        // The random toxic of T7 (§5, D7/Q6): the event's engine reaches every source role
        // through its own proxy (the T3 hooks), one toxic from the bounded table of
        // [`draw_toxic`] is armed on one of them while the writers keep going, and every proxy
        // goes through the same `Drop` guard the chaos scenarios use (§2.2) **before** the
        // event's `quiesce_and_check`. Writers and oracle stay on direct connections (Q8).
        // The smoke never draws this kind (D6) and the long run draws it only with the
        // `LAZYVIEW_TEST_SOAK_TOXI` flag (`tests/soak.rs`).
        Chaos::NetworkFault => {
            if toxi::endpoint().is_none() {
                extra.push(
                    "no LAZYVIEW_TEST_TOXI: the links stay whole (the daemon is not on this \
                     host, reported skipped)"
                        .to_string(),
                );
            } else {
                // No `toxi::exclusive()` here: its guard is a `std` guard and `scenario()`
                // requires a `Send` future. Isolation still holds: nothing on this path calls
                // the global `POST /reset` (only the chaos self-tests do, another binary), the
                // proxy names and the `Drop` guard are scoped to this schema, the port band is
                // an atomic, and the suite runs one binary at a time under the heavy-suite lock.
                {
                    let tx = toxi::Toxi::open(ctx).await?;
                    let _guard = tx.for_schema(&run.src.name);
                    let up = link_upstream(&run.src.dsn)?;
                    let mut proxies = Vec::with_capacity(ROLES.len());
                    for role in ROLES {
                        let p = tx
                            .proxy(&run.src.name, &format!("soak_{role}"), &up)
                            .await?;
                        tx.wait_listening(&p).await?;
                        proxies.push(p);
                    }
                    // The event's engine, every source role through its proxy.
                    eng.stop().await?;
                    let base = engine_dsn(run.src);
                    let dsns: Vec<String> = proxies
                        .iter()
                        .map(|p| toxi::dsn_via_proxy(&base, p.listen))
                        .collect::<Result<Vec<_>>>()?;
                    let mut ev = run.start_engine_with(&[
                        ("LAZYVIEW_TEST_BINLOG_DSN", dsns[0].as_str()),
                        ("LAZYVIEW_TEST_QUERY_DSN", dsns[1].as_str()),
                        ("LAZYVIEW_TEST_META_DSN", dsns[2].as_str()),
                    ])?;
                    let mut c = run.src.conn().await?;
                    wait_ready(&mut c, &mut ev, &names(run.specs), t_ready()).await?;
                    c.disconnect().await.ok();
                    let (drawn, role, toxic) = draw_toxic(run.seed, run.toxi_i);
                    tx.toxic(&proxies[role], &toxic).await?;
                    let armed = tx
                        .toxics(&proxies[role])
                        .await?
                        .iter()
                        .any(|t| t.name == toxic.name);
                    if !armed {
                        problems.push(format!(
                            "the drawn toxic ({drawn}) is not armed on {}",
                            proxies[role].name
                        ));
                    }
                    // Hold the fault while the writers keep producing: bounded on the engine's own
                    // counters (at most 10 s for any counter to move, no sleep). A severing toxic
                    // legitimately freezes them, so this is the report's line, not an assertion —
                    // the exactness check below is the assertion.
                    let since = ev.snapshot();
                    let moved = wait_until(
                        "a counter to move on the event's engine under the fault",
                        Duration::from_secs(10),
                        || async { Ok(counters(&ev, &since).iter().any(|(_, v)| *v > 0)) },
                    )
                    .await
                    .is_ok();
                    extra.push(format!(
                        "{drawn} on the {} link: {}",
                        ROLES[role],
                        if moved {
                            "counters moved under the fault"
                        } else {
                            "no counter moved during the bounded fault phase (reported)"
                        }
                    ));
                    // Lift before the check: deleting the proxies deletes their toxics too (§1),
                    // which is what the `Drop` guard does — done explicitly here so the fresh
                    // engine's direct connections never race the cleanup.
                    tx.remove_all_for(&run.src.name).await?;
                    ev.stop().await?;
                    eng = run.start_engine()?;
                    let mut c = run.src.conn().await?;
                    wait_ready(&mut c, &mut eng, &names(run.specs), t_ready()).await?;
                    c.disconnect().await.ok();
                }
            }
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

    // The budget attribution of #266: the two kinds that stop the engine leave the write they had
    // in flight alive — `eng.stop()` releases the pool without killing it — and the engine this
    // event starts fences that orphan on its way through the catch-up (`lost_backend` →
    // `Endpoint::fence`, `src/supervisor.rs:173`, `:331-343`), which is the counted, designed §4.3
    // recovery. Its own check and the scheduled checks that follow read the counter from the
    // engine's start, so the window is recorded here and `soakcheck::budget_problems` allows the
    // rise only inside it; outside it, or for any other counter, a rise stays a hard failure.
    if matches!(kind, Chaos::EngineStop | Chaos::EngineCrash) {
        let tail_s = t_catchup().as_secs() + eng.cfg.target_timeout.as_secs();
        run.attribution
            .record_fence_window(&what, began_unix, unix_now(), tail_s);
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
        run.approved,
        run.attribution,
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

/// How often the `SourceDdl` event re-reads the `/stats` bars while it waits for its own rebuild
/// wave to settle before it closes the ledger (one read per second, so this is also its budget in
/// seconds). Reads only agree meaningfully once the wave is over: the 28-view soak rebuilds serially
/// at ~1.5 s per view (the f1 burst's wave ran ~45 s), and a completion gap of a few seconds lets
/// seconds-apart reads agree mid-wave — which is how `w_chain_a@source = 1` was still held to a bar
/// of 0 in the mariadb1011 smoke of 2026-11-16 (its dependent rebuild landed ~40 s after the
/// burst, past a ledger that had closed on five equal all-zero reads). So the loop wants
/// [`LEDGER_STABLE_READS`] consecutive reads that are equal *and* [`LEDGER_QUIET_S`] after the last
/// refresh of an owned view, and never before [`LEDGER_MIN_WAVE_S`] of the burst.
const LEDGER_READS: usize = 180;

/// Consecutive quiet, equal reads that close the ledger: the last one is the bar.
const LEDGER_STABLE_READS: usize = 5;

/// The wave's floor: the ledger may not close before this many seconds since the burst. The
/// rebuilds of a 28-view wave land over tens of seconds, and `wait_ready` returns when the *queued*
/// work of the DDL is done, not when the last dependent view has been swapped.
const LEDGER_MIN_WAVE_S: u64 = 20;

/// An owned view counts as quiet once its `last_whole_view_refresh` is this many seconds old: a
/// refresh more recent than this is the wave still landing (`src/stats.rs:337`).
const LEDGER_QUIET_S: u64 = 5;

/// How long after the ledger has closed a check may still credit a whole-view refresh to this
/// event (`soakcheck::Ledger::close_wave`): the race between the ledger's last read and the
/// exactness check that follows it, plus a refresh landing then and being read by a scheduled check
/// before the event's own one.
const WAVE_TAIL_S: u64 = 20;

/// The `(name, target, access)` rows of `_lazyview_state` — the engine's own records
/// (`src/meta.rs:308`), never view-name patterns.
async fn state_rows(run: &ChaosRun<'_>) -> Result<Vec<(String, String, String)>> {
    let mut c = run.src.conn().await?;
    let state = rows(
        &mut c,
        "SELECT CAST(name AS CHAR), CAST(target AS CHAR), COALESCE(CAST(access AS CHAR), '') \
         FROM _lazyview_state",
    )
    .await?;
    c.disconnect().await.ok();
    let names: Vec<&str> = run.specs.iter().map(|s| s.name.as_str()).collect();
    Ok(state
        .into_iter()
        .filter_map(|r| match (r[0].clone(), r[1].clone(), r[2].clone()) {
            (Some(name), Some(target), Some(access)) if names.contains(&name.as_str()) => {
                Some((name, target, access))
            }
            _ => None,
        })
        .collect())
}

/// The tables one `access` text's plan mentions: the `<table>` of every
/// `occ <alias> <schema>.<table> read <list>` line (`src/access.rs:776`), read lists ignored.
pub fn occ_tables(access: &str) -> HashSet<String> {
    access
        .lines()
        .filter_map(|l| l.strip_prefix("occ "))
        .filter_map(|r| r.split_whitespace().nth(1))
        .map(|tref| tref.rsplit_once('.').map_or(tref, |(_, t)| t).to_string())
        .collect()
}

/// The parents of the cascading foreign keys one `access` text records: the `<parent>` of every
/// `cascade <schema>.<parent> -> <schema>.<child> …` line. A change a cascading FK makes happens
/// inside the storage engine, with no binlog rows on the child, so the engine counts a DDL of the
/// parent as affecting this view too: "Tables the view reads, and parents of cascading FKs into
/// them (their column positions are part of the plan too)", `p.tables.iter().any(hit) ||
/// p.cascades.iter().any(hit(parent))` (`src/engine.rs:3800-3807`). The `occ` lines never name
/// such a parent, so a rule that reads only them cannot approve the rebuild the engine owes a
/// view like `wc_refs_by_city` (its plan reads `entity_refs` alone, whose FK cascades from
/// `entities`): one legitimate whole refresh of it would be an unavoidable FAIL (the f1 finding).
pub fn cascade_parents(access: &str) -> HashSet<String> {
    access
        .lines()
        .filter_map(|l| l.strip_prefix("cascade "))
        .filter_map(|r| r.split_whitespace().next())
        .map(|tref| tref.rsplit_once('.').map_or(tref, |(_, t)| t).to_string())
        .collect()
}

/// The tables of one `access` text the engine counts as the plan's own: the `occ` tables plus the
/// parents of the cascading FKs into them (`src/engine.rs:3800-3807`). What [`stale_closure`] —
/// and so [`plan_stale_of`] — matches a DDL's tables against.
pub fn plan_tables(access: &str) -> HashSet<String> {
    let mut tables = occ_tables(access);
    tables.extend(cascade_parents(access));
    tables
}

/// The fixpoint [`plan_stale_of`] computes: the `view@target` keys whose plan names one of
/// `seeds`, closed over "a view whose plan references an affected view's own table" (chains).
/// `refs` is `(view@target, view name, the tables of its plan)` per view. Public so
/// `tests/soak_widen.rs` can pin the rule itself against what the engine treats as affected.
pub fn stale_closure(
    refs: &[(String, String, HashSet<String>)],
    seeds: &[&str],
) -> HashSet<String> {
    let mut stale: HashSet<String> = refs
        .iter()
        .filter(|(_, _, tabs)| tabs.iter().any(|t| seeds.contains(&t.as_str())))
        .map(|(key, _, _)| key.clone())
        .collect();
    loop {
        let stale_names: HashSet<&str> = stale
            .iter()
            .filter_map(|k| refs.iter().find(|(key, _, _)| key == k))
            .map(|(_, name, _)| name.as_str())
            .collect();
        let grown: Vec<String> = refs
            .iter()
            .filter(|(key, _, tabs)| {
                !stale.contains(key) && tabs.iter().any(|t| stale_names.contains(t.as_str()))
            })
            .map(|(key, _, _)| key.clone())
            .collect();
        if grown.is_empty() {
            return stale;
        }
        stale.extend(grown);
    }
}

/// The `view@target` keys of the views the DDL of the `SourceDdl` event may rebuild (Q3). The
/// engine decides this at table granularity, not column: `on_tables_changed`
/// (`src/engine.rs:3774`) replans every slot whose plan is affected by a changed table — its
/// `occ` tables **or the parents of the cascading FKs into them** ([`plan_tables`],
/// `src/engine.rs:3800-3807`) — whatever the read lists, and rebuilds without freezing when a
/// chained view's parent table was swapped. So the expected set is the fixpoint: the views whose
/// plan references one of `seeds`, plus the views whose plan references an affected view's own
/// table (chains), closed under that rule. A whole-view refresh of an incremental view outside
/// it is a named failure; inside it the event approves the refresh into the ledger.
async fn plan_stale_of(run: &ChaosRun<'_>, seeds: &[&str]) -> Result<HashSet<String>> {
    let state = state_rows(run).await?;
    let refs: Vec<(String, String, HashSet<String>)> = state
        .iter()
        .map(|(name, target, access)| {
            (
                format!("{name}@{target}"),
                name.clone(),
                plan_tables(access),
            )
        })
        .collect();
    let stale = stale_closure(&refs, seeds);
    ensure!(
        !stale.is_empty(),
        "no view's plan references {}: the access records or the schema changed",
        seeds.join("/")
    );
    Ok(stale)
}

/// The `view@target` keys of the views whose *plan* reads column `kind` of `entities`, for the
/// event's report (the documented F5 rebuild, reported not assumed): the `occ` read lists carry
/// the 0-based column ordinals the plan may read (`*` for every column, `src/access.rs:766`),
/// the column's ordinal comes from `information_schema`.
async fn kind_readers_of(run: &ChaosRun<'_>) -> Result<HashSet<String>> {
    let mut c = run.src.conn().await?;
    let ordinal = scalar_i64(
        &mut c,
        "SELECT ORDINAL_POSITION - 1 FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'entities' AND COLUMN_NAME = 'kind'",
    )
    .await
    .context("`entities`.`kind` has no ordinal: the schema changed")?;
    c.disconnect().await.ok();
    let state = state_rows(run).await?;
    Ok(state
        .iter()
        .filter(|(_, _, access)| reads_column(access, "entities", ordinal))
        .map(|(name, target, _)| format!("{name}@{target}"))
        .collect())
}

/// Whether one `access` text reads the column at 0-based `ordinal` of `table`: any `occ` line for
/// the table whose read list is `*` (the whole table) or contains the ordinal.
fn reads_column(access: &str, table: &str, ordinal: i64) -> bool {
    access.lines().any(|line| {
        // `occ <alias> <schema>.<table> read <list> [filter …] -> <path>` (`src/access.rs:776`),
        // the list `*` or `(n, n, …)` joined with ", " (`src/access.rs:766`) — with spaces, so
        // one list spans several whitespace-separated tokens and is collected whole.
        let mut it = match line.strip_prefix("occ ") {
            Some(rest) => rest.split_whitespace(),
            None => return false,
        };
        let (Some(_alias), Some(tref), Some("read")) = (it.next(), it.next(), it.next()) else {
            return false;
        };
        if tref.rsplit_once('.').map_or(tref, |(_, t)| t) != table {
            return false;
        }
        let mut list = String::new();
        for tok in it {
            list.push_str(tok);
            if list == "*" || list.ends_with(')') {
                break;
            }
        }
        list == "*"
            || list
                .trim_matches(|c| c == '(' || c == ')')
                .split(',')
                .any(|o| o.trim().parse::<i64>() == Ok(ordinal))
    })
}

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
pub async fn saved_position(db: &Db) -> Result<Position> {
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
pub fn grep_log(path: &PathBuf, needle: &str) -> Result<Option<String>> {
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

/// The engine log's lines, once, for a witness that asks the same question of several views (one
/// pass per view, one read of the file): the owner's rule on test waiting — poll through
/// `wait_until`, never a sleep loop — covers reading a growing log file inside a loop as well.
/// [`Self::grep_log`] is the one-line answer for a witness that asks once; `None` when the engine
/// has not written the log yet, which is a witness that found nothing, not an error.
pub fn log_lines(path: &PathBuf) -> Result<Option<Vec<String>>> {
    match std::fs::read_to_string(path) {
        Ok(t) => Ok(Some(t.lines().map(str::to_string).collect())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read engine log {}", path.display())),
    }
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
pub async fn wait_streaming(eng: &mut Engine) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::reads_column;

    // The lines are the engine's own shape (`src/access.rs:776`), including the ", "-joined
    // read list that broke an earlier token-by-token parser, a `*`, and a filter whose
    // `IN (…)` parens must not be mistaken for the read list.
    const ACCESS: &str = "occ e lvt.entities read (0, 2) -> direct id = (0)\n\
         occ i lvt.items read (1, 3, 4) -> direct id = (1)\n\
         occ t lvt.tags read * -> untraced (`t` is a semi-join table)\n\
         occ g lvt.groups read (0) filter g.col IN (3, 5) -> untraced\n\
         occ e lvt.entities read () -> whole\n";

    #[test]
    fn reads_column_parses_the_engine_read_lists() {
        assert!(reads_column(ACCESS, "entities", 0));
        assert!(reads_column(ACCESS, "entities", 2)); // the second element of `(0, 2)`
        assert!(!reads_column(ACCESS, "entities", 3)); // only in the items/groups filters
        assert!(reads_column(ACCESS, "items", 4)); // last element of `(1, 3, 4)`
        assert!(reads_column(ACCESS, "tags", 7)); // `*` reads every column
        assert!(!reads_column(ACCESS, "groups", 3)); // `(0)` only; 3 is in the filter
        assert!(!reads_column(ACCESS, "missing", 0));
    }
}
