//! The engine as **its own process**: spawn the real `lazyview` binary the way a deployment runs
//! it, drive it through its health endpoints, sample its CPU and RSS from `/proc`, kill it with a
//! real signal, and pick the kill instant from a seed.
//!
//! Why (`brutal_production_readiness_f2_refined.md` §5, the gap at 512–513: "The crash is simulated
//! inside the test process. The real binary is not killed"): `tests/common/engine.rs` crashes an
//! engine by dropping its futures. That is not a `kill -9` — it does not close sockets the way the
//! kernel does, and it never exercises spawn, config parse, the health bind, the instance lock or
//! the saved position across a process boundary. Recovery measurement (plan
//! `tmp/out/lvs-perf/plan/breakdown.md` §2, `tests/chaos_recovery.rs`) has to kill the thing
//! operators run, so it needs a handle on a process; the throughput table (§1) needs its CPU and RSS.
//!
//! Owned here: [`engine_binary`] (which binary to run), [`EngineProc`] (spawn, endpoints,
//! signals, teardown), [`parse_prometheus`] (a tiny reader for the text `/metrics` serves),
//! [`Sampler`] (CPU/RSS from `/proc`), [`KillPointPlan`] (a seeded, reproducible "any
//! instant"), [`run_measured`] (the sampler's own cross-check against the kernel's
//! report, `time -v`), and [`finished_time_run_result`] (what a `time` run that was over before the
//! sampler attached reports).
//!
//! Rules kept: **no new dependency** (HTTP/1.1 over one short-lived connection like
//! `super::engine::http_json`, signals through `kill`, `/proc` through `std::fs`); **teardown
//! always** ([`EngineProc`]'s `Drop` kills and reaps, so a panic, an `?` or a scenario timeout
//! cannot leave an engine holding the instance lock or a binlog dump connection on a server every
//! suite shares); **the shared toxiproxy daemon only** (nothing here starts a daemon — a caller
//! proxies an engine by handing [`EngineProc::start_with`] role DSNs pointing at a
//! [`super::toxi::Proxy`] listen address, which is how `toxi::proxied_target_env` already works);
//! **the copy set stays verbatim-copyable** (this file reads the engine's public surface and the
//! engine never reads it); **no hot-path cost** (nothing here runs per binlog event — the engine is
//! untouched, which is why this PR is tests only).

use super::*;

use anyhow::ensure;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::JoinHandle;

/// Seconds per `/proc/<pid>/stat` tick: the kernel's `USER_HZ` is 100 on every Linux architecture
/// lazyview runs on. A wrong value would show up as a CPU error in `selftest_engineproc_sampler`.
const TICKS_PER_SEC: f64 = 100.0;

/// The most bytes [`http_get`] will accumulate from one answer. The health endpoints answer in KiB
/// (`/metrics`, the probes, `/stats`); an answer past this is a server that floods or never closes
/// the connection, and reading it without a bound is how the 2026-11-18 harness reached 2.6 GiB
/// RSS and set off the host's OOM killer. Cap it and trip, never absorb.
const MAX_RESPONSE_BYTES: usize = 8 << 20;

// ---------------------------------------------------------------------------------------------
// The binary under test
// ---------------------------------------------------------------------------------------------

/// Names the binary to run (absolute path): set it when the binary was built outside this test's
/// own cargo invocation, e.g. `LAZYVIEW_TEST_ENGINE_BIN=target/release/lazyview`.
pub const BIN_ENV: &str = "LAZYVIEW_TEST_ENGINE_BIN";

/// The `lazyview` binary to spawn: [`BIN_ENV`], else what cargo built for this test target
/// (`CARGO_BIN_EXE_lazyview`, so `cargo test` runs the binary it just built), else the sibling of
/// this test binary (`<target>/<profile>/lazyview`, then `release`/`debug`). Every candidate must
/// exist: a harness that quietly runs a stale binary is worse than one that refuses.
pub fn engine_binary() -> Result<PathBuf> {
    let mut cands: Vec<PathBuf> = Vec::new();
    if let Ok(v) = std::env::var(BIN_ENV) {
        if !v.trim().is_empty() {
            cands.push(PathBuf::from(v.trim()));
        }
    }
    if let Some(p) = option_env!("CARGO_BIN_EXE_lazyview") {
        cands.push(PathBuf::from(p));
    }
    if let Ok(exe) = std::env::current_exe() {
        // .../target/<profile>/deps/harness_selftest-<hash> → .../target/<profile>/lazyview
        if let Some(profile) = exe.parent().and_then(|d| d.parent()) {
            cands.push(profile.join("lazyview"));
        }
        if let Some(target) = exe
            .parent()
            .and_then(|d| d.parent())
            .and_then(|d| d.parent())
        {
            cands.push(target.join("release/lazyview"));
            cands.push(target.join("debug/lazyview"));
        }
    }
    for c in &cands {
        if c.is_file() {
            return Ok(c.clone());
        }
    }
    bail!(
        "no lazyview binary to run (tried: {}); build it (`cargo build --release --bin lazyview`) \
         or name it with {BIN_ENV}",
        cands
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

// ---------------------------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------------------------

/// A signal a test sends to a real engine process. `Kill` is the `kill -9` / OOM-killer model (the
/// kernel closes the sockets, so the servers see the drop at once); `Stop` is the pull-the-plug
/// model (frozen process, sockets stay open, no FIN/RST); `Cont` thaws it; `Term` is the shutdown a
/// deployment asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Term,
    Kill,
    Stop,
    Cont,
}

impl Signal {
    pub fn name(self) -> &'static str {
        match self {
            Signal::Term => "SIGTERM",
            Signal::Kill => "SIGKILL",
            Signal::Stop => "SIGSTOP",
            Signal::Cont => "SIGCONT",
        }
    }

    fn kill_arg(self) -> &'static str {
        match self {
            Signal::Term => "-TERM",
            Signal::Kill => "-9",
            Signal::Stop => "-STOP",
            Signal::Cont => "-CONT",
        }
    }
}

/// Sends `sig` with the `kill` program (no `libc`/`nix` dependency for three calls). A pid that
/// already left is not an error: the caller waits for the exit anyway, and "is it alive" racing
/// "signal it" is normal in a crash test.
pub fn signal(pid: u32, sig: Signal) -> Result<()> {
    let out = Command::new("kill")
        .arg(sig.kill_arg())
        .arg(pid.to_string())
        .output()
        .with_context(|| format!("spawn `kill` to send {} to {pid}", sig.name()))?;
    let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if out.status.success() || err.contains("No such process") {
        return Ok(());
    }
    bail!("kill {} {pid}: {err}", sig.kill_arg());
}

/// The state character of `/proc/<pid>/stat`, or `None` when the process is gone. `T` is stopped by
/// a signal, `Z` an unreaped zombie.
pub fn proc_state(pid: u32) -> Option<char> {
    read_proc(pid).ok().map(|s| s.state)
}

/// Whether `pid` exists and is not a zombie waiting to be reaped.
pub fn pid_alive(pid: u32) -> bool {
    matches!(proc_state(pid), Some(c) if c != 'Z')
}

/// The pids whose parent is `ppid`: the process GNU `time` runs for us is our child's child.
pub fn child_pids(ppid: u32) -> Vec<u32> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir("/proc") else {
        return out;
    };
    for e in rd.flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        if let Some(f) = stat
            .rsplit_once(')')
            .map(|(_, r)| r.split_whitespace().collect::<Vec<_>>())
        {
            // state(3) ppid(4): the second field after the last `)`.
            if f.len() > 1 && f[1].parse::<u32>().ok() == Some(ppid) {
                out.push(pid);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// /proc
// ---------------------------------------------------------------------------------------------

/// What the kernel knows about a process at one instant (`/proc/<pid>/{stat,status}`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcStat {
    /// `R` running, `S` sleeping, `D` uninterruptible, `T` stopped, `Z` zombie.
    pub state: char,
    /// `utime + stime` in clock ticks: the kernel's own CPU accounting, which is also what
    /// `getrusage` (and so `time -v`) reports.
    pub cpu_ticks: u64,
    /// Current resident set, KiB.
    pub rss_kib: u64,
    /// Peak resident set since exec, KiB: the kernel's high-water mark, i.e. `time -v`'s number.
    pub vmhwm_kib: u64,
}

impl ProcStat {
    pub fn cpu_seconds(&self) -> f64 {
        self.cpu_ticks as f64 / TICKS_PER_SEC
    }
}

pub fn read_proc(pid: u32) -> Result<ProcStat> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .with_context(|| format!("read /proc/{pid}/stat"))?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .with_context(|| format!("read /proc/{pid}/status"))?;
    let (state, cpu_ticks) =
        parse_stat(&stat).with_context(|| format!("unparsable stat of {pid}"))?;
    let (rss_kib, vmhwm_kib) = parse_status_mem(&status)
        .with_context(|| format!("no VmRSS/VmHWM in /proc/{pid}/status"))?;
    Ok(ProcStat {
        state,
        cpu_ticks,
        rss_kib,
        vmhwm_kib,
    })
}

/// `(state, utime+stime)` from a `/proc/<pid>/stat` line. The command name is parenthesised and may
/// hold spaces and parentheses, so fields are counted from the **last** `)`. Pure, for the selftest.
pub fn parse_stat(line: &str) -> Option<(char, u64)> {
    let f: Vec<&str> = line.rsplit_once(')')?.1.split_whitespace().collect();
    if f.len() < 13 {
        return None;
    }
    Some((
        f[0].chars().next()?,
        f[11]
            .parse::<u64>()
            .ok()?
            .checked_add(f[12].parse::<u64>().ok()?)?,
    ))
}

/// `(VmRSS, VmHWM)` in KiB from a `/proc/<pid>/status` text. Pure, like `soakcheck::parse_vm_rss`.
pub fn parse_status_mem(status: &str) -> Option<(u64, u64)> {
    let field = |name: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
    };
    Some((field("VmRSS:")?, field("VmHWM:")?))
}

/// One `/proc` observation with the instant it was taken.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub at: Duration,
    pub stat: ProcStat,
}

/// The reduction of a [`Sampler`] run: the numbers a recovery or throughput table prints.
#[derive(Debug, Clone)]
pub struct ProcUsage {
    pub samples: usize,
    pub wall: Duration,
    /// CPU seconds between the first and the last sample (the kernel's `utime+stime` delta).
    pub cpu_seconds: f64,
    pub rss_first_kib: u64,
    pub rss_med_kib: u64,
    pub rss_mean_kib: u64,
    pub rss_peak_kib: u64,
    /// The kernel's peak: what `time -v` prints as maximum resident set size.
    pub vmhwm_kib: u64,
    pub stopped_samples: usize,
}

impl ProcUsage {
    pub fn line(&self) -> String {
        format!(
            "samples={} wall={:.2}s cpu={:.3}s rss_first={}KiB rss_med={}KiB rss_mean={}KiB \
             rss_peak={}KiB vmhwm={}KiB stopped={}",
            self.samples,
            self.wall.as_secs_f64(),
            self.cpu_seconds,
            self.rss_first_kib,
            self.rss_med_kib,
            self.rss_mean_kib,
            self.rss_peak_kib,
            self.vmhwm_kib,
            self.stopped_samples,
        )
    }
}

/// Samples one process's `/proc` line on a fixed interval, from its own thread (a `SIGSTOP`ped
/// engine must not be able to starve the sampler, and the async runtime gains nothing from these
/// syscalls). The interval is a knob, not a wait: [`ProcUsage`] carries both the sampled peak and the
/// kernel's `VmHWM`, so a peak that falls between two samples is a *measured* error, and
/// `selftest_engineproc_sampler_vs_time` is what keeps the interval honest.
pub struct Sampler {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<JoinHandle<Vec<Sample>>>,
}

impl Sampler {
    pub fn start(pid: u32, interval: Duration) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let thread = std::thread::Builder::new()
            .name(format!("lv-proc-sample-{pid}"))
            .spawn(move || {
                let mut v = Vec::new();
                let t0 = Instant::now();
                while !stop2.load(Ordering::Relaxed) {
                    if let Ok(stat) = read_proc(pid) {
                        v.push(Sample {
                            at: t0.elapsed(),
                            stat,
                        });
                    }
                    std::thread::sleep(interval);
                }
                v
            })
            .expect("spawn the /proc sampler");
        Sampler {
            stop,
            thread: Some(thread),
        }
    }

    /// Stops the thread and reduces what it saw. A process that died mid-run simply stops
    /// contributing samples.
    pub fn finish(mut self) -> ProcUsage {
        self.stop.store(true, Ordering::Relaxed);
        let samples = self
            .thread
            .take()
            .expect("sampler thread")
            .join()
            .unwrap_or_default();
        reduce_samples(samples)
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        // Dropped without `finish` (the engine died, a scenario unwound before `stop_sampler`): tell
        // the thread to leave, so it does not keep pushing samples nobody will ever join.
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// The reduction of a sample list, split out so the selftest can feed it a fixture.
pub fn reduce_samples(samples: Vec<Sample>) -> ProcUsage {
    let Some(first) = samples.first() else {
        return ProcUsage {
            samples: 0,
            wall: Duration::ZERO,
            cpu_seconds: 0.0,
            rss_first_kib: 0,
            rss_med_kib: 0,
            rss_mean_kib: 0,
            rss_peak_kib: 0,
            vmhwm_kib: 0,
            stopped_samples: 0,
        };
    };
    let (first, last) = (*first, *samples.last().expect("non-empty"));
    let mut rss: Vec<u64> = samples.iter().map(|s| s.stat.rss_kib).collect();
    rss.sort_unstable();
    let sum: u128 = rss.iter().map(|v| *v as u128).sum();
    ProcUsage {
        samples: samples.len(),
        wall: last.at,
        cpu_seconds: last.stat.cpu_ticks.saturating_sub(first.stat.cpu_ticks) as f64
            / TICKS_PER_SEC,
        rss_first_kib: first.stat.rss_kib,
        rss_med_kib: rss[rss.len() / 2],
        rss_mean_kib: (sum / rss.len() as u128) as u64,
        rss_peak_kib: *rss.last().expect("non-empty"),
        vmhwm_kib: samples.iter().map(|s| s.stat.vmhwm_kib).max().unwrap_or(0),
        stopped_samples: samples.iter().filter(|s| s.stat.state == 'T').count(),
    }
}

// ---------------------------------------------------------------------------------------------
// The text `/metrics` serves
// ---------------------------------------------------------------------------------------------

/// One `name{labels} value` line of the exposition format.
#[derive(Debug, Clone, PartialEq)]
pub struct PromSample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

impl PromSample {
    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Reads the text `/metrics` serves: `#` lines are skipped, the rest is
/// `name[{k="v",...}] value`. Strict on shape — a line that is neither is a bug in
/// `src/health/prometheus.rs` or here, and both are worth a failure. Prometheus's full grammar
/// (`+Inf`, `NaN`, UTF-8 label names) is not needed for what that endpoint writes. Pure:
/// `selftest_engineproc_prom_parse` covers it without a server.
pub fn parse_prometheus(text: &str) -> Result<Vec<PromSample>> {
    let mut out = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let n = i + 1;
        let (left, value) = line
            .rsplit_once(' ')
            .with_context(|| format!("line {n}: no value: {line}"))?;
        let value = value
            .parse::<f64>()
            .with_context(|| format!("line {n}: bad value `{value}`"))?;
        let (name, labels) = match left.split_once('{') {
            Some((name, rest)) => (
                name,
                parse_labels(
                    rest.strip_suffix('}')
                        .with_context(|| format!("line {n}: unterminated labels: {line}"))?,
                    n,
                )?,
            ),
            None => (left, Vec::new()),
        };
        ensure!(!name.is_empty(), "line {n}: empty metric name");
        out.push(PromSample {
            name: name.to_string(),
            labels,
            value,
        });
    }
    Ok(out)
}

/// `k="v",k2="w"` → pairs; values are quoted and escape `"` and `\` (`escape_label` in
/// `src/health/prometheus.rs`).
fn parse_labels(s: &str, line: usize) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && (b[i] == b',' || b[i].is_ascii_whitespace()) {
            i += 1;
        }
        let start = i;
        while i < b.len() && b[i] != b'=' {
            i += 1;
        }
        ensure!(
            i < b.len() && i > start,
            "line {line}: label without `=`: {s}"
        );
        let key = s[start..i].trim().to_string();
        i += 1;
        ensure!(
            i < b.len() && b[i] == b'"',
            "line {line}: unquoted value of `{key}`"
        );
        i += 1;
        let mut val = String::new();
        loop {
            ensure!(i < b.len(), "line {line}: unterminated value of `{key}`");
            match b[i] {
                b'\\' => {
                    ensure!(i + 1 < b.len(), "line {line}: dangling escape in `{key}`");
                    // `escape_label` emits `\\`, `\"` and `\n`; the other escapes are the byte.
                    val.push(match s.as_bytes()[i + 1] {
                        b'n' => '\n',
                        other => other as char,
                    });
                    i += 2;
                }
                b'"' => {
                    i += 1;
                    break;
                }
                c => {
                    // The plain byte — and it must advance: a `push` without `i += 1` here re-reads
                    // the same byte forever (the 2026-11-18 OOM: the first quoted label value of a
                    // real `/metrics` answer grew this String to gigabytes until the kernel struck).
                    val.push(c as char);
                    i += 1;
                }
            }
        }
        out.push((key, val));
    }
    Ok(out)
}

/// One scrape of `/metrics`: read values out of it, or difference it against an earlier one.
#[derive(Debug, Clone)]
pub struct Scraped {
    pub at: Instant,
    pub samples: Vec<PromSample>,
}

impl Scraped {
    pub fn parse(at: Instant, text: &str) -> Result<Self> {
        Ok(Scraped {
            at,
            samples: parse_prometheus(text)?,
        })
    }

    fn find(&self, name: &str, label: Option<(&str, &str)>) -> Option<&PromSample> {
        self.samples
            .iter()
            .find(|s| s.name == name && label.is_none_or(|(k, v)| s.label(k) == Some(v)))
    }

    /// Any metric by name, the `lazyview_` prefix optional; 0.0 when the endpoint does not carry it.
    pub fn gauge(&self, name: &str) -> f64 {
        let bare = name.strip_prefix("lazyview_").unwrap_or(name);
        [format!("lazyview_{bare}"), bare.to_string()]
            .iter()
            .find_map(|c| self.find(c, None))
            .map_or(0.0, |s| s.value)
    }

    /// A counter, in the endpoint's shape: `lazyview_<name>_total`.
    pub fn counter(&self, name: &str) -> u64 {
        self.gauge(&format!("{name}_total")).round().max(0.0) as u64
    }

    /// A labelled metric: `("applied_binlog_pos", "file", "binlog.000001")`.
    pub fn labelled(&self, name: &str, key: &str, value: &str) -> Option<f64> {
        let bare = name.strip_prefix("lazyview_").unwrap_or(name);
        self.find(&format!("lazyview_{bare}"), Some((key, value)))
            .map(|s| s.value)
    }

    /// The applied binlog position (`Metrics::applied`), once the engine has applied anything.
    pub fn applied_position(&self) -> Option<Position> {
        let s = self.find("lazyview_applied_binlog_pos", None)?;
        Some(Position {
            file: s.label("file")?.to_string(),
            pos: s.value as u64,
        })
    }

    /// `self - earlier` for one counter, saturating (a fresh process's counter below an old one is
    /// what a restart looks like).
    pub fn delta(&self, earlier: &Scraped, name: &str) -> u64 {
        self.counter(name).saturating_sub(earlier.counter(name))
    }

    /// The engine's liveness shape: the gauges a kill point is chosen from, in one line.
    pub fn state_line(&self) -> String {
        format!(
            "streaming={} starting={} views={} views_building={} inflight_rows={} standby={} \
             heartbeat_age={:.1}s",
            self.gauge("streaming"),
            self.gauge("starting"),
            self.gauge("views"),
            self.gauge("views_building"),
            self.gauge("inflight_rows"),
            self.gauge("standby"),
            self.gauge("heartbeat_age_seconds"),
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Health endpoints
// ---------------------------------------------------------------------------------------------

/// One `GET` on a short-lived connection (`Connection: close`, as `src/health/server.rs` answers),
/// with the bearer token when the endpoint is gated: `/metrics` and `/stats` answer 401 without it,
/// `/livez`, `/startupz` and `/readyz` are open (`src/health/mod.rs`, "Who may look").
pub async fn http_get(addr: &str, path: &str, token: Option<&str>) -> Result<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let auth = token.map_or_else(String::new, |t| format!("Authorization: Bearer {t}\r\n"));
    let mut s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .with_context(|| format!("GET {addr}{path}: connect timeout"))?
        .with_context(|| format!("GET {addr}{path}: connect"))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n{auth}\r\n");
    tokio::time::timeout(Duration::from_secs(5), s.write_all(req.as_bytes()))
        .await
        .with_context(|| format!("GET {addr}{path}: write timeout"))?
        .with_context(|| format!("GET {addr}{path}: write"))?;
    // Bounded read (MAX_RESPONSE_BYTES): chunk by chunk so a server that streams forever can grow
    // the buffer to the cap and no further; `read_to_end` had no such ceiling.
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut chunk))
            .await
            .with_context(|| format!("GET {addr}{path}: read timeout"))?
            .with_context(|| format!("GET {addr}{path}: read"))?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        ensure!(
            buf.len() <= MAX_RESPONSE_BYTES,
            "GET {addr}{path}: answer passed the {MAX_RESPONSE_BYTES} B cap (at {} B): a health \
             endpoint must answer and close on Connection: close — reading on would OOM the harness",
            buf.len()
        );
    }
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .with_context(|| format!("GET {addr}{path}: no header terminator in {text}"))?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .with_context(|| format!("GET {addr}{path}: unparsable status line: {head}"))?;
    Ok((status, body.to_string()))
}

/// A free loopback port for a spawned engine's health endpoint (bind `:0`, read it, release it). The
/// race it leaves costs a spawned engine a failed bind, which `src/health/server.rs` retries every
/// 5 s and the startup wait outlives.
pub fn free_port() -> Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").context("bind a free loopback port")?;
    Ok(l.local_addr()
        .context("local_addr of the free port")?
        .port())
}

/// A per-engine metrics token, so a spawned process proves the `LAZYVIEW_HEALTH_TOKEN` gate instead
/// of running with an open `/metrics`.
pub fn health_token(seed: u64) -> String {
    let mut r = Rng::new(seed ^ 0x005e_ed17);
    (0..8).map(|_| format!("{:08x}", r.next_u32())).collect()
}

// ---------------------------------------------------------------------------------------------
// Bounded file reads and the harness's own accounting
// ---------------------------------------------------------------------------------------------

/// The most bytes [`tail_lines`] reads from the end of a file. Engine logs are streamed to disk at
/// spawn (`Stdio::from(file)`), so their size is the engine's business; *reporting* a failure must
/// never pay that size in memory — a `read_to_string` of a runaway log is the second way the
/// 2026-11-18 harness OOMed itself.
const TAIL_READ_BYTES: u64 = 256 * 1024;

/// The last `lines` lines of `path`, reading at most the last [`TAIL_READ_BYTES`] bytes. A missing
/// or unreadable file has no tail (empty string).
pub fn tail_lines(path: &Path, lines: usize) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(len) = std::fs::metadata(path).map(|m| m.len()) else {
        return String::new();
    };
    let take = len.min(TAIL_READ_BYTES) as usize;
    let mut buf = vec![0u8; take];
    let Ok(mut f) = std::fs::File::open(path) else {
        return String::new();
    };
    if f.seek(SeekFrom::End(-(take as i64))).is_err() || f.read_exact(&mut buf).is_err() {
        return String::new();
    }
    let text = String::from_utf8_lossy(&buf);
    let v: Vec<&str> = text.lines().rev().take(lines).collect();
    v.into_iter().rev().collect::<Vec<_>>().join("\n")
}

/// The harness process's own peak RSS (`VmHWM` of `/proc/self/status`), in MiB. The engineproc
/// tests print it so a green run *proves* the harness stayed under its memory budget (the engine
/// child is measured separately, by the sampler). `/proc` status files are kernel-generated and
/// small; reading one whole is bounded.
pub fn self_vmhwm_mib() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| {
            v.trim()
                .strip_suffix("kB")
                .unwrap_or(v.trim())
                .trim()
                .parse::<u64>()
                .ok()
        })
        .map_or(0, |kib| kib / 1024)
}

/// Prints a measured number past the test harness's stdout capture (stderr, like `skip_scenario`),
/// so the times and RSS a selftest records are in the suite log of a *green* run, not only on
/// failure. One line per fact: `MEASURED <what> <value>`.
pub fn measured(what: impl AsRef<str>) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "MEASURED {}", what.as_ref());
}

// ---------------------------------------------------------------------------------------------
// Kill points
// ---------------------------------------------------------------------------------------------

/// SplitMix64, so a kill sequence is reproducible from the printed seed and independent of a `rand`
/// version's algorithm (the harness's sub-seeds use the same generator).
#[derive(Debug, Clone, Copy)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// `0..n`, by multiplication (no modulo bias at these sizes).
    pub fn below(&mut self, n: usize) -> usize {
        ((self.next_u64() >> 33) as usize) % n.max(1)
    }
}

/// The instants a kill is worth landing, from the plan (§2 M1: startup/connect, setup/migration,
/// first build, catch-up, steady state, position save, a swap, shutdown). Each name is the
/// *observable* the caller waits for before it pulls the trigger, which is what turns "killed at any
/// instant" into something reproducible from a seed. The waits themselves are the recovery suite's
/// (`tests/common/recovery.rs`); this module proves the picker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillPoint {
    /// After spawn, before `/startupz` ever answered: config parse, first connect, metadata check.
    Startup,
    /// The setup and migration run (`lazyview setup`) instead of the operations run.
    Setup,
    /// A view is building: `lazyview_views_building > 0` (the "a build restarts from zero" case).
    FirstBuild,
    /// Streaming with a backlog: the applied position is behind the server's.
    CatchUp,
    /// Streaming, nothing in flight, the applied position caught up.
    Steady,
    /// Streaming, and the applied position has advanced since spawn (a position save has run).
    PositionSave,
    /// A swap is pending with the target transaction open. Only the caller can induce it (it holds
    /// the blocking lock itself), so the picker only schedules it.
    Swap,
    /// Stop rather than kill: a graceful shutdown after the point of interest.
    Shutdown,
}

impl KillPoint {
    pub fn all() -> [KillPoint; 8] {
        [
            KillPoint::Startup,
            KillPoint::Setup,
            KillPoint::FirstBuild,
            KillPoint::CatchUp,
            KillPoint::Steady,
            KillPoint::PositionSave,
            KillPoint::Swap,
            KillPoint::Shutdown,
        ]
    }

    pub fn name(self) -> &'static str {
        match self {
            KillPoint::Startup => "startup",
            KillPoint::Setup => "setup",
            KillPoint::FirstBuild => "first-build",
            KillPoint::CatchUp => "catch-up",
            KillPoint::Steady => "steady",
            KillPoint::PositionSave => "position-save",
            KillPoint::Swap => "swap",
            KillPoint::Shutdown => "shutdown",
        }
    }
}

/// A seeded sequence of kill points: a shuffled deck of [`KillPoint::all`], dealt without
/// repetition and reshuffled when empty. A crash loop's `K` kills then cover every point before any
/// repeats, which is what makes a failure attributable, and the same seed gives the same sequence in
/// every run and on every flavor (the seed is printed in the test's header).
#[derive(Debug, Clone)]
pub struct KillPointPlan {
    rng: Rng,
    deck: Vec<KillPoint>,
}

impl KillPointPlan {
    pub fn new(seed: u64) -> Self {
        let mut p = KillPointPlan {
            rng: Rng::new(seed),
            deck: Vec::new(),
        };
        p.shuffle();
        p
    }

    fn shuffle(&mut self) {
        self.deck = KillPoint::all().to_vec();
        for i in (1..self.deck.len()).rev() {
            let j = self.rng.below(i + 1);
            self.deck.swap(i, j);
        }
    }

    pub fn next(&mut self) -> KillPoint {
        if self.deck.is_empty() {
            self.shuffle();
        }
        self.deck.pop().expect("a non-empty deck")
    }

    /// The next `n` points without consuming the plan (what a test prints in its header).
    pub fn peek(&self, n: usize) -> Vec<KillPoint> {
        let mut c = self.clone();
        (0..n).map(|_| c.next()).collect()
    }
}

// ---------------------------------------------------------------------------------------------
// The process handle
// ---------------------------------------------------------------------------------------------

/// An engine running as its own process, with the knobs a deployment has: env configuration, a
/// loopback health endpoint, a metrics token, real signals.
///
/// `Drop` tears the process down (SIGKILL, then reap), so no path — panic, `?`, scenario timeout —
/// leaves an engine holding the instance lock on a server every suite shares.
pub struct EngineProc {
    pub label: String,
    pub health_addr: String,
    pub token: String,
    pub log_path: PathBuf,
    /// When the process was created. Every duration a recovery table prints is measured from here
    /// (owner's ruling: "process start" is the health port answering, and spawn→answer is the other
    /// half of that number).
    pub spawned_at: Instant,
    child: Child,
    exit: Option<ExitStatus>,
    sampler: Option<Sampler>,
}

impl EngineProc {
    pub async fn start(ctx: &Ctx, db: &Db, overrides: &[(&str, &str)]) -> Result<Self> {
        Self::start_with(ctx, db, overrides, &[]).await
    }

    /// Spawns the binary against `db` with the harness's test defaults — the same ones
    /// `Ctx::start_engine` uses for the in-process engine — plus `overrides` and `args`
    /// (`["setup"]` for the setup and migration run).
    ///
    /// It waits for nothing: a caller that means to kill the engine during boot calls this and kills,
    /// while [`Self::wait_startup`] is the normal next step. Lazyview's own tables are created first
    /// the way the initContainer does, unless `LAZYVIEW_TEST_SKIP_SETUP=1` leaves the schema alone
    /// (which is how `KillPoint::Setup` is reached).
    pub async fn start_with(
        ctx: &Ctx,
        db: &Db,
        overrides: &[(&str, &str)],
        args: &[&str],
    ) -> Result<Self> {
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        let dsn = match std::env::var("LAZYVIEW_TEST_ENGINE_USER") {
            Ok(u) if !u.is_empty() => dsn_with_user(&db.dsn, &u),
            _ => db.dsn.clone(),
        };
        let port = free_port()?;
        let token = health_token(ctx.sub_seed(&format!("token-{}", db.name)));
        env.insert("LAZYVIEW_DSN".into(), dsn);
        env.insert("LAZYVIEW_SERVER_ID".into(), unique_server_id().to_string());
        env.insert("LAZYVIEW_INSTANCE_ID".into(), "test".into());
        env.insert("LAZYVIEW_BATCH_WINDOW_MS".into(), "50".into());
        env.insert("LAZYVIEW_HEARTBEAT_MS".into(), "1000".into());
        env.insert("LAZYVIEW_HEALTH_ADDR".into(), format!("127.0.0.1:{port}"));
        env.insert("LAZYVIEW_HEALTH_TOKEN".into(), token.clone());
        env.insert("LAZYVIEW_LOG_FORMAT".into(), "text".into());
        env.insert(
            "RUST_LOG".into(),
            std::env::var("LAZYVIEW_TEST_LOG").unwrap_or_else(|_| "lazyview=info,warn".into()),
        );
        // The CI leg's supervisor mode reaches every engine unless a test sets its own.
        if let Ok(v) = std::env::var("LAZYVIEW_SUPERVISOR") {
            env.insert("LAZYVIEW_SUPERVISOR".into(), v);
        }
        for (k, v) in overrides {
            env.insert((*k).to_string(), (*v).to_string());
        }
        if env.get("LAZYVIEW_TEST_SKIP_SETUP").map(|v| v.as_str()) != Some("1") {
            setup_run(db).await?;
        }

        let bin = engine_binary()?;
        let seq = PROC_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = logs_root().join(&db.name);
        std::fs::create_dir_all(&dir).ok();
        let log_path = dir.join(format!("engineproc-{}-{seq}.log", db.flavor));
        let file = std::fs::File::create(&log_path)
            .with_context(|| format!("create {}", log_path.display()))?;
        ctx.log(format!(
            "start engine-as-process {}#{seq} bin={} health=127.0.0.1:{port} args={:?} {} log={}",
            db.flavor,
            bin.display(),
            args,
            overrides
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(" "),
            log_path.display(),
        ));
        let mut cmd = Command::new(&bin);
        cmd.args(args)
            .env_clear()
            // A minimal, predictable world: the child gets nothing of the test's own configuration
            // beyond the explicit env built above.
            .env(
                "PATH",
                std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
            )
            .env("HOME", "/")
            .env("LANG", "C.UTF-8")
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::from(
                file.try_clone()
                    .with_context(|| format!("clone {}", log_path.display()))?,
            ))
            .stderr(Stdio::from(file));
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn {} {:?}", bin.display(), args))?;
        Ok(EngineProc {
            label: format!("{}#{}", db.flavor, seq),
            health_addr: format!("127.0.0.1:{port}"),
            token,
            log_path,
            spawned_at: Instant::now(),
            child,
            exit: None,
            sampler: None,
        })
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn exit_status(&self) -> Option<ExitStatus> {
        self.exit
    }

    /// The tail of the engine's own log, for failure messages. Reads at most the last
    /// [`TAIL_READ_BYTES`] of the file ([`tail_lines`]) — never the whole log: the failure report
    /// that loaded a runaway log into memory is what OOMed the harness on 2026-11-18.
    pub fn log_tail(&self, lines: usize) -> String {
        tail_lines(&self.log_path, lines)
    }

    // ---- endpoints --------------------------------------------------------------------------

    pub async fn get(&self, path: &str, authorized: bool) -> Result<(u16, String)> {
        http_get(
            &self.health_addr,
            path,
            if authorized { Some(&self.token) } else { None },
        )
        .await
    }

    /// Polls an open probe until it answers 200 and returns the time **from the spawn**. The wait
    /// ends on the observable, never on a pause; a trip names the last verdict and the log tail.
    pub async fn wait_probe(&self, path: &str, timeout: Duration) -> Result<Duration> {
        let t0 = Instant::now();
        let mut last;
        loop {
            match self.get(path, false).await {
                Ok((200, _)) => return Ok(self.spawned_at.elapsed()),
                Ok((code, body)) => last = format!("{code} {}", body.lines().next().unwrap_or("")),
                Err(e) => last = format!("{e:#}"),
            }
            ensure!(
                t0.elapsed() < timeout,
                "{path} never answered on {} within {timeout:?} (last: {last}); log tail:\n{}",
                self.label,
                self.log_tail(15)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn wait_startup(&self, timeout: Duration) -> Result<Duration> {
        self.wait_probe("/startupz", timeout).await
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<Duration> {
        self.wait_probe("/readyz", timeout).await
    }

    pub async fn scrape(&self) -> Result<Scraped> {
        let (code, body) = self.get("/metrics", true).await?;
        ensure!(code == 200, "/metrics answered {code} on {}", self.label);
        Scraped::parse(Instant::now(), &body)
    }

    /// Captures the source's binlog end and waits until the engine applied it, `rounds` times: the
    /// process-leg twin of `Engine::wait_caught_up_rounds` (the engine's own writes to an upstream
    /// view advance the binlog, so a chained view needs one more round per chain level). One
    /// `timeout` bounds all rounds.
    pub async fn wait_caught_up_rounds(
        &self,
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
            self.wait_scrape(left, &format!("applied position {target}"), |s| {
                s.applied_position().is_some_and(|p| p >= target)
            })
            .await?;
        }
        Ok(())
    }

    /// `/metrics` without the token: the gate's verdict (401 when `LAZYVIEW_HEALTH_TOKEN` is set).
    pub async fn scrape_unauthorized(&self) -> Result<u16> {
        Ok(self.get("/metrics", false).await?.0)
    }

    /// Waits until a scrape satisfies `want`; returns the time from the spawn and that scrape. Every
    /// kill point and every recovery measurement waits on this, so a test never guesses how long a
    /// state takes to appear.
    pub async fn wait_scrape<F>(
        &self,
        timeout: Duration,
        what: &str,
        want: F,
    ) -> Result<(Duration, Scraped)>
    where
        F: Fn(&Scraped) -> bool,
    {
        let t0 = Instant::now();
        let mut last;
        loop {
            match self.scrape().await {
                Ok(s) => {
                    if want(&s) {
                        return Ok((self.spawned_at.elapsed(), s));
                    }
                    last = s.state_line();
                }
                Err(e) => last = format!("{e:#}"),
            }
            ensure!(
                t0.elapsed() < timeout,
                "{} never reached {what} within {timeout:?} (last: {last}); log tail:\n{}",
                self.label,
                self.log_tail(15)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn wait_streaming(&self, timeout: Duration) -> Result<Duration> {
        Ok(self
            .wait_scrape(timeout, "streaming", |s| s.gauge("streaming") == 1.0)
            .await?
            .0)
    }

    // ---- the process ------------------------------------------------------------------------

    /// Starts the `/proc` sampler for the engine's lifetime (or until [`Self::stop_sampler`]).
    pub fn start_sampler(&mut self, interval: Duration) {
        self.sampler = Some(Sampler::start(self.pid(), interval));
    }

    /// Stops the sampler and returns what it saw.
    pub fn stop_sampler(&mut self) -> Result<ProcUsage> {
        let s = self
            .sampler
            .take()
            .with_context(|| format!("no sampler running on {}", self.label))?;
        Ok(s.finish())
    }

    /// Sends a signal. `SIGKILL` goes through `kill(2)` while we own the child (the same number, no
    /// program to spawn on the way to a crash test's most important instant); the rest through `kill`,
    /// which cannot be faked by a std API. A signal to a process that already left is an error worth
    /// reporting: a test that believes it froze a dead engine is measuring nothing.
    pub fn send(&mut self, sig: Signal) -> Result<()> {
        ensure!(
            self.exit.is_none(),
            "{} already exited: no {sig:?}",
            self.label
        );
        ensure!(
            pid_alive(self.pid()),
            "{} (pid {}) is gone",
            self.label,
            self.pid()
        );
        match sig {
            Signal::Kill => self
                .child
                .kill()
                .with_context(|| format!("kill(2) {} on {}", self.pid(), self.label)),
            s => signal(self.pid(), s),
        }
    }

    pub fn freeze(&mut self) -> Result<()> {
        self.send(Signal::Stop)
    }

    pub fn resume(&mut self) -> Result<()> {
        self.send(Signal::Cont)
    }

    pub fn kill_now(&mut self) -> Result<()> {
        self.send(Signal::Kill)
    }

    /// Waits for the process to leave. One that ignores the signal (a stopped process cannot die) is
    /// escalated to `SIGKILL` once and then waited out, so a crash test cannot hang the suite.
    pub fn wait_exit(&mut self, timeout: Duration) -> Result<ExitStatus> {
        if let Some(st) = self.exit {
            return Ok(st);
        }
        let t0 = Instant::now();
        let mut escalated = false;
        loop {
            match self.child.try_wait().context("try_wait")? {
                Some(st) => {
                    self.exit = Some(st);
                    self.sampler.take();
                    return Ok(st);
                }
                None if t0.elapsed() >= timeout => {
                    if escalated {
                        bail!(
                            "{} (pid {}) still alive {timeout:?} after the signal (state {:?}); \
                             log tail:\n{}",
                            self.label,
                            self.pid(),
                            proc_state(self.pid()),
                            self.log_tail(15)
                        );
                    }
                    escalated = true;
                    let _ = self.child.kill();
                }
                None => std::thread::sleep(Duration::from_millis(5)),
            }
        }
    }

    /// Stops the engine the way a deployment does: `SIGTERM`, escalating to `SIGKILL` when it does
    /// not leave within the grace.
    pub fn stop(&mut self) -> Result<ExitStatus> {
        if let Some(st) = self.exit {
            return Ok(st);
        }
        signal(self.pid(), Signal::Term)?;
        self.wait_exit(Duration::from_secs(10))
    }

    /// Waits until the kernel has no live process with this pid (a reaped child, or a process killed
    /// elsewhere and reaped by its own parent).
    pub fn wait_pid_gone(pid: u32, timeout: Duration) -> Result<()> {
        let t0 = Instant::now();
        while pid_alive(pid) {
            ensure!(
                t0.elapsed() < timeout,
                "pid {pid} is still alive after {timeout:?} (state {:?})",
                proc_state(pid)
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }
}

static PROC_SEQ: AtomicU32 = AtomicU32::new(1);

impl Drop for EngineProc {
    fn drop(&mut self) {
        // Never panic here: a panic in a destructor would turn a passing scenario into a confusing
        // double failure and could skip the harness's own cleanup.
        if self.exit.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.sampler.take();
    }
}

// ---------------------------------------------------------------------------------------------
// The sampler's own cross-check (`time -v`)
// ---------------------------------------------------------------------------------------------

/// What `time -v` reports for one process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GnuTime {
    pub user_s: f64,
    pub sys_s: f64,
    pub max_rss_kib: u64,
}

/// The path of GNU `time`, when the host has it. It is a **cross-check**: `getrusage`'s `ru_maxrss`
/// *is* the kernel's `VmHWM` and its user+sys *are* `/proc`'s `utime+stime`, so the selftest's
/// assertions hold without it and a host without it (this container, some images) still runs it.
pub fn gnu_time() -> Option<PathBuf> {
    let p = PathBuf::from("/usr/bin/time");
    if !p.is_file() {
        return None;
    }
    let ok = Command::new(&p)
        .args(["-v", "/bin/true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if ok {
        Some(p)
    } else {
        None
    }
}

/// Parses the three fields the harness compares; a locale or a version that renames them yields
/// `None`, which the caller reports rather than trusts.
pub fn parse_gnu_time(text: &str) -> Option<GnuTime> {
    let num = |label: &str| {
        text.lines()
            .find(|l| l.trim_start().starts_with(label))
            .and_then(|l| l.rsplit(':').next())
            .and_then(|v| v.trim().parse::<f64>().ok())
    };
    Some(GnuTime {
        user_s: num("User time (seconds)")?,
        sys_s: num("System time (seconds)")?,
        max_rss_kib: num("Maximum resident set size (kbytes)")? as u64,
    })
}

/// Waits for GNU `time` to fork the command it was given and returns that child's pid, or `None`
/// when `time` exited first.
///
/// There is deliberately **no deadline** here. `time` forks before its child can run, so the two
/// ways out of this loop are the two ways the run itself ends — a child to sample, or `time` done —
/// and `child.wait()` below waits for the second one anyway; a duration would only ever be a guess
/// at how long the runner may take to schedule this thread. It used to be a fixed 5 s, and a runner
/// that did not schedule it for tens of ms could let a short `sort` finish inside that: `time` had
/// exited, no child was left to find, and a **finished** run was reported as one that "never
/// forked" (#360).
///
/// Public because `selftest_engineproc_time_child_ends_both_ways` hands it an ordinary process and
/// asks: which of the two ends it takes is then the process's doing, not the runner's.
pub fn time_child(child: &mut Child) -> Result<Option<u32>> {
    loop {
        if let Some(p) = child_pids(child.id()).first().copied() {
            return Ok(Some(p));
        }
        // After the scan, not before: a child born and gone within one scan is a finished run too.
        if child
            .try_wait()
            .with_context(|| "ask whether `time` exited")?
            .is_some()
        {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The two facts a run under `time` owes its caller, checked one way for both outcomes below: `time`
/// exited 0, and its `-v` report parsed (a locale that renamed the fields, or a `time` that is not
/// GNU's, yields `None` from [`parse_gnu_time`] and is not a report to trust). `report_text` is what
/// its stderr held — `None` when the run never went under `time`, which reports no numbers.
fn time_run_report(
    program: &Path,
    status: ExitStatus,
    report_text: Option<String>,
) -> Result<Option<GnuTime>> {
    ensure!(status.success(), "{:?} exited with {status:?}", program);
    match report_text {
        Some(text) => {
            let parsed = parse_gnu_time(&text);
            ensure!(parsed.is_some(), "cannot read `time -v`'s report:\n{text}");
            Ok(parsed)
        }
        None => Ok(None),
    }
}

/// Builds the result [`run_measured`] gives for a `time -v` run whose child the fork wait **never
/// saw** — `time` had already exited when [`time_child`] scanned, so the program is gone (#360). It
/// is `Ok`, not an error, and carries two things:
///
/// * the **empty measurement** — 0 samples and every counter 0, `reduce_samples` of an empty list,
///   which is the same shape [`Sampler::finish`] reduces a child that died before the sampler's
///   first read to, so a caller cannot tell the two runs apart and its "too few samples, double the
///   input" loop reads both as "give me work";
/// * `time`'s own numbers, from the report on its stderr — proof the run was taken as finished and
///   its report read, not dropped on the floor as one that "never forked".
///
/// Split out of `run_measured` so `selftest_engineproc_finished_time_run_before_attach` can demand
/// this outcome from the step that produces it, with a report it hands in, instead of hoping a
/// runner lets `/bin/true` die before the scan catches it.
pub fn finished_time_run_result(
    program: &Path,
    status: ExitStatus,
    report_text: Option<String>,
) -> Result<(ProcUsage, Option<GnuTime>)> {
    let report = time_run_report(program, status, report_text)?;
    Ok((reduce_samples(Vec::new()), report))
}

/// Runs `program args...` under the sampler, and under `/usr/bin/time -v` when the host has it, so
/// **one** run yields both the kernel's report and the sampler's numbers for the *same* process (the
/// measured pid is `time`'s child when it runs, our own child otherwise). A sampler whose interval or
/// parsing were wrong shows up here immediately.
///
/// A program that finishes before `time`'s child can be seen — a short `sort`, or any program on a
/// runner slow to schedule the scan — is a run that is **over**, not one that never started: it
/// returns the empty measurement (0 samples) with `time`'s own numbers when `time` printed them, so
/// the caller decides whether the work was big enough to measure. [`finished_time_run_result`] builds
/// that result, and `selftest_engineproc_finished_time_run_before_attach` tests it directly;
/// `selftest_engineproc_finished_time_run` keeps the real two-process version of it ([`time_child`]).
pub fn run_measured(
    program: &Path,
    args: &[String],
    interval: Duration,
) -> Result<(ProcUsage, Option<GnuTime>)> {
    let gnu = gnu_time();
    let mut cmd = match &gnu {
        Some(t) => {
            let mut c = Command::new(t);
            c.arg("-v").arg(program).args(args);
            c
        }
        None => {
            let mut c = Command::new(program);
            c.args(args);
            c
        }
    };
    cmd.stdin(Stdio::null()).stdout(Stdio::null());
    cmd.stderr(if gnu.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {:?}", program))?;
    let target = match &gnu {
        // `time`'s child once it forks; no target at all once `time` has exited (#360).
        Some(_) => time_child(&mut child)?,
        None => Some(child.id()),
    };
    let sampler = target.map(|pid| Sampler::start(pid, interval));
    let status = child
        .wait()
        .with_context(|| format!("wait for {:?}", program))?;
    // `time`'s report is the only stderr piped; anything else the program wrote went to /dev/null.
    let report_text = match (&gnu, child.stderr.take()) {
        (Some(_), Some(mut err)) => {
            use std::io::Read;
            let mut text = String::new();
            err.read_to_string(&mut text).ok();
            Some(text)
        }
        _ => None,
    };
    match (sampler, report_text) {
        // No sampler means `time` was already gone before the first scan: the run is over, there was
        // nothing to sample, and the step that says so is the one the selftest drives by hand (#360).
        (None, text) => finished_time_run_result(program, status, text),
        (Some(s), text) => {
            let usage = s.finish();
            Ok((usage, time_run_report(program, status, text)?))
        }
    }
}
