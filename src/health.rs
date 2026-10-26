//! HTTP endpoint for Kubernetes probes, container health checks and Prometheus scraping.
//!
//! | path | probe | 200 when | 503 when |
//! |---|---|---|---|
//! | `/livez` (alias `/healthz`) | liveness | the engine loop made progress recently | the process is wedged |
//! | `/startupz` | startup | the process serves and the engine loop makes progress (as `/livez`) | the same |
//! | `/readyz` | readiness | active, streaming, every view has a live version | see below |
//! | `/metrics` | — | always (Prometheus text format) | — |
//! | `/stats` | — | always (JSON: per-view update statistics, see src/stats.rs) | — |
//!
//! `?verbose` lists every check (Kubernetes apiserver style), `?format=json` or
//! `Accept: application/json` returns JSON. `HEAD` is answered like `GET` without a body.
//!
//! # Semantics (and why)
//!
//! **Liveness** fails only when a restart is the right remedy: the engine loop has not reported
//! progress (`Metrics::beat` / `gauges.heartbeat_unix`) for `LAZYVIEW_LIVENESS_STALL_SECS`
//! (default 120, at least 30). The engine beats on every loop iteration (an idle source still
//! sends binlog heartbeats every `LAZYVIEW_HEARTBEAT_MS`), between flush/build chunks, while
//! retrying database connections and while waiting for the instance lock, so long view builds,
//! long chunked flushes, binlog lag and hot standby never fail it. A missing beat means an await
//! that never returns (a hung statement on a half-open connection, a deadlock), which a restart
//! does fix. Nothing that depends on the database contents or on user definitions is part of
//! liveness: restarting would not help and would only add a takeover or a rebuild.
//!
//! While the engine **boots** (`gauges.starting` and not standby: connecting, migrating metadata,
//! and after taking the lock loading definitions, resuming views and opening the first binlog
//! stream) the limit is `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` (default 900) instead. Some boot
//! steps do not report progress yet, a connect to an unreachable host can take ~130 s (TCP SYN
//! retries), and a hot standby boots again after it took over, long after any Kubernetes
//! startupProbe has passed: without this, a slow takeover would be killed and hand over again.
//!
//! **Startup** (`/startupz`) has the same verdict as `/livez` (the boot budget lives in liveness,
//! so it also covers the boot after a takeover). It deliberately does not wait for the database:
//! a startupProbe that needs the database restarts pods during a database outage, which a restart
//! does not fix. `?verbose` shows the boot phase.
//!
//! **Readiness** answers "is this instance maintaining fresh views right now". It fails while
//! * the engine loop is stalled (as liveness);
//! * hot standby: another replica holds the instance lock (`gauges.standby`) — unless
//!   `?standby=ok`, which counts a healthy standby as ready (the Kubernetes readinessProbe uses
//!   this: a singleton workload whose standby is never Ready deadlocks `RollingUpdate` rollouts
//!   and makes every PodDisruptionBudget block node drains);
//! * starting: this instance holds no lock yet, or has not connected the binlog stream yet;
//! * the binlog stream has been disconnected for longer than
//!   `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (default 30; short reconnects do not flap);
//! * a view has no live version yet (first build) — unless `?building=ok` (the Kubernetes probe
//!   uses it: an active pod that streams and builds is the pod to keep; reporting it unready
//!   marks the Deployment unavailable for the whole build, blocks drains of the standby's node
//!   through the PodDisruptionBudget and makes the ReplicaSet delete the builder first). A view
//!   whose live table is served while a new version builds in the background always counts as live;
//! * a view is in error, **only** with `LAZYVIEW_READY_REQUIRE_ALL=true` (or `?require_all=1`).
//!   By default error views are reported (body, `lazyview_views_error`, `_lazyview_state`) but
//!   ignored: an invalid user definition is not a property of the pod, and an unready pod would
//!   only block rollouts and drains without fixing anything. Alert on `lazyview_views_error`.
//!
//! # Server
//!
//! Tiny on purpose (tokio only): one request per connection (`Connection: close`), request heads
//! up to 8 KiB read within 2 s (slow clients get `408`), unspecified addresses (`0.0.0.0` /
//! `[::]`) listen on IPv4 and IPv6, and a failed bind is retried every 5 s without affecting the
//! engine. Connections are accepted without waiting for a free slot. At 512 open connections
//! a new one replaces an idle one: first one that sent nothing for 100 ms, from the client
//! address holding the most connections, oldest first. Kubelet probes send their request right
//! after connecting, so idle connections from other clients (a slow-loris) cannot starve them.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::binlog::Position;
use crate::metrics::{unix_now, Metrics};

pub const DEFAULT_STALL_SECS: u64 = 120;
pub const DEFAULT_BOOT_STALL_SECS: u64 = 900;
pub const DEFAULT_DISCONNECT_GRACE_SECS: u64 = 30;
/// Smallest accepted `LAZYVIEW_LIVENESS_STALL_SECS`: the engine beats every few seconds (standby
/// polls ≤ 5 s, binlog heartbeats), lower values would flap.
pub const MIN_STALL_SECS: u64 = 30;
const DEFAULT_HEARTBEAT_MS: u64 = 5_000;

const HEAD_TIMEOUT: Duration = Duration::from_secs(2);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HEAD: usize = 8 * 1024;
const MAX_CONNECTIONS: usize = 512;
/// A connection that sent nothing for this long is idle (a probe sends its request at once).
const SILENT_GRACE: Duration = Duration::from_millis(100);
/// Accept queue length (the kernel caps it at `net.core.somaxconn`).
const LISTEN_BACKLOG: u32 = 4096;
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
const REBIND_EVERY: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const BUSY: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

// ------------------------------------------------------------------------------------ settings

/// Probe policy from `LAZYVIEW_LIVENESS_STALL_SECS`, `LAZYVIEW_LIVENESS_BOOT_STALL_SECS`,
/// `LAZYVIEW_READY_REQUIRE_ALL` and `LAZYVIEW_READY_DISCONNECT_GRACE_SECS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Liveness fails after this many seconds without an engine heartbeat.
    pub stall_secs: u64,
    /// The same while the engine boots (never below `stall_secs`).
    pub boot_stall_secs: u64,
    /// Readiness also requires that no view is in error.
    pub require_all: bool,
    /// Readiness tolerates a disconnected binlog stream for this long.
    pub disconnect_grace_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            stall_secs: DEFAULT_STALL_SECS,
            boot_stall_secs: DEFAULT_BOOT_STALL_SECS,
            require_all: false,
            disconnect_grace_secs: DEFAULT_DISCONNECT_GRACE_SECS,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Settings> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Strict: every invalid variable is named in the error (for configuration validation).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings> {
        let (s, errors) = Self::from_lookup_lenient(get);
        if !errors.is_empty() {
            bail!("{}", errors.join("; "));
        }
        Ok(s)
    }

    /// Keeps every valid variable; an invalid one falls back to its own default and is reported.
    pub fn from_lookup_lenient(get: impl Fn(&str) -> Option<String>) -> (Settings, Vec<String>) {
        let d = Settings::default();
        let mut errors = Vec::new();
        let val = |k: &str| {
            get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let mut num = |k: &str, min: u64| -> Option<u64> {
            let v = val(k)?;
            match v.parse::<u64>() {
                Ok(n) if n >= min => Some(n),
                Ok(n) => {
                    errors.push(format!(
                        "{k} must be at least {min}, got {n}; using the default"
                    ));
                    None
                }
                Err(_) => {
                    errors.push(format!(
                        "{k} must be a whole number of seconds, got {v:?}; using the default"
                    ));
                    None
                }
            }
        };
        let explicit_stall = num("LAZYVIEW_LIVENESS_STALL_SECS", MIN_STALL_SECS);
        let boot = num("LAZYVIEW_LIVENESS_BOOT_STALL_SECS", 1);
        let grace = num("LAZYVIEW_READY_DISCONNECT_GRACE_SECS", 0);
        // An idle source wakes the engine loop only with binlog heartbeats.
        let heartbeat_secs = val("LAZYVIEW_HEARTBEAT_MS")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_HEARTBEAT_MS)
            .max(100)
            .div_ceil(1000);
        let min_for_heartbeat = 2 * heartbeat_secs + 10;
        let stall_secs = match explicit_stall {
            Some(s) if s < min_for_heartbeat => {
                errors.push(format!(
                    "LAZYVIEW_LIVENESS_STALL_SECS must be at least {min_for_heartbeat} with LAZYVIEW_HEARTBEAT_MS={} \
                     (an idle source only wakes the engine with binlog heartbeats), got {s}; using {min_for_heartbeat}",
                    heartbeat_secs * 1000
                ));
                min_for_heartbeat
            }
            Some(s) => s,
            None => d.stall_secs.max(min_for_heartbeat),
        };
        let require_all = match val("LAZYVIEW_READY_REQUIRE_ALL") {
            None => d.require_all,
            Some(v) => parse_bool(&v).unwrap_or_else(|| {
                errors.push(format!(
                    "LAZYVIEW_READY_REQUIRE_ALL must be true or false, got {v:?}; using false"
                ));
                d.require_all
            }),
        };
        let s = Settings {
            stall_secs,
            boot_stall_secs: boot.unwrap_or(d.boot_stall_secs).max(stall_secs),
            require_all,
            disconnect_grace_secs: grace.unwrap_or(d.disconnect_grace_secs),
        };
        (s, errors)
    }
}

fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

// ------------------------------------------------------------------------------------ state

/// One reading of everything the probes depend on: the engine's gauges plus what the health
/// server tracks itself (sampled every second and on every request).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observed {
    pub now: u64,
    /// Unix seconds of the engine's last heartbeat.
    pub heartbeat: u64,
    pub streaming: bool,
    pub starting: bool,
    pub standby: bool,
    pub views: u64,
    pub views_ready: u64,
    pub views_building: u64,
    pub views_error: u64,
    /// Unix seconds the binlog stream was last seen connected (0 = never).
    pub stream_seen: u64,
    /// Unix seconds the health server started watching.
    pub since: u64,
    /// Unix seconds the applied binlog position last changed (0 = none applied yet).
    pub applied_changed: u64,
}

impl Observed {
    pub fn heartbeat_age(&self) -> u64 {
        self.now.saturating_sub(self.heartbeat)
    }

    /// The engine is booting: before its first binlog connect, not waiting as hot standby
    /// (also the boot after a standby took over).
    pub fn booting(&self) -> bool {
        self.starting && !self.standby
    }

    /// Seconds without engine progress after which liveness fails.
    pub fn stall_limit(&self, s: &Settings) -> u64 {
        if self.booting() {
            s.stall_secs.max(s.boot_stall_secs)
        } else {
            s.stall_secs
        }
    }

    /// Seconds an instance that streamed before has been without a connected binlog stream
    /// (0 while streaming, on standby and before the first connect).
    pub fn disconnected_for(&self) -> u64 {
        if self.streaming || self.standby || self.stream_seen == 0 {
            0
        } else {
            self.now.saturating_sub(self.stream_seen)
        }
    }

    /// Holds the lock and has connected the binlog stream at least once.
    pub fn started(&self) -> bool {
        !self.standby && !self.starting && self.stream_seen > 0
    }

    /// Views with neither a live version nor an error (first build running).
    pub fn pending(&self) -> u64 {
        self.views
            .saturating_sub(self.views_ready)
            .saturating_sub(self.views_error)
    }

    pub fn role(&self) -> &'static str {
        if self.standby {
            "standby"
        } else if self.started() {
            "active"
        } else {
            "starting"
        }
    }
}

struct Tracker {
    since: u64,
    stream_seen: AtomicU64,
    applied: Mutex<(Option<Position>, u64)>,
}

impl Tracker {
    fn new() -> Tracker {
        Tracker {
            since: unix_now(),
            stream_seen: AtomicU64::new(0),
            applied: Mutex::new((None, 0)),
        }
    }

    fn observe(&self, m: &Metrics) -> Observed {
        let g = &m.gauges;
        let now = unix_now();
        let streaming = Metrics::get(&g.streaming) != 0;
        if streaming {
            self.stream_seen.fetch_max(now, Ordering::Relaxed);
        }
        let applied_changed = {
            let cur = m.applied();
            let mut a = self.applied.lock().unwrap_or_else(|e| e.into_inner());
            if cur != a.0 {
                *a = (cur, now);
            }
            if a.0.is_some() {
                a.1
            } else {
                0
            }
        };
        Observed {
            now,
            heartbeat: Metrics::get(&g.heartbeat_unix),
            streaming,
            starting: Metrics::get(&g.starting) != 0,
            standby: Metrics::get(&g.standby) != 0,
            views: Metrics::get(&g.views),
            views_ready: Metrics::get(&g.views_ready),
            views_building: Metrics::get(&g.views_building),
            views_error: Metrics::get(&g.views_error),
            stream_seen: self.stream_seen.load(Ordering::Relaxed),
            since: self.since,
            applied_changed,
        }
    }
}

// ------------------------------------------------------------------------------------ probes

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    Live,
    Ready,
    Startup,
}

impl Probe {
    pub fn name(self) -> &'static str {
        match self {
            Probe::Live => "livez",
            Probe::Ready => "readyz",
            Probe::Startup => "startupz",
        }
    }
}

/// Per-request readiness options (query string).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadyOpts {
    /// `?standby=ok`: a healthy hot standby counts as ready.
    pub standby_ok: bool,
    /// `?building=ok`: views still building their first version do not fail readiness.
    pub building_ok: bool,
    /// `?require_all=1|0`: overrides `LAZYVIEW_READY_REQUIRE_ALL`.
    pub require_all: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

fn check(name: &'static str, ok: bool, detail: impl Into<String>) -> Check {
    Check {
        name,
        ok,
        detail: detail.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub probe: Probe,
    pub ok: bool,
    pub role: &'static str,
    pub checks: Vec<Check>,
    /// Error views ignored for readiness (reported in the summary).
    pub ignored_errors: u64,
    /// Views without a live version ignored for readiness (`?building=ok`).
    pub ignored_building: u64,
}

impl Report {
    /// One line: the verdict, or the first failing check.
    pub fn summary(&self) -> String {
        if let Some(c) = self.checks.iter().find(|c| !c.ok) {
            return match self.probe {
                Probe::Live | Probe::Startup => c.detail.clone(),
                Probe::Ready => format!("not ready: {}", c.detail),
            };
        }
        match self.probe {
            Probe::Live | Probe::Startup => "ok".into(),
            Probe::Ready => {
                let mut s = String::from("ready");
                if self.role == "standby" {
                    s.push_str(" (hot standby)");
                }
                if self.ignored_building > 0 {
                    let _ = write!(
                        s,
                        "; {} view(s) building their first version",
                        self.ignored_building
                    );
                }
                if self.ignored_errors > 0 {
                    let _ = write!(
                        s,
                        "; {} view(s) in error, ignored (see _lazyview_state.last_error)",
                        self.ignored_errors
                    );
                }
                s
            }
        }
    }
}

/// Evaluates one probe against an observation (pure: unit-tested for every state).
pub fn evaluate(probe: Probe, o: &Observed, s: &Settings, opts: ReadyOpts) -> Report {
    let mut checks = Vec::new();
    let mut ignored_errors = 0;
    let mut ignored_building = 0;
    let age = o.heartbeat_age();
    let limit = o.stall_limit(s);
    let phase = if o.booting() { " while booting" } else { "" };
    checks.push(if age >= limit {
        check(
            "engine",
            false,
            format!("engine loop stalled: no progress for {age}s (limit {limit}s{phase})"),
        )
    } else {
        check(
            "engine",
            true,
            format!("engine loop progressing (last heartbeat {age}s ago, limit {limit}s{phase})"),
        )
    });
    match probe {
        Probe::Live => {}
        Probe::Startup => checks.push(check(
            "phase",
            true,
            match o.role() {
                "standby" => "hot standby".to_string(),
                "active" => "binlog stream connected".to_string(),
                _ => "booting: waiting for the database, migrating metadata, acquiring the instance lock or loading views".to_string(),
            },
        )),
        Probe::Ready => 'ready: {
            if o.standby {
                checks.push(if opts.standby_ok {
                    check("role", true, "hot standby: waiting for the instance lock, takes over when the active instance stops")
                } else {
                    check("role", false, "hot standby: another instance holds the lock for this schema (/readyz?standby=ok counts a standby as ready)")
                });
                break 'ready;
            }
            if !o.started() {
                checks.push(check("role", false, "starting: the binlog stream has not been connected yet"));
                break 'ready;
            }
            checks.push(check("role", true, "active"));
            let down = o.disconnected_for();
            let grace = s.disconnect_grace_secs;
            checks.push(if o.streaming {
                check("binlog", true, "streaming")
            } else if down <= grace {
                check("binlog", true, format!("reconnecting for {down}s (grace {grace}s)"))
            } else {
                check("binlog", false, format!("disconnected from the binlog for {down}s (grace {grace}s)"))
            });
            let pending = o.pending();
            checks.push(if pending > 0 && opts.building_ok {
                ignored_building = pending;
                check("views", true, format!("{pending} of {} view(s) building their first version (building=ok)", o.views))
            } else if pending > 0 {
                check("views", false, format!("{pending} of {} view(s) have no live version yet (building)", o.views))
            } else {
                let mut d = format!("{} view(s) live", o.views_ready);
                if o.views_building > 0 {
                    let _ = write!(d, ", {} new version(s) building in the background", o.views_building);
                }
                check("views", true, d)
            });
            let require_all = opts.require_all.unwrap_or(s.require_all);
            checks.push(match o.views_error {
                0 => check("errors", true, "no view in error"),
                n if require_all => check("errors", false, format!("{n} view(s) in error (see _lazyview_state.last_error)")),
                n => {
                    ignored_errors = n;
                    check("errors", true, format!("{n} view(s) in error, ignored (LAZYVIEW_READY_REQUIRE_ALL=false)"))
                }
            });
        }
    }
    let ok = checks.iter().all(|c| c.ok);
    Report {
        probe,
        ok,
        role: o.role(),
        checks,
        ignored_errors,
        ignored_building,
    }
}

fn render_text(r: &Report, verbose: bool) -> String {
    if !verbose {
        return format!("{}\n", r.summary());
    }
    let mut out = String::new();
    for c in &r.checks {
        let _ = writeln!(
            out,
            "[{}]{} {}: {}",
            if c.ok { '+' } else { '-' },
            c.name,
            if c.ok { "ok" } else { "failed" },
            c.detail
        );
    }
    let _ = writeln!(
        out,
        "{} check {}",
        r.probe.name(),
        if r.ok { "passed" } else { "failed" }
    );
    out
}

fn render_json(r: &Report, o: &Observed, s: &Settings) -> String {
    let checks: Vec<_> = r
        .checks
        .iter()
        .map(|c| json!({"name": c.name, "ok": c.ok, "detail": c.detail}))
        .collect();
    let v = json!({
        "probe": r.probe.name(),
        "status": if r.ok { "ok" } else { "failed" },
        "message": r.summary(),
        "role": r.role,
        "checks": checks,
        "engine": {
            "heartbeat_age_seconds": o.heartbeat_age(),
            "liveness_limit_seconds": o.stall_limit(s),
            "booting": o.booting(),
            "streaming": o.streaming,
            "starting": o.starting,
            "standby": o.standby,
            "binlog_disconnected_seconds": o.disconnected_for(),
        },
        "views": {
            "total": o.views,
            "ready": o.views_ready,
            "building": o.views_building,
            "error": o.views_error,
            "pending": o.pending(),
        },
        "settings": {
            "liveness_stall_secs": s.stall_secs,
            "liveness_boot_stall_secs": s.boot_stall_secs,
            "ready_require_all": s.require_all,
            "ready_disconnect_grace_secs": s.disconnect_grace_secs,
        },
    });
    format!("{v}\n")
}

// ------------------------------------------------------------------------------------ metrics

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn prometheus(m: &Metrics, o: &Observed, s: &Settings) -> String {
    let mut out = String::new();
    for (name, v) in m.counters() {
        let _ = writeln!(
            out,
            "# TYPE lazyview_{name}_total counter\nlazyview_{name}_total {v}"
        );
    }
    let alive = evaluate(Probe::Live, o, s, ReadyOpts::default()).ok;
    let ready = evaluate(Probe::Ready, o, s, ReadyOpts::default()).ok;
    let b = |v: bool| v as u64;
    let mut gauges = vec![
        ("views", "View definitions loaded.", o.views),
        (
            "views_ready",
            "Views with a live, error-free version.",
            o.views_ready,
        ),
        (
            "views_building",
            "Views with a version building in the background.",
            o.views_building,
        ),
        (
            "views_error",
            "Views in error state (see _lazyview_state.last_error).",
            o.views_error,
        ),
        (
            "streaming",
            "1 while connected to the binlog stream.",
            b(o.streaming),
        ),
        (
            "standby",
            "1 while waiting for another instance's lock on the schema (hot standby).",
            b(o.standby),
        ),
        (
            "starting",
            "1 until the binlog stream is first connected.",
            b(o.starting),
        ),
        ("alive", "1 while /livez passes.", b(alive)),
        (
            "ready",
            "1 while plain /readyz passes (not on hot standby or while a view builds its first version).",
            b(ready),
        ),
        (
            "heartbeat_age_seconds",
            "Seconds since the engine loop last reported progress.",
            o.heartbeat_age(),
        ),
        (
            "liveness_limit_seconds",
            "Heartbeat age at which /livez fails (larger while booting).",
            o.stall_limit(s),
        ),
        (
            "binlog_disconnected_seconds",
            "Seconds an instance that streamed before has been without a connected binlog stream (0 while streaming, on standby and before the first connect).",
            o.disconnected_for(),
        ),
    ];
    if o.applied_changed > 0 {
        gauges.push((
            "applied_position_age_seconds",
            "Seconds since the applied binlog position last changed (also grows while the source is idle).",
            o.now.saturating_sub(o.applied_changed),
        ));
    }
    for (name, help, v) in gauges {
        let _ = writeln!(
            out,
            "# HELP lazyview_{name} {help}\n# TYPE lazyview_{name} gauge\nlazyview_{name} {v}"
        );
    }
    let _ = writeln!(
        out,
        "# HELP lazyview_inflight_rows Rows read from the sources and not yet written to every target (concurrent refreshes).\n# TYPE lazyview_inflight_rows gauge\nlazyview_inflight_rows {}",
        Metrics::get(&m.gauges.inflight_rows)
    );
    // Per connection pool of the refresh supervisor (src/supervisor.rs).
    let pools = m.pool_gauges();
    if let Some(first) = pools.first() {
        for (k, (name, help, _)) in first.values().into_iter().enumerate() {
            let _ = writeln!(
                out,
                "# HELP lazyview_supervisor_{name} {help}\n# TYPE lazyview_supervisor_{name} gauge"
            );
            for p in &pools {
                let _ = writeln!(
                    out,
                    "lazyview_supervisor_{name}{{pool=\"{}\",endpoint=\"{}\"}} {}",
                    escape_label(&p.pool),
                    escape_label(&p.endpoint),
                    p.values()[k].2
                );
            }
        }
    }
    if let Some(p) = m.applied() {
        let _ = writeln!(
            out,
            "# TYPE lazyview_applied_binlog_pos gauge\nlazyview_applied_binlog_pos{{file=\"{}\"}} {}",
            escape_label(&p.file),
            p.pos
        );
    }
    // Per view (label `view`, see src/stats.rs).
    m.view_stats
        .snapshot()
        .write_prometheus(&mut out, escape_label);
    let _ = writeln!(
        out,
        "# HELP lazyview_build_info lazyview version.\n# TYPE lazyview_build_info gauge\nlazyview_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );
    out
}

// ------------------------------------------------------------------------------------ HTTP

#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    head_only: bool,
    method: String,
    path: String,
    query: Vec<(String, String)>,
    accept_json: bool,
}

impl Request {
    /// `Some(true)` for `?key`, `?key=1|true|yes|on|ok|ready`, `Some(false)` for `0|false|no|off`.
    fn flag(&self, key: &str) -> Option<bool> {
        let (_, v) = self.query.iter().rev().find(|(k, _)| k == key)?;
        Some(
            matches!(v.to_ascii_lowercase().as_str(), "" | "ok" | "ready")
                || parse_bool(v).unwrap_or(true),
        )
    }

    fn wants_json(&self) -> bool {
        match self
            .query
            .iter()
            .rev()
            .find(|(k, _)| k == "format")
            .map(|(_, v)| v.to_ascii_lowercase())
        {
            Some(f) => f == "json",
            None => self.accept_json,
        }
    }
}

/// Parses a request head (request line + headers). `Err` carries the status to answer with.
fn parse_head(head: &[u8]) -> std::result::Result<Request, u16> {
    let text = std::str::from_utf8(head).map_err(|_| 400u16)?;
    let mut lines = text.split('\n').map(|l| l.strip_suffix('\r').unwrap_or(l));
    let line = lines.next().unwrap_or("");
    let mut parts = line.split(' ').filter(|p| !p.is_empty());
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(400);
    };
    if !version.starts_with("HTTP/1.") {
        return Err(if version.starts_with("HTTP/") {
            505
        } else {
            400
        });
    }
    // absolute-form (`GET http://host:8080/readyz HTTP/1.1`, sent through proxies)
    let target = match target.split_once("://") {
        Some((_, rest)) if !target.starts_with('/') => match rest.find('/') {
            Some(i) => &rest[i..],
            None => "/",
        },
        _ => target,
    };
    if !target.starts_with('/') {
        return Err(400);
    }
    let target = target.split('#').next().unwrap_or(target);
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = match path.trim_end_matches('/') {
        "" => "/",
        p => p,
    };
    let query = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k.to_ascii_lowercase(), v.to_string())
        })
        .collect();
    let accept_json = lines.any(|h| {
        h.split_once(':')
            .map(|(k, v)| {
                k.trim().eq_ignore_ascii_case("accept")
                    && v.to_ascii_lowercase().contains("application/json")
            })
            .unwrap_or(false)
    });
    Ok(Request {
        head_only: method == "HEAD",
        method: method.to_string(),
        path: path.to_string(),
        query,
        accept_json,
    })
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        _ => "Error",
    }
}

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

struct Response {
    status: u16,
    content_type: &'static str,
    body: String,
    allow: bool,
}

impl Response {
    fn text(status: u16, body: impl Into<String>) -> Response {
        Response {
            status,
            content_type: TEXT,
            body: body.into(),
            allow: false,
        }
    }

    fn bytes(&self, head_only: bool) -> Vec<u8> {
        let mut out = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
            self.status,
            reason(self.status),
            self.content_type,
            self.body.len()
        );
        if self.allow {
            out.push_str("Allow: GET, HEAD\r\n");
        }
        out.push_str("\r\n");
        if !head_only {
            out.push_str(&self.body);
        }
        out.into_bytes()
    }
}

const INDEX: &str = "lazyview health endpoint\n\
/livez      liveness (alias /healthz): the engine loop is making progress\n\
/startupz   startup: same verdict as /livez, ?verbose shows the boot phase\n\
/readyz     readiness: active, streaming, every view live\n\
            (?standby=ok counts a hot standby as ready, ?building=ok ignores first builds,\n\
             ?require_all=1 fails on views in error)\n\
/metrics    Prometheus metrics\n\
/stats      per-view update statistics (JSON)\n\
?verbose lists every check; ?format=json (or Accept: application/json) returns JSON\n";

// Connection states (what eviction may drop).
const SILENT: u8 = 0;
const RECEIVING: u8 = 1;
const ANSWERING: u8 = 2;

struct ConnEntry {
    ip: IpAddr,
    accepted: Instant,
    state: Arc<AtomicU8>,
    /// Dropping it (eviction) ends the connection's task.
    _cancel: oneshot::Sender<()>,
}

#[derive(Default)]
struct ConnTable {
    next: u64,
    conns: HashMap<u64, ConnEntry>,
    last_warn: Option<Instant>,
}

impl ConnTable {
    /// The connection to drop so that a new one fits: never one being answered; first one that
    /// sent nothing for `SILENT_GRACE`, from the address holding the most connections, oldest
    /// first (a kubelet probe sends its request at once and is never the oldest silent one).
    fn victim(&self, now: Instant) -> Option<u64> {
        let mut per_ip: HashMap<IpAddr, usize> = HashMap::new();
        for e in self.conns.values() {
            *per_ip.entry(e.ip).or_default() += 1;
        }
        self.conns
            .iter()
            .filter(|(_, e)| e.state.load(Ordering::Relaxed) != ANSWERING)
            .max_by_key(|(id, e)| {
                let age = now.saturating_duration_since(e.accepted);
                let idle = e.state.load(Ordering::Relaxed) == SILENT && age >= SILENT_GRACE;
                (idle, per_ip[&e.ip], age, std::cmp::Reverse(**id))
            })
            .map(|(id, _)| *id)
    }
}

/// A registered connection; dropping it unregisters it.
struct Slot {
    ctx: Arc<Ctx>,
    id: u64,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.ctx.table().conns.remove(&self.id);
    }
}

struct Ctx {
    metrics: Arc<Metrics>,
    settings: Settings,
    tracker: Tracker,
    head_timeout: Duration,
    max_conns: usize,
    conns: Mutex<ConnTable>,
}

impl Ctx {
    fn new(metrics: Arc<Metrics>, settings: Settings) -> Ctx {
        Ctx {
            metrics,
            settings,
            tracker: Tracker::new(),
            head_timeout: HEAD_TIMEOUT,
            max_conns: MAX_CONNECTIONS,
            conns: Mutex::new(ConnTable::default()),
        }
    }

    fn table(&self) -> std::sync::MutexGuard<'_, ConnTable> {
        self.conns.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers a new connection, evicting an idle one at the limit. `None`: every open
    /// connection is being answered; refuse this one.
    fn admit(self: &Arc<Self>, ip: IpAddr) -> Option<(Slot, Arc<AtomicU8>, oneshot::Receiver<()>)> {
        let mut t = self.table();
        if t.conns.len() >= self.max_conns {
            let now = Instant::now();
            let victim = t.victim(now)?;
            let e = t.conns.remove(&victim)?;
            if t.last_warn
                .is_none_or(|w| now.duration_since(w) >= Duration::from_secs(60))
            {
                t.last_warn = Some(now);
                warn!(
                    "health endpoint: {} connections open; closing idle ones (from {}), probes are not affected",
                    self.max_conns, e.ip
                );
            }
            debug!(peer = %e.ip, "health endpoint: evicted an idle connection");
        }
        let id = t.next;
        t.next += 1;
        let (tx, rx) = oneshot::channel();
        let state = Arc::new(AtomicU8::new(SILENT));
        t.conns.insert(
            id,
            ConnEntry {
                ip,
                accepted: Instant::now(),
                state: state.clone(),
                _cancel: tx,
            },
        );
        Some((
            Slot {
                ctx: self.clone(),
                id,
            },
            state,
            rx,
        ))
    }

    fn respond(&self, req: &Request) -> Response {
        let probe = match req.path.as_str() {
            "/livez" | "/healthz" => Some(Probe::Live),
            "/readyz" => Some(Probe::Ready),
            "/startupz" => Some(Probe::Startup),
            "/metrics" | "/" | "/stats" => None,
            _ => return Response::text(404, "not found\n"),
        };
        if req.method != "GET" && req.method != "HEAD" {
            return Response {
                allow: true,
                ..Response::text(405, "only GET and HEAD are supported\n")
            };
        }
        if req.path == "/stats" {
            return Response {
                status: 200,
                content_type: JSON,
                body: format!("{}\n", self.metrics.view_stats.snapshot().to_json()),
                allow: false,
            };
        }
        let o = self.tracker.observe(&self.metrics);
        let Some(probe) = probe else {
            return if req.path == "/" {
                Response::text(200, INDEX)
            } else {
                Response {
                    status: 200,
                    content_type: PROMETHEUS,
                    body: prometheus(&self.metrics, &o, &self.settings),
                    allow: false,
                }
            };
        };
        let opts = ReadyOpts {
            standby_ok: req.flag("standby").unwrap_or(false),
            building_ok: req.flag("building").unwrap_or(false),
            require_all: req.flag("require_all"),
        };
        let r = evaluate(probe, &o, &self.settings, opts);
        let status = if r.ok { 200 } else { 503 };
        if req.wants_json() {
            Response {
                status,
                content_type: JSON,
                body: render_json(&r, &o, &self.settings),
                allow: false,
            }
        } else {
            Response::text(
                status,
                render_text(&r, req.flag("verbose").unwrap_or(false)),
            )
        }
    }
}

enum Head {
    Complete(Vec<u8>),
    TooLarge,
    Closed,
}

/// Reads up to the blank line ending the request head (the request body, if any, is ignored).
async fn read_head<S: AsyncRead + Unpin>(sock: &mut S, state: &AtomicU8) -> io::Result<Head> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    loop {
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            // Peer closed (or half-closed) before the blank line: answer what we have, if anything.
            return Ok(if buf.is_empty() {
                Head::Closed
            } else {
                Head::Complete(buf)
            });
        }
        state.store(RECEIVING, Ordering::Relaxed);
        let from = buf.len().saturating_sub(3);
        buf.extend_from_slice(&chunk[..n]);
        let tail = &buf[from..];
        let end = tail
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|i| i + 4)
            .or_else(|| tail.windows(2).position(|w| w == b"\n\n").map(|i| i + 2));
        if let Some(e) = end {
            buf.truncate(from + e);
            return Ok(if buf.len() > MAX_HEAD {
                Head::TooLarge
            } else {
                Head::Complete(buf)
            });
        }
        if buf.len() > MAX_HEAD {
            return Ok(Head::TooLarge);
        }
    }
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut sock: S,
    ctx: &Ctx,
    state: &AtomicU8,
) -> io::Result<()> {
    let head = tokio::time::timeout(ctx.head_timeout, read_head(&mut sock, state)).await;
    state.store(ANSWERING, Ordering::Relaxed);
    let (resp, head_only) = match head {
        Err(_) => (
            Response::text(408, "request head not received in time\n"),
            false,
        ),
        Ok(Err(e)) => return Err(e),
        Ok(Ok(Head::Closed)) => return Ok(()),
        Ok(Ok(Head::TooLarge)) => (Response::text(431, "request head too large\n"), false),
        Ok(Ok(Head::Complete(head))) => match parse_head(&head) {
            Ok(req) => (ctx.respond(&req), req.head_only),
            Err(status) => (Response::text(status, "bad request\n"), false),
        },
    };
    let bytes = resp.bytes(head_only);
    let write = async {
        sock.write_all(&bytes).await?;
        sock.flush().await?;
        sock.shutdown().await
    };
    match tokio::time::timeout(WRITE_TIMEOUT, write).await {
        Ok(r) => r,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "response write timed out",
        )),
    }
}

/// Accepts without ever waiting for a free slot (a full table evicts an idle connection), so a
/// probe never queues behind idle connections in the listen backlog.
async fn accept_loop(listener: TcpListener, ctx: Arc<Ctx>) {
    let mut conns = JoinSet::new();
    loop {
        while conns.try_join_next().is_some() {}
        match listener.accept().await {
            Ok((sock, peer)) => {
                let Some((slot, state, cancel)) = ctx.admit(peer.ip().to_canonical()) else {
                    // Every slot is answering a request right now: refuse without blocking.
                    let _ = sock.try_write(BUSY);
                    continue;
                };
                let _ = sock.set_nodelay(true);
                let ctx = ctx.clone();
                conns.spawn(async move {
                    tokio::select! {
                        r = handle(sock, &ctx, &state) => if let Err(e) = r {
                            debug!(%peer, "health request failed: {e}");
                        },
                        _ = cancel => {}
                    }
                    drop(slot);
                });
            }
            Err(e) => {
                // EMFILE / ENFILE / ECONNABORTED …: back off instead of spinning.
                debug!("health endpoint accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// One listener with a deep accept backlog (a connection flood overflows it later).
fn listen(addr: SocketAddr) -> io::Result<TcpListener> {
    let s = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    s.set_reuseaddr(true)?;
    s.bind(addr)?;
    s.listen(LISTEN_BACKLOG)
}

/// Unspecified addresses listen on both IPv6 (`[::]`, dual-stack where the kernel allows) and
/// IPv4 (`0.0.0.0`), so the probes work in IPv4, IPv6 and dual-stack clusters alike.
async fn bind(addr: SocketAddr) -> io::Result<Vec<TcpListener>> {
    if !addr.ip().is_unspecified() || addr.port() == 0 {
        return Ok(vec![listen(addr)?]);
    }
    let mut out = Vec::new();
    let mut first_err = None;
    for a in [
        SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), addr.port()),
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), addr.port()),
    ] {
        match listen(a) {
            Ok(l) => out.push(l),
            // `[::]` is dual-stack (net.ipv6.bindv6only=0) and already accepts IPv4.
            Err(e) if a.is_ipv4() && !out.is_empty() && e.kind() == io::ErrorKind::AddrInUse => {}
            Err(e) => {
                debug!("health endpoint: cannot listen on {a}: {e}");
                first_err.get_or_insert(e);
            }
        }
    }
    match (out.is_empty(), first_err) {
        (false, _) => Ok(out),
        (true, Some(e)) => Err(e),
        (true, None) => Err(io::Error::other("no address to listen on")),
    }
}

/// Serves the endpoint until the future is dropped (policy from the environment; an invalid
/// variable falls back to its own default and is named in a warning).
pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) {
    let (settings, errors) = Settings::from_lookup_lenient(|k| std::env::var(k).ok());
    for e in errors {
        warn!("health settings: {e}");
    }
    serve_with(addr, metrics, settings).await
}

/// Serves the endpoint until the future is dropped. A failed bind is retried every 5 s; it
/// never stops the engine. Dropping the future stops the listeners and open connections.
pub async fn serve_with(addr: SocketAddr, metrics: Arc<Metrics>, settings: Settings) {
    serve_ctx(addr, Arc::new(Ctx::new(metrics, settings))).await
}

async fn serve_ctx(addr: SocketAddr, ctx: Arc<Ctx>) {
    let mut listeners = JoinSet::new();
    let mut bound = false;
    let mut warned = false;
    let mut next_bind = tokio::time::Instant::now();
    loop {
        if !bound && tokio::time::Instant::now() >= next_bind {
            match bind(addr).await {
                Ok(ls) => {
                    let shown: Vec<String> = ls
                        .iter()
                        .filter_map(|l| l.local_addr().ok())
                        .map(|a| match a.ip() {
                            // dual-stack socket (bindv6only=0): also accepts IPv4
                            IpAddr::V6(ip) if ip.is_unspecified() && ls.len() == 1 => {
                                format!("http://{a} (IPv4 and IPv6)")
                            }
                            _ => format!("http://{a}"),
                        })
                        .collect();
                    let s = &ctx.settings;
                    info!(
                        "health endpoint on {} (/livez /readyz /startupz /metrics; stall {}s, {}s while booting, require_all {}, disconnect grace {}s)",
                        shown.join(" and "),
                        s.stall_secs,
                        s.boot_stall_secs,
                        s.require_all,
                        s.disconnect_grace_secs
                    );
                    for l in ls {
                        listeners.spawn(accept_loop(l, ctx.clone()));
                    }
                    bound = true;
                }
                Err(e) => {
                    if !warned {
                        warn!("health endpoint: cannot listen on {addr}: {e}; retrying every {REBIND_EVERY:?} (set LAZYVIEW_HEALTH_ADDR)");
                        warned = true;
                    }
                    next_bind = tokio::time::Instant::now() + REBIND_EVERY;
                }
            }
        }
        ctx.tracker.observe(&ctx.metrics);
        tokio::time::sleep(SAMPLE_EVERY).await;
    }
}

/// `lazyview healthcheck`: true when `/livez` answers 200.
pub async fn probe(addr: SocketAddr) -> bool {
    probe_path(addr, "/livez").await
}

/// True when `GET path` on the local endpoint answers 200 within 3 s. Unspecified addresses are
/// reached through loopback (IPv4 first for `0.0.0.0`, IPv6 first for `[::]`).
pub async fn probe_path(addr: SocketAddr, path: &str) -> bool {
    let targets: Vec<SocketAddr> = if addr.ip().is_unspecified() {
        let v4 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), addr.port());
        let v6 = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), addr.port());
        if addr.is_ipv4() {
            vec![v4, v6]
        } else {
            vec![v6, v4]
        }
    } else {
        vec![addr]
    };
    let attempt = async {
        for t in targets {
            match status_of(t, path).await {
                Ok(code) => return code == 200,
                Err(e) => debug!("healthcheck {t}{path}: {e}"),
            }
        }
        false
    };
    tokio::time::timeout(PROBE_TIMEOUT, attempt)
        .await
        .unwrap_or(false)
}

async fn status_of(addr: SocketAddr, path: &str) -> io::Result<u16> {
    let mut s = TcpStream::connect(addr).await?;
    s.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await?;
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await?;
    let line = resp.split(|b| *b == b'\n').next().unwrap_or(&[]);
    std::str::from_utf8(line)
        .ok()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not an HTTP response"))
}

// ------------------------------------------------------------------------------------ tests

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;

    /// Active, streaming, 3 views live, heartbeat fresh.
    fn healthy() -> Observed {
        Observed {
            now: NOW,
            heartbeat: NOW - 1,
            streaming: true,
            starting: false,
            standby: false,
            views: 3,
            views_ready: 3,
            views_building: 0,
            views_error: 0,
            stream_seen: NOW,
            since: NOW - 600,
            applied_changed: NOW - 2,
        }
    }

    /// Waiting for the database / migrating metadata (no lock yet).
    fn booting() -> Observed {
        Observed {
            streaming: false,
            starting: true,
            views: 0,
            views_ready: 0,
            stream_seen: 0,
            since: NOW - 5,
            applied_changed: 0,
            ..healthy()
        }
    }

    fn standby() -> Observed {
        Observed {
            standby: true,
            ..booting()
        }
    }

    /// Took the lock, still booting (loading views, first binlog connect).
    fn took_over() -> Observed {
        Observed {
            standby: false,
            ..standby()
        }
    }

    fn aged(o: Observed, secs: u64) -> Observed {
        Observed {
            heartbeat: NOW - secs,
            ..o
        }
    }

    fn stalled(o: Observed) -> Observed {
        let limit = o.stall_limit(&Settings::default());
        aged(o, limit)
    }

    fn run(p: Probe, o: &Observed) -> Report {
        evaluate(p, o, &Settings::default(), ReadyOpts::default())
    }

    fn failing(r: &Report) -> Vec<&'static str> {
        r.checks.iter().filter(|c| !c.ok).map(|c| c.name).collect()
    }

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn settings_from_lookup() {
        let s = |pairs: &[(&str, &str)]| Settings::from_lookup(lookup(pairs));
        assert_eq!(
            s(&[]).unwrap(),
            Settings {
                stall_secs: 120,
                boot_stall_secs: 900,
                require_all: false,
                disconnect_grace_secs: 30
            }
        );
        assert_eq!(
            s(&[
                ("LAZYVIEW_LIVENESS_STALL_SECS", " 300 "),
                ("LAZYVIEW_LIVENESS_BOOT_STALL_SECS", "1800"),
                ("LAZYVIEW_READY_REQUIRE_ALL", "TRUE"),
                ("LAZYVIEW_READY_DISCONNECT_GRACE_SECS", "0")
            ])
            .unwrap(),
            Settings {
                stall_secs: 300,
                boot_stall_secs: 1800,
                require_all: true,
                disconnect_grace_secs: 0
            }
        );
        // the boot budget never undercuts the normal one
        let r = s(&[("LAZYVIEW_LIVENESS_STALL_SECS", "1200")]).unwrap();
        assert_eq!((r.stall_secs, r.boot_stall_secs), (1200, 1200));
        assert!(
            !s(&[("LAZYVIEW_READY_REQUIRE_ALL", "")])
                .unwrap()
                .require_all
        );
        assert_eq!(
            s(&[("LAZYVIEW_LIVENESS_STALL_SECS", "30")])
                .unwrap()
                .stall_secs,
            30
        );
        for bad in [
            ("LAZYVIEW_LIVENESS_STALL_SECS", "0"),
            ("LAZYVIEW_LIVENESS_STALL_SECS", "29"),
            ("LAZYVIEW_LIVENESS_STALL_SECS", "2m"),
            ("LAZYVIEW_LIVENESS_BOOT_STALL_SECS", "0"),
            ("LAZYVIEW_READY_REQUIRE_ALL", "maybe"),
            ("LAZYVIEW_READY_DISCONNECT_GRACE_SECS", "-1"),
        ] {
            let e = s(&[bad]).unwrap_err().to_string();
            assert!(e.contains(bad.0), "{bad:?}: {e}");
        }
        // slow binlog heartbeats need a longer stall window
        let hb = |stall: Option<&str>| {
            let mut p = vec![("LAZYVIEW_HEARTBEAT_MS", "60000")];
            if let Some(v) = stall {
                p.push(("LAZYVIEW_LIVENESS_STALL_SECS", v));
            }
            Settings::from_lookup_lenient(lookup(&p))
        };
        assert_eq!((hb(None).0.stall_secs, hb(None).1.len()), (130, 0));
        let (r, errs) = hb(Some("60"));
        assert_eq!(r.stall_secs, 130);
        assert!(errs[0].contains("LAZYVIEW_HEARTBEAT_MS=60000"), "{errs:?}");
        assert!(hb(Some("200")).1.is_empty());
    }

    #[test]
    fn lenient_settings_keep_valid_values() {
        let (s, errs) = Settings::from_lookup_lenient(lookup(&[
            ("LAZYVIEW_LIVENESS_STALL_SECS", "2m"),
            ("LAZYVIEW_LIVENESS_BOOT_STALL_SECS", "x"),
            ("LAZYVIEW_READY_REQUIRE_ALL", "true"),
            ("LAZYVIEW_READY_DISCONNECT_GRACE_SECS", "45"),
        ]));
        assert_eq!(
            s,
            Settings {
                stall_secs: 120,
                boot_stall_secs: 900,
                require_all: true,
                disconnect_grace_secs: 45
            }
        );
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs[0].contains("LAZYVIEW_LIVENESS_STALL_SECS") && errs[0].contains("\"2m\""));
        assert!(errs[1].contains("LAZYVIEW_LIVENESS_BOOT_STALL_SECS"));
        let e = Settings::from_lookup(lookup(&[
            ("LAZYVIEW_LIVENESS_STALL_SECS", "2m"),
            ("LAZYVIEW_READY_REQUIRE_ALL", "maybe"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("LAZYVIEW_LIVENESS_STALL_SECS") && e.contains("LAZYVIEW_READY_REQUIRE_ALL"),
            "{e}"
        );
    }

    #[test]
    fn liveness_only_fails_when_wedged() {
        let building = Observed {
            views_ready: 0,
            views_building: 3,
            ..healthy()
        };
        let db_outage = Observed {
            streaming: false,
            stream_seen: NOW - 3600,
            ..healthy()
        };
        let lagging = Observed {
            applied_changed: NOW - 7200,
            ..healthy()
        };
        let errors = Observed {
            views_ready: 1,
            views_error: 2,
            ..healthy()
        };
        for o in [
            healthy(),
            booting(),
            standby(),
            took_over(),
            building,
            db_outage,
            lagging,
            errors,
        ] {
            let r = run(Probe::Live, &o);
            assert!(r.ok, "{o:?} → {r:?}");
            assert_eq!(r.summary(), "ok");
            assert!(!run(Probe::Live, &stalled(o.clone())).ok, "stalled {o:?}");
        }
        let edge = aged(healthy(), DEFAULT_STALL_SECS - 1);
        assert!(run(Probe::Live, &edge).ok);
        let r = run(Probe::Live, &stalled(healthy()));
        assert_eq!(failing(&r), ["engine"]);
        assert!(
            r.summary()
                .contains("stalled: no progress for 120s (limit 120s)"),
            "{}",
            r.summary()
        );
        let s = Settings {
            stall_secs: 600,
            ..Settings::default()
        };
        assert!(evaluate(Probe::Live, &stalled(healthy()), &s, ReadyOpts::default()).ok);
        assert!(!run(Probe::Live, &aged(healthy(), NOW)).ok, "never beat");
    }

    #[test]
    fn boot_budget() {
        // Booting (before or after taking the lock): the boot budget applies.
        for o in [booting(), took_over()] {
            assert_eq!(o.stall_limit(&Settings::default()), DEFAULT_BOOT_STALL_SECS);
            assert!(run(Probe::Live, &aged(o.clone(), 600)).ok, "{o:?}");
            let r = run(Probe::Live, &aged(o.clone(), DEFAULT_BOOT_STALL_SECS));
            assert!(!r.ok);
            assert!(
                r.summary().contains("limit 900s while booting"),
                "{}",
                r.summary()
            );
        }
        // Standby polls every few seconds and the active instance beats every iteration: normal.
        for o in [standby(), healthy()] {
            assert_eq!(o.stall_limit(&Settings::default()), DEFAULT_STALL_SECS);
            assert!(!run(Probe::Live, &aged(o, 600)).ok);
        }
        // Reconnecting after having streamed is not booting.
        let reconnecting = Observed {
            streaming: false,
            ..healthy()
        };
        assert!(!reconnecting.booting());
    }

    #[test]
    fn startup_does_not_wait_for_the_database() {
        for o in [booting(), standby(), took_over(), healthy()] {
            let r = run(Probe::Startup, &o);
            assert!(r.ok, "{o:?} → {r:?}");
            assert_eq!(r.summary(), "ok");
            assert!(!run(Probe::Startup, &stalled(o)).ok);
        }
        let v = render_text(&run(Probe::Startup, &booting()), true);
        assert!(
            v.contains("[+]phase ok: booting: waiting for the database"),
            "{v}"
        );
        let v = render_text(&run(Probe::Startup, &standby()), true);
        assert!(v.contains("[+]phase ok: hot standby"), "{v}");
        let v = render_text(&run(Probe::Startup, &healthy()), true);
        assert!(v.contains("[+]phase ok: binlog stream connected"), "{v}");
    }

    #[test]
    fn readiness_states() {
        let ok = |o: &Observed| run(Probe::Ready, o).ok;
        assert!(ok(&healthy()));
        assert_eq!(run(Probe::Ready, &healthy()).summary(), "ready");
        assert_eq!(run(Probe::Ready, &healthy()).role, "active");

        // starting (booting, or the lock was just acquired and the stream is not connected yet)
        let r = run(Probe::Ready, &booting());
        assert_eq!(
            (r.ok, r.role, failing(&r)),
            (false, "starting", vec!["role"])
        );
        assert!(!ok(&took_over()));
        // window before the engine sets `starting`: never streamed → not ready
        assert!(!ok(&Observed {
            starting: false,
            ..booting()
        }));
        // `starting` still set although streaming (gauge order): not ready until cleared
        assert!(!ok(&Observed {
            starting: true,
            ..healthy()
        }));

        // hot standby: not ready, unless ?standby=ok
        let r = run(Probe::Ready, &standby());
        assert_eq!((r.ok, r.role), (false, "standby"));
        assert!(r.summary().contains("hot standby"), "{}", r.summary());
        let so = ReadyOpts {
            standby_ok: true,
            ..ReadyOpts::default()
        };
        let r = evaluate(Probe::Ready, &standby(), &Settings::default(), so);
        assert!(r.ok, "{r:?}");
        assert_eq!(r.summary(), "ready (hot standby)");
        assert!(!evaluate(Probe::Ready, &stalled(standby()), &Settings::default(), so).ok);
        // ?standby=ok changes nothing for an active or starting instance
        assert!(!evaluate(Probe::Ready, &booting(), &Settings::default(), so).ok);
        assert!(!evaluate(Probe::Ready, &took_over(), &Settings::default(), so).ok);
        assert!(evaluate(Probe::Ready, &healthy(), &Settings::default(), so).ok);

        // first build: no live version yet
        let first_build = Observed {
            views_ready: 1,
            views_building: 2,
            ..healthy()
        };
        let r = run(Probe::Ready, &first_build);
        assert_eq!(failing(&r), ["views"]);
        assert!(
            r.summary()
                .contains("2 of 3 view(s) have no live version yet"),
            "{}",
            r.summary()
        );
        // ... unless ?building=ok (the Kubernetes probe): streaming and building is fine
        let k8s = ReadyOpts {
            standby_ok: true,
            building_ok: true,
            require_all: None,
        };
        let r = evaluate(Probe::Ready, &first_build, &Settings::default(), k8s);
        assert!(r.ok, "{r:?}");
        assert_eq!(r.summary(), "ready; 2 view(s) building their first version");
        // building=ok does not hide a stalled engine, a lost stream or a starting instance
        assert!(
            !evaluate(
                Probe::Ready,
                &stalled(first_build.clone()),
                &Settings::default(),
                k8s
            )
            .ok
        );
        let lost = Observed {
            streaming: false,
            stream_seen: NOW - 300,
            ..first_build
        };
        assert!(!evaluate(Probe::Ready, &lost, &Settings::default(), k8s).ok);
        assert!(!evaluate(Probe::Ready, &took_over(), &Settings::default(), k8s).ok);
        assert!(evaluate(Probe::Ready, &standby(), &Settings::default(), k8s).ok);

        // live version served while a new version builds: ready
        let r = run(
            Probe::Ready,
            &Observed {
                views_building: 2,
                ..healthy()
            },
        );
        assert!(r.ok);
        assert!(r
            .checks
            .iter()
            .any(|c| c.detail.contains("2 new version(s) building")));
        // no definitions at all: ready
        assert!(ok(&Observed {
            views: 0,
            views_ready: 0,
            ..healthy()
        }));

        // error views: ignored by default, fail with require_all (setting or query)
        let errs = Observed {
            views_ready: 2,
            views_error: 1,
            ..healthy()
        };
        let r = run(Probe::Ready, &errs);
        assert!(r.ok);
        assert!(
            r.summary().contains("1 view(s) in error, ignored"),
            "{}",
            r.summary()
        );
        let strict = Settings {
            require_all: true,
            ..Settings::default()
        };
        let r = evaluate(Probe::Ready, &errs, &strict, ReadyOpts::default());
        assert_eq!(failing(&r), ["errors"]);
        assert!(!evaluate(Probe::Ready, &errs, &strict, k8s).ok);
        let q = |v| ReadyOpts {
            require_all: Some(v),
            ..ReadyOpts::default()
        };
        assert!(!evaluate(Probe::Ready, &errs, &Settings::default(), q(true)).ok);
        assert!(evaluate(Probe::Ready, &errs, &strict, q(false)).ok);
        // error views do not count as pending builds
        assert!(ok(&Observed {
            views_ready: 0,
            views_error: 3,
            ..healthy()
        }));

        // binlog disconnected: within grace ready, beyond grace not
        let down = |secs: u64| Observed {
            streaming: false,
            stream_seen: NOW - secs,
            ..healthy()
        };
        assert!(ok(&down(0)));
        assert!(ok(&down(DEFAULT_DISCONNECT_GRACE_SECS)));
        let r = run(Probe::Ready, &down(DEFAULT_DISCONNECT_GRACE_SECS + 1));
        assert_eq!(failing(&r), ["binlog"]);
        assert!(
            r.summary().contains("disconnected from the binlog for 31s"),
            "{}",
            r.summary()
        );
        let no_grace = Settings {
            disconnect_grace_secs: 0,
            ..Settings::default()
        };
        assert!(!evaluate(Probe::Ready, &down(1), &no_grace, ReadyOpts::default()).ok);

        // stalled engine: not ready either
        assert_eq!(failing(&run(Probe::Ready, &stalled(healthy()))), ["engine"]);
    }

    #[test]
    fn disconnected_only_counts_for_an_instance_that_streamed() {
        assert_eq!(standby().disconnected_for(), 0);
        assert_eq!(aged(booting(), 0).disconnected_for(), 0);
        assert_eq!(took_over().disconnected_for(), 0);
        assert_eq!(healthy().disconnected_for(), 0);
        let down = Observed {
            streaming: false,
            stream_seen: NOW - 40,
            ..healthy()
        };
        assert_eq!(down.disconnected_for(), 40);
        // lost the lock again after streaming: standby, not "disconnected"
        assert_eq!(
            Observed {
                standby: true,
                ..down
            }
            .disconnected_for(),
            0
        );
    }

    #[test]
    fn rendering() {
        let o = Observed {
            views_ready: 2,
            views_error: 1,
            ..healthy()
        };
        let r = run(Probe::Ready, &o);
        let v = render_text(&r, true);
        assert!(v.starts_with("[+]engine ok: "), "{v}");
        assert!(
            v.contains("[+]errors ok: 1 view(s) in error, ignored"),
            "{v}"
        );
        assert!(v.ends_with("readyz check passed\n"), "{v}");
        let r = run(Probe::Ready, &standby());
        let v = render_text(&r, true);
        assert!(v.contains("[-]role failed: hot standby"), "{v}");
        assert!(v.ends_with("readyz check failed\n"), "{v}");
        let j: serde_json::Value =
            serde_json::from_str(&render_json(&r, &standby(), &Settings::default())).unwrap();
        assert_eq!(j["status"], "failed");
        assert_eq!(j["role"], "standby");
        assert_eq!(j["checks"][1]["name"], "role");
        assert_eq!(j["checks"][1]["ok"], false);
        assert_eq!(j["engine"]["standby"], true);
        assert_eq!(j["engine"]["binlog_disconnected_seconds"], 0);
        assert_eq!(j["engine"]["liveness_limit_seconds"], 120);
        assert_eq!(j["settings"]["liveness_stall_secs"], 120);
        assert_eq!(j["settings"]["liveness_boot_stall_secs"], 900);
        let j: serde_json::Value = serde_json::from_str(&render_json(
            &run(Probe::Live, &booting()),
            &booting(),
            &Settings::default(),
        ))
        .unwrap();
        assert_eq!(
            (
                &j["engine"]["booting"],
                &j["engine"]["liveness_limit_seconds"]
            ),
            (&json!(true), &json!(900))
        );
    }

    #[test]
    fn prometheus_output() {
        let m = Metrics::default();
        Metrics::set(&m.gauges.views, 2);
        Metrics::set(&m.gauges.views_ready, 2);
        m.set_applied(Position {
            file: "bin\"log.000001".into(),
            pos: 42,
        });
        let o = Observed {
            views: 2,
            views_ready: 2,
            ..healthy()
        };
        let pool = std::sync::Arc::new(crate::metrics::PoolGauges::new("write", "an\"x"));
        Metrics::set(&pool.max, 4);
        Metrics::set_f64(&pool.limit, 4.0);
        m.register_pool(pool);
        let text = prometheus(&m, &o, &Settings::default());
        assert!(
            text.contains("lazyview_supervisor_max{pool=\"write\",endpoint=\"an\\\"x\"} 4"),
            "{text}"
        );
        assert!(text.contains("lazyview_inflight_rows 0"), "{text}");
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (name, value) = line.rsplit_once(' ').unwrap();
            assert!(name.starts_with("lazyview_"), "{line}");
            assert!(value.parse::<f64>().is_ok(), "{line}");
        }
        for want in [
            "lazyview_events_total 0",
            "lazyview_views 2",
            "lazyview_streaming 1",
            "lazyview_standby 0",
            "lazyview_starting 0",
            "lazyview_alive 1",
            "lazyview_ready 1",
            "lazyview_heartbeat_age_seconds 1",
            "lazyview_liveness_limit_seconds 120",
            "lazyview_binlog_disconnected_seconds 0",
            "lazyview_applied_position_age_seconds 2",
            "lazyview_applied_binlog_pos{file=\"bin\\\"log.000001\"} 42",
            "# TYPE lazyview_ready gauge",
        ] {
            assert!(
                text.lines().any(|l| l == want),
                "missing {want:?} in\n{text}"
            );
        }
        assert!(text.contains(&format!(
            "lazyview_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        )));
        // a standby that has been waiting for 10 minutes is not "disconnected"
        let waiting = Observed {
            since: NOW - 600,
            ..standby()
        };
        let text = prometheus(&Metrics::default(), &waiting, &Settings::default());
        for want in [
            "lazyview_ready 0",
            "lazyview_standby 1",
            "lazyview_binlog_disconnected_seconds 0",
        ] {
            assert!(
                text.lines().any(|l| l == want),
                "missing {want:?} in\n{text}"
            );
        }
        assert!(!text.contains("applied_position_age"), "{text}");
        let text = prometheus(&Metrics::default(), &booting(), &Settings::default());
        assert!(
            text.contains("lazyview_liveness_limit_seconds 900\n"),
            "{text}"
        );
    }

    #[test]
    fn tracker_measures() {
        let m = Metrics::default();
        let t = Tracker::new();
        Metrics::set(&m.gauges.starting, 1);
        let o = t.observe(&m);
        assert!(o.booting() && o.stream_seen == 0 && o.applied_changed == 0);
        Metrics::set(&m.gauges.standby, 1);
        let o = t.observe(&m);
        assert!(!o.booting() && o.role() == "standby");
        Metrics::set(&m.gauges.standby, 0);
        let o = t.observe(&m);
        assert!(o.booting() && !o.started());
        Metrics::set(&m.gauges.streaming, 1);
        Metrics::set(&m.gauges.starting, 0);
        m.set_applied(Position {
            file: "f".into(),
            pos: 1,
        });
        let o = t.observe(&m);
        assert!(o.started() && o.stream_seen >= o.since && o.applied_changed >= o.since);
        Metrics::set(&m.gauges.streaming, 0);
        let o = t.observe(&m);
        assert!(o.started() && o.stream_seen > 0 && o.disconnected_for() <= 1);
    }

    #[test]
    fn request_parsing() {
        let p = |s: &str| parse_head(s.as_bytes());
        let r = p("GET /readyz?verbose&standby=ok&building=ok&require_all=0 HTTP/1.1\r\nHost: x\r\nAccept: text/html, Application/JSON\r\n\r\n").unwrap();
        assert_eq!(
            (r.method.as_str(), r.path.as_str(), r.head_only),
            ("GET", "/readyz", false)
        );
        assert_eq!(r.flag("verbose"), Some(true));
        assert_eq!(r.flag("standby"), Some(true));
        assert_eq!(r.flag("building"), Some(true));
        assert_eq!(r.flag("require_all"), Some(false));
        assert_eq!(r.flag("nope"), None);
        assert!(r.wants_json());
        let r = p("HEAD http://pod:8080/livez/?format=text HTTP/1.0\n\n").unwrap();
        assert_eq!(
            (r.path.as_str(), r.head_only, r.wants_json()),
            ("/livez", true, false)
        );
        assert_eq!(p("GET http://pod:8080 HTTP/1.1\r\n\r\n").unwrap().path, "/");
        assert_eq!(
            p("GET /metrics#x HTTP/1.1\r\n\r\n").unwrap().path,
            "/metrics"
        );
        assert!(p("GET /readyz?format=json HTTP/1.1\r\n\r\n")
            .unwrap()
            .wants_json());
        assert_eq!(
            p("GET /readyz?standby=0 HTTP/1.1\r\n\r\n")
                .unwrap()
                .flag("standby"),
            Some(false)
        );
        assert_eq!(p("GET /readyz\r\n\r\n"), Err(400));
        assert_eq!(p("GET /readyz HTTP/2\r\n\r\n"), Err(505));
        assert_eq!(p("GET readyz HTTP/1.1\r\n\r\n"), Err(400));
        assert_eq!(p("\r\n\r\n"), Err(400));
        assert_eq!(parse_head(b"GET /\xff HTTP/1.1\r\n\r\n"), Err(400));
    }

    async fn exchange(ctx: &Ctx, raw: &[u8]) -> String {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        client.write_all(raw).await.unwrap();
        // Half-close: a request without the final blank line is still answered.
        client.shutdown().await.unwrap();
        let state = AtomicU8::new(SILENT);
        handle(server, ctx, &state).await.unwrap();
        let mut out = Vec::new();
        client.read_to_end(&mut out).await.unwrap();
        String::from_utf8(out).unwrap()
    }

    fn status(resp: &str) -> u16 {
        resp.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    fn body(resp: &str) -> &str {
        resp.split_once("\r\n\r\n").unwrap().1
    }

    #[tokio::test]
    async fn http_exchanges() {
        let m = Arc::new(Metrics::default());
        m.beat();
        Metrics::set(&m.gauges.starting, 1);
        let ctx = Ctx::new(m.clone(), Settings::default());

        let r = exchange(&ctx, b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status(&r), 200);
        assert!(
            r.contains("Connection: close\r\n") && r.contains("Content-Length: 3\r\n"),
            "{r}"
        );
        assert_eq!(body(&r), "ok\n");
        assert_eq!(
            status(&exchange(&ctx, b"GET /healthz HTTP/1.0\r\n\r\n").await),
            200
        );
        let r = exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!(status(&r), 503);
        assert!(body(&r).starts_with("not ready: starting"), "{r}");
        // booting (e.g. waiting for the database): started, not ready
        let r = exchange(&ctx, b"GET /startupz?verbose HTTP/1.1\r\n\r\n").await;
        assert_eq!(status(&r), 200, "{r}");
        assert!(body(&r).contains("[+]phase ok: booting"), "{r}");

        // standby: alive + started, not ready unless ?standby=ok
        Metrics::set(&m.gauges.standby, 1);
        assert_eq!(
            status(&exchange(&ctx, b"GET /startupz HTTP/1.1\r\n\r\n").await),
            200
        );
        assert_eq!(
            status(&exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await),
            503
        );
        let r = exchange(&ctx, b"GET /readyz?standby=ok HTTP/1.1\r\n\r\n").await;
        assert_eq!((status(&r), body(&r)), (200, "ready (hot standby)\n"));

        // active, streaming, first build running
        Metrics::set(&m.gauges.standby, 0);
        Metrics::set(&m.gauges.starting, 0);
        Metrics::set(&m.gauges.streaming, 1);
        Metrics::set(&m.gauges.views, 1);
        let r = exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!(status(&r), 503, "{r}");
        let r = exchange(&ctx, b"GET /readyz?standby=ok&building=ok HTTP/1.1\r\n\r\n").await;
        assert_eq!(
            (status(&r), body(&r)),
            (200, "ready; 1 view(s) building their first version\n")
        );
        Metrics::set(&m.gauges.views_ready, 1);
        let r = exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!((status(&r), body(&r)), (200, "ready\n"));

        // HEAD: same status and headers, no body
        let r = exchange(&ctx, b"HEAD /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!(status(&r), 200);
        assert!(
            r.contains("Content-Length: 6\r\n") && r.ends_with("\r\n\r\n"),
            "{r:?}"
        );

        // JSON via Accept or ?format=json
        let r = exchange(
            &ctx,
            b"GET /readyz HTTP/1.1\r\nAccept: application/json\r\n\r\n",
        )
        .await;
        assert!(r.contains("Content-Type: application/json\r\n"), "{r}");
        let j: serde_json::Value = serde_json::from_str(body(&r)).unwrap();
        assert_eq!(
            (j["status"].as_str(), j["role"].as_str()),
            (Some("ok"), Some("active"))
        );
        let r = exchange(&ctx, b"GET /livez?format=json HTTP/1.1\r\n\r\n").await;
        assert!(
            serde_json::from_str::<serde_json::Value>(body(&r)).is_ok(),
            "{r}"
        );

        let r = exchange(&ctx, b"GET /metrics HTTP/1.1\r\n\r\n").await;
        assert!(
            r.contains("Content-Type: text/plain; version=0.0.4")
                && body(&r).contains("lazyview_ready 1\n"),
            "{r}"
        );
        assert_eq!(
            status(&exchange(&ctx, b"GET / HTTP/1.1\r\n\r\n").await),
            200
        );
        assert_eq!(
            status(&exchange(&ctx, b"GET /nope HTTP/1.1\r\n\r\n").await),
            404
        );
        let r = exchange(&ctx, b"POST /readyz HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await;
        assert_eq!(status(&r), 405);
        assert!(r.contains("Allow: GET, HEAD\r\n"), "{r}");
        assert_eq!(
            status(&exchange(&ctx, b"BREW /coffee HTTP/1.1\r\n\r\n").await),
            404
        );
        assert_eq!(status(&exchange(&ctx, b"garbage\r\n\r\n").await), 400);
        // request line only, then the client half-closes: still answered
        assert_eq!(
            status(&exchange(&ctx, b"GET /livez HTTP/1.0\r\n").await),
            200
        );
        let mut big = b"GET /livez HTTP/1.1\r\nX: ".to_vec();
        big.extend(std::iter::repeat_n(b'a', MAX_HEAD + 10));
        big.extend_from_slice(b"\r\n\r\n");
        assert_eq!(status(&exchange(&ctx, &big).await), 431);
        // connection closed without a request: no response, no error
        assert_eq!(exchange(&ctx, b"").await, "");
    }

    #[tokio::test]
    async fn slow_client_gets_408() {
        let ctx = Ctx {
            head_timeout: Duration::from_millis(200),
            ..Ctx::new(Arc::new(Metrics::default()), Settings::default())
        };
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"GET /livez HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        let h = tokio::spawn(async move {
            let state = AtomicU8::new(SILENT);
            let r = handle(server, &ctx, &state).await;
            (r, state.load(Ordering::Relaxed))
        });
        let mut out = Vec::new();
        client.read_to_end(&mut out).await.unwrap();
        let (r, state) = h.await.unwrap();
        r.unwrap();
        assert_eq!(state, ANSWERING);
        assert!(String::from_utf8(out).unwrap().starts_with("HTTP/1.1 408 "));
    }

    fn entry(ip: &str, age_ms: u64, state: u8, now: Instant) -> ConnEntry {
        ConnEntry {
            ip: ip.parse().unwrap(),
            accepted: now - Duration::from_millis(age_ms),
            state: Arc::new(AtomicU8::new(state)),
            _cancel: oneshot::channel().0,
        }
    }

    #[test]
    fn eviction_picks_idle_connections_of_the_heaviest_client() {
        let now = Instant::now() + Duration::from_secs(10);
        let table = |entries: Vec<ConnEntry>| ConnTable {
            next: entries.len() as u64,
            conns: entries
                .into_iter()
                .enumerate()
                .map(|(i, e)| (i as u64, e))
                .collect(),
            last_warn: None,
        };
        // node (kubelet) with a fresh connection, a pod hogging idle ones
        let t = table(vec![
            entry("10.0.0.1", 5, SILENT, now),    // 0 kubelet, just connected
            entry("10.1.0.9", 1500, SILENT, now), // 1 hog, oldest
            entry("10.1.0.9", 900, SILENT, now),  // 2 hog
            entry("10.0.0.1", 3000, ANSWERING, now), // 3 being answered
        ]);
        assert_eq!(t.victim(now), Some(1));
        // idle (silent past the grace) beats a busier client whose connections are all fresh
        let t = table(vec![
            entry("10.0.0.1", 5, SILENT, now),
            entry("10.0.0.1", 10, SILENT, now),
            entry("10.0.0.1", 20, RECEIVING, now),
            entry("10.2.0.1", 200, SILENT, now), // 3
        ]);
        assert_eq!(t.victim(now), Some(3));
        // among idle ones: the address with the most connections first, then the oldest
        let t = table(vec![
            entry("10.2.0.1", 5000, SILENT, now),
            entry("10.3.0.1", 300, SILENT, now), // 1
            entry("10.3.0.1", 400, SILENT, now), // 2
        ]);
        assert_eq!(t.victim(now), Some(2));
        // partial heads are dropped before fresh silent ones, never a response being written
        let t = table(vec![
            entry("10.0.0.1", 5, SILENT, now),
            entry("10.4.0.1", 50, RECEIVING, now), // 1
            entry("10.4.0.1", 5000, ANSWERING, now),
        ]);
        assert_eq!(t.victim(now), Some(1));
        let t = table(vec![entry("10.0.0.1", 5000, ANSWERING, now)]);
        assert_eq!(t.victim(now), None);
    }

    async fn get_livez(addr: SocketAddr) -> Result<(Duration, String)> {
        let t0 = Instant::now();
        let mut s = TcpStream::connect(addr).await?;
        s.write_all(b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n")
            .await?;
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out)).await??;
        Ok((t0.elapsed(), String::from_utf8_lossy(&out).to_string()))
    }

    /// Idle connections beyond the limit (from this and other addresses) never delay a probe.
    #[tokio::test]
    async fn idle_connections_do_not_starve_probes() {
        let m = Arc::new(Metrics::default());
        m.beat();
        let addr: SocketAddr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let ctx = Arc::new(Ctx {
            max_conns: 16,
            head_timeout: Duration::from_secs(30),
            ..Ctx::new(m, Settings::default())
        });
        let server = tokio::spawn(serve_ctx(addr, ctx.clone()));
        let mut up = false;
        for _ in 0..50 {
            if probe(addr).await {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(up);
        // 40 idle connections from 127.0.0.1 and 40 from 127.0.0.2 (limit 16)
        let mut idle = Vec::new();
        for i in 0..80 {
            let sock = tokio::net::TcpSocket::new_v4().unwrap();
            let src: IpAddr = if i % 2 == 0 {
                [127, 0, 0, 1]
            } else {
                [127, 0, 0, 2]
            }
            .into();
            sock.bind(SocketAddr::new(src, 0)).unwrap();
            idle.push(sock.connect(addr).await.unwrap());
        }
        tokio::time::sleep(SILENT_GRACE * 2).await;
        for _ in 0..10 {
            let (took, resp) = get_livez(addr).await.unwrap();
            assert!(resp.starts_with("HTTP/1.1 200 "), "{resp:?}");
            assert!(took < Duration::from_secs(1), "probe took {took:?}");
        }
        assert!(ctx.table().conns.len() <= 16);
        // the oldest idle connections were closed by the server
        let mut closed = 0;
        for s in &mut idle {
            let mut b = [0u8; 1];
            if let Ok(Ok(0)) = tokio::time::timeout(Duration::from_millis(20), s.read(&mut b)).await
            {
                closed += 1;
            }
        }
        assert!(closed >= 60, "only {closed} idle connections were closed");
        server.abort();
    }

    #[tokio::test]
    async fn tcp_serve_bind_and_probe() {
        let m = Arc::new(Metrics::default());
        m.beat();
        // Port in use: serve keeps retrying instead of failing.
        let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = blocker.local_addr().unwrap();
        let server = tokio::spawn(serve_with(addr, m.clone(), Settings::default()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!server.is_finished());
        drop(blocker);
        let mut up = false;
        for _ in 0..100 {
            if probe(addr).await {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            up,
            "health endpoint did not come up after the port was freed"
        );
        assert!(!probe_path(addr, "/readyz").await);
        // `0.0.0.0` is probed through loopback.
        assert!(probe(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), addr.port())).await);
        server.abort();
        let _ = server.await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !probe(addr).await,
            "listener must stop with the serve future"
        );
    }
}
