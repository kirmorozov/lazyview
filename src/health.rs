//! HTTP endpoint for Kubernetes probes, container health checks and Prometheus scraping.
//!
//! | path | probe | 200 when | 503 when |
//! |---|---|---|---|
//! | `/livez` (alias `/healthz`) | liveness | the engine loop made progress within `LAZYVIEW_LIVENESS_STALL_SECS` | the process is wedged |
//! | `/readyz` | readiness | active, streaming, every view has a live version | see below |
//! | `/startupz` | startup | booted: reached hot standby or connected the binlog stream once | still booting |
//! | `/metrics` | — | always (Prometheus text format) | — |
//!
//! `?verbose` lists every check (Kubernetes apiserver style), `?format=json` or
//! `Accept: application/json` returns JSON. `HEAD` is answered like `GET` without a body.
//!
//! # Semantics (and why)
//!
//! **Liveness** fails only when a restart is the right remedy: the engine loop has not reported
//! progress (`Metrics::beat` / `gauges.heartbeat_unix`) for `LAZYVIEW_LIVENESS_STALL_SECS`
//! (default 120). The engine beats on every loop iteration, between flush/build chunks, while
//! retrying database connections and while waiting for the instance lock, so long view builds,
//! long flushes, database outages, binlog lag and hot standby never fail it. A missing beat
//! means an await that never returns (a hung statement on a half-open connection, a deadlock),
//! which a restart does fix. Nothing that depends on the database or on user definitions is
//! part of liveness: restarting would not help and would only add a rebuild.
//!
//! **Readiness** answers "is this instance maintaining fresh views right now". It fails while
//! * the engine loop is stalled (as liveness);
//! * hot standby: another replica holds the instance lock (`gauges.standby`) — unless
//!   `?standby=ok`, which counts a healthy standby as ready (the Kubernetes readinessProbe uses
//!   this: a singleton workload whose standby is never Ready deadlocks `RollingUpdate` rollouts
//!   and makes every PodDisruptionBudget block node drains);
//! * starting: the binlog stream was never connected (`gauges.starting`);
//! * the binlog stream has been disconnected for longer than
//!   `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (default 30; short reconnects do not flap);
//! * a view has no live version yet (first build). A view whose live table is served while a new
//!   version builds in the background counts as live;
//! * a view is in error, **only** with `LAZYVIEW_READY_REQUIRE_ALL=true` (or `?require_all=1`).
//!   By default error views are reported (body, `lazyview_views_error`, `_lazyview_state`) but
//!   ignored: an invalid user definition is not a property of the pod, and an unready pod would
//!   only block rollouts and drains without fixing anything. Alert on `lazyview_views_error`.
//!
//! **Startup** (`/startupz`) passes once the process booted: it reached hot standby or connected
//! the binlog stream at least once (latched). Kubernetes runs liveness only after the startup
//! probe passed, so the startup probe's budget (`failureThreshold × periodSeconds`) bounds the
//! boot (waiting for the database, metadata migrations, loading definitions, acquiring the lock).
//!
//! The server is tiny on purpose (tokio only): one request per connection (`Connection: close`),
//! request heads up to 8 KiB read within 5 s (slow clients get `408` and are dropped), at most
//! 64 connections at once, unspecified addresses (`0.0.0.0` / `[::]`) listen on IPv4 and IPv6,
//! and a failed bind is retried every 5 s without affecting the engine.

use std::fmt::Write as _;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::binlog::Position;
use crate::metrics::{unix_now, Metrics};

pub const DEFAULT_STALL_SECS: u64 = 120;
pub const DEFAULT_DISCONNECT_GRACE_SECS: u64 = 30;

const HEAD_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HEAD: usize = 8 * 1024;
const MAX_CONNECTIONS: usize = 64;
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
const REBIND_EVERY: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

// ------------------------------------------------------------------------------------ settings

/// Probe policy from `LAZYVIEW_LIVENESS_STALL_SECS`, `LAZYVIEW_READY_REQUIRE_ALL` and
/// `LAZYVIEW_READY_DISCONNECT_GRACE_SECS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Liveness fails after this many seconds without an engine heartbeat.
    pub stall_secs: u64,
    /// Readiness also requires that no view is in error.
    pub require_all: bool,
    /// Readiness tolerates a disconnected binlog stream for this long.
    pub disconnect_grace_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            stall_secs: DEFAULT_STALL_SECS,
            require_all: false,
            disconnect_grace_secs: DEFAULT_DISCONNECT_GRACE_SECS,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Settings> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings> {
        let d = Settings::default();
        let num = |k: &str, default: u64| -> Result<u64> {
            match get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
            {
                None => Ok(default),
                Some(v) => v
                    .parse()
                    .with_context(|| format!("{k} must be a number of seconds, got {v}")),
            }
        };
        let stall_secs = num("LAZYVIEW_LIVENESS_STALL_SECS", d.stall_secs)?;
        if stall_secs == 0 {
            bail!("LAZYVIEW_LIVENESS_STALL_SECS must be > 0");
        }
        let require_all = match get("LAZYVIEW_READY_REQUIRE_ALL")
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
        {
            None => d.require_all,
            Some(v) => parse_bool(&v).with_context(|| {
                format!("LAZYVIEW_READY_REQUIRE_ALL: expected true/false, got {v}")
            })?,
        };
        Ok(Settings {
            stall_secs,
            require_all,
            disconnect_grace_secs: num(
                "LAZYVIEW_READY_DISCONNECT_GRACE_SECS",
                d.disconnect_grace_secs,
            )?,
        })
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
    /// Reached hot standby or a connected binlog stream at least once (latched).
    pub booted: bool,
    /// Unix seconds the applied binlog position last changed (0 = none applied yet).
    pub applied_changed: u64,
}

impl Observed {
    pub fn heartbeat_age(&self) -> u64 {
        self.now.saturating_sub(self.heartbeat)
    }

    /// Seconds without a connected binlog stream (0 while streaming).
    pub fn disconnected_for(&self) -> u64 {
        if self.streaming {
            0
        } else {
            self.now.saturating_sub(if self.stream_seen > 0 {
                self.stream_seen
            } else {
                self.since
            })
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
    booted: AtomicBool,
    applied: Mutex<(Option<Position>, u64)>,
}

impl Tracker {
    fn new() -> Tracker {
        Tracker {
            since: unix_now(),
            stream_seen: AtomicU64::new(0),
            booted: AtomicBool::new(false),
            applied: Mutex::new((None, 0)),
        }
    }

    fn observe(&self, m: &Metrics) -> Observed {
        let g = &m.gauges;
        let now = unix_now();
        let streaming = Metrics::get(&g.streaming) != 0;
        let standby = Metrics::get(&g.standby) != 0;
        if streaming {
            self.stream_seen.fetch_max(now, Ordering::Relaxed);
        }
        if streaming || standby {
            self.booted.store(true, Ordering::Relaxed);
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
            standby,
            views: Metrics::get(&g.views),
            views_ready: Metrics::get(&g.views_ready),
            views_building: Metrics::get(&g.views_building),
            views_error: Metrics::get(&g.views_error),
            stream_seen: self.stream_seen.load(Ordering::Relaxed),
            since: self.since,
            booted: self.booted.load(Ordering::Relaxed),
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
}

impl Report {
    /// One line: the verdict, or the first failing check.
    pub fn summary(&self) -> String {
        if let Some(c) = self.checks.iter().find(|c| !c.ok) {
            return match self.probe {
                Probe::Live => c.detail.clone(),
                Probe::Ready => format!("not ready: {}", c.detail),
                Probe::Startup => format!("not started: {}", c.detail),
            };
        }
        match self.probe {
            Probe::Live | Probe::Startup => "ok".into(),
            Probe::Ready => {
                let mut s = String::from("ready");
                if self.role == "standby" {
                    s.push_str(" (hot standby)");
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
    let age = o.heartbeat_age();
    checks.push(if age >= s.stall_secs {
        check(
            "engine",
            false,
            format!(
                "engine loop stalled: no progress for {age}s (limit {}s)",
                s.stall_secs
            ),
        )
    } else {
        check(
            "engine",
            true,
            format!("engine loop progressing (last heartbeat {age}s ago)"),
        )
    });
    match probe {
        Probe::Live => {}
        Probe::Startup => checks.push(if o.booted {
            check("booted", true, if o.standby { "hot standby" } else { "binlog stream connected" })
        } else {
            check("booted", false, "starting: waiting for the database, migrating metadata or acquiring the instance lock")
        }),
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
            checks.push(if pending > 0 {
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
            "1 while /readyz passes (a hot standby is not ready).",
            b(ready),
        ),
        (
            "heartbeat_age_seconds",
            "Seconds since the engine loop last reported progress.",
            o.heartbeat_age(),
        ),
        (
            "binlog_disconnected_seconds",
            "Seconds without a connected binlog stream (0 while streaming).",
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
    if let Some(p) = m.applied() {
        let _ = writeln!(
            out,
            "# TYPE lazyview_applied_binlog_pos gauge\nlazyview_applied_binlog_pos{{file=\"{}\"}} {}",
            escape_label(&p.file),
            p.pos
        );
    }
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
/readyz     readiness: active, streaming, every view live (?standby=ok counts a hot standby as ready)\n\
/startupz   startup: booted (hot standby, or the binlog stream connected once)\n\
/metrics    Prometheus metrics\n\
?verbose lists every check; ?format=json (or Accept: application/json) returns JSON\n";

struct Ctx {
    metrics: Arc<Metrics>,
    settings: Settings,
    tracker: Tracker,
    head_timeout: Duration,
}

impl Ctx {
    fn new(metrics: Arc<Metrics>, settings: Settings) -> Ctx {
        Ctx {
            metrics,
            settings,
            tracker: Tracker::new(),
            head_timeout: HEAD_TIMEOUT,
        }
    }

    fn respond(&self, req: &Request) -> Response {
        let probe = match req.path.as_str() {
            "/livez" | "/healthz" => Some(Probe::Live),
            "/readyz" => Some(Probe::Ready),
            "/startupz" => Some(Probe::Startup),
            "/metrics" | "/" => None,
            _ => return Response::text(404, "not found\n"),
        };
        if req.method != "GET" && req.method != "HEAD" {
            return Response {
                allow: true,
                ..Response::text(405, "only GET and HEAD are supported\n")
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
async fn read_head<S: AsyncRead + Unpin>(sock: &mut S) -> io::Result<Head> {
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

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(mut sock: S, ctx: &Ctx) -> io::Result<()> {
    let (resp, head_only) = match tokio::time::timeout(ctx.head_timeout, read_head(&mut sock)).await
    {
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

async fn accept_loop(listener: TcpListener, ctx: Arc<Ctx>, permits: Arc<Semaphore>) {
    let mut conns = JoinSet::new();
    loop {
        while conns.try_join_next().is_some() {}
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok((sock, peer)) => {
                let _ = sock.set_nodelay(true);
                let ctx = ctx.clone();
                conns.spawn(async move {
                    if let Err(e) = handle(sock, &ctx).await {
                        debug!(%peer, "health request failed: {e}");
                    }
                    drop(permit);
                });
            }
            Err(e) => {
                // EMFILE / ENFILE / ECONNABORTED …: back off instead of spinning.
                debug!("health endpoint accept failed: {e}");
                drop(permit);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Unspecified addresses listen on both IPv6 (`[::]`, dual-stack where the kernel allows) and
/// IPv4 (`0.0.0.0`), so the probes work in IPv4, IPv6 and dual-stack clusters alike.
async fn bind(addr: SocketAddr) -> io::Result<Vec<TcpListener>> {
    if !addr.ip().is_unspecified() || addr.port() == 0 {
        return Ok(vec![TcpListener::bind(addr).await?]);
    }
    let mut out = Vec::new();
    let mut first_err = None;
    for a in [
        SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), addr.port()),
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), addr.port()),
    ] {
        match TcpListener::bind(a).await {
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

/// Serves the endpoint until the future is dropped (policy from the environment).
pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) {
    let settings = Settings::from_env().unwrap_or_else(|e| {
        warn!("health settings: {e:#}; using defaults");
        Settings::default()
    });
    serve_with(addr, metrics, settings).await
}

/// Serves the endpoint until the future is dropped. A failed bind is retried every 5 s; it
/// never stops the engine. Dropping the future stops the listeners and open connections.
pub async fn serve_with(addr: SocketAddr, metrics: Arc<Metrics>, settings: Settings) {
    let ctx = Arc::new(Ctx::new(metrics, settings));
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
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
                            std::net::IpAddr::V6(ip) if ip.is_unspecified() && ls.len() == 1 => {
                                format!("http://{a} (IPv4 and IPv6)")
                            }
                            _ => format!("http://{a}"),
                        })
                        .collect();
                    info!(
                        "health endpoint on {} (/livez /readyz /startupz /metrics; stall {}s, require_all {}, disconnect grace {}s)",
                        shown.join(" and "),
                        ctx.settings.stall_secs,
                        ctx.settings.require_all,
                        ctx.settings.disconnect_grace_secs
                    );
                    for l in ls {
                        listeners.spawn(accept_loop(l, ctx.clone(), permits.clone()));
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
    use std::collections::HashMap;

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
            booted: true,
            applied_changed: NOW - 2,
        }
    }

    fn booting() -> Observed {
        Observed {
            streaming: false,
            starting: true,
            views: 0,
            views_ready: 0,
            stream_seen: 0,
            booted: false,
            since: NOW - 5,
            applied_changed: 0,
            ..healthy()
        }
    }

    fn standby() -> Observed {
        Observed {
            standby: true,
            booted: true,
            ..booting()
        }
    }

    fn stalled(o: Observed) -> Observed {
        Observed {
            heartbeat: NOW - DEFAULT_STALL_SECS,
            ..o
        }
    }

    fn run(p: Probe, o: &Observed) -> Report {
        evaluate(p, o, &Settings::default(), ReadyOpts::default())
    }

    fn failing(r: &Report) -> Vec<&'static str> {
        r.checks.iter().filter(|c| !c.ok).map(|c| c.name).collect()
    }

    #[test]
    fn settings_from_lookup() {
        let s = |pairs: &[(&str, &str)]| {
            let m: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            Settings::from_lookup(|k| m.get(k).cloned())
        };
        assert_eq!(
            s(&[]).unwrap(),
            Settings {
                stall_secs: 120,
                require_all: false,
                disconnect_grace_secs: 30
            }
        );
        assert_eq!(
            s(&[
                ("LAZYVIEW_LIVENESS_STALL_SECS", " 300 "),
                ("LAZYVIEW_READY_REQUIRE_ALL", "TRUE"),
                ("LAZYVIEW_READY_DISCONNECT_GRACE_SECS", "0")
            ])
            .unwrap(),
            Settings {
                stall_secs: 300,
                require_all: true,
                disconnect_grace_secs: 0
            }
        );
        assert!(
            !s(&[("LAZYVIEW_READY_REQUIRE_ALL", "")])
                .unwrap()
                .require_all
        );
        assert!(s(&[("LAZYVIEW_LIVENESS_STALL_SECS", "0")]).is_err());
        assert!(s(&[("LAZYVIEW_LIVENESS_STALL_SECS", "2m")]).is_err());
        assert!(s(&[("LAZYVIEW_READY_REQUIRE_ALL", "maybe")]).is_err());
        assert!(s(&[("LAZYVIEW_READY_DISCONNECT_GRACE_SECS", "-1")]).is_err());
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
        let edge = Observed {
            heartbeat: NOW - DEFAULT_STALL_SECS + 1,
            ..healthy()
        };
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
        let never = Observed {
            heartbeat: 0,
            ..healthy()
        };
        assert!(!run(Probe::Live, &never).ok);
    }

    #[test]
    fn startup_passes_once_booted() {
        let r = run(Probe::Startup, &booting());
        assert!(!r.ok);
        assert_eq!(failing(&r), ["booted"]);
        assert!(
            r.summary().starts_with("not started: starting"),
            "{}",
            r.summary()
        );
        assert!(run(Probe::Startup, &standby()).ok);
        assert!(run(Probe::Startup, &healthy()).ok);
        // A standby that took over is still `starting` until its stream connects: stays started.
        let took_over = Observed {
            standby: false,
            ..standby()
        };
        assert!(run(Probe::Startup, &took_over).ok);
        // Reconnecting after the first stream: still started.
        assert!(
            run(
                Probe::Startup,
                &Observed {
                    streaming: false,
                    ..healthy()
                }
            )
            .ok
        );
        assert!(!run(Probe::Startup, &stalled(standby())).ok);
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
        assert!(!ok(&Observed {
            standby: false,
            ..standby()
        }));
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
        // ?standby=ok changes nothing for an active instance
        assert!(!evaluate(Probe::Ready, &booting(), &Settings::default(), so).ok);
        assert!(evaluate(Probe::Ready, &healthy(), &Settings::default(), so).ok);

        // first build: no live version yet
        let r = run(
            Probe::Ready,
            &Observed {
                views_ready: 1,
                views_building: 2,
                ..healthy()
            },
        );
        assert_eq!(failing(&r), ["views"]);
        assert!(
            r.summary()
                .contains("2 of 3 view(s) have no live version yet"),
            "{}",
            r.summary()
        );
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
        assert_eq!(j["settings"]["liveness_stall_secs"], 120);
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
        let text = prometheus(&m, &o, &Settings::default());
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
        let text = prometheus(&Metrics::default(), &standby(), &Settings::default());
        assert!(
            text.contains("lazyview_ready 0\n") && text.contains("lazyview_standby 1\n"),
            "{text}"
        );
        assert!(!text.contains("applied_position_age"), "{text}");
    }

    #[test]
    fn tracker_latches_and_measures() {
        let m = Metrics::default();
        let t = Tracker::new();
        Metrics::set(&m.gauges.starting, 1);
        let o = t.observe(&m);
        assert!(!o.booted && o.stream_seen == 0 && o.applied_changed == 0);
        Metrics::set(&m.gauges.standby, 1);
        assert!(t.observe(&m).booted);
        Metrics::set(&m.gauges.standby, 0);
        let o = t.observe(&m);
        assert!(o.booted && !o.started());
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
        let r = p("GET /readyz?verbose&standby=ok&require_all=0 HTTP/1.1\r\nHost: x\r\nAccept: text/html, Application/JSON\r\n\r\n").unwrap();
        assert_eq!(
            (r.method.as_str(), r.path.as_str(), r.head_only),
            ("GET", "/readyz", false)
        );
        assert_eq!(r.flag("verbose"), Some(true));
        assert_eq!(r.flag("standby"), Some(true));
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
        handle(server, ctx).await.unwrap();
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
        assert_eq!(
            status(&exchange(&ctx, b"GET /startupz HTTP/1.1\r\n\r\n").await),
            503
        );

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

        // active and streaming
        Metrics::set(&m.gauges.standby, 0);
        Metrics::set(&m.gauges.starting, 0);
        Metrics::set(&m.gauges.streaming, 1);
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
        let h = tokio::spawn(async move { handle(server, &ctx).await });
        let mut out = Vec::new();
        client.read_to_end(&mut out).await.unwrap();
        h.await.unwrap().unwrap();
        assert!(String::from_utf8(out).unwrap().starts_with("HTTP/1.1 408 "));
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
