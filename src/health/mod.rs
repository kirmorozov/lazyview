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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::{bail, Result};

use crate::binlog::Position;
use crate::metrics::{unix_now, Metrics};

mod probes;
mod prometheus;
mod server;

pub use probes::{evaluate, Check, Probe, ReadyOpts, Report};
pub use server::{probe, probe_path, serve, serve_with};

pub const DEFAULT_STALL_SECS: u64 = 120;
pub const DEFAULT_BOOT_STALL_SECS: u64 = 900;
pub const DEFAULT_DISCONNECT_GRACE_SECS: u64 = 30;
/// Smallest accepted `LAZYVIEW_LIVENESS_STALL_SECS`: the engine beats every few seconds (standby
/// polls ≤ 5 s, binlog heartbeats), lower values would flap.
pub const MIN_STALL_SECS: u64 = 30;
const DEFAULT_HEARTBEAT_MS: u64 = 5_000;

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

pub(super) fn parse_bool(v: &str) -> Option<bool> {
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

pub(super) struct Tracker {
    since: u64,
    stream_seen: AtomicU64,
    applied: Mutex<(Option<Position>, u64)>,
}

impl Tracker {
    pub(super) fn new() -> Tracker {
        Tracker {
            since: unix_now(),
            stream_seen: AtomicU64::new(0),
            applied: Mutex::new((None, 0)),
        }
    }

    pub(super) fn observe(&self, m: &Metrics) -> Observed {
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

// ------------------------------------------------------------------------------------ tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    pub(super) const NOW: u64 = 1_800_000_000;

    /// Active, streaming, 3 views live, heartbeat fresh.
    pub(super) fn healthy() -> Observed {
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
    pub(super) fn booting() -> Observed {
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

    pub(super) fn standby() -> Observed {
        Observed {
            standby: true,
            ..booting()
        }
    }

    /// Took the lock, still booting (loading views, first binlog connect).
    pub(super) fn took_over() -> Observed {
        Observed {
            standby: false,
            ..standby()
        }
    }

    pub(super) fn aged(o: Observed, secs: u64) -> Observed {
        Observed {
            heartbeat: NOW - secs,
            ..o
        }
    }

    pub(super) fn stalled(o: Observed) -> Observed {
        let limit = o.stall_limit(&Settings::default());
        aged(o, limit)
    }

    pub(super) fn run(p: Probe, o: &Observed) -> Report {
        evaluate(p, o, &Settings::default(), ReadyOpts::default())
    }

    pub(super) fn failing(r: &Report) -> Vec<&'static str> {
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
}
