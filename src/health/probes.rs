// ------------------------------------------------------------------------------------ probes

use std::fmt::Write as _;

use serde_json::json;

use super::{Observed, Settings};

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

pub(super) fn render_text(r: &Report, verbose: bool) -> String {
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

pub(super) fn render_json(r: &Report, o: &Observed, s: &Settings) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::tests::*;
    use crate::health::{
        DEFAULT_BOOT_STALL_SECS, DEFAULT_DISCONNECT_GRACE_SECS, DEFAULT_STALL_SECS,
    };

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
}
