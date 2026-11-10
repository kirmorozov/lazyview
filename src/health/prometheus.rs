// ------------------------------------------------------------------------------------ metrics

use std::fmt::Write as _;

use crate::metrics::Metrics;

use super::{evaluate, Observed, Probe, ReadyOpts, Settings};

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(super) fn prometheus(m: &Metrics, o: &Observed, s: &Settings) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binlog::Position;
    use crate::health::tests::{booting, healthy, standby, NOW};

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
}
