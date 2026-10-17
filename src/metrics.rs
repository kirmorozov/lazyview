//! Counters exposed for logs and tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::binlog::Position;

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default, Debug)]
        pub struct Metrics {
            $(pub $name: AtomicU64,)*
            applied: Mutex<Option<Position>>,
            pub gauges: Gauges,
        }

        #[derive(Default, Debug, Clone, PartialEq, Eq)]
        pub struct Snapshot {
            $(pub $name: u64,)*
        }

        impl Metrics {
            pub fn snapshot(&self) -> Snapshot {
                Snapshot { $($name: self.$name.load(Ordering::Relaxed),)* }
            }

            /// `(name, value)` of every counter, for the Prometheus endpoint.
            pub fn counters(&self) -> Vec<(&'static str, u64)> {
                vec![$((stringify!($name), self.$name.load(Ordering::Relaxed)),)*]
            }
        }

        impl Snapshot {
            /// Counter differences `self - earlier`.
            pub fn since(&self, earlier: &Snapshot) -> Snapshot {
                Snapshot { $($name: self.$name - earlier.$name,)* }
            }
        }
    };
}

counters!(
    events,
    rows_seen,
    rows_skipped,
    flushes,
    units_refreshed,
    units_changed,
    lookup_queries,
    source_queries,
    mv_queries,
    mv_rows_inserted,
    mv_rows_deleted,
    full_rebuilds,
    definition_reloads,
);

/// Point-in-time values (may go down), maintained by the engine.
#[derive(Default, Debug)]
pub struct Gauges {
    pub views: AtomicU64,
    pub views_ready: AtomicU64,
    pub views_building: AtomicU64,
    pub views_error: AtomicU64,
    /// 1 while connected to the binlog stream.
    pub streaming: AtomicU64,
    /// Unix seconds of the engine loop's last iteration (liveness).
    pub heartbeat_unix: AtomicU64,
    /// 1 while waiting for another instance's lock on the schema (hot standby).
    pub standby: AtomicU64,
    /// 1 from process start until the binlog stream is first connected.
    pub starting: AtomicU64,
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl Metrics {
    /// Marks the engine as making progress (liveness).
    pub fn beat(&self) {
        Metrics::set(&self.gauges.heartbeat_unix, unix_now());
    }

    pub fn set(g: &AtomicU64, v: u64) {
        g.store(v, Ordering::Relaxed);
    }

    pub fn get(g: &AtomicU64) -> u64 {
        g.load(Ordering::Relaxed)
    }

    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    /// Binlog position up to which every change has been applied to the views.
    pub fn applied(&self) -> Option<Position> {
        self.applied.lock().unwrap().clone()
    }

    pub fn set_applied(&self, p: Position) {
        *self.applied.lock().unwrap() = Some(p);
    }
}
