//! Counters exposed for logs and tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::binlog::Position;

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default, Debug)]
        pub struct Metrics {
            $(pub $name: AtomicU64,)*
            applied: Mutex<Option<Position>>,
            pub gauges: Gauges,
            /// Per-pool supervisor gauges (one entry per connection pool).
            pub pools: Mutex<Vec<Arc<PoolGauges>>>,
            /// Per-view update statistics (`/stats`, see src/stats.rs), shared with the
            /// aggregator task the engine starts.
            pub view_stats: Arc<crate::stats::Registry>,
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
    // Of mv_rows_inserted: rows upserted by their row key (INSERT ... ON DUPLICATE KEY UPDATE).
    mv_rows_upserted,
    mv_rows_deleted,
    full_rebuilds,
    // Targets brought up to date from the changes kept for them (`retry_lag`).
    target_catchups,
    // Failed refresh attempts of live views (each retried with backoff, never a rebuild).
    refresh_errors,
    definition_reloads,
    // Definitions whose planning took longer than LAZYVIEW_PLAN_TIMEOUT_MS.
    plans_timed_out,
    // Kept for dashboards: lookups no longer give up (they run in chunks), so this stays 0.
    lookback_overflows,
    // Refresh supervisor (docs/design-supervisor.md §5).
    supervisor_increases,
    supervisor_decreases,
    supervisor_overload_errors,
    supervisor_retries,
    supervisor_zombie_kills,
    supervisor_cpu_offloads,
    flushes_overlapped,
    binlog_backpressure,
    // Child rows a mapped cascade changed, replayed from the in-memory cascade cache
    // (src/cascade.rs): the global sum over views, added in publish_stats.
    cascade_rows,
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
    /// Rows read from the sources and not yet written to every target.
    pub inflight_rows: AtomicU64,
}

/// Gauges of one supervisor connection pool (`pool` = `read` | `write`, `endpoint` = `source` or
/// a target name). Floats are stored as `f64` bits.
#[derive(Debug, Default)]
pub struct PoolGauges {
    pub pool: String,
    pub endpoint: String,
    /// Current concurrency limit L (float).
    pub limit: AtomicU64,
    pub max: AtomicU64,
    pub inflight: AtomicU64,
    /// Most operations in flight at once since start.
    pub peak: AtomicU64,
    pub queued: AtomicU64,
    /// Open connections of the pool (in use + idle).
    pub connections: AtomicU64,
    /// Median op time of the last limiter round (float seconds).
    pub rtt_seconds: AtomicU64,
    /// Baseline of the bucket with the most samples in the last round (float seconds).
    pub rtt_baseline_seconds: AtomicU64,
    /// Last gradient g = median(rtt / baseline) (float).
    pub gradient: AtomicU64,
}

impl PoolGauges {
    pub fn new(pool: &str, endpoint: &str) -> PoolGauges {
        PoolGauges {
            pool: pool.to_string(),
            endpoint: endpoint.to_string(),
            ..Default::default()
        }
    }

    /// `(name, help, value)` of every gauge that is maintained: with fixed limits the
    /// connection count and the limiter's round statistics (`connections`, `rtt_seconds`,
    /// `rtt_baseline_seconds`, `gradient`) are not, so they are not shown either.
    pub fn values(&self) -> Vec<(&'static str, &'static str, f64)> {
        let f = |g: &AtomicU64| Metrics::get_f64(g);
        let u = |g: &AtomicU64| Metrics::get(g) as f64;
        vec![
            (
                "limit",
                "Current concurrency limit of the pool.",
                f(&self.limit),
            ),
            ("max", "Maximum concurrency (pool size).", u(&self.max)),
            (
                "inflight",
                "Operations running on the pool.",
                u(&self.inflight),
            ),
            (
                "peak_inflight",
                "Most operations running on the pool at once since start.",
                u(&self.peak),
            ),
            (
                "queued",
                "Operations waiting for a permit.",
                u(&self.queued),
            ),
        ]
    }
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Locks `m`, recovering a poisoned mutex (a thread panicked while holding it): the mutexes this
/// is used for hold metrics and per-refresh bookkeeping, where the value a panicking thread left
/// is still usable, so one panic does not turn into a panic in every later caller.
pub fn lock<T: ?Sized>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
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

    pub fn set_f64(g: &AtomicU64, v: f64) {
        g.store(v.to_bits(), Ordering::Relaxed);
    }

    pub fn get_f64(g: &AtomicU64) -> f64 {
        f64::from_bits(g.load(Ordering::Relaxed))
    }

    /// Registers the gauges of a supervisor pool (rendered on `/metrics`).
    pub fn register_pool(&self, g: Arc<PoolGauges>) {
        lock(&self.pools).push(g);
    }

    pub fn pool_gauges(&self) -> Vec<Arc<PoolGauges>> {
        lock(&self.pools).clone()
    }

    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    /// Binlog position up to which every change has been applied to the views.
    pub fn applied(&self) -> Option<Position> {
        lock(&self.applied).clone()
    }

    pub fn set_applied(&self, p: Position) {
        *lock(&self.applied) = Some(p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_recovers_a_poisoned_mutex() {
        let m = Mutex::new(1);
        let r = std::panic::catch_unwind(|| {
            let _g = m.lock().unwrap();
            panic!("poisons the mutex");
        });
        assert!(r.is_err() && m.is_poisoned());
        *lock(&m) += 1;
        assert_eq!(*lock(&m), 2);
    }
}
