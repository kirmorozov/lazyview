//! Concurrent refreshes on connection pools (docs/design-supervisor.md).
//!
//! A refresh of a view splits its dirty units into chunks (each unit in exactly one chunk: the
//! units are de-duplicated and the lookups merged before chunking). Each chunk is one **read
//! op** on the source's `read` pool, then one **write op** per target on that target's `write`
//! pool (old rows, diff, one transaction), all of them running concurrently up to the pools'
//! permits. An op never holds a read and a write permit at once. A refresh returns only after
//! every op it started has finished, so the next refresh of the view never overtakes an older
//! write (invariant I1). Tables without a unit index get one write op at a time per target:
//! their DELETEs scan the table and would wait on each other's row locks.
//!
//! Limits are fixed (`LAZYVIEW_SUPERVISOR=fixed`; `adaptive` runs with the same fixed limits
//! until the adaptive limiter of §4 exists). Overload errors (too many connections, lock wait
//! timeouts, deadlocks, lost connections, timeouts) are retried with backoff inside the op
//! that owns the chunk. A write op that times out or loses its connection after its
//! transaction began may still commit on the server: its connection is killed and the op
//! fails only once the server no longer lists it (the zombie fence, §4.3); a target whose fence
//! could not complete takes no writes until it does.
//!
//! Code constants: chunks of at least [`MIN_CHUNK`] units, [`RETRIES`] retries from
//! [`BACKOFF`], diffs of [`OFFLOAD_ROWS`] rows or [`OFFLOAD_DATUMS`] values off the async
//! threads, idle pooled connections closed after [`INACTIVE_TTL`].

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::accumulate::{ChunkCursor, UnitChunk, UnitSet};
use crate::analyze::ViewPlan;
use crate::config::{SupervisorConfig, SupervisorMode};
use crate::metrics::{Metrics, PoolGauges};
use crate::mv::{self, Applied, Diff, Layout, Prepared, WriteOpts};

/// Smallest chunk a view's units are split into (unless `LAZYVIEW_QUERY_CHUNK` is smaller).
pub const MIN_CHUNK: usize = 100;
/// Retries of an op that failed with an overload error.
pub const RETRIES: u32 = 3;
/// First retry delay (doubling, ± 50 % jitter).
pub const BACKOFF: Duration = Duration::from_millis(50);
/// Diffs (old + new rows) of at least this many rows run off the async threads...
pub const OFFLOAD_ROWS: usize = 4096;
/// ... or of this many values.
pub const OFFLOAD_DATUMS: usize = 65_536;
/// Idle pooled connections close after this long.
pub const INACTIVE_TTL: Duration = Duration::from_secs(30);
/// How long a killed connection may take to leave the server's process list.
const FENCE_WAIT: Duration = Duration::from_secs(5);
/// Getting a pooled connection (connect + session setup when a new one is needed).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// An op exceeded `LAZYVIEW_TARGET_TIMEOUT_MS` (an overload error).
#[derive(Debug)]
pub struct Timeout(pub Duration);

impl std::fmt::Display for Timeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "timed out after {:?}", self.0)
    }
}

impl std::error::Error for Timeout {}

/// Errors that mean the server is overloaded or unreachable rather than that the statement is
/// wrong (§4.2): retried with backoff, and never a reason to rebuild a view.
pub fn is_overload(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.is::<Timeout>()
            || match c.downcast_ref::<mysql_async::Error>() {
                // 1040 too many connections, 1203 max_user_connections, 1205 lock wait timeout,
                // 1213 deadlock, 1053 server shutdown, 2006 / 2013 gone away / lost, 3024 / 1969
                // statement time limit.
                Some(mysql_async::Error::Server(s)) => matches!(
                    s.code,
                    1040 | 1203 | 1205 | 1213 | 1053 | 2006 | 2013 | 3024 | 1969
                ),
                Some(mysql_async::Error::Io(_)) => true,
                Some(mysql_async::Error::Driver(d)) => {
                    matches!(d, mysql_async::DriverError::ConnectionClosed)
                }
                _ => false,
            }
    })
}

/// A connection-class failure: the connection is unusable (and a transaction it began may or
/// may not have committed).
fn is_conn_lost(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(
            c.downcast_ref::<mysql_async::Error>(),
            Some(mysql_async::Error::Io(_))
                | Some(mysql_async::Error::Driver(
                    mysql_async::DriverError::ConnectionClosed
                ))
        ) || matches!(
            c.downcast_ref::<mysql_async::Error>(),
            Some(mysql_async::Error::Server(s)) if s.code == 2006 || s.code == 2013
        )
    })
}

/// Retry delay `BACKOFF · 2^(attempt-1)` with ± 50 % jitter.
fn backoff(attempt: u32) -> Duration {
    let base = BACKOFF * (1u32 << attempt.saturating_sub(1).min(6));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // 0.5 .. 1.5
    base.mul_f64(0.5 + f64::from(nanos % 1000) / 1000.0)
}

/// One connection pool with a fixed number of permits (the pool size).
pub struct Endpoint {
    /// `source` or the target's name.
    pub name: String,
    opts: Opts,
    pool: Pool,
    permits: Arc<Semaphore>,
    pub max: usize,
    gauges: Arc<PoolGauges>,
    /// Connection ids of write ops that may still commit (their fence did not complete). Held
    /// while they are fenced, so no write starts before that is settled.
    zombies: tokio::sync::Mutex<Vec<u32>>,
}

/// A permit of an [`Endpoint`] (the in-flight gauge follows it).
struct Permit {
    _permit: OwnedSemaphorePermit,
    gauges: Arc<PoolGauges>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.gauges.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Endpoint {
    /// A pool of at most `max` connections, created on demand with the service's session
    /// settings (`sql_mode`: the permissive mode `mv::init_session` sets, see there) and never
    /// reset between uses, closing when idle for [`INACTIVE_TTL`].
    pub fn new(
        kind: &str,
        name: &str,
        opts: &Opts,
        max: usize,
        sql_mode: &str,
        metrics: &Metrics,
    ) -> Arc<Endpoint> {
        let max = max.max(1);
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, max).expect("0 <= max, max > 0"))
            .with_reset_connection(false)
            .with_inactive_connection_ttl(INACTIVE_TTL)
            .with_ttl_check_interval(Duration::from_secs(5));
        let setup = vec![
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED".to_string(),
            format!("SET SESSION sql_mode = '{}'", sql_mode.replace('\'', "")),
        ];
        let pool = Pool::new(
            OptsBuilder::from_opts(opts.clone())
                .pool_opts(pool_opts)
                .setup(setup),
        );
        let gauges = Arc::new(PoolGauges::new(kind, name));
        Metrics::set(&gauges.max, max as u64);
        Metrics::set_f64(&gauges.limit, max as f64);
        metrics.register_pool(gauges.clone());
        Arc::new(Endpoint {
            name: name.to_string(),
            opts: opts.clone(),
            pool,
            permits: Arc::new(Semaphore::new(max)),
            max,
            gauges,
            zombies: tokio::sync::Mutex::new(Vec::new()),
        })
    }

    async fn acquire(&self) -> Permit {
        self.gauges.queued.fetch_add(1, Ordering::Relaxed);
        let p = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("permits are never closed");
        self.gauges.queued.fetch_sub(1, Ordering::Relaxed);
        let now = self.gauges.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.gauges.peak.fetch_max(now, Ordering::Relaxed);
        Permit {
            _permit: p,
            gauges: self.gauges.clone(),
        }
    }

    async fn conn(&self) -> Result<Conn> {
        match tokio::time::timeout(CONNECT_TIMEOUT, self.pool.get_conn()).await {
            Ok(r) => r.with_context(|| format!("connecting to {}", self.name)),
            Err(_) => Err(anyhow::Error::new(Timeout(CONNECT_TIMEOUT))
                .context(format!("connecting to {}", self.name))),
        }
    }

    /// Kills connection `id` (a write op that may still commit) and waits until the server no
    /// longer lists it; on failure it stays pending and blocks further writes (see
    /// [`Endpoint::settle_zombies`]).
    async fn fence(&self, id: u32, metrics: &Metrics) -> Result<()> {
        Metrics::add(&metrics.supervisor_zombie_kills, 1);
        let mut pending = self.zombies.lock().await;
        let r = kill_and_wait(&self.opts, id).await;
        if let Err(e) = &r {
            warn!(endpoint = %self.name, connection = id, "could not confirm that a timed-out write is gone: {e:#}; no writes to this target until it is");
            pending.push(id);
        }
        r
    }

    /// Fences the connections of earlier write ops that could not be fenced yet.
    async fn settle_zombies(&self) -> Result<()> {
        let mut pending = self.zombies.lock().await;
        let mut err = None;
        let mut left = Vec::new();
        for &id in pending.iter() {
            if let Err(e) = kill_and_wait(&self.opts, id).await {
                left.push(id);
                err.get_or_insert(e);
            }
        }
        *pending = left;
        match err {
            Some(e) => Err(e.context(format!(
                "{}: an earlier write that timed out may still commit",
                self.name
            ))),
            None => Ok(()),
        }
    }

    pub async fn disconnect(&self) {
        let _ = self.pool.clone().disconnect().await;
    }
}

/// `KILL CONNECTION id` on a fresh connection, then waits until `SHOW PROCESSLIST` (which lists
/// the user's own threads without privileges) no longer has it.
async fn kill_and_wait(opts: &Opts, id: u32) -> Result<()> {
    let fut = async {
        let mut c = Conn::new(opts.clone()).await?;
        match c.query_drop(format!("KILL CONNECTION {id}")).await {
            // 1094: unknown thread id (already gone).
            Ok(()) | Err(mysql_async::Error::Server(_)) => {}
            Err(e) => return Err(anyhow::Error::from(e)),
        }
        let deadline = tokio::time::Instant::now() + FENCE_WAIT;
        loop {
            let rows: Vec<mysql_async::Row> = c.query("SHOW PROCESSLIST").await?;
            let listed = rows.iter().any(|r| {
                r.get::<Option<u64>, _>(0)
                    .flatten()
                    .is_some_and(|x| x == u64::from(id))
            });
            if !listed {
                let _ = c.disconnect().await;
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = c.disconnect().await;
                return Err(anyhow!(
                    "connection {id} still listed {FENCE_WAIT:?} after KILL"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    match tokio::time::timeout(FENCE_WAIT + CONNECT_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(anyhow::Error::new(Timeout(FENCE_WAIT + CONNECT_TIMEOUT))),
    }
}

/// Rows read from the sources but not yet written to every target (admission budget).
struct Budget {
    max: usize,
    used: AtomicUsize,
    freed: Notify,
}

/// Rows held in [`Budget`] until dropped.
struct Held {
    budget: Arc<Budget>,
    rows: usize,
    metrics: Arc<Metrics>,
}

impl Drop for Held {
    fn drop(&mut self) {
        let left = self.budget.used.fetch_sub(self.rows, Ordering::SeqCst) - self.rows;
        Metrics::set(&self.metrics.gauges.inflight_rows, left as u64);
        self.budget.freed.notify_waiters();
    }
}

impl Budget {
    /// Waits until the rows in flight are under the budget.
    async fn admit(&self) {
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            if self.used.load(Ordering::SeqCst) < self.max {
                return;
            }
            freed.await;
        }
    }

    fn hold(self: &Arc<Self>, rows: usize, metrics: &Arc<Metrics>) -> Held {
        let now = self.used.fetch_add(rows, Ordering::SeqCst) + rows;
        Metrics::set(&metrics.gauges.inflight_rows, now as u64);
        Held {
            budget: self.clone(),
            rows,
            metrics: metrics.clone(),
        }
    }
}

/// One target of a [`Job`].
pub struct TargetJob {
    pub name: String,
    pub schema: String,
    pub write: WriteOpts,
    pub endpoint: Arc<Endpoint>,
}

/// One view refresh: the rows of `table` on `targets` for a set of units.
pub struct Job {
    pub plan: Arc<ViewPlan>,
    pub layout: Arc<Layout>,
    pub table: String,
    pub targets: Vec<TargetJob>,
    /// Largest chunk (`LAZYVIEW_QUERY_CHUNK`, within the placeholder limit).
    pub max_chunk: usize,
    /// Bound of one statement round trip (`LAZYVIEW_TARGET_TIMEOUT_MS`).
    pub timeout: Duration,
    /// Source reads prepare their statement afresh (MariaDB, see `mv::exec_source`).
    pub fresh: bool,
}

/// What a [`Supervisor::run`] did.
#[derive(Default)]
pub struct Outcome {
    /// Targets whose writes failed (skipped for the rest of the run).
    pub failed: Vec<(String, anyhow::Error)>,
    /// Ops started (chunks read) and the most write ops in flight at once.
    pub chunks: usize,
    pub peak_writes: usize,
    /// View rows written per kind, over every target.
    pub rows: crate::stats::Rows,
}

/// State shared by the ops of one run.
struct Run {
    job: Job,
    read: Arc<Endpoint>,
    cpu: Arc<Semaphore>,
    budget: Arc<Budget>,
    metrics: Arc<Metrics>,
    /// Targets whose writes failed in this run.
    failed: Mutex<BTreeMap<String, anyhow::Error>>,
    /// One write op at a time per target (tables without a unit index).
    serial: Option<HashMap<String, Arc<Semaphore>>>,
    writes: AtomicUsize,
    peak: AtomicUsize,
    /// Rows written (one lock per chunk and target).
    rows: Mutex<crate::stats::Rows>,
}

/// The pools and limits of the refresh supervisor.
pub struct Supervisor {
    pub mode: SupervisorMode,
    cfg: SupervisorConfig,
    read: Arc<Endpoint>,
    write: BTreeMap<String, Arc<Endpoint>>,
    cpu: Arc<Semaphore>,
    budget: Arc<Budget>,
    metrics: Arc<Metrics>,
}

impl Supervisor {
    /// `None` for `LAZYVIEW_SUPERVISOR=off`. `src_sql_mode`: the session sql_mode of a source
    /// connection set up by `mv::init_session`.
    pub fn new(
        cfg: &SupervisorConfig,
        src_opts: &Opts,
        src_sql_mode: &str,
        metrics: Arc<Metrics>,
    ) -> Option<Supervisor> {
        if cfg.mode == SupervisorMode::Off {
            return None;
        }
        let read = Endpoint::new(
            "read",
            "source",
            src_opts,
            cfg.max_source,
            src_sql_mode,
            &metrics,
        );
        Some(Supervisor {
            mode: cfg.mode,
            cfg: cfg.clone(),
            read,
            write: BTreeMap::new(),
            cpu: Arc::new(Semaphore::new(cfg.cpu_workers.max(1))),
            budget: Arc::new(Budget {
                max: cfg.max_inflight_rows.max(1),
                used: AtomicUsize::new(0),
                freed: Notify::new(),
            }),
            metrics,
        })
    }

    /// The write pool of target `name` (created on first use).
    pub fn write_endpoint(&mut self, name: &str, opts: &Opts, sql_mode: &str) -> Arc<Endpoint> {
        let max = self.cfg.max_for_target(name);
        let metrics = self.metrics.clone();
        self.write
            .entry(name.to_string())
            .or_insert_with(|| Endpoint::new("write", name, opts, max, sql_mode, &metrics))
            .clone()
    }

    /// Chunk size for `units` units: about two chunks per op in flight, at least [`MIN_CHUNK`]
    /// and at most `max_chunk` (§2.5).
    pub fn chunk_size(units: usize, parallel: usize, max_chunk: usize) -> usize {
        let per = units.div_ceil(2 * parallel.max(1));
        per.max(MIN_CHUNK).min(max_chunk.max(1))
    }

    /// Refreshes `units` (all rows of the view: `None`) of `job.table` on every target of the
    /// job. Fails if a source read fails (after its retries); a target whose write fails is
    /// reported in [`Outcome::failed`] and skipped from then on. Returns only once every op it
    /// started has finished.
    /// `partial`: per set of the plan's partial units, partial unit keys; their chunks constrain
    /// only those unit positions (every unit matching a key is refreshed).
    pub async fn run(
        &self,
        job: Job,
        units: Option<&UnitSet>,
        partial: &[UnitSet],
    ) -> Result<Outcome> {
        let parallel = job
            .targets
            .iter()
            .map(|t| t.endpoint.max)
            .min()
            .unwrap_or(1)
            .min(self.read.max);
        let keys = partial.iter().map(UnitSet::len).sum::<usize>();
        let size = units.map(|u| Self::chunk_size(u.len() + keys, parallel, job.max_chunk));
        let serial = (!job.layout.has_unit_index()).then(|| {
            job.targets
                .iter()
                .map(|t| (t.name.clone(), Arc::new(Semaphore::new(1))))
                .collect()
        });
        let run = Arc::new(Run {
            job,
            read: self.read.clone(),
            cpu: self.cpu.clone(),
            budget: self.budget.clone(),
            metrics: self.metrics.clone(),
            failed: Mutex::new(BTreeMap::new()),
            serial,
            writes: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rows: Mutex::new(Default::default()),
        });
        // The units, then each set of partial unit keys.
        let mut cursors: Vec<(Option<Vec<usize>>, ChunkCursor)> = units
            .map(|u| (None, ChunkCursor::new(u.clone())))
            .into_iter()
            .chain(
                run_sets(&run.job.plan.partial.sets, partial)
                    .map(|(at, s)| (Some(at.clone()), ChunkCursor::new(s.clone()))),
            )
            .collect();
        let mut whole = units.is_none();
        let mut ops: JoinSet<Result<()>> = JoinSet::new();
        let mut read_err: Option<anyhow::Error> = None;
        let mut chunks = 0;
        let reap = |r: Result<Result<()>, tokio::task::JoinError>,
                    read_err: &mut Option<anyhow::Error>| {
            let e = match r {
                Ok(Ok(())) => return,
                Ok(Err(e)) => e,
                Err(j) => anyhow!("internal: refresh op failed: {j}"),
            };
            read_err.get_or_insert(e);
        };
        loop {
            while let Some(r) = ops.try_join_next() {
                reap(r, &mut read_err);
            }
            if read_err.is_some() {
                break;
            }
            let chunk = if whole {
                whole = false;
                Some(None)
            } else {
                next_chunk(&mut cursors, size.unwrap_or(1)).map(Some)
            };
            let Some(chunk) = chunk else { break };
            self.budget.admit().await;
            let permit = self.read.acquire().await;
            chunks += 1;
            ops.spawn(chunk_op(run.clone(), chunk, permit));
        }
        while let Some(r) = ops.join_next().await {
            reap(r, &mut read_err);
        }
        if let Some(e) = read_err {
            return Err(e);
        }
        let failed = std::mem::take(&mut *run.failed.lock().unwrap());
        debug!(
            view = %run.job.plan.name,
            chunks,
            chunk_size = size.unwrap_or(0),
            peak_writes = run.peak.load(Ordering::Relaxed),
            "refreshed concurrently"
        );
        let rows = *run.rows.lock().unwrap();
        Ok(Outcome {
            failed: failed.into_iter().collect(),
            chunks,
            peak_writes: run.peak.load(Ordering::Relaxed),
            rows,
        })
    }

    pub async fn disconnect(&self) {
        self.read.disconnect().await;
        for e in self.write.values() {
            e.disconnect().await;
        }
    }
}

/// Runs `op` until it succeeds, fails with an error that is no overload, or [`RETRIES`]
/// retries are used up.
async fn retrying<T, F, Fut>(metrics: &Metrics, what: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(e) if is_overload(&e) => {
                Metrics::add(&metrics.supervisor_overload_errors, 1);
                if attempt >= RETRIES {
                    return Err(e);
                }
                attempt += 1;
                Metrics::add(&metrics.supervisor_retries, 1);
                debug!("{what}: {e:#}; retry {attempt} of {RETRIES}");
                tokio::time::sleep(backoff(attempt)).await;
            }
            r => return r,
        }
    }
}

/// The non-empty sets of partial unit keys with their unit positions.
fn run_sets<'a>(
    sets: &'a [Vec<usize>],
    partial: &'a [UnitSet],
) -> impl Iterator<Item = (&'a Vec<usize>, &'a UnitSet)> {
    sets.iter().zip(partial).filter(|(_, s)| !s.is_empty())
}

/// The next chunk of at most `n` keys from the first cursor that has one (a partial set's
/// chunks constrain only its positions).
fn next_chunk(cursors: &mut Vec<(Option<Vec<usize>>, ChunkCursor)>, n: usize) -> Option<UnitChunk> {
    while let Some((at, c)) = cursors.first_mut() {
        match c.next_chunk(n) {
            Some(chunk) => {
                return Some(match at {
                    Some(at) => UnitChunk::Partial(at.clone(), Box::new(chunk)),
                    None => chunk,
                })
            }
            None => {
                cursors.remove(0);
            }
        }
    }
    None
}

/// Reads a chunk from the sources, then writes it to every target (concurrently).
async fn chunk_op(run: Arc<Run>, chunk: Option<UnitChunk>, permit: Permit) -> Result<()> {
    let job = &run.job;
    let rows = retrying(&run.metrics, "reading the sources", || async {
        let mut c = run.read.conn().await?;
        match tokio::time::timeout(
            job.timeout,
            mv::read_source(
                &mut c,
                &job.plan,
                &job.layout,
                chunk.as_ref(),
                &run.metrics,
                job.fresh,
            ),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(anyhow::Error::new(Timeout(job.timeout))),
        }
    })
    .await;
    drop(permit);
    let rows = rows?;
    let _held = run.budget.hold(rows.len(), &run.metrics);
    let prepared = if rows.len() >= OFFLOAD_ROWS {
        let layout = job.layout.clone();
        let _cpu = run.cpu.clone().acquire_owned().await;
        Metrics::add(&run.metrics.supervisor_cpu_offloads, 1);
        tokio::task::spawn_blocking(move || mv::prepare(&layout, rows))
            .await
            .map_err(|e| anyhow!("internal: preparing rows failed: {e}"))?
    } else {
        mv::prepare(&job.layout, rows)
    };
    let prepared = Arc::new(prepared);
    let chunk = Arc::new(chunk);
    let mut writes: JoinSet<()> = JoinSet::new();
    for t in 0..job.targets.len() {
        let (run, chunk, prepared) = (run.clone(), chunk.clone(), prepared.clone());
        writes.spawn(async move {
            let name = run.job.targets[t].name.clone();
            if run.failed.lock().unwrap().contains_key(&name) {
                return;
            }
            match write_op(&run, t, &chunk, &prepared).await {
                Ok(a) => run.rows.lock().unwrap().applied(&a),
                Err(e) => {
                    run.failed.lock().unwrap().entry(name).or_insert(e);
                }
            }
        });
    }
    while writes.join_next().await.is_some() {}
    Ok(())
}

/// Writes a prepared chunk to target `t` (retried on overload errors).
async fn write_op(
    run: &Run,
    t: usize,
    chunk: &Option<UnitChunk>,
    prepared: &Arc<Prepared>,
) -> Result<Applied> {
    let target = &run.job.targets[t];
    let _serial = match run.serial.as_ref().and_then(|s| s.get(&target.name)) {
        Some(s) => Some(s.clone().acquire_owned().await),
        None => None,
    };
    retrying(
        &run.metrics,
        &format!("writing to target {}", target.name),
        || async {
            let _permit = target.endpoint.acquire().await;
            let now = run.writes.fetch_add(1, Ordering::SeqCst) + 1;
            run.peak.fetch_max(now, Ordering::SeqCst);
            let r = write_once(run, target, chunk, prepared).await;
            run.writes.fetch_sub(1, Ordering::SeqCst);
            r
        },
    )
    .await
    .with_context(|| format!("target {}", target.name))
}

async fn write_once(
    run: &Run,
    target: &TargetJob,
    chunk: &Option<UnitChunk>,
    prepared: &Arc<Prepared>,
) -> Result<Applied> {
    let job = &run.job;
    target.endpoint.settle_zombies().await?;
    let mut conn = target.endpoint.conn().await?;
    let id = conn.id();
    let began = AtomicBool::new(false);
    let r = tokio::time::timeout(job.timeout, async {
        let old = mv::read_old(
            &mut conn,
            &target.schema,
            &job.table,
            &job.layout,
            chunk.as_ref(),
            &run.metrics,
        )
        .await?;
        let d = diff(run, old, prepared).await?;
        mv::write_diff(
            &mut conn,
            &target.schema,
            &job.table,
            &job.layout,
            &d,
            &target.write,
            &run.metrics,
            &began,
        )
        .await
    })
    .await;
    let r = match r {
        Ok(r) => r,
        Err(_) => Err(anyhow::Error::new(Timeout(job.timeout))),
    };
    if let Err(e) = &r {
        let lost = e.chain().any(|c| c.is::<Timeout>()) || is_conn_lost(e);
        if lost && began.load(Ordering::SeqCst) {
            // The transaction may still commit, after a newer write: kill it first.
            drop(conn);
            target.endpoint.fence(id, &run.metrics).await?;
        }
    }
    r
}

/// [`mv::diff`], off the async threads when it is large.
async fn diff(run: &Run, old: Vec<Vec<Value>>, new: &Arc<Prepared>) -> Result<Diff> {
    let datums: usize = old.iter().map(Vec::len).sum::<usize>() + new.datums;
    if old.len() + new.rows < OFFLOAD_ROWS && datums < OFFLOAD_DATUMS {
        return mv::diff(&run.job.layout, old, new);
    }
    let (layout, new) = (run.job.layout.clone(), new.clone());
    let _cpu = run.cpu.clone().acquire_owned().await;
    Metrics::add(&run.metrics.supervisor_cpu_offloads, 1);
    tokio::task::spawn_blocking(move || mv::diff(&layout, old, &new))
        .await
        .map_err(|e| anyhow!("internal: diff failed: {e}"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_sizes() {
        // About two chunks per op in flight, at least MIN_CHUNK, at most the configured chunk.
        assert_eq!(Supervisor::chunk_size(600, 4, 1000), 100);
        assert_eq!(Supervisor::chunk_size(8000, 4, 1000), 1000);
        assert_eq!(Supervisor::chunk_size(1600, 4, 1000), 200);
        assert_eq!(Supervisor::chunk_size(50, 1, 1000), 100);
        assert_eq!(Supervisor::chunk_size(600, 1, 1000), 300);
        assert_eq!(
            Supervisor::chunk_size(600, 4, 13),
            13,
            "LAZYVIEW_QUERY_CHUNK below 100"
        );
        assert_eq!(Supervisor::chunk_size(0, 0, 0), 1);
    }

    /// The chunks of one run (see [`Supervisor::run`]) hold every unit exactly once, for random
    /// unit sets (integer keys, strings, composite keys with NULLs, each inserted several times)
    /// and every pool size and chunk limit.
    #[test]
    fn run_chunks_hold_each_unit_once() {
        use std::collections::HashSet;

        use crate::datum::{Datum, Key};

        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let keys = |c: &UnitChunk| -> Vec<Key> {
            match c {
                UnitChunk::Ints(v) => v.iter().map(|&i| Key::from([Datum::Int(i)])).collect(),
                UnitChunk::Keys(v) => v.clone(),
                UnitChunk::Partial(..) => unreachable!("no partial keys here"),
            }
        };
        for round in 0..300 {
            let mut set = UnitSet::default();
            let mut all: HashSet<Key> = HashSet::new();
            for _ in 0..(rnd() % 3000) {
                let k: Key = match rnd() % 5 {
                    0 => Box::new([Datum::Bytes(
                        ["a", "A", "a ", "b", "\u{e9}"][(rnd() % 5) as usize]
                            .repeat(1 + (rnd() % 3) as usize)
                            .into_bytes(),
                    )]),
                    1 => Box::new([
                        if rnd() % 3 == 0 {
                            Datum::Null
                        } else {
                            Datum::Int((rnd() % 40) as i64)
                        },
                        Datum::Bytes(format!("k{}", rnd() % 30).into_bytes()),
                    ]),
                    2 => Box::new([Datum::Null]),
                    _ => Box::new([Datum::Int((rnd() % 5000) as i64 - 2500)]),
                };
                // Duplicates are inserted again and again (as dirty keys are).
                for _ in 0..1 + rnd() % 3 {
                    set.insert(k.clone());
                }
                all.insert(k);
            }
            assert_eq!(set.len(), all.len());
            let parallel = 1 + (round % 8);
            let max_chunk = [7, 100, 1000][round % 3];
            let size = Supervisor::chunk_size(set.len(), parallel, max_chunk);
            let mut cursor = ChunkCursor::new(set.clone());
            let mut seen: HashSet<Key> = HashSet::new();
            while let Some(c) = cursor.next_chunk(size) {
                assert!(!c.is_empty() && c.len() <= size);
                for k in keys(&c) {
                    assert!(
                        seen.insert(k.clone()),
                        "unit {k:?} in two chunks of one run"
                    );
                }
            }
            assert_eq!(seen, all, "every unit is in a chunk");
        }
    }

    #[test]
    fn overload_classes() {
        let server = |code: u16| {
            anyhow::Error::from(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message: String::new(),
                state: String::new(),
            }))
        };
        for code in [1040, 1203, 1205, 1213, 2013] {
            assert!(is_overload(&server(code)), "{code}");
        }
        for code in [1054, 1146, 1062, 1064] {
            assert!(!is_overload(&server(code)), "{code}");
        }
        assert!(is_overload(
            &anyhow::Error::new(Timeout(Duration::from_secs(1))).context("target t")
        ));
        assert!(!is_overload(&anyhow!("view is broken")));
        for n in 1..=4 {
            let d = backoff(n);
            let base = BACKOFF * (1 << (n - 1));
            assert!(d >= base / 2 && d <= base * 3 / 2, "{d:?}");
        }
    }

    #[tokio::test]
    async fn budget_admits_below_the_limit() {
        let metrics = Arc::new(Metrics::default());
        let b = Arc::new(Budget {
            max: 10,
            used: AtomicUsize::new(0),
            freed: Notify::new(),
        });
        b.admit().await;
        let held = b.hold(25, &metrics);
        assert_eq!(Metrics::get(&metrics.gauges.inflight_rows), 25);
        let waiter = {
            let b = b.clone();
            tokio::spawn(async move { b.admit().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "over budget: waits");
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("admitted once rows are written")
            .unwrap();
        assert_eq!(Metrics::get(&metrics.gauges.inflight_rows), 0);
    }
}
