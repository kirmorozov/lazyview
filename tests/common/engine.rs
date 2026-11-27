use super::*;

// ---------------------------------------------------------------------------------------------
// Engine handle
// ---------------------------------------------------------------------------------------------

/// An engine running `lazyview::engine::run` on its own runtime + thread.
///
/// Dropping the handle kills the engine (like `kill -9`: its futures are dropped, its runtime shut
/// down, open DB transactions are rolled back by the server).
pub struct Engine {
    pub label: String,
    pub metrics: Arc<Metrics>,
    pub cfg: Config,
    pub log_path: PathBuf,
    shutdown: watch::Sender<bool>,
    kill: Option<oneshot::Sender<()>>,
    done: oneshot::Receiver<std::result::Result<(), String>>,
    exit: Option<std::result::Result<(), String>>,
}

impl Engine {
    pub(crate) fn spawn(cfg: Config, label: String, log_path: PathBuf) -> Result<Engine> {
        let metrics = Arc::new(Metrics::default());
        let (sd_tx, sd_rx) = watch::channel(false);
        let (kill_tx, kill_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = oneshot::channel();
        let file = Arc::new(
            std::fs::File::create(&log_path)
                .with_context(|| format!("create {}", log_path.display()))?,
        );
        let filter = std::env::var("LAZYVIEW_TEST_LOG")
            .or_else(|_| std::env::var("RUST_LOG"))
            .unwrap_or_else(|_| "lazyview=debug,warn".into());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(file)
            .with_ansi(false)
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let m = metrics.clone();
        let run_cfg = cfg.clone();
        let thread_label = label.clone();
        std::thread::Builder::new()
            .name(format!("lv-engine-{label}"))
            .spawn(move || {
                let _guard = tracing::dispatcher::set_default(&dispatch);
                let d2 = dispatch.clone();
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name(format!("lv-{thread_label}"))
                    .on_thread_start(move || {
                        // Thread-local subscriber for the whole life of the worker thread.
                        std::mem::forget(tracing::dispatcher::set_default(&d2));
                    })
                    .build()
                    .expect("engine runtime");
                let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(async move {
                        tokio::select! {
                            r = lazyview::engine::run(run_cfg, m, sd_rx) => Some(r),
                            _ = kill_rx => None,
                        }
                    })
                }));
                rt.shutdown_timeout(Duration::from_secs(2));
                let out = match res {
                    Ok(Some(Ok(()))) => Ok(()),
                    Ok(Some(Err(e))) => Err(format!("engine returned error: {e:#}")),
                    Ok(None) => Err("engine killed".to_string()),
                    Err(p) => Err(format!("engine panicked: {}", panic_msg(p))),
                };
                let _ = done_tx.send(out);
            })
            .context("spawn engine thread")?;
        Ok(Engine {
            label,
            metrics,
            cfg,
            log_path,
            shutdown: sd_tx,
            kill: Some(kill_tx),
            done: done_rx,
            exit: None,
        })
    }

    /// `Some(result)` once the engine has exited.
    pub fn exited(&mut self) -> Option<&std::result::Result<(), String>> {
        if self.exit.is_none() {
            match self.done.try_recv() {
                Ok(r) => self.exit = Some(r),
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.exit = Some(Err("engine thread vanished".into()))
                }
            }
        }
        self.exit.as_ref()
    }

    /// Errors if the engine has exited (for any reason).
    pub fn check_alive(&mut self) -> Result<()> {
        let label = self.label.clone();
        let log = self.log_path.display().to_string();
        match self.exited() {
            None => Ok(()),
            Some(Ok(())) => bail!("engine {label} exited unexpectedly (Ok) — log {log}"),
            Some(Err(e)) => bail!("engine {label} died: {e} — log {log}"),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.metrics.snapshot()
    }

    pub fn applied(&self) -> Option<Position> {
        self.metrics.applied()
    }

    async fn wait_exit(&mut self, timeout: Duration) -> Result<std::result::Result<(), String>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(r) = self.exited() {
                return Ok(r.clone());
            }
            if Instant::now() > deadline {
                bail!(
                    "engine {} did not exit within {timeout:?} — log {}",
                    self.label,
                    self.log_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Graceful shutdown; errors if the engine failed or does not stop within 60 s.
    pub async fn stop(mut self) -> Result<()> {
        let _ = self.shutdown.send(true);
        match self.wait_exit(Duration::from_secs(60)).await? {
            Ok(()) => Ok(()),
            Err(e) => bail!(
                "engine {} stopped with error: {e} — log {}",
                self.label,
                self.log_path.display()
            ),
        }
    }

    /// Waits for the engine to exit on its own (`once` / `LAZYVIEW_SYNC=false`); returns its result.
    pub async fn wait_finished(
        mut self,
        timeout: Duration,
    ) -> Result<std::result::Result<(), String>> {
        self.wait_exit(timeout).await
    }

    /// Simulates a crash: drops the engine's futures and runtime at an arbitrary await point.
    pub async fn crash(mut self) -> Result<()> {
        if let Some(k) = self.kill.take() {
            let _ = k.send(());
        }
        match self.wait_exit(Duration::from_secs(30)).await? {
            Err(e) if e == "engine killed" => Ok(()),
            Err(e) => bail!(
                "engine {} had already failed before the crash: {e} — log {}",
                self.label,
                self.log_path.display()
            ),
            Ok(()) => bail!(
                "engine {} had already exited (Ok) before the crash — log {}",
                self.label,
                self.log_path.display()
            ),
        }
    }

    /// Waits until a line of this engine's log contains `needle`, checking every turn that the
    /// engine is still alive, and returns the tail of the log last read.
    ///
    /// This belongs to the engine rather than to a leg: `Engine` owns the log path and the exit
    /// status, and a test that polls either from its own closure would have to hold `&mut self`
    /// across the `FnMut` boundary `wait_until` polls through, which does not compile. Same rule
    /// as [`Self::wait_exit`] and [`Self::wait_applied`]: one bounded deadline, 50 ms a turn.
    pub async fn wait_log_line(&mut self, needle: &str, timeout: Duration) -> Result<String> {
        let deadline = Instant::now() + timeout;
        loop {
            self.check_alive()?;
            let log = engineproc::tail_lines(&self.log_path, 400);
            if log.contains(needle) {
                return Ok(log);
            }
            if Instant::now() > deadline {
                bail!(
                    "engine {} did not log {needle:?} within {timeout:?} — log tail:\n{}",
                    self.label,
                    super::clip(&log, 2000),
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits up to `timeout` for the engine to exit on its own and returns how it ended — the
    /// message it died with, or `None` if it is still running when the deadline passes. Whether a
    /// death is the expected outcome is the caller's business: a leg that exists to prove the
    /// engine *refuses* to run asserts on the message, and a leg that would be broken by a death
    /// uses [`Self::check_alive`] instead.
    pub async fn wait_death(&mut self, timeout: Duration) -> Option<String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(r) = self.exited() {
                return Some(match r {
                    Ok(()) => "engine exited Ok".to_string(),
                    Err(e) => e.clone(),
                });
            }
            if Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits until `applied() >= target`, with a fixed wall-clock `timeout`. The
    /// [`Self::wait_applied_progressive`] variant is what a *catch-up* uses (#248); this one is
    /// for the waits that want a hard wall — a scenario that proves the engine does *not* catch
    /// up, or that measures how long something takes.
    pub async fn wait_applied(&mut self, target: &Position, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(a) = self.metrics.applied() {
                if &a >= target {
                    return Ok(());
                }
            }
            if let Err(e) = self.check_alive() {
                // A transient probe error spends budget, it does not abort the wait; a dead
                // engine (the only non-transient error here) still fails at once.
                if !super::is_transient(&e) || Instant::now() > deadline {
                    return Err(e);
                }
            }
            if Instant::now() > deadline {
                bail!(
                    "engine {} not caught up within {timeout:?}: applied={:?} target={target} metrics={:?} — log {}",
                    self.label,
                    self.metrics.applied().map(|p| p.to_string()),
                    self.metrics.snapshot(),
                    self.log_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    /// Captures the source's binlog end and waits until the engine applied it, `rounds` times
    /// (MV writes advance the binlog; chained views need one extra round per chain level).
    pub async fn wait_caught_up_rounds(
        &mut self,
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
            self.wait_applied(&target, left).await?;
        }
        Ok(())
    }

    /// `wait_caught_up_rounds` with 3 rounds (enough for one level of chained views + margin).
    pub async fn wait_caught_up(&mut self, src: &mut Conn, timeout: Duration) -> Result<()> {
        self.wait_caught_up_rounds(src, timeout, 3).await
    }

    /// The catch-up wait of issue #248: the same target test as [`Self::wait_applied`], bounded
    /// by the engine's own numbers instead of a fixed wall clock.
    ///
    /// `floor` (the caller's [`t_catchup`]) is exactly what a healthy engine is held to today —
    /// nothing is raised for an engine that catches up inside it. Past the floor the wait goes on
    /// while `applied()` advances, and it still fails the moment a living engine stands still
    /// longer than [`engine_bounds::stall`] accounts for. A catch-up that is genuinely stuck
    /// therefore fails, and its message names the bound it died on instead of guessing 180 s.
    pub async fn wait_applied_progressive(
        &mut self,
        target: &Position,
        floor: Duration,
    ) -> Result<()> {
        // The op bound of the engine this handle runs: an operator who raises
        // `LAZYVIEW_TARGET_TIMEOUT_MS` above the default gets the longer allowance that engine
        // can actually need; one who lowers it is still bounded by the harness's patience floor.
        let timeout = self.cfg.target_timeout.max(Duration::from_millis(
            lazyview::config::DEFAULT_TARGET_TIMEOUT_MS,
        ));
        let start = Instant::now();
        let mut rule = CatchUp::new(start, floor, engine_bounds::stall(timeout));
        loop {
            let applied = self.metrics.applied();
            if let Some(a) = &applied {
                if a >= target {
                    return Ok(());
                }
            }
            if let Err(e) = self.check_alive() {
                // As in `wait_applied`: a transient probe error spends budget, a dead engine fails
                // at once.
                if !super::is_transient(&e) || Instant::now() > start + floor {
                    return Err(e);
                }
            }
            if let CatchUpVerdict::Failed(why) = rule.sample(Instant::now(), applied.as_ref()) {
                bail!(
                    "engine {} not caught up: applied={:?} target={target} metrics={:?} — log {}\n{why}",
                    self.label,
                    self.metrics.applied().map(|p| p.to_string()),
                    self.metrics.snapshot(),
                    self.log_path.display(),
                );
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    /// [`Self::wait_caught_up_rounds`] with the progress rule of
    /// [`Self::wait_applied_progressive`] (#248): every round keeps the caller's floor, and an
    /// engine that is still advancing is never cut off at it. The fixed variant stays for the
    /// waits that *want* a hard wall (a scenario that proves the engine cannot catch up).
    pub async fn wait_caught_up_rounds_progressive(
        &mut self,
        src: &mut Conn,
        floor: Duration,
        rounds: usize,
    ) -> Result<()> {
        let deadline = Instant::now() + floor;
        for _ in 0..rounds.max(1) {
            let target = server_position(src).await?;
            let left = deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            self.wait_applied_progressive(&target, left).await?;
        }
        Ok(())
    }

    /// [`Self::wait_caught_up`] with the progress rule of [`Self::wait_applied_progressive`].
    pub async fn wait_caught_up_progressive(
        &mut self,
        src: &mut Conn,
        floor: Duration,
    ) -> Result<()> {
        self.wait_caught_up_rounds_progressive(src, floor, 3).await
    }

    /// Update statistics of view `view` of `db`, as `GET /stats` reports them (src/stats.rs).
    /// Every flush hands them to the aggregator task before it advances the applied position;
    /// this waits (at most 5 s) until the task published every batch handed over so far, so
    /// after a catch-up they include everything applied. `None` until a flush listed the view.
    pub fn view_stats(&self, db: &Db, view: &str) -> Option<ViewStats> {
        let reg = &self.metrics.view_stats;
        let sent = reg.sent();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !reg.includes(sent) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        reg.view(&format!("{}.{view}", db.name))
    }

    /// [`Self::view_stats`], failing when the view is not listed.
    pub fn stats_of(&self, db: &Db, view: &str) -> Result<ViewStats> {
        self.view_stats(db, view)
            .with_context(|| format!("view `{view}` is not listed in /stats"))
    }
}

// ---------------------------------------------------------------------------------------------
// Catch-up bounds (issue #248): waits sized from the engine, never from a wall-clock guess
// ---------------------------------------------------------------------------------------------

/// The engine's own bounds on one catch-up.
///
/// `t_catchup()` — 180 s of wall clock, fixed — was the only bound on "the engine has applied
/// everything", and it failed a catch-up that was still going:
/// `exclusive_server_restarts_are_not_fatal` died 1 run in 114 on mariadb130 with `applied` on the
/// old binlog file and the target on the next one, i.e. with the engine inside a reconnect cycle.
/// The engine puts no bound on a catch-up (refined review N-03/N-05), so a fixed guess is wrong by
/// construction; the honest number is the engine's own arithmetic, the way `BH_CONNECT_*` sizes the
/// blackhole scenario's waits (`tests/chaos.rs`). Read from `src/` rather than mirrored there, so
/// it cannot drift (lv-super's ruling of 2026-11-18, #2).
///
/// Two things keep `applied()` from advancing while the engine is alive and the harness is waiting:
///
/// * one failed `Policy::CONNECT` run of the stream loop ([`engine_bounds::connect_cycle`]):
///   `CONNECT_ATTEMPTS` tries of `BINLOG_CONNECT_TIMEOUT` each, the widest backoff between them,
///   plus the one cap wait a wholly failed run pays before the next run (`engine.rs`, `'reconnect`);
/// * one statement of the flush being caught up ([`engine_bounds::statement_op`]):
///   `STATEMENT_ATTEMPTS + 1` tries of the target timeout each, the widest backoff between them
///   (`supervisor.rs`, `retrying`).
///
/// They can come one after the other — a flush whose statement times out is followed by the
/// stream's own reconnect — so [`engine_bounds::stall`] sums them: 152.5 s + 240.5 s = 393 s on the
/// library's defaults, more than twice the 180 s the harness used to insist on, and the same for
/// every flavor. No bound here is a guess; each term names its `src/` number.
pub mod engine_bounds {
    use std::time::Duration;

    use lazyview::{
        config::DEFAULT_TARGET_TIMEOUT_MS,
        engine::BINLOG_CONNECT_TIMEOUT,
        supervisor::{
            self, CONNECT_ATTEMPTS, CONNECT_BACKOFF_BASE, CONNECT_BACKOFF_CAP, CONNECT_JITTER,
            STATEMENTS_JITTER, STATEMENT_ATTEMPTS, STATEMENT_BACKOFF_BASE, STATEMENT_BACKOFF_CAP,
        },
    };

    /// The widest a policy's wait between two tries gets at try `n` (`supervisor.rs::backoff`,
    /// whose jitter tops out at 1.5 × `min(base · 2^(n−1), cap)`).
    fn backoff(n: u32, base: Duration, cap: Duration, jitter: bool) -> Duration {
        supervisor::backoff_max(n, base, cap, jitter)
    }

    /// One whole failed `Policy::CONNECT` run of the binlog stream loop, including the cap wait
    /// that follows a run that failed entirely.
    pub fn connect_cycle() -> Duration {
        (0..CONNECT_ATTEMPTS)
            .map(|_| BINLOG_CONNECT_TIMEOUT)
            .sum::<Duration>()
            + (1..CONNECT_ATTEMPTS)
                .map(|n| backoff(n, CONNECT_BACKOFF_BASE, CONNECT_BACKOFF_CAP, CONNECT_JITTER))
                .sum::<Duration>()
            + CONNECT_BACKOFF_CAP
    }

    /// One statement of a refresh under `Policy::STATEMENTS`, every try at the target's own
    /// timeout: the cost of a target that answers nothing until `LAZYVIEW_TARGET_TIMEOUT_MS`.
    pub fn statement_op(target_timeout: Duration) -> Duration {
        (0..=STATEMENT_ATTEMPTS)
            .map(|_| target_timeout)
            .sum::<Duration>()
            + (1..=STATEMENT_ATTEMPTS)
                .map(|n| {
                    backoff(
                        n,
                        STATEMENT_BACKOFF_BASE,
                        STATEMENT_BACKOFF_CAP,
                        STATEMENTS_JITTER,
                    )
                })
                .sum::<Duration>()
    }

    /// The longest a **living** engine can stand still without advancing `applied()`: one timed-out
    /// statement of the flush it is catching up, then one failed reconnect cycle before the flush
    /// can be tried again. Past this, the wait fails: the engine is not slow, it is stuck.
    pub fn stall(target_timeout: Duration) -> Duration {
        connect_cycle() + statement_op(target_timeout)
    }

    /// `LAZYVIEW_TARGET_TIMEOUT_MS` as the engine ships it: what a catch-up's statements are
    /// bounded by unless an operator says otherwise.
    pub fn default_target_timeout() -> Duration {
        Duration::from_millis(DEFAULT_TARGET_TIMEOUT_MS)
    }
}

/// The rule of [`Engine::wait_applied_progressive`] as a state machine over (`now`, `applied`), so
/// its arithmetic is testable without an engine and without a server (#248).
pub struct CatchUp {
    floor: Duration,
    stall: Duration,
    start: Instant,
    last_advance: Instant,
    last_applied: Option<Position>,
    ticks: u64,
}

/// Where [`CatchUp`] stands after one sample.
#[derive(Debug)]
pub enum CatchUpVerdict {
    /// The wait may keep going: the engine is inside what its own bounds allow.
    Waiting,
    /// The wait is over and the engine did not catch up: the string says which bound died.
    Failed(String),
}

impl CatchUp {
    /// `floor` is the fixed bound a healthy engine is held to (its `t_catchup()`), `stall` the
    /// engine's own bound on one silent stretch ([`engine_bounds::stall`]). The floor is a floor
    /// only: a bound never comes out *shorter* than the wait its caller asked for.
    pub fn new(start: Instant, floor: Duration, stall: Duration) -> CatchUp {
        CatchUp {
            floor,
            stall: stall.max(floor),
            start,
            last_advance: start,
            last_applied: None,
            ticks: 0,
        }
    }

    /// One sample of the clock and of `applied()`. Strict progress resets the stall clock; the
    /// wait ends when a stretch with no progress outlasts the engine's own bound. The first
    /// position the wait sees is **not** progress: the stall clock starts when the caller started
    /// waiting, so an engine that was already frozen when the wait began is measured from then.
    pub fn sample(&mut self, now: Instant, applied: Option<&Position>) -> CatchUpVerdict {
        if let Some(a) = applied {
            let advanced = match &self.last_applied {
                None => false,
                Some(p) => a > p,
            };
            if self.last_applied.is_none() {
                self.last_applied = Some(a.clone());
            } else if advanced {
                self.last_advance = now;
                self.last_applied = Some(a.clone());
                self.ticks += 1;
            }
        }
        let stalled = now.saturating_duration_since(self.last_advance);
        if stalled <= self.stall {
            return CatchUpVerdict::Waiting;
        }
        let elapsed = now.saturating_duration_since(self.start);
        let where_ = match &self.last_applied {
            Some(p) => format!("applied {p} has not advanced"),
            None => "the engine has never reported an applied position".to_string(),
        };
        CatchUpVerdict::Failed(format!(
            "{where_} for {stalled:?}, past the {:?} a living engine can account for \
             ({} progress tick(s) so far, floor {:?}, {elapsed:?} elapsed): one failed connect \
             cycle of {:?} plus one timed-out statement of {:?}",
            self.stall,
            self.ticks,
            self.floor,
            engine_bounds::connect_cycle(),
            engine_bounds::statement_op(engine_bounds::default_target_timeout()),
        ))
    }
}

/// The rule of the `ready`/`rebuilt` waits of issue #231, as a state machine over
/// (`now`, a signature of what those waits observe), so its arithmetic is testable without an
/// engine and without a server — the shape [`CatchUp`] established for #248.
///
/// A build is **progressing** when either channel moves: a state row of a view the wait names
/// (`state`, `refreshed_at`, `definition_id`, `definition_hash`, target — the fields the swap
/// and the refresh stamp) **or** the engine's `applied()` position. The build task runs off the
/// binlog loop on its own connection (refined review A-04) while the loop keeps applying, so a
/// building view shows up on one channel or the other for as long as it is honestly working;
/// what no live engine can do is go silent on **both** for longer than
/// [`engine_bounds::stall`] accounts for — one failed `Policy::CONNECT` run (the stream
/// reconnecting) plus one timed-out statement of the flush (the source read or target write
/// the catch-up behind the build waits on). `t_ready()` stays the **floor**: an engine that
/// finishes in time is held to exactly what it is held to today, and the wait that extends
/// prints its elapsed and tick count — the review's unmeasured build length (N-04), measured.
pub struct ReadyProgress {
    what: String,
    floor: Duration,
    stall: Duration,
    start: Instant,
    last_advance: Instant,
    last_sig: Option<String>,
    ticks: u64,
}

impl ReadyProgress {
    /// `what` names the views in every failure; `floor` is the fixed bound a healthy engine is
    /// held to (its `t_ready()`); `stall` the engine's own bound on one silent stretch on both
    /// channels ([`engine_bounds::stall`]). As in [`CatchUp::new`], the bound never comes out
    /// shorter than the wait its caller asked for.
    pub fn new(
        start: Instant,
        floor: Duration,
        stall: Duration,
        what: impl Into<String>,
    ) -> ReadyProgress {
        ReadyProgress {
            what: what.into(),
            floor,
            stall: stall.max(floor),
            start,
            last_advance: start,
            last_sig: None,
            ticks: 0,
        }
    }

    /// One sample of the clock and of the observed signature (see [`CatchUp::sample`] for the
    /// same rule on `applied` alone). The **first** signature is not progress: the stall clock
    /// starts when the caller started waiting, so a view that was already silent when the wait
    /// began is measured from then.
    pub fn sample(&mut self, now: Instant, sig: &str) -> CatchUpVerdict {
        match &self.last_sig {
            None => self.last_sig = Some(sig.to_string()),
            Some(p) if p != sig => {
                self.last_advance = now;
                self.last_sig = Some(sig.to_string());
                self.ticks += 1;
            }
            Some(_) => {}
        }
        let stalled = now.saturating_duration_since(self.last_advance);
        if stalled <= self.stall {
            return CatchUpVerdict::Waiting;
        }
        let elapsed = now.saturating_duration_since(self.start);
        CatchUpVerdict::Failed(format!(
            "the {} did not move its state rows nor advance `applied` for {stalled:?}, past \
             the {:?} a living engine can account for ({} progress tick(s) so far, floor {:?}, \
             {elapsed:?} elapsed): one failed connect cycle of {:?} plus one timed-out statement \
             of {:?}",
            self.what,
            self.stall,
            self.ticks,
            self.floor,
            engine_bounds::connect_cycle(),
            engine_bounds::statement_op(engine_bounds::default_target_timeout()),
        ))
    }

    /// Progress ticks so far — reported by the ready waits when they run past their floor, so
    /// the unmeasured build length (review N-04) is measured in every flake-adjacent run.
    pub fn ticks(&self) -> u64 {
        self.ticks
    }

    /// The floor the wait was given (what a healthy engine is held to).
    pub fn floor(&self) -> Duration {
        self.floor
    }
}

/// Waits until the engine is caught up and idle, so a following step or measurement starts a new
/// batch window (two catch-ups on one connection around one fixed idle sleep).
pub async fn quiesce(engine: &mut Engine, db: &Db) -> Result<()> {
    let mut c = db.conn().await?;
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    Ok(())
}

pub use lazyview::stats::ViewSnapshot as ViewStats;

/// A free local TCP address for an engine's `LAZYVIEW_HEALTH_ADDR`.
pub fn free_local_addr() -> Result<String> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    Ok(format!("127.0.0.1:{port}"))
}

/// An HTTP response read off one short-lived connection to an engine's health endpoint.
pub struct Resp {
    pub status: u16,
    pub head: String,
    pub body: String,
}

/// Decode a `Transfer-Encoding: chunked` answer (`size\r\nbytes\r\n` repetitions, terminated by
/// `0\r\n`); the identity when the header is absent. Chunk extensions are not used by the engines
/// of the harness (the health endpoint, the toxiproxy API), so a size line is parsed as plain hex;
/// on anything unparsable the function stops with what it has (the caller's JSON parse then fails
/// loudly, as it must). The single decode of the harness: a Go server (toxiproxy, whose
/// `GET /proxies` answer is far too large for one write) answers `Transfer-Encoding: chunked`
/// whenever it does not know the length up front, and reading that body as plain hands the caller
/// the size lines as if they were payload (issue #216).
fn decode_chunked(head: &str, answer: &str) -> String {
    if !head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return answer.to_string();
    }
    let mut out = String::new();
    let mut rest = answer;
    while let Some((line, tail)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(line.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        let take = size.min(tail.len());
        out.push_str(&tail[..take]);
        rest = tail[take..].strip_prefix("\r\n").unwrap_or(&tail[take..]);
    }
    out
}

/// The shared read of one HTTP/1.1 answer, used by both [`request`] and [`http_json`]: status from
/// the status line, the header block as it came (callers match headers in it), and the body with a
/// `Transfer-Encoding: chunked` answer decoded by [`decode_chunked`]. `what` labels the exchange in
/// every failure (`"GET 127.0.0.1:8080/stats"`).
fn split_response(what: &str, text: &str) -> Result<(u16, String, String)> {
    let (head, answer) = text
        .split_once("\r\n\r\n")
        .with_context(|| format!("{what}: bad HTTP response {text:?}"))?;
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<u16>().ok())
        .with_context(|| format!("{what}: bad status line {head:?}"))?;
    Ok((status, head.to_string(), decode_chunked(head, answer)))
}

/// One HTTP request to an engine's health endpoint (`addr` as given in `LAZYVIEW_HEALTH_ADDR`):
/// 5 s connect and read timeouts, the response read by [`split_response`].
pub async fn request(addr: &str, method: &str, path: &str, headers: &str) -> Result<Resp> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .context("connect timeout")??;
    s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n{headers}\r\n").as_bytes())
        .await?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .context("read timeout")??;
    let text = String::from_utf8_lossy(&buf).to_string();
    let (status, head, body) = split_response(&format!("{method} {addr}{path}"), &text)?;
    Ok(Resp { status, head, body })
}

/// `GET <path>` on an engine's health endpoint (`addr` as given in `LAZYVIEW_HEALTH_ADDR`):
/// status and body.
pub async fn http_get(addr: &str, path: &str) -> Result<(u16, String)> {
    let r = request(addr, "GET", path, "").await?;
    Ok((r.status, r.body))
}

/// One HTTP request **with a JSON body** and the parsed answer, on the same one-shot connection as
/// [`request`] (the shape chaos plan §2.1 asks for: `Host:`, `Content-Length`, `Connection: close`,
/// `read_to_end` behind the 5 s timeouts). It does not call [`request`]: `request` sends no body,
/// so a `POST` there would sit waiting for the bytes its own `Content-Length` promised; both do
/// share [`split_response`], so a chunked answer is decoded here too (issue #216). A
/// body-less answer (the 204 of toxiproxy's `DELETE` and `POST /reset`) parses as
/// [`serde_json::Value::Null`]. No new dependency: `serde_json` is already here for [`fetch_stats`].
pub async fn http_json(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<(u16, serde_json::Value)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .with_context(|| format!("{method} {addr}{path}: connect timeout"))?
        .with_context(|| format!("{method} {addr}{path}: connect"))?;
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: \
         application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    tokio::time::timeout(Duration::from_secs(5), s.write_all(req.as_bytes()))
        .await
        .with_context(|| format!("{method} {addr}{path}: write timeout"))?
        .with_context(|| format!("{method} {addr}{path}: write"))?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .with_context(|| format!("{method} {addr}{path}: read timeout"))?
        .with_context(|| format!("{method} {addr}{path}: read"))?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let (status, _head, answer) = split_response(&format!("{method} {addr}{path}"), &text)?;
    if answer.trim().is_empty() {
        return Ok((status, serde_json::Value::Null));
    }
    let v = serde_json::from_str(answer.trim())
        .with_context(|| format!("{method} {addr}{path}: status {status}, not JSON: {answer}"))?;
    Ok((status, v))
}

/// `GET /stats` of an engine started with `LAZYVIEW_HEALTH_ADDR=addr`, as JSON.
pub async fn fetch_stats(addr: &str) -> Result<serde_json::Value> {
    let (status, body) = http_get(addr, "/stats").await?;
    if status != 200 {
        bail!("GET {addr}/stats: status {status}: {body}");
    }
    serde_json::from_str(&body).with_context(|| format!("GET {addr}/stats: not JSON: {body}"))
}

/// `LAZYVIEW_FULL_REFRESH_MS` given to test engines: `LAZYVIEW_TEST_FULL_REFRESH_MS` (default 0 =
/// rebuild full-mode / unknown-unit views on every flush, so checks are exact right after
/// catch-up). With a cadence > 0, [`verify`] checks incrementally maintained views right after
/// catch-up and polls the lazily rebuilt ones (Full, Chained, FK-child) for up to 2 × cadence.
pub fn test_full_refresh_ms() -> u64 {
    std::env::var("LAZYVIEW_TEST_FULL_REFRESH_MS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// Views rebuilt on the `LAZYVIEW_FULL_REFRESH_MS` cadence rather than per flush.
pub fn is_cadence_rebuilt(spec: &ViewSpec) -> bool {
    spec.kind != Kind::Incremental || workload::reads_fk_child(&spec.name)
}

/// Default timeouts (scaled). `t_catchup` is a **floor** for the catch-up waits of #248:
/// [`Engine::wait_applied_progressive`] holds a healthy engine to exactly this and lets only the
/// engine's own numbers ([`engine_bounds`]) take it longer; every other wait ends at it.
/// `t_ready` is the same kind of **floor** for the ready/rebuilt waits of #231:
/// [`wait_ready`](crate::common::wait_ready) and [`wait_rebuilt`](crate::common::wait_rebuilt)
/// hold a healthy engine to exactly this and let only observable progress (state rows moving,
/// `applied` advancing) past it, bounded by [`engine_bounds::stall`].
pub fn t_ready() -> Duration {
    Duration::from_secs_f64(180.0 * scale().max(1.0))
}
pub fn t_catchup() -> Duration {
    Duration::from_secs_f64(180.0 * scale().max(1.0))
}

// ---------------------------------------------------------------------------------------------
// The catch-up rule of #248, scripted: no engine, no server, no sleep
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(file: &str, pos: u64) -> Position {
        Position {
            file: file.to_string(),
            pos,
        }
    }

    /// The premise of #248: what a living engine may legitimately take is more than the fixed
    /// 180 s it used to be held to — and it is the same for every flavor, because it is made of
    /// the engine's own constants and not of anything the server is.
    #[test]
    fn the_engine_own_bound_exceeds_the_old_fixed_guess() {
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        assert!(
            stall > t_catchup(),
            "{stall:?} vs the fixed {:?}: if the engine's numbers came down to this, the \
             progress rule is dead weight and #248's premise needs re-reading",
            t_catchup()
        );
    }

    /// The two terms, pinned to the arithmetic of the `src/` numbers they are made of — a drift
    /// guard on both sides (ruling 2026-11-18 #2: no literals feed a bound, but a bound's shape
    /// must still be written down somewhere).
    #[test]
    fn bounds_follow_the_engine_constants() {
        // src/engine.rs: 5 attempts × 20 s of connect. src/supervisor.rs: CONNECT backs off
        // 1 s doubling to 30 s, jittered (× 1.5 at the top) → 1.5 + 3 + 6 + 12 = 22.5 s, and a
        // wholly failed run pays one 30 s cap; STATEMENTS runs 3 retries + the first try at the
        // 60 s target timeout, backing off 50 ms doubling to 500 ms → 0.075 + 0.15 + 0.3 s.
        let cases = [
            (
                engine_bounds::connect_cycle(),
                Duration::from_secs_f64(152.5),
                "connect cycle",
            ),
            (
                engine_bounds::statement_op(Duration::from_secs(60)),
                Duration::from_secs_f64(240.525),
                "timed-out statement",
            ),
            (
                engine_bounds::stall(Duration::from_secs(60)),
                Duration::from_secs_f64(393.025),
                "stall",
            ),
        ];
        for (got, want, what) in cases {
            assert!(
                got.abs_diff(want) <= Duration::from_millis(1),
                "{what}: {got:?}, expected {want:?} from the engine's own numbers"
            );
        }
    }

    /// An engine that keeps advancing is never cut off at the floor — the case the old fixed
    /// deadline got wrong.
    #[test]
    fn a_progressing_engine_outlives_the_floor() {
        let start = Instant::now();
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        let mut rule = CatchUp::new(start, Duration::from_secs(180), stall);
        let step = Duration::from_secs(30);
        for n in 1..=30u64 {
            // 15 minutes of steady progress: 5× the floor, and never a stall near the bound.
            let v = rule.sample(
                start + step * (n as u32),
                Some(&pos("mariadb-bin.000011", n * 100)),
            );
            assert!(
                matches!(v, CatchUpVerdict::Waiting),
                "sample {n} ended a wait that was still advancing: {v:?}"
            );
        }
    }

    /// A frozen engine still fails, at the bound the engine's numbers allow — not at 180 s, and
    /// not forever.
    #[test]
    fn a_frozen_engine_fails_at_its_bound() {
        let start = Instant::now();
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        let mut rule = CatchUp::new(start, Duration::from_secs(180), stall);
        let frozen = Some(pos("mariadb-bin.000011", 1841));
        // Past the floor and still inside what a reconnect cycle plus a timed-out statement
        // explain: the wait that flaked (#248) is allowed to run.
        assert!(matches!(
            rule.sample(start + Duration::from_secs(200), frozen.as_ref()),
            CatchUpVerdict::Waiting
        ));
        assert!(matches!(
            rule.sample(start + stall - Duration::from_millis(1), frozen.as_ref()),
            CatchUpVerdict::Waiting
        ));
        let v = rule.sample(start + stall + Duration::from_millis(1), frozen.as_ref());
        match v {
            CatchUpVerdict::Failed(why) => {
                assert!(why.contains("has not advanced"), "{why}");
                assert!(why.contains("1841"), "{why}");
            }
            CatchUpVerdict::Waiting => panic!("a stall past the engine's own bound must fail"),
        }
    }

    /// An engine that never reports a position is stuck too, on the same clock.
    #[test]
    fn an_engine_that_never_reports_fails_on_the_same_clock() {
        let start = Instant::now();
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        let mut rule = CatchUp::new(start, Duration::from_secs(180), stall);
        assert!(matches!(
            rule.sample(start + stall, None),
            CatchUpVerdict::Waiting
        ));
        match rule.sample(start + stall + Duration::from_millis(1), None) {
            CatchUpVerdict::Failed(why) => assert!(why.contains("never reported"), "{why}"),
            CatchUpVerdict::Waiting => panic!("no position forever must fail"),
        }
    }

    /// The floor is a floor: a wait never ends *earlier* than the bound its caller asked for, so
    /// swapping in the rule cannot make any other test fail sooner.
    #[test]
    fn the_floor_is_never_shortened() {
        let start = Instant::now();
        let mut rule = CatchUp::new(start, Duration::from_secs(600), Duration::from_secs(60));
        assert!(matches!(
            rule.sample(start + Duration::from_secs(599), None),
            CatchUpVerdict::Waiting
        ));
        assert!(matches!(
            rule.sample(start + Duration::from_secs(601), None),
            CatchUpVerdict::Failed(_)
        ));
    }

    // ---- the ready/rebuilt rule of #231, scripted: fake state rows, no engine, no sleep ----

    /// The premise of #231 in the same shape as #248's: the engine's own bound on a *building*
    /// view (silent on both channels) is more than the fixed 180 s it was held to, and it is the
    /// same for every flavor.
    #[test]
    fn the_engine_own_bound_exceeds_the_old_fixed_ready_guess() {
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        assert!(
            stall > t_ready(),
            "{stall:?} vs the fixed {:?}: if the engine's numbers came down to this, the \
             progress rule is dead weight and #231's premise needs re-reading",
            t_ready()
        );
    }

    /// Fake state rows that keep moving (a `refreshed_at` stamp, then a `definition_id` swap,
    /// and `applied` advancing in the tail of the signature) are never cut off at the floor —
    /// the case the old fixed deadline got wrong when the throttled host stretched a build.
    #[test]
    fn slowly_progressing_state_rows_outlive_the_ready_floor() {
        let start = Instant::now();
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        let mut rule = ReadyProgress::new(start, Duration::from_secs(180), stall, "view vm_x");
        let step = Duration::from_secs(30);
        for n in 1..=30u64 {
            // 15 minutes of steady progress: 5× the floor. Two channels tick alternately: the
            // state row (refreshed_at / definition_id) and the applied position in the tail.
            let sig = if n % 2 == 0 {
                format!("source|building|t{n}|{:?}|h1@pos{n}", n / 2)
            } else {
                format!("source|building|t0|{{Some(7)}}|h1@pos{n}")
            };
            let v = rule.sample(start + step * (n as u32), &sig);
            assert!(
                matches!(v, CatchUpVerdict::Waiting),
                "sample {n} ended a wait that was still progressing: {v:?}"
            );
        }
        // 29, not 30: the **first** sample is the baseline, not progress (same rule as `CatchUp`
        // — an engine already frozen when the wait begins is measured from then).
        assert_eq!(
            rule.ticks(),
            29,
            "every sample after the first moved a channel"
        );
    }

    /// A view whose state rows and `applied` are both frozen still fails, at the bound the
    /// engine's numbers allow — not at 180 s, and not forever.
    #[test]
    fn a_frozen_ready_wait_fails_at_its_bound() {
        let start = Instant::now();
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        let mut rule = ReadyProgress::new(start, Duration::from_secs(180), stall, "view vm_x");
        let frozen = "source|building|t0|{Some(3)}|h1@pos841";
        // Past the floor and inside what one reconnect plus one timed-out statement explain.
        assert!(matches!(
            rule.sample(start + Duration::from_secs(200), frozen),
            CatchUpVerdict::Waiting
        ));
        assert!(matches!(
            rule.sample(start + stall - Duration::from_millis(1), frozen),
            CatchUpVerdict::Waiting
        ));
        match rule.sample(start + stall + Duration::from_millis(1), frozen) {
            CatchUpVerdict::Failed(why) => {
                assert!(why.contains("did not move its state rows"), "{why}");
                assert!(why.contains("view vm_x"), "{why}");
                assert!(why.contains("0 progress tick(s)"), "{why}");
            }
            CatchUpVerdict::Waiting => panic!("a stall past the engine's own bound must fail"),
        }
    }

    /// The floor is a floor here too: a ready wait never ends earlier than the bound its caller
    /// asked for, so making the waits progress-aware cannot make any other test fail sooner.
    #[test]
    fn the_ready_floor_is_never_shortened() {
        let start = Instant::now();
        let mut rule = ReadyProgress::new(
            start,
            Duration::from_secs(600),
            Duration::from_secs(60),
            "v",
        );
        assert!(matches!(
            rule.sample(start + Duration::from_secs(599), "same"),
            CatchUpVerdict::Waiting
        ));
        assert!(matches!(
            rule.sample(start + Duration::from_secs(601), "same"),
            CatchUpVerdict::Failed(_)
        ));
    }
}
