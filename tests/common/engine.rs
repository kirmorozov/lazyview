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

    /// Waits until `applied() >= target`.
    pub async fn wait_applied(&mut self, target: &Position, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(a) = self.metrics.applied() {
                if &a >= target {
                    return Ok(());
                }
            }
            self.check_alive()?;
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

/// One HTTP request to an engine's health endpoint (`addr` as given in `LAZYVIEW_HEALTH_ADDR`):
/// 5 s connect and read timeouts, the response split at `\r\n\r\n`, status from the status line.
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
    let (head, body) = text
        .split_once("\r\n\r\n")
        .with_context(|| format!("bad HTTP response {text:?}"))?;
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .with_context(|| format!("bad status line {head:?}"))?;
    Ok(Resp {
        status,
        head: head.to_string(),
        body: body.to_string(),
    })
}

/// `GET <path>` on an engine's health endpoint (`addr` as given in `LAZYVIEW_HEALTH_ADDR`):
/// status and body.
pub async fn http_get(addr: &str, path: &str) -> Result<(u16, String)> {
    let r = request(addr, "GET", path, "").await?;
    Ok((r.status, r.body))
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

/// Default timeouts (scaled).
pub fn t_ready() -> Duration {
    Duration::from_secs_f64(180.0 * scale().max(1.0))
}
pub fn t_catchup() -> Duration {
    Duration::from_secs_f64(180.0 * scale().max(1.0))
}
