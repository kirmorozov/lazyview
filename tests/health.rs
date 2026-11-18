//! Probe endpoints of a real engine (`/livez`, `/readyz`, `/startupz`, `/metrics`) through every
//! state a Kubernetes pod goes through: waiting for the database, first build, streaming, a view
//! in error, hot standby and takeover, a database outage, plus HTTP edge cases (HEAD, 404, 405,
//! JSON, a slow client, a flood of idle connections).

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use common::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Liveness stall window (the minimum), boot budget and readiness disconnect grace used by every
/// engine in this binary (the health settings come from the process environment).
const STALL_SECS: u64 = lazyview::health::MIN_STALL_SECS;
const BOOT_STALL_SECS: u64 = 600;
const GRACE_SECS: u64 = 2;

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("LAZYVIEW_LIVENESS_STALL_SECS", STALL_SECS.to_string());
        std::env::set_var(
            "LAZYVIEW_LIVENESS_BOOT_STALL_SECS",
            BOOT_STALL_SECS.to_string(),
        );
        std::env::set_var(
            "LAZYVIEW_READY_DISCONNECT_GRACE_SECS",
            GRACE_SECS.to_string(),
        );
        std::env::remove_var("LAZYVIEW_READY_REQUIRE_ALL");
        std::env::remove_var("LAZYVIEW_HEARTBEAT_MS");
    });
}

/// The readiness probe of deploy/k8s/base/deployment.yaml.
const K8S_READY: &str = "/readyz?standby=ok&building=ok";

async fn get(addr: &str, path: &str) -> Result<Resp> {
    request(addr, "GET", path, "").await
}

async fn status(addr: &str, path: &str) -> Result<u16> {
    Ok(get(addr, path).await?.status)
}

async fn wait_status(addr: &str, path: &str, want: u16, timeout: Duration) -> Result<Resp> {
    let deadline = Instant::now() + timeout;
    loop {
        let last = match get(addr, path).await {
            Ok(r) if r.status == want => return Ok(r),
            Ok(r) => format!("{} {}", r.status, r.body.trim()),
            Err(e) => format!("{e:#}"),
        };
        if Instant::now() > deadline {
            bail!("GET {addr}{path}: wanted {want} within {timeout:?}, last: {last}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Fails unless `/livez` answers 200.
async fn assert_alive(addr: &str, when: &str) -> Result<()> {
    let r = get(addr, "/livez")
        .await
        .with_context(|| format!("/livez {when}"))?;
    ensure!(
        r.status == 200,
        "/livez must be 200 {when}, got {} {}",
        r.status,
        r.body.trim()
    );
    Ok(())
}

fn json(r: &Resp) -> Result<serde_json::Value> {
    serde_json::from_str(&r.body).with_context(|| format!("not JSON: {:?}", r.body))
}

fn metric(body: &str, name: &str) -> Option<f64> {
    body.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
}

async fn setup(db: &Db) -> Result<mysql_async::Conn> {
    let mut c = db.conn().await?;
    exec(
        &mut c,
        "CREATE TABLE t (id INT PRIMARY KEY, g INT NOT NULL, v INT NOT NULL)",
    )
    .await?;
    exec(
        &mut c,
        "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)",
    )
    .await?;
    ensure_lazyview_table(&mut c).await?;
    add_definition(
        &mut c,
        "CREATE VIEW v_sum AS SELECT g, SUM(v) AS s, COUNT(*) AS n FROM t GROUP BY g",
        None,
    )
    .await?;
    Ok(c)
}

/// Probe transitions: waiting for the database → alive, started (startup does not wait for the
/// database), not ready; first build → ready; an invalid definition → still ready unless strict;
/// a second engine on the same schema → hot standby (alive, started, not ready, ready for the
/// Kubernetes probe) → takes over when the first stops; HTTP edge cases.
#[test]
fn health_probe_transitions() {
    init_env();
    scenario("health_probes", |ctx| async move {
        let db = ctx.new_db("health").await?;
        let mut c = setup(&db).await?;

        // 1. Database unreachable at boot: alive (the connect loop beats), started, not ready.
        let dead = free_local_addr()?;
        let dead_dsn = with_authority_host(&db.dsn, &dead);
        let w = free_local_addr()?;
        let waiting = ctx.start_engine(
            &db,
            &[
                ("LAZYVIEW_DSN", dead_dsn.as_str()),
                ("LAZYVIEW_HEALTH_ADDR", w.as_str()),
            ],
        )?;
        wait_status(&w, "/livez", 200, Duration::from_secs(30)).await?;
        let r = get(&w, "/readyz?format=json").await?;
        ensure!(
            r.status == 503,
            "/readyz while waiting for the database: {} {}",
            r.status,
            r.body
        );
        ensure!(
            json(&r)?["role"] == "starting",
            "role while waiting for the database: {}",
            r.body
        );
        let j = json(&r)?;
        ensure!(
            j["engine"]["booting"] == true
                && j["engine"]["liveness_limit_seconds"] == BOOT_STALL_SECS
                && j["engine"]["binlog_disconnected_seconds"] == 0,
            "engine while waiting for the database: {}",
            r.body
        );
        let r = get(&w, "/startupz?verbose").await?;
        ensure!(
            r.status == 200 && r.body.contains("[+]phase ok: booting"),
            "/startupz must pass while waiting for the database (a restart does not help): {} {:?}",
            r.status,
            r.body
        );
        // The connect loop beats (backoff 1, 2, 4, 8 s, doubling toward the 30 s cap -
        // `connect_wait` in src/engine.rs): the heartbeat age keeps resetting. What this
        // pins is the cadence - a beat at least once per backoff step while the engine
        // waits for the database - so the bound is derived from that schedule in elapsed
        // time, not from a count against a fixed wall clock a slow runner can miss: the
        // first sample only seeds the comparison and the watch may open just after a beat,
        // so counted beat n arrives with the (n+2)th real beat at the latest, which the
        // schedule places (2^(n+1) - 1) s from the watch opening. A saturated runner
        // stretches every sleep and GET, so the schedule time is scaled by SLACK and the
        // window is widened by the measured poll round trips (a GET slower than the beat
        // interval is what hides a reset from the age samples). The live liveness pin is
        // unchanged: every sample must still find the age under the stall limit.
        const BEATS: u32 = 3;
        const SLACK: u32 = 4;
        let (mut beats, mut last_age) = (0, u64::MAX);
        let watch = Instant::now();
        let mut cycle = Duration::ZERO; // the worst /livez round trip seen so far
        while beats < BEATS {
            let due = Duration::from_secs((1u64 << (beats + 2)) - 1) * SLACK + cycle * BEATS;
            ensure!(
                watch.elapsed() < due,
                "only {beats}/{BEATS} heartbeat(s) {}s into the watch: beat {} is overdue \
                 against its {due:?} bound (connect backoff schedule x{SLACK} for runner slack)",
                watch.elapsed().as_secs_f64(),
                beats + 1,
            );
            let t0 = Instant::now();
            let r = get(&w, "/livez?format=json").await?;
            cycle = cycle.max(t0.elapsed());
            ensure!(
                r.status == 200,
                "/livez while waiting for the database: {}",
                r.body
            );
            let age = json(&r)?["engine"]["heartbeat_age_seconds"]
                .as_u64()
                .context("heartbeat_age_seconds")?;
            ensure!(
                age < STALL_SECS,
                "heartbeat {age}s old while waiting for the database"
            );
            if age < last_age && last_age != u64::MAX {
                beats += 1;
            }
            last_age = age;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        waiting
            .stop()
            .await
            .context("stop the engine waiting for the database")?;

        // 2. First engine: ready once the view is built.
        let a_addr = free_local_addr()?;
        let mut a = ctx.start_engine(&db, &[("LAZYVIEW_HEALTH_ADDR", a_addr.as_str())])?;
        wait_status(&a_addr, "/livez", 200, Duration::from_secs(30)).await?;
        wait_ready(&mut c, &mut a, &["v_sum"], t_ready()).await?;
        let r = wait_status(&a_addr, "/readyz", 200, Duration::from_secs(60)).await?;
        ensure!(r.body == "ready\n", "/readyz body: {:?}", r.body);
        ensure!(
            status(&a_addr, "/startupz").await? == 200,
            "/startupz once streaming"
        );
        let r = get(&a_addr, "/readyz?verbose").await?;
        ensure!(
            r.body.contains("[+]role ok: active") && r.body.ends_with("readyz check passed\n"),
            "verbose: {}",
            r.body
        );
        ensure!(
            status(&a_addr, K8S_READY).await? == 200,
            "Kubernetes readiness of the active engine"
        );

        // 3. A view in error: still ready (reported), not ready in strict mode.
        add_definition(
            &mut c,
            "CREATE VIEW v_broken AS SELECT nope FROM no_such_table",
            None,
        )
        .await?;
        wait_state(&mut c, &mut a, "v_broken", "error", t_ready()).await?;
        wait_until("lazyview_views_error 1", Duration::from_secs(30), || {
            let a = a_addr.clone();
            async move {
                Ok(metric(&get(&a, "/metrics").await?.body, "lazyview_views_error") == Some(1.0))
            }
        })
        .await?;
        let r = get(&a_addr, "/readyz").await?;
        ensure!(
            r.status == 200 && r.body.contains("1 view(s) in error, ignored"),
            "/readyz with an error view: {} {:?}",
            r.status,
            r.body
        );
        let r = get(&a_addr, "/readyz?require_all=1").await?;
        ensure!(
            r.status == 503 && r.body.contains("in error"),
            "strict /readyz with an error view: {} {:?}",
            r.status,
            r.body
        );
        assert_alive(&a_addr, "with a view in error").await?;

        // 4. Second engine on the same schema: hot standby.
        let b_addr = free_local_addr()?;
        let mut b = ctx.start_engine(&db, &[("LAZYVIEW_HEALTH_ADDR", b_addr.as_str())])?;
        wait_until("second engine in standby", Duration::from_secs(60), || {
            let b = b_addr.clone();
            async move {
                Ok(get(&b, "/metrics")
                    .await
                    .map(|r| metric(&r.body, "lazyview_standby") == Some(1.0))
                    .unwrap_or(false))
            }
        })
        .await?;
        b.check_alive()?;
        assert_alive(&b_addr, "in standby").await?;
        let r = get(&b_addr, "/startupz?verbose").await?;
        ensure!(
            r.status == 200 && r.body.contains("[+]phase ok: hot standby"),
            "/startupz must pass in standby: {} {:?}",
            r.status,
            r.body
        );
        let r = get(&b_addr, "/readyz").await?;
        ensure!(
            r.status == 503 && r.body.contains("hot standby"),
            "standby /readyz: {} {:?}",
            r.status,
            r.body
        );
        for path in ["/readyz?standby=ok", K8S_READY] {
            let r = get(&b_addr, path).await?;
            ensure!(
                r.status == 200 && r.body == "ready (hot standby)\n",
                "standby {path}: {} {:?}",
                r.status,
                r.body
            );
        }
        ensure!(
            lazyview::health::probe(b_addr.parse()?).await,
            "`lazyview healthcheck` probe of the standby"
        );
        let m = get(&b_addr, "/metrics").await?.body;
        ensure!(
            metric(&m, "lazyview_ready") == Some(0.0)
                && metric(&m, "lazyview_alive") == Some(1.0)
                && metric(&m, "lazyview_binlog_disconnected_seconds") == Some(0.0)
                && metric(&m, "lazyview_liveness_limit_seconds") == Some(STALL_SECS as f64),
            "standby metrics:\n{m}"
        );
        ensure!(
            status(&a_addr, "/readyz").await? == 200,
            "active stays ready while a standby waits"
        );
        // Keep the fixed sleep: the assertion is that nothing happens while the standby
        // idles — only a fixed stretch of time shows whether the watchdog kills it.
        tokio::time::sleep(Duration::from_secs(3)).await;
        b.check_alive()?;
        assert_alive(&b_addr, "after waiting in standby").await?;

        // 5. HTTP edge cases (against the active engine).
        let r = request(&a_addr, "HEAD", "/readyz", "").await?;
        ensure!(
            r.status == 200 && r.body.is_empty() && r.head.contains("Content-Length: "),
            "HEAD /readyz: {} {:?} {:?}",
            r.status,
            r.head,
            r.body
        );
        let r = request(&a_addr, "HEAD", "/livez", "").await?;
        ensure!(
            r.status == 200 && r.body.is_empty(),
            "HEAD /livez: {}",
            r.status
        );
        ensure!(status(&a_addr, "/healthz").await? == 200, "/healthz alias");
        ensure!(status(&a_addr, "/nope").await? == 404, "unknown path");
        let r = request(&a_addr, "POST", "/readyz", "Content-Length: 0\r\n").await?;
        ensure!(
            r.status == 405 && r.head.contains("Allow: GET, HEAD"),
            "POST /readyz: {} {:?}",
            r.status,
            r.head
        );
        let r = request(&a_addr, "GET", "/readyz", "Accept: application/json\r\n").await?;
        let j = json(&r)?;
        ensure!(
            j["status"] == "ok" && j["role"] == "active" && j["views"]["error"] == 1,
            "JSON readiness: {}",
            r.body
        );
        let m = get(&a_addr, "/metrics").await?;
        ensure!(
            m.head.contains("text/plain; version=0.0.4"),
            "metrics content type: {}",
            m.head
        );
        for name in [
            "lazyview_units_refreshed_total",
            "lazyview_streaming",
            "lazyview_standby",
            "lazyview_starting",
            "lazyview_ready",
            "lazyview_alive",
            "lazyview_heartbeat_age_seconds",
            "lazyview_binlog_disconnected_seconds",
        ] {
            ensure!(
                metric(&m.body, name).is_some(),
                "/metrics lacks {name}:\n{}",
                clip(&m.body, 3000)
            );
        }
        ensure!(
            metric(&m.body, "lazyview_ready") == Some(1.0)
                && metric(&m.body, "lazyview_starting") == Some(0.0),
            "active metrics:\n{}",
            m.body
        );
        // Slow client: the head must arrive within 2 s, the endpoint keeps answering meanwhile.
        let mut slow = TcpStream::connect(&a_addr).await?;
        slow.write_all(b"GET /livez HTTP/1.1\r\n").await?;
        assert_alive(&a_addr, "while a slow client is connected").await?;
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), slow.read_to_end(&mut buf))
            .await
            .context("slow client not cut off")??;
        ensure!(
            buf.starts_with(b"HTTP/1.1 408 "),
            "slow client: {:?}",
            String::from_utf8_lossy(&buf)
        );
        // A flood of idle connections (more than the 512 the endpoint keeps open) from the same
        // address as the probe: probes are still answered at once.
        let mut idle = Vec::new();
        for _ in 0..700 {
            idle.push(TcpStream::connect(&a_addr).await?);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        for _ in 0..5 {
            let t0 = Instant::now();
            assert_alive(&a_addr, "with 700 idle connections open").await?;
            ensure!(
                t0.elapsed() < Duration::from_secs(1),
                "/livez took {:?} with 700 idle connections open",
                t0.elapsed()
            );
        }
        drop(idle);

        // 6. The active engine stops: the standby takes over and becomes ready.
        a.stop().await?;
        ensure!(
            get(&a_addr, "/livez").await.is_err(),
            "the stopped engine's endpoint must be gone"
        );
        let r = wait_status(&b_addr, "/readyz", 200, Duration::from_secs(60)).await?;
        ensure!(r.body.contains("ready"), "after takeover: {:?}", r.body);
        let m = get(&b_addr, "/metrics").await?.body;
        ensure!(
            metric(&m, "lazyview_standby") == Some(0.0)
                && metric(&m, "lazyview_streaming") == Some(1.0),
            "after takeover:\n{m}"
        );
        exec(&mut c, "INSERT INTO t VALUES (4, 2, 40)").await?;
        let mut sc = db.conn().await?;
        b.wait_caught_up(&mut sc, t_catchup()).await?;
        ensure!(
            scalar_i64(&mut c, "SELECT s FROM v_sum WHERE g = 2").await? == 70,
            "the new active engine maintains the view"
        );
        assert_alive(&b_addr, "after takeover").await?;
        b.stop().await?;
        Ok(())
    });
}

/// TCP forwarder in front of the database that can simulate an outage.
struct Proxy {
    addr: SocketAddr,
    up: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept: JoinHandle<()>,
}

impl Proxy {
    async fn start(upstream: SocketAddr) -> Result<Proxy> {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        let up = Arc::new(AtomicBool::new(true));
        let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let (up2, conns2) = (up.clone(), conns.clone());
        let accept = tokio::spawn(async move {
            while let Ok((mut client, _)) = l.accept().await {
                if !up2.load(Ordering::SeqCst) {
                    continue; // dropped: the client sees the connection closed
                }
                let h = tokio::spawn(async move {
                    if let Ok(mut server) = TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
                conns2.lock().unwrap().push(h);
            }
        });
        Ok(Proxy {
            addr,
            up,
            conns,
            accept,
        })
    }

    /// Cuts every connection and refuses new ones.
    fn down(&self) {
        self.up.store(false, Ordering::SeqCst);
        for h in self.conns.lock().unwrap().drain(..) {
            h.abort();
        }
    }

    fn up(&self) {
        self.up.store(true, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.down();
        self.accept.abort();
    }
}

/// `mysql://user:pass@host:port/db?x` with `host:port` replaced.
fn with_authority_host(dsn: &str, hostport: &str) -> String {
    let (scheme, rest) = dsn.split_once("://").unwrap_or(("mysql", dsn));
    let (auth, tail) = rest.split_at(rest.find(['/', '?']).unwrap_or(rest.len()));
    let creds = auth
        .rsplit_once('@')
        .map(|(c, _)| format!("{c}@"))
        .unwrap_or_default();
    format!("{scheme}://{creds}{hostport}{tail}")
}

fn upstream_of(dsn: &str) -> Result<SocketAddr> {
    let opts = mysql_async::Opts::from_url(dsn)?;
    let host = opts.ip_or_hostname();
    let host = if host == "localhost" {
        "127.0.0.1"
    } else {
        host
    };
    Ok(format!("{host}:{}", opts.tcp_port()).parse()?)
}

/// Database outage longer than the liveness stall window: liveness keeps passing (the engine
/// keeps beating while it reconnects), readiness fails once the binlog has been disconnected for
/// longer than the grace period and recovers after the database is back.
#[test]
fn health_database_outage() {
    init_env();
    scenario("health_outage", |ctx| async move {
        let db = ctx.new_db("health_outage").await?;
        let mut c = setup(&db).await?;
        let proxy = Proxy::start(upstream_of(&db.dsn)?).await?;
        // TCP only: mysql_async would otherwise switch 127.0.0.1 connections to the server's
        // unix socket (`prefer_socket`), bypassing the proxy.
        let dsn = with_authority_host(&db.dsn, &proxy.addr.to_string());
        let dsn = format!(
            "{dsn}{}prefer_socket=false",
            if dsn.contains('?') { '&' } else { '?' }
        );
        let addr = free_local_addr()?;
        let mut eng = ctx.start_engine(
            &db,
            &[
                ("LAZYVIEW_DSN", dsn.as_str()),
                ("LAZYVIEW_HEALTH_ADDR", addr.as_str()),
            ],
        )?;
        wait_ready(&mut c, &mut eng, &["v_sum"], t_ready()).await?;
        wait_status(&addr, "/readyz", 200, Duration::from_secs(60)).await?;

        proxy.down();
        let outage = Duration::from_secs(STALL_SECS + 10);
        let started = Instant::now();
        let mut saw_unready = false;
        while started.elapsed() < outage {
            eng.check_alive().context("engine during the outage")?;
            assert_alive(
                &addr,
                &format!(
                    "{:.0}s into a database outage",
                    started.elapsed().as_secs_f64()
                ),
            )
            .await?;
            let r = get(&addr, "/readyz").await?;
            if r.status == 503 {
                ensure!(
                    r.body.contains("disconnected from the binlog"),
                    "/readyz during the outage: {:?}",
                    r.body
                );
                saw_unready = true;
            } else {
                ensure!(
                    started.elapsed() < Duration::from_secs(GRACE_SECS + 5),
                    "still ready {:?} into the outage",
                    started.elapsed()
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        ensure!(
            saw_unready,
            "/readyz never failed during a {outage:?} outage"
        );

        proxy.up();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            eng.check_alive()
                .context("engine after the database came back")?;
            if matches!(get(&addr, "/readyz").await, Ok(r) if r.status == 200) {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "not ready again within 90 s after the outage"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_alive(&addr, "after the outage").await?;
        eng.stop().await?;
        drop(proxy);
        Ok(())
    });
}

// ── Per-view seconds behind the source (review O-01, ruling 1) ──────────────────────────────
// The gauge `lazyview_view_source_lag_seconds{view}` (and `/stats` `source_lag_seconds`): 0 while
// a view has applied every change the reader has seen (an idle source reads exactly 0), the real
// age during a backlog (an engine restarted over a gap reads the gap's length until it catches up),
// and growing while binlog reading is held (the "paused reader"). The engine's unit tests pin the
// formula; these run it against a real server on every flavor.

/// A grouped view over a neutral `entity` table.
fn lag_view() -> ViewSpec {
    ViewSpec::new(
        "vs_lag",
        Kind::Incremental,
        "SELECT grp, SUM(val) AS total, COUNT(*) AS n FROM entity GROUP BY grp",
    )
}

/// Creates `entity` and seeds `rows` rows into it.
async fn lag_seed(db: &Db, rows: i64) -> Result<()> {
    let mut c = db.conn().await?;
    exec(
        &mut c,
        "CREATE TABLE entity (id BIGINT PRIMARY KEY, grp INT NOT NULL, val BIGINT NOT NULL)",
    )
    .await?;
    let vals: Vec<String> = (1..=rows)
        .map(|i| format!("({i}, {}, {i})", i % 5))
        .collect();
    exec(
        &mut c,
        &format!(
            "INSERT INTO entity (id, grp, val) VALUES {}",
            vals.join(",")
        ),
    )
    .await?;
    c.disconnect().await.ok();
    Ok(())
}

/// The view's `source_lag_seconds` as served by `/stats`.
async fn lag_of(addr: &str, key: &str) -> Result<u64> {
    let j = fetch_stats(addr).await?;
    let v = j["views"]
        .get(key)
        .with_context(|| format!("{key} missing in /stats: {j}"))?;
    v["source_lag_seconds"]
        .as_u64()
        .with_context(|| format!("no source_lag_seconds for {key}: {v}"))
}

/// Polls until `key` reports 0 s behind the source (the first heartbeat after the load).
async fn lag_wait_zero(addr: &str, key: &str, budget: Duration) -> Result<()> {
    let t = Instant::now();
    loop {
        if let Ok(l) = lag_of(addr, key).await {
            if l == 0 {
                return Ok(());
            }
        }
        ensure!(
            t.elapsed() < budget,
            "{key} never reached 0 s behind the source"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Acceptance criterion 1 (review O-01): after a write burst settles, an idle source keeps every
/// view at exactly 0 s behind it while heartbeats flow — the old position-age gauge could not say
/// this (it grew while idle). The figure is checked on `/stats` and on `/metrics`.
#[test]
fn lag_idle_source_stays_zero() {
    init_env();
    scenario("lag_idle", |ctx| async move {
        let db = ctx.new_db("lag_idle").await?;
        lag_seed(&db, 200).await?;
        let addr = free_local_addr()?;
        let (eng, _) = start_with_views(
            &ctx,
            &db,
            &[lag_view()],
            &[
                ("LAZYVIEW_HEALTH_ADDR", addr.as_str()),
                ("LAZYVIEW_HEARTBEAT_MS", "500"),
            ],
        )
        .await?;
        let key = format!("{}.vs_lag", db.name);
        lag_wait_zero(&addr, &key, Duration::from_secs(90)).await?;
        // Keep the source idle with heartbeats flowing and assert the number stays 0 throughout.
        let mut samples = 0;
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(30) {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let l = lag_of(&addr, &key).await?;
            ensure!(l == 0, "an idle source put {key} {l} s behind the source");
            samples += 1;
        }
        ensure!(samples >= 5, "only {samples} idle samples taken");
        let (status, metrics) = http_get(&addr, "/metrics").await?;
        ensure!(status == 200, "/metrics status {status}");
        let series = format!("lazyview_view_source_lag_seconds{{view=\"{key}\"}} 0\n");
        ensure!(metrics.contains(&series), "/metrics lacks {series:?}");
        ctx.log(format!(
            "lag_idle: 0 s behind the source over {samples} idle samples"
        ));
        eng.stop().await?;
        Ok(())
    });
}

/// Appends rows `first..=first+count-1` to `entity`, 1000 per statement (the same shape
/// [`lag_seed`] writes, so the replayed backlog exercises the same decode and refresh path).
async fn lag_rows(db: &Db, first: i64, count: i64) -> Result<()> {
    let mut c = db.conn().await?;
    let last = first + count - 1;
    let mut id = first;
    while id <= last {
        let end = (id + 999).min(last);
        let vals: Vec<String> = (id..=end)
            .map(|i| format!("({i}, {}, {i})", i % 5))
            .collect();
        exec(
            &mut c,
            &format!(
                "INSERT INTO entity (id, grp, val) VALUES {}",
                vals.join(",")
            ),
        )
        .await?;
        id = end + 1;
    }
    c.disconnect().await.ok();
    Ok(())
}

/// The source's own clock: the gap lengths are measured on the same clock the gauge subtracts
/// from (ruling 1 keeps the host clock out of the number entirely).
async fn lag_source_now(db: &Db) -> Result<i64> {
    let mut c = db.conn().await?;
    let t = scalar_i64(&mut c, "SELECT UNIX_TIMESTAMP()").await?;
    c.disconnect().await.ok();
    Ok(t)
}

/// The view's gauge as served by `/metrics` (the series an alert reads).
async fn lag_metric(addr: &str, key: &str) -> Result<u64> {
    let (status, m) = http_get(addr, "/metrics").await?;
    ensure!(status == 200, "/metrics status {status}");
    let prefix = format!("lazyview_view_source_lag_seconds{{view=\"{key}\"}} ");
    for line in m.lines() {
        if let Some(v) = line.strip_prefix(&prefix) {
            let f: f64 = v
                .trim()
                .parse()
                .with_context(|| format!("bad gauge value in {line:?}"))?;
            return Ok(f as u64);
        }
    }
    bail!("no lazyview_view_source_lag_seconds series for {key} in /metrics")
}

/// The final `entity` sums vs the `vs_lag` rows: the proof a backlog (or a held reader's queue)
/// landed in full — the gauge being honest is no substitute for the rows being right.
async fn lag_rows_match(c: &mut mysql_async::Conn, view: &str) -> Result<()> {
    let want = rows(
        c,
        "SELECT grp, SUM(val), COUNT(*) FROM entity GROUP BY grp ORDER BY grp",
    )
    .await?;
    let got = rows(c, &format!("SELECT grp, total, n FROM {view} ORDER BY grp")).await?;
    ensure!(
        want == got,
        "{view} rows do not match the source:\nwant {want:?}\ngot  {got:?}"
    );
    Ok(())
}

/// Ruling 1's "a paused reader shows a growing lag", against a real server: a second view whose
/// build sleeps (`LAZYVIEW_TEST_BUILD_DELAY`) queues its incoming changes past the 1-byte
/// build-hold budget (`LAZYVIEW_TEST_BUILD_HOLD_BYTES`), so the engine holds binlog reading
/// (`Engine::hold_for_builds`, `SourceClock::reading = false`) until the build is applied. The
/// building view — the one whose changes sit undelivered behind the hold — reads a number
/// **above zero and growing** (ruling 1: a held reader is a growing lag, not a stale-but-green
/// 0); the ready view, which keeps applying everything it is handed, reads its honest 0, and
/// both read 0 once the reader resumes and the queued changes land in full.
#[test]
fn lag_grows_while_the_reader_is_held() {
    init_env();
    scenario("lag_reader_held", |ctx| async move {
        let db = ctx.new_db("lag_reader_held").await?;
        lag_seed(&db, 200).await?;
        let addr = free_local_addr()?;
        let (mut eng, _) = start_with_views(
            &ctx,
            &db,
            &[lag_view()],
            &[
                ("LAZYVIEW_HEALTH_ADDR", addr.as_str()),
                ("LAZYVIEW_HEARTBEAT_MS", "500"),
                ("LAZYVIEW_TEST_BUILD_HOLD_BYTES", "1"),
                ("LAZYVIEW_TEST_BUILD_DELAY", "/*hold_reader*/:45000"),
            ],
        )
        .await?;
        let key = format!("{}.vs_lag", db.name);
        lag_wait_zero(&addr, &key, Duration::from_secs(90)).await?;
        // The second view's build sleeps 45 s; every change queued for it during that time sits
        // past the 1-byte build budget, which is what holds the reader. It is that building
        // view whose gauge must speak: the changes handed to it wait undelivered, while the
        // ready view keeps applying what it is handed and reads its honest 0 throughout.
        let hkey = format!("{}.vs_hold", db.name);
        let mut c = db.conn().await?;
        add_views(
            &mut c,
            &[ViewSpec::new(
                "vs_hold",
                Kind::Incremental,
                "SELECT grp, MAX(val) AS mx FROM entity WHERE /*hold_reader*/ 1=1 GROUP BY grp",
            )],
        )
        .await?;
        wait_state(
            &mut c,
            &mut eng,
            "vs_hold",
            "building",
            Duration::from_secs(60),
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(1500)).await; // into the build's sleep
        lag_rows(&db, 1001, 300).await?; // the backlog that queues behind the held reader
                                         // Above zero while the reader is held (ruling 1: not "green while a backlog sits").
        let t = Instant::now();
        let mut g1 = 0;
        // (Samples before the building view's first publish carry no entry yet and do not count.)
        loop {
            if let Ok(g) = lag_of(&addr, &hkey).await {
                g1 = g;
                if g1 >= 3 {
                    break;
                }
            }
            ensure!(
                t.elapsed() < Duration::from_secs(45),
                "{hkey} never went above zero while the reader was held (last {g1} s): \
                 the build-hold never engaged or the gauge ignores it"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert_alive(&addr, "while the reader is held").await?;
        // And growing: the source keeps writing while the build's queue waits. The number only
        // moves at publishes (the flush cadence of the hold loop), so the budget is that
        // cadence plus a margin.
        let t = Instant::now();
        let mut grew = false;
        while !grew {
            let g = lag_of(&addr, &hkey).await?;
            ensure!(
                g >= g1,
                "{hkey} went backwards ({g1} s to {g} s) while the reader was held"
            );
            grew = g >= g1 + 3;
            ensure!(
                t.elapsed() < Duration::from_secs(40),
                "{hkey} did not grow past {g1} s while the reader was held (last {g} s)"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let m = lag_metric(&addr, &hkey).await?;
        ensure!(m > 0, "/metrics reports {hkey} at {m} s during the hold");
        ctx.log(format!(
            "lag_reader_held: the building view at {g1} s and grown past {g1}+3 s while the \
             reader was held by the build budget; /metrics agrees ({m} s)"
        ));
        // The build finishes, the reader resumes, the queue lands and the number reads 0 again.
        wait_state(
            &mut c,
            &mut eng,
            "vs_hold",
            "ready",
            Duration::from_secs(120),
        )
        .await?;
        lag_wait_zero(&addr, &key, Duration::from_secs(120)).await?;
        lag_wait_zero(
            &addr,
            &format!("{}.vs_hold", db.name),
            Duration::from_secs(120),
        )
        .await?;
        lag_rows_match(&mut c, "vs_lag").await?;
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}

/// Ruling 1's "a stopped engine with writes in the gap, restarted, reports about the gap's
/// length until it has caught up, then 0" — with a backlog no single flush applies: the engine
/// stops, 600 000 rows are written while it is down, and the gauge is sampled while the restarted
/// engine replays the gap. `LAZYVIEW_TEST_LAG_HOLD_BYTES` (256 B, where the constant is 256 MiB)
/// keeps the view's target accounted as behind while the stream has read past what the flush
/// applied — the backlog genuinely sits (its keys are kept, the saved position waits) instead of
/// being swallowed by one catch-up flush. The sampled number is the age of the gap on the
/// **source's** clock (downtime plus replay), never the idle 0, and it returns to 0 only once the
/// view has caught up; the rows land in full.
#[test]
fn lag_reports_the_gap_after_a_restart() {
    init_env();
    scenario("lag_gap_restart", |ctx| async move {
        let db = ctx.new_db("lag_gap_restart").await?;
        lag_seed(&db, 200).await?;
        let addr1 = free_local_addr()?;
        let (eng, _) = start_with_views(
            &ctx,
            &db,
            &[lag_view()],
            &[
                ("LAZYVIEW_HEALTH_ADDR", addr1.as_str()),
                ("LAZYVIEW_HEARTBEAT_MS", "500"),
            ],
        )
        .await?;
        let key = format!("{}.vs_lag", db.name);
        lag_wait_zero(&addr1, &key, Duration::from_secs(90)).await?;
        eng.stop().await?;
        // The gap: a backlog written while the engine is down, then time for it to age so "about
        // the gap's length" is a number no single-flush catch-up can fake. Measured on the
        // source's own clock.
        let down_t0 = lag_source_now(&db).await?;
        lag_rows(&db, 201, 600_000).await?;
        tokio::time::sleep(Duration::from_secs(45)).await;
        let down_t1 = lag_source_now(&db).await?;
        let gap = (down_t1 - down_t0) as u64;
        ensure!(
            gap >= 45,
            "the gap only lasted {gap} s (measured on the source clock)"
        );
        ctx.log(format!(
            "lag_gap_restart: {gap} s of writes in the gap with the engine down; restarting \
             over it (LAZYVIEW_TEST_LAG_HOLD_BYTES=256)"
        ));
        let addr2 = free_local_addr()?;
        let mut eng = ctx.start_engine(
            &db,
            &[
                ("LAZYVIEW_HEALTH_ADDR", addr2.as_str()),
                ("LAZYVIEW_HEARTBEAT_MS", "500"),
                ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "256"),
            ],
        )?;
        let mut c = db.conn().await?;
        wait_ready(&mut c, &mut eng, &["vs_lag"], t_ready()).await?;
        // Sample the replay: the figure is the age of the gap, not the idle 0. (Samples that
        // land before the first publish carry no views yet and do not count.)
        let (mut samples, mut high, mut maxg) = (0u32, 0u32, 0u64);
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(30) {
            if let Ok(g) = lag_of(&addr2, &key).await {
                samples += 1;
                if g >= gap.saturating_sub(10) {
                    high += 1;
                }
                maxg = maxg.max(g);
                ensure!(
                    g <= gap + 180,
                    "{key} reports {g} s behind the source — no gap of {gap} s explains that"
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ctx.log(format!(
            "lag_gap_restart: {samples} samples while the backlog sat, {high} at >= \
             {} s (gap {gap} s, max {maxg} s)",
            gap.saturating_sub(10)
        ));
        ensure!(
            samples >= 40,
            "only {samples} /stats samples fell inside the backlog window"
        );
        ensure!(
            high >= 8,
            "the restarted engine reported the backlog as caught up: only {high} of {samples} \
             samples showed the gap ({gap} s) instead of the idle 0"
        );
        // ... and 0 once it has caught up (ruling 1's full sentence).
        lag_wait_zero(&addr2, &key, Duration::from_secs(240)).await?;
        // The replay lost nothing: every row of the gap is summed.
        lag_rows_match(&mut c, "vs_lag").await?;
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}
