//! Probe endpoints of a real engine (`/livez`, `/readyz`, `/startupz`, `/metrics`) through every
//! state a Kubernetes pod goes through: waiting for the database, first build, streaming, a view
//! in error, hot standby and takeover, a database outage, plus HTTP edge cases.

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

/// Liveness stall window and readiness disconnect grace used by every engine in this binary
/// (the health settings come from the process environment).
const STALL_SECS: u64 = 15;
const GRACE_SECS: u64 = 2;

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("LAZYVIEW_LIVENESS_STALL_SECS", STALL_SECS.to_string());
        std::env::set_var(
            "LAZYVIEW_READY_DISCONNECT_GRACE_SECS",
            GRACE_SECS.to_string(),
        );
        std::env::remove_var("LAZYVIEW_READY_REQUIRE_ALL");
    });
}

fn free_addr() -> Result<String> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    Ok(format!("127.0.0.1:{port}"))
}

struct Resp {
    status: u16,
    head: String,
    body: String,
}

async fn request(addr: &str, method: &str, path: &str, headers: &str) -> Result<Resp> {
    let mut s = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
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

/// Probe transitions: waiting for the database → starting/not ready; first build → ready; an
/// invalid definition → still ready unless strict; a second engine on the same schema → hot
/// standby (alive, started, not ready) → takes over when the first stops; HTTP edge cases.
#[test]
fn health_probe_transitions() {
    init_env();
    scenario("health_probes", |ctx| async move {
        let db = ctx.new_db("health").await?;
        let mut c = setup(&db).await?;

        // 1. Database unreachable at boot: alive (the connect loop beats), not started, not ready.
        let dead = free_addr()?;
        let dead_dsn = with_authority_host(&db.dsn, &dead);
        let w = free_addr()?;
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
        ensure!(
            status(&w, "/startupz").await? == 503,
            "/startupz must fail while waiting for the database"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_alive(&w, "while waiting for the database").await?;
        waiting
            .stop()
            .await
            .context("stop the engine waiting for the database")?;

        // 2. First engine: ready once the view is built.
        let a_addr = free_addr()?;
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
        let b_addr = free_addr()?;
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
        ensure!(
            status(&b_addr, "/startupz").await? == 200,
            "/startupz must pass in standby"
        );
        let r = get(&b_addr, "/readyz").await?;
        ensure!(
            r.status == 503 && r.body.contains("hot standby"),
            "standby /readyz: {} {:?}",
            r.status,
            r.body
        );
        let r = get(&b_addr, "/readyz?standby=ok").await?;
        ensure!(
            r.status == 200 && r.body == "ready (hot standby)\n",
            "standby /readyz?standby=ok: {} {:?}",
            r.status,
            r.body
        );
        ensure!(
            lazyview::health::probe(b_addr.parse()?).await,
            "`lazyview healthcheck` probe of the standby"
        );
        let m = get(&b_addr, "/metrics").await?.body;
        ensure!(
            metric(&m, "lazyview_ready") == Some(0.0) && metric(&m, "lazyview_alive") == Some(1.0),
            "standby metrics:\n{m}"
        );
        ensure!(
            status(&a_addr, "/readyz").await? == 200,
            "active stays ready while a standby waits"
        );
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
        // Slow client: the head must arrive within 5 s, the endpoint keeps answering meanwhile.
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
///
/// Ignored until the engine (a) beats in the binlog reconnect loop and bounds each connect
/// attempt, and (b) re-opens its source/metadata connections after an outage instead of exiting
/// with "connection closed": `cargo test --test health -- --ignored`.
#[test]
#[ignore = "needs engine support: heartbeat in the binlog reconnect loop, reconnecting stale connections"]
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
        let addr = free_addr()?;
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
