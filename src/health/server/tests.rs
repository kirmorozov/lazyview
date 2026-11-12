use super::*;
use anyhow::Result;

#[test]
fn request_parsing() {
    let p = |s: &str| parse_head(s.as_bytes());
    let r = p("GET /readyz?verbose&standby=ok&building=ok&require_all=0 HTTP/1.1\r\nHost: x\r\nAccept: text/html, Application/JSON\r\n\r\n").unwrap();
    assert_eq!(
        (r.method.as_str(), r.path.as_str(), r.head_only),
        ("GET", "/readyz", false)
    );
    assert_eq!(r.flag("verbose"), Some(true));
    assert_eq!(r.flag("standby"), Some(true));
    assert_eq!(r.flag("building"), Some(true));
    assert_eq!(r.flag("require_all"), Some(false));
    assert_eq!(r.flag("nope"), None);
    assert!(r.wants_json());
    let r = p("HEAD http://pod:8080/livez/?format=text HTTP/1.0\n\n").unwrap();
    assert_eq!(
        (r.path.as_str(), r.head_only, r.wants_json()),
        ("/livez", true, false)
    );
    assert_eq!(p("GET http://pod:8080 HTTP/1.1\r\n\r\n").unwrap().path, "/");
    assert_eq!(
        p("GET /metrics#x HTTP/1.1\r\n\r\n").unwrap().path,
        "/metrics"
    );
    assert!(p("GET /readyz?format=json HTTP/1.1\r\n\r\n")
        .unwrap()
        .wants_json());
    assert_eq!(
        p("GET /readyz?standby=0 HTTP/1.1\r\n\r\n")
            .unwrap()
            .flag("standby"),
        Some(false)
    );
    assert_eq!(p("GET /readyz\r\n\r\n"), Err(400));
    assert_eq!(p("GET /readyz HTTP/2\r\n\r\n"), Err(505));
    assert_eq!(p("GET readyz HTTP/1.1\r\n\r\n"), Err(400));
    assert_eq!(p("\r\n\r\n"), Err(400));
    assert_eq!(parse_head(b"GET /\xff HTTP/1.1\r\n\r\n"), Err(400));
}

async fn exchange(ctx: &Ctx, raw: &[u8]) -> String {
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    client.write_all(raw).await.unwrap();
    // Half-close: a request without the final blank line is still answered.
    client.shutdown().await.unwrap();
    let state = AtomicU8::new(SILENT);
    handle(server, ctx, &state).await.unwrap();
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    String::from_utf8(out).unwrap()
}

fn status(resp: &str) -> u16 {
    resp.split_whitespace().nth(1).unwrap().parse().unwrap()
}

fn body(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").unwrap().1
}

#[tokio::test]
async fn http_exchanges() {
    let m = Arc::new(Metrics::default());
    m.beat();
    Metrics::set(&m.gauges.starting, 1);
    let ctx = Ctx::new(m.clone(), Settings::default());

    let r = exchange(&ctx, b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert_eq!(status(&r), 200);
    assert!(
        r.contains("Connection: close\r\n") && r.contains("Content-Length: 3\r\n"),
        "{r}"
    );
    assert_eq!(body(&r), "ok\n");
    assert_eq!(
        status(&exchange(&ctx, b"GET /healthz HTTP/1.0\r\n\r\n").await),
        200
    );
    let r = exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await;
    assert_eq!(status(&r), 503);
    assert!(body(&r).starts_with("not ready: starting"), "{r}");
    // booting (e.g. waiting for the database): started, not ready
    let r = exchange(&ctx, b"GET /startupz?verbose HTTP/1.1\r\n\r\n").await;
    assert_eq!(status(&r), 200, "{r}");
    assert!(body(&r).contains("[+]phase ok: booting"), "{r}");

    // standby: alive + started, not ready unless ?standby=ok
    Metrics::set(&m.gauges.standby, 1);
    assert_eq!(
        status(&exchange(&ctx, b"GET /startupz HTTP/1.1\r\n\r\n").await),
        200
    );
    assert_eq!(
        status(&exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await),
        503
    );
    let r = exchange(&ctx, b"GET /readyz?standby=ok HTTP/1.1\r\n\r\n").await;
    assert_eq!((status(&r), body(&r)), (200, "ready (hot standby)\n"));

    // active, streaming, first build running
    Metrics::set(&m.gauges.standby, 0);
    Metrics::set(&m.gauges.starting, 0);
    Metrics::set(&m.gauges.streaming, 1);
    Metrics::set(&m.gauges.views, 1);
    let r = exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await;
    assert_eq!(status(&r), 503, "{r}");
    let r = exchange(&ctx, b"GET /readyz?standby=ok&building=ok HTTP/1.1\r\n\r\n").await;
    assert_eq!(
        (status(&r), body(&r)),
        (200, "ready; 1 view(s) building their first version\n")
    );
    Metrics::set(&m.gauges.views_ready, 1);
    let r = exchange(&ctx, b"GET /readyz HTTP/1.1\r\n\r\n").await;
    assert_eq!((status(&r), body(&r)), (200, "ready\n"));

    // HEAD: same status and headers, no body
    let r = exchange(&ctx, b"HEAD /readyz HTTP/1.1\r\n\r\n").await;
    assert_eq!(status(&r), 200);
    assert!(
        r.contains("Content-Length: 6\r\n") && r.ends_with("\r\n\r\n"),
        "{r:?}"
    );

    // JSON via Accept or ?format=json
    let r = exchange(
        &ctx,
        b"GET /readyz HTTP/1.1\r\nAccept: application/json\r\n\r\n",
    )
    .await;
    assert!(r.contains("Content-Type: application/json\r\n"), "{r}");
    let j: serde_json::Value = serde_json::from_str(body(&r)).unwrap();
    assert_eq!(
        (j["status"].as_str(), j["role"].as_str()),
        (Some("ok"), Some("active"))
    );
    let r = exchange(&ctx, b"GET /livez?format=json HTTP/1.1\r\n\r\n").await;
    assert!(
        serde_json::from_str::<serde_json::Value>(body(&r)).is_ok(),
        "{r}"
    );

    let r = exchange(&ctx, b"GET /metrics HTTP/1.1\r\n\r\n").await;
    assert!(
        r.contains("Content-Type: text/plain; version=0.0.4")
            && body(&r).contains("lazyview_ready 1\n"),
        "{r}"
    );
    assert_eq!(
        status(&exchange(&ctx, b"GET / HTTP/1.1\r\n\r\n").await),
        200
    );
    assert_eq!(
        status(&exchange(&ctx, b"GET /nope HTTP/1.1\r\n\r\n").await),
        404
    );
    let r = exchange(&ctx, b"POST /readyz HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await;
    assert_eq!(status(&r), 405);
    assert!(r.contains("Allow: GET, HEAD\r\n"), "{r}");
    assert_eq!(
        status(&exchange(&ctx, b"BREW /coffee HTTP/1.1\r\n\r\n").await),
        404
    );
    assert_eq!(status(&exchange(&ctx, b"garbage\r\n\r\n").await), 400);
    // request line only, then the client half-closes: still answered
    assert_eq!(
        status(&exchange(&ctx, b"GET /livez HTTP/1.0\r\n").await),
        200
    );
    let mut big = b"GET /livez HTTP/1.1\r\nX: ".to_vec();
    big.extend(std::iter::repeat_n(b'a', MAX_HEAD + 10));
    big.extend_from_slice(b"\r\n\r\n");
    assert_eq!(status(&exchange(&ctx, &big).await), 431);
    // connection closed without a request: no response, no error
    assert_eq!(exchange(&ctx, b"").await, "");
}

#[tokio::test]
async fn slow_client_gets_408() {
    let ctx = Ctx {
        head_timeout: Duration::from_millis(200),
        ..Ctx::new(Arc::new(Metrics::default()), Settings::default())
    };
    let (mut client, server) = tokio::io::duplex(1024);
    client
        .write_all(b"GET /livez HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    let h = tokio::spawn(async move {
        let state = AtomicU8::new(SILENT);
        let r = handle(server, &ctx, &state).await;
        (r, state.load(Ordering::Relaxed))
    });
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    let (r, state) = h.await.unwrap();
    r.unwrap();
    assert_eq!(state, ANSWERING);
    assert!(String::from_utf8(out).unwrap().starts_with("HTTP/1.1 408 "));
}

fn entry(ip: &str, age_ms: u64, state: u8, now: Instant) -> ConnEntry {
    ConnEntry {
        ip: ip.parse().unwrap(),
        accepted: now - Duration::from_millis(age_ms),
        state: Arc::new(AtomicU8::new(state)),
        _cancel: oneshot::channel().0,
    }
}

#[test]
fn eviction_picks_idle_connections_of_the_heaviest_client() {
    let now = Instant::now() + Duration::from_secs(10);
    let table = |entries: Vec<ConnEntry>| ConnTable {
        next: entries.len() as u64,
        conns: entries
            .into_iter()
            .enumerate()
            .map(|(i, e)| (i as u64, e))
            .collect(),
        last_warn: None,
    };
    // node (kubelet) with a fresh connection, a pod hogging idle ones
    let t = table(vec![
        entry("10.0.0.1", 5, SILENT, now),    // 0 kubelet, just connected
        entry("10.1.0.9", 1500, SILENT, now), // 1 hog, oldest
        entry("10.1.0.9", 900, SILENT, now),  // 2 hog
        entry("10.0.0.1", 3000, ANSWERING, now), // 3 being answered
    ]);
    assert_eq!(t.victim(now), Some(1));
    // idle (silent past the grace) beats a busier client whose connections are all fresh
    let t = table(vec![
        entry("10.0.0.1", 5, SILENT, now),
        entry("10.0.0.1", 10, SILENT, now),
        entry("10.0.0.1", 20, RECEIVING, now),
        entry("10.2.0.1", 200, SILENT, now), // 3
    ]);
    assert_eq!(t.victim(now), Some(3));
    // among idle ones: the address with the most connections first, then the oldest
    let t = table(vec![
        entry("10.2.0.1", 5000, SILENT, now),
        entry("10.3.0.1", 300, SILENT, now), // 1
        entry("10.3.0.1", 400, SILENT, now), // 2
    ]);
    assert_eq!(t.victim(now), Some(2));
    // partial heads are dropped before fresh silent ones, never a response being written
    let t = table(vec![
        entry("10.0.0.1", 5, SILENT, now),
        entry("10.4.0.1", 50, RECEIVING, now), // 1
        entry("10.4.0.1", 5000, ANSWERING, now),
    ]);
    assert_eq!(t.victim(now), Some(1));
    let t = table(vec![entry("10.0.0.1", 5000, ANSWERING, now)]);
    assert_eq!(t.victim(now), None);
}

async fn get_livez(addr: SocketAddr) -> Result<(Duration, String)> {
    let t0 = Instant::now();
    let mut s = TcpStream::connect(addr).await?;
    s.write_all(b"GET /livez HTTP/1.1\r\nHost: x\r\n\r\n")
        .await?;
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out)).await??;
    Ok((t0.elapsed(), String::from_utf8_lossy(&out).to_string()))
}

/// Idle connections beyond the limit (from this and other addresses) never delay a probe.
#[tokio::test]
async fn idle_connections_do_not_starve_probes() {
    let m = Arc::new(Metrics::default());
    m.beat();
    let addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let ctx = Arc::new(Ctx {
        max_conns: 16,
        head_timeout: Duration::from_secs(30),
        ..Ctx::new(m, Settings::default())
    });
    let server = tokio::spawn(serve_ctx(addr, ctx.clone()));
    let mut up = false;
    for _ in 0..50 {
        if probe(addr).await {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(up);
    // 40 idle connections from 127.0.0.1 and 40 from 127.0.0.2 (limit 16)
    let mut idle = Vec::new();
    for i in 0..80 {
        let sock = tokio::net::TcpSocket::new_v4().unwrap();
        let src: IpAddr = if i % 2 == 0 {
            [127, 0, 0, 1]
        } else {
            [127, 0, 0, 2]
        }
        .into();
        sock.bind(SocketAddr::new(src, 0)).unwrap();
        idle.push(sock.connect(addr).await.unwrap());
    }
    tokio::time::sleep(SILENT_GRACE * 2).await;
    for _ in 0..10 {
        let (took, resp) = get_livez(addr).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 "), "{resp:?}");
        assert!(took < Duration::from_secs(1), "probe took {took:?}");
    }
    assert!(lock(&ctx.conns).conns.len() <= 16);
    // the oldest idle connections were closed by the server
    let mut closed = 0;
    for s in &mut idle {
        let mut b = [0u8; 1];
        if let Ok(Ok(0)) = tokio::time::timeout(Duration::from_millis(20), s.read(&mut b)).await {
            closed += 1;
        }
    }
    assert!(closed >= 60, "only {closed} idle connections were closed");
    server.abort();
}

#[tokio::test]
async fn tcp_serve_bind_and_probe() {
    let m = Arc::new(Metrics::default());
    m.beat();
    // Port in use: serve keeps retrying instead of failing.
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = blocker.local_addr().unwrap();
    let server = tokio::spawn(serve_with(addr, m.clone(), Settings::default()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!server.is_finished());
    drop(blocker);
    let mut up = false;
    for _ in 0..100 {
        if probe(addr).await {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        up,
        "health endpoint did not come up after the port was freed"
    );
    assert!(!probe_path(addr, "/readyz").await);
    // `0.0.0.0` is probed through loopback.
    assert!(probe(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), addr.port())).await);
    server.abort();
    let _ = server.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !probe(addr).await,
        "listener must stop with the serve future"
    );
}
