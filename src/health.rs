//! Minimal HTTP endpoint for container health checks and Prometheus scraping:
//! `GET /healthz` (process alive), `GET /readyz` (streaming, every view live, none failing),
//! `GET /metrics` (Prometheus text format). No shell or extra tooling is needed in the image:
//! `lazyview healthcheck` queries `/healthz` and sets the exit code.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::metrics::{unix_now, Metrics};

/// Seconds without an engine-loop iteration after which `/healthz` fails.
const STALL_SECS: u64 = 120;

pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!("health endpoint disabled: cannot listen on {addr}: {e} (set LAZYVIEW_HEALTH_ADDR)");
            return;
        }
    };
    info!("health endpoint on http://{addr} (/healthz /readyz /metrics)");
    loop {
        let Ok((sock, _)) = listener.accept().await else { continue };
        let m = metrics.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle(sock, m)).await;
        });
    }
}

async fn handle(mut sock: TcpStream, m: Arc<Metrics>) -> std::io::Result<()> {
    let mut buf = vec![0u8; 4096];
    let mut n = 0;
    while n < buf.len() {
        let r = sock.read(&mut buf[n..]).await?;
        if r == 0 {
            break;
        }
        n += r;
        if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || buf[..n].contains(&b'\n') {
            break;
        }
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req.split_whitespace().nth(1).unwrap_or("/");
    let (status, body) = match path {
        "/healthz" | "/livez" => {
            if alive(&m) {
                (200, "ok\n".to_string())
            } else {
                (503, "engine loop stalled\n".to_string())
            }
        }
        "/readyz" => match readiness(&m) {
            Ok(()) => (200, "ready\n".to_string()),
            Err(why) => (503, format!("{why}\n")),
        },
        "/metrics" => (200, prometheus(&m)),
        _ => (404, "not found\n".to_string()),
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(resp.as_bytes()).await?;
    sock.shutdown().await
}

fn alive(m: &Metrics) -> bool {
    unix_now().saturating_sub(Metrics::get(&m.gauges.heartbeat_unix)) < STALL_SECS
}

fn readiness(m: &Metrics) -> Result<(), String> {
    let g = &m.gauges;
    if !alive(m) {
        return Err("engine loop stalled".into());
    }
    if Metrics::get(&g.streaming) == 0 {
        return Err("not streaming the binlog yet".into());
    }
    let (views, ready, err) = (Metrics::get(&g.views), Metrics::get(&g.views_ready), Metrics::get(&g.views_error));
    if err > 0 {
        return Err(format!("{err} view(s) in error state (see _lazyview_state)"));
    }
    if ready < views {
        return Err(format!("{}/{views} views still building", views - ready));
    }
    Ok(())
}

fn prometheus(m: &Metrics) -> String {
    let mut out = String::new();
    for (name, v) in m.counters() {
        out.push_str(&format!("# TYPE lazyview_{name}_total counter\nlazyview_{name}_total {v}\n"));
    }
    let g = &m.gauges;
    for (name, v) in [
        ("views", &g.views),
        ("views_ready", &g.views_ready),
        ("views_building", &g.views_building),
        ("views_error", &g.views_error),
        ("streaming", &g.streaming),
    ] {
        out.push_str(&format!("# TYPE lazyview_{name} gauge\nlazyview_{name} {}\n", Metrics::get(v)));
    }
    if let Some(p) = m.applied() {
        out.push_str(&format!(
            "# TYPE lazyview_applied_binlog_pos gauge\nlazyview_applied_binlog_pos{{file=\"{}\"}} {}\n",
            p.file, p.pos
        ));
    }
    out
}

/// `lazyview healthcheck`: exit status 0 when `/healthz` answers 200.
pub async fn probe(addr: SocketAddr) -> bool {
    let target: SocketAddr = if addr.ip().is_unspecified() {
        SocketAddr::new(if addr.is_ipv4() { [127, 0, 0, 1].into() } else { std::net::Ipv6Addr::LOCALHOST.into() }, addr.port())
    } else {
        addr
    };
    let attempt = async {
        let mut s = TcpStream::connect(target).await?;
        s.write_all(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n").await?;
        let mut resp = Vec::new();
        s.read_to_end(&mut resp).await?;
        Ok::<_, std::io::Error>(resp.starts_with(b"HTTP/1.1 200"))
    };
    matches!(tokio::time::timeout(Duration::from_secs(3), attempt).await, Ok(Ok(true)))
}
