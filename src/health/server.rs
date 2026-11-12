// ------------------------------------------------------------------------------------ HTTP

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::metrics::Metrics;

use super::probes::{render_json, render_text};
use super::prometheus::prometheus;
use super::{evaluate, parse_bool, Probe, ReadyOpts, Settings, Tracker};

const HEAD_TIMEOUT: Duration = Duration::from_secs(2);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HEAD: usize = 8 * 1024;
const MAX_CONNECTIONS: usize = 512;
/// A connection that sent nothing for this long is idle (a probe sends its request at once).
const SILENT_GRACE: Duration = Duration::from_millis(100);
/// Accept queue length (the kernel caps it at `net.core.somaxconn`).
const LISTEN_BACKLOG: u32 = 4096;
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
const REBIND_EVERY: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const BUSY: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    head_only: bool,
    method: String,
    path: String,
    query: Vec<(String, String)>,
    accept_json: bool,
}

impl Request {
    /// `Some(true)` for `?key`, `?key=1|true|yes|on|ok|ready`, `Some(false)` for `0|false|no|off`.
    fn flag(&self, key: &str) -> Option<bool> {
        let (_, v) = self.query.iter().rev().find(|(k, _)| k == key)?;
        Some(
            matches!(v.to_ascii_lowercase().as_str(), "" | "ok" | "ready")
                || parse_bool(v).unwrap_or(true),
        )
    }

    fn wants_json(&self) -> bool {
        match self
            .query
            .iter()
            .rev()
            .find(|(k, _)| k == "format")
            .map(|(_, v)| v.to_ascii_lowercase())
        {
            Some(f) => f == "json",
            None => self.accept_json,
        }
    }
}

/// Parses a request head (request line + headers). `Err` carries the status to answer with.
fn parse_head(head: &[u8]) -> std::result::Result<Request, u16> {
    let text = std::str::from_utf8(head).map_err(|_| 400u16)?;
    let mut lines = text.split('\n').map(|l| l.strip_suffix('\r').unwrap_or(l));
    let line = lines.next().unwrap_or("");
    let mut parts = line.split(' ').filter(|p| !p.is_empty());
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(400);
    };
    if !version.starts_with("HTTP/1.") {
        return Err(if version.starts_with("HTTP/") {
            505
        } else {
            400
        });
    }
    // absolute-form (`GET http://host:8080/readyz HTTP/1.1`, sent through proxies)
    let target = match target.split_once("://") {
        Some((_, rest)) if !target.starts_with('/') => match rest.find('/') {
            Some(i) => &rest[i..],
            None => "/",
        },
        _ => target,
    };
    if !target.starts_with('/') {
        return Err(400);
    }
    let target = target.split('#').next().unwrap_or(target);
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = match path.trim_end_matches('/') {
        "" => "/",
        p => p,
    };
    let query = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k.to_ascii_lowercase(), v.to_string())
        })
        .collect();
    let accept_json = lines.any(|h| {
        h.split_once(':')
            .map(|(k, v)| {
                k.trim().eq_ignore_ascii_case("accept")
                    && v.to_ascii_lowercase().contains("application/json")
            })
            .unwrap_or(false)
    });
    Ok(Request {
        head_only: method == "HEAD",
        method: method.to_string(),
        path: path.to_string(),
        query,
        accept_json,
    })
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        _ => "Error",
    }
}

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

struct Response {
    status: u16,
    content_type: &'static str,
    body: String,
    allow: bool,
}

impl Response {
    fn text(status: u16, body: impl Into<String>) -> Response {
        Response {
            status,
            content_type: TEXT,
            body: body.into(),
            allow: false,
        }
    }

    fn bytes(&self, head_only: bool) -> Vec<u8> {
        let mut out = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
            self.status,
            reason(self.status),
            self.content_type,
            self.body.len()
        );
        if self.allow {
            out.push_str("Allow: GET, HEAD\r\n");
        }
        out.push_str("\r\n");
        if !head_only {
            out.push_str(&self.body);
        }
        out.into_bytes()
    }
}

const INDEX: &str = "lazyview health endpoint\n\
/livez      liveness (alias /healthz): the engine loop is making progress\n\
/startupz   startup: same verdict as /livez, ?verbose shows the boot phase\n\
/readyz     readiness: active, streaming, every view live\n\
            (?standby=ok counts a hot standby as ready, ?building=ok ignores first builds,\n\
             ?require_all=1 fails on views in error)\n\
/metrics    Prometheus metrics\n\
/stats      per-view update statistics (JSON)\n\
?verbose lists every check; ?format=json (or Accept: application/json) returns JSON\n";

// Connection states (what eviction may drop).
const SILENT: u8 = 0;
const RECEIVING: u8 = 1;
const ANSWERING: u8 = 2;

struct ConnEntry {
    ip: IpAddr,
    accepted: Instant,
    state: Arc<AtomicU8>,
    /// Dropping it (eviction) ends the connection's task.
    _cancel: oneshot::Sender<()>,
}

#[derive(Default)]
struct ConnTable {
    next: u64,
    conns: HashMap<u64, ConnEntry>,
    last_warn: Option<Instant>,
}

impl ConnTable {
    /// The connection to drop so that a new one fits: never one being answered; first one that
    /// sent nothing for `SILENT_GRACE`, from the address holding the most connections, oldest
    /// first (a kubelet probe sends its request at once and is never the oldest silent one).
    fn victim(&self, now: Instant) -> Option<u64> {
        let mut per_ip: HashMap<IpAddr, usize> = HashMap::new();
        for e in self.conns.values() {
            *per_ip.entry(e.ip).or_default() += 1;
        }
        self.conns
            .iter()
            .filter(|(_, e)| e.state.load(Ordering::Relaxed) != ANSWERING)
            .max_by_key(|(id, e)| {
                let age = now.saturating_duration_since(e.accepted);
                let idle = e.state.load(Ordering::Relaxed) == SILENT && age >= SILENT_GRACE;
                (idle, per_ip[&e.ip], age, std::cmp::Reverse(**id))
            })
            .map(|(id, _)| *id)
    }
}

/// A registered connection; dropping it unregisters it.
struct Slot {
    ctx: Arc<Ctx>,
    id: u64,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.ctx.table().conns.remove(&self.id);
    }
}

struct Ctx {
    metrics: Arc<Metrics>,
    settings: Settings,
    tracker: Tracker,
    head_timeout: Duration,
    max_conns: usize,
    conns: Mutex<ConnTable>,
}

impl Ctx {
    fn new(metrics: Arc<Metrics>, settings: Settings) -> Ctx {
        Ctx {
            metrics,
            settings,
            tracker: Tracker::new(),
            head_timeout: HEAD_TIMEOUT,
            max_conns: MAX_CONNECTIONS,
            conns: Mutex::new(ConnTable::default()),
        }
    }

    fn table(&self) -> std::sync::MutexGuard<'_, ConnTable> {
        self.conns.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers a new connection, evicting an idle one at the limit. `None`: every open
    /// connection is being answered; refuse this one.
    fn admit(self: &Arc<Self>, ip: IpAddr) -> Option<(Slot, Arc<AtomicU8>, oneshot::Receiver<()>)> {
        let mut t = self.table();
        if t.conns.len() >= self.max_conns {
            let now = Instant::now();
            let victim = t.victim(now)?;
            let e = t.conns.remove(&victim)?;
            if t.last_warn
                .is_none_or(|w| now.duration_since(w) >= Duration::from_secs(60))
            {
                t.last_warn = Some(now);
                warn!(
                    "health endpoint: {} connections open; closing idle ones (from {}), probes are not affected",
                    self.max_conns, e.ip
                );
            }
            debug!(peer = %e.ip, "health endpoint: evicted an idle connection");
        }
        let id = t.next;
        t.next += 1;
        let (tx, rx) = oneshot::channel();
        let state = Arc::new(AtomicU8::new(SILENT));
        t.conns.insert(
            id,
            ConnEntry {
                ip,
                accepted: Instant::now(),
                state: state.clone(),
                _cancel: tx,
            },
        );
        Some((
            Slot {
                ctx: self.clone(),
                id,
            },
            state,
            rx,
        ))
    }

    fn respond(&self, req: &Request) -> Response {
        let probe = match req.path.as_str() {
            "/livez" | "/healthz" => Some(Probe::Live),
            "/readyz" => Some(Probe::Ready),
            "/startupz" => Some(Probe::Startup),
            "/metrics" | "/" | "/stats" => None,
            _ => return Response::text(404, "not found\n"),
        };
        if req.method != "GET" && req.method != "HEAD" {
            return Response {
                allow: true,
                ..Response::text(405, "only GET and HEAD are supported\n")
            };
        }
        if req.path == "/stats" {
            return Response {
                status: 200,
                content_type: JSON,
                body: format!("{}\n", self.metrics.view_stats.snapshot().to_json()),
                allow: false,
            };
        }
        let o = self.tracker.observe(&self.metrics);
        let Some(probe) = probe else {
            return if req.path == "/" {
                Response::text(200, INDEX)
            } else {
                Response {
                    status: 200,
                    content_type: PROMETHEUS,
                    body: prometheus(&self.metrics, &o, &self.settings),
                    allow: false,
                }
            };
        };
        let opts = ReadyOpts {
            standby_ok: req.flag("standby").unwrap_or(false),
            building_ok: req.flag("building").unwrap_or(false),
            require_all: req.flag("require_all"),
        };
        let r = evaluate(probe, &o, &self.settings, opts);
        let status = if r.ok { 200 } else { 503 };
        if req.wants_json() {
            Response {
                status,
                content_type: JSON,
                body: render_json(&r, &o, &self.settings),
                allow: false,
            }
        } else {
            Response::text(
                status,
                render_text(&r, req.flag("verbose").unwrap_or(false)),
            )
        }
    }
}

enum Head {
    Complete(Vec<u8>),
    TooLarge,
    Closed,
}

/// Reads up to the blank line ending the request head (the request body, if any, is ignored).
async fn read_head<S: AsyncRead + Unpin>(sock: &mut S, state: &AtomicU8) -> io::Result<Head> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    loop {
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            // Peer closed (or half-closed) before the blank line: answer what we have, if anything.
            return Ok(if buf.is_empty() {
                Head::Closed
            } else {
                Head::Complete(buf)
            });
        }
        state.store(RECEIVING, Ordering::Relaxed);
        let from = buf.len().saturating_sub(3);
        buf.extend_from_slice(&chunk[..n]);
        let tail = &buf[from..];
        let end = tail
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|i| i + 4)
            .or_else(|| tail.windows(2).position(|w| w == b"\n\n").map(|i| i + 2));
        if let Some(e) = end {
            buf.truncate(from + e);
            return Ok(if buf.len() > MAX_HEAD {
                Head::TooLarge
            } else {
                Head::Complete(buf)
            });
        }
        if buf.len() > MAX_HEAD {
            return Ok(Head::TooLarge);
        }
    }
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut sock: S,
    ctx: &Ctx,
    state: &AtomicU8,
) -> io::Result<()> {
    let head = tokio::time::timeout(ctx.head_timeout, read_head(&mut sock, state)).await;
    state.store(ANSWERING, Ordering::Relaxed);
    let (resp, head_only) = match head {
        Err(_) => (
            Response::text(408, "request head not received in time\n"),
            false,
        ),
        Ok(Err(e)) => return Err(e),
        Ok(Ok(Head::Closed)) => return Ok(()),
        Ok(Ok(Head::TooLarge)) => (Response::text(431, "request head too large\n"), false),
        Ok(Ok(Head::Complete(head))) => match parse_head(&head) {
            Ok(req) => (ctx.respond(&req), req.head_only),
            Err(status) => (Response::text(status, "bad request\n"), false),
        },
    };
    let bytes = resp.bytes(head_only);
    let write = async {
        sock.write_all(&bytes).await?;
        sock.flush().await?;
        sock.shutdown().await
    };
    match tokio::time::timeout(WRITE_TIMEOUT, write).await {
        Ok(r) => r,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "response write timed out",
        )),
    }
}

/// Accepts without ever waiting for a free slot (a full table evicts an idle connection), so a
/// probe never queues behind idle connections in the listen backlog.
async fn accept_loop(listener: TcpListener, ctx: Arc<Ctx>) {
    let mut conns = JoinSet::new();
    loop {
        while conns.try_join_next().is_some() {}
        match listener.accept().await {
            Ok((sock, peer)) => {
                let Some((slot, state, cancel)) = ctx.admit(peer.ip().to_canonical()) else {
                    // Every slot is answering a request right now: refuse without blocking.
                    let _ = sock.try_write(BUSY);
                    continue;
                };
                let _ = sock.set_nodelay(true);
                let ctx = ctx.clone();
                conns.spawn(async move {
                    tokio::select! {
                        r = handle(sock, &ctx, &state) => if let Err(e) = r {
                            debug!(%peer, "health request failed: {e}");
                        },
                        _ = cancel => {}
                    }
                    drop(slot);
                });
            }
            Err(e) => {
                // EMFILE / ENFILE / ECONNABORTED …: back off instead of spinning.
                debug!("health endpoint accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// One listener with a deep accept backlog (a connection flood overflows it later).
fn listen(addr: SocketAddr) -> io::Result<TcpListener> {
    let s = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    s.set_reuseaddr(true)?;
    s.bind(addr)?;
    s.listen(LISTEN_BACKLOG)
}

/// Unspecified addresses listen on both IPv6 (`[::]`, dual-stack where the kernel allows) and
/// IPv4 (`0.0.0.0`), so the probes work in IPv4, IPv6 and dual-stack clusters alike.
async fn bind(addr: SocketAddr) -> io::Result<Vec<TcpListener>> {
    if !addr.ip().is_unspecified() || addr.port() == 0 {
        return Ok(vec![listen(addr)?]);
    }
    let mut out = Vec::new();
    let mut first_err = None;
    for a in [
        SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), addr.port()),
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), addr.port()),
    ] {
        match listen(a) {
            Ok(l) => out.push(l),
            // `[::]` is dual-stack (net.ipv6.bindv6only=0) and already accepts IPv4.
            Err(e) if a.is_ipv4() && !out.is_empty() && e.kind() == io::ErrorKind::AddrInUse => {}
            Err(e) => {
                debug!("health endpoint: cannot listen on {a}: {e}");
                first_err.get_or_insert(e);
            }
        }
    }
    match (out.is_empty(), first_err) {
        (false, _) => Ok(out),
        (true, Some(e)) => Err(e),
        (true, None) => Err(io::Error::other("no address to listen on")),
    }
}

/// Serves the endpoint until the future is dropped (policy from the environment; an invalid
/// variable falls back to its own default and is named in a warning).
pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) {
    let (settings, errors) = Settings::from_lookup_lenient(|k| std::env::var(k).ok());
    for e in errors {
        warn!("health settings: {e}");
    }
    serve_with(addr, metrics, settings).await
}

/// Serves the endpoint until the future is dropped. A failed bind is retried every 5 s; it
/// never stops the engine. Dropping the future stops the listeners and open connections.
pub async fn serve_with(addr: SocketAddr, metrics: Arc<Metrics>, settings: Settings) {
    serve_ctx(addr, Arc::new(Ctx::new(metrics, settings))).await
}

async fn serve_ctx(addr: SocketAddr, ctx: Arc<Ctx>) {
    let mut listeners = JoinSet::new();
    let mut bound = false;
    let mut warned = false;
    let mut next_bind = tokio::time::Instant::now();
    loop {
        if !bound && tokio::time::Instant::now() >= next_bind {
            match bind(addr).await {
                Ok(ls) => {
                    let shown: Vec<String> = ls
                        .iter()
                        .filter_map(|l| l.local_addr().ok())
                        .map(|a| match a.ip() {
                            // dual-stack socket (bindv6only=0): also accepts IPv4
                            IpAddr::V6(ip) if ip.is_unspecified() && ls.len() == 1 => {
                                format!("http://{a} (IPv4 and IPv6)")
                            }
                            _ => format!("http://{a}"),
                        })
                        .collect();
                    let s = &ctx.settings;
                    info!(
                        "health endpoint on {} (/livez /readyz /startupz /metrics; stall {}s, {}s while booting, require_all {}, disconnect grace {}s)",
                        shown.join(" and "),
                        s.stall_secs,
                        s.boot_stall_secs,
                        s.require_all,
                        s.disconnect_grace_secs
                    );
                    for l in ls {
                        listeners.spawn(accept_loop(l, ctx.clone()));
                    }
                    bound = true;
                }
                Err(e) => {
                    if !warned {
                        warn!("health endpoint: cannot listen on {addr}: {e}; retrying every {REBIND_EVERY:?} (set LAZYVIEW_HEALTH_ADDR)");
                        warned = true;
                    }
                    next_bind = tokio::time::Instant::now() + REBIND_EVERY;
                }
            }
        }
        ctx.tracker.observe(&ctx.metrics);
        tokio::time::sleep(SAMPLE_EVERY).await;
    }
}

/// `lazyview healthcheck`: true when `/livez` answers 200.
pub async fn probe(addr: SocketAddr) -> bool {
    probe_path(addr, "/livez").await
}

/// True when `GET path` on the local endpoint answers 200 within 3 s. Unspecified addresses are
/// reached through loopback (IPv4 first for `0.0.0.0`, IPv6 first for `[::]`).
pub async fn probe_path(addr: SocketAddr, path: &str) -> bool {
    let targets: Vec<SocketAddr> = if addr.ip().is_unspecified() {
        let v4 = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), addr.port());
        let v6 = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), addr.port());
        if addr.is_ipv4() {
            vec![v4, v6]
        } else {
            vec![v6, v4]
        }
    } else {
        vec![addr]
    };
    let attempt = async {
        for t in targets {
            match status_of(t, path).await {
                Ok(code) => return code == 200,
                Err(e) => debug!("healthcheck {t}{path}: {e}"),
            }
        }
        false
    };
    tokio::time::timeout(PROBE_TIMEOUT, attempt)
        .await
        .unwrap_or(false)
}

async fn status_of(addr: SocketAddr, path: &str) -> io::Result<u16> {
    let mut s = TcpStream::connect(addr).await?;
    s.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await?;
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await?;
    let line = resp.split(|b| *b == b'\n').next().unwrap_or(&[]);
    std::str::from_utf8(line)
        .ok()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not an HTTP response"))
}

#[cfg(test)]
mod tests;
