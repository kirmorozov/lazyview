//! The Toxiproxy client the chaos suite arms network faults with (plan
//! `tmp/out/lvs-chaos/plan/breakdown.md` §2, findings F1–F3).
//!
//! One daemon serves the whole host (`ghcr.io/shopify/toxiproxy:2.13.0`, the `chaos` compose
//! profile of T1), its API published on loopback only. The suite is opt-in, in the shape of
//! `LAZYVIEW_TEST_EXCLUSIVE`: `LAZYVIEW_TEST_TOXI=<api addr>` (CI: `127.0.0.1:18474`); unset, every
//! chaos test prints the harness's own [`Ctx::skip`] line and passes, so the jobs that have no
//! container never run them. The harness starts no container (F9): CI's chaos job or
//! `scripts/test-servers.sh compose up --chaos toxiproxy <flavor>` does.
//!
//! The rules this module exists to enforce:
//! * **no new dependency** (F9): the API speaks plain HTTP/1.1 over one short-lived connection, so
//!   it uses [`http_json`] and `serde_json`, both already in the harness;
//! * **every proxy is `lv_<schema>_<role>`** and is deleted by [`Cleanup`], which is created
//!   *before* the first proxy — a panic, a `run_timeout()` trip or an early `?` must not be able to
//!   leave a black hole armed on a server every suite shares (§2.2, the safety property of the plan);
//! * **`POST /reset` is global** (F1): it clears every proxy's toxics on the host, so only the
//!   self-tests may call it, never a scenario — `scenario()` runs every flavor in parallel against
//!   this one daemon;
//! * **no `sleep`, no fixed pause** (§2.5): a fault is armed and then *proven to have landed* by a
//!   bounded wait on an observable ([`wait_until`], [`read_up_to`]), never by giving it a moment.

use super::*;
use std::sync::atomic::AtomicU16;
use std::sync::MutexGuard;

use anyhow::ensure;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The API address of the host's daemon (`LAZYVIEW_TEST_TOXI`, e.g. `127.0.0.1:18474`); `None` when
/// unset or empty, which is how the whole suite opts out (§2.2).
pub fn endpoint() -> Option<String> {
    std::env::var("LAZYVIEW_TEST_TOXI")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// The line a skipped chaos test prints: what to set and how to get the container.
fn opt_in_hint() -> String {
    "LAZYVIEW_TEST_TOXI is not set (chaos tests: export LAZYVIEW_TEST_TOXI=127.0.0.1:18474 and \
     start the daemon with `docker compose -f docker-compose.test.yml --profile chaos up -d \
     --wait toxiproxy`, or `scripts/test-servers.sh compose up --chaos toxiproxy <flavor>`)"
        .to_string()
}

/// Opens the handle when the suite is opted in: [`Ctx::skip`] + `None` when `LAZYVIEW_TEST_TOXI` is
/// unset, so every chaos test starts with
/// `let Some(toxi) = toxi::open_if_enabled(&ctx).await? else { return Ok(()) };`.
pub async fn open_if_enabled(ctx: &Ctx) -> Result<Option<Toxi>> {
    if endpoint().is_none() {
        ctx.skip(&opt_in_hint());
        return Ok(None);
    }
    Ok(Some(Toxi::open(ctx).await?))
}

/// The host the container dials to reach this test process: the `extra_hosts` line of the `chaos`
/// profile maps `host.docker.internal` to the docker bridge gateway. Override it with
/// `LAZYVIEW_TEST_TOXI_HOST` when the host is reachable under another name.
fn host_for_container() -> String {
    std::env::var("LAZYVIEW_TEST_TOXI_HOST")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "host.docker.internal".to_string())
}

/// The upstream a source/target proxy dials by default: the flavor's compose service name
/// (`<flavor>:3306`, §2.4). `LAZYVIEW_TEST_TOXI_UPSTREAM=<host:port>` overrides it for the tarball
/// mode of `scripts/test-servers.sh`, whose servers run on the host (F8/D4), e.g.
/// `host.docker.internal:33084`.
pub fn upstream(flavor: &str) -> String {
    std::env::var("LAZYVIEW_TEST_TOXI_UPSTREAM")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("{flavor}:3306"))
}

/// One chaos test at a time in this process. They share the host's single daemon: the port band,
/// and above all the global `POST /reset` (F1), must not interleave with another test's toxic —
/// one test's reset would clear the other's fault and make it look like it never landed. Held for
/// the whole body of every test in `tests/chaos.rs` (which the suite also runs `--test-threads=2`).
static TOXI_LOCK: Mutex<()> = Mutex::new(());

pub fn exclusive() -> MutexGuard<'static, ()> {
    TOXI_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------------------------
// Port band (§1 "Port allocation")
// ---------------------------------------------------------------------------------------------

/// The listen band the `chaos` compose profile publishes on loopback (T1).
const PORT_LOW: u16 = 17100;
const PORT_HIGH: u16 = 17199;
/// How many ports one proxy creation tries before giving up (an "address already in use" from a
/// proxy a crashed run left behind moves on, bounded).
const PORT_TRIES: usize = 20;
static NEXT_PORT: AtomicU16 = AtomicU16::new(PORT_LOW);

/// The next port in the band, wrapping.
fn next_port() -> u16 {
    let n = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    PORT_LOW + n.wrapping_sub(PORT_LOW) % (PORT_HIGH - PORT_LOW + 1)
}

// ---------------------------------------------------------------------------------------------
// Toxic vocabulary (F2)
// ---------------------------------------------------------------------------------------------

/// Which half of a proxied connection a directional toxic sits on (F2: `stream` `downstream` is
/// server→client, `upstream` is client→server; a symmetric RTT needs one toxic per direction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Downstream,
    Upstream,
}

impl Dir {
    pub fn as_str(self) -> &'static str {
        match self {
            Dir::Downstream => "downstream",
            Dir::Upstream => "upstream",
        }
    }
}

static TOXIC_SEQ: AtomicU32 = AtomicU32::new(0);

/// One toxic as the API sees it (F2: `name`, `type`, `stream`, `toxicity`, `attributes`). Built by
/// the constructors below, read back by [`Toxi::toxics`] (the self-tests assert what they armed).
/// `toxicity` is always 1.0 here: every scenario is deterministic, randomness is T7's job.
#[derive(Debug, Clone, PartialEq)]
pub struct Toxic {
    pub name: String,
    pub kind: String,
    pub stream: String,
    pub toxicity: f64,
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

impl Toxic {
    fn new(kind: &str, dir: Dir, attrs: &[(&str, serde_json::Value)]) -> Toxic {
        let mut attributes = serde_json::Map::new();
        for (k, v) in attrs {
            attributes.insert((*k).to_string(), v.clone());
        }
        Toxic {
            name: format!("{kind}_{}", TOXIC_SEQ.fetch_add(1, Ordering::Relaxed)),
            kind: kind.to_string(),
            stream: dir.as_str().to_string(),
            toxicity: 1.0,
            attributes,
        }
    }

    /// Pins the name so the test can [`Toxi::remove_toxic`] it by name (names are otherwise unique
    /// per process).
    pub fn named(mut self, name: &str) -> Toxic {
        self.name = name.to_string();
        self
    }

    /// Moves the toxic to the other half of the connection (the two builders below start on
    /// `downstream`, which is the direction a cut is usually aimed at).
    pub fn on(mut self, dir: Dir) -> Toxic {
        self.stream = dir.as_str().to_string();
        self
    }

    /// `reset_peer{timeout}`: a TCP RST on the stream, after `timeout` ms (0 = at once). On 2.13.0
    /// the RST reaches the **upstream** peer — the server sees `Connection reset by peer` — while
    /// the client's side ends at EOF; `selftest_toxi_reset_peer` pins exactly that.
    pub fn reset_peer(timeout_ms: u64) -> Toxic {
        Toxic::new(
            "reset_peer",
            Dir::Downstream,
            &[("timeout", timeout_ms.into())],
        )
    }

    /// `timeout{timeout}`: stops all data and closes the stream after `timeout` ms. `0` is the
    /// black hole that never closes (F2) — only a toxic removal or a proxy stop ends it.
    pub fn timeout(timeout_ms: u64) -> Toxic {
        Toxic::new(
            "timeout",
            Dir::Downstream,
            &[("timeout", timeout_ms.into())],
        )
    }

    /// `latency{latency,jitter}` in ms, one direction (F2: symmetric RTT needs one per direction).
    pub fn latency(latency_ms: u64, jitter_ms: u64, dir: Dir) -> Toxic {
        Toxic::new(
            "latency",
            dir,
            &[("latency", latency_ms.into()), ("jitter", jitter_ms.into())],
        )
    }

    /// `bandwidth{rate}` in KB/s, one direction.
    pub fn bandwidth(rate_kbps: u64, dir: Dir) -> Toxic {
        Toxic::new("bandwidth", dir, &[("rate", rate_kbps.into())])
    }

    /// `slicer{average_size,size_variation,delay}`: chops the stream into slices of ~`avg` bytes
    /// (`±var`), `delay_us` microseconds apart. Only bites a busy link (D1).
    pub fn slicer(average_size: u64, size_variation: u64, delay_us: u64, dir: Dir) -> Toxic {
        Toxic::new(
            "slicer",
            dir,
            &[
                ("average_size", average_size.into()),
                ("size_variation", size_variation.into()),
                ("delay", delay_us.into()),
            ],
        )
    }

    /// `limit_data{bytes}`: lets `bytes` cross and then closes the stream — the deterministic
    /// "cut in the middle of a result set" once the byte count is measured, never guessed (T4).
    pub fn limit_data(bytes: u64, dir: Dir) -> Toxic {
        Toxic::new("limit_data", dir, &[("bytes", bytes.into())])
    }

    fn body(&self) -> String {
        let mut m = serde_json::Map::new();
        m.insert("name".into(), self.name.clone().into());
        m.insert("type".into(), self.kind.clone().into());
        m.insert("stream".into(), self.stream.clone().into());
        m.insert("toxicity".into(), self.toxicity.into());
        m.insert(
            "attributes".into(),
            serde_json::Value::Object(self.attributes.clone()),
        );
        serde_json::Value::Object(m).to_string()
    }

    fn from_value(v: &serde_json::Value) -> Result<Toxic> {
        let field = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
        Ok(Toxic {
            name: field("name").with_context(|| format!("toxic without a name: {v}"))?,
            kind: field("type").with_context(|| format!("toxic without a type: {v}"))?,
            stream: field("stream").unwrap_or_else(|| Dir::Downstream.as_str().to_string()),
            toxicity: v.get("toxicity").and_then(|x| x.as_f64()).unwrap_or(1.0),
            attributes: v
                .get("attributes")
                .and_then(|x| x.as_object())
                .cloned()
                .unwrap_or_default(),
        })
    }
}

// ---------------------------------------------------------------------------------------------
// The API client
// ---------------------------------------------------------------------------------------------

/// One proxy the harness owns: `lv_<schema>_<role>`, listening inside the published band.
#[derive(Debug, Clone)]
pub struct Proxy {
    pub name: String,
    pub listen: u16,
    pub upstream: String,
}

impl Proxy {
    /// `127.0.0.1:<port>` — what a test running on the host connects to (the band is published on
    /// loopback only, D5).
    pub fn listen_addr(&self) -> String {
        format!("127.0.0.1:{}", self.listen)
    }
}

/// A handle to the host's toxiproxy API (one daemon per host, the upstream's own recommendation).
#[derive(Debug, Clone)]
pub struct Toxi {
    api: String,
}

impl Toxi {
    /// Reads `LAZYVIEW_TEST_TOXI`, checks the pin (F1: every toxic attribute and the drop-live-
    /// streams behaviour of F3 is written against 2.13.0) and returns the handle. Fails loudly, with
    /// the command that starts the container, when the API is missing or the wrong version: a chaos
    /// test that silently ran against a different toxiproxy would be testing nothing.
    pub async fn open(ctx: &Ctx) -> Result<Toxi> {
        let api = endpoint().with_context(opt_in_hint)?;
        let toxi = Toxi { api };
        let v = toxi.version().await.with_context(|| {
            format!(
                "GET /version of the toxiproxy API at {}: is the container up? {}",
                toxi.api,
                opt_in_hint()
            )
        })?;
        ensure!(
            v.starts_with("2.13."),
            "toxiproxy at {} is {v}, but this harness is written against the 2.13.0 pin (F1): \
             toxic attributes and the live-stream behaviour of F3 differ between releases",
            toxi.api
        );
        ctx.log(format!("toxiproxy {v} at {}", toxi.api));
        Ok(toxi)
    }

    /// `GET /version`.
    pub async fn version(&self) -> Result<String> {
        let (st, v) = self.call("GET", "/version", "").await?;
        ensure!(st == 200, "GET /version: status {st}: {v}");
        Ok(v.get("version")
            .and_then(|x| x.as_str())
            .with_context(|| format!("GET /version: no version field in {v}"))?
            .to_string())
    }

    async fn call(&self, method: &str, path: &str, body: &str) -> Result<(u16, serde_json::Value)> {
        http_json(&self.api, method, path, body)
            .await
            .with_context(|| format!("toxiproxy {method} {path} at {}", self.api))
    }

    /// The API's own words about a rejected call.
    fn message(v: &serde_json::Value) -> String {
        v.get("message")
            .and_then(|m| m.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| v.to_string())
    }

    /// `POST /proxies`: a proxy named `lv_<schema>_<role>` listening on the next free port of the
    /// band (§1). An "address already in use" (a proxy a crashed run left behind) takes the next
    /// port, a name that already exists is deleted first: both are bounded by [`PORT_TRIES`].
    pub async fn proxy(&self, schema: &str, role: &str, upstream: &str) -> Result<Proxy> {
        let name = format!("lv_{schema}_{role}");
        let mut last = String::new();
        for _ in 0..PORT_TRIES {
            let listen = next_port();
            let body = format!(
                "{{\"name\":\"{name}\",\"listen\":\"0.0.0.0:{listen}\",\
                 \"upstream\":\"{upstream}\",\"enabled\":true}}"
            );
            let (st, v) = self.call("POST", "/proxies", &body).await?;
            if (200..300).contains(&st) {
                return Ok(Proxy {
                    name,
                    listen,
                    upstream: upstream.to_string(),
                });
            }
            last = Self::message(&v);
            if last.contains("already exists") {
                // Ours, from a run that died before its `Cleanup` ran: the name is ours to reuse.
                self.delete_by_name(&name).await?;
            } else if !last.contains("address already in use") && !last.contains("bind:") {
                bail!("POST /proxies {name} (upstream {upstream}): status {st}: {last}");
            }
        }
        bail!("no free proxy port in {PORT_LOW}..={PORT_HIGH} for {name} after {PORT_TRIES} attempts (last: {last})")
    }

    /// `DELETE /proxies/{name}`: deleting the proxy removes its toxics too, so this is the whole
    /// cleanup (§1).
    pub async fn delete(&self, p: &Proxy) -> Result<()> {
        self.delete_by_name(&p.name).await
    }

    async fn delete_by_name(&self, name: &str) -> Result<()> {
        let (st, v) = self.call("DELETE", &format!("/proxies/{name}"), "").await?;
        // 404 is fine: the guard and the create-retry both race a proxy that is already gone.
        ensure!(
            st == 204 || st == 404,
            "DELETE /proxies/{name}: status {st}: {v}"
        );
        Ok(())
    }

    /// Deletes every `lv_<schema>_*` proxy (what [`Cleanup`] does on `Drop`).
    pub async fn remove_all_for(&self, schema: &str) -> Result<()> {
        let (st, v) = self.call("GET", "/proxies", "").await?;
        ensure!(st == 200, "GET /proxies: status {st}: {v}");
        let prefix = format!("lv_{schema}_");
        let names: Vec<String> = v
            .as_object()
            .map(|all| {
                all.keys()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        for name in names {
            self.delete_by_name(&name).await?;
        }
        Ok(())
    }

    /// `PATCH /proxies/{name}` `{"enabled":false}`: the proxy stops serving and its live streams
    /// end (the listen socket itself stays open, so this is not a way to free a port).
    pub async fn disable(&self, p: &Proxy) -> Result<()> {
        self.patch(p, false).await
    }

    /// `PATCH /proxies/{name}` `{"enabled":true}`.
    pub async fn enable(&self, p: &Proxy) -> Result<()> {
        self.patch(p, true).await
    }

    async fn patch(&self, p: &Proxy, enabled: bool) -> Result<()> {
        let body = format!(
            "{{\"listen\":\"0.0.0.0:{}\",\"upstream\":\"{}\",\"enabled\":{}}}",
            p.listen, p.upstream, enabled
        );
        let (st, v) = self
            .call("PATCH", &format!("/proxies/{}", p.name), &body)
            .await?;
        if st != 200 {
            bail!(
                "PATCH /proxies/{} enabled={enabled}: status {st}: {}",
                p.name,
                Self::message(&v)
            );
        }
        Ok(())
    }

    /// The deterministic "kill every connection through this proxy today" (F3): `proxy.stop()`
    /// closes every live `net.Conn`, and `enabled:false` then `true` is the API call that reaches
    /// it. A plain `Close()` is a FIN (EOF at the client) — a *reset* comes from the `reset_peer`
    /// toxic, not from here. Pinned by `selftest_toxi_restart_drops_live_streams`.
    pub async fn restart(&self, p: &Proxy) -> Result<()> {
        self.disable(p).await?;
        self.enable(p).await?;
        self.wait_listening(p).await
    }

    /// `POST /proxies/{name}/toxics`: arms `t` on the proxy. Toxiproxy applies it to the streams
    /// that are already open as well as to new ones (verified by the self-tests).
    pub async fn toxic(&self, p: &Proxy, t: &Toxic) -> Result<()> {
        let (st, v) = self
            .call("POST", &format!("/proxies/{}/toxics", p.name), &t.body())
            .await?;
        if !(200..300).contains(&st) {
            let (kind, tname) = (t.kind.as_str(), t.name.as_str());
            bail!(
                "POST toxic {kind}/{tname} on {}: status {st}: {}",
                p.name,
                Self::message(&v)
            );
        }
        Ok(())
    }

    /// `DELETE /proxies/{name}/toxics/{toxic}`. 2.13.0 does not hang here the way 2.12.0 did when
    /// the stream was already gone (F1, Shopify/toxiproxy#760).
    pub async fn remove_toxic(&self, p: &Proxy, name: &str) -> Result<()> {
        let (st, v) = self
            .call("DELETE", &format!("/proxies/{}/toxics/{name}", p.name), "")
            .await?;
        ensure!(
            st == 204 || st == 404,
            "DELETE /proxies/{}/toxics/{name}: status {st}: {v}",
            p.name
        );
        Ok(())
    }

    /// `GET /proxies/{name}/toxics` — what is armed right now (the self-tests check both directions
    /// of this: what they armed is what the API reports).
    pub async fn toxics(&self, p: &Proxy) -> Result<Vec<Toxic>> {
        let (st, v) = self
            .call("GET", &format!("/proxies/{}/toxics", p.name), "")
            .await?;
        ensure!(
            st == 200,
            "GET /proxies/{}/toxics: status {st}: {v}",
            p.name
        );
        let items = v.as_array().cloned().unwrap_or_default();
        items.iter().map(Toxic::from_value).collect()
    }

    /// `POST /reset`: clears every proxy's toxics on the host and re-enables them all. **Global**
    /// (F1), so only the self-tests call it — never a scenario, because `scenario()` runs its
    /// flavors in parallel against this one daemon.
    pub async fn reset_all(&self) -> Result<()> {
        let (st, v) = self.call("POST", "/reset", "").await?;
        ensure!(st == 204, "POST /reset: status {st}: {v}");
        Ok(())
    }

    /// Bounded wait until the proxy accepts on its listen port, so a test's connect never races
    /// setup (§2.5).
    pub async fn wait_listening(&self, p: &Proxy) -> Result<()> {
        let addr = p.listen_addr();
        wait_until(
            &format!("the proxy {} to accept on {addr}", p.name),
            Duration::from_secs(5),
            || {
                let addr = addr.clone();
                async move {
                    Ok(
                        tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(&addr))
                            .await
                            .is_ok_and(|r| r.is_ok()),
                    )
                }
            },
        )
        .await
    }

    /// The `Drop` guard of §2.2: deletes every `lv_<schema>_*` proxy when dropped. Create it
    /// *before* the first proxy so nothing between there and here can leave a fault armed on a
    /// server every suite shares.
    pub fn for_schema(&self, schema: &str) -> Cleanup {
        Cleanup {
            schema: schema.to_string(),
            api: self.api.clone(),
        }
    }
}

/// Deletes every `lv_<schema>_*` proxy of one schema (see [`Toxi::for_schema`]).
#[derive(Debug)]
pub struct Cleanup {
    schema: String,
    api: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let (api, schema) = (self.api.clone(), self.schema.clone());
        let label = schema.clone();
        // `Drop` is sync and runs on a runtime worker, so the DELETEs go on a thread of their own
        // (with a runtime of their own): they still happen while the test unwinds out of a panic.
        let out = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(anyhow::Error::from)
                .and_then(|rt| rt.block_on(Toxi { api }.remove_all_for(&schema)))
        })
        .join();
        match out {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("toxi: deleting lv_{label}_* on Drop failed: {e:#}"),
            Err(_) => eprintln!("toxi: the cleanup thread for lv_{label}_* panicked"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The self-tests' "server" and their bounded reads (§2.6)
// ---------------------------------------------------------------------------------------------

/// The self-tests' upstream: a listener on this host that a proxy dials, so the five self-tests
/// prove the mechanisms with no engine and no database traffic. Binds an ephemeral port on every
/// interface; [`Echo::upstream`] is how the container is told to reach it.
#[derive(Debug)]
pub struct Echo {
    listener: TcpListener,
    port: u16,
}

impl Echo {
    pub async fn bind() -> Result<Echo> {
        let listener = TcpListener::bind(("0.0.0.0", 0))
            .await
            .context("bind the echo listener the proxies dial")?;
        let port = listener
            .local_addr()
            .context("the echo listener's port")?
            .port();
        Ok(Echo { listener, port })
    }

    /// The upstream string for [`Toxi::proxy`]: `<host>:<port>` with `<host>` the name the container
    /// uses for this host ([`host_for_container`]).
    pub fn upstream(&self) -> String {
        format!("{}:{}", host_for_container(), self.port)
    }

    /// The proxy's side of one accepted connection. Bounded, and worded like a fault that never
    /// landed: a proxy that does not dial its upstream is the same failure (§2.5). Skips the ghost
    /// of a [`Toxi::wait_listening`] probe: that connect makes the proxy dial us too, and it drops
    /// the connection again, so the first socket in the queue can already be a closed one.
    pub async fn accept(&self) -> Result<TcpStream> {
        let word = format!(
            "the proxy never dialed its upstream {} — the fault never landed",
            self.upstream()
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("{word}");
            }
            let (s, _) = tokio::time::timeout(left, self.listener.accept())
                .await
                .with_context(|| word.clone())?
                .context("accept on the echo listener")?;
            if !closed(&s).await {
                return Ok(s);
            }
        }
    }
}

/// True when the peer of a freshly accepted socket had already closed it. `peek`, not `read`: bytes
/// that are already on their way must stay in the buffer for the caller to read.
async fn closed(s: &TcpStream) -> bool {
    let mut b = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_millis(100), s.peek(&mut b)).await,
        Ok(Ok(0))
    )
}

/// How a bounded read ([`read_up_to`]) ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The peer closed it: `read` returned 0 (a FIN).
    Eof,
    /// The bound elapsed with the stream still open — the shape of a black hole.
    Open,
    /// The read failed with this `errno` (104 = `Connection reset by peer`).
    Errno(i32),
    /// The requested number of bytes arrived inside the bound.
    Full,
}

impl End {
    /// `Connection reset by peer` (os error 104).
    pub fn is_reset(&self) -> bool {
        *self == End::Errno(104)
    }
}

impl std::fmt::Display for End {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            End::Eof => f.write_str("eof"),
            End::Open => f.write_str("still open when the bound elapsed"),
            End::Errno(e) => write!(f, "os error {e}"),
            End::Full => f.write_str("complete"),
        }
    }
}

/// Reads up to `want` bytes with a hard bound, returning what arrived and how it ended: the
/// harness's alternative to "give it a moment" (§2.5). A black hole is `Open`, a cut is `Eof` or
/// `Errno`, and a test that cannot tell those apart cannot fail when the fault never lands.
pub async fn read_up_to(s: &mut TcpStream, want: usize, bound: Duration) -> (Vec<u8>, End) {
    let deadline = Instant::now() + bound;
    let mut got = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        if got.len() >= want {
            return (got, End::Full);
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return (got, End::Open);
        }
        match tokio::time::timeout(left, s.read(&mut buf)).await {
            Err(_) => return (got, End::Open),
            Ok(Err(e)) => return (got, End::Errno(e.raw_os_error().unwrap_or_default())),
            Ok(Ok(0)) => return (got, End::Eof),
            Ok(Ok(n)) => got.extend_from_slice(&buf[..n]),
        }
    }
}

/// Writes `bytes` with a bound: a write into a black hole fails the test, it never hangs it.
pub async fn write_all(s: &mut TcpStream, bytes: &[u8], bound: Duration) -> Result<()> {
    tokio::time::timeout(bound, s.write_all(bytes))
        .await
        .with_context(|| format!("a write of {} bytes never finished", bytes.len()))?
        .with_context(|| format!("write of {} bytes failed", bytes.len()))
}

/// Connects to a proxy's listen address with a bound (call [`Toxi::wait_listening`] first, so this
/// never races the proxy's own setup).
pub async fn connect(addr: &str, bound: Duration) -> Result<TcpStream> {
    tokio::time::timeout(bound, TcpStream::connect(addr))
        .await
        .with_context(|| format!("connect to the proxy at {addr} timed out"))?
        .with_context(|| format!("connect to the proxy at {addr}"))
}

/// `base` DSN with host and port replaced by the proxy's listen address, credentials, schema and
/// `?options` kept (§2.4): how the engine's binlog/query/meta connection or a target's writes are
/// routed through one proxy. Everything the harness itself does stays on the unproxied `db.dsn`.
pub fn dsn_via_proxy(base: &str, listen: u16) -> Result<String> {
    let (scheme, rest) = base
        .split_once("://")
        .with_context(|| format!("not a DSN: {base}"))?;
    let (before_q, query) = match rest.find('?') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (authority, path) = match before_q.find('/') {
        Some(i) => (&before_q[..i], &before_q[i..]),
        None => (before_q, ""),
    };
    let creds = authority.split_once('@').map_or("", |(c, _)| c);
    let at = if creds.is_empty() { "" } else { "@" };
    Ok(format!(
        "{scheme}://{creds}{at}127.0.0.1:{listen}{path}{query}"
    ))
}
