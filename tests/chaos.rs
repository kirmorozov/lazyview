//! The chaos suite: network-layer faults between the engine and its servers, armed over the
//! Toxiproxy API (plan `tmp/out/lvs-chaos/plan/breakdown.md` §2–§3, findings F1–F5).
//!
//! What lives here today is the harness's own contract — the five `selftest_*` of §2.6: a proxy that
//! passes bytes both ways, `limit_data` closing an established stream at exactly N bytes,
//! `reset_peer`, the `timeout` black hole and `restart()` dropping every live stream. The scenarios
//! of the follow-up PRs may depend on nothing else, and a scenario whose mechanism has no passing
//! self-test here is a review reject (F3). The self-tests need no engine and no database traffic:
//! their upstream is the harness's own [`Echo`] listener, so they pin the *mechanism*, not MySQL.
//!
//! Opt-in, in the shape of `tests/exclusive.rs`: without `LAZYVIEW_TEST_TOXI` every test prints the
//! harness's `SKIP` line and passes, so no existing job runs them. The daemon comes up with the
//! `chaos` compose profile (CI's chaos job, or `scripts/test-servers.sh compose up --chaos toxiproxy
//! <flavor>`); the harness never starts a container (F9).

mod common;

use std::time::Duration;

use anyhow::{ensure, Result};
use common::toxi::{self, Dir, Echo, End, Proxy, Toxi, Toxic};
use common::*;
use tokio::net::TcpStream;

/// Every bounded wait in this file: patient enough for a proxy to bind and for bytes to cross,
/// small enough that a chaos test can never outlast `run_timeout()` (F5).
const BOUND: Duration = Duration::from_secs(10);
/// How long a black hole is watched: the point of `timeout{timeout:0}` is that data never arrives,
/// so the *absence* of data inside this bound is the assertion (§2.5 — a bounded wait on the
/// observable, never a fixed pause followed by "it was probably cut").
const BLACK_HOLE: Duration = Duration::from_secs(3);

/// One proxied stream, established and *proven* live in both directions before anything is armed:
/// the "established" of §2.6. A fault that lands on a connection that never worked proves nothing,
/// so every self-test starts here.
async fn established(
    toxi: &Toxi,
    echo: &Echo,
    schema: &str,
    role: &str,
) -> Result<(Proxy, TcpStream, TcpStream)> {
    let p = toxi.proxy(schema, role, &echo.upstream()).await?;
    toxi.wait_listening(&p).await?;
    let mut cli = toxi::connect(&p.listen_addr(), BOUND).await?;
    let mut srv = echo.accept().await?;
    toxi::write_all(&mut cli, b"ping", BOUND).await?;
    let (got, end) = toxi::read_up_to(&mut srv, 4, BOUND).await;
    ensure!(
        got == b"ping",
        "selftest {role}: the proxy never carried client→server bytes ({got:?}, {end}) — the fault \
         could not land on a stream that never worked"
    );
    toxi::write_all(&mut srv, b"pong", BOUND).await?;
    let (got, end) = toxi::read_up_to(&mut cli, 4, BOUND).await;
    ensure!(
        got == b"pong",
        "selftest {role}: the proxy never carried server→client bytes ({got:?}, {end}) — the fault \
         could not land on a stream that never worked"
    );
    Ok((p, cli, srv))
}

/// The proxy itself: created over the API with a name the Drop guard cleans up by, listening on the
/// published band, byte-transparent in both directions, and with nothing armed on it (§2.1–2.3).
#[test]
fn selftest_toxi_proxy_passes_bytes_both_ways() {
    let _one = toxi::exclusive();
    scenario("selftest_toxi_echo", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("toxi_echo").await?;
        // The guard first: from here on, a panic cannot leave a proxy armed on a shared server.
        let _clean = toxi.for_schema(&db.name);
        let echo = Echo::bind().await?;
        let (p, mut cli, mut srv) = established(&toxi, &echo, &db.name, "echo").await?;
        ensure!(
            p.name == format!("lv_{}_echo", db.name),
            "selftest echo: proxy name {} is not lv_<schema>_<role>",
            p.name
        );
        ensure!(
            p.listen >= 17100 && p.listen <= 17199,
            "selftest echo: proxy listens on {}, outside the published band 17100-17199",
            p.listen
        );
        // More bytes, both ways, after the handshake of `established`.
        toxi::write_all(&mut cli, b"client-to-server", BOUND).await?;
        let (got, end) = toxi::read_up_to(&mut srv, 16, BOUND).await;
        ensure!(
            got == b"client-to-server" && end == End::Full,
            "selftest echo: client→server crossed as {got:?} ({end})"
        );
        toxi::write_all(&mut srv, b"server-to-client", BOUND).await?;
        let (got, end) = toxi::read_up_to(&mut cli, 16, BOUND).await;
        ensure!(
            got == b"server-to-client" && end == End::Full,
            "selftest echo: server→client crossed as {got:?} ({end})"
        );
        ensure!(
            toxi.toxics(&p).await?.is_empty(),
            "selftest echo: a fresh proxy came back with toxics on it"
        );
        // The `GET` the self-tests read their own state back from.
        toxi.toxic(&p, &Toxic::latency(1, 0, Dir::Downstream).named("lat"))
            .await?;
        let armed = toxi.toxics(&p).await?;
        ensure!(
            armed.len() == 1
                && armed[0].name == "lat"
                && armed[0].kind == "latency"
                && armed[0].stream == "downstream"
                && armed[0].toxicity == 1.0,
            "selftest echo: GET /toxics reports {armed:?}, expected the latency toxic just armed"
        );
        toxi.remove_toxic(&p, "lat").await?;
        ensure!(
            toxi.toxics(&p).await?.is_empty(),
            "selftest echo: the toxic survived its DELETE"
        );
        ctx.log(format!("proxy {} on {}", p.name, p.listen_addr()));
        Ok(())
    });
}

/// `limit_data{bytes:N}` closes an established stream after exactly N bytes of a 10 × N transfer
/// (§2.6): the deterministic "cut in the middle of a result set", which is why T4 measures its byte
/// limits instead of guessing them.
#[test]
fn selftest_toxi_limit_data_closes_the_stream_at_n_bytes() {
    let _one = toxi::exclusive();
    const N: usize = 4_096;
    scenario("selftest_toxi_limit", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("toxi_limit").await?;
        let _clean = toxi.for_schema(&db.name);
        let echo = Echo::bind().await?;
        let (p, mut cli, mut srv) = established(&toxi, &echo, &db.name, "limit").await?;
        toxi.toxic(
            &p,
            &Toxic::limit_data(N as u64, Dir::Downstream).named("lim"),
        )
        .await?;
        toxi::write_all(&mut srv, &b"z".repeat(10 * N), BOUND).await?;
        let (got, end) = toxi::read_up_to(&mut cli, 10 * N, BOUND).await;
        ensure!(
            got.len() == N && (end == End::Eof || end.is_reset()),
            "selftest limit_data: the fault never landed — {} of the {} bytes offered crossed with \
             limit_data{{bytes:{N}}} armed; exactly {N} may cross, then the stream must close \
             (ended {end})",
            got.len(),
            10 * N
        );
        ctx.log(format!(
            "limit_data: exactly {} bytes of {} crossed, then the stream closed ({end})",
            got.len(),
            10 * N
        ));
        Ok(())
    });
}

/// `reset_peer{timeout:0}` (F2, §2.6) — and the fact every scenario needs to know: on 2.13.0 the
/// RST goes to the **upstream** peer (the server sees `Connection reset by peer`, os error 104)
/// while the client's side ends at EOF, and only when the toxic was armed *before* the stream was
/// created. Armed on a live stream it ends the stream as a plain FIN on both sides, which is what
/// `restart()` (F3) does too — so a scenario that wants a real RST cuts a stream it has just opened.
#[test]
fn selftest_toxi_reset_peer_resets_the_established_stream() {
    let _one = toxi::exclusive();
    scenario("selftest_toxi_reset", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("toxi_reset").await?;
        let _clean = toxi.for_schema(&db.name);
        let echo = Echo::bind().await?;
        // (a) armed before the stream is created: a real TCP reset on the server's side of the proxy.
        let (p, cli, srv) = established(&toxi, &echo, &db.name, "reset").await?;
        drop((cli, srv));
        let t = Toxic::reset_peer(0).named("rst");
        toxi.toxic(&p, &t).await?;
        ensure!(
            toxi.toxics(&p).await?.iter().any(|x| x.name == "rst"),
            "selftest reset_peer: the armed toxic is not in GET /toxics — the fault never landed"
        );
        let mut cli = toxi::connect(&p.listen_addr(), BOUND).await?;
        let mut srv = echo.accept().await?; // both halves are established before anything is cut
        toxi::write_all(&mut srv, b"cut-me", BOUND).await?;
        let (cg, ce) = toxi::read_up_to(&mut cli, 1024, BOUND).await;
        let (sg, se) = toxi::read_up_to(&mut srv, 1024, BOUND).await;
        ensure!(
            ce != End::Open,
            "selftest reset_peer: the fault never landed — the client's side of the proxy was still \
             open after reset_peer{{timeout:0}} ({ce}, {} bytes)",
            cg.len()
        );
        ensure!(
            se.is_reset(),
            "selftest reset_peer: the fault never landed — the server's side of the proxy saw {se}, \
             expected ECONNRESET (os error 104), {} bytes read",
            sg.len()
        );
        ctx.log(format!(
            "reset_peer armed before the stream: client {ce}, server {se} (2.13.0 sends the RST \
             upstream, to the server)"
        ));
        // (b) armed on a live stream: it ends, but as a FIN on both sides — no RST. A scenario that
        // reads this as "the server saw a reset" would be asserting something that did not happen.
        toxi.remove_toxic(&p, "rst").await?;
        let (live, mut cli, mut srv) = established(&toxi, &echo, &db.name, "resetlive").await?;
        toxi.toxic(&live, &Toxic::reset_peer(0).named("rst2"))
            .await?;
        toxi::write_all(&mut srv, b"cut-me-too", BOUND).await?;
        let (g, e) = toxi::read_up_to(&mut cli, 1024, BOUND).await;
        ensure!(
            e != End::Open,
            "selftest reset_peer: the fault never landed — a live stream survived reset_peer \
             ({e}, {} bytes)",
            g.len()
        );
        let (_, se) = toxi::read_up_to(&mut srv, 1024, BOUND).await;
        ctx.log(format!(
            "reset_peer armed on a live stream: client {e}, server {se} (a FIN, not a reset: arm \
             reset_peer before the stream, or use restart())"
        ));
        Ok(())
    });
}

/// `timeout{timeout:0}` black-holes an established stream until its toxic is removed (§2.6): data
/// offered to it never arrives, and the harness proves both halves of that — the silence, and the
/// proxy serving again afterwards. `POST /reset` is exercised here and nowhere else in the plan: it
/// is global, so a scenario may never call it (F1).
#[test]
fn selftest_toxi_timeout_black_holes_the_stream() {
    let _one = toxi::exclusive();
    scenario("selftest_toxi_timeout", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("toxi_timeout").await?;
        let _clean = toxi.for_schema(&db.name);
        let echo = Echo::bind().await?;
        let (p, mut cli, mut srv) = established(&toxi, &echo, &db.name, "timeout").await?;
        toxi.toxic(&p, &Toxic::timeout(0).named("bh")).await?;
        toxi::write_all(&mut srv, b"into-the-black-hole", BOUND).await?;
        let (got, end) = toxi::read_up_to(&mut cli, 19, BLACK_HOLE).await;
        ensure!(
            got.is_empty() && end == End::Open,
            "selftest timeout: the fault never landed — {} bytes crossed the black hole ({end})",
            got.len()
        );
        ctx.log(format!(
            "timeout: {:?} of silence across an established stream",
            BLACK_HOLE
        ));
        toxi.remove_toxic(&p, "bh").await?;
        // 2.13.0 ends the black-holed stream (a FIN) when its toxic is removed rather than letting
        // it resume; what a scenario must be able to rely on is that the proxy is usable again.
        let (g, e) = toxi::read_up_to(&mut cli, 19, BOUND).await;
        let mut cli2 = toxi::connect(&p.listen_addr(), BOUND).await?;
        let mut srv2 = echo.accept().await?;
        toxi::write_all(&mut srv2, b"after-removal", BOUND).await?;
        let (got, end) = toxi::read_up_to(&mut cli2, 13, BOUND).await;
        ensure!(
            got == b"after-removal" && end == End::Full,
            "selftest timeout: the fault never went away — the new stream got {got:?} ({end}) after \
             the toxic was removed (the black-holed one ended {e} with {} bytes)",
            g.len()
        );
        // F1: the only `POST /reset` call in the plan. It clears every proxy's toxics on the host,
        // which is why scenarios never call it: `scenario()` runs its flavors in parallel against
        // this one daemon. `exclusive()` is what keeps even the self-tests from racing it.
        toxi.toxic(&p, &Toxic::bandwidth(1, Dir::Downstream).named("bw"))
            .await?;
        ensure!(
            !toxi.toxics(&p).await?.is_empty(),
            "selftest timeout: POST /reset has nothing to clear — the toxic was never listed"
        );
        toxi.reset_all().await?;
        ensure!(
            toxi.toxics(&p).await?.is_empty(),
            "selftest timeout: POST /reset left a toxic armed on {}",
            p.name
        );
        Ok(())
    });
}

/// `toxi.restart()` — `enabled:false` then `true`, the API call that reaches `proxy.stop()`, which
/// closes every live `net.Conn` (F3). This is the deterministic "kill every connection through this
/// proxy today" that the mid-transaction scenarios are built on, and it must come back serving on
/// the same port.
#[test]
fn selftest_toxi_restart_drops_live_streams() {
    let _one = toxi::exclusive();
    scenario("selftest_toxi_restart", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("toxi_restart").await?;
        let _clean = toxi.for_schema(&db.name);
        let echo = Echo::bind().await?;
        let (p, mut cli, mut srv) = established(&toxi, &echo, &db.name, "restart").await?;
        toxi.restart(&p).await?;
        let (g, e) = toxi::read_up_to(&mut cli, 1024, BOUND).await;
        ensure!(
            e != End::Open,
            "selftest restart: the fault never landed — the live client stream survived restart() \
             ({e}, {} bytes)",
            g.len()
        );
        let (sg, se) = toxi::read_up_to(&mut srv, 1024, BOUND).await;
        // And the proxy is serving again, on the same port it was dropped from.
        let mut cli2 = toxi::connect(&p.listen_addr(), BOUND).await?;
        let mut srv2 = echo.accept().await?;
        toxi::write_all(&mut cli2, b"after-restart", BOUND).await?;
        let (got, end) = toxi::read_up_to(&mut srv2, 13, BOUND).await;
        ensure!(
            got == b"after-restart" && end == End::Full,
            "selftest restart: the proxy never came back up on {} — {got:?} ({end})",
            p.listen_addr()
        );
        ctx.log(format!(
            "restart: live streams ended client {e} / server {se} ({} bytes seen by the server), \
             proxy serving again",
            sg.len()
        ));
        Ok(())
    });
}
