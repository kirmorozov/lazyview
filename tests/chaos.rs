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
//!
//! Below the self-tests live the five source-side scenarios of §3 (T4): a `reset_peer` in the middle
//! of one transaction's row events, a `timeout` black hole on the dump link, a throttled source that
//! must lag and then recover, a `limit_data` cut inside one binlog event, and a `limit_data` cut
//! inside a refresh's result set. Each isolates one source role through the per-role DSN hooks of
//! F4, asserts its own counter expectations *and* the three that every scenario owes: the fault
//! landed, `verify` is exact at the end, and no incremental view was ever rebuilt (F6).
//!
//! Below them also live the target-side scenarios of T5 (§3 rows 6–9): a `reset_peer` cut
//! mid-write-transaction on the target link, a black hole during a flush with the healthy target
//! proving independence, bandwidth starvation and its drain, and fragmentation on both sides at
//! once.

mod common;

use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use common::soakchaos;
use common::soakcheck::{counters, set_writers, view_rows, Window};
use common::soakwork::{
    run_paced, run_soak_batches, setup_soak, soak_views, SoakIds, SoakWorkload,
};
use common::toxi::{self, dsn_via_proxy, Cleanup, Dir, Echo, End, Proxy, Toxi, Toxic};
use common::workload::Sizes;
use common::*;
use lazyview::Snapshot;
use mysql_async::prelude::Queryable;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::watch;

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

// =============================================================================================
// The source-side scenarios of §3 (T4): one fault each, aimed at one source role through the
// per-role DSN hooks (`LAZYVIEW_TEST_{BINLOG,QUERY,META}_DSN`, F4/D1). Everything the harness does
// — DDL, seed, writers, oracle — stays on direct connections (Q8); only the engine is proxied.
// A scenario that showed a real product bug is a finding for lv-super, never a workaround here.
// =============================================================================================

/// How long a scenario waits for a fault to land or for the engine to come back: the engine's own
/// bounds are ~10.6 s (binlog silence, F5) and 30 s (first event after a reconnect), so 60 s is
/// patient for one of those rounds without ever approaching `run_timeout()` (F5).
const FAULT: Duration = Duration::from_secs(60);
/// The writers of a scenario: two paced tasks, a share of the neutral mix each.
const WRITERS: usize = 2;
const WRITER_TPS: u32 = 15;

/// The neutral slice of §3 (F7): five access paths of the soak's view set plus the base of its one
/// `Chained` view. Every definition is `soakwork`'s — no new tables, no application names (F9).
const CHAOS_VIEWS: [&str; 7] = [
    "w_items_direct",     // direct, one `WHERE` filter over `items`
    "w_item_tags",        // partial: a join whose rows flip NULL <-> matched
    "w_hop_partners",     // the `links` hop: entities → links → entities
    "wc_notes_by_set",    // a `sets_notes` cascade child, `GROUP BY`
    "w_total_amount",     // global aggregate, no `GROUP BY`
    "w_groups_by_region", // the base the chained view reads
    "w_chain_a",          // `Chained`: outside the no-rebuild rule by design (F6)
];

/// `Sizes::small()` of the plan (there is no `small()` in `workload`): a short deterministic slice of
/// the neutral seed, because a network-fault test wants a short reproducible run and the soak keeps
/// the breadth (F7).
fn small_sizes() -> Sizes {
    Sizes {
        regions: 6,
        customers: 60,
        products: 40,
        orders: 80,
        events: 60,
    }
}

/// [`CHAOS_VIEWS`] narrowed out of the neutral set, failing rather than silently testing less.
fn chaos_views() -> Result<Vec<ViewSpec>> {
    let all = soak_views();
    CHAOS_VIEWS
        .iter()
        .map(|n| {
            all.iter()
                .find(|s| s.name == **n)
                .cloned()
                .with_context(|| format!("the neutral set has no view `{n}` to narrow to"))
        })
        .collect()
}

/// The DSN the engine uses for its source connections, honouring `LAZYVIEW_TEST_ENGINE_USER`: the
/// lvdoc leg stays (Q4), so the proxies carry the RDS privilege model too. This is the base
/// [`toxi::dsn_via_proxy`] rewrites onto a proxy.
fn engine_dsn(db: &Db) -> String {
    match std::env::var("LAZYVIEW_TEST_ENGINE_USER") {
        Ok(u) if !u.is_empty() => dsn_with_user(&db.dsn, &u),
        _ => db.dsn.clone(),
    }
}

/// One scenario's fixtures (§3): the neutral schema on its own `sub_seed`, the narrowed views
/// (also on one target of the same server, so `verify` covers every (view, target) pair), and the
/// engine with the source roles the scenario named routed through proxies it owns.
struct Fix {
    db: Db,
    targets: HashMap<String, Db>,
    specs: Vec<ViewSpec>,
    ids: Arc<SoakIds>,
    eng: Engine,
    toxi: Toxi,
    proxies: HashMap<String, Proxy>,
    /// The §2.2 guard, created *before* the first proxy: a panic or an early `?` cannot leave a
    /// black hole armed on a server every suite shares.
    _clean: Cleanup,
    /// Opens at the engine's counters after the first builds, so the no-rebuild rule of F6 counts
    /// everything after them ("outside the first build").
    window: Window,
}

/// The roles of F4 a scenario may isolate: `binlog`, `query`, `meta`, `lock` (the leadership
/// lock's own session, review A-06), or `src` for all three source roles at once (scenario 3,
/// where the fault is deliberately not attributable to one role).
async fn setup(ctx: &Arc<Ctx>, tag: &str, toxi: &Toxi, roles: &[&str]) -> Result<Fix> {
    setup_extra(ctx, tag, toxi, roles, &[]).await
}

/// [`setup`] with the engine settings `extra`, appended after the scenario's own so a scenario can
/// pin a number of its own — the lock scenarios shrink the lease to the smallest the rules allow.
async fn setup_extra(
    ctx: &Arc<Ctx>,
    tag: &str,
    toxi: &Toxi,
    roles: &[&str],
    extra: &[(&str, &str)],
) -> Result<Fix> {
    let db = ctx.new_db(tag).await?;
    let ids = setup_soak(&db, ctx.sub_seed(tag), small_sizes()).await?;
    let t1 = ctx.new_db(&format!("{tag}_t1")).await?;
    let specs: Vec<ViewSpec> = chaos_views()?
        .into_iter()
        .map(|s| {
            if s.kind == Kind::Chained {
                s // an MV over an MV stays where its base is
            } else {
                s.with_targets("source,t1")
            }
        })
        .collect();
    // The guard first: from here on nothing can leave a proxy armed.
    let clean = toxi.for_schema(&db.name);
    let base = engine_dsn(&db);
    let mut env: Vec<(String, String)> = vec![
        // F5: the engine's own timeouts decide how long a fault lasts, not the harness's patience.
        ("LAZYVIEW_HEARTBEAT_MS".into(), "200".into()),
        ("LAZYVIEW_TARGET_TIMEOUT_MS".into(), "2000".into()),
    ];
    let mut proxies = HashMap::new();
    for role in roles {
        let p = toxi
            .proxy(&db.name, role, &toxi::upstream(ctx.flavor()))
            .await?;
        toxi.wait_listening(&p).await?;
        let dsn = toxi::dsn_via_proxy(&base, p.listen)?;
        env.push((
            format!("LAZYVIEW_TEST_{}_DSN", role.to_ascii_uppercase()),
            dsn.clone(),
        ));
        if *role == "src" {
            // One proxy for every source role: the plain source DSN goes through it too (F4).
            env.push(("LAZYVIEW_DSN".into(), dsn));
            for r in ["binlog", "query", "meta"] {
                proxies.insert(r.to_string(), p.clone());
            }
        }
        proxies.insert((*role).to_string(), p);
    }
    env.extend(target_env(&[("t1", t1.dsn.as_str())]));
    for (k, v) in extra {
        env.push((k.to_string(), v.to_string()));
    }
    let (mut eng, _) = start_with_views(ctx, &db, &specs, &ov_refs(&env)).await?;
    // The first builds happen here; the no-rebuild window opens after them (F6).
    let mut c = db.conn().await?;
    eng.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    let window = Window::now(&eng);
    Ok(Fix {
        db,
        targets: HashMap::from([("t1".to_string(), t1)]),
        specs,
        ids,
        eng,
        toxi: toxi.clone(),
        proxies,
        _clean: clean,
        window,
    })
}

impl Fix {
    fn proxy(&self, role: &str) -> Result<&Proxy> {
        self.proxies
            .get(role)
            .with_context(|| format!("this scenario has no `{role}` proxy"))
    }

    /// The engine's own words about a fault: the first line of its log containing `needle`.
    fn log_line(&self, needle: &str) -> Result<Option<String>> {
        soakchaos::grep_log(&self.eng.log_path, needle)
    }

    /// Waits until the engine is connected to the binlog stream again (`streaming == 1`).
    async fn wait_streaming(&mut self) -> Result<()> {
        soakchaos::wait_streaming(&mut self.eng).await
    }

    /// The gauge the same waiter reads: `false` once the dump link is gone.
    fn streaming(&self) -> bool {
        self.eng.metrics.gauges.streaming.load(Ordering::Relaxed) == 1
    }

    /// Arms one toxic on the role's proxy and reads it back: every scenario proves the fault is
    /// armed before it proves it landed (§2.5).
    async fn arm(&self, role: &str, t: &Toxic) -> Result<()> {
        let p = self.proxy(role)?;
        self.toxi.toxic(p, t).await?;
        let armed = self.toxi.toxics(p).await?;
        ensure!(
            armed.iter().any(|x| x.name == t.name),
            "the fault never landed on {}: {} is not in GET /toxics ({:?})",
            p.name,
            t.name,
            armed.iter().map(|x| (&x.name, &x.kind)).collect::<Vec<_>>()
        );
        Ok(())
    }

    async fn disarm(&self, role: &str, name: &str) -> Result<()> {
        let p = self.proxy(role)?;
        self.toxi.remove_toxic(p, name).await
    }

    /// The binlog gap between the server and what the engine has applied, in bytes of the current
    /// file (a file rollover counts as [`u64::MAX`]: the engine is behind by at least a whole file).
    async fn gap_bytes(&self) -> Result<u64> {
        let mut c = self.db.conn().await?;
        let s = server_position(&mut c).await?;
        c.disconnect().await.ok();
        let a = self
            .eng
            .applied()
            .context("the engine has not applied a binlog position yet")?;
        Ok(if s.file == a.file {
            s.pos.saturating_sub(a.pos)
        } else {
            u64::MAX
        })
    }

    /// The delta of one budget-table counter since the window opened (the same table the soak's
    /// budget uses, F6).
    fn counter(&self, name: &str) -> u64 {
        counters(&self.eng, &self.window.0)
            .into_iter()
            .find(|(n, _)| *n == name)
            .map_or(0, |(_, v)| v)
    }
}

/// The writers of §3: paced `SoakWorkload` tasks on **direct** connections (Q8 — the fault is
/// aimed at the engine's links, never at the load's).
type Writers = (
    Vec<watch::Sender<bool>>,
    Vec<tokio::task::JoinHandle<Result<usize>>>,
);

fn spawn_writers(db: &Db, ids: &Arc<SoakIds>, seed: u64) -> Writers {
    let mut senders = Vec::with_capacity(WRITERS);
    let mut handles = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
        let (tx, rx) = watch::channel(false);
        let db = db.clone();
        let ids = ids.clone();
        handles.push(tokio::spawn(async move {
            run_paced(
                &db,
                ids,
                derive_seed(seed, &format!("writer{w}")),
                1,
                WRITER_TPS,
                rx,
            )
            .await
        }));
        senders.push(tx);
    }
    (senders, handles)
}

/// Stops the writers and joins them, so the final `verify` starts from a quiet schema (§3).
async fn quiesce_writers(w: &mut Writers) -> Result<u64> {
    let (senders, handles) = w;
    set_writers(senders, true);
    let mut total = 0u64;
    for h in handles.drain(..) {
        let n = h
            .await
            .context("a paced writer task was cancelled")?
            .context("a paced writer failed")?;
        total += n as u64;
    }
    Ok(total)
}

/// The tail of every scenario (§3): quiet schema, applied everything, then the oracle over every
/// (view, target) pair and the no-rebuild rule.
async fn finish(ctx: &Arc<Ctx>, f: &mut Fix, w: &mut Writers, when: &str) -> Result<()> {
    quiesce_writers(w).await?;
    let mut c = f.db.conn().await?;
    f.eng.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    verify_targets(
        ctx,
        &mut f.eng,
        &f.db,
        &f.targets,
        &HashMap::new(),
        &f.specs,
        when,
    )
    .await?;
    // The per-view rule of F6 (soak-plan F5/D5): an `incremental` view never took a whole-view
    // refresh, on any target. A `Chained`/`Full` view is outside the rule by design.
    let rows = view_rows(&f.eng, &f.db, &f.specs).await?;
    let bad: Vec<String> = rows
        .iter()
        .filter(|v| v.mode.eq_ignore_ascii_case("incremental") && v.whole_view_refreshes > 0)
        .map(|v| format!("{}@{} = {}", v.name, v.target, v.whole_view_refreshes))
        .collect();
    ensure!(
        bad.is_empty(),
        "{when}: an incremental view took a whole-view refresh: {}",
        bad.join(", ")
    );
    Ok(())
}

/// One `items` transaction in the `chunked_insert` shape: `rows` rows in statements of 200 rows
/// each, inside one transaction, so the row events span many network writes (scenario 1) and the
/// size of the block is known from the seed rather than guessed at run time (scenario 4 derives its
/// `limit_data{bytes}` from it, per the plan's "never a magic guess"). Ids come from the soak
/// allocator, so a writer running at the same time cannot collide with them.
fn items_txn(ids: &SoakIds, rows: usize) -> (Vec<String>, usize) {
    let entities = ids.seed_entities.max(1);
    // `SoakIds::fresh` hands out the value `fetch_add` *returned*, so the block is
    // `from ..= from + rows - 1` and the allocator resumes at `from + rows`: reserving `rows` and
    // starting at `+ 1` left the block's last id inside the range the paced writers are still
    // handed, and a writer's `INSERT … ON DUPLICATE KEY` took that row first, so the transaction's
    // last statement died on ERROR 1062 for `items.PRIMARY` (issue #229's second flake).
    let from = ids.item.fetch_add(rows as i64, Ordering::Relaxed);
    let mut stmts = Vec::new();
    let mut bytes = 0usize;
    let mut done = 0usize;
    while done < rows {
        let take = 200usize.min(rows - done);
        let mut vals = Vec::with_capacity(take);
        for k in 0..take {
            let id = from + (done + k) as i64;
            let entity = (id % entities) + 1;
            // `qty > 0` every time: `w_items_direct` filters on it, so every row is visible.
            let row = format!(
                "({id}, {entity}, {}, {}, {}, 'blk{id}')",
                (id % 7) + 1,
                (id % 9) + 1,
                (id % 40_000) as f64 / 100.0
            );
            bytes += row.len();
            vals.push(row);
        }
        stmts.push(format!(
            "INSERT INTO items (id, entity_id, grp, qty, price, label) VALUES {}",
            vals.join(",")
        ));
        done += take;
    }
    (stmts, bytes)
}

/// The dump stream dies in the middle of one transaction's row events (`reset_peer` on the binlog
/// link while a 50 000-row `items` transaction is in flight, with a `slicer` in front of the reset
/// so the bytes really do cross as many small writes). The engine must reconnect, re-register and
/// resume from the **persisted** position, and no partial transaction may ever be visible.
#[test]
fn chaos_src_binlog_reset_mid_txn() {
    let _one = toxi::exclusive();
    scenario("chaos_src_binlog_reset_mid_txn", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_src_binlog_reset_mid_txn");
        let mut f = setup(&ctx, "chaos_s1", &toxi, &["binlog"]).await?;
        let mut w = spawn_writers(&f.db, &f.ids, seed);
        f.wait_streaming().await?;
        // The slicer first: 64-byte slices on the dump link, so the transaction is in flight for
        // many network writes and the reset cannot land before it started (F2).
        f.arm(
            "binlog",
            &Toxic::slicer(64, 0, 0, Dir::Downstream).named("slice"),
        )
        .await?;
        const TXN_ROWS: usize = 50_000;
        let (stmts, bytes) = items_txn(&f.ids, TXN_ROWS);
        let before = f.eng.snapshot();
        // The transaction runs on its own task so the reset goes in *while* it is in flight.
        let db = f.db.clone();
        let h = tokio::spawn(async move {
            let mut c = db.conn().await?;
            txn(&mut c, &stmts, false).await?;
            c.disconnect().await.ok();
            Ok::<(), anyhow::Error>(())
        });
        f.arm("binlog", &Toxic::reset_peer(0).named("rst")).await?;
        h.await
            .context("the big transaction task panicked")?
            .context("the big transaction failed")?;
        // The commit is acknowledged, so the transaction's last row event is in the server's binlog
        // now: this is the position the engine has to reach before any assertion below means
        // anything. The *persisted* position cannot serve as the target — it is from before the
        // transaction and the engine was already past it when the stream died, so a wait on it
        // returns at once and the check that follows races the reconnected stream (issue #229).
        let mut c = f.db.conn().await?;
        let head = server_position(&mut c).await?;
        c.disconnect().await.ok();
        // The fault landed: the dump link is gone (selftest_toxi_reset_peer: a stream armed with
        // reset_peer ends; on 2.13.0 the RST goes upstream, to the server, and the client sees EOF
        // — so this asserts the engine's stream died, not that the server saw a reset).
        let m = f.eng.metrics.clone();
        wait_until("the binlog dump stream to be cut", FAULT, || {
            let m = m.clone();
            async move { Ok(m.gauges.streaming.load(Ordering::Relaxed) == 0) }
        })
        .await
        .with_context(|| {
            format!(
                "the fault never landed: 50 000 rows went through the proxied \
             dump link and the engine never lost the stream; {}",
                ctx.label()
            )
        })?;
        f.disarm("binlog", "rst").await?;
        f.disarm("binlog", "slice").await?;
        // It resumes from the position it had *persisted*, not from the head of the binlog: the
        // read stays here as that evidence (the engine's own reconnect line is checked at the end),
        // and it is what a resume-from-scratch would have thrown away.
        let saved = soakchaos::saved_position(&f.db).await?;
        f.wait_streaming().await?;
        // Not `saved`: the whole transaction, up to the source's position at its commit. Anything
        // less lets the delta below be taken before the reconnected stream delivered the rows.
        f.eng.wait_applied(&head, t_catchup()).await?;
        let d = f.eng.snapshot().since(&before);
        ensure!(
            d.events > 0 && d.rows_seen >= TXN_ROWS as u64,
            "the stream never carried the whole transaction after the reset: {} events and {} \
             rows applied since the fault, expected at least the transaction's {TXN_ROWS} rows \
             (persisted position {saved:?}, waited for the source's {head:?})",
            d.events,
            d.rows_seen
        );
        ctx.log(format!(
            "reset mid-transaction: {} bytes of row events cut, resumed from {saved:?} and applied \
             through {head:?} (+{} events, +{} rows)",
            bytes, d.events, d.rows_seen
        ));
        finish(&ctx, &mut f, &mut w, "after the mid-transaction reset").await?;
        ensure!(
            f.counter("full_rebuilds") == 0,
            "a cut dump stream cost {} whole rebuild(s) of the engine",
            f.counter("full_rebuilds")
        );
        ctx.log(format!(
            "the engine logged its reconnect: {:?}",
            f.log_line("reconnect")?
        ));
        Ok(())
    });
}

// The engine's own connect bounds, mirrored here because the product keeps them `pub(crate)`:
// `BINLOG_CONNECT_TIMEOUT` (src/engine.rs) and `Policy::CONNECT` (src/supervisor.rs). Every
// wait below is sized from these numbers, never from a guess.
const BH_CONNECT_TIMEOUT: Duration = Duration::from_secs(20); // engine.rs: BINLOG_CONNECT_TIMEOUT
const BH_CONNECT_ATTEMPTS: u32 = 5; // supervisor.rs: Policy::CONNECT.attempts
const BH_CONNECT_BASE: Duration = Duration::from_secs(1); // supervisor.rs: Policy::CONNECT.base
const BH_CONNECT_CAP: Duration = Duration::from_secs(30); // supervisor.rs: Policy::CONNECT.cap

/// The widest the engine's `backoff(n, base, cap, jitter)` (supervisor.rs) gets for retry `n`:
/// `min(base · 2^(n−1), cap)` times the +50 % top of the CONNECT policy's jitter.
fn bh_backoff_max(n: u32) -> Duration {
    (BH_CONNECT_BASE * (1u32 << (n - 1).min(6)))
        .min(BH_CONNECT_CAP)
        .mul_f64(1.5)
}

/// What a failed recovery proof of the black-hole scenario reports (#264): how many times the
/// stream actually came back (each successful reconnect logs one `streaming binlog` line,
/// src/engine.rs; the attempts themselves log at debug only), the `supervisor_retries` delta
/// since the proved tick, and where `applied` ended — so a real never-comes-back (the
/// production path of G3/G5) is attributable in one run instead of needing a rerun.
fn bh_recovery_evidence(f: &Fix, retries_at_proof: u64) -> String {
    let reconnects = std::fs::read_to_string(&f.eng.log_path)
        .map(|t| t.lines().filter(|l| l.contains("streaming binlog")).count())
        .unwrap_or(0);
    let m = f.eng.metrics.snapshot();
    format!(
        "recovery evidence after the black hole: {reconnects} 'streaming binlog' line(s), \
         supervisor_retries +{} since the proved tick, applied={:?}",
        m.supervisor_retries.saturating_sub(retries_at_proof),
        f.eng.applied().map(|p| p.to_string()),
    )
}

/// `timeout{timeout:0}` on the dump link never closes it and never delivers anything, so the only
/// thing that can end the fault is the engine's own bounds (the one place they live now that the
/// reader has no idle timeout of its own): the stream loop's silence detector,
/// `(heartbeat × 3).max(15 s)` = 15 s with `LAZYVIEW_HEARTBEAT_MS=200` (F5), and then
/// `BINLOG_CONNECT_TIMEOUT` on the reconnect attempt. The harness waits for the silence line,
/// then for the first `supervisor_retries` tick of the reconnect's `Policy::CONNECT` run (the
/// attempts log at debug only and the engine's `binlog connect failed` line waits for the whole
/// run, so the counter is the only in-run evidence of the attempt), removes the toxic, and gives
/// the recovery the rest of a CONNECT run plus one cap: the engine may be inside a 20 s attempt
/// or inside the cap wait when the toxic goes away. Recovery is then proven by *delivery* — the
/// `events` counter moving past its value at disarm and `applied` reaching the source's binlog
/// end captured after disarm — not by the `streaming` gauge alone, which is set before the first
/// event is read (the #264 false positive).
#[test]
fn chaos_src_binlog_blackhole() {
    let _one = toxi::exclusive();
    scenario("chaos_src_binlog_blackhole", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_src_binlog_blackhole");
        let mut f = setup(&ctx, "chaos_s2", &toxi, &["binlog"]).await?;
        let mut w = spawn_writers(&f.db, &f.ids, seed);
        f.wait_streaming().await?;
        f.arm("binlog", &Toxic::timeout(0).named("black-hole"))
            .await?;
        // The engine notices on its own: the stream loop's silence detector, then the reconnect
        // attempt that the connect timeout kills (the toxic applies to the new stream too, F5).
        wait_until(
            "the silence detector to declare the stream dead",
            FAULT,
            || {
                let f = &f;
                async move { Ok(f.log_line("no binlog event or heartbeat")?.is_some()) }
            },
        )
        .await
        .with_context(|| {
            format!(
                "the fault never landed: {} of silence across the dump link and the engine never \
                 logged 'no binlog event or heartbeat'",
                FAULT.as_secs()
            )
        })?;
        ensure!(
            !f.streaming(),
            "the fault never landed: the engine still reported itself streaming through a \
             black-holed dump link"
        );
        ctx.log(format!(
            "black hole: {}",
            f.log_line("no binlog event or heartbeat")?
                .unwrap_or_default()
        ));
        // The attempt itself, not the silence line above, is what proves the stream was retried.
        // Each failed CONNECT attempt counts `supervisor_retries` (src/supervisor.rs) and logs
        // at debug only, so the counter is the evidence to wait on; the engine's `binlog connect
        // failed` line appears only after a whole policy run (~attempts × the connect timeout)
        // and never at all once the toxic is gone before the run ends. One failed attempt plus
        // the backoff that follows it bounds the wait for the first tick.
        let m = f.eng.metrics.clone();
        let started = m.snapshot();
        let one_attempt = BH_CONNECT_TIMEOUT + bh_backoff_max(1);
        wait_until("the engine to attempt the reconnect", one_attempt, || {
            let m = m.clone();
            async move { Ok(m.snapshot().supervisor_retries > started.supervisor_retries) }
        })
        .await
        .with_context(|| {
            format!(
                "the black-holed stream was never retried: no supervisor retry within \
                 {one_attempt:?} (one {BH_CONNECT_TIMEOUT:?} connect attempt plus its backoff) \
                 after the silence detector fired"
            )
        })?;
        let ticked = m.snapshot().supervisor_retries;
        ctx.log(format!(
            "reconnect proved: supervisor_retries {} -> {}",
            started.supervisor_retries, ticked
        ));
        f.disarm("binlog", "black-hole").await?;
        // The engine may be anywhere in a CONNECT run when the toxic goes away: inside one
        // attempt (≤ the connect timeout) or inside the one-cap wait that follows a whole
        // failed run (src/engine.rs), so the recovery waits cover the rest of a run — every
        // attempt plus every backoff between attempts — plus one cap, before the run that
        // connects succeeds. All three proofs below share this one budget (#264).
        let connect_run =
            Duration::from_secs(u64::from(BH_CONNECT_ATTEMPTS) * BH_CONNECT_TIMEOUT.as_secs())
                + (1..BH_CONNECT_ATTEMPTS)
                    .map(bh_backoff_max)
                    .sum::<Duration>();
        let reconnect = connect_run + BH_CONNECT_CAP;
        let deadline = Instant::now() + reconnect;
        let left = || {
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1))
        };
        // The proof is delivery, not registration (#264). The `streaming` gauge is set the
        // moment `BinlogReader::connect` returns — registered as a replica, zero events read
        // (src/engine.rs) — and the engine's saved position lags `applied` by the save
        // cadence (soakchaos::saved_position), so the old `f.streaming()` + one-shot
        // `wait_applied(&saved)` + `ensure!(d.events > 0)` could sample the real window
        // between registration and the first counted event (src/engine.rs counts at delivery)
        // and cry "never came back" on a stream that recovers a moment later. What proves
        // recovery instead: the `events` counter (incremented at delivery) moving past its
        // value at disarm, and `applied` reaching the source's binlog end captured *after*
        // disarm — the writers wrote behind the removed toxic the whole time (direct links,
        // §3), so only a stream that came back and delivered can apply it. The streaming
        // gauge stays as the first gate: a stream that never re-registers fails there, and
        // delivery cannot start without it.
        let at_disarm = f.eng.snapshot().events;
        let mut c = f.db.conn().await?;
        let head = server_position(&mut c).await?;
        c.disconnect().await.ok();
        if let Err(e) = wait_until(
            "the engine streaming again after the black hole",
            left(),
            || {
                let f = &f;
                async move { Ok(f.streaming()) }
            },
        )
        .await
        {
            ctx.log(bh_recovery_evidence(&f, ticked));
            return Err(e).with_context(|| {
                "the stream never re-registered after the black hole was removed".to_string()
            });
        }
        if let Err(e) = wait_until("the recovered stream to deliver an event", left(), || {
            let f = &f;
            async move { Ok(f.eng.snapshot().events > at_disarm) }
        })
        .await
        {
            ctx.log(bh_recovery_evidence(&f, ticked));
            return Err(e).with_context(|| {
                format!(
                    "the stream never came back after the black hole was removed: it \
                     re-registered but the events counter stayed at {at_disarm} (its value \
                     at disarm) for the rest of a CONNECT run ({reconnect:?})"
                )
            });
        }
        if let Err(e) = f.eng.wait_applied(&head, left()).await {
            ctx.log(bh_recovery_evidence(&f, ticked));
            return Err(e).with_context(|| {
                format!(
                    "the delivered stream never applied the source's binlog end {head} \
                     captured after the black hole was removed"
                )
            });
        }
        finish(&ctx, &mut f, &mut w, "after the black hole").await?;
        ensure!(
            f.counter("full_rebuilds") == 0,
            "a black-holed dump link cost {} whole rebuild(s)",
            f.counter("full_rebuilds")
        );
        Ok(())
    });
}

/// Issue #248, reproduced on purpose: the dump link stays black-holed **longer than the fixed
/// 180 s a catch-up used to be given**, which is the exact shape of the flake
/// (`exclusive_server_restarts_are_not_fatal` on mariadb130, 1 run in 114: `applied` on the old
/// binlog file, the target on the next one, the engine alive inside a reconnect cycle). The fault
/// is proven to have landed before anything is waited on, and both halves of the proof are
/// assertions in this one run:
///
/// * **red, the old bound**: with the fault still armed, the fixed
///   [`Engine::wait_applied`](`Engine::wait_applied`) deadline expires on a live engine. Had that
///   wait been the catch-up's only bound — as it was — this is where #248 failed.
/// * **green, the engine's own bound**: the same target reached by
///   [`Engine::wait_applied_progressive`], which lets a catch-up run while the engine is alive and
///   not stalled past what `engine_bounds` says a living engine can spend.
///
/// The engine is then verified exact, and no incremental view may have taken a whole rebuild.
#[test]
fn chaos_src_binlog_catchup_after_a_slow_reconnect() {
    let _one = toxi::exclusive();
    scenario(
        "chaos_src_binlog_catchup_after_a_slow_reconnect",
        |ctx| async move {
            let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
                return Ok(());
            };
            let seed = ctx.sub_seed("chaos_src_binlog_catchup_after_a_slow_reconnect");
            let mut f = setup(&ctx, "chaos_s9", &toxi, &["binlog"]).await?;
            let mut w = spawn_writers(&f.db, &f.ids, seed);
            f.wait_streaming().await?;
            f.arm("binlog", &Toxic::timeout(0).named("black-hole"))
                .await?;
            wait_until(
                "the silence detector to declare the stream dead",
                FAULT,
                || {
                    let f = &f;
                    async move { Ok(f.log_line("no binlog event or heartbeat")?.is_some()) }
                },
            )
            .await
            .with_context(|| {
                format!(
                "the fault never landed: {} of silence across the dump link and the engine never \
                 logged 'no binlog event or heartbeat'",
                FAULT.as_secs()
            )
            })?;
            ensure!(
                !f.streaming(),
                "the fault never landed: the engine still reported itself streaming through a \
             black-holed dump link"
            );
            // The backlog the catch-up has to bring over: written behind the fault, so the position to
            // reach is ahead of the frozen one by construction, exactly as in #248 (the writers keep
            // going too, as they did in the restart test that flaked).
            let (stmts, bytes) = items_txn(&f.ids, 400);
            let mut c = f.db.conn().await?;
            for s in &stmts {
                exec(&mut c, s).await?;
            }
            let target = server_position(&mut c).await?;
            c.disconnect().await.ok();
            let frozen = f
                .eng
                .applied()
                .context("the engine had not applied a binlog position before the fault")?;
            ensure!(
                target > frozen,
                "the reproduction did not open a backlog: applied {frozen} already covers the \
             source's {target}"
            );
            // --- red: the fixed bound, on a live engine, with the fault still armed.
            let floor = t_catchup();
            let t = std::time::Instant::now();
            let red = f.eng.wait_applied(&target, floor).await;
            let expired = t.elapsed();
            ensure!(
            red.is_err(),
            "the reproduction is not one: the fixed bound of {floor:?} caught the engine anyway \
             ({expired:?}), so no catch-up of this shape could have flaked — #248's premise needs \
             re-reading. Underlying error: {red:#?}"
        );
            f.eng.check_alive()?;
            ensure!(
                !f.streaming() && f.eng.applied().as_ref() == Some(&frozen),
                "the engine was not frozen inside the fixed bound as #248 was: streaming={}, \
             applied={:?}, expected {frozen} and no stream",
                f.streaming(),
                f.eng.applied(),
            );
            ctx.log(format!(
            "reproduced #248: the fixed catch-up bound expired at {expired:?} on a live engine \
             frozen at {frozen} (target {target}, {} bytes of row events behind the fault)",
            bytes
        ));
            // --- green: the same target, bounded by the engine's own numbers. The toxic is removed
            // here, not before: the engine may still be inside a `Policy::CONNECT` run, and the wait
            // that has to survive it is the one under test.
            f.disarm("binlog", "black-hole").await?;
            let green = std::time::Instant::now();
            f.eng
                .wait_applied_progressive(&target, floor)
                .await
                .with_context(|| {
                    format!(
                    "the progress-aware catch-up gave up on an engine that was only reconnecting: \
                     frozen at {frozen} for {expired:?} under the fault and the engine's own bound \
                     is {:?} (engine_bounds::stall)",
                    engine_bounds::stall(engine_bounds::default_target_timeout())
                )
                })?;
            ctx.log(format!(
                "caught up {floor:?} past the old bound: applied {frozen} -> {target} in {:?} \
             from the fault's end",
                green.elapsed()
            ));
            finish(&ctx, &mut f, &mut w, "after the slow reconnect").await?;
            ensure!(
                f.counter("full_rebuilds") == 0,
                "a black-holed dump link cost {} whole rebuild(s) of the engine",
                f.counter("full_rebuilds")
            );
            Ok(())
        },
    );
}

/// All three source roles behind one proxy with `latency{120,60}` both ways and `bandwidth{64}`
/// both ways: the view must lag while the fault is armed and return to ~0 after it is removed,
/// without ever losing exactness. Both samples are bounded waits on an observable, never a sleep
/// (§2.5).
#[test]
fn chaos_src_slow_recovers() {
    let _one = toxi::exclusive();
    scenario("chaos_src_slow_recovers", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_src_slow_recovers");
        let mut f = setup(&ctx, "chaos_s3", &toxi, &["src"]).await?;
        // The fault first: the load then has to live inside it for its whole rise.
        for (name, t) in [
            ("lat-down", Toxic::latency(120, 60, Dir::Downstream)),
            ("lat-up", Toxic::latency(120, 60, Dir::Upstream)),
            ("bw-down", Toxic::bandwidth(64, Dir::Downstream)),
            ("bw-up", Toxic::bandwidth(64, Dir::Upstream)),
        ] {
            f.arm("src", &t.named(name)).await?;
        }
        let mut w = spawn_writers(&f.db, &f.ids, seed);
        // Lag grows: wait until the binlog gap is measurably open (a floor the unthrottled engine
        // closes in well under a second).
        const FLOOR: u64 = 8_192;
        wait_until(
            "the lag to open under the throttled source",
            FAULT,
            || async { Ok(f.gap_bytes().await? >= FLOOR) },
        )
        .await
        .with_context(|| {
            format!(
                "the fault never landed: with latency 120/60 and bandwidth 64 KB/s on every source \
                 role, the applied position never fell {FLOOR} bytes behind the server"
            )
        })?;
        let armed = f.gap_bytes().await?;
        ctx.log(format!(
            "lag under the fault: {armed} bytes of binlog behind the server"
        ));
        for name in ["lat-down", "lat-up", "bw-down", "bw-up"] {
            f.disarm("src", name).await?;
        }
        // And falls back to nothing: the applied position catches the server.
        wait_until(
            "the lag to close after the fault was removed",
            FAULT,
            || async { Ok(f.gap_bytes().await? == 0) },
        )
        .await
        .with_context(|| {
            format!(
                "the lag never closed: it was {armed} bytes open when the toxics were removed and \
                 was still open after {} s",
                FAULT.as_secs()
            )
        })?;
        finish(&ctx, &mut f, &mut w, "after the throttled source recovered").await?;
        ensure!(
            f.counter("full_rebuilds") == 0,
            "a slow source cost {} whole rebuild(s) of an incremental view",
            f.counter("full_rebuilds")
        );
        ctx.log(format!(
            "lag sampled at {armed} bytes while armed, 0 after the removal"
        ));
        Ok(())
    });
}

/// `limit_data{bytes:B}` inside the first big row event of the dump link: the stream dies in the
/// middle of one binlog event, the engine reconnects and re-reads the event **whole** from the
/// persisted position. `B` is half the byte length of the block the scenario is about to write,
/// derived from the seed (`items_txn`), never guessed at run time.
#[test]
fn chaos_src_limit_data_mid_binlog_event() {
    let _one = toxi::exclusive();
    scenario("chaos_src_limit_data_mid_binlog_event", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_src_limit_data_mid_binlog_event");
        let mut f = setup(&ctx, "chaos_s4", &toxi, &["binlog"]).await?;
        let mut w = spawn_writers(&f.db, &f.ids, seed);
        f.wait_streaming().await?;
        quiesce_writers(&mut w).await?; // only the dump link is busy from here
        let (stmts, payload) = items_txn(&f.ids, 5_000);
        let budget = (payload / 2).max(4_096) as u64;
        f.arm(
            "binlog",
            &Toxic::limit_data(budget, Dir::Downstream).named("cut"),
        )
        .await?;
        let mut c = f.db.conn().await?;
        txn(&mut c, &stmts, false).await?;
        // The cut landed inside the block: the link died while the event was still coming, so the
        // applied position is behind the server's by part of the block.
        let m = f.eng.metrics.clone();
        wait_until("the dump stream to be cut inside the event", FAULT, || {
            let m = m.clone();
            async move { Ok(m.gauges.streaming.load(Ordering::Relaxed) == 0) }
        })
        .await
        .with_context(|| {
            format!(
                "the fault never landed: {payload} bytes of row events crossed the dump link with \
                 limit_data{{bytes:{budget}}} armed and the stream was never closed"
            )
        })?;
        let gap = f.gap_bytes().await?;
        ensure!(
            gap > 0,
            "the cut never landed inside the event: the engine had already applied the whole \
             {payload}-byte block when the stream closed (gap {gap})"
        );
        c.disconnect().await.ok();
        f.disarm("binlog", "cut").await?;
        f.wait_streaming().await?;
        let mut c = f.db.conn().await?;
        let head = server_position(&mut c).await?;
        c.disconnect().await.ok();
        f.eng.wait_applied(&head, t_catchup()).await?;
        ctx.log(format!(
            "limit_data: cut at {budget} of {payload} event bytes with {gap} bytes unapplied, \
             re-read whole from the persisted position"
        ));
        // A half-decoded event would show as a wrong count here; `refresh_errors` must stay at 0,
        // because a stream cut is not a refresh failure (the plan's scenario 4).
        finish(&ctx, &mut f, &mut w, "after the mid-event cut").await?;
        ensure!(
            f.counter("refresh_errors") == 0,
            "a cut binlog stream was counted as {} refresh failure(s); a stream cut is not one",
            f.counter("refresh_errors")
        );
        ensure!(
            f.counter("full_rebuilds") == 0,
            "a mid-event cut cost {} whole rebuild(s)",
            f.counter("full_rebuilds")
        );
        Ok(())
    });
}

/// `limit_data` inside the *result set* of a refresh's lookup query (`lv_<s>_query`, downstream):
/// the lookup dies at the network edge, the refresh is retried with backoff and must never apply a
/// partial result. `reg_failing_refresh_retries_without_rebuild` (`availability.rs:186`) is the
/// model for the retry-without-rebuild assertion; the fan-out is the `links` hop of §3.
#[test]
fn chaos_src_refresh_result_cut() {
    let _one = toxi::exclusive();
    scenario("chaos_src_refresh_result_cut", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_src_refresh_result_cut");
        let mut f = setup(&ctx, "chaos_s5", &toxi, &["query"]).await?;
        let mut w = spawn_writers(&f.db, &f.ids, seed);
        f.wait_streaming().await?;
        quiesce_writers(&mut w).await?; // writers quiesced: only the refresh's link is busy
                                        // A change set that makes the hop view's lookup read thousands of rows: a fan-out of link
                                        // rows over the seeded entities, one transaction. `B` is a quarter of the bytes that go in,
                                        // derived from the same statements (never a magic guess).
        const FANOUT: usize = 8_000;
        let entities = f.ids.seed_entities.max(2) as usize;
        let from = f.ids.link_seq.fetch_add(FANOUT as i64, Ordering::Relaxed) + 1;
        let mut vals = Vec::with_capacity(FANOUT);
        let mut payload = 0usize;
        for k in 0..FANOUT {
            let row = format!(
                "(1, {}, {}, 'fan')",
                (k % (entities - 1)) + 2,
                from + k as i64
            );
            payload += row.len();
            vals.push(row);
        }
        let stmts = vec![format!(
            "INSERT INTO links (src_id, dst_id, seq, kind) VALUES {}",
            vals.join(",")
        )];
        f.arm(
            "query",
            &Toxic::limit_data((payload / 4).max(4_096) as u64, Dir::Downstream).named("cut"),
        )
        .await?;
        let mut c = f.db.conn().await?;
        txn(&mut c, &stmts, false).await?;
        c.disconnect().await.ok();
        // The refresh fails at the network edge: the counter the engine keeps for it must move.
        wait_until("the refresh to fail on the cut result set", FAULT, || {
            let f = &f;
            async move { Ok(f.counter("refresh_errors") >= 1) }
        })
        .await
        .with_context(|| {
            format!(
                "the fault never landed: {payload} bytes of fan-out committed with \
                 limit_data{{bytes:{}}}|4 armed on the lookup link and no refresh failed",
                payload
            )
        })?;
        ctx.log(format!(
            "the lookup's result set was cut at {} bytes of a {payload}-byte fan-out: {}",
            (payload / 4).max(4_096),
            f.log_line("refresh failed")?
                .unwrap_or_else(|| "no log line".into())
        ));
        f.disarm("query", "cut").await?;
        // The retry applies what the failed refresh never may have applied: a partial result.
        finish(&ctx, &mut f, &mut w, "after the refresh was retried").await?;
        ensure!(
            f.counter("refresh_errors") >= 1,
            "the cut refresh never reached refresh_errors"
        );
        ensure!(
            f.counter("full_rebuilds") == 0,
            "a failed refresh escalated to {} whole rebuild(s); it must be retried, never \
             rebuilt (the rule of reg_failing_refresh_retries_without_rebuild)",
            f.counter("full_rebuilds")
        );
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// The alert-state scenario of PR O-2 (review A-03 "Remains", work list #1 / O-01+O-02): the
// state in which the process keeps running while a view stops advancing — a refresh that fails
// on every attempt — must be visible on the scrape. `lazyview_view_refresh_failures` climbs to
// the shipped rule's threshold while `ready`, `/readyz` and `lazyview_views_error` say nothing;
// `lazyview_refresh_errors_total` (the backstop rule's series) keeps climbing; the next
// success clears the streak (src/stats.rs, src/engine.rs `publish_stats`).
// ---------------------------------------------------------------------------------------------

/// The `max` of the `name{view="…"}` series of a `/metrics` body (`None` when the scrape has
/// none): the left-hand side of the shipped rule `LazyviewRefreshFailing`
/// (`max by (namespace,job,view)(lazyview_view_refresh_failures)`), evaluated over one live
/// scrape the way Prometheus would.
fn view_metric_max(body: &str, name: &str) -> Option<f64> {
    let prefix = format!("{name}{{view=\"");
    let mut best: Option<f64> = None;
    for line in body.lines() {
        let Some(rest) = line.strip_prefix(&prefix) else {
            continue;
        };
        let Some((_, v)) = rest.rsplit_once(' ') else {
            continue;
        };
        if let Ok(v) = v.trim().parse::<f64>() {
            best = Some(best.map_or(v, |b| b.max(v)));
        }
    }
    best
}

/// One unlabeled series (`name value`) of a `/metrics` body: the instance gauges and counters
/// the health endpoint exports (`name` matches exactly — the trailing space keeps
/// `lazyview_views` from matching `lazyview_views_ready`).
fn metric(body: &str, name: &str) -> Option<f64> {
    let exact = format!("{name} ");
    body.lines().find_map(|l| {
        l.strip_prefix(&exact)?
            .split_whitespace()
            .next()?
            .parse::<f64>()
            .ok()
    })
}

/// The delta of one budget-table counter since a window opened (the table of `Fix::counter`,
/// readable while the test still owns the engine handle).
fn counter_after(eng: &Engine, window: &Window, name: &str) -> u64 {
    counters(eng, &window.0)
        .into_iter()
        .find(|(n, _)| *n == name)
        .map_or(0, |(_, v)| v)
}

#[test]
fn chaos_refresh_failing_while_ready_is_visible() {
    let _one = toxi::exclusive();
    scenario("chaos_refresh_failing_while_ready", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        refresh_failing_while_ready(&ctx, &toxi).await
    });
}

/// The review's state, made to happen: the refresh's own read of the source dies mid-result-set
/// on every attempt, and the view keeps its live version — retried with backoff on the same
/// changes, never re-planned, never rebuilt (owner rule). Everything the probes and the six old
/// rules read must stay green while the new gauge and the backstop counter climb; after the link
/// recovers the gauge must clear and the oracle must be exact.
async fn refresh_failing_while_ready(ctx: &Ctx, toxi: &Toxi) -> Result<()> {
    let tag = "chaos_ref_fail";
    let db = ctx.new_db(tag).await?;
    let _clean = toxi.for_schema(&db.name); // the §2.2 guard, before the first proxy
    let ids = setup_soak(&db, ctx.sub_seed(tag), small_sizes()).await?;
    let t1 = ctx.new_db(&format!("{tag}_t1")).await?;
    // The narrowed neutral set on two direct targets: the fault is only the engine's read link,
    // so a failing refresh must still leave everything else advancing.
    let specs: Vec<ViewSpec> = chaos_views()?
        .into_iter()
        .map(|s| {
            if s.kind == Kind::Chained {
                s
            } else {
                s.with_targets("source,t1")
            }
        })
        .collect();
    let p = toxi
        .proxy(&db.name, "query", &toxi::upstream(&db.flavor))
        .await?;
    toxi.wait_listening(&p).await?;
    let qdsn = dsn_via_proxy(&engine_dsn(&db), p.listen)?;
    let mut env: Vec<(String, String)> = target_env(&[("t1", t1.dsn.as_str())]);
    for (k, v) in [
        // The same knobs `setup` pins: the engine's own timeouts decide how long a fault lasts.
        ("LAZYVIEW_HEARTBEAT_MS", "200"),
        ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
        ("LAZYVIEW_TEST_QUERY_DSN", qdsn.as_str()),
        // Pinned to the mode production runs: the refresh's chunk reads then go through the
        // supervisor's pools, whose reads are bounded, so one cut costs one failed attempt and the
        // engine's loop returns to the top instead of waiting on one dead read.
        ("LAZYVIEW_SUPERVISOR", "adaptive"),
    ] {
        env.push((k.to_string(), v.to_string()));
    }
    let health = free_local_addr()?;
    env.push(("LAZYVIEW_HEALTH_ADDR".to_string(), health.clone()));
    let (mut eng, _) = start_with_views(ctx, &db, &specs, &ov_refs(&env)).await?;
    let mut c = db.conn().await?;
    eng.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    let window = Window::now(&eng);
    let mut w = spawn_writers(&db, &ids, ctx.sub_seed(tag));
    soakchaos::wait_streaming(&mut eng).await?;
    let errs0 = counter_after(&eng, &window, "refresh_errors");
    // The fault: the refresh's read of the source dies mid-result-set, on every attempt and on
    // every retry. `limit_data` passes `bytes` of the downstream and then ends the stream, and its
    // budget belongs to the connection, so a reconnect does not escape it; a failed refresh keeps
    // its changes for the next attempt (src/engine.rs `live_end`), so the retry reads the same rows
    // and dies in the same place — the streak of `REFRESH_ERROR_SHOWN_AFTER` forms on the backoff
    // ladder. Two faults were tried here first and are the wrong fault on this link, both for the
    // same reason: they kill the flush's *fence* read as well, and a flush that cannot take its
    // fence never attempts a refresh at all, so `refresh_failures` would never move — the link
    // outage (`toxi.disable`, every connect refused) and the black hole (`timeout{timeout:0}`,
    // §2.6, and these reads carry no timeout of their own).
    // The fan-out is what makes the read big enough to die, and the budget is what makes it die
    // EVERY time: the refresh's source reads are chunked through the supervisor's pools
    // (`chunk_size` ≤ `LAZYVIEW_QUERY_CHUNK` = 1000 rows, ≥ `supervisor::MIN_CHUNK`,
    // src/supervisor.rs), so the budget must sit under the bytes of the SMALLEST chunked read,
    // not under the bytes of the whole result. S-5's quarter-of-the-result budget (tried here
    // first) fits every single chunk — a chunk read lands on a pool connection of its own with
    // a fresh budget — so the chunked retries complete between the cuts and the streak never
    // forms: on mariadb123 in CI run 44474008134 that was 71 failed refreshes, no view past
    // attempt 2. A quarter of ONE fan row's bytes times `MIN_CHUNK` (derived from the same
    // statements again, never a magic guess), floored at what the flush's fence reads still
    // need to pass through (a few hundred bytes of `SHOW MASTER STATUS` /
    // `SELECT @@gtid_binlog_pos`): every chunked read of the held fan rows (≥ 100 rows, ≈ 1.5 KB
    // on the wire) crosses the budget in its first result packet and the attempt dies.
    const FANOUT: usize = 8_000;
    let entities = ids.seed_entities.max(2) as usize;
    let fan_rows = |n: usize, from: i64| {
        let mut vals = Vec::with_capacity(n);
        let mut payload = 0usize;
        for k in 0..n {
            let row = format!(
                "(1, {}, {}, 'fan')",
                (k % (entities - 1)) + 2,
                from + k as i64
            );
            payload += row.len();
            vals.push(row);
        }
        let stmt = format!(
            "INSERT INTO links (src_id, dst_id, seq, kind) VALUES {}",
            vals.join(",")
        );
        (stmt, payload)
    };
    let (stmt, payload) = fan_rows(
        FANOUT,
        ids.link_seq.fetch_add(FANOUT as i64, Ordering::Relaxed) + 1,
    );
    let per_row = (payload / FANOUT).max(1) as u64;
    let cut = (per_row * lazyview::supervisor::MIN_CHUNK as u64 / 4).max(4_096);
    toxi.arm(&p, &Toxic::limit_data(cut, Dir::Downstream).named("cut"))
        .await?;
    let mut c = db.conn().await?;
    txn(&mut c, &[stmt], false).await?;
    c.disconnect().await.ok();
    // 1. The rule's condition forms: the per-view streak reaches `REFRESH_ERROR_SHOWN_AFTER`
    //    (3 = the threshold in `prometheusrule.yaml`). A smaller fan-out every ~2 s keeps the
    //    pending change set big, so no small retry can slip through and reset the streak.
    let polls = std::sync::atomic::AtomicU32::new(0);
    wait_until(
        "a view's failure streak to reach the rule threshold (lazyview_view_refresh_failures >= 3)",
        FAULT_BUDGET,
        || async {
            let (_, body) = http_get(&health, "/metrics").await?;
            let streak = view_metric_max(&body, "lazyview_view_refresh_failures").unwrap_or(0.0);
            let top_up = FANOUT / 4;
            if streak < 3.0 && polls.fetch_add(1, Ordering::Relaxed) % 40 == 39 {
                let (stmt, _) = fan_rows(
                    top_up,
                    ids.link_seq.fetch_add(top_up as i64, Ordering::Relaxed) + 1,
                );
                let mut c = db.conn().await?;
                txn(&mut c, &[stmt], false).await?;
                c.disconnect().await.ok();
            }
            Ok(streak >= 3.0)
        },
    )
    .await
    .with_context(|| {
        format!(
            "the fault never landed on the refresh's read link: {} failed refreshes without \
             any view reaching the streak of 3",
            counter_after(&eng, &window, "refresh_errors")
        )
    })?;
    // 2. One scrape, both shipped rule exprs evaluated on it, and the blind spot itself:
    //    everything the probes and the old rules read still says the instance is healthy.
    let (status, body) = http_get(&health, "/metrics").await?;
    ensure!(status == 200, "/metrics answered {status}");
    ensure!(
        view_metric_max(&body, "lazyview_view_refresh_failures").unwrap_or(0.0) >= 3.0,
        "the scraped streak dropped below the threshold between the wait and this scrape"
    );
    let views = metric(&body, "lazyview_views").context("no lazyview_views in the scrape")?;
    let ready = metric(&body, "lazyview_views_ready").context("no lazyview_views_ready")?;
    let in_error = metric(&body, "lazyview_views_error").context("no lazyview_views_error")?;
    ensure!(
        views >= 1.0 && ready == views && in_error == 0.0,
        "a refresh that fails forever must not move `ready` or `views_error` (that is the \
         blind spot this alert exists for): views={views} ready={ready} error={in_error}"
    );
    ensure!(
        metric(&body, "lazyview_streaming") == Some(1.0),
        "only the refresh's read link is cut: the dump link must still be streaming"
    );
    let (status, _) = http_get(&health, "/readyz").await?;
    ensure!(
        status == 200,
        "/readyz while a refresh fails forever: {status}"
    );
    // The backstop rule's LHS is `increase(lazyview_refresh_errors_total[30m])`: over the one
    // scrape it is that instance counter having moved. Both sides are read — the scraped series
    // exists and is above 0, and the engine's own tally moved by at least the 3 attempts the
    // streak of step 1 implies (`errs0` is the tally before the outage).
    let scraped = metric(&body, "lazyview_refresh_errors_total");
    let attempts = counter_after(&eng, &window, "refresh_errors");
    ensure!(
        scraped.unwrap_or(0.0) > 0.0 && attempts >= errs0 + 3,
        "the backstop rule's series (increase(lazyview_refresh_errors_total[30m]) > 0) did not \
         climb: scraped {scraped:?}, {attempts} failed refreshes in the window (before: {errs0})"
    );
    // 3. From the 3rd failure in a row the state rows show the error (the engine writes it at
    //    exactly the rule's threshold, `REFRESH_ERROR_SHOWN_AFTER`).
    wait_until(
        "_lazyview_state.last_error to show the failing refresh",
        FAULT_BUDGET,
        || async {
            let mut c = db.conn().await?;
            let n: Option<i64> = c
                .query_first(
                    "SELECT COUNT(*) FROM _lazyview_state WHERE last_error LIKE 'refresh failed%'",
                )
                .await?;
            c.disconnect().await.ok();
            Ok(n.unwrap_or(0) > 0)
        },
    )
    .await?;
    // 4. The resolve side: clear the cut, the next success clears the streak, and what the failed
    //    attempts kept is applied — never by a rebuild.
    toxi.remove_toxic(&p, "cut").await?;
    wait_until(
        "the streak to clear once refreshes succeed again",
        FAULT_BUDGET,
        || async {
            let (_, body) = http_get(&health, "/metrics").await?;
            Ok(view_metric_max(&body, "lazyview_view_refresh_failures").unwrap_or(0.0) == 0.0)
        },
    )
    .await?;
    quiesce_writers(&mut w).await?;
    let mut c = db.conn().await?;
    eng.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmap(&t1),
        &HashMap::new(),
        &specs,
        "after the refresh link recovered",
    )
    .await?;
    ensure!(
        counter_after(&eng, &window, "full_rebuilds") == 0,
        "a failing refresh escalated to a whole rebuild; it must be retried, never rebuilt \
         (the rule of reg_failing_refresh_retries_without_rebuild)"
    );
    ctx.log(format!(
        "failing-while-ready visible: {} failed refreshes behind a green readyz",
        counter_after(&eng, &window, "refresh_errors")
    ));
    eng.check_alive()?;
    eng.stop().await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The target-side scenarios (plan §3 rows 6–9): faults on the write link of a target and
// fragmentation on both sides at once. Everything the engine does to a target goes through that
// target's proxy (`lv_<schema>_t1`); the harness — DDL, seed, writers, oracle — stays on direct
// connections (F4/Q8), so every fault is attributable to the engine's own target traffic.
// ---------------------------------------------------------------------------------------------

/// The views the target-side scenarios exercise (§3 "common shape", narrowed from `soak_views()`
/// to the paths at issue): a direct `WHERE` filter, a three-way join, a `links` hop, a
/// `sets_items` cascade child and a global aggregate — all incremental — plus one `Full` view,
/// which the per-view no-rebuild rule of the soak plan (F5/F6) keeps out. `targets` is the
/// (view, target) fan-out, e.g. `"source,t1"`.
fn chaos_specs(targets: &str) -> Vec<ViewSpec> {
    const WANT: [&str; 6] = [
        "w_items_direct",
        "w_item_entity_grp",
        "w_hop_partners",
        "wc_child_only",
        "w_global_entities",
        "w_union_kinds",
    ];
    soak_views()
        .into_iter()
        .filter(|s| WANT.contains(&s.name.as_str()))
        .map(|s| s.with_targets(targets))
        .collect()
}

/// The per-view no-rebuild rule (F6): `mode = 'incremental'` ⇒ `whole_view_refreshes == 0`, and
/// every whole rebuild the engine did since `before` (a snapshot taken after the initial
/// `verify_targets`, so the first build is outside the window) is accounted for by the Full-mode
/// view — the same rule `scripts/stats-check.sh` enforces, with the one `Full` view kept out of
/// it exactly as the soak plan's F5/D5 keeps a `Chained`/`Full` view out.
fn assert_no_rebuilds(
    eng: &Engine,
    db: &Db,
    specs: &[ViewSpec],
    before: &Snapshot,
    what: &str,
) -> Result<()> {
    let d = eng.snapshot().since(before);
    let mut full_whole = 0u64;
    for s in specs {
        let vs = eng.stats_of(db, &s.name)?;
        if vs.mode.eq_ignore_ascii_case("incremental") {
            ensure!(
                vs.whole_view_refreshes == 0,
                "{what}: incremental view {} was rebuilt {} time(s) outside the first build",
                s.name,
                vs.whole_view_refreshes
            );
        } else {
            full_whole += vs.whole_view_refreshes;
        }
    }
    ensure!(
        d.full_rebuilds <= full_whole,
        "{what}: engine full_rebuilds delta {} exceeds the {} whole refreshes of the Full-mode \
         views — an incremental view was rebuilt, not refreshed",
        d.full_rebuilds,
        full_whole
    );
    Ok(())
}

/// Did the engine itself notice that a write to target `{target}` died at the network edge? Its
/// log is the witness when the counters do not move (the plan's #6: the *fate* of the write must
/// become known to the engine, through the fence, a refresh error, a supervisor retry or its own
/// log). The needles cover the error classes of both executors (T8, plan §6): the MySQL driver's
/// resets and closes, and the PostgreSQL one's — `tokio_postgres` reports a cut extended-protocol
/// stream as a closed connection or a `db error`, and the PG zombie fence kills the lost backend
/// with `pg_terminate_backend`, whose message reaches the write as "terminating connection".
fn target_write_error_logged(log_path: &std::path::Path, target: &str) -> bool {
    let Ok(text) = std::fs::read_to_string(log_path) else {
        return false;
    };
    text.lines().any(|l| {
        l.contains(target)
            && [
                "reset by peer",
                "connection closed",
                "connection reset",
                "broken pipe",
                "io error",
                "os error 104",
                "server closed the connection unexpectedly",
                "db error",
                "terminating connection",
            ]
            .iter()
            .any(|needle| l.contains(needle))
    })
}

/// `t1` → its schema, the shape `verify_targets` takes for one target.
fn tmap(t1: &Db) -> HashMap<String, Db> {
    [("t1".to_string(), t1.clone())].into()
}

/// The engine's own report that target `{target}` became too slow to keep up (T5 §3 row 8's
/// declared observables, F2: the test names what proves the toxic landed): a supervisor retry on a
/// write that `timed out`, a `writing to target {target}` / `write failed: target {target}` line,
/// or a `slow flush` of seconds while the link is capped. Each is attributable to the armed
/// toxics; none of them requires the target to have been declared behind.
fn slow_target_reported(log_path: &std::path::Path, target: &str) -> bool {
    let Ok(text) = std::fs::read_to_string(log_path) else {
        return false;
    };
    text.lines().any(|l| {
        (l.contains(target)
            && [
                "timed out after",
                &format!("writing to target {target}")[..],
                &format!("write failed: target {target}")[..],
            ]
            .iter()
            .any(|needle| l.contains(needle)))
            || l.contains("slow flush")
    })
}

/// The engine's own source DSN (the suite's least-privilege user included, as `start_engine`
/// does) routed through a proxy — what a T3 role hook is set to (§2.4).
fn proxied_engine_dsn(db: &Db, listen: u16) -> Result<String> {
    let base = match std::env::var("LAZYVIEW_TEST_ENGINE_USER") {
        Ok(u) if !u.is_empty() => dsn_with_user(&db.dsn, &u),
        _ => db.dsn.clone(),
    };
    dsn_via_proxy(&base, listen)
}

/// Pushes bounded pressure onto the target's write link from direct connections and hands back
/// the stop handle plus its join handle; the caller stops and joins before the final oracle.
fn spawn_target_writers(
    ctx: &Ctx,
    db: &Db,
    ids: Arc<SoakIds>,
    label: &str,
    tps: u32,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<Result<usize>>) {
    let (tx, rx) = watch::channel(false);
    let (db_w, ids_w) = (db.clone(), ids);
    let seed = ctx.sub_seed(label);
    let h = tokio::spawn(async move { run_paced(&db_w, ids_w, seed, 4, tps, rx).await });
    (tx, h)
}

async fn stop_writers(
    ctx: &Ctx,
    tx: watch::Sender<bool>,
    h: tokio::task::JoinHandle<Result<usize>>,
) -> Result<usize> {
    let _ = tx.send(true);
    let n = h.await.context("the paced writers task panicked")??;
    ctx.log(format!(
        "writers: {n} transactions through direct connections"
    ));
    Ok(n)
}

/// Every fault wait: the engine's own bounds (F5: a 2 s target timeout plus retry backoff) decide
/// how long a fault takes to become observable; this only outlasts them.
const FAULT_BUDGET: Duration = Duration::from_secs(90);

/// The engine's target name for the PostgreSQL family (T8, plan §6): `LAZYVIEW_TARGET_PG_DSN`,
/// the proxy `lv_<s>_pgt1` — the shape the plan fixes for the PG target scenarios.
const PG_TARGET: &str = "pg";

/// #6 (§3), parameterised over the target family (T8, plan §6): `lv_<s>_<t1|pgt1>` **upstream**
/// `reset_peer` while a flush is writing. The reset is armed with `timeout:300` (F2: "a TCP RST
/// immediately **or after** `timeout`" — a non-zero timeout is what lets the write transaction
/// get into flight before the RST cuts it, the plan's "batch big enough that the write transaction
/// crosses the reset"); `LAZYVIEW_TARGET_TIMEOUT_MS=2000` and `LAZYVIEW_SUPERVISOR=fixed` per F5.
/// The write's fate is genuinely unknown to the engine; the zombie fence (#28), a refresh error, a
/// supervisor retry or the engine's own log must observe it, no row of a half-applied transaction
/// may survive on the target, and the retry from the kept changes must land exactly. The MV row
/// counts of the target right after the reset are logged — the evidence for the finding the PR
/// body records (T5's step). `pg_server: Some` runs the identical fault against a PostgreSQL
/// target (T8): the write path is then `target/postgres`'s own pool and its zombie fence
/// (`src/target/postgres.rs`), the shares of which are the same supervisor counters, and the log
/// witness accepts PG's own error classes (`target_write_error_logged`).
async fn tgt_reset(ctx: &Ctx, toxi: &Toxi, pg_server: Option<&pg::Server>) -> Result<()> {
    let (tag, tname) = match pg_server {
        Some(_) => ("chaos_tgt_pg_reset", PG_TARGET),
        None => ("chaos_tgt_reset", "t1"),
    };
    let db = ctx.new_db(tag).await?;
    // The guard first: no armed proxy can outlive a panic on a shared server (§2.2).
    let _clean = toxi.for_schema(&db.name);
    let (base, upstream, t1, pgdb) = match pg_server {
        Some(s) => {
            let pgdb = pg::new_pg_db(s, "cx_tpgr").await?;
            (pgdb.dsn.clone(), toxi::pg_upstream(), None, Some(pgdb))
        }
        None => {
            let t1 = ctx.new_db(&format!("{tag}_t1")).await?;
            (t1.dsn.clone(), toxi::upstream(&db.flavor), Some(t1), None)
        }
    };
    let ids = setup_soak(&db, ctx.sub_seed(tag), Sizes::tiny()).await?;
    let specs = chaos_specs(&format!("source,{tname}"));
    let p1 = toxi
        .proxy(
            &db.name,
            if pgdb.is_some() { "pgt1" } else { "t1" },
            &upstream,
        )
        .await?;
    toxi.wait_listening(&p1).await?;
    let mut ov = toxi::proxied_target_env(&[(tname, base.as_str(), Some(&p1))])?;
    for (k, v) in [
        ("LAZYVIEW_HEARTBEAT_MS", "200"),
        ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
        ("LAZYVIEW_SUPERVISOR", "fixed"),
    ] {
        ov.push((k.to_string(), v.to_string()));
    }
    let (mut eng, _) = start_with_views(ctx, &db, &specs, &ov_refs(&ov)).await?;
    let tmysql = match &t1 {
        Some(t1) => tmap(t1),
        None => HashMap::new(),
    };
    let tpg: HashMap<String, pg::PgDb> = match &pgdb {
        Some(pgdb) => [(tname.to_string(), pgdb.clone())].into(),
        None => HashMap::new(),
    };
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmysql,
        &tpg,
        &specs,
        &format!("initial build on {tname}"),
    )
    .await?;
    let before = eng.snapshot();
    // Arm on the target's link (the read-back says it landed), then drive flushes across it.
    toxi.arm(&p1, &Toxic::reset_peer(300).on(Dir::Upstream).named("rst"))
        .await?;
    let (tx, writers) = spawn_target_writers(ctx, &db, ids.clone(), &format!("{tag}_w"), 60);
    // A cut write is never a silent success: the fence kills the lost backend, the flush
    // fails as a refresh error, the supervisor retries it, or the engine logs the lost
    // connection — one of them must move (§2.5, "the fault never landed").
    wait_until(
        "the reset target write to be observed (the fault never landed)",
        FAULT_BUDGET,
        || {
            let m = eng.metrics.clone();
            let lp = eng.log_path.clone();
            async move {
                let s = m.snapshot();
                Ok(s.supervisor_zombie_kills > before.supervisor_zombie_kills
                    || s.refresh_errors > before.refresh_errors
                    || s.supervisor_retries > before.supervisor_retries
                    || target_write_error_logged(&lp, tname))
            }
        },
    )
    .await?;
    // Evidence for the finding: which side of the reset the server ended up on — the row
    // count of every target MV while the fault is still armed.
    for s in &specs {
        let n = match &pgdb {
            Some(pgdb) => {
                let client = pgdb.client().await?;
                let t = pg::fetch(&client, &pgdb.schema, &s.name).await?;
                drop(client);
                t.rows.len() as i64
            }
            None => {
                let mut tc = t1
                    .as_ref()
                    .context("the t1 handle of the MySQL family")?
                    .conn()
                    .await?;
                let rows: Vec<mysql_async::Row> = tc
                    .query(format!("SELECT COUNT(*) FROM `{}`", s.name))
                    .await?;
                rows.first().and_then(|r| r.get::<i64, _>(0)).unwrap_or(-1)
            }
        };
        ctx.log(format!(
            "{tag}: {tname} MV `{}` row count right after the reset: {n}",
            s.name
        ));
    }
    toxi.remove_toxic(&p1, "rst").await?;
    // Removal is read back, not assumed: `remove_toxic` accepts a 404 (an absent toxic is
    // already gone), so only the proxy's own list says whether the fault really went away.
    let tx_read = toxi.clone();
    let p_read = p1.clone();
    wait_until(
        "the reset toxic to be off the proxy (the fault never went away)",
        Duration::from_secs(10),
        || {
            let (toxi, p) = (tx_read.clone(), p_read.clone());
            async move { Ok(!toxi.toxics(&p).await?.iter().any(|t| t.name == "rst")) }
        },
    )
    .await?;
    stop_writers(ctx, tx, writers).await?;
    // The target catches up from the kept changes. Both families are polled the same way — the
    // oracle on the target's own rows, without trusting `applied()` (`eventually_equal` for the
    // MySQL target, `eventually_equal_pg` for the PostgreSQL one). A counter is not the check:
    // a write the network edge retries into success never marks the target behind at all
    // (`note_refresh` opens a lag entry only for a write the retry gave up on), so
    // `target_catchups` stays flat precisely when the engine recovered as designed, and an
    // unrelated view's catch-up could move it either way. The rows are what a lost write can
    // have cost, so the rows are what this waits for; `verify_targets` below says it again.
    if pgdb.is_some() {
        eventually_equal_pg(
            ctx,
            &mut eng,
            &db,
            &tpg,
            &specs,
            FAULT_BUDGET,
            &format!("{tname} after the reset"),
        )
        .await?;
        // Kept for the finding, not asserted: how the recovery moved.
        let d = eng.snapshot().since(&before);
        ctx.log(format!(
            "{tag}: recovery after the reset — retries {}, refresh errors {}, zombie kills {}, \
             catch-ups {}",
            d.supervisor_retries, d.refresh_errors, d.supervisor_zombie_kills, d.target_catchups
        ));
    } else {
        eventually_equal(
            ctx,
            &mut eng,
            &db,
            &tmysql,
            &specs,
            FAULT_BUDGET,
            &format!("{tname} after the reset"),
        )
        .await?;
    }
    let mut c = db.conn().await?;
    let pos = server_position(&mut c).await?;
    eng.wait_applied(&pos, t_catchup()).await?;
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmysql,
        &tpg,
        &specs,
        &format!("final after the reset on {tname}"),
    )
    .await?;
    let d = eng.snapshot().since(&before);
    ensure!(
        d.supervisor_zombie_kills >= 1
            || d.refresh_errors >= 1
            || d.supervisor_retries >= 1
            || target_write_error_logged(&eng.log_path, tname),
        "{tag}: the lost write stayed invisible to the engine — no zombie kill, \
         no refresh error, no supervisor retry, nothing in the log: {}",
        fmt_snapshot(&d)
    );
    assert_no_rebuilds(&eng, &db, &specs, &before, tag)?;
    eng.check_alive()?;
    eng.stop().await?;
    if let Some(pgdb) = &pgdb {
        pg::drop_dbs(std::slice::from_ref(pgdb)).await?;
    }
    Ok(())
}

#[test]
fn chaos_tgt_reset_mid_write_txn() {
    let _one = toxi::exclusive();
    scenario("chaos_tgt_reset", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        tgt_reset(&ctx, &toxi, None).await
    });
}

/// #6 against a PostgreSQL target (T8, plan §6): the same reset on `lv_<s>_pgt1`, upstream
/// `pg16:5432` — a different executor (`target/postgres`'s write pool and its zombie fence) eats
/// the same fault. Skips, not fails, when `LAZYVIEW_TEST_PG_DSNS` lists no server, in C1's shape
/// (`pg::servers_or_skip`).
#[test]
fn chaos_tgt_pg_reset_mid_write_txn() {
    let _one = toxi::exclusive();
    let Some(pgs) = pg::servers_or_skip("chaos_tgt_pg_reset") else {
        return;
    };
    scenario("chaos_tgt_pg_reset", move |ctx| {
        let pgs = pgs.clone();
        async move {
            let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
                return Ok(());
            };
            tgt_reset(&ctx, &toxi, pgs.first()).await
        }
    });
}

/// #7 (§3), parameterised over the target family (T8, plan §6): `lv_<s>_<t1|pgt1>` upstream
/// `timeout{timeout:0}` for a bounded window while writers keep pushing, with
/// `LAZYVIEW_TEST_LAG_HOLD_BYTES=1` so any lag crosses the budget. The target is reported behind,
/// keeps its keys, and catches up when the toxic goes. The **other** target — direct, never
/// proxied — must be exact throughout: the writers are quiesced for the check (an oracle
/// comparison mid-stream compares two instants, not a state), and convergence must complete
/// **while the proxied target is still black-holed** — a flush pipeline that let one dead target
/// stall the healthy one fails here, and that independence is the point. In the PostgreSQL family
/// the black-holed target is `pg` on the `pg16` proxy (T8) and the healthy direct target stays a
/// MySQL `t2`: a dead PG target may not stall the MySQL write path either.
async fn tgt_blackhole(ctx: &Ctx, toxi: &Toxi, pg_server: Option<&pg::Server>) -> Result<()> {
    let (tag, sname, tname) = match pg_server {
        Some(_) => ("chaos_tgt_pg_bh", "chaos_tgt_pg_bh", PG_TARGET),
        None => ("chaos_tgt_bh", "chaos_tgt_blackhole", "t1"),
    };
    let db = ctx.new_db(tag).await?;
    let _clean = toxi.for_schema(&db.name);
    let t2 = ctx.new_db(&format!("{tag}_t2")).await?;
    let (base, upstream, t1, pgdb) = match pg_server {
        Some(s) => {
            let pgdb = pg::new_pg_db(s, "cx_tpgb").await?;
            (pgdb.dsn.clone(), toxi::pg_upstream(), None, Some(pgdb))
        }
        None => {
            let t1 = ctx.new_db(&format!("{tag}_t1")).await?;
            (t1.dsn.clone(), toxi::upstream(&db.flavor), Some(t1), None)
        }
    };
    let ids = setup_soak(&db, ctx.sub_seed(tag), Sizes::tiny()).await?;
    let specs = chaos_specs(&format!("source,{tname},t2"));
    let p1 = toxi
        .proxy(
            &db.name,
            if pgdb.is_some() { "pgt1" } else { "t1" },
            &upstream,
        )
        .await?;
    toxi.wait_listening(&p1).await?;
    let mut ov =
        toxi::proxied_target_env(&[(tname, base.as_str(), Some(&p1)), ("t2", &t2.dsn, None)])?;
    for (k, v) in [
        ("LAZYVIEW_HEARTBEAT_MS", "200"),
        ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
        ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "1"),
    ] {
        ov.push((k.to_string(), v.to_string()));
    }
    let (mut eng, _) = start_with_views(ctx, &db, &specs, &ov_refs(&ov)).await?;
    let mut tmysql: HashMap<String, Db> = [("t2".to_string(), t2.clone())].into();
    if let Some(t1) = &t1 {
        tmysql.insert("t1".to_string(), t1.clone());
    }
    let tpg: HashMap<String, pg::PgDb> = match &pgdb {
        Some(pgdb) => [(tname.to_string(), pgdb.clone())].into(),
        None => HashMap::new(),
    };
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmysql,
        &tpg,
        &specs,
        "initial build on both targets",
    )
    .await?;
    let before = eng.snapshot();
    toxi.arm(&p1, &Toxic::timeout(0).on(Dir::Upstream).named("bh"))
        .await?;
    let (tx, writers) = spawn_target_writers(ctx, &db, ids.clone(), &format!("{tag}_w"), 60);
    // The black-holed target must be *reported* behind (the budget is one byte): a Toxiproxy
    // that silently did nothing fails right here.
    wait_until(
        "the black-holed target to be reported behind (the fault never landed)",
        FAULT_BUDGET,
        || {
            let m = eng.metrics.clone();
            async move { Ok(m.snapshot().lag_over_budget > before.lag_over_budget) }
        },
    )
    .await?;
    // Quiesce, then prove independence: with the proxied target still black-holed, the source and
    // the direct target `t2` must converge to the oracle — the proxied target's (view, target)
    // pairs are skipped by leaving it out of the map (oracle.rs). A healthy engine excludes the
    // failed target from the refresh and retries it from the kept keys; if a dead target can
    // stall `t2`, this wait fails and the suite says so.
    stop_writers(ctx, tx, writers).await?;
    wait_until(
        "the direct target to stay exact while the proxied target is black-holed (independence broken)",
        FAULT_BUDGET,
        || async { Ok(check_views_targets(&db, &tmap(&t2), &specs).await.is_ok()) },
    )
    .await?;
    toxi.remove_toxic(&p1, "bh").await?;
    wait_until(
        &format!("{tname} to catch up after the black hole (the fault never went away)"),
        FAULT_BUDGET,
        || {
            let m = eng.metrics.clone();
            async move { Ok(m.snapshot().target_catchups > before.target_catchups) }
        },
    )
    .await?;
    let mut c = db.conn().await?;
    let pos = server_position(&mut c).await?;
    eng.wait_applied(&pos, t_catchup()).await?;
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmysql,
        &tpg,
        &specs,
        "final after the catch-up",
    )
    .await?;
    let d = eng.snapshot().since(&before);
    ensure!(
        d.lag_over_budget >= 1 && d.target_catchups >= 1,
        "{sname}: neither the lag report nor the catch-up was observed: {}",
        fmt_snapshot(&d)
    );
    assert_no_rebuilds(&eng, &db, &specs, &before, sname)?;
    eng.check_alive()?;
    eng.stop().await?;
    if let Some(pgdb) = &pgdb {
        pg::drop_dbs(std::slice::from_ref(pgdb)).await?;
    }
    Ok(())
}

#[test]
fn chaos_tgt_blackhole_during_flush() {
    let _one = toxi::exclusive();
    scenario("chaos_tgt_blackhole", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        tgt_blackhole(&ctx, &toxi, None).await
    });
}

/// #7 against a PostgreSQL target (T8, plan §6): the black hole on `lv_<s>_pgt1`, upstream
/// `pg16:5432`, while the direct MySQL `t2` proves the PG write pool's stall does not spread.
/// Skips, not fails, with no server in `LAZYVIEW_TEST_PG_DSNS` (C1's shape).
#[test]
fn chaos_tgt_pg_blackhole_during_flush() {
    let _one = toxi::exclusive();
    let Some(pgs) = pg::servers_or_skip("chaos_tgt_pg_bh") else {
        return;
    };
    scenario("chaos_tgt_pg_bh", move |ctx| {
        let pgs = pgs.clone();
        async move {
            let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
                return Ok(());
            };
            tgt_blackhole(&ctx, &toxi, pgs.first()).await
        }
    });
}

/// #8 (§3), parameterised over the target family (T8, plan §6): `lv_<s>_<t1|pgt1>`
/// `bandwidth{rate:32}` + `latency{latency:80}` under sustained writes above what the link can
/// drain, armed in **both** directions (F2: a symmetric RTT needs one toxic per direction — a
/// downstream-only cap would throttle nothing but the small ACKs of a write), with the F5 bound
/// `LAZYVIEW_TARGET_TIMEOUT_MS=2000` deciding when a too-slow write counts as behind. The target
/// falls behind, is reported over budget, and then drains: the applied-position gap is measured at
/// both points, as bounded waits (writers keep the source position moving; the two samples
/// straddle the toxic removal). In the PostgreSQL family (T8) the starved link is the `pg16`
/// proxy: the PG extended-protocol write round-trips are what the cap starves, and the same
/// `lag_over_budget`/`target_catchups` pair plus the engine's own too-slow reports are the proof.
async fn tgt_bandwidth(ctx: &Ctx, toxi: &Toxi, pg_server: Option<&pg::Server>) -> Result<()> {
    let (tag, sname, tname) = match pg_server {
        Some(_) => ("chaos_tgt_pg_bw", "chaos_tgt_pg_bw", PG_TARGET),
        None => ("chaos_tgt_bw", "chaos_tgt_bw", "t1"),
    };
    let db = ctx.new_db(tag).await?;
    let _clean = toxi.for_schema(&db.name);
    let (base, upstream, t1, pgdb) = match pg_server {
        Some(s) => {
            let pgdb = pg::new_pg_db(s, "cx_tpge").await?;
            (pgdb.dsn.clone(), toxi::pg_upstream(), None, Some(pgdb))
        }
        None => {
            let t1 = ctx.new_db(&format!("{tag}_t1")).await?;
            (t1.dsn.clone(), toxi::upstream(&db.flavor), Some(t1), None)
        }
    };
    let ids = setup_soak(&db, ctx.sub_seed(tag), Sizes::tiny()).await?;
    let specs = chaos_specs(&format!("source,{tname}"));
    let p1 = toxi
        .proxy(
            &db.name,
            if pgdb.is_some() { "pgt1" } else { "t1" },
            &upstream,
        )
        .await?;
    toxi.wait_listening(&p1).await?;
    let mut ov = toxi::proxied_target_env(&[(tname, base.as_str(), Some(&p1))])?;
    for (k, v) in [
        ("LAZYVIEW_HEARTBEAT_MS", "200"),
        ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
        ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "1"),
    ] {
        ov.push((k.to_string(), v.to_string()));
    }
    let (mut eng, _) = start_with_views(ctx, &db, &specs, &ov_refs(&ov)).await?;
    let tmysql = match &t1 {
        Some(t1) => tmap(t1),
        None => HashMap::new(),
    };
    let tpg: HashMap<String, pg::PgDb> = match &pgdb {
        Some(pgdb) => [(tname.to_string(), pgdb.clone())].into(),
        None => HashMap::new(),
    };
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmysql,
        &tpg,
        &specs,
        &format!("initial build on {tname}"),
    )
    .await?;
    let before = eng.snapshot();
    for t in [
        Toxic::bandwidth(32, Dir::Upstream).named("bwu"),
        Toxic::bandwidth(32, Dir::Downstream).named("bwd"),
        Toxic::latency(80, 0, Dir::Upstream).named("latu"),
        Toxic::latency(80, 0, Dir::Downstream).named("latd"),
    ] {
        toxi.arm(&p1, &t).await?;
    }
    // Sustained pressure above what 32 KB/s + 80 ms of RTT can drain (F2: a fault that
    // never lands is a test failure). ~600 rows land every 20 ms while a flush of the
    // same window needs seconds to push through the capped link — the write queue must
    // grow, writes must cross the 2 s timeout, and the target must be reported behind.
    // The landing observables are declared: the behind-report counter (`lag_over_budget`)
    // or any of the engine's own too-slow reports for the target (`slow_target_reported`).
    let (tx, writers) = spawn_target_writers(ctx, &db, ids.clone(), &format!("{tag}_w"), 600);
    wait_until(
        "the starved target to be reported behind (the fault never landed)",
        FAULT_BUDGET,
        || {
            let m = eng.metrics.clone();
            let lp = eng.log_path.clone();
            async move {
                Ok(m.snapshot().lag_over_budget > before.lag_over_budget
                    || slow_target_reported(&lp, tname))
            }
        },
    )
    .await?;
    // The gap, first sample: while the link starves, the engine is behind the server.
    let mut c = db.conn().await?;
    let pos = server_position(&mut c).await?;
    let applied = eng.applied();
    ctx.log(format!(
        "{sname}: gap while armed — server {pos}, applied {applied:?}"
    ));
    ensure!(
        applied.as_ref().is_some_and(|a| a < &pos),
        "{sname}: the applied position never fell behind the server while the target \
         was starved (applied {applied:?}, server {pos})"
    );
    for name in ["bwu", "bwd", "latu", "latd"] {
        toxi.remove_toxic(&p1, name).await?;
    }
    wait_until(
        &format!("{tname} to drain after the starvation (the fault never went away)"),
        FAULT_BUDGET,
        || {
            let m = eng.metrics.clone();
            async move { Ok(m.snapshot().target_catchups > before.target_catchups) }
        },
    )
    .await?;
    stop_writers(ctx, tx, writers).await?;
    // The gap, second sample: the same position, now applied.
    eng.wait_applied(&pos, t_catchup()).await?;
    ctx.log(format!("{sname}: gap after the drain — applied {pos}"));
    verify_targets(
        ctx,
        &mut eng,
        &db,
        &tmysql,
        &tpg,
        &specs,
        &format!("final after the drain on {tname}"),
    )
    .await?;
    let d = eng.snapshot().since(&before);
    ensure!(
        d.target_catchups >= 1 && d.lag_over_budget >= 1,
        "{sname}: neither the starvation lag nor the drain was observed: {}",
        fmt_snapshot(&d)
    );
    assert_no_rebuilds(&eng, &db, &specs, &before, sname)?;
    eng.check_alive()?;
    eng.stop().await?;
    if let Some(pgdb) = &pgdb {
        pg::drop_dbs(std::slice::from_ref(pgdb)).await?;
    }
    Ok(())
}

#[test]
fn chaos_tgt_bandwidth_starvation() {
    let _one = toxi::exclusive();
    scenario("chaos_tgt_bw", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        tgt_bandwidth(&ctx, &toxi, None).await
    });
}

/// #8 against a PostgreSQL target (T8, plan §6): the capped link is `lv_<s>_pgt1` upstream
/// `pg16:5432`, starving the PG executor's own write pool. Skips, not fails, with no server in
/// `LAZYVIEW_TEST_PG_DSNS` (C1's shape).
#[test]
fn chaos_tgt_pg_bandwidth_starvation() {
    let _one = toxi::exclusive();
    let Some(pgs) = pg::servers_or_skip("chaos_tgt_pg_bw") else {
        return;
    };
    scenario("chaos_tgt_pg_bw", move |ctx| {
        let pgs = pgs.clone();
        async move {
            let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
                return Ok(());
            };
            tgt_bandwidth(&ctx, &toxi, pgs.first()).await
        }
    });
}

/// #9 (§3): both sides fragmented at once — `slicer{average_size:1,size_variation:0,delay:2000µs}`
/// upstream **and** downstream on every source link of the engine (the T3 role proxies: binlog,
/// query, meta), `slicer{average_size:16}` + `latency{latency:40,jitter:40}` on the target link.
/// A bounded workload (N transactions, the applied-waits bound the run — never a sleep) must come
/// out exactly on every (view, target) with no refresh error and no rebuild of an incremental
/// view: correctness under one-byte-per-packet traffic everywhere.
#[test]
fn chaos_both_sides_fragmented() {
    let _one = toxi::exclusive();
    scenario("chaos_both_sides", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("chaos_both_sides").await?;
        let _clean = toxi.for_schema(&db.name);
        let t1 = ctx.new_db("chaos_both_sides_t1").await?;
        let ids = setup_soak(&db, ctx.sub_seed("chaos_both_sides"), Sizes::tiny()).await?;
        let specs = chaos_specs("source,t1");
        // Every engine connection — binlog dump, source reads, metadata, target writes — behind
        // its own proxy; only the harness stays direct (§2.4).
        let mut src_ps = Vec::new();
        for role in ["binlog", "query", "meta"] {
            let p = toxi
                .proxy(&db.name, role, &toxi::upstream(&db.flavor))
                .await?;
            toxi.wait_listening(&p).await?;
            src_ps.push(p);
        }
        let p1 = toxi
            .proxy(&db.name, "t1", &toxi::upstream(&db.flavor))
            .await?;
        toxi.wait_listening(&p1).await?;
        let mut ov = toxi::proxied_target_env(&[("t1", &t1.dsn, Some(&p1))])?;
        for (k, p) in [
            ("LAZYVIEW_TEST_BINLOG_DSN", &src_ps[0]),
            ("LAZYVIEW_TEST_QUERY_DSN", &src_ps[1]),
            ("LAZYVIEW_TEST_META_DSN", &src_ps[2]),
        ] {
            ov.push((k.to_string(), proxied_engine_dsn(&db, p.listen)?));
        }
        ov.push(("LAZYVIEW_HEARTBEAT_MS".to_string(), "200".to_string()));
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov_refs(&ov)).await?;
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &HashMap::new(),
            &specs,
            "initial build, unfragmented",
        )
        .await?;
        let before = eng.snapshot();
        // One byte every 2 ms in both directions on every source link; 16-byte slices and
        // jittery latency both ways on the target link (F2).
        for p in &src_ps {
            toxi.arm(p, &Toxic::slicer(1, 0, 2_000, Dir::Upstream).named("su"))
                .await?;
            toxi.arm(p, &Toxic::slicer(1, 0, 2_000, Dir::Downstream).named("sd"))
                .await?;
        }
        for t in [
            Toxic::slicer(16, 0, 0, Dir::Upstream).named("su"),
            Toxic::slicer(16, 0, 0, Dir::Downstream).named("sd"),
            Toxic::latency(40, 40, Dir::Upstream).named("lu"),
            Toxic::latency(40, 40, Dir::Downstream).named("ld"),
        ] {
            toxi.arm(&p1, &t).await?;
        }
        // The bounded window: N transactions through a direct connection, and "applied" waits —
        // not sleeps — decide how long it takes (F5/§2.5).
        let mut c = db.conn().await?;
        let mut wl = SoakWorkload::new(ctx.sub_seed("chaos_both_sides_w"), ids.clone());
        let n = run_soak_batches(&mut c, &mut wl, 10).await?;
        let pos = server_position(&mut c).await?;
        eng.wait_applied(&pos, Duration::from_secs(120))
            .await
            .context(
                "chaos_both_sides: applied never caught up through the fragmentation — a \
                      finding: something blocks on a 1-byte-per-packet stream",
            )?;
        // The faults are gone before the oracle speaks (removal proven by the catch-up below).
        for p in &src_ps {
            toxi.remove_toxic(p, "su").await?;
            toxi.remove_toxic(p, "sd").await?;
        }
        for name in ["su", "sd", "lu", "ld"] {
            toxi.remove_toxic(&p1, name).await?;
        }
        let pos = server_position(&mut c).await?;
        eng.wait_applied(&pos, t_catchup()).await?;
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &HashMap::new(),
            &specs,
            "final under fragmentation",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.refresh_errors == 0,
            "chaos_both_sides: fragmentation broke a refresh — a finding: {}",
            fmt_snapshot(&d)
        );
        assert_no_rebuilds(&eng, &db, &specs, &before, "chaos_both_sides")?;
        ctx.log(format!(
            "chaos_both_sides: {n} transactions applied on a one-byte-per-slice link: {}",
            fmt_snapshot(&d)
        ));
        eng.check_alive()?;
        eng.stop().await?;
        Ok(())
    });
}

// =============================================================================================
// The leadership lock under a fault (review A-06, plan `tmp/out/lvs-lock-hb` PR 4).
//
// Three things the lock and its own beat task owe a proof against a real server:
//  1. a lock connection that stops answering (a black hole, a partitioned node) really *ends*: the
//     server waits out the session's own `wait_timeout` (the lease) and the named lock dies with the
//     session, so the replacement streams within the bound — release, not only detection (cond. 1);
//  2. an instance whose write path is behind for longer than the lease is NOT replaced: the beats
//     are answered by their own task, so a busy engine never loses its lock (condition 2 — the
//     regression test for the days the lock rode the binlog loop, `src/coord.rs`, `src/engine.rs`);
//  3. an instance that has lost its lock writes nothing: neither the one whose beats stop being
//     answered (`Uncertain`, every write gate closed, lv-super ruling 1 of 2026-11-16) nor the
//     frozen one of the owner's "pull the plug / power out case" (SIGSTOP, 2026-11-17) writes a
//     view row or saves a position after another instance holds the lock.
//
// The numbers are the smallest the rules allow: `LAZYVIEW_LOCK_HEARTBEAT_MS` floors at 1 s and
// `Lease::new` demands `lease >= 4 x heartbeat` (`src/coord.rs`), so a takeover is bounded in
// seconds here. Every real duration is printed with the harness's `MEASURED` line, as the owner
// asked for the recovery numbers to be measured and reported.
// =============================================================================================

use common::engineproc::{measured, proc_state, EngineProc};

/// The lock's own settings for these three scenarios: the floor the rules allow (a 1 s beat, and a
/// lease of exactly the `4 x` `Lease::new` demands), so a scenario runs in seconds and asserts about
/// the mechanism rather than about waiting out a 15 s lease.
const LOCK_ENV: [(&str, &str); 2] = [
    ("LAZYVIEW_LOCK_HEARTBEAT_MS", "1000"),
    ("LAZYVIEW_LOCK_LEASE_MS", "4000"),
];
/// The two numbers above as durations, for the bounds and the reports.
const LOCK_BEAT: Duration = Duration::from_millis(1_000);
const LOCK_LEASE: Duration = Duration::from_millis(4_000);
/// The `LAZYVIEW_INSTANCE_ID` every harness engine shares (`Ctx::start_engine`, and the standby of
/// `tests/health.rs:268`): the lock name is schema + this id, so two engines on one schema and one
/// id are exactly the rolling-update overlap the lock exists to settle (owner 2026-11-17).
const LOCK_INSTANCE: &str = "test";
/// A bounded wait for the takeover: the lease (4 s) + the standby's own poll (≤ 5 s,
/// `src/coord.rs`) + connect and start (`supervisor::CONNECT_TIMEOUT` 10 s) + room for the catch-up.
/// The *measured* number is what the scenario prints; this bound only catches a mechanism that
/// never happens at all.
const TAKEOVER: Duration = Duration::from_secs(90);

/// The fencing epoch of this schema's instance row — the witness that exactly one takeover happened
/// (`coord::bump_epoch` runs once per takeover, and only on a takeover).
async fn lock_epoch(db: &Db) -> Result<u64> {
    let mut c = db.conn().await?;
    let e: Option<u64> = c
        .exec_first(
            "SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (LOCK_INSTANCE,),
        )
        .await
        .with_context(|| format!("the epoch row of instance `{LOCK_INSTANCE}`"))?;
    c.disconnect().await.ok();
    e.with_context(|| format!("`_lazyview_status` has no row for instance `{LOCK_INSTANCE}`"))
}

/// The saved position of this schema's instance row *with the epoch that wrote it*, read in one
/// statement so the two cannot come from two states of the row. Both engines of a takeover run
/// under `LAZYVIEW_INSTANCE_ID` = `LOCK_INSTANCE` — sharing the id is what makes them contend for
/// one lock — so `_lazyview_status` holds ONE row for the two of them, and the epoch says which of
/// them last owned it. [`soakchaos::saved_position`] takes the highest position of any row and
/// cannot tell the two apart; this can.
async fn saved_row(db: &Db) -> Result<(lazyview::binlog::Position, u64)> {
    let mut c = db.conn().await?;
    let row: Option<(String, String, u64)> = c
        .exec_first(
            "SELECT CAST(`binlog_file` AS CHAR), CAST(`binlog_pos` AS CHAR), `epoch` \
             FROM `_lazyview_status` WHERE `instance_id` = ?",
            (LOCK_INSTANCE,),
        )
        .await
        .with_context(|| format!("the saved row of instance `{LOCK_INSTANCE}`"))?;
    c.disconnect().await.ok();
    let (file, pos, epoch) = row
        .with_context(|| format!("`_lazyview_status` has no row for instance `{LOCK_INSTANCE}`"))?;
    Ok((
        lazyview::binlog::Position {
            file,
            pos: pos.parse().unwrap_or(0),
        },
        epoch,
    ))
}

/// Both halves of the connection black-holed: `timeout{timeout:0}` on `downstream` alone still lets
/// the client's bytes reach the server, and a session the server is still reading from never looks
/// idle — the lease *is* the server's socket-read timeout, so a black hole that is meant to end the
/// session has to eat both directions.
async fn arm_black_hole_both_ways(f: &Fix, role: &str) -> Result<()> {
    f.arm(role, &Toxic::timeout(0).named("black-hole-down"))
        .await?;
    f.arm(
        role,
        &Toxic::timeout(0).named("black-hole-up").on(Dir::Upstream),
    )
    .await
}

/// One counter of a running engine, by name — the same table its `/metrics` serves.
fn ctr(m: &Arc<lazyview::Metrics>, name: &str) -> u64 {
    m.counters()
        .into_iter()
        .find(|(n, _)| *n == name)
        .map_or(0, |(_, v)| v)
}

/// A node whose lock link is black-holed — the partition, with the process still alive. The server
/// must end the silent lock session when the lease runs out, the replacement must stream within the
/// bound, and the old instance must write nothing from the moment its gates closed.
#[test]
fn chaos_lock_blackhole_takeover() {
    let _one = toxi::exclusive();
    scenario("chaos_lock_blackhole_takeover", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_lock_blackhole_takeover");
        // The lock is the only role behind a proxy: the replacement's session and the old
        // instance's other links are ordinary connections, so everything below is about the lock.
        let mut f = setup_extra(&ctx, "chaos_lock1", &toxi, &["lock"], &LOCK_ENV).await?;
        let db = f.db.clone();
        let specs = f.specs.clone();
        let targets = f.targets.clone();
        let mut w = spawn_writers(&db, &f.ids, seed);
        f.wait_streaming().await?;
        // The replacement runs the deployment the fenced one ran: the fixture's targets included.
        // `Ctx::start_engine` gives an engine the source DSN and the harness defaults and nothing
        // else, so without the target configuration every view the fixture put on `t1` comes up in
        // error ("unknown target `t1`") and the final oracle check compares tables nothing wrote.
        // The lock is deliberately NOT proxied for it — `LOCK_ENV` carries no `LAZYVIEW_TEST_LOCK_DSN`,
        // so its own session dials the server directly, which is what lets it take over at all
        // while the old instance's lock link is black-holed.
        let t1 = targets
            .get("t1")
            .expect("the t1 target the fixture put the views on");
        let t1_env = target_env(&[("t1", t1.dsn.as_str())]);
        let mut b_env: Vec<(&str, &str)> = LOCK_ENV.iter().map(|(k, v)| (*k, *v)).collect();
        b_env.extend(t1_env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let mut b = ctx.start_engine(&db, &b_env)?;
        let bm = b.metrics.clone();
        wait_until("the replacement in hot standby", FAULT, || {
            let bm = bm.clone();
            async move { Ok(bm.gauges.standby.load(Ordering::Relaxed) == 1) }
        })
        .await?;
        ensure!(
            bm.gauges.streaming.load(Ordering::Relaxed) == 0,
            "the replacement streamed while another instance held the lock"
        );
        let epoch_before = lock_epoch(&db).await?;
        let am = f.eng.metrics.clone();
        let beats_before = ctr(&am, "leader_beat_failures");
        // Timed from the instant the fault was armed: what is measured is how long the instance
        // and the standby together take to go from a silent lock link to a streaming replacement.
        let t0 = Instant::now();
        arm_black_hole_both_ways(&f, "lock").await?;
        wait_until(
            "the replaced instance to close its write gates",
            FAULT,
            || {
                let am = am.clone();
                async move {
                    Ok(ctr(&am, "leader_uncertain") >= 1 || ctr(&am, "leader_fenced") >= 1)
                }
            },
        )
        .await
        .with_context(|| {
            format!(
                "the black hole never reached the lock: {} s of silence on the lock link and the \
                 instance neither went Uncertain nor was fenced",
                FAULT.as_secs()
            )
        })?;
        ensure!(
            f.log_line("no beat answered")?.is_some(),
            "the lock task never said the lease had stopped being provable"
        );
        // The baseline is taken once the loop has actually run into the closed gate (its own
        // `flush held` line), so anything still draining from before the gate is behind us.
        wait_until("the loop to hit the closed gate", FAULT, || {
            let log = f.eng.log_path.clone();
            async move { Ok(soakchaos::grep_log(&log, "flush held")?.is_some()) }
        })
        .await?;
        let held = f.eng.snapshot();
        let applied_held = f.eng.applied();
        // The position baseline at the same closed-gate point as `held`/`applied_held`: the
        // instance was still the rightful leader (until two beats went unanswered) between arming
        // the fault and the gate closing, so a position it saved in that window is legitimate. The
        // invariant is that nothing is written AFTER the gate closed — so baseline here, not earlier.
        let position_held = soakchaos::saved_position(&db).await?;
        // Nothing more may be saved from here on — and this is the only window in which that is
        // observable at all. Both engines run under the same `LAZYVIEW_INSTANCE_ID`
        // (`LOCK_INSTANCE`, which is what makes them contend for one lock), so `_lazyview_status`
        // holds ONE row for the two of them: the epoch that wrote it says which of them did.
        // `coord::bump_epoch` IS the takeover and it happens before the replacement saves
        // anything, so while the row still carries the replaced instance's epoch, that instance is
        // the only one that can write it — and an ungated loop moves it every 50 ms
        // (`soakchaos::saved_position`). The sample stops when the epoch moves: from then on the
        // row is the replacement's own to save and the check would only measure the takeover.
        let mut samples = 0;
        let mut seen_takeover = false;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let (now, epoch_now) = saved_row(&db).await?;
            if epoch_now != epoch_before {
                seen_takeover = true;
                break;
            }
            samples += 1;
            ensure!(
                now == position_held,
                "the replaced instance saved a position after its gates closed: {position_held:?} \
                 -> {now:?} while `_lazyview_status` still carried its own epoch {epoch_now}"
            );
        }
        measured(format!(
            "chaos_lock_blackhole_takeover: the fenced instance saved nothing over {samples} \
             sample(s) of 100 ms of a row still under its own epoch{}",
            if seen_takeover {
                " (the takeover arrived inside the window)"
            } else {
                ""
            }
        ));
        // The takeover.
        wait_until("the replacement streaming", TAKEOVER, || {
            let bm = bm.clone();
            async move { Ok(bm.gauges.streaming.load(Ordering::Relaxed) == 1) }
        })
        .await
        .with_context(|| {
            format!(
                "the replacement never streamed: the lease did not end the black-holed lock \
                 session within {TAKEOVER:?} (lease {LOCK_LEASE:?} + standby poll <= 5 s + start)"
            )
        })?;
        let takeover = t0.elapsed();
        measured(format!(
            "chaos_lock_blackhole_takeover: replacement streaming {takeover:?} after the lock was \
             black-holed (lease {LOCK_LEASE:?}, beat {LOCK_BEAT:?}, standby poll <= 5 s)"
        ));
        // One takeover, and only the replacement bumped the epoch.
        let epoch_now = lock_epoch(&db).await?;
        ensure!(
            epoch_now == epoch_before + 1,
            "the epoch moved from {epoch_before} to {epoch_now}: expected exactly one takeover"
        );
        // The replaced instance wrote nothing from the closed gate on: no flush completed, no row to
        // any target, and the position it had saved stays exactly where it was.
        let d = f.eng.snapshot().since(&held);
        ensure!(
            d.flushes == 0
                && d.mv_rows_inserted == 0
                && d.mv_rows_upserted == 0
                && d.mv_queries == 0
                && d.mv_rows_deleted == 0,
            "the replaced instance kept writing while its lock was gone: {}",
            fmt_snapshot(&d)
        );
        ensure!(
            f.eng.applied() == applied_held,
            "the replaced instance advanced its applied position from {applied_held:?} to {:?} \
             after its gates closed",
            f.eng.applied()
        );
        ensure!(
            ctr(&am, "leader_beat_failures") > beats_before,
            "no beat of the black-holed session ever failed"
        );
        // `_lazyview_status` has one row for both engines (`LOCK_INSTANCE`), so from the takeover
        // on the replacement saves to it as well and the row is no longer the fenced instance's
        // own. What still has to hold is that it never went backwards: a stale instance writing
        // the position it stopped at is the split-brain that replays or loses changes.
        let now = soakchaos::saved_position(&db).await?;
        ensure!(
            now >= position_held,
            "the saved position went backwards from {position_held:?} to {now:?} while the \
             replaced instance was still running"
        );
        // Now let the old instance learn what happened: with the fault gone, its rejoin finds the
        // lock held elsewhere, and it must leave as the replaced one.
        f.disarm("lock", "black-hole-down").await?;
        f.disarm("lock", "black-hole-up").await?;
        let a = f.eng;
        let exit = a.wait_finished(Duration::from_secs(90)).await?;
        let why = match &exit {
            Ok(()) => String::new(),
            Err(e) => e.clone(),
        };
        ensure!(
            why.contains("fenced"),
            "the replaced instance exited with {exit:?}, not as the fenced one"
        );
        ensure!(
            ctr(&am, "leader_fenced") == 1,
            "leader_fenced is {}, expected one fencing of the black-holed instance",
            ctr(&am, "leader_fenced")
        );
        ctx.log(format!("replaced instance fenced: {why}"));
        // The replacement owns the schema: it applies everything the writers did through the whole
        // fault, exactly, and without ever rebuilding an incremental view.
        quiesce_writers(&mut w).await?;
        let mut c = db.conn().await?;
        b.wait_caught_up(&mut c, t_catchup()).await?;
        c.disconnect().await.ok();
        verify_targets(
            &ctx,
            &mut b,
            &db,
            &targets,
            &HashMap::new(),
            &specs,
            "after the black-holed lock",
        )
        .await?;
        assert_no_rebuilds(&b, &db, &specs, &b.snapshot(), "after the black-holed lock")?;
        b.check_alive()?;
        b.stop().await?;
        Ok(())
    });
}

/// A healthy instance whose write path is behind by far more than the lease must keep its lock:
/// the beats are answered by their own task, so a slow engine is not a lost node (condition 2: the
/// false alarm that would end a healthy instance and lose its running builds). Here the source-read
/// link is throttled until flushes take many lease-periods, while the lock's own session is untouched.
#[test]
fn chaos_lock_slow_loop_no_takeover() {
    let _one = toxi::exclusive();
    scenario("chaos_lock_slow_loop_no_takeover", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let seed = ctx.sub_seed("chaos_lock_slow_loop_no_takeover");
        let mut f = setup_extra(&ctx, "chaos_lock2", &toxi, &["lock", "query"], &LOCK_ENV).await?;
        let db = f.db.clone();
        let mut w = spawn_writers(&db, &f.ids, seed);
        f.wait_streaming().await?;
        let mut b = ctx.start_engine(&db, &LOCK_ENV)?;
        let bm = b.metrics.clone();
        wait_until("the would-be replacement in hot standby", FAULT, || {
            let bm = bm.clone();
            async move { Ok(bm.gauges.standby.load(Ordering::Relaxed) == 1) }
        })
        .await?;
        let epoch_before = lock_epoch(&db).await?;
        let am = f.eng.metrics.clone();
        let last_beat = am.gauges.leader_last_beat_ok.load(Ordering::Relaxed);
        // Throttle the reads every refresh needs: slow round trips and a trickle of result bytes.
        // Nothing touches the lock link — that is the point of the scenario.
        f.arm(
            "query",
            &Toxic::latency(1_200, 0, Dir::Downstream).named("slow-read-down"),
        )
        .await?;
        f.arm(
            "query",
            &Toxic::latency(1_200, 0, Dir::Upstream).named("slow-read-up"),
        )
        .await?;
        f.arm(
            "query",
            &Toxic::bandwidth(16, Dir::Downstream).named("thin-read"),
        )
        .await?;
        // Watch the engine for six lease-periods: how long does it go without applying anything,
        // and does a single beat go unanswered in that time? Every sample is an observable and the
        // loop ends on a bound, never on a pause the harness guessed.
        let watch = 6 * LOCK_LEASE;
        let t0 = Instant::now();
        let mut last_applied = f.eng.applied();
        let mut moved = Instant::now();
        let mut stalled = Duration::ZERO;
        let mut backlog = 0u64;
        let mut next_gap = Instant::now();
        while t0.elapsed() < watch {
            let a = f.eng.applied();
            if a != last_applied {
                last_applied = a;
                moved = Instant::now();
            }
            stalled = stalled.max(moved.elapsed());
            if Instant::now() >= next_gap {
                backlog = backlog.max(f.gap_bytes().await?);
                next_gap = Instant::now() + Duration::from_secs(1);
            }
            ensure!(
                ctr(&am, "leader_fenced") == 0,
                "a slow engine was fenced by its own lock task"
            );
            ensure!(
                bm.gauges.standby.load(Ordering::Relaxed) == 1
                    && bm.gauges.streaming.load(Ordering::Relaxed) == 0,
                "a second instance took the lock from an engine that only had its reads throttled"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(
            ctr(&am, "leader_uncertain") == 0,
            "the lock task went Uncertain {} time(s) while the engine was only slow at its reads \
             (beat {LOCK_BEAT:?}, lease {LOCK_LEASE:?}): a slow loop must not be a lost node",
            ctr(&am, "leader_uncertain")
        );
        ensure!(
            ctr(&am, "leader_beat_failures") == 0,
            "the beat task missed {} beat(s) while the reads were throttled — the beats may not \
             depend on any other link",
            ctr(&am, "leader_beat_failures")
        );
        let now_beat = am.gauges.leader_last_beat_ok.load(Ordering::Relaxed);
        ensure!(
            now_beat >= last_beat + watch.as_secs() / 2,
            "the lock session's own beats stopped being answered while the reads were throttled: \
             last_ok {last_beat} -> {now_beat}"
        );
        ensure!(
            stalled >= LOCK_LEASE,
            "the scenario tested nothing: the engine applied something every {stalled:?}, never \
             past the lease ({LOCK_LEASE:?}) it has to survive"
        );
        ensure!(
            lock_epoch(&db).await? == epoch_before,
            "an engine that was merely slow lost the lock: the epoch moved"
        );
        measured(format!(
            "chaos_lock_slow_loop_no_takeover: applied position stalled {stalled:?} ({}x the lease) \
             with a {backlog}-byte binlog backlog behind it: 0 unanswered beats, 0 Uncertain \
             entries, no takeover (lease {LOCK_LEASE:?})",
            (stalled.as_secs_f64() / LOCK_LEASE.as_secs_f64()).max(1.0) as u64
        ));
        for n in ["slow-read-down", "slow-read-up", "thin-read"] {
            f.disarm("query", n).await?;
        }
        // The engine was healthy all along: it catches up and every view is exact.
        f.wait_streaming().await?;
        finish(&ctx, &mut f, &mut w, "after the slow reads cleared").await?;
        ensure!(
            ctr(&am, "leader_fenced") == 0 && ctr(&am, "leader_uncertain") == 0,
            "a throttled read link cost the instance {} fencing(s) and {} Uncertain entry(ies)",
            ctr(&am, "leader_fenced"),
            ctr(&am, "leader_uncertain")
        );
        ensure!(
            bm.gauges.standby.load(Ordering::Relaxed) == 1,
            "the second instance ever stopped standing by: it took a lock it was never given"
        );
        ensure!(
            lock_epoch(&db).await? == epoch_before,
            "the epoch moved during a scenario that must not have a takeover"
        );
        b.check_alive()?;
        b.stop().await?;
        Ok(())
    });
}

/// The owner's "connection crash / pull the plug / power out case" (2026-11-17): a frozen host sends
/// no FIN, so the server keeps its sessions until the lease says otherwise. The old instance here is
/// a real process and gets SIGSTOP; the replacement starts and takes over by the lease; then the old
/// one is SIGCONTinued and must write nothing — it self-fences (ruling 1) and exits.
///
/// "Write nothing" is what is checked, by the counters of its writes and of the epoch it must not
/// move again: bar (a) of lv-super's ruling of 2026-11-26. The applied watermark scraped between the
/// wake and the exit is deliberately not compared — it is an in-memory reading taken mid-way, before
/// the instance had reached its own exit, and the owner's rule is to compare at convergence. The
/// convergence this scenario compares is the one at its bottom: the replacement's views against the
/// oracle once the writers have stopped.
#[test]
fn chaos_lock_frozen_old_instance_writes_nothing() {
    let _one = toxi::exclusive();
    scenario(
        "chaos_lock_frozen_old_instance_writes_nothing",
        |ctx| async move {
            let Some(_toxi) = toxi::open_if_enabled(&ctx).await? else {
                return Ok(());
            };
            let seed = ctx.sub_seed("chaos_lock_frozen_old_instance_writes_nothing");
            let db = ctx.new_db("chaos_lock3").await?;
            let ids = setup_soak(&db, ctx.sub_seed("chaos_lock3"), small_sizes()).await?;
            let specs = chaos_views()?;
            let mut c = db.conn().await?;
            ensure_lazyview_table(&mut c).await?;
            add_views(&mut c, &specs).await?;
            c.disconnect().await.ok();
            // The old instance as a deployment runs it: its own process, its own health endpoint.
            let mut a = EngineProc::start(&ctx, &db, &LOCK_ENV).await?;
            a.wait_startup(t_ready()).await?;
            let up = a.wait_streaming(t_catchup()).await?;
            ctx.log(format!(
                "old instance streaming {up:?} after spawn (pid {})",
                a.pid()
            ));
            let mut b = ctx.start_engine(&db, &LOCK_ENV)?;
            let bm = b.metrics.clone();
            wait_until("the replacement waiting for the lock", FAULT, || {
                let bm = bm.clone();
                async move { Ok(bm.gauges.standby.load(Ordering::Relaxed) == 1) }
            })
            .await?;
            let epoch_before = lock_epoch(&db).await?;
            let mut w = spawn_writers(&db, &ids, seed);
            // The freeze has to land on an instance that is writing. "Wrote nothing after the freeze"
            // is a claim about the freeze only if there was writing before it, and a metric the
            // endpoint does not carry reads as 0 (`Scraped::counter`), so every zero below has to be
            // a zero that could have counted something. The engine logs `streaming binlog` before its
            // first flush has run, so scraping at that line can freeze an instance that had nothing to
            // lose (mysql84: "the instance had written no view row and issued no statement against a
            // target before the freeze, so the counters this scenario holds at zero say nothing").
            // Wait for the first row and the first statement — the initial build and the writers above
            // produce both — and sample then.
            let t_pre = Instant::now();
            let pre = loop {
                let s = a.scrape().await?;
                if s.counter("mv_rows_inserted") > 0 && s.counter("mv_queries") > 0 {
                    break s;
                }
                ensure!(
                    t_pre.elapsed() < t_catchup(),
                    "the old instance wrote no view row and issued no statement against a target in \
                     the {:?} the writers were given: there was nothing to freeze, so every \
                     \"wrote nothing\" below would have been proved by an idle run",
                    t_catchup()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            // Pull the plug: the process stops, its sockets stay open, and the server cannot know it is
            // gone — the only thing that can end its lock session is the lease.
            let pre_rows = pre.counter("mv_rows_inserted");
            let pre_deletes = pre.counter("mv_rows_deleted");
            let pre_upserts = pre.counter("mv_rows_upserted");
            let pre_rebuilds = pre.counter("full_rebuilds");
            // The wait above established this; it is read again here because every "wrote nothing"
            // below rests on it, and a metric the endpoint does not carry reads as 0
            // (`Scraped::counter`) — so a renamed metric would otherwise be proved by a typo.
            ensure!(
                pre_rows > 0 && pre.counter("mv_queries") > 0,
                "the instance had written no view row and issued no statement against a target \
                 before the freeze, so the counters this scenario holds at zero say nothing; log \
                 tail:\n{}",
                a.log_tail(15)
            );
            a.freeze()?;
            let pid = a.pid();
            wait_until("the old process to be stopped (state T)", BOUND, || async {
                Ok(proc_state(pid) == Some('T'))
            })
            .await?;
            let t0 = Instant::now();
            wait_until("the replacement streaming", TAKEOVER, || {
                let bm = bm.clone();
                async move { Ok(bm.gauges.streaming.load(Ordering::Relaxed) == 1) }
            })
            .await
            .with_context(|| {
                format!(
                "the replacement never took over from a frozen instance within {TAKEOVER:?}: the \
                 lease ({LOCK_LEASE:?}) is what ends a session whose host sent nothing at all"
            )
            })?;
            let takeover = t0.elapsed();
            measured(format!(
            "chaos_lock_frozen_old_instance_writes_nothing: replacement streaming {takeover:?} \
             after SIGSTOP of the holder (lease {LOCK_LEASE:?}, standby poll <= 5 s, then start)"
        ));
            ensure!(
                lock_epoch(&db).await? == epoch_before + 1,
                "the epoch did not move exactly once on the takeover by the replacement"
            );
            // Wake the frozen one. Its lock session is the one the server ended at the end of the lease,
            // so it must notice on its next beat, close every gate, and leave; between the wake and its
            // exit not one row may reach a target.
            a.resume()?;
            let mut post = None;
            let tw = Instant::now();
            while post.is_none() && common::engineproc::pid_alive(pid) && tw.elapsed() < BOUND {
                if let Ok(s) = a.scrape().await {
                    post = Some(s);
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            let post_rows = post
                .as_ref()
                .map_or(pre_rows, |s| s.counter("mv_rows_inserted"));
            if let Some(s) = &post {
                // Bar (a) of the fencing property, and the only one a test can decide: an instance
                // the replacement has fenced WRITES NOTHING — no view row by any of the three write
                // paths, no whole rebuild (lv-super's ruling of 2026-11-26). What it does in memory
                // on the way to noticing is not a safety property: the applied watermark scraped
                // here would have been read after SIGCONT and before its exit, i.e. mid-way, and the
                // owner's rule is to compare at convergence (the convergence this scenario does
                // compare is the one at the bottom of it — the replacement's views against the
                // oracle, once the writers have stopped). So the position the woken instance had
                // reached is deliberately not compared; its writes are.
                ensure!(
                    s.counter("mv_rows_inserted") == pre_rows,
                    "the woken instance wrote {} row(s) to the targets after its lock was gone",
                    s.counter("mv_rows_inserted").saturating_sub(pre_rows)
                );
                ensure!(
                    s.counter("mv_rows_deleted") == pre_deletes,
                    "the woken instance deleted {} row(s) from the targets after its lock was gone",
                    s.counter("mv_rows_deleted").saturating_sub(pre_deletes)
                );
                ensure!(
                    s.counter("mv_rows_upserted") == pre_upserts,
                    "the woken instance upserted {} row(s) into the targets after its lock was \
                     gone",
                    s.counter("mv_rows_upserted").saturating_sub(pre_upserts)
                );
                ensure!(
                    s.counter("full_rebuilds") == pre_rebuilds,
                    "the woken instance rebuilt {} view(s) after its lock was gone",
                    s.counter("full_rebuilds").saturating_sub(pre_rebuilds)
                );
            }
            let st = a.wait_exit(Duration::from_secs(90))?;
            ensure!(
            !st.success(),
            "the woken instance exited successfully ({st}) — a replaced instance must leave as the \
             fenced one; log tail:\n{}",
            a.log_tail(15)
        );
            let fenced = soakchaos::grep_log(&a.log_path, "fenced")?.is_some()
                || soakchaos::grep_log(&a.log_path, "another lazyview instance")?.is_some();
            ensure!(
                fenced,
                "the woken instance left without ever naming the takeover; log tail:\n{}",
                a.log_tail(15)
            );
            // The row half of bar (a). Both position saves of a run are `UPDATE … WHERE instance_id
            // = ? AND epoch = ?`, so a fenced instance cannot move the row the replacement owns —
            // unless it re-claimed the lock, which is what the epoch would show. One takeover, ever.
            ensure!(
                lock_epoch(&db).await? == epoch_before + 1,
                "the fencing epoch moved a second time: the woken instance re-claimed the lock, and \
                 with it the row the saved position lives in — the epoch guard of its two position \
                 saves is what a fenced run is supposed to be refused by"
            );
            measured(format!(
                "chaos_lock_frozen_old_instance_writes_nothing: old process wrote {} row(s) after \
             SIGCONT and exited {st} having named the takeover",
                post_rows.saturating_sub(pre_rows)
            ));
            // The replacement owns the schema: everything the writers did — during the freeze and after
            // the wake — is in the views exactly, and no incremental view was rebuilt for it.
            quiesce_writers(&mut w).await?;
            let mut c = db.conn().await?;
            b.wait_caught_up(&mut c, t_catchup()).await?;
            c.disconnect().await.ok();
            verify_targets(
                &ctx,
                &mut b,
                &db,
                &HashMap::new(),
                &HashMap::new(),
                &specs,
                "after the frozen instance was replaced",
            )
            .await?;
            assert_no_rebuilds(
                &b,
                &db,
                &specs,
                &b.snapshot(),
                "after the frozen instance was replaced",
            )?;
            b.check_alive()?;
            b.stop().await?;
            Ok(())
        },
    );
}
