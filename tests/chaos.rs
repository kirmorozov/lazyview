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

use std::time::Duration;

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

/// The roles of F4 a scenario may isolate: `binlog`, `query`, `meta`, or `src` for all three at
/// once (scenario 3, where the fault is deliberately not attributable to one role).
async fn setup(ctx: &Arc<Ctx>, tag: &str, toxi: &Toxi, roles: &[&str]) -> Result<Fix> {
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
    let from = ids.item.fetch_add(rows as i64, Ordering::Relaxed) + 1;
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
        let (stmts, bytes) = items_txn(&f.ids, 50_000);
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
        // It resumes from the position it had *persisted*, not from the head of the binlog.
        let saved = soakchaos::saved_position(&f.db).await?;
        f.wait_streaming().await?;
        f.eng.wait_applied(&saved, t_catchup()).await?;
        let d = f.eng.snapshot().since(&before);
        ensure!(
            d.events > 0 && d.rows_seen > 0,
            "the stream never resumed after the reset: {} events and {} rows applied since the \
             fault, expected the rest of the transaction (saved position {saved:?})",
            d.events,
            d.rows_seen
        );
        ctx.log(format!(
            "reset mid-transaction: {} bytes of row events cut, resumed from {saved:?} (+{} \
             events, +{} rows)",
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

/// `timeout{timeout:0}` on the dump link never closes it and never delivers anything, so the only
/// thing that can end the fault is the engine's own bounds (the one place they live now that the
/// reader has no idle timeout of its own): the stream loop's silence detector,
/// `(heartbeat × 3).max(15 s)` = 15 s with `LAZYVIEW_HEARTBEAT_MS=200` (F5), and then
/// `BINLOG_CONNECT_TIMEOUT` on the reconnect attempt. The harness waits for both, removes the
/// toxic, and the stream comes back from the saved position.
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
        let before = f.eng.snapshot();
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
        wait_until("the engine to attempt the reconnect", FAULT, || {
            let f = &f;
            async move { Ok(f.log_line("binlog connect failed")?.is_some()) }
        })
        .await
        .with_context(|| "the black-holed stream was never retried: no reconnect in the log")?;
        let saved = soakchaos::saved_position(&f.db).await?;
        f.disarm("binlog", "black-hole").await?;
        f.wait_streaming().await?;
        f.eng.wait_applied(&saved, t_catchup()).await?;
        let d = f.eng.snapshot().since(&before);
        ensure!(
            d.events > 0,
            "the stream never came back after the black hole was removed: {} events since the \
             fault, expected the backlog (saved position {saved:?})",
            d.events
        );
        finish(&ctx, &mut f, &mut w, "after the black hole").await?;
        ensure!(
            f.counter("full_rebuilds") == 0,
            "a black-holed dump link cost {} whole rebuild(s)",
            f.counter("full_rebuilds")
        );
        Ok(())
    });
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

/// Did the engine itself notice that a write to `t1` died at the network edge? Its log is the
/// witness when the counters do not move (the plan's #6: the *fate* of the write must become
/// known to the engine, through the fence, a refresh error, a supervisor retry or its own log).
fn target_write_error_logged(log_path: &std::path::Path) -> bool {
    let Ok(text) = std::fs::read_to_string(log_path) else {
        return false;
    };
    text.lines().any(|l| {
        l.contains("t1")
            && [
                "reset by peer",
                "connection closed",
                "connection reset",
                "broken pipe",
                "io error",
                "os error 104",
            ]
            .iter()
            .any(|needle| l.contains(needle))
    })
}

/// `t1` → its schema, the shape `verify_targets` takes for one target.
fn tmap(t1: &Db) -> HashMap<String, Db> {
    [("t1".to_string(), t1.clone())].into()
}

/// The engine's own report that `t1` became too slow to keep up (T5 §3 row 8's declared
/// observables, F2: the test names what proves the toxic landed): a supervisor retry on a
/// write that `timed out`, a `writing to target t1` / `write failed: target t1` line, or a
/// `slow flush` of seconds while the link is capped. Each is attributable to the armed
/// toxics; none of them requires the target to have been declared behind.
fn slow_target_reported(log_path: &std::path::Path) -> bool {
    let Ok(text) = std::fs::read_to_string(log_path) else {
        return false;
    };
    text.lines().any(|l| {
        (l.contains("t1")
            && [
                "timed out after",
                "writing to target t1",
                "write failed: target t1",
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

/// #6 (§3): `lv_<s>_t1` **upstream** `reset_peer` while a flush is writing. The reset is armed
/// with `timeout:300` (F2: "a TCP RST immediately **or after** `timeout`" — a non-zero timeout is
/// what lets the write transaction get into flight before the RST cuts it, the plan's "batch big
/// enough that the write transaction crosses the reset"); `LAZYVIEW_TARGET_TIMEOUT_MS=2000` and
/// `LAZYVIEW_SUPERVISOR=fixed` per F5. The write's fate is genuinely unknown to the engine; the
/// zombie fence (#28), a refresh error, a supervisor retry or the engine's own log must observe
/// it, no row of a half-applied transaction may survive on `t1`, and the retry from the kept
/// changes must land exactly. The MV row counts of `t1` right after the reset are logged — the
/// evidence for the finding the PR body records (T5's step).
#[test]
fn chaos_tgt_reset_mid_write_txn() {
    let _one = toxi::exclusive();
    scenario("chaos_tgt_reset", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("chaos_tgt_reset").await?;
        // The guard first: no armed proxy can outlive a panic on a shared server (§2.2).
        let _clean = toxi.for_schema(&db.name);
        let t1 = ctx.new_db("chaos_tgt_reset_t1").await?;
        let ids = setup_soak(&db, ctx.sub_seed("chaos_tgt_reset"), Sizes::tiny()).await?;
        let specs = chaos_specs("source,t1");
        let p1 = toxi
            .proxy(&db.name, "t1", &toxi::upstream(&db.flavor))
            .await?;
        toxi.wait_listening(&p1).await?;
        let mut ov = toxi::proxied_target_env(&[("t1", &t1.dsn, Some(&p1))])?;
        for (k, v) in [
            ("LAZYVIEW_HEARTBEAT_MS", "200"),
            ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
            ("LAZYVIEW_SUPERVISOR", "fixed"),
        ] {
            ov.push((k.to_string(), v.to_string()));
        }
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov_refs(&ov)).await?;
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &specs,
            "initial build on t1",
        )
        .await?;
        let before = eng.snapshot();
        // Arm on the target's link (the read-back says it landed), then drive flushes across it.
        toxi.arm(&p1, &Toxic::reset_peer(300).on(Dir::Upstream).named("rst"))
            .await?;
        let (tx, writers) = spawn_target_writers(&ctx, &db, ids.clone(), "chaos_tgt_reset_w", 60);
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
                        || target_write_error_logged(&lp))
                }
            },
        )
        .await?;
        // Evidence for the finding: which side of the reset the server ended up on — the row
        // count of every `t1` MV while the fault is still armed.
        let mut tc = t1.conn().await?;
        for s in &specs {
            let rows: Vec<mysql_async::Row> = tc
                .query(format!("SELECT COUNT(*) FROM `{}`", s.name))
                .await?;
            let n = rows.first().and_then(|r| r.get::<i64, _>(0)).unwrap_or(-1);
            ctx.log(format!(
                "chaos_tgt_reset: t1 MV `{}` row count right after the reset: {n}",
                s.name
            ));
        }
        toxi.remove_toxic(&p1, "rst").await?;
        stop_writers(&ctx, tx, writers).await?;
        // `t1` catches up from the kept changes; `eventually_equal` polls the oracle without
        // trusting `applied()` (built exactly for a target whose catch-up may take a moment).
        eventually_equal(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &specs,
            FAULT_BUDGET,
            "t1 after the reset",
        )
        .await?;
        let mut c = db.conn().await?;
        let pos = server_position(&mut c).await?;
        eng.wait_applied(&pos, t_catchup()).await?;
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &specs,
            "final after the reset",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.supervisor_zombie_kills >= 1
                || d.refresh_errors >= 1
                || d.supervisor_retries >= 1
                || target_write_error_logged(&eng.log_path),
            "chaos_tgt_reset: the lost write stayed invisible to the engine — no zombie kill, \
             no refresh error, no supervisor retry, nothing in the log: {}",
            fmt_snapshot(&d)
        );
        assert_no_rebuilds(&eng, &db, &specs, &before, "chaos_tgt_reset")?;
        eng.check_alive()?;
        eng.stop().await?;
        Ok(())
    });
}

/// #7 (§3): `lv_<s>_t1` upstream `timeout{timeout:0}` for a bounded window while writers keep
/// pushing, with `LAZYVIEW_TEST_LAG_HOLD_BYTES=1` so any lag crosses the budget. The target is
/// reported behind, keeps its keys, and catches up when the toxic goes. The **other** target —
/// direct, never proxied — must be exact throughout: the writers are quiesced for the check (an
/// oracle comparison mid-stream compares two instants, not a state), and convergence must
/// complete **while `t1` is still black-holed** — a flush pipeline that let one dead target
/// stall the healthy one fails here, and that independence is the point.
#[test]
fn chaos_tgt_blackhole_during_flush() {
    let _one = toxi::exclusive();
    scenario("chaos_tgt_blackhole", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("chaos_tgt_bh").await?;
        let _clean = toxi.for_schema(&db.name);
        let t1 = ctx.new_db("chaos_tgt_bh_t1").await?;
        let t2 = ctx.new_db("chaos_tgt_bh_t2").await?;
        let ids = setup_soak(&db, ctx.sub_seed("chaos_tgt_bh"), Sizes::tiny()).await?;
        let specs = chaos_specs("source,t1,t2");
        let p1 = toxi
            .proxy(&db.name, "t1", &toxi::upstream(&db.flavor))
            .await?;
        toxi.wait_listening(&p1).await?;
        let mut ov =
            toxi::proxied_target_env(&[("t1", &t1.dsn, Some(&p1)), ("t2", &t2.dsn, None)])?;
        for (k, v) in [
            ("LAZYVIEW_HEARTBEAT_MS", "200"),
            ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
            ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "1"),
        ] {
            ov.push((k.to_string(), v.to_string()));
        }
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov_refs(&ov)).await?;
        let t12: HashMap<String, Db> = [
            ("t1".to_string(), t1.clone()),
            ("t2".to_string(), t2.clone()),
        ]
        .into();
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &t12,
            &specs,
            "initial build on both targets",
        )
        .await?;
        let before = eng.snapshot();
        toxi.arm(&p1, &Toxic::timeout(0).on(Dir::Upstream).named("bh"))
            .await?;
        let (tx, writers) = spawn_target_writers(&ctx, &db, ids.clone(), "chaos_tgt_bh_w", 60);
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
        // Quiesce, then prove independence: with `t1` still black-holed, the source and the
        // direct target `t2` must converge to the oracle — `t1`'s (view, target) pairs are
        // skipped by leaving `t1` out of the map (oracle.rs). A healthy engine excludes the
        // failed target from the refresh and retries it from the kept keys; if a dead `t1` can
        // stall `t2`, this wait fails and the suite says so.
        stop_writers(&ctx, tx, writers).await?;
        wait_until(
            "the direct target to stay exact while t1 is black-holed (independence broken)",
            FAULT_BUDGET,
            || async { Ok(check_views_targets(&db, &tmap(&t2), &specs).await.is_ok()) },
        )
        .await?;
        toxi.remove_toxic(&p1, "bh").await?;
        wait_until(
            "t1 to catch up after the black hole (the fault never went away)",
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
            &ctx,
            &mut eng,
            &db,
            &t12,
            &specs,
            "final after the catch-up",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.lag_over_budget >= 1 && d.target_catchups >= 1,
            "chaos_tgt_blackhole: neither the lag report nor the catch-up was observed: {}",
            fmt_snapshot(&d)
        );
        assert_no_rebuilds(&eng, &db, &specs, &before, "chaos_tgt_blackhole")?;
        eng.check_alive()?;
        eng.stop().await?;
        Ok(())
    });
}

/// #8 (§3): `lv_<s>_t1` `bandwidth{rate:32}` + `latency{latency:80}` under sustained writes above
/// what the link can drain, armed in **both** directions (F2: a symmetric RTT needs one toxic per
/// direction — a downstream-only cap would throttle nothing but the small ACKs of a MySQL write),
/// with the F5 bound `LAZYVIEW_TARGET_TIMEOUT_MS=2000` deciding when a too-slow write counts as
/// behind. The target falls behind, is reported over budget, and then drains: the applied-position
/// gap is measured at both points, as bounded waits (writers keep the source position moving;
/// the two samples straddle the toxic removal).
#[test]
fn chaos_tgt_bandwidth_starvation() {
    let _one = toxi::exclusive();
    scenario("chaos_tgt_bandwidth", |ctx| async move {
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let db = ctx.new_db("chaos_tgt_bw").await?;
        let _clean = toxi.for_schema(&db.name);
        let t1 = ctx.new_db("chaos_tgt_bw_t1").await?;
        let ids = setup_soak(&db, ctx.sub_seed("chaos_tgt_bw"), Sizes::tiny()).await?;
        let specs = chaos_specs("source,t1");
        let p1 = toxi
            .proxy(&db.name, "t1", &toxi::upstream(&db.flavor))
            .await?;
        toxi.wait_listening(&p1).await?;
        let mut ov = toxi::proxied_target_env(&[("t1", &t1.dsn, Some(&p1))])?;
        for (k, v) in [
            ("LAZYVIEW_HEARTBEAT_MS", "200"),
            ("LAZYVIEW_TARGET_TIMEOUT_MS", "2000"),
            ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "1"),
        ] {
            ov.push((k.to_string(), v.to_string()));
        }
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov_refs(&ov)).await?;
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &specs,
            "initial build on t1",
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
        // or any of the engine's own too-slow reports for t1 (`slow_target_reported`).
        let (tx, writers) = spawn_target_writers(&ctx, &db, ids.clone(), "chaos_tgt_bw_w", 600);
        wait_until(
            "the starved target to be reported behind (the fault never landed)",
            FAULT_BUDGET,
            || {
                let m = eng.metrics.clone();
                let lp = eng.log_path.clone();
                async move {
                    Ok(m.snapshot().lag_over_budget > before.lag_over_budget
                        || slow_target_reported(&lp))
                }
            },
        )
        .await?;
        // The gap, first sample: while the link starves, the engine is behind the server.
        let mut c = db.conn().await?;
        let pos = server_position(&mut c).await?;
        let applied = eng.applied();
        ctx.log(format!(
            "chaos_tgt_bw: gap while armed — server {pos}, applied {applied:?}"
        ));
        ensure!(
            applied.as_ref().is_some_and(|a| a < &pos),
            "chaos_tgt_bw: the applied position never fell behind the server while the target \
             was starved (applied {applied:?}, server {pos})"
        );
        for name in ["bwu", "bwd", "latu", "latd"] {
            toxi.remove_toxic(&p1, name).await?;
        }
        wait_until(
            "t1 to drain after the starvation (the fault never went away)",
            FAULT_BUDGET,
            || {
                let m = eng.metrics.clone();
                async move { Ok(m.snapshot().target_catchups > before.target_catchups) }
            },
        )
        .await?;
        stop_writers(&ctx, tx, writers).await?;
        // The gap, second sample: the same position, now applied.
        eng.wait_applied(&pos, t_catchup()).await?;
        ctx.log(format!("chaos_tgt_bw: gap after the drain — applied {pos}"));
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &tmap(&t1),
            &specs,
            "final after the drain",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.target_catchups >= 1 && d.lag_over_budget >= 1,
            "chaos_tgt_bw: neither the starvation lag nor the drain was observed: {}",
            fmt_snapshot(&d)
        );
        assert_no_rebuilds(&eng, &db, &specs, &before, "chaos_tgt_bw")?;
        eng.check_alive()?;
        eng.stop().await?;
        Ok(())
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
