//! The kill-and-recover suite: the **real binary** is killed, and what a recovery costs is
//! measured (`brutal_production_readiness_f2_refined.md` §5, the gap at 512–513 — "The crash is
//! simulated inside the test process. The real binary is not killed"; plan
//! `tmp/out/lvs-perf/plan/breakdown.md` §2, PR2).
//!
//! Three failure modes, each measured and asserted on its own (the owner's binding message):
//!
//! | mode | what happens | what it proves | review |
//! |---|---|---|---|
//! | **M1** | `SIGKILL` of the engine process at seeded points across the whole lifecycle, and a crash loop of K kills each landing during the previous recovery | the next instance resumes from the saved position, loses no row and duplicates none, and never rebuilds an incremental view | §5 |
//! | **M2** | `SIGSTOP` — a host that dies without closing a socket, so no FIN and no RST; the replacement starts while the old process is frozen | the takeover happens (the lock's lease, `LAZYVIEW_LOCK_LEASE_MS`), and the replaced-but-alive old process writes **nothing** | A-06 |
//! | **M3** | the shared toxiproxy `reset_peer` on the engine↔source link while a flush is in flight | the engine survives and reconnects, or the replacement takes over, and the views stay exact | A-02, A-04 |
//!
//! **PR 2 records the times, it asserts no budget** (the owner's rule: no unmeasured durations).
//! Every time is printed per kill and as p50/p95/max per mode by `common::recovery::Table`; PR 5
//! sets the budgets from the nightly distribution. What *is* asserted, after every kill: the views
//! are exact against their own definition over the base tables (no lost and no duplicated row), and
//! no incremental view fell back to a rebuild — a refresh of a whole view is allowed, a rebuild of
//! an incrementally-maintained one is not. `/livez` is sampled through every catch-up wait and
//! `livez_red` is printed with the kill (review item 20: how close a catch-up comes to the 120 s
//! liveness limit). A crash caused by the input repeating (the §5 exception row 1) is measured, not
//! fixed here: the loop asserts the saved position advances every cycle.
//!
//! Costs: none on the hot path — the engine is not touched, every line here runs in the harness once
//! per kill or per poll of a bounded wait. The copy set (`src/{analyze,analyzer,access,binlog,
//! catalog,datum,intset,fence,cascade,dedup,source,dialect}`) is untouched. Definitions go to
//! `_lazyview` as every other test puts them; the only other writes to the source are the workload's
//! own DML (there is no control store to write them to instead).
//!
//! Runs through `tmp/lvs-suite.sh` as `chaos_recovery` — the `chaos-*` name is what gets the shared
//! toxiproxy wired and the "not a skipped run" proof. `LAZYVIEW_TEST_RECOVERY_KILLS` (3 on the gate,
//! 20 nightly) and `LAZYVIEW_TEST_RECOVERY_LOOP` (5) size the loops; the kill points and the workload
//! come from the run seed, which every test prints. What the engine is (its binary, its env) is
//! `common::engineproc`'s; where a kill lands and what a recovery prints is `common::recovery`'s.

mod common;

use anyhow::{ensure, Context, Result};
use common::engineproc::{self, EngineProc, KillPoint, KillPointPlan};
use common::pg;
use common::recovery::{self, Table, WriteCounters};
use common::toxi::{self, Toxic};
use common::*;
use lazyview::source::Position;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Boot + first build of three small views, patient enough for a loaded shared server.
const T_BOOT: Duration = Duration::from_secs(120);
/// The binlog stream opening, and the drain a kill or a cut leaves behind.
const T_STREAM: Duration = Duration::from_secs(150);
/// How long a replaced process gets to prove it wrote nothing (one lock beat is 1 s).
const T_FENCE: Duration = Duration::from_secs(40);
/// How long an armed `reset_peer` gets to show up in the engine's own reconnect counters. The RST
/// only goes out when the proxy next touches the stream, so this is patience for the *fault*, not a
/// recovery budget: a shared, loaded server delivered it in 30 s on the first run of this mode.
const T_CUT: Duration = Duration::from_secs(60);

/// Id space for the bursts: above the seeded rows and above the writer's own ids, so no two writers
/// of one schema ever collide on a primary key.
static BURST_SEQ: AtomicI64 = AtomicI64::new(1_000_000);

/// The env every engine of this suite runs with: a position save every 500 ms (so what one kill
/// loses is at most that tail, and "the saved position advanced" is measurable), a small batch window
/// so a flush is in flight often enough for a swap or a cut to land on one, and the lease the
/// takeover of M2 waits on.
fn engine_env<'a>(extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut v = vec![
        ("LAZYVIEW_POSITION_PERSIST_MS", "500"),
        ("LAZYVIEW_BATCH_WINDOW_MS", "50"),
        ("LAZYVIEW_LOCK_HEARTBEAT_MS", "1000"),
        ("LAZYVIEW_LOCK_LEASE_MS", "4000"),
    ];
    v.extend_from_slice(extra);
    v
}

/// One schema: the neutral tables, seeded rows, and the three incremental views in `_lazyview`.
/// `with_views = false` leaves `_lazyview` empty, which is how a first build is started on demand.
async fn setup(
    ctx: &Ctx,
    tag: &str,
    seed: u64,
    with_views: bool,
) -> Result<(Db, Vec<recovery::View>)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    exec_all(&mut c, &recovery::neutral_ddl()).await?;
    ensure_lazyview_table(&mut c).await?;
    let views = recovery::neutral_views();
    if with_views {
        let specs: Vec<ViewSpec> = views.iter().map(|v| v.spec.clone()).collect();
        add_views(&mut c, &specs).await?;
    }
    exec_all(&mut c, &recovery::seed_statements(seed, 200)).await?;
    c.disconnect().await.ok();
    Ok((db, views))
}

/// Puts `n` rows in flight through the source and answers with the source's binlog position after
/// them: the tail a killed process must replay, and the position a catch-up wait ends on.
async fn burst(db: &Db, seed: u64, n: usize) -> Result<Position> {
    let mut rng = engineproc::Rng::new(seed);
    let base = BURST_SEQ.fetch_add(n as i64, Ordering::SeqCst);
    let mut c = db.conn().await?;
    for chunk in 0..n.div_ceil(500) {
        let lo = base + (chunk * 500) as i64;
        let hi = (base + ((chunk + 1) * 500) as i64).min(base + n as i64);
        let mut sql = String::from("INSERT INTO items (id, entity_id, qty, price) VALUES ");
        for id in lo..hi {
            let _ = write!(
                sql,
                "({id}, {}, {}, '{}.{:02}'),",
                1 + rng.below(8) as i64,
                rng.below(40) as i64,
                rng.below(500),
                rng.below(100),
            );
        }
        sql.pop();
        exec(&mut c, &sql).await?;
    }
    let p = server_position(&mut c).await?;
    c.disconnect().await.ok();
    Ok(p)
}

async fn server_position_of(db: &Db) -> Result<Position> {
    let mut c = db.conn().await?;
    let p = server_position(&mut c).await?;
    c.disconnect().await.ok();
    Ok(p)
}

/// `_lazyview_status.epoch` for this suite's instance: the number every position write of a replaced
/// instance is checked against (the fencing of review A-06 condition 3).
async fn status_epoch(db: &Db) -> Result<i64> {
    let mut c = db.conn().await?;
    let e = scalar_i64(
        &mut c,
        "SELECT COALESCE(MAX(`epoch`), 0) FROM `_lazyview_status` WHERE `instance_id` = 'test'",
    )
    .await?;
    c.disconnect().await.ok();
    Ok(e)
}

/// The four reasons the engine's reader says a binlog stream came back for, and no others: a cut
/// that lands moves these and nothing else does (F6 — a scenario whose fault never reached the
/// counters is a broken test, not a recovered system).
const RECONNECT_REASONS: [&str; 4] = [
    "stream_reconnect_transient",
    "stream_reconnect_closed",
    "stream_reconnect_silence",
    "stream_reconnect_undecodable",
];

/// Reconnects the engine counted between two scrapes of the same process.
fn reconnects_since(a: &engineproc::Scraped, b: &engineproc::Scraped) -> u64 {
    RECONNECT_REASONS.iter().map(|n| b.delta(a, n)).sum()
}

/// Reconnects a process has counted since it started — the only window a replacement can speak for.
fn reconnects_since_start(s: &engineproc::Scraped) -> u64 {
    RECONNECT_REASONS.iter().map(|n| s.counter(n)).sum()
}

/// `SIGKILL`s the process, or reports that it had already left (a `setup` run that finished before
/// the signal arrived is a fact the table prints, not a failure).
fn kill_or_gone(e: &mut EngineProc) -> bool {
    e.kill_now().is_ok()
}

/// Kills `old` and measures the process that replaces it: the times are always the replacement's,
/// from its own spawn (the owner's ruling on where "process start" is).
async fn kill_and_recover(
    ctx: &Ctx,
    db: &Db,
    table: &mut Table,
    mode: &str,
    point: &str,
    n_views: usize,
    mut old: EngineProc,
) -> Result<()> {
    let pid = old.pid();
    let killed = kill_or_gone(&mut old);
    let st = old.wait_exit(Duration::from_secs(30))?;
    let mut next = EngineProc::start(ctx, db, &engine_env(&[])).await?;
    let at_spawn = next.scrape().await.ok().and_then(|s| s.applied_position());
    let t = recovery::recover(&mut next, db, T_STREAM).await?;
    let s = next.scrape().await?;
    ensure!(
        recovery::rebuilds_over(s.counter("full_rebuilds"), n_views) == 0,
        "{} rebuilt an incremental view after the kill (full_rebuilds {} for {n_views} views); \
         log tail:\n{}",
        next.label,
        s.counter("full_rebuilds"),
        next.log_tail(15)
    );
    recovery::assert_views_exact(db, &recovery::neutral_views()).await?;
    table.record(
        mode,
        point,
        t,
        format!(
            "old pid {pid} {} exit {st:?}, applied at spawn {at_spawn:?}, saved {saved:?}",
            if killed { "killed" } else { "already gone" },
            saved = recovery::saved_position(db).await?
        ),
    );
    next.stop()?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// M1 — SIGKILL: the kernel's own kill. The sockets close, the servers see the drop, and the session
// lock goes with the connection.
// ---------------------------------------------------------------------------------------------

/// Kills the real binary at seeded points across the whole lifecycle — startup, the setup and
/// migration run, mid first build, mid catch-up, steady state, after a position save, with a swap
/// pending and the target transaction open, and on a graceful stop — and measures what the next
/// instance takes to come back. The two "any instant" points fire at a seeded wall-clock offset
/// after the spawn, which is also the model of the OOM killer picking a process at random.
#[test]
fn m1_sigkill_at_seeded_kill_points() {
    scenario("rec_m1_kill", |ctx| async move {
        let seed = ctx.sub_seed("m1");
        let (db, views) = setup(&ctx, "rec_m1", seed, true).await?;
        let n_views = views.len();
        let mut plan = KillPointPlan::new(seed);
        let mut table = Table::default();
        let n = recovery::kills_n();
        ctx.log(format!(
            "M1 SIGKILL: seed {seed:#x}, {n} kills, points {:?}",
            plan.peek(n).iter().map(|p| p.name()).collect::<Vec<_>>()
        ));
        let mut rng = engineproc::Rng::new(seed);
        for i in 0..n {
            let point = plan.next();
            let backlog = burst(&db, seed ^ i as u64, 3000).await?;
            // The setup run, and a build that starts on demand, need the process started a
            // different way; every other point kills one engine against the schema as it stands.
            let mut e = match point {
                KillPoint::Setup => {
                    EngineProc::start_with(
                        &ctx,
                        &db,
                        &engine_env(&[("LAZYVIEW_TEST_SKIP_SETUP", "1")]),
                        &["setup"],
                    )
                    .await?
                }
                KillPoint::FirstBuild => {
                    let e = EngineProc::start(&ctx, &db, &engine_env(&[])).await?;
                    let mut c = db.conn().await?;
                    exec(&mut c, "DELETE FROM `_lazyview`").await?;
                    c.disconnect().await.ok();
                    // Streaming with nothing defined, then the definitions appear: the build starts
                    // here and the kill lands inside it (also §5's "a build starts again from zero").
                    e.wait_streaming(T_STREAM).await?;
                    let mut c = db.conn().await?;
                    let specs: Vec<ViewSpec> = views.iter().map(|v| v.spec.clone()).collect();
                    add_views(&mut c, &specs).await?;
                    c.disconnect().await.ok();
                    e
                }
                _ => EngineProc::start(&ctx, &db, &engine_env(&[])).await?,
            };
            if matches!(point, KillPoint::Startup | KillPoint::Setup) {
                // "Killed -9 at any instant": a seeded offset after the spawn, no observable to
                // wait for — the same shape as the OOM killer choosing the process at random.
                tokio::time::sleep(Duration::from_millis(rng.below(2000) as u64)).await;
            } else if point == KillPoint::Swap {
                // The swap is induced black-box: our own session holds the view table with
                // `LOCK TABLES … WRITE`, so the engine's next flush waits with its target transaction
                // open (the N-01/N-02 mechanic) and `inflight_rows` stays above zero for the kill to
                // land on. The table has to be live first — an engine killed before its first build
                // has nothing to swap into, and a lock on a table that is not there is an error, not
                // a swap. And the process itself has to be caught up before the lock lands: a fresh
                // one still building its views writes into that very table during the build, so the
                // lock would stall the build — not a flush — and the counter would never leave zero.
                recovery::recover(&mut e, &db, T_STREAM).await?;
                recovery::wait_table(&db, "v_items", T_STREAM).await?;
                let held = recovery::hold_writer_lock(&db, "v_items").await?;
                // And the work has to arrive *behind* the lock: a flush of events the engine has
                // already digested is no flush at all, and the counter would never leave zero.
                let pending = burst(&db, seed ^ 0x5a1, 3000).await?;
                let landed = recovery::wait_kill_point(&e, point, &pending, &None, T_STREAM).await;
                recovery::release(held).await;
                landed?;
            } else if point == KillPoint::CatchUp {
                // A backlog only exists for a process that resumes: an engine that starts without a
                // saved position takes the whole burst into its snapshot build and streams from the
                // current position, so the applied counter never sits behind one and the wait is
                // undecidable. Bring this process up to the source first (the shape of a crash after
                // a position was saved), then put a second burst behind it and land the kill inside
                // that drain — the same sequence M1-targets measures the catch-up on.
                recovery::recover(&mut e, &db, T_STREAM).await?;
                let pending = burst(&db, seed ^ ((i as u64) + 31), 3000).await?;
                recovery::wait_kill_point(&e, point, &pending, &None, T_STREAM).await?;
            } else {
                recovery::wait_kill_point(&e, point, &backlog, &None, T_STREAM).await?;
            }
            if point == KillPoint::Shutdown {
                // Stop, not kill: the graceful path, measured the same way.
                let pid = e.pid();
                let t0 = Instant::now();
                let st = e.stop()?;
                let mut next = EngineProc::start(&ctx, &db, &engine_env(&[])).await?;
                let t = recovery::recover(&mut next, &db, T_STREAM).await?;
                table.record(
                    "M1-stop",
                    point.name(),
                    t,
                    format!(
                        "graceful exit in {:.2?}: {st:?}, old pid {pid}",
                        t0.elapsed()
                    ),
                );
                next.stop()?;
                continue;
            }
            kill_and_recover(&ctx, &db, &mut table, "M1", point.name(), n_views, e).await?;
        }
        table.summary();
        Ok(())
    });
}

/// The crash loop: K consecutive kills, each landing during the previous recovery (the health port
/// has answered, streaming is up, the backlog is still draining, plus a seeded jitter). Every cycle
/// must move the saved position forward: "a crash caused by the input repeats — the restart reads
/// the same events" (§5 exception row 1) is exactly a position that never advances.
#[test]
fn m1_crash_loop_each_kill_lands_during_the_previous_recovery() {
    scenario("rec_m1_loop", |ctx| async move {
        let seed = ctx.sub_seed("loop");
        let (db, views) = setup(&ctx, "rec_m1_loop", seed, true).await?;
        let k = recovery::loop_k();
        let mut table = Table::default();
        let mut rng = engineproc::Rng::new(seed);
        ctx.log(format!(
            "M1 crash loop: seed {seed:#x}, {k} consecutive kills, each during the previous recovery"
        ));
        let w = recovery::Writer::start(&db, seed, 8, Duration::from_millis(50))?;
        let mut e = EngineProc::start(&ctx, &db, &engine_env(&[])).await?;
        let mut prev = recovery::saved_position(&db).await?;
        for i in 0..k {
            e.wait_startup(T_BOOT).await?;
            let backlog = burst(&db, seed ^ (i as u64 + 7), 2000).await?;
            // A kill in the middle of the recovery that is still running: the position save is what
            // proves the instance got far enough to have something to lose — and it has to be the
            // saved *row*, not the applied counter, whose lead over the row is exactly the tail this
            // mode is about losing and replaying.
            let saved = recovery::wait_position_saved(&db, &prev, T_STREAM).await?;
            tokio::time::sleep(Duration::from_millis(rng.below(400) as u64)).await;
            let pid = e.pid();
            let killed = kill_or_gone(&mut e);
            let st = e.wait_exit(Duration::from_secs(30))?;
            let now = recovery::saved_position(&db).await?;
            ensure!(
                now > prev,
                "cycle {}: the saved position did not advance ({} → {}): this restart is reading \
                 the same events again instead of moving forward",
                i + 1,
                prev.map(|p| p.to_string()).unwrap_or_default(),
                now.map(|p| p.to_string()).unwrap_or_default()
            );
            prev = now.clone();
            recovery::measured(format!(
                "M1-loop kill #{i:02}: pid {pid} {} after {:.2?} alive, exit {st:?}, saved {now:?} \
                 (this instance saved {saved:?} and was killed with the source at {backlog:?})",
                if killed { "killed" } else { "already gone" },
                e.spawned_at.elapsed()
            ));
            e = EngineProc::start(&ctx, &db, &engine_env(&[])).await?;
        }
        // The last one gets to finish — but only after the workload stops: a writer still running
        // would keep putting rows into the base tables that the view has not seen yet, and an
        // exactness check taken then compares a view against a table that moved under it.
        let written = w.stop().await?;
        let t = recovery::recover(&mut e, &db, T_STREAM).await?;
        ensure!(written > 0, "the writer ran no statements at all");
        let s = e.scrape().await?;
        ensure!(
            recovery::rebuilds_over(s.counter("full_rebuilds"), views.len()) == 0,
            "the crash loop rebuilt an incremental view (full_rebuilds {})",
            s.counter("full_rebuilds")
        );
        recovery::assert_views_exact(&db, &views).await?;
        table.record(
            "M1-loop",
            "final drain",
            t,
            format!("after {k} consecutive kills, writer ran {written} statements"),
        );
        table.summary();
        e.stop()?;
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// M2 — pull the plug. A host that dies without closing its sockets: the server sees no FIN and no
// RST, so the session — and with it the instance lock — would live out `wait_timeout` (8 h by
// default) if the lock did not carry a lease (review A-06).
// ---------------------------------------------------------------------------------------------

#[test]
fn m2_pull_the_plug_sigstop_takeover() {
    scenario("rec_m2_stop", |ctx| async move {
        let seed = ctx.sub_seed("m2");
        let (db, views) = setup(&ctx, "rec_m2", seed, true).await?;
        let mut table = Table::default();
        let mut old = EngineProc::start(&ctx, &db, &engine_env(&[])).await?;
        old.wait_streaming(T_STREAM).await?;
        old.wait_ready(T_BOOT).await?;
        // Quiesce first: the old instance has applied everything it has to apply, so anything it
        // writes after the takeover is the defect, not a race with the workload.
        let backlog = burst(&db, seed, 1500).await?;
        recovery::wait_kill_point(&old, KillPoint::Steady, &backlog, &None, T_STREAM).await?;
        let quiet = WriteCounters::of(&old.scrape().await?);
        let epoch0 = status_epoch(&db).await?;

        old.freeze()?;
        recovery::wait_frozen(old.pid(), Duration::from_secs(5)).await?;
        ctx.log(format!(
            "old instance frozen (pid {}, sockets open, no FIN and no RST); starting the replacement",
            old.pid()
        ));
        // Work arrives while the host is dead: this is what the replacement has to catch up on, and
        // what the replaced instance must never write.
        burst(&db, seed ^ 0x5eed, 2000).await?;

        let mut next = EngineProc::start(&ctx, &db, &engine_env(&[])).await?;
        // It waits on the *lock*, not on the old process giving it up: standby is what it must be
        // while the frozen host still holds it (A-06 condition 1 — something has to release it).
        next.wait_scrape(T_BOOT, "the replacement to stand by", |s| {
            s.gauge("standby") == 1.0
        })
        .await
        .context(
            "the replacement never even saw the lock held elsewhere: the frozen host released \
                  it at once, which is the SIGKILL case, not this one",
        )?;
        let t = recovery::recover(&mut next, &db, T_STREAM).await?;
        let mech = recovery::takeover_mechanism(&next.log_tail(400));
        table.record(
            "M2",
            "sigstop-takeover",
            t,
            format!(
                "standby observed; takeover by {mech}; lease {} ms, beat {} ms",
                recovery::lease_env()[1].1,
                recovery::lease_env()[0].1
            ),
        );

        // A-06 condition 3: the replaced instance that is still alive writes nothing.
        old.resume()?;
        let t0 = Instant::now();
        loop {
            if !engineproc::pid_alive(old.pid()) {
                // It left on its own: nothing more it could write.
                break;
            }
            match old.scrape().await {
                Ok(s) => {
                    let now = WriteCounters::of(&s);
                    ensure!(
                        (now.rows, now.flushes) == (quiet.rows, quiet.flushes),
                        "the replaced instance wrote {} more MV rows in {} more flushes after the \
                         takeover (A-06 condition 3: a replaced instance must stop writing); its \
                         own state: {}",
                        now.rows.saturating_sub(quiet.rows),
                        now.flushes.saturating_sub(quiet.flushes),
                        s.state_line()
                    );
                    if now.fenced > 0 {
                        // Its own lock task saw the lock held elsewhere and published the verdict
                        // that stops its writes: the counter is the engine's own words.
                        break;
                    }
                }
                Err(e) => ctx.log(format!("old instance not answering on /metrics yet: {e:#}")),
            }
            ensure!(
                t0.elapsed() < T_FENCE,
                "the replaced instance neither self-fenced nor exited within {T_FENCE:?} (no fence, \
                 no exit); log tail:\n{}",
                old.log_tail(15)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        recovery::measured(format!(
            "M2 replaced instance: wrote nothing after the takeover, and noticed it had been \
             replaced in {:.2?}",
            t0.elapsed()
        ));
        let epoch1 = status_epoch(&db).await?;
        ensure!(
            epoch1 > epoch0,
            "the fencing epoch did not move on the takeover ({epoch0} → {epoch1}): a position write \
             of the old instance would not have been refused"
        );
        let _ = old.wait_exit(Duration::from_secs(20));
        recovery::assert_views_exact(&db, &views).await?;
        let s = next.scrape().await?;
        ensure!(
            recovery::rebuilds_over(s.counter("full_rebuilds"), views.len()) == 0,
            "the takeover rebuilt an incremental view (full_rebuilds {})",
            s.counter("full_rebuilds")
        );
        table.summary();
        next.stop()?;
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// M3 — the connection crash: the shared toxiproxy cuts every connection the engine has to the
// source while a flush is in flight.
// ---------------------------------------------------------------------------------------------

#[test]
fn m3_connection_crash_mid_flush() {
    let _one = toxi::exclusive();
    scenario("rec_m3_cut", |ctx| async move {
        let seed = ctx.sub_seed("m3");
        let (db, views) = setup(&ctx, "rec_m3", seed, true).await?;
        let Some(toxi) = toxi::open_if_enabled(&ctx).await? else {
            return Ok(());
        };
        let _clean = toxi.for_schema(&db.name);
        let p = toxi
            .proxy(&db.name, "src", &toxi::upstream(ctx.flavor()))
            .await?;
        toxi.wait_listening(&p).await?;
        // Every connection the engine has to the source crosses the proxy, as every connection of a
        // pod crosses one NIC: the dump link, the write link, the lock session.
        let dsn = toxi::dsn_via_proxy(&db.dsn, p.listen)?;
        let mut e =
            EngineProc::start_with(&ctx, &db, &engine_env(&[("LAZYVIEW_DSN", &dsn)]), &[]).await?;
        let mut table = Table::default();
        let t = recovery::recover(&mut e, &db, T_STREAM).await?;
        table.record("M3", "boot via proxy", t, format!("proxy {}", p.name));
        let (mut boot_s, mut stream_s) = (t.boot_s, t.stream_s);

        // Widen the window the cut has to land in: a big burst plus the writers, so a flush is open
        // when the RST goes out (the 20k-row transaction precedent of `tests/efficiency.rs`).
        let w = recovery::Writer::start(&db, seed, 12, Duration::from_millis(25))?;
        let backlog = burst(&db, seed ^ 11, 20_000).await?;
        recovery::wait_kill_point(&e, KillPoint::Swap, &backlog, &None, T_STREAM).await?;
        let before = e.scrape().await?;
        let target = server_position_of(&db).await?;

        let t0 = Instant::now();
        toxi.arm(&p, &Toxic::reset_peer(0).named("cut")).await?;
        // First the fault has to land, in the engine's own words: a round of the binlog loop ended
        // and the reader reconnected. A process the cut took down with it landed too — its stream
        // died with it — and that is the plan's other branch, measured on the replacement.
        let (mut landed, mut replaced, mut red) = (false, false, false);
        loop {
            if !engineproc::pid_alive(e.pid()) {
                // The cut took the process with it: its stream died with it, which is the fault
                // landing, and the plan's other branch — measured on the replacement below.
                landed = true;
                break;
            }
            if let Ok(s) = e.scrape().await {
                landed |= reconnects_since(&before, &s) > 0;
                if e.get("/livez", false)
                    .await
                    .map(|(c, _)| c != 200)
                    .unwrap_or(false)
                {
                    red = true;
                }
            }
            if landed {
                break;
            }
            ensure!(
                t0.elapsed() <= T_CUT,
                "reset_peer never reached the engine's binlog stream within {T_CUT:?}: no reconnect \
                 was counted; log tail:\n{}",
                e.log_tail(15)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let land_s = t0.elapsed().as_secs_f64();
        ensure!(
            landed,
            "reset_peer never reached the engine's binlog stream: no reconnect was counted and the \
             process survived; log tail:\n{}",
            e.log_tail(15)
        );
        // Heal the link before asking anything to come back. `reset_peer` at toxicity 1.0 resets
        // every *new* connection as well, so a process started into an armed proxy never streams at
        // all (found on this mode's first run): the operator's first move against a dead link is on
        // the link, not on the process.
        toxi.remove_toxic(&p, "cut").await?;
        // The number the plan asks for: toxic-on to lag 0, on whichever process gets there.
        let mut t1 = Instant::now();
        loop {
            let alive = engineproc::pid_alive(e.pid());
            if alive {
                // A scrape that fails is the link still healing (the health port is not proxied, so
                // this is the engine being mid-restart of its own stream, not a lost test).
                if let Ok(s) = e.scrape().await {
                    let caught = s.gauge("streaming") == 1.0
                        && s.gauge("inflight_rows") == 0.0
                        && s.applied_position().as_ref().is_some_and(|a| *a >= target);
                    if caught {
                        break;
                    }
                    if e.get("/livez", false)
                        .await
                        .map(|(c, _)| c != 200)
                        .unwrap_or(false)
                    {
                        red = true;
                    }
                }
            }
            if !alive || t1.elapsed() > T_STREAM {
                // The plan's other branch: the operator's move is a replacement, and the number that
                // matters is toxic-on to lag 0 whichever process got there.
                ensure!(
                    !replaced,
                    "neither the engine nor its replacement caught up within {T_STREAM:?} after the \
                     cut healed; log tail:\n{}",
                    e.log_tail(15)
                );
                replaced = true;
                let _ = e.wait_exit(Duration::from_secs(10));
                e = EngineProc::start_with(&ctx, &db, &engine_env(&[("LAZYVIEW_DSN", &dsn)]), &[])
                    .await?;
                let nt = recovery::recover(&mut e, &db, T_STREAM).await?;
                (boot_s, stream_s) = (nt.boot_s, nt.stream_s);
                // The replacement gets its own patience: the window that produced this number is
                // over, and the next process's name is not in it.
                t1 = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let cut_s = t0.elapsed().as_secs_f64();
        let written = w.stop().await?;
        // Nothing may still be on the link when the exactness check reads the tables: the drain above
        // ended on the position taken before the cut, and the writer kept writing after that, so the
        // last of its statements are in flight. Stop the writer, then drain to the source's own end.
        let _ = recovery::recover(&mut e, &db, T_STREAM).await?;
        let after = e.scrape().await?;
        // The reconnects are the cut process's own story when it survived; a replacement can only
        // report its own, counted from its own start.
        let reconnects = if replaced {
            reconnects_since_start(&after)
        } else {
            reconnects_since(&before, &after)
        };
        ensure!(written > 0, "the writer ran no statements at all");
        let events = if replaced {
            after.counter("events")
        } else {
            after.delta(&before, "events")
        };
        if replaced {
            ensure!(
                recovery::rebuilds_over(after.counter("full_rebuilds"), views.len()) == 0,
                "the replacement rebuilt an incremental view after the cut (full_rebuilds {})",
                after.counter("full_rebuilds")
            );
        } else {
            ensure!(
                after.delta(&before, "full_rebuilds") == 0,
                "the cut made an incremental view rebuild ({} more full rebuilds)",
                after.delta(&before, "full_rebuilds")
            );
        }
        recovery::assert_views_exact(&db, &views).await?;
        table.record(
            "M3",
            if replaced { "reset-peer (replacement)" } else { "reset-peer" },
            recovery::Times {
                boot_s,
                stream_s,
                catch_s: cut_s,
                livez_red: red,
            },
            format!(
                "landed in {land_s:.3}s, healed (toxic off) and reached lag 0 {cut_s:.3}s after the \
                 cut over {reconnects} stream reconnects, {written} writer statements, {events} \
                 binlog events read",
            ),
        );
        table.summary();
        e.stop()?;
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// M1 with the other target families: the same kill, with view tables the engine maintains on a
// second MySQL server and on PostgreSQL. A kill mid-flush must lose and duplicate nothing on a
// target either, and a target's rows are compared the same way — server-side aggregates only, never
// a copy of the view.
// ---------------------------------------------------------------------------------------------

#[test]
fn m1_kill_with_a_mysql_and_a_postgresql_target() {
    let pgs = match pg::servers_or_skip("rec_m1_targets") {
        Some(p) => Arc::new(p),
        None => return,
    };
    scenario_multi("rec_m1_targets", 1, move |ctx| {
        let pgs = pgs.clone();
        async move {
            let seed = ctx.sub_seed("m1t");
            let (src, views) = setup(&ctx, "rec_m1t_src", seed, false).await?;
            let my = ctx.new_db_on(1, "rec_m1t_my").await?;
            let pgdb = pg::new_pg_db(&pgs[0], "rec_m1t_pg").await?;
            let join = views
                .iter()
                .find(|v| v.spec.name == "v_join")
                .context("the neutral set has a join view")?
                .clone();
            let mut c = src.conn().await?;
            let mut specs: Vec<ViewSpec> = views
                .iter()
                .filter(|v| v.spec.name != "v_join")
                .map(|v| v.spec.clone())
                .collect();
            specs.push(join.spec.clone().with_targets("my,pg"));
            add_views(&mut c, &specs).await?;
            c.disconnect().await.ok();

            let env = target_env(&[("my", &my.dsn), ("pg", &pgdb.dsn)]);
            let ov = ov_refs(&env);
            let mut table = Table::default();
            let mut e = EngineProc::start(&ctx, &src, &engine_env(&ov)).await?;
            let t = recovery::recover(&mut e, &src, T_STREAM).await?;
            table.record("M1-targets", "first build", t, "both targets live");

            // The kill lands with work in flight on both targets.
            let backlog = burst(&src, seed ^ 3, 8000).await?;
            recovery::wait_kill_point(&e, KillPoint::CatchUp, &backlog, &None, T_STREAM).await?;
            let pid = e.pid();
            let _ = kill_or_gone(&mut e);
            let st = e.wait_exit(Duration::from_secs(30))?;
            let mut next = EngineProc::start(&ctx, &src, &engine_env(&ov)).await?;
            let t = recovery::recover(&mut next, &src, T_STREAM).await?;
            let s = next.scrape().await?;
            ensure!(
                recovery::rebuilds_over(s.counter("full_rebuilds"), views.len()) == 0,
                "the kill rebuilt an incremental view on a target (full_rebuilds {})",
                s.counter("full_rebuilds")
            );
            table.record(
                "M1-targets",
                "catch-up after the kill",
                t,
                format!("old pid {pid} exit {st:?}"),
            );

            // The source's own views exact, and the join view exact on each target: the same
            // aggregate read from the target's table and from the definition over the source's base
            // tables. PostgreSQL has no CRC32, so its half compares the two numbers that mean the
            // same on both servers — the row count and the sum of the money column.
            // The source's own views exact — every view but the join, which this scenario
            // materializes on the targets only, so the source has no table for it to check. The
            // join's own rows are checked against the definition over the source's base tables.
            let src_views: Vec<recovery::View> = views
                .iter()
                .filter(|v| v.spec.name != "v_join")
                .cloned()
                .collect();
            recovery::assert_views_exact(&src, &src_views).await?;
            let want = {
                let mut c = src.conn().await?;
                let w = recovery::checksum(
                    &mut c,
                    &format!("({}) lv_expected", join.spec.select),
                    &join.cols,
                )
                .await?;
                c.disconnect().await.ok();
                w
            };
            let mut mc = my.conn().await?;
            let got = recovery::checksum(&mut mc, &ident(&join.spec.name), &join.cols).await?;
            mc.disconnect().await.ok();
            ensure!(
                got == want,
                "the join view on the MySQL target holds {got:?}, its definition over the source \
                 holds {want:?}"
            );
            let (want_n, want_sum) = {
                let mut c = src.conn().await?;
                let r = rows(
                    &mut c,
                    &format!(
                        "SELECT COUNT(*), COALESCE(SUM(price), 0) FROM ({}) lv_expected",
                        join.spec.select
                    ),
                )
                .await?;
                c.disconnect().await.ok();
                let one = r.first().context("the server answered no row")?;
                (
                    one.first()
                        .and_then(|v| v.clone())
                        .unwrap_or_default()
                        .parse::<i64>()?,
                    one.get(1)
                        .and_then(|v| v.clone())
                        .unwrap_or_else(|| "0".to_string()),
                )
            };
            let pc = pgdb.client().await?;
            let row = pc
                .query_one(
                    &format!(
                        "SELECT COUNT(*), COALESCE(SUM(price), 0)::text FROM {}.{}",
                        pgdb.schema, join.spec.name
                    ),
                    &[],
                )
                .await
                .with_context(|| format!("reading {} off the PostgreSQL target", join.spec.name))?;
            let pg_n: i64 = row.get(0);
            let pg_sum: String = row.get(1);
            drop(pc);
            ensure!(
                pg_n == want_n && pg_sum == want_sum,
                "the join view on the PostgreSQL target holds {pg_n} rows summing to {pg_sum}, its \
                 definition over the source holds {want_n} rows summing to {want_sum}"
            );
            recovery::measured(format!(
                "M1-targets: PostgreSQL target {} exact after the kill ({} rows, sum {pg_sum})",
                pgdb.flavor, pg_n
            ));
            table.summary();
            next.stop()?;
            pg::drop_dbs(&[pgdb]).await?;
            Ok(())
        }
    });
}
