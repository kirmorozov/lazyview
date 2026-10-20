//! Concurrent refreshes (`LAZYVIEW_SUPERVISOR=fixed`, src/supervisor.rs): many transactions hit
//! the same units across consecutive flushes while chunks are read and written concurrently
//! (small chunks, several writes in flight per target) and a reader holds row locks on the
//! view tables now and then (a slowed target). No write may be lost or land after a newer one:
//! a monitor polls the views during the load and fails if a row's sequence number ever goes
//! back, and every phase ends with the oracle matching. Each test runs on every flavor in
//! `LAZYVIEW_TEST_DSNS`.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Result};
use common::*;
use mysql_async::prelude::Queryable;

/// Rows with a sequence number that only ever grows (`seq`): 1..=HOT.
const HOT: i64 = 60;
const GROUPS: i64 = 12;

const DDL: &[&str] = &[
    "CREATE TABLE grps (id INT PRIMARY KEY, name VARCHAR(40) NOT NULL)",
    "CREATE TABLE ctr (id INT PRIMARY KEY, grp INT NOT NULL, v BIGINT NOT NULL, seq BIGINT NOT NULL, \
     note VARCHAR(60) NULL, KEY (grp))",
];

fn specs(targets: Option<&str>) -> Vec<ViewSpec> {
    let v = vec![
        // One row per unit (PRIMARY KEY): upserts.
        ViewSpec::new(
            "s_ctr",
            Kind::Incremental,
            "SELECT id, grp, v, seq, note FROM ctr",
        ),
        ViewSpec::new(
            "s_grp",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n, SUM(v) AS sv, MAX(seq) AS mseq FROM ctr GROUP BY grp",
        ),
        // Unit + lineage (`_lv_row`).
        ViewSpec::new(
            "s_join",
            Kind::Incremental,
            "SELECT c.id, c.seq, c.v, g.name FROM ctr c JOIN grps g ON g.id = c.grp",
        ),
        // Many rows per unit, written per unit.
        ViewSpec::new(
            "s_fan",
            Kind::Incremental,
            "SELECT g.id, g.name, c.id AS cid, c.v, c.seq FROM grps g JOIN ctr c ON c.grp = g.id",
        ),
    ];
    match targets {
        Some(t) => v.into_iter().map(|s| s.with_targets(t)).collect(),
        None => v,
    }
}

async fn setup(db: &Db) -> Result<()> {
    let mut c = db.conn().await?;
    exec_all(
        &mut c,
        &DDL.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    )
    .await?;
    let grps: Vec<String> = (1..=GROUPS)
        .map(|g| format!("({g}, 'group {g}')"))
        .collect();
    exec(
        &mut c,
        &format!("INSERT INTO grps VALUES {}", grps.join(", ")),
    )
    .await?;
    let rows: Vec<String> = (1..=HOT)
        .chain(1000..1400)
        .map(|i| format!("({i}, {}, 0, 0, 'n{i}')", i % GROUPS + 1))
        .collect();
    exec(
        &mut c,
        &format!(
            "INSERT INTO ctr (id, grp, v, seq, note) VALUES {}",
            rows.join(", ")
        ),
    )
    .await?;
    c.disconnect().await.ok();
    Ok(())
}

fn xorshift(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

/// One writer: transactions over a few hot rows (always `seq + 1`), plus moves, inserts and
/// deletes of cold rows (ids >= 1000) and group renames.
async fn writer(db: Db, mut seed: u64, stop: Arc<AtomicBool>, txns: Arc<AtomicU64>) -> Result<()> {
    let mut c = db.conn().await?;
    while !stop.load(Ordering::Relaxed) {
        let r = xorshift(&mut seed);
        let hot: Vec<String> = (0..1 + r % 5)
            .map(|k| ((xorshift(&mut seed) + k) % HOT as u64 + 1).to_string())
            .collect();
        let mut stmts = vec![format!(
            "UPDATE ctr SET v = v + {}, seq = seq + 1 WHERE id IN ({})",
            r % 7 + 1,
            hot.join(",")
        )];
        let cold = 1000 + (xorshift(&mut seed) % 600) as i64;
        stmts.push(match r % 5 {
            0 => format!(
                "UPDATE ctr SET grp = {} WHERE id = {cold}",
                xorshift(&mut seed) % GROUPS as u64 + 1
            ),
            1 => format!(
                "INSERT INTO ctr (id, grp, v, seq, note) VALUES ({cold}, {}, 1, 0, 'ins') \
                 ON DUPLICATE KEY UPDATE v = v + 1",
                cold % GROUPS + 1
            ),
            2 => format!("DELETE FROM ctr WHERE id = {cold}"),
            3 => format!(
                "UPDATE grps SET name = CONCAT('group ', id, ' r', {}) WHERE id = {}",
                r % 1000,
                xorshift(&mut seed) % GROUPS as u64 + 1
            ),
            _ => format!(
                "UPDATE ctr SET note = CONCAT('n', {}) WHERE id = {cold}",
                r % 100
            ),
        });
        let r = async {
            exec(&mut c, "START TRANSACTION").await?;
            for s in &stmts {
                exec(&mut c, s).await?;
            }
            exec(&mut c, "COMMIT").await
        }
        .await;
        match r {
            Ok(()) => {
                txns.fetch_add(1, Ordering::Relaxed);
            }
            // Writers deadlock with each other now and then: drop the transaction.
            Err(e) if retryable(&e) => {
                exec(&mut c, "ROLLBACK").await.ok();
            }
            Err(e) => return Err(e),
        }
    }
    c.disconnect().await.ok();
    Ok(())
}

/// A deadlock or lock wait timeout of the test's own transactions (they compete with each
/// other and with the engine's writes).
fn retryable(e: &anyhow::Error) -> bool {
    matches!(server_code(e), Some(1213 | 1205))
}

/// Holds row locks on view rows of `db` for 300 ms every 700 ms (the engine's writes to them
/// wait meanwhile).
async fn locker(db: Db, stop: Arc<AtomicBool>) -> Result<u64> {
    let mut c = db.conn().await?;
    let mut n = 0;
    while !stop.load(Ordering::Relaxed) {
        let r = async {
            exec(&mut c, "START TRANSACTION").await?;
            let _: Vec<mysql_async::Row> = c
                .query(format!(
                    "SELECT id FROM s_ctr WHERE id <= {} FOR UPDATE",
                    HOT / 3
                ))
                .await?;
            let _: Vec<mysql_async::Row> = c
                .query("SELECT grp FROM s_grp WHERE grp <= 3 FOR UPDATE")
                .await?;
            tokio::time::sleep(Duration::from_millis(300)).await;
            exec(&mut c, "COMMIT").await
        }
        .await;
        match r {
            Ok(()) => n += 1,
            // The engine's write won the deadlock: InnoDB rolled this one back.
            Err(e) if retryable(&e) => {
                exec(&mut c, "ROLLBACK").await.ok();
            }
            Err(e) => return Err(e),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    c.disconnect().await.ok();
    Ok(n)
}

/// Polls the hot rows of `s_ctr` and `s_join` on `db`: a row's `seq` must never go back (an
/// older write landing after a newer one).
async fn monitor(db: Db, stop: Arc<AtomicBool>) -> Result<u64> {
    let mut c = db.conn().await?;
    let mut seen: HashMap<(&str, i64), i64> = HashMap::new();
    let mut polls = 0;
    while !stop.load(Ordering::Relaxed) {
        for view in ["s_ctr", "s_join"] {
            let rows: Vec<(i64, i64)> = c
                .query(format!("SELECT id, seq FROM {view} WHERE id <= {HOT}"))
                .await?;
            for (id, seq) in rows {
                let last = seen.entry((view, id)).or_insert(seq);
                if seq < *last {
                    bail!(
                        "{}.{view}: row {id} went back from seq {} to {seq}",
                        db.name,
                        *last
                    );
                }
                *last = seq;
            }
        }
        polls += 1;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    c.disconnect().await.ok();
    Ok(polls)
}

/// Runs writers, the locker on `slowed` and monitors on `watched` for `secs` seconds.
async fn load(
    ctx: &Ctx,
    src: &Db,
    slowed: &Db,
    watched: &[Db],
    secs: u64,
    label: &str,
) -> Result<()> {
    let stop = Arc::new(AtomicBool::new(false));
    let txns = Arc::new(AtomicU64::new(0));
    let mut writers = Vec::new();
    for w in 0..6u64 {
        writers.push(tokio::spawn(writer(
            src.clone(),
            ctx.sub_seed(&format!("{label}/w{w}")) | 1,
            stop.clone(),
            txns.clone(),
        )));
    }
    let lock = tokio::spawn(locker(slowed.clone(), stop.clone()));
    let mons: Vec<_> = watched
        .iter()
        .map(|d| tokio::spawn(monitor(d.clone(), stop.clone())))
        .collect();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await??;
    }
    let locks = lock.await??;
    let mut polls = 0;
    for m in mons {
        polls += m.await??;
    }
    ctx.log(format!(
        "{label}: {} transactions, {locks} lock holds, {polls} monitor polls",
        txns.load(Ordering::Relaxed)
    ));
    ensure!(
        txns.load(Ordering::Relaxed) > 50,
        "{label}: too little load"
    );
    Ok(())
}

/// Most write ops the engine ran at once on any write pool.
fn peak_writes(eng: &Engine) -> u64 {
    eng.metrics
        .pool_gauges()
        .iter()
        .filter(|p| p.pool == "write")
        .map(|p| p.peak.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0)
}

/// Hot units across consecutive flushes, chunks of 7 units, 4 reads and 4 writes in flight,
/// batch windows 0 and 200 ms; the view tables are locked now and then.
#[test]
fn sup_hot_units_converge_without_reordering() {
    scenario("sup_hot", |ctx| async move {
        for window in ["0", "200"] {
            let db = ctx.new_db(&format!("sup_hot_{window}")).await?;
            setup(&db).await?;
            let specs = specs(None);
            let ov = [
                ("LAZYVIEW_SUPERVISOR", "fixed"),
                ("LAZYVIEW_QUERY_CHUNK", "7"),
                ("LAZYVIEW_MAX_SOURCE_CONCURRENCY", "4"),
                ("LAZYVIEW_MAX_TARGET_CONCURRENCY", "4"),
                ("LAZYVIEW_BATCH_WINDOW_MS", window),
            ];
            let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov).await?;
            verify(&ctx, &mut eng, &db, &specs, "initial").await?;
            for phase in 1..=2 {
                let label = format!("window {window} ms, phase {phase}");
                load(&ctx, &db, &db, std::slice::from_ref(&db), 12, &label).await?;
                verify(&ctx, &mut eng, &db, &specs, &label).await?;
            }
            let peak = peak_writes(&eng);
            let d = eng.snapshot();
            ctx.log(format!(
                "window {window} ms: peak writes in flight {peak}; {}",
                fmt_snapshot(&d)
            ));
            ensure!(
                peak > 1,
                "window {window} ms: writes never ran concurrently (peak {peak})"
            );
            ensure!(d.mv_rows_upserted > 0, "no upserts");
            eng.stop().await?;
        }
        Ok(())
    });
}

/// The same with a second server as a target that is slowed by row locks: the source's copy
/// keeps up meanwhile, and both converge.
#[test]
fn sup_two_targets_one_slowed() {
    scenario_multi("sup_targets", 1, |ctx| async move {
        let src = ctx.new_db("sup_t_src").await?;
        let tb = ctx.new_db_on(1, "sup_t_b").await?;
        setup(&src).await?;
        let specs = specs(Some("source,b"));
        let env = target_env(&[("b", &tb.dsn)]);
        let mut ov = ov_refs(&env);
        ov.extend([
            ("LAZYVIEW_SUPERVISOR", "fixed"),
            ("LAZYVIEW_QUERY_CHUNK", "7"),
            ("LAZYVIEW_MAX_TARGET_CONCURRENCY", "3"),
            ("LAZYVIEW_TARGET_B_MAX_CONCURRENCY", "2"),
        ]);
        let (mut eng, _) = start_with_views(&ctx, &src, &specs, &ov).await?;
        let tmap: HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
        verify_targets(&ctx, &mut eng, &src, &tmap, &specs, "initial").await?;
        load(
            &ctx,
            &src,
            &tb,
            &[src.clone(), tb.clone()],
            15,
            "slowed target b",
        )
        .await?;
        verify_targets(&ctx, &mut eng, &src, &tmap, &specs, "after the load").await?;
        let caps: HashMap<String, u64> = eng
            .metrics
            .pool_gauges()
            .iter()
            .filter(|p| p.pool == "write")
            .map(|p| (p.endpoint.clone(), p.peak.load(Ordering::Relaxed)))
            .collect();
        ctx.log(format!("peak writes per target: {caps:?}"));
        ensure!(
            caps.get("b").is_some_and(|&n| (1..=2).contains(&n)),
            "target b: {caps:?} (limit 2)"
        );
        ensure!(
            caps.get("source").is_some_and(|&n| (1..=3).contains(&n)),
            "source: {caps:?} (limit 3)"
        );
        eng.stop().await?;
        Ok(())
    });
}

/// Writes that time out inside their transaction (view rows locked for longer than
/// `LAZYVIEW_TARGET_TIMEOUT_MS`) are killed before they are retried (the zombie fence), and the
/// view converges once the lock is gone.
#[test]
fn sup_timed_out_writes_are_fenced() {
    scenario("sup_fence", |ctx| async move {
        let db = ctx.new_db("sup_fence").await?;
        setup(&db).await?;
        let specs: Vec<ViewSpec> = specs(None)
            .into_iter()
            .filter(|s| s.name == "s_ctr")
            .collect();
        let ov = [
            ("LAZYVIEW_SUPERVISOR", "fixed"),
            ("LAZYVIEW_TARGET_TIMEOUT_MS", "1000"),
        ];
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let before = eng.snapshot();
        let mut lock = db.conn().await?;
        exec(&mut lock, "START TRANSACTION").await?;
        let _: Vec<mysql_async::Row> = lock
            .query("SELECT id FROM s_ctr WHERE id <= 10 FOR UPDATE")
            .await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "UPDATE ctr SET seq = seq + 1, v = v + 5 WHERE id <= 10",
        )
        .await?;
        // The engine's writes wait for the lock, time out after 1 s, are killed and retried.
        let kills = before.supervisor_zombie_kills;
        wait_until("a write to be fenced", Duration::from_secs(60), || {
            let m = eng.metrics.clone();
            async move { Ok(m.snapshot().supervisor_zombie_kills > kills) }
        })
        .await?;
        exec(&mut lock, "COMMIT").await?;
        lock.disconnect().await.ok();
        verify(&ctx, &mut eng, &db, &specs, "after the lock").await?;
        let d = eng.snapshot().since(&before);
        ctx.log(format!("fence: {}", fmt_snapshot(&d)));
        ensure!(d.supervisor_retries > 0, "no retry after a timeout");
        // Nothing of the killed transactions remains: the oracle matched above, and the
        // engine's connections that timed out are gone.
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}

/// Crash consistency (invariant I2): the engine is killed at random points while concurrent
/// refreshes are in flight under load, restarted in resume mode each time, and the views
/// converge: the saved position never passed a change that was not applied.
#[test]
fn sup_crash_during_concurrent_refreshes() {
    scenario("sup_crash", |ctx| async move {
        let db = ctx.new_db("sup_crash").await?;
        setup(&db).await?;
        let specs = specs(None);
        let ov = [
            ("LAZYVIEW_SUPERVISOR", "fixed"),
            ("LAZYVIEW_QUERY_CHUNK", "7"),
            ("LAZYVIEW_POSITION_PERSIST_MS", "100"),
        ];
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let stop = Arc::new(AtomicBool::new(false));
        let txns = Arc::new(AtomicU64::new(0));
        let writers: Vec<_> = (0..4u64)
            .map(|w| {
                tokio::spawn(writer(
                    db.clone(),
                    ctx.sub_seed(&format!("crash/w{w}")) | 1,
                    stop.clone(),
                    txns.clone(),
                ))
            })
            .collect();
        let mut seed = ctx.sub_seed("crash/at") | 1;
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        let mut c = db.conn().await?;
        for round in 1..=5 {
            tokio::time::sleep(Duration::from_millis(500 + xorshift(&mut seed) % 2500)).await;
            let flushes = eng.snapshot().flushes;
            eng.crash().await?;
            ctx.log(format!("round {round}: crashed after {flushes} flushes"));
            eng = ctx.start_engine(&db, &ov)?;
            wait_ready(&mut c, &mut eng, &names, t_ready()).await?;
        }
        stop.store(true, Ordering::Relaxed);
        for w in writers {
            w.await??;
        }
        ctx.log(format!("{} transactions", txns.load(Ordering::Relaxed)));
        verify(&ctx, &mut eng, &db, &specs, "after 5 crashes").await?;
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}
