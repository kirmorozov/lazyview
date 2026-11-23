//! Online migration: while definitions change (columns, joins, filters, mode), views are renamed
//! or a source table is ALTERed under concurrent writes, a reader polling the MV every few ms must
//! never get an error ("table doesn't exist", …), never see it empty (the oracle never is), and
//! must observe the old shape, then the new one — never back. Afterwards the MV equals the new
//! oracle.

mod common;

use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use common::oracle::fetch;
use common::workload::{ecommerce_views, names, run_until_stopped, setup_ecommerce, Ids, Sizes};
use common::*;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
struct ReadLog {
    polls: u64,
    errors: Vec<String>,
    empties: Vec<String>,
    /// Distinct consecutive (table, columns) shapes in observation order.
    shapes: Vec<(String, Vec<String>)>,
}

impl ReadLog {
    fn check(&self, what: &str) -> Result<()> {
        ensure!(self.polls > 0, "{what}: reader never polled");
        if !self.errors.is_empty() {
            bail!(
                "{what}: reader got {} error(s) in {} polls, first: {:?}",
                self.errors.len(),
                self.polls,
                &self.errors[..self.errors.len().min(3)]
            );
        }
        if !self.empties.is_empty() {
            bail!(
                "{what}: reader saw the MV empty {} time(s) in {} polls, first: {:?}",
                self.empties.len(),
                self.polls,
                &self.empties[..self.empties.len().min(3)]
            );
        }
        for (i, s) in self.shapes.iter().enumerate() {
            if self.shapes[..i].contains(s) {
                bail!(
                    "{what}: reader went back to an earlier shape: {:?}",
                    self.shapes
                );
            }
        }
        Ok(())
    }

    fn last_shape(&self) -> Option<&(String, Vec<String>)> {
        self.shapes.last()
    }
}

/// Polls `SELECT * FROM t LIMIT 1` every ~3 ms, trying `tables` in order (the first one that
/// exists counts). Order matters for renames: old name first (the new one must exist before the
/// old one disappears).
fn spawn_reader(
    db: Db,
    tables: Vec<String>,
    stop: watch::Receiver<bool>,
) -> JoinHandle<Result<ReadLog>> {
    tokio::spawn(async move {
        let mut c = db.conn().await?;
        let mut log = ReadLog::default();
        let started = Instant::now();
        while !*stop.borrow() {
            let mut outcome = None;
            for t in &tables {
                match fetch(&mut c, &format!("SELECT * FROM {} LIMIT 1", ident(t))).await {
                    Ok(tab) => {
                        let shape = (t.clone(), tab.columns.clone());
                        if log.shapes.last() != Some(&shape) {
                            log.shapes.push(shape);
                        }
                        if tab.rows.is_empty() {
                            log.empties.push(format!(
                                "{t} empty at +{:.3}s",
                                started.elapsed().as_secs_f64()
                            ));
                        }
                        outcome = Some(Ok(()));
                        break;
                    }
                    Err(e) if server_code(&e) == Some(1146) => {
                        outcome = Some(Err(format!("{e:#}")))
                    }
                    Err(e) => {
                        outcome = Some(Err(format!("{e:#}")));
                        break;
                    }
                }
            }
            if let Some(Err(e)) = outcome {
                log.errors.push(format!(
                    "+{:.3}s {}",
                    started.elapsed().as_secs_f64(),
                    clip(&e, 300)
                ));
            }
            log.polls += 1;
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        c.disconnect().await.ok();
        Ok(log)
    })
}

struct Background {
    stop: watch::Sender<bool>,
    writers: JoinHandle<Result<usize>>,
    readers: Vec<(String, JoinHandle<Result<ReadLog>>)>,
}

fn start_background(db: &Db, ids: Arc<Ids>, seed: u64, readers: &[Vec<&str>]) -> Background {
    let (tx, rx) = watch::channel(false);
    let (dbc, rxc) = (db.clone(), rx.clone());
    let writers = tokio::spawn(async move { run_until_stopped(&dbc, ids, seed, 2, rxc).await });
    let readers = readers
        .iter()
        .map(|ts| {
            (
                ts.join("|"),
                spawn_reader(
                    db.clone(),
                    ts.iter().map(|s| s.to_string()).collect(),
                    rx.clone(),
                ),
            )
        })
        .collect();
    Background {
        stop: tx,
        writers,
        readers,
    }
}

impl Background {
    async fn finish(self, ctx: &Ctx, what: &str) -> Result<Vec<ReadLog>> {
        let _ = self.stop.send(true);
        let n = self.writers.await.context("writers panicked")??;
        let mut logs = Vec::new();
        for (name, h) in self.readers {
            let log = h.await.context("reader panicked")??;
            ctx.log(format!(
                "{what}: reader {name}: {} polls, shapes {:?}; writers did {n} txns",
                log.polls,
                log.shapes.iter().map(|s| s.1.len()).collect::<Vec<_>>()
            ));
            log.check(&format!("{what} [{name}]"))?;
            logs.push(log);
        }
        Ok(logs)
    }
}

async fn setup(
    ctx: &Ctx,
    tag: &str,
    extra: Vec<ViewSpec>,
) -> Result<(
    Db,
    Arc<Ids>,
    Vec<ViewSpec>,
    std::collections::HashMap<String, u64>,
    Engine,
)> {
    setup_with_env(ctx, tag, extra, &[]).await
}

/// [`setup`] with extra engine overrides (e.g. `LAZYVIEW_TEST_BUILD_DELAY` for the #231
/// reproduction). The batch window stays as every migration scenario runs it.
async fn setup_with_env(
    ctx: &Ctx,
    tag: &str,
    extra: Vec<ViewSpec>,
    env: &[(&str, &str)],
) -> Result<(
    Db,
    Arc<Ids>,
    Vec<ViewSpec>,
    std::collections::HashMap<String, u64>,
    Engine,
)> {
    let db = ctx.new_db(tag).await?;
    let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
    let keep = [
        "v_customer_stats",
        "v_vip_stats",
        "v_status_counts",
        "v_order_lines",
    ];
    let mut specs: Vec<ViewSpec> = ecommerce_views()
        .into_iter()
        .filter(|v| keep.contains(&v.name.as_str()))
        .collect();
    specs.extend(extra);
    let mut ov: Vec<(&str, &str)> = vec![("LAZYVIEW_BATCH_WINDOW_MS", "50")];
    ov.extend_from_slice(env);
    let (mut eng, idmap) = start_with_views(ctx, &db, &specs, &ov).await?;
    verify(ctx, &mut eng, &db, &specs, "initial").await?;
    Ok((db, ids, specs, idmap, eng))
}

#[test]
fn migration_definition_changes_online() {
    scenario("mig_def", |ctx| async move {
        let v0 = ViewSpec::new(
            "vm_orders",
            Kind::Incremental,
            "SELECT id, status, customer_id FROM orders",
        );
        let (db, ids, mut specs, idmap, mut eng) = setup(&ctx, "mig_def", vec![v0]).await?;
        let mut c = db.conn().await?;
        let versions = vec![
            // columns + join + filter change
            ViewSpec::new(
                "vm_orders",
                Kind::Incremental,
                "SELECT o.id, o.status, c.name AS customer, c.tier FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.id > 0",
            ),
            // mode change: grouped
            ViewSpec::new("vm_orders", Kind::Incremental, "SELECT status, COUNT(*) AS n, MAX(created_at) AS latest FROM orders GROUP BY status"),
            // mode change: full refresh
            ViewSpec::new("vm_orders", Kind::Full, "SELECT DISTINCT status, customer_id FROM orders"),
            // filter-only change (same shape), then back to plain SPJ
            ViewSpec::new("vm_orders", Kind::Full, "SELECT DISTINCT status, customer_id FROM orders WHERE status <> 'refunded'"),
            ViewSpec::new("vm_orders", Kind::Incremental, "SELECT id, status, customer_id FROM orders WHERE status <> 'zzz'"),
        ];
        for (k, v) in versions.into_iter().enumerate() {
            let what = format!("definition change #{k}");
            let old_hash = current_hash(&mut c, "vm_orders").await?;
            let bg = start_background(
                &db,
                ids.clone(),
                ctx.sub_seed(&what),
                &[vec!["vm_orders"], vec!["v_customer_stats"]],
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
            exec(
                &mut c,
                &format!(
                    "UPDATE _lazyview SET definition = {} WHERE id = {}",
                    q(&v.definition),
                    idmap["vm_orders"]
                ),
            )
            .await?;
            wait_rebuilt(&mut c, &mut eng, "vm_orders", &old_hash, t_ready())
                .await
                .with_context(|| what.clone())?;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let logs = bg.finish(&ctx, &what).await?;
            let cols = fetch(&mut c, "SELECT * FROM vm_orders LIMIT 0")
                .await?
                .columns;
            ensure!(
                logs[0].last_shape().map(|s| &s.1) == Some(&cols),
                "{what}: reader's last shape {:?} is not the new one {cols:?}",
                logs[0].last_shape()
            );
            for s in specs.iter_mut() {
                if s.name == "vm_orders" {
                    *s = v.clone();
                }
            }
            verify(&ctx, &mut eng, &db, &specs, &what).await?;
        }
        eng.stop().await?;
        Ok(())
    });
}

/// #231, reproduced on purpose with the engine's own knob for a long build:
/// `LAZYVIEW_TEST_BUILD_DELAY=<marker>:<ms>` (see `src/engine.rs`, `build_task`) makes the build
/// of a definition whose text carries `<marker>` sleep before it reads. The flake's recorded
/// shape (issue #231, local migration-suite run on mariadb1011 under shared-server load): a
/// definition change whose rebuild was still running when the harness's fixed 180 s ready wall
/// expired — `state: "building"` — and the same seed passed on re-run. The crawl here is that
/// delay, sized from the harness's own numbers: `t_ready()` plus half the room between the floor
/// and the engine's stall bound — strictly past what a healthy engine was ever held to, strictly
/// inside what one failed connect cycle plus one timed-out statement can account for. Both
/// halves of the proof are assertions in this one run:
///
/// * **red, the old bound**: with the crawl underway, the fixed [`wait_rebuilt_fixed`] deadline
///   expires at the floor on an alive engine whose view's state rows still say `building` — had
///   that wait been the only bound, as it was, this is where #231 failed. Note what that wait
///   had to go on: nothing moves on either progress channel while an honest def-change build
///   reads. The engine holds the *reported* position at the build's own start for as long as a
///   new version builds (`after_boundary`: "new versions still building ... hold it at their
///   start, as do pending definition changes"), and a `building` state row stamps nothing until
///   the swap. The bound was all the old wait had, and the guess is what it died on — which is
///   why the green half is the engine's own number, not a claim about what should be moving.
/// * **green, the engine's own bound**: the same rebuild — the delay is never lifted — brought
///   home by the progress-aware [`wait_rebuilt`]: the floor holds a healthy engine to exactly
///   what it is held to today (it is never shortened), and silence is spent against
///   [`engine_bounds::stall`] instead of against the guess. The scripted rule tests in
///   `common/engine.rs` cover the channels that extend the wait past the bound (state rows
///   moving, `applied` advancing) and that a view frozen past it still fails there.
#[test]
fn migration_definition_change_crawl_outlives_the_fixed_ready_floor() {
    scenario("mig_s231", |ctx| async move {
        let v0 = ViewSpec::new(
            "vm_orders",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        );
        // The crawl is sized from the harness's own numbers, never guessed at: it must outlive
        // the floor a healthy engine is held to (else the reproduction is not one) and stay
        // inside the silence the engine can itself account for (else a wait that dies there is
        // right, and proving it green would be cheating).
        let floor = t_ready();
        let stall = engine_bounds::stall(engine_bounds::default_target_timeout());
        ensure!(
            stall > floor + Duration::from_secs(60),
            "#231's premise is gone: the engine's own stall bound {stall:?} must sit at least a \
             minute above the {floor:?} floor — with the floor scaled past it there is no crawl \
             that outlives one bound and not the other"
        );
        let crawl = floor + (stall - floor) / 2;
        let delay = format!("/*lv-slow-build*/:{}", crawl.as_millis());
        let changed = ViewSpec::new(
            "vm_orders",
            Kind::Incremental,
            "SELECT id, status, customer_id FROM orders WHERE id > 0 /*lv-slow-build*/",
        );
        let (db, ids, mut specs, idmap, mut eng) = setup_with_env(
            &ctx,
            "mig_s231",
            vec![v0],
            &[("LAZYVIEW_TEST_BUILD_DELAY", &delay)],
        )
        .await?;
        let mut c = db.conn().await?;
        let old_hash = current_hash(&mut c, "vm_orders").await?;
        // The flake's shape is a migration under concurrent traffic: the writers and the two
        // readers of `migration_definition_changes_online` run across the whole crawl, the old
        // MV staying readable until the swap (issue #231 was seen with the suite's own load on
        // a busy host).
        let bg = start_background(
            &db,
            ids.clone(),
            ctx.sub_seed("crawl"),
            &[vec!["vm_orders"], vec!["v_customer_stats"]],
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {}",
                q(&changed.definition),
                idmap["vm_orders"]
            ),
        )
        .await?;
        // The crawl is proven underway before anything is waited on.
        let dsn = db.dsn.clone();
        let want = old_hash.clone();
        wait_until("the delayed rebuild to be underway", t_ready(), || {
            let dsn = dsn.clone();
            let want = want.clone();
            async move {
                let mut c = connect(&dsn).await?;
                let st = view_state(&mut c, "vm_orders").await?;
                c.disconnect().await.ok();
                Ok(st.iter().any(|s| s.state == "building")
                    || st.iter().any(|s| s.definition_hash != want))
            }
        })
        .await?;
        // --- red: the fixed wall, on a live engine, with the crawl still running.
        let t = Instant::now();
        let red = wait_rebuilt_fixed(&mut c, &mut eng, "vm_orders", &old_hash, floor).await;
        let expired = t.elapsed();
        ensure!(
            red.is_err(),
            "the reproduction is not one: the fixed ready bound of {floor:?} caught the crawl \
             anyway ({expired:?} elapsed, the build was delayed {crawl:?}) — #231's shape, the \
             180 s timeout with the view still building on a live engine, cannot be reproduced \
             here. Underlying error: {red:#?}"
        );
        eng.check_alive()?;
        let st = view_state(&mut c, "vm_orders").await?;
        ensure!(
            st.iter().any(|s| s.state == "building"),
            "vm_orders was not 'building' when the fixed bound expired (the flake's shape); \
             state: {st:?}",
        );
        ctx.log(format!(
            "reproduced #231: the fixed ready wall of {floor:?} expired at {expired:?} with \
             vm_orders 'building' on a live engine (that build sleeps {crawl:?} before reading)"
        ));
        // --- green: the same rebuild under the progress-aware wait, delay still armed.
        wait_rebuilt(&mut c, &mut eng, "vm_orders", &old_hash, floor)
            .await
            .with_context(|| {
                format!(
                    "the progress-aware ready wait gave up on a build that was only delayed: it \
                     had already run {expired:?} past the {floor:?} floor with the engine alive \
                     and the view building, and the engine's own stall bound is {stall:?}"
                )
            })?;
        ctx.log(format!(
            "the same crawl passed under the progress rule: green {:.0?} after the fixed bound \
             expired",
            t.elapsed() - expired
        ));
        let logs = bg.finish(&ctx, "the crawl rebuild").await?;
        let cols = fetch(&mut c, "SELECT * FROM vm_orders LIMIT 0")
            .await?
            .columns;
        ensure!(
            logs[0].last_shape().map(|s| &s.1) == Some(&cols),
            "the crawl rebuild: reader's last shape {:?} is not the new one {cols:?}",
            logs[0].last_shape()
        );
        for s in specs.iter_mut() {
            if s.name == "vm_orders" {
                *s = changed.clone();
            }
        }
        verify(&ctx, &mut eng, &db, &specs, "after the crawl rebuild").await?;
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn migration_rename_view_online() {
    scenario("mig_rename", |ctx| async move {
        let v0 = ViewSpec::new(
            "vm_old",
            Kind::Incremental,
            "SELECT o.id, o.status, c.name FROM orders o JOIN customers c ON c.id = o.customer_id",
        );
        let (db, ids, mut specs, idmap, mut eng) = setup(&ctx, "mig_rename", vec![v0]).await?;
        let mut c = db.conn().await?;
        let renamed = ViewSpec::new("vm_new", Kind::Incremental, "SELECT o.id, o.status, c.name, c.tier FROM orders o JOIN customers c ON c.id = o.customer_id");
        let bg = start_background(
            &db,
            ids.clone(),
            ctx.sub_seed("rename"),
            &[vec!["vm_old", "vm_new"]],
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {}",
                q(&renamed.definition),
                idmap["vm_old"]
            ),
        )
        .await?;
        wait_ready(&mut c, &mut eng, &["vm_new"], t_ready()).await?;
        wait_until("old name dropped after rename", t_ready(), || {
            let dsn = db.dsn.clone();
            async move {
                let mut c = connect(&dsn).await?;
                let gone = !table_exists(&mut c, "vm_old").await?;
                c.disconnect().await.ok();
                Ok(gone)
            }
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let logs = bg.finish(&ctx, "rename").await?;
        ensure!(
            logs[0].last_shape().map(|s| s.0.as_str()) == Some("vm_new"),
            "reader never saw vm_new: {:?}",
            logs[0].shapes
        );
        specs.retain(|s| s.name != "vm_old");
        specs.push(renamed);
        verify(&ctx, &mut eng, &db, &specs, "after rename").await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn migration_source_alter_under_writes() {
    scenario("mig_alter", |ctx| async move {
        let v0 = ViewSpec::new(
            "vm_orders",
            Kind::Incremental,
            "SELECT id, status, customer_id, created_at FROM orders",
        );
        let (db, ids, specs, _idmap, mut eng) = setup(&ctx, "mig_alter", vec![v0]).await?;
        let mut c = db.conn().await?;
        let alters = [
            "ALTER TABLE orders ADD COLUMN x INT NOT NULL DEFAULT 0 AFTER id",
            "ALTER TABLE order_items ADD COLUMN y VARCHAR(5) NULL FIRST",
            "ALTER TABLE orders DROP COLUMN x",
            "ALTER TABLE orders MODIFY COLUMN note VARCHAR(300) NULL",
            "ALTER TABLE order_items DROP COLUMN y",
        ];
        for (k, ddl) in alters.iter().enumerate() {
            let what = format!("ALTER #{k} under writes: {ddl}");
            let bg = start_background(
                &db,
                ids.clone(),
                ctx.sub_seed(&what),
                &[
                    vec!["vm_orders"],
                    vec!["v_customer_stats"],
                    vec!["v_order_lines"],
                ],
            );
            tokio::time::sleep(Duration::from_millis(400)).await;
            exec(&mut c, ddl).await?;
            tokio::time::sleep(Duration::from_millis(800)).await;
            let mut sc = db.conn().await?;
            eng.wait_caught_up(&mut sc, t_catchup()).await?;
            bg.finish(&ctx, &what).await?;
            wait_ready_tolerant(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(&ctx, &mut eng, &db, &specs, &what).await?;
        }
        eng.stop().await?;
        Ok(())
    });
}
