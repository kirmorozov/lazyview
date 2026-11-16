use super::*;

/// Full-refresh views must be rebuilt when their sources change (also when an incremental view
/// reads the same table), around XA transactions too.
#[test]
fn reg_full_view_refresh_around_xa() {
    scenario("reg_full_xa", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_distinct",
                Kind::Full,
                "SELECT DISTINCT method, status FROM pay",
            ),
            ViewSpec::new(
                "r_pay",
                Kind::Incremental,
                "SELECT id, method, status FROM pay",
            ),
        ];
        let ddl = [
            "CREATE TABLE pay (id INT PRIMARY KEY, method VARCHAR(10) NOT NULL, status VARCHAR(10) NOT NULL)",
            "INSERT INTO pay VALUES (1, 'card', 'ok'), (2, 'wire', 'ok')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_full_xa", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, &["INSERT INTO pay VALUES (3, 'cash', 'pending')"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "plain insert").await?;
        exec_all(
            &mut c,
            &[
                "XA START 'r1'",
                "INSERT INTO pay VALUES (4, 'card', 'failed')",
                "XA END 'r1'",
                "XA PREPARE 'r1'",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "XA prepared").await?;
        exec_all(&mut c, &["XA COMMIT 'r1'"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "XA committed").await?;
        exec_all(
            &mut c,
            &[
                "XA START 'r2'",
                "INSERT INTO pay VALUES (5, 'gift', 'ok')",
                "XA END 'r2'",
                "XA PREPARE 'r2'",
                "XA ROLLBACK 'r2'",
            ],
        )
        .await?;
        exec_all(
            &mut c,
            &[
                "INSERT INTO pay VALUES (6, 'paypal', 'ok')",
                "UPDATE pay SET status = 'void' WHERE id = 1",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "XA rolled back, then plain writes",
        )
        .await?;
        exec_all(
            &mut c,
            &[
                "XA START 'r3'",
                "UPDATE pay SET method = 'cheque' WHERE id = 2",
                "XA END 'r3'",
                "XA COMMIT 'r3' ONE PHASE",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "XA one phase").await?;
        eng.stop().await
    });
}

/// Updates of columns no view reads must not rebuild full-refresh views, also next to
/// incremental views over the same tables.
#[test]
fn reg_unreferenced_update_full_mode() {
    scenario("reg_unref_full", |ctx| async move {
        let db = ctx.new_db("reg_unref_full").await?;
        common::workload::setup_ecommerce(
            &db,
            ctx.sub_seed("seed"),
            common::workload::Sizes::tiny(),
        )
        .await?;
        // Full-refresh views plus incremental views over the same tables (as in eff_unref).
        let keep = [
            "v_union_names",
            "v_top_products",
            "v_big_spenders",
            "v_distinct_methods",
            "v_customer_cards",
            "v_noisy_products",
            "v_status_counts",
            "vf_addresses",
        ];
        let specs: Vec<ViewSpec> = common::workload::ecommerce_views()
            .into_iter()
            .filter(|v| keep.contains(&v.name.as_str()))
            .collect();
        let (mut eng, _) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[("LAZYVIEW_POSITION_PERSIST_MS", "600000")],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let mut c = db.conn().await?;
        let mut sc = db.conn().await?;
        for (what, stmt) in [
            (
                "customers.last_seen_at",
                "UPDATE customers SET last_seen_at = '2024-05-01 10:00:00.000001'",
            ),
            (
                "products.updated_at",
                "UPDATE products SET updated_at = '2024-05-01 11:00:00.123'",
            ),
            ("orders.note", "UPDATE orders SET note = CONCAT('n', id)"),
            ("regions.notes", "UPDATE regions SET notes = 'x'"),
        ] {
            eng.wait_caught_up(&mut sc, t_catchup()).await?;
            let before = eng.snapshot();
            exec(&mut c, stmt).await?;
            eng.wait_caught_up(&mut sc, t_catchup()).await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                d.full_rebuilds == 0 && d.source_queries == 0 && d.mv_queries == 0,
                "update of unreferenced {what} caused work: {}",
                fmt_snapshot(&d)
            );
        }
        eng.stop().await
    });
}

/// E1: a full-refresh view rebuilt in every flush (cadence 0) under continuous writes must not
/// keep the engine from reporting progress. Catch-up used to need a moment without any build
/// in flight, which never came: each swap started the next rebuild in the same flush (mig_alter
/// / ddl / brutal catch-up timeouts behind a chained full view).
#[test]
fn reg_catch_up_while_full_view_rebuilds() {
    scenario("reg_full_busy", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_per_customer",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
            // Full: the upstream table has no primary key (customer_id is nullable).
            ViewSpec::new(
                "r_busy_full",
                Kind::Chained,
                "SELECT customer_id, n FROM r_per_customer WHERE n > 1",
            ),
        ];
        let (db, mut eng) = setup(
            &ctx,
            "reg_full_busy",
            &[],
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "10")],
        )
        .await?;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (dsn, stop) = (db.dsn.clone(), stop.clone());
            tokio::spawn(async move {
                let mut c = connect(&dsn).await?;
                let mut id = 100;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    exec(
                        &mut c,
                        &format!("INSERT INTO orders VALUES ({id}, {}, 'new')", id % 5),
                    )
                    .await?;
                    id += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok::<_, anyhow::Error>(id)
            })
        };
        let mut sc = db.conn().await?;
        let caught_up = async {
            for round in 0..3 {
                eng.wait_caught_up(&mut sc, Duration::from_secs(60))
                    .await
                    .with_context(|| format!("round {round}: catch-up while writes continue"))?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.await.context("writer panicked")??;
        caught_up?;
        verify(&ctx, &mut eng, &db, &specs, "after continuous writes").await?;
        eng.stop().await
    });
}

/// E1b: a failed rebuild of a full-refresh view is retried by the re-plan with backoff; it
/// used to re-arm an immediate rebuild in every flush (ddl_rename: ~80 failed builds per
/// second while a source table was renamed away).
#[test]
fn reg_failed_full_rebuild_backs_off() {
    scenario("reg_full_backoff", |ctx| async move {
        let spec = ViewSpec::new(
            "r_skus",
            Kind::Full,
            // NOW() keeps it a full-refresh view (DISTINCT alone is maintained incrementally).
            "SELECT DISTINCT sku FROM products WHERE active = 1 AND NOW() > '2000-01-01'",
        );
        let specs = vec![spec.clone()];
        let (db, mut eng) = setup(
            &ctx,
            "reg_full_backoff",
            &[],
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        // Every build of this definition fails from now on: its probe view name is taken.
        let probe = format!(
            "_lv_probe_{}",
            lazyview::analyze::definition_hash(&rename_for(&db, &spec.definition)?)
        );
        exec(&mut c, &format!("CREATE TABLE `{probe}` (x INT)")).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        exec_all(&mut c, &["UPDATE products SET sku = 'zz' WHERE id = 1"]).await?;
        // Keep the fixed sleep: the assertion is that nothing (more) happens in this window —
        // the retries must stay within the backoff rate, which only a fixed stretch of time shows.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let d = eng.snapshot().since(&before);
        ensure!(
            (1..=4).contains(&d.full_rebuilds),
            "a failing full rebuild was retried {} times in 4 s (expected backoff): {}",
            d.full_rebuilds,
            fmt_snapshot(&d)
        );
        let st = wait_state(&mut c, &mut eng, "r_skus", "error", t_ready()).await?;
        ensure!(
            st.mode.as_deref() == Some("full"),
            "a view in error keeps the mode of its plan, got {st:?}"
        );
        // Unblocked: the next retry builds it.
        exec(&mut c, &format!("DROP TABLE `{probe}`")).await?;
        wait_ready_tolerant(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after the probe name was freed",
        )
        .await?;
        eng.stop().await
    });
}

/// A-05 (N-01, N-02): a reader transaction on the live table of a full-mode view. The swap
/// waits for the table's metadata lock only its own setting (`LAZYVIEW_SWAP_LOCK_WAIT_S`) and
/// a bounded number of tries, and it does that off the binlog loop: when the wait runs out the
/// swap gives up without a crash, reports at WARN naming the view and the wait, and the binlog
/// loop keeps flowing (the incremental view of the same table advances). The failed swap is
/// left to the next cadence rebuild, which swaps once the reader is gone.
#[test]
fn reg_full_swap_lock_wait_gives_up_warns_and_next_rebuild_swaps() {
    scenario("reg_swap_lock_wait", |ctx| async move {
        use std::time::Instant;
        // A group key on the NULL side of a LEFT JOIN: the planner's Full mode for sure
        // (DISTINCT over a unit plans as Grouped), so this exercises the cadence-rebuild path.
        let specs = vec![
            ViewSpec::new(
                "items_all",
                Kind::Full,
                "SELECT p.id, COUNT(*) AS n FROM orders o LEFT JOIN products p ON p.id = o.customer_id GROUP BY p.id",
            ),
            ViewSpec::new(
                "items_rows",
                Kind::Incremental,
                "SELECT id, status FROM orders",
            ),
        ];
        let (db, mut eng) = setup(
            &ctx,
            "reg_swap_lock_wait",
            &[],
            &specs,
            &[
                ("LAZYVIEW_FULL_REFRESH_MS", "3000"),
                ("LAZYVIEW_SWAP_LOCK_WAIT_S", "1"),
            ],
        )
        .await?;
        let mut c = db.conn().await?;
        // A reader with an open transaction holds the full view table's metadata lock: every
        // swap of `items_all` queues behind it and times out after 1 s per try.
        let mut reader = db.conn().await?;
        exec(&mut reader, "START TRANSACTION").await?;
        let _: Option<i64> = typed_first(&mut reader, "SELECT COUNT(*) FROM items_all").await?;
        // The changes mark the full view dirty: the cadence starts a rebuild whose swap
        // cannot get the lock. (The product id 7 row can only come from a rebuilt table.)
        exec(&mut c, "INSERT INTO products VALUES (7, 'g', 7.00, 1)").await?;
        exec(&mut c, "INSERT INTO orders VALUES (14, 7, 'new')").await?;
        let r = async {
            // The swap gives up within its setting (not the 60 s general lock wait times four)
            // and reports at the default log level, naming the view and the wait.
            let t = Instant::now();
            loop {
                let log = std::fs::read_to_string(&eng.log_path).unwrap_or_default();
                if log.contains("items_all")
                    && log.contains("the swap could not get the table lock")
                {
                    break;
                }
                ensure!(
                    t.elapsed() < Duration::from_secs(45),
                    "the swap never reported its lock-wait give-up; metrics {}",
                    fmt_snapshot(&eng.snapshot())
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // The binlog loop never waited for that lock: the incremental view of the same
            // source stays current while the full view's swap is left to the next rebuild.
            exec(&mut c, "INSERT INTO orders VALUES (15, 7, 'paid')").await?;
            let t = Instant::now();
            loop {
                let n: Option<i64> =
                    typed_first(&mut c, "SELECT COUNT(*) FROM items_rows WHERE id = 15").await?;
                if n == Some(1) {
                    break;
                }
                ensure!(
                    t.elapsed() < Duration::from_secs(15),
                    "the binlog loop stalled behind the swap's lock wait"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // The failed swap is not a view failure: the view reports `ready` on its old
            // table while the new one waits for the lock.
            let state: Option<String> = typed_first(
                &mut c,
                "SELECT state FROM _lazyview_state WHERE name = 'items_all'",
            )
            .await?;
            ensure!(
                state.as_deref() == Some("ready"),
                "the full view left `ready` when its swap gave up: {state:?}"
            );
            Ok(())
        }
        .await;
        // With the reader gone, the next cadence rebuild swaps the new rows in.
        exec(&mut reader, "COMMIT").await?;
        r?;
        let t = Instant::now();
        loop {
            // The rebuilt table holds the group of the orders written behind the lock.
            let n: Option<i64> =
                typed_first(&mut c, "SELECT COUNT(*) FROM items_all WHERE id = 7").await?;
            if n == Some(1) {
                break;
            }
            ensure!(
                t.elapsed() < t_ready(),
                "the next rebuild did not swap the view in after the reader went away; metrics {}",
                fmt_snapshot(&eng.snapshot())
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        verify(&ctx, &mut eng, &db, &specs, "after the lock wait").await?;
        eng.stop().await
    });
}
