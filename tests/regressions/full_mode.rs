use super::*;

/// A view whose targets include an unreachable instance must still be built and maintained on
/// the reachable ones (v_orders_left with targets 'b,down' never appeared on b).
#[test]
fn reg_multi_target_with_one_down() {
    scenario_multi("reg_target_down", 1, |ctx| async move {
        let src = ctx.new_db("reg_tdown_src").await?;
        let tb = ctx.new_db_on(1, "reg_tdown_b").await?;
        let mut c = src.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        ensure_lazyview_table(&mut c).await?;
        let spec = join_view().with_targets("b,down");
        add_views(&mut c, std::slice::from_ref(&spec)).await?;
        let env = target_env(&[
            ("b", &tb.dsn),
            ("down", "mysql://lv:lv@127.0.0.1:1/nowhere"),
        ]);
        let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
        let tmap: std::collections::HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
        let healthy = vec![spec.clone().with_targets("b")];
        let limit = Duration::from_secs(90);
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            limit,
            "built on b although 'down' is unreachable",
        )
        .await?;
        exec_all(&mut c, &["UPDATE customers SET name = 'zed' WHERE id = 3"]).await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            limit,
            "maintained on b",
        )
        .await?;
        if eng.exited().is_some() {
            bail!("engine exited");
        }
        Ok(())
    });
}

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
