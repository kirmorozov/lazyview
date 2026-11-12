use super::*;

/// Every `_lazyview` change made while streaming must be picked up, also in quick succession
/// (changes were intermittently missed on all flavors: a definition never got a state row, a
/// disabled view was never dropped, a re-enabled one never re-created, a retarget ignored).
#[test]
fn reg_live_definition_changes_in_quick_succession() {
    scenario("reg_live_defs", |ctx| async move {
        let base = vec![ViewSpec::new(
            "r_base",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_live_defs", &[], &base, &[]).await?;
        let mut c = db.conn().await?;
        let t = Duration::from_secs(60);
        for round in 0..scaled(5) {
            let name = format!("r_live_{round}");
            let spec = ViewSpec::new(
                &name,
                Kind::Incremental,
                "SELECT id, customer_id FROM orders WHERE status = 'paid'",
            );
            let id = add_definition(&mut c, &spec.definition, None).await?;
            wait_ready(&mut c, &mut eng, &[name.as_str()], t)
                .await
                .with_context(|| format!("round {round}: insert"))?;
            exec(
                &mut c,
                &format!("UPDATE _lazyview SET enabled = 0 WHERE id = {id}"),
            )
            .await?;
            wait_until(
                &format!("round {round}: {name} dropped after enabled=0"),
                t,
                || {
                    let dsn = db.dsn.clone();
                    let name = name.clone();
                    async move { Ok(!table_exists(&mut connect(&dsn).await?, &name).await?) }
                },
            )
            .await?;
            exec(
                &mut c,
                &format!("UPDATE _lazyview SET enabled = 1 WHERE id = {id}"),
            )
            .await?;
            wait_until(
                &format!("round {round}: {name} re-created after enabled=1"),
                t,
                || {
                    let dsn = db.dsn.clone();
                    let name = name.clone();
                    async move { table_exists(&mut connect(&dsn).await?, &name).await }
                },
            )
            .await?;
            wait_ready(&mut c, &mut eng, &[name.as_str()], t).await?;
            exec(&mut c, &format!("DELETE FROM _lazyview WHERE id = {id}")).await?;
            wait_until(
                &format!("round {round}: {name} dropped after DELETE"),
                t,
                || {
                    let dsn = db.dsn.clone();
                    let name = name.clone();
                    async move { Ok(!table_exists(&mut connect(&dsn).await?, &name).await?) }
                },
            )
            .await?;
            exec(
                &mut c,
                "UPDATE orders SET status = IF(status = 'paid', 'new', 'paid') WHERE id = 10",
            )
            .await?;
        }
        verify(
            &ctx,
            &mut eng,
            &db,
            &base,
            "base view after definition churn",
        )
        .await?;
        eng.stop().await
    });
}

/// Chained view (MV over MV) inserted in the same transaction as its upstream, chained first.
#[test]
fn reg_chained_definitions_in_one_transaction() {
    scenario("reg_chain_txn", |ctx| async move {
        let base = vec![ViewSpec::new(
            "r_base",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_chain_txn", &[], &base, &[]).await?;
        let mut c = db.conn().await?;
        let more = vec![
            ViewSpec::new(
                "r_top_customers",
                Kind::Chained,
                "SELECT customer_id, n FROM r_per_customer WHERE n > 1",
            ),
            ViewSpec::new(
                "r_per_customer",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
        ];
        exec(&mut c, "START TRANSACTION").await?;
        for s in &more {
            add_definition(&mut c, &s.definition, None).await?;
        }
        exec(&mut c, "COMMIT").await?;
        wait_ready(&mut c, &mut eng, &names(&more), t_ready()).await?;
        exec_all(&mut c, &["INSERT INTO orders VALUES (14, 2, 'new')"]).await?;
        verify(&ctx, &mut eng, &db, &more, "chained view after writes").await?;
        eng.stop().await
    });
}

/// E1: DDL on the parent of a cascading foreign key must re-plan the views over the child (the
/// parent's column positions are part of their plans). The stale plan's column count used to
/// drop the parent's rows for every view and re-plan the others in every transaction.
#[test]
fn reg_ddl_on_cascade_parent() {
    scenario("reg_fk_parent_ddl", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_addr_by_city",
                Kind::Incremental,
                "SELECT city, COUNT(*) AS n FROM addresses GROUP BY city",
            ),
            join_view(),
        ];
        let fk = [
            "CREATE TABLE addresses (customer_id INT NOT NULL, seq INT NOT NULL, city VARCHAR(10) NOT NULL, PRIMARY KEY (customer_id, seq),
             FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "INSERT INTO addresses VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 1, 'x'), (3, 2, 'z')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_fk_parent_ddl", &fk, &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &["ALTER TABLE customers ADD COLUMN c0 INT NULL FIRST"],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "after ALTER of the FK parent").await?;
        exec_all(
            &mut c,
            &[
                "UPDATE customers SET name = 'zed' WHERE id = 3",
                "DELETE FROM customers WHERE id = 1",
                "UPDATE customers SET id = 30 WHERE id = 3",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "parent update, cascading delete and key update after the ALTER",
        )
        .await?;
        eng.stop().await
    });
}

/// E2: a chained view defined before its upstream, both present at the first start, waits for
/// the upstream (state `waiting`) instead of failing with "table ... does not exist".
#[test]
fn reg_chained_defined_first_at_startup() {
    scenario("reg_chain_start", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_top_customers",
                Kind::Chained,
                "SELECT customer_id, n FROM r_per_customer WHERE n > 1",
            ),
            ViewSpec::new(
                "r_per_customer",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
        ];
        let db = ctx.new_db("reg_chain_start").await?;
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let deadline = std::time::Instant::now() + t_ready();
        loop {
            eng.check_alive()?;
            let st = view_states(&mut c).await?;
            if let Some(e) = st.iter().find(|s| s.state == "error") {
                bail!("chained view went into error instead of waiting: {e:?}");
            }
            if specs
                .iter()
                .all(|v| st.iter().any(|s| s.name == v.name && s.state == "ready"))
            {
                break;
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "not ready in time: {st:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        exec_all(&mut c, &["INSERT INTO orders VALUES (14, 2, 'new')"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "chained view after writes").await?;
        eng.stop().await
    });
}

/// E4: the view name in a `CREATE VIEW` definition is the source of truth: a definition that
/// renames the view updates `_lazyview.name` and renames online (it used to fail with a
/// name/definition mismatch). A name used by another row is an error; a bare SELECT keeps the
/// name it was given.
#[test]
fn reg_rename_via_definition() {
    scenario("reg_rename_def", |ctx| async move {
        let specs = vec![join_view()];
        let (db, mut eng) = setup(&ctx, "reg_rename_def", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        let id = scalar_i64(
            &mut c,
            "SELECT id FROM _lazyview WHERE name = 'r_order_customer'",
        )
        .await?;
        let renamed = ViewSpec::new("r_renamed", Kind::Incremental, &specs[0].select);
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {id}",
                q(&renamed.definition)
            ),
        )
        .await?;
        wait_ready(&mut c, &mut eng, &["r_renamed"], t_ready()).await?;
        let name = scalar(
            &mut c,
            &format!("SELECT name FROM _lazyview WHERE id = {id}"),
        )
        .await?;
        ensure!(
            name.as_deref() == Some("r_renamed"),
            "_lazyview.name not updated: {name:?}"
        );
        wait_until("old table dropped", t_ready(), || {
            let dsn = db.dsn.clone();
            async move { Ok(!table_exists(&mut connect(&dsn).await?, "r_order_customer").await?) }
        })
        .await?;
        exec_all(&mut c, &["UPDATE customers SET name = 'zed' WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&renamed),
            "after rename",
        )
        .await?;
        // Another row renaming itself to a used name: error with the row that has it.
        let other =
            add_definition(&mut c, "CREATE VIEW r_other AS SELECT id FROM orders", None).await?;
        wait_ready(&mut c, &mut eng, &["r_other"], t_ready()).await?;
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {other}",
                q(&renamed.definition)
            ),
        )
        .await?;
        let st = wait_def_state(&mut c, &mut eng, other, "error", t_ready()).await?;
        let want = format!("name `r_renamed` already used by row #{id}");
        ensure!(
            st.last_error.as_deref().is_some_and(|e| e.contains(&want)),
            "expected '{want}', got {st:?}"
        );
        // A bare SELECT takes its name from `name`.
        exec(
            &mut c,
            "INSERT INTO _lazyview (name, definition) VALUES ('r_bare', 'SELECT id, status FROM orders')",
        )
        .await?;
        wait_ready(&mut c, &mut eng, &["r_bare"], t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&renamed),
            "unchanged",
        )
        .await?;
        eng.stop().await
    });
}

/// E5: the saved position never passes a change a full-refresh view has not been rebuilt for
/// (here: not due under a long LAZYVIEW_FULL_REFRESH_MS). After a crash the change is read
/// again and the view rebuilt; it used to resume stale forever.
#[test]
fn reg_crash_with_stale_full_view() {
    scenario("reg_crash_full", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_statuses",
                Kind::Full,
                // NOW() keeps it a full-refresh view (DISTINCT alone is maintained incrementally).
                "SELECT DISTINCT status FROM orders WHERE NOW() > '2000-01-01'",
            ),
            ViewSpec::new(
                "r_orders",
                Kind::Incremental,
                "SELECT id, status FROM orders",
            ),
        ];
        let (db, eng) = setup(
            &ctx,
            "reg_crash_full",
            &[],
            &specs,
            &[
                ("LAZYVIEW_FULL_REFRESH_MS", "600000"),
                ("LAZYVIEW_POSITION_PERSIST_MS", "50"),
            ],
        )
        .await?;
        let mut c = db.conn().await?;
        // The crash must land after the update was consumed (incremental refresh) and the
        // 50 ms save loop saved its position: poll for both instead of sleeping 2 s.
        let saved: (String, String) = typed_first(
            &mut c,
            "SELECT CAST(binlog_file AS CHAR), CAST(binlog_pos AS CHAR) FROM _lazyview_status",
        )
        .await?
        .context("no saved position")?;
        exec_all(&mut c, &["UPDATE orders SET status = 'void' WHERE id = 10"]).await?;
        // Poll for the save instead of sleeping 2 s. The save can only move once the update
        // is consumed (the floor pins it to the update's start: the stale full view holds
        // everything beyond back), so this covers the refresh too — no wait_caught_up here,
        // the stale full view would hold `applied()` back for its whole 600 s cadence.
        wait_until("the update's position saved to _lazyview_status", t_ready(), || {
            let dsn = db.dsn.clone();
            let saved = saved.clone();
            async move {
                let mut pc = connect(&dsn).await?;
                let cur: Option<(String, String)> = typed_first(
                    &mut pc,
                    "SELECT CAST(binlog_file AS CHAR), CAST(binlog_pos AS CHAR) FROM _lazyview_status",
                )
                .await?;
                Ok(cur != Some(saved))
            }
        })
        .await?;
        eng.crash().await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after crash and resume").await?;
        eng.stop().await
    });
}
