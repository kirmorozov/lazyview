use super::*;

/// The table of the tests below (their own, with neutral names): `(id, grp, status)`.
const ENTITIES: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
    "INSERT INTO entities VALUES (10, 1, 'new'), (11, 1, 'paid'), (12, 2, 'new'), (13, 3, 'paid')",
];

/// A view defined while an XA transaction was prepared was rebuilt at its `XA COMMIT`: lazyview
/// kept the transaction's rows only for the tables a view already read, so the change was not
/// traceable (owner rule: an incremental view never falls back to a rebuild). Those rows are
/// kept now and the view is fed from them, whether it is live already or still building
/// (`LAZYVIEW_TEST_BUILD_DELAY` holds the second build past the commit).
#[test]
fn reg_xa_view_defined_while_prepared() {
    scenario("reg_xa_def", |ctx| async move {
        let mut ddl: Vec<&str> = ENTITIES.to_vec();
        ddl.push("CREATE TABLE items (id INT PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)");
        ddl.push("INSERT INTO items VALUES (1, 1, 1), (2, 1, 2)");
        let mut specs = vec![ViewSpec::new(
            "r_entities",
            Kind::Incremental,
            "SELECT id, grp, status FROM entities",
        )];
        let ov = [("LAZYVIEW_TEST_BUILD_DELAY", "/*xa_build*/:20000")];
        let (db, mut eng) = setup(&ctx, "reg_xa_def", &ddl, &specs, &ov).await?;
        let mut c = db.conn().await?;
        let mut xc = db.conn().await?;
        let before = eng.snapshot();
        // `items` is read by no view: these are the rows origin/main drops.
        exec_all(
            &mut xc,
            &[
                "XA START 'r1'",
                "INSERT INTO items VALUES (3, 2, 30), (4, 2, 40)",
                "UPDATE items SET val = val + 100 WHERE id = 1",
                "XA END 'r1'",
                "XA PREPARE 'r1'",
            ],
        )
        .await?;
        // (a) defined and built while the transaction is prepared: no prepared row may show,
        // and at the commit all of them must, without a rebuild.
        let v1 = ViewSpec::new(
            "r_items",
            Kind::Incremental,
            "SELECT id, grp, val FROM items",
        );
        add_definition(&mut c, &v1.definition, None).await?;
        wait_ready(
            &mut c,
            &mut eng,
            &names(std::slice::from_ref(&v1)),
            t_ready(),
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&v1),
            "prepared rows invisible",
        )
        .await?;
        // (b) defined while it is prepared and still building when it commits.
        let v2 = ViewSpec::new(
            "r_items_sum",
            Kind::Incremental,
            "SELECT grp, SUM(val) AS s FROM items /*xa_build*/ GROUP BY grp",
        );
        add_definition(&mut c, &v2.definition, None).await?;
        wait_state(
            &mut c,
            &mut eng,
            "r_items_sum",
            "building",
            Duration::from_secs(10),
        )
        .await?;
        exec(&mut xc, "XA COMMIT 'r1'").await?;
        specs.extend([v1, v2]);
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after the XA COMMIT").await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 2,
            "both views must be fed from the staged rows, not rebuilt: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// A view's own table dropped by hand stayed `ready` while every refresh failed with 1146, until
/// `rebuild = 1`. lazyview sees the DROP in the binlog (it never drops a live view's table) and
/// rebuilds the view online, which creates the table again.
#[test]
fn reg_dropped_view_table_is_rebuilt() {
    scenario("reg_drop_mv", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_counts",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp",
        )];
        let (db, mut eng) = setup(&ctx, "reg_drop_mv", ENTITIES, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let before = eng.snapshot();
        // Nothing is written afterwards: no failing refresh can bring the table back.
        exec(&mut c, "DROP TABLE r_counts").await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &db,
            &std::collections::HashMap::new(),
            &specs,
            t_ready(),
            "after DROP of the view table",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds >= 1,
            "the dropped table came back without a rebuild?: {}",
            fmt_snapshot(&d)
        );
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "UPDATE entities SET grp = 3 WHERE id = 10").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the rebuild").await?;
        eng.stop().await
    });
}

/// Only a removed view table is rebuilt: `DROP INDEX … ON <view table>` names the table but
/// leaves it and its rows in place, as do `ALTER TABLE … ADD / DROP INDEX` and `DROP TEMPORARY
/// TABLE` of the same name. None of them may send the incremental view through a rebuild.
#[test]
fn reg_index_ddl_on_view_table_does_not_rebuild() {
    scenario("reg_idx_mv", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_idx",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp",
        )];
        let (db, mut eng) = setup(&ctx, "reg_idx_mv", ENTITIES, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let before = eng.snapshot();
        exec_all(
            &mut c,
            &[
                "CREATE INDEX x_n ON r_idx (n)",
                "DROP INDEX x_n ON r_idx",
                "ALTER TABLE r_idx ADD INDEX x_n2 (n)",
                "ALTER TABLE r_idx DROP INDEX x_n2",
                "DROP TEMPORARY TABLE IF EXISTS r_idx",
                "INSERT INTO entities VALUES (14, 2, 'new')",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after index DDL on the view table",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "index DDL on the view table rebuilt the view: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// A refresh that kept failing for another reason than an overloaded server re-planned the view
/// after three attempts, which rebuilt it (owner rule: an incremental view never falls back to
/// a rebuild). It is retried with backoff until it succeeds; from the third failure on
/// `last_error` of the view's `ready` state rows says so, and the next success clears it. A
/// source table held by `LOCK TABLES … WRITE` fails every refresh with a lock wait timeout after
/// the source session's `LAZYVIEW_TEST_LOCK_WAIT_S` (2 s here; the default 60 s would be too
/// slow for a test): three failures cost 3 × 2 s of blocking plus the 0.5 s + 1 s backoffs,
/// about 7.5 s. Only the supervisor counts that as overload, so the engine runs unsupervised.
#[test]
fn reg_failing_refresh_retries_without_rebuild() {
    scenario("reg_refresh_retry", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_entities",
            Kind::Incremental,
            "SELECT id, grp, status FROM entities",
        )];
        let ov = [
            ("LAZYVIEW_SUPERVISOR", "off"),
            ("LAZYVIEW_TEST_LOCK_WAIT_S", "2"),
        ];
        let (db, mut eng) = setup(&ctx, "reg_refresh_retry", ENTITIES, &specs, &ov).await?;
        let mut c = db.conn().await?;
        let mut lock = db.conn().await?;
        let before = eng.snapshot();
        exec(&mut lock, "LOCK TABLES entities WRITE").await?;
        exec(
            &mut lock,
            "UPDATE entities SET status = 'held' WHERE id = 10",
        )
        .await?;
        let t = std::time::Instant::now();
        loop {
            eng.check_alive()?;
            let rows = view_state(&mut c, "r_entities").await?;
            let shown = !rows.is_empty()
                && rows.iter().all(|r| {
                    r.state == "ready"
                        && r.last_error
                            .as_deref()
                            .is_some_and(|e| e.contains("refresh failed"))
                });
            if shown {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(90),
                "no refresh error shown while the table is locked: {rows:?}; {}",
                fmt_snapshot(&eng.snapshot().since(&before))
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        exec(&mut lock, "UNLOCK TABLES").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the lock is released").await?;
        let t = std::time::Instant::now();
        while view_state(&mut c, "r_entities")
            .await?
            .iter()
            .any(|r| r.last_error.is_some())
        {
            ensure!(
                t.elapsed() < Duration::from_secs(30),
                "the refresh error stayed after a successful refresh"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0 && d.refresh_errors >= 3,
            "failed refreshes must be retried, never rebuilt: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// A target that failed five writes was reported down and then *rebuilt*, although lazyview
/// still knew exactly which units it owed (owner rule: an incremental view never falls back to a
/// rebuild for a change it can trace). It keeps them now and catches up when it answers. The
/// target refuses writes through a `BEFORE INSERT` trigger on the view's own table (1644 is
/// neither an overload nor a connection error), so the sources are read and the changes traced
/// the whole time; the engine runs unsupervised, as the failure is not congestion. The budget
/// of the kept changes is set to one byte, so the target is also past it while it is down:
/// passing the budget only warns and counts (`lag_over_budget`), nothing is dropped and the
/// target is still not rebuilt.
#[test]
fn reg_down_target_keeps_its_keys() {
    scenario("reg_target_keys", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_entities",
            Kind::Incremental,
            "SELECT id, grp, status FROM entities",
        )];
        let ov = [
            ("LAZYVIEW_SUPERVISOR", "off"),
            ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "1"),
        ];
        let (db, mut eng) = setup(&ctx, "reg_target_keys", ENTITIES, &specs, &ov).await?;
        let mut c = db.conn().await?;
        let mut block = db.conn().await?;
        let before = eng.snapshot();
        exec(
            &mut block,
            "CREATE TRIGGER lv_block BEFORE INSERT ON r_entities FOR EACH ROW \
             SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'lazyview test: the target refuses writes'",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO entities VALUES (20, 4, 'held'), (21, 4, 'held')",
        )
        .await?;
        // Five failed retries (1 s doubling, 30 s cap) report the target down.
        let t = std::time::Instant::now();
        loop {
            eng.check_alive()?;
            let rows = view_state(&mut c, "r_entities").await?;
            if rows.iter().any(|r| r.state == "error") {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(150),
                "the target was never reported down: {rows:?}; {}",
                fmt_snapshot(&eng.snapshot().since(&before))
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        // Changes while it is down are kept with the ones it already owes.
        exec(&mut c, "INSERT INTO entities VALUES (22, 5, 'more')").await?;
        exec(&mut block, "DROP TRIGGER lv_block").await?;
        // The target is retried at the capped backoff (up to 30 s), so its `error` row outlives
        // `wait_ready`'s 5 s grace: tolerate it here, the assertions below still require `ready`.
        wait_ready_tolerant(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after the target answered").await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0 && d.target_catchups >= 1 && d.lag_over_budget == 1,
            "the target must catch up from the changes kept for it, past the budget and never \
             rebuilt (the budget pass warns and counts once): {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// A source DDL that keeps every row and leaves what a view reads alone (an added index, an added
/// column, columns moved) must not rebuild the views that read the table. The re-planned view
/// reads the same columns, keys and SQL, so its plan is put in place with the new column
/// positions: zero rebuilds, and the views stay equal to their oracles with writes before, during
/// and after the DDL. A retyped read column still rebuilds the view. An online schema change's
/// table swap replaces the table's rows, so the views rebuild.
#[test]
fn reg_source_ddl_with_same_reads_needs_no_rebuild() {
    scenario("reg_ddl_same", |ctx| async move {
        use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};
        use std::sync::Arc;
        let specs = vec![
            ViewSpec::new(
                "r_grp",
                Kind::Incremental,
                "SELECT grp, COUNT(*) AS n, SUM(id) AS s FROM entities GROUP BY grp",
            ),
            ViewSpec::new(
                "r_join",
                Kind::Incremental,
                "SELECT e.id, e.status, l.tag FROM entities e JOIN links l ON l.ref_id = e.id",
            ),
            ViewSpec::new(
                "r_tags",
                Kind::Incremental,
                "SELECT l.tag, COUNT(*) AS n FROM links l JOIN entities e ON e.id = l.ref_id \
                 WHERE e.status = 'new' GROUP BY l.tag",
            ),
        ];
        let mut ddl: Vec<&str> = ENTITIES.to_vec();
        ddl.extend([
            "CREATE TABLE links (id INT PRIMARY KEY, ref_id INT NOT NULL, tag VARCHAR(10) NOT NULL)",
            "INSERT INTO links VALUES (1, 10, 'a'), (2, 11, 'b'), (3, 12, 'a')",
        ]);
        let (db, mut eng) = setup(&ctx, "reg_ddl_same", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let seq = Arc::new(AtomicI64::new(100));
        // (what, links_only, rebuilds, statements): `rebuilds` is true when a DDL replaces the
        // table's rows, so the views that read it must rebuild.
        let steps: [(&str, bool, bool, &[&str]); 4] = [
            (
                "added indexes",
                false,
                false,
                &[
                    "ALTER TABLE entities ADD INDEX x_status (status)",
                    "CREATE INDEX x_tag ON links (tag)",
                ],
            ),
            (
                "added columns",
                false,
                false,
                &[
                    "ALTER TABLE entities ADD COLUMN extra INT NULL AFTER id",
                    "ALTER TABLE links ADD COLUMN extra2 VARCHAR(5) NULL FIRST",
                ],
            ),
            (
                "columns moved",
                false,
                false,
                &[
                    "ALTER TABLE entities MODIFY status VARCHAR(10) NOT NULL AFTER id",
                    "ALTER TABLE links MODIFY tag VARCHAR(10) NOT NULL FIRST",
                ],
            ),
            // gh-ost / pt-osc: a copy with the new definition replaces the table by RENAME
            // (writes during it go to `links` only: the copy does not follow `entities`).
            (
                "an online schema change's table swap",
                true,
                true,
                &[
                    "CREATE TABLE _entities_gho LIKE entities",
                    "ALTER TABLE _entities_gho ADD COLUMN extra3 INT NULL",
                    "INSERT INTO _entities_gho (id, extra, status, grp) \
                     SELECT id, extra, status, grp FROM entities",
                    "RENAME TABLE entities TO _entities_del, _entities_gho TO entities",
                    "DROP TABLE _entities_del",
                ],
            ),
        ];
        for (what, links_only, rebuilds, stmts) in steps {
            let before = eng.snapshot();
            let stop = Arc::new(AtomicBool::new(false));
            // Writes before the DDL (the gh-ost case stops writing to `entities` before its copy).
            let first = tokio::spawn(churn(db.conn().await?, seq.clone(), false, stop.clone()));
            tokio::time::sleep(Duration::from_millis(150)).await;
            stop.store(true, Relaxed);
            first.await??;
            let stop = Arc::new(AtomicBool::new(false));
            let during = tokio::spawn(churn(
                db.conn().await?,
                seq.clone(),
                links_only,
                stop.clone(),
            ));
            tokio::time::sleep(Duration::from_millis(100)).await;
            for s in stmts {
                exec(&mut c, s).await?;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            stop.store(true, Relaxed);
            during.await??;
            // And after it.
            let stop = Arc::new(AtomicBool::new(true));
            churn(db.conn().await?, seq.clone(), false, stop).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            let d = eng.snapshot().since(&before);
            if rebuilds {
                ensure!(
                    d.full_rebuilds >= 1,
                    "{what}: a RENAME swap replaces the table's rows, so the views must rebuild: {}",
                    fmt_snapshot(&d)
                );
            } else {
                ensure!(
                    d.full_rebuilds == 0,
                    "{what}: the views were rebuilt: {}",
                    fmt_snapshot(&d)
                );
            }
        }
        // The same table changed twice in a row, with rows between: when the re-plan for the
        // first DDL already sees the second, those rows must not be decoded with the second's
        // layout (here `grp` would read `y`); the view is rebuilt then. Right either way.
        let before = eng.snapshot();
        let stop = Arc::new(AtomicBool::new(false));
        let during = tokio::spawn(churn(db.conn().await?, seq.clone(), false, stop.clone()));
        for stmt in [
            "ALTER TABLE entities ADD COLUMN y INT NOT NULL DEFAULT 7 AFTER id",
            "INSERT INTO entities (id, grp, status) VALUES (90001, 1, 'new'), (90002, 2, 'new')",
            "ALTER TABLE entities MODIFY y INT NOT NULL DEFAULT 7 AFTER status",
        ] {
            exec(&mut c, stmt).await?;
        }
        stop.store(true, Relaxed);
        during.await??;
        churn(
            db.conn().await?,
            seq.clone(),
            false,
            Arc::new(AtomicBool::new(true)),
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "the same table changed twice").await?;
        let d = eng.snapshot().since(&before);
        ctx.log(format!(
            "the same table changed twice in a row: {} rebuild(s)",
            d.full_rebuilds
        ));
        // While lazyview is stopped the same table changes twice, rows before, between and after:
        // the resumed plan comes from the catalog after both DDLs and cannot decode the replayed
        // rows before them, so the first replayed DDL rebuilds the views (no swap in place).
        eng.stop().await?;
        for stmt in [
            "INSERT INTO entities (id, grp, status) VALUES (90011, 7, 'new')",
            "ALTER TABLE entities ADD COLUMN z INT NOT NULL DEFAULT 5 AFTER id",
            "INSERT INTO entities (id, grp, status) VALUES (90012, 8, 'new')",
            "ALTER TABLE entities MODIFY z INT NOT NULL DEFAULT 5 AFTER status",
            "INSERT INTO entities (id, grp, status) VALUES (90013, 0, 'new')",
            "UPDATE entities SET grp = grp + 1 WHERE id IN (10, 11)",
        ] {
            exec(&mut c, stmt).await?;
        }
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "DDLs while stopped").await?;
        let rebuilt = eng.snapshot().full_rebuilds;
        ensure!(
            rebuilt >= 1,
            "DDLs replayed after a restart were swapped in place (the resumed plan postdates them)"
        );
        // A column the views read changes type: they are rebuilt (and stay right).
        let before = eng.snapshot();
        exec(&mut c, "ALTER TABLE entities MODIFY grp BIGINT NOT NULL").await?;
        churn(
            db.conn().await?,
            seq.clone(),
            false,
            Arc::new(AtomicBool::new(true)),
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "a read column retyped").await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds >= 1,
            "a retyped read column did not rebuild: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// `updated_at` of a view's state row (as text, comparable).
async fn state_updated_at(c: &mut mysql_async::Conn, name: &str) -> Result<Option<String>> {
    typed_exec_first(
        c,
        "SELECT CAST(updated_at AS CHAR) FROM _lazyview_state WHERE name = ?",
        (name,),
    )
    .await
}

/// `error` rows across a restart: a definition deleted while lazyview was stopped left its row
/// behind for good (only rows of tables lazyview built were cleaned), and an error row written
/// again unchanged kept its old `updated_at`, so it looked like nothing was checked. At startup
/// every definition in error is planned again: still failing = error with a fresh `updated_at`,
/// fixed while stopped = built; rows of deleted definitions go.
#[test]
fn reg_error_rows_are_rechecked_at_startup() {
    scenario("reg_err_restart", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_ok",
            Kind::Incremental,
            "SELECT id, status FROM entities",
        )];
        let (db, mut eng) = setup(&ctx, "reg_err_restart", ENTITIES, &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "INSERT INTO _lazyview (name, definition) VALUES ('r_gone', 'SELECT id FROM no_such_a')",
                "INSERT INTO _lazyview (name, definition) VALUES ('r_still', 'SELECT id FROM no_such_b')",
                "INSERT INTO _lazyview (name, definition) VALUES ('r_fixed', 'SELECT id FROM no_such_c')",
            ],
        )
        .await?;
        for n in ["r_gone", "r_still", "r_fixed"] {
            wait_state(&mut c, &mut eng, n, "error", t_ready()).await?;
        }
        let still_before = state_updated_at(&mut c, "r_still").await?;
        eng.stop().await?;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        exec_all(
            &mut c,
            &[
                "DELETE FROM _lazyview WHERE name = 'r_gone'",
                "CREATE TABLE no_such_c (id INT PRIMARY KEY)",
                "INSERT INTO no_such_c VALUES (1), (2)",
            ],
        )
        .await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let all = vec![
            specs[0].clone(),
            ViewSpec::new("r_fixed", Kind::Incremental, "SELECT id FROM no_such_c"),
        ];
        // (Not wait_ready: until the new engine has planned it, r_fixed shows its old error.)
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        for n in names(&all) {
            wait_state(&mut c, &mut eng, n, "ready", t_ready()).await?;
        }
        let row = wait_state(&mut c, &mut eng, "r_still", "error", t_ready()).await?;
        ensure!(
            row.last_error
                .as_deref()
                .is_some_and(|e| e.contains("no_such_b") && e.contains("does not exist")),
            "r_still: {row:?}"
        );
        let still_after = state_updated_at(&mut c, "r_still").await?;
        ensure!(
            still_after > still_before,
            "r_still was not checked again at startup: updated_at {still_before:?} -> {still_after:?}"
        );
        let gone: Option<i64> = typed_first(
            &mut c,
            "SELECT COUNT(*) FROM _lazyview_state WHERE name = 'r_gone'",
        )
        .await?;
        ensure!(
            gone == Some(0),
            "the state row of a definition deleted while stopped stayed"
        );
        verify(&ctx, &mut eng, &db, &all, "after the restart").await?;
        eng.stop().await
    });
}

/// A live definition edited into one that does not parse: the view is in error (the last good
/// table stays readable), but at the next restart its table was dropped as if the definition
/// had been deleted. It keeps its name and table until the row is deleted or fixed.
#[test]
fn reg_unparseable_definition_keeps_its_table() {
    scenario("reg_bad_parse", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_keep",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp",
        )];
        let (db, eng) = setup(&ctx, "reg_bad_parse", ENTITIES, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let good: Vec<(i64, i64)> =
            typed_rows(&mut c, "SELECT grp, n FROM r_keep ORDER BY grp").await?;
        exec(
            &mut c,
            "UPDATE _lazyview SET definition = 'CREATE VIEW r_keep AS SELEC grp FROM entities' WHERE name = 'r_keep'",
        )
        .await?;
        let mut eng = eng;
        for when in ["running", "after a restart"] {
            if when != "running" {
                eng.stop().await?;
                eng = ctx.start_engine(&db, &[])?;
                // Startup (and its cleanup of tables of deleted definitions) is over.
                eng.wait_caught_up(&mut c, t_catchup()).await?;
            }
            let row = wait_state(&mut c, &mut eng, "r_keep", "error", t_ready()).await?;
            ensure!(
                row.last_error
                    .as_deref()
                    .is_some_and(|e| e.contains("SELEC")),
                "{when}: {row:?}"
            );
            ensure!(
                table_exists(&mut c, "r_keep").await?,
                "{when}: the last good table of r_keep was dropped"
            );
            let now: Vec<(i64, i64)> =
                typed_rows(&mut c, "SELECT grp, n FROM r_keep ORDER BY grp").await?;
            ensure!(now == good, "{when}: r_keep changed: {now:?} vs {good:?}");
        }
        exec(
            &mut c,
            "UPDATE _lazyview SET definition = 'SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp' WHERE name = 'r_keep'",
        )
        .await?;
        exec(&mut c, "INSERT INTO entities VALUES (14, 2, 'new')").await?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after the definition was fixed",
        )
        .await?;
        eng.stop().await
    });
}

/// A global `sql_mode` with parser flags (`ANSI_QUOTES`, `PIPES_AS_CONCAT`) must not change
/// what lazyview maintains: definitions are read and SQL is written in the default mode, so the
/// engine's sessions drop those flags. Before, the build of a view with `status = "paid"` failed
/// with an unknown column `paid`. Opt-in (changes a GLOBAL): `LAZYVIEW_TEST_GLOBAL_SQL_MODE=1`.
#[test]
fn reg_global_sql_mode_parser_flags() {
    scenario("reg_sql_mode", |ctx| async move {
        if !opted_in("LAZYVIEW_TEST_GLOBAL_SQL_MODE") {
            ctx.note(
                "SKIP: changes GLOBAL sql_mode; set LAZYVIEW_TEST_GLOBAL_SQL_MODE=1 to run it",
            );
            return Ok(());
        }
        let db = ctx.new_db("reg_sql_mode").await?;
        let mut admin = db.conn().await?;
        // The mode to set: the server's current one plus the two flags (what the old
        // `CONCAT(@@GLOBAL.sql_mode, ...)` built, from one read), so the guard sets a literal.
        let old = scalar(&mut admin, "SELECT CAST(@@GLOBAL.sql_mode AS CHAR)")
            .await?
            .unwrap_or_default();
        let want = if old.is_empty() {
            "ANSI_QUOTES,PIPES_AS_CONCAT".to_string()
        } else {
            format!("{old},ANSI_QUOTES,PIPES_AS_CONCAT")
        };
        let mut globals = Globals::set(&mut admin, &db.dsn, &[("sql_mode", &want)]).await?;
        let r = sql_mode_scenario(&ctx, &db).await;
        let restored = globals.restore(&mut admin).await;
        r.and(restored)
    });
}

async fn sql_mode_scenario(ctx: &Ctx, db: &Db) -> Result<()> {
    // This connection inherits the global mode: the test's own SQL uses single quotes.
    let mut c = db.conn().await?;
    let mode = scalar(&mut c, "SELECT CAST(@@SESSION.sql_mode AS CHAR)")
        .await?
        .unwrap_or_default();
    ensure!(
        mode.contains("ANSI_QUOTES"),
        "new sessions do not inherit the global mode: {mode}"
    );
    exec_all(
        &mut c,
        &[
            "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL, status VARCHAR(10) NOT NULL)",
            "INSERT INTO orders VALUES (10, 1, 'new'), (11, 1, 'paid'), (12, 2, 'new'), (13, 3, 'paid')",
        ],
    )
    .await?;
    // Written for the default mode: "paid" is a string.
    let specs = vec![ViewSpec::new(
        "r_paid",
        Kind::Incremental,
        r#"SELECT customer_id, COUNT(*) AS n FROM orders WHERE status = "paid" GROUP BY customer_id"#,
    )];
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, &specs).await?;
    let mut eng = ctx.start_engine(db, &[])?;
    wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
    let mode: Option<String> = typed_first(
        &mut c,
        "SELECT `mode` FROM _lazyview_state WHERE name = 'r_paid'",
    )
    .await?;
    ensure!(
        mode.as_deref() == Some("incremental"),
        "r_paid is not incremental: {mode:?}"
    );
    async fn same(c: &mut mysql_async::Conn, what: &str) -> Result<()> {
        let mv: Vec<(i64, i64)> =
            typed_rows(c, "SELECT customer_id, n FROM r_paid ORDER BY 1").await?;
        let src: Vec<(i64, i64)> = typed_rows(
            c,
            "SELECT customer_id, COUNT(*) FROM orders WHERE status = 'paid' GROUP BY customer_id ORDER BY 1",
        )
        .await?;
        ensure!(mv == src, "{what}: r_paid {mv:?} != source {src:?}");
        Ok(())
    }
    same(&mut c, "initial").await?;
    let mut sc = db.conn().await?;
    for (what, stmt) in [
        (
            "status change",
            "UPDATE orders SET status = 'paid' WHERE id = 10",
        ),
        ("insert", "INSERT INTO orders VALUES (14, 2, 'paid')"),
        ("delete", "DELETE FROM orders WHERE id IN (11, 13)"),
    ] {
        exec(&mut c, stmt).await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        same(&mut c, what).await?;
    }
    eng.stop().await
}

/// A transient failure of the consistent-snapshot read used to end the flush's exact fence: the
/// error fell through to `LAZYVIEW_VISIBILITY_DELAY_MS` and nothing re-probed the fence. The whole
/// read is repeated now (`retry_src`), so with the delay at 0 the rows are still read at the
/// fenced position — and after `heal_src` (the engine's source sessions killed) the next flush
/// probes the fence again. The retry assertions run only where the probe is the fence, which the
/// flavor answers (`Flavor::snapshot_position_is_watermark`, `src/source/mod.rs`); where it is
/// not, reaching the probe would be the bug, so that is asserted the other way. Which fence each
/// flavor gets (the engine's `fence`, `src/engine.rs`):
///
/// - MariaDB: the consistent-snapshot position. Its pair is a watermark of committed
///   transactions and there is no GTID wait, so this is the flavor that probes the snapshot and
///   reaches `LAZYVIEW_TEST_FAIL_SNAPSHOT`.
/// - MySQL / Percona with GTIDs: `WAIT_FOR_EXECUTED_GTID_SET`, which comes first, so the snapshot
///   is never probed.
/// - MySQL / Percona without GTIDs: the commit-order row (`src/source/mysql.rs`, `src/fence.rs`).
///   Their `binlog_snapshot_%` pair is the raw binlog write position — it has passed transactions
///   still in flight that the snapshot cannot read — so it is no watermark and the engine never
///   probes the snapshot: the hook is not reached.
///
/// Every flavor pins the writes exact with the delay at 0, before and after the reconnect.
#[test]
fn reg_transient_snapshot_failure_keeps_exact_fence() {
    scenario("reg_snap_fence", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_entities",
            Kind::Incremental,
            "SELECT id, grp, status FROM entities",
        )];
        let ov = [
            ("LAZYVIEW_VISIBILITY_DELAY_MS", "0"),
            ("LAZYVIEW_BATCH_WINDOW_MS", "0"),
            ("LAZYVIEW_TEST_FAIL_SNAPSHOT", "1"),
        ];
        let (db, mut eng) = setup(&ctx, "reg_snap_fence", ENTITIES, &specs, &ov).await?;
        // `retry_src` warns on every try it repeats: the proof the read was retried, not given up.
        let retried = |e: &Engine| -> usize {
            std::fs::read_to_string(&e.log_path)
                .unwrap_or_default()
                .matches("reading the binlog snapshot position failed (try 1 of ")
                .count()
        };
        let before = eng.snapshot();
        let mut c = db.conn().await?;
        // The engine's `fence` decides the same two things: the flavor's own answer about its
        // snapshot position, and whether a GTID wait covers the transaction before it. The flavor
        // comes from the server's version strings, as the engine's connect reads them
        // (`Flavor::from_version_comment`), so the test follows the engine and names no product.
        let version = scalar(&mut c, "SELECT VERSION()")
            .await?
            .unwrap_or_default();
        let comment = scalar(&mut c, "SELECT @@version_comment")
            .await?
            .unwrap_or_default();
        let watermark = lazyview::source::Flavor::from_version_comment(&version, &comment)
            .snapshot_position_is_watermark();
        // MariaDB has no `gtid_mode`: no answer is no GTID fence.
        let gtid = scalar(&mut c, "SELECT @@gtid_mode")
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        let probes_snapshot = watermark && !gtid.eq_ignore_ascii_case("ON");
        ctx.log(format!(
            "fence here: {}",
            if probes_snapshot {
                "the consistent-snapshot position"
            } else if gtid.eq_ignore_ascii_case("ON") {
                "the GTID wait"
            } else {
                "the commit-order row"
            }
        ));
        exec_all(
            &mut c,
            &[
                "INSERT INTO entities VALUES (20, 4, 'new')",
                "UPDATE entities SET status = 'paid' WHERE id = 20",
            ],
        )
        .await?;
        eng.wait_caught_up(&mut c, t_ready()).await?;
        if probes_snapshot {
            ensure!(
                retried(&eng) >= 1,
                "the injected fence failure was never retried: the flush fell back to the delay — log {}",
                eng.log_path.display()
            );
        } else {
            // Not this flavor's fence: the read must not happen at all, so the hook stays
            // untouched and the flush is exact through the GTID wait or the commit-order row.
            ensure!(
                retried(&eng) == 0,
                "the engine probed a snapshot position that is not this flavor's fence: {} probes — log {}",
                retried(&eng),
                eng.log_path.display()
            );
        }
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "injected snapshot failure, no visibility delay",
        )
        .await?;
        // The engine's own source sessions die (a dropped connection stand-in): `heal_src` opens
        // a new one and the next flush must re-probe the fence — fail once, then succeed.
        let probes = retried(&eng);
        drop(c);
        let mut admin = db.conn().await?;
        for id in session_ids(&mut admin, &format!("DB = {}", q(&db.name))).await? {
            let _ = exec(&mut admin, &format!("KILL {id}")).await;
        }
        let mut c = db.conn().await?;
        exec(&mut c, "INSERT INTO entities VALUES (21, 5, 'new')").await?;
        exec(&mut c, "UPDATE entities SET status = 'paid' WHERE id = 21").await?;
        eng.wait_caught_up(&mut c, t_ready()).await?;
        if probes_snapshot {
            ensure!(
                retried(&eng) > probes,
                "the snapshot fence was not re-probed after the reconnect: {probes} probes before, log {}",
                eng.log_path.display()
            );
        } else {
            ensure!(
                retried(&eng) == probes,
                "the engine probed a snapshot position after the reconnect that is not this \
                 flavor's fence: {probes} probes before, log {}",
                eng.log_path.display()
            );
        }
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "a retried fence must never rebuild: {}",
            fmt_snapshot(&d)
        );
        verify(&ctx, &mut eng, &db, &specs, "after the source reconnect").await?;
        eng.stop().await
    });
}

/// Review C-03: a session that twists `binlog_format` to STATEMENT logs its DML as QUERY
/// events, so the rows are not in the stream and the views reading the table stay wrong and
/// `ready`. The engine must not drop such an event silently: one WARN per table per interval
/// with kind, schema and binlog position only — never the statement text, which carries the
/// row values (F2 S-08) — and one `statement_events_ignored` count per statement event.
#[test]
fn reg_statement_format_dml_is_warned_not_dropped() {
    scenario("reg_stmt_dml", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_entities",
            Kind::Incremental,
            "SELECT id, grp, status FROM entities",
        )];
        let mut ddl: Vec<&str> = ENTITIES.to_vec();
        ddl.push("CREATE TABLE items (id INT PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)");
        let (db, mut eng) = setup(&ctx, "reg_stmt_dml", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        wait_ready(&mut c, &mut eng, &["r_entities"], t_ready()).await?;
        // Three DML statements from one twisted session: two on the view's table (they must
        // share one WARN line, the interval is far longer than this test), one on another
        // table (its own line: the rate limit is per table).
        let mut t = db.conn().await?;
        exec(&mut t, "SET SESSION binlog_format = STATEMENT").await?;
        exec_all(
            &mut t,
            &[
                "INSERT INTO entities VALUES (99, 9, 'rv42mark')",
                "UPDATE entities SET status = 'twstmark' WHERE id = 99",
                "INSERT INTO items VALUES (7, 1, 70)",
            ],
        )
        .await?;
        quiesce(&mut eng, &db).await?;
        // Every statement-format DML event is counted, for the alert work (03-obs-plan).
        let ignored = eng.snapshot().statement_events_ignored;
        ensure!(
            ignored == 3,
            "want three counted statement-format DML events, got {ignored}: log {}",
            eng.log_path.display()
        );
        // The rows did not reach the view: it stays wrong and `ready` (the ruling — the log
        // is what tells the operator).
        let n: Option<i64> =
            typed_first(&mut c, "SELECT COUNT(*) FROM r_entities WHERE id = 99").await?;
        ensure!(n == Some(0), "statement-format rows leaked into the view");
        wait_ready(&mut c, &mut eng, &["r_entities"], t_catchup()).await?;
        let log_path = eng.log_path.clone();
        eng.stop().await?;
        let log = std::fs::read_to_string(&log_path)?;
        let warned: Vec<&str> = log
            .lines()
            .filter(|l| l.contains("statement format"))
            .collect();
        ensure!(
            warned.len() == 2,
            "want one WARN line per table, got {}:\n{log}",
            warned.len()
        );
        for line in &warned {
            ensure!(line.contains("WARN"), "the line is not a WARN: {line}");
            ensure!(line.contains("kind="), "kind missing: {line}");
            let at = line
                .split("at=")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .unwrap_or("");
            ensure!(at.contains(':'), "binlog position missing: {line}");
            let schema = line
                .split("schema=")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .unwrap_or("");
            ensure!(
                schema.trim_matches('"') == db.name,
                "schema wrong in {line} (want {})",
                db.name
            );
            ensure!(
                !line.contains("rv42mark") && !line.contains("twstmark"),
                "a row value reached the log: {line}"
            );
            ensure!(
                !line.to_ascii_uppercase().contains("INSERT INTO")
                    && !line.to_ascii_uppercase().contains("UPDATE ENTITIES"),
                "the statement text reached the log: {line}"
            );
        }
        ensure!(
            warned.iter().any(|l| l.contains("kind=INSERT")),
            "no INSERT WARN line:\n{}",
            warned.join("\n")
        );
        Ok(())
    });
}

/// A-02: an event the decoder cannot map is not a network fault. On the reviewed commit the
/// reader reconnected to the same position every 200 ms forever — `/livez` green, nothing on
/// `/metrics`, the view late with no signal. Now the reason is counted per round, the stuck
/// position is named once rather than once per retry, and the wait follows the connect ladder
/// (1 s doubling to 30 s, jittered). `LAZYVIEW_TEST_UNDECODABLE_ROWS` makes the reader refuse the
/// next rows events of a table a view reads: no server gives a reason to fail on bytes it has
/// just written, so the hook stands in for the decoder bug while the handling is the real one.
/// Nothing is dropped: the reader re-reads from the saved boundary, so the view ends correct.
#[test]
fn reg_undecodable_event_backs_off_and_counts() {
    scenario("reg_undecodable", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_entities",
            Kind::Incremental,
            "SELECT id, grp, status FROM entities",
        )];
        let addr = free_local_addr()?;
        let ov = [
            ("LAZYVIEW_TEST_UNDECODABLE_ROWS", "3"),
            ("LAZYVIEW_HEALTH_ADDR", addr.as_str()),
        ];
        let (db, mut eng) = setup(&ctx, "reg_undecodable", ENTITIES, &specs, &ov).await?;
        let mut c = db.conn().await?;
        let s0 = eng.snapshot();
        ensure!(
            s0.stream_reconnect_undecodable == 0 && s0.events_undecodable == 0,
            "the hook fired before any row of the view's table was written: {:?}",
            (s0.events_undecodable, s0.stream_reconnect_undecodable)
        );
        // The saved position while the reader is stuck must be the one it is stuck at: captured
        // here so the stuck rounds can be compared with it.
        let pinned = eng.applied().context("no applied position at ready")?;
        let t0 = std::time::Instant::now();
        exec(&mut c, "INSERT INTO entities VALUES (20, 4, 'new')").await?;
        // The three injected rounds: each ends at the same position and backs off, so the last
        // one lands no earlier than the first two steps of the ladder (≥ 0.5 s and ≥ 1 s).
        let three = loop {
            let s = eng.snapshot();
            if s.stream_reconnect_undecodable >= 3 {
                break s;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "the reader never reported an undecodable round: {} of 3 — log {}",
                s.stream_reconnect_undecodable,
                eng.log_path.display()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        eng.check_alive()?;
        ensure!(
            three.stream_reconnect_undecodable == 3 && three.events_undecodable == 3,
            "the reader reported {:?} undecodable events/reconnects for the 3 injected ones",
            (three.events_undecodable, three.stream_reconnect_undecodable)
        );
        for (name, got) in [
            ("transient", three.stream_reconnect_transient),
            ("closed", three.stream_reconnect_closed),
            ("silence", three.stream_reconnect_silence),
        ] {
            ensure!(
                got == 0,
                "a stuck round counted as a {name} reconnect too ({got})"
            );
        }
        // The position is pinned: nothing was applied while the reader was coming back to the
        // same event, which is what makes the counter (not a moved position) the signal.
        ensure!(
            eng.applied().as_ref() == Some(&pinned),
            "the applied position moved while the reader was stuck: {:?} != {pinned:?}",
            eng.applied()
        );
        // The probes stay green: the process *is* alive, the counter is what shows the view is not
        // advancing. And the counter is on `/metrics` with the name the alert rule of the
        // observability plan reads.
        let (code, _) = http_get(&addr, "/livez").await?;
        ensure!(code == 200, "/livez while the reader was stuck: {code}");
        let (code, body) = http_get(&addr, "/metrics").await?;
        ensure!(code == 200, "/metrics: {code}");
        ensure!(
            body.contains("lazyview_stream_reconnect_undecodable_total 3"),
            "/metrics does not show the 3 stuck reconnects:\n{}",
            body.lines()
                .filter(|l| l.contains("stream_reconnect") || l.contains("events_undecodable"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        // Once per position, not once per retry: one line for three rounds at the same place.
        let log = std::fs::read_to_string(&eng.log_path).unwrap_or_default();
        let named = log.matches("binlog event cannot be decoded").count();
        ensure!(
            named == 1,
            "the stuck position was named {named} times for 3 rounds: log {}",
            eng.log_path.display()
        );
        // The ladder, in wall time: the three pauses are the connect steps (1 s, 2 s, 4 s) times
        // a jitter of 0.5 .. 1.5, so catching up cannot take less than their lowest 3.5 s — where
        // the flat 200 ms of the reviewed commit took under a second.
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        let elapsed = t0.elapsed();
        ensure!(
            elapsed >= Duration::from_secs(3),
            "three stuck rounds took {elapsed:?}: the reader did not back off to the connect ladder"
        );
        ctx.log(format!("stuck for {elapsed:?}, then caught up"));
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after three undecodable rounds",
        )
        .await?;
        eng.stop().await
    });
}
