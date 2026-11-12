use super::*;

/// The first build of a view (here: rebuilt at startup) swaps its table in off the binlog loop:
/// an open reader transaction on the old table holds its metadata lock, and the swap waits and
/// retries for as long as it is open. On the loop, every attempt stalled the engine for up to
/// `lock_wait_timeout` (5 s), so no other view was refreshed meanwhile (brutal `window_0ms`:
/// 30 first builds serialized at 5-21 s per swap on slow servers).
#[test]
fn reg_first_build_swap_does_not_stall_other_views() {
    scenario("reg_swap_bg", |ctx| async move {
        use std::time::Instant;
        let specs = vec![
            ViewSpec::new(
                "r_names",
                Kind::Incremental,
                "SELECT id, name FROM customers",
            ),
            ViewSpec::new(
                "r_locked",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
        ];
        let (db, eng) = setup(&ctx, "reg_swap_bg", &[], &specs, &[]).await?;
        eng.stop().await?;
        // A reader transaction on r_locked: the swap's RENAME waits for its metadata lock.
        let mut reader = db.conn().await?;
        exec(&mut reader, "START TRANSACTION").await?;
        let _: Option<i64> = typed_first(&mut reader, "SELECT COUNT(*) FROM r_locked").await?;
        let mut c = db.conn().await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_START_MODE", "rebuild")])?;
        let r = async {
            // Both builds started (their state rows say `building`), then r_names is live.
            let t = Instant::now();
            while eng.snapshot().full_rebuilds < 2 {
                ensure!(
                    t.elapsed() < t_ready(),
                    "the startup rebuilds did not start"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            wait_ready(&mut c, &mut eng, &["r_names"], t_ready()).await?;
            // While r_locked's swap waits, changes to r_names are applied promptly.
            for k in 0..6 {
                let id = 100 + k;
                let t = Instant::now();
                exec(
                    &mut c,
                    &format!("INSERT INTO customers VALUES ({id}, 'n{id}', NULL)"),
                )
                .await?;
                loop {
                    let n: Option<i64> = typed_first(
                        &mut c,
                        format!("SELECT COUNT(*) FROM r_names WHERE id = {id}"),
                    )
                    .await?;
                    if n == Some(1) {
                        break;
                    }
                    ensure!(
                        t.elapsed() < Duration::from_secs(3),
                        "insert {k}: r_names not refreshed within 3 s while r_locked's swap waits \
                         for a metadata lock; metrics {}",
                        fmt_snapshot(&eng.snapshot())
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                tokio::time::sleep(Duration::from_millis(700)).await;
            }
            let state: Option<String> = typed_first(
                &mut c,
                "SELECT state FROM _lazyview_state WHERE name = 'r_locked'",
            )
            .await?;
            ensure!(
                state.as_deref() == Some("building"),
                "r_locked went live although its table is locked: {state:?}"
            );
            Ok(())
        }
        .await;
        exec(&mut reader, "COMMIT").await?;
        r?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after the swap").await?;
        eng.stop().await
    });
}

/// `(state, mode, detail, last_error)` of a view's state row on target `source`.
async fn state_detail(
    c: &mut mysql_async::Conn,
    name: &str,
) -> Result<Option<(String, String, Option<String>, Option<String>)>> {
    typed_exec_first(
        c,
        "SELECT CAST(state AS CHAR), CAST(mode AS CHAR), CAST(detail AS CHAR), CAST(last_error AS CHAR) \
             FROM _lazyview_state WHERE name = ? AND target = 'source'",
        (name,),
    )
    .await
}

/// A view reading a view in `error` said `waiting for upstream view ... to be built` forever,
/// so the troubleshooting query `WHERE state = 'error'` missed it. It is in `error` naming the
/// upstream's error (down a chain of such views) and is built once the upstream is fixed.
#[test]
fn reg_dependent_of_view_in_error_shows_its_error() {
    scenario("reg_blocked", |ctx| async move {
        let db = ctx.new_db("reg_blocked").await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, DDL).await?;
        ensure_lazyview_table(&mut c).await?;
        exec_all(
            &mut c,
            &[
                "INSERT INTO _lazyview (name, definition) VALUES ('lz_bad', 'SELECT nope FROM orders')",
                "INSERT INTO _lazyview (name, definition) VALUES ('lz_dep', 'SELECT * FROM lz_bad')",
                "INSERT INTO _lazyview (name, definition) VALUES ('lz_dep2', 'SELECT * FROM lz_dep')",
            ],
        )
        .await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let bad = wait_state(&mut c, &mut eng, "lz_bad", "error", t_ready()).await?;
        let bad_err = bad.last_error.clone().unwrap_or_default();
        let dep = wait_state(&mut c, &mut eng, "lz_dep", "error", t_ready()).await?;
        let dep_err = dep.last_error.clone().unwrap_or_default();
        ensure!(
            dep_err == format!("upstream view `lz_bad` is in error: {bad_err}"),
            "lz_dep: {dep:?}"
        );
        let dep2 = wait_state(&mut c, &mut eng, "lz_dep2", "error", t_ready()).await?;
        ensure!(
            dep2.last_error.as_deref()
                == Some(format!("upstream view `lz_dep` is in error: {dep_err}").as_str()),
            "lz_dep2: {dep2:?}"
        );
        let listed: i64 = typed_first(
            &mut c,
            "SELECT COUNT(*) FROM _lazyview_overview WHERE state = 'error'",
        )
        .await?
        .unwrap_or(0);
        ensure!(
            listed == 3,
            "the troubleshooting query lists {listed} of the 3 views in error"
        );
        // Fixing the upstream builds the whole chain.
        exec(
            &mut c,
            "UPDATE _lazyview SET definition = 'SELECT id, status FROM orders' WHERE name = 'lz_bad'",
        )
        .await?;
        let specs = vec![
            ViewSpec::new("lz_bad", Kind::Incremental, "SELECT id, status FROM orders"),
            ViewSpec::new("lz_dep", Kind::Chained, "SELECT * FROM lz_bad"),
            ViewSpec::new("lz_dep2", Kind::Chained, "SELECT * FROM lz_dep"),
        ];
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "INSERT INTO orders VALUES (14, 2, 'new')").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "chain after the upstream was fixed",
        )
        .await?;
        eng.stop().await
    });
}

/// A `_lazyview_state` row deleted by hand left the view maintained but with NULL state, mode
/// and detail in `_lazyview_overview` until a restart, which then rebuilt it. The row is written
/// back as it was (also for a view in error), and it still lets a restart resume the table.
#[test]
fn reg_deleted_state_row_is_written_back() {
    scenario("reg_state_row", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_state",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_state_row", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "INSERT INTO _lazyview (name, definition) VALUES ('r_err', 'SELECT nope FROM orders')",
        )
        .await?;
        wait_state(&mut c, &mut eng, "r_err", "error", t_ready()).await?;
        let before = (
            state_detail(&mut c, "r_state").await?,
            state_detail(&mut c, "r_err").await?,
        );
        exec(
            &mut c,
            "DELETE FROM _lazyview_state WHERE name IN ('r_state', 'r_err')",
        )
        .await?;
        let t = std::time::Instant::now();
        loop {
            let now = (
                state_detail(&mut c, "r_state").await?,
                state_detail(&mut c, "r_err").await?,
            );
            if now == before {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(15),
                "deleted state rows not written back within 15 s: {now:?}, before {before:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let managed: Option<i64> = typed_first(
            &mut c,
            "SELECT managed FROM _lazyview_state WHERE name = 'r_state'",
        )
        .await?;
        ensure!(
            managed == Some(1),
            "r_state's table is no longer recorded as lazyview's"
        );
        exec(&mut c, "INSERT INTO orders VALUES (14, 2, 'new')").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the state row came back").await?;
        // The row written back is a valid resume record: a restart resumes the table.
        eng.stop().await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let d = eng.snapshot();
        ensure!(
            d.full_rebuilds == 0,
            "restart rebuilt instead of resuming: {}",
            fmt_snapshot(&d)
        );
        verify(&ctx, &mut eng, &db, &specs, "resumed").await?;
        eng.stop().await
    });
}

/// A view whose build fails keeps the mode and detail it was planned with, the reason in
/// `last_error` (here: a table of that name that lazyview does not manage); only a definition
/// that cannot be planned shows mode `unknown`.
#[test]
fn reg_failed_build_keeps_planned_mode() {
    scenario("reg_err_mode", |ctx| async move {
        let db = ctx.new_db("reg_err_mode").await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, DDL).await?;
        ensure_lazyview_table(&mut c).await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE manual (id INT PRIMARY KEY)",
                "INSERT INTO _lazyview (name, definition) VALUES ('manual', 'SELECT id, status FROM orders')",
                "INSERT INTO _lazyview (name, definition) VALUES ('r_unplannable', 'SELECT nope FROM orders')",
            ],
        )
        .await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_state(&mut c, &mut eng, "manual", "error", t_ready()).await?;
        let (state, mode, detail, err) = state_detail(&mut c, "manual")
            .await?
            .context("no state row for manual")?;
        ensure!(
            (state.as_str(), mode.as_str(), detail.as_deref())
                == ("error", "incremental", Some("spj")),
            "manual: {state} {mode} {detail:?}"
        );
        let err = err.unwrap_or_default();
        ensure!(
            err.contains("refusing to replace existing base table")
                && err.contains("that lazyview does not manage"),
            "manual: last_error {err}"
        );
        wait_state(&mut c, &mut eng, "r_unplannable", "error", t_ready()).await?;
        let row = state_detail(&mut c, "r_unplannable").await?;
        ensure!(
            matches!(&row, Some((s, m, None, Some(_))) if s == "error" && m == "unknown"),
            "r_unplannable: {row:?}"
        );
        eng.stop().await
    });
}

/// `_lazyview.rebuild` of view `name` (0 once lazyview has done the requested rebuild).
async fn rebuild_flag(c: &mut mysql_async::Conn, name: &str) -> Result<i64> {
    typed_exec_first(c, "SELECT rebuild FROM _lazyview WHERE name = ?", (name,))
        .await?
        .context("no _lazyview row")
}

/// Waits until lazyview cleared `_lazyview.rebuild` of `name`.
async fn wait_rebuild_flag_cleared(
    c: &mut mysql_async::Conn,
    eng: &mut Engine,
    name: &str,
) -> Result<()> {
    let t = std::time::Instant::now();
    while rebuild_flag(c, name).await? != 0 {
        eng.check_alive()?;
        ensure!(
            t.elapsed() < t_ready(),
            "{name}: _lazyview.rebuild still set after {:?}",
            t_ready()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// There was no way to rebuild one view (hand edits of its table are not noticed, and the
/// tricks a user tries, `SET definition = definition`, do nothing). `UPDATE _lazyview SET
/// rebuild = 1` rebuilds that view online and is cleared once the new table is live, also when
/// set while lazyview is stopped; the other views are left alone.
#[test]
fn reg_rebuild_flag_rebuilds_one_view() {
    scenario("reg_rebuild_flag", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_counts",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
            ViewSpec::new(
                "r_names",
                Kind::Incremental,
                "SELECT id, name FROM customers",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_rebuild_flag", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        ensure!(
            rebuild_flag(&mut c, "r_counts").await? == 0,
            "rebuild set without a request"
        );
        for restart in [false, true] {
            // A hand edit lazyview cannot notice.
            exec(&mut c, "DELETE FROM r_counts WHERE customer_id = 1").await?;
            let ask = "UPDATE _lazyview SET rebuild = 1 WHERE name = 'r_counts'";
            let updated_sql =
                "SELECT CAST(updated_at AS CHAR) FROM _lazyview WHERE name = 'r_counts'";
            let updated: Option<String> = if restart {
                // Set while lazyview is stopped: the restart rebuilds instead of resuming.
                let running = eng;
                running.stop().await?;
                exec(&mut c, ask).await?;
                let u = typed_first(&mut c, updated_sql).await?;
                eng = ctx.start_engine(&db, &[])?;
                u
            } else {
                exec(&mut c, ask).await?;
                typed_first(&mut c, updated_sql).await?
            };
            let before = eng.snapshot();
            wait_rebuild_flag_cleared(&mut c, &mut eng, "r_counts").await?;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            let what = if restart { "restart" } else { "running" };
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("rebuild flag ({what})"),
            )
            .await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                d.full_rebuilds == 1,
                "{what}: expected one rebuild (r_counts only): {}",
                fmt_snapshot(&d)
            );
            let after: Option<String> = typed_first(
                &mut c,
                "SELECT CAST(updated_at AS CHAR) FROM _lazyview WHERE name = 'r_counts'",
            )
            .await?;
            ensure!(
                after == updated,
                "{what}: clearing the flag moved updated_at ({updated:?} -> {after:?})"
            );
            exec(&mut c, "INSERT INTO orders VALUES (20 + 1, 1, 'new')").await?;
            exec(&mut c, "DELETE FROM orders WHERE id = 21").await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after the rebuild ({what})"),
            )
            .await?;
        }
        eng.stop().await
    });
}

/// `TRUNCATE` of a view's table (the lazy user's "reset") went unnoticed: the view stayed
/// `ready` and groups that were not touched again were missing for good. lazyview never
/// truncates its tables, so it rebuilds the view online.
#[test]
fn reg_truncated_view_table_is_rebuilt() {
    scenario("reg_truncate_mv", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_counts",
            Kind::Incremental,
            "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
        )];
        let (db, mut eng) = setup(&ctx, "reg_truncate_mv", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        let before = eng.snapshot();
        exec_all(
            &mut c,
            &[
                "TRUNCATE r_counts",
                "INSERT INTO orders VALUES (14, 1, 'new')",
            ],
        )
        .await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &db,
            &std::collections::HashMap::new(),
            &specs,
            t_ready(),
            "after TRUNCATE of the view table",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds >= 1,
            "the truncated view converged without a rebuild?: {}",
            fmt_snapshot(&d)
        );
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "UPDATE orders SET customer_id = 3 WHERE id = 10").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the rebuild").await?;
        eng.stop().await
    });
}

/// Two definitions whose view names differ only in letter case (`Lz_Case`, `lz_case`): the
/// second one was shown as `#<id>` with "name `lz_case` already used by row #N" (`_lazyview.name`
/// ignores case, the tables do not). It keeps its name, the error names both rows and how to
/// remove this one, and the first view is unaffected (its state row is not overwritten).
#[test]
fn reg_case_only_name_clash() {
    scenario("reg_case_clash", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "Lz_Case",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_case_clash", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        let first: u64 = typed_first(&mut c, "SELECT id FROM _lazyview WHERE name = 'Lz_Case'")
            .await?
            .context("no Lz_Case row")?;
        exec(
            &mut c,
            "INSERT INTO _lazyview (definition) VALUES ('CREATE VIEW lz_case AS SELECT id, customer_id FROM orders')",
        )
        .await?;
        let second: u64 = typed_first(&mut c, "SELECT LAST_INSERT_ID()")
            .await?
            .context("no id")?;
        let row = wait_state(&mut c, &mut eng, "lz_case", "error", t_ready()).await?;
        let expected = format!(
            "name `lz_case` is already used by row #{first} as `Lz_Case`: view names that differ \
             only in letter case are the same name to lazyview; rename this view, or remove this \
             row with DELETE FROM _lazyview WHERE id = {second}"
        );
        ensure!(
            row.last_error.as_deref() == Some(expected.as_str()),
            "lz_case: {row:?}"
        );
        let shown: Vec<(u64, String, Option<String>)> = typed_rows(&mut c, "SELECT id, CAST(name AS CHAR), CAST(state AS CHAR) FROM _lazyview_overview ORDER BY id")
            .await?;
        ensure!(
            shown
                == vec![
                    (first, "Lz_Case".to_string(), Some("ready".to_string())),
                    (second, "lz_case".to_string(), Some("error".to_string())),
                ],
            "overview: {shown:?}"
        );
        exec(&mut c, "INSERT INTO orders VALUES (14, 2, 'new')").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "Lz_Case next to the clashing row",
        )
        .await?;
        // Removed as the message says: only lz_case's row goes.
        exec(
            &mut c,
            &format!("DELETE FROM _lazyview WHERE id = {second}"),
        )
        .await?;
        let t = std::time::Instant::now();
        loop {
            let n: Option<i64> = typed_first(
                &mut c,
                "SELECT COUNT(*) FROM _lazyview_state WHERE name = 'lz_case'",
            )
            .await?;
            if n == Some(0) {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(15),
                "lz_case's state row stayed after its definition was deleted"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_ready(&mut c, &["Lz_Case"]).await?;
        eng.stop().await
    });
}

/// A view named like the source table it reads (`orders`) said "view `orders` cannot read
/// itself", which reads like recursion. It gets the documented refusal for a table lazyview
/// does not manage, other views over `orders` keep working, and a definition that really
/// reads its own view still gets "cannot read itself".
#[test]
fn reg_view_named_like_its_source_table() {
    scenario("reg_self_named", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_orders",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_self_named", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "INSERT INTO _lazyview (name, definition) VALUES ('orders', 'SELECT id FROM orders')",
                "INSERT INTO _lazyview (name, definition) VALUES ('r_self', 'SELECT * FROM r_self')",
            ],
        )
        .await?;
        let row = wait_state(&mut c, &mut eng, "orders", "error", t_ready()).await?;
        let expected = format!(
            "refusing to replace existing base table `{}`.`orders` on target source that \
             lazyview does not manage (the view reads it: give the view another name)",
            db.name
        );
        ensure!(
            row.last_error.as_deref() == Some(expected.as_str()),
            "orders: {row:?}"
        );
        let row = wait_state(&mut c, &mut eng, "r_self", "error", t_ready()).await?;
        ensure!(
            row.last_error.as_deref() == Some("view `r_self` cannot read itself"),
            "r_self: {row:?}"
        );
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id = 10").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "a view over orders next to them",
        )
        .await?;
        let n: Option<i64> = typed_first(&mut c, "SELECT COUNT(*) FROM orders").await?;
        ensure!(
            n == Some(4),
            "the source table orders was touched: {n:?} rows"
        );
        eng.stop().await
    });
}
