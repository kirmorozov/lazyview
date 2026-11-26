use super::*;

const TWO_HOP_DDL: &[&str] = &[
    "CREATE TABLE regs (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
    "CREATE TABLE custs (id INT PRIMARY KEY, reg_id INT NULL, name VARCHAR(20) NOT NULL)",
    "INSERT INTO regs VALUES (1, 'north'), (2, 'south'), (3, 'east')",
    "INSERT INTO custs VALUES (1, 1, 'ann'), (2, 1, 'bob'), (3, 2, 'cid')",
];

/// E6: two tables resolved by lookups change together (a customer moves to a region that is
/// deleted in the same transaction): each before image joined with the current state of the
/// other found nothing, so the orders of that customer stayed (v_order_customer_region).
#[test]
fn reg_two_lookup_tables_change_together() {
    scenario("reg_two_lookups", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_order_region",
            Kind::Incremental,
            "SELECT o.id, o.status, c.name AS customer, r.name AS region FROM orders o JOIN custs c ON c.id = o.customer_id JOIN regs r ON r.id = c.reg_id",
        )];
        let (db, mut eng) = setup(&ctx, "reg_two_lookups", TWO_HOP_DDL, &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "START TRANSACTION",
                "UPDATE custs SET reg_id = 3 WHERE id = 1",
                "DELETE FROM regs WHERE id = 3",
                "COMMIT",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer moved to a region deleted with it",
        )
        .await?;
        exec_all(
            &mut c,
            &[
                "START TRANSACTION",
                "INSERT INTO regs VALUES (4, 'west')",
                "UPDATE custs SET reg_id = 4 WHERE id = 1",
                "UPDATE regs SET id = 9 WHERE id = 2",
                "UPDATE custs SET reg_id = NULL WHERE id = 3",
                "COMMIT",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "both lookup tables changed again",
        )
        .await?;
        eng.stop().await
    });
}

/// E6, grouped: the group of an order is two lookups away. Orders move to another customer
/// while their old customer moves to another region in the same transaction: the old group
/// was found through neither table and kept its count.
#[test]
fn reg_two_lookup_tables_change_together_grouped() {
    scenario("reg_two_lookups_g", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_orders_per_region",
            Kind::Incremental,
            "SELECT r.name, COUNT(*) AS n FROM orders o JOIN custs c ON c.id = o.customer_id JOIN regs r ON r.id = c.reg_id GROUP BY r.name",
        )];
        let (db, mut eng) = setup(&ctx, "reg_two_lookups_g", TWO_HOP_DDL, &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "START TRANSACTION",
                "UPDATE orders SET customer_id = 3 WHERE customer_id = 1",
                "UPDATE custs SET reg_id = 2 WHERE id = 1",
                "COMMIT",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "orders and their customer moved together",
        )
        .await?;
        eng.stop().await
    });
}

/// E8: unit filters bind one placeholder per unit column; with composite units and a raised
/// LAZYVIEW_QUERY_CHUNK one chunk exceeded the 65535 placeholders of a statement, every refresh
/// failed and the view was rebuilt.
#[test]
fn reg_unit_chunks_stay_below_placeholder_limit() {
    scenario("reg_big_chunk", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_pairs",
            Kind::Incremental,
            "SELECT a, b, COUNT(*) AS n FROM ev GROUP BY a, b",
        )];
        let nums: Vec<String> = (0..200).map(|i| format!("({i})")).collect();
        let ddl = [
            "CREATE TABLE ev (id INT AUTO_INCREMENT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)"
                .to_string(),
            "CREATE TABLE nums (n INT PRIMARY KEY)".to_string(),
            format!("INSERT INTO nums VALUES {}", nums.join(",")),
        ];
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let (db, mut eng) = setup(
            &ctx,
            "reg_big_chunk",
            &ddl,
            &specs,
            &[("LAZYVIEW_QUERY_CHUNK", "40000")],
        )
        .await?;
        let mut c = db.conn().await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        // 40000 groups in one transaction.
        exec_all(
            &mut c,
            &["INSERT INTO ev (a, b) SELECT x.n, y.n FROM nums x JOIN nums y"],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "40000 new groups at once").await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "the refresh failed and the view was rebuilt: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// E5: while a stale full-refresh view holds the saved position back (its rebuild is not due),
/// the engine must not spin: the position save returned early without moving its deadline, so
/// the loop flushed thousands of times per second.
#[test]
fn reg_held_back_position_does_not_spin() {
    scenario("reg_held_spin", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_statuses",
            Kind::Full,
            // NOW() keeps it a full-refresh view (DISTINCT alone is maintained incrementally).
            "SELECT DISTINCT status FROM orders WHERE NOW() > '2000-01-01'",
        )];
        let (db, mut eng) = setup(
            &ctx,
            "reg_held_spin",
            &[],
            &specs,
            &[
                ("LAZYVIEW_FULL_REFRESH_MS", "600000"),
                ("LAZYVIEW_POSITION_PERSIST_MS", "50"),
            ],
        )
        .await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, &["UPDATE orders SET status = 'void' WHERE id = 10"]).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let before = eng.snapshot();
        // Keep the fixed sleep: "nothing happens" is the assertion — no spinning (flushes
        // stay below 200) within this 2 s window; a poll has no value to wait for.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.flushes < 200,
            "{} flushes in 2 s while idle with a held-back position: {}",
            d.flushes,
            fmt_snapshot(&d)
        );
        eng.check_alive()?;
        eng.stop().await
    });
}

/// Payments → orders → customers → regions: three lookup tables of a grouped view.
const CHAIN_DDL: &[&str] = &[
    "CREATE TABLE regs (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
    "CREATE TABLE custs (id INT PRIMARY KEY, reg_id INT NULL, name VARCHAR(20) NOT NULL)",
    "CREATE TABLE pays (id INT PRIMARY KEY, order_id INT NULL, amount DECIMAL(10,2) NOT NULL)",
    "INSERT INTO regs VALUES (1, 'north'), (2, 'south'), (3, 'east')",
    "INSERT INTO custs VALUES (1, 1, 'ann'), (2, 1, 'bob'), (3, 2, 'cid')",
    "INSERT INTO pays VALUES (1, 10, 5.00), (2, 11, 7.00), (3, 12, 1.50), (4, 13, 2.00)",
];

fn region_revenue() -> ViewSpec {
    ViewSpec::new(
        "r_region_revenue",
        Kind::Incremental,
        "SELECT r.name, COUNT(*) AS n, SUM(p.amount) AS paid FROM pays p JOIN orders o ON o.id = p.order_id \
         JOIN custs c ON c.id = o.customer_id JOIN regs r ON r.id = c.reg_id GROUP BY r.name",
    )
}

/// Stops the engine, runs each statement as its own transaction while it is down, and restarts
/// it with a zero batch window: the catch-up flushes after every transaction while the later
/// ones are already visible (the read-ahead window, made deterministic).
async fn replay_after_restart(
    ctx: &Ctx,
    db: &Db,
    eng: Engine,
    stmts: &[&str],
    ov: &[(&str, &str)],
) -> Result<Engine> {
    eng.stop().await?;
    let mut c = db.conn().await?;
    exec_all(&mut c, stmts).await?;
    let mut ov = ov.to_vec();
    ov.push(("LAZYVIEW_BATCH_WINDOW_MS", "0"));
    ctx.start_engine(db, &ov)
}

/// E6, grouped: changes in two and three lookup tables of a chain in one transaction. Each
/// single lookup joins an old row with the current (already changed) partners and misses the
/// old group; the combinations find it.
#[test]
fn reg_grouped_chain_changes_in_one_window() {
    scenario("reg_chain_one", |ctx| async move {
        let specs = vec![region_revenue()];
        let (db, mut eng) = setup(&ctx, "reg_chain_one", CHAIN_DDL, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "order and its customer moved together",
                &[
                    "START TRANSACTION",
                    "UPDATE orders SET customer_id = 3 WHERE id = 12",
                    "UPDATE custs SET reg_id = 3 WHERE id = 2",
                    "COMMIT",
                ],
            ),
            (
                "payment, order and customer moved together",
                &[
                    "START TRANSACTION",
                    "UPDATE pays SET order_id = 13 WHERE id = 1",
                    "UPDATE orders SET customer_id = 2 WHERE id = 10",
                    "UPDATE custs SET reg_id = 2 WHERE id = 1",
                    "COMMIT",
                ],
            ),
            (
                "far partner deleted with a key move",
                &[
                    "START TRANSACTION",
                    "DELETE FROM regs WHERE id = 2",
                    "UPDATE custs SET id = 30 WHERE id = 3",
                    "COMMIT",
                ],
            ),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// E6, grouped: the same changes in separate transactions that are already visible when the
/// first one is resolved (catch-up after a restart). The partner's change is read in a later
/// flush; only the images remembered from the earlier flush still find the old group.
#[test]
fn reg_grouped_chain_changes_across_windows() {
    scenario("reg_chain_across", |ctx| async move {
        let specs = vec![region_revenue()];
        let (db, eng) = setup(&ctx, "reg_chain_across", CHAIN_DDL, &specs, &[]).await?;
        let mut eng = replay_after_restart(
            &ctx,
            &db,
            eng,
            &[
                "UPDATE custs SET reg_id = 3 WHERE id = 2",
                "UPDATE orders SET customer_id = 3 WHERE id = 12",
                "UPDATE custs SET reg_id = 3 WHERE id = 1",
                "UPDATE orders SET customer_id = 3 WHERE id = 10",
                "UPDATE pays SET order_id = 13 WHERE id = 2",
            ],
            &[],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "two and three tables, one per transaction",
        )
        .await?;
        let mut eng = replay_after_restart(
            &ctx,
            &db,
            eng,
            &[
                "DELETE FROM regs WHERE id = 3",
                "UPDATE custs SET id = 40 WHERE id = 3",
                "UPDATE orders SET customer_id = 1 WHERE id = 13",
            ],
            &[],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "far partner deleted, key moves",
        )
        .await?;
        let d = eng.snapshot();
        ensure!(
            d.full_rebuilds == 0,
            "the view was rebuilt: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// E6, grouped: a long catch-up remembers many images (past the old cap of 10 x
/// LAZYVIEW_BATCH_MAX_KEYS, which rebuilt the view). The lookups run in chunks, one combination
/// after the other, never a rebuild instead; the view ends up correct.
#[test]
fn reg_grouped_chain_long_catch_up_is_chunked() {
    scenario("reg_chain_cap", |ctx| async move {
        let specs = vec![region_revenue()];
        let (db, eng) = setup(&ctx, "reg_chain_cap", CHAIN_DDL, &specs, &[]).await?;
        let mut stmts = Vec::new();
        for i in 0..40 {
            stmts.push(format!(
                "UPDATE custs SET reg_id = {} WHERE id = {}",
                1 + i % 3,
                1 + i % 3
            ));
            stmts.push(format!(
                "UPDATE orders SET customer_id = {} WHERE id = {}",
                1 + (i + 1) % 3,
                10 + i % 4
            ));
            stmts.push(format!(
                "UPDATE pays SET order_id = {} WHERE id = {}",
                10 + (i + 2) % 4,
                1 + i % 4
            ));
        }
        let stmts: Vec<&str> = stmts.iter().map(String::as_str).collect();
        let mut eng =
            replay_after_restart(&ctx, &db, eng, &stmts, &[("LAZYVIEW_BATCH_MAX_KEYS", "2")])
                .await?;
        verify(&ctx, &mut eng, &db, &specs, "after a long catch-up").await?;
        let d = eng.snapshot();
        ensure!(
            d.full_rebuilds == 0 && d.lookup_queries > 0,
            "the long catch-up gave up its lookups or rebuilt the view: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// E6, select/join: old rows are traced through the lineage columns of the materialized table.
/// A region and then its customer are deleted in separate transactions, both visible before the
/// first is read: joining either old row with the current state of the other finds nothing.
#[test]
fn reg_lineage_traces_old_rows_across_windows() {
    scenario("reg_lineage", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_order_region",
                Kind::Incremental,
                "SELECT o.id, o.status, c.name AS customer, r.name AS region FROM orders o \
                 JOIN custs c ON c.id = o.customer_id JOIN regs r ON r.id = c.reg_id",
            ),
            ViewSpec::new(
                "r_order_region_left",
                Kind::Incremental,
                "SELECT o.id, c.name AS customer, r.name AS region FROM orders o \
                 LEFT JOIN custs c ON c.id = o.customer_id LEFT JOIN regs r ON r.id = c.reg_id",
            ),
        ];
        let (db, eng) = setup(&ctx, "reg_lineage", TWO_HOP_DDL, &specs, &[]).await?;
        let mut eng = replay_after_restart(
            &ctx,
            &db,
            eng,
            &[
                "DELETE FROM regs WHERE id = 1",
                "DELETE FROM custs WHERE id = 1",
                "UPDATE custs SET id = 20 WHERE id = 2",
                "UPDATE custs SET reg_id = 3 WHERE id = 3",
                "DELETE FROM regs WHERE id = 3",
            ],
            &[],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "partners deleted and moved").await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "partners back (NULL-extended rows join again)",
                &[
                    "INSERT INTO regs VALUES (1, 'north2'), (3, 'east2')",
                    "INSERT INTO custs VALUES (1, 1, 'ann2'), (2, 3, 'bob2')",
                ],
            ),
            (
                "region key moves",
                &[
                    "UPDATE regs SET id = 7 WHERE id = 1",
                    "UPDATE custs SET reg_id = 7 WHERE id = 1",
                ],
            ),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// E7: binlog events reach the replica before their commit is visible. MariaDB had only a fixed
/// sleep (LAZYVIEW_VISIBILITY_DELAY_MS) between reading a transaction and reading the sources,
/// so under load a refresh could read the old row and keep it (rows missing from / extra in
/// v_paid_orders). Now the consistent-snapshot binlog position is the fence there. Concurrent
/// writers with the delay set to 0 must leave the views exact.
#[test]
fn reg_visibility_fence_without_delay() {
    scenario("reg_fence", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_paid",
                Kind::Incremental,
                "SELECT id, customer_id, status FROM orders WHERE status = 'paid'",
            ),
            join_view(),
        ];
        let ov = [
            ("LAZYVIEW_VISIBILITY_DELAY_MS", "0"),
            ("LAZYVIEW_BATCH_WINDOW_MS", "0"),
        ];
        let (db, mut eng) = setup(&ctx, "reg_fence", &[], &specs, &ov).await?;
        let mut writers = Vec::new();
        for w in 0..4i64 {
            let dsn = db.dsn.clone();
            writers.push(tokio::spawn(async move {
                let mut c = connect(&dsn).await?;
                for i in 0..150i64 {
                    let id = 100 + w * 1000 + i % 20;
                    let status = if (i / 20) % 2 == 0 { "paid" } else { "new" };
                    exec(
                        &mut c,
                        &format!(
                            "INSERT INTO orders VALUES ({id}, {}, '{status}') \
                             ON DUPLICATE KEY UPDATE status = VALUES(status), customer_id = VALUES(customer_id)",
                            1 + (i + w) % 3
                        ),
                    )
                    .await?;
                    if i % 7 == 0 {
                        exec(
                            &mut c,
                            &format!("UPDATE customers SET name = 'n{w}_{i}' WHERE id = {}", 1 + i % 3),
                        )
                        .await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }));
        }
        for w in writers {
            w.await.context("writer panicked")??;
        }
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "concurrent writers, no visibility delay",
        )
        .await?;
        eng.stop().await
    });
}

/// E1c: a view that joins the parent of a cascading foreign key on the key (here two levels:
/// groups → parents → children) finds the rows a cascade changes through the parent's own row
/// (lineage), instead of rebuilding on every parent delete or key update.
#[test]
fn reg_cascade_traced_through_the_parent() {
    scenario("reg_fk_traced", |ctx| async move {
        let ddl = [
            "CREATE TABLE grps (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
            "CREATE TABLE pars (id INT PRIMARY KEY, grp_id INT NULL, label VARCHAR(20) NOT NULL, KEY (grp_id),
             FOREIGN KEY (grp_id) REFERENCES grps (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE kids (par_id INT NOT NULL, seq INT NOT NULL, qty INT NOT NULL, PRIMARY KEY (par_id, seq),
             FOREIGN KEY (par_id) REFERENCES pars (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "INSERT INTO grps VALUES (1, 'g1'), (2, 'g2'), (3, 'g3')",
            "INSERT INTO pars VALUES (10, 1, 'a'), (11, 1, 'b'), (12, 2, 'c'), (13, NULL, 'd'), (14, 3, 'e')",
            "INSERT INTO kids VALUES (10, 1, 5), (10, 2, 6), (11, 1, 7), (12, 1, 8), (13, 1, 9), (14, 1, 1)",
        ];
        let specs = vec![ViewSpec::new(
            "r_kids",
            Kind::Incremental,
            "SELECT k.par_id, k.seq, k.qty, p.label, g.name AS grp FROM kids k \
             JOIN pars p ON p.id = k.par_id LEFT JOIN grps g ON g.id = p.grp_id",
        )];
        let (db, mut eng) = setup(&ctx, "reg_fk_traced", &ddl, &specs, &[]).await?;
        let before = eng.snapshot();
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "parent deleted (children cascade)",
                &["DELETE FROM pars WHERE id = 12"],
            ),
            (
                "parent key moved (children follow)",
                &["UPDATE pars SET id = 20 WHERE id = 10"],
            ),
            (
                "group deleted (two levels cascade)",
                &["DELETE FROM grps WHERE id = 1"],
            ),
            ("group key moved", &["UPDATE grps SET id = 30 WHERE id = 3"]),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "cascades rebuilt the view: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// E8: a saved position belongs to the server that wrote it (`_lazyview_status.server_identity`:
/// `@@server_uuid`, MariaDB server id + GTID domain) — until PR D: a cursor that carries a
/// GTID set is verified against the new server's own state and RESUMES there (adoption rules,
/// review A-01's fix); the stop through LAZYVIEW_ON_MISSING_BINLOG is the pre-GTID behavior,
/// kept for a source whose saved cursor carries no set (`gtid_mode=OFF`, or a pre-GTID text).
#[test]
fn reg_saved_position_from_another_server() {
    scenario("reg_identity", |ctx| async move {
        let specs = vec![join_view()];
        let (db, mut eng) = setup(&ctx, "reg_identity", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        // One foreign transaction while the engine streams: a GTID source then carries its
        // set on the saved cursor, which is what the adoption rules ask about.
        exec(&mut c, "UPDATE customers SET name = 'seen' WHERE id = 1").await?;
        verify(&ctx, &mut eng, &db, &specs, "first update").await?;
        eng.stop().await?;
        let saved = scalar(&mut c, "SELECT server_identity FROM _lazyview_status").await?;
        ensure!(
            saved.as_deref().is_some_and(|s| !s.is_empty()),
            "no server identity saved: {saved:?}"
        );
        let cursor = scalar(&mut c, "SELECT binlog_cursor FROM _lazyview_status").await?;
        let carries_set = cursor.as_deref().is_some_and(|s| s.contains("[g="));
        exec(
            &mut c,
            "UPDATE _lazyview_status SET server_identity = 'another-server'",
        )
        .await?;
        exec_all(
            &mut c,
            &["UPDATE customers SET name = 'moved' WHERE id = 1"],
        )
        .await?;
        let eng = ctx.start_engine(&db, &[])?;
        if carries_set {
            // Resume by GTID on the "other" server: no stop, no rebuild, and the row written
            // during the switch arrives through the stream.
            let before = eng.snapshot();
            let mut eng = eng;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                "resumed on another server by GTID",
            )
            .await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                d.full_rebuilds == 0,
                "the adoption rules rebuilt instead of resuming: {}",
                fmt_snapshot(&d)
            );
            eng.stop().await?;
        } else {
            match eng.wait_finished(Duration::from_secs(60)).await? {
                Err(e) if e.contains("another-server") => {}
                other => bail!("expected a failure naming the saved server, got {other:?}"),
            }
        }
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_ON_MISSING_BINLOG", "rebuild")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "rebuilt after a position from another server",
        )
        .await?;
        let now = scalar(&mut c, "SELECT server_identity FROM _lazyview_status").await?;
        ensure!(
            now == saved,
            "identity not re-recorded: {now:?} (was {saved:?})"
        );
        eng.stop().await
    });
}

/// Binlogs purged while the engine is down (the Amazon RDS / Aurora retention case): the saved
/// position is gone, the engine fails by default naming that, and with
/// LAZYVIEW_ON_MISSING_BINLOG=rebuild it rebuilds every view and streams on, equal to the oracle.
/// Opt-in (purges the server's binlogs): LAZYVIEW_TEST_PURGE_BINLOGS=1.
#[test]
fn reg_saved_position_purged_while_down() {
    scenario("reg_purged_down", |ctx| async move {
        if std::env::var("LAZYVIEW_TEST_PURGE_BINLOGS")
            .map(|v| v != "1")
            .unwrap_or(true)
        {
            ctx.skip(
                "purges the server's binlogs; set LAZYVIEW_TEST_PURGE_BINLOGS=1 to run it (only on a server no other binlog reader uses)",
            );
            return Ok(());
        }
        let specs = vec![join_view()];
        let (db, eng) = setup(&ctx, "reg_purged_down", &[], &specs, &[]).await?;
        eng.stop().await?;
        let mut c = db.conn().await?;
        let saved = scalar(&mut c, "SELECT binlog_file FROM _lazyview_status")
            .await?
            .unwrap_or_default();
        ensure!(!saved.is_empty(), "no saved binlog file");
        // Writes in newer files, then everything before the newest file is purged.
        for i in 0..2 {
            exec(&mut c, "FLUSH BINARY LOGS").await?;
            let update = format!("UPDATE customers SET name = 'purged{i}' WHERE id = 1");
            exec_all(&mut c, &[update.as_str()]).await?;
        }
        exec(&mut c, "FLUSH BINARY LOGS").await?;
        let logs = common::rows(&mut c, "SHOW BINARY LOGS").await?;
        let newest = logs
            .last()
            .and_then(|r| r.first().cloned().flatten())
            .context("no binary logs listed")?;
        exec(&mut c, &format!("PURGE BINARY LOGS TO '{newest}'")).await?;
        let left = common::rows(&mut c, "SHOW BINARY LOGS").await?;
        ensure!(
            left.iter()
                .all(|r| r.first().cloned().flatten().as_deref() != Some(saved.as_str())),
            "{saved} was not purged"
        );
        let eng = ctx.start_engine(&db, &[])?;
        match eng.wait_finished(Duration::from_secs(60)).await? {
            Err(e) if e.contains("no longer available") => {}
            other => bail!("expected a missing-binlog failure, got {other:?}"),
        }
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_ON_MISSING_BINLOG", "rebuild")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "rebuilt after the saved binlog was purged",
        )
        .await?;
        exec_all(
            &mut c,
            &["UPDATE customers SET name = 'after' WHERE id = 1"],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "streaming after the rebuild").await?;
        eng.stop().await
    });
}

/// E6, lookup table without a primary key: its whole row is the lineage key. Equal duplicate
/// rows, membership moves and an ENUM predicate in numeric context (`p.e + 0 >= 2`, which a
/// row image bound as a label string evaluated wrongly) stay exact.
#[test]
fn reg_lineage_without_primary_key() {
    scenario("reg_lineage_nopk", |ctx| async move {
        let ddl = [
            "CREATE TABLE tags (status VARCHAR(10) NOT NULL, label VARCHAR(10) NOT NULL, e ENUM('a','b','c') NULL)",
            "INSERT INTO tags VALUES ('new', 'fresh', 'a'), ('new', 'fresh', 'a'), ('paid', 'done', 'c'), ('paid', 'cash', 'b')",
        ];
        let specs = vec![
            ViewSpec::new(
                "r_tagged",
                Kind::Incremental,
                "SELECT o.id, t.label, t.e FROM orders o JOIN tags t ON t.status = o.status",
            ),
            ViewSpec::new(
                "r_tagged_hi",
                Kind::Incremental,
                "SELECT o.id, t.label FROM orders o JOIN tags t ON t.status = o.status WHERE t.e + 0 >= 2",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_lineage_nopk", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "one of two duplicates deleted",
                &["DELETE FROM tags WHERE label = 'fresh' LIMIT 1"],
            ),
            (
                "ENUM moves across the numeric threshold",
                &[
                    "UPDATE tags SET e = 'a' WHERE label = 'done'",
                    "UPDATE tags SET e = 'c' WHERE label = 'fresh'",
                ],
            ),
            (
                "membership moves",
                &["UPDATE tags SET status = 'paid' WHERE label = 'fresh'"],
            ),
            (
                "duplicate added, then all deleted",
                &[
                    "INSERT INTO tags VALUES ('paid', 'cash', 'b')",
                    "DELETE FROM tags WHERE label = 'cash'",
                ],
            ),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// #391 (the 1 in 10 left of `chaos_tgt_blackhole_during_flush`): at most one lag retry may be
/// in flight per view and target. `lag_service` drains the retries' results, folds them (the
/// fold of a catch-up awaits its state row), and then frees the target of every retry whose task
/// has ended as "lost". A retry that sent its result during that fold has ended, yet its result
/// still waits in the channel: the target is claimed again, the late fold then removes the new
/// claim's handle, and a third claim starts next to the second. Two retries write the same units
/// at once. On a view without row identity both insert the rows of a unit the target never had,
/// and since the later fold finds no entry left to owe them, the target keeps them twice: extra
/// rows after convergence, on that target only.
///
/// Each step is placed by the test, none is raced. A `BEFORE INSERT` trigger on each target's
/// table reads a gate row `FOR UPDATE`, so the test refuses the writes (gate 0), holds them (the
/// gate row locked) or lets them through. The fold of `t2`'s catch-up is held on the test's lock
/// of its state row, and `t1`'s retry is let through only while that fold is held. The result is
/// read at convergence.
#[test]
fn reg_one_lag_retry_per_target_at_a_time() {
    scenario("reg_lag_claims", |ctx| async move {
        use std::collections::HashMap;
        let db = ctx.new_db("reg_lag_claims").await?;
        let t1 = ctx.new_db("reg_lag_claims_t1").await?;
        let t2 = ctx.new_db("reg_lag_claims_t2").await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, DDL).await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE payments (id INT PRIMARY KEY, order_id INT NOT NULL, \
                 amount DECIMAL(10,2) NOT NULL)",
                "INSERT INTO payments VALUES (100, 10, 5.00), (101, 11, 7.50)",
            ],
        )
        .await?;
        ensure_lazyview_table(&mut c).await?;
        let specs = vec![
            // `p` is a unit alias: no row identity, written per unit, no unique key on a target.
            ViewSpec::new(
                "r_lag_units",
                Kind::Incremental,
                "SELECT o.id, o.status, p.id AS pay_id, p.amount FROM orders o \
                 LEFT JOIN payments p ON p.order_id = o.id",
            )
            .with_targets("source,t1"),
            // The view whose catch-up on `t2` is held in the middle of its fold.
            ViewSpec::new(
                "r_lag_hold",
                Kind::Incremental,
                "SELECT id, name FROM customers",
            )
            .with_targets("source,t2"),
        ];
        add_views(&mut c, &specs).await?;
        let mut ov = target_env(&[("t1", t1.dsn.as_str()), ("t2", t2.dsn.as_str())]);
        // A held write waits for the test: it never times out on its own.
        ov.push(("LAZYVIEW_TARGET_TIMEOUT_MS".into(), "300000".into()));
        let ov: Vec<(&str, &str)> = ov.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let mut eng = ctx.start_engine(&db, &ov)?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let targets: HashMap<String, Db> = [
            ("t1".to_string(), t1.clone()),
            ("t2".to_string(), t2.clone()),
        ]
        .into();
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &targets,
            &HashMap::new(),
            &specs,
            "initial",
        )
        .await?;

        let mut g1 = t1.conn().await?;
        let mut g2 = t2.conn().await?;
        for (g, table) in [(&mut g1, "r_lag_units"), (&mut g2, "r_lag_hold")] {
            exec_all(
                g,
                &[
                    format!(
                        "CREATE TABLE lv_gate_{table} (id INT PRIMARY KEY, v TINYINT NOT NULL)"
                    ),
                    format!("INSERT INTO lv_gate_{table} VALUES (1, 0)"),
                ],
            )
            .await?;
            exec(
                g,
                &format!(
                    "CREATE TRIGGER lv_gate_{table} BEFORE INSERT ON {table} FOR EACH ROW BEGIN \
                     DECLARE gv TINYINT; \
                     SELECT v INTO gv FROM lv_gate_{table} WHERE id = 1 FOR UPDATE; \
                     IF gv = 0 THEN \
                     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'lazyview test: the gate is closed'; \
                     END IF; \
                     END"
                ),
            )
            .await?;
        }
        let log = eng.log_path.clone();
        let logged = |view: &str, target: &str, what: &str, from: u64| -> usize {
            let text = std::fs::read(&log).unwrap_or_default();
            let tail =
                String::from_utf8_lossy(&text[(from as usize).min(text.len())..]).into_owned();
            let (v, t) = (format!("view={view} "), format!("target={target}"));
            tail.lines()
                .filter(|l| l.contains(what) && l.contains(&v) && l.contains(&t))
                .count()
        };
        // Engine writes of `view` held at its gate on `on`. The process list names either the
        // INSERT or the statement of its trigger that waits (servers differ), so both are matched;
        // the INSERT by its schema, as the source's own table has the same name.
        async fn at_gate(on: &Db, view: &str) -> Result<usize> {
            let mut p = on.conn().await?;
            let filter = format!(
                "(INFO LIKE 'INSERT%{}%{view}%' OR INFO LIKE '%FROM lv_gate_{view} %')",
                on.name
            );
            let n = session_ids(&mut p, &filter).await?.len();
            p.disconnect().await.ok();
            Ok(n)
        }
        // The fold's state-row write held by the test's lock.
        async fn state_held(on: &Db) -> Result<usize> {
            let mut p = on.conn().await?;
            let filter = format!(
                "DB = '{}' AND INFO LIKE 'INSERT INTO `_lazyview_state`%'",
                on.name
            );
            let n = session_ids(&mut p, &filter).await?.len();
            p.disconnect().await.ok();
            Ok(n)
        }
        let state_at = |name: &'static str| {
            let db = db.clone();
            async move {
                let mut p = db.conn().await?;
                let at = scalar(
                    &mut p,
                    &format!(
                        "SELECT CAST(refreshed_at AS CHAR) FROM _lazyview_state \
                         WHERE name = '{name}' AND target = 'source'"
                    ),
                )
                .await?;
                p.disconnect().await.ok();
                anyhow::Ok(at)
            }
        };

        // 1. Both gates refuse: one change per view, and each target lags.
        let hold_touched = state_at("r_lag_hold").await?;
        exec_all(
            &mut c,
            &[
                "START TRANSACTION",
                "INSERT INTO orders VALUES (20, 2, 'new')",
                "INSERT INTO payments VALUES (200, 20, 3.00)",
                "UPDATE customers SET name = 'ann2' WHERE id = 1",
                "COMMIT",
            ],
        )
        .await?;
        wait_until("each target's first write refused", t_ready(), || {
            let n = logged("r_lag_units", "t1", "write failed", 0)
                * logged("r_lag_hold", "t2", "write failed", 0);
            async move { Ok(n > 0) }
        })
        .await?;
        // 2. Both gates hold instead: each target's next retry waits at its gate.
        exec_all(
            &mut g1,
            &[
                "START TRANSACTION",
                "UPDATE lv_gate_r_lag_units SET v = 1 WHERE id = 1",
            ],
        )
        .await?;
        exec_all(
            &mut g2,
            &[
                "START TRANSACTION",
                "UPDATE lv_gate_r_lag_hold SET v = 1 WHERE id = 1",
            ],
        )
        .await?;
        let (t1r, t2r) = (&t1, &t2);
        wait_until("a retry held at each gate", t_ready(), || async move {
            Ok(at_gate(t1r, "r_lag_units").await? >= 1 && at_gate(t2r, "r_lag_hold").await? >= 1)
        })
        .await?;
        // 3. While `t1`'s retry is held, a second unit is owed to it (the source's table has it).
        exec_all(
            &mut c,
            &[
                "START TRANSACTION",
                "INSERT INTO orders VALUES (21, 2, 'new')",
                "INSERT INTO payments VALUES (210, 21, 4.00)",
                "COMMIT",
            ],
        )
        .await?;
        let dbr = &db;
        wait_until("the second unit on the source", t_ready(), || async move {
            let mut p = dbr.conn().await?;
            let n = scalar_i64(&mut p, "SELECT COUNT(*) FROM r_lag_units WHERE id = 21").await?;
            p.disconnect().await.ok();
            Ok(n == 1)
        })
        .await?;
        // 4. The periodic `refreshed_at` touch of `r_lag_hold` is behind us, so the only write to
        // its state row from here on is the catch-up's.
        wait_until("r_lag_hold's refreshed_at touched", t_ready(), || {
            let (now, before) = (state_at("r_lag_hold"), hold_touched.clone());
            async move { Ok(now.await? != before) }
        })
        .await?;
        let mut ls = db.conn().await?;
        exec(&mut ls, "START TRANSACTION").await?;
        exec(
            &mut ls,
            "SELECT name FROM _lazyview_state WHERE name = 'r_lag_hold' AND target = 't2' FOR UPDATE",
        )
        .await?;
        // 5. `t2`'s retry goes through; the fold of its catch-up waits on the state row.
        exec(&mut g2, "COMMIT").await?;
        wait_until("t2's catch-up held in its fold", t_ready(), || async move {
            Ok(state_held(dbr).await? == 1)
        })
        .await?;
        // 6. Inside that fold, `t1`'s held retry goes through and ends.
        exec(&mut g1, "COMMIT").await?;
        wait_until("t1's held retry written", t_ready(), || async move {
            let mut p = t1r.conn().await?;
            let n = scalar_i64(&mut p, "SELECT COUNT(*) FROM r_lag_units WHERE id = 20").await?;
            p.disconnect().await.ok();
            Ok(n == 1 && at_gate(t1r, "r_lag_units").await? == 0)
        })
        .await?;
        // 7. Hold `t1` again, then let the fold finish: every retry of `t1` claimed from now on
        // waits at the gate after its read.
        exec(&mut g1, "START TRANSACTION").await?;
        exec(
            &mut g1,
            "SELECT v FROM lv_gate_r_lag_units WHERE id = 1 FOR UPDATE",
        )
        .await?;
        let mark = std::fs::metadata(&log)?.len();
        exec(&mut ls, "COMMIT").await?;
        // The held retry's result folds (the group still owes the second unit), and a retry
        // claimed after it is held; every claim since the mark is held at the gate.
        wait_until("each claim of t1 held at its gate", t_ready(), || {
            let text = std::fs::read(&log).unwrap_or_default();
            let tail =
                String::from_utf8_lossy(&text[(mark as usize).min(text.len())..]).into_owned();
            let mine: Vec<&str> = tail
                .lines()
                .filter(|l| l.contains("view=r_lag_units ") && l.contains("target=t1"))
                .collect();
            let fold = mine
                .iter()
                .position(|l| l.contains("lag retry wrote its claim"));
            let after =
                fold.is_some_and(|f| mine[f..].iter().any(|l| l.contains("lag refresh claimed")));
            let claims = mine
                .iter()
                .filter(|l| l.contains("lag refresh claimed"))
                .count();
            async move { Ok(after && at_gate(t1r, "r_lag_units").await? == claims) }
        })
        .await?;
        let claims = logged("r_lag_units", "t1", "lag refresh claimed", mark);
        exec(&mut g1, "COMMIT").await?;
        verify_targets(
            &ctx,
            &mut eng,
            &db,
            &targets,
            &HashMap::new(),
            &specs,
            &format!("after the catch-up ({claims} claim(s) of t1 since the held fold)"),
        )
        .await?;
        eng.stop().await
    });
}

// Helpers of the mv-level JSON unit tests (they drive `mv::apply` directly).
