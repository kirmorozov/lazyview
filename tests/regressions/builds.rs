use super::*;

/// A definition that plans pathologically slowly stalled the binlog loop (every view) and, past
/// `LAZYVIEW_LIVENESS_STALL_SECS`, `/livez` restarted the process, which planned it again.
/// Planning runs off the loop within `LAZYVIEW_PLAN_TIMEOUT_MS`: the definition goes into error
/// in time, the other views keep being maintained while the abandoned planner still runs, and it
/// is not planned again until it changes (the hook `LAZYVIEW_TEST_PLAN_DELAY` makes it slow).
#[test]
fn reg_slow_planning_is_bounded() {
    scenario("reg_plan_budget", |ctx| async move {
        use std::time::Instant;
        let mut specs = vec![ViewSpec::new(
            "r_fast",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let ov = [
            ("LAZYVIEW_PLAN_TIMEOUT_MS", "1000"),
            ("LAZYVIEW_TEST_PLAN_DELAY", "/*slow*/:12000"),
        ];
        let (db, mut eng) = setup(&ctx, "reg_plan_budget", &[], &specs, &ov).await?;
        let mut c = db.conn().await?;
        let t = Instant::now();
        exec(
            &mut c,
            "INSERT INTO _lazyview (name, definition) VALUES ('r_slow', 'SELECT id FROM orders /*slow*/')",
        )
        .await?;
        let row = wait_state(&mut c, &mut eng, "r_slow", "error", Duration::from_secs(8)).await?;
        ensure!(
            row.last_error.as_deref()
                == Some("planning took longer than 1 s; simplify the definition or raise LAZYVIEW_PLAN_TIMEOUT_MS"),
            "r_slow: {row:?}"
        );
        // While the abandoned planner still sleeps, r_fast follows its source promptly.
        let t1 = Instant::now();
        exec(&mut c, "UPDATE orders SET status = 'held' WHERE id = 11").await?;
        loop {
            let s: Option<String> =
                typed_first(&mut c, "SELECT status FROM r_fast WHERE id = 11").await?;
            if s.as_deref() == Some("held") {
                break;
            }
            ensure!(
                t1.elapsed() < Duration::from_secs(3),
                "r_fast not refreshed within 3 s while a planner is stuck"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(
            t.elapsed() < Duration::from_secs(11),
            "the stuck planner held the engine up ({:?})",
            t.elapsed()
        );
        // A change to a table it reads re-plans views in error, but not this one.
        exec(&mut c, "ALTER TABLE orders ADD COLUMN extra INT NULL").await?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after ALTER").await?;
        // Keep the fixed sleep: nothing must happen in this window — a re-plan of the timed-out
        // view would add a second timeout, and there is no value to poll for.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let n = eng.snapshot().plans_timed_out;
        ensure!(n == 1, "planned again after timing out: {n} timeouts");
        // A new definition is planned (and built) again.
        exec(
            &mut c,
            "UPDATE _lazyview SET definition = 'SELECT id FROM orders' WHERE name = 'r_slow'",
        )
        .await?;
        specs.push(ViewSpec::new(
            "r_slow",
            Kind::Incremental,
            "SELECT id FROM orders",
        ));
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after the definition changed").await?;
        eng.stop().await
    });
}

/// A building version kept its buffered changes only up to max(10 × LAZYVIEW_BATCH_MAX_KEYS,
/// 100 000) keys and traded them for a rebuild after the swap (owner rule: an incremental view
/// never falls back to a rebuild). It keeps them now; past the byte budget of building buffers
/// the binlog reader waits for the build instead, counted in `binlog_backpressure`. The hooks
/// make the build long (`LAZYVIEW_TEST_BUILD_DELAY`) and the budget small
/// (`LAZYVIEW_TEST_BUILD_HOLD_BYTES`: 1 700 000 bytes, about 106 000 integer ids).
#[test]
fn reg_long_build_keeps_its_keys() {
    scenario("reg_build_keys", |ctx| async move {
        let nums: Vec<String> = (0..400).map(|i| format!("({i})")).collect();
        let ddl = [
            "CREATE TABLE items (id INT AUTO_INCREMENT PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)"
                .to_string(),
            "CREATE TABLE nums (n INT PRIMARY KEY)".to_string(),
            format!("INSERT INTO nums VALUES {}", nums.join(",")),
        ];
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let mut specs = vec![ViewSpec::new(
            "r_fast",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let ov = [
            ("LAZYVIEW_TEST_BUILD_DELAY", "/*long_build*/:20000"),
            ("LAZYVIEW_TEST_BUILD_HOLD_BYTES", "1700000"),
        ];
        let (db, mut eng) = setup(&ctx, "reg_build_keys", &ddl, &specs, &ov).await?;
        let mut c = db.conn().await?;
        let before = eng.snapshot();
        let items = ViewSpec::new(
            "r_items",
            Kind::Incremental,
            "SELECT id, grp, val FROM items /*long_build*/",
        );
        add_definition(&mut c, &items.definition, None).await?;
        wait_state(
            &mut c,
            &mut eng,
            "r_items",
            "building",
            Duration::from_secs(10),
        )
        .await?;
        // 112 000 new ids while the build sleeps: more than the old cap of 100 000 keys, and
        // past the budget.
        exec(
            &mut c,
            "INSERT INTO items (grp, val) SELECT x.n, y.n FROM nums x JOIN nums y WHERE y.n < 280",
        )
        .await?;
        let t = std::time::Instant::now();
        while eng.snapshot().since(&before).rows_seen < 112_000 {
            ensure!(
                t.elapsed() < Duration::from_secs(15),
                "the new rows were not read: {}",
                fmt_snapshot(&eng.snapshot().since(&before))
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let states = view_state(&mut c, "r_items").await?;
        ensure!(
            states.iter().all(|s| s.state == "building"),
            "the build ended before its changes were read (raise LAZYVIEW_TEST_BUILD_DELAY): {states:?}"
        );
        // Committed while the reader is held: read once the build is applied.
        exec_all(
            &mut c,
            &[
                "UPDATE items SET val = val + 1 WHERE id <= 10",
                "UPDATE orders SET status = 'held' WHERE id = 11",
            ],
        )
        .await?;
        specs.push(items);
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after the long build").await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 1 && d.binlog_backpressure >= 1,
            "expected one build (no rebuild after the swap) and one hold: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// Server canary: MariaDB (10.11, 13.0 and 13.2 at least) returns a stale result for a
/// re-executed prepared statement with `NOT IN (subquery)` once the subquery's result changes
/// between empty and non-empty: it keeps answering as for the empty subquery (rows with a NULL
/// `ref` stay in). lazyview prepares its source reads afresh on MariaDB (`mv::exec_source`), so
/// such reads are right on every flavor, and a view with this predicate stays equal to its
/// oracle through the flips. The cached statement's answer is only noted (it tells whether the
/// server still has the bug); a MySQL-family server must answer it correctly.
#[test]
fn reg_not_in_prepared_statement_canary() {
    scenario("reg_nin_ps", |ctx| async move {
        use lazyview::mv::exec_source;
        let db = ctx.new_db("reg_nin_ps").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE c (id INT PRIMARY KEY, ref INT NULL)",
                "CREATE TABLE o (id INT PRIMARY KEY, customer_id INT NULL)",
                "CREATE TABLE p (id INT PRIMARY KEY, order_id INT NULL, amount DECIMAL(10,2) NULL)",
                "INSERT INTO c VALUES (1,2),(2,NULL),(3,1),(4,4),(5,NULL)",
                "INSERT INTO o VALUES (20,2)",
                "INSERT INTO p VALUES (200,20,1.00)",
            ],
        )
        .await?;
        let sql = "SELECT c.id FROM c WHERE (c.ref NOT IN (SELECT o.customer_id FROM o, p \
                   WHERE p.order_id = o.id AND p.amount > 400)) AND c.id IN (1,2,5) ORDER BY c.id";
        let mut cached = db.conn().await?;
        let mut fresh = db.conn().await?;
        let mut stale = Vec::new();
        for (amount, expected) in [
            ("1.00", vec![1i64, 2, 5]),
            ("401.00", vec![]),
            ("1.00", vec![1, 2, 5]),
            ("401.00", vec![]),
        ] {
            exec(
                &mut c,
                &format!("UPDATE p SET amount = {amount} WHERE id = 200"),
            )
            .await?;
            let got: Vec<i64> =
                exec_source(&mut fresh, rename_for(&db, sql)?, Vec::new(), true).await?;
            ensure!(
                got == expected,
                "amount {amount}: a freshly prepared read gave {got:?}, expected {expected:?}"
            );
            let got: Vec<i64> =
                exec_source(&mut cached, rename_for(&db, sql)?, Vec::new(), false).await?;
            if got != expected {
                stale.push(format!("amount {amount}: {got:?} instead of {expected:?}"));
            }
        }
        ensure!(
            stale.is_empty() || ctx.servers[0].is_mariadb(),
            "re-executed prepared statement gave stale results: {stale:?}"
        );
        ctx.log(if stale.is_empty() {
            "re-executed prepared NOT IN statement: correct".to_string()
        } else {
            format!("re-executed prepared NOT IN statement: STALE (MariaDB bug present): {stale:?}")
        });
        // lazyview over the same predicate, through the same flips.
        let specs = vec![ViewSpec::new(
            "r_nin",
            Kind::Full,
            "SELECT c.id, c.ref FROM c WHERE c.ref NOT IN (SELECT o.customer_id FROM o, p \
             WHERE p.order_id = o.id AND p.amount > 400)",
        )];
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        for amount in ["1.00", "401.00", "1.00"] {
            exec(
                &mut c,
                &format!("UPDATE p SET amount = {amount} WHERE id = 200"),
            )
            .await?;
            verify(&ctx, &mut eng, &db, &specs, &format!("amount {amount}")).await?;
        }
        eng.stop().await
    });
}

/// G6: a view grouped by a NULLable column with a UNIQUE index. A flush that refreshes only the
/// NULL group read it with a prepared `(code) <=> ?` bound to NULL, which MySQL-family servers
/// (8.4, 9.x, Percona) run as a one-row unique lookup: the NULL group lost all but one of its
/// rows. NULL unit components are `IS NULL` literals now; the view (and a control view over the
/// same rows without the index) stays equal to its oracle and never falls back to a rebuild. The
/// prepared `<=> ?` answer is only noted (it tells whether the server still has the bug); a
/// MariaDB server must answer it correctly, and `IS NULL` must be right on every flavor.
#[test]
fn reg_null_unit_on_nullable_unique_index() {
    scenario("reg_null_uq", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_nopk",
                Kind::Incremental,
                "SELECT n.code, SUM(n.v) AS s FROM s_nopk n GROUP BY n.code",
            ),
            ViewSpec::new(
                "r_nokey",
                Kind::Incremental,
                "SELECT n.code, SUM(n.v) AS s FROM s_nokey n GROUP BY n.code",
            ),
        ];
        let (db, mut eng) = setup(
            &ctx,
            "reg_null_uq",
            &[
                "CREATE TABLE s_nopk (code INT NULL, v INT NULL, UNIQUE KEY u_code (code))",
                "CREATE TABLE s_nokey (code INT NULL, v INT NULL)",
                "INSERT INTO s_nopk VALUES (NULL,1),(NULL,2),(5,3),(7,4)",
                "INSERT INTO s_nokey VALUES (NULL,1),(NULL,2),(5,3),(7,4)",
            ],
            &specs,
            &[],
        )
        .await?;
        let mut c = db.conn().await?;
        // Server canary.
        let null = (mysql_async::Value::NULL,);
        let prepared: Option<i64> = typed_exec_first(
            &mut c,
            "SELECT COUNT(*) FROM s_nopk WHERE (code) <=> ?",
            null,
        )
        .await?;
        let literal: Option<i64> = typed_exec_first(
            &mut c,
            "SELECT COUNT(*) FROM s_nopk WHERE (code) IS NULL",
            (),
        )
        .await?;
        ensure!(
            literal == Some(2),
            "prepared `IS NULL` found {literal:?} of the 2 NULL rows"
        );
        ensure!(
            prepared == Some(2) || !ctx.servers[0].is_mariadb(),
            "MariaDB: prepared `<=> ?` bound to NULL found {prepared:?} of the 2 NULL rows"
        );
        ctx.log(if prepared == Some(2) {
            "prepared `(code) <=> ?` bound to NULL on a UNIQUE index: correct".to_string()
        } else {
            format!(
                "prepared `(code) <=> ?` bound to NULL on a UNIQUE index: {prepared:?} of 2 rows \
                 (MySQL-family bug present)"
            )
        });
        let mode: Option<String> = typed_first(
            &mut c,
            "SELECT `mode` FROM _lazyview_state WHERE name = 'r_nopk'",
        )
        .await?;
        ensure!(
            mode.as_deref() == Some("incremental"),
            "r_nopk is not maintained incrementally: {mode:?}"
        );
        let both = |stmt: &str| format!("{stmt}; {}", stmt.replace("s_nopk", "s_nokey"));
        for (what, stmt) in [
            (
                "NULL-key rows: update one, delete the other",
                "UPDATE s_nopk SET v = 9 WHERE code IS NULL AND v = 1; \
                 DELETE FROM s_nopk WHERE code IS NULL AND v = 2",
            ),
            (
                "NULL key becomes a value",
                "UPDATE s_nopk SET code = 1 WHERE v = 9",
            ),
            (
                "back to NULL plus a second NULL row",
                "UPDATE s_nopk SET code = NULL WHERE v = 9; INSERT INTO s_nopk VALUES (NULL, 6)",
            ),
            ("a third NULL row", "INSERT INTO s_nopk VALUES (NULL, 100)"),
            (
                "update inside the NULL group only",
                "UPDATE s_nopk SET v = v + 1 WHERE code IS NULL AND v = 6",
            ),
            (
                "delete inside the NULL group only",
                "DELETE FROM s_nopk WHERE code IS NULL AND v = 100",
            ),
        ] {
            let before = eng.snapshot();
            for s in both(stmt).split("; ") {
                exec(&mut c, s).await?;
            }
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                d.full_rebuilds == 0,
                "{what}: fell back to a rebuild: {}",
                fmt_snapshot(&d)
            );
        }
        eng.stop().await
    });
}
