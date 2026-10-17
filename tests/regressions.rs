//! Small deterministic reproductions of engine bugs found by the randomized suites (each runs on
//! every flavor in `LAZYVIEW_TEST_DSNS`, takes seconds, and names the bug it pins down).

mod common;

use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use common::*;

/// Tiny schema used by most reproductions.
const DDL: &[&str] = &[
    "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(30) NOT NULL, note VARCHAR(30) NULL)",
    "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NULL, status VARCHAR(10) NOT NULL)",
    "CREATE TABLE products (id INT PRIMARY KEY, sku VARCHAR(10) NOT NULL, price DECIMAL(10,2) NOT NULL, active TINYINT NOT NULL DEFAULT 1)",
    "INSERT INTO customers VALUES (1, 'ann', NULL), (2, 'bob', NULL), (3, 'cid', NULL)",
    "INSERT INTO orders VALUES (10, 1, 'new'), (11, 1, 'paid'), (12, 2, 'new'), (13, 3, 'paid')",
    "INSERT INTO products VALUES (1, 'a', 5.00, 1), (2, 'b', 50.00, 1), (3, 'c', 20.00, 1), (4, 'd', 99.00, 0), (5, 'e', 70.00, 1), (6, 'f', 1.00, 1)",
];

async fn setup(
    ctx: &Ctx,
    tag: &str,
    extra_ddl: &[&str],
    specs: &[ViewSpec],
    ov: &[(&str, &str)],
) -> Result<(Db, Engine)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    for s in DDL.iter().chain(extra_ddl) {
        exec(&mut c, s).await?;
    }
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, specs).await?;
    let mut eng = ctx.start_engine(&db, ov)?;
    wait_ready(&mut c, &mut eng, &names(specs), t_ready()).await?;
    verify(ctx, &mut eng, &db, specs, "initial").await?;
    Ok((db, eng))
}

fn names(specs: &[ViewSpec]) -> Vec<&str> {
    specs.iter().map(|s| s.name.as_str()).collect()
}

async fn run(c: &mut mysql_async::Conn, stmts: &[&str]) -> Result<()> {
    for s in stmts {
        exec(c, s).await?;
    }
    Ok(())
}

fn join_view() -> ViewSpec {
    ViewSpec::new(
        "r_order_customer",
        Kind::Incremental,
        "SELECT o.id, o.status, c.name FROM orders o JOIN customers c ON c.id = o.customer_id",
    )
}

/// PK update / delete of a joined, non-anchor table must remove the rows that joined to the
/// old key (seen as stale rows in v_line_details / v_order_customer_region).
#[test]
fn reg_join_partner_pk_update() {
    scenario("reg_join_pk", |ctx| async move {
        let specs = vec![join_view()];
        let (db, mut eng) = setup(&ctx, "reg_join_pk", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        run(&mut c, &["UPDATE customers SET id = 100 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer PK 1 → 100 (orders 10, 11 must vanish)",
        )
        .await?;
        run(&mut c, &["UPDATE customers SET id = 1 WHERE id = 100"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer PK 100 → 1 (orders come back)",
        )
        .await?;
        run(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "customer 2 deleted").await?;
        eng.stop().await
    });
}

/// Anchor two joins away (composite-PK `items`): when a parent row changes its join key to a
/// value that no longer joins (or the grandparent row is deleted), the units can only be found
/// through the before-image — found as stale v_line_details rows after
/// `UPDATE orders SET customer_id = <deleted customer> WHERE id = 166`.
#[test]
fn reg_before_image_lookup_when_after_image_no_longer_joins() {
    scenario("reg_before_img", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_item_customer",
            Kind::Incremental,
            "SELECT i.order_id, i.line_no, i.qty, c.name FROM items i JOIN orders o ON o.id = i.order_id JOIN customers c ON c.id = o.customer_id",
        )];
        let ddl = [
            "CREATE TABLE items (order_id INT NOT NULL, line_no INT NOT NULL, qty INT NOT NULL, PRIMARY KEY (order_id, line_no))",
            "INSERT INTO items VALUES (10, 1, 1), (10, 2, 2), (11, 3, 3), (12, 4, 4), (13, 5, 5)",
        ];
        let (db, mut eng) = setup(&ctx, "reg_before_img", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &["UPDATE orders SET customer_id = 999 WHERE id = 10"],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "order 10 → non-existent customer (its items must vanish)",
        )
        .await?;
        run(&mut c, &["UPDATE orders SET customer_id = 2 WHERE id = 10"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "order 10 → customer 2 (items back)",
        )
        .await?;
        run(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer 2 deleted (items of orders 10, 12 vanish)",
        )
        .await?;
        run(
            &mut c,
            &["UPDATE orders SET customer_id = NULL WHERE id = 13"],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "order 13 → NULL customer").await?;
        run(&mut c, &["UPDATE customers SET id = 50 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer PK 1 → 50 (items of order 11 vanish)",
        )
        .await?;
        eng.stop().await
    });
}

/// The brutal-run case exactly: the grandparent (`customers`, two joins from the composite-PK
/// anchor) is also the parent of a cascading FK, and its PK is updated (`customers 1 → 67` left
/// v_line_details rows of order 166 stale).
#[test]
fn reg_before_image_lookup_fk_parent_two_joins_away() {
    scenario("reg_before_img_fk", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_item_customer",
            Kind::Incremental,
            "SELECT i.order_id, i.line_no, i.qty, c.name FROM items i JOIN orders o ON o.id = i.order_id JOIN customers c ON c.id = o.customer_id",
        )];
        let ddl = [
            "CREATE TABLE items (order_id INT NOT NULL, line_no INT NOT NULL, qty INT NOT NULL, PRIMARY KEY (order_id, line_no))",
            "INSERT INTO items VALUES (10, 1, 1), (10, 2, 2), (11, 3, 3), (12, 4, 4), (13, 5, 5)",
            "CREATE TABLE addresses (customer_id INT NOT NULL, seq INT NOT NULL, city VARCHAR(10) NOT NULL, PRIMARY KEY (customer_id, seq),
             FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "INSERT INTO addresses VALUES (1, 1, 'x'), (2, 1, 'y')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_before_img_fk", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        run(&mut c, &["UPDATE customers SET id = 67 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "FK-parent customer PK 1 → 67 (items of orders 10, 11 vanish)",
        )
        .await?;
        run(
            &mut c,
            &["UPDATE orders SET customer_id = 59 WHERE id = 10"],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "order 10 → non-existent customer",
        )
        .await?;
        run(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "FK-parent customer 2 deleted").await?;
        eng.stop().await
    });
}

/// Same, when the joined table is also the parent of a cascading foreign key.
#[test]
fn reg_join_partner_pk_update_fk_parent() {
    scenario("reg_join_pk_fk", |ctx| async move {
        let specs = vec![join_view()];
        let fk = [
            "CREATE TABLE addresses (customer_id INT NOT NULL, seq INT NOT NULL, city VARCHAR(10) NOT NULL, PRIMARY KEY (customer_id, seq),
             FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "INSERT INTO addresses VALUES (1, 1, 'x'), (2, 1, 'y')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_join_pk_fk", &fk, &specs, &[]).await?;
        let mut c = db.conn().await?;
        run(&mut c, &["UPDATE customers SET id = 100 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "FK parent customer PK 1 → 100 (orders 10, 11 must vanish)",
        )
        .await?;
        run(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "FK parent customer 2 deleted").await?;
        eng.stop().await
    });
}

/// Full-refresh view with ORDER BY … LIMIT must keep the top rows (v_top_products held the
/// first rows by id instead).
#[test]
fn reg_order_by_limit() {
    scenario("reg_limit", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_top",
            Kind::Full,
            "SELECT id, sku, price FROM products WHERE active = 1 ORDER BY price DESC, id LIMIT 3",
        )];
        let (db, mut eng) = setup(&ctx, "reg_limit", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        run(&mut c, &["UPDATE products SET price = 1000 WHERE id = 6"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "new top product").await?;
        eng.stop().await
    });
}

/// `CREATE VIEW v (a, b, c) AS …`: the MV columns must be named by the column list.
#[test]
fn reg_view_column_list() {
    scenario("reg_cols", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_cols",
                Kind::Incremental,
                "SELECT id, sku, price FROM products",
            )
            .noisy("", Some("pid, psku, pprice"), ""),
            ViewSpec::new(
                "r_cols_noisy",
                Kind::Incremental,
                "SELECT id, sku FROM products WHERE active = 1",
            )
            .noisy(
                "OR REPLACE ALGORITHM=MERGE DEFINER=CURRENT_USER SQL SECURITY INVOKER",
                Some("k, s"),
                "WITH CHECK OPTION",
            ),
        ];
        let (_db, eng) = setup(&ctx, "reg_cols", &[], &specs, &[]).await?;
        eng.stop().await
    });
}

/// BIT(8) values must survive (b'11111111' arrived as 0x20 / 0x00 on MySQL / Percona).
#[test]
fn reg_bit_column() {
    scenario("reg_bit", |ctx| async move {
        let specs = vec![
            ViewSpec::new("r_bits", Kind::Incremental, "SELECT id, b, b2 FROM bits"),
            ViewSpec::new(
                "r_bits_wide",
                Kind::Incremental,
                "SELECT id, m, blb, txt, b FROM bits",
            ),
        ];
        let ddl = [
            "CREATE TABLE bits (id BIGINT UNSIGNED PRIMARY KEY, m MEDIUMINT NULL, blb BLOB NULL, txt TEXT NULL, b BIT(8) NULL, b2 BIT(1) NULL)",
            "INSERT INTO bits VALUES (1, 1, X'00010203', 'x', b'11111111', b'1'), (2, NULL, NULL, '', b'0', b'0'), (3, -8388608, '', NULL, NULL, NULL), (4, 0, X'FF', 'multi\nline', b'1010', b'1')",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "reg_bit",
            &ddl,
            &specs,
            &[
                ("LAZYVIEW_QUERY_CHUNK", "2"),
                ("LAZYVIEW_BATCH_WINDOW_MS", "0"),
            ],
        )
        .await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "UPDATE bits SET b = b'11111111' WHERE id = 2",
                "INSERT INTO bits VALUES (5, 5, NULL, 't', b'10000000', b'0')",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "BIT values after changes").await?;
        // Other columns change, BIT stays 0xff: the refresh re-reads and re-writes the row.
        run(
            &mut c,
            &[
                "UPDATE bits SET m = m + 1, txt = CONCAT(COALESCE(txt, ''), '!')",
                "UPDATE bits SET blb = X'00' WHERE id IN (1, 2)",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "other columns changed, BIT unchanged",
        )
        .await?;
        run(
            &mut c,
            &[
                "REPLACE INTO bits VALUES (1, 9, NULL, 'r', b'11111111', b'1')",
                "UPDATE bits SET id = 77 WHERE id = 4",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "REPLACE / PK change with BIT values",
        )
        .await?;
        eng.stop().await
    });
}

/// A join partner whose key changes only in letter case (case-insensitive collation) must be
/// re-read: the MV kept 'ABC' after the partner changed to 'abc' (seen on MariaDB 10.11).
#[test]
fn reg_case_only_change_of_join_partner() {
    scenario("reg_ci", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_ci",
            Kind::Incremental,
            "SELECT t.id, t.k, r.id AS rid, r.k AS rk FROM tk t JOIN rk r ON r.k = t.k",
        )];
        let ddl = [
            "CREATE TABLE tk (id INT PRIMARY KEY, k VARCHAR(10) COLLATE utf8mb4_unicode_ci NULL)",
            "CREATE TABLE rk (id INT PRIMARY KEY, k VARCHAR(10) COLLATE utf8mb4_unicode_ci NULL)",
            "INSERT INTO tk VALUES (1, 'abc'), (2, 'ABC'), (3, 'xyz')",
            "INSERT INTO rk VALUES (1, 'ABC'), (2, 'xyz')",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "reg_ci",
            &ddl,
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        run(&mut c, &["UPDATE rk SET k = 'abc' WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "partner key 'ABC' → 'abc'").await?;
        run(&mut c, &["UPDATE tk SET k = 'Abc' WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "anchor key 'abc' → 'Abc'").await?;
        eng.stop().await
    });
}

/// Updating a column no view reads, on the parent of a cascading FK, must cost nothing (it
/// cannot cascade); it caused two full rebuilds of the child views.
#[test]
fn reg_unreferenced_update_on_fk_parent() {
    scenario("reg_fk_unref", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_addr",
                Kind::Incremental,
                "SELECT customer_id, seq, city FROM addresses",
            ),
            ViewSpec::new(
                "r_addr_by_city",
                Kind::Incremental,
                "SELECT city, COUNT(*) AS n FROM addresses GROUP BY city",
            ),
        ];
        let fk = [
            "CREATE TABLE addresses (customer_id INT NOT NULL, seq INT NOT NULL, city VARCHAR(10) NOT NULL, PRIMARY KEY (customer_id, seq),
             FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "INSERT INTO addresses VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 1, 'x')",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "reg_fk_unref",
            &fk,
            &specs,
            &[("LAZYVIEW_POSITION_PERSIST_MS", "600000")],
        )
        .await?;
        let mut c = db.conn().await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        run(
            &mut c,
            &[
                "UPDATE customers SET note = 'touched'",
                "UPDATE customers SET name = UPPER(name) WHERE id = 2",
            ],
        )
        .await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0 && d.source_queries == 0 && d.mv_queries == 0,
            "non-key update of a cascading-FK parent caused work: {}",
            fmt_snapshot(&d)
        );
        // A real cascade must still be applied.
        run(
            &mut c,
            &[
                "UPDATE customers SET id = 30 WHERE id = 3",
                "DELETE FROM customers WHERE id = 1",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after cascading PK update + delete",
        )
        .await?;
        eng.stop().await
    });
}

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
        run(&mut c, &["INSERT INTO orders VALUES (14, 2, 'new')"]).await?;
        verify(&ctx, &mut eng, &db, &more, "chained view after writes").await?;
        eng.stop().await
    });
}

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
        run(&mut c, &["UPDATE customers SET name = 'zed' WHERE id = 3"]).await?;
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
        run(&mut c, &["INSERT INTO pay VALUES (3, 'cash', 'pending')"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "plain insert").await?;
        run(
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
        run(&mut c, &["XA COMMIT 'r1'"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "XA committed").await?;
        run(
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
        run(
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
        run(
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
            "SELECT DISTINCT sku FROM products WHERE active = 1",
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
            lazyview::analyze::definition_hash(&spec.definition)
        );
        exec(&mut c, &format!("CREATE TABLE `{probe}` (x INT)")).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        run(&mut c, &["UPDATE products SET sku = 'zz' WHERE id = 1"]).await?;
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
        run(
            &mut c,
            &["ALTER TABLE customers ADD COLUMN c0 INT NULL FIRST"],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "after ALTER of the FK parent").await?;
        run(
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
        run(&mut c, &["INSERT INTO orders VALUES (14, 2, 'new')"]).await?;
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
        run(&mut c, &["UPDATE customers SET name = 'zed' WHERE id = 1"]).await?;
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
                "SELECT DISTINCT status FROM orders",
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
        run(&mut c, &["UPDATE orders SET status = 'void' WHERE id = 10"]).await?;
        // Long enough for several position saves (and the incremental refresh).
        tokio::time::sleep(Duration::from_secs(2)).await;
        eng.crash().await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after crash and resume").await?;
        eng.stop().await
    });
}

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
        run(
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
        run(
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
        run(
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
        run(
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
            "SELECT DISTINCT status FROM orders",
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
        run(&mut c, &["UPDATE orders SET status = 'void' WHERE id = 10"]).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let before = eng.snapshot();
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
    run(&mut c, stmts).await?;
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
            run(&mut c, stmts).await?;
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

/// E6, grouped: a long catch-up remembers more images than the cap allows; the view is rebuilt
/// online instead (counted in `lookback_overflows`) and ends up correct.
#[test]
fn reg_grouped_chain_catch_up_hits_the_cap() {
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
            d.lookback_overflows > 0 && d.full_rebuilds > 0,
            "expected the cap to rebuild the view: {}",
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
            run(&mut c, stmts).await?;
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
