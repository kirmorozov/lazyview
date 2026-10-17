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
        let specs = vec![ViewSpec::new(
            "r_bits",
            Kind::Incremental,
            "SELECT id, b, b2 FROM bits",
        )];
        let ddl = [
            "CREATE TABLE bits (id INT PRIMARY KEY, b BIT(8) NULL, b2 BIT(1) NULL)",
            "INSERT INTO bits VALUES (1, b'11111111', b'1'), (2, b'0', b'0'), (3, NULL, NULL), (4, b'1010', b'1')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_bit", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "UPDATE bits SET b = b'11111111' WHERE id = 2",
                "INSERT INTO bits VALUES (5, b'10000000', b'0')",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "BIT values after changes").await?;
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
