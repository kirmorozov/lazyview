//! Small deterministic reproductions of engine bugs found by the randomized suites (each runs on
//! every flavor in `LAZYVIEW_TEST_DSNS`, takes seconds, and names the bug it pins down).

mod common;

use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use common::*;
use lazyview::accumulate::UnitChunk;
use lazyview::mv::{Layout, MvCol};
use mysql_async::prelude::Queryable;
use mysql_async::Value;

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
            run(&mut c, stmts).await?;
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
/// `@@server_uuid`, MariaDB server id + GTID domain). Restored onto or failed over to another
/// server, the same file name may hold other events: that goes through
/// LAZYVIEW_ON_MISSING_BINLOG (fail by default, rebuild on request) instead of replaying them.
#[test]
fn reg_saved_position_from_another_server() {
    scenario("reg_identity", |ctx| async move {
        let specs = vec![join_view()];
        let (db, eng) = setup(&ctx, "reg_identity", &[], &specs, &[]).await?;
        eng.stop().await?;
        let mut c = db.conn().await?;
        let saved = scalar(&mut c, "SELECT server_identity FROM _lazyview_status").await?;
        ensure!(
            saved.as_deref().is_some_and(|s| !s.is_empty()),
            "no server identity saved: {saved:?}"
        );
        exec(
            &mut c,
            "UPDATE _lazyview_status SET server_identity = 'another-server'",
        )
        .await?;
        run(
            &mut c,
            &["UPDATE customers SET name = 'moved' WHERE id = 1"],
        )
        .await?;
        let eng = ctx.start_engine(&db, &[])?;
        match eng.wait_finished(Duration::from_secs(60)).await? {
            Err(e) if e.contains("another-server") => {}
            other => bail!("expected a failure naming the saved server, got {other:?}"),
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
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

// Helpers of the mv-level JSON unit tests (they drive `mv::apply` directly).

/// The MV layout as `mv::probe` builds it: column types from information_schema (`json`
/// on MySQL/Percona, `longtext` on MariaDB), the first `units` columns as the unit.
async fn probe(
    c: &mut mysql_async::Conn,
    schema: &str,
    table: &str,
    units: usize,
) -> Result<Layout> {
    let rows: Vec<(String, String, String, String)> = c
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(IS_NULLABLE AS CHAR) \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        .await?;
    let cols: Vec<MvCol> = rows
        .into_iter()
        .map(|(name, column_type, data_type, nullable)| MvCol {
            src_name: name.clone(),
            name,
            column_type,
            data_type: data_type.to_ascii_lowercase(),
            charset: None,
            collation: None,
            nullable: nullable == "YES",
        })
        .collect();
    Ok(Layout {
        n_visible: cols.len(),
        hidden: Vec::new(),
        unit_pos: (0..units).collect(),
        primary: false,
        lineage: Vec::new(),
        epoch: Vec::new(),
        cols,
    })
}

/// Rows of the view's query for the chunk's units (what `mv::read_source` does).
async fn rows(
    c: &mut mysql_async::Conn,
    layout: &Layout,
    select: &str,
    unit_exprs: &[&str],
    chunk: Option<&UnitChunk>,
) -> Result<Vec<Vec<Value>>> {
    let exprs: Vec<String> = unit_exprs.iter().map(|e| e.to_string()).collect();
    let (f, p) = match chunk {
        Some(ch) => ch.filter_typed(&exprs, &layout.unit_kinds()),
        None => ("TRUE".into(), Vec::new()),
    };
    let sql = select.replace("{F}", &f);
    let rows: Vec<mysql_async::Row> = c.exec(sql, p).await?;
    Ok(rows
        .into_iter()
        .map(|r| layout.normalize_source(r.unwrap()))
        .collect())
}

/// The MV must hold exactly the oracle's groups (JSON-equal keys, equal counts).
async fn check(
    c: &mut mysql_async::Conn,
    mv: &str,
    oracle: &str,
    on: &str,
    what: &str,
) -> Result<()> {
    let (a, b, both): (i64, i64, i64) = c
        .query_first(format!(
            "SELECT (SELECT COUNT(*) FROM {mv}), (SELECT COUNT(*) FROM ({oracle}) o), \
             (SELECT COUNT(*) FROM {mv} m JOIN ({oracle}) o ON {on} AND o.n = m.n)"
        ))
        .await?
        .context("counts")?;
    ensure!(
        a == b && b == both,
        "{what}: {mv} has {a} rows, the oracle {b}, {both} agree"
    );
    Ok(())
}

/// JSON unit keys (MySQL/Percona) must be matched as JSON values: the server groups JSON-equal
/// values (`{"a": 1}` with `{"a": 1.0}`) but compares a JSON column with a plain string
/// parameter as text, so the refresh filters (source read, MV read and the DELETE) never found a
/// dirty unit's rows and the MV went stale. Drives `mv::apply` directly with the filters the
/// engine uses: the planner still runs JSON group keys in full mode, so no engine path reaches
/// them yet. MariaDB (JSON is LONGTEXT) runs the plain path.
#[test]
fn reg_json_unit_filters_match_json_equal_groups() {
    scenario("reg_json_units", |ctx| async move {
        use lazyview::accumulate::UnitKind;
        use lazyview::datum::Datum;
        use lazyview::mv;
        use lazyview::Metrics;

        let db = ctx.new_db("reg_json_units").await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "CREATE TABLE jt (id INT PRIMARY KEY, g INT NULL, doc JSON NULL)",
                "CREATE TABLE mv_j (doc JSON NULL, n BIGINT NOT NULL)",
                "CREATE TABLE mv_gj (g INT NULL, doc JSON NULL, n BIGINT NOT NULL)",
                r#"INSERT INTO jt VALUES (1, 1, '{"a": 1}'), (2, 1, '{"a": 1.0}'), (3, 2, '{"b": [1, 2]}'), (4, 2, NULL), (5, NULL, '{"a": 1}')"#,
            ],
        )
        .await?;
        let is_json = |l: &Layout| l.unit_kinds().contains(&UnitKind::Json);
        let l_j = probe(&mut c, &db.name, "mv_j", 1).await?;
        let l_gj = probe(&mut c, &db.name, "mv_gj", 2).await?;
        ensure!(
            is_json(&l_j) == !ctx.servers[0].is_mariadb(),
            "JSON unit kind: {:?} (MariaDB stores JSON as LONGTEXT)",
            l_j.unit_kinds()
        );
        let metrics = Metrics::default();
        let j = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
        let sel_j = "SELECT doc, COUNT(*) AS n FROM jt WHERE {F} GROUP BY doc";
        let orc_j = "SELECT doc, COUNT(*) AS n FROM jt GROUP BY doc";
        let on_j = "o.doc <=> m.doc";
        let sel_gj = "SELECT g, doc, COUNT(*) AS n FROM jt WHERE {F} GROUP BY g, doc";
        let orc_gj = "SELECT g, doc, COUNT(*) AS n FROM jt GROUP BY g, doc";
        let on_gj = "o.g <=> m.g AND o.doc <=> m.doc";
        // Dirty keys as the engine sees them: on MySQL the binlog decoder renders the binary JSON
        // (serde_json: no spaces, floats with a fraction) and the server compares JSON values; on
        // MariaDB the text is stored as written and compared as text, so every text is its own
        // group and the images are the exact texts.
        let maria = ctx.servers[0].is_mariadb();
        let key = |mysql: &str, mariadb: &str| j(if maria { mariadb } else { mysql });
        // Initial fill of the single-column view (the whole view).
        let r = rows(&mut c, &l_j, sel_j, &["doc"], None).await?;
        mv::apply(&mut c, &db.name, "mv_j", &l_j, None, &r, 1000, &metrics).await?;
        check(&mut c, "mv_j", orc_j, on_j, "initial fill").await?;
        // Row 3 moves to the {"a": 1} group (a new text group on MariaDB).
        exec(&mut c, r#"UPDATE jt SET doc = '{"a": 1.00}' WHERE id = 3"#).await?;
        let chunk = UnitChunk::Keys(vec![
            Box::new([key(r#"{"b":[1,2]}"#, r#"{"b": [1, 2]}"#)]),
            Box::new([key(r#"{"a":1.0}"#, r#"{"a": 1.00}"#)]),
        ]);
        let r = rows(&mut c, &l_j, sel_j, &["doc"], Some(&chunk)).await?;
        ensure!(
            r.len() == 1,
            "source read of the two dirty JSON units found {} groups, expected 1",
            r.len()
        );
        mv::apply(
            &mut c,
            &db.name,
            "mv_j",
            &l_j,
            Some(&chunk),
            &r,
            1000,
            &metrics,
        )
        .await?;
        check(
            &mut c,
            "mv_j",
            orc_j,
            on_j,
            "update into a JSON-equal group",
        )
        .await?;
        // The {"a": 1} group disappears: the DELETE must find the MV row(s).
        exec(&mut c, "DELETE FROM jt WHERE id IN (1, 2, 3, 5)").await?;
        let chunk = UnitChunk::Keys(if maria {
            vec![
                Box::new([j(r#"{"a": 1}"#)]),
                Box::new([j(r#"{"a": 1.0}"#)]),
                Box::new([j(r#"{"a": 1.00}"#)]),
            ]
        } else {
            vec![Box::new([j(r#"{"a":1}"#)])]
        });
        let r = rows(&mut c, &l_j, sel_j, &["doc"], Some(&chunk)).await?;
        ensure!(
            r.is_empty(),
            "deleted JSON unit still has {} source groups",
            r.len()
        );
        mv::apply(
            &mut c,
            &db.name,
            "mv_j",
            &l_j,
            Some(&chunk),
            &r,
            1000,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_j", orc_j, on_j, "delete of a JSON group").await?;
        // Composite units with NULLs: plain tuples and the null-safe branch bind per position.
        let r = rows(&mut c, &l_gj, sel_gj, &["g", "doc"], None).await?;
        mv::apply(&mut c, &db.name, "mv_gj", &l_gj, None, &r, 1000, &metrics).await?;
        check(&mut c, "mv_gj", orc_gj, on_gj, "initial fill (composite)").await?;
        run(
            &mut c,
            &[
                r#"INSERT INTO jt VALUES (6, 1, '{"a": 1.0}'), (7, NULL, '{"a":1}'), (8, 3, NULL)"#,
                "UPDATE jt SET g = 3 WHERE id = 4",
            ],
        )
        .await?;
        let chunk = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), key(r#"{"a":1.0}"#, r#"{"a": 1.0}"#)]),
            Box::new([Datum::Int(1), key(r#"{"a":1}"#, r#"{"a": 1.0}"#)]),
            Box::new([Datum::Null, key(r#"{"a":1.0}"#, r#"{"a":1}"#)]),
            Box::new([Datum::Int(2), Datum::Null]),
            Box::new([Datum::Int(3), Datum::Null]),
        ]);
        let r = rows(&mut c, &l_gj, sel_gj, &["g", "doc"], Some(&chunk)).await?;
        ensure!(
            r.len() == 3,
            "source read of the composite JSON units found {} groups, expected 3",
            r.len()
        );
        mv::apply(
            &mut c,
            &db.name,
            "mv_gj",
            &l_gj,
            Some(&chunk),
            &r,
            1000,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_gj", orc_gj, on_gj, "composite units with NULLs").await?;
        Ok(())
    });
}

/// A view grouped by a JSON column stays equal to its oracle through inserts, updates and
/// deletes of JSON-equal and NULL values. The planner runs it in full mode today ("JSON group
/// key"); once it is maintained incrementally (typed unit filters, see
/// `reg_json_unit_filters_match_json_equal_groups`), no change may fall back to a rebuild.
#[test]
fn reg_json_group_key_view() {
    scenario("reg_json_view", |ctx| async move {
        use mysql_async::prelude::Queryable;
        let specs = vec![ViewSpec::new(
            "r_docs",
            Kind::Full,
            "SELECT doc, COUNT(*) AS n FROM jdocs GROUP BY doc",
        )];
        let (db, mut eng) = setup(
            &ctx,
            "reg_json_view",
            &[
                "CREATE TABLE jdocs (id INT PRIMARY KEY, doc JSON NULL)",
                r#"INSERT INTO jdocs VALUES (1, '{"a": 1}'), (2, '{"b": [1, 2]}'), (3, NULL)"#,
            ],
            &specs,
            &[],
        )
        .await?;
        let mut c = db.conn().await?;
        let mode: Option<String> = c
            .query_first("SELECT `mode` FROM _lazyview_state WHERE name = 'r_docs'")
            .await?;
        let incremental = mode.as_deref() == Some("incremental");
        for (what, stmt) in [
            (
                "JSON-equal inserts",
                r#"INSERT INTO jdocs VALUES (4, '{"a": 1.0}'), (5, '{"a": 1.00}'), (6, '{"b": [1.0, 2]}')"#,
            ),
            (
                "update into a JSON-equal group",
                r#"UPDATE jdocs SET doc = '{"b": [1, 2.0]}' WHERE id = 1"#,
            ),
            ("update to NULL", "UPDATE jdocs SET doc = NULL WHERE id = 4"),
            ("delete", "DELETE FROM jdocs WHERE id IN (2, 5, 6)"),
        ] {
            let before = eng.snapshot();
            exec(&mut c, stmt).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                !incremental || d.full_rebuilds == 0,
                "{what}: an incremental JSON group key view fell back to a rebuild: {}",
                fmt_snapshot(&d)
            );
        }
        eng.stop().await
    });
}

/// A JSON unit key in a layout that also reads TIMESTAMP columns as epochs: the DELETE of a
/// changed unit runs in the `+00:00` session with the keys from the rows and bound JSON keys as
/// plain strings, which a MySQL JSON column never equals, so the unit's old row stayed next to
/// the new one. Drives `mv::apply` directly (the planner runs JSON group keys in full mode).
#[test]
fn reg_json_unit_delete_with_epoch_timestamps() {
    scenario("reg_json_epoch", |ctx| async move {
        use lazyview::datum::Datum;
        use lazyview::mv;
        use lazyview::Metrics;

        let db = ctx.new_db("reg_json_epoch").await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "CREATE TABLE jt (id INT PRIMARY KEY, doc JSON NULL, ts TIMESTAMP(6) NULL)",
                "CREATE TABLE mv_jt (doc JSON NULL, mx TIMESTAMP(6) NULL, n BIGINT NOT NULL)",
                r#"INSERT INTO jt VALUES (1, '{"a": 1}', '2024-01-01 10:00:00.5'), (2, '{"a": 1.0}', '2024-01-02 10:00:00'), (3, '[1, 2]', '2024-01-03 10:00:00'), (4, NULL, NULL)"#,
            ],
        )
        .await?;
        let mut l = probe(&mut c, &db.name, "mv_jt", 1).await?;
        l.epoch = vec![(1, "MAX(ts)".to_string())];
        let sel = "SELECT UNIX_TIMESTAMP(MAX(ts)) AS `_lv_ts0`, doc, MAX(ts) AS mx, COUNT(*) AS n \
                   FROM jt WHERE {F} GROUP BY doc";
        let orc = "SELECT doc, MAX(ts) AS mx, COUNT(*) AS n FROM jt GROUP BY doc";
        let on = "o.doc <=> m.doc AND o.mx <=> m.mx";
        let metrics = Metrics::default();
        let r = rows(&mut c, &l, sel, &["doc"], None).await?;
        mv::apply(&mut c, &db.name, "mv_jt", &l, None, &r, 1000, &metrics).await?;
        check(&mut c, "mv_jt", orc, on, "initial fill").await?;
        // Dirty keys as the binlog decoder renders them on MySQL; the text as stored on MariaDB.
        let key = |mysql: &str, mariadb: &str| {
            let k = if ctx.servers[0].is_mariadb() {
                mariadb
            } else {
                mysql
            };
            Datum::Bytes(k.as_bytes().to_vec())
        };
        for (what, stmt, keys) in [
            (
                "newer timestamp in a JSON group",
                "UPDATE jt SET ts = '2024-02-01 10:00:00.25' WHERE id = 2",
                vec![key(r#"{"a":1.0}"#, r#"{"a": 1.0}"#)],
            ),
            (
                "row moves between JSON groups",
                r#"UPDATE jt SET doc = '[1, 2.0]' WHERE id = 1"#,
                vec![key(r#"{"a":1}"#, r#"{"a": 1}"#), key("[1,2.0]", "[1, 2.0]")],
            ),
            (
                "NULL group gets a timestamp",
                "UPDATE jt SET ts = '2024-03-01 00:00:00' WHERE id = 4",
                vec![Datum::Null],
            ),
        ] {
            exec(&mut c, stmt).await?;
            let chunk = UnitChunk::Keys(keys.into_iter().map(|k| Box::new([k]) as _).collect());
            let r = rows(&mut c, &l, sel, &["doc"], Some(&chunk)).await?;
            mv::apply(
                &mut c,
                &db.name,
                "mv_jt",
                &l,
                Some(&chunk),
                &r,
                1000,
                &metrics,
            )
            .await?;
            check(&mut c, "mv_jt", orc, on, what).await?;
        }
        Ok(())
    });
}

/// DECIMALs inside JSON values a view computes (`JSON_ARRAYAGG(amount)`, `JSON_OBJECT(..,
/// SUM(amount))`, `CAST(MIN(amount) AS JSON)`): MySQL stores every number of a JSON text that
/// is not an integer as a DOUBLE, so the MV holds `[10.0, 20.5]` where the view returns
/// `[10.00, 20.50]` (no client can write a DECIMAL into JSON; docs/views.md "JSON values"). The
/// MV must hold exactly what the server stores for the view's JSON text, and refreshes that
/// re-read such units must not rewrite them: the diff compared the texts and rewrote every
/// re-read unit on every refresh. MariaDB (JSON is LONGTEXT) keeps the text as computed.
#[test]
fn reg_json_decimal_values_converge() {
    scenario("reg_json_dec", |ctx| async move {
        let maria = ctx.servers[0].is_mariadb();
        let select = format!(
            "SELECT g, JSON_ARRAYAGG(amount) AS amts, \
             JSON_OBJECT('sum', SUM(amount), 'big', MAX(big)) AS tot, {} AS mn, COUNT(*) AS n \
             FROM amounts WHERE flag > 0 GROUP BY g",
            if maria {
                "JSON_ARRAY(MIN(amount))"
            } else {
                "CAST(MIN(amount) AS JSON)"
            }
        );
        let specs = vec![ViewSpec::new("r_json_dec", Kind::Incremental, &select)];
        let db = ctx.new_db("reg_json_dec").await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "CREATE TABLE amounts (id INT PRIMARY KEY, g INT NOT NULL, amount DECIMAL(10,2) NOT NULL, \
                 big DECIMAL(30,10) NOT NULL, flag INT NOT NULL)",
                "INSERT INTO amounts VALUES (1, 1, 10.00, 12345678901234567890.1234567890, 1), \
                 (2, 1, 20.50, 0.0000000001, 1), (3, 2, 3.25, 1.5, 1), (4, 2, 0.10, -99999999999999999999.9999999999, 1), \
                 (5, 3, 100.00, 0, 1)",
            ],
        )
        .await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let mode: Option<String> = c
            .query_first("SELECT `mode` FROM _lazyview_state WHERE name = 'r_json_dec'")
            .await?;
        ensure!(
            mode.as_deref() == Some("incremental"),
            "r_json_dec is not incremental: {mode:?}"
        );
        // The view's rows with every JSON value as the server stores it (MySQL: re-parsed from
        // its text; MariaDB: the text), compared as strings with the MV's.
        type Rows = Vec<(i64, String, String, String, i64)>;
        let stored = |e: &str| {
            if maria {
                format!("CAST({e} AS CHAR)")
            } else {
                format!("CAST(CAST(CAST({e} AS CHAR) AS JSON) AS CHAR)")
            }
        };
        let expected_sql = format!(
            "SELECT g, {}, {}, {}, n FROM ({select}) o ORDER BY g",
            stored("amts"),
            stored("tot"),
            stored("mn")
        );
        let computed_sql = format!(
            "SELECT g, CAST(amts AS CHAR), CAST(tot AS CHAR), CAST(mn AS CHAR), n FROM ({select}) o ORDER BY g"
        );
        let mv_sql = "SELECT g, CAST(amts AS CHAR), CAST(tot AS CHAR), CAST(mn AS CHAR), n \
                      FROM r_json_dec ORDER BY g";
        let mut sc = db.conn().await?;
        let mut touches = 0;
        for (what, stmt) in [
            ("initial", None),
            (
                "insert",
                Some("INSERT INTO amounts VALUES (6, 1, 7.10, 0.5, 1), (7, 4, 0.05, 1234.5678900000, 1)"),
            ),
            (
                "update",
                Some("UPDATE amounts SET amount = 3.30, big = 2.25 WHERE id = 3"),
            ),
            ("delete", Some("DELETE FROM amounts WHERE id IN (4, 7)")),
        ] {
            if let Some(stmt) = stmt {
                exec(&mut c, stmt).await?;
            }
            eng.wait_caught_up(&mut sc, t_catchup()).await?;
            let expected: Rows = c.query(expected_sql.as_str()).await?;
            let actual: Rows = c.query(mv_sql).await?;
            ensure!(
                actual == expected,
                "{what}: r_json_dec\n  {actual:?}\nis not the view's rows as stored\n  {expected:?}"
            );
            let computed: Rows = c.query(computed_sql.as_str()).await?;
            ensure!(
                maria || computed != expected,
                "{what}: the view's JSON text is stored unchanged ({computed:?}): the server keeps \
                 DECIMALs in JSON text now; docs/views.md \"JSON values\" is out of date"
            );
            // Refreshes that re-read every unit (the WHERE column changes, the rows do not) must
            // leave the MV alone, every time.
            for _ in 0..2 {
                touches += 1;
                let before = eng.snapshot();
                exec(&mut c, "UPDATE amounts SET flag = flag + 1").await?;
                eng.wait_caught_up(&mut sc, t_catchup()).await?;
                let d = eng.snapshot().since(&before);
                ensure!(
                    d.units_refreshed >= expected.len() as u64
                        && d.units_changed == 0
                        && d.mv_rows_deleted == 0
                        && d.mv_rows_inserted == 0,
                    "{what}, refresh {touches} of unchanged units: expected {} units re-read and none \
                     written: {}",
                    expected.len(),
                    fmt_snapshot(&d)
                );
                let actual: Rows = c.query(mv_sql).await?;
                ensure!(
                    actual == expected,
                    "{what}, refresh {touches}: r_json_dec changed\n  {actual:?}\n  {expected:?}"
                );
            }
        }
        eng.stop().await
    });
}

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
        let _: Option<i64> = reader.query_first("SELECT COUNT(*) FROM r_locked").await?;
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
                    let n: Option<i64> = c
                        .query_first(format!("SELECT COUNT(*) FROM r_names WHERE id = {id}"))
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
            let state: Option<String> = c
                .query_first("SELECT state FROM _lazyview_state WHERE name = 'r_locked'")
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
    Ok(c
        .exec_first(
            "SELECT CAST(state AS CHAR), CAST(mode AS CHAR), CAST(detail AS CHAR), CAST(last_error AS CHAR) \
             FROM _lazyview_state WHERE name = ? AND target = 'source'",
            (name,),
        )
        .await?)
}

/// A view reading a view in `error` said `waiting for upstream view ... to be built` forever,
/// so the troubleshooting query `WHERE state = 'error'` missed it. It is in `error` naming the
/// upstream's error (down a chain of such views) and is built once the upstream is fixed.
#[test]
fn reg_dependent_of_view_in_error_shows_its_error() {
    scenario("reg_blocked", |ctx| async move {
        let db = ctx.new_db("reg_blocked").await?;
        let mut c = db.conn().await?;
        run(&mut c, DDL).await?;
        ensure_lazyview_table(&mut c).await?;
        run(
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
        let listed: i64 = c
            .query_first("SELECT COUNT(*) FROM _lazyview_overview WHERE state = 'error'")
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
        let managed: Option<i64> = c
            .query_first("SELECT managed FROM _lazyview_state WHERE name = 'r_state'")
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
        run(&mut c, DDL).await?;
        ensure_lazyview_table(&mut c).await?;
        run(
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
    c.exec_first("SELECT rebuild FROM _lazyview WHERE name = ?", (name,))
        .await?
        .context("no _lazyview row")
}

/// Waits until lazyview cleared `_lazyview.rebuild` of `name`.
async fn wait_rebuilt(c: &mut mysql_async::Conn, eng: &mut Engine, name: &str) -> Result<()> {
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
                let u = c.query_first(updated_sql).await?;
                eng = ctx.start_engine(&db, &[])?;
                u
            } else {
                exec(&mut c, ask).await?;
                c.query_first(updated_sql).await?
            };
            let before = eng.snapshot();
            wait_rebuilt(&mut c, &mut eng, "r_counts").await?;
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
            let after: Option<String> = c
                .query_first(
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
        run(
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

/// A global `sql_mode` with parser flags (`ANSI_QUOTES`, `PIPES_AS_CONCAT`) must not change
/// what lazyview maintains: definitions are read and SQL is written in the default mode, so the
/// engine's sessions drop those flags. Before, the build of a view with `status = "paid"` failed
/// with an unknown column `paid`. Opt-in (changes a GLOBAL): `LAZYVIEW_TEST_GLOBAL_SQL_MODE=1`.
#[test]
fn reg_global_sql_mode_parser_flags() {
    scenario("reg_sql_mode", |ctx| async move {
        if std::env::var("LAZYVIEW_TEST_GLOBAL_SQL_MODE")
            .map(|v| v != "1")
            .unwrap_or(true)
        {
            ctx.note(
                "SKIP: changes GLOBAL sql_mode; set LAZYVIEW_TEST_GLOBAL_SQL_MODE=1 to run it",
            );
            return Ok(());
        }
        let db = ctx.new_db("reg_sql_mode").await?;
        let mut admin = db.conn().await?;
        let old = scalar(&mut admin, "SELECT CAST(@@GLOBAL.sql_mode AS CHAR)")
            .await?
            .unwrap_or_default();
        exec(
            &mut admin,
            "SET GLOBAL sql_mode = CONCAT(@@GLOBAL.sql_mode, ',ANSI_QUOTES,PIPES_AS_CONCAT')",
        )
        .await?;
        let r = sql_mode_scenario(&ctx, &db).await;
        let restored = exec(&mut admin, &format!("SET GLOBAL sql_mode = {}", q(&old))).await;
        r.and(restored)
    });
}

async fn sql_mode_scenario(ctx: &Ctx, db: &Db) -> Result<()> {
    use mysql_async::prelude::Queryable;
    // This connection inherits the global mode: the test's own SQL uses single quotes.
    let mut c = db.conn().await?;
    let mode = scalar(&mut c, "SELECT CAST(@@SESSION.sql_mode AS CHAR)")
        .await?
        .unwrap_or_default();
    ensure!(
        mode.contains("ANSI_QUOTES"),
        "new sessions do not inherit the global mode: {mode}"
    );
    run(
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
    let mode: Option<String> = c
        .query_first("SELECT `mode` FROM _lazyview_state WHERE name = 'r_paid'")
        .await?;
    ensure!(
        mode.as_deref() == Some("incremental"),
        "r_paid is not incremental: {mode:?}"
    );
    async fn same(c: &mut mysql_async::Conn, what: &str) -> Result<()> {
        let mv: Vec<(i64, i64)> = c
            .query("SELECT customer_id, n FROM r_paid ORDER BY 1")
            .await?;
        let src: Vec<(i64, i64)> = c
            .query("SELECT customer_id, COUNT(*) FROM orders WHERE status = 'paid' GROUP BY customer_id ORDER BY 1")
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
