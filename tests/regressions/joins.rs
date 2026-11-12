use super::*;

/// PK update / delete of a joined, non-anchor table must remove the rows that joined to the
/// old key (seen as stale rows in v_line_details / v_order_customer_region).
#[test]
fn reg_join_partner_pk_update() {
    scenario("reg_join_pk", |ctx| async move {
        let specs = vec![join_view()];
        let (db, mut eng) = setup(&ctx, "reg_join_pk", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, &["UPDATE customers SET id = 100 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer PK 1 → 100 (orders 10, 11 must vanish)",
        )
        .await?;
        exec_all(&mut c, &["UPDATE customers SET id = 1 WHERE id = 100"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer PK 100 → 1 (orders come back)",
        )
        .await?;
        exec_all(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "customer 2 deleted").await?;
        eng.stop().await
    });
}

/// A one-element `IN (column)` join is a key equality: the grouped view plans incremental and a
/// docs or items change refreshes it without a whole-view rebuild.
#[test]
fn reg_one_element_in_join() {
    scenario("reg_in_eq", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_item_doc",
            Kind::Incremental,
            "SELECT i.id, d.doc, COUNT(*) n FROM items i JOIN docs d ON d.id IN (i.doc_id) GROUP BY i.id, d.doc",
        )];
        let ddl = [
            "CREATE TABLE items (id INT PRIMARY KEY, doc_id INT NOT NULL)",
            "CREATE TABLE docs (id INT PRIMARY KEY, doc JSON NOT NULL)",
            "INSERT INTO items VALUES (1, 1), (2, 2), (3, 3)",
            "INSERT INTO docs VALUES (1, '[1]'), (2, '[2]'), (3, '[3]')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_in_eq", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let mut sc = db.conn().await?;
        let before = eng.snapshot();
        exec_all(&mut c, &["UPDATE docs SET doc = '[9]' WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "doc 1 updated").await?;
        exec_all(&mut c, &["DELETE FROM docs WHERE id = 2"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "doc 2 deleted").await?;
        exec_all(&mut c, &["INSERT INTO docs VALUES (2, '[2]')"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "doc 2 re-inserted").await?;
        exec_all(&mut c, &["UPDATE items SET doc_id = 2 WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "item 1 re-pointed to doc 2").await?;
        exec_all(&mut c, &["DELETE FROM items WHERE id = 3"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "item 3 deleted").await?;
        exec_all(&mut c, &["INSERT INTO items VALUES (3, 3)"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "item 3 re-inserted").await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "one-element IN join caused a full rebuild: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// A window unit reduced by keys (step 5B): the JSON column in `PARTITION BY id, doc` is
/// determined by the primary key, so the unit is `id` alone and updates to the JSON or to the
/// other partition column stay incremental.
#[test]
fn reg_window_units_reduced_by_key() {
    scenario("reg_win_red", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_doc_sum",
            Kind::Incremental,
            "SELECT id, grp, SUM(grp) OVER (PARTITION BY id, doc) AS s FROM docs",
        )];
        let ddl = [
            "CREATE TABLE docs (id INT PRIMARY KEY, grp INT NOT NULL, doc JSON NOT NULL)",
            "INSERT INTO docs VALUES (1, 10, '{\"a\": 1}'), (2, 20, '{\"b\": 2}'), (3, 30, '{\"c\": 3}')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_win_red", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let mut sc = db.conn().await?;
        let before = eng.snapshot();
        exec_all(&mut c, &["UPDATE docs SET doc = '{\"a\": 9}' WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "doc updated").await?;
        exec_all(&mut c, &["UPDATE docs SET grp = 11 WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "grp updated").await?;
        exec_all(
            &mut c,
            &[
                "DELETE FROM docs WHERE id = 2",
                "INSERT INTO docs VALUES (2, 20, '{\"b\": 2}')",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "row deleted and re-inserted").await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "window unit reduced by key still caused a full rebuild: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// Hop paths (step 5E): for the views of the one-element IN join, the window-unit reduction
/// and the partner-PK join, each `Hop.select`, pinned to all partner values, parses on the
/// server and returns ids covering the view's unit ids.
#[test]
fn reg_hop_selects_cover_the_view_ids() {
    scenario("reg_hop", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_item_doc",
                Kind::Incremental,
                "SELECT i.id, d.doc, COUNT(*) n FROM items i JOIN docs d ON d.id IN (i.doc_id) GROUP BY i.id, d.doc",
            ),
            ViewSpec::new(
                "r_entity_sum",
                Kind::Incremental,
                "SELECT id, grp, SUM(grp) OVER (PARTITION BY id, doc) AS s FROM entities",
            ),
            join_view(),
        ];
        let ddl = [
            "CREATE TABLE items (id INT PRIMARY KEY, doc_id INT NOT NULL)",
            "CREATE TABLE docs (id INT PRIMARY KEY, doc JSON NOT NULL)",
            "INSERT INTO items VALUES (1, 1), (2, 2), (3, 3)",
            "INSERT INTO docs VALUES (1, '[1]'), (2, '[2]'), (3, '[3]')",
            "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, doc JSON NOT NULL)",
            "INSERT INTO entities VALUES (1, 10, '{\"a\": 1}'), (2, 20, '{\"b\": 2}')",
        ];
        let (db, eng) = setup(&ctx, "reg_hop", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let mut collations = std::collections::HashMap::new();
        let mut tables = std::collections::HashMap::new();
        for name in ["items", "docs", "entities", "orders", "customers"] {
            let t = lazyview::catalog::load_table(
                &mut c,
                &db.name,
                name,
                ctx.server().is_mariadb(),
                &mut collations,
            )
            .await?
            .with_context(|| format!("table {name}"))?;
            tables.insert((db.name.clone(), name.to_string()), std::sync::Arc::new(t));
        }
        let mut hops = 0usize;
        for s in &specs {
            let p = lazyview::analyze::plan(
                &lazyview::analyze::parse_definition(&s.definition).unwrap(),
                &db.name,
                &tables,
            )
            .unwrap();
            let ap = lazyview::access::AccessPlan::of(&p);
            let id = p
                .unit_in_projection
                .first()
                .copied()
                .flatten()
                .with_context(|| format!("{}: the unit is not an output column", s.name))?;
            let id_name = p.projection[id]
                .rsplit_once(" AS ")
                .map(|(_, a)| a.to_string())
                .unwrap_or_else(|| {
                    p.projection[id]
                        .rsplit_once('.')
                        .map_or_else(|| p.projection[id].clone(), |(_, c)| c.to_string())
                })
                .trim_matches('`')
                .to_string();
            let want: std::collections::BTreeSet<i64> = typed_rows(
                &mut c,
                &format!(
                    "SELECT DISTINCT `{id_name}` FROM `{}` WHERE `{id_name}` IS NOT NULL",
                    s.name
                ),
            )
            .await?
            .into_iter()
            .map(|(v,)| v)
            .collect();
            for o in &ap.occurrences {
                let lazyview::access::Path::Hop(h) = &o.path else {
                    continue;
                };
                hops += 1;
                let (pa, pc) = h.partner[0]
                    .split_once("`.`")
                    .map(|(a, b)| {
                        (
                            a.trim_matches('`').to_string(),
                            b.trim_matches('`').to_string(),
                        )
                    })
                    .with_context(|| format!("{}: partner {}", s.name, h.partner[0]))?;
                let ai = p.aliases.iter().position(|a| a.name == pa).unwrap();
                let tbl = p.aliases[ai].table.name.clone();
                let vals: Vec<(i64,)> = typed_rows(
                    &mut c,
                    &format!("SELECT DISTINCT `{pc}` FROM `{tbl}` WHERE `{pc}` IS NOT NULL"),
                )
                .await?;
                let full = format!(
                    "{} AND {} IN ({})",
                    h.select,
                    h.partner[0],
                    vals.iter()
                        .map(|(v,)| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let got: Vec<(i64, i64)> = typed_rows(&mut c, &full).await?;
                let got_ids: std::collections::BTreeSet<i64> = got.iter().map(|r| r.1).collect();
                ensure!(
                    got_ids.is_superset(&want),
                    "hop of {} ({}): view ids {want:?} not covered by {got_ids:?}",
                    o.alias,
                    full
                );
            }
        }
        ensure!(
            hops >= 2,
            "expected hops on the IN-join and partner-PK views, got {hops}"
        );
        eng.stop().await
    });
}

/// A link row deleted in the same window (F2-F5): the hop SELECT reads the chain's tables
/// only, so pinning it to the occurrence's old link value still finds the ids that value
/// reached, with no join to the occurrence's own table.
#[test]
fn reg_hop_select_after_link_delete() {
    scenario("reg_hop_del", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_item_doc",
            Kind::Incremental,
            "SELECT i.id, d.doc, COUNT(*) n FROM items i JOIN docs d ON d.id IN (i.doc_id) GROUP BY i.id, d.doc",
        )];
        let ddl = [
            "CREATE TABLE items (id INT PRIMARY KEY, doc_id INT NOT NULL)",
            "CREATE TABLE docs (id INT PRIMARY KEY, doc JSON NOT NULL)",
            "INSERT INTO items VALUES (1, 1), (2, 2), (3, 3)",
            "INSERT INTO docs VALUES (1, '[1]'), (2, '[2]'), (3, '[3]')",
        ];
        let (db, mut eng) = setup(&ctx, "reg_hop_del", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, &["DELETE FROM docs WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "doc 1 deleted").await?;
        let mut collations = std::collections::HashMap::new();
        let mut tables = std::collections::HashMap::new();
        for name in ["items", "docs"] {
            let t = lazyview::catalog::load_table(
                &mut c,
                &db.name,
                name,
                ctx.server().is_mariadb(),
                &mut collations,
            )
            .await?
            .with_context(|| format!("table {name}"))?;
            tables.insert((db.name.clone(), name.to_string()), std::sync::Arc::new(t));
        }
        let p = lazyview::analyze::plan(
            &lazyview::analyze::parse_definition(&specs[0].definition).unwrap(),
            &db.name,
            &tables,
        )
        .unwrap();
        let ap = lazyview::access::AccessPlan::of(&p);
        let o = ap
            .occurrences
            .iter()
            .find(|o| o.alias == "d")
            .with_context(|| "no occurrence `d`".to_string())?;
        let lazyview::access::Path::Hop(h) = &o.path else {
            bail!("expected a hop for `d`, got {:?}", o.path);
        };
        let sql = format!("{} AND {} IN (1)", h.select, h.partner[0]);
        let got: Vec<(i64, i64)> = typed_rows(&mut c, &sql).await?;
        let ids: Vec<i64> = got.iter().map(|r| r.1).collect();
        ensure!(
            ids.contains(&1),
            "item 1 not reached through its doc id after the doc row was gone: {sql} -> {got:?}"
        );
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
        exec_all(
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
        exec_all(&mut c, &["UPDATE orders SET customer_id = 2 WHERE id = 10"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "order 10 → customer 2 (items back)",
        )
        .await?;
        exec_all(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "customer 2 deleted (items of orders 10, 12 vanish)",
        )
        .await?;
        exec_all(
            &mut c,
            &["UPDATE orders SET customer_id = NULL WHERE id = 13"],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "order 13 → NULL customer").await?;
        exec_all(&mut c, &["UPDATE customers SET id = 50 WHERE id = 1"]).await?;
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
        exec_all(&mut c, &["UPDATE customers SET id = 67 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "FK-parent customer PK 1 → 67 (items of orders 10, 11 vanish)",
        )
        .await?;
        exec_all(
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
        exec_all(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
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
        exec_all(&mut c, &["UPDATE customers SET id = 100 WHERE id = 1"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "FK parent customer PK 1 → 100 (orders 10, 11 must vanish)",
        )
        .await?;
        exec_all(&mut c, &["DELETE FROM customers WHERE id = 2"]).await?;
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
        exec_all(&mut c, &["UPDATE products SET price = 1000 WHERE id = 6"]).await?;
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
        exec_all(
            &mut c,
            &[
                "UPDATE bits SET b = b'11111111' WHERE id = 2",
                "INSERT INTO bits VALUES (5, 5, NULL, 't', b'10000000', b'0')",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "BIT values after changes").await?;
        // Other columns change, BIT stays 0xff: the refresh re-reads and re-writes the row.
        exec_all(
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
        exec_all(
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
        exec_all(&mut c, &["UPDATE rk SET k = 'abc' WHERE id = 1"]).await?;
        verify(&ctx, &mut eng, &db, &specs, "partner key 'ABC' → 'abc'").await?;
        exec_all(&mut c, &["UPDATE tk SET k = 'Abc' WHERE id = 1"]).await?;
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
        exec_all(
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
        exec_all(
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
