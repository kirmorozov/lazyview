//! Source DDL: TRUNCATE, ALTER TABLE (add / drop / reorder columns, modify; STORED generated and
//! INVISIBLE columns in the middle of a table), RENAME away and back, and the online-schema-change
//! style table swap (`RENAME TABLE t TO t_old, t_new TO t`).

mod common;

use anyhow::{ensure, Result};
use common::workload::{ecommerce_views, names, run_batches, setup_ecommerce, Sizes, Workload};
use common::*;

async fn start_all(
    ctx: &Ctx,
    tag: &str,
    overrides: &[(&str, &str)],
) -> Result<(Db, Engine, Vec<ViewSpec>, Workload)> {
    let db = ctx.new_db(tag).await?;
    let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
    let specs = ecommerce_views();
    let (mut eng, _ids) = start_with_views(ctx, &db, &specs, overrides).await?;
    verify(ctx, &mut eng, &db, &specs, "initial").await?;
    let wl = Workload::new(ctx.sub_seed("wl"), ids);
    Ok((db, eng, specs, wl))
}

#[test]
fn ddl_truncate_empties_views() {
    scenario("ddl_truncate", |ctx| async move {
        let (db, mut eng, specs, mut wl) = start_all(&ctx, "ddl_truncate", &[]).await?;
        let mut c = db.conn().await?;
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        exec(&mut c, "TRUNCATE TABLE orders").await?;
        verify(&ctx, &mut eng, &db, &specs, "after TRUNCATE orders").await?;
        ensure!(
            count(&mut c, "v_paid_orders").await? == 0,
            "v_paid_orders not emptied"
        );
        ensure!(
            count(&mut c, "v_order_customer_region").await? == 0,
            "v_order_customer_region not emptied"
        );
        ensure!(
            scalar(&mut c, "SELECT n FROM v_global_orders")
                .await?
                .as_deref()
                == Some("0"),
            "global aggregate over empty table must be one row with n = 0"
        );
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        verify(&ctx, &mut eng, &db, &specs, "writes after TRUNCATE").await?;
        // Truncate a table without primary key and a composite-PK table inside the same window.
        exec(&mut c, "TRUNCATE TABLE events_log").await?;
        exec(&mut c, "TRUNCATE TABLE inventory").await?;
        exec(&mut c, "INSERT INTO events_log (customer_id, kind, amount, happened_at) VALUES (1, 'login', 99.00, '2024-01-01 00:00:00'), (1, 'login', 99.00, '2024-01-01 00:00:00')").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after TRUNCATE events_log / inventory",
        )
        .await?;
        assert_ready(&mut c, &names(&specs)).await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn ddl_alter_columns_keeps_views_correct() {
    scenario("ddl_alter", |ctx| async move {
        let (db, mut eng, specs, mut wl) = start_all(&ctx, "ddl_alter", &[]).await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            ("ADD COLUMN at the end", &["ALTER TABLE orders ADD COLUMN extra INT NOT NULL DEFAULT 5"]),
            // Shifts the ordinal of every column: binlog images must be re-mapped.
            ("ADD COLUMN FIRST", &["ALTER TABLE customers ADD COLUMN c0 INT NULL FIRST"]),
            ("ADD COLUMN in the middle", &["ALTER TABLE order_items ADD COLUMN memo VARCHAR(10) NULL AFTER order_id"]),
            ("ADD unused column", &["ALTER TABLE payments ADD COLUMN legacy_code VARCHAR(10) NULL AFTER order_id"]),
            ("DROP unused column", &["ALTER TABLE payments DROP COLUMN legacy_code"]),
            ("DROP unused column (shifts ordinals)", &["ALTER TABLE customers DROP COLUMN c0"]),
            ("MODIFY referenced column width", &["ALTER TABLE products MODIFY COLUMN name VARCHAR(120) NOT NULL"]),
            ("ADD INDEX + several changes in one statement", &["ALTER TABLE orders ADD INDEX k_status (status), ADD COLUMN extra2 DATETIME NULL AFTER status, DROP COLUMN extra"]),
        ];
        for (what, ddl) in steps {
            run_batches(&mut c, &mut wl, scaled(15)).await?;
            for d in *ddl {
                exec(&mut c, d).await?;
            }
            // Writes right after the DDL, possibly in the same batch window.
            run_batches(&mut c, &mut wl, scaled(25)).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        assert_ready(&mut c, &names(&specs)).await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn ddl_rename_away_and_back() {
    scenario("ddl_rename", |ctx| async move {
        let (db, mut eng, specs, mut wl) = start_all(&ctx, "ddl_rename", &[]).await?;
        let mut c = db.conn().await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;

        // Away: the views reading products lose their source for a while; changes happen meanwhile.
        exec(&mut c, "RENAME TABLE products TO products_parked").await?;
        exec(
            &mut c,
            "UPDATE products_parked SET price = price + 1 WHERE id % 2 = 0",
        )
        .await?;
        exec(&mut c, "INSERT INTO products_parked (id, sku, name, category, price) VALUES (900001, 'SKU-P', 'parked', 'books', 1.00)").await?;
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id % 5 = 0").await?;
        exec(&mut c, "INSERT INTO order_items (order_id, line_no, product_id, qty, price) VALUES (1, 900001, 900001, 3, 1.00)").await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        eng.check_alive()?;
        // And back.
        exec(&mut c, "RENAME TABLE products_parked TO products").await?;
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        wait_ready_tolerant(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after RENAME away and back").await?;

        // Online-schema-change style swap: payments is replaced by a rebuilt copy.
        exec(&mut c, "CREATE TABLE payments_new LIKE payments").await?;
        exec(
            &mut c,
            "INSERT INTO payments_new SELECT * FROM payments WHERE id % 3 <> 0",
        )
        .await?;
        exec(
            &mut c,
            "UPDATE payments_new SET amount = amount * 2 WHERE id % 2 = 0",
        )
        .await?;
        exec(
            &mut c,
            "RENAME TABLE payments TO payments_old, payments_new TO payments",
        )
        .await?;
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        wait_ready_tolerant(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after table swap").await?;
        exec(&mut c, "DROP TABLE payments_old").await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &specs, "after dropping the old copy").await?;
        assert_ready(&mut c, &names(&specs)).await?;
        eng.stop().await?;
        Ok(())
    });
}

/// Whether row images carry STORED generated columns here: MariaDB >= 11.4 leaves generated
/// columns out of row images (issue #7; STORED ones on 13.x too), so keys over them miss row
/// moves there until that is fixed.
async fn images_carry_stored(ctx: &Ctx, c: &mut mysql_async::Conn) -> Result<bool> {
    if !ctx.server().is_mariadb() {
        return Ok(true);
    }
    Ok(server_version(c).await? < (11, 4))
}

/// STORED generated and INVISIBLE columns in the middle of a table, then reorder ALTERs
/// (`MODIFY ... AFTER` / `FIRST`, an invisible column made visible, ADD in the middle), each
/// followed in the same stream by changes of the columns after the moved ones: row images are
/// mapped by ordinal, so every ALTER moves the later columns. `SELECT *` leaves invisible
/// columns out, also after one moves first. STORED keys run where row images carry them
/// (skipped visibly on MariaDB >= 11.4, issue #7); VIRTUAL columns are issue #7 too.
#[test]
fn ddl_generated_and_invisible_columns_mid_table() {
    scenario("ddl_mid_columns", |ctx| async move {
        let db = ctx.new_db("ddl_mid_columns").await?;
        let mut c = db.conn().await?;
        let stored = images_carry_stored(&ctx, &mut c).await?;
        if !stored {
            ctx.skip(
                "STORED part: MariaDB >= 11.4 leaves generated columns out of row images \
                 (issue #7); the INVISIBLE and reorder parts run",
            );
        }
        let s_col = if stored {
            "s INT AS (a + 1) STORED, "
        } else {
            ""
        };
        exec(
            &mut c,
            &format!(
                "CREATE TABLE entity (id INT PRIMARY KEY, a INT NULL, {s_col}inv INT NULL INVISIBLE, \
                 c INT NULL, d VARCHAR(10) NULL, KEY k_c (c))"
            ),
        )
        .await?;
        exec(
            &mut c,
            "CREATE TABLE item (id INT PRIMARY KEY, eid INT NULL, hid VARCHAR(10) NULL INVISIBLE, \
             attr VARCHAR(10) NULL)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO entity (id, a, inv, c, d) VALUES (1, 1, 10, 100, 'p'), (2, 2, 20, 100, 'q'), \
             (3, 3, NULL, 300, 'r'), (4, 1, 10, NULL, NULL), (5, NULL, 50, 500, 's'), (6, 6, 20, 100, 't')",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO item (id, eid, hid, attr) VALUES (1, 1, 'h1', 'x'), (2, 2, 'h2', 'y'), \
             (3, 2, NULL, 'z'), (4, 4, 'h4', NULL), (5, 7, 'h5', 'w')",
        )
        .await?;
        let mut specs = vec![
            ViewSpec::new(
                "v_entity",
                Kind::Incremental,
                "SELECT id, a, inv, c, d FROM entity",
            ),
            ViewSpec::new(
                "v_entity_by_inv",
                Kind::Incremental,
                "SELECT inv, COUNT(*) AS n, SUM(c) AS sc FROM entity GROUP BY inv",
            ),
            ViewSpec::new(
                "v_entity_by_c",
                Kind::Incremental,
                "SELECT c, COUNT(*) AS n, MAX(d) AS md FROM entity GROUP BY c",
            ),
            ViewSpec::new(
                "v_entity_item",
                Kind::Incremental,
                "SELECT e.id, e.d, i.id AS iid, i.hid, i.attr FROM entity e JOIN item i ON i.eid = e.id",
            ),
            ViewSpec::new("v_item", Kind::Incremental, "SELECT * FROM item"),
        ];
        if stored {
            specs.extend([
                ViewSpec::new(
                    "v_entity_s",
                    Kind::Incremental,
                    "SELECT id, s, c FROM entity",
                ),
                ViewSpec::new(
                    "v_entity_by_s",
                    Kind::Incremental,
                    "SELECT s, COUNT(*) AS n, SUM(inv) AS si FROM entity GROUP BY s",
                ),
                ViewSpec::new(
                    "v_entity_s_item",
                    Kind::Incremental,
                    "SELECT e.id, e.s, i.attr FROM entity e JOIN item i ON i.eid = e.s",
                ),
            ]);
        }
        // A wide window: an ALTER and the changes after it can share a batch.
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "300")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        // After every step, in the same stream: changes of the columns after the moved ones.
        let later = [
            "UPDATE entity SET c = c + 1 WHERE id % 2 = 0",
            "UPDATE entity SET d = CONCAT(COALESCE(d, ''), 'x') WHERE id < 4",
            "UPDATE entity SET inv = COALESCE(inv, 0) + 10 WHERE id % 3 = 0",
            "UPDATE entity SET a = COALESCE(a, 0) + 1 WHERE id IN (1, 5)",
            "UPDATE item SET hid = CONCAT(COALESCE(hid, ''), 'h'), attr = 'v' WHERE id = 2",
        ];
        let mut steps: Vec<(&str, Vec<&str>)> = vec![
            ("no DDL", vec![]),
            (
                "last column moved to the front",
                vec!["ALTER TABLE entity MODIFY COLUMN d VARCHAR(10) NULL AFTER id"],
            ),
            (
                "invisible column made visible and moved after a later one",
                vec!["ALTER TABLE entity MODIFY COLUMN inv INT NULL AFTER c"],
            ),
        ];
        if stored {
            steps.push((
                "STORED column moved to the end",
                vec!["ALTER TABLE entity MODIFY COLUMN s INT AS (a + 1) STORED AFTER inv"],
            ));
        }
        steps.extend([
            (
                "ADD in the middle, also a new invisible column",
                vec![
                    "ALTER TABLE entity ADD COLUMN extra INT NOT NULL DEFAULT 7 AFTER id, \
                     ADD COLUMN inv2 INT NULL INVISIBLE AFTER a",
                    "UPDATE entity SET extra = extra + id, inv2 = id WHERE id > 2",
                ],
            ),
            (
                "invisible column moved first (SELECT * unchanged)",
                vec!["ALTER TABLE item MODIFY COLUMN hid VARCHAR(10) NULL INVISIBLE FIRST"],
            ),
            (
                "insert / delete after the reorders",
                vec![
                    "INSERT INTO entity (id, a, inv, c, d) VALUES (7, 2, 10, 100, 'u'), (8, NULL, NULL, NULL, NULL)",
                    "DELETE FROM entity WHERE id = 3",
                    "DELETE FROM item WHERE id = 1",
                    "INSERT INTO item (id, eid, hid, attr) VALUES (6, 7, 'h6', 'n')",
                ],
            ),
        ]);
        for (what, stmts) in &steps {
            for s in stmts.iter().chain(&later) {
                exec(&mut c, s).await?;
            }
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        assert_ready(&mut c, &names(&specs)).await?;
        eng.stop().await
    });
}
