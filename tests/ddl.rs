//! Source DDL: TRUNCATE, ALTER TABLE (add / drop / reorder columns, modify), RENAME away and back,
//! and the online-schema-change style table swap (`RENAME TABLE t TO t_old, t_new TO t`).

mod common;

use anyhow::{ensure, Result};
use common::workload::{ecommerce_views, names, run_batches, setup_ecommerce, Sizes, Workload};
use common::*;

async fn start_all(ctx: &Ctx, tag: &str, overrides: &[(&str, &str)]) -> Result<(Db, Engine, Vec<ViewSpec>, Workload)> {
    let db = ctx.new_db(tag).await?;
    let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
    let specs = ecommerce_views();
    let (mut eng, _ids) = start_with_views(&ctx, &db, &specs, overrides).await?;
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
        ensure!(count(&mut c, "v_paid_orders").await? == 0, "v_paid_orders not emptied");
        ensure!(count(&mut c, "v_order_customer_region").await? == 0, "v_order_customer_region not emptied");
        ensure!(scalar(&mut c, "SELECT n FROM v_global_orders").await?.as_deref() == Some("0"), "global aggregate over empty table must be one row with n = 0");
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        verify(&ctx, &mut eng, &db, &specs, "writes after TRUNCATE").await?;
        // Truncate a table without primary key and a composite-PK table inside the same window.
        exec(&mut c, "TRUNCATE TABLE events_log").await?;
        exec(&mut c, "TRUNCATE TABLE inventory").await?;
        exec(&mut c, "INSERT INTO events_log (customer_id, kind, amount, happened_at) VALUES (1, 'login', 99.00, '2024-01-01 00:00:00'), (1, 'login', 99.00, '2024-01-01 00:00:00')").await?;
        verify(&ctx, &mut eng, &db, &specs, "after TRUNCATE events_log / inventory").await?;
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
        exec(&mut c, "UPDATE products_parked SET price = price + 1 WHERE id % 2 = 0").await?;
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
        exec(&mut c, "INSERT INTO payments_new SELECT * FROM payments WHERE id % 3 <> 0").await?;
        exec(&mut c, "UPDATE payments_new SET amount = amount * 2 WHERE id % 2 = 0").await?;
        exec(&mut c, "RENAME TABLE payments TO payments_old, payments_new TO payments").await?;
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
