//! XA transactions: prepared-but-unresolved XA data must not appear in views; `XA COMMIT`
//! (same session, another session after the preparing session disconnected, `ONE PHASE`) must
//! appear; `XA ROLLBACK` (after PREPARE, also from another session) must never appear.

mod common;

use anyhow::Context;
use common::workload::{ecommerce_views, names, run_batches, setup_ecommerce, Sizes, Workload};
use common::*;

/// XA branch body: inserts a new order with lines and a payment, and modifies rows of orders
/// `touch`, `touch + 1`, `touch + 2` (distinct per concurrently prepared branch: prepared
/// branches keep their row locks).
fn xa_dml(xid: &str, order: i64, line: i64, pay: i64, touch: i64) -> Vec<String> {
    vec![
        format!("XA START '{xid}'"),
        format!("INSERT INTO orders (id, customer_id, status, created_at) VALUES ({order}, 1, 'paid', '2024-03-01 10:00:00.000001')"),
        format!("INSERT INTO order_items (order_id, line_no, product_id, qty, price) VALUES ({order}, {line}, 1, 7, 7.77), ({order}, {}, 2, 1, 1.11)", line + 1),
        format!("INSERT INTO payments (id, order_id, method, amount) VALUES ({pay}, {order}, 'wire', 500.00)"),
        format!("UPDATE orders SET status = 'shipped' WHERE id = {touch}"),
        format!("UPDATE order_items SET qty = qty + 100 WHERE order_id = {}", touch + 1),
        format!("DELETE FROM payments WHERE order_id = {}", touch + 2),
        format!("XA END '{xid}'"),
    ]
}

#[test]
fn xa_transactions() {
    scenario("xa", |ctx| async move {
        let db = ctx.new_db("xa").await?;
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let keep = [
            "v_paid_orders",
            "v_order_lines",
            "v_status_counts",
            "v_global_orders",
            "v_customer_stats",
            "v_orders_left",
            "v_distinct_methods",
        ];
        let specs: Vec<ViewSpec> = ecommerce_views()
            .into_iter()
            .filter(|v| keep.contains(&v.name.as_str()))
            .collect();
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_BATCH_WINDOW_MS", "50")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
        let mut xc = db.conn().await?;

        // 1. Prepared, unresolved: must not be visible; unrelated commits keep flowing.
        exec_all(&mut xc, &xa_dml("x1", 800_001, 8_000_001, 8_000_001, 1)).await?;
        exec(&mut xc, "XA PREPARE 'x1'").await?;
        exec(&mut c, "INSERT INTO orders (id, customer_id, status, created_at) VALUES (800100, 2, 'new', '2024-03-02 00:00:00')").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA prepared (uncommitted)")
            .await
            .context("prepared XA data leaked or catch-up stalled")?;

        // 2. Two-phase commit from the same session.
        exec(&mut xc, "XA COMMIT 'x1'").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA COMMIT (two-phase)").await?;

        // 3. Two-phase rollback.
        exec_all(&mut xc, &xa_dml("x2", 800_002, 8_000_011, 8_000_011, 4)).await?;
        exec(&mut xc, "XA PREPARE 'x2'").await?;
        verify(&ctx, &mut eng, &db, &specs, "second XA prepared").await?;
        exec(&mut xc, "XA ROLLBACK 'x2'").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA ROLLBACK after PREPARE").await?;

        // 4. One-phase commit.
        exec_all(&mut xc, &xa_dml("x3", 800_003, 8_000_021, 8_000_021, 7)).await?;
        exec(&mut xc, "XA COMMIT 'x3' ONE PHASE").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA COMMIT ONE PHASE").await?;

        // 5. Rollback before prepare (XA END then XA ROLLBACK).
        exec_all(&mut xc, &xa_dml("x4", 800_004, 8_000_031, 8_000_031, 10)).await?;
        exec(&mut xc, "XA ROLLBACK 'x4'").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA ROLLBACK before PREPARE").await?;

        // 6. Prepared in one session, which disconnects; resolved from another session.
        exec_all(&mut xc, &xa_dml("x5", 800_005, 8_000_041, 8_000_041, 1)).await?;
        exec(&mut xc, "XA PREPARE 'x5'").await?;
        xc.disconnect().await.ok();
        let mut xc2 = db.conn().await?;
        exec_all(&mut xc2, &xa_dml("x6", 800_006, 8_000_051, 8_000_051, 4)).await?;
        exec(&mut xc2, "XA PREPARE 'x6'").await?;
        xc2.disconnect().await.ok();
        // Normal writes that do not touch rows locked by the prepared branches.
        exec(&mut c, "INSERT INTO orders (id, customer_id, status, created_at) VALUES (800200, 3, 'paid', '2024-03-03 00:00:00')").await?;
        exec(
            &mut c,
            "UPDATE orders SET status = 'refunded' WHERE id = 12",
        )
        .await?;
        exec(&mut c, "UPDATE customers SET name = 'xa-side' WHERE id = 3").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "two detached prepared XA + normal writes",
        )
        .await?;
        let mut other = db.conn().await?;
        exec(&mut other, "XA COMMIT 'x5'").await?;
        exec(&mut other, "XA ROLLBACK 'x6'").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "detached XA committed / rolled back from another session",
        )
        .await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &specs, "normal writes after XA").await?;
        assert_ready(&mut c, &names(&specs)).await?;
        eng.stop().await?;
        Ok(())
    });
}
