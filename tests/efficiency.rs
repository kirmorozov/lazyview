//! Efficiency claims, checked through `Metrics` deltas (counters are global, so each scenario
//! uses a dedicated, minimal view set):
//! * 30 writes on one order across orders / order_items / payments inside one window ⇒ exactly one
//!   refreshed unit per view and a bounded number of queries;
//! * updates of columns no view references ⇒ zero source / lookup / MV queries;
//! * one row change feeding N views ⇒ each view refreshes exactly one unit;
//! * updates that do not change any view output ⇒ zero MV rows written (units_changed == 0);
//! * a 20k-row transaction is applied correctly.

mod common;

use std::time::Duration;

use anyhow::{ensure, Result};
use common::workload::{ecommerce_views, setup_ecommerce, Sizes};
use common::*;
use lazyview::Snapshot;

/// Upper bounds per refreshed view (one unit, one chunk): read MV unit rows + DELETE + INSERT,
/// +1 slack for status/position bookkeeping.
const MV_QUERIES_PER_VIEW_MAX: u64 = 4;
/// One SELECT of the unit from the sources (+1 slack).
const SOURCE_QUERIES_PER_VIEW_MAX: u64 = 2;

/// Waits until the engine is caught up and idle, so a following measurement starts a new window.
async fn quiesce(engine: &mut Engine, db: &Db) -> Result<()> {
    let mut c = db.conn().await?;
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    Ok(())
}

/// Runs `stmts` (as one transaction or as autocommit statements), waits for catch-up and returns
/// the metric delta.
async fn measure_writes(
    engine: &mut Engine,
    db: &Db,
    stmts: &[String],
    one_txn: bool,
) -> Result<Snapshot> {
    Ok(measure_writes_timed(engine, db, stmts, one_txn).await?.0)
}

/// Like [`measure_writes`], also returning how long the writes themselves took.
async fn measure_writes_timed(
    engine: &mut Engine,
    db: &Db,
    stmts: &[String],
    one_txn: bool,
) -> Result<(Snapshot, Duration)> {
    quiesce(engine, db).await?;
    let mut c = db.conn().await?;
    let before = engine.snapshot();
    let t0 = std::time::Instant::now();
    if one_txn {
        exec(&mut c, "START TRANSACTION").await?;
    }
    exec_all(&mut c, stmts).await?;
    if one_txn {
        exec(&mut c, "COMMIT").await?;
    }
    let took = t0.elapsed();
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    let d = engine.snapshot().since(&before);
    c.disconnect().await.ok();
    Ok((d, took))
}

fn hammer_statements(order: i64, line_base: i64, pay_base: i64) -> Vec<String> {
    let mut v = Vec::new();
    for k in 0..30i64 {
        v.push(match k % 6 {
            0 => format!("UPDATE orders SET status = '{}' WHERE id = {order}", ["paid", "shipped", "new"][(k as usize / 6) % 3]),
            1 => format!("UPDATE order_items SET qty = qty + 1 WHERE order_id = {order}"),
            2 => format!("UPDATE payments SET amount = amount + 1.50 WHERE order_id = {order}"),
            3 => format!(
                "INSERT INTO order_items (order_id, line_no, product_id, qty, price) VALUES ({order}, {}, 1, 2, 9.99)",
                line_base + k
            ),
            4 => format!("INSERT INTO payments (id, order_id, method, amount) VALUES ({}, {order}, 'card', 5.00)", pay_base + k),
            _ => format!("UPDATE order_items SET price = price + 0.01 WHERE order_id = {order} AND line_no = {}", line_base + k - 2),
        });
    }
    v
}

#[test]
fn efficiency_dedup_30_writes_one_unit() {
    scenario("eff_dedup", |ctx| async move {
        let db = ctx.new_db("eff_dedup").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let specs = vec![
            ViewSpec::new(
                "ev_order_full",
                Kind::Incremental,
                "SELECT o.id, o.status, i.line_no, i.qty, i.price, pay.id AS pay_id, pay.amount
                 FROM orders o LEFT JOIN order_items i ON i.order_id = o.id LEFT JOIN payments pay ON pay.order_id = o.id",
            ),
            ViewSpec::new(
                "ev_order_totals",
                Kind::Incremental,
                "SELECT o.id, COUNT(i.line_no) AS n_lines, SUM(i.qty * i.price) AS total
                 FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.id",
            ),
        ];
        let mut c = db.conn().await?;
        exec(&mut c, "INSERT INTO orders (id, customer_id, status, created_at) VALUES (424242, 1, 'new', '2024-01-01 00:00:00')").await?;
        exec(&mut c, "INSERT INTO order_items (order_id, line_no, product_id, qty, price) VALUES (424242, 9000000, 1, 1, 1.00)").await?;
        exec(&mut c, "INSERT INTO payments (id, order_id, method, amount) VALUES (9000000, 424242, 'cash', 1.00)").await?;
        // A long window: 30 autocommit commits (each fsynced) must fit into it even on a busy host.
        let window = Duration::from_secs(10);
        let window_ms = window.as_millis().to_string();
        let (mut eng, _ids) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[
                ("LAZYVIEW_BATCH_WINDOW_MS", window_ms.as_str()),
                ("LAZYVIEW_POSITION_PERSIST_MS", "600000"),
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;

        let n_views = specs.len() as u64;
        for (round, one_txn) in [(0i64, true), (1, false)] {
            let stmts = hammer_statements(424242, 9_100_000 + round * 100, 9_100_000 + round * 100);
            let (d, took) = measure_writes_timed(&mut eng, &db, &stmts, one_txn).await?;
            let mode = if one_txn {
                "one transaction"
            } else {
                "30 autocommit transactions"
            };
            ctx.log(format!(
                "30 writes ({mode}) took {:.2}s: {}",
                took.as_secs_f64(),
                fmt_snapshot(&d)
            ));
            if took + Duration::from_secs(1) >= window {
                ctx.log(format!("WARNING: writes took {:.1}s, longer than the {window:?} window — dedup assertions skipped (host too slow)", took.as_secs_f64()));
                verify(
                    &ctx,
                    &mut eng,
                    &db,
                    &specs,
                    &format!("after 30 writes ({mode})"),
                )
                .await?;
                continue;
            }
            ensure!(
                d.units_refreshed == n_views,
                "{mode}: units_refreshed = {} (expected exactly 1 per view = {n_views}); delta {}",
                d.units_refreshed,
                fmt_snapshot(&d)
            );
            ensure!(
                d.units_changed == n_views,
                "{mode}: units_changed = {} (expected {n_views}); delta {}",
                d.units_changed,
                fmt_snapshot(&d)
            );
            ensure!(d.lookup_queries == 0, "{mode}: {} lookup queries although every table maps to the unit directly; delta {}", d.lookup_queries, fmt_snapshot(&d));
            ensure!(
                d.source_queries <= SOURCE_QUERIES_PER_VIEW_MAX * n_views,
                "{mode}: {} source queries; delta {}",
                d.source_queries,
                fmt_snapshot(&d)
            );
            ensure!(
                d.mv_queries <= MV_QUERIES_PER_VIEW_MAX * n_views,
                "{mode}: {} MV queries; delta {}",
                d.mv_queries,
                fmt_snapshot(&d)
            );
            ensure!(
                d.full_rebuilds == 0,
                "{mode}: full rebuilds {}",
                d.full_rebuilds
            );
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after 30 writes ({mode})"),
            )
            .await?;
        }
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn efficiency_unreferenced_columns_cost_nothing() {
    scenario("eff_unref", |ctx| async move {
        let db = ctx.new_db("eff_unref").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
        let specs = ecommerce_views();
        let (mut eng, _ids) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[
                ("LAZYVIEW_BATCH_WINDOW_MS", "100"),
                ("LAZYVIEW_POSITION_PERSIST_MS", "600000"),
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;

        let mut stmts = Vec::new();
        for k in 0..3 {
            stmts.push(format!(
                "UPDATE customers SET last_seen_at = '2024-05-0{} 10:00:00.{k:06}'",
                k + 1
            ));
            stmts.push(format!(
                "UPDATE orders SET note = CONCAT('note ', id, ' {k}')"
            ));
            stmts.push(format!(
                "UPDATE products SET updated_at = '2024-05-0{} 11:00:00.{k:03}'",
                k + 1
            ));
            stmts.push(format!("UPDATE regions SET notes = 'n{k}'"));
            stmts.push(format!(
                "UPDATE customers SET last_seen_at = NULL WHERE id % 3 = {k}"
            ));
        }
        for one_txn in [true, false] {
            let d = measure_writes(&mut eng, &db, &stmts, one_txn).await?;
            ctx.log(format!(
                "unreferenced-column updates (one_txn={one_txn}): {}",
                fmt_snapshot(&d)
            ));
            ensure!(
                d.source_queries == 0
                    && d.lookup_queries == 0
                    && d.mv_queries == 0
                    && d.units_refreshed == 0
                    && d.full_rebuilds == 0,
                "updates of unreferenced columns caused work: {}",
                fmt_snapshot(&d)
            );
            ensure!(
                d.rows_seen > 0 || d.events > 0,
                "engine saw no events at all — measurement is meaningless: {}",
                fmt_snapshot(&d)
            );
        }
        verify(&ctx, &mut eng, &db, &specs, "after unreferenced updates").await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn efficiency_one_row_fans_out_once_per_view() {
    scenario("eff_fanout", |ctx| async move {
        let db = ctx.new_db("eff_fanout").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let specs = vec![
            ViewSpec::new("pv_filter", Kind::Incremental, "SELECT id, sku, price FROM products WHERE price > 0"),
            ViewSpec::new("pv_expr", Kind::Incremental, "SELECT id, UPPER(name) AS uname, price * 2 AS dbl FROM products"),
            ViewSpec::new(
                "pv_by_category",
                Kind::Incremental,
                "SELECT category, COUNT(*) AS n, SUM(price) AS total FROM products GROUP BY category",
            ),
            ViewSpec::new("pv_global", Kind::Incremental, "SELECT COUNT(*) AS n, MAX(price) AS max_price, SUM(price) AS total FROM products"),
            ViewSpec::new(
                "pv_stock",
                Kind::Incremental,
                "SELECT p.id, MAX(p.price) AS price, SUM(inv.qty) AS qty FROM products p LEFT JOIN inventory inv ON inv.product_id = p.id GROUP BY p.id",
            ),
        ];
        let (mut eng, _ids) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[
                ("LAZYVIEW_BATCH_WINDOW_MS", "200"),
                ("LAZYVIEW_POSITION_PERSIST_MS", "600000"),
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let n = specs.len() as u64;
        for (k, price) in ["123456.78", "0.01", "777.77"].iter().enumerate() {
            let d = measure_writes(
                &mut eng,
                &db,
                &[format!("UPDATE products SET price = {price} WHERE id = 2")],
                false,
            )
            .await?;
            ctx.log(format!("price change #{k}: {}", fmt_snapshot(&d)));
            ensure!(d.units_refreshed == n, "one product row feeds {n} views; units_refreshed = {} (expected exactly {n}); delta {}", d.units_refreshed, fmt_snapshot(&d));
            ensure!(
                d.units_changed == n,
                "units_changed = {} (expected {n}); delta {}",
                d.units_changed,
                fmt_snapshot(&d)
            );
            ensure!(
                d.source_queries <= SOURCE_QUERIES_PER_VIEW_MAX * n,
                "source queries {}; delta {}",
                d.source_queries,
                fmt_snapshot(&d)
            );
            ensure!(
                d.mv_queries <= MV_QUERIES_PER_VIEW_MAX * n,
                "MV queries {}; delta {}",
                d.mv_queries,
                fmt_snapshot(&d)
            );
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after price change #{k}"),
            )
            .await?;
        }
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn efficiency_output_neutral_updates_write_nothing() {
    scenario("eff_noop", |ctx| async move {
        let db = ctx.new_db("eff_noop").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "UPDATE products SET price = 10.00, category = 'books', active = 1 WHERE id = 1",
        )
        .await?;
        let specs = vec![
            ViewSpec::new(
                "nv_skus",
                Kind::Incremental,
                "SELECT id, sku FROM products WHERE price > 5",
            ),
            ViewSpec::new(
                "nv_cat_counts",
                Kind::Incremental,
                "SELECT category, COUNT(*) AS n FROM products GROUP BY category",
            ),
            ViewSpec::new(
                "nv_active",
                Kind::Incremental,
                "SELECT active, COUNT(*) AS n FROM products GROUP BY active",
            ),
        ];
        let (mut eng, _ids) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[
                ("LAZYVIEW_BATCH_WINDOW_MS", "2000"),
                ("LAZYVIEW_POSITION_PERSIST_MS", "600000"),
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;

        let cases: Vec<(&str, Vec<String>, bool)> = vec![
            (
                "filter column changes but stays inside the filter",
                vec!["UPDATE products SET price = 12.00 WHERE id = 1".into()],
                true,
            ),
            (
                "value toggled out of the filter and back within one window",
                vec![
                    "UPDATE products SET price = 3.00 WHERE id = 1".into(),
                    "UPDATE products SET price = 12.00 WHERE id = 1".into(),
                ],
                true,
            ),
            (
                "group key moved away and back within one window",
                vec![
                    "UPDATE products SET category = 'zz-temp' WHERE id = 1".into(),
                    "UPDATE products SET category = 'books' WHERE id = 1".into(),
                ],
                true,
            ),
            (
                "group flag toggled twice in one transaction",
                vec![
                    "UPDATE products SET active = 0 WHERE id = 1".into(),
                    "UPDATE products SET active = 1 WHERE id = 1".into(),
                ],
                false,
            ),
            (
                "identical update repeated (no row change at all)",
                vec![
                    "UPDATE products SET price = 12.00 WHERE id = 1".into(),
                    "UPDATE products SET price = price".into(),
                ],
                false,
            ),
        ];
        for (what, stmts, autocommit) in cases {
            let d = measure_writes(&mut eng, &db, &stmts, !autocommit).await?;
            ctx.log(format!("{what}: {}", fmt_snapshot(&d)));
            ensure!(
                d.units_changed == 0 && d.mv_rows_inserted == 0 && d.mv_rows_deleted == 0,
                "{what}: view output unchanged but MV rows were written: {}",
                fmt_snapshot(&d)
            );
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn efficiency_big_transaction() {
    scenario("eff_bigtxn", |ctx| async move {
        let db = ctx.new_db("eff_bigtxn").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let keep = [
            "v_order_lines",
            "v_line_details",
            "v_customer_stats",
            "v_category_sales",
            "v_global_orders",
            "v_orders_left",
            "v_vip_stats",
        ];
        let specs: Vec<ViewSpec> = ecommerce_views()
            .into_iter()
            .filter(|v| keep.contains(&v.name.as_str()))
            .collect();
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "200")]).await?;

        let n_rows = scaled(20_000) as i64;
        let per_order = 10i64;
        let n_orders = n_rows / per_order;
        let base = 1_000_000i64;
        let mut stmts = Vec::new();
        let orders: Vec<String> = (0..n_orders)
            .map(|k| {
                format!(
                    "({}, {}, 'paid', '2024-02-{:02} 10:00:00.{:06}')",
                    base + k,
                    1 + k % 8,
                    1 + k % 28,
                    k
                )
            })
            .collect();
        for ch in orders.chunks(1000) {
            stmts.push(format!(
                "INSERT INTO orders (id, customer_id, status, created_at) VALUES {}",
                ch.join(", ")
            ));
        }
        let items: Vec<String> = (0..n_rows)
            .map(|k| {
                format!(
                    "({}, {}, {}, {}, {}.{:02})",
                    base + k / per_order,
                    base + k,
                    1 + k % 6,
                    1 + k % 5,
                    1 + k % 90,
                    k % 100
                )
            })
            .collect();
        for ch in items.chunks(1000) {
            stmts.push(format!(
                "INSERT INTO order_items (order_id, line_no, product_id, qty, price) VALUES {}",
                ch.join(", ")
            ));
        }
        let steps: Vec<(String, Vec<String>)> = vec![
            (format!("insert {n_rows} rows in one transaction"), stmts),
            (format!("update {n_rows} rows in one statement"), vec![format!("UPDATE order_items SET qty = qty + 1, price = price + 1 WHERE order_id >= {base}")]),
            (
                format!("move {n_orders} orders between customer groups"),
                vec![format!("UPDATE orders SET customer_id = 1 + (id % 5), status = IF(id % 7 = 0, 'cancelled', status) WHERE id >= {base}")],
            ),
            (
                "delete half of the rows".into(),
                vec![format!("DELETE FROM order_items WHERE order_id >= {base} AND line_no % 2 = 0")],
            ),
            (
                "delete everything again".into(),
                vec![format!("DELETE FROM order_items WHERE order_id >= {base}"), format!("DELETE FROM orders WHERE id >= {base}")],
            ),
        ];
        for (k, (what, stmts)) in steps.into_iter().enumerate() {
            let t0 = std::time::Instant::now();
            let d = measure_writes(&mut eng, &db, &stmts, true).await?;
            ctx.log(format!(
                "{what}: applied in {:.1}s, {}",
                t0.elapsed().as_secs_f64(),
                fmt_snapshot(&d)
            ));
            if k == 0 {
                ensure!(
                    d.mv_rows_inserted >= n_rows as u64,
                    "only {} MV rows inserted for {n_rows} new lines",
                    d.mv_rows_inserted
                );
            }
            verify(&ctx, &mut eng, &db, &specs, &what).await?;
        }
        eng.stop().await?;
        Ok(())
    });
}

/// `LAZYVIEW_FULL_REFRESH_MS`: a burst of changes to the sources of full-refresh views causes at
/// most about one rebuild per view per interval, and the views converge afterwards.
#[test]
fn efficiency_full_refresh_cadence() {
    scenario("eff_cadence", |ctx| async move {
        let db = ctx.new_db("eff_cadence").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        // Full-refresh copies of the workload's v_distinct_methods / v_events_big: DISTINCT and
        // PK-less sources are maintained incrementally now, so NOW() (non-deterministic) keeps
        // these plans Full while the always-true predicate leaves the rows as in the workload.
        let specs = vec![
            ViewSpec::new(
                "v_distinct_methods",
                Kind::Full,
                "SELECT DISTINCT method, status FROM payments WHERE NOW() > '2000-01-01'",
            ),
            ViewSpec::new(
                "v_events_big",
                Kind::Full,
                "SELECT customer_id, kind, amount FROM events_log WHERE amount > 50 AND NOW() > '2000-01-01'",
            ),
        ];
        let interval = Duration::from_millis(3000);
        let interval_ms = interval.as_millis().to_string();
        let (mut eng, _ids) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[
                ("LAZYVIEW_FULL_REFRESH_MS", interval_ms.as_str()),
                ("LAZYVIEW_BATCH_WINDOW_MS", "50"),
            ],
        )
        .await?;
        let none = std::collections::HashMap::new();
        let converge = Duration::from_secs(60);
        eventually_equal(&ctx, &mut eng, &db, &none, &specs, converge, "initial").await?;
        quiesce(&mut eng, &db).await?;
        tokio::time::sleep(interval).await;

        let mut c = db.conn().await?;
        let before = eng.snapshot();
        let t0 = std::time::Instant::now();
        let methods = ["card", "paypal", "wire", "cash"];
        let statuses = ["ok", "pending", "failed", "refund", "void"];
        for k in 0..40usize {
            exec(
                &mut c,
                &format!(
                    "INSERT INTO payments (id, order_id, method, amount, status) VALUES ({}, 1, '{}', 10.00, '{}')",
                    9_500_000 + k,
                    methods[k % 4],
                    statuses[k % 5]
                ),
            )
            .await?;
            exec(
                &mut c,
                &format!("INSERT INTO events_log (customer_id, kind, amount, happened_at) VALUES ({}, 'promo', {}.00, '2024-01-01 00:00:00')", k % 5, 60 + k),
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        let burst = t0.elapsed();
        eventually_equal(
            &ctx,
            &mut eng,
            &db,
            &none,
            &specs,
            converge,
            "after the burst",
        )
        .await?;
        let total = t0.elapsed();
        let d = eng.snapshot().since(&before);
        let per_view_max = (total.as_millis() / interval.as_millis()) as u64 + 1;
        let max = per_view_max * specs.len() as u64;
        ctx.log(format!(
            "burst {:.1}s, converged after {:.1}s: {} full rebuilds (max {max}); {}",
            burst.as_secs_f64(),
            total.as_secs_f64(),
            d.full_rebuilds,
            fmt_snapshot(&d)
        ));
        ensure!(
            d.full_rebuilds >= 1,
            "the burst changed both views but no full rebuild ran: {}",
            fmt_snapshot(&d)
        );
        ensure!(
            d.full_rebuilds <= max,
            "{} full rebuilds within {:.1}s exceed one per view per {interval:?} ({max}): {}",
            d.full_rebuilds,
            total.as_secs_f64(),
            fmt_snapshot(&d)
        );
        eng.stop().await?;
        Ok(())
    });
}
