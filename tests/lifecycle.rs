//! Lifecycle: bootstrap of metadata tables, live definition changes (insert / update / disable /
//! re-enable / delete), errors (invalid SQL, missing table, unknown garbage), refusing to clobber
//! foreign tables, start modes (resume / rebuild / once / LAZYVIEW_SYNC=false) and crash
//! consistency.

mod common;

use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use common::oracle::fingerprint;
use common::workload::{ecommerce_views, names, run_batches, run_concurrent, setup_ecommerce, views_of, Sizes, Workload};
use common::*;
use rand::RngExt;

fn spec_named(name: &str) -> ViewSpec {
    ecommerce_views().into_iter().find(|v| v.name == name).expect("known view")
}

async fn wait_table(conn: &mut mysql_async::Conn, engine: &mut Engine, table: &str, exists: bool, timeout: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        if table_exists(conn, table).await? == exists {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!("table {table} {} within {timeout:?}", if exists { "did not appear" } else { "was not dropped" });
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn lifecycle_bootstrap_and_live_definitions() {
    scenario("lc_live", |ctx| async move {
        let db = ctx.new_db("lc_live").await?;
        let mut c = db.conn().await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;

        // Bootstrap: metadata tables are created on an empty schema.
        let t = Duration::from_secs(60);
        for table in ["_lazyview", "_lazyview_state", "_lazyview_status"] {
            wait_table(&mut c, &mut eng, table, true, t).await.with_context(|| format!("bootstrap {table}"))?;
        }
        let cols: Vec<String> = rows(
            &mut c,
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_lazyview'",
        )
        .await?
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
        for needed in ["id", "definition", "enabled", "targets"] {
            ensure!(cols.iter().any(|c| c.eq_ignore_ascii_case(needed)), "_lazyview lacks column {needed}: {cols:?}");
        }
        wait_until("_lazyview_status row for instance 'test'", t, || {
            let dsn = db.dsn.clone();
            async move {
                let mut c = connect(&dsn).await?;
                let n = scalar_i64(&mut c, "SELECT COUNT(*) FROM _lazyview_status WHERE instance_id = 'test' AND binlog_file <> ''").await?;
                c.disconnect().await.ok();
                Ok(n == 1)
            }
        })
        .await?;
        ctx.log("bootstrap ok");

        // Sources appear while streaming, then definitions are inserted live.
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
        let mut live = vec![spec_named("v_paid_orders"), spec_named("v_order_lines"), spec_named("v_customer_stats")];
        let mut idmap = add_views(&mut c, &live).await?;
        wait_ready(&mut c, &mut eng, &names(&live), t_ready()).await?;
        // A chained view added once its upstream is live.
        let chained = vec![spec_named("v_vip_stats")];
        idmap.extend(add_views(&mut c, &chained).await?);
        wait_ready(&mut c, &mut eng, &names(&chained), t_ready()).await?;
        live.extend(chained);
        verify(&ctx, &mut eng, &db, &live, "inserted while streaming").await?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
        run_batches(&mut c, &mut wl, scaled(60)).await?;
        verify(&ctx, &mut eng, &db, &live, "after writes").await?;

        // Update a definition: different columns and filter → rebuilt, hash changes.
        let old_hash = view_state(&mut c, "v_paid_orders").await?.first().and_then(|s| s.definition_hash.clone());
        let new_spec = ViewSpec::new(
            "v_paid_orders",
            Kind::Incremental,
            "SELECT o.id, o.status, o.customer_id, c.name AS customer FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.status IN ('new','refunded')",
        );
        exec(&mut c, &format!("UPDATE _lazyview SET definition = {} WHERE id = {}", q(&new_spec.definition), idmap["v_paid_orders"])).await?;
        live[0] = new_spec.clone();
        wait_until("v_paid_orders rebuilt with the new definition", t_ready(), || {
            let dsn = db.dsn.clone();
            let old = old_hash.clone();
            async move {
                let mut c = connect(&dsn).await?;
                let st = view_state(&mut c, "v_paid_orders").await?;
                c.disconnect().await.ok();
                Ok(st.iter().all(|s| s.state == "ready" && s.definition_hash != old) && !st.is_empty())
            }
        })
        .await?;
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        verify(&ctx, &mut eng, &db, &live, "after definition update").await?;

        // Disable → dropped; re-enable → rebuilt.
        exec(&mut c, &format!("UPDATE _lazyview SET enabled = 0 WHERE id = {}", idmap["v_order_lines"])).await?;
        wait_table(&mut c, &mut eng, "v_order_lines", false, t_ready()).await.context("disable")?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        exec(&mut c, &format!("UPDATE _lazyview SET enabled = 1 WHERE id = {}", idmap["v_order_lines"])).await?;
        wait_table(&mut c, &mut eng, "v_order_lines", true, t_ready()).await.context("re-enable")?;
        wait_ready(&mut c, &mut eng, &["v_order_lines"], t_ready()).await?;
        verify(&ctx, &mut eng, &db, &live, "after disable + re-enable").await?;

        // Delete (default LAZYVIEW_DROP_REMOVED=true) → dropped.
        exec(&mut c, &format!("DELETE FROM _lazyview WHERE id = {}", idmap["v_vip_stats"])).await?;
        wait_table(&mut c, &mut eng, "v_vip_stats", false, t_ready()).await.context("delete v_vip_stats")?;
        live.retain(|s| s.name != "v_vip_stats");
        idmap.remove("v_vip_stats");

        // Errors: invalid SQL, missing table, total garbage — the others keep working.
        let bad_sql = add_definition(&mut c, "CREATE VIEW v_bad_sql AS SELEC id FRM orders WHERE", None).await?;
        let missing = add_definition(&mut c, "CREATE VIEW v_missing AS SELECT id, label FROM no_such_table", None).await?;
        add_definition(&mut c, "this is not sql at all", None).await?;
        add_definition(&mut c, "", None).await?;
        for name in ["v_bad_sql", "v_missing"] {
            let st = wait_state(&mut c, &mut eng, name, "error", t_ready()).await?;
            let msg = st.last_error.unwrap_or_default();
            ensure!(!msg.trim().is_empty(), "{name}: state=error but last_error is empty");
            ctx.log(format!("{name}: error as expected: {}", clip(&msg, 160)));
            ensure!(!table_exists(&mut c, name).await?, "{name}: MV table created despite error");
        }
        if missing_msg_lacks_table(&mut c).await? {
            ctx.log("note: v_missing's error message does not mention the missing table");
        }
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        verify(&ctx, &mut eng, &db, &live, "other views unaffected by broken definitions").await?;
        assert_ready(&mut c, &names(&live)).await?;

        // Fixing a broken definition makes it ready.
        let fixed = ViewSpec::new("v_missing", Kind::Incremental, "SELECT id, status AS label FROM orders WHERE id % 3 = 0");
        exec(&mut c, &format!("UPDATE _lazyview SET definition = {} WHERE id = {missing}", q(&fixed.definition))).await?;
        exec(&mut c, &format!("UPDATE _lazyview SET enabled = 0 WHERE id = {bad_sql}")).await?;
        live.push(fixed);
        wait_ready_tolerant(&mut c, &mut eng, &["v_missing"], t_ready()).await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &live, "after fixing the broken definition").await?;
        eng.stop().await?;
        Ok(())
    });
}

async fn missing_msg_lacks_table(c: &mut mysql_async::Conn) -> Result<bool> {
    let st = view_state(c, "v_missing").await?;
    Ok(!st.iter().any(|s| s.last_error.as_deref().unwrap_or("").contains("no_such_table")))
}

#[test]
fn lifecycle_refuses_foreign_tables() {
    scenario("lc_foreign", |ctx| async move {
        let db = ctx.new_db("lc_foreign").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        exec(&mut c, "CREATE TABLE v_conflict (id INT PRIMARY KEY, payload VARCHAR(20))").await?;
        exec(&mut c, "INSERT INTO v_conflict VALUES (1, 'precious'), (2, 'data')").await?;
        ensure_lazyview_table(&mut c).await?;
        add_definition(&mut c, "CREATE VIEW v_conflict AS SELECT id, status FROM orders", None).await?;
        let good = vec![spec_named("v_paid_orders"), spec_named("v_status_counts")];
        add_views(&mut c, &good).await?;
        let fp_conflict = fingerprint(&mut c, "v_conflict").await?;
        let fp_orders = fingerprint(&mut c, "orders").await?;

        let mut eng = ctx.start_engine(&db, &[])?;
        let st = wait_state(&mut c, &mut eng, "v_conflict", "error", t_ready()).await?;
        ctx.log(format!("v_conflict: {}", clip(&st.last_error.unwrap_or_default(), 160)));
        wait_ready(&mut c, &mut eng, &names(&good), t_ready()).await?;

        // While streaming: a definition named like a *source* table must not touch it either.
        add_definition(&mut c, "CREATE VIEW orders AS SELECT id FROM customers", None).await?;
        wait_state(&mut c, &mut eng, "orders", "error", t_ready()).await?;
        exec(&mut c, "UPDATE customers SET name = 'renamed' WHERE id = 1").await?;
        verify(&ctx, &mut eng, &db, &good, "good views next to conflicting ones").await?;
        ensure!(fingerprint(&mut c, "v_conflict").await? == fp_conflict, "pre-existing table v_conflict was modified");
        ensure!(fingerprint(&mut c, "orders").await? == fp_orders, "source table orders was modified");
        ensure!(table_type(&mut c, "orders").await?.as_deref() == Some("BASE TABLE"), "orders replaced");
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn lifecycle_drop_removed_false_keeps_table() {
    scenario("lc_keep", |ctx| async move {
        let db = ctx.new_db("lc_keep").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        let specs = vec![spec_named("v_paid_orders"), spec_named("v_global_orders")];
        let ids = add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_DROP_REMOVED", "false")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        exec(&mut c, &format!("DELETE FROM _lazyview WHERE id = {}", ids["v_paid_orders"])).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        ensure!(table_exists(&mut c, "v_paid_orders").await?, "MV dropped although LAZYVIEW_DROP_REMOVED=false");
        verify(&ctx, &mut eng, &db, &specs[1..], "remaining view").await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn lifecycle_start_modes() {
    scenario("lc_modes", |ctx| async move {
        let db = ctx.new_db("lc_modes").await?;
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
        // Only incrementally maintained views: a resume must not need any full rebuild.
        let specs = views_of(&[Kind::Incremental]);
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
        let ov = [("LAZYVIEW_POSITION_PERSIST_MS", "300")];

        // First start (resume mode, nothing saved yet → builds everything).
        let mut eng = ctx.start_engine(&db, &ov)?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        verify(&ctx, &mut eng, &db, &specs, "first run").await?;
        eng.stop().await?;

        // Resume after more writes: catches up from the stored position without rebuilding.
        run_batches(&mut c, &mut wl, scaled(60)).await?;
        let mut eng = ctx.start_engine(&db, &ov)?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "resume caught up offline writes").await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &specs, "resume streaming").await?;
        let snap = eng.snapshot();
        ensure!(snap.full_rebuilds == 0, "resume performed {} full rebuild(s); metrics {}", snap.full_rebuilds, fmt_snapshot(&snap));
        eng.stop().await?;

        // Rebuild: tamper with MVs while stopped; rebuild mode must repair them.
        exec(&mut c, "DELETE FROM v_status_counts").await?;
        exec(&mut c, "UPDATE v_order_lines SET qty = -999").await?;
        exec(&mut c, "INSERT INTO v_customer_regions (id, name) VALUES (-1, 'bogus')").await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_START_MODE", "rebuild")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "rebuild repaired tampered MVs").await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &specs, "rebuild then streaming").await?;
        ctx.log(format!("rebuild metrics: {}", fmt_snapshot(&eng.snapshot())));
        eng.stop().await?;

        // Once: refresh everything and exit by itself.
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        let eng = ctx.start_engine(&db, &[("LAZYVIEW_START_MODE", "once")])?;
        match eng.wait_finished(t_ready()).await? {
            Ok(()) => {}
            Err(e) => bail!("once mode did not exit cleanly: {e}"),
        }
        check_views(&db, &specs).await.context("after once")?;
        assert_ready(&mut c, &names(&specs)).await?;

        // LAZYVIEW_SYNC=false: creates metadata and exits; on a fresh schema too.
        let eng = ctx.start_engine(&db, &[("LAZYVIEW_SYNC", "false")])?;
        match eng.wait_finished(Duration::from_secs(60)).await? {
            Ok(()) => {}
            Err(e) => bail!("LAZYVIEW_SYNC=false did not exit cleanly: {e}"),
        }
        let empty = ctx.new_db("lc_modes_nosync").await?;
        let eng = ctx.start_engine(&empty, &[("LAZYVIEW_SYNC", "false")])?;
        match eng.wait_finished(Duration::from_secs(60)).await? {
            Ok(()) => {}
            Err(e) => bail!("LAZYVIEW_SYNC=false (fresh schema) did not exit cleanly: {e}"),
        }
        let mut ec = empty.conn().await?;
        for t in ["_lazyview", "_lazyview_state", "_lazyview_status"] {
            ensure!(table_exists(&mut ec, t).await?, "LAZYVIEW_SYNC=false did not create {t}");
        }
        Ok(())
    });
}

#[test]
fn lifecycle_crash_consistency() {
    scenario("lc_crash", |ctx| async move {
        let db = ctx.new_db("lc_crash").await?;
        let seed = ctx.sub_seed("seed");
        let ids = setup_ecommerce(&db, seed, Sizes::scaled()).await?;
        let all = ecommerce_views();
        let (chained, base): (Vec<ViewSpec>, Vec<ViewSpec>) = all.iter().cloned().partition(|s| s.kind == Kind::Chained);
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &base).await?;
        let mut rng = workload::Gen::new(derive_seed(seed, "crash"));
        let windows = ["0", "50", "200"];
        let persist = ["0", "100", "1000"];
        let ov_for = |rng: &mut workload::Gen| -> Vec<(&'static str, &'static str)> {
            vec![("LAZYVIEW_BATCH_WINDOW_MS", *rng.pick(&windows)), ("LAZYVIEW_POSITION_PERSIST_MS", *rng.pick(&persist))]
        };

        // Round 0 crashes right from the start (also during the initial build); round 1 adds the
        // chained views first.
        let mut eng = ctx.start_engine(&db, &ov_for(&mut rng))?;
        let mut specs = base.clone();
        for round in 0..2 {
            if round == 1 {
                wait_ready(&mut c, &mut eng, &names(&base), t_ready()).await?;
                add_views(&mut c, &chained).await?;
                wait_ready(&mut c, &mut eng, &names(&chained), t_ready()).await?;
                specs = all.clone();
            }
            let (dbc, idc) = (db.clone(), ids.clone());
            let wseed = derive_seed(seed, &format!("round{round}"));
            let writers = tokio::spawn(async move { run_concurrent(&dbc, idc, wseed, 3, scaled(80)).await });
            let mut crashes = 0;
            while !writers.is_finished() && crashes < 4 {
                // Crash at arbitrary points: during builds, mid-flush, between flushes.
                let wait = rng.rng.random_range(150..1500u64);
                tokio::time::sleep(Duration::from_millis(wait)).await;
                eng.check_alive()?;
                eng.crash().await?;
                crashes += 1;
                tokio::time::sleep(Duration::from_millis(rng.rng.random_range(0..400u64))).await;
                eng = ctx.start_engine(&db, &ov_for(&mut rng))?;
            }
            let n = writers.await.context("writers panicked")??;
            ctx.log(format!("round {round}: {crashes} crashes during {n} txns"));
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(&ctx, &mut eng, &db, &specs, &format!("round {round}: after {crashes} crashes")).await?;
        }

        // One more crash right after a burst (likely mid-flush), then a final restart.
        let mut wl = Workload::new(derive_seed(seed, "tail"), ids.clone());
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        tokio::time::sleep(Duration::from_millis(rng.rng.random_range(0..100u64))).await;
        eng.crash().await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after final crash + restart").await?;
        eng.stop().await?;
        Ok(())
    });
}

/// Chained views whose definitions arrive together with (or before) their upstream view — at
/// startup and while streaming — must still be built (in dependency order) and become ready.
#[test]
fn lifecycle_chained_definitions_together() {
    scenario("lc_chain", |ctx| async move {
        let db = ctx.new_db("lc_chain").await?;
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        // Startup: chained view inserted BEFORE its upstream.
        let first = vec![spec_named("v_vip_stats"), spec_named("v_customer_stats"), spec_named("v_tier_revenue")];
        add_views(&mut c, &first).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&first), t_ready()).await.context("chained definitions present at startup")?;
        verify(&ctx, &mut eng, &db, &first, "chained views built at startup").await?;
        // Streaming: upstream + chained inserted in one transaction (chained first).
        let more = vec![
            ViewSpec::new("v_vip2", Kind::Chained, "SELECT customer_id, revenue FROM v_cs2 WHERE revenue > 100"),
            ViewSpec::new(
                "v_cs2",
                Kind::Incremental,
                "SELECT o.customer_id, SUM(i.qty * i.price) AS revenue FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.customer_id",
            ),
        ];
        exec(&mut c, "START TRANSACTION").await?;
        for s in &more {
            add_definition(&mut c, &s.definition, None).await?;
        }
        exec(&mut c, "COMMIT").await?;
        wait_ready(&mut c, &mut eng, &names(&more), t_ready()).await.context("chained definitions inserted together while streaming")?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids);
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        let mut all = first.clone();
        all.extend(more);
        verify(&ctx, &mut eng, &db, &all, "chained views after writes").await?;
        eng.stop().await?;
        Ok(())
    });
}

async fn http_get(addr: &str, path: &str) -> Result<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.with_context(|| format!("connect {addr}"))?;
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text.split_whitespace().nth(1).and_then(|v| v.parse().ok()).with_context(|| format!("bad HTTP response: {text:?}"))?;
    let body = text.split_once("\r\n\r\n").map(|x| x.1.to_string()).unwrap_or_default();
    Ok((status, body))
}

/// Health endpoint: /healthz 200 while alive, /readyz 200 once all views are ready, /metrics in
/// Prometheus format with the engine counters.
#[test]
fn lifecycle_health_endpoint() {
    scenario("lc_health", |ctx| async move {
        let db = ctx.new_db("lc_health").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let specs = vec![spec_named("v_paid_orders"), spec_named("v_status_counts")];
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let addr = format!("127.0.0.1:{port}");
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_HEALTH_ADDR", addr.as_str())])?;
        wait_until("health endpoint listening", Duration::from_secs(60), || {
            let addr = addr.clone();
            async move { Ok(http_get(&addr, "/healthz").await.map(|r| r.0 == 200).unwrap_or(false)) }
        })
        .await?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id % 2 = 0").await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        wait_until("/readyz 200 once all views are ready", Duration::from_secs(60), || {
            let addr = addr.clone();
            async move { Ok(http_get(&addr, "/readyz").await.map(|r| r.0 == 200).unwrap_or(false)) }
        })
        .await?;
        let (st, body) = http_get(&addr, "/metrics").await?;
        ensure!(st == 200, "/metrics status {st}");
        ensure!(body.contains("lazyview_units_refreshed_total"), "/metrics lacks lazyview_units_refreshed_total:\n{}", clip(&body, 2000));
        let (st, _) = http_get(&addr, "/nope").await?;
        ensure!(st == 404, "unknown path status {st}");
        // A view in error state must make /readyz fail.
        add_definition(&mut c, "CREATE VIEW v_broken AS SELECT nope FROM no_such_table", None).await?;
        wait_state(&mut c, &mut eng, "v_broken", "error", t_ready()).await?;
        wait_until("/readyz 503 while a view is failing", Duration::from_secs(30), || {
            let addr = addr.clone();
            async move { Ok(http_get(&addr, "/readyz").await.map(|r| r.0 == 503).unwrap_or(false)) }
        })
        .await?;
        let (st, _) = http_get(&addr, "/healthz").await?;
        ensure!(st == 200, "/healthz must stay 200 with a failing view, got {st}");
        eng.stop().await?;
        Ok(())
    });
}

