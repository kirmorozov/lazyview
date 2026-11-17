//! Lifecycle: bootstrap of metadata tables, live definition changes (insert / update / disable /
//! re-enable / delete), errors (invalid SQL, missing table, unknown garbage), refusing to clobber
//! foreign tables, start modes (resume / rebuild / once / LAZYVIEW_SYNC=false), crash
//! consistency, and binlog compression (each family runs its own: MySQL compressed transactions
//! end to end, MariaDB's `log_bin_compress` refused and reported mid-stream).

mod common;

use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use common::oracle::fingerprint;
use common::workload::{
    ecommerce_views, names, run_batches, run_concurrent, setup_ecommerce, views_of, Sizes, Workload,
};
use common::*;
use rand::RngExt;

fn spec_named(name: &str) -> ViewSpec {
    ecommerce_views()
        .into_iter()
        .find(|v| v.name == name)
        .expect("known view")
}

async fn wait_table(
    conn: &mut mysql_async::Conn,
    engine: &mut Engine,
    table: &str,
    exists: bool,
    timeout: Duration,
) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        if table_exists(conn, table).await? == exists {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!(
                "table {table} {} within {timeout:?}",
                if exists {
                    "did not appear"
                } else {
                    "was not dropped"
                }
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls an engine's log file until a line contains `needle`, returning its text on the first
/// hit (for a mid-stream condition the test waits to see reported; `chaos.rs` has the same shape).
async fn wait_log_contains(
    log: &std::path::Path,
    needle: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(text) = std::fs::read_to_string(log) {
            if text.contains(needle) {
                return Ok(text);
            }
        }
        ensure!(
            std::time::Instant::now() <= deadline,
            "no {needle:?} line in the engine log {} within {timeout:?}",
            log.display()
        );
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
            wait_table(&mut c, &mut eng, table, true, t)
                .await
                .with_context(|| format!("bootstrap {table}"))?;
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
            ensure!(
                cols.iter().any(|c| c.eq_ignore_ascii_case(needed)),
                "_lazyview lacks column {needed}: {cols:?}"
            );
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
        let mut live = vec![
            spec_named("v_paid_orders"),
            spec_named("v_order_lines"),
            spec_named("v_customer_stats"),
        ];
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
        let old_hash = view_state(&mut c, "v_paid_orders")
            .await?
            .first()
            .and_then(|s| s.definition_hash.clone());
        let new_spec = ViewSpec::new(
            "v_paid_orders",
            Kind::Incremental,
            "SELECT o.id, o.status, o.customer_id, c.name AS customer FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.status IN ('new','refunded')",
        );
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {}",
                q(&new_spec.definition),
                idmap["v_paid_orders"]
            ),
        )
        .await?;
        live[0] = new_spec.clone();
        wait_until(
            "v_paid_orders rebuilt with the new definition",
            t_ready(),
            || {
                let dsn = db.dsn.clone();
                let old = old_hash.clone();
                async move {
                    let mut c = connect(&dsn).await?;
                    let st = view_state(&mut c, "v_paid_orders").await?;
                    c.disconnect().await.ok();
                    Ok(st
                        .iter()
                        .all(|s| s.state == "ready" && s.definition_hash != old)
                        && !st.is_empty())
                }
            },
        )
        .await?;
        run_batches(&mut c, &mut wl, scaled(30)).await?;
        verify(&ctx, &mut eng, &db, &live, "after definition update").await?;

        // Disable → dropped; re-enable → rebuilt.
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET enabled = 0 WHERE id = {}",
                idmap["v_order_lines"]
            ),
        )
        .await?;
        wait_table(&mut c, &mut eng, "v_order_lines", false, t_ready())
            .await
            .context("disable")?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET enabled = 1 WHERE id = {}",
                idmap["v_order_lines"]
            ),
        )
        .await?;
        wait_table(&mut c, &mut eng, "v_order_lines", true, t_ready())
            .await
            .context("re-enable")?;
        wait_ready(&mut c, &mut eng, &["v_order_lines"], t_ready()).await?;
        verify(&ctx, &mut eng, &db, &live, "after disable + re-enable").await?;

        // Delete (default LAZYVIEW_DROP_REMOVED=true) → dropped.
        exec(
            &mut c,
            &format!("DELETE FROM _lazyview WHERE id = {}", idmap["v_vip_stats"]),
        )
        .await?;
        wait_table(&mut c, &mut eng, "v_vip_stats", false, t_ready())
            .await
            .context("delete v_vip_stats")?;
        live.retain(|s| s.name != "v_vip_stats");
        idmap.remove("v_vip_stats");

        // Errors: invalid SQL, missing table, total garbage — the others keep working.
        let bad_sql = add_definition(
            &mut c,
            "CREATE VIEW v_bad_sql AS SELEC id FRM orders WHERE",
            None,
        )
        .await?;
        let missing = add_definition(
            &mut c,
            "CREATE VIEW v_missing AS SELECT id, label FROM no_such_table",
            None,
        )
        .await?;
        let garbage = add_definition(&mut c, "this is not sql at all", None).await?;
        let empty = add_definition(&mut c, "", None).await?;
        let mut problems = Vec::new();
        for (what, id) in [
            ("invalid SQL", bad_sql),
            ("missing table", missing),
            ("garbage", garbage),
            ("empty definition", empty),
        ] {
            match wait_def_state(&mut c, &mut eng, id, "error", t_ready()).await {
                Ok(st) => {
                    let msg = st.last_error.clone().unwrap_or_default();
                    if msg.trim().is_empty() {
                        problems.push(format!(
                            "{what} (#{id}): state=error but last_error is empty"
                        ));
                    }
                    ctx.log(format!(
                        "{what} (#{id}) → state row '{}' error: {}",
                        st.name,
                        clip(&msg, 160)
                    ));
                }
                Err(e) => problems.push(format!("{what} (#{id}): {e:#}")),
            }
        }
        for name in ["v_bad_sql", "v_missing"] {
            if table_exists(&mut c, name).await? {
                problems.push(format!("{name}: MV table created despite the error"));
            }
        }
        if missing_msg_lacks_table(&mut c).await? {
            ctx.log("note: v_missing's error message does not mention the missing table");
        }
        ensure!(
            problems.is_empty(),
            "broken definitions not reported correctly:\n  {}",
            problems.join("\n  ")
        );
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &live,
            "other views unaffected by broken definitions",
        )
        .await?;
        assert_ready(&mut c, &names(&live)).await?;

        // Fixing a broken definition makes it ready.
        let fixed = ViewSpec::new(
            "v_missing",
            Kind::Incremental,
            "SELECT id, status AS label FROM orders WHERE id % 3 = 0",
        );
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {missing}",
                q(&fixed.definition)
            ),
        )
        .await?;
        exec(
            &mut c,
            &format!("UPDATE _lazyview SET enabled = 0 WHERE id = {bad_sql}"),
        )
        .await?;
        live.push(fixed);
        wait_ready_tolerant(&mut c, &mut eng, &["v_missing"], t_ready()).await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &live,
            "after fixing the broken definition",
        )
        .await?;
        eng.stop().await?;
        Ok(())
    });
}

async fn missing_msg_lacks_table(c: &mut mysql_async::Conn) -> Result<bool> {
    let st = view_state(c, "v_missing").await?;
    Ok(!st.iter().any(|s| {
        s.last_error
            .as_deref()
            .unwrap_or("")
            .contains("no_such_table")
    }))
}

#[test]
fn lifecycle_refuses_foreign_tables() {
    scenario("lc_foreign", |ctx| async move {
        let db = ctx.new_db("lc_foreign").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "CREATE TABLE v_conflict (id INT PRIMARY KEY, payload VARCHAR(20))",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO v_conflict VALUES (1, 'precious'), (2, 'data')",
        )
        .await?;
        ensure_lazyview_table(&mut c).await?;
        add_definition(
            &mut c,
            "CREATE VIEW v_conflict AS SELECT id, status FROM orders",
            None,
        )
        .await?;
        let good = vec![spec_named("v_paid_orders"), spec_named("v_status_counts")];
        add_views(&mut c, &good).await?;
        let fp_conflict = fingerprint(&mut c, "v_conflict").await?;
        let fp_orders = fingerprint(&mut c, "orders").await?;

        let mut eng = ctx.start_engine(&db, &[])?;
        let st = wait_state(&mut c, &mut eng, "v_conflict", "error", t_ready()).await?;
        ctx.log(format!(
            "v_conflict: {}",
            clip(&st.last_error.unwrap_or_default(), 160)
        ));
        wait_ready(&mut c, &mut eng, &names(&good), t_ready()).await?;

        // While streaming: a definition named like a *source* table must not touch it either.
        add_definition(
            &mut c,
            "CREATE VIEW orders AS SELECT id FROM customers",
            None,
        )
        .await?;
        wait_state(&mut c, &mut eng, "orders", "error", t_ready()).await?;
        exec(&mut c, "UPDATE customers SET name = 'renamed' WHERE id = 1").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &good,
            "good views next to conflicting ones",
        )
        .await?;
        ensure!(
            fingerprint(&mut c, "v_conflict").await? == fp_conflict,
            "pre-existing table v_conflict was modified"
        );
        ensure!(
            fingerprint(&mut c, "orders").await? == fp_orders,
            "source table orders was modified"
        );
        ensure!(
            table_type(&mut c, "orders").await?.as_deref() == Some("BASE TABLE"),
            "orders replaced"
        );
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
        exec(
            &mut c,
            &format!("DELETE FROM _lazyview WHERE id = {}", ids["v_paid_orders"]),
        )
        .await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        // Keep the fixed sleep: "nothing happened" is the assertion — the MV table must
        // still exist after the engine has had time to (wrongly) drop it.
        tokio::time::sleep(Duration::from_secs(2)).await;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        ensure!(
            table_exists(&mut c, "v_paid_orders").await?,
            "MV dropped although LAZYVIEW_DROP_REMOVED=false"
        );
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
        // Only incrementally maintained views: a resume must not need any full rebuild. Views over
        // children of cascading foreign keys are left out: parent deletes / PK updates trigger
        // (by design) an online rebuild of those.
        let specs: Vec<ViewSpec> = views_of(&[Kind::Incremental])
            .into_iter()
            .filter(|v| !workload::reads_fk_child(&v.name))
            .collect();
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
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "resume caught up offline writes",
        )
        .await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &specs, "resume streaming").await?;
        let snap = eng.snapshot();
        ensure!(
            snap.full_rebuilds == 0,
            "resume performed {} full rebuild(s); metrics {}",
            snap.full_rebuilds,
            fmt_snapshot(&snap)
        );
        eng.stop().await?;

        // Rebuild: tamper with MVs while stopped; rebuild mode must repair them.
        exec(&mut c, "DELETE FROM v_status_counts").await?;
        exec(&mut c, "UPDATE v_order_lines SET qty = -999").await?;
        exec(
            &mut c,
            "INSERT INTO v_customer_regions (id, name) VALUES (-1, 'bogus')",
        )
        .await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_START_MODE", "rebuild")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "rebuild repaired tampered MVs").await?;
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(&ctx, &mut eng, &db, &specs, "rebuild then streaming").await?;
        ctx.log(format!(
            "rebuild metrics: {}",
            fmt_snapshot(&eng.snapshot())
        ));
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
            ensure!(
                table_exists(&mut ec, t).await?,
                "LAZYVIEW_SYNC=false did not create {t}"
            );
        }
        Ok(())
    });
}

/// A start whose saved position cannot be read while its binlog file still exists (a position
/// past the end of the file, which the dump thread answers with 1236; a position saved on another
/// server) stops lazyview but leaves the `ready` state rows alone: only a purged file makes the
/// views stale for good (tests/exclusive.rs). Once the cause is undone, the next start resumes
/// without a rebuild.
#[test]
fn lifecycle_unreadable_position_keeps_ready_rows() {
    scenario("lc_unreadable", |ctx| async move {
        use mysql_async::prelude::Queryable;
        let db = ctx.new_db("lc_unreadable").await?;
        let mut c = db.conn().await?;
        for s in [
            "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
            "INSERT INTO entities VALUES (10, 1, 'new'), (11, 1, 'paid'), (12, 2, 'new')",
        ] {
            exec(&mut c, s).await?;
        }
        let specs = vec![ViewSpec::new(
            "lc_counts",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp",
        )];
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        eng.stop().await?;
        // The start reads the saved cursor (`binlog_cursor`, meta::load_position), so an
        // unreadable saved position has to be simulated in the cursor's own text; the two
        // pre-cursor columns move with it, as they do with every engine save.
        let (file, pos, cursor, identity): (String, u64, String, Option<String>) = c
            .query_first(
                "SELECT CAST(binlog_file AS CHAR), binlog_pos, CAST(binlog_cursor AS CHAR), \
                 CAST(server_identity AS CHAR) FROM _lazyview_status",
            )
            .await?
            .context("no saved position")?;
        ensure!(identity.is_some(), "no saved server identity");
        ensure!(
            !cursor.is_empty(),
            "the engine saved no cursor to make unreadable"
        );
        for (what, set, expect) in [
            (
                "a position past the end of its file",
                format!(
                    "UPDATE _lazyview_status SET binlog_pos = {}, binlog_cursor = '{}:{}'",
                    pos + 1_000_000_000,
                    file,
                    pos + 1_000_000_000
                ),
                "is no longer available",
            ),
            (
                "a position saved on another server",
                "UPDATE _lazyview_status SET server_identity = 'another-server'".to_string(),
                "was written by server",
            ),
        ] {
            exec(&mut c, &set).await?;
            let eng = ctx.start_engine(&db, &[])?;
            match eng.wait_finished(Duration::from_secs(60)).await? {
                Ok(()) => bail!("{what}: lazyview did not stop"),
                Err(e) => ensure!(e.contains(expect), "{what}: unexpected stop: {e}"),
            }
            let rows: Vec<(String, String)> = c
                .query("SELECT CAST(name AS CHAR), CAST(state AS CHAR) FROM _lazyview_state")
                .await?;
            ensure!(
                !rows.is_empty() && rows.iter().all(|(_, s)| s == "ready"),
                "{what}: the state rows changed: {rows:?}"
            );
            c.exec_drop(
                "UPDATE _lazyview_status SET binlog_file = ?, binlog_pos = ?, \
                 binlog_cursor = ?, server_identity = ?",
                (&file, pos, &cursor, identity.clone()),
            )
            .await?;
        }
        exec(&mut c, "INSERT INTO entities VALUES (13, 3, 'new')").await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "resumed once the cause was undone",
        )
        .await?;
        let snap = eng.snapshot();
        ensure!(
            snap.full_rebuilds == 0,
            "the resume rebuilt: {}",
            fmt_snapshot(&snap)
        );
        eng.stop().await
    });
}

#[test]
fn lifecycle_crash_consistency() {
    scenario("lc_crash", |ctx| async move {
        let db = ctx.new_db("lc_crash").await?;
        let seed = ctx.sub_seed("seed");
        let ids = setup_ecommerce(&db, seed, Sizes::scaled()).await?;
        let mut c = db.conn().await?;
        // Views over children of cascading foreign keys are left out where the storage engine
        // cascades without binlog events: a crash resumes from a saved position before the startup
        // seed of the cascade cache, and a parent deleted in that window leaves their rows stale
        // until they change again (the restart gap, by design: counted in cascade_gaps and tested
        // by reg_cascade_restart_gap). Where the server logs the cascaded rows, the replay carries
        // them and every view stays in.
        let logged = cascades_logged(&mut c).await?;
        let all: Vec<ViewSpec> = ecommerce_views()
            .into_iter()
            .filter(|v| logged || !workload::reads_fk_child(&v.name))
            .collect();
        let (chained, base): (Vec<ViewSpec>, Vec<ViewSpec>) =
            all.iter().cloned().partition(|s| s.kind == Kind::Chained);
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &base).await?;
        let mut rng = workload::Gen::new(derive_seed(seed, "crash"));
        let windows = ["0", "50", "200"];
        let persist = ["0", "100", "1000"];
        let ov_for = |rng: &mut workload::Gen| -> Vec<(&'static str, &'static str)> {
            vec![
                ("LAZYVIEW_BATCH_WINDOW_MS", *rng.pick(&windows)),
                ("LAZYVIEW_POSITION_PERSIST_MS", *rng.pick(&persist)),
            ]
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
            let writers =
                tokio::spawn(async move { run_concurrent(&dbc, idc, wseed, 3, scaled(80)).await });
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
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("round {round}: after {crashes} crashes"),
            )
            .await?;
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
        let first = vec![
            spec_named("v_vip_stats"),
            spec_named("v_customer_stats"),
            spec_named("v_tier_revenue"),
        ];
        add_views(&mut c, &first).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&first), t_ready())
            .await
            .context("chained definitions present at startup")?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &first,
            "chained views built at startup",
        )
        .await?;
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
        wait_ready(&mut c, &mut eng, &names(&more), t_ready())
            .await
            .context("chained definitions inserted together while streaming")?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids);
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        let mut all = first.clone();
        all.extend(more);
        verify(&ctx, &mut eng, &db, &all, "chained views after writes").await?;
        eng.stop().await?;
        Ok(())
    });
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
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let addr = format!("127.0.0.1:{port}");
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_HEALTH_ADDR", addr.as_str())])?;
        wait_until("health endpoint listening", Duration::from_secs(60), || {
            let addr = addr.clone();
            async move {
                Ok(http_get(&addr, "/healthz")
                    .await
                    .map(|r| r.0 == 200)
                    .unwrap_or(false))
            }
        })
        .await?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id % 2 = 0").await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        wait_until(
            "/readyz 200 once all views are ready",
            Duration::from_secs(60),
            || {
                let addr = addr.clone();
                async move {
                    Ok(http_get(&addr, "/readyz")
                        .await
                        .map(|r| r.0 == 200)
                        .unwrap_or(false))
                }
            },
        )
        .await?;
        let (st, body) = http_get(&addr, "/metrics").await?;
        ensure!(st == 200, "/metrics status {st}");
        ensure!(
            body.contains("lazyview_units_refreshed_total"),
            "/metrics lacks lazyview_units_refreshed_total:\n{}",
            clip(&body, 2000)
        );
        let (st, _) = http_get(&addr, "/nope").await?;
        ensure!(st == 404, "unknown path status {st}");
        // A view in error state must make /readyz fail.
        add_definition(
            &mut c,
            "CREATE VIEW v_broken AS SELECT nope FROM no_such_table",
            None,
        )
        .await?;
        wait_state(&mut c, &mut eng, "v_broken", "error", t_ready()).await?;
        // Views in error do not make the instance unready by default (LAZYVIEW_READY_REQUIRE_ALL
        // is process-wide, so the per-request override is used for the strict variant).
        let (st, body) = http_get(&addr, "/readyz").await?;
        ensure!(
            st == 200,
            "/readyz must stay 200 with a failing view by default, got {st}: {}",
            clip(&body, 300)
        );
        wait_until(
            "/readyz?require_all=1 503 while a view is failing",
            Duration::from_secs(30),
            || {
                let addr = addr.clone();
                async move {
                    Ok(http_get(&addr, "/readyz?require_all=1")
                        .await
                        .map(|r| r.0 == 503)
                        .unwrap_or(false))
                }
            },
        )
        .await?;
        let (st, _) = http_get(&addr, "/healthz").await?;
        ensure!(
            st == 200,
            "/healthz must stay 200 with a failing view, got {st}"
        );
        eng.stop().await?;
        Ok(())
    });
}

async fn columns_of(c: &mut mysql_async::Conn, table: &str) -> Result<Vec<String>> {
    Ok(rows(
        c,
        &format!(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            q(table)
        ),
    )
    .await?
    .into_iter()
    .filter_map(|r| r[0].clone())
    .collect())
}

async fn name_of(dsn: &str, id: u64) -> Result<Option<String>> {
    let mut c = connect(dsn).await?;
    let n = scalar(
        &mut c,
        &format!("SELECT name FROM _lazyview WHERE id = {id}"),
    )
    .await?;
    c.disconnect().await.ok();
    Ok(n)
}

/// `_lazyview.name`: definitions by name with a bare SELECT, UPDATE / DELETE by name, duplicate
/// names rejected by the table, name ≠ view name → error, NULL name backfilled.
#[test]
fn lifecycle_named_definitions() {
    scenario("lc_named", |ctx| async move {
        let db = ctx.new_db("lc_named").await?;
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_table(&mut c, &mut eng, "_lazyview", true, Duration::from_secs(60)).await?;
        ensure!(
            columns_of(&mut c, "_lazyview")
                .await?
                .iter()
                .any(|n| n == "name"),
            "_lazyview has no name column"
        );

        // Bare SELECT under a name.
        let v1 = ViewSpec::new(
            "v_named",
            Kind::Incremental,
            "SELECT id, status, customer_id FROM orders WHERE status <> 'cancelled'",
        );
        exec(
            &mut c,
            &format!(
                "INSERT INTO _lazyview (name, definition) VALUES ('v_named', {})",
                q(&v1.select)
            ),
        )
        .await?;
        wait_ready(&mut c, &mut eng, &["v_named"], t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&v1),
            "bare SELECT by name",
        )
        .await?;

        // UPDATE … WHERE name = (bare SELECT again).
        let v2 = ViewSpec::new(
            "v_named",
            Kind::Incremental,
            "SELECT o.id, o.status, c.name AS customer FROM orders o LEFT JOIN customers c ON c.id = o.customer_id",
        );
        let old = current_hash(&mut c, "v_named").await?;
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE name = 'v_named'",
                q(&v2.select)
            ),
        )
        .await?;
        wait_rebuilt(&mut c, &mut eng, "v_named", &old, t_ready()).await?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids);
        run_batches(&mut c, &mut wl, scaled(20)).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&v2),
            "updated by name",
        )
        .await?;

        // Duplicate name: rejected by the table itself.
        match exec(
            &mut c,
            "INSERT INTO _lazyview (name, definition) VALUES ('v_named', 'SELECT id FROM orders')",
        )
        .await
        {
            Ok(()) => bail!("duplicate _lazyview.name was accepted"),
            Err(e) => ensure!(
                server_code(&e) == Some(1062),
                "duplicate name: expected ER_DUP_ENTRY (1062), got {e:#}"
            ),
        }

        // Name ≠ the definition's view name: the CREATE VIEW name wins and `name` follows it.
        exec(&mut c, "INSERT INTO _lazyview (name, definition) VALUES ('v_a', 'CREATE VIEW v_b AS SELECT id FROM orders')").await?;
        let renamed = scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64;
        wait_ready(&mut c, &mut eng, &["v_b"], t_ready()).await?;
        ensure!(
            name_of(&db.dsn, renamed).await?.as_deref() == Some("v_b"),
            "_lazyview.name not updated to the CREATE VIEW name"
        );
        ensure!(
            !table_exists(&mut c, "v_a").await?,
            "v_a materialized despite the CREATE VIEW name v_b"
        );

        // A CREATE VIEW name another row already uses → error naming that row, nothing built.
        exec(&mut c, "INSERT INTO _lazyview (name, definition) VALUES ('v_c', 'CREATE VIEW v_b AS SELECT id, status FROM orders')").await?;
        let dup = scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64;
        let st = wait_def_state(&mut c, &mut eng, dup, "error", t_ready()).await?;
        let err = st.last_error.unwrap_or_default();
        ensure!(
            err.contains(&format!("already used by row #{renamed}")),
            "duplicate CREATE VIEW name: unexpected error {err:?}"
        );
        ctx.log(format!("duplicate CREATE VIEW name → {}", clip(&err, 160)));
        ensure!(
            !table_exists(&mut c, "v_c").await?,
            "v_c materialized despite the duplicate CREATE VIEW name"
        );

        // CREATE VIEW with NULL name → name backfilled.
        let v3 = ViewSpec::new(
            "v_backfilled",
            Kind::Incremental,
            "SELECT id, name FROM customers WHERE id % 2 = 1",
        );
        let id3 = add_definition(&mut c, &v3.definition, None).await?;
        wait_until("name backfilled from the CREATE VIEW", t_ready(), || {
            let dsn = db.dsn.clone();
            async move { Ok(name_of(&dsn, id3).await?.as_deref() == Some("v_backfilled")) }
        })
        .await?;
        wait_ready(&mut c, &mut eng, &["v_backfilled"], t_ready()).await?;

        // DELETE … WHERE name = → dropped.
        exec(&mut c, "DELETE FROM _lazyview WHERE name = 'v_named'").await?;
        wait_table(&mut c, &mut eng, "v_named", false, t_ready())
            .await
            .context("delete by name")?;
        run_batches(&mut c, &mut wl, scaled(10)).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&v3),
            "remaining named view",
        )
        .await?;
        eng.stop().await?;
        Ok(())
    });
}

/// Upgrade path: a pre-existing minimal `_lazyview(id, definition, enabled)` (no name / targets /
/// timestamps) is migrated on start, names are backfilled and the views get built.
#[test]
fn lifecycle_metadata_upgrade_from_minimal_table() {
    scenario("lc_upgrade", |ctx| async move {
        let db = ctx.new_db("lc_upgrade").await?;
        setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "CREATE TABLE _lazyview (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, definition MEDIUMTEXT NOT NULL, enabled TINYINT(1) NOT NULL DEFAULT 1)",
        )
        .await?;
        let specs = vec![spec_named("v_paid_orders"), spec_named("v_status_counts")];
        for s in &specs {
            exec(
                &mut c,
                &format!(
                    "INSERT INTO _lazyview (definition) VALUES ({})",
                    q(&s.definition)
                ),
            )
            .await?;
        }
        exec(&mut c, "INSERT INTO _lazyview (definition, enabled) VALUES ('CREATE VIEW v_disabled AS SELECT id FROM orders', 0)").await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "views from a migrated minimal _lazyview",
        )
        .await?;
        let cols = columns_of(&mut c, "_lazyview").await?;
        for needed in [
            "id",
            "name",
            "definition",
            "enabled",
            "targets",
            "created_at",
            "updated_at",
        ] {
            ensure!(
                cols.iter().any(|c| c == needed),
                "_lazyview not migrated: no column {needed} in {cols:?}"
            );
        }
        // Names are backfilled once a definition plans; the disabled one (#3) is never planned.
        let named = rows(
            &mut c,
            "SELECT id, name FROM _lazyview WHERE enabled = 1 ORDER BY id",
        )
        .await?;
        let got: Vec<Option<String>> = named.iter().map(|r| r[1].clone()).collect();
        ensure!(
            got == vec![
                Some("v_paid_orders".to_string()),
                Some("v_status_counts".to_string())
            ],
            "names not backfilled: {named:?}"
        );
        ensure!(
            !table_exists(&mut c, "v_disabled").await?,
            "disabled definition was materialized"
        );
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id % 3 = 0").await?;
        verify(&ctx, &mut eng, &db, &specs, "streaming after the upgrade").await?;
        eng.stop().await?;
        Ok(())
    });
}

/// Each family's own binlog compression end to end, so no flavor skips.
///
/// MySQL / Percona, `binlog_transaction_compression=ON` (session level): every transaction of
/// the writing session arrives as one TRANSACTION_PAYLOAD whose inner events have log_pos 0, so
/// the saved position must be the payload's end. Compressed transactions alone, interleaved with
/// plain ones of another session, one large compressed transaction over several tables with a
/// binlog rotate right after it, then a resume over compressed transactions written while the
/// engine was down (no full rebuild) and a restart after a crash.
///
/// MariaDB has no session setting; its own compression is the GLOBAL `log_bin_compress`: events
/// written while it is ON are the compressed types 165..171, which this reader cannot decode.
/// lazyview streams normally before them, reports the documented error on reaching one (it
/// cannot skip ahead: those events stay unreadable, and starting over while the GLOBAL is ON is
/// `lifecycle_refuses_log_bin_compress`), and the compressed transaction never reaches a view.
/// That window changes a GLOBAL every session on the server writes with, so on MariaDB the
/// scenario is opt-in (`LAZYVIEW_TEST_GLOBAL_BINLOG_COMPRESS=1`, which CI sets: one binary and
/// one test at a time there).
#[test]
fn lifecycle_compressed_transactions() {
    scenario("lc_compressed", |ctx| async move {
        if ctx.server().is_mariadb() {
            // The window needs `SET GLOBAL log_bin_compress`, which every session on the server
            // writes with: opt-in, as CI sets it (one binary at a time there).
            if !opted_in("LAZYVIEW_TEST_GLOBAL_BINLOG_COMPRESS") {
                ctx.skip(
                    "changes GLOBAL log_bin_compress; set \
                     LAZYVIEW_TEST_GLOBAL_BINLOG_COMPRESS=1 to run it",
                );
                return Ok(());
            }
            let db = ctx.new_db("lc_compressed").await?;
            let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
            let specs: Vec<ViewSpec> = views_of(&[Kind::Incremental])
                .into_iter()
                .filter(|v| !workload::reads_fk_child(&v.name))
                .collect();
            let ov = [
                ("LAZYVIEW_POSITION_PERSIST_MS", "300"),
                ("LAZYVIEW_QUERY_CHUNK", "7"),
            ];
            let (mut eng, _ids) = start_with_views(&ctx, &db, &specs, &ov).await?;
            let mut wl = Workload::new(ctx.sub_seed("wl"), ids);
            let mut c = db.conn().await?;
            run_batches(&mut c, &mut wl, scaled(40)).await?;
            verify(&ctx, &mut eng, &db, &specs, "streaming before compression").await?;

            // MariaDB compresses per record: only events whose records are at least
            // `log_bin_compress_min_len` (256) bytes are written compressed. Widen `note`
            // (no view reads it) so the window's row images pass that line.
            exec(&mut c, "ALTER TABLE orders MODIFY note VARCHAR(1000) NULL").await?;
            verify(&ctx, &mut eng, &db, &specs, "the ALTER before the window").await?;
            // The compressed window, with the GLOBAL back OFF as fast as possible (a GLOBAL
            // change on a shared server; the suite holds the heavy-suite lock and CI runs
            // one test binary at a time).
            let mut globals = Globals::set(&mut c, &db.dsn, &[("log_bin_compress", "ON")]).await?;
            exec_all(
                &mut c,
                &[
                    "START TRANSACTION".to_string(),
                    // The marker's row image is 600 bytes of note: its Write_rows event is
                    // written compressed.
                    "INSERT INTO orders (id, customer_id, status, created_at, note) \
                     VALUES (987654, 1, 'paid', '2026-01-01 00:00:00.000000', REPEAT('m', 600))"
                        .to_string(),
                    // Bulk wide images: every row event of this transaction is compressed too.
                    "UPDATE orders SET note = REPEAT('u', 600) WHERE id <> 987654".to_string(),
                    "COMMIT".to_string(),
                ],
            )
            .await?;
            let restored = globals.restore(&mut c).await;
            restored?;

            // The window really was compressed: the stream that reads past it must report the
            // unreadable events with the documented fix hint, keep retrying (alive, not dead)
            // and its position before them.
            let log = wait_log_contains(
                &eng.log_path,
                "MariaDB compressed binlog event (type",
                Duration::from_secs(60),
            )
            .await
            .with_context(|| format!("engine log {}", eng.log_path.display()))?;
            ensure!(
                log.contains("set log_bin_compress=OFF"),
                "the reported error lacks the documented fix hint — log {}",
                eng.log_path.display()
            );
            eng.check_alive()?;
            // Nothing half-applied: the marker is in the table but stays out of the view.
            let mut mc = db.conn().await?;
            ensure!(
                scalar_i64(&mut mc, "SELECT COUNT(*) FROM orders WHERE id = 987654").await? == 1,
                "the compressed marker row was not written to orders"
            );
            ensure!(
                scalar_i64(
                    &mut mc,
                    "SELECT COUNT(*) FROM v_paid_orders WHERE order_id = 987654"
                )
                .await?
                    == 0,
                "a compressed transaction reached v_paid_orders although its events are unreadable"
            );
            // The stream cannot pass those events, so the engine ends crashed, not stopped.
            eng.crash().await
        } else {
            let db = ctx.new_db("lc_compressed").await?;
            let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
            // Incrementally maintained views only, so the resume can be checked for full rebuilds
            // (as in lifecycle_start_modes).
            let specs: Vec<ViewSpec> = views_of(&[Kind::Incremental])
                .into_iter()
                .filter(|v| !workload::reads_fk_child(&v.name))
                .collect();
            let ov = [
                ("LAZYVIEW_POSITION_PERSIST_MS", "300"),
                ("LAZYVIEW_QUERY_CHUNK", "7"),
            ];
            let (mut eng, _ids) = start_with_views(&ctx, &db, &specs, &ov).await?;
            let mut wl = Workload::new(ctx.sub_seed("wl"), ids);
            let mut c = db.conn().await?;
            exec(&mut c, "SET SESSION binlog_transaction_compression = ON").await?;
            let start = server_position(&mut c).await?;
            run_batches(&mut c, &mut wl, scaled(40)).await?;
            // The writes really were compressed.
            let payloads = rows(
                &mut c,
                &format!(
                    "SHOW BINLOG EVENTS IN {} FROM {} LIMIT 2000",
                    q(&start.file),
                    start.pos
                ),
            )
            .await?
            .iter()
            .filter(|r| r.get(2).cloned().flatten().as_deref() == Some("Transaction_payload"))
            .count();
            ensure!(payloads > 0, "no Transaction_payload event after {start}");
            verify(&ctx, &mut eng, &db, &specs, "compressed transactions").await?;

            let mut plain = db.conn().await?;
            for i in 0..scaled(20) {
                let conn = if i % 2 == 0 { &mut c } else { &mut plain };
                run_batches(conn, &mut wl, 1).await?;
            }
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                "compressed and plain interleaved",
            )
            .await?;

            exec_all(
                &mut c,
                &[
                    "START TRANSACTION".to_string(),
                    "UPDATE order_items SET qty = qty + 1".to_string(),
                    "UPDATE orders SET status = 'paid' WHERE id % 3 = 0".to_string(),
                    "UPDATE payments SET amount = amount + 1".to_string(),
                    "COMMIT".to_string(),
                    "FLUSH BINARY LOGS".to_string(),
                ],
            )
            .await?;
            run_batches(&mut c, &mut wl, scaled(10)).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                "large compressed transaction, rotate after it",
            )
            .await?;
            eng.stop().await?;

            run_batches(&mut c, &mut wl, scaled(30)).await?;
            let mut eng = ctx.start_engine(&db, &ov)?;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                "resume over compressed transactions",
            )
            .await?;
            let snap = eng.snapshot();
            ensure!(
                snap.full_rebuilds == 0,
                "resume performed {} full rebuild(s); metrics {}",
                snap.full_rebuilds,
                fmt_snapshot(&snap)
            );

            run_batches(&mut c, &mut wl, scaled(10)).await?;
            eng.crash().await?;
            run_batches(&mut c, &mut wl, scaled(10)).await?;
            let mut eng = ctx.start_engine(&db, &ov)?;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                "restart after a crash amid compressed transactions",
            )
            .await?;
            eng.stop().await
        }
    });
}

/// The GLOBAL-scope binlog compression each family has, and lazyview's behaviour with it, so
/// no flavor skips.
///
/// MariaDB `log_bin_compress=ON`: compressed events cannot be read, so the engine refuses to
/// start with the documented message (docs/server-setup.md) before it writes anything, and
/// starts once it is OFF again.
///
/// MySQL / Percona have no such variable; their `binlog_transaction_compression` is supported
/// (docs/server-setup.md), so the same shape runs at GLOBAL scope: sessions opened while it is
/// ON write TRANSACTION_PAYLOADs, and the engine starts and streams through them and through
/// plain writes again once the GLOBAL is OFF. Both halves change a GLOBAL that every session on
/// the server writes with, so the scenario is opt-in (`LAZYVIEW_TEST_GLOBAL_BINLOG_COMPRESS=1`,
/// which CI sets: it runs one test binary at a time); the window itself lasts seconds.
#[test]
fn lifecycle_refuses_log_bin_compress() {
    scenario("lc_bin_compress", |ctx| async move {
        if !opted_in("LAZYVIEW_TEST_GLOBAL_BINLOG_COMPRESS") {
            ctx.skip(
                "changes GLOBAL binlog compression; set \
                 LAZYVIEW_TEST_GLOBAL_BINLOG_COMPRESS=1 to run it",
            );
            return Ok(());
        }
        if ctx.server().is_mariadb() {
            let db = ctx.new_db("lc_bin_compress").await?;
            setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
            let specs = vec![spec_named("v_paid_orders"), spec_named("v_status_counts")];
            let mut c = db.conn().await?;
            ensure_lazyview_table(&mut c).await?;
            add_views(&mut c, &specs).await?;
            let mut globals = Globals::set(&mut c, &db.dsn, &[("log_bin_compress", "ON")]).await?;
            let refused = match ctx.start_engine(&db, &[]) {
                Ok(eng) => eng.wait_finished(Duration::from_secs(60)).await,
                Err(e) => Err(e),
            };
            let restored = globals.restore(&mut c).await;
            match refused? {
                Err(e) if e.contains("log_bin_compress is ON") => {}
                other => bail!("expected a refusal naming log_bin_compress, got {other:?}"),
            }
            restored?;

            let mut eng = ctx.start_engine(&db, &[])?;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id % 2 = 0").await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                "started once log_bin_compress is OFF",
            )
            .await?;
            eng.stop().await?;
            return Ok(());
        }

        // MySQL / Percona: the GLOBAL counterpart of `log_bin_compress` is
        // `binlog_transaction_compression`, which lazyview supports. Start the engine while
        // the GLOBAL is ON, prove a fresh session's writes really arrive as payloads, and
        // stream through them; then OFF again and through plain writes.
        let db = ctx.new_db("lc_bin_compress").await?;
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let specs = vec![spec_named("v_paid_orders"), spec_named("v_status_counts")];
        let mut c = db.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut globals =
            Globals::set(&mut c, &db.dsn, &[("binlog_transaction_compression", "ON")]).await?;
        // A session opened after the change inherits the GLOBAL: its transactions are the
        // payloads. (One opened before it keeps its own session value until it reconnects.)
        let mut w = db.conn().await?;
        let start = server_position(&mut w).await?;
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids);
        run_batches(&mut w, &mut wl, scaled(40)).await?;
        // The writes really were compressed.
        let payloads = rows(
            &mut w,
            &format!(
                "SHOW BINLOG EVENTS IN {} FROM {} LIMIT 2000",
                q(&start.file),
                start.pos
            ),
        )
        .await?
        .iter()
        .filter(|r| r.get(2).cloned().flatten().as_deref() == Some("Transaction_payload"))
        .count();
        ensure!(payloads > 0, "no Transaction_payload event after {start}");
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "started with GLOBAL compressed transactions",
        )
        .await?;
        let restored = globals.restore(&mut c).await;
        restored?;
        // A fresh session now writes plain transactions and they still stream.
        let mut p = db.conn().await?;
        run_batches(&mut p, &mut wl, scaled(10)).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "streaming after the GLOBAL is OFF again",
        )
        .await?;
        eng.stop().await
    });
}

/// The analyzer's plan of a spec's definition, against the live schema (unqualified names).
async fn plan_of(db: &Db, s: &ViewSpec) -> Result<lazyview::analyze::ViewPlan> {
    use lazyview::catalog::Catalog;
    use std::collections::HashMap;
    use std::sync::Arc;
    let d = lazyview::analyze::parse_definition(&s.definition)?;
    let mut c = db.conn().await?;
    let mut cat = Catalog::default();
    let mut tables: HashMap<lazyview::catalog::TableRef, Arc<lazyview::catalog::TableInfo>> =
        HashMap::new();
    let mut queue: Vec<(lazyview::catalog::TableRef, usize)> =
        lazyview::analyzer::referenced_tables(&d.query, &db.name)
            .into_iter()
            .map(|t| (t, 0))
            .collect();
    let mariadb = db.flavor.contains("mariadb");
    while let Some((t, depth)) = queue.pop() {
        if tables.contains_key(&t) || depth > 8 {
            continue;
        }
        if let Some(info) = cat.get(&mut c, &t.0, &t.1, mariadb).await? {
            queue.extend(info.cascades.iter().map(|f| (f.parent.clone(), depth + 1)));
            tables.insert(t, info);
        }
    }
    c.disconnect().await.ok();
    lazyview::analyzer::plan(&d, &db.name, &tables).context("analyzer plan")
}

/// `_lazyview_state.route` and `.access` hold the view's access plan (route + EXPLAIN text),
/// and `LAZYVIEW_SYNC=false` compares the fresh plans against the stored ones.
#[test]
fn lifecycle_state_carries_route_and_access() {
    scenario("lc_state_access", |ctx| async move {
        use mysql_async::prelude::Queryable;
        let db = ctx.new_db("lc_state_access").await?;
        let mut c = db.conn().await?;
        for s in [
            "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL)",
            "INSERT INTO entities VALUES (1, 1), (2, 2)",
        ] {
            exec(&mut c, s).await?;
        }
        let specs = vec![
            ViewSpec::new("lc_a", Kind::Incremental, "SELECT id, grp FROM entities"),
            ViewSpec::new(
                "lc_b",
                Kind::Incremental,
                "SELECT COUNT(*) AS n FROM entities",
            ),
        ];
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        eng.stop().await?;

        // (1) the state rows hold the route and the EXPLAIN text the plan gives.
        for s in &specs {
            let ap = lazyview::access::AccessPlan::of(&plan_of(&db, s).await?);
            let (route, access): (Option<String>, Option<String>) = c
                .query_first(
                    format!(
                        "SELECT CAST(route AS CHAR), CAST(access AS CHAR) FROM _lazyview_state WHERE name = {} AND target = 'source'",
                        q(&s.name)
                    ),
                )
                .await?
                .context("no state row")?;
            ensure!(
                route.as_deref() == Some(ap.route_name()),
                "{}: stored route {route:?} != {}",
                s.name,
                ap.route_name()
            );
            ensure!(
                access.as_deref() == Some(&ap.to_string()),
                "{}: stored access differs:\n{access:?}\n!=\n{}",
                s.name,
                ap
            );
        }

        // (2) change one definition, add one without a state row, then run the sync check.
        let old_a = specs[0].clone();
        let new_a = ViewSpec::new("lc_a", Kind::Incremental, "SELECT id FROM entities");
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE name = {}",
                q(&new_a.definition),
                q(&new_a.name)
            ),
        )
        .await?;
        add_definition(
            &mut c,
            &ViewSpec::new("lc_c", Kind::Incremental, "SELECT grp FROM entities").definition,
            None,
        )
        .await?;
        let fp_old = lazyview::access::AccessPlan::of(&plan_of(&db, &old_a).await?).fingerprint;
        let fp_new = lazyview::access::AccessPlan::of(&plan_of(&db, &new_a).await?).fingerprint;
        ensure!(
            fp_old != fp_new,
            "the definition change must move the fingerprint"
        );

        let eng = ctx.start_engine(&db, &[("LAZYVIEW_SYNC", "false")])?;
        let log_path = eng.log_path.clone();
        match eng.wait_finished(Duration::from_secs(60)).await? {
            Ok(()) => {}
            Err(e) => bail!("LAZYVIEW_SYNC=false did not exit cleanly: {e}"),
        }
        let log = std::fs::read_to_string(&log_path)?;
        ensure!(log.contains("access same"), "no 'access same' line: {log}");
        ensure!(
            log.contains(&format!("access changed (stored {fp_old}, now {fp_new})")),
            "no 'access changed' for lc_a: {log}"
        );
        ensure!(
            log.contains("access new (no state)"),
            "no 'access new' for lc_c: {log}"
        );
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// Two runs (owner 2026-11-16): `lazyview setup` owns the DDL of lazyview's own tables; the
// operations run only checks them (information_schema, SELECT rights) and refuses to start
// without them, naming the setup run.
// ---------------------------------------------------------------------------------------------

/// Neutral sources shared by the two-runs scenarios (no application names).
const TWO_RUNS_DDL: &[&str] = &[
    "CREATE TABLE entities (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL)",
    "CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, entity_id BIGINT NOT NULL, price INT NOT NULL)",
];

/// One named definition, inserted directly: the operations run must never write `_lazyview`,
/// so the `name` column is filled by the setup account.
const TWO_RUNS_VIEW_SQL: &str = "SELECT i.id, i.entity_id, i.price FROM items i JOIN \
    entities e ON e.id = i.entity_id WHERE i.price > 0";

/// One named definition; `fn` so the exact SQL string is shared with the probe-name
/// computation (the hash is sha256 over the effective definition, src/analyze/parse.rs).
fn two_runs_def() -> String {
    format!(
        "INSERT INTO `_lazyview` (`name`, `definition`, `targets`) VALUES ('items_by_entity', \
         '{}', 'source')",
        TWO_RUNS_VIEW_SQL
    )
}

/// The name of the planner's temporary probe view for [`TWO_RUNS_VIEW_SQL`].
fn two_runs_probe_view() -> String {
    let eff = format!(
        "CREATE VIEW {} AS {}",
        lazyview::analyze::quote_ident("items_by_entity"),
        TWO_RUNS_VIEW_SQL.trim()
    );
    format!("_lv_probe_{}", lazyview::analyze::definition_hash(&eff))
}

#[test]
fn lifecycle_ops_run_requires_setup() {
    scenario("lc_setup_required", |ctx| async move {
        let db = ctx.new_db("lc_setreq").await?;
        use mysql_async::prelude::Queryable;
        let mut c = db.conn().await?;
        exec_all(&mut c, TWO_RUNS_DDL).await?;
        c.query_drop("INSERT INTO entities (id, name) VALUES (1, 'alpha')")
            .await?;
        c.query_drop("INSERT INTO items (id, entity_id, price) VALUES (1, 1, 10), (2, 1, 0)")
            .await?;

        // The operations run against a fresh schema: nothing lazyview owns exists. It must
        // refuse to start, name the fix, and create nothing itself.
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_TEST_SKIP_SETUP", "1")])?;
        let exit = tokio::time::timeout(t_ready(), async {
            loop {
                if let Some(r) = eng.exited() {
                    return r.clone();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("operations run on a fresh schema never refused to start")?;
        let err = exit.as_ref().err().context("operations run did not fail")?;
        ensure!(
            err.contains("run `lazyview setup` with the setup account"),
            "error does not name the fix: {err}"
        );
        for table in [
            "_lazyview",
            "_lazyview_state",
            "_lazyview_status",
            "_lazyview_fence",
        ] {
            ensure!(
                !table_exists(&mut c, table).await?,
                "the operations run created {table}"
            );
        }
        ctx.log("operations run refused to start on a fresh schema, created nothing");

        // After the setup run, the operations run starts and refreshes the view.
        setup_run(&db).await?;
        c.query_drop(two_runs_def()).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &["items_by_entity"], t_ready()).await?;
        let count_in_view = move || {
            let dsn = db.dsn.clone();
            async move {
                let mut c = connect(&dsn).await?;
                let n = scalar_i64(&mut c, "SELECT COUNT(*) FROM items_by_entity").await?;
                c.disconnect().await.ok();
                Ok::<i64, anyhow::Error>(n)
            }
        };
        let count_is = |want: i64| {
            let count_in_view = &count_in_view;
            move || {
                let f = count_in_view();
                async move { Ok::<bool, anyhow::Error>(f.await? == want) }
            }
        };
        wait_until("seeded row in items_by_entity", t_catchup(), count_is(1)).await?;
        c.query_drop("INSERT INTO items (id, entity_id, price) VALUES (3, 2, 5)")
            .await?;
        c.query_drop("INSERT INTO entities (id, name) VALUES (2, 'beta')")
            .await?;
        wait_until(
            "incremental row in items_by_entity",
            t_catchup(),
            count_is(2),
        )
        .await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn lifecycle_setup_run_is_idempotent() {
    scenario("lc_setup_idem", |ctx| async move {
        let db = ctx.new_db("lc_idem").await?;
        use mysql_async::prelude::Queryable;
        let mut c = db.conn().await?;
        exec_all(&mut c, TWO_RUNS_DDL).await?;
        setup_run(&db).await?; // first setup run: creates
        c.query_drop(two_runs_def()).await?;
        async fn snap(c: &mut mysql_async::Conn) -> Result<(Vec<String>, Vec<String>)> {
            let tables: Vec<String> = rows(
                c,
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() \
                 AND TABLE_NAME LIKE '\\_lazyview%' ORDER BY 1",
            )
            .await?
            .into_iter()
            .filter_map(|r| r[0].clone())
            .collect();
            let cols: Vec<String> = rows(
                c,
                "SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME, ':', COLUMN_TYPE) \
                 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
                 AND TABLE_NAME LIKE '\\_lazyview%' ORDER BY 1",
            )
            .await?
            .into_iter()
            .filter_map(|r| r[0].clone())
            .collect();
            Ok((tables, cols))
        }
        let before = snap(&mut c).await?;
        ensure!(!before.0.is_empty(), "the first setup run created nothing");
        setup_run(&db).await?; // second setup run: must change nothing
        let after = snap(&mut c).await?;
        ensure!(
            before == after,
            "the second setup run changed the metadata:\n before {before:?}\n after {after:?}"
        );
        // It writes nothing else: no view tables, no saved position.
        ensure!(
            !table_exists(&mut c, "items_by_entity").await?,
            "setup created a view table"
        );
        ensure!(
            scalar_i64(&mut c, "SELECT COUNT(*) FROM `_lazyview_status`").await? == 0,
            "setup wrote a saved position"
        );
        ctx.log("setup run twice: identical metadata, nothing else written");
        Ok(())
    });
}

#[test]
fn lifecycle_ops_run_restricted_user() {
    scenario("lc_restricted", |ctx| async move {
        let db = ctx.new_db("lc_restr").await?;
        use mysql_async::prelude::Queryable;
        let mut c = db.conn().await?;
        exec_all(&mut c, TWO_RUNS_DDL).await?;
        c.query_drop("INSERT INTO entities (id, name) VALUES (1, 'alpha')")
            .await?;
        c.query_drop("INSERT INTO items (id, entity_id, price) VALUES (1, 1, 10)")
            .await?;
        // The setup run (here: the schema owner) creates everything the service needs.
        setup_run(&db).await?;
        c.query_drop(two_runs_def()).await?;
        // MySQL grants table-level privileges only on existing names, so the setup account
        // pre-creates the planner's probe view (the same name the planner will `CREATE OR
        // REPLACE` on each validate).
        c.query_drop(format!(
            "CREATE OR REPLACE VIEW `{}`.`{}` AS SELECT 1 AS a",
            db.name,
            two_runs_probe_view()
        ))
        .await?;

        // A restricted account (neutral name, created and dropped by the test): SELECT on the
        // definitions, DML on lazyview's own status tables, no DDL on any of them; the view
        // tables are its own (their DDL belongs to the operations run); the probe view of the
        // planner is granted by name; the binlog needs its replication rights. It runs on its
        // own connection (`account_conn`), and everything it puts on the server is dropped
        // again below — whatever the outcome of the run.
        let user = format!("lvop{:x}", rand::rng().random_range(0..0xFFFF_FFFF_u32));
        let d = format!("`{}`", db.name);
        let probe = two_runs_probe_view();
        let mut acct = account_conn(&db).await?;
        account_stmt(&mut acct, &format!("DROP USER IF EXISTS '{user}'@'%'")).await?;
        account_stmt(
            &mut acct,
            &format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"),
        )
        .await?;

        // The grants, then the operations run as that account: it starts (its own tables pass
        // the check) and refreshes the view.
        let outcome = async {
            for grant in [
                format!("GRANT SELECT ON {d}.`_lazyview` TO '{user}'@'%'"),
                format!(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON {d}.`_lazyview_state` TO '{user}'@'%'"
                ),
                format!(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON {d}.`_lazyview_status` TO '{user}'@'%'"
                ),
                format!("GRANT INSERT, UPDATE ON {d}.`_lazyview_fence` TO '{user}'@'%'"),
                format!("GRANT SELECT ON {d}.`entities` TO '{user}'@'%'"),
                format!("GRANT SELECT ON {d}.`items` TO '{user}'@'%'"),
                format!("GRANT ALL PRIVILEGES ON {d}.`items_by_entity` TO '{user}'@'%'"),
                format!("GRANT ALL PRIVILEGES ON {d}.`items_by_entity__lv_new` TO '{user}'@'%'"),
                format!("GRANT ALL PRIVILEGES ON {d}.`items_by_entity__lv_old` TO '{user}'@'%'"),
                format!("GRANT SELECT, CREATE VIEW, DROP ON {d}.`{probe}` TO '{user}'@'%'",),
                format!("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"),
            ] {
                account_stmt(&mut acct, &grant)
                    .await
                    .with_context(|| format!("grant: {grant}"))?;
            }
            let rdsn = dsn_with_user(&db.dsn, &format!("{user}:pw"));
            let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_DSN", rdsn.as_str())])?;
            wait_ready(&mut c, &mut eng, &["items_by_entity"], t_ready()).await?;
            c.query_drop("INSERT INTO items (id, entity_id, price) VALUES (2, 1, 7)")
                .await?;
            let seen = |want: i64| {
                let dsn = db.dsn.clone();
                move || {
                    let dsn = dsn.clone();
                    async move {
                        let mut c = connect(&dsn).await?;
                        let n = scalar_i64(&mut c, "SELECT COUNT(*) FROM items_by_entity").await?;
                        c.disconnect().await.ok();
                        Ok::<bool, anyhow::Error>(n == want)
                    }
                }
            };
            wait_until(
                "restricted engine applied the new row",
                t_catchup(),
                seen(2),
            )
            .await?;
            eng.stop().await
        }
        .await;

        // Nothing of this test survives it on any path, not only the successful one: the
        // account (with the server-wide replication grants it needs) goes with it, and so does
        // the probe view this scenario pre-created. Both are named by this run alone.
        let mut cleanup = account_conn(&db).await?;
        for sql in [
            format!("DROP USER IF EXISTS '{user}'@'%'"),
            format!("DROP VIEW IF EXISTS {d}.`{probe}`"),
        ] {
            account_stmt(&mut cleanup, &sql).await.ok();
        }
        cleanup.disconnect().await.ok();
        acct.disconnect().await.ok();
        outcome?;
        ctx.log("operations run started and refreshed as the restricted account");
        Ok(())
    });
}

/// A connection for the account work of [`lifecycle_ops_run_restricted_user`], with
/// `lock_wait_timeout` bounded: these statements take the server-wide ACL / global DDL lock,
/// so with the suite's 2 threads one can be queued behind another scenario's DDL, and the
/// server default (a year) would let that eat the scenario's whole budget.
async fn account_conn(db: &Db) -> Result<mysql_async::Conn> {
    let mut c = db.conn().await?;
    account_stmt(&mut c, "SET SESSION lock_wait_timeout = 120").await?;
    Ok(c)
}

/// One account-management statement (`CREATE USER` / `GRANT` / `DROP USER`) or the session
/// setting that makes waiting for them bounded. A statement queued behind a concurrent
/// scenario's DDL comes back as a lock-wait timeout (1205) or a deadlock (1213): retried,
/// with a short backoff. Any other error, or a timeout that survives every attempt, is
/// returned with the statement in its context.
async fn account_stmt(c: &mut mysql_async::Conn, sql: &str) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let mut attempt = 0;
    loop {
        attempt += 1;
        match c.query_drop(sql).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let e = anyhow::Error::from(e);
                if matches!(server_code(&e), Some(1205 | 1213)) && attempt < 12 {
                    tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
                    continue;
                }
                return Err(e).context(format!("account statement: {sql}"));
            }
        }
    }
}
