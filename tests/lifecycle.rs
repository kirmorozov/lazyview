//! Lifecycle: bootstrap of metadata tables, live definition changes (insert / update / disable /
//! re-enable / delete), errors (invalid SQL, missing table, unknown garbage), refusing to clobber
//! foreign tables, start modes (resume / rebuild / once / LAZYVIEW_SYNC=false), crash
//! consistency, and binlog compression (MySQL's session-level `binlog_transaction_compression`
//! end to end; the MariaDB family runs the same transaction shapes, since its own compression is
//! the GLOBAL `log_bin_compress`, whose window needs a server of its own and so is
//! `tests/exclusive.rs`; here it is the start-up refusal of that variable that runs).

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
/// server with no GTID set to vouch for it) stops lazyview but leaves the `ready` state rows
/// alone: only a purged file makes the views stale for good (tests/exclusive.rs). Once the cause
/// is undone, the next start resumes without a rebuild.
///
/// A foreign identity is the one leg the adoption rules of the resume-by-GTID plan answer for
/// (ruling 4, and `reg_unreadable_identity_keeps_resume_guard` in tests/regressions/lifecycle.rs
/// branches the same way): a cursor that carries a set this server's own state covers is adopted
/// and the stream opens by GTID, so that leg waits for the run to go ready instead of for a stop;
/// a cursor without a set is still refused by the identity rule, and the `ready` rows are checked
/// either way.
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
        // Whether the row carries a GTID set decides the second leg (see the doc above): a
        // covered set is adopted by the rules of the resume-by-GTID plan, no set at all (the
        // upgrade shape, or a `gtid_mode=OFF` source) is refused by the identity rule.
        let carries_set = cursor.contains("[g=");
        for (what, set, expect, adopt) in [
            (
                "a position past the end of its file",
                format!(
                    "UPDATE _lazyview_status SET binlog_pos = {}, binlog_cursor = '{}:{}'",
                    pos + 1_000_000_000,
                    file,
                    pos + 1_000_000_000
                ),
                "is no longer available",
                false,
            ),
            (
                "a position saved on another server",
                "UPDATE _lazyview_status SET server_identity = 'another-server'".to_string(),
                "was written by server",
                true,
            ),
        ] {
            exec(&mut c, &set).await?;
            if adopt && carries_set {
                // Ruling 4: this server's own executed state is the proof, not its name. The
                // stream opens by GTID on the saved set and the views say what the oracle says.
                let mut eng = ctx.start_engine(&db, &[])?;
                wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
                verify(
                    &ctx,
                    &mut eng,
                    &db,
                    &specs,
                    "adopted by GTID across a foreign identity",
                )
                .await?;
                eng.stop().await?;
            } else {
                let eng = ctx.start_engine(&db, &[])?;
                match eng.wait_finished(Duration::from_secs(60)).await? {
                    Ok(()) => bail!("{what}: lazyview did not stop"),
                    Err(e) => ensure!(e.contains(expect), "{what}: unexpected stop: {e}"),
                }
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
/// names rejected by the table, name ≠ view name → the definition wins and the row is left alone,
/// a CREATE VIEW name another row uses → error naming that row.
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

        // Name ≠ the definition's view name: the CREATE VIEW name wins, and `_lazyview.name` is
        // left exactly as the management user wrote it — the name is derived from the definition on
        // every read and never written back (the table is the management user's, so the database's
        // audit log keeps every change as theirs).
        exec(&mut c, "INSERT INTO _lazyview (name, definition) VALUES ('v_a', 'CREATE VIEW v_b AS SELECT id FROM orders')").await?;
        let renamed = scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64;
        wait_ready(&mut c, &mut eng, &["v_b"], t_ready()).await?;
        ensure!(
            name_of(&db.dsn, renamed).await?.as_deref() == Some("v_a"),
            "the service rewrote _lazyview.name"
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

        // CREATE VIEW with a NULL name: the view is built under the name its own statement gives,
        // and the row keeps `name` NULL (the name is derived per read, never stored back).
        let v3 = ViewSpec::new(
            "v_backfilled",
            Kind::Incremental,
            "SELECT id, name FROM customers WHERE id % 2 = 1",
        );
        let id3 = add_definition(&mut c, &v3.definition, None).await?;
        wait_ready(&mut c, &mut eng, &["v_backfilled"], t_ready()).await?;
        ensure!(
            name_of(&db.dsn, id3).await?.is_none(),
            "the service wrote the derived name into _lazyview"
        );

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
/// timestamps) is migrated on start, the views get built under the names their definitions give,
/// and `name` stays NULL (the service never writes the table).
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
        // The names stay NULL: they are derived from the definitions on every read, and the
        // definitions table is the management user's. The disabled one (#3) is never planned.
        let named = rows(
            &mut c,
            "SELECT id, name FROM _lazyview WHERE enabled = 1 ORDER BY id",
        )
        .await?;
        let got: Vec<Option<String>> = named.iter().map(|r| r[1].clone()).collect();
        ensure!(
            got == vec![None, None],
            "the service wrote derived names into _lazyview: {named:?}"
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

/// Binlog compression, end to end, on the scope each family offers without needing a server of
/// its own — so no flavor skips.
///
/// MySQL / Percona, `binlog_transaction_compression=ON` (session level): every transaction of
/// the writing session arrives as one TRANSACTION_PAYLOAD whose inner events have log_pos 0, so
/// the saved position must be the payload's end. Compressed transactions alone, interleaved with
/// plain ones of another session, one large compressed transaction over several tables with a
/// binlog rotate right after it, then a resume over compressed transactions written while the
/// engine was down (no full rebuild) and a restart after a crash.
///
/// MariaDB has no session-level compression: its own is the GLOBAL `log_bin_compress`, and a
/// window of it writes events of the compressed types 165..171, which this reader can never
/// decode — so every other engine reading that server would stall on them for good. Switching it
/// therefore needs a server with no other binlog reader on it, which is this repository's
/// `tests/exclusive.rs` (`exclusive_mariadb_compressed_window_is_unreadable`: lazyview streams
/// normally up to the window, reports the documented error on reaching one of those events, keeps
/// its position before them, and the compressed transaction never reaches a view). What the
/// MariaDB family runs here is the same scenario's shapes uncompressed — the interleaved sessions,
/// the large multi-table transaction with a rotate right after it, the resume over transactions
/// written while the engine was down (no full rebuild), the restart after a crash — so no flavor
/// skips this test.
#[test]
fn lifecycle_compressed_transactions() {
    scenario("lc_compressed", |ctx| async move {
        // The family whose compression is a session setting writes compressed here; the other
        // (MariaDB) writes the same shapes plainly, its global-only window being the exclusive
        // suite's. `kind` keeps the step names saying what was actually written.
        let mariadb = ctx.server().is_mariadb();
        let kind = if mariadb { "plain" } else { "compressed" };
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
        if !mariadb {
            exec(&mut c, "SET SESSION binlog_transaction_compression = ON").await?;
        }
        let start = server_position(&mut c).await?;
        run_batches(&mut c, &mut wl, scaled(40)).await?;
        // The writes really were compressed, on the family that compresses at session scope
        // (MariaDB's own compressed events are checked in tests/exclusive.rs).
        if !mariadb {
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
        }
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            &format!("{kind} transactions of the writing session"),
        )
        .await?;

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
            if mariadb {
                "two plain sessions interleaved"
            } else {
                "compressed and plain interleaved"
            },
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
            &format!("large {kind} multi-table transaction, rotate after it"),
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
            &format!("resume over {kind} transactions written while down"),
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
            &format!("restart after a crash amid {kind} transactions"),
        )
        .await?;
        eng.stop().await
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
                "UPDATE _lazyview SET definition = {} WHERE id = \
                 (SELECT definition_id FROM `_lazyview_state` WHERE `name` = {} LIMIT 1)",
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

/// One interval of a fabricated consumed set, in the flavor's own spelling: `uuid:interval` on the
/// MySQL family, `domain-server-seq` on MariaDB — the shape a server with a hole after nearly every
/// transaction gives its consumed state.
fn fragmented_set_item(flavor: lazyview::source::Flavor, i: usize) -> String {
    let uuid = "1ec44047-e72e-11f1-81ab-7ab4f2cf23bf";
    match flavor {
        lazyview::source::Flavor::MySql | lazyview::source::Flavor::Percona => {
            format!("{uuid}:{}-{}", i * 3 + 1, i * 3 + 2)
        }
        lazyview::source::Flavor::MariaDb => {
            format!("{}-33111-{}", 4_000_000_000u64 + i as u64, i + 1)
        }
    }
}

/// A `[g=…]` set of `count` intervals. For the legs that measure the column rather than ask the
/// server about the set (a made-up set is no answer to [`lazyview::source::gtid_covers`], so the
/// engine leg grows a real one instead).
fn fragmented_set(flavor: lazyview::source::Flavor, count: usize) -> String {
    (0..count)
        .map(|i| fragmented_set_item(flavor, i))
        .collect::<Vec<_>>()
        .join(",")
}

/// The same, at least `at_least` bytes long — and no more than one interval past it: these sets
/// are megabytes long and one test holds several of them at once.
fn fragmented_set_of(flavor: lazyview::source::Flavor, at_least: usize) -> String {
    let mut items: Vec<String> = Vec::new();
    let mut len = 0;
    while len < at_least {
        let item = fragmented_set_item(flavor, items.len());
        len += item.len() + 1; // the comma that joins it
        items.push(item);
    }
    items.join(",")
}

/// The width `ensure` gave `binlog_cursor` in this schema, as the server spells it.
async fn cursor_column_type(c: &mut mysql_async::Conn) -> Result<String> {
    Ok(scalar(
        c,
        "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME = '_lazyview_status' AND COLUMN_NAME = 'binlog_cursor'",
    )
    .await?
    .unwrap_or_default())
}

/// The row of one instance, as `meta::save_position` left it: its text and the length the *server*
/// says it holds. Both come from the server, so a value the server shortened is caught here rather
/// than in the text the caller asked for; a row that was claimed and never saved reads as empty.
async fn saved_cursor_row(c: &mut mysql_async::Conn, instance: &str) -> Result<(String, u64)> {
    use mysql_async::prelude::Queryable;
    c.query_first(format!(
        "SELECT COALESCE(CAST(binlog_cursor AS CHAR), ''), \
         COALESCE(LENGTH(binlog_cursor), 0) FROM _lazyview_status WHERE instance_id = {}",
        q(instance)
    ))
    .await?
    .context("no status row for the instance")
}

/// The three row polls the legs below run through [`wait_until`], each owning what it reads.
///
/// They are functions rather than closures, and each takes its DSN and instance **by value**, for
/// one reason: `wait_until` polls an `FnMut() -> Fut`, and rustc will not let a future borrow out
/// of the closure call that produced it — the borrow has to survive until the next turn, 50 ms
/// later, while the closure itself is only borrowed while it runs ("captured variable cannot
/// escape `FnMut` closure body"). So a poll reconnects on a DSN it owns, the way the other row
/// waits in this file do (see the bootstrap wait at the top), and the leg reads the row it asserts
/// on once the wait is over. Nothing here invents a wait of its own: the deadline is the caller's,
/// and the 50 ms step is the helper's.
async fn cursor_holds_whole_set(dsn: String, instance: String) -> Result<bool> {
    let mut c = connect(&dsn).await?;
    let row = saved_cursor_row(&mut c, &instance).await?;
    c.disconnect().await.ok();
    Ok(row.0.len() > 65_535)
}

/// The first cursor a run puts in the row — the one the resume leg later puts *back*.
async fn cursor_written(dsn: String, instance: String) -> Result<bool> {
    let mut c = connect(&dsn).await?;
    let row = saved_cursor_row(&mut c, &instance).await?;
    c.disconnect().await.ok();
    Ok(!row.0.is_empty())
}

/// The row following the applied position instead of standing still on the cursor this leg seeded.
async fn cursor_moved(dsn: String, instance: String, behind: String) -> Result<bool> {
    let mut c = connect(&dsn).await?;
    let row = saved_cursor_row(&mut c, &instance).await?;
    c.disconnect().await.ok();
    Ok(row.0 != behind)
}

/// The same row's other half: the position the pre-cursor columns hold. [`saved_row_shape`] is the
/// pair read together, for the legs that have to watch a row through a migration no save is making.
async fn saved_position_row(c: &mut mysql_async::Conn, instance: &str) -> Result<(String, u64)> {
    use mysql_async::prelude::Queryable;
    c.query_first(format!(
        "SELECT `binlog_file`, `binlog_pos` FROM _lazyview_status WHERE instance_id = {}",
        q(instance)
    ))
    .await?
    .context("no status row for the instance")
}

/// Both halves of one instance's row as the server reads them: its cursor text (empty when the
/// column is NULL), and the `binlog_file`/`binlog_pos` the same save wrote. A row that has been
/// through an ALTER, a refused start and a migration should answer the same twice — and only the
/// server's own reading of it says so.
async fn saved_row_shape(
    c: &mut mysql_async::Conn,
    instance: &str,
) -> Result<(String, String, u64)> {
    use mysql_async::prelude::Queryable;
    c.query_first(format!(
        "SELECT COALESCE(CAST(binlog_cursor AS CHAR), ''), `binlog_file`, `binlog_pos` \
         FROM _lazyview_status WHERE instance_id = {}",
        q(instance)
    ))
    .await?
    .context("no status row for the instance")
}

/// The whole consumed set rides `binlog_cursor` and the restart reads it back — #347, and the
/// ruling that replaced #356's shed: a cursor between the 65,535 bytes the column used to hold and
/// the 16 MB it holds now is stored **whole**, and the resume by GTID that needs it still works.
///
/// Nothing here is a made-up set: the run fragments the server's own GTID stream (a gap after every
/// transaction) until the cursor the engine has saved for itself passes the old limit, and asks the
/// row twice — the test's own read of it, and `source::gtid_covers`, the question a resume on
/// another server asks. The fail-before is that shape at the head before the widening — the server
/// kept the first 65,535 bytes of it (strict mode: `ERROR 1406 ... Data too long for column
/// 'binlog_cursor'`) and the next start died on `the saved position "…[g=…" is no cursor this
/// source speaks`; with #356's shed the row comes back holding no set at all, which the
/// `gtid_covers` leg below refuses. The last leg takes the column back under the living run: a save
/// it cannot prove stops the engine, naming the column.
#[test]
fn lifecycle_saved_cursor_holds_the_whole_set_and_resumes_by_gtid() {
    scenario("lc_cursor_whole", |ctx| async move {
        use lazyview::source::Flavor;
        use mysql_async::prelude::Queryable;
        let db = ctx.new_db("lc_cursor_whole").await?;
        let mut c = db.conn().await?;
        let maria = ctx.flavor().starts_with("mariadb");
        let flavor = if maria {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        };
        // The fragmentation this scenario measures is the server's own GTID stream, so it needs a
        // server that hands out GTIDs at all. `mysql80` and `percona84` (the flavors whose compose
        // service starts without `--gtid-mode=ON`, CI included) answer the injector's
        // `SET @@SESSION.GTID_NEXT = '<uuid>:<n>'` with `ERROR 1781 (HY000): @@SESSION.GTID_NEXT
        // cannot be set to UUID:NUMBER when @@GLOBAL.GTID_MODE = OFF`, and no made-up set can stand
        // in for it: the leg below asks the server the resume question
        // (`lazyview::source::gtid_covers`), which a fabricated set cannot answer. So the scenario
        // skips there — visibly, and never in a job that requires it (CI's
        // `LAZYVIEW_TEST_STRICT` covers `targets_`/`visibility_`/`chaos_`, not this test).
        if !maria {
            let mode = scalar(&mut c, "SELECT CAST(@@GLOBAL.gtid_mode AS CHAR)").await?;
            if mode.as_deref() != Some("ON") {
                ctx.skip(&format!(
                    "gtid_mode is {} on this server: it assigns no GTIDs, so there is no set to \
                     grow and no resume by GTID to check",
                    mode.as_deref().unwrap_or("OFF")
                ));
                return Ok(());
            }
        }
        for s in [
            "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
            // The fragmentation lands here: no view reads it, so the engine only has to read its
            // events, not refresh anything for them.
            "CREATE TABLE lc_noise (id INT PRIMARY KEY, pad VARCHAR(8) NOT NULL)",
            "INSERT INTO entities VALUES (10, 1, 'new'), (11, 1, 'paid')",
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
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;

        // What the engine saves by itself — the test writes nothing to the row. The instance id is
        // the one `coord::claim` wrote when the engine took the leadership lock.
        let instance: String = c
            .query_first("SELECT CAST(instance_id AS CHAR) FROM _lazyview_status")
            .await?
            .context("no status row to save into")?;
        ensure!(
            !instance.is_empty(),
            "the engine saved no position to measure"
        );

        let (groups, saved, server_bytes) =
            fragment_gtid_stream(&mut c, &instance, maria, "lc_noise", 70_000).await?;
        ctx.log(format!(
            "fragmented the server's GTID stream with {groups} transactions: the row holds a \
             {saved}-byte cursor, and the server's own set text is {server_bytes} bytes"
        ));
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        // The row catches up with the last of them on the save cadence (200 ms here), so the read
        // waits for the cursor to pass the old limit instead of guessing how long that takes; the
        // deadline is the guard against a set that never got that big.
        let (dsn, inst) = (db.dsn.clone(), instance.clone());
        let held_the_whole_set = wait_until(
            "the row's cursor to pass the 65,535 bytes the column used to hold",
            Duration::from_secs(60),
            || cursor_holds_whole_set(dsn.clone(), inst.clone()),
        )
        .await
        .is_ok();
        let (text, stored) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            held_the_whole_set && text.len() > 65_535,
            "the run saved only {} bytes: the set never grew past the limit the column used to              have, so this proves nothing (fragmented with {groups} transactions)",
            text.len()
        );
        ensure!(
            stored as usize == text.len(),
            "the server reports {stored} bytes for the {} bytes of cursor the row holds",
            text.len()
        );
        ensure!(
            text.len() > 65_535,
            "the run saved only {} bytes: the set never grew past the limit the column used to \
             have, so this proves nothing (fragmented with {groups} transactions)",
            text.len()
        );
        ctx.log(format!(
            "the engine saved a {}-byte cursor with its set",
            text.len()
        ));
        // The read of that row is where #347's engine died.
        let back = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the saved cursor reads back as no cursor")?;
        ensure!(
            back.to_string() == text,
            "the cursor reads back as {back}, not the {} bytes the row holds",
            text.len()
        );
        let set = back.gtid().unwrap_or_default();
        ensure!(
            !set.is_empty(),
            "the row holds a {}-byte cursor with no [g=…] set in it: the set was shed",
            text.len()
        );
        // And it is the server's own set, whole — the question a resume on another server asks
        // before it trusts a cursor. A set cut short is either unparsable (the line above) or does
        // not answer here.
        ensure!(
            lazyview::source::gtid_covers(&mut c, &back).await?,
            "this server does not cover the saved set: the row does not hold what the engine \
             consumed ({} bytes of set)",
            set.len()
        );

        // The other half of #347: the restart. It must come up on that cursor, open the dump the
        // only way this server allows, and bring the view up to date.
        exec(&mut c, "INSERT INTO entities VALUES (13, 3, 'new')").await?;
        eng.stop().await?;
        // Which anchor the restart may use is the server's answer, not the test's choice.
        // `SET @slave_connect_state` names the state the dump starts *after*, and MariaDB locates a
        // dump on such a state only when the binlog file it picks already carries every
        // `domain-server` pair the state names (`contains_all_slave_gtid`, `sql/sql_repl.cc`): a
        // pair the file carries and the state omits sends the dump thread hunting ever-older files
        // until it dies with `Could not find GTID state requested by slave in any binlog files`,
        // which is how every MariaDB leg of CI run 45446296665 went red. A run that joined this
        // stream mid-way never saw a domain some earlier writer used, so on this server — one long
        // lived server, shared by the 29 tests of this binary — the honest expectation is the
        // product's own rule, asked of the product (`source::dump_state_covers`, the function the
        // engine gates on) rather than asserted from a log line. Either anchor has to catch up
        // below; what must not happen is the run dying, or a rebuild hiding the gap.
        // Read in the variable *this* family has: `gtid_binlog_state` is MariaDB's own, and asking
        // a MySQL server for it is `ERROR 1193 (HY000): Unknown system variable`, which is how this
        // leg went red on every MySQL flavor of CI run 45670984231. `common::server_gtid_set` picks
        // per flavor (`@@GLOBAL.gtid_executed` on the MySQL family), which is also the set the rule
        // is really asked against there.
        // Both sides of that prediction are taken at rest, never one mid-flight (lv-super's ruling,
        // CI run 45896362347, red on every MariaDB leg). The engine decides the anchor from the set
        // it *loads* when it starts — the row as the final flush of `stop()` left it (the flush loop
        // writes the truth about what the run consumed whenever that truth changed) — while the row
        // read above was taken while this run was still writing. That last save can be newer and
        // wider than the read above: the INSERT two lines back is itself a GTID, and so is whatever
        // the flush still owed. A `domain-server` pair the newer set names and the older one omits
        // is exactly what flips MariaDB's answer (`contains_all_slave_gtid`), which is how the leg
        // came to say "the saved set names its pairs false" next to a log line saying the dump
        // anchored on the set. So: re-read the row now that the writer has stopped, and read the
        // server's state in the same moment. One snapshot of a quiet system — the only point where
        // what the engine will load and what the server can locate are comparable at all.
        let at_rest = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the final flush saved no cursor at all")?;
        let set = at_rest.gtid().unwrap_or_default();
        ensure!(
            !set.is_empty(),
            "the final flush saved a cursor with no set in it: {at_rest}"
        );
        let node_state = common::server_gtid_set(&mut c, maria).await?;
        let covers = !maria || lazyview::source::dump_state_covers(&node_state, set);
        let anchored = if covers {
            "binlog dump anchored on the consumed GTID set"
        } else {
            "binlog dump anchored on the file and position"
        };
        ctx.log(format!(
            "the server's state carries {} bytes of GTIDs; the saved set names them {}so the \
             restart is expected to open its dump on the {}",
            node_state.len(),
            if covers { "whole, " } else { "partly, " },
            if covers { "set" } else { "file and position" }
        ));
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        // The line the restart has to earn: the run opened its dump the way the server allows. It
        // is written once per connect, so the wait is on the log rather than a guess at how long
        // a start takes — `wait_ready` above answers from the state rows, which the first run
        // already left `ready`.
        let log = eng
            .wait_log_line("binlog dump anchored on the", Duration::from_secs(60))
            .await
            .with_context(|| "the restart never opened a dump at all".to_string())?;
        ensure!(
            log.contains(anchored),
            "the restart did not open the dump on the {anchored} this server's state allows (the \
             saved set names its pairs {covers}); its log says:\n{log}"
        );
        if !covers {
            // Held back, the run has to say which rule held it: `set_bytes=0` alone cannot tell an
            // operator a foreign `gtid_domain_id` from a run that saved no set at all.
            ensure!(
                log.contains("held_back="),
                "the restart fell back to the file and position without naming the rule that \
                 held the set back; its log says:\n{log}"
            );
        }
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "resumed from a cursor the old column cut in half",
        )
        .await?;
        // A run that resumes by GTID keeps the set it resumed on: the row still holds a set past
        // the old limit, whole, after the second run's own saves (which carry it forward).
        let (text, stored) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            stored as usize == text.len() && text.len() > 65_535 && text.contains("[g="),
            "after the restart the row holds {stored} bytes: {text}"
        );
        let back = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the cursor of the restarted run reads back as no cursor")?;
        ensure!(
            back.to_string() == text,
            "the restarted run's cursor reads back as {back}"
        );

        // The same wall from the engine's side, on the row the engine owns. The column is taken
        // back to what an install that never ran `lazyview setup` has — too narrow for this
        // cursor — and lazyview's own sessions run a `sql_mode` without `STRICT_TRANS_TABLES`
        // (`dialect/mysql.rs`), so the server *accepts* the over-long cursor and keeps its first
        // bytes: the save's own round trip is what notices. The run stops naming the column, and
        // the row is left startable — the cursor cleared to NULL, the file and position that save
        // wrote still there — rather than holding the truncated text #347's next start died on.
        // (The refusal *before* the server sees anything, and the untouched row it leaves, is the
        // leg of `lifecycle_a_cursor_past_the_column_is_refused_and_nothing_is_stored`.)
        // The session's own `sql_mode` goes non-strict only so the ALTER below can shrink the
        // column under the 70 KB value the row still holds at that moment. The save cadence is
        // 200 ms, so the engine can slip a whole cursor back into the column between the clear and
        // the ALTER — the ALTER then answers 1406, and clearing and retrying is the answer rather
        // than a wider window.
        exec(&mut c, "SET SESSION sql_mode = ''").await?;
        for attempt in 0..40 {
            exec(&mut c, "UPDATE _lazyview_status SET binlog_cursor = NULL").await?;
            match exec(
                &mut c,
                "ALTER TABLE `_lazyview_status` MODIFY `binlog_cursor` VARCHAR(2000) NULL",
            )
            .await
            {
                Ok(()) => break,
                Err(e) => {
                    let why = format!("{e:#}");
                    ensure!(
                        why.contains("1406") || why.contains("Data too long"),
                        "narrowing the cursor column failed on attempt {attempt}: {why}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        // Something has to make the engine save again: a position save follows a position that
        // moved, and this run is idle now (no view reads the noise table, so the writes cost no
        // refresh). Without them the narrow column would meet no save and the engine would have
        // nothing to die on.
        for i in 0..20 {
            exec(
                &mut c,
                &format!(
                    "INSERT INTO lc_noise (id, pad) VALUES ({}, 'after')",
                    900_000 + i
                ),
            )
            .await?;
        }
        let died = eng
            .wait_death(Duration::from_secs(60))
            .await
            .unwrap_or_else(|| "it is still running".to_string());
        ctx.log(format!("the engine met the narrow column saying: {died}"));
        ensure!(
            died.contains("the server kept") && died.contains("2000"),
            "the engine kept running with a column that cannot hold its cursor ({died:?})"
        );
        let (kept, stored) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            kept.is_empty() && stored == 0,
            "the save the server shortened left {stored} bytes ({}) in the column instead of \
             clearing it",
            kept.chars().take(120).collect::<String>()
        );
        // Startable, not merely empty. `load_position` reads a NULL cursor as "no cursor yet" and
        // hands back the `binlog_file`/`binlog_pos` the refused statement did write, so the row has
        // to answer with a position carrying **no set** — a set here would be the truncated text
        // #347's next start died on — at exactly the coordinate the row holds. (Asking it for
        // `None` was wrong: `None` is a row with no file either, which the save did not leave.)
        let (file, pos): (String, u64) = c
            .query_first(format!(
                "SELECT CAST(binlog_file AS CHAR), binlog_pos FROM _lazyview_status WHERE instance_id = {}",
                q(&instance)
            ))
            .await?
            .context("the status row vanished")?;
        ensure!(
            !file.is_empty() && pos > 0,
            "the cleared row lost its file and position as well: {file}:{pos}"
        );
        let back = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the cleared row hands the next start no position at all")?;
        ensure!(
            back.gtid().is_none() && !back.to_string().contains("[g="),
            "the cleared row still hands the next start a set to read: {back}"
        );
        let at = back.to_position();
        ensure!(
            at.file == file && at.pos == pos,
            "the next start reads {}:{}, not the {file}:{pos} the cleared row holds",
            at.file,
            at.pos
        );
        c.disconnect().await.ok();
        Ok(())
    });
}

/// A cursor that does not fit even the MEDIUMTEXT column is **refused**: `meta::save_position`
/// answers an error naming the size, the limit and the cause, the engine stops on it, and the row
/// keeps the cursor it had — never the first bytes of the one it was given. #356 shed the set
/// instead (a save that quietly disabled resume-by-GTID); shortening is not an answer, so neither
/// is storing half of it.
#[test]
fn lifecycle_a_cursor_past_the_column_is_refused_and_nothing_is_stored() {
    scenario("lc_cursor_refused", |ctx| async move {
        use lazyview::source::{Cursor, Flavor};
        let db = ctx.new_db("lc_cursor_refused").await?;
        let mut c = db.conn().await?;
        let flavor = if ctx.flavor().starts_with("mariadb") {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        };
        // The row as `coord::claim` writes it: an instance, an epoch, no cursor yet.
        setup_run(&db).await?;
        exec(
            &mut c,
            "INSERT INTO _lazyview_status (instance_id, binlog_file, binlog_pos, epoch) \
             VALUES ('lc_refused', 'binlog.000004', 228705009, 7)",
        )
        .await?;
        let at = Cursor::from_parts(flavor, "binlog.000004".into(), 228_705_009);

        // #347's own shape — one interval per gap, the size that killed the smoke's restart — is
        // the first thing that must simply work now: it rides the save whole. This is the leg
        // #356's shed failed: it answered a cursor with no set at all.
        let frag = fragmented_set(flavor, 4_000);
        ensure!(
            frag.len() > 65_535,
            "the #347-shaped set is only {} bytes: it must pass the limit the old column had",
            frag.len()
        );
        let wanted = at.clone().with_gtid(Some(frag.clone()));
        lazyview::meta::save_position(&mut c, "lc_refused", &wanted, 7)
            .await
            .context("a cursor the old TEXT column could not hold was refused")?;
        let (text, stored) = saved_cursor_row(&mut c, "lc_refused").await?;
        ensure!(
            stored as usize == wanted.to_string().len() && text == wanted.to_string(),
            "the row holds {stored} bytes, not the {} of the #347-shaped cursor",
            wanted.to_string().len()
        );
        let back = lazyview::meta::load_position(&mut c, "lc_refused", flavor)
            .await?
            .context("the cursor past the old limit reads back as no cursor")?;
        ensure!(
            back.gtid() == Some(frag.as_str()),
            "the set came back as {:?} bytes, not the {} it was saved as",
            back.gtid().map(str::len),
            frag.len()
        );

        // The same, an order of magnitude bigger: the column is 16 MB, so megabytes ride the save
        // and come back byte for byte.
        let big = fragmented_set_of(flavor, 4 * 1024 * 1024);
        let wanted = at.clone().with_gtid(Some(big.clone()));
        let saved = lazyview::meta::save_position(&mut c, "lc_refused", &wanted, 7)
            .await
            .context("a cursor the MEDIUMTEXT column holds was refused")?;
        ensure!(
            saved.gtid() == Some(big.as_str()),
            "a 4 MB set did not ride the save"
        );
        let (text, stored) = saved_cursor_row(&mut c, "lc_refused").await?;
        let want = wanted.to_string();
        ensure!(
            stored as usize == want.len() && text == want,
            "the row holds {stored} bytes, not the {} the save wrote",
            want.len()
        );
        let back = lazyview::meta::load_position(&mut c, "lc_refused", flavor)
            .await?
            .context("the 4 MB cursor reads back as no cursor")?;
        ensure!(
            back.gtid() == Some(big.as_str()),
            "the 4 MB set came back as {:?} bytes",
            back.gtid().map(str::len)
        );

        // Past the column: refused, loudly, and the row is untouched.
        let monster = fragmented_set_of(flavor, lazyview::meta::MAX_CURSOR_BYTES + 1);
        let too_big = at.clone().with_gtid(Some(monster.clone()));
        let too_big_len = too_big.to_string().len();
        let err = lazyview::meta::save_position(&mut c, "lc_refused", &too_big, 7)
            .await
            .expect_err("a cursor past the column must be refused, not saved");
        let msg = format!("{err:#}");
        ctx.log(format!("refused as: {msg}"));
        let size = too_big_len.to_string();
        let limit = lazyview::meta::MAX_CURSOR_BYTES.to_string();
        for names in [
            size.as_str(),
            limit.as_str(),
            "pathologically fragmented",
            "nothing was stored",
        ] {
            ensure!(
                msg.contains(names),
                "the refusal does not name {names:?}: {msg}"
            );
        }
        let (still, stored) = saved_cursor_row(&mut c, "lc_refused").await?;
        ensure!(
            stored as usize == want.len() && still == want,
            "the refused save changed the row: it holds {stored} bytes of {still}"
        );
        // And the save works again afterwards: a refusal is a refusal of that cursor, not a
        // broken connection or a poisoned row.
        lazyview::meta::save_position(&mut c, "lc_refused", &at, 7)
            .await
            .context("the save did not work again after a refusal")?;
        let (text, stored) = saved_cursor_row(&mut c, "lc_refused").await?;
        ensure!(
            stored as usize == at.to_string().len() && text == at.to_string(),
            "the plain cursor after the refusal is {stored} bytes: {text}"
        );

        // The third leg, and the one no server setting can take away: an install whose column is
        // narrower than the cursor, on a session whose `sql_mode` does not refuse an over-long
        // value. That is exactly #347's silence — the server keeps the first bytes, warns nobody,
        // and the next start reads a cut cursor. `save_position` measures what the server says it
        // kept, so the save fails here instead of at the next start.
        exec(&mut c, "SET SESSION sql_mode = ''").await?;
        exec(
            &mut c,
            "ALTER TABLE `_lazyview_status` MODIFY `binlog_cursor` VARCHAR(2000) NULL",
        )
        .await?;
        let narrow = at.clone().with_gtid(Some(fragmented_set(flavor, 200)));
        let err = lazyview::meta::save_position(&mut c, "lc_refused", &narrow, 7)
            .await
            .expect_err("a column narrower than the cursor must fail the save, not truncate it");
        let msg = format!("{err:#}");
        ctx.log(format!("the narrow column answered: {msg}"));
        ensure!(
            msg.contains("the server kept") && msg.contains("2000"),
            "the round-trip check says {msg:?}, which does not name what the server kept"
        );
        // And `lazyview setup` is the fix it names: after the migration the same cursor rides the
        // save again on the same connection.
        setup_run(&db).await?;
        lazyview::meta::save_position(&mut c, "lc_refused", &narrow, 7)
            .await
            .context("the same cursor was refused after `lazyview setup` widened the column")?;
        let (text, stored) = saved_cursor_row(&mut c, "lc_refused").await?;
        ensure!(
            stored as usize == narrow.to_string().len() && text == narrow.to_string(),
            "after the migration the row holds {stored} bytes, not {}",
            narrow.to_string().len()
        );
        c.disconnect().await.ok();
        Ok(())
    });
}

/// An install that stopped at the old, narrow `binlog_cursor` is migrated by `lazyview setup`
/// (`meta::ensure`, the same idempotent path every earlier column change uses), on the metadata
/// server the status row lives on — and only then is a cursor wider than that column storable. The
/// operations run refuses to start on the un-migrated column at all (`meta::verify`), naming the
/// setup run: the round trip is the guarantee, but a schema it can already see is fixed at startup.
#[test]
fn lifecycle_setup_widens_an_old_cursor_column() {
    scenario("lc_cursor_widen", |ctx| async move {
        use lazyview::source::{Cursor, Flavor};
        let db = ctx.new_db("lc_cursor_widen").await?;
        let mut c = db.conn().await?;
        let flavor = if ctx.flavor().starts_with("mariadb") {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        };
        // A new install gets the wide column from `CREATE TABLE`.
        setup_run(&db).await?;
        ensure!(
            cursor_column_type(&mut c).await? == "mediumtext",
            "a new install did not get a MEDIUMTEXT cursor column"
        );
        // The old install: the width `ensure` left behind before #356 widened it, with a row in it.
        exec(
            &mut c,
            "ALTER TABLE `_lazyview_status` MODIFY `binlog_cursor` TEXT NULL",
        )
        .await?;
        ensure!(
            cursor_column_type(&mut c).await? == "text",
            "the simulated old install is not a TEXT column"
        );
        exec(
            &mut c,
            "INSERT INTO _lazyview_status (instance_id, binlog_file, binlog_pos, epoch) \
             VALUES ('lc_widen', 'binlog.000001', 4242, 3)",
        )
        .await?;
        // The setup run migrates it, and does it twice without minding: idempotent, like every
        // other column change in `MIGRATIONS`.
        setup_run(&db).await?;
        ensure!(
            cursor_column_type(&mut c).await? == "mediumtext",
            "`lazyview setup` left an old TEXT cursor column narrow"
        );
        setup_run(&db).await?;
        ensure!(
            cursor_column_type(&mut c).await? == "mediumtext",
            "the second setup run narrowed or lost the cursor column"
        );
        let (text, stored) = saved_cursor_row(&mut c, "lc_widen").await?;
        ensure!(
            stored == 0 && text.is_empty(),
            "the migration rewrote the row it was only supposed to widen: {text}"
        );
        // The point of the width: the set that used to be cut rides the migrated row whole.
        let set = fragmented_set_of(flavor, 70_000);
        let at = Cursor::from_parts(flavor, "binlog.000001".into(), 4242);
        let wanted = at.clone().with_gtid(Some(set));
        lazyview::meta::save_position(&mut c, "lc_widen", &wanted, 3)
            .await
            .context(
                "a cursor the old TEXT column could not hold was refused after the migration",
            )?;
        let (text, stored) = saved_cursor_row(&mut c, "lc_widen").await?;
        ensure!(
            stored as usize == wanted.to_string().len() && text == wanted.to_string(),
            "the migrated row holds {stored} bytes, not the {} the save wrote",
            wanted.to_string().len()
        );
        // Before it is migrated, the operations run says so instead of truncating: `verify` is that
        // check, and its error names the setup run. The row is emptied first — the leg is about the
        // width of the column, and strict mode rightly refuses to shrink a column under a value it
        // cannot hold (the same refusal that made #347 loud on a strict server).
        exec(&mut c, "UPDATE _lazyview_status SET binlog_cursor = NULL").await?;
        exec(
            &mut c,
            "ALTER TABLE `_lazyview_status` MODIFY `binlog_cursor` TEXT NULL",
        )
        .await?;
        let err = lazyview::meta::verify(&mut c, &db.name, &db.name)
            .await
            .expect_err("the operations run must not accept a narrow cursor column");
        let msg = format!("{err:#}");
        ensure!(
            msg.contains("binlog_cursor") && msg.contains("lazyview setup"),
            "the pre-flight says {msg:?}, which names neither the column nor the fix"
        );
        setup_run(&db).await?;
        lazyview::meta::verify(&mut c, &db.name, &db.name)
            .await
            .context("the operations run still refuses after the migration")?;
        c.disconnect().await.ok();
        Ok(())
    });
}

/// The upgrade path of #347's cursor column, in the order an operator meets it: the operations run
/// against a schema whose `binlog_cursor` still has the width an old install left it **refuses to
/// start** (exit 2, naming the column and `lazyview setup`) rather than starting and truncating its
/// own saves; `lazyview setup` widens the column without touching the row; and the next run resumes
/// on the position that row still holds — the pre-cursor columns an install from before #347 left —
/// and brings the writes that landed while every run was down up to the view with no whole rebuild,
/// and fills the widened column with its whole consumed set.
///
/// Provenance, stated once and plainly: this scenario is **new**, and its four legs are not equal
/// evidence. The refusal leg is the reproduced failure: at the PR's base commit 1068cbb (`git
/// rev-parse` of the branch's base; the merge-base with `main` is cf572ac) `verify` did not look at
/// the column's width, so the run started against the narrow column and #347's truncation followed
/// — that leg is red there. The other three legs (the row unchanged by the refusal, the widened
/// column with the row intact, the resume on the surviving cursor) are **newly covered, never red**:
/// nothing in the suite walked that round trip before, which is why the round trip is where the
/// bug lived.
#[test]
fn lifecycle_setup_refuses_and_migrates_a_narrow_cursor_column() {
    scenario("lc_cursor_upgrade", |ctx| async move {
        use lazyview::source::Flavor;
        use mysql_async::prelude::Queryable;
        let db = ctx.new_db("lc_cursor_upgrade").await?;
        let mut c = db.conn().await?;
        let maria = ctx.flavor().starts_with("mariadb");
        let flavor = if maria {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        };
        for s in [
            "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
            "INSERT INTO entities VALUES (10, 1, 'new'), (11, 1, 'paid')",
        ] {
            exec(&mut c, s).await?;
        }
        let specs = vec![ViewSpec::new(
            "lc_counts",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n FROM entities GROUP BY grp",
        )];
        // A new install: `ensure` gives the cursor column its MEDIUMTEXT width.
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        for id in 20..25 {
            exec(
                &mut c,
                &format!("INSERT INTO entities VALUES ({id}, 2, 'paid')"),
            )
            .await?;
        }
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        let instance: String = c
            .query_first("SELECT CAST(instance_id AS CHAR) FROM _lazyview_status")
            .await?
            .context("no status row to save into")?;
        eng.stop().await?; // the clean stop's forced save is the row's last word
        let (saved, stored) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            !saved.is_empty() && stored as usize == saved.len(),
            "the run left no cursor in the row to migrate (stored {stored} of {} bytes)",
            saved.len()
        );
        // A run whose source assigns GTIDs carries its consumed set in the cursor, and that is the
        // half a resume is checked against; on a server started without `--gtid-mode=ON` (the
        // `mysql80`/`percona84` services, CI included) there is no set to carry, and the legs below
        // ask only for what the source can give.
        let has_gtid = maria
            || scalar(&mut c, "SELECT CAST(@@GLOBAL.gtid_mode AS CHAR)")
                .await?
                .as_deref()
                == Some("ON");
        if has_gtid {
            ensure!(
                saved.contains("[g="),
                "the cursor this server assigns GTIDs on holds no set: {saved}"
            );
        }
        // The old install, as the next run sees it: the width `ensure` left before #347 widened it,
        // and the row such an install actually holds — no cursor text, the position in the two
        // pre-cursor columns, which is where [`lazyview::meta::load_position`] reads a row written
        // before the cursor existed. Clearing the text is not a convenience: on the server CI's legs
        // share, where every earlier run's injector has been writing GTIDs for hours, the set a run
        // carries is larger than a TEXT can hold, and the ALTER below answers `ERROR 1406 (22001):
        // Data too long for column 'binlog_cursor'` (every MySQL and MariaDB leg of CI run
        // 45670984231) — a story about the value, not about the column. What the set costs is
        // `lifecycle_saved_cursor_holds_the_whole_set_and_resumes_by_gtid`'s to tell. This leg is
        // about the column, and about a row that keeps its position through all three steps.
        exec(
            &mut c,
            &format!(
                "UPDATE `_lazyview_status` SET `binlog_cursor` = NULL WHERE `instance_id` = {}",
                q(&instance)
            ),
        )
        .await?;
        let (at_file, at_pos) = saved_position_row(&mut c, &instance).await?;
        exec(
            &mut c,
            "ALTER TABLE `_lazyview_status` MODIFY `binlog_cursor` TEXT NULL",
        )
        .await?;
        ensure!(
            cursor_column_type(&mut c).await? == "text",
            "the simulated old install is not a TEXT column"
        );
        let (kept, kept_file, kept_pos) = saved_row_shape(&mut c, &instance).await?;
        ensure!(
            kept.is_empty() && kept_file == at_file && kept_pos == at_pos,
            "narrowing the column disturbed the row: it holds cursor {kept:?} at \
             {kept_file}:{kept_pos}, expected no cursor at {at_file}:{at_pos}"
        );

        // 1. The refusal, as the operator sees it: the *binary*, because the exit code is the
        // signal, and with the harness's own setup skipped so nothing widens the column behind the
        // test's back.
        let mut eng = common::engineproc::EngineProc::start_with(
            &ctx,
            &db,
            &[("LAZYVIEW_TEST_SKIP_SETUP", "1")],
            &[],
        )
        .await?;
        let status = eng.wait_exit(Duration::from_secs(90))?;
        let log = eng.log_tail(200);
        ensure!(
            status.code() == Some(2),
            "the operations run on the un-migrated column exited {status:?} instead of 2; log:\n{log}"
        );
        ensure!(
            log.contains("binlog_cursor")
                && log.contains("mediumtext")
                && log.contains("lazyview setup"),
            "the refusal names neither the column, its width, and the fix; log:\n{log}"
        );
        let col = cursor_column_type(&mut c).await?;
        let (kept, kept_file, kept_pos) = saved_row_shape(&mut c, &instance).await?;
        ensure!(
            kept.is_empty() && kept_file == at_file && kept_pos == at_pos && col == "text",
            "the refused run left its mark: the row reads cursor {kept:?} at {kept_file}:{kept_pos} \
             under a {col:?} column — expected no cursor at {at_file}:{at_pos} under a text column"
        );
        ctx.log("the operations run refused on the narrow column and touched nothing");

        // 2. `lazyview setup`, run the way an operator runs it — with the account that owns DDL.
        let mut su = common::engineproc::EngineProc::start_with(
            &ctx,
            &db,
            &[("LAZYVIEW_TEST_SKIP_SETUP", "1"), ("LAZYVIEW_DSN", &db.dsn)],
            &["setup"],
        )
        .await?;
        let status = su.wait_exit(Duration::from_secs(90))?;
        let log = su.log_tail(200);
        ensure!(
            status.success(),
            "`lazyview setup` on the un-migrated column exited {status:?}; log:\n{log}"
        );
        ensure!(
            cursor_column_type(&mut c).await? == "mediumtext",
            "`lazyview setup` left the cursor column narrow"
        );
        let (kept, kept_file, kept_pos) = saved_row_shape(&mut c, &instance).await?;
        ensure!(
            kept.is_empty() && kept_file == at_file && kept_pos == at_pos,
            "the migration rewrote the row it widened: it reads cursor {kept:?} at \
             {kept_file}:{kept_pos}, expected no cursor at {at_file}:{at_pos}"
        );

        // 3. The resume, on that surviving cursor. The writes below land while no run is up, so a
        // start that believed a stale position had it covered would miss them, and a start that
        // rebuilt the view would hide the miss.
        for id in 30..40 {
            exec(
                &mut c,
                &format!("INSERT INTO entities VALUES ({id}, 3, 'new')"),
            )
            .await?;
        }
        let resumed_on = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the row did not survive the migration and the refusal")?;
        ensure!(
            resumed_on.gtid().is_none()
                && resumed_on.to_position().file == at_file
                && resumed_on.to_position().pos == at_pos,
            "the row the refusal and the migration left reads back as {resumed_on}, not the \
             position the run stopped at ({at_file}:{at_pos})"
        );
        let from = resumed_on.to_position();
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        let log = common::engineproc::tail_lines(&eng.log_path, 400);
        ensure!(
            log.contains(&format!("streaming binlog position={from}")),
            "the restarted run did not start from the cursor the row held ({from}); its log says:\n{log}"
        );
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "resume after the cursor-column migration",
        )
        .await?;
        let snap = eng.snapshot();
        ensure!(
            snap.full_rebuilds == 0,
            "resuming after the migration cost {} full rebuild(s): the gap was repaired by \
             rebuilding, not by streaming; metrics {}",
            snap.full_rebuilds,
            fmt_snapshot(&snap)
        );
        // The run's own word that the row is current again, in the server's own reading of it.
        eng.stop().await?;
        let back = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the resumed run saved no cursor")?;
        ensure!(
            back.to_position() >= from,
            "the resumed run left the row behind where it found it: {} vs {from}",
            back.to_position()
        );
        ensure!(
            lazyview::source::gtid_covers(&mut c, &back).await?,
            "the cursor the resumed run saved ({}) is not one this server covers",
            back
        );
        // And the widened column is what the run fills: whole in the server's own reading, and past
        // the 65,535 bytes the old column stopped at whenever the set is that big. A fresh server's
        // set is smaller than that, and there the leg has nothing to prove about the width.
        let (text, stored) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            stored as usize == text.len() && back.to_string() == text,
            "the resumed run's cursor is not the row's own word: the server holds {stored} of {} \
             bytes and the row parses as {back}",
            text.len()
        );
        if has_gtid && saved.len() > 65_535 {
            ensure!(
                text.len() > 65_535 && text.contains("[g="),
                "the resumed run wrote back a {}-byte cursor through the widened column although \
                 the run before the migration carried {} bytes of set: the set was shed",
                text.len(),
                saved.len()
            );
        }
        c.disconnect().await.ok();
        Ok(())
    });
}

/// The rule the pacing work of #356 will lean on, proved here where it stands on its own: the row's
/// `binlog_cursor` can name a position **behind** what the run has applied. The two pre-cursor
/// columns and the cursor column are whatever whoever held the row last left there — a save that
/// failed midway, a row an older build wrote, a row the harness wrote by hand — and a restart then
/// opens its dump at the older position and re-reads, and re-applies, binlog it has already applied.
///
/// That is safe only because re-applying a change is idempotent — which this scenario exists to
/// *prove*, not to assume: the cursor is put back by hand (the row's two columns left ahead), the
/// run has to open its dump at that older position — the log line says so — and the oracle has to
/// answer equal afterwards, with no whole rebuild allowed to hide the double work or the miss.
#[test]
fn lifecycle_resumes_from_a_cursor_behind_the_applied_position() {
    scenario("lc_cursor_behind", |ctx| async move {
        use lazyview::source::Flavor;
        use mysql_async::prelude::Queryable;
        let db = ctx.new_db("lc_cursor_behind").await?;
        let mut c = db.conn().await?;
        let flavor = if ctx.flavor().starts_with("mariadb") {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        };
        for s in [
            "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
            "INSERT INTO entities VALUES (10, 1, 'new'), (11, 1, 'paid')",
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
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let instance: String = c
            .query_first("SELECT CAST(instance_id AS CHAR) FROM _lazyview_status")
            .await?
            .context("no status row to save into")?;
        // The cursor of the run's first word: this is the one the row is put back to.
        let (dsn, inst) = (db.dsn.clone(), instance.clone());
        wait_until(
            "the run's first cursor in the row",
            Duration::from_secs(60),
            || cursor_written(dsn.clone(), inst.clone()),
        )
        .await
        .context("the run saved no cursor at all, so there is nothing to put back")?;
        let (behind, _) = saved_cursor_row(&mut c, &instance).await?;
        // The applied position runs ahead of it, and the row follows on the save cadence.
        for id in 20..60 {
            exec(
                &mut c,
                &format!("INSERT INTO entities VALUES ({id}, 2, 'paid')"),
            )
            .await?;
        }
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        let (dsn, inst, seeded) = (db.dsn.clone(), instance.clone(), behind.clone());
        let moved = wait_until(
            "the row's cursor to move past the one this test puts back",
            Duration::from_secs(60),
            || cursor_moved(dsn.clone(), inst.clone(), seeded.clone()),
        )
        .await
        .is_ok();
        let (ahead, _) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            moved && ahead != behind,
            "the row's cursor never moved past the {}-byte one this test puts back, so the              restart below would prove nothing about a lagging cursor",
            behind.len()
        );
        ctx.log(format!(
            "the row holds a {}-byte cursor; the restart is put back to the {}-byte one behind it",
            ahead.len(),
            behind.len()
        ));
        eng.stop().await?;
        // The gap no run ever saw, so the view is behind by these and by nothing else.
        for id in 60..80 {
            exec(
                &mut c,
                &format!("INSERT INTO entities VALUES ({id}, 3, 'new')"),
            )
            .await?;
        }
        // The row as a save that stopped between the halves leaves it: the two pre-cursor columns
        // ahead, the cursor from before the applied position. `load_position` reads the cursor, so
        // the start below begins there and has to re-apply what the view has already counted.
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview_status SET binlog_cursor = {} WHERE instance_id = {}",
                q(&behind),
                q(&instance)
            ),
        )
        .await?;
        let (now, _) = saved_cursor_row(&mut c, &instance).await?;
        ensure!(
            now == behind,
            "the row does not hold the cursor this test wrote: {now}"
        );
        let stale = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the put-back cursor does not read back as a cursor")?;
        let stale_pos = stale.to_position();

        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        eng.wait_caught_up(&mut c, t_catchup()).await?;
        // The re-read really happened: the stream opened where the row's cursor said, not where the
        // server had got to.
        let log = common::engineproc::tail_lines(&eng.log_path, 400);
        ensure!(
            log.contains(&format!("streaming binlog position={stale_pos}")),
            "the restarted run did not start from the stale cursor's {stale_pos}, so it never \
             re-read what it had already applied; its log says:\n{log}"
        );
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "resume from a cursor behind the applied position",
        )
        .await?;
        let snap = eng.snapshot();
        ensure!(
            snap.events > 0,
            "the restarted run read no binlog events, so it proved nothing about re-applying: {}",
            fmt_snapshot(&snap)
        );
        ensure!(
            snap.full_rebuilds == 0,
            "resuming from a cursor behind the applied position cost {} full rebuild(s): the view \
             was repaired by rebuilding rather than by re-applying the binlog, which is the \
             idempotence this scenario exists to prove; metrics {}",
            snap.full_rebuilds,
            fmt_snapshot(&snap)
        );
        // And the run puts the row right again: its own saves carry the cursor past where the test
        // left it, in a set this server answers for.
        eng.stop().await?;
        let back = lazyview::meta::load_position(&mut c, &instance, flavor)
            .await?
            .context("the restarted run saved no cursor")?;
        ensure!(
            back.to_position() > stale_pos,
            "the restarted run left the row at {} — the stale cursor is still what a start would \
             read: {back}",
            back.to_position()
        );
        ensure!(
            lazyview::source::gtid_covers(&mut c, &back).await?,
            "the repaired cursor {back} is not one this server covers",
        );
        c.disconnect().await.ok();
        Ok(())
    });
}
