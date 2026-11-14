//! Targets: MVs materialized on other instances (other flavors). The oracle VIEW lives on the
//! source; the MV is read from each target. Fan-out (one view → several instances), online
//! retargeting (add / remove a target), errors (chained view whose upstream is not on the source,
//! unknown target) and one target being down must not affect the others.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use common::workload::{ecommerce_views, names, run_batches, setup_ecommerce, Sizes, Workload};
use common::*;

async fn exists_on(db: &Db, table: &str) -> Result<bool> {
    let mut c = db.conn().await?;
    let e = table_exists(&mut c, table).await?;
    c.disconnect().await.ok();
    Ok(e)
}

/// Asserts that every view's MV exists exactly on the instances it targets.
async fn check_placement(
    src: &Db,
    targets: &HashMap<String, Db>,
    specs: &[ViewSpec],
) -> Result<()> {
    for s in specs {
        let want = s.target_names();
        let on_src = exists_on(src, &s.name).await?;
        ensure!(
            on_src == want.contains(&"source".to_string()),
            "view {} (targets {:?}): exists on source = {on_src}",
            s.name,
            s.targets
        );
        for (t, db) in targets {
            let here = exists_on(db, &s.name).await?;
            ensure!(
                here == want.contains(t),
                "view {} (targets {:?}): exists on target {t} = {here}",
                s.name,
                s.targets
            );
        }
    }
    Ok(())
}

async fn wait_exists(eng: &mut Engine, db: &Db, table: &str, exists: bool) -> Result<()> {
    let deadline = std::time::Instant::now() + t_ready();
    loop {
        eng.check_alive()?;
        if exists_on(db, table).await? == exists {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!(
                "{table} on {} ({}): expected exists={exists}",
                db.name,
                db.flavor
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn targets_fan_out_and_retarget() {
    scenario_multi("tg_fanout", 2, |ctx| async move {
        let src = ctx.new_db("tg_src").await?;
        let tb = ctx.new_db_on(1, "tg_b").await?;
        let tc = ctx.new_db_on(2, "tg_c").await?;
        let ids = setup_ecommerce(&src, ctx.sub_seed("seed"), Sizes::scaled()).await?;
        let patterns = [
            None,
            Some("b"),
            Some("c"),
            Some("b,c"),
            Some("source,b"),
            Some("source,b,c"),
        ];
        let mut specs = Vec::new();
        let mut k = 0;
        for v in ecommerce_views() {
            let v = match v.name.as_str() {
                // upstream of the chained views must live on the source
                "v_customer_stats" => v.with_targets("source,c"),
                "v_vip_stats" => v.with_targets("c"),
                "v_tier_revenue" => v.with_targets("source,b"),
                _ => {
                    k += 1;
                    match patterns[k % patterns.len()] {
                        Some(t) => v.with_targets(t),
                        None => v,
                    }
                }
            };
            specs.push(v);
        }
        let tmap: HashMap<String, Db> =
            [("b".to_string(), tb.clone()), ("c".to_string(), tc.clone())].into();
        let env = target_env(&[("b", &tb.dsn), ("c", &tc.dsn)]);
        let (mut eng, idmap) = start_with_views(&ctx, &src, &specs, &ov_refs(&env)).await?;
        let mut c = src.conn().await?;
        verify_targets(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &HashMap::new(),
            &specs,
            "initial build on all targets",
        )
        .await?;
        check_placement(&src, &tmap, &specs).await?;

        let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
        for phase in 1..=2 {
            let n = run_batches(&mut c, &mut wl, scaled(60)).await?;
            verify_targets(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &HashMap::new(),
                &specs,
                &format!("phase {phase} ({n} txns)"),
            )
            .await?;
        }

        // Online retargeting: add a target, remove a target, drop the source copy.
        let find = |specs: &Vec<ViewSpec>, t: &str| {
            specs
                .iter()
                .position(|s| s.targets.as_deref() == Some(t) && s.kind != Kind::Chained)
                .context("pattern")
        };
        let changes = [
            (find(&specs, "b")?, "b,c"),
            (find(&specs, "b,c")?, "c"),
            (find(&specs, "source,b")?, "b"),
        ];
        for (i, new_t) in changes {
            let name = specs[i].name.clone();
            let old = specs[i].target_names();
            exec(
                &mut c,
                &format!(
                    "UPDATE _lazyview SET targets = {} WHERE id = {}",
                    q(new_t),
                    idmap[&name]
                ),
            )
            .await?;
            specs[i].targets = Some(new_t.to_string());
            let new = specs[i].target_names();
            for t in old.iter().chain(new.iter()) {
                let db = if t == "source" { &src } else { &tmap[t] };
                wait_exists(&mut eng, db, &name, new.contains(t))
                    .await
                    .with_context(|| format!("retarget {name} {old:?} → {new:?}"))?;
            }
            run_batches(&mut c, &mut wl, scaled(15)).await?;
            wait_ready_tolerant(&mut c, &mut eng, &[name.as_str()], t_ready()).await?;
            ctx.log(format!("retargeted {name}: {old:?} → {new:?}"));
        }
        verify_targets(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &HashMap::new(),
            &specs,
            "after retargeting",
        )
        .await?;
        check_placement(&src, &tmap, &specs).await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn targets_invalid_configurations_error() {
    scenario_multi("tg_errors", 1, |ctx| async move {
        let src = ctx.new_db("tg_err_src").await?;
        let tb = ctx.new_db_on(1, "tg_err_b").await?;
        setup_ecommerce(&src, ctx.sub_seed("seed"), Sizes::tiny()).await?;
        let all = ecommerce_views();
        let get = |n: &str| all.iter().find(|v| v.name == n).unwrap().clone();
        let good = vec![
            get("v_status_counts").with_targets("b"),
            get("v_paid_orders"),
        ];
        let upstream_on_target = get("v_customer_stats").with_targets("b");
        let chained = get("v_vip_stats"); // reads v_customer_stats, which only exists on target b
        let unknown = get("v_global_orders").with_targets("nope");
        let mut c = src.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &good).await?;
        add_views(
            &mut c,
            &[upstream_on_target.clone(), chained.clone(), unknown.clone()],
        )
        .await?;
        let tmap: HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
        let env = target_env(&[("b", &tb.dsn)]);
        let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
        wait_ready(&mut c, &mut eng, &["v_customer_stats"], t_ready()).await?;
        for bad in [&chained, &unknown] {
            let st = wait_state(&mut c, &mut eng, &bad.name, "error", t_ready()).await?;
            ctx.log(format!(
                "{} (targets {:?}): error as expected: {}",
                bad.name,
                bad.targets,
                clip(&st.last_error.unwrap_or_default(), 160)
            ));
        }
        ensure!(
            !exists_on(&src, "v_global_orders").await?
                && !exists_on(&tb, "v_global_orders").await?,
            "view with unknown target was materialized"
        );
        wait_ready(&mut c, &mut eng, &names(&good), t_ready()).await?;
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id % 2 = 0").await?;
        let mut checked = good.clone();
        checked.push(upstream_on_target);
        verify_targets(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &HashMap::new(),
            &checked,
            "valid views next to invalid ones",
        )
        .await?;
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn targets_one_target_down() {
    scenario_multi("tg_down", 1, |ctx| async move {
        let src = ctx.new_db("tg_down_src").await?;
        let tb = ctx.new_db_on(1, "tg_down_b").await?;
        let ids = setup_ecommerce(&src, ctx.sub_seed("seed"), Sizes::scaled()).await?;
        let all = ecommerce_views();
        let get = |n: &str| all.iter().find(|v| v.name == n).unwrap().clone();
        let healthy = vec![
            get("v_order_lines").with_targets("b"),
            get("v_customer_stats"),
            get("v_status_counts").with_targets("source,b"),
            get("v_orders_left").with_targets("b,down"),
            get("v_category_sales").with_targets("b,down"),
        ];
        let only_down = vec![get("v_paid_orders").with_targets("down")];
        let tmap: HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
        let mut c = src.conn().await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &healthy).await?;
        add_views(&mut c, &only_down).await?;
        // Port 1 on localhost: connection refused.
        let dead = dsn_with_db("mysql://lv:lv@127.0.0.1:1/nowhere", "nowhere");
        let env = target_env(&[("b", &tb.dsn), ("down", &dead)]);
        let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
        let converge = Duration::from_secs_f64(150.0 * scale().max(1.0));
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            converge,
            "healthy targets with one target down",
        )
        .await?;

        let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
        run_batches(&mut c, &mut wl, scaled(50)).await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            converge,
            "after writes, one target down",
        )
        .await?;

        // Transient outage of target b: its connections get killed repeatedly during writes.
        let killer_db = tb.clone();
        let killer = tokio::spawn(async move {
            let mut admin = killer_db.conn().await?;
            let mut killed = 0;
            for _ in 0..20 {
                let ids = session_ids(&mut admin, &format!("DB = {}", q(&killer_db.name))).await?;
                for id in ids {
                    if exec(&mut admin, &format!("KILL {id}")).await.is_ok() {
                        killed += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Ok::<usize, anyhow::Error>(killed)
        });
        run_batches(&mut c, &mut wl, scaled(50)).await?;
        let killed = killer.await.context("killer panicked")??;
        ctx.log(format!("killed {killed} target connections during writes"));
        run_batches(&mut c, &mut wl, scaled(10)).await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            converge,
            "after transient outage of b",
        )
        .await?;
        for s in view_states(&mut c).await? {
            ctx.log(format!(
                "state {} = {} {}",
                s.name,
                s.state,
                clip(s.last_error.as_deref().unwrap_or(""), 120)
            ));
        }
        eng.check_alive()?;
        drop(eng); // kill: a graceful stop may legitimately wait for the dead target
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// PostgreSQL target (PR T6): the first build of one view onto a `postgres://` target.
// ---------------------------------------------------------------------------------------------

use mysql_async::prelude::Queryable as _;
use mysql_async::{Row, Value};

/// Runs `stmts` on the PostgreSQL at `dsn` and, when `select` is given, fetches its rows as
/// text (every column cast to `::text` by the caller, the T2 text forms). The driver task is
/// joined when the client drops, so no backend outlives the call.
async fn pg_exec(dsn: &str, stmts: &[&str], select: Option<&str>) -> Result<Vec<Vec<String>>> {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connecting to {dsn}"))?;
    let driver = tokio::spawn(conn);
    let r = async {
        for s in stmts {
            client
                .batch_execute(s)
                .await
                .with_context(|| format!("PostgreSQL statement `{s}`"))?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut out = Vec::new();
    if r.is_ok() {
        if let Some(q) = select {
            let rows = client
                .query(q, &[])
                .await
                .with_context(|| format!("PostgreSQL query `{q}`"))?;
            for row in rows {
                out.push(
                    (0..row.len())
                        .map(|i| row.get::<_, Option<String>>(i).unwrap_or_default())
                        .collect(),
                );
            }
        }
    }
    drop(client);
    let _ = driver.await;
    r?;
    Ok(out)
}

/// The first build of a view onto a PostgreSQL target (T6): engine on a MySQL source, one
/// neutral view (`links` over `entities`/`items`) targeted at a `postgres://` DSN, the build's
/// rows compared with the source query. The throwaway database/schema is `lvsrc_t6_<flavor>`
/// (created here, dropped at the end). Runs only when a PostgreSQL is published:
/// `LAZYVIEW_T6_PG_DSN` (or C1's `LAZYVIEW_TEST_PG_DSNS`, first entry); otherwise it prints a
/// SKIP line, so CI without a PostgreSQL server is unaffected.
#[test]
fn targets_pg_first_build() {
    let base = std::env::var("LAZYVIEW_T6_PG_DSN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("LAZYVIEW_TEST_PG_DSNS")
                .ok()
                .and_then(|l| {
                    l.split(',').next().map(|e| {
                        e.split_once('=')
                            .map_or_else(|| e.to_string(), |(_, d)| d.to_string())
                    })
                })
                .filter(|s| !s.trim().is_empty())
        });
    let Some(base) = base else {
        println!("SKIP targets_pg_first_build: no PostgreSQL DSN (LAZYVIEW_T6_PG_DSN / LAZYVIEW_TEST_PG_DSNS)");
        return;
    };
    scenario("tg_pg_build", move |ctx| {
        let base = base.clone();
        async move {
            let src = ctx.new_db("tg_pg_src").await?;
            let schema = format!("lvsrc_t6_{}", src.flavor.replace('-', "_"));
            let rest = base.split_once("://").map_or(base.as_str(), |(_, r)| r);
            let rest = rest.split(['?', '#']).next().unwrap_or(rest);
            let admin_dsn = format!("postgres://{rest}"); // as given: its path is the admin database
            let pg_dsn = match admin_dsn.rfind('/') {
                Some(i) => format!("{}{schema}", &admin_dsn[..=i]),
                None => bail!("the PostgreSQL DSN must name a database: {base}"),
            };
            // (re)create the throwaway database and its schema, the target's `schema_of` path.
            pg_exec(
                &admin_dsn,
                &[
                    &format!("DROP DATABASE IF EXISTS {schema} WITH (FORCE)"),
                    &format!("CREATE DATABASE {schema}"),
                ],
                None,
            )
            .await?;
            pg_exec(&pg_dsn, &[&format!("CREATE SCHEMA {schema}")], None).await?;
            let mut c = src.conn().await?;
            exec_all(
            &mut c,
            &[
                "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
                "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL)",
                "INSERT INTO entities VALUES (1,'alpha'),(2,'beta'),(3,'gamma')",
                "INSERT INTO items VALUES (1,1,10.50),(2,1,20.25),(3,2,5.00),(4,3,7.75),(5,2,0.00)",
            ],
        )
        .await?;
            ensure_lazyview_table(&mut c).await?;
            let spec = ViewSpec::new(
            "links",
            Kind::Incremental,
            "SELECT e.id AS id, e.name AS name, i.amount AS amount FROM entities e JOIN items i ON i.entity_id = e.id",
        )
        .with_targets("pg");
            add_views(&mut c, std::slice::from_ref(&spec)).await?;
            let env = target_env(&[("pg", &pg_dsn)]);
            let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
            wait_ready(&mut c, &mut eng, &["links"], t_ready()).await?;
            // The source's own answer to the view's SELECT (raw text per cell).
            let mut want: Vec<Vec<String>> = Vec::new();
            let mut it = c
            .query_iter(
                "SELECT e.id, e.name, i.amount FROM entities e JOIN items i ON i.entity_id = e.id",
            )
            .await?;
            let rows: Vec<Row> = it.collect().await?;
            drop(it);
            for row in rows {
                want.push(
                    (0..row.len())
                        .map(|i| match row.get::<Value, _>(i) {
                            Some(Value::Int(v)) => v.to_string(),
                            Some(Value::Bytes(b)) => String::from_utf8_lossy(&b).into_owned(),
                            Some(Value::NULL) | None => String::new(),
                            Some(other) => format!("{other:?}"),
                        })
                        .collect(),
                );
            }
            // The table the build created on the target, as text, with its ownership marker.
            let got = pg_exec(
            &pg_dsn,
            &[],
            Some(&format!(
                "SELECT id::text, name::text, amount::text FROM \"{schema}\".links ORDER BY id, amount"
            )),
        )
        .await?;
            let marker = pg_exec(
                &pg_dsn,
                &[],
                Some(&format!(
                    "SELECT obj_description(c.oid)::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = '{schema}' AND c.relname = 'links'"
                )),
            )
            .await?;
            eng.stop().await?;
            pg_exec(
                &admin_dsn,
                &[&format!("DROP DATABASE IF EXISTS {schema} WITH (FORCE)")],
                None,
            )
            .await?;
            want.sort();
            let mut got = got;
            got.sort();
            ctx.log(format!(
                "PostgreSQL build: {} rows in `{schema}`.links, marker {:?}",
                got.len(),
                marker.first().and_then(|m| m.first())
            ));
            ensure!(
                marker
                    .first()
                    .and_then(|m| m.first())
                    .is_some_and(|m| m.starts_with("lazyview:v1")),
                "the built table carries no lazyview ownership marker: {marker:?}"
            );
            ensure!(
                got == want,
                "rows differ:\nsource: {want:?}\ntarget: {got:?}"
            );
            Ok(())
        }
    });
}

// ---------------------------------------------------------------------------------------------
// PostgreSQL target (PR C1): the harness scenarios. The source stays MySQL-family; PostgreSQL
// is only ever a target (LAZYVIEW_TEST_PG_DSNS). The new objects are neutral: pg_a / pg_b and
// entities / items / links.
// ---------------------------------------------------------------------------------------------

async fn exists_on_pg(pgdb: &pg::PgDb, table: &str) -> Result<bool> {
    let c = pg::connect(&pgdb.dsn).await?;
    let e = pg::table_exists(&c, &pgdb.schema, table).await?;
    drop(c);
    Ok(e)
}

/// [`wait_exists`] for a PostgreSQL target table.
async fn wait_exists_pg(
    eng: &mut Engine,
    pgdb: &pg::PgDb,
    table: &str,
    exists: bool,
) -> Result<()> {
    let deadline = std::time::Instant::now() + t_ready();
    loop {
        eng.check_alive()?;
        if exists_on_pg(pgdb, table).await? == exists {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!(
                "{table} on {} ({}) : expected exists={exists}",
                pgdb.flavor,
                pgdb.schema
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The fan-out pattern of [`targets_fan_out_and_retarget`] with one MySQL target **and** one
/// PostgreSQL target side by side: the same `Prepared`/`Diff` feeds both engines. Includes a
/// retarget (`source,b,pg`) and `verify_targets` after each phase, and D5's zero-date coverage:
/// a view over a `datetime` column holding `'0000-00-00 00:00:00'`, asserted NULL on the target.
#[test]
fn targets_pg_fan_out_and_retarget() {
    let Some(pgs) = pg::servers_or_skip("targets_pg_fan_out_and_retarget") else {
        return;
    };
    scenario("tg_pgfout", move |ctx| {
        let pgs1 = pgs.clone();
        async move {
            let src = ctx.new_db("tg_pgf_src").await?;
            let tb = ctx.new_db("tg_pgf_b").await?;
            let pgdb = pg::new_pg_db(&pgs1[0], "tg_pgf").await?;
            let ids = setup_ecommerce(&src, ctx.sub_seed("seed"), Sizes::scaled()).await?;
            let mut c = src.conn().await?;
            // D5 coverage: a DATETIME NOT NULL column holding '0000-00-00 00:00:00' (the session
            // allows zero dates, like values_zero_dates) plus a nullable twin holding one — that
            // column is skipped by the PostgreSQL comparison and the row count is asserted below.
            exec_all(
            &mut c,
            &[
                "SET SESSION sql_mode = ''",
                "CREATE TABLE pg_a (id INT PRIMARY KEY, dt DATETIME NOT NULL, dt_opt DATETIME NULL)",
                "INSERT INTO pg_a VALUES (1, '0000-00-00 00:00:00', '2024-02-29 23:59:58'), \
                 (2, '2024-02-29 23:59:58', '0000-00-00 00:00:00'), (3, '2024-03-01 07:08:09', NULL)",
            ],
        )
        .await?;
            let mut specs = Vec::new();
            for v in ecommerce_views() {
                specs.push(match v.name.as_str() {
                    // upstream of the chained views must live on the source (and on both targets)
                    "v_customer_stats" => v.with_targets("source,b,pg"),
                    "v_vip_stats" => v.with_targets("pg"),
                    "v_tier_revenue" => v.with_targets("source,b"),
                    _ => v.with_targets("b,pg"),
                });
            }
            specs.push(
                ViewSpec::new("pg_b", Kind::Incremental, "SELECT id, dt, dt_opt FROM pg_a")
                    .with_targets("pg"),
            );
            let tmap: HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
            let pgmap: HashMap<String, pg::PgDb> = [("pg".to_string(), pgdb.clone())].into();
            let env = target_env(&[("b", &tb.dsn), ("pg", &pgdb.dsn)]);
            let (mut eng, idmap) = start_with_views(&ctx, &src, &specs, &ov_refs(&env)).await?;
            verify_targets(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &pgmap,
                &specs,
                "initial build on the MySQL and the PostgreSQL target",
            )
            .await?;
            let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
            for phase in 1..=2 {
                let n = run_batches(&mut c, &mut wl, scaled(40)).await?;
                verify_targets(
                    &ctx,
                    &mut eng,
                    &src,
                    &tmap,
                    &pgmap,
                    &specs,
                    &format!("phase {phase} ({n} txns)"),
                )
                .await?;
            }
            // D5: the zero date reads as NULL on the target; the nullable zero-date column was
            // skipped by the comparison, so the row count is asserted here.
            let client = pgdb.client().await?;
            let t = pg::fetch(&client, &pgdb.schema, "pg_b").await?;
            drop(client);
            ensure!(
                t.rows.len() == 3,
                "pg_b on the target has {} rows, expected 3",
                t.rows.len()
            );
            let zero = t
                .rows
                .iter()
                .find(|r| r.first().is_some_and(|cell| cell.as_deref() == Some(b"1")))
                .context("pg_b row id 1")?;

            ensure!(
                zero[1].is_none(),
                "the zero date is {:?} on the target, expected NULL",
                zero[1]
            );
            ctx.log("D5: zero date is NULL on the PostgreSQL target (3 rows)");
            // Online retargeting across both engines: drop b, then take the source copy back too.
            let find = |specs: &Vec<ViewSpec>, t: &str| {
                specs
                    .iter()
                    .position(|s| {
                        s.targets.as_deref() == Some(t)
                            && s.kind != Kind::Chained
                            && s.name != "pg_b"
                    })
                    .context("pattern")
            };
            let changes = [
                (find(&specs, "b,pg")?, "pg"),
                (find(&specs, "b,pg")?, "source,b,pg"),
            ];
            for (i, new_t) in changes {
                let name = specs[i].name.clone();
                let old = specs[i].target_names();
                exec(
                    &mut c,
                    &format!(
                        "UPDATE _lazyview SET targets = {} WHERE id = {}",
                        q(new_t),
                        idmap[&name]
                    ),
                )
                .await?;
                specs[i].targets = Some(new_t.to_string());
                let new = specs[i].target_names();
                for t in old.iter().chain(new.iter()) {
                    let r = if t == "source" {
                        wait_exists(&mut eng, &src, &name, new.contains(t)).await
                    } else if t == "pg" {
                        wait_exists_pg(&mut eng, &pgdb, &name, new.contains(t)).await
                    } else {
                        wait_exists(&mut eng, &tmap[t], &name, new.contains(t)).await
                    };
                    r.with_context(|| format!("retarget {name} {old:?} → {new:?} (on {t})"))?;
                }
                run_batches(&mut c, &mut wl, scaled(15)).await?;
                wait_ready_tolerant(&mut c, &mut eng, &[name.as_str()], t_ready()).await?;
                ctx.log(format!("retargeted {name}: {old:?} → {new:?}"));
            }
            verify_targets(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &pgmap,
                &specs,
                "after retargeting",
            )
            .await?;
            eng.stop().await?;
            pg::drop_dbs(std::slice::from_ref(&pgdb)).await?;
            Ok(())
        }
    });
}

/// Issue #56's acceptance: after the load, no incremental view was ever rebuilt whole (the
/// engine's `whole_view_refreshes` counter via `Engine::stats_of`), and the row key is
/// exercised — upserts, not delete-by-unit — by asserting the unique row-key index exists
/// (`has_row_index` through the PG client).
#[test]
fn targets_pg_incremental_never_rebuilds() {
    let Some(pgs) = pg::servers_or_skip("targets_pg_incremental_never_rebuilds") else {
        return;
    };
    scenario("tg_pgnorebuild", move |ctx| {
        let pgs1 = pgs.clone();
        async move {
            let src = ctx.new_db("tg_pgn_src").await?;
            let pgdb = pg::new_pg_db(&pgs1[0], "tg_pgn").await?;
            let mut c = src.conn().await?;
            exec_all(
            &mut c,
            &[
                "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
                "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL)",
                "INSERT INTO entities VALUES (1,'alpha'),(2,'beta'),(3,'gamma')",
                "INSERT INTO items VALUES (1,1,10.50),(2,2,5.00),(3,3,7.75)",
            ],
        )
        .await?;
            ensure_lazyview_table(&mut c).await?;
            // items drives and entities is joined on ITS PK (the u_items shape, tests/upsert.rs):
            // that is what gives the layout a row key (`_lv_row`) — joined on the FK instead
            // (the u_cat_lines shape) there is no lineage key and even MySQL keeps no unique index.
            let spec = ViewSpec::new(
            "links",
            Kind::Incremental,
            "SELECT i.id AS id, e.name AS name, i.amount AS amount FROM items i JOIN entities e ON e.id = i.entity_id",
        )
        .with_targets("pg");
            add_views(&mut c, std::slice::from_ref(&spec)).await?;
            let env = target_env(&[("pg", &pgdb.dsn)]);
            let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
            wait_ready(&mut c, &mut eng, &["links"], t_ready()).await?;
            for k in 1..=3 {
                exec(
                    &mut c,
                    &format!(
                        "INSERT INTO items VALUES ({}, {}, {}.25)",
                        10 + k,
                        (k % 3) + 1,
                        k
                    ),
                )
                .await?;
                exec(
                    &mut c,
                    &format!("UPDATE items SET amount = amount + 1 WHERE id = {k}"),
                )
                .await?;
                exec(&mut c, &format!("DELETE FROM items WHERE id = {}", 10 + k)).await?;
                eng.wait_caught_up(&mut c, t_catchup()).await?;
            }
            let vs = eng.stats_of(&src, "links")?;
            ensure!(
                vs.whole_view_refreshes == 0,
                "links took {} whole-view refreshes, expected none (incremental)",
                vs.whole_view_refreshes
            );
            let client = pgdb.client().await?;
            ensure!(
                pg::has_row_index(&client, &pgdb.schema, "links").await?,
                "links has no unique row-key index on the PostgreSQL target"
            );
            drop(client);
            ctx.log("incremental: 0 whole-view refreshes, row-key index present");
            let tmap: HashMap<String, Db> = HashMap::new();
            let pgmap: HashMap<String, pg::PgDb> = [("pg".to_string(), pgdb.clone())].into();
            verify_targets(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &pgmap,
                std::slice::from_ref(&spec),
                "incremental rows",
            )
            .await?;
            eng.stop().await?;
            pg::drop_dbs(std::slice::from_ref(&pgdb)).await?;
            Ok(())
        }
    });
}

/// The transactional swap under a reader: while a reader holds the MV table open, a definition
/// edit forces a whole-view rebuild and its swap — which must either complete or fail with a
/// lock_timeout and retry, never leaving `links__lv_new` behind ([`wait_exists`]'s rule).
#[test]
fn targets_pg_swap_keeps_readers() {
    let Some(pgs) = pg::servers_or_skip("targets_pg_swap_keeps_readers") else {
        return;
    };
    scenario("tg_pgsubswap", move |ctx| {
        let pgs1 = pgs.clone();
        async move {
            let src = ctx.new_db("tg_pgs_src").await?;
            let pgdb = pg::new_pg_db(&pgs1[0], "tg_pgs").await?;
            let mut c = src.conn().await?;
            exec_all(
            &mut c,
            &[
                "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
                "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL)",
                "INSERT INTO entities VALUES (1,'alpha'),(2,'beta')",
                "INSERT INTO items VALUES (1,1,10.50),(2,2,5.00)",
            ],
        )
        .await?;
            ensure_lazyview_table(&mut c).await?;
            let spec = ViewSpec::new(
            "links",
            Kind::Incremental,
            "SELECT i.id AS id, e.name AS name, i.amount AS amount FROM entities e JOIN items i ON i.entity_id = e.id",
        )
        .with_targets("pg");
            let ids = add_views(&mut c, std::slice::from_ref(&spec)).await?;
            let env = target_env(&[("pg", &pgdb.dsn)]);
            let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
            wait_ready(&mut c, &mut eng, &["links"], t_ready()).await?;
            // The reader: an open transaction on the MV table (ACCESS SHARE) the swap must wait for.
            let reader = pgdb.client().await?;
            reader.batch_execute("BEGIN").await?;
            let rows = reader.query("SELECT count(*) FROM \"links\"", &[]).await?;
            let n: i64 = rows.first().context("count row")?.get(0);
            exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {}",
                q("CREATE VIEW links AS SELECT i.id AS id, e.name AS name, i.amount AS amount FROM entities e JOIN items i ON i.entity_id = e.id WHERE i.id >= 1"),
                ids["links"]
            ),
        )
        .await?;
            // The swap meets the reader's lock (T5's lock_timeout makes it fail and retry); the
            // reader then rolls back and the retry completes.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            drop(reader);
            wait_ready(&mut c, &mut eng, &["links"], t_ready()).await?;
            wait_exists_pg(&mut eng, &pgdb, "links__lv_new", false)
                .await
                .context("the swap left a shadow table behind")?;
            ensure!(
                exists_on_pg(&pgdb, "links").await?,
                "links is gone after the swap"
            );
            ctx.log(format!(
                "swap completed around a reader holding {n} rows, no shadow left"
            ));
            let tmap: HashMap<String, Db> = HashMap::new();
            let pgmap: HashMap<String, pg::PgDb> = [("pg".to_string(), pgdb.clone())].into();
            verify_targets(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &pgmap,
                std::slice::from_ref(&spec),
                "after the swap",
            )
            .await?;
            eng.stop().await?;
            pg::drop_dbs(std::slice::from_ref(&pgdb)).await?;
            Ok(())
        }
    });
}

/// The mirror of [`targets_one_target_down`] with the PostgreSQL target unreachable (port 1):
/// the MySQL target keeps up and `GET /stats` keeps advancing.
#[test]
fn targets_pg_target_down_does_not_hold_the_others() {
    let Some(pgs) = pg::servers_or_skip("targets_pg_target_down_does_not_hold_the_others") else {
        return;
    };
    scenario("tg_pgdown", move |ctx| {
        let pgs1 = pgs.clone();
        async move {
            let src = ctx.new_db("tg_pgd_src").await?;
            let tb = ctx.new_db("tg_pgd_b").await?;
            let ids = setup_ecommerce(&src, ctx.sub_seed("seed"), Sizes::scaled()).await?;
            let all = ecommerce_views();
            let get = |n: &str| all.iter().find(|v| v.name == n).unwrap().clone();
            let healthy = vec![
                get("v_order_lines").with_targets("b"),
                get("v_status_counts").with_targets("source,b"),
            ];
            let with_down = vec![
                get("v_paid_orders").with_targets("b,down"),
                get("v_category_sales").with_targets("b,down"),
            ];
            let mut c = src.conn().await?;
            ensure_lazyview_table(&mut c).await?;
            add_views(&mut c, &healthy).await?;
            add_views(&mut c, &with_down).await?;
            let tmap: HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
            // Port 1 on localhost: connection refused (the dead target is the PostgreSQL one).
            ctx.log(format!(
                "unreachable PostgreSQL target ({} pinned to port 1)",
                pgs1[0].flavor
            ));
            let dead = "postgres://lv:lv@127.0.0.1:1/nowhere";
            let env = target_env(&[("b", &tb.dsn), ("down", dead)]);
            let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
            let converge = Duration::from_secs_f64(300.0 * scale().max(1.0));
            let checked: Vec<ViewSpec> = healthy.iter().chain(with_down.iter()).cloned().collect();
            eventually_equal(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &checked,
                converge,
                "MySQL targets keep up with the PostgreSQL target down",
            )
            .await?;
            let before = format!("{:?}", eng.snapshot());
            let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
            run_batches(&mut c, &mut wl, scaled(50)).await?;
            eventually_equal(
                &ctx,
                &mut eng,
                &src,
                &tmap,
                &checked,
                converge,
                "after writes, the PostgreSQL target still down",
            )
            .await?;
            let after = format!("{:?}", eng.snapshot());
            ensure!(
                before != after,
                "GET /stats did not advance with the PostgreSQL target down"
            );
            eng.check_alive()?;
            drop(eng); // kill: a graceful stop may legitimately wait for the dead target
            Ok(())
        }
    });
}
