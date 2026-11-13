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
        verify_targets(&ctx, &mut eng, &src, &tmap, &specs, "after retargeting").await?;
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
