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
                let ids = rows(
                    &mut admin,
                    &format!("SELECT ID FROM information_schema.PROCESSLIST WHERE DB = {} AND ID <> CONNECTION_ID()", q(&killer_db.name)),
                )
                .await?;
                for id in ids.into_iter().filter_map(|r| r[0].clone()) {
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
