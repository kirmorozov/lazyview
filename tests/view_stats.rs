//! Per-view update statistics (`GET /stats`, src/stats.rs) used as assertions:
//! * a burst of row changes, most of them irrelevant to a grouped view pinned by a constant key,
//!   refreshes exactly the items that changed, never the whole view, and leaves a second view
//!   untouched;
//! * the same ids marked many times before one flush are refreshed once: deduplication absorbs
//!   the rest, and `ids_marked = ids_absorbed + entities_updated + ids_pending` holds.
//!
//! Sizes scale with `LAZYVIEW_TEST_SCALE` (default 2 000 items).

mod common;

use anyhow::{ensure, Context, Result};
use common::*;

/// The key value the grouped view is pinned to.
const PINNED: i64 = 7;
/// Other key values written per item by the burst (irrelevant to the view).
const OTHER_KEYS: i64 = 10;
/// Rows per INSERT statement.
const BATCH: usize = 500;

fn pinned_view() -> ViewSpec {
    ViewSpec::new(
        "vs_pinned",
        Kind::Incremental,
        &format!(
            "SELECT item_id, SUM(val) AS total, COUNT(*) AS n FROM item_value \
             WHERE key_id = {PINNED} GROUP BY item_id"
        ),
    )
}

fn untouched_view() -> ViewSpec {
    ViewSpec::new(
        "vs_entity",
        Kind::Incremental,
        "SELECT grp, SUM(amount) AS total, COUNT(*) AS n FROM entity GROUP BY grp",
    )
}

/// `INSERT` statements for `rows` (value tuples) into `table`, `BATCH` rows each.
fn inserts(table: &str, cols: &str, rows: &[String]) -> Vec<String> {
    rows.chunks(BATCH)
        .map(|c| format!("INSERT INTO {table} ({cols}) VALUES {}", c.join(",")))
        .collect()
}

/// Item-value and entity tables: `items` items with a pinned-key row each, 50 entities.
async fn setup(db: &Db, items: usize) -> Result<()> {
    let mut c = db.conn().await?;
    exec_all(
        &mut c,
        &[
            "CREATE TABLE item_value (item_id BIGINT NOT NULL, key_id INT NOT NULL, \
             val BIGINT NOT NULL, PRIMARY KEY (item_id, key_id))"
                .to_string(),
            "CREATE TABLE entity (id BIGINT PRIMARY KEY, grp INT NOT NULL, amount BIGINT NOT NULL)"
                .to_string(),
        ],
    )
    .await?;
    let rows: Vec<String> = (1..=items as i64)
        .map(|i| format!("({i}, {PINNED}, 1)"))
        .collect();
    exec_all(
        &mut c,
        &inserts("item_value", "item_id, key_id, val", &rows),
    )
    .await?;
    let rows: Vec<String> = (1..=50i64)
        .map(|i| format!("({i}, {}, {i})", i % 5))
        .collect();
    exec_all(&mut c, &inserts("entity", "id, grp, amount", &rows)).await?;
    c.disconnect().await.ok();
    Ok(())
}

/// Counter differences `now - before` of the cumulative counters (the rest as in `now`).
fn delta(now: &ViewStats, before: &ViewStats) -> ViewStats {
    ViewStats {
        source_rows: now.source_rows - before.source_rows,
        source_rows_skipped: now.source_rows_skipped - before.source_rows_skipped,
        ids_marked: now.ids_marked - before.ids_marked,
        ids_absorbed: now.ids_absorbed - before.ids_absorbed,
        entities_updated: now.entities_updated - before.entities_updated,
        flushes: now.flushes - before.flushes,
        whole_view_refreshes: now.whole_view_refreshes - before.whole_view_refreshes,
        ..now.clone()
    }
}

#[test]
fn view_stats_burst_follows_the_change() {
    scenario("vstats_burst", |ctx| async move {
        let db = ctx.new_db("vstats_burst").await?;
        let items = scaled(2000);
        setup(&db, items).await?;
        let specs = vec![pinned_view(), untouched_view()];
        let addr = free_local_addr()?;
        let (mut eng, _) = start_with_views(
            &ctx,
            &db,
            &specs,
            &[("LAZYVIEW_HEALTH_ADDR", addr.as_str())],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let s0 = eng.stats_of(&db, "vs_pinned")?;
        ensure!(
            s0.entities_distinct == 0 && s0.whole_view_refreshes == 0,
            "before the burst: {s0:?}"
        );

        let mut c = db.conn().await?;
        // Burst: every item gets OTHER_KEYS key values (rows the view does not read), then they
        // are all updated, and the pinned value of every fourth item changes.
        let rows: Vec<String> = (1..=items as i64)
            .flat_map(|i| (0..OTHER_KEYS).map(move |k| format!("({i}, {}, {k})", 100 + k)))
            .collect();
        for stmts in inserts("item_value", "item_id, key_id, val", &rows).chunks(4) {
            exec(&mut c, "START TRANSACTION").await?;
            exec_all(&mut c, stmts).await?;
            exec(&mut c, "COMMIT").await?;
        }
        exec(
            &mut c,
            "UPDATE item_value SET val = val + 1 WHERE key_id >= 100",
        )
        .await?;
        let rounds = 5u64;
        for _ in 0..=rounds {
            exec(
                &mut c,
                &format!(
                    "UPDATE item_value SET val = val + 1 WHERE key_id = {PINNED} AND item_id % 4 = 0"
                ),
            )
            .await?;
        }
        verify(&ctx, &mut eng, &db, &specs, "after the burst").await?;

        let changed = (items / 4) as u64;
        let irrelevant = 2 * items as u64 * OTHER_KEYS as u64;
        let s = eng.stats_of(&db, "vs_pinned")?;
        let d = delta(&s, &s0);
        ctx.log(format!("vs_pinned after the burst: {s:?}"));
        ensure!(
            d.whole_view_refreshes == 0,
            "the burst rebuilt the whole view: {s:?}"
        );
        ensure!(
            s.entities_distinct == changed,
            "distinct ids refreshed {} != items changed {changed}: {s:?}",
            s.entities_distinct
        );
        ensure!(
            d.entities_updated >= changed
                && d.entities_updated <= d.flushes * s.entities_distinct
                && d.entities_updated <= (1 + rounds) * changed,
            "entities_updated {} outside [{changed}, min(flushes {} x distinct, {} x changed)]: {s:?}",
            d.entities_updated,
            d.flushes,
            1 + rounds
        );
        ensure!(
            d.source_rows == irrelevant + (1 + rounds) * changed
                && d.source_rows_skipped == irrelevant,
            "rows fed {} / skipped {} (expected {} / {irrelevant}): {s:?}",
            d.source_rows,
            d.source_rows_skipped,
            irrelevant + (1 + rounds) * changed
        );
        ensure!(
            d.ids_marked == (1 + rounds) * changed,
            "ids marked {} (expected one per relevant row change, {}): {s:?}",
            d.ids_marked,
            (1 + rounds) * changed
        );
        ensure!(
            s.balanced() && s.ids_pending == 0,
            "marked != absorbed + updated + pending: {s:?}"
        );
        ensure!(s.lookup_queries == 0, "lookups for a direct key: {s:?}");
        ensure!(
            s.rows_inserted + s.rows_updated >= changed,
            "view rows written: {s:?}"
        );
        let other = eng.stats_of(&db, "vs_entity")?;
        ensure!(
            other.source_rows == 0
                && other.ids_marked == 0
                && other.entities_updated == 0
                && other.flushes == 0
                && other.whole_view_refreshes == 0,
            "a view the burst did not touch shows work: {other:?}"
        );

        // The HTTP endpoint serves the same numbers.
        let j = fetch_stats(&addr).await?;
        let key = format!("{}.vs_pinned", db.name);
        let v = j["views"]
            .get(&key)
            .with_context(|| format!("{key} missing in /stats: {j}"))?;
        ctx.log(format!("GET /stats → views[{key}] = {v}"));
        ensure!(
            v["entities_distinct"] == changed
                && v["whole_view_refreshes"] == 0
                && v["balanced"] == true
                && v["mode"] == "incremental",
            "/stats differs: {v}"
        );
        ensure!(
            j["views"][format!("{}.vs_entity", db.name)]["entities_updated"] == 0,
            "/stats vs_entity: {j}"
        );
        let (status, metrics) = http_get(&addr, "/metrics").await?;
        let series = format!("lazyview_view_entities_distinct{{view=\"{key}\"}} {changed}\n");
        ensure!(
            status == 200 && metrics.contains(&series),
            "/metrics lacks {series:?}"
        );
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}

#[test]
fn view_stats_dedup_absorbs_repeated_marks() {
    scenario("vstats_dedup", |ctx| async move {
        let db = ctx.new_db("vstats_dedup").await?;
        let items = scaled(2000);
        setup(&db, items).await?;
        let specs = vec![pinned_view()];
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &[]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let s0 = eng.stats_of(&db, "vs_pinned")?;

        // Every item hit `hits` times in one transaction: all marks land before one flush.
        let hits = 5u64;
        let mut c = db.conn().await?;
        exec(&mut c, "START TRANSACTION").await?;
        for _ in 0..hits {
            exec(
                &mut c,
                &format!("UPDATE item_value SET val = val + 1 WHERE key_id = {PINNED}"),
            )
            .await?;
        }
        exec(&mut c, "COMMIT").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the repeated updates").await?;

        let n = items as u64;
        let s = eng.stats_of(&db, "vs_pinned")?;
        let d = delta(&s, &s0);
        ctx.log(format!("vs_pinned after {hits} hits per item: {s:?}"));
        ensure!(
            d.ids_marked == n * hits,
            "ids marked {} (expected {}): {s:?}",
            d.ids_marked,
            n * hits
        );
        ensure!(
            d.entities_updated == n,
            "entities_updated {} (expected each item once, {n}): {s:?}",
            d.entities_updated
        );
        ensure!(
            d.ids_absorbed == n * (hits - 1),
            "ids absorbed {} (expected {}): {s:?}",
            d.ids_absorbed,
            n * (hits - 1)
        );
        ensure!(d.flushes == 1, "refreshes {}: {s:?}", d.flushes);
        ensure!(
            s.balanced() && s.ids_pending == 0 && d.whole_view_refreshes == 0,
            "invariant: {s:?}"
        );
        ensure!(s.entities_distinct == n, "distinct: {s:?}");
        c.disconnect().await.ok();
        eng.stop().await?;
        Ok(())
    });
}
