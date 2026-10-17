//! Brutal randomized correctness: ~24 heavily overlapping views over 8 source tables, seeded
//! random transactions, several batch windows × query chunk sizes, concurrent writers, oracle
//! checks after the initial build, mid-run and at the end.
//!
//! Reproduce a failure with `LAZYVIEW_TEST_SEED=<printed seed>`; scale with `LAZYVIEW_TEST_SCALE`.

mod common;

use std::time::Duration;

use anyhow::{Context, Result};
use common::workload::{
    ecommerce_views, names, run_batches, run_concurrent, setup_ecommerce, Sizes, Workload,
};
use common::*;

/// Seeds per (window, chunk) combination: 1 at the default scale, more when scaled up.
fn seeds_per_combo() -> usize {
    scale().floor().max(1.0) as usize
}

async fn brutal_combo(
    ctx: &Ctx,
    label: &str,
    seed: u64,
    window_ms: u64,
    chunk: Option<usize>,
) -> Result<()> {
    let db = ctx.new_db(&format!("brutal_{label}")).await?;
    let ids = setup_ecommerce(&db, seed, Sizes::scaled()).await?;
    let specs = ecommerce_views();
    let window = window_ms.to_string();
    let chunk_s = chunk.map(|c| c.to_string());
    let mut ov: Vec<(&str, &str)> = vec![("LAZYVIEW_BATCH_WINDOW_MS", window.as_str())];
    if let Some(ch) = chunk_s.as_deref() {
        // Tiny query chunks force multi-chunk refreshes and lookups; small write batches and
        // max-keys force multi-statement writes and early flushes.
        ov.push(("LAZYVIEW_QUERY_CHUNK", ch));
        ov.push(("LAZYVIEW_WRITE_BATCH_ROWS", "7"));
        ov.push(("LAZYVIEW_BATCH_MAX_KEYS", "40"));
    }
    let (mut eng, _) = start_with_views(ctx, &db, &specs, &ov).await?;
    let mut c = db.conn().await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;

    let mut wl = Workload::new(seed, ids.clone());
    let per_phase = scaled(50);
    for phase in 1..=3 {
        let n = run_batches(&mut c, &mut wl, per_phase)
            .await
            .with_context(|| format!("{label} phase {phase} workload"))?;
        verify(
            ctx,
            &mut eng,
            &db,
            &specs,
            &format!("{label}: phase {phase} ({n} txns)"),
        )
        .await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    ctx.log(format!(
        "{label}: ops {}; metrics {}",
        wl.stats_line(),
        fmt_snapshot(&eng.snapshot())
    ));
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

fn brutal_window(window_ms: u64) {
    scenario(&format!("brutal_w{window_ms}"), move |ctx| async move {
        for chunk in [None, Some(3usize)] {
            for k in 0..seeds_per_combo() {
                let label = format!(
                    "w{window_ms}_c{}_{k}",
                    chunk.map(|c| c.to_string()).unwrap_or_else(|| "def".into())
                );
                let seed = ctx.sub_seed(&label);
                brutal_combo(&ctx, &label, seed, window_ms, chunk)
                    .await
                    .with_context(|| format!("combination {label} (sub-seed {seed})"))?;
            }
        }
        Ok(())
    });
}

#[test]
fn brutal_window_0ms() {
    brutal_window(0);
}

#[test]
fn brutal_window_50ms() {
    brutal_window(50);
}

#[test]
fn brutal_window_500ms() {
    brutal_window(500);
}

/// Several writers at once; the first burst overlaps the initial build (snapshot/stream overlap),
/// the second runs against a streaming engine.
#[test]
fn brutal_concurrent_writers() {
    scenario("brutal_conc", |ctx| async move {
        for (k, (window, chunk)) in [("50", "1000"), ("0", "4")].into_iter().enumerate() {
            let label = format!("conc{k}_w{window}_c{chunk}");
            let seed = ctx.sub_seed(&label);
            let db = ctx.new_db(&label).await?;
            let ids = setup_ecommerce(&db, seed, Sizes::scaled()).await?;
            let specs = ecommerce_views();
            let mut c = db.conn().await?;

            // Burst 1 starts before the engine: initial builds race with writes.
            let (dbc, idc) = (db.clone(), ids.clone());
            let burst1 =
                tokio::spawn(async move { run_concurrent(&dbc, idc, seed, 4, scaled(50)).await });
            tokio::time::sleep(Duration::from_millis(200)).await;
            let (mut eng, _) = start_with_views(
                &ctx,
                &db,
                &specs,
                &[
                    ("LAZYVIEW_BATCH_WINDOW_MS", window),
                    ("LAZYVIEW_QUERY_CHUNK", chunk),
                ],
            )
            .await?;
            let n1 = burst1.await.context("burst 1 panicked")??;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("{label}: after burst 1 ({n1} txns, overlapping build)"),
            )
            .await?;

            // Burst 2: more writers against the streaming engine, plus a single-writer tail.
            let n2 =
                run_concurrent(&db, ids.clone(), derive_seed(seed, "b2"), 6, scaled(40)).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("{label}: after burst 2 ({n2} txns)"),
            )
            .await?;
            let mut wl = Workload::new(derive_seed(seed, "tail"), ids.clone());
            let n3 = run_batches(&mut c, &mut wl, scaled(40)).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("{label}: after tail ({n3} txns)"),
            )
            .await?;
            assert_ready(&mut c, &names(&specs)).await?;
            eng.stop().await?;
            c.disconnect().await.ok();
        }
        Ok(())
    });
}
