//! The soak harness's workload tests (PR S1 of `tmp/out/lvs-soak/breakdown.md`): the neutral
//! schema, the neutral view set and the seeded mix of `tests/common/soakwork.rs`. The checks, the
//! samplers and the one-page report are the callers in `tests/common/soakcheck.rs` (PR S2); the
//! seeded chaos schedule and the two modes come with PR S3. What lives here proves the generator
//! and the view set before anything runs for hours.

mod common;

use anyhow::ensure;
use common::oracle::fingerprint;
use common::soakwork::{run_soak_batches, setup_soak, soak_views, SoakWorkload, SOAK_TABLES};
use common::workload::Sizes;
use common::*;

/// The neutral set plans, builds and equals its oracle, and the generator is deterministic: the
/// same seed in two independent schemas writes the same rows into every table. Guards F8–F10
/// (the soak has its own neutral schema and mix, a seeded generator) without a long run.
#[test]
fn soak_workload_is_deterministic() {
    scenario("soak_det", |ctx| async move {
        let seed = ctx.sub_seed("seed");
        let wseed = ctx.sub_seed("wl");
        let specs = soak_views();
        ensure!(
            specs.len() >= 20,
            "the neutral set must cover every access path, got {}",
            specs.len()
        );

        // Two independent schemas, the same seed: setup twice, 200 batches each.
        let a = ctx.new_db("soak_det_a").await?;
        let b = ctx.new_db("soak_det_b").await?;
        let ids_a = setup_soak(&a, seed, Sizes::tiny()).await?;
        let ids_b = setup_soak(&b, seed, Sizes::tiny()).await?;
        let mut ca = a.conn().await?;
        let mut wa = SoakWorkload::new(wseed, ids_a);
        let n_a = run_soak_batches(&mut ca, &mut wa, 200).await?;
        let mut cb = b.conn().await?;
        let mut wb = SoakWorkload::new(wseed, ids_b);
        let n_b = run_soak_batches(&mut cb, &mut wb, 200).await?;
        ctx.log(format!(
            "{n_a} / {n_b} txns (same seed); ops: {}",
            wa.stats_line()
        ));
        ensure!(
            n_a == n_b,
            "the two runs wrote {n_a} and {n_b} transactions"
        );

        // The fingerprint of every table must agree between the two runs (and be non-trivial).
        let mut rows = 0;
        for t in SOAK_TABLES {
            let (na, ha) = fingerprint(&mut ca, t).await?;
            let (nb, hb) = fingerprint(&mut cb, t).await?;
            ctx.log(format!("fingerprint {t}: {na}/{ha} vs {nb}/{hb}"));
            ensure!(
                (na, ha) == (nb, hb),
                "table {t} differs between the two runs with the same seed: {na}/{ha} vs {nb}/{hb}"
            );
            rows += na;
        }
        ensure!(rows > 0, "the seed data is empty: {rows} rows in total");

        // The whole set (every access path, the two noisy definitions, the chained pair) builds
        // and equals its oracle in one check.
        let (mut eng, _) = start_with_views(&ctx, &a, &specs, &[]).await?;
        verify(&ctx, &mut eng, &a, &specs, "after 200 batches").await?;
        eng.stop().await?;
        ca.disconnect().await.ok();
        cb.disconnect().await.ok();
        Ok(())
    });
}
