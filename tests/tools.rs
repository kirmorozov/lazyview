//! `--ignored` maintenance tools for the integration harness. These are not self-tests:
//! they run by hand on demand (`cargo test --test tools -- --ignored <name>`) and CI never
//! executes them.

mod common;

use std::sync::Arc;

use common::workload::{seed_statements, Sizes, Workload};
use common::*;

/// Drops leftover `lvt_*` schemas on every server (run explicitly with `--ignored`).
#[test]
#[ignore]
fn selftest_cleanup_test_schemas() {
    let Some(servers) = servers() else { return };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for s in servers {
            let mut c = connect(&s.base_dsn).await.unwrap();
            let names = rows(&mut c, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'lvt\\_%'")
                .await
                .unwrap();
            for n in names.into_iter().filter_map(|r| r[0].clone()) {
                println!("[{}] drop {n}", s.flavor);
                exec(&mut c, &format!("DROP DATABASE {}", ident(&n))).await.unwrap();
            }
        }
    });
    std::fs::remove_dir_all(logs_root()).ok();
}

/// Debug tool: prints the statements of a workload seed (as used by brutal/crash runs) to
/// `tmp/out/workload-<seed>.sql`. `LAZYVIEW_DUMP_SEED=<sub-seed> LAZYVIEW_DUMP_BATCHES=150 cargo
/// test --test tools -- --ignored selftest_dump_workload`.
#[test]
#[ignore]
fn selftest_dump_workload() {
    let seed: u64 = std::env::var("LAZYVIEW_DUMP_SEED")
        .expect("LAZYVIEW_DUMP_SEED")
        .parse()
        .unwrap();
    let batches: usize = std::env::var("LAZYVIEW_DUMP_BATCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    let (seed_sql, ids) = seed_statements(seed, Sizes::scaled());
    let mut w = Workload::new(seed, Arc::new(ids));
    let mut out = String::new();
    for s in seed_sql {
        out.push_str(&s);
        out.push_str(";\n");
    }
    for b in 0..batches {
        for t in w.next_batch() {
            out.push_str(&format!(
                "-- batch {b}: {}{}\n",
                t.label,
                if t.rollback { " (ROLLBACK)" } else { "" }
            ));
            for s in &t.stmts {
                out.push_str(&common::oracle::normalize_ws(s));
                out.push_str(";\n");
            }
        }
    }
    let path = repo_dir().join(format!("tmp/out/workload-{seed}.sql"));
    std::fs::write(&path, out).unwrap();
    println!("wrote {}", path.display());
}
