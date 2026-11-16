//! Debug tool: prints the statements of a workload seed (as used by brutal/crash runs) to
//! `tmp/out/workload-<seed>.sql`. This is the moved `selftest_dump_workload` from
//! `tests/tools.rs`: an ignored test is a skip no CI guard can see, a debug dumper is an
//! example, not a test (the default `cargo test` never builds or runs it). Run by hand:
//!
//! ```text
//! LAZYVIEW_DUMP_SEED=<sub-seed> LAZYVIEW_DUMP_BATCHES=150 \
//!     cargo test --example dump_workload -- --exact tool --nocapture
//! ```

// The harness compiles only in the test build (`cargo test --example ...` sets `cfg(test)`,
// the plain example build does not need it and would not compile it).
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[cfg(test)]
#[test]
fn tool() {
    use std::sync::Arc;

    use common::workload::{seed_statements, Sizes, Workload};
    use common::*;
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

fn main() {
    eprintln!(
        "dump_workload is a debug tool, not a program: run it with\n  \
         LAZYVIEW_DUMP_SEED=<sub-seed> cargo test --example dump_workload -- --exact tool --nocapture"
    );
}
