//! Maintenance tool: drops leftover `lvt_*` schemas on every server in `LAZYVIEW_TEST_DSNS`
//! and clears the engine log dir. This is the moved `selftest_cleanup_test_schemas` from
//! `tests/tools.rs`: an ignored test is a skip no CI guard can see, a maintenance tool is an
//! example, not a test (the default `cargo test` never builds or runs it). Run by hand:
//!
//! ```text
//! LAZYVIEW_TEST_DSNS="mysql84=mysql://lv:lv@127.0.0.1:33084/lvtest" \
//!     cargo test --example cleanup_test_schemas -- --exact tool --nocapture
//! ```

// The harness compiles only in the test build (`cargo test --example ...` sets `cfg(test)`,
// the plain example build does not need it and would not compile it).
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[cfg(test)]
#[test]
fn tool() {
    use common::*;
    let Some(servers) = servers() else {
        eprintln!("LAZYVIEW_TEST_DSNS not set: nothing to clean");
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for s in servers {
            let mut c = connect(&s.base_dsn).await.unwrap();
            let names = rows(&mut c, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'lvt\\_%'")
                .await
                .unwrap();
            for n in names.into_iter().filter_map(|r| r[0].clone()) {
                println!("[{}] drop {n}", s.flavor);
                exec(&mut c, &format!("DROP DATABASE {}", ident(&n)))
                    .await
                    .unwrap();
            }
        }
    });
    std::fs::remove_dir_all(common::logs_root()).ok();
}

fn main() {
    eprintln!(
        "cleanup_test_schemas is a maintenance tool, not a program: run it with\n  \
         cargo test --example cleanup_test_schemas -- --exact tool --nocapture"
    );
}
