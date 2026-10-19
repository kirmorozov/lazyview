//! Self-tests of the integration harness that do not need the engine: fresh schemas, binlog
//! positions, workload generator + all view SQL on every flavor, the oracle (hand-filled table vs
//! view, mismatch detection and diff formatting).
//!
//! `cargo test --test harness_selftest -- --ignored cleanup_test_schemas` drops leftover
//! `lvt_*` schemas (kept by failed runs) on every server.

mod common;

use std::sync::Arc;

use anyhow::{bail, ensure};
use common::oracle::{diff_tables, hand_materialize, Table};
use common::workload::{
    ecommerce_views, run_batches, run_concurrent, seed_statements, setup_ecommerce, Sizes, Workload,
};
use common::*;

#[test]
fn selftest_schema_and_position() {
    scenario("selftest_pos", |ctx| async move {
        let db = ctx.new_db("selftest_pos").await?;
        let mut c = db.conn().await?;
        ensure!(
            scalar(&mut c, "SELECT DATABASE()").await?.as_deref() == Some(db.name.as_str()),
            "DSN schema not selected"
        );
        let version = scalar(&mut c, "SELECT VERSION()")
            .await?
            .unwrap_or_default();
        let fmt = rows(&mut c, "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@time_zone, @@system_time_zone").await?;
        ctx.log(format!(
            "version {version}, log_bin/format/image/tz = {:?}",
            fmt[0]
        ));
        let p1 = server_position(&mut c).await?;
        exec(&mut c, "CREATE TABLE t (id INT PRIMARY KEY)").await?;
        exec(&mut c, "INSERT INTO t VALUES (1), (2)").await?;
        let p2 = server_position(&mut c).await?;
        ensure!(p2 > p1, "binlog position did not advance: {p1} → {p2}");
        ensure!(table_exists(&mut c, "t").await?, "table_exists");
        ensure!(!table_exists(&mut c, "nope").await?, "table_exists(nope)");
        ensure_lazyview_table(&mut c).await?;
        let id =
            add_definition(&mut c, "CREATE VIEW v AS SELECT id FROM t", Some("source")).await?;
        ensure!(id >= 1, "definition id");
        ensure!(view_states(&mut c).await?.is_empty(), "no state table yet");
        c.disconnect().await.ok();
        Ok(())
    });
}

#[test]
fn selftest_workload_views_and_oracle() {
    scenario("selftest_wl", |ctx| async move {
        let db = ctx.new_db("selftest_wl").await?;
        let ids = setup_ecommerce(&db, ctx.sub_seed("seed"), Sizes::scaled()).await?;
        let specs = ecommerce_views();
        ensure!(specs.len() >= 12, "view count");
        hand_materialize(&db, &specs, true).await?;
        check_views(&db, &specs).await?;

        // Sanity: every view is non-trivial on the seed data (except possibly a few).
        let mut c = db.conn().await?;
        let mut empty = Vec::new();
        for s in &specs {
            if count(&mut c, &s.oracle_name()).await? == 0 {
                empty.push(s.name.clone());
            }
        }
        ctx.log(format!("views empty on seed data: {empty:?}"));
        ensure!(
            empty.len() <= 2,
            "too many empty views on seed data: {empty:?}"
        );

        // Single writer, then concurrent writers.
        let mut wl = Workload::new(ctx.sub_seed("wl"), ids.clone());
        let n = run_batches(&mut c, &mut wl, scaled(300)).await?;
        ctx.log(format!("single writer: {n} txns; ops: {}", wl.stats_line()));
        let n = run_concurrent(&db, ids.clone(), ctx.sub_seed("conc"), 4, scaled(80)).await?;
        ctx.log(format!("4 concurrent writers: {n} txns"));

        // The hand-filled MVs are stale now: must be detected.
        match check_views(&db, &specs).await {
            Ok(()) => bail!("oracle did not notice stale hand-filled MVs"),
            Err(e) => {
                let msg = format!("{e:#}");
                ensure!(
                    msg.contains("differs from oracle"),
                    "unexpected error: {msg}"
                );
                ctx.log(format!(
                    "stale MVs detected as expected ({} chars of diff)",
                    msg.len()
                ));
            }
        }
        hand_materialize(&db, &specs, false).await?;
        check_views(&db, &specs).await?;

        // Tamper: change one value, delete one row, duplicate one row.
        exec(
            &mut c,
            "UPDATE v_order_lines SET qty = qty + 1000 ORDER BY order_id, line_no LIMIT 1",
        )
        .await?;
        exec(&mut c, "DELETE FROM v_customer_cards ORDER BY id LIMIT 1").await?;
        exec(
            &mut c,
            "INSERT INTO v_status_counts SELECT * FROM v_status_counts LIMIT 1",
        )
        .await?;
        let err = match check_views(&db, &specs).await {
            Ok(()) => bail!("tampering not detected"),
            Err(e) => format!("{e:#}"),
        };
        for needle in [
            "v_order_lines",
            "v_customer_cards",
            "v_status_counts",
            "3 view(s) differ",
        ] {
            ensure!(
                err.contains(needle),
                "diff does not mention {needle}:\n{err}"
            );
        }
        ctx.log(format!("tamper diff:\n{err}"));
        c.disconnect().await.ok();
        Ok(())
    });
}

#[test]
fn selftest_typed_schema() {
    scenario("selftest_typed", |ctx| async move {
        let db = ctx.new_db("selftest_typed").await?;
        let mut c = db.conn().await?;
        for s in common::typed::ddl() {
            exec(&mut c, &s).await?;
        }
        let mut g = common::workload::Gen::new(ctx.sub_seed("typed"));
        for s in common::typed::seed(&mut g, 60) {
            exec(&mut c, &s).await?;
        }
        let specs = common::typed::views();
        hand_materialize(&db, &specs, true).await?;
        check_views(&db, &specs).await?;
        for _ in 0..scaled(150) {
            let stmts = common::typed::random_change(&mut g);
            exec(&mut c, "START TRANSACTION").await?;
            exec_all(&mut c, &stmts).await?;
            exec(&mut c, "COMMIT").await?;
        }
        hand_materialize(&db, &specs, false).await?;
        check_views(&db, &specs).await?;
        c.disconnect().await.ok();
        Ok(())
    });
}

#[test]
fn selftest_deterministic_generator() {
    let ids = || Arc::new(seed_statements(42, Sizes::tiny()).1);
    let a: Vec<Vec<String>> = {
        let mut w = Workload::new(7, ids());
        (0..300)
            .flat_map(|_| w.next_batch())
            .map(|t| t.stmts)
            .collect()
    };
    let b: Vec<Vec<String>> = {
        let mut w = Workload::new(7, ids());
        (0..300)
            .flat_map(|_| w.next_batch())
            .map(|t| t.stmts)
            .collect()
    };
    assert_eq!(a, b, "same seed must give the same statements");
    let c: Vec<Vec<String>> = {
        let mut w = Workload::new(8, ids());
        (0..300)
            .flat_map(|_| w.next_batch())
            .map(|t| t.stmts)
            .collect()
    };
    assert_ne!(a, c);
    assert_eq!(
        seed_statements(1, Sizes::tiny()).0,
        seed_statements(1, Sizes::tiny()).0
    );
    assert_eq!(derive_seed(1, "x"), derive_seed(1, "x"));
    assert_ne!(derive_seed(1, "x"), derive_seed(1, "y"));
}

#[test]
fn selftest_diff_and_dsn() {
    let t = |rows: &[&[Option<&str>]]| Table {
        columns: vec!["id".into(), "v".into()],
        rows: rows
            .iter()
            .map(|r| r.iter().map(|c| c.map(|s| s.as_bytes().to_vec())).collect())
            .collect(),
    };
    let a = t(&[
        &[Some("1"), Some("x")],
        &[Some("2"), None],
        &[Some("2"), None],
    ]);
    let b = t(&[
        &[Some("2"), None],
        &[Some("1"), Some("x")],
        &[Some("2"), None],
    ]);
    assert!(diff_tables(&a, &b, 10).is_none(), "order must not matter");
    let c = t(&[
        &[Some("1"), Some("y")],
        &[Some("2"), None],
        &[Some("3"), Some("z")],
    ]);
    let d = diff_tables(&a, &c, 10).expect("diff");
    assert!(d.contains("≠ expected ('1', 'x')"), "{d}");
    assert!(d.contains("MV has ('1', 'y')"), "{d}");
    assert!(d.contains("- missing  ('2', NULL)"), "{d}");
    assert!(d.contains("+ extra    ('3', 'z')"), "{d}");
    let mut e = a.clone();
    e.columns[1] = "w".into();
    assert!(diff_tables(&a, &e, 10).unwrap().contains("column mismatch"));

    assert_eq!(
        dsn_with_db("mysql://lv:lv@127.0.0.1:33084/lvtest", "x"),
        "mysql://lv:lv@127.0.0.1:33084/x"
    );
    assert_eq!(
        dsn_with_db("mysql://lv:lv@h:1/lvtest?require_ssl=true", "y"),
        "mysql://lv:lv@h:1/y?require_ssl=true"
    );
    assert_eq!(dsn_with_db("mysql://lv:lv@h:1", "z"), "mysql://lv:lv@h:1/z");
    assert_eq!(
        dsn_with_db("mysql://lv:lv@h:1?a=b", "z"),
        "mysql://lv:lv@h:1/z?a=b"
    );
    assert_eq!(q("a'b\\c"), "'a\\'b\\\\c'");
}

/// A multi-server scenario with too few servers: a visible, counted `SKIP` on stderr outside CI,
/// a failure in CI (`CI=true`) or with `LAZYVIEW_TEST_REQUIRE_MULTI=1`. Runs
/// `selftest_multi_skip_probe` in a child test process (the only way to control its environment
/// and to see past the harness's output capture).
#[test]
fn selftest_multi_skip_or_fail() {
    let run = |ci: Option<&str>, require: Option<&str>| {
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "--ignored", "selftest_multi_skip_probe"])
            .env_remove("LAZYVIEW_TEST_DSNS")
            .env_remove("CI")
            .env_remove("LAZYVIEW_TEST_REQUIRE_MULTI");
        if let Some(v) = ci {
            cmd.env("CI", v);
        }
        if let Some(v) = require {
            cmd.env("LAZYVIEW_TEST_REQUIRE_MULTI", v);
        }
        let out = cmd.output().expect("run the probe");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), text)
    };
    let skip_line = "SKIP selftest_multi: needs 3 servers in LAZYVIEW_TEST_DSNS, have 0";
    // Outside CI: skipped, and the SKIP line shows without --nocapture.
    let (ok, text) = run(None, None);
    assert!(ok, "skip outside CI should pass:\n{text}");
    assert!(text.contains(skip_line), "no visible SKIP line:\n{text}");
    assert!(
        text.contains("[multi-server scenario 1 skipped"),
        "no count:\n{text}"
    );
    // ...and once more after the harness's summary, which counts it as passed.
    let summary = "SKIPPED 1 multi-server scenario(s), counted as passed above: \
                   selftest_multi (needs 3, have 0)";
    assert!(
        text.find("test result: ok")
            .zip(text.find(summary))
            .is_some_and(|(result, skipped)| skipped > result),
        "no skip summary after the test result:\n{text}"
    );
    let (ok, text) = run(Some("false"), None);
    assert!(
        ok && text.contains(skip_line),
        "CI=false should skip:\n{text}"
    );
    // CI, or an explicit requirement: a failure naming the rule.
    for (ci, require) in [
        (Some("true"), None),
        (None, Some("1")),
        (Some("1"), Some("1")),
    ] {
        let (ok, text) = run(ci, require);
        assert!(!ok, "CI={ci:?} REQUIRE={require:?} should fail:\n{text}");
        assert!(
            text.contains("selftest_multi: needs 3 servers")
                && text.contains("multi-server scenarios must run here"),
            "CI={ci:?} REQUIRE={require:?}: wrong failure:\n{text}"
        );
        assert!(
            !text.contains("SKIP selftest_multi") && !text.contains("SKIPPED"),
            "failed AND skipped:\n{text}"
        );
    }
    // An explicit `0` allows the skip even in CI (the single-server matrix jobs).
    let (ok, text) = run(Some("true"), Some("0"));
    assert!(
        ok && text.contains(skip_line),
        "REQUIRE=0 should skip in CI:\n{text}"
    );
}

/// Child of `selftest_multi_skip_or_fail` (never runs on its own: `--ignored`).
#[test]
#[ignore]
fn selftest_multi_skip_probe() {
    scenario_multi("selftest_multi", 2, |_ctx| async move {
        bail!("the probe must not run against servers")
    });
}

/// Drops leftover `lvt_*` schemas on every server (run explicitly with `--ignored`).
#[test]
#[ignore]
fn cleanup_test_schemas() {
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
/// test --test harness_selftest -- --ignored dump_workload`.
#[test]
#[ignore]
fn dump_workload() {
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
