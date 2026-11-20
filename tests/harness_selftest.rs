//! Self-tests of the integration harness that do not need the in-process engine: fresh schemas,
//! binlog positions, workload generator + all view SQL on every flavor, the oracle (hand-filled
//! table vs view, mismatch detection and diff formatting), and the engine-as-process harness
//! (`common::engineproc`: the real binary spawned, scraped, sampled and killed).
//!
//! The maintenance tools that used to live here (schema cleanup, workload dumping) are
//! examples: `cargo run --example cleanup_test_schemas`, `cargo run --example dump_workload`.

mod common;

use std::sync::Arc;

use anyhow::{bail, ensure};
use common::engineproc::{self, EngineProc, KillPoint, KillPointPlan, ProcStat, Sample, Scraped};
use common::oracle::{diff_tables, hand_materialize, Table};
use common::soakcheck::{
    percentiles, scan_warn_text, slope_kib_per_hour, soak_rss_slope_mib_h, unexpected_warns,
};
use common::workload::{
    ecommerce_views, run_batches, run_concurrent, run_txn, seed_statements, setup_ecommerce, Sizes,
    Txn, Workload,
};
use common::*;
use std::fmt::Write as _;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

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

/// run_txn retries a duplicate key (ERROR 1062) for transactions that delete a row and
/// re-create it by its key (`retry_duplicate`, MySQL 26.x with DDL in the schema) and fails on it
/// otherwise: here the re-created key is held by another row until a second session removes it.
#[test]
fn selftest_duplicate_key_retry() {
    scenario("selftest_dup", |ctx| async move {
        let db = ctx.new_db("selftest_dup").await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "CREATE TABLE entity (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
        )
        .await?;
        exec(&mut c, "INSERT INTO entity VALUES (1, 'held'), (2, 'item')").await?;
        let txn = |retry_duplicate| Txn {
            label: "recreate".into(),
            stmts: vec![
                "DELETE FROM entity WHERE id = 2".into(),
                "INSERT INTO entity (id, name) VALUES (1, 'reborn')".into(),
            ],
            rollback: false,
            retry_duplicate,
        };
        let e = run_txn(&mut c, &txn(false)).await.unwrap_err();
        ensure!(
            server_code(&e) == Some(1062),
            "expected ERROR 1062, got {e:#}"
        );
        ensure!(
            scalar_i64(&mut c, "SELECT COUNT(*) FROM entity").await? == 2,
            "the failed transaction was rolled back"
        );
        let dbc = db.clone();
        let release = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let mut c = dbc.conn().await?;
            exec(&mut c, "DELETE FROM entity WHERE id = 1").await
        });
        run_txn(&mut c, &txn(true)).await?;
        release.await??;
        ensure!(
            scalar(&mut c, "SELECT GROUP_CONCAT(name ORDER BY id) FROM entity")
                .await?
                .as_deref()
                == Some("reborn"),
            "the retried transaction applied once"
        );
        Ok(())
    });
}

// ---------------------------------------------------------------------------------------------
// The soak checks' pure parts (S2.8 of `tmp/out/lvs-soak/breakdown.md`): no server, no engine.
// ---------------------------------------------------------------------------------------------

/// Nearest-rank percentiles (§4's lag numbers): the 1..=100 case, an unsorted slice, one sample,
/// and the empty case a run that never reached a check shows.
#[test]
fn selftest_percentiles() {
    let hundred: Vec<f64> = (1..=100).map(|x| x as f64).collect();
    assert_eq!(percentiles(&hundred), (50.0, 95.0, 100.0));
    assert_eq!(percentiles(&[3.0, 1.0, 2.0, 10.0]), (2.0, 10.0, 10.0));
    assert_eq!(percentiles(&[7.5]), (7.5, 7.5, 7.5));
    assert_eq!(percentiles(&[]), (0.0, 0.0, 0.0));
}

/// The RSS trend rule of §4/D7: flat samples are flat, +1 KiB/s is 3600 KiB/h, one point or none
/// cannot show a trend, and a flat run is inside the default budget.
#[test]
fn selftest_slope_flat_and_growing() {
    let flat: Vec<(f64, u64)> = (0u64..20).map(|i| (i as f64 * 60.0, 500_000)).collect();
    assert!(
        slope_kib_per_hour(&flat).abs() < 1e-9,
        "flat samples: {}",
        slope_kib_per_hour(&flat)
    );
    // One KiB per second (every sample one minute later, 60 KiB higher).
    let growing: Vec<(f64, u64)> = (0u64..20)
        .map(|i| (i as f64 * 60.0, 500_000 + i * 60))
        .collect();
    let slope = slope_kib_per_hour(&growing);
    assert!(
        (slope - 3600.0).abs() < 1e-6,
        "1 KiB/s must be 3600 KiB/h, got {slope}"
    );
    assert_eq!(slope_kib_per_hour(&[(0.0, 1)]), 0.0);
    assert_eq!(slope_kib_per_hour(&[]), 0.0);
    // D7's default budget is 64 MiB/h: a flat run is inside it, a 1 KiB/s leak is not a leak yet.
    assert!(slope_kib_per_hour(&flat) <= soak_rss_slope_mib_h() * 1024.0);
    assert!(slope < soak_rss_slope_mib_h() * 1024.0);
    assert!(slope_kib_per_hour(&growing) > slope_kib_per_hour(&flat));
}

/// WARN accounting (§4): a synthetic 40-line log, two allowlisted buckets (a target behind and
/// reconnecting, `EXPECTED_WARNS`) and one unexpected one; an `ERROR` line and a panic are hard
/// failures, and a level word inside a message must not move the line's bucket.
#[test]
fn selftest_warn_bucketing() {
    let mut lines: Vec<String> = (0..37)
        .map(|i| format!("2026-11-12T11:00:{i:02}Z  INFO lazyview::engine: flush {i} rows=3"))
        .collect();
    lines.push(
        "2026-11-12T11:01:00Z  WARN lazyview::engine: view=items target=t1 buffered_mib=2 the \
         changes kept for this target exceed their 4194304 byte budget"
            .to_string(),
    );
    lines.push(
        "2026-11-12T11:01:01Z  WARN lazyview::engine: view=items target=t1 write failed: \
         connection reset; retrying in 250ms"
            .to_string(),
    );
    lines
        .push("2026-11-12T11:01:02Z  WARN lazyview::engine: something nobody expected".to_string());
    assert_eq!(lines.len(), 40, "the synthetic log is 40 lines");

    let warns = scan_warn_text(&lines.join("\n"), "synthetic").expect("the log has no ERROR line");
    assert_eq!(warns.len(), 3, "one bucket per WARN: {warns:?}");
    assert_eq!(warns.values().sum::<u64>(), 3);
    let bad = unexpected_warns(&warns);
    assert_eq!(bad.len(), 1, "one unexpected bucket: {warns:?}");
    assert!(
        bad[0].starts_with("something nobody"),
        "the unexpected bucket: {}",
        bad[0]
    );
    // The two expected buckets are bucketed by their message, not by their `view=`/`target=` fields.
    assert!(warns
        .keys()
        .any(|k| k.starts_with("the changes kept for this target")));
    assert!(warns
        .keys()
        .any(|k| k.starts_with("write failed: connection reset")));

    // A level word inside a message does not make the line an ERROR, and an ERROR line fails.
    let quiet = "2026-11-12T11:02:00Z  INFO lazyview::engine: no WARN here, ERROR words stay INFO";
    assert!(scan_warn_text(quiet, "synthetic").unwrap().is_empty());
    assert!(
        scan_warn_text(
            "2026-11-12T11:03:00Z ERROR lazyview::engine: refresh failed: ERROR 1062 (23000)",
            "synthetic"
        )
        .is_err(),
        "an ERROR line fails the run"
    );
    assert!(
        scan_warn_text(
            "thread 'main' panicked at src/engine.rs:1: boom",
            "synthetic"
        )
        .is_err(),
        "a panic fails the run"
    );
}

// ---------------------------------------------------------------------------------------------
// The harness's own HTTP client (issue #216): no server, no engine.
// ---------------------------------------------------------------------------------------------

/// Both helpers of `common::engine` decode a `Transfer-Encoding: chunked` answer. The toxiproxy
/// API answers chunked once a body is large, and a chunk-blind read hands the caller the size
/// lines as payload (`"200, not JSON: 14de"`), which is how the chaos suite's `Drop` cleanup
/// leaked proxies into the listen band. A canned listener stands in for the server: chunked on one
/// connection, plain `Content-Length` on the next (where the decode must be the identity).
#[tokio::test]
async fn selftest_http_decodes_chunked_body() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Big enough that the size lines are two hex digits, as the leaked `GET /proxies` answer's
    // were (`14de` = 5342 bytes) — a decoder that only handled `0`..=`9` would pass a toy body.
    let payload = format!(
        "{{\"name\":\"{}\",\"n\":7,\"items\":[1,2,3],\"ok\":true}}",
        "entity".repeat(8)
    );
    let mut chunked: Vec<u8> =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for part in payload.as_bytes().chunks(23) {
        chunked.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
        chunked.extend_from_slice(part);
        chunked.extend_from_slice(b"\r\n");
    }
    chunked.extend_from_slice(b"0\r\n\r\n");
    let plain = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let serve = tokio::spawn(async move {
        for answer in [chunked, plain.into_bytes()] {
            // One canned answer per connection: the client asks once and the server closes.
            let (mut s, _) = listener
                .accept()
                .await
                .expect("the harness client connects");
            let mut request = [0u8; 512];
            let got = s.read(&mut request).await.expect("the request arrives");
            assert!(got > 0, "the client sent its request");
            s.write_all(&answer).await.expect("the answer goes out");
            s.shutdown().await.expect("the answer is the whole body");
        }
    });

    let (status, v) = http_json(&addr, "POST", "/proxies", "{}").await?;
    assert_eq!(status, 200, "the chunked answer's status");
    assert_eq!(v["name"].as_str(), Some("entity".repeat(8).as_str()));
    assert_eq!(v["n"].as_i64(), Some(7));
    assert_eq!(v["items"].as_array().map(|a| a.len()), Some(3));

    let r = request(&addr, "GET", "/stats", "").await?;
    assert_eq!(r.status, 200, "the plain answer's status");
    assert_eq!(
        r.body, payload,
        "Content-Length body passes through untouched"
    );
    assert!(
        r.head.contains("Content-Length: "),
        "the head stays raw: {}",
        r.head
    );
    serve.await.expect("the canned server finished");
    Ok(())
}

/// Strict mode (`LAZYVIEW_TEST_STRICT`): the prefix list decides which tests may not skip, and a
/// refused skip names the reason. Checked on the pure functions, since the environment is shared
/// by the parallel tests of this binary.
#[test]
fn selftest_strict_skip_rules() {
    // "1" covers every test; a comma list covers the tests whose name starts with an entry.
    assert!(strict_applies("1", "tg_fanout"));
    assert!(strict_applies("targets_", "targets_one_target_down"));
    assert!(!strict_applies("targets_", "tg_fanout"));
    assert!(strict_applies(
        "selftest_x, targets_",
        "targets_fan_out_and_retarget"
    ));
    assert!(!strict_applies("selftest_x, targets_", "soak_run"));
    assert!(!strict_applies(" , ", "targets_fan_out_and_retarget"));
    // Strict on and covering the test: the skip is a failure, and the message names the reason.
    let msg = skip_failure(
        Some("targets_"),
        "targets_pg_first_build",
        "no PostgreSQL DSN",
    )
    .expect("a covered skip must fail");
    assert!(msg.contains("targets_pg_first_build"), "{msg}");
    assert!(msg.contains("no PostgreSQL DSN"), "{msg}");
    // Strict off, or not covering the test: the skip stays a visible skip.
    assert!(skip_failure(None, "targets_pg_first_build", "no PostgreSQL DSN").is_none());
    assert!(skip_failure(Some("targets_"), "tg_fanout", "no DSN").is_none());
}

// ---------------------------------------------------------------------------------------------
// The engine-as-process harness (tests/common/engineproc.rs, plan tmp/out/lvs-perf/plan §PR1)
// ---------------------------------------------------------------------------------------------

/// The Prometheus reader, on the shape `src/health/prometheus.rs` writes: counters carry `_total`,
/// gauges are plain, labels are quoted and escaped; and a malformed exposition must not sneak
/// through. Pure: no server, so it can never print a SKIP.
#[test]
fn selftest_engineproc_prom_parse() {
    let text = "# HELP lazyview_events_total x\n# TYPE lazyview_events_total counter\n\
                lazyview_events_total 17\nlazyview_flushes_total 3\nlazyview_streaming 1\n\
                lazyview_inflight_rows 0\nlazyview_applied_binlog_pos{file=\"binlog.000007\"} 4213\n\
                lazyview_supervisor_max{pool=\"write\",endpoint=\"an\\\"x\"} 4\n\
                lazyview_heartbeat_age_seconds 1.5\n";
    let s = Scraped::parse(Instant::now(), text).expect("parse the exposition");
    assert_eq!(s.counter("events"), 17, "counters carry the _total suffix");
    assert_eq!(
        s.gauge("lazyview_streaming"),
        1.0,
        "the lazyview_ prefix is optional"
    );
    assert_eq!(s.gauge("no_such_metric"), 0.0, "an absent metric reads 0");
    assert_eq!(
        s.labelled("supervisor_max", "endpoint", "an\"x"),
        Some(4.0),
        "an escaped label value"
    );
    assert_eq!(s.gauge("heartbeat_age_seconds"), 1.5, "a fractional gauge");
    let p = s.applied_position().expect("the applied position");
    assert_eq!((p.file.as_str(), p.pos), ("binlog.000007", 4213));
    assert!(s.state_line().contains("streaming=1"), "{}", s.state_line());
    // Counter deltas; a restart whose counter went back down saturates at 0.
    let later = Scraped::parse(Instant::now(), "lazyview_events_total 25\n").unwrap();
    assert_eq!(later.delta(&s, "events"), 8);
    assert_eq!(s.delta(&later, "events"), 0);
    for bad in [
        "lazyview_x",
        "lazyview_x notanumber",
        "lazyview_x{a=\"1\" 1",
        " 5",
    ] {
        assert!(
            engineproc::parse_prometheus(bad).is_err(),
            "accepted {bad:?}"
        );
    }
}

/// `/proc` parsing on fixtures — a `comm` holding spaces and parentheses is the classic trap — and
/// the reduction of a sample list: median, peak, the kernel's high-water mark, the CPU delta.
#[test]
fn selftest_engineproc_proc_parse() {
    let line = "42 (lazy view (x)) S 1 42 41 0 -1 0 0 0 0 0 1200 34 0 0 2 0 4195868 0 0 0 0";
    let (state, ticks) = engineproc::parse_stat(line).expect("parse stat");
    assert_eq!(state, 'S');
    assert_eq!(ticks, 1234, "utime+stime, counted from the last `)`");
    assert_eq!(
        engineproc::parse_stat("1 (x) S 1"),
        None,
        "too few fields is not a reading"
    );

    let status = "Name:lazyview\nVmRSS:   123456 kB\nRssAnon:     100 kB\nVmHWM:   234567 kB\n";
    assert_eq!(
        engineproc::parse_status_mem(status),
        Some((123456, 234567)),
        "VmRSS and VmHWM in KiB"
    );
    assert_eq!(
        engineproc::parse_status_mem("Name:lazyview\nVmRSS: 1 kB\n"),
        None,
        "a status without VmHWM is not a reading"
    );

    let st = |state: char, cpu_ticks: u64, rss_kib: u64, vmhwm_kib: u64| ProcStat {
        state,
        cpu_ticks,
        rss_kib,
        vmhwm_kib,
    };
    let samples = vec![
        Sample {
            at: Duration::from_millis(0),
            stat: st('R', 100, 100, 120),
        },
        Sample {
            at: Duration::from_millis(10),
            stat: st('T', 100, 300, 320),
        },
        Sample {
            at: Duration::from_millis(20),
            stat: st('T', 100, 200, 320),
        },
        Sample {
            at: Duration::from_millis(30),
            stat: st('S', 150, 500, 520),
        },
        Sample {
            at: Duration::from_millis(40),
            stat: st('S', 150, 400, 520),
        },
    ];
    let u = engineproc::reduce_samples(samples);
    assert_eq!(
        (u.samples, u.stopped_samples),
        (5, 2),
        "the stopped window is counted"
    );
    assert_eq!(u.rss_med_kib, 300, "the median of 100,200,300,400,500");
    assert_eq!((u.rss_first_kib, u.rss_peak_kib), (100, 500));
    assert_eq!(u.rss_mean_kib, 300);
    assert_eq!(u.vmhwm_kib, 520, "the kernel's peak, not the sampled one");
    assert_eq!(u.wall, Duration::from_millis(40));
    assert!((u.cpu_seconds - 0.5).abs() < 1e-9, "{}", u.cpu_seconds);
    assert_eq!(
        engineproc::reduce_samples(vec![]).samples,
        0,
        "no samples, no claim"
    );
}

/// The kill-point picker: the same seed gives the same sequence (so a red run reproduces), every
/// point is dealt once per deck (a crash loop covers every mode before any repeats), and `peek` does
/// not consume the plan.
#[test]
fn selftest_engineproc_kill_points_seeded() {
    let seq = |seed: u64| -> Vec<&'static str> {
        KillPointPlan::new(seed)
            .peek(24)
            .iter()
            .map(|k| k.name())
            .collect()
    };
    assert_eq!(seq(7), seq(7), "the same seed must give the same sequence");
    assert_ne!(seq(7), seq(8), "different seeds must differ");
    for seed in [1u64, 7, 99, 1_234_567] {
        let s = seq(seed);
        for k in KillPoint::all() {
            let n = s[..KillPoint::all().len()]
                .iter()
                .filter(|x| **x == k.name())
                .count();
            assert_eq!(
                n,
                1,
                "seed {seed}: {} dealt {n} times in the first deck",
                k.name()
            );
        }
    }
    let mut p = KillPointPlan::new(3);
    let expected = p.peek(3);
    let dealt: Vec<KillPoint> = (0..3).map(|_| p.next()).collect();
    assert_eq!(expected, dealt, "peek previews exactly what next() deals");
    assert!(
        KillPoint::all().contains(&expected[0]),
        "a point outside the deck"
    );
    // Every name is distinct, so a table row per point cannot collide.
    let names: Vec<&str> = KillPoint::all().iter().map(|k| k.name()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), names.len(), "{names:?}");
}

/// The sampler against the kernel's own accounting for **one** process: `VmHWM` is what `time -v`
/// prints as maximum resident set size and `utime+stime` what it prints as user+system (both come
/// from `getrusage`'s source), so the sampled peak RSS has to land within 5 % of the peak the kernel
/// recorded. GNU `time` is compared as well when the host has it — it is not installed here, and a
/// host without it still gets the kernel's numbers, which are the same ones. `sort` on a seeded file:
/// deterministic, and it holds megabytes in memory, so a 5 % band says something.
///
/// The window is sized by **work**, not by the runner: the sampler takes one sample per interval, so a
/// fixed input measures a different number of samples on every machine class — a fast runner sorted the
/// first input in 20 ms and the window came out at 9 samples, too few to say anything about a peak. So
/// the input doubles from the size that proves the claim on a slow host until the run outlasts the
/// minimum window by a margin. A fast runner gets more work, never a shorter look. The bar below is
/// what has to be cleared either way.
#[test]
fn selftest_engineproc_sampler_vs_time() {
    /// The bar: below it a "peak" is one `/proc` read of one process and nothing more.
    const MIN_SAMPLES: usize = 10;
    /// How far above the bar the window is driven, so a runner several times faster than this host
    /// cannot land the sample count on the edge of the bar.
    const WINDOW_MARGIN: usize = 3;
    /// Ceiling on the growth: room to clear the margin on a very fast runner, small enough that
    /// `sort` stays a fraction of the harness's own memory.
    const MAX_LINES: usize = 600_000;
    const INTERVAL: Duration = Duration::from_millis(2);

    let dir = logs_root().join(format!("selftest-sampler-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("sort-in.txt");
    let sort = ["/usr/bin/sort", "/bin/sort"]
        .iter()
        .map(PathBuf::from)
        .find(|p| p.is_file())
        .expect("sort");

    let mut lines = 60_000;
    let (u, gnu) = loop {
        let mut r = engineproc::Rng::new(0x243f_6a88_85a3_08d3); // fixed: every size is the same file, longer
        let mut data = String::new();
        for i in 0..lines {
            writeln!(data, "{:020x} line-{i}", r.next_u64()).unwrap();
        }
        assert!(
            data.len() >= 1 << 20,
            "the workload is too small to measure: {} B",
            data.len()
        );
        std::fs::write(&input, &data).unwrap();
        let (u, gnu) = engineproc::run_measured(
            &sort,
            &[input.display().to_string(), "-o".into(), "/dev/null".into()],
            INTERVAL,
        )
        .expect("run sort under the sampler");
        engineproc::measured(format!(
            "sampler: {lines} lines, {} KiB in: {}",
            data.len() / 1024,
            u.line()
        ));
        if u.samples >= MIN_SAMPLES * WINDOW_MARGIN {
            break (u, gnu);
        }
        assert!(
            lines * 2 <= MAX_LINES,
            "sort of {} lines still only filled the window with {}/{} samples: {}",
            lines,
            u.samples,
            MIN_SAMPLES,
            u.line()
        );
        lines *= 2;
    };
    engineproc::measured(format!("kernel : time -v: {gnu:?}"));
    assert!(
        u.samples >= MIN_SAMPLES,
        "too few samples to say anything: {}",
        u.line()
    );
    assert!(u.cpu_seconds > 0.0, "sort used no CPU: {}", u.line());
    assert!(
        u.rss_peak_kib > u.rss_first_kib,
        "the sampler never saw the memory grow: {}",
        u.line()
    );
    assert!(
        u.rss_first_kib <= u.rss_med_kib && u.rss_med_kib <= u.rss_peak_kib,
        "{}",
        u.line()
    );
    assert!(
        u.vmhwm_kib >= u.rss_peak_kib,
        "the kernel's peak below the sampled one: {}",
        u.line()
    );
    // The fidelity claim: at 2 ms the sampled peak came within 5 % of the peak the kernel recorded.
    let missed = 100.0 - 100.0 * u.rss_peak_kib as f64 / u.vmhwm_kib as f64;
    assert!(
        u.vmhwm_kib.saturating_sub(u.rss_peak_kib) <= (u.vmhwm_kib * 5 / 100).max(256),
        "the sampler missed {missed:.1}% of the peak: {}",
        u.line()
    );
    if let Some(t) = gnu {
        let near = |a: f64, b: f64, floor: f64| (a - b).abs() <= (a.max(b) * 0.05).max(floor);
        assert!(
            near(u.cpu_seconds, t.user_s + t.sys_s, 0.05),
            "sampler cpu {:.3}s vs time -v {:.3}+{:.3}s",
            u.cpu_seconds,
            t.user_s,
            t.sys_s
        );
        assert!(
            near(u.vmhwm_kib as f64, t.max_rss_kib as f64, 256.0),
            "sampler vmhwm {}KiB vs time -v {}KiB",
            u.vmhwm_kib,
            t.max_rss_kib
        );
    } else {
        println!(
            "NOTE: /usr/bin/time (GNU time) is not on this host: the kernel's own VmHWM and \
                  utime+stime were compared, which is what `time -v` reports"
        );
    }
    engineproc::measured(format!(
        "sampler-test harness peak RSS {} MiB (cap: well under 500)",
        engineproc::self_vmhwm_mib()
    ));
    std::fs::remove_dir_all(&dir).ok();
}

/// The engine as a real process killed with real signals: `/startupz` answers, `/metrics` is gated by
/// `LAZYVIEW_HEALTH_TOKEN` and parses, the sampler sees the process, `SIGSTOP` freezes it (its health
/// endpoint with it — the pull-the-plug model, not a simulation), `SIGCONT` thaws it, and `kill -9`
/// leaves the signal in the exit status. The timings are printed: PR2's recovery table starts here.
#[test]
fn selftest_engineproc_process_lifecycle() {
    scenario("selftest_engineproc_live", |ctx| async move {
        let db = ctx.new_db("eplive").await?;
        let mut e = EngineProc::start(&ctx, &db, &[]).await?;
        let pid = e.pid();
        let t_boot = e.wait_startup(Duration::from_secs(60)).await?;
        engineproc::measured(format!(
            "startupz: {} pid {pid} answered after {t_boot:.2?} (spawn→health port)",
            e.label
        ));
        ctx.log(format!(
            "engine {} pid {pid}: /startupz after {t_boot:.2?}, log {}",
            e.label,
            e.log_path.display()
        ));
        // The open probes answer; `/metrics` is not open to an untokened request.
        assert_eq!(e.get("/livez", false).await?.0, 200, "/livez is open");
        assert_eq!(
            e.scrape_unauthorized().await?,
            401,
            "an untokened /metrics must be refused"
        );

        e.start_sampler(Duration::from_millis(20));
        let t_stream = e.wait_streaming(Duration::from_secs(90)).await?;
        let s = e.scrape().await?;
        assert!(
            s.samples.iter().any(|p| p.name == "lazyview_events_total"),
            "the counters block is missing from /metrics"
        );
        assert_eq!(s.gauge("streaming"), 1.0);
        assert_eq!(
            s.gauge("starting"),
            0.0,
            "starting goes to 0 when the stream opens"
        );
        engineproc::measured(format!(
            "streaming: {} reached streaming after {t_stream:.2?}",
            e.label
        ));
        ctx.log(format!(
            "engine streaming after {t_stream:.2?}; {}",
            s.state_line()
        ));

        // SIGSTOP: the process stops, so its health endpoint stops with it.
        e.freeze()?;
        let t0 = Instant::now();
        while engineproc::proc_state(pid) != Some('T') {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "never stopped (state {:?})",
                engineproc::proc_state(pid)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let frozen = tokio::time::timeout(Duration::from_millis(1500), e.scrape()).await;
        assert!(frozen.is_err(), "the frozen engine answered /metrics");

        // SIGCONT: the process and its endpoint come back.
        e.resume()?;
        let t0 = Instant::now();
        while !matches!(engineproc::proc_state(pid), Some('R') | Some('S')) {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "never resumed (state {:?})",
                engineproc::proc_state(pid)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            e.get("/livez", false).await?.0,
            200,
            "the resumed engine answers again"
        );

        let u = e.stop_sampler()?;
        engineproc::measured(format!("sampler: {} pid {pid}: {}", e.label, u.line()));
        ctx.log(format!("engine {} pid {pid}: {}", e.label, u.line()));
        assert!(u.samples >= 5, "the sampler saw {} samples", u.samples);
        assert!(
            u.rss_med_kib > 0 && u.rss_peak_kib >= u.rss_med_kib,
            "{}",
            u.line()
        );
        assert!(
            u.stopped_samples >= 1,
            "the SIGSTOP window was never sampled: {}",
            u.line()
        );

        // kill -9: the kernel closes the sockets, so the servers see the drop and the instance lock
        // goes with the connection; the harness sees the signal in the exit status.
        e.kill_now()?;
        let st = e.wait_exit(Duration::from_secs(10))?;
        assert_eq!(st.signal(), Some(9), "not killed by SIGKILL: {st:?}");
        EngineProc::wait_pid_gone(pid, Duration::from_secs(10))?;
        assert!(e.exit_status().is_some(), "the exit status is recorded");
        engineproc::measured(format!(
            "lifecycle-test harness peak RSS {} MiB (cap: well under 500)",
            engineproc::self_vmhwm_mib()
        ));
        Ok(())
    });
}

/// The real binary applying real binlog events: a filter/projection view over a neutral `items`
/// table converges while the process runs, its counters move, an incremental view does not fall back
/// to a rebuild (`full_rebuilds` delta 0 over the incremental phase), and `SIGTERM` stops it. This is
/// what PR2 measures the recovery *of*, so the times are printed here first.
#[test]
fn selftest_engineproc_applies_changes() {
    scenario("selftest_engineproc_applies", |ctx| async move {
        let db = ctx.new_db("epapply").await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "CREATE TABLE items (id BIGINT PRIMARY KEY, qty INT NOT NULL, kind VARCHAR(16) NOT NULL)",
        )
        .await?;
        ensure_lazyview_table(&mut c).await?;
        add_definition(
            &mut c,
            "CREATE VIEW items_big AS SELECT id, qty FROM items WHERE qty > 100",
            None,
        )
        .await?;
        c.disconnect().await.ok();

        let mut e =
            EngineProc::start(&ctx, &db, &[("LAZYVIEW_POSITION_PERSIST_MS", "500")]).await?;
        let t_boot = e.wait_startup(Duration::from_secs(60)).await?;
        let t_stream = e.wait_streaming(Duration::from_secs(90)).await?;
        // /readyz is red until every view has a live version, so green here means the first build is
        // done and what comes next is the incremental path the rule is about.
        let t_ready = e.wait_ready(Duration::from_secs(120)).await?;
        engineproc::measured(format!(
            "first build: {} startupz {t_boot:.2?}, streaming {t_stream:.2?}, readyz {t_ready:.2?} (spawn→)",
            e.label
        ));
        ctx.log(format!(
            "engine {} built its first view: startupz {t_boot:.2?}, streaming {t_stream:.2?}, \
             readyz {t_ready:.2?}",
            e.label
        ));
        let before = e.scrape().await?;

        // The workload, through the source: half the rows are above the filter.
        let (mut want_n, mut want_sum) = (0i64, 0i64);
        let mut ins = String::from("INSERT INTO items (id, qty, kind) VALUES ");
        for id in 1..=500i64 {
            let qty = if id % 2 == 0 { 150 } else { 50 };
            if qty > 100 {
                want_n += 1;
                want_sum += id;
            }
            write!(ins, "({id}, {qty}, 'k{}'),", id % 5).unwrap();
        }
        ins.truncate(ins.len() - 1);
        let mut c = db.conn().await?;
        exec(&mut c, &ins).await?;
        c.disconnect().await.ok();

        // The view converges on what the test wrote, compared as aggregates in SQL: the harness never
        // holds a whole view in memory.
        let t0 = Instant::now();
        wait_until(
            "items_big to hold the rows written after the first build",
            Duration::from_secs(120),
            || {
                // Copies, so the FnMut can be called again after the first future is consumed.
                let (db, want_n, want_sum) = (&db, want_n, want_sum);
                async move {
                    let mut c = db.conn().await?;
                    let n = count(&mut c, "items_big").await?;
                    let s =
                        scalar_i64(&mut c, "SELECT COALESCE(SUM(id), 0) FROM items_big").await?;
                    c.disconnect().await.ok();
                    Ok(n == want_n && s == want_sum)
                }
            },
        )
        .await?;
        let t_caught = t0.elapsed();

        let after = e.scrape().await?;
        let events = after.delta(&before, "events");
        let applied = after.delta(&before, "mv_rows_inserted")
            + after.delta(&before, "mv_rows_upserted")
            + after.delta(&before, "mv_rows_deleted");
        engineproc::measured(format!(
            "apply: {} applied {applied} MV rows from {events} binlog events, caught up in {t_caught:.2?}, \
             full_rebuilds delta 0",
            e.label
        ));
        ctx.log(format!(
            "engine applied {applied} MV rows from {events} binlog events, caught up in {t_caught:.2?}; \
             {}",
            after.state_line()
        ));
        assert!(events > 0, "the binlog loop read no events");
        assert!(
            applied >= want_n as u64,
            "the engine wrote {applied} MV rows, expected at least {want_n}"
        );
        assert_eq!(
            after.delta(&before, "full_rebuilds"),
            0,
            "an incremental view fell back to a full rebuild"
        );

        // And it stops when asked: the graceful shutdown, whose time is recorded (no budget yet —
        // PR5 sets budgets from the measured distribution).
        let t_stop = Instant::now();
        let st = e.stop()?;
        engineproc::measured(format!(
            "stop: {} left on SIGTERM in {:.2?}: {st:?}",
            e.label,
            t_stop.elapsed()
        ));
        engineproc::measured(format!(
            "apply-test harness peak RSS {} MiB (cap: well under 500)",
            engineproc::self_vmhwm_mib()
        ));
        ctx.log(format!(
            "engine stopped in {:.2?}: {st:?}",
            e.spawned_at.elapsed()
        ));
        EngineProc::wait_pid_gone(e.pid(), Duration::from_secs(10))?;
        Ok(())
    });
}
