//! The module's own tests: the seam's grammar, what the ON path really puts on the wire, that the
//! OFF path builds nothing, and that a collector which is not there changes nothing about a run.
//!
//! The exporter is a capturing one (`Capture`), so these tests assert what a collector would have
//! been sent without needing a collector. The environment of a collector setup is set only while
//! the module's test lock is held: `OTEL_*` is process-wide, and this is the one place in the
//! crate that writes it.

use super::*;
use opentelemetry::Value;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::SpanData;
use std::sync::{Arc, Mutex};

/// Spans as the SDK produced them: what an OTLP collector would have received.
#[derive(Clone, Default, Debug)]
struct Capture(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for Capture {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let me = self.0.clone();
        me.lock().unwrap_or_else(|e| e.into_inner()).extend(batch);
        Ok(())
    }
}

impl Capture {
    /// Everything the exporter was given. Called after [`super::with_telemetry`] returned: its
    /// `shutdown` is what flushes the batch processor's queue.
    fn taken(&self) -> Vec<SpanData> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn one(&self, name: &str) -> SpanData {
        let hit = self
            .taken()
            .into_iter()
            .find(|s| s.name.as_ref() == name)
            .unwrap_or_else(|| panic!("no span named {name} was exported"));
        hit
    }
}

/// An exporter whose collector is not there: every export fails, which the module must survive.
#[derive(Default, Debug)]
struct Unreachable;

impl SpanExporter for Unreachable {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        Err(OTelSdkError::InternalFailure("connection refused".into()))
    }
}

fn host<'a>() -> Host<'a> {
    Host {
        config_file: None,
        service_name: "lazyview-test",
        service_version: "0.0.0-test",
        source: Some("source-1"),
    }
}

/// The same host, naming the declarative-config file the operator would have named.
fn host_with<'a>(config_file: Option<&'a std::path::Path>) -> Host<'a> {
    Host {
        config_file,
        ..host()
    }
}

/// Files a test needs go under the target directory — never `/tmp`, and never next to the sources
/// where a run would leave them behind.
fn scratch_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        p = std::path::PathBuf::from(dir);
    }
    p.join("lvs-otel-tests")
}

fn write_scratch(name: &str, contents: &str) -> std::path::PathBuf {
    let dir = scratch_dir();
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write config");
    path
}

fn attr(s: &SpanData, key: &str) -> Option<Value> {
    s.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.clone())
}

fn str_attr(s: &SpanData, key: &str) -> Option<String> {
    match attr(s, key) {
        Some(Value::String(v)) => Some(v.as_str().to_string()),
        _ => None,
    }
}

fn int_attr(s: &SpanData, key: &str) -> Option<i64> {
    match attr(s, key) {
        Some(Value::I64(v)) => Some(v),
        _ => None,
    }
}

fn bool_attr(s: &SpanData, key: &str) -> Option<bool> {
    match attr(s, key) {
        Some(Value::Bool(v)) => Some(v),
        _ => None,
    }
}

fn res(r: &Resource, key: &str) -> Option<String> {
    match r.get_ref(&opentelemetry::Key::new(key.to_string())) {
        Some(Value::String(v)) => Some(v.as_str().to_string()),
        _ => None,
    }
}

// ---------------------------------------------------------------- the gate

/// The gate's four answers, and that none of them is ever a failure: `OTEL_CONFIG_FILE` is the
/// whole switch, so a value that is not a readable regular file leaves telemetry off and says so
/// once, naming the variable and the path.
#[test]
fn the_file_named_by_the_environment_is_the_switch() {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let named = manifest.join("deploy/k8s/base/otel-config.json");

    // Unset, empty and whitespace-only are off, and off quietly: an ambient collector that exports
    // by itself must not make this process say anything about telemetry.
    for v in [None, Some(""), Some("   ")] {
        let g = gate(CONFIG_FILE_ENV, v);
        assert_eq!(g.file, None, "{v:?} names no file");
        assert_eq!(
            g.warning, None,
            "{v:?} is the documented default, not a mistake"
        );
    }

    // A file that is there is the on switch, and the only thing the on switch needs.
    let g = gate(CONFIG_FILE_ENV, named.to_str());
    assert_eq!(
        g.file.as_deref(),
        Some(named.as_path()),
        "the named file is on"
    );
    assert_eq!(g.warning, None, "nothing to complain about");

    // Anything else is off *with* the one sentence that tells the operator why.
    for v in [
        "/definitely/not/mounted/lazyview-otel.json",
        manifest.to_str().unwrap(),
    ] {
        let g = gate(CONFIG_FILE_ENV, Some(v));
        assert_eq!(
            g.file, None,
            "{v} is not a file telemetry can be built from"
        );
        let w = g
            .warning
            .unwrap_or_else(|| panic!("{v} must say why telemetry stayed off"));
        assert!(w.contains(CONFIG_FILE_ENV), "{w} names the variable");
        assert!(w.contains(v), "{w} names the path");
    }
}

/// `init` is the caller's safety net: whatever it is handed — no file, a missing path, a directory,
/// a file full of things this build cannot carry — it answers `false` and installs nothing. It is
/// never an error, never a panic, and never a half-built provider.
#[test]
fn a_start_is_never_failed_by_a_telemetry_setup_it_cannot_carry() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset();
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let missing = manifest.join("deploy/k8s/base/not-mounted.json");
    let scratch = scratch_dir();

    for path in [None, Some(missing.as_path()), Some(scratch.as_path())] {
        assert!(
            !init(&host_with(path)),
            "{path:?} builds nothing rather than failing the start"
        );
        assert!(!enabled(), "{path:?} leaves no provider installed");
        assert!(!initialised(), "{path:?} leaves no exporter either");
    }

    // A file whose every exporter this build does not carry: off, with the sentence about it.
    let refused = write_scratch(
        "refused-http.json",
        r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"exporter":{"otlp_http":{"endpoint":"http://127.0.0.1:4318"}}}}]}}"#,
    );
    assert!(!init(&host_with(Some(&refused))), "gRPC only");
    assert!(!enabled());

    // A file that is there and readable but names no processor this build can use: the same answer.
    let mute = write_scratch(
        "mute.json",
        r#"{"file_format":"1.2","tracer_provider":{"enabled":false}}"#,
    );
    assert!(!init(&host_with(Some(&mute))), "an off file builds nothing");
    assert!(!enabled());
    reset();
}

/// The file the operator is pointed at is accepted by the code that reads it: `init` says yes to
/// it, in the same kind of runtime context the engine starts in. The endpoint points at a closed
/// port — building an exporter never connects, so this proves the read and the build, and the
/// shutdown afterwards proves a collector that never answers cannot wedge a start.
#[test]
fn the_starting_file_the_repository_ships_is_accepted() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset();
    let named = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("deploy/k8s/base/otel-config.json");
    let g = gate(CONFIG_FILE_ENV, named.to_str());
    assert!(g.file.is_some(), "the shipped file is a gate");
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime to build the exporter in");
    let on = rt.block_on(async { init(&host_with(g.file.as_deref())) });
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    assert!(
        on,
        "the file the documentation points operators at is accepted"
    );
    assert!(enabled());
    // Outside `block_on`: `shutdown` drives the batch flush with its own `block_on`, which refuses
    // to run from inside a runtime's context.
    drop(rt);
    shutdown();
    reset();
}

// ---------------------------------------------------------------- the seam

#[test]
fn the_seam_round_trips_a_traceparent() {
    let tp = TraceParent::parse("00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01")
        .expect("a valid parent");
    assert_eq!(
        tp.to_header(),
        "00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01"
    );
    assert_eq!(tp.to_string(), tp.to_header(), "Display is the field value");
    assert_eq!(TraceParent::parse(&tp.to_header()), Some(tp), "round trip");
    // The sampled bit is the only flag this side reads; the rest of the field is ignored.
    assert_eq!(
        TraceParent::parse("00-0102030405060708090a0b0c0d0e0f10-1122334455667788-09"),
        Some(tp),
        "the flags other than the sampled bit carry no meaning here"
    );
    assert!(tp
        .span_context()
        .is_some_and(|c| c.is_sampled() && c.is_valid()));
}

#[test]
fn the_seam_carries_nothing_that_is_not_a_sampled_span() {
    // An unsampled parent says nothing, so it is dropped rather than carried (agreement §3).
    for bad in [
        "",
        "00",
        "00-0102030405060708090a0b0c0d0e0f10",
        "00-0102030405060708090a0b0c0d0e0f10-1122334455667788-00", // not sampled
        "00-00000000000000000000000000000000-1122334455667788-01", // all-zero trace id
        "00-0102030405060708090a0b0c0d0e0f10-0000000000000000-01", // all-zero span id
        "zz-0102030405060708090a0b0c0d0e0f10-1122334455667788-01", // not hex
        "00-0102030405060708090a0b0c0d0e0f-1122334455667788-01",   // short trace id
        "00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01-extra", // a later version
    ] {
        assert_eq!(TraceParent::parse(bad), None, "{bad:?} is not a parent");
    }
}

#[test]
fn the_seam_reads_a_carrier_with_the_w3c_propagator() {
    // The same parser on both sides, by construction: the SDK's propagator, not our own reader.
    let got = extract_from(|k| {
        (k == TRACEPARENT).then_some("00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01")
    });
    assert!(got.is_some(), "the carrier's traceparent is read");
    assert!(
        extract_from(|_| None).is_none(),
        "a carrier without the field carries no parent"
    );
    // A parent extracted from a carrier is a parent a span can be opened under.
    let tp = got.unwrap();
    assert!(extract(&tp).is_some_and(|cx| cx.has_active_span()));
}

// ---------------------------------------------------------------- off

/// The guarantee the agreement asks for, in its unit-test form: with the once-cell unset, the
/// helpers neither reach the SDK nor report anything. (`tests/otel_off_free.rs` is the same claim
/// measured in allocations, through the public API.)
#[test]
fn off_builds_nothing_and_reports_nothing() {
    with_off(|| {
        assert!(!enabled());
        assert!(!initialised());
        assert_eq!(export_errors(), 0);
        let mut s = span(span::VIEW_FLUSH, |a| {
            a.str(attr::VIEW_NAME, "v".to_owned());
            a.u64(attr::KEYS, 7);
            a.bool(attr::FORCED, true);
            a.link(
                &TraceParent::parse("00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01")
                    .unwrap(),
            );
        });
        assert!(!s.sampled(), "a disabled handle records nothing");
        assert!(s.trace_parent().is_none(), "and stamps nothing");
        s.ok();
        s.fail("flush");
        s.set_str(attr::SOURCE_POSITION, "mysql-bin.000003:4");
        s.set_u64(attr::ROWS, 9);
        drop(s);
        assert!(!enabled());
        assert_eq!(
            export_errors(),
            0,
            "nothing was exported, so nothing failed"
        );
        shutdown(); // the exit path of a process that never turned telemetry on
        assert!(!enabled());
    });
}

// ---------------------------------------------------------------- on

/// What a collector is sent: the agreed names, and only values a backend may group on.
#[test]
fn on_spans_carry_the_agreed_names_and_values() {
    let cap = Capture::default();
    let parent = TraceParent::parse("00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01")
        .expect("a valid parent");
    let other = TraceParent::parse("00-ffffffffffffffffffffffffffffffff-0102030405060708-01")
        .expect("a valid parent");
    let stamped = with_telemetry(&host(), cap.clone(), || {
        assert!(enabled() && initialised());
        let mut flush = span(span::VIEW_FLUSH, |a| {
            a.u64(attr::EPOCH, 7);
            a.links(&[parent, other]);
        });
        assert!(
            flush.sampled(),
            "a root span is sampled by the default sampler"
        );
        let stamp = flush
            .trace_parent()
            .expect("a sampled span stamps a parent");
        {
            let mut refresh = span(span::VIEW_REFRESH, |a| {
                a.u64(attr::VIEW_ID, 42);
                a.str(attr::VIEW_NAME, "orders_by_day");
                a.str(attr::MODE, "incremental");
                a.u64(attr::KEYS, 3);
                a.u64(attr::TARGETS, 2);
                a.bool(attr::FORCED, false);
                a.links(&[parent]);
                a.parent(&flush);
            });
            refresh.set_u64(attr::ROWS, 11);
            refresh.ok();
        }
        {
            let mut write = span(span::TARGET_WRITE_BATCH, |a| {
                a.str(attr::TARGET_NAME, "analytics");
                a.parent(&flush);
            });
            write.fail("timeout");
        }
        flush.ok();
        // The read-batch and transaction spans of the copy-set reader, and the position save.
        drop(span(span::BINLOG_READ_BATCH, |a| {
            a.str(attr::SOURCE_POSITION, "mysql-bin.000003:4");
            a.str(attr::SOURCE_FLAVOR, "mysql");
            a.parent_from(&stamp);
        }));
        drop(span(span::POSITION_PERSIST, |a| {
            a.str(attr::SOURCE_POSITION, "mysql-bin.000003:4");
            a.parent_from(&stamp);
        }));
        drop(span(span::EPOCH_TAKEOVER, |_| {}));
        stamp
    });

    let flush = cap.one(span::VIEW_FLUSH);
    assert_eq!(int_attr(&flush, attr::EPOCH), Some(7));
    assert_eq!(
        flush.links.links.len(),
        2,
        "the merge of two transactions is two links"
    );
    assert_eq!(
        flush.links.links[0].span_context.trace_id(),
        parent.span_context().unwrap().trace_id(),
        "a link points at the source transaction's trace, not at its parent"
    );
    assert_eq!(
        flush.status,
        Status::Unset,
        "an outcome is an attribute, not an error"
    );
    assert_eq!(
        str_attr(&flush, attr::OUTCOME).as_deref(),
        Some(outcome::SUCCESS)
    );
    assert!(flush.span_context.is_sampled());

    let refresh = cap.one(span::VIEW_REFRESH);
    assert_eq!(
        refresh.parent_span_id,
        flush.span_context.span_id(),
        "under the flush"
    );
    assert_eq!(int_attr(&refresh, attr::VIEW_ID), Some(42));
    assert_eq!(
        str_attr(&refresh, attr::VIEW_NAME).as_deref(),
        Some("orders_by_day")
    );
    assert_eq!(
        str_attr(&refresh, attr::MODE).as_deref(),
        Some("incremental")
    );
    assert_eq!(int_attr(&refresh, attr::KEYS), Some(3));
    assert_eq!(int_attr(&refresh, attr::ROWS), Some(11));
    assert_eq!(bool_attr(&refresh, attr::FORCED), Some(false));
    assert_eq!(refresh.links.links.len(), 1);
    assert_eq!(
        str_attr(&refresh, attr::OUTCOME).as_deref(),
        Some(outcome::SUCCESS)
    );

    let write = cap.one(span::TARGET_WRITE_BATCH);
    assert_eq!(write.parent_span_id, flush.span_context.span_id());
    assert_eq!(
        str_attr(&write, attr::TARGET_NAME).as_deref(),
        Some("analytics")
    );
    assert_eq!(
        str_attr(&write, attr::OUTCOME).as_deref(),
        Some(outcome::ERROR)
    );
    assert!(
        matches!(write.status, Status::Error { .. }),
        "a failure is an error status"
    );

    let read = cap.one(span::BINLOG_READ_BATCH);
    assert_eq!(
        read.parent_span_id,
        stamped.span_context().unwrap().span_id(),
        "the parent the flush stamped"
    );
    assert_eq!(
        str_attr(&read, attr::SOURCE_FLAVOR).as_deref(),
        Some("mysql")
    );
    assert_eq!(
        cap.one(span::POSITION_PERSIST).parent_span_id,
        stamped.span_context().unwrap().span_id()
    );
    assert_eq!(
        int_attr(&cap.one(span::EPOCH_TAKEOVER), attr::EPOCH),
        None,
        "a span with no attributes is still a span"
    );
    // Every span says who produced it (the resource is asserted by `the_resource_names_the_producer`).
    for s in cap.taken() {
        assert_eq!(s.instrumentation_scope.name(), SCOPE);
        for kv in &s.attributes {
            assert!(
                !kv.value.as_str().to_lowercase().contains("select "),
                "span {} carries what looks like a statement: {}",
                s.name,
                kv.value
            );
        }
    }
}

/// The unsampled path is the one a low `trace_id_ratio_based` ratio puts the hot path on: nothing
/// is stamped, so nothing downstream pays for a parent (agreement §3). The sampler is what the file
/// said — no flat `OTEL_TRACES_SAMPLER` is consulted, which is what the second half checks.
#[test]
fn an_unsampled_span_stamps_nothing() {
    let cap = Capture::default();
    let plan = Plan {
        traces: Some(config::Traces {
            sampler: Sampler::AlwaysOff,
            processors: Vec::new(),
            limits: config::Limits::default(),
        }),
        ..Plan::default()
    };
    // The flat name says the opposite of the file: whatever the plan says is what runs, so the
    // span below must still be unsampled. (`with_plan` holds the module's test lock; taking it
    // here as well would be a self-deadlock, and the name is one nothing here reads anyway.)
    std::env::set_var("OTEL_TRACES_SAMPLER", "always_on");
    with_plan(
        &host(),
        &plan,
        vec![(
            cap.clone(),
            Processor::Simple {
                exporter: Exporter::Console,
            },
        )],
        || {
            let mut s = span(span::BINLOG_TRANSACTION, |a| {
                a.str(attr::SOURCE_POSITION, "mysql-bin.000003:4");
            });
            assert!(!s.sampled(), "the sampler the file chose took it out");
            assert!(s.trace_parent().is_none(), "so there is nothing to stamp");
            s.ok();
        },
    );
    std::env::remove_var("OTEL_TRACES_SAMPLER");
    assert!(cap.taken().is_empty(), "an unsampled span is not exported");
}

/// The `batch` the file names is the processor that runs: spans wait for their turn instead of
/// going out one by one, and [`flush`] — the exit path's door — is what empties the queue, so a
/// process that stops normally loses nothing the file asked to be sent.
#[test]
fn a_batch_processor_holds_its_spans_until_the_process_flushes() {
    let cap = Capture::default();
    let plan = Plan {
        traces: Some(config::Traces {
            sampler: Sampler::AlwaysOn,
            processors: Vec::new(),
            limits: config::Limits::default(),
        }),
        ..Plan::default()
    };
    with_plan(
        &host(),
        &plan,
        vec![(
            cap.clone(),
            Processor::Batch {
                exporter: Exporter::Console,
                batch: config::Batch {
                    schedule_delay: config::BATCH_SCHEDULE_DELAY,
                    export_timeout: config::BATCH_EXPORT_TIMEOUT,
                    max_queue_size: config::BATCH_MAX_QUEUE_SIZE,
                    max_export_batch_size: config::BATCH_MAX_EXPORT_BATCH_SIZE,
                },
            },
        )],
        || {
            {
                let mut s = span(span::VIEW_FLUSH, |a| {
                    a.u64(attr::EPOCH, 7);
                });
                s.ok();
            } // `end` is Drop, and an ended span is what the batch has to hold.
            assert!(
                cap.taken().is_empty(),
                "a batch processor is not a simple one: nothing has been exported yet"
            );
            flush();
            assert_eq!(
                cap.taken()
                    .iter()
                    .filter(|s| s.name.as_ref() == span::VIEW_FLUSH)
                    .count(),
                1,
                "the flush the exit path makes is what sends the batch"
            );
        },
    );
}

/// The resource is what a collector groups by: the host's defaults underneath, the file's
/// attributes on top, and nothing else — no `EnvResourceDetector`, no `OTEL_RESOURCE_ATTRIBUTES`,
/// no per-process instance id.
#[test]
fn the_resource_names_the_producer_and_the_file_wins() {
    let plan = Plan {
        resource: vec![
            ("service.name".to_string(), "named-by-the-file".to_string()),
            (
                "deployment.environment.name".to_string(),
                "prod".to_string(),
            ),
        ],
        ..Plan::default()
    };
    let r = resource(
        &Host {
            config_file: None,
            service_name: "lazyview",
            service_version: "1.2.3",
            source: Some("src-1"),
        },
        &plan,
    );
    assert_eq!(
        res(&r, SERVICE_NAME).as_deref(),
        Some("named-by-the-file"),
        "the file outranks the host's default name"
    );
    assert_eq!(
        res(&r, SERVICE_VERSION).as_deref(),
        Some("1.2.3"),
        "the version the host decided is still ours"
    );
    assert_eq!(
        res(&r, "deployment.environment.name").as_deref(),
        Some("prod")
    );
    assert_eq!(res(&r, attr::SOURCE_SERVER).as_deref(), Some("src-1"));
    assert!(
        res(&r, "telemetry.sdk.name").is_some(),
        "the SDK says who produced it"
    );
    assert_eq!(
        r.get(&opentelemetry::Key::from_static_str("service.instance.id")),
        None,
        "no per-process identity: a pod's identity is the file's `resource`, not an ambient name"
    );
    // A host with no source server says nothing about one: the attribute appears when the engine has
    // a source to name, and not before.
    let quiet = resource(
        &Host {
            config_file: None,
            service_name: "lazyview",
            service_version: "1.2.3",
            source: None,
        },
        &Plan::default(),
    );
    assert!(
        res(&quiet, attr::SOURCE_SERVER).is_none(),
        "no source, no attribute"
    );

    // And the flat names the SDK used to read for itself change nothing: with the file in charge,
    // it is the only source of a resource attribute.
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("OTEL_SERVICE_NAME", "collector-side-name");
    std::env::set_var(
        "OTEL_RESOURCE_ATTRIBUTES",
        "deployment.environment.name=from-the-environment",
    );
    let r = resource(&host(), &Plan::default());
    std::env::remove_var("OTEL_SERVICE_NAME");
    std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
    assert_eq!(
        res(&r, SERVICE_NAME).as_deref(),
        Some("lazyview-test"),
        "the host's name stands: no flat variable overrides it"
    );
    assert_eq!(
        res(&r, "deployment.environment.name"),
        None,
        "OTEL_RESOURCE_ATTRIBUTES is not consulted"
    );
}

/// A collector that is not there is a rate-limited WARN and a counter — never an error the pipeline
/// sees, and never a log line per batch.
#[test]
fn a_dead_collector_changes_nothing_about_the_run() {
    let dead = Unreachable;
    with_telemetry(&host(), dead, || {
        for i in 0..50 {
            let mut s = span(span::VIEW_FLUSH, |a| a.u64(attr::EPOCH, i));
            s.ok();
        }
        // The run goes on while the failures pile up: a span after them behaves like any other.
        assert!(
            span(span::VIEW_REFRESH, |_| {}).sampled(),
            "a failing exporter never takes the pipeline down"
        );
        shutdown(); // flushes the queue, so every export attempt has been made
        assert!(
            export_errors() > 0,
            "the failed exports are counted (the WARN they carry is rate-limited)"
        );
    });
}

/// The exact sequence the copy-set reader performs (`src/binlog/stream.rs`), and what the engine
/// does with what it was handed. Written as the reader writes it — plain `span` calls, the parent
/// carried out as a `traceparent` value — because that is the contract the vendored copy depends
/// on: nobody outside the reader needs to know a span was involved (agreement §2).
#[test]
fn the_reader_stamps_and_the_engine_parents_and_links_what_it_was_handed() {
    let cap = Capture::default();
    let stamped = with_telemetry(&host(), cap.clone(), || {
        // — the batch's first event (`StreamAdapter::otel_read`) —
        let mut batch = span(span::BINLOG_READ_BATCH, |a| {
            a.str(attr::SOURCE_POSITION, "mysql-bin.000003:4");
            a.str(attr::SOURCE_FLAVOR, "mysql");
        });
        // — the transaction inside it, parented on the batch —
        let batch_parent = batch.trace_parent();
        let mut tx = span(span::BINLOG_TRANSACTION, |a| {
            a.str(attr::SOURCE_POSITION, "mysql-bin.000003:4");
            if let Some(p) = &batch_parent {
                a.parent_from(p);
            }
        });
        // — the seam: the stamp taken at read time, handed to the consumer as an opaque value —
        let stamped = tx
            .trace_parent()
            .expect("a sampled transaction stamps a parent");
        assert!(stamped.span_context().is_some_and(|sc| sc.is_sampled()));
        tx.set_u64(attr::ROWS, 2);
        tx.ok();
        drop(tx); // `otel_end_tx`: dropping the handle is what ends the span
        batch.ok();
        drop(batch); // `end_read_batch`
        stamped
    });
    // — what `flush_draining` does with the link the engine kept beside the rows —
    with_telemetry(&host(), cap.clone(), || {
        let mut flush = span(span::VIEW_FLUSH, |a| {
            a.u64(attr::EPOCH, 3);
            a.links(&[stamped]);
        });
        flush.ok();
    });

    let tx = cap.one(span::BINLOG_TRANSACTION);
    assert_eq!(
        str_attr(&tx, attr::SOURCE_POSITION).as_deref(),
        Some("mysql-bin.000003:4")
    );
    assert_eq!(int_attr(&tx, attr::ROWS), Some(2));
    let read = cap.one(span::BINLOG_READ_BATCH);
    assert_eq!(
        tx.parent_span_id,
        read.span_context.span_id(),
        "the transaction sits under the batch that read it"
    );
    let flush = cap.one(span::VIEW_FLUSH);
    assert_eq!(flush.links.links.len(), 1, "the merge is a link");
    assert_eq!(
        flush.links.links[0].span_context.span_id(),
        tx.span_context.span_id(),
        "and it points at the transaction, not at what contained it"
    );
    assert_ne!(
        flush.parent_span_id,
        tx.span_context.span_id(),
        "…and is NOT its parent: the transaction outlives this flush"
    );
}

/// A transaction the engine's core delivered has no reader to stamp it: the engine recorded it
/// under the batch-wide key (`OTEL_CORE`) and the flush still links it.
#[test]
fn a_core_delivered_transaction_links_through_the_bucket() {
    let cap = Capture::default();
    let orphan = with_telemetry(&host(), cap.clone(), || {
        span(span::BINLOG_TRANSACTION, |_| {})
            .trace_parent()
            .expect("sampled")
    });
    with_telemetry(&host(), cap.clone(), || {
        let mut flush = span(span::VIEW_FLUSH, |a| {
            a.links(&[orphan]);
        });
        flush.ok();
    });
    let flush = cap.one(span::VIEW_FLUSH);
    assert_eq!(flush.links.links.len(), 1);
    assert_eq!(
        flush.links.links[0].span_context.trace_id(),
        orphan
            .span_context()
            .expect("the bucket keeps the trace id")
            .trace_id(),
        "the link still points at the source transaction's trace"
    );
}
