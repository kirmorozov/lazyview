//! The reader's own tests: what a file can say, and that every thing it cannot say is a `WARN` and
//! off rather than an error or a panic.
//!
//! The shape of every test here is the same — a piece of JSON, and the answer [`read_text`] gives
//! for it — because that is the whole risk this module carries: an operator writes a file, and the
//! worker either builds from it or starts without telemetry. Nothing in between.

use super::*;

/// The smallest file that turns telemetry on: a model version and a processor to export through.
const MINIMAL: &str = r#"{"file_format":"1.2","tracer_provider":{"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#;

/// A file wrapped in the one key a test is not interested in.
fn tracer(inner: &str) -> String {
    format!(r#"{{"file_format":"1.2","tracer_provider":{inner}}}"#)
}

fn on(text: &str) -> Plan {
    match read_text(text) {
        Reading::On(plan) => *plan,
        Reading::Off { reason, warn } => panic!("expected on, got off ({reason}, warn={warn})"),
    }
}

/// The off answer, with the two things that make it useful: why, and whether it is worth saying.
fn off(text: &str) -> (String, bool) {
    match read_text(text) {
        Reading::Off { reason, warn } => (reason, warn),
        Reading::On(plan) => panic!("expected off, got on: {plan:?}"),
    }
}

/// The gRPC exporter's fields, as the file named them.
fn grpc(plan: &Plan) -> OtlpGrpc {
    match &plan.traces.as_ref().expect("traces").processors[0] {
        Processor::Batch {
            exporter: Exporter::OtlpGrpc(e),
            ..
        }
        | Processor::Simple {
            exporter: Exporter::OtlpGrpc(e),
        } => e.clone(),
        other => panic!("expected an otlp_grpc exporter, got {other:?}"),
    }
}

fn batch(plan: &Plan) -> Batch {
    match &plan.traces.as_ref().expect("traces").processors[0] {
        Processor::Batch { batch, .. } => batch.clone(),
        other => panic!("expected a batch processor, got {other:?}"),
    }
}

// ------------------------------------------------------------------ what turns it on

#[test]
fn a_minimal_file_names_one_exporter_and_nothing_else() {
    let p = on(MINIMAL);
    assert_eq!(p.traces.as_ref().unwrap().processors.len(), 1);
    assert!(matches!(
        p.traces.as_ref().unwrap().processors[0],
        Processor::Simple {
            exporter: Exporter::Console
        }
    ));
    assert_eq!(p.traces.as_ref().unwrap().sampler, Sampler::AlwaysOn);
    assert_eq!(p.traces.as_ref().unwrap().limits, Limits::default());
    // The seam the embedder depends on is always propagated, whatever the file said (§3).
    assert_eq!(p.propagators, vec!["tracecontext".to_owned()]);
    assert!(
        p.resource.is_empty(),
        "the host's defaults carry the resource"
    );
    assert!(
        p.warnings.is_empty(),
        "a good file has nothing to complain about: {p:?}"
    );
}

/// The file the repository points operators at must be the shape this reader accepts, silently: it
/// is the golden fixture, so a change that makes it warn or refuse shows up here first.
#[test]
fn the_starting_file_the_repository_ships_reads_cleanly() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/k8s/base/otel-config.json");
    std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    match read_file(&path) {
        Reading::On(p) => {
            let t = p.traces.as_ref().expect("traces");
            assert_eq!(t.processors.len(), 1, "one batch processor");
            assert_eq!(
                t.limits,
                Limits {
                    attribute_count: Some(128),
                    event_count: Some(128),
                    link_count: Some(128),
                    ..Limits::default()
                }
            );
            assert_eq!(p.propagators, vec!["tracecontext", "baggage"]);
            let e = grpc(&p);
            assert_eq!(
                e.endpoint, GRPC_ENDPOINT_DEFAULT,
                "the default, with nothing set"
            );
            assert_eq!(e.timeout, EXPORT_TIMEOUT);
            assert!(e.headers.is_empty());
            assert!(e.compression.is_none());
            assert!(
                p.warnings.is_empty(),
                "the sample file warns about nothing: {:?}",
                p.warnings
            );
            assert!(p.deferred.is_empty());
        }
        Reading::Off { reason, warn } => {
            panic!("the shipped file must be accepted: {reason} ({warn})")
        }
    }
}

/// A `tracer_provider` with no processors at all is the SDK's own default: batch, OTLP/gRPC, at the
/// specification's numbers.
#[test]
fn a_tracer_provider_with_no_processors_takes_the_default_batch_and_grpc() {
    let p = on(r#"{"file_format":"1.2","tracer_provider":{}}"#);
    let t = p.traces.as_ref().unwrap();
    assert!(matches!(t.processors[0], Processor::Batch { .. }));
    assert_eq!(
        batch(&p),
        Batch {
            schedule_delay: BATCH_SCHEDULE_DELAY,
            export_timeout: BATCH_EXPORT_TIMEOUT,
            max_queue_size: BATCH_MAX_QUEUE_SIZE,
            max_export_batch_size: BATCH_MAX_EXPORT_BATCH_SIZE,
        }
    );
    assert_eq!(grpc(&p).endpoint, GRPC_ENDPOINT_DEFAULT);
}

// ------------------------------------------------------------------ what keeps it off

/// Every way a file can fail is off, and none of them panics: the caller's start cannot fail here.
#[test]
fn every_refusal_is_off_with_a_reason_and_never_a_panic() {
    for (text, names) in [
        // The model version this build does not have.
        (
            r#"{"file_format":"2.0","tracer_provider":{}}"#,
            &["file_format"][..],
        ),
        (r#"{"file_format":"not-a-version"}"#, &["file_format"]),
        (r#"{"tracer_provider":{}}"#, &["file_format"]),
        // Not JSON at all: the four ways an operator leaves a YAML file's habits behind.
        (r#"file_format: "1.2""#, &["JSON"]),
        (r#"{"file_format": "1.2",}"#, &["JSON"]),
        ("{'file_format': '1.2'}", &["JSON"]),
        ("{file_format: 1.2}", &["JSON"]),
        (r#"{"file_format":"1.2"} trailing"#, &["JSON"]),
        // The transport and the TLS material this gRPC build does not carry.
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"exporter":{"otlp_http":{"endpoint":"http://c:4318"}}}}]}}"#,
            &["otlp_http"],
        ),
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"exporter":{"otlp":{"protocol":"http/protobuf"}}}}]}}"#,
            &["protocol"],
        ),
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"exporter":{"otlp_grpc":{"certificate":"/tls/ca.pem"}}}}]}}"#,
            &["certificate"],
        ),
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"exporter":{"otlp_grpc":{"client_key":"/tls/client.key"}}}}]}}"#,
            &["client_key"],
        ),
        // A processor that cannot do anything: no exporter, or no kind.
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{}}]}}"#,
            &["exporter"],
        ),
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"otlp_grpc":{}}]}}"#,
            &["processor"],
        ),
        // A required value of the wrong shape.
        (
            r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"max_queue_size":"not-a-number","exporter":{"console":{}}}}]}}"#,
            &["max_queue_size"],
        ),
        (
            r#"{"file_format":"1.2","tracer_provider":{"limits":{"attribute_count_limit":"soon"},"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#,
            &["attribute_count_limit"],
        ),
        // A section where an array belongs, and a document that is not an object at all.
        (r#"[{"file_format":"1.2"}]"#, &["object"]),
        (r#""file_format""#, &["object"]),
        // An empty file, and a file that says only `null`: both say nothing about telemetry.
        ("", &["empty", "nothing", "JSON"]),
        ("null", &["empty", "nothing"]),
    ] {
        let (reason, warn) = off(text);
        assert!(warn, "{text:?} is something an operator should hear about");
        assert!(
            names.iter().any(|n| reason.contains(n)),
            "{text:?} refused without naming {names:?}: {reason}",
        );
    }
}

/// `disabled: true` is the operator asking for off. It is off, and it is not something to complain
/// about: the one case whose `warn` is false.
#[test]
fn disabled_is_off_without_a_word() {
    for text in [
        r#"{"file_format":"1.2","disabled":true}"#,
        r#"{"file_format":"1.2","disabled":"yes","tracer_provider":{}}"#,
    ] {
        let (reason, warn) = off(text);
        assert!(!warn, "{text:?} is what the operator asked for: {reason}");
        assert!(reason.contains("disabled"), "{reason}");
    }
    // `disabled: false` is not off, and an unknown value there is a mistake worth one sentence.
    assert!(
        on(r#"{"file_format":"1.2","disabled":false,"tracer_provider":{}}"#)
            .traces
            .is_some()
    );
    let (reason, warn) = off(r#"{"file_format":"1.2","disabled":"maybe","tracer_provider":{}}"#);
    assert!(warn && reason.contains("disabled"), "{reason}");
    // `tracer_provider.enabled: false` means the same thing where an operator wrote it.
    let (reason, warn) = off(
        r#"{"file_format":"1.2","tracer_provider":{"enabled":false,"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#,
    );
    assert!(warn && reason.contains("enabled"), "{reason}");
}

/// The two sections the later phases will fill are recognised, reported once, and otherwise left
/// alone — a file that carries them is still on for traces.
#[test]
fn the_sections_the_later_phases_own_are_deferred_with_one_warn_each() {
    let p = on(
        r#"{"file_format":"1.2","meter_provider":{},"logger_provider":{},"tracer_provider":{"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#,
    );
    assert_eq!(p.deferred, vec!["meter_provider", "logger_provider"]);
    assert_eq!(
        p.warnings
            .iter()
            .filter(|w| w.contains("meter_provider"))
            .count(),
        1
    );
    assert_eq!(
        p.warnings
            .iter()
            .filter(|w| w.contains("logger_provider"))
            .count(),
        1
    );
    assert!(
        p.traces.is_some(),
        "metrics being early says nothing about traces"
    );
}

/// An unknown top-level key is warn-and-skip, not an error: a file written for a newer model still
/// gets the traces the operator configured.
#[test]
fn an_unknown_key_is_ignored_with_one_warn() {
    let p = on(
        r#"{"file_format":"1.2","instrumentation":{"jvm":{}},"tracer_provider":{"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#,
    );
    assert_eq!(
        p.warnings
            .iter()
            .filter(|w| w.contains("instrumentation"))
            .count(),
        1,
        "one sentence, not one per mention: {:?}",
        p.warnings
    );
    assert!(p.traces.is_some());
    // `serde_json` keeps one spelling of a duplicate key, so a key written twice is one note.
    let p = on(r#"{"file_format":"1.2","baggage_limit":1,"tracer_provider":{}}"#);
    assert_eq!(
        p.warnings
            .iter()
            .filter(|w| w.contains("baggage_limit"))
            .count(),
        1
    );
}

/// A sampler this build does not have costs the spans nothing: the SDK's own default takes over,
/// and says so once.
#[test]
fn an_unknown_sampler_falls_back_to_the_sdk_default_with_a_word() {
    let p = on(&tracer(
        r#"{"sampler":{"jmx":{}},"processors":[{"simple":{"exporter":{"console":{}}}}]}"#,
    ));
    assert_eq!(p.traces.unwrap().sampler, Sampler::AlwaysOn);
    assert_eq!(
        p.warnings.iter().filter(|w| w.contains("sampler")).count(),
        1
    );
}

// ------------------------------------------------------------------ the parts of the model

#[test]
fn the_samplers_the_model_has_all_read() {
    let cases: &[(&str, Sampler)] = &[
        (r#""always_on""#, Sampler::AlwaysOn),
        (r#""always_off""#, Sampler::AlwaysOff),
        (r#"{"always_on":{}}"#, Sampler::AlwaysOn),
        (
            r#"{"trace_id_ratio_based":{"ratio":0.25}}"#,
            Sampler::Ratio(0.25),
        ),
        // The flat name's `arg` spelling is the same thing.
        (
            r#"{"trace_id_ratio_based":{"arg":0.5}}"#,
            Sampler::Ratio(0.5),
        ),
        // `1` and `0` are whole numbers in JSON and fractions here.
        (
            r#"{"trace_id_ratio_based":{"ratio":1}}"#,
            Sampler::Ratio(1.0),
        ),
    ];
    for (json, want) in cases {
        let inner = r#"{"sampler":%S%,"processors":[{"simple":{"exporter":{"console":{}}}}]}"#
            .replace("%S%", json);
        let p = on(&tracer(&inner));
        assert_eq!(&p.traces.unwrap().sampler, want, "sampler: {json}");
    }
    // `parent_based`, with both spellings of the two remote branches.
    let p = on(&tracer(
        r#"{"sampler":{"parent_based":{"root":{"trace_id_ratio_based":{"ratio":0.1}},"remote_parent_parented":"always_on","remote_parent_unsampled":"always_off","local_parent_sampled":"always_on"}},"processors":[{"simple":{"exporter":{"console":{}}}}]}"#,
    ));
    let want = Sampler::ParentBased {
        root: Box::new(Sampler::Ratio(0.1)),
        remote_parent_sampled: Some(Box::new(Sampler::AlwaysOn)),
        remote_parent_not_sampled: Some(Box::new(Sampler::AlwaysOff)),
        local_parent_sampled: Some(Box::new(Sampler::AlwaysOn)),
        local_parent_not_sampled: None,
    };
    assert_eq!(p.traces.unwrap().sampler, want);

    // A ratio out of range, and a name this build does not have, are refused with the path in them.
    let (reason, _) = off(&tracer(
        r#"{"sampler":{"trace_id_ratio_based":{"ratio":4}},"processors":[{"simple":{"exporter":{"console":{}}}}]}"#,
    ));
    assert!(
        reason.contains("ratio") && reason.contains("0..=1"),
        "{reason}"
    );
    let (reason, _) = off(&tracer(
        r#"{"sampler":"trace_id_ratio_based","processors":[{"simple":{"exporter":{"console":{}}}}]}"#,
    ));
    assert!(
        reason.contains("ratio"),
        "{reason} must say how to spell it"
    );
}

#[test]
fn the_exporter_the_file_names_is_the_exporter_that_is_built() {
    let p = on(
        r#"{"file_format":"1.2","tracer_provider":{"processors":[{"batch":{"schedule_delay":"250ms","export_timeout":"1.5s","max_queue_size":4096,"max_export_batch_size":64,"exporter":{"otlp_grpc":{"endpoint":"collector:4317","compression":"gzip","timeout":"7s","headers":"authorization=Bearer%20tok,x-priority=high"}}}}]}}"#,
    );
    let b = batch(&p);
    assert_eq!(b.schedule_delay, Duration::from_millis(250));
    assert_eq!(b.export_timeout, Duration::from_millis(1500));
    assert_eq!(b.max_queue_size, 4096);
    assert_eq!(b.max_export_batch_size, 64);
    let e = grpc(&p);
    assert_eq!(e.endpoint, "https://collector:4317", "no scheme means TLS");
    assert_eq!(e.timeout, Duration::from_secs(7));
    assert!(matches!(e.compression, Some(Compression::Gzip)));
    assert_eq!(
        e.headers,
        vec![
            ("authorization".to_owned(), "Bearer%20tok".to_owned()),
            ("x-priority".to_owned(), "high".to_owned()),
        ],
        "names are lower-cased, values kept as written"
    );

    // `insecure: true` says "no TLS" the way gRPC means it: the scheme.
    let p = on(
        r#"{"file_format":"1.2","tracer_provider":{"processors":[{"simple":{"exporter":{"otlp_grpc":{"endpoint":"https://localhost:4317","insecure":true}}}}]}}"#,
    );
    assert_eq!(grpc(&p).endpoint, "http://localhost:4317");

    // An object of headers is the same list, a bare number is a timeout in seconds, and an
    // unsupported compression is dropped with a word.
    let p = on(
        r#"{"file_format":"1.2","tracer_provider":{"processors":[{"simple":{"exporter":{"otlp_grpc":{"endpoint":"http://127.0.0.1:4317","compression":"brotli","timeout":9,"headers":{"Authorization":"a b","X-Empty":""}}}}}]}}"#,
    );
    let e = grpc(&p);
    assert!(
        e.compression.is_none(),
        "an unsupported codec exports uncompressed"
    );
    assert_eq!(e.timeout, Duration::from_secs(9));
    assert_eq!(e.headers[0], ("authorization".to_owned(), "a b".to_owned()));
    assert!(p
        .warnings
        .iter()
        .any(|w| w.contains("compression") && w.contains("brotli")));
}

#[test]
fn the_span_limits_the_model_names_are_the_limits_the_sdk_gets() {
    let p = on(&tracer(
        r#"{"limits":{"attribute_count_limit":32,"event_count_limit":8,"link_count_limit":4,"event_attribute_count_limit":2,"link_attribute_count_limit":3},"processors":[{"simple":{"exporter":{"console":{}}}}]}"#,
    ));
    assert_eq!(
        p.traces.unwrap().limits,
        Limits {
            attribute_count: Some(32),
            attribute_value_length: None,
            event_count: Some(8),
            link_count: Some(4),
            event_attribute_count: Some(2),
            link_attribute_count: Some(3),
        }
    );
}

#[test]
fn resource_attributes_come_from_both_spellings_in_the_files_order() {
    std::env::set_var(
        "OTEL_RESOURCE_ATTRIBUTES",
        "service.instance.id=pod-7,deployment.environment.name=prod",
    );
    let p = on(
        r#"{"file_format":"1.2","resource":{"attributes":[{"name":"service.name","value":"staging-lazyview"},{"name":"deployment.environment.name","value":"from-attributes"}],"attributes_list":"${OTEL_RESOURCE_ATTRIBUTES}"},"tracer_provider":{}}"#,
    );
    std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
    // The later spelling of a key wins, and both forms reach the same list.
    assert_eq!(
        p.resource,
        vec![
            ("service.name".to_owned(), "staging-lazyview".to_owned()),
            ("deployment.environment.name".to_owned(), "prod".to_owned()),
            ("service.instance.id".to_owned(), "pod-7".to_owned()),
        ]
    );
    // A pair with no `=` is dropped with the one sentence that says which one.
    let p = on(
        r#"{"file_format":"1.2","resource":{"attributes_list":"good=1,nonsense"},"tracer_provider":{}}"#,
    );
    assert_eq!(p.resource, vec![("good".to_owned(), "1".to_owned())]);
    assert!(p.warnings.iter().any(|w| w.contains("nonsense")));
}

#[test]
fn propagators_are_the_names_the_build_carries_and_always_include_the_seam() {
    let p = on(r#"{"file_format":"1.2","propagator":["baggage","b3"],"tracer_provider":{}}"#);
    assert_eq!(p.propagators, vec!["baggage", "tracecontext"]);
    assert!(
        p.warnings.iter().any(|w| w.contains("b3")),
        "one word about the one it cannot carry"
    );
    assert!(
        p.warnings.iter().any(|w| w.contains("tracecontext")),
        "and one that the seam needs it: {:?}",
        p.warnings
    );
    // One name, a comma-separated string, and `composite` are all the same thing.
    for json in [
        r#""b3,baggage""#,
        r#"["baggage"]"#,
        r#"{"composite":["baggage","tracecontext"]}"#,
    ] {
        let p = on(&format!(
            r#"{{"file_format":"1.2","propagator":{json},"tracer_provider":{{}}}}"#
        ));
        assert!(p.propagators.iter().any(|n| n == "tracecontext"), "{json}");
        assert!(p.propagators.iter().any(|n| n == "baggage"), "{json}");
    }
    // Nothing the build carries at all is still W3C: the list is never left empty.
    let p = on(r#"{"file_format":"1.2","propagator":[],"tracer_provider":{}}"#);
    assert_eq!(p.propagators, vec!["tracecontext"]);
}

// ------------------------------------------------------------------ substitution

/// Substitution happens only inside the file, only in values, and only for the four forms the
/// specification gives it. The names it mentions are read from the environment; nothing else is,
/// and a *key* is never substituted — the environment must not be able to rename a section.
#[test]
fn values_substitute_and_keys_never_do() {
    std::env::set_var("LVS_OTEL_TEST_COLLECTOR", "https://otel.example:4317");
    std::env::remove_var("LVS_OTEL_TEST_ABSENT");
    let p = on(
        r#"{"file_format":"1.2","tracer_provider":{"processors":[{"simple":{"exporter":{"otlp_grpc":{"endpoint":"${LVS_OTEL_TEST_COLLECTOR}","headers":"authorization=${env:LVS_OTEL_TEST_COLLECTOR:-none},x-missing=${LVS_OTEL_TEST_ABSENT:-fallback},x-default=${env:LVS_OTEL_TEST_ABSENT:-none}","timeout":"${LVS_OTEL_TEST_ABSENT:-5s}"}}}}]}}"#,
    );
    std::env::remove_var("LVS_OTEL_TEST_COLLECTOR");
    let e = grpc(&p);
    assert_eq!(e.endpoint, "https://otel.example:4317");
    assert_eq!(
        e.headers[0].1, "https://otel.example:4317",
        "the `env:` form reads the same name the `${{}}` form does, and a value with `:` and `/` in \
         it is still one header value"
    );
    assert_eq!(
        e.headers[1],
        ("x-missing".to_owned(), "fallback".to_owned())
    );
    assert_eq!(
        e.headers[2],
        ("x-default".to_owned(), "none".to_owned()),
        "an unset name with a default takes the default"
    );
    assert_eq!(e.timeout, Duration::from_secs(5));

    // A key is not a value: the section is still `tracer_provider` whatever the environment says.
    std::env::set_var("LVS_OTEL_TEST_KEY", "meter_provider");
    let p = on(
        r#"{"file_format":"1.2","${LVS_OTEL_TEST_KEY}":{},"tracer_provider":{"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#,
    );
    std::env::remove_var("LVS_OTEL_TEST_KEY");
    assert!(
        p.traces.is_some(),
        "the environment did not move the section"
    );
    assert!(
        p.warnings
            .iter()
            .any(|w| w.contains("${LVS_OTEL_TEST_KEY}")),
        "and it said so: {:?}",
        p.warnings
    );
}

/// A substituted value is still one string: a `:` or a newline inside what the environment said is
/// data, not syntax, and `$$` is a literal dollar sign rather than a reference.
#[test]
fn a_substituted_value_stays_one_string() {
    std::env::set_var("LVS_OTEL_TEST_ODD", "a: b\nc: d\n$$not-a-reference");
    let p = on(
        r#"{"file_format":"1.2","resource":{"attributes":[{"name":"odd","value":"${LVS_OTEL_TEST_ODD}"}]},"tracer_provider":{}}"#,
    );
    std::env::remove_var("LVS_OTEL_TEST_ODD");
    assert_eq!(
        p.resource,
        vec![("odd".to_owned(), "a: b\nc: d\n$$not-a-reference".to_owned())],
        "the value is exactly what the environment said — newlines, `:` and `$$` and all: what the          environment said is data, and only the file's own `$$` is an escape"
    );
    // An unclosed reference is left alone and said, rather than swallowing the rest of the value.
    std::env::remove_var("LVS_OTEL_TEST_ABSENT");
    let p = on(
        r#"{"file_format":"1.2","resource":{"attributes":[{"name":"u","value":"${UNCLOSED"}]},"tracer_provider":{}}"#,
    );
    assert_eq!(p.resource[0].1, "${UNCLOSED");
    assert!(p.warnings.iter().any(|w| w.contains("unclosed")));
}

// ------------------------------------------------------------------ the gate and the environment

#[test]
fn the_gate_reads_only_the_one_variable_and_never_fails() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/k8s/base");
    let file = dir.join("otel-config.json");

    for v in [None, Some(""), Some("   "), Some("\t\n")] {
        let g = gate(CONFIG_FILE_ENV, v);
        assert_eq!(g.file, None, "{v:?} is off");
        assert_eq!(g.warning, None, "{v:?} says nothing");
    }
    let g = gate(CONFIG_FILE_ENV, file.to_str());
    assert_eq!(g.file.as_deref(), Some(file.as_path()));
    assert_eq!(g.warning, None);

    // A directory, a missing path and an unreadable file are off with the one sentence naming both
    // the variable and the path; none of them is an error.
    for v in [dir.to_str().unwrap(), "/definitely/not/mounted/otel.json"] {
        let g = gate(CONFIG_FILE_ENV, Some(v));
        assert_eq!(g.file, None, "{v} is not a file");
        let w = g.warning.unwrap_or_else(|| panic!("{v} must say why"));
        assert!(w.contains(CONFIG_FILE_ENV) && w.contains(v), "{w}");
    }
    // Whitespace around a real path is the same path.
    let g = gate(CONFIG_FILE_ENV, Some(&format!("  {}  ", file.display())));
    assert_eq!(g.file.as_deref(), Some(file.as_path()));
}

/// Reading a file that cannot be read is the same shape as a file that says nothing: off, with the
/// reason, and never an error the caller has to handle.
#[test]
fn a_file_that_cannot_be_read_is_off_with_the_reason() {
    let missing = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/otel/nope.json");
    match read_file(&missing) {
        Reading::Off { reason, warn } => {
            assert!(warn);
            assert!(reason.contains(&missing.display().to_string()), "{reason}");
        }
        Reading::On(_) => panic!("a file that is not there cannot be on"),
    }
    // The shipped file, read through the same door, is on.
    assert!(matches!(
        read_file(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("deploy/k8s/base/otel-config.json")
        ),
        Reading::On(_)
    ));
}

/// With the file in charge, no flat `OTEL_*` name is consulted: neither the batch knobs the SDK's
/// own builder seeds from the environment, nor the exporter's endpoint. The values below are what
/// the file and this module's own constants say, while the environment says something else.
#[test]
fn no_flat_otel_variable_reaches_the_plan() {
    std::env::set_var("OTEL_BSP_MAX_QUEUE_SIZE", "3");
    std::env::set_var("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "3");
    std::env::set_var("OTEL_BSP_SCHEDULE_DELAY", "1");
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://from-the-environment:4317",
    );
    std::env::set_var("OTEL_TRACES_SAMPLER", "always_off");
    let p = on(r#"{"file_format":"1.2","tracer_provider":{}}"#);
    assert_eq!(batch(&p).max_queue_size, BATCH_MAX_QUEUE_SIZE);
    assert_eq!(batch(&p).max_export_batch_size, BATCH_MAX_EXPORT_BATCH_SIZE);
    assert_eq!(batch(&p).schedule_delay, BATCH_SCHEDULE_DELAY);
    assert_eq!(grpc(&p).endpoint, GRPC_ENDPOINT_DEFAULT);
    assert_eq!(p.traces.unwrap().sampler, Sampler::AlwaysOn);
    std::env::remove_var("OTEL_BSP_MAX_QUEUE_SIZE");
    std::env::remove_var("OTEL_BSP_MAX_EXPORT_BATCH_SIZE");
    std::env::remove_var("OTEL_BSP_SCHEDULE_DELAY");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_TRACES_SAMPLER");
}

// ------------------------------------------------------------------ the copy set

/// The copy set is what lazy-wire-fire mirrors, so the two rules that keep it mirror-able are
/// asserted rather than promised: this module reads no host-environment variable of its own, and
/// it carries no name of ours anywhere in it — the switch and everything in it is the standard's.
#[test]
fn the_copy_set_reads_no_host_environment_and_carries_no_name_of_ours() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    // The names must be built rather than written, or this test would be the thing it forbids.
    let ours = [
        format!("LAZYVIEW_{}", "OTEL"),
        format!("LAZYVIEW_{}", "INSTANCE_ID"),
        String::from("instance_id"),
    ];
    for rel in ["src/otel.rs", "src/otel/config.rs"] {
        let text = std::fs::read_to_string(root.join(rel)).expect("the module is there");
        for name in &ours {
            assert!(
                !text.contains(name.as_str()),
                "{rel} mentions {name}: telemetry's configuration comes from the standard file \
                 only, so no name of ours belongs in the copy set"
            );
        }
        // `config.rs` is where the file's own `${NAME}` references are resolved, and that is the
        // one reading this module is allowed; the module above it takes everything as a parameter.
        if rel.ends_with("otel.rs") {
            assert!(
                !text.contains("std::env::var("),
                "{rel} reads the process environment: the host passes the gate's answer in"
            );
        }
    }
    // And no telemetry type from the SDK or from `tracing` is re-exported, which is what keeps the
    // dependency out of every module that only calls `span`.
    let facade = std::fs::read_to_string(root.join("src/otel.rs")).expect("the facade is there");
    for line in facade.lines() {
        let l = line.trim();
        assert!(
            !(l.starts_with("pub use opentelemetry") || l.starts_with("pub use tracing")),
            "the facade leaks a dependency: {l}"
        );
    }
}
