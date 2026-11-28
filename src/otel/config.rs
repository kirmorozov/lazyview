//! The declarative-config file: what `OTEL_CONFIG_FILE` points at, and what a worker builds from it.
//!
//! One file is the whole of the telemetry configuration. The standard's Declarative Configuration
//! model decides its shape — `resource`, `propagator`, `tracer_provider`, and the two sections the
//! later phases own — and its JSON encoding is what we read, with the official `serde_json` the
//! workspace already depends on: no YAML crate, no bespoke parser, and no name of ours anywhere in
//! the surface. `OTEL_CONFIG_FILE` unset is off; a path to a readable, valid file is on; anything
//! else is off with the one `WARN` that says why ([`gate`], [`read_file`], [`read_text`]).
//!
//! Three rules hold this together, and the tests in `config/tests.rs` are the evidence for each:
//!
//! 1. **Nothing here can fail a start.** Every problem — a missing file, malformed JSON, a model
//!    version this build does not have, an exporter this build cannot carry, a TLS key — is
//!    [`Reading::Off`] with a sentence, never an error type the caller has to match and never a
//!    panic. Telemetry is a visitor in this process: a refused visit is reported, and the run goes
//!    on without it.
//! 2. **The file is the only config source.** No `OTEL_*` variable other than `OTEL_CONFIG_FILE` is
//!    read by anything in this module or in `crate::otel`: the SDK's own env-seeded builders are
//!    worked around by naming every one of their fields ([`Batch`], [`build_exporter`]'s callers),
//!    which is why `OTEL_BSP_MAX_QUEUE_SIZE` in the environment changes nothing. The names a file
//!    *references* inside `${…}` are read from the environment, and only those.
//! 3. **Warn and skip, never silently.** A key this build does not have is ignored with one note in
//!    [`Plan::warnings`], deduplicated, so a file written for a newer model still gets the operator
//!    the traces they configured, and the log says precisely what was left behind.
//!
//! JSON, not YAML, is lv-super's ruling (2026-11-22): no YAML crate and no hand-rolled YAML parser,
//! and the file is read as JSON with the `serde_json` this crate already depends on. JSON is a strict
//! subset of YAML 1.2, so anything the model needs
//! to say is sayable, and taking the whole YAML language (anchors, aliases, tags, multi-document
//! streams, tabs) would mean a dependency for syntax this configuration never uses.

use opentelemetry_otlp::Compression;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The standard declarative-configuration variable (the specification's name;
/// `OTEL_EXPERIMENTAL_CONFIG_FILE` is deprecated in its favour).
pub const CONFIG_FILE_ENV: &str = "OTEL_CONFIG_FILE";

/// The SDK's own batch defaults, spelled out rather than taken from `BatchConfig::default()` — that
/// builder seeds itself from the flat `OTEL_BSP_*` names, and with the file set no other `OTEL_*`
/// variable is consulted. A test pins that `OTEL_BSP_MAX_QUEUE_SIZE` in the environment changes
/// nothing.
pub const BATCH_MAX_QUEUE_SIZE: usize = 2_048;
/// The specification's default delay between batch exports.
pub const BATCH_SCHEDULE_DELAY: Duration = Duration::from_secs(5);
/// The specification's default export timeout.
pub const BATCH_EXPORT_TIMEOUT: Duration = Duration::from_secs(30);
/// The specification's default number of spans in one export.
pub const BATCH_MAX_EXPORT_BATCH_SIZE: usize = 512;
/// The exporter's default timeout — what the flat `OTEL_EXPORTER_OTLP_TIMEOUT` would have said.
pub const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);
/// Where gRPC export goes when the file does not say (the SDK's own gRPC default).
pub const GRPC_ENDPOINT_DEFAULT: &str = "http://localhost:4317";

/// What the model's `tracer_provider` and the two later sections are called at the top level.
const SECTIONS: [&str; 5] = [
    "resource",
    "propagator",
    "tracer_provider",
    "meter_provider",
    "logger_provider",
];

// ---------------------------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------------------------

/// What an operator's value for `OTEL_CONFIG_FILE` means. The host reads the variable — the copy set
/// reads no host env of its own — and asks [`gate`] what follows from the value: either a file to
/// build from, or off.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Gate {
    /// The file to build from: only ever an existing, readable, regular file.
    pub file: Option<PathBuf>,
    /// The one `WARN` naming the variable and the path, for a value that named something else (a
    /// missing path, a directory, an unreadable file). `None` when the variable is simply unset:
    /// unset is the documented default, not a mistake, and an ambient collector must stay silent.
    pub warning: Option<String>,
}

impl Gate {
    /// `true` when the value named a file to build from: the host stores that file and only that
    /// file, so `false` here is what makes an off start free (`init` is handed `None`).
    pub fn is_on(&self) -> bool {
        self.file.is_some()
    }
}

/// Resolves a value read from `OTEL_CONFIG_FILE` (or, for another host's own variable, whatever name
/// `name` says). Unset, empty or whitespace-only is off; an existing readable regular file is on, and
/// that file carries everything; anything else is off with [`Gate::warning`] and never a startup
/// failure — telemetry must not be able to stop a pipeline.
pub fn gate(name: &str, raw: Option<&str>) -> Gate {
    let Some(v) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Gate::default();
    };
    let path = PathBuf::from(v);
    match std::fs::File::open(&path) {
        // Opening is the readability test and `is_file` rules out a directory or a device: a config
        // file is a file, and finding that out here is what keeps a half-built provider out.
        Ok(f) if f.metadata().is_ok_and(|m| m.is_file()) => Gate {
            file: Some(path),
            warning: None,
        },
        Ok(_) => Gate {
            file: None,
            warning: Some(format!(
                "{name}={} is not a regular file; telemetry stays off",
                path.display()
            )),
        },
        Err(e) => Gate {
            file: None,
            warning: Some(format!(
                "{name}={} cannot be read ({e}); telemetry stays off",
                path.display()
            )),
        },
    }
}

// ---------------------------------------------------------------------------------------------
// The plan a file yields
// ---------------------------------------------------------------------------------------------

/// What a file said, every `${…}` already substituted. [`crate::otel::init`] builds the SDK from
/// this and nothing else. Phases 2 and 3 add their section to it.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    /// `resource.attributes` and `resource.attributes_list`, in the file's order, later spellings
    /// of a key winning over earlier ones. The host's defaults go *under* what the file says.
    pub resource: Vec<(String, String)>,
    /// The propagators the file named, lower-cased, unknown ones already dropped.
    pub propagators: Vec<String>,
    /// `None` when no processor survived: the file asked only for something this build cannot carry.
    pub traces: Option<Traces>,
    /// Sections recognised and left to a later phase — one `WARN` each.
    pub deferred: Vec<&'static str>,
    /// What warn-and-skip found, in the file's order, deduplicated. `init` says each one once.
    pub warnings: Vec<String>,
}

/// `tracer_provider`, reduced to what the SDK is built with.
#[derive(Debug, Clone)]
pub struct Traces {
    pub sampler: Sampler,
    pub processors: Vec<Processor>,
    pub limits: Limits,
}

/// One `sampler` entry.
#[derive(Debug, Clone, PartialEq)]
pub enum Sampler {
    AlwaysOn,
    AlwaysOff,
    /// `trace_id_ratio_based{ratio}`; `arg` is accepted as the alias the flat name uses.
    Ratio(f64),
    /// `parent_based{root, remote_parent_sampled, remote_parent_not_sampled,
    /// local_parent_sampled, local_parent_not_sampled}`, mapped to the SDK's parent-based sampler.
    /// The `_parented` and `_unsampled` spellings of the two remote branches are aliases.
    ParentBased {
        root: Box<Sampler>,
        remote_parent_sampled: Option<Box<Sampler>>,
        remote_parent_not_sampled: Option<Box<Sampler>>,
        local_parent_sampled: Option<Box<Sampler>>,
        local_parent_not_sampled: Option<Box<Sampler>>,
    },
}

/// One entry of `tracer_provider.processors[]`.
#[derive(Debug, Clone)]
pub enum Processor {
    Batch { exporter: Exporter, batch: Batch },
    Simple { exporter: Exporter },
}

/// The four knobs `batch` may set. Every one is always handed to the SDK's own builder — including
/// the ones the file leaves out, so that no flat `OTEL_BSP_*` value can reach the processor behind
/// the file's back.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Batch {
    pub schedule_delay: Duration,
    pub export_timeout: Duration,
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
}

/// One exporter. gRPC only: a second transport would mean a second HTTP stack for one signal.
#[derive(Debug, Clone)]
pub enum Exporter {
    OtlpGrpc(OtlpGrpc),
    Console,
}

/// `otlp_grpc` (and `otlp` with `protocol: grpc`).
#[derive(Debug, Clone)]
pub struct OtlpGrpc {
    pub endpoint: String,
    /// `k1=v1,k2=v2`, the form the model and the flat variable both use; a pair without `=` is
    /// dropped with a `WARN`.
    pub headers: Vec<(String, String)>,
    pub compression: Option<Compression>,
    pub timeout: Duration,
}

/// `tracer_provider.limits`, honoured as the SDK's span limits.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Limits {
    pub attribute_count: Option<u32>,
    pub attribute_value_length: Option<u32>,
    pub event_count: Option<u32>,
    pub link_count: Option<u32>,
    pub link_attribute_count: Option<u32>,
    pub event_attribute_count: Option<u32>,
}

/// Reading one file yields a plan to build from, or the reason telemetry stays off. `warn` is false
/// only where the operator asked for off, which is not something to complain about.
#[derive(Debug, Clone)]
pub enum Reading {
    On(Box<Plan>),
    Off { reason: String, warn: bool },
}

impl Reading {
    fn off(reason: impl Into<String>) -> Reading {
        Reading::Off {
            reason: reason.into(),
            warn: true,
        }
    }
}

/// Reads the text of a file. Every problem is [`Reading::Off`] with the reason to `WARN`, and no
/// input panics: malformed JSON, a stray trailing document and an absurdly nested file all come
/// back as the parser's own error, which is off. `serde_json`'s own recursion limit is what stops a
/// hand-written file from costing the process anything.
pub fn read_text(text: &str) -> Reading {
    let mut cfg = Cfg::default();
    let root = match serde_json::from_str::<Value>(text) {
        Ok(v) => v,
        Err(e) => return Reading::off(format!("the file is not valid JSON: {e}")),
    };
    if root.is_null() {
        return Reading::off("the file is empty, so it says nothing about telemetry");
    }
    let mut plan = match read_root(&root, &mut cfg) {
        Ok(Some(p)) => p,
        Ok(None) => {
            return Reading::Off {
                // The operator asked for this, in the file they pointed us at: nothing went wrong, so
                // it is not something to complain about in the log.
                reason: "the file says telemetry is off (`disabled: true`)".to_string(),
                warn: false,
            };
        }
        Err(reason) => return Reading::off(reason),
    };
    if plan.traces.is_none() && plan.deferred.is_empty() {
        return Reading::off("the file asks for no trace processor telemetry can use");
    }
    plan.warnings = cfg.take();
    Reading::On(Box::new(plan))
}

/// Reads the file at `path`. An unreadable file is off with the reason, like every other problem.
pub fn read_file(path: &Path) -> Reading {
    match std::fs::read_to_string(path) {
        Ok(text) => read_text(&text),
        Err(e) => Reading::off(format!("{} cannot be read ({e})", path.display())),
    }
}

// ---------------------------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------------------------

/// The warn-and-skip notes. Deduplicated, because `init` says each one once per run and a repeated
/// unknown key must not become a log loop.
#[derive(Debug, Default)]
struct Cfg {
    notes: Vec<String>,
}

impl Cfg {
    fn warn(&mut self, note: String) {
        if !self.notes.contains(&note) {
            self.notes.push(note);
        }
    }

    fn take(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notes)
    }
}

/// One JSON object as an ordered-by-name list of pairs, which is the shape every `want_*` below
/// reads. `serde_json`'s map is sorted, so the notes this module emits come out in a stable order.
fn pairs(y: &Value) -> Option<Vec<(String, &Value)>> {
    y.as_object().map(|m| {
        m.iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<Vec<(String, &Value)>>()
    })
}

fn mapping<'a>(y: &'a Value, path: &str) -> Result<Vec<(String, &'a Value)>, String> {
    pairs(y).ok_or_else(|| format!("{path} must be a JSON object"))
}

fn seq<'a>(y: &'a Value, path: &str) -> Result<&'a Vec<Value>, String> {
    y.as_array()
        .ok_or_else(|| format!("{path} must be a JSON array"))
}

fn field<'a>(m: &'a [(String, &Value)], key: &str) -> Option<&'a Value> {
    m.iter().find(|(k, _)| k == key).map(|(_, v)| *v)
}

fn present(m: &[(String, &Value)], key: &str) -> bool {
    matches!(field(m, key), Some(v) if !v.is_null())
}

/// A scalar as text. Numbers and booleans are accepted where a string is expected — an operator who
/// writes `"timeout": 10` means it — and objects and arrays are not text.
fn text(y: &Value) -> Option<String> {
    match y {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// `${NAME}`, `${env:NAME}` and `${NAME:-default}` in one string, with `$$` for a literal `$`.
/// An unset name with no default is empty, which is what the model's own substitution does.
fn substitute(text: &str, cfg: &mut Cfg) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'$' {
            // Copy one character at a time so a multi-byte character is never split.
            let len = 1 + text[i..]
                .chars()
                .skip(1)
                .take_while(|c| !c.is_ascii())
                .count();
            out.push_str(&text[i..i + len.min(text.len() - i)]);
            i += len.min(text.len() - i);
            continue;
        }
        if bytes.get(i + 1) == Some(&b'$') {
            out.push('$');
            i += 2;
            continue;
        }
        if bytes.get(i + 1) != Some(&b'{') {
            out.push('$');
            i += 1;
            continue;
        }
        // `${` opens a reference; an unterminated one is copied as written rather than guessed at.
        match text[i + 2..].find('}') {
            Some(end) => {
                let body = &text[i + 2..i + 2 + end];
                out.push_str(&reference(body, cfg, &text[i..i + end + 3]));
                i += end + 3;
            }
            None => {
                cfg.warn(format!(
                    "`{}` has an unclosed `${{` and was left exactly as written",
                    &text[i..]
                ));
                out.push_str(&text[i..]);
                break;
            }
        }
    }
    out
}

/// The inside of one `${…}`: a name, optionally `env:`-prefixed, optionally `:-`-defaulted.
fn reference(body: &str, cfg: &mut Cfg, whole: &str) -> String {
    let (name, default) = match body.split_once(":-") {
        Some((n, d)) => (n, Some(d)),
        None => (body, None),
    };
    let name = name.strip_prefix("env:").unwrap_or(name).trim();
    if name.is_empty() {
        cfg.warn(format!(
            "`{whole}` names no variable and expanded to nothing"
        ));
        return String::new();
    }
    match std::env::var(name) {
        Ok(v) => v,
        Err(_) => default.unwrap_or_default().to_owned(),
    }
}

/// Substitution over the whole document, values only: a *key* is what the model calls a thing, and
/// substituting there would let the environment rename a section.
fn expand(y: &Value, cfg: &mut Cfg) -> Value {
    match y {
        Value::String(s) => Value::String(substitute(s, cfg)),
        Value::Array(v) => Value::Array(v.iter().map(|e| expand(e, cfg)).collect()),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, cfg)))
                .collect::<serde_json::Map<String, Value>>(),
        ),
        other => other.clone(),
    }
}

/// The whole file. `Ok(None)` is off by the operator's own choice (`disabled: true`).
fn read_root(root: &Value, cfg: &mut Cfg) -> Result<Option<Plan>, String> {
    // `file_format` is required and must be a `1.x` model version. Reading a model this build does
    // not have as if it did would silently drop what the operator configured, so it is off.
    let top_raw = mapping(root, "the file")?;
    let format = field(&top_raw, "file_format")
        .and_then(text)
        .ok_or_else(|| "file_format is required and must be a `1.x` version".to_owned())?;
    let version: f64 = format
        .trim()
        .parse()
        .map_err(|_| format!("file_format={format:?} is not a `1.x` version"))?;
    if !(1.0..2.0).contains(&version) {
        return Err(format!(
            "file_format={format:?} is not supported (this build reads the 1.x model)"
        ));
    }
    if want_bool(&top_raw, "disabled", "disabled")?.unwrap_or(false) {
        return Ok(None);
    }
    // From here the document is the operator's, with every `${…}` resolved.
    let doc = expand(root, cfg);
    let top = mapping(&doc, "the file")?;
    let mut plan = Plan::default();

    if present(&top, "resource") {
        plan.resource = read_resource(field(&top, "resource").unwrap(), cfg)?;
    }
    if present(&top, "propagator") {
        plan.propagators = read_propagator(field(&top, "propagator").unwrap(), cfg)?;
    }
    // Whatever the file named — or did not name — the seam's own propagator is in the list: a file
    // that says nothing gets `tracecontext` alone, so the plan never carries an empty set.
    if !plan.propagators.iter().any(|n| n == "tracecontext") {
        plan.propagators.push("tracecontext".to_owned());
    }
    if present(&top, "tracer_provider") {
        plan.traces = Some(read_tracer_provider(
            field(&top, "tracer_provider").unwrap(),
            cfg,
        )?);
    }
    for section in ["meter_provider", "logger_provider"] {
        if present(&top, section) {
            if !plan.deferred.contains(&section) {
                plan.deferred.push(section);
            }
            cfg.warn(format!(
                "`{section}` is not supported in this version and was ignored"
            ));
        }
    }
    for (k, _) in &top {
        if k != "file_format" && !SECTIONS.contains(&k.as_str()) {
            cfg.warn(format!(
                "`{k}` is not part of the model this build reads and was ignored"
            ));
        }
    }
    Ok(Some(plan))
}

/// `resource.attributes: [{name, value}]` and `resource.attributes_list: "k=v,k=v"`.
fn read_resource(y: &Value, cfg: &mut Cfg) -> Result<Vec<(String, String)>, String> {
    let m = mapping(y, "resource")?;
    let mut out: Vec<(String, String)> = Vec::new();
    if let Some(list) = field(&m, "attributes").filter(|y| !y.is_null()) {
        for (i, e) in seq(list, "resource.attributes")?.iter().enumerate() {
            let path = format!("resource.attributes[{i}]");
            let em = mapping(e, &path)?;
            let name = want_str(&em, "name", &format!("{path}.name"))?.unwrap_or_default();
            let value = want_str(&em, "value", &format!("{path}.value"))?.unwrap_or_default();
            put_attribute(&mut out, name, value, cfg);
        }
    }
    // The recommended starting file reaches the standard resource attributes this way
    // (`attributes_list: ${OTEL_RESOURCE_ATTRIBUTES}`), which is how an operator carries
    // `service.name` and `service.instance.id` without any bespoke name of ours.
    if present(&m, "attributes_list") {
        let s = want_str(&m, "attributes_list", "resource.attributes_list")?.unwrap_or_default();
        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            match pair.split_once('=') {
                Some((k, v)) => {
                    put_attribute(&mut out, k.trim().to_owned(), v.trim().to_owned(), cfg)
                }
                None => cfg.warn(format!(
                    "resource.attributes_list entry `{pair}` has no `=` and was ignored"
                )),
            }
        }
    }
    Ok(out)
}

fn put_attribute(out: &mut Vec<(String, String)>, name: String, value: String, cfg: &mut Cfg) {
    if name.is_empty() {
        cfg.warn("a resource attribute with an empty name was ignored".to_owned());
        return;
    }
    match out.iter_mut().find(|(k, _)| *k == name) {
        Some(slot) => slot.1 = value,
        None => out.push((name, value)),
    }
}

/// `propagator`: an array of names, one name, or `{composite: [...]}`.
fn read_propagator(y: &Value, cfg: &mut Cfg) -> Result<Vec<String>, String> {
    let named: Vec<String> =
        if let Some(v) = y.as_array() {
            let mut out = Vec::with_capacity(v.len());
            for e in v {
                out.push(text(e).ok_or_else(|| "propagator entries must be names".to_owned())?);
            }
            out
        } else if let Some(s) = text(y) {
            s.split(',').map(|p| p.trim().to_owned()).collect()
        } else if let Some(m) = pairs(y) {
            match field(&m, "composite") {
                Some(c) if !c.is_null() => {
                    let mut out = Vec::new();
                    for e in seq(c, "propagator.composite")? {
                        out.push(text(e).ok_or_else(|| {
                            "propagator.composite entries must be names".to_owned()
                        })?);
                    }
                    out
                }
                _ => {
                    let keys: Vec<String> = m.into_iter().map(|(k, _)| k).collect();
                    return Err(format!(
                    "propagator must be an array of names or a `composite` object (it names {})",
                    keys.join(", ")
                ));
                }
            }
        } else {
            return Err("propagator must be an array of names or a `composite` object".to_owned());
        };
    let mut out = Vec::new();
    for n in named {
        let n = n.trim().to_ascii_lowercase();
        if n.is_empty() {
            continue;
        }
        if !matches!(n.as_str(), "tracecontext" | "baggage") {
            cfg.warn(format!(
                "propagator `{n}` is not supported (tracecontext, baggage) and was skipped"
            ));
            continue;
        }
        if !out.contains(&n) {
            out.push(n);
        }
    }
    // The shared-core seam *is* W3C `traceparent` (agreement §3): lazy-wire-fire reads the stamp this
    // module writes at read time, so a file that left tracecontext out is a mistake, not a choice.
    if !out.iter().any(|n| n == "tracecontext") {
        cfg.warn(
            "the file does not propagate `tracecontext`; it is kept because the shared-core seam \
             needs it (agreement §3)"
                .to_owned(),
        );
        out.push("tracecontext".to_owned());
    }
    Ok(out)
}

fn read_tracer_provider(y: &Value, cfg: &mut Cfg) -> Result<Traces, String> {
    let m = mapping(y, "tracer_provider")?;
    // `enabled` is not a key of the 1.x model — `disabled` at the top level is the switch — but an
    // operator who writes it means it, so `false` is honoured as off rather than ignored.
    if !want_bool(&m, "enabled", "tracer_provider.enabled")?.unwrap_or(true) {
        return Err("tracer_provider.enabled is false; traces stay off".to_owned());
    }
    let sampler = match field(&m, "sampler").filter(|y| !y.is_null()) {
        // A sampler this build does not have is not a reason to lose the pipeline's spans: that
        // one warns and takes the SDK's own default (inside `read_sampler`). A sampler it does have
        // and cannot read — a `ratio` that is missing or is not a fraction — is a required thing of
        // the wrong shape, and the file is refused: silently sampling everything when the operator
        // wrote 5% is a cost and privacy incident, not a fallback.
        Some(s) => read_sampler(s, "tracer_provider.sampler", cfg)?,
        None => Sampler::AlwaysOn,
    };
    let limits = match field(&m, "limits").filter(|y| !y.is_null()) {
        Some(l) => read_limits(l)?,
        None => Limits::default(),
    };
    let mut processors = Vec::new();
    match field(&m, "processors").filter(|y| !y.is_null()) {
        Some(p) => {
            let listed = seq(p, "tracer_provider.processors")?;
            for (i, e) in listed.iter().enumerate() {
                let path = format!("tracer_provider.processors[{i}]");
                match read_processor(e, &path, cfg) {
                    Ok(Some(p)) => processors.push(p),
                    Ok(None) => {}
                    Err(required) => return Err(required),
                }
            }
            // A file that named processors and had none this build can use is a mistake, not a
            // preference: falling back to the default exporter would send the operator's spans to a
            // collector they never chose, so the file is refused and traces stay off with the reason.
            if !listed.is_empty() && processors.is_empty() {
                return Err(format!(
                    "tracer_provider.processors names {} processor(s) and this build can use none                      of them, so there is nothing to export through",
                    listed.len()
                ));
            }
        }
        // A `tracer_provider` with no processors at all is the SDK's own default: batch, OTLP/gRPC.
        None => processors.push(Processor::Batch {
            exporter: Exporter::OtlpGrpc(OtlpGrpc {
                endpoint: GRPC_ENDPOINT_DEFAULT.to_owned(),
                headers: Vec::new(),
                compression: None,
                timeout: EXPORT_TIMEOUT,
            }),
            batch: Batch {
                schedule_delay: BATCH_SCHEDULE_DELAY,
                export_timeout: BATCH_EXPORT_TIMEOUT,
                max_queue_size: BATCH_MAX_QUEUE_SIZE,
                max_export_batch_size: BATCH_MAX_EXPORT_BATCH_SIZE,
            },
        }),
    }
    Ok(Traces {
        sampler,
        processors,
        limits,
    })
}

/// One `{"<kind>": {…}}` processor entry. `Err` is a required thing that is missing; `Ok(None)` a
/// component this build skips with a `WARN`.
fn read_processor(y: &Value, path: &str, cfg: &mut Cfg) -> Result<Option<Processor>, String> {
    let m = mapping(y, path)?;
    let (kind, body) = one_entry(&m, path, "processor")?;
    match kind {
        "batch" => {
            let bm = mapping(body, &format!("{path}.batch"))?;
            let exporter = match field(&bm, "exporter").filter(|y| !y.is_null()) {
                Some(e) => read_exporter(e, &format!("{path}.batch.exporter"), cfg)?,
                None => {
                    return Err(format!(
                        "{path}.batch has no exporter, so there is nothing to export to"
                    ))
                }
            };
            Ok(Some(Processor::Batch {
                exporter,
                batch: Batch {
                    schedule_delay: want_duration(
                        &bm,
                        "schedule_delay",
                        &format!("{path}.batch.schedule_delay"),
                    )?
                    .unwrap_or(BATCH_SCHEDULE_DELAY),
                    export_timeout: want_duration(
                        &bm,
                        "export_timeout",
                        &format!("{path}.batch.export_timeout"),
                    )?
                    .unwrap_or(BATCH_EXPORT_TIMEOUT),
                    max_queue_size: want_usize(
                        &bm,
                        "max_queue_size",
                        &format!("{path}.batch.max_queue_size"),
                    )?
                    .unwrap_or(BATCH_MAX_QUEUE_SIZE),
                    max_export_batch_size: want_usize(
                        &bm,
                        "max_export_batch_size",
                        &format!("{path}.batch.max_export_batch_size"),
                    )?
                    .unwrap_or(BATCH_MAX_EXPORT_BATCH_SIZE),
                },
            }))
        }
        "simple" => {
            let sm = mapping(body, &format!("{path}.simple"))?;
            let exporter = match field(&sm, "exporter").filter(|y| !y.is_null()) {
                Some(e) => read_exporter(e, &format!("{path}.simple.exporter"), cfg)?,
                None => {
                    return Err(format!(
                        "{path}.simple has no exporter, so there is nothing to export to"
                    ))
                }
            };
            Ok(Some(Processor::Simple { exporter }))
        }
        other => {
            cfg.warn(format!(
                "{path} names the processor `{other}`, which this build does not have, and was \
                 skipped"
            ));
            Ok(None)
        }
    }
}

/// `{"otlp_grpc": {…}}`, `{"console": {…}}`, or `{"otlp": {"protocol": "grpc", …}}`. Anything HTTP
/// is refused: gRPC is the transport this build carries, and the HTTP one would mean taking a second
/// HTTP stack on for one signal (the PR body records the tree delta).
fn read_exporter(y: &Value, path: &str, cfg: &mut Cfg) -> Result<Exporter, String> {
    let m = mapping(y, path)?;
    let (kind, body) = one_entry(&m, path, "exporter")?;
    match kind {
        "console" => Ok(Exporter::Console),
        "otlp_http" | "otlp_http/json" => Err(format!(
            "{path} asks for `{kind}`; only `otlp_grpc` is supported (gRPC is the transport this \
             build carries) and traces stay off"
        )),
        "otlp" | "otlp_grpc" => {
            let here = format!("{path}.{kind}");
            let bm = mapping(body, &here)?;
            let protocol = want_str(&bm, "protocol", &format!("{here}.protocol"))?
                .map(|p| p.trim().to_ascii_lowercase());
            if let Some(p) = protocol.filter(|p| p != "grpc") {
                return Err(format!(
                    "{here}.protocol={p} is not supported; only `otlp_grpc` is and traces stay off"
                ));
            }
            for key in ["certificate", "client_key", "client_certificate"] {
                if present(&bm, key) {
                    return Err(format!(
                        "{here}.{key} is a TLS option this gRPC build does not carry; terminate \
                         TLS at the collector, or export to a local endpoint with `insecure: true`"
                    ));
                }
            }
            let endpoint = want_str(&bm, "endpoint", &format!("{here}.endpoint"))?
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| GRPC_ENDPOINT_DEFAULT.to_owned());
            let insecure =
                want_bool(&bm, "insecure", &format!("{here}.insecure"))?.unwrap_or(false);
            // gRPC says "no TLS" by the endpoint's scheme, which is what `insecure` means.
            let endpoint = if insecure {
                match endpoint.strip_prefix("https://") {
                    Some(rest) => format!("http://{rest}"),
                    None if endpoint.contains("://") => endpoint,
                    None => format!("http://{endpoint}"),
                }
            } else if endpoint.contains("://") {
                endpoint
            } else {
                format!("https://{endpoint}")
            };
            let compression = match want_str(&bm, "compression", &format!("{here}.compression"))?
                .map(|c| c.trim().to_ascii_lowercase())
            {
                None => None,
                Some(c) if c.is_empty() || c == "none" => None,
                Some(c) if c == "gzip" => Some(Compression::Gzip),
                Some(c) if c == "zstd" => Some(Compression::Zstd),
                Some(c) => {
                    cfg.warn(format!(
                        "{here}.compression={c} is not supported (none, gzip, zstd); exports go \
                         uncompressed"
                    ));
                    None
                }
            };
            Ok(Exporter::OtlpGrpc(OtlpGrpc {
                endpoint,
                headers: headers(&bm, &here, cfg)?,
                compression,
                timeout: want_duration(&bm, "timeout", &format!("{here}.timeout"))?
                    .unwrap_or(EXPORT_TIMEOUT),
            }))
        }
        other => Err(format!(
            "{path} names the exporter `{other}`, which this build does not have (otlp_grpc, \
             console)"
        )),
    }
}

/// The one-key `{"kind": body}` shape the model uses for samplers, processors and exporters.
fn one_entry<'a>(
    m: &'a [(String, &Value)],
    path: &str,
    what: &str,
) -> Result<(&'a str, &'a Value), String> {
    match m.len() {
        0 => Err(format!("{path} is empty, so it names no {what}")),
        1 => Ok((m[0].0.as_str(), m[0].1)),
        _ => Err(format!(
            "{path} must name exactly one {what} (it names {})",
            m.iter()
                .map(|(k, _)| k.as_str())
                .collect::<Vec<&str>>()
                .join(", ")
        )),
    }
}

fn headers(
    bm: &[(String, &Value)],
    here: &str,
    cfg: &mut Cfg,
) -> Result<Vec<(String, String)>, String> {
    let h = match field(bm, "headers") {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(h) => h,
    };
    let mut out: Vec<(String, String)> = Vec::new();
    let mut add = |k: String, v: String, cfg: &mut Cfg| {
        let k = k.trim().to_ascii_lowercase();
        if k.is_empty() {
            cfg.warn(format!("{here}.headers has an empty name and was ignored"));
            return;
        }
        match out.iter_mut().find(|(ek, _)| *ek == k) {
            Some(slot) => slot.1 = v,
            None => out.push((k, v)),
        }
    };
    match h {
        // The flat form the model and the flat variable both use.
        Value::String(_) | Value::Number(_) => {
            let s = text(h).unwrap_or_default();
            for pair in s.split(',') {
                let pair = pair.trim();
                if pair.is_empty() {
                    continue;
                }
                match pair.split_once('=') {
                    Some((k, v)) => add(k.to_owned(), v.trim().to_owned(), cfg),
                    None => cfg.warn(format!(
                        "{here}.headers entry `{pair}` has no `=` and was ignored"
                    )),
                }
            }
        }
        Value::Object(_) => {
            for (k, v) in mapping(h, &format!("{here}.headers"))? {
                add(k, text(v).unwrap_or_default(), cfg);
            }
        }
        _ => return Err(format!("{here}.headers must be `k=v,k=v` or an object")),
    }
    Ok(out)
}

fn read_sampler(y: &Value, path: &str, cfg: &mut Cfg) -> Result<Sampler, String> {
    if let Some(s) = text(y) {
        // The shorthand an operator reaches for from the flat `OTEL_TRACES_SAMPLER` names.
        return match s.trim().to_ascii_lowercase().as_str() {
            "always_on" => Ok(Sampler::AlwaysOn),
            "always_off" => Ok(Sampler::AlwaysOff),
            "trace_id_ratio_based" | "traceid_ratio" => Err(format!(
                "{path}={s} needs its ratio: `\"sampler\": {{\"trace_id_ratio_based\": {{\"ratio\": 0.05}}}}`"
            )),
            other => {
                cfg.warn(format!(
                    "{path} names the sampler `{other}`, which this build does not have; the SDK                      default sampler (always_on) is used"
                ));
                Ok(Sampler::AlwaysOn)
            }
        };
    }
    let m = mapping(y, path)?;
    let (kind, body) = one_entry(&m, path, "sampler")?;
    match kind {
        "always_on" => Ok(Sampler::AlwaysOn),
        "always_off" => Ok(Sampler::AlwaysOff),
        "trace_id_ratio_based" | "traceid_ratio" => {
            let here = format!("{path}.{kind}");
            let bm = mapping(body, &here)?;
            let ratio = want_f64(&bm, "ratio", &format!("{here}.ratio"))?.or(want_f64(
                &bm,
                "arg",
                &format!("{here}.arg"),
            )?);
            match ratio {
                Some(r) if (0.0..=1.0).contains(&r) => Ok(Sampler::Ratio(r)),
                Some(r) => Err(format!("{here}.ratio={r} is not a fraction of 0..=1")),
                None => Err(format!("{here} needs a `ratio`")),
            }
        }
        "parent_based" => {
            let here = format!("{path}.parent_based");
            let bm = mapping(body, &here)?;
            let branch = |key: &str, cfg: &mut Cfg| -> Result<Option<Box<Sampler>>, String> {
                match field(&bm, key).filter(|y| !y.is_null()) {
                    None => Ok(None),
                    Some(y) => Ok(Some(Box::new(read_sampler(
                        y,
                        &format!("{here}.{key}"),
                        cfg,
                    )?))),
                }
            };
            // The two spellings the model and the flat argument use for the same branches.
            let remote_sampled =
                branch("remote_parent_sampled", cfg)?.or(branch("remote_parent_parented", cfg)?);
            let remote_unsampled = branch("remote_parent_not_sampled", cfg)?
                .or(branch("remote_parent_unsampled", cfg)?);
            Ok(Sampler::ParentBased {
                root: match field(&bm, "root").filter(|y| !y.is_null()) {
                    Some(r) => Box::new(read_sampler(r, &format!("{here}.root"), cfg)?),
                    None => Box::new(Sampler::AlwaysOn),
                },
                remote_parent_sampled: remote_sampled,
                remote_parent_not_sampled: remote_unsampled,
                local_parent_sampled: branch("local_parent_sampled", cfg)?,
                local_parent_not_sampled: branch("local_parent_not_sampled", cfg)?,
            })
        }
        other => {
            cfg.warn(format!(
                "{path} names the sampler `{other}`, which this build does not have; the SDK                  default sampler (always_on) is used"
            ));
            Ok(Sampler::AlwaysOn)
        }
    }
}

fn read_limits(y: &Value) -> Result<Limits, String> {
    let m = mapping(y, "tracer_provider.limits")?;
    let here = "tracer_provider.limits";
    Ok(Limits {
        attribute_count: want_u32(
            &m,
            "attribute_count_limit",
            &format!("{here}.attribute_count_limit"),
        )?,
        attribute_value_length: want_u32(
            &m,
            "attribute_value_length_limit",
            &format!("{here}.attribute_value_length_limit"),
        )?,
        event_count: want_u32(
            &m,
            "event_count_limit",
            &format!("{here}.event_count_limit"),
        )?,
        link_count: want_u32(&m, "link_count_limit", &format!("{here}.link_count_limit"))?,
        event_attribute_count: want_u32(
            &m,
            "event_attribute_count_limit",
            &format!("{here}.event_attribute_count_limit"),
        )?,
        link_attribute_count: want_u32(
            &m,
            "link_attribute_count_limit",
            &format!("{here}.link_attribute_count_limit"),
        )?,
    })
}

// ---------------------------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------------------------

fn want_str(m: &[(String, &Value)], key: &str, path: &str) -> Result<Option<String>, String> {
    match field(m, key) {
        None | Some(Value::Null) => Ok(None),
        Some(y) => text(y)
            .map(Some)
            .ok_or_else(|| format!("{path} must be a string")),
    }
}

fn want_bool(m: &[(String, &Value)], key: &str, path: &str) -> Result<Option<bool>, String> {
    match field(m, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        // The strings a JSON writer reaches for when it has no booleans, and the ones YAML 1.1 read
        // as true and false; the intent is the same either way.
        Some(y) => match text(y).map(|s| s.trim().to_ascii_lowercase()) {
            Some(s) if matches!(s.as_str(), "true" | "yes" | "on" | "1") => Ok(Some(true)),
            Some(s) if matches!(s.as_str(), "false" | "no" | "off" | "0") => Ok(Some(false)),
            _ => Err(format!("{path} must be true or false")),
        },
    }
}

fn want_u32(m: &[(String, &Value)], key: &str, path: &str) -> Result<Option<u32>, String> {
    want_usize(m, key, path).map(|v| v.map(|v| v.min(u32::MAX as usize) as u32))
}

fn want_usize(m: &[(String, &Value)], key: &str, path: &str) -> Result<Option<usize>, String> {
    match field(m, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => match n.as_u64() {
            Some(v) => Ok(Some(v as usize)),
            None => Err(format!("{path} must be a whole number")),
        },
        Some(y) => match text(y).and_then(|s| s.trim().parse::<usize>().ok()) {
            Some(v) => Ok(Some(v)),
            None => Err(format!("{path} must be a whole number")),
        },
    }
}

fn want_f64(m: &[(String, &Value)], key: &str, path: &str) -> Result<Option<f64>, String> {
    match field(m, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| format!("{path} must be a number")),
        Some(y) => match text(y).and_then(|s| s.trim().parse::<f64>().ok()) {
            Some(v) => Ok(Some(v)),
            None => Err(format!("{path} must be a number")),
        },
    }
}

/// A duration: the model's `"5s"` / `"200ms"` / `"1.5s"` forms, or a bare number of seconds.
fn want_duration(
    m: &[(String, &Value)],
    key: &str,
    path: &str,
) -> Result<Option<Duration>, String> {
    match field(m, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => match n.as_u64() {
            Some(v) => Ok(Some(Duration::from_secs(v))),
            None => Err(format!("{path} must be a duration like `\"5s\"`")),
        },
        Some(y) => {
            let s = text(y).ok_or_else(|| format!("{path} must be a duration like `\"5s\"`"))?;
            parse_duration(&s)
                .map(Some)
                .ok_or_else(|| format!("{path}={s:?} is not a duration (a number of seconds, or a value with ns, us, ms, s, m, h)"))
        }
    }
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let split = s
        .char_indices()
        .find(|(_, c)| !(c.is_ascii_digit() || *c == '.'))
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let (num, unit) = (s[..split].trim(), s[split..].trim());
    let n: f64 = num
        .parse::<f64>()
        .ok()
        .filter(|v: &f64| v.is_finite() && *v >= 0.0)?;
    let scale: f64 = match unit {
        "" | "s" => 1.0,
        "ns" => 1e-9,
        "us" | "µs" => 1e-6,
        "ms" => 1e-3,
        "m" | "min" => 60.0,
        "h" => 3_600.0,
        _ => return None,
    };
    let secs = n * scale;
    if !secs.is_finite() || secs > 31_536_000.0 {
        return None;
    }
    Some(Duration::from_secs_f64(secs))
}

#[cfg(test)]
mod tests;
