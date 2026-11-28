//! Opt-in OpenTelemetry: the declarative-config reader, the init + conventions module, and the W3C
//! seam.
//!
//! **Copy set.** `src/otel.rs` and `src/otel/**` are copied verbatim by lazy-wire-fire (its
//! `vendor/lazyview.lock` takes the glob at the re-sync that first carries them), so the rules of
//! the copy set hold here: no `crate::` import of anything outside the copy set, and no
//! lazyview-only state read — what the host knows (the resolved config file, the service name and
//! version, the source's identity) arrives as parameters of [`init`], which is how lazywf feeds its
//! own values too. The environment is read in exactly two places, both the specification's rather
//! than lazyview's: the host reads the standard `OTEL_CONFIG_FILE` name itself and passes what
//! [`gate`] makes of it, and [`config`] substitutes `${…}` references inside that file. No
//! `LAZYVIEW_*`, and no flat `OTEL_*` switch, sampler or exporter name is read here (`agreement` §2
//! as amended, `tmp/out/lwf-coordination/otel-agreement.md`).
//!
//! # One switch, and why "off" is provable
//!
//! `OTEL_CONFIG_FILE` — the standard declarative-configuration variable — is the switch *and* the
//! config source: unset is off, a path that is not an existing readable regular file is off with one
//! `WARN`, and the file that is there says everything (lv-super ruling, 2026-11-22, verified in the
//! orchestration store: "why would we need explicit params if OTEL_CONFIG_FILE can have everything we
//! need for otel"). [`config`] is the
//! reader; there is no bespoke telemetry name any more. Off — every one of those three cases — no
//! provider and no exporter exist and the once-cell is unset, so every span helper below returns a
//! disabled handle **before it builds an attribute**, and the instrumented hot paths cost one relaxed
//! atomic load each. That is not a comment: `tests/otel_off_free.rs` counts allocations around those
//! calls with a counting allocator and requires zero, for all three off reasons — the guarantee the
//! agreement asks for (lwf's pending-key budget of ~19 B/key must not grow, so nothing per entry is
//! added anywhere: the parents of a batch live on the reader, one per transaction, and in the
//! engine's capped per-view sidecar).
//!
//! # What is exported, and what never is
//!
//! Span and attribute names are [`span`] and [`attr`], the agreed vocabulary. Values are names,
//! ids, counts, positions and outcomes — **never customer data**: no key values, no payloads, no
//! SQL, no DSNs. [`Span::fail`] takes a `&'static str` error class rather than an error's text so
//! a statement or a row value cannot reach a span by accident. The resource is the file's
//! (`resource.attributes`, and `attributes_list` for the standard `OTEL_RESOURCE_ATTRIBUTES`, which
//! is how an operator carries `service.name` and `service.instance.id`); `service.version` is the
//! one attribute lazyview still decides from the crate version unless the file sets it, and the
//! source's server identity comes from [`Host::source`]. Nothing here reads an instance id.
//!
//! Telemetry must never break the pipeline: a file that is missing, unreadable, malformed, or that
//! asks for something this build cannot carry leaves tracing off with one `WARN` — never a failed
//! start, never a panic — and an unreachable collector only ever reaches [`Counters`]' rate-limited
//! WARN. The engine runs on either way.
//!
//! # Signals
//!
//! Traces only, for now: `meter_provider` and `logger_provider` are recognised and reported as
//! ignored by [`config`], and the metrics and logs phases fill those sections in on this module's
//! once-cell and resource rather than re-shaping it.

mod config;
mod prop;
#[cfg(test)]
mod tests;

pub use config::{gate, Gate, CONFIG_FILE_ENV};
pub use prop::{extract, extract_from, inject, TraceParent, TRACEPARENT};

use opentelemetry::trace::{
    Link, Span as _, SpanContext, SpanKind, Status, TraceContextExt, TracerProvider as _,
};
use opentelemetry::{Context, Key, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::resource::TelemetryResourceDetector;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, Sampler as SdkSampler, SdkTracer, SdkTracerProvider,
    ShouldSample, SimpleSpanProcessor, Span as SdkSpan, SpanData, SpanExporter, SpanLimits,
};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use self::config::{Exporter, Plan, Processor, Sampler};

/// The instrumentation scope of every span this module opens (`otel.library.name`).
const SCOPE: &str = "lazyview";

/// Span names — lazyview's half of the vocabulary (agreement §1 gives lazyview the `lazyview.*`
/// namespace; the attribute names below are the agreed ones, these names are ours and are part of
/// the conformance contract the copy set ships with).
pub mod span {
    /// One batch of events read from the binlog between two flushes.
    pub const BINLOG_READ_BATCH: &str = "lazyview.binlog.read_batch";
    /// One source transaction read from the binlog: the span the shared-core seam LINKS to, so a
    /// row event and the delivery it caused are in one trace without being parented (agreement §3).
    pub const BINLOG_TRANSACTION: &str = "lazyview.binlog.transaction";
    /// One engine flush: the fence, the refreshes, the writes and the position save of one batch.
    pub const VIEW_FLUSH: &str = "lazyview.view.flush";
    /// One view brought up to date with the changes it was marked by.
    pub const VIEW_REFRESH: &str = "lazyview.view.refresh";
    /// One batch of statements written to one target.
    pub const TARGET_WRITE_BATCH: &str = "lazyview.target.write_batch";
    /// One save of the replay position to the metadata tables.
    pub const POSITION_PERSIST: &str = "lazyview.position.persist";
    /// One leadership takeover: the fencing epoch moved to this instance.
    pub const EPOCH_TAKEOVER: &str = "lazyview.epoch.takeover";
}

/// Attribute names, lazyview's half of the agreed vocabulary (agreement §1). The six the two
/// teams agreed are the first block; the rest are counts and outcomes, bounded by construction.
/// No attribute of any span here may carry a value that came from a row.
pub mod attr {
    pub const VIEW_ID: &str = "lazyview.view.id";
    pub const VIEW_NAME: &str = "lazyview.view.name";
    pub const TARGET_NAME: &str = "lazyview.target.name";
    pub const SOURCE_POSITION: &str = "lazyview.source.position";
    pub const SOURCE_GTID: &str = "lazyview.source.gtid";
    /// The fencing epoch this instance holds.
    pub const EPOCH: &str = "lazyview.epoch";
    // lazyview's own, same discipline: a name, a count, or a coordinate — never a row value.
    pub const SOURCE_FLAVOR: &str = "lazyview.source.flavor";
    /// The source's identity (`server_identity`): a server name or uuid, never a DSN.
    pub const SOURCE_SERVER: &str = "lazyview.source.server";
    pub const EVENTS: &str = "lazyview.events";
    pub const ROWS: &str = "lazyview.rows";
    pub const BYTES: &str = "lazyview.bytes";
    pub const KEYS: &str = "lazyview.keys";
    pub const VIEWS: &str = "lazyview.views";
    pub const TARGETS: &str = "lazyview.targets";
    pub const MODE: &str = "lazyview.mode";
    pub const OUTCOME: &str = "lazyview.outcome";
    pub const FORCED: &str = "lazyview.forced";
    /// Source transactions linked from this span (the merge may have folded in more).
    pub const LINKS: &str = "lazyview.links";
}

/// The `…outcome` values: bounded, so a backend can group on them.
pub mod outcome {
    pub const SUCCESS: &str = "success";
    pub const ERROR: &str = "error";
    pub const TIMEOUT: &str = "timeout";
}

/// Source-transaction spans one flush may link to. A cap keeps a span's size bounded whatever the
/// batch did, and it is why the parents live in a per-view sidecar instead of a pending-key entry
/// (agreement §3: links, and links that cannot grow the hot path).
pub const MAX_LINKS: usize = 16;

/// What the host tells the module: everything this copy-set file may not read for itself. Every
/// field is plain data the host already has, and each side feeds it from its own configuration.
#[derive(Debug, Clone)]
pub struct Host<'a> {
    /// The operator's declarative-config file, as [`gate`] resolves it from the standard
    /// `OTEL_CONFIG_FILE`: `Some` only for an existing, readable, regular file, and `None` is off.
    /// The host reads the variable, this module reads the file — that is the whole of the
    /// host/module boundary for the switch.
    pub config_file: Option<&'a Path>,
    /// `service.name` unless the file says otherwise: `lazyview` here, `lazy-wire-fire` for the
    /// embedder.
    pub service_name: &'static str,
    /// `service.version` — the host's `CARGO_PKG_VERSION`, unless the file sets it. The one
    /// attribute lazyview still decides, and it is not identity.
    pub service_version: &'static str,
    /// The source's identity (`server_identity`): a name, never a credential or a DSN.
    pub source: Option<&'a str>,
}

/// The once-cell's states. `UNSET` is what a process that never called [`init`] holds; the hot
/// paths read `UNSET` and `OFF` the same way, which is what makes "off" provable rather than a
/// promise about the environment.
const UNSET: u8 = 0;
const OFF: u8 = 1;
const ON: u8 = 2;

/// What a live exporter needs. Behind the once-cell; built once, by [`init`].
struct Telemetry {
    provider: SdkTracerProvider,
    tracer: SdkTracer,
    counters: Arc<Counters>,
}

/// `STATE` is the hot path's single relaxed load. The `Mutex` guards what only the on-path and the
/// tests touch: the installed provider (the tests point it at a capturing exporter).
static STATE: AtomicU8 = AtomicU8::new(UNSET);
static TELEMETRY: Mutex<Option<Arc<Telemetry>>> = Mutex::new(None);

/// At most one WARN per this interval about failing exports: a collector that is down must not
/// turn the log into a flood, and must not cost the pipeline anything either.
const RATE_LIMIT: Duration = Duration::from_secs(60);

/// The exporter's own failures, counted and rate-limited: telemetry that cannot get out is
/// reported, never raised.
#[derive(Default, Debug)]
struct Counters {
    errors: AtomicU64,
    /// Unix seconds of the last WARN (0 = none yet).
    last_warn: AtomicU64,
}

impl Counters {
    fn note(&self, err: &str) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let now = unix_seconds();
        let last = self.last_warn.swap(now, Ordering::Relaxed);
        if last == 0 || now.saturating_sub(last) >= RATE_LIMIT.as_secs() {
            tracing::warn!(
                "OpenTelemetry export failed: {err}; spans are dropped and the pipeline is \
                 unaffected (next such WARN in {}s)",
                RATE_LIMIT.as_secs()
            );
        }
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

/// `true` when spans are being collected: one relaxed atomic load, no lock, no allocation. Every
/// instrumented hot path checks this (usually by calling [`span`], which checks it for them).
#[inline]
pub fn enabled() -> bool {
    STATE.load(Ordering::Relaxed) == ON
}

/// `true` once [`init`] has built a provider and an exporter. With no file to read it stays
/// `false`, which is the off test's probe: there is provably no tracer and no exporter in the
/// process, because nothing ever created one.
#[inline]
pub fn initialised() -> bool {
    STATE.load(Ordering::Relaxed) == ON
}

/// Exports that failed since the process started (the rate-limited WARN says nothing per batch;
/// this says whether anything failed at all).
pub fn export_errors() -> u64 {
    telemetry()
        .map(|t| t.counters.errors.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// One `WARN` per distinct message per run. Reading a file is a once-per-process thing, but the
/// notes are collected rather than logged as they are found — so a repeated unknown key, or the same
/// problem reached twice from two sections, is said once and cannot become a log loop.
fn warn_once(what: &str) {
    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
    if seen.iter().any(|s| s == what) {
        return;
    }
    seen.push(what.to_owned());
    drop(seen);
    tracing::warn!("OpenTelemetry: {what}");
}

/// Builds the provider and the exporters the file names, and installs them in the once-cell.
/// `true` when telemetry is on; every problem — a file that cannot be read, a model version this
/// build does not have, an exporter that cannot be built — is one `WARN` and `false`, never an error
/// the caller has to handle. Idempotent: the first call decides.
pub fn init(host: &Host) -> bool {
    if enabled() {
        return true;
    }
    let Some(path) = host.config_file else {
        // Unset is the documented default: nothing to say, nothing to build.
        STATE.store(OFF, Ordering::Relaxed);
        return false;
    };
    match config::read_file(path) {
        config::Reading::Off { reason, warn } => {
            // Telemetry is a visitor: a refused visit is reported, and the run goes on without it.
            if warn {
                warn_once(&reason);
            }
            STATE.store(OFF, Ordering::Relaxed);
            false
        }
        config::Reading::On(plan) => {
            let plan: Plan = *plan;
            for note in &plan.warnings {
                warn_once(note);
            }
            if let Some(t) = &plan.traces {
                if t.limits.attribute_value_length.is_some() {
                    warn_once(
                        "tracer_provider.limits.attribute_value_length_limit is not supported by \
                         this SDK build and was ignored",
                    );
                }
            }
            match build_exporters(&plan) {
                Ok(ps) if ps.is_empty() => {
                    warn_once("the file names no processor telemetry can use");
                    STATE.store(OFF, Ordering::Relaxed);
                    false
                }
                Ok(ps) => install_processors(host, &plan, ps, Arc::new(Counters::default())),
                Err(reason) => {
                    warn_once(&reason);
                    STATE.store(OFF, Ordering::Relaxed);
                    false
                }
            }
        }
    }
}

/// The exporters the file's processors name, each paired with the processor that asked for it.
fn build_exporters(plan: &Plan) -> Result<Vec<(Built, Processor)>, String> {
    let Some(t) = &plan.traces else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(t.processors.len());
    for p in &t.processors {
        let exporter = match p {
            Processor::Batch { exporter, .. } | Processor::Simple { exporter, .. } => exporter,
        };
        out.push((build_exporter(exporter)?, p.clone()));
    }
    Ok(out)
}

/// Builds one exporter. The OTLP/gRPC one has every field the flat `OTEL_EXPORTER_OTLP_*` names
/// would otherwise feed the SDK given here, so the file really is the only config source (the three
/// the SDK still resolves itself are named in `docs/configuration.md`).
fn build_exporter(exporter: &Exporter) -> Result<Built, String> {
    match exporter {
        Exporter::Console => Ok(Built::Console),
        Exporter::OtlpGrpc(g) => {
            if !g.headers.is_empty() {
                // Read, reported, and deliberately not applied: the official gRPC exporter sends
                // per-request headers only as `tonic` metadata (`WithTonicConfig::with_metadata`),
                // and that setter is reachable only through a direct `tonic` dependency, which this
                // crate does not name — `tonic` is in the lock only as a transitive of
                // `opentelemetry-otlp`'s `grpc-tonic` feature, which the export itself needs.
                // Saying so once is the whole difference between a collector that rejects the
                // exports in silence and an operator who knows why.
                warn_once(&format!(
                    "{}: the file's `headers` are read but not sent — this build's OTLP/gRPC \
                         exporter sets no per-request headers without a direct tonic dependency; \
                         serve them at the collector or the network path",
                    g.endpoint
                ));
            }
            let b = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(g.endpoint.clone())
                .with_timeout(g.timeout);
            let b = match g.compression {
                Some(c) => b.with_compression(c),
                None => b,
            };
            b.build().map(|e| Built::Otlp(Box::new(e))).map_err(|e| {
                format!(
                    "the OTLP/gRPC exporter for {} cannot be built ({e})",
                    g.endpoint
                )
            })
        }
    }
}

/// Installs a provider with one processor per entry, each holding `exporter`. Split out of [`init`]
/// so the tests can put a capturing exporter here and assert what the product actually does with
/// the values the file carried.
fn install_processors<E: SpanExporter + 'static>(
    host: &Host,
    plan: &Plan,
    processors: Vec<(E, Processor)>,
    counters: Arc<Counters>,
) -> bool {
    let mut b = SdkTracerProvider::builder().with_resource(resource(host, plan));
    if let Some(t) = &plan.traces {
        b = b.with_sampler(Box::new(sdk_sampler(&t.sampler)) as Box<dyn ShouldSample>);
        b = b.with_span_limits(span_limits(&t.limits));
    }
    for (exporter, spec) in processors {
        let wrapped = RateLimited {
            inner: exporter,
            counters: counters.clone(),
        };
        b = match spec {
            // Every knob of the batch processor is given explicitly, the ones the file left out
            // included, because `BatchConfig`'s own defaults seed themselves from `OTEL_BSP_*`.
            Processor::Batch { batch, .. } => {
                // The SDK's builder has no setter for the export timeout outside its experimental
                // async-runtime feature, so this one knob stays the environment's if the file names
                // a value of its own — said once, like every other gap, and named in the docs.
                if batch.export_timeout != config::BATCH_EXPORT_TIMEOUT {
                    warn_once(&format!(
                        "the file's `export_timeout` of {:?} cannot be set on the batch processor                          by this SDK build (OTEL_BSP_MAX_EXPORT_TIMEOUT is the SDK's own door);                          exports use {:?}",
                        batch.export_timeout, config::BATCH_EXPORT_TIMEOUT
                    ));
                }
                b.with_span_processor(BatchSpanProcessor::new(
                    wrapped,
                    BatchConfigBuilder::default()
                        .with_max_queue_size(batch.max_queue_size)
                        .with_max_export_batch_size(batch.max_export_batch_size)
                        .with_scheduled_delay(batch.schedule_delay)
                        .build(),
                ))
            }
            Processor::Simple { .. } => b.with_span_processor(SimpleSpanProcessor::new(wrapped)),
        };
    }
    let provider = b.build();
    let tracer = provider.tracer(SCOPE);
    *TELEMETRY.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(Telemetry {
        provider,
        tracer,
        counters,
    }));
    STATE.store(ON, Ordering::Relaxed);
    true
}

/// The file's sampler, as the SDK's own. `parent_based` maps to the SDK's parent-based sampler with
/// only the branches the file named overridden, so the specification's defaults stay the SDK's.
fn sdk_sampler(s: &Sampler) -> SdkSampler {
    match s {
        Sampler::AlwaysOn => SdkSampler::AlwaysOn,
        Sampler::AlwaysOff => SdkSampler::AlwaysOff,
        Sampler::Ratio(r) => SdkSampler::TraceIdRatioBased(*r),
        Sampler::ParentBased {
            root,
            remote_parent_sampled,
            remote_parent_not_sampled,
            local_parent_sampled,
            local_parent_not_sampled,
        } => {
            let mut p = SdkSampler::parent_based(box_sampler(root));
            if let Some(x) = remote_parent_sampled {
                p = p.with_remote_parent_sampled(box_sampler(x));
            }
            if let Some(x) = remote_parent_not_sampled {
                p = p.with_remote_parent_not_sampled(box_sampler(x));
            }
            if let Some(x) = local_parent_sampled {
                p = p.with_local_parent_sampled(box_sampler(x));
            }
            if let Some(x) = local_parent_not_sampled {
                p = p.with_local_parent_not_sampled(box_sampler(x));
            }
            SdkSampler::ParentBased(Box::new(p))
        }
    }
}

fn box_sampler(s: &Sampler) -> Box<dyn ShouldSample> {
    Box::new(sdk_sampler(s)) as Box<dyn ShouldSample>
}

/// The file's `tracer_provider.limits`, over the SDK's own defaults.
fn span_limits(l: &config::Limits) -> SpanLimits {
    let mut sl = SpanLimits::default();
    if let Some(v) = l.attribute_count {
        sl.max_attributes_per_span = v;
    }
    if let Some(v) = l.event_count {
        sl.max_events_per_span = v;
    }
    if let Some(v) = l.link_count {
        sl.max_links_per_span = v;
    }
    if let Some(v) = l.event_attribute_count {
        sl.max_attributes_per_event = v;
    }
    if let Some(v) = l.link_attribute_count {
        sl.max_attributes_per_link = v;
    }
    sl
}

/// The resource: who produced this telemetry. What the file's `resource` section says wins; the
/// defaults *under* it are the host's service name and the crate version. `telemetry.sdk.*` comes
/// from the SDK's own detector, which reads no environment — the `OTEL_RESOURCE_ATTRIBUTES` and
/// `OTEL_SERVICE_NAME` names reach the resource through the file's `${…}` substitution, which is
/// why the SDK's env detector is deliberately not installed here.
fn resource(host: &Host, plan: &Plan) -> Resource {
    let mut ours: Vec<KeyValue> = plan
        .resource
        .iter()
        .map(|(k, v)| KeyValue::new(Key::new(Cow::Owned(k.clone())), Cow::Owned(v.clone())))
        .collect();
    let has = |key: &str| ours.iter().any(|a| a.key.as_str() == key);
    let (want_name, want_version) = (!has(SERVICE_NAME), !has(SERVICE_VERSION));
    if want_name {
        ours.push(KeyValue::new(
            Key::from_static_str(SERVICE_NAME),
            host.service_name,
        ));
    }
    if want_version {
        ours.push(KeyValue::new(
            Key::from_static_str(SERVICE_VERSION),
            host.service_version,
        ));
    }
    if let Some(s) = host.source {
        // Owned: `host` is a borrow of the caller's, and a resource attribute outlives this call.
        ours.push(KeyValue::new(
            Key::from_static_str(attr::SOURCE_SERVER),
            s.to_owned(),
        ));
    }
    Resource::builder_empty()
        .with_attributes(ours)
        .with_detector(Box::new(TelemetryResourceDetector))
        .build()
}

/// How long the exit path waits for the last batch.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

/// Flushes and closes the exporter. On the engine's exit path, bounded by [`FLUSH_TIMEOUT`] so a
/// collector that has gone cannot hold a shutdown open.
pub fn shutdown() {
    let t = TELEMETRY.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if let Some(t) = t {
        let _ = t.provider.shutdown_with_timeout(FLUSH_TIMEOUT);
    }
}

/// The OTLP exporter with its failures turned into a rate-limited WARN: an unreachable collector
/// is never an error the pipeline sees, and never a log line per batch either.
#[derive(Debug)]
struct RateLimited<E> {
    inner: E,
    counters: Arc<Counters>,
}

impl<E: SpanExporter + 'static> SpanExporter for RateLimited<E> {
    async fn export(&self, batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner
            .export(batch)
            .await
            .inspect_err(|e| self.counters.note(&e.to_string()))
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
}

/// The exporter the file names, as one type so that the SDK's own processors can hold whichever
/// combination of exporters the file lists.
#[derive(Debug)]
enum Built {
    // Boxed: the OTLP exporter is hundreds of bytes and the console one is nothing, and this enum
    // is held by every processor the file names.
    Otlp(Box<opentelemetry_otlp::SpanExporter>),
    Console,
}

impl SpanExporter for Built {
    async fn export(&self, batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        match self {
            Built::Otlp(e) => e.export(batch).await,
            Built::Console => {
                for s in &batch {
                    // The model's `console` exporter: what a span is, on stdout. Only ever
                    // reached when the operator asked for it.
                    println!(
                        "otel span name={} trace_id={} span_id={} kind={:?} attributes={} \
                         links={} status={:?}",
                        s.name,
                        hex(&s.span_context.trace_id().to_bytes()),
                        hex(&s.span_context.span_id().to_bytes()),
                        s.span_kind,
                        s.attributes.len(),
                        s.links.len(),
                        s.status
                    );
                }
                Ok(())
            }
        }
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        match self {
            Built::Otlp(e) => e.force_flush(),
            Built::Console => Ok(()),
        }
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        match self {
            Built::Otlp(e) => e.shutdown(),
            Built::Console => Ok(()),
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        match self {
            Built::Otlp(e) => e.shutdown_with_timeout(timeout),
            Built::Console => Ok(()),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// The current telemetry, when there is one. Only the on-path reaches the lock.
fn telemetry() -> Option<Arc<Telemetry>> {
    if !enabled() {
        return None;
    }
    TELEMETRY.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// The span handle. The `Default` value is a *disabled* handle: no SDK span, no context, every
/// method a no-op — so a call site reads the same whether telemetry is on or off.
#[derive(Default)]
pub struct Span {
    inner: Option<SdkSpan>,
    /// This span's own context, kept for the seam ([`Span::trace_parent`]) and for children.
    me: Option<SpanContext>,
}

impl Span {
    /// `true` when the sampler took this span, i.e. whether stamping or linking to it says
    /// anything. A disabled handle and a recorded-but-unsampled one both answer `false`.
    pub fn sampled(&self) -> bool {
        self.me.is_some()
    }

    /// The value to stamp for the seam: this span's W3C parent, or `None` when it was not sampled,
    /// which is what keeps an unsampled batch from carrying or allocating anything.
    pub fn trace_parent(&self) -> Option<TraceParent> {
        self.me.as_ref().and_then(TraceParent::from_span_context)
    }

    /// Records a failure. `what` names the *class* of failure (`"timeout"`, `"target"`), and the
    /// signature is `&'static str` on purpose: an error's text can carry a table name or a value,
    /// and nothing of the sort belongs in a span.
    pub fn fail(&mut self, what: &'static str) {
        if let Some(s) = self.inner.as_mut() {
            s.set_status(Status::error(what.to_owned()));
            s.set_attributes([KeyValue::new(
                Key::from_static_str(attr::OUTCOME),
                outcome::ERROR,
            )]);
        }
    }

    /// Records an attribute that is only known when the work is done (a count, the boundary a
    /// transaction reached). No-op on a disabled handle.
    pub fn set_str(&mut self, k: &'static str, v: impl Into<Cow<'static, str>>) {
        if let Some(s) = self.inner.as_mut() {
            s.set_attributes([KeyValue::new(Key::from_static_str(k), v.into())]);
        }
    }

    /// [`Span::set_str`] for a count or an id.
    pub fn set_u64(&mut self, k: &'static str, v: u64) {
        if let Some(s) = self.inner.as_mut() {
            s.set_attributes([KeyValue::new(Key::from_static_str(k), v as i64)]);
        }
    }

    /// Records that the work succeeded, so a backend can tell "span with no outcome" (never
    /// finished) from "finished, nothing to do".
    pub fn ok(&mut self) {
        if let Some(s) = self.inner.as_mut() {
            s.set_attributes([KeyValue::new(
                Key::from_static_str(attr::OUTCOME),
                outcome::SUCCESS,
            )]);
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        // Ending is what hands a span to the exporter; a disabled handle has nothing to end.
        if let Some(mut s) = self.inner.take() {
            s.end();
        }
    }
}

/// The attributes and links a span is built with. [`span`] runs its closure **only when tracing is
/// on**, so the off path never builds a `Vec`, a `KeyValue`, or touches the SDK at all.
pub struct SpanInit {
    attrs: Vec<KeyValue>,
    links: Vec<Link>,
    parent: Option<SpanContext>,
    kind: SpanKind,
}

impl SpanInit {
    fn push(&mut self, k: &'static str, v: impl Into<opentelemetry::Value>) {
        self.attrs.push(KeyValue::new(Key::from_static_str(k), v));
    }

    /// A string attribute; `Cow` so a `&'static str` costs nothing.
    pub fn str(&mut self, k: &'static str, v: impl Into<Cow<'static, str>>) {
        self.push(k, v.into());
    }

    pub fn i64(&mut self, k: &'static str, v: i64) {
        self.push(k, v);
    }

    pub fn u64(&mut self, k: &'static str, v: u64) {
        // OTel's integer attribute is signed; what goes in here are counts, ids and offsets.
        self.push(k, v as i64);
    }

    pub fn bool(&mut self, k: &'static str, v: bool) {
        self.push(k, v);
    }

    /// Links this span to the span `p` names. The merge of N events into one delivery is a set of
    /// LINKS, never a parent-child relation (agreement §3). `false` once [`MAX_LINKS`] is reached:
    /// the batch is still applied in full, only the links are capped.
    pub fn link(&mut self, p: &TraceParent) -> bool {
        if self.links.len() >= MAX_LINKS {
            return false;
        }
        match p.span_context() {
            Some(cx) => {
                self.links.push(Link::with_context(cx));
                true
            }
            None => false,
        }
    }

    /// Links every parent of `batch` that still fits; returns how many were taken.
    pub fn links(&mut self, batch: &[TraceParent]) -> usize {
        let mut n = 0;
        for p in batch {
            if self.links.len() >= MAX_LINKS {
                break;
            }
            if self.link(p) {
                n += 1;
            }
        }
        n
    }

    /// Makes `parent`'s span this span's parent (a no-op when `parent` is disabled, leaving a root).
    pub fn parent(&mut self, parent: &Span) {
        self.parent = parent.me.clone();
    }

    /// Makes the stamped seam value `p` this span's parent — the embedder's side of the seam, where
    /// a delivery span hangs off the binlog span lazyview stamped at read time.
    pub fn parent_from(&mut self, p: &TraceParent) {
        self.parent = p.span_context();
    }

    /// The span kind: `Internal` everywhere except where work crosses a process boundary.
    pub fn kind(&mut self, kind: SpanKind) {
        self.kind = kind;
    }
}

/// Opens a span named `name`. `build` — the attributes and the links — runs **only when tracing is
/// on**: with `OTEL_CONFIG_FILE` unset this returns a disabled handle after one atomic load, which is
/// the whole off-is-free mechanism, and is what `tests/otel_off_free.rs` measures in allocations.
///
/// The span ends when the handle is dropped, so bind it for as long as the work it covers: for an
/// `async` span, across its `.await`s.
#[must_use = "the span ends when the handle is dropped: bind it to the work it covers"]
pub fn span(name: &'static str, build: impl FnOnce(&mut SpanInit)) -> Span {
    let Some(t) = telemetry() else {
        return Span::default();
    };
    let mut si = SpanInit {
        attrs: Vec::with_capacity(8),
        links: Vec::new(),
        parent: None,
        kind: SpanKind::Internal,
    };
    build(&mut si);
    let parent_cx = match &si.parent {
        Some(p) => Context::new().with_remote_span_context(p.clone()),
        None => Context::new(),
    };
    let b = opentelemetry::trace::SpanBuilder::from_name(name)
        .with_kind(si.kind)
        .with_attributes(si.attrs)
        .with_links(si.links);
    let s = b.start_with_context(&t.tracer, &parent_cx);
    // The started span's own context is what the seam stamps and what children point at. Taken
    // from the span, not the parent: an unsampled span's context is invalid, and `None` then means
    // "nothing is stamped", which is how the unsampled path stays free.
    let me = s.span_context().clone();
    Span {
        inner: Some(s),
        me: (me.is_valid() && me.is_sampled()).then_some(me),
    }
}

/// The W3C propagator, for a host that wants to read a `traceparent` a caller sent it or write one
/// into an outgoing request (lazywf's delivery glue does exactly this).
pub fn propagator() -> &'static opentelemetry_sdk::propagation::TraceContextPropagator {
    static P: OnceLock<opentelemetry_sdk::propagation::TraceContextPropagator> = OnceLock::new();
    P.get_or_init(opentelemetry_sdk::propagation::TraceContextPropagator::new)
}

/// Test-only: installs the provider [`init`] would have built from `plan`, with each given exporter
/// in the processor the file said, runs `f`, and puts the cell back the way it was. Serialised:
/// exactly one test may hold telemetry at a time. The `exporter` inside each processor spec only
/// says which *kind* of processor to build — the exporter itself is the one passed in.
#[cfg(test)]
pub(crate) fn with_plan<R, E: SpanExporter + 'static>(
    host: &Host,
    plan: &Plan,
    processors: Vec<(E, Processor)>,
    f: impl FnOnce() -> R,
) -> R {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset();
    install_processors(host, plan, processors, Arc::new(Counters::default()));
    let out = f();
    shutdown();
    reset();
    out
}

/// Test-only: the ordinary case — one simple processor sending to `exporter`, the SDK's default
/// sampler and limits, the file's resource empty.
#[cfg(test)]
pub(crate) fn with_telemetry<R>(
    host: &Host,
    exporter: impl SpanExporter + 'static,
    f: impl FnOnce() -> R,
) -> R {
    let plan = Plan {
        traces: Some(self::config::Traces {
            sampler: Sampler::AlwaysOn,
            processors: Vec::new(),
            limits: config::Limits::default(),
        }),
        ..Plan::default()
    };
    with_plan(
        host,
        &plan,
        vec![(
            exporter,
            Processor::Simple {
                exporter: Exporter::Console,
            },
        )],
        f,
    )
}

/// Test-only: gives a batch processor its turn to export (the exit path's flush, without its
/// shutdown), so a test can see what a batch actually left in.
#[cfg(test)]
pub(crate) fn flush() {
    if let Some(t) = telemetry() {
        let _ = t.provider.force_flush();
    }
}

/// Test-only: runs `f` with the cell forced back to unset (the state a process that never called
/// [`init`] holds), so the off-path tests do not depend on test order.
#[cfg(test)]
pub(crate) fn with_off<R>(f: impl FnOnce() -> R) -> R {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset();
    let out = f();
    reset();
    out
}

#[cfg(test)]
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
fn reset() {
    STATE.store(UNSET, Ordering::Relaxed);
    *TELEMETRY.lock().unwrap_or_else(|e| e.into_inner()) = None;
}
