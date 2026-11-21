//! Opt-in OpenTelemetry: the init + conventions module, and the W3C seam.
//!
//! **Copy set.** `src/otel.rs` and `src/otel/**` are copied verbatim by lazy-wire-fire (its
//! `vendor/lazyview.lock` takes the glob at the re-sync that first carries them), so the rules of
//! the copy set hold here: no `crate::` import of anything outside the copy set, and no
//! lazyview-only state read — the host's own settings (`LAZYVIEW_OTEL`, the instance id, the
//! source's identity) arrive as parameters of [`init`], which is how lazywf feeds its own
//! (`LAZYWF_OTEL`) too. The standard OTel environment variables (`OTEL_*`) *are* read here: they
//! are the specification's, not lazyview's (agreement §2, `tmp/out/lwf-coordination/otel-agreement.md`).
//!
//! # One switch, and why "off" is provable
//!
//! `LAZYVIEW_OTEL=off|otlp` (default **off**) is the only way this ever runs: an ambient collector
//! on the pod network changes nothing. Off, no provider and no exporter exist and the once-cell is
//! unset, so every span helper below returns a disabled handle **before it builds an attribute**,
//! and the instrumented hot paths cost one relaxed atomic load each. That is not a comment:
//! `tests/otel_off_free.rs` counts allocations around those calls with a counting allocator and
//! requires zero — the guarantee the agreement asks for (lwf's pending-key budget of ~19 B/key
//! must not grow, so nothing per entry is added anywhere: the parents of a batch live on the
//! reader, one per transaction, and in the engine's capped per-view sidecar).
//!
//! # What is exported, and what never is
//!
//! Span and attribute names are [`span`] and [`attr`], the agreed vocabulary. Values are names,
//! ids, counts, positions and outcomes — **never customer data**: no key values, no payloads, no
//! SQL, no DSNs. [`Span::fail`] takes a `&'static str` error class rather than an error's text so
//! a statement or a row value cannot reach a span by accident. The resource carries
//! `service.name`, `service.version`, `service.instance.id` and the source's server identity.
//!
//! Telemetry must never break the pipeline: a bad `OTEL_*` value, an unsupported protocol or an
//! unreachable collector leaves tracing off (one WARN) or makes exports fail into
//! [`Counters`]' rate-limited WARN, and the engine runs on.
//!
//! # Signals
//!
//! Traces only, for now. Metrics (over the same engine fields `/metrics` and `/stats` read) and
//! the `tracing` → OTLP logs bridge land in their own PRs on this module's once-cell, resource and
//! per-signal exporter switches (`OTEL_METRICS_EXPORTER` / `OTEL_LOGS_EXPORTER` are read there).

mod prop;
#[cfg(test)]
mod tests;

pub use prop::{extract, extract_from, inject, TraceParent, TRACEPARENT};

use opentelemetry::trace::{
    Link, Span as _, SpanContext, SpanKind, Status, TraceContextExt, TracerProvider as _,
};
use opentelemetry::{Context, Key, KeyValue};
use opentelemetry_sdk::resource::{EnvResourceDetector, TelemetryResourceDetector};
use opentelemetry_sdk::trace::{
    SdkTracer, SdkTracerProvider, Span as SdkSpan, SpanData, SpanExporter,
};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::{
    SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_VERSION,
};
use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// `LAZYVIEW_OTEL` / `LAZYWF_OTEL`: only `otlp` turns telemetry on. Anything else — including a
/// typo — is off, because a misspelled switch must not silently start sending telemetry (and must
/// not fail startup either; `Config` WARNs on the value it did not understand).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Switch {
    Off,
    Otlp,
}

impl Switch {
    /// Parses the host's switch value (`None` when the variable is unset).
    pub fn parse(value: Option<&str>) -> Switch {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            Some(v) if v.eq_ignore_ascii_case("otlp") => Switch::Otlp,
            _ => Switch::Off,
        }
    }

    pub fn is_on(self) -> bool {
        self == Switch::Otlp
    }
}

/// What the host tells the module: everything this copy-set file may not read for itself. Every
/// field is plain data the host already has, and each side feeds it from its own configuration.
#[derive(Debug, Clone)]
pub struct Host<'a> {
    /// The host's own switch value, parsed with [`Switch::parse`].
    pub switch: Switch,
    /// `service.name` unless `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` say otherwise:
    /// `lazyview` here, `lazy-wire-fire` for the embedder.
    pub service_name: &'static str,
    /// `service.version` — the host's `CARGO_PKG_VERSION`.
    pub service_version: &'static str,
    /// `service.instance.id` — `LAZYVIEW_INSTANCE_ID`. Two instances working on one schema must
    /// be tellable apart, which is the same reason the fencing epoch exists.
    pub instance_id: &'a str,
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

/// `true` once [`init`] has built a provider and an exporter. With no environment set it stays
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

/// Builds the tracer provider and installs it in the once-cell. `true` when telemetry is on;
/// every failure (a value this build does not support, an exporter that cannot be built) is one
/// WARN and `false` — never an error the caller has to handle. Idempotent: the first call decides.
pub fn init(host: &Host) -> bool {
    if !host.switch.is_on() {
        STATE.store(OFF, Ordering::Relaxed);
        return false;
    }
    if enabled() {
        return true;
    }
    match refused() {
        Some(why) => {
            // Telemetry is a visitor: a refused visit is reported, and the run goes on without it.
            tracing::warn!("OpenTelemetry stays off ({why})");
            STATE.store(OFF, Ordering::Relaxed);
            false
        }
        None => match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
            .map_err(|e| format!("building the OTLP exporter: {e}"))
        {
            Ok(exporter) => install(host, exporter),
            Err(e) => {
                tracing::warn!("OpenTelemetry stays off ({e})");
                STATE.store(OFF, Ordering::Relaxed);
                false
            }
        },
    }
}

/// Installs a provider whose spans go to `exporter`. Split out of [`init`] so the tests can put a
/// capturing exporter here and assert what the product actually emits.
fn install<E: SpanExporter + 'static>(host: &Host, exporter: E) -> bool {
    let counters = Arc::new(Counters::default());
    // The sampler and the span limits come from the SDK's own default config, which is what reads
    // `OTEL_TRACES_SAMPLER` / `_ARG` and the `OTEL_SPAN_*_COUNT_LIMIT` names; only the resource is
    // ours to say (below), so nothing here re-implements the specification's config.
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(RateLimited {
            inner: exporter,
            counters: counters.clone(),
        })
        .with_resource(resource(host))
        .build();
    let tracer = provider.tracer(SCOPE);
    *TELEMETRY.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(Telemetry {
        provider,
        tracer,
        counters,
    }));
    STATE.store(ON, Ordering::Relaxed);
    true
}

/// Why the switch is on but this build will not export, or `None` to go ahead. Everything here is
/// the standard environment: the same names every OTel SDK reads, so a collector setup that works
/// for one language's SDK works here (agreement §2).
fn refused() -> Option<String> {
    // `OTEL_TRACES_EXPORTER=none` disables traces even with the switch on: one collector setup
    // can then carry metrics and logs and no spans, which is what the spec's per-signal switch is for.
    match std::env::var("OTEL_TRACES_EXPORTER")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
    {
        Some(v) if v.is_empty() || v == "none" => return Some("OTEL_TRACES_EXPORTER=none".into()),
        Some(v) if v != "otlp" => {
            return Some(format!(
                "OTEL_TRACES_EXPORTER={v} is not supported (otlp, none)"
            ))
        }
        _ => {}
    }
    if std::env::var("OTEL_SDK_DISABLED")
        .ok()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
    {
        return Some("OTEL_SDK_DISABLED=true".into());
    }
    // OTLP/gRPC only: the http/* transports would add reqwest and prost to a binary that has no
    // other use for them, and gRPC is the transport lazy-wire-fire already carries (agreement §4).
    let protocol = std::env::var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL"))
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    match protocol.as_deref() {
        Some(p) if p != "grpc" => Some(format!(
            "OTEL_EXPORTER_OTLP_PROTOCOL={p} is not supported (grpc)"
        )),
        _ => None,
    }
}

/// The resource: who produced this telemetry. `telemetry.sdk.*` says which SDK produced it.
/// The operator's `OTEL_SERVICE_NAME` is adopted as our own `service.name` — the SDK's detectors
/// merge *under* programmatic attributes (the specification's precedence), so taking it in is the
/// only way an env value beats the default; `OTEL_RESOURCE_ATTRIBUTES` arrives through the env
/// detector for every key we did not set ourselves.
fn resource(host: &Host) -> Resource {
    let service = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let mut ours = vec![
        KeyValue::new(
            Key::from_static_str(SERVICE_NAME),
            service.unwrap_or_else(|| host.service_name.to_owned()),
        ),
        KeyValue::new(Key::from_static_str(SERVICE_VERSION), host.service_version),
        KeyValue::new(
            Key::from_static_str(SERVICE_INSTANCE_ID),
            host.instance_id.to_owned(),
        ),
    ];
    if let Some(s) = host.source {
        ours.push(KeyValue::new(
            Key::from_static_str(attr::SOURCE_SERVER),
            s.to_owned(),
        ));
    }
    Resource::builder_empty()
        .with_attributes(ours)
        .with_detector(Box::new(EnvResourceDetector::new()))
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
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        let inner = &self.inner;
        let counters = self.counters.clone();
        async move {
            inner.export(batch).await.inspect_err(|e| {
                counters.note(&e.to_string());
            })
        }
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
/// on**: with `LAZYVIEW_OTEL` unset this returns a disabled handle after one atomic load, which is
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

/// Test-only: replaces the once-cell with a provider whose spans go to `exporter`, runs `f`, and
/// puts the cell back the way it was. Serialised: exactly one test may hold telemetry at a time.
#[cfg(test)]
pub(crate) fn with_telemetry<R>(
    host: &Host,
    exporter: impl SpanExporter + 'static,
    f: impl FnOnce() -> R,
) -> R {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset();
    install(host, exporter);
    let out = f();
    shutdown();
    reset();
    out
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
