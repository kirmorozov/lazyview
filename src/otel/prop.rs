//! W3C `traceparent` across the shared-core seam (agreement §3): what the copy-set reader stamps
//! at read time, and what the embedder's delivery spans link to.
//!
//! `traceparent` is the only propagation format either side uses. The value is deliberately a
//! `Copy` inline struct rather than a `String`: an entry of the dedup core must not grow for
//! telemetry (lwf budgets ~19 B/key), so a parent lives where it costs nothing — one per
//! transaction in the reader, at most [`super::MAX_LINKS`] per flush in the engine's sidecar — and
//! is rendered to text only where it actually leaves the process.

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;

/// The field the seam carries the parent in (`traceparent`, lower case as W3C requires).
pub const TRACEPARENT: &str = "traceparent";

/// A sampled span's identity as the seam carries it: the two ids and the sampled flag, inline.
///
/// The absent parent is `None` — no allocation and no field growth anywhere, which the agreement
/// makes a contract rather than a preference (see this module's header).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TraceParent {
    trace: [u8; 16],
    span: [u8; 8],
}

impl TraceParent {
    /// The parent of the span `context` carries, or `None` when it carries no valid sampled span:
    /// an unsampled parent is dropped here rather than carried, so nothing downstream pays for it.
    pub fn from_context(context: &Context) -> Option<TraceParent> {
        TraceParent::from_span_context(context.span().span_context())
    }

    /// The parent of `sc`, or `None` when it is not a valid sampled span context.
    pub fn from_span_context(sc: &SpanContext) -> Option<TraceParent> {
        if !sc.is_valid() || !sc.is_sampled() {
            return None;
        }
        Some(TraceParent {
            trace: sc.trace_id().to_bytes(),
            span: sc.span_id().to_bytes(),
        })
    }

    /// Parses a `traceparent` field value — the seam's ingress, where lazywf reads what lazyview
    /// stamped. Only the `"00"` version this code itself emits is understood, so anything else —
    /// malformed, unknown version, unsampled, all-zero ids — is `None` rather than a guess; the
    /// flags other than the sampled bit are ignored, as W3C requires for fields a receiver does
    /// not know. (Carriers from foreign SDKs at a later version belong to the W3C propagator,
    /// [`extract_from`], not to this closed seam.)
    pub fn parse(value: &str) -> Option<TraceParent> {
        let mut parts = value.split('-');
        let (Some(version), Some(trace), Some(span), Some(flags)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        if parts.next().is_some() {
            return None; // a version this code does not know: do not guess at its fields
        }
        if version != "00" {
            return None; // only the version stamped by [`TraceParent::write`] is understood here
        }
        // The sampled bit is the only flag this side reads, and an unsampled parent says nothing:
        // the seam drops it rather than carrying a link to a span no backend has (agreement §3).
        if flags.len() != 2 || u8::from_str_radix(flags, 16).ok()? & 0x01 == 0 {
            return None;
        }
        // Exact lengths: the SDK's decoders left-pad short fields, and a half-length id is not a
        // parent — it is a carrier from a code that does not know the format.
        if trace.len() != 32 || span.len() != 16 {
            return None;
        }
        let trace = TraceId::from_hex(trace).ok()?.to_bytes();
        let span = SpanId::from_hex(span).ok()?.to_bytes();
        Self { trace, span }
            .span_context()
            .map(|_| Self { trace, span }) // rejects the all-zero ids
    }

    /// The `traceparent` field value (`00-…-…-01`). Allocates, so it is called where the value
    /// leaves the process (an embedder putting it into a job row or an outbound request), not on
    /// the hot path.
    pub fn to_header(&self) -> String {
        let mut out = String::with_capacity(55);
        self.write(&mut out);
        out
    }

    fn write(&self, out: &mut String) {
        use std::fmt::Write as _;
        let _ = write!(out, "00-{}-{}-01", hex(&self.trace), hex(&self.span));
    }

    /// The SDK-side context of the parent — remote and sampled: what a span LINK is made of, and
    /// what makes a span opened from it a child of a span in another process.
    pub fn span_context(&self) -> Option<SpanContext> {
        if self.trace.iter().all(|b| *b == 0) || self.span.iter().all(|b| *b == 0) {
            return None;
        }
        Some(SpanContext::new(
            TraceId::from_bytes(self.trace),
            SpanId::from_bytes(self.span),
            TraceFlags::new(0x01),
            true,
            TraceState::default(),
        ))
    }
}

impl std::fmt::Display for TraceParent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::with_capacity(55);
        self.write(&mut s);
        f.write_str(&s)
    }
}

/// Lower-case hex of a fixed-size id: no dependency, no allocation beyond the returned text.
fn hex(bytes: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(D[(*b >> 4) as usize] as char);
        s.push(D[(*b & 0xf) as usize] as char);
    }
    s
}

/// The `traceparent` of the span `cx` carries, for stamping at read time. `None` when `cx` holds no
/// valid sampled span, so an unsampled batch carries nothing at all.
pub fn inject(cx: &Context) -> Option<TraceParent> {
    TraceParent::from_context(cx)
}

/// The context of a stamped parent: the parent of the span the embedder opens when it delivers what
/// this process read (lazywf's flush/deliver span), and the source of our own links.
pub fn extract(tp: &TraceParent) -> Option<Context> {
    tp.span_context()
        .map(|sc| Context::new().with_remote_span_context(sc))
}

/// Reads a `traceparent` out of a carrier (an embedder's job row, an inbound request header). The
/// SDK's propagator does the parsing, so both sides agree on the format by construction.
pub fn extract_from<'a>(get: impl Fn(&str) -> Option<&'a str>) -> Option<TraceParent> {
    struct Get<'a, F: Fn(&str) -> Option<&'a str>>(F);
    impl<'a, F: Fn(&str) -> Option<&'a str>> opentelemetry::propagation::Extractor for Get<'a, F> {
        fn get(&self, key: &str) -> Option<&str> {
            (self.0)(key)
        }
        fn keys(&self) -> Vec<&str> {
            vec![TRACEPARENT]
        }
    }
    inject(&super::propagator().extract(&Get(get)))
}
