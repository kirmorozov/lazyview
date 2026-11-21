//! "Off is free" as an executed claim, not a comment (`src/otel.rs`'s header, lwf agreement §3).
//!
//! Two things are proved here, from outside the crate and through its public API only:
//!
//! 1. with `LAZYVIEW_OTEL` never set, there is provably no tracer provider and no exporter in this
//!    process — nothing ever created one — and
//! 2. the calls the instrumented hot paths make ([`otel::span`] with its attribute closure, the
//!    seam's [`TraceParent`], the span handle's methods) allocate **zero** while the once-cell is
//!    unset, counted by the process's own allocator.
//!
//! The reader and the engine reach telemetry through exactly these calls, so the count is the cost
//! of a hot path and not of a mock. The per-entry half of the agreement — nothing added to a
//! pending-key entry, wherever it lives — is what [`TraceParent`]'s being `Copy` and inline keeps
//! true structurally; it is asserted too, because a future `String` in that struct would silently
//! undo the whole arrangement.
//!
//! The counting allocator is this binary's own (a copy of the lib test binary's
//! `src/alloc_measure.rs`, which is `cfg(test)` and so not reachable from here): thread-local and
//! only on inside [`count`], so a `#[test]` running on another thread cannot perturb the number.

use lazyview::otel::{self, TraceParent};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

thread_local! {
    // Const-initialized and non-`Drop`: touching these from inside the allocator cannot itself
    // allocate (no lazy init, no TLS destructor to register).
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static ON: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

// SAFETY: every call delegates to `System` with the same `Layout` and returns its pointer
// unchanged; the hook only tallies the calling thread's successful allocations while its own flag
// is on. `dealloc` adds nothing.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && ON.with(|on| on.get()) {
            ALLOCS.with(|c| c.set(c.get() + 1));
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// Runs `f` on this thread, returning its value and the number of allocations this thread made
/// while it ran (allocations of other threads are not counted).
fn count<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ALLOCS.with(|c| c.set(0));
    ON.with(|on| on.set(true));
    let out = f();
    let n = ALLOCS.with(|c| c.get());
    ON.with(|on| on.set(false));
    (out, n)
}

/// The state of a process that never turned telemetry on: nothing was built, so nothing is there.
#[test]
fn off_leaves_no_provider_and_no_exporter() {
    assert_eq!(
        std::env::var("LAZYVIEW_OTEL").ok().as_deref(),
        None,
        "the test process must not run with telemetry switched on"
    );
    assert!(!otel::enabled(), "no provider");
    assert!(!otel::initialised(), "no exporter either");
    assert_eq!(otel::export_errors(), 0, "and nothing has failed");
    assert_eq!(otel::Switch::parse(None), otel::Switch::Off);
    // The instrumented paths behave as no-ops rather than as errors, and so does the exit path.
    let mut s = otel::span(otel::span::VIEW_FLUSH, |a| {
        a.str(otel::attr::VIEW_NAME, "v");
        a.u64(otel::attr::KEYS, 3);
    });
    assert!(!s.sampled());
    assert!(s.trace_parent().is_none());
    s.ok();
    s.fail("flush");
    otel::shutdown();
    assert!(!otel::enabled());
}

/// The count itself: the off path of every span helper, a thousand passes, allocates nothing.
#[test]
fn the_off_path_allocates_nothing() {
    assert!(!otel::enabled(), "this measures the cell being unset");
    // One pass is what a hot path does per source transaction and per merged delivery — and
    // *unguarded*: the helpers themselves must be free with the cell unset, not only the call sites
    // that test for it first. `span` runs its attribute closure only when tracing is on, so the
    // builder below is not even called.
    let pass = || {
        let mut s = otel::span(otel::span::BINLOG_TRANSACTION, |a| {
            a.str(otel::attr::SOURCE_POSITION, "mysql-bin.000003:4");
            a.u64(otel::attr::EVENTS, 4);
            a.links(&[]);
        });
        s.set_u64(otel::attr::ROWS, 9);
        s.set_str(otel::attr::SOURCE_GTID, "3e11:12");
        s.ok();
        let stamped = s.trace_parent();
        let mut child = otel::span(otel::span::VIEW_REFRESH, |a| {
            if let Some(p) = stamped {
                a.parent_from(&p);
                a.links(std::slice::from_ref(&p));
            }
        });
        child.fail("target");
        // The seam's own cost: the `None` a reader hands back when nothing was sampled.
        black_box(stamped.map(|p| p.to_header()));
        black_box(otel::export_errors());
    };
    // Warm up outside the counted window: a first call can pay a one-off (a `tracing` subscriber's
    // lazy state, a TLS slot) that is not the code's cost.
    for _ in 0..50 {
        pass();
    }
    let (_, n) = count(|| {
        for _ in 0..1_000 {
            pass();
        }
    });
    // BASELINE_OFF = 0, AFTER_OFF = 0: the number is zero on the unmodified code and must stay
    // zero (the pair the module docs ask for; there is no "before" this instrumentation because
    // these calls did not exist, so the baseline is the same call sequence with the cell unset).
    assert_eq!(
        n, 0,
        "the off path allocated {n} times in 1000 passes: the agreement's zero-cost guarantee \
         is broken (a disabled span handle must not build an attribute, a String or a Vec)"
    );
}

/// The structural half of the agreement: what a stamped parent is made of. It is `Copy` and inline,
/// so carrying one per transaction — or per merged delivery — is a fixed number of bytes in a place
/// that already exists: never a heap allocation, and never a growth of the dedup core's pending-key
/// entry, which lwf budgets at ~19 B/key.
#[test]
fn a_stamped_parent_is_inline_and_copy() {
    fn assert_copy<T: Copy + 'static>() {}
    assert_copy::<TraceParent>();
    assert_eq!(
        std::mem::size_of::<TraceParent>(),
        24,
        "two ids and nothing else: a `String`, a `Vec` or a `Box` in here would put telemetry's \
         cost on the dedup core's per-key budget"
    );
    assert!(
        std::mem::size_of::<Option<TraceParent>>() <= 32,
        "the reader's per-transaction field stays a couple of registers wide"
    );
    // The header is what leaves the process, and only where it does: 55 characters.
    let p = TraceParent::parse("00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01").unwrap();
    assert_eq!(p.to_header().len(), 55);
}
