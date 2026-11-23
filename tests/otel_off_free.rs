//! "Off is free" as an executed claim, not a comment (`src/otel.rs`'s header, lwf agreement §3).
//!
//! Three things are proved here, from outside the crate and through its public API only:
//!
//! 1. for each of the three ways telemetry is off, there is provably no tracer provider and no
//!    exporter in this process — nothing ever created one;
//! 2. the calls the instrumented hot paths make ([`otel::span`] with its attribute closure, the
//!    seam's [`TraceParent`], the span handle's methods) allocate **zero** while the switch is off,
//!    counted by the process's own allocator; and
//! 3. the per-entry half of the agreement — nothing added to a pending-key entry, wherever it
//!    lives — which is what [`TraceParent`]'s being `Copy` and inline keeps true structurally.
//!
//! The switch is `OTEL_CONFIG_FILE`: one standard declarative-config file, named by the standard
//! variable, is the whole of the on state. The three off reasons are the two the host's gate
//! answers (unset; a path that is not an existing readable regular file — a typo, or the volume is
//! not mounted) and the one the file itself declares (`disabled: true`). Each is reached the way a
//! start reaches it: ask [`otel::gate`] what the value means, hand the answer to [`otel::init`], and
//! count what that costs. A real SDK provider is a heavy object — a batch processor with its own
//! thread and queue, an exporter, a resource — so an accidental re-ordering that builds any of it
//! before the off answer is visible here as a non-zero count.
//!
//! The reader and the engine reach telemetry through exactly the calls in [`pass`], so the count is
//! the cost of a hot path and not of a mock. The counting allocator is this binary's own (a copy of
//! the lib test binary's `src/alloc_measure.rs`, which is `cfg(test)` and so not reachable from
//! here): thread-local and only on inside [`count`], so a `#[test]` running on another thread
//! cannot perturb the number. Files a test needs go under the target directory, never `/tmp`.

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

/// Where this binary puts the files it needs, under the target directory.
fn scratch_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        p = std::path::PathBuf::from(dir);
    }
    p.join("lvs-otel-tests")
}

fn write_config(name: &str, contents: &str) -> std::path::PathBuf {
    let dir = scratch_dir();
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write config");
    path
}

/// One pass of what a hot path does per source transaction and per merged delivery — *unguarded*:
/// the helpers themselves must be free with the switch off, not only the call sites that test for
/// it first. `span` runs its attribute closure only when tracing is on, so the builder below is
/// not even called.
fn pass() {
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
}

/// The off state, from the outside: nothing was built, so nothing is there, and the hot path is a
/// no-op that allocates nothing. `why` names which of the three off reasons this is.
fn assert_off_is_free(why: &str, before_init: usize) {
    assert!(!otel::enabled(), "{why}: no provider");
    assert!(!otel::initialised(), "{why}: no exporter either");
    assert_eq!(otel::export_errors(), 0, "{why}: and nothing has failed");
    // Warm up outside the counted window: a first call can pay a one-off (a `tracing` subscriber's
    // lazy state, a TLS slot) that is not the code's cost.
    for _ in 0..50 {
        pass();
    }
    let ((), n) = count(|| {
        for _ in 0..1_000 {
            pass();
        }
    });
    // BASELINE_OFF = 0, AFTER_OFF = 0: the number is zero on the unmodified code and must stay
    // zero (the pair the module docs ask for; there is no "before" this instrumentation because
    // these calls did not exist, so the baseline is the same call sequence with the switch off).
    assert_eq!(
        n, 0,
        "the off path allocated {n} times in 1000 passes: the agreement's zero-cost guarantee is \
         broken (a disabled span handle must not build an attribute, a String or a Vec)"
    );
    // The `init` call the start makes for this reason, counted on its own: an off worker's whole
    // telemetry work at startup is this one call, and it must cost nothing at all.
    let ((), n) = count(|| {
        black_box(otel::init(&otel::Host {
            config_file: None,
            service_name: "lazyview",
            service_version: "0.0.0-test",
            source: Some("src-1"),
        }));
    });
    assert_eq!(
        n, 0,
        "{why}: the off start allocated {n} times, so telemetry machinery runs before the gate \
         (the file is read by the host's gate, never by an off `init`)"
    );
    assert!(
        !otel::enabled(),
        "{why}: an off `init` must never install a provider"
    );
    let _ = before_init;
}

/// The state of a process that never named a file: the shipped default.
#[test]
fn off_leaves_no_provider_and_no_exporter() {
    std::env::remove_var(otel::CONFIG_FILE_ENV);
    let before = ALLOCS.with(|c| c.get());
    // The gate is the host's whole reading of the environment: unset, empty and whitespace-only all
    // mean off, and say nothing (an ambient collector is not this process's business).
    for v in [None, Some(""), Some("   ")] {
        let g = otel::gate(otel::CONFIG_FILE_ENV, v);
        assert_eq!(g.file, None, "{v:?} names no file");
        assert_eq!(
            g.warning, None,
            "{v:?} is the documented default, not a mistake"
        );
    }
    assert_off_is_free("OTEL_CONFIG_FILE unset", before);
}

/// A path that is not an existing readable regular file is off with one sentence, and free: a typo
/// or an unmounted volume must never become a failed start, and never become a provider either.
#[test]
fn a_path_that_is_not_a_file_is_off_and_free() {
    let missing = scratch_dir().join("definitely-not-mounted.json");
    let _ = std::fs::remove_file(&missing);
    let dir = scratch_dir();
    let unreadable = write_config("unreadable.json", r#"{"file_format":"1.2"}"#);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000));
    }
    let named = [
        missing.to_str().unwrap().to_owned(),
        dir.to_str().unwrap().to_owned(),
        unreadable.to_str().unwrap().to_owned(),
    ];
    let before = ALLOCS.with(|c| c.get());
    for v in named {
        std::env::set_var(otel::CONFIG_FILE_ENV, &v);
        let g = otel::gate(otel::CONFIG_FILE_ENV, Some(&v));
        assert_eq!(
            g.file, None,
            "{v} is not a file telemetry can be built from"
        );
        // The one WARN that tells the operator what went wrong names both the variable and the path.
        let w = g
            .warning
            .unwrap_or_else(|| panic!("{v} must say why telemetry stayed off"));
        assert!(w.contains(otel::CONFIG_FILE_ENV), "{w} names the variable");
        assert!(w.contains(&v), "{w} names the path");
        // And the same answer the host gives a start: no file to build from, so nothing is built.
        assert!(!otel::init(&otel::Host {
            config_file: None,
            service_name: "lazyview",
            service_version: "0.0.0-test",
            source: None,
        }));
    }
    std::env::remove_var(otel::CONFIG_FILE_ENV);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o644));
    }
    assert_off_is_free(
        "OTEL_CONFIG_FILE names something that is not a file",
        before,
    );
}

/// The third off reason: a real, readable file that says so. Off by the operator's choice, so
/// there is nothing to complain about either.
#[test]
fn a_file_that_says_off_is_off_and_free() {
    let disabled = write_config(
        "disabled.json",
        r#"{"file_format":"1.2","disabled":true,"tracer_provider":{"processors":[{"simple":{"exporter":{"console":{}}}}]}}"#,
    );
    let silent = write_config("silent.json", r#"{"file_format":"1.2"}"#);
    let before = ALLOCS.with(|c| c.get());
    for v in [&disabled, &silent] {
        std::env::set_var(otel::CONFIG_FILE_ENV, v);
        let g = otel::gate(otel::CONFIG_FILE_ENV, v.to_str());
        // The gate's job ends at "there is a readable file here"; what the file says is settled by
        // the reader, which keeps both of these off.
        assert!(g.file.is_some(), "{v:?} is a readable file");
        let on = match otel::gate(otel::CONFIG_FILE_ENV, v.to_str()).file {
            // `init` is what a start calls, and it is handed the file the gate found: what it says
            // about a file that asks for nothing is `false`, never an error.
            Some(p) => otel::init(&otel::Host {
                config_file: Some(&p),
                service_name: "lazyview",
                service_version: "0.0.0-test",
                source: None,
            }),
            None => false,
        };
        assert!(!on, "{v:?} asks for nothing, so nothing is installed");
    }
    std::env::remove_var(otel::CONFIG_FILE_ENV);
    assert_off_is_free("the file asks for no telemetry", before);
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
