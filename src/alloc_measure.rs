//! Per-thread allocation counting, for the tests of the copy set's hot path (`src/dedup/`,
//! `src/intset.rs`). Declared `#[cfg(test)] mod alloc_measure;` from `lib.rs`, so the
//! `#[global_allocator]` below is compiled only into the lib's own test binary: the library and
//! the shipping binary keep the system allocator, and no non-test build sees this file.
//!
//! `count` runs a closure and reports how many allocations the *calling* thread made while it
//! ran, so `#[test]`s running in parallel on other threads cannot perturb the number. Build the
//! data under test outside the counted window and warm the function up first: a first call can
//! pay a one-off (a hash table's first allocation, a lazy seed) that is not the code's cost.
//!
//! The numbers the asserts pin are stated as a `BASELINE_*` (what the same scenario cost on the
//! unmodified code) and an `AFTER_*` (what it costs now) pair, so a future "optimisation" that
//! allocates again fails here instead of in a profile.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use crate::datum::Datum;
use crate::dedup::buffer::IdSet;
use crate::dedup::key::Key;

thread_local! {
    // Const-initialized and non-`Drop`: touching these from inside the allocator cannot itself
    // allocate (no lazy init, no TLS destructor to register).
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static ON: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

// SAFETY: every call delegates to `System` with the same `Layout` and returns its pointer
// unchanged; the hook only tallies the calling thread's successful allocations while its own
// flag is on. `dealloc` adds nothing.
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

/// Reports a measured cost, so `cargo test --lib alloc_measure -- --nocapture` shows every
/// scenario's number next to the constants that pin it.
fn report(what: &str, n: usize, baseline: &str) {
    println!("alloc_measure: {what} = {n} allocations (unmodified code: {baseline})");
}

/// The size of every `IdSet` scenario: bounded, like a worker's chunk budget.
const N: usize = 64;

/// The integer ids of a scenario, in the `IntSet`'s own encoding (`from_i64`, which keeps the
/// `u64` order equal to the `i64` order).
fn ids(n: usize) -> Vec<u64> {
    (1..=n as i64).map(crate::intset::from_i64).collect()
}

/// `n` integer ids, `1..=n`.
fn ints(n: usize) -> IdSet {
    let mut s = IdSet::default();
    for v in ids(n) {
        s.insert_int(v);
    }
    s
}

/// `n` tuple ids, `b"t0"` .. `b"t{n-1}"` (a one-part `Bytes` id, so none of them lands in the
/// integers).
fn tuples(n: usize) -> IdSet {
    let mut s = IdSet::default();
    for i in 0..n {
        assert!(s.insert_tuple(Box::new([Datum::Bytes(format!("t{i}").into_bytes())])));
    }
    s
}

// ---------------------------------------------------------------- the two measured fixes ----

/// `Key::from_text` proved canonicity with `n.to_string()`, which allocates a `String` for every
/// key that parses as `i64`; the fix checks the text's shape instead. Measured on the unmodified
/// base (origin/main a9f03c1) with the scenario below: one allocation per call, none after.
const BASELINE_FROM_TEXT: usize = 1; // measured on origin/main a9f03c1: the `to_string` per key
const AFTER_FROM_TEXT: usize = 0;

/// `IdSet::take` moved the integers through a `Vec<u64>` from `IntSet::pop_smallest` and
/// re-inserted them one by one, so taking a whole space cost that `Vec` plus every hash insert of
/// `out.ints`. Baseline / after, for taking all `N` integers of one space.
const BASELINE_TAKE_INTS: usize = 8; // measured on origin/main a9f03c1: the pop_smallest Vec +
                                     // every hash insert of `out.ints`
const AFTER_TAKE_INTS: usize = 0;

/// `IdSet::take` cloned every tuple into an intermediate `Vec` before removing and re-inserting
/// it, so taking a whole space cost the `Vec` plus a `Box<[Datum]>` and the `Bytes` payload per
/// id. Baseline / after, for taking all `N` tuple ids of one space.
const BASELINE_TAKE_TUPLES: usize = 135; // measured on origin/main a9f03c1: the intermediate Vec
                                         // plus a `Box<[Datum]>` and a `Bytes` payload per id
const AFTER_TAKE_TUPLES: usize = 0;

/// The partial paths (`n` below the space's size) keep their copies: the `pop_smallest` `Vec`,
/// and the intermediate `Vec<Tuple>` the set forces (dropping the latter needs mutation while
/// iterating, which would change when the ids leave the set). Both still measure what they measured
/// before the change, 7 and 70: pinned here so that stays true.
const BASELINE_TAKE_PARTIAL_INTS: usize = 7; // measured on origin/main a9f03c1
const BASELINE_TAKE_PARTIAL_TUPLES: usize = 70; // measured on origin/main a9f03c1

// The reductions themselves, checked at compile time: a change that stops these paths from being
// cheaper than the unmodified code fails to build, not just to run.
const _: () = assert!(AFTER_FROM_TEXT < BASELINE_FROM_TEXT);
const _: () = assert!(AFTER_TAKE_INTS < BASELINE_TAKE_INTS);
const _: () = assert!(AFTER_TAKE_TUPLES < BASELINE_TAKE_TUPLES);

#[test]
fn from_text_numeric_path_allocates_nothing() {
    for s in ["42", "-7", "0"] {
        black_box(Key::from_text(s)); // warm-up: outside the counted window
        let (k, n) = count(|| black_box(Key::from_text(black_box(s))));
        assert!(matches!(k, Key::Int(_)), "{s:?} must stay an Int key");
        // The whole cost of the numeric path was the proof of canonicity; the same call must now
        // touch the allocator not at all.
        assert_eq!(
            n, AFTER_FROM_TEXT,
            "from_text({s:?}): {n} allocations (expected {AFTER_FROM_TEXT}, \
             unmodified baseline {BASELINE_FROM_TEXT})"
        );
        report(
            &format!("from_text({s:?}) numeric"),
            n,
            "1 (the `to_string`)",
        );
    }
    // The non-canonical arm keeps its `Vec` + `Box`: the counter must still see real work, or the
    // zero above would only prove the harness is deaf.
    let (k, n) = count(|| black_box(Key::from_text(black_box("A-1"))));
    assert!(matches!(k, Key::Tuple(_)), "\"A-1\" must stay a Tuple key");
    assert!(
        n >= 2,
        "the Bytes arm must still allocate its Vec + Box, got {n}"
    );
    report("from_text(\"A-1\") Bytes arm", n, "2 (the Vec + Box)");
}

#[test]
fn take_all_ints_allocates_less() {
    let mut set = ints(N);
    black_box(set.take(1)); // warm-up
    let mut set = ints(N);
    let (out, n) = count(|| black_box(set.take(N)));
    assert!(
        n < BASELINE_TAKE_INTS,
        "IdSet::take all {N} ints: {n} allocations, unmodified baseline {BASELINE_TAKE_INTS} \
         (the pop_smallest Vec plus a hash insert per id) — taking a whole space must move its set"
    );
    assert_eq!(
        n, AFTER_TAKE_INTS,
        "IdSet::take all {N} ints: {n} allocations, expected {AFTER_TAKE_INTS} (a move)"
    );
    report("IdSet::take all ints", n, "8");
    assert!(set.is_empty(), "a take-all leaves the space empty");
    assert_eq!(out.keys.len(), 0);
    assert_eq!(
        out.ints.to_sorted_vec(),
        ids(N),
        "the same integers, ascending"
    );
}

#[test]
fn take_all_tuples_allocates_less() {
    let mut set = tuples(N);
    black_box(set.take(1)); // warm-up
    let mut set = tuples(N);
    let (out, n) = count(|| black_box(set.take(N)));
    assert!(
        n < BASELINE_TAKE_TUPLES,
        "IdSet::take all {N} tuples: {n} allocations, unmodified baseline {BASELINE_TAKE_TUPLES} \
         (the intermediate Vec plus a boxed slice and a payload per id) — taking a whole space \
         must move its set"
    );
    assert_eq!(
        n, AFTER_TAKE_TUPLES,
        "IdSet::take all {N} tuples: {n} allocations, expected {AFTER_TAKE_TUPLES} (a move)"
    );
    report("IdSet::take all tuples", n, "135");
    assert!(set.is_empty(), "a take-all leaves the space empty");
    assert_eq!(out.ints.len(), 0);
    assert_eq!(out.keys.len(), N, "every tuple id moved out");
}

#[test]
fn take_partial_ints_keeps_its_cost() {
    let mut set = ints(N);
    let (out, n) = count(|| black_box(set.take(N / 2)));
    assert!(
        n >= 1,
        "the partial integer path still pays the pop_smallest Vec, got {n}"
    );
    assert!(
        n <= BASELINE_TAKE_PARTIAL_INTS,
        "IdSet::take {}/{} ints: {n} allocations regressed against the baseline {}",
        N / 2,
        N,
        BASELINE_TAKE_PARTIAL_INTS,
    );
    report("IdSet::take half of the ints", n, "7");
    assert_eq!(
        out.ints.to_sorted_vec(),
        ids(N / 2),
        "the smallest half, ascending"
    );
    assert_eq!(
        set.ints.to_sorted_vec(),
        ids(N)[N / 2..].to_vec(),
        "the rest stays pending for the next chunk"
    );
}

#[test]
fn take_partial_tuples_keeps_its_cost() {
    let mut set = tuples(N);
    let (out, n) = count(|| black_box(set.take(N / 2)));
    assert!(
        n >= N / 2,
        "the partial tuple path still clones each id out and back in, got {n}"
    );
    assert!(
        n <= BASELINE_TAKE_PARTIAL_TUPLES,
        "IdSet::take {}/{} tuples: {n} allocations regressed against the baseline {}",
        N / 2,
        N,
        BASELINE_TAKE_PARTIAL_TUPLES,
    );
    report("IdSet::take half of the tuples", n, "70");
    assert_eq!(out.keys.len(), N / 2);
    assert_eq!(set.keys.len(), N / 2);
    let mut left = out.keys.clone();
    left.extend(set.keys.clone());
    assert_eq!(left, tuples(N).keys, "nothing lost and nothing duplicated");
}

/// The contract `Buffer::take` (and with it the worker's chunk drain at `Core::due`) relies on:
/// at most `n` ids, the integers ascending first, then tuples; a budget that covers only the
/// integers leaves the tuples for the next chunk. Pinned for both the moved and the copied paths.
#[test]
fn take_hands_out_the_same_ids_as_before() {
    let mut set = ints(N);
    for i in 0..N {
        assert!(set.insert_tuple(Box::new([Datum::Bytes(format!("u{i}").into_bytes())])));
    }
    assert_eq!(set.len(), 2 * N);

    assert!(set.take(0).is_empty(), "a zero budget takes nothing");

    // A budget of exactly the integers: they go, the tuples wait.
    let out = set.take(N);
    assert_eq!(out.ints.len() as usize, N);
    assert_eq!(out.keys.len(), 0, "no tuple id crosses the budget");
    let keys = out.to_keys();
    assert_eq!(
        keys.iter()
            .map(|k| match k {
                Key::Int(u) => crate::intset::to_i64(*u),
                Key::Tuple(_) => unreachable!("an integer id"),
            })
            .collect::<Vec<i64>>(),
        (1..=N as i64).collect::<Vec<i64>>(),
        "integers ascending"
    );

    // The next chunk takes the tuples.
    let out = set.take(N);
    assert_eq!(out.keys.len(), N);
    assert!(set.is_empty());
    assert!(set.take(N).is_empty(), "an empty space hands out nothing");

    // A mixed budget: half the integers, and the tuples only if the integers run out.
    let mut set = ints(N);
    for i in 0..N {
        set.insert_tuple(Box::new([Datum::Bytes(format!("u{i}").into_bytes())]));
    }
    let out = set.take(N / 2);
    assert_eq!(out.ints.len() as usize, N / 2);
    assert_eq!(out.keys.len(), 0);
    let out = set.take(2 * N);
    assert_eq!(out.len(), 3 * N / 2, "everything that is left");
    assert!(set.is_empty());
}
