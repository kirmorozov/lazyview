//! Shared integer-set module (COPY SET).
//!
//! A set of `u64` integer keys, with order-preserving `i64 <-> u64`
//! conversions, so signed integer ids can be stored without distorting
//! their natural order.
//!
//! Two backends, picked at compile time:
//!
//! - default: a plain `HashSet<u64, foldhash::fast::RandomState>` plus a
//!   sorted cache that stays fresh across inserts: `sorted` holds the
//!   set's elements in DESCENDING order (the smallest sits at the end),
//!   and `pending` collects the values inserted while the cache is fresh,
//!   so a batch of the smallest keys pops from the two sorted runs
//!   without re-sorting the whole set. Only `remove` and `clear` drop the
//!   cache; the next pop rebuilds it with a single sort. The benchmark
//!   (lazyview `tmp/out/lv-bench-sets/report.md`) showed it beats roaring
//!   except on dense id ranges; it is the general choice.
//! - the `roaring` feature: a `roaring::RoaringTreemap`, meant for dense id
//!   ranges. `pop_smallest` removes each taken key individually:
//!   lazy-wire-fire measured `remove_range` at about 155 us per key on 1M
//!   sparse ids, so it is never used.
//!
//! Owner decision 2026-11-04: integer key sets default to the plain hash
//! set; roaring stays available behind the Cargo feature `roaring`.
//!
//! This module is in lazyview's COPY SET: lazy-wire-fire copies it byte for
//! byte and declares the same `roaring` feature (as
//! `roaring = ["dep:roaring"]`). The `cfg(feature = "roaring")` appears only
//! in this file. It depends on std, foldhash and (under the feature)
//! roaring only: no intra-crate imports, no logging, no env.

#[cfg(not(feature = "roaring"))]
use std::collections::{BinaryHeap, HashSet};

/// Maps an `i64` to a `u64` with the sign flipped, so the natural order of
/// the `u64` is the natural order of the original `i64`.
pub fn from_i64(i: i64) -> u64 {
    (i as u64) ^ (1 << 63)
}

/// Inverse of [`from_i64`].
pub fn to_i64(u: u64) -> i64 {
    (u ^ (1 << 63)) as i64
}

#[cfg(not(feature = "roaring"))]
#[derive(Default, Clone, Debug)]
pub struct IntSet {
    set: HashSet<u64, foldhash::fast::RandomState>,
    /// The set's elements sorted DESCENDING, so the smallest sits at the
    /// end.
    sorted: Vec<u64>,
    /// Values inserted while the cache is fresh; its `peek` is their
    /// minimum.
    pending: BinaryHeap<std::cmp::Reverse<u64>>,
    /// True only while `set` equals the elements of `sorted` plus the
    /// elements of `pending`, no duplicates.
    fresh: bool,
}

#[cfg(feature = "roaring")]
#[derive(Default, Clone, Debug)]
pub struct IntSet {
    set: roaring::RoaringTreemap,
}

#[cfg(not(feature = "roaring"))]
impl IntSet {
    /// Creates an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts `v`. Returns true when the value was new.
    pub fn insert(&mut self, v: u64) -> bool {
        let new = self.set.insert(v);
        if new && self.fresh {
            self.pending.push(std::cmp::Reverse(v));
        }
        new
    }

    /// Reports whether `v` is in the set.
    pub fn contains(&self, v: u64) -> bool {
        self.set.contains(&v)
    }

    /// Removes `v`. Returns true when the value was present.
    pub fn remove(&mut self, v: u64) -> bool {
        let removed = self.set.remove(&v);
        if removed {
            // A removal can invalidate either run; drop the cache.
            self.fresh = false;
            self.sorted.clear();
            self.pending.clear();
        }
        removed
    }

    /// The number of elements.
    pub fn len(&self) -> u64 {
        self.set.len() as u64
    }

    /// Whether the set has no elements.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Removes every element.
    pub fn clear(&mut self) {
        self.set.clear();
        self.sorted.clear();
        self.pending.clear();
        self.fresh = false;
    }

    /// Adds every element of `other` (consumed) to the set.
    pub fn union_with(&mut self, other: IntSet) {
        for v in other.set {
            let new = self.set.insert(v);
            if new && self.fresh {
                self.pending.push(std::cmp::Reverse(v));
            }
        }
    }

    /// The elements, ascending.
    pub fn to_sorted_vec(&self) -> Vec<u64> {
        if self.fresh {
            // One linear merge of two ascending runs: `sorted` walked from
            // its end, and the pending values sorted once.
            let mut pending: Vec<u64> = self.pending.iter().map(|r| r.0).collect();
            pending.sort_unstable();
            let mut out = Vec::with_capacity(self.sorted.len() + pending.len());
            let mut p = 0usize;
            let mut s = self.sorted.len();
            while p < pending.len() && s > 0 {
                if pending[p] <= self.sorted[s - 1] {
                    out.push(pending[p]);
                    p += 1;
                } else {
                    out.push(self.sorted[s - 1]);
                    s -= 1;
                }
            }
            out.extend_from_slice(&pending[p..]);
            out.extend(self.sorted[..s].iter().rev());
            out
        } else {
            let mut v = self.set.iter().copied().collect::<Vec<u64>>();
            v.sort_unstable();
            v
        }
    }

    /// Removes and returns the `min(n, len())` smallest elements, ascending.
    ///
    /// A fresh cache pops each key from the smaller end of its two sorted
    /// runs (`sorted` and `pending`); a stale cache rebuilds with one sort
    /// first. Never a full re-sort or partition per batch when `n < len()`.
    pub fn pop_smallest(&mut self, n: usize) -> Vec<u64> {
        let k = n.min(self.set.len());
        if k == 0 {
            return Vec::new();
        }
        if !self.fresh {
            // One sort rebuilds the cache; `pending` is empty whenever
            // stale.
            self.sorted = self.set.iter().copied().collect();
            self.sorted.sort_unstable();
            self.sorted.reverse();
            self.fresh = true;
        }
        let mut out = Vec::with_capacity(k);
        for _ in 0..k {
            // The smaller of the two current minima; both runs are read
            // ascending, so `out` stays ascending by construction.
            let sorted_wins = match (self.sorted.last(), self.pending.peek()) {
                (Some(a), Some(b)) => *a <= b.0,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => unreachable!("a fresh cache covers the set"),
            };
            let v = if sorted_wins {
                self.sorted.pop().unwrap()
            } else {
                self.pending.pop().unwrap().0
            };
            self.set.remove(&v);
            out.push(v);
        }
        out
    }
}

#[cfg(not(feature = "roaring"))]
impl PartialEq for IntSet {
    /// By contents only; the cache is ignored.
    fn eq(&self, other: &Self) -> bool {
        self.set.len() == other.set.len() && self.set.iter().all(|v| other.set.contains(v))
    }
}

#[cfg(feature = "roaring")]
impl IntSet {
    /// Creates an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts `v`. Returns true when the value was new.
    pub fn insert(&mut self, v: u64) -> bool {
        self.set.insert(v)
    }

    /// Reports whether `v` is in the set.
    pub fn contains(&self, v: u64) -> bool {
        self.set.contains(v)
    }

    /// Removes `v`. Returns true when the value was present.
    pub fn remove(&mut self, v: u64) -> bool {
        self.set.remove(v)
    }

    /// The number of elements.
    pub fn len(&self) -> u64 {
        self.set.len()
    }

    /// Whether the set has no elements.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Removes every element.
    pub fn clear(&mut self) {
        self.set.clear()
    }

    /// Adds every element of `other` (consumed) to the set.
    pub fn union_with(&mut self, other: IntSet) {
        self.set |= other.set;
    }

    /// The elements, ascending.
    pub fn to_sorted_vec(&self) -> Vec<u64> {
        self.set.iter().collect()
    }

    /// Removes and returns the `min(n, len())` smallest elements, ascending.
    ///
    /// Each taken key is removed individually: lazy-wire-fire measured
    /// `remove_range` at about 155 us per key on 1M sparse ids.
    pub fn pop_smallest(&mut self, n: usize) -> Vec<u64> {
        let out: Vec<u64> = self.set.iter().take(n).collect();
        for v in &out {
            self.set.remove(*v);
        }
        out
    }
}

#[cfg(feature = "roaring")]
impl PartialEq for IntSet {
    /// By contents only.
    fn eq(&self, other: &Self) -> bool {
        self.set == other.set
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Fixed-seed SplitMix64, so no rand crate is needed.
    struct SplitMix64(u64);

    impl SplitMix64 {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF5A_45C1_37B4_B8B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    #[test]
    fn i64_round_trip_and_order() {
        for i in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert_eq!(to_i64(from_i64(i)), i);
        }
        let us = [
            from_i64(i64::MIN),
            from_i64(-1),
            from_i64(0),
            from_i64(1),
            from_i64(i64::MAX),
        ];
        assert!(us.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn partial_eq_ignores_cache() {
        let mut a = IntSet::new();
        for v in 1..=10 {
            a.insert(v);
        }
        a.pop_smallest(5); // a holds 6..=10; on the hash backend its cache stays fresh here
        let mut b = IntSet::new();
        for v in 6..=10 {
            b.insert(v);
        }
        assert_eq!(a, b);
        assert_eq!(b, a);
        a.insert(0);
        assert_ne!(a, b);
    }

    #[test]
    fn pops_interleaved_with_inserts() {
        let mut s = IntSet::new();
        let mut refset: BTreeSet<u64> = BTreeSet::new();
        for v in 0..100 {
            s.insert(v);
            refset.insert(v);
        }
        let got = s.pop_smallest(50);
        assert_eq!(got, (0..50).collect::<Vec<u64>>());
        for v in &got {
            refset.remove(v);
        }
        // Insert values smaller and larger than the remaining minimum (50).
        for v in [3, 17, 500] {
            s.insert(v);
            refset.insert(v);
        }
        let got = s.pop_smallest(7);
        assert_eq!(got, refset.iter().take(7).copied().collect::<Vec<u64>>());
        assert_eq!(got, [3, 17, 50, 51, 52, 53, 54]);
        for v in &got {
            refset.remove(v);
        }
        s.insert(0);
        refset.insert(0);
        assert_eq!(
            s.to_sorted_vec(),
            refset.iter().copied().collect::<Vec<u64>>()
        );
        assert_eq!(s.len(), refset.len() as u64);
        let got = s.pop_smallest(s.len() as usize + 1);
        assert_eq!(got, refset.iter().copied().collect::<Vec<u64>>());
        refset.clear();
        assert!(s.is_empty());
        assert_eq!(s.pop_smallest(5), Vec::<u64>::new());
    }

    #[cfg(not(feature = "roaring"))]
    #[test]
    fn cache_freshness_tracks_pending() {
        let mut s = IntSet::new();
        s.insert(5);
        s.insert(1);
        assert!(!s.fresh);
        assert_eq!(s.pop_smallest(1), [1]);
        assert!(s.fresh);
        assert_eq!(s.sorted, [5]);
        assert!(s.pending.is_empty());
        s.insert(3);
        assert!(s.fresh); // the value lands in `pending`, cache stays fresh
        assert_eq!(s.pending.len(), 1);
        assert_eq!(s.pending.peek(), Some(&std::cmp::Reverse(3)));
        s.remove(3);
        assert!(!s.fresh);
        assert!(s.sorted.is_empty());
        assert!(s.pending.is_empty());
        assert!(s.contains(5));
        assert_eq!(s.to_sorted_vec(), [5]);
    }

    #[test]
    fn random_operation_sequences() {
        let mut rng = SplitMix64(0x0123_4567_89AB_CDEF);
        let mut s = IntSet::new();
        let mut refset: BTreeSet<u64> = BTreeSet::new();
        const OPS: u64 = 3000;
        for op in 0..OPS {
            let v = rng.next_u64() % 2000;
            match op % 11 {
                0..=3 => assert_eq!(s.insert(v), refset.insert(v)),
                4..=6 => assert_eq!(s.remove(v), refset.remove(&v)),
                7 => assert_eq!(s.contains(v), refset.contains(&v)),
                8 => {
                    assert_eq!(s.len(), refset.len() as u64);
                    assert_eq!(s.is_empty(), refset.is_empty());
                }
                9 => {
                    let mut s_other = IntSet::new();
                    s_other.insert(rng.next_u64() % 2000);
                    s_other.insert(rng.next_u64() % 2000);
                    let other = s_other.to_sorted_vec();
                    for w in &other {
                        refset.insert(*w);
                    }
                    s.union_with(s_other);
                }
                _ => {
                    let n = match (rng.next_u64() % 5) as usize {
                        0 => 1,
                        1 => 7,
                        2 => 500,
                        3 => refset.len(),
                        _ => refset.len() + 1,
                    };
                    let want: Vec<u64> = refset.iter().take(n).copied().collect();
                    for w in &want {
                        refset.remove(w);
                    }
                    assert_eq!(s.pop_smallest(n), want);
                    assert_eq!(s.len(), refset.len() as u64);
                }
            }
        }
        assert_eq!(
            s.to_sorted_vec(),
            refset.iter().copied().collect::<Vec<u64>>()
        );
    }

    #[test]
    fn clear_empties_and_reinserts() {
        let mut s = IntSet::new();
        for v in [1, 2, 3] {
            s.insert(v);
        }
        s.remove(2);
        s.clear();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert!(!s.contains(1));
        assert!(!s.contains(3));
        assert_eq!(s.to_sorted_vec(), Vec::<u64>::new());
        assert_eq!(s.pop_smallest(3), Vec::<u64>::new());
        // The set is usable after clear.
        s.insert(5);
        s.insert(1);
        assert_eq!(s.to_sorted_vec(), [1, 5]);
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn partial_eq_equal_len_differs_in_content() {
        let mut a = IntSet::new();
        a.insert(1);
        a.insert(2);
        let mut b = IntSet::new();
        b.insert(3);
        b.insert(4);
        // Same length, different contents: not equal either way.
        assert_ne!(a, b);
        assert_ne!(b, a);
        // Same contents, different cache state: still equal.
        let mut c = IntSet::new();
        for v in [0, 1, 2] {
            c.insert(v);
        }
        c.pop_smallest(1); // c now holds 1 and 2
        assert_eq!(a, c);
        assert_eq!(c, a);
        // Different lengths: not equal.
        let mut d = IntSet::new();
        for v in [1, 2, 3] {
            d.insert(v);
        }
        assert_ne!(a, d);
        assert_ne!(d, a);
    }

    #[test]
    fn sorted_vec_fresh_with_empty_pending() {
        let mut s = IntSet::new();
        s.insert(2);
        s.insert(1);
        assert_eq!(s.pop_smallest(1), [1]);
        // The cache is now fresh with an empty pending run; the merge
        // loop must stop exactly at that run's end.
        assert_eq!(s.to_sorted_vec(), [2]);
        s.insert(0);
        assert_eq!(s.to_sorted_vec(), [0, 2]);
    }
}
