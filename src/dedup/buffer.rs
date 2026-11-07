//! The per-target buffer of the dedup core: the exact ids per key space, the deferred items, and the
//! generation being worked off (keymap §4, proposal §6). Copy set: std, foldhash, `crate::datum`,
//! `crate::intset` and `super::key` only.
//!
//! [`Buffer::begin`] moves everything pending into a generation in O(1) (the per-space sets are
//! swapped, no id is copied). Jobs take bounded chunks from it ([`Buffer::take`]); ids marked meanwhile
//! go to the new pending sets and fire with the next generation. A generation pins its origin (the
//! replay position of its oldest release) until every chunk completed; retried ids go back to pending
//! with that origin. Exact ids only: no overflow, no saturation, no per-id ledger.
//!
//! Cost per mark: one hash insert (an integer id allocates nothing) and one `Option` test (the
//! origin is cloned once per pending cycle; marks come in binlog order, so the first is the oldest).

use std::collections::HashSet;
use std::hash::Hash;

use crate::datum::{Datum, Key as Tuple};
use crate::intset::{self, IntSet};

use super::key::{Item, Key, SpaceId};

/// A hash set of tuple ids (std `HashSet`, foldhash hasher).
pub type TupleSet = HashSet<Tuple, foldhash::fast::RandomState>;

/// The ids of one space: one-part integer ids in an [`IntSet`], the others in a [`TupleSet`].
#[derive(Default, Debug, Clone, PartialEq)]
pub struct IdSet {
    pub ints: IntSet,
    pub keys: TupleSet,
}

impl IdSet {
    /// `false`: the id was in the set already.
    pub fn insert(&mut self, k: Key) -> bool {
        match k {
            Key::Int(v) => self.insert_int(v),
            Key::Tuple(t) => self.insert_tuple(t),
        }
    }

    pub fn insert_int(&mut self, v: u64) -> bool {
        self.ints.insert(v)
    }

    /// A tuple id; a one-part `Datum::Int` tuple goes to the integers.
    pub fn insert_tuple(&mut self, t: Tuple) -> bool {
        match &*t {
            [Datum::Int(i)] => self.ints.insert(intset::from_i64(*i)),
            _ => self.keys.insert(t),
        }
    }

    pub fn contains(&self, k: &Key) -> bool {
        match k {
            Key::Int(v) => self.ints.contains(*v),
            Key::Tuple(t) => match &**t {
                [Datum::Int(i)] => self.ints.contains(intset::from_i64(*i)),
                _ => self.keys.contains(t),
            },
        }
    }

    pub fn remove(&mut self, k: &Key) -> bool {
        match k {
            Key::Int(v) => self.ints.remove(*v),
            Key::Tuple(t) => match &**t {
                [Datum::Int(i)] => self.ints.remove(intset::from_i64(*i)),
                _ => self.keys.remove(t),
            },
        }
    }

    pub fn len(&self) -> usize {
        self.ints.len() as usize + self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds every id of `other`.
    pub fn extend(&mut self, other: IdSet) {
        self.ints.union_with(other.ints);
        self.keys.extend(other.keys);
    }

    /// Removes and returns at most `n` ids: the smallest integers first, then tuples.
    pub fn take(&mut self, n: usize) -> IdSet {
        let mut out = IdSet::default();
        let ints = self.ints.pop_smallest(n);
        for v in ints {
            out.ints.insert(v);
        }
        let k = (n - out.len()).min(self.keys.len());
        let tuples: Vec<Tuple> = self.keys.iter().take(k).cloned().collect();
        for t in tuples {
            self.keys.remove(&t);
            out.keys.insert(t);
        }
        out
    }

    /// Every id: the integers ascending, then the tuples.
    pub fn to_keys(&self) -> Vec<Key> {
        let mut out: Vec<Key> = self
            .ints
            .to_sorted_vec()
            .into_iter()
            .map(Key::Int)
            .collect();
        out.extend(self.keys.iter().cloned().map(Key::Tuple));
        out
    }

    /// Estimated heap bytes: 16 per integer, 32 plus 40 per part per tuple.
    pub fn bytes(&self) -> usize {
        16 * self.ints.len() as usize + self.keys.iter().map(|t| 32 + 40 * t.len()).sum::<usize>()
    }
}

/// A chunk taken from the generation in flight.
#[derive(Debug)]
pub struct Taken<P, U> {
    /// The spaces with ids in the chunk, in space order.
    pub ids: Vec<(SpaceId, IdSet)>,
    pub items: Vec<Item<U>>,
    /// The replay position of the generation's oldest release (lazyview pins lagging targets there).
    pub origin: P,
    /// The generation's newest release: the caller fences on it.
    pub high: P,
}

#[derive(Debug)]
struct Generation<P, U> {
    sets: Vec<IdSet>,
    items: Vec<Item<U>>,
    origin: P,
    high: P,
    /// Chunks taken and not completed yet.
    out: u32,
}

/// One target's buffer.
#[derive(Debug)]
pub struct Buffer<P, U> {
    pending: Vec<IdSet>,
    items: HashSet<Item<U>, foldhash::fast::RandomState>,
    origin: Option<P>,
    high: Option<P>,
    flight: Option<Generation<P, U>>,
}

impl<P: Ord + Clone, U: Hash + Eq + Clone> Buffer<P, U> {
    /// An empty buffer of `spaces` key spaces.
    pub fn new(spaces: usize) -> Self {
        Self {
            pending: vec![IdSet::default(); spaces],
            items: HashSet::default(),
            origin: None,
            high: None,
            flight: None,
        }
    }

    /// Marks integer id `v` of space `s`; `origin`: the replay position of the transaction that
    /// marked it. `true`: the id was not pending.
    pub fn mark_int(&mut self, s: SpaceId, v: u64, origin: &P) -> bool {
        let r = self.pending[s.index()].insert_int(v);
        if self.origin.is_none() {
            self.origin = Some(origin.clone());
        }
        r
    }

    pub fn mark_tuple(&mut self, s: SpaceId, t: Tuple, origin: &P) -> bool {
        let r = self.pending[s.index()].insert_tuple(t);
        if self.origin.is_none() {
            self.origin = Some(origin.clone());
        }
        r
    }

    pub fn mark(&mut self, s: SpaceId, k: Key, origin: &P) -> bool {
        let r = self.pending[s.index()].insert(k);
        if self.origin.is_none() {
            self.origin = Some(origin.clone());
        }
        r
    }

    /// A deferred item; `true`: it was not pending.
    pub fn item(&mut self, i: Item<U>, origin: &P) -> bool {
        let r = self.items.insert(i);
        if self.origin.is_none() {
            self.origin = Some(origin.clone());
        }
        r
    }

    /// Lowers the pending origin to `origin` (retried ids, released XA transactions).
    pub fn pin(&mut self, origin: &P) {
        if self.origin.as_ref().is_none_or(|e| origin < e) {
            self.origin = Some(origin.clone());
        }
    }

    /// A transaction that marked ids here ended at `end` (the newest release).
    pub fn released(&mut self, end: &P) {
        if self.high.as_ref().is_none_or(|h| end > h) {
            self.high = Some(end.clone());
        }
    }

    /// Moves everything pending into a new generation (O(1)). `false`: one is in flight, or
    /// nothing is pending.
    pub fn begin(&mut self) -> bool {
        if self.flight.is_some() || self.pending_len() == 0 {
            return false;
        }
        let n = self.pending.len();
        let origin = self.origin.take().expect("pending ids carry their origin");
        let high = self.high.take().unwrap_or_else(|| origin.clone());
        self.flight = Some(Generation {
            sets: std::mem::replace(&mut self.pending, vec![IdSet::default(); n]),
            items: self.items.drain().collect(),
            origin,
            high,
            out: 0,
        });
        true
    }

    /// At most `ids` ids (spaces in order, integers ascending first) and `items` items of the
    /// generation. `None`: no generation, or nothing left in it.
    pub fn take(&mut self, ids: usize, items: usize) -> Option<Taken<P, U>> {
        let g = self.flight.as_mut()?;
        let mut out: Vec<(SpaceId, IdSet)> = Vec::new();
        let mut left = ids;
        for s in 0..g.sets.len() {
            if left == 0 {
                break;
            }
            let set = g.sets[s].take(left);
            let k = set.len();
            if !set.is_empty() {
                out.push((SpaceId(s as u16), set));
                left -= k;
            }
        }
        let k = items.min(g.items.len());
        let taken_items: Vec<Item<U>> = g.items.drain(..k).collect();
        if out.is_empty() && taken_items.is_empty() {
            return None;
        }
        g.out += 1;
        Some(Taken {
            ids: out,
            items: taken_items,
            origin: g.origin.clone(),
            high: g.high.clone(),
        })
    }

    /// A taken chunk completed: `retry` ids and `items` go back to pending with the generation's
    /// origin. The generation ends once nothing is left in it and every chunk completed.
    pub fn done(&mut self, retry: Vec<(SpaceId, IdSet)>, items: Vec<Item<U>>) {
        let origin = self
            .flight
            .as_ref()
            .expect("done follows a take")
            .origin
            .clone();
        let mut back = false;
        for (s, set) in retry {
            if !set.is_empty() {
                self.pending[s.index()].extend(set);
                back = true;
            }
        }
        if !items.is_empty() {
            back = true;
        }
        for i in items {
            self.items.insert(i);
        }
        if back {
            self.pin(&origin);
        }
        let ends = {
            let g = self.flight.as_mut().unwrap();
            g.out -= 1;
            g.out == 0 && g.sets.iter().all(|s| s.is_empty()) && g.items.is_empty()
        };
        if ends {
            self.flight = None;
        }
    }

    /// The oldest replay position this buffer pins.
    pub fn pinned(&self) -> Option<&P> {
        match (&self.flight, &self.origin) {
            (Some(g), Some(o)) => {
                if o <= &g.origin {
                    Some(o)
                } else {
                    Some(&g.origin)
                }
            }
            (Some(g), None) => Some(&g.origin),
            (None, o) => o.as_ref(),
        }
    }

    /// Ids and items pending (not in the generation).
    pub fn pending_len(&self) -> usize {
        self.pending.iter().map(|s| s.len()).sum::<usize>() + self.items.len()
    }

    /// Ids and items left in the generation (not taken yet).
    pub fn flight_len(&self) -> usize {
        match &self.flight {
            Some(g) => g.sets.iter().map(|s| s.len()).sum::<usize>() + g.items.len(),
            None => 0,
        }
    }

    pub fn in_flight(&self) -> bool {
        self.flight.is_some()
    }

    /// Estimated heap bytes of every id and item held (64 per item).
    pub fn bytes(&self) -> usize {
        let mut b: usize = self.pending.iter().map(|s| s.bytes()).sum();
        b += self.items.len() * 64;
        if let Some(g) = &self.flight {
            b += g.sets.iter().map(|s| s.bytes()).sum::<usize>() + g.items.len() * 64;
        }
        b
    }

    /// Everything held (pending and the generation's rest), per space, and the items. For a target
    /// being removed: the buffer is empty afterwards; `done` must not follow.
    pub fn drain(&mut self) -> (Vec<IdSet>, Vec<Item<U>>) {
        let n = self.pending.len();
        let mut items: Vec<Item<U>> = self.items.drain().collect();
        if let Some(g) = self.flight.take() {
            for (s, set) in self.pending.iter_mut().zip(g.sets) {
                s.extend(set);
            }
            items.extend(g.items);
        }
        let sets = std::mem::take(&mut self.pending);
        *self = Self::new(n);
        (sets, items)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{HopId, NoItem};

    use super::*;

    #[test]
    fn marks_dedup_and_pin_the_first_origin() {
        let s = SpaceId::FULL;
        let mut b = Buffer::<u64, NoItem>::new(1);
        assert!(b.mark_int(s, 5, &10));
        assert!(!b.mark_int(s, 5, &11));
        assert!(b.mark_int(s, 7, &12));
        assert_eq!(b.pinned(), Some(&10));
    }

    #[test]
    fn begin_swaps_and_new_marks_wait() {
        let s = SpaceId::FULL;
        let mut b = Buffer::<u64, NoItem>::new(1);
        for v in 1..=3u64 {
            assert!(b.mark_int(s, v, &10));
        }
        assert!(b.begin());
        assert!(b.mark_int(s, 4, &20));
        assert_eq!(b.flight_len(), 3);
        assert_eq!(b.pending_len(), 1);
        assert!(!b.begin());
    }

    #[test]
    fn take_is_bounded_ascending_and_complete() {
        let s = SpaceId::FULL;
        let mut b = Buffer::<u64, NoItem>::new(1);
        for v in 0..10_000u64 {
            assert!(b.mark_int(s, v, &1));
        }
        for t in 0..50u64 {
            assert!(b.mark_tuple(s, Box::new([Datum::UInt(t)]), &1));
        }
        assert!(b.begin());
        let mut ints: Vec<u64> = Vec::new();
        let mut tuples = 0;
        let mut prev: Option<u64> = None;
        let mut saw_tuples = false;
        let mut chunks = 0;
        while let Some(taken) = b.take(1000, 0) {
            let keys = taken.ids[0].1.to_keys();
            assert!(keys.len() <= 1000, "a chunk holds at most 1000 ids");
            if keys.first().is_none_or(|k| matches!(k, Key::Tuple(_))) {
                saw_tuples = true;
                tuples += keys.len();
            } else {
                assert!(!saw_tuples, "the tuples come after the integers");
                for k in keys {
                    let v = match k {
                        Key::Int(v) => v,
                        Key::Tuple(_) => panic!("a chunk mixes kinds"),
                    };
                    assert!(prev.is_none_or(|p| p < v), "integers ascend");
                    prev = Some(v);
                    ints.push(v);
                }
            }
            chunks += 1;
        }
        assert_eq!(chunks, 11);
        assert_eq!(tuples, 50);
        assert_eq!(
            ints,
            (0..10_000u64).collect::<Vec<u64>>(),
            "disjoint, complete"
        );
    }

    #[test]
    fn retry_keeps_the_generation_origin() {
        let s = SpaceId::FULL;
        let mut b = Buffer::<u64, NoItem>::new(1);
        assert!(b.mark_int(s, 1, &10));
        assert!(b.begin());
        let c1 = b.take(100, 0).unwrap();
        assert!(b.mark_int(s, 2, &30));
        b.done(c1.ids, vec![]);
        assert_eq!(
            b.pinned(),
            Some(&10),
            "the retry keeps the generation origin"
        );
        assert!(b.begin());
        let c2 = b.take(100, 0).unwrap();
        assert_eq!(c2.origin, 10, "the retried ids fire with the older origin");
        b.done(vec![], vec![]);
        assert!(b.mark_int(s, 3, &30));
        assert_eq!(b.pinned(), Some(&30));
        assert!(b.begin());
        let c3 = b.take(100, 0).unwrap();
        assert_eq!(c3.origin, 30);
        b.done(vec![], vec![]);
        assert_eq!(b.pinned(), None, "nothing pins once everything completed");
        assert!(!b.in_flight());
    }

    #[test]
    fn generation_unpins_after_every_chunk() {
        let s = SpaceId::FULL;
        let mut b = Buffer::<u64, NoItem>::new(1);
        for v in 1..=4u64 {
            assert!(b.mark_int(s, v, &7));
        }
        assert!(b.begin());
        let c1 = b.take(2, 0).unwrap();
        let c2 = b.take(2, 0).unwrap();
        assert_eq!(c1.origin, 7);
        assert_eq!(c2.origin, 7);
        b.done(vec![], vec![]);
        assert_eq!(
            b.pinned(),
            Some(&7),
            "the origin holds until every chunk completed"
        );
        b.done(vec![], vec![]);
        assert!(!b.in_flight());
        assert_eq!(b.pinned(), None);
    }

    #[test]
    fn items_dedup_and_take_bounded() {
        let mut b = Buffer::<u64, NoItem>::new(1);
        let first = Item::<NoItem>::Hop {
            hop: HopId(0),
            link: Box::new([Datum::Int(0)]),
        };
        assert!(b.item(first.clone(), &10));
        assert!(!b.item(first, &10));
        for i in 1..25u32 {
            let hop = Item::<NoItem>::Hop {
                hop: HopId(i),
                link: Box::new([Datum::Int(i as i64)]),
            };
            assert!(b.item(hop, &10));
        }
        assert_eq!(b.pending_len(), 25, "the duplicate hop counted once");
        assert!(b.begin());
        let mut chunks = 0;
        while let Some(taken) = b.take(0, 10) {
            assert!(taken.ids.is_empty());
            assert_eq!(taken.items.len(), 10.min(25 - chunks * 10));
            chunks += 1;
        }
        assert_eq!(chunks, 3, "25 items at 10 at a time give 3 chunks");
    }

    #[test]
    fn int_tuples_go_to_ints() {
        let mut set = IdSet::default();
        assert!(set.insert_tuple(Box::new([Datum::Int(3)])));
        assert!(set.contains(&Key::Int(intset::from_i64(3))));
        assert!(set.contains(&Key::Tuple(Box::new([Datum::Int(3)]))));
        assert!(set.remove(&Key::Int(intset::from_i64(3))));
        assert!(set.is_empty());
    }

    #[test]
    fn drain_returns_everything() {
        let s = SpaceId::FULL;
        let mut b = Buffer::<u64, NoItem>::new(1);
        for v in 1..=5u64 {
            assert!(b.mark_int(s, v, &10));
        }
        assert!(b.begin());
        let c = b.take(3, 0).unwrap();
        assert_eq!(c.ids[0].1.len(), 3);
        assert!(b.mark_int(s, 9, &20));
        let (sets, items) = b.drain();
        assert_eq!(sets.len(), 1);
        assert_eq!(
            sets[0].to_keys(),
            vec![Key::Int(4), Key::Int(5), Key::Int(9)]
        );
        assert!(items.is_empty());
        assert_eq!(b.pinned(), None, "the drained buffer pins nothing");
        assert!(!b.in_flight());
        assert_eq!(b.pending_len(), 0);
    }
}
