//! The hop cache of the dedup core (keymap §5, decision 4): per hop, the ids each link value reaches
//! through the hop's chain, filled from the action's answers to deferred hop items. Copy set: std,
//! foldhash, `crate::datum` and `super::key` only.
//!
//! An answer is cached only when no event invalidated its link value between the job's take and its
//! completion (a flag per link value, kept only while an item for it is in flight); empty answers
//! are cached too. A row change of a chain table evicts the entries of its old and new partner values
//! when every partner column is on that table's alias, and clears the hop's entries otherwise. At the
//! entry bound every entry is dropped: misses go back to the batched hop query, never a rebuild.

use std::collections::HashMap;

use crate::datum::{Datum, Key as Tuple};

use super::key::{HopId, Key, SpaceId};

type Map<V> = HashMap<Tuple, V, foldhash::fast::RandomState>;

/// One hop: the occurrence it belongs to and how its rows and its chain reach the cache.
#[derive(Debug, Clone, PartialEq)]
pub struct HopPlan {
    /// The target slot, the plan index within the target, and the occurrence's alias.
    pub target: usize,
    pub plan: usize,
    pub alias: usize,
    /// The space of the id positions the hop reaches.
    pub space: SpaceId,
    /// The occurrence's link columns (`Hop.link_cols`).
    pub link_cols: Vec<usize>,
    /// The chain's aliases (`Hop.aliases`).
    pub chain: Vec<usize>,
    /// When every partner column is on one chain alias: that alias and its columns in link order.
    pub partner: Option<(usize, Vec<usize>)>,
}

/// Every hop of a core and its cache.
#[derive(Debug)]
pub struct Hops {
    plans: Vec<Option<HopPlan>>,
    cache: Vec<Map<Vec<Key>>>,
    /// Per hop, the link values in flight; `true`: invalidated since taken.
    flight: Vec<Map<bool>>,
    entries: usize,
    bound: usize,
    /// Times the whole cache was dropped at the bound.
    pub clears: u64,
}

impl Hops {
    /// An empty table; at more than `bound` entries the cache is dropped.
    pub fn new(bound: usize) -> Self {
        Self {
            plans: Vec::new(),
            cache: Vec::new(),
            flight: Vec::new(),
            entries: 0,
            bound,
            clears: 0,
        }
    }

    pub fn add(&mut self, p: HopPlan) -> HopId {
        let i = self.plans.len();
        self.plans.push(Some(p));
        self.cache.push(Map::default());
        self.flight.push(Map::default());
        HopId(i as u32)
    }

    /// Drops target `t`'s hops (their ids are not reused).
    pub fn remove_target(&mut self, t: usize) {
        for i in 0..self.plans.len() {
            if self.plans[i].as_ref().is_some_and(|p| p.target == t) {
                self.plans[i] = None;
                self.entries -= self.cache[i].len();
                self.cache[i].clear();
                self.flight[i].clear();
            }
        }
    }

    pub fn plan(&self, h: HopId) -> Option<&HopPlan> {
        self.plans.get(h.0 as usize)?.as_ref()
    }

    /// The cached ids of `link` (`None`: a miss).
    pub fn get(&self, h: HopId, link: &[Datum]) -> Option<&[Key]> {
        self.cache[h.0 as usize].get(link).map(Vec::as_slice)
    }

    /// A job took the item of `link`.
    pub fn taken(&mut self, h: HopId, link: &Tuple) {
        self.flight[h.0 as usize].insert(link.clone(), false);
    }

    /// A partner value changed: its entry goes, and an answer in flight for it is not cached.
    pub fn evict(&mut self, h: HopId, link: &[Datum]) {
        if self.cache[h.0 as usize].remove(link).is_some() {
            self.entries -= 1;
        }
        if let Some(f) = self.flight[h.0 as usize].get_mut(link) {
            *f = true;
        }
    }

    /// A chain row changed that is not the partner alias's: every entry of `h` goes.
    pub fn clear(&mut self, h: HopId) {
        let h = h.0 as usize;
        self.entries -= self.cache[h].len();
        self.cache[h].clear();
        for f in self.flight[h].values_mut() {
            *f = true;
        }
    }

    /// The answer to a taken link; cached unless invalidated meanwhile. `true`: cached.
    pub fn answer(&mut self, h: HopId, link: Tuple, ids: Vec<Key>) -> bool {
        let h = h.0 as usize;
        match self.flight[h].remove(&link) {
            Some(false) => {
                self.cache[h].insert(link, ids);
                self.entries += 1;
                if self.entries > self.bound {
                    for c in &mut self.cache {
                        c.clear();
                    }
                    self.entries = 0;
                    self.clears += 1;
                }
                true
            }
            _ => false,
        }
    }

    /// The item of `link` failed: it is no longer in flight.
    pub fn forget(&mut self, h: HopId, link: &Tuple) {
        self.flight[h.0 as usize].remove(link);
    }

    /// Cached entries.
    pub fn len(&self) -> usize {
        self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Estimated heap bytes: 48 plus 40 per link part plus 24 per id.
    pub fn bytes(&self) -> usize {
        self.cache
            .iter()
            .flat_map(|m| m.iter())
            .map(|(link, ids)| 48 + 40 * link.len() + 24 * ids.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Key, SpaceId};

    use super::*;

    fn plan(target: usize) -> HopPlan {
        HopPlan {
            target,
            plan: 0,
            alias: 0,
            space: SpaceId::FULL,
            link_cols: vec![0],
            chain: vec![1],
            partner: Some((1, vec![0])),
        }
    }

    fn link(v: i64) -> Tuple {
        Box::new([Datum::Int(v)])
    }

    #[test]
    fn hits_after_an_answer() {
        let mut hops = Hops::new(10);
        let h = hops.add(plan(0));
        let l = link(1);
        assert_eq!(hops.get(h, &l), None);
        hops.taken(h, &l);
        assert!(hops.answer(h, l.clone(), vec![Key::Int(7)]));
        assert_eq!(hops.get(h, &l), Some(&[Key::Int(7)][..]));
        assert_eq!(hops.len(), 1);
        assert!(!hops.is_empty());
        assert_eq!(hops.bytes(), 48 + 40 + 24);
    }

    #[test]
    fn empty_answers_are_cached() {
        let mut hops = Hops::new(10);
        let h = hops.add(plan(0));
        let l = link(2);
        hops.taken(h, &l);
        assert!(hops.answer(h, l.clone(), vec![]));
        assert_eq!(hops.get(h, &l), Some(&[][..]));
        assert_eq!(hops.len(), 1);
    }

    #[test]
    fn invalidated_in_flight_is_not_cached() {
        let mut hops = Hops::new(10);
        let h = hops.add(plan(0));
        let l = link(3);
        hops.taken(h, &l);
        hops.evict(h, &l);
        assert!(!hops.answer(h, l.clone(), vec![Key::Int(9)]));
        assert_eq!(hops.get(h, &l), None);
        assert!(hops.is_empty());
    }

    #[test]
    fn clear_drops_entries_and_flags() {
        let mut hops = Hops::new(10);
        let h = hops.add(plan(0));
        let (a, b) = (link(1), link(2));
        hops.taken(h, &a);
        assert!(hops.answer(h, a.clone(), vec![Key::Int(1)]));
        hops.taken(h, &b);
        hops.clear(h);
        assert_eq!(hops.len(), 0);
        assert_eq!(hops.get(h, &a), None);
        assert!(!hops.answer(h, b.clone(), vec![Key::Int(2)]));
        // a fresh take after the clear is cached again.
        hops.taken(h, &b);
        assert!(hops.answer(h, b.clone(), vec![Key::Int(2)]));
        assert_eq!(hops.get(h, &b), Some(&[Key::Int(2)][..]));
    }

    #[test]
    fn bound_drops_everything() {
        let mut hops = Hops::new(2);
        let h = hops.add(plan(0));
        let ls: Vec<Tuple> = (1..=3).map(link).collect();
        for l in &ls {
            hops.taken(h, l);
        }
        assert!(hops.answer(h, ls[0].clone(), vec![Key::Int(1)]));
        assert!(hops.answer(h, ls[1].clone(), vec![Key::Int(2)]));
        assert!(hops.answer(h, ls[2].clone(), vec![Key::Int(3)]));
        assert_eq!(hops.len(), 0);
        assert_eq!(hops.clears, 1);
        for l in &ls {
            assert_eq!(hops.get(h, l), None);
        }
    }

    #[test]
    fn remove_target_frees_its_hops() {
        let mut hops = Hops::new(10);
        let h0 = hops.add(plan(0));
        let h1 = hops.add(plan(1));
        let l = link(1);
        hops.taken(h0, &l);
        assert!(hops.answer(h0, l.clone(), vec![Key::Int(1)]));
        hops.taken(h1, &l);
        assert!(hops.answer(h1, l.clone(), vec![Key::Int(2)]));
        assert_eq!(hops.len(), 2);
        hops.remove_target(0);
        assert_eq!(hops.len(), 1);
        assert!(hops.plan(h0).is_none());
        assert!(hops.plan(h1).is_some());
        assert_eq!(hops.get(h0, &l), None);
        assert_eq!(hops.get(h1, &l), Some(&[Key::Int(2)][..]));
        assert_eq!(hops.plan(h1).unwrap().target, 1);
    }
}
