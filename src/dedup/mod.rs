//! The shared dedup core (roadmap step 6; keymap-design.md §4; lazywf's proposal §0-§9): a synchronous,
//! sans-IO state machine between the binlog reader and a target's action. The caller feeds it row changes
//! and transaction boundaries, asks which jobs are due, runs them on its own runtime and connections
//! (after its own fence on `Chunk::high`), and hands the results back. The core opens no connection,
//! spawns nothing, holds no lock across a job and logs nothing: what the caller must know comes back as
//! [`CoreEvent`]s.
//!
//! Copy set: lazy-wire-fire copies `src/dedup/**` verbatim. These files import only std, external crates
//! (mysql_async's row types, foldhash) and the copy-set modules access, analyze, catalog, cascade, datum
//! and intset. No logging, no environment, no IO, no tokio.
//!
//! lazyview: one target per live version of a view on the Dedup route; its action writes every target
//! database (src/exec.rs). Facts off, no caller items (`NoItem`), no executor jobs (`NoJob`), direct mode.

pub mod buffer;
pub mod consumer;
pub mod hop;
pub mod key;
pub mod lane;
pub mod stage;
#[cfg(test)]
pub(crate) mod testkit;

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::access::{AccessPlan, Route};
use crate::analyze::ViewPlan;
use crate::cascade::Cache;

pub use buffer::{Fact, Facts, IdSet};
pub use consumer::{Note, Sink, Source};
pub use key::{HopId, Item, Key, NoItem, NoJob, Op, SpaceId, Spaces};
pub use lane::{JobId, Schedule};

use buffer::Buffer;
use consumer::Consumer;
use hop::Hops;
use lane::Lane;
use stage::Stage;

/// A target of the core: its slot (never reused) and incarnation (results of an older one are stale).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetId {
    pub slot: u32,
    pub incarnation: u32,
}

/// What a target declares besides its plans: target parameters, never a kind of target (guard R5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetNeeds {
    /// Each id carries its merged op for its window (lazy-wire-fire); off for lazyview.
    pub facts: bool,
}

/// A target to add.
#[derive(Debug, Clone)]
pub struct TargetSpec<P> {
    /// The plans whose ids the target takes (lazyview: one, the version's plan). Each must be on the
    /// Dedup route, and their ids must have the same number of parts.
    pub plans: Vec<Arc<ViewPlan>>,
    /// Labeled key spaces for the caller's own keys (lazy-wire-fire's prefixes); lazyview: none.
    pub labels: Vec<String>,
    /// Transactions that began before it are not fed to the target.
    pub start: P,
    pub schedule: Schedule,
    pub needs: TargetNeeds,
}

/// Core-wide settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreConfig {
    /// Stage normal transactions too (released at COMMIT, dropped at ROLLBACK).
    pub stage_txns: bool,
    /// The byte bound of every buffer, running chunk and the hop cache together (`over_bound`).
    pub max_bytes: usize,
    /// Hop cache entries before the cache is dropped.
    pub max_hop_entries: usize,
    /// An XA PREPARE older than this stops pinning the floor.
    pub max_prepared_age: Duration,
}

impl CoreConfig {
    /// lazyview's: direct mode, `max_bytes`, 1,000,000 hop entries, XA pins for one hour.
    pub fn direct(max_bytes: usize) -> CoreConfig {
        CoreConfig {
            stage_txns: false,
            max_bytes,
            max_hop_entries: 1_000_000,
            max_prepared_age: Duration::from_secs(3600),
        }
    }
}

/// A target's counters since the last `take_stats` (plain integers; whole-target refreshes do not
/// exist on this route).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TargetStats {
    pub rows: u64,
    pub rows_skipped: u64,
    pub ids_marked: u64,
    pub ids_absorbed: u64,
    pub items_marked: u64,
    pub items_absorbed: u64,
    pub hop_hits: u64,
    pub hop_misses: u64,
    pub cascade_rows: u64,
    pub cascade_gaps: u64,
    pub jobs: u64,
    pub ids_taken: u64,
    pub ids_retried: u64,
    pub items_taken: u64,
    pub items_retried: u64,
    pub ids_resolved: u64,
}

/// What the caller must know.
#[derive(Debug, Clone, PartialEq)]
pub enum CoreEvent<P> {
    /// XA COMMIT of an xid the core never saw prepared.
    UnknownPrepare { xid: String, at: P },
    /// A PREPARE older than `max_prepared_age` stopped pinning the floor.
    PrepareTooOld { xid: String, origin: P },
    /// The ids of a change of `target` are unknown (`why`): the caller's safety net.
    Unknown { target: TargetId, why: &'static str },
    /// A mapped cascade's parent changed during its cache's restart gap.
    CascadeGap { target: TargetId },
}

/// What `remove_target` hands back: everything the target held, per space, and its items.
#[derive(Debug)]
pub struct Drained<U> {
    pub ids: Vec<IdSet>,
    pub items: Vec<Item<U>>,
}

/// A target's flush job in flight: a copy of its chunk, put back when it fails or is stale.
#[allow(dead_code)] // read from PR 6O on, which removes this line
#[derive(Debug)]
struct Running<U> {
    id: JobId,
    ids: Vec<(SpaceId, IdSet)>,
    items: Vec<Item<U>>,
    facts: Option<Vec<(SpaceId, Key, Fact)>>,
}

#[allow(dead_code)] // read from PR 6N / 6O on; 6O removes this line
struct Target<P, U, J> {
    id: TargetId,
    spec: TargetSpec<P>,
    access: Vec<Arc<AccessPlan>>,
    spaces: Spaces,
    buffer: Buffer<P, U>,
    lane: Lane<J>,
    stats: TargetStats,
    /// One per plan.
    caches: Vec<Cache>,
    /// Held while a cascade cache seeds (`Note::Logged`), until `install_cache`.
    seed_pin: Option<P>,
    started: bool,
    held: bool,
    touched: bool,
    running: Option<Running<U>>,
}

/// The dedup core.
#[allow(dead_code)] // read from PR 6N / 6O on; 6O removes this line
pub struct Core<P, U = NoItem, J = NoJob> {
    cfg: CoreConfig,
    targets: Vec<Option<Target<P, U, J>>>,
    consumer: Consumer,
    hops: Hops,
    stage: Stage<P, U>,
    events: Vec<CoreEvent<P>>,
    /// Slots of targets not started yet (their `start` is ahead of the boundary).
    starting: Vec<usize>,
    next_job: u64,
    next_incarnation: u32,
}

impl<P: Ord + Clone + Debug, U: Hash + Eq + Clone + Debug, J> Core<P, U, J> {
    /// A core whose stream is at `boundary`.
    pub fn new(boundary: P, cfg: CoreConfig) -> Self {
        Self {
            stage: Stage::new(boundary, cfg.stage_txns),
            hops: Hops::new(cfg.max_hop_entries),
            cfg,
            targets: Vec::new(),
            consumer: Consumer::default(),
            events: Vec::new(),
            starting: Vec::new(),
            next_job: 0,
            next_incarnation: 1,
        }
    }

    /// Adds a target. `Err` names a plan that is not on the Dedup route, ids of different arity, or a
    /// path the consumer refuses (nothing is registered then).
    pub fn add_target(&mut self, spec: TargetSpec<P>) -> Result<TargetId, String> {
        let mut access = Vec::with_capacity(spec.plans.len());
        let mut parts = 0;
        for (pi, plan) in spec.plans.iter().enumerate() {
            let ap = AccessPlan::of(plan);
            if ap.route != Route::Dedup {
                return Err(format!("{}: route {}", plan.name, ap.route_name()));
            }
            if pi == 0 {
                parts = plan.unit_exprs.len();
            } else if plan.unit_exprs.len() != parts {
                return Err(format!(
                    "{}: {} id parts, the earlier plans have {}",
                    plan.name,
                    plan.unit_exprs.len(),
                    parts
                ));
            }
            access.push(Arc::new(ap));
        }
        let mut spaces = Spaces::of(parts, &spec.plans[0].partial.sets);
        for plan in &spec.plans[1..] {
            for set in &plan.partial.sets {
                spaces.add(set);
            }
        }
        for l in &spec.labels {
            spaces.labeled(l);
        }
        let slot = self.targets.len();
        for (pi, plan) in spec.plans.iter().enumerate() {
            if let Err(e) = self
                .consumer
                .add(slot, pi, plan, &access[pi], &spaces, &mut self.hops)
            {
                self.consumer.remove(slot);
                self.hops.remove_target(slot);
                return Err(e);
            }
        }
        let started = self.stage.boundary() >= &spec.start;
        let facts = spec.needs.facts;
        let nplans = spec.plans.len();
        let id = TargetId {
            slot: slot as u32,
            incarnation: self.next_incarnation,
        };
        self.targets.push(Some(Target {
            id,
            spec,
            access,
            buffer: Buffer::new(spaces.len(), facts),
            spaces,
            lane: Lane::new(),
            stats: TargetStats::default(),
            caches: (0..nplans).map(|_| Cache::default()).collect(),
            seed_pin: None,
            started,
            held: false,
            touched: false,
            running: None,
        }));
        if !started {
            self.starting.push(slot);
        }
        self.next_incarnation += 1;
        Ok(id)
    }

    /// New plans, schedule or needs for `t`: the incarnation changes; the buffer, caches and position
    /// are kept. The spaces must be equal, else `Err` (remove and add instead). A job of the old
    /// incarnation completes `Stale` and its chunk goes back to pending.
    pub fn replace_target(&mut self, t: TargetId, spec: TargetSpec<P>) -> Result<TargetId, String> {
        let Some(slot) = self.slot(t) else {
            return Err("unknown target".to_string());
        };
        let mut access = Vec::with_capacity(spec.plans.len());
        let mut parts = 0;
        for (pi, plan) in spec.plans.iter().enumerate() {
            let ap = AccessPlan::of(plan);
            if ap.route != Route::Dedup {
                return Err(format!("{}: route {}", plan.name, ap.route_name()));
            }
            if pi == 0 {
                parts = plan.unit_exprs.len();
            } else if plan.unit_exprs.len() != parts {
                return Err(format!(
                    "{}: {} id parts, the earlier plans have {}",
                    plan.name,
                    plan.unit_exprs.len(),
                    parts
                ));
            }
            access.push(Arc::new(ap));
        }
        let mut spaces = Spaces::of(parts, &spec.plans[0].partial.sets);
        for plan in &spec.plans[1..] {
            for set in &plan.partial.sets {
                spaces.add(set);
            }
        }
        for l in &spec.labels {
            spaces.labeled(l);
        }
        if self.targets[slot].as_ref().unwrap().spaces != spaces {
            return Err("the spaces differ: remove and add the target".to_string());
        }
        let old = self.targets[slot].as_ref().unwrap();
        let old_plans = old.spec.plans.clone();
        let old_access = old.access.clone();
        self.consumer.remove(slot);
        self.hops.remove_target(slot);
        for (pi, plan) in spec.plans.iter().enumerate() {
            if let Err(e) = self
                .consumer
                .add(slot, pi, plan, &access[pi], &spaces, &mut self.hops)
            {
                // The same plans, spaces and access were accepted before: this cannot fail.
                for (pi, plan) in old_plans.iter().enumerate() {
                    let _ =
                        self.consumer
                            .add(slot, pi, plan, &old_access[pi], &spaces, &mut self.hops);
                }
                return Err(e);
            }
        }
        let tg = self.targets[slot].as_mut().unwrap();
        tg.id.incarnation = self.next_incarnation;
        tg.spec = spec;
        tg.access = access;
        self.next_incarnation += 1;
        Ok(tg.id)
    }

    /// Removes `t` and hands back everything it held (the running chunk's copy included).
    pub fn remove_target(&mut self, t: TargetId) -> Option<Drained<U>> {
        let slot = self.slot(t)?;
        self.consumer.remove(slot);
        self.hops.remove_target(slot);
        self.stage.drop_target(slot);
        let mut tg = self.targets[slot].take()?;
        self.starting.retain(|s| *s != slot);
        let (mut ids, mut items) = tg.buffer.drain();
        if let Some(r) = tg.running {
            for (s, set) in r.ids {
                let s = s.index();
                if ids.len() <= s {
                    ids.resize_with(s + 1, IdSet::default);
                }
                ids[s].extend(set);
            }
            items.extend(r.items);
        }
        Some(Drained { ids, items })
    }

    /// Held: fed nothing, no job started, its position pinned where it is.
    pub fn hold(&mut self, t: TargetId, on: bool) {
        if let Some(s) = self.slot(t) {
            if let Some(tg) = self.targets[s].as_mut() {
                tg.held = on;
            }
        }
    }

    /// Paused: fed as usual, no job started.
    pub fn pause(&mut self, t: TargetId, on: bool) {
        if let Some(s) = self.slot(t) {
            if let Some(tg) = self.targets[s].as_mut() {
                tg.lane.pause(on);
            }
        }
    }

    /// Replaces `t`'s cascade cache of plan `pi` by a ready one (lazyview: the build's).
    pub fn set_cache(&mut self, t: TargetId, pi: usize, cache: Cache) {
        if let Some(c) = self.cache_mut(t, pi) {
            *c = cache;
        }
    }

    pub fn cache_mut(&mut self, t: TargetId, pi: usize) -> Option<&mut Cache> {
        let s = self.slot(t)?;
        self.targets[s].as_mut()?.caches.get_mut(pi)
    }

    pub fn spaces(&self, t: TargetId) -> Option<&Spaces> {
        let s = self.slot(t)?;
        self.targets[s].as_ref().map(|tg| &tg.spaces)
    }

    pub fn access(&self, t: TargetId) -> Option<&[Arc<AccessPlan>]> {
        let s = self.slot(t)?;
        self.targets[s].as_ref().map(|tg| tg.access.as_slice())
    }

    /// The target, plan index and alias of hop `h`.
    pub fn hop_of(&self, h: HopId) -> Option<(TargetId, usize, usize)> {
        let p = self.hops.plan(h)?;
        let tg = self.targets.get(p.target)?.as_ref()?;
        Some((tg.id, p.plan, p.alias))
    }

    /// Every target.
    pub fn targets(&self) -> Vec<TargetId> {
        self.targets.iter().flatten().map(|tg| tg.id).collect()
    }

    /// The slot of `t` when `t` is its current incarnation.
    fn slot(&self, t: TargetId) -> Option<usize> {
        let s = t.slot as usize;
        self.targets.get(s)?.as_ref().filter(|tg| tg.id == t)?;
        Some(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A core of `u64` positions: direct mode, room for the hop cache.
    fn core() -> Core<u64> {
        Core::<u64, NoItem, NoJob>::new(0, CoreConfig::direct(8 << 20))
    }

    /// A target of `plans` at the boundary, with the defaults for everything else.
    fn spec(plans: Vec<Arc<ViewPlan>>) -> TargetSpec<u64> {
        TargetSpec {
            plans,
            labels: Vec::new(),
            start: 0,
            schedule: Schedule::immediate(100, 100),
            needs: TargetNeeds::default(),
        }
    }

    #[test]
    fn add_target_refuses_other_routes() {
        let mut core = core();
        // A bare DISTINCT is the wrong example: the analyzer plans it as grouped, so it routes
        // Dedup. A LIMIT without an ORDER BY cannot be traced to rows, so it plans full.
        let full = "SELECT id FROM items LIMIT 5";
        assert!(
            matches!(testkit::plan(full).mode, crate::analyze::Mode::Full(_)),
            "{full}: {:?}",
            testkit::plan(full).mode
        );
        for (sql, want) in [
            ("SELECT COUNT(*) n FROM items", "route recompute"),
            (full, "route full"),
        ] {
            let e = core
                .add_target(spec(vec![testkit::plan(sql)]))
                .expect_err(sql);
            assert!(e.contains(want), "{sql}: {e}");
        }
        assert!(core.targets().is_empty(), "a refusal registers nothing");
    }

    #[test]
    fn ids_of_different_arity_are_refused() {
        let mut core = core();
        let plans = vec![
            testkit::plan("SELECT i.id FROM items i"),
            testkit::plan("SELECT l.child_id, l.parent_id FROM item_links l"),
        ];
        let e = core.add_target(spec(plans)).expect_err("mixed arity");
        assert!(e.contains("id parts"), "{e}");
        assert!(core.targets().is_empty(), "a refusal registers nothing");
    }

    #[test]
    fn replace_keeps_the_buffer_and_bumps_the_incarnation() {
        let mut core = core();
        let p = testkit::plan("SELECT i.id FROM items i");
        let old = core.add_target(spec(vec![p.clone()])).unwrap();
        let slot = old.slot as usize;
        core.targets[slot]
            .as_mut()
            .unwrap()
            .buffer
            .mark(SpaceId(0), Key::Int(7), &7u64);
        let fresh = core.replace_target(old, spec(vec![p])).unwrap();
        assert_eq!(fresh.slot, old.slot, "the same slot");
        assert!(fresh.incarnation > old.incarnation, "a new incarnation");
        assert_eq!(core.spaces(old), None, "the old id no longer resolves");
        let tg = core.targets[slot].as_ref().unwrap();
        assert_eq!(tg.id, fresh);
        assert_eq!(tg.buffer.pending_len(), 1, "the buffer kept the id");
        let tg = core.targets[slot].as_mut().unwrap();
        assert!(tg.buffer.begin(), "the pending id becomes a chunk");
        assert_eq!(
            tg.buffer.take(10, 0).unwrap().ids[0].1.to_keys(),
            vec![Key::Int(7)]
        );
    }

    #[test]
    fn remove_hands_back_everything() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan("SELECT i.id FROM items i")]))
            .unwrap();
        let slot = t.slot as usize;
        let mut held = IdSet::default();
        held.insert(Key::Int(9));
        let tg = core.targets[slot].as_mut().unwrap();
        tg.buffer.mark(SpaceId(0), Key::Int(3), &3u64);
        tg.running = Some(Running {
            id: JobId(7),
            ids: vec![(SpaceId(0), held)],
            items: Vec::new(),
            facts: None,
        });
        let d = core.remove_target(t).expect("the target was there");
        assert_eq!(d.ids[0].to_keys(), vec![Key::Int(3), Key::Int(9)]);
        assert!(d.items.is_empty());
        assert_eq!(core.targets(), vec![], "the target is gone");
        assert_eq!(core.spaces(t), None);
    }

    #[test]
    fn slots_are_never_reused() {
        let mut core = core();
        let a = core
            .add_target(spec(vec![testkit::plan("SELECT i.id FROM items i")]))
            .unwrap();
        let b = core
            .add_target(spec(vec![testkit::plan("SELECT k.id FROM kinds k")]))
            .unwrap();
        assert_eq!((a.slot, b.slot), (0, 1));
        assert!(core.remove_target(b).is_some());
        let c = core
            .add_target(spec(vec![testkit::plan("SELECT o.id FROM owners o")]))
            .unwrap();
        assert_eq!(c.slot, 2, "the freed slot 1 is not handed out again");
        assert_eq!(core.targets(), vec![a, c]);
        assert_eq!(core.remove_target(a).unwrap().ids.len(), 1, "one space");
    }

    #[test]
    fn hop_of_names_the_occurrence() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(
                "SELECT i.id, l.child_id FROM items i \
                 JOIN item_links l ON l.parent_id = i.kind_id",
            )]))
            .unwrap();
        let hops: Vec<HopId> = (0..16u32)
            .map(HopId)
            .filter(|h| core.hops.plan(*h).is_some())
            .collect();
        assert_eq!(hops.len(), 1, "the join has one hop");
        assert_eq!(core.hop_of(hops[0]), Some((t, 0, 1)));
    }

    /// Plan 0 reads one table; plan 1 joins three and its `item_values` occurrence is a partial
    /// path, so only the merge over `plans[1..]` can bring that set into the target's spaces.
    fn two_plans() -> (Arc<ViewPlan>, Arc<ViewPlan>) {
        (
            testkit::plan(
                "SELECT v.item_id, v.key_id, MAX(v.val) AS m FROM item_values v GROUP BY \
                 v.item_id, v.key_id",
            ),
            testkit::plan(
                "SELECT l.parent_id, i.id, MAX(v.val) AS v FROM item_links l JOIN items i ON \
                 i.id = l.child_id JOIN item_values v ON v.item_id = i.id AND v.key_id = 7 \
                 GROUP BY l.parent_id, i.id",
            ),
        )
    }

    #[test]
    fn two_plan_target_merges_spaces_access_and_caches() {
        let mut core = core();
        let (p0, p1) = two_plans();
        let t = core
            .add_target(spec(vec![p0, p1]))
            .expect("two Dedup plans of the same arity");
        let sp = core.spaces(t).unwrap();
        assert_eq!(sp.find(&[0, 1]), Some(SpaceId::FULL), "the merged full id");
        assert_eq!(
            sp.find(&[1]),
            Some(SpaceId(1)),
            "plan 1's partial set is merged in (only plans[1..] can add it)"
        );
        assert_eq!(sp.len(), 2, "the full id plus plan 1's set");
        let acc = core.access(t).unwrap();
        assert_eq!(acc.len(), 2, "one access plan per plan");
        assert_eq!(
            acc[0].occurrences.len(),
            1,
            "plan 0 reads item_values alone"
        );
        assert_eq!(
            acc[1].occurrences.len(),
            3,
            "plan 1 reads item_links, items, item_values"
        );
        assert_ne!(acc[0], acc[1], "each plan keeps its own access");
        assert_eq!(
            core.targets[t.slot as usize].as_ref().unwrap().caches.len(),
            2,
            "one cascade cache per plan"
        );
        let watched = |t: &str| core.consumer.watched(&("db".to_string(), t.to_string()));
        assert!(
            watched("item_values") && watched("item_links") && watched("items"),
            "consumer.add ran for both plans"
        );
        assert!(!watched("kinds"), "only the plans' tables are watched");
    }

    #[test]
    fn replace_target_refusals_leave_the_target_alone() {
        let mut core = core();
        let (p0, p1) = two_plans();
        let t = core.add_target(spec(vec![p0.clone(), p1.clone()])).unwrap();
        let before = core.targets[t.slot as usize].as_ref().unwrap().id;
        let e = core
            .replace_target(t, spec(vec![testkit::plan("SELECT COUNT(*) n FROM items")]))
            .expect_err("not on the Dedup route");
        assert!(e.contains("route recompute"), "{e}");
        let e = core
            .replace_target(
                t,
                spec(vec![testkit::plan("SELECT i.id FROM items i"), p1.clone()]),
            )
            .expect_err("1 id parts against 2");
        assert!(e.contains("id parts"), "{e}");
        let e = core
            .replace_target(t, spec(vec![p0.clone()]))
            .expect_err("plan 0 alone loses plan 1's partial set");
        assert!(e.contains("the spaces differ"), "{e}");
        assert_eq!(core.targets(), vec![t], "a refusal changes nothing");
        assert_eq!(core.targets[t.slot as usize].as_ref().unwrap().id, before);
        assert_eq!(core.spaces(t).unwrap().len(), 2, "still the merged spaces");
        assert!(
            core.consumer
                .watched(&("db".to_string(), "item_links".to_string())),
            "still registered with the consumer"
        );
    }

    #[test]
    fn composite_ids_are_accepted_and_mixed_arity_refused() {
        let mut core = core();
        let three = "SELECT v.item_id, v.key_id, v.scope_id FROM item_values v";
        let p3 = testkit::plan(three);
        assert_eq!(
            p3.unit_exprs.len(),
            3,
            "{three} plans its unit as {:?}",
            p3.unit_exprs
        );
        let t = core.add_target(spec(vec![p3.clone()])).unwrap();
        let sp = core.spaces(t).unwrap();
        assert_eq!(
            sp.find(&[0, 1, 2]),
            Some(SpaceId::FULL),
            "the three-part id"
        );
        assert_eq!(sp.find(&[0, 1]), None, "and only that space");
        assert_eq!(core.access(t).unwrap().len(), 1);
        assert!(core.remove_target(t).is_some());
        let one = testkit::plan("SELECT i.id FROM items i");
        let e = core
            .add_target(spec(vec![one.clone(), p3.clone()]))
            .expect_err("1 part then 3");
        assert!(e.contains("3 id parts"), "{e}");
        let e = core
            .add_target(spec(vec![p3.clone(), one.clone()]))
            .expect_err("3 parts then 1");
        assert!(e.contains("1 id parts"), "{e}");
        assert!(core.targets().is_empty(), "a refusal registers nothing");
    }

    /// All hop ids the core knows (the cache ids fill densely from 0 at add time).
    fn hop_ids(core: &Core<u64>) -> Vec<HopId> {
        (0..32u32)
            .map(HopId)
            .filter(|h| core.hops.plan(*h).is_some())
            .collect()
    }

    #[test]
    fn hop_of_maps_each_hop_of_a_two_hop_chain() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(
                "SELECT i.id AS unit_0, l.child_id, o.id AS unit_1 FROM items i JOIN item_links \
                 l ON l.parent_id = i.kind_id JOIN owners o ON o.id = l.parent_id",
            )]))
            .unwrap();
        let hops = hop_ids(&core);
        assert_eq!(hops.len(), 2, "items->item_links->owners: two hops");
        assert_eq!(core.hop_of(hops[0]), Some((t, 0, 1)), "the first hop");
        assert_eq!(core.hop_of(hops[1]), Some((t, 0, 2)), "the second hop");
    }

    #[test]
    fn hop_of_follows_positional_aliases_not_names() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(
                "SELECT x.id AS unit_0, y.child_id, z.id AS unit_1, k.id AS unit_2 FROM items x \
                 JOIN item_links y ON y.parent_id = x.kind_id JOIN owners z ON z.id = \
                 y.parent_id JOIN kinds k ON k.id = x.kind_id",
            )]))
            .unwrap();
        let hops = hop_ids(&core);
        assert_eq!(
            hops.len(),
            3,
            "three joined tables after the base: three hops"
        );
        let mapped: Vec<(TargetId, usize, usize)> =
            hops.iter().map(|h| core.hop_of(*h).unwrap()).collect();
        assert_eq!(
            mapped,
            vec![(t, 0, 1), (t, 0, 2), (t, 0, 3)],
            "the aliases stay positional however they are spelled"
        );
    }
}
