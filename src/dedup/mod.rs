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

#[cfg(test)]
mod acceptance;
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
use std::time::{Duration, Instant};

use mysql_async::binlog::row::BinlogRow;

use crate::access::{AccessPlan, Route};
use crate::analyze::ViewPlan;
use crate::cascade::Cache;
use crate::catalog::TableRef;
use crate::datum::Key as Tuple;

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
#[derive(Debug)]
struct Running<U> {
    id: JobId,
    ids: Vec<(SpaceId, IdSet)>,
    items: Vec<Item<U>>,
    facts: Option<Vec<(SpaceId, Key, Fact)>>,
}

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
    /// Started jobs not completed yet: their target.
    jobs: std::collections::HashMap<JobId, TargetId>,
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
            jobs: std::collections::HashMap::new(),
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

/// A chunk of a flush job.
#[derive(Debug)]
pub struct Chunk<P, U> {
    pub ids: Vec<(SpaceId, IdSet)>,
    pub items: Vec<Item<U>>,
    /// Per id, its merged fact (targets that declare facts).
    pub facts: Option<Vec<(SpaceId, Key, Fact)>>,
    /// The generation's oldest release (lazyview pins lagging target databases there).
    pub origin: P,
    /// The generation's newest release: the caller's fence covers it before reading.
    pub high: P,
}

#[derive(Debug)]
pub enum JobKind<P, U, J> {
    Flush(Chunk<P, U>),
    Executor(J),
}

#[derive(Debug)]
pub struct Job<P, U, J> {
    pub id: JobId,
    pub target: TargetId,
    pub kind: JobKind<P, U, J>,
}

/// What the action did. Everything not listed is done.
#[derive(Debug)]
pub struct JobResult<U> {
    /// Every id and item of the chunk goes back to pending (the job failed as a whole).
    pub retry_all: bool,
    /// Ids to act on again (transient failures for them).
    pub retry: Vec<(SpaceId, IdSet)>,
    /// Items to resolve again (a failed hop query chunk: only these).
    pub items_retry: Vec<Item<U>>,
    /// Per hop, the (link value, id) pairs its queries returned; a taken link without a pair reached no id.
    pub hop_answers: Vec<(HopId, Vec<(Tuple, Key)>)>,
    /// Ids the action found from items and acted on in this job.
    pub resolved: u64,
    /// The lane starts nothing before this (the action's backoff).
    pub not_before: Option<Instant>,
}

impl<U> JobResult<U> {
    /// Everything done.
    pub fn ok() -> Self {
        JobResult {
            retry_all: false,
            retry: Vec::new(),
            items_retry: Vec::new(),
            hop_answers: Vec::new(),
            resolved: 0,
            not_before: None,
        }
    }

    /// Everything back to pending, nothing started before `not_before`.
    pub fn failed(not_before: Option<Instant>) -> Self {
        JobResult {
            retry_all: true,
            retry: Vec::new(),
            items_retry: Vec::new(),
            hop_answers: Vec::new(),
            resolved: 0,
            not_before,
        }
    }
}

/// How `complete` took a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completed {
    Done,
    /// The target was replaced or removed meanwhile: its chunk went back to pending (or away).
    Stale,
}

/// Where the consumer's output goes: the targets' buffers, or the open staged transaction.
struct Feed<'a, P, U, J> {
    targets: &'a mut [Option<Target<P, U, J>>],
    origin: &'a P,
    staged: Option<&'a mut stage::Staged<U>>,
    touched: &'a mut Vec<usize>,
    events: &'a mut Vec<CoreEvent<P>>,
}

impl<P: Ord + Clone + Debug, U: Hash + Eq + Clone + Debug, J> Sink<U> for Feed<'_, P, U, J> {
    fn open(&self, t: usize) -> bool {
        self.targets[t]
            .as_ref()
            .is_some_and(|tg| tg.started && !tg.held)
    }

    fn row(&mut self, t: usize, skipped: bool) {
        let tg = self.targets[t].as_mut().unwrap();
        tg.stats.rows += 1;
        tg.stats.rows_skipped += u64::from(skipped);
        if !skipped && self.staged.is_none() && !tg.touched {
            tg.touched = true;
            self.touched.push(t);
        }
    }

    fn int(&mut self, t: usize, s: SpaceId, v: u64, op: Op, cascaded: bool) {
        match self.staged.as_mut() {
            Some(staged) => {
                let n = self.targets[t].as_ref().unwrap().spaces.len();
                staged.mark(t, n, s, Key::Int(v)); // counted at release
            }
            None => {
                let tg = self.targets[t].as_mut().unwrap();
                let new = tg.buffer.mark_int(s, v, self.origin);
                tg.stats.ids_marked += 1;
                tg.stats.ids_absorbed += u64::from(!new);
                if tg.spec.needs.facts {
                    tg.buffer.note(s, &Key::Int(v), op, cascaded, new);
                }
            }
        }
    }

    fn tuple(&mut self, t: usize, s: SpaceId, k: Tuple, op: Op, cascaded: bool) {
        match self.staged.as_mut() {
            Some(staged) => {
                let n = self.targets[t].as_ref().unwrap().spaces.len();
                staged.mark(t, n, s, Key::Tuple(k)); // counted at release
            }
            None => {
                let tg = self.targets[t].as_mut().unwrap();
                let new = tg.buffer.mark_tuple(s, k.clone(), self.origin);
                tg.stats.ids_marked += 1;
                tg.stats.ids_absorbed += u64::from(!new);
                if tg.spec.needs.facts {
                    tg.buffer.note(s, &Key::Tuple(k), op, cascaded, new);
                }
            }
        }
    }

    fn item(&mut self, t: usize, i: Item<U>) {
        match self.staged.as_mut() {
            Some(staged) => {
                let n = self.targets[t].as_ref().unwrap().spaces.len();
                staged.item(t, n, i); // counted at release
            }
            None => {
                let tg = self.targets[t].as_mut().unwrap();
                let new = tg.buffer.item(i, self.origin);
                tg.stats.items_marked += 1;
                tg.stats.items_absorbed += u64::from(!new);
            }
        }
    }

    fn hop(&mut self, t: usize, hit: bool) {
        let tg = self.targets[t].as_mut().unwrap();
        if hit {
            tg.stats.hop_hits += 1;
        } else {
            tg.stats.hop_misses += 1;
        }
    }

    fn note(&mut self, t: usize, n: Note) {
        let tg = self.targets[t].as_mut().unwrap();
        match n {
            Note::ShortParentImage => self.events.push(CoreEvent::Unknown {
                target: tg.id,
                why: "a cascade parent's row image lacks columns",
            }),
            Note::IncompleteKeptRow => self.events.push(CoreEvent::Unknown {
                target: tg.id,
                why: "a kept cascade child row lacks columns",
            }),
            Note::CascadeGap => {
                tg.stats.cascade_gaps += 1;
                self.events.push(CoreEvent::CascadeGap { target: tg.id });
            }
            Note::CascadeRows(n) => tg.stats.cascade_rows += n,
            Note::Logged => {
                tg.seed_pin.get_or_insert_with(|| self.origin.clone());
            }
        }
    }

    fn cache(&mut self, t: usize, pi: usize) -> &mut Cache {
        &mut self.targets[t].as_mut().unwrap().caches[pi]
    }
}

impl<P: Ord + Clone + Debug, U: Hash + Eq + Clone + Debug, J> Core<P, U, J> {
    /// `table` as one TABLE_MAP shows it (`columns`: its column count; `None`: any shape).
    pub fn source(&self, table: &TableRef, columns: Option<usize>) -> Option<Source> {
        self.consumer.source(table, columns)
    }

    /// The targets with an occurrence of another shape in `s` (the caller re-plans them).
    pub fn mismatched(&self, s: &Source) -> Vec<TargetId> {
        self.consumer
            .mismatched(s)
            .into_iter()
            .filter_map(|slot| self.targets[slot].as_ref().map(|tg| tg.id))
            .collect()
    }

    /// One row change of `s`'s table in the open transaction.
    pub fn row(&mut self, s: &Source, before: Option<&BinlogRow>, after: Option<&BinlogRow>) {
        let Self {
            targets,
            consumer,
            hops,
            stage,
            events,
            ..
        } = self;
        let (origin, staged, touched) = stage.parts();
        let mut feed = Feed {
            targets,
            origin,
            staged,
            touched,
            events,
        };
        consumer.row(s, before, after, hops, &mut feed);
    }

    /// [`Self::row`] with replay position `origin` (rows the caller kept, e.g. an XA transaction's at its
    /// XA COMMIT); never staged.
    pub fn row_at(
        &mut self,
        origin: &P,
        s: &Source,
        before: Option<&BinlogRow>,
        after: Option<&BinlogRow>,
    ) {
        let Self {
            targets,
            consumer,
            hops,
            stage,
            events,
            ..
        } = self;
        let mut feed = Feed {
            targets,
            origin,
            staged: None,
            touched: stage.touched_mut(),
            events,
        };
        consumer.row(s, before, after, hops, &mut feed);
    }

    /// A caller's own key for target `t` in the open transaction (lazy-wire-fire's manual triggers).
    pub fn stage_key(&mut self, t: TargetId, s: SpaceId, k: Key, op: Op) {
        let Some(slot) = self.slot(t) else { return };
        let Self {
            targets,
            stage,
            events,
            ..
        } = self;
        let (origin, staged, touched) = stage.parts();
        let mut feed = Feed {
            targets,
            origin,
            staged,
            touched,
            events,
        };
        feed.row(slot, false);
        match k {
            Key::Int(v) => feed.int(slot, s, v, op, false),
            Key::Tuple(k) => feed.tuple(slot, s, k, op, false),
        }
    }

    /// A caller's own item for target `t` in the open transaction.
    pub fn stage_item(&mut self, t: TargetId, i: U) {
        let Some(slot) = self.slot(t) else { return };
        let Self {
            targets,
            stage,
            events,
            ..
        } = self;
        let (origin, staged, touched) = stage.parts();
        let mut feed = Feed {
            targets,
            origin,
            staged,
            touched,
            events,
        };
        feed.row(slot, false);
        feed.item(slot, Item::User(i));
    }

    /// A boundary at `end` (COMMIT, or a statement / heartbeat boundary); `settle == false`: a
    /// transaction of the caller's own tables only, the boundary stays.
    pub fn commit(&mut self, end: P, settle: bool, now: Instant) {
        if let Some(r) = self.stage.commit(end.clone(), settle) {
            self.release(r, &end, now);
        }
        self.ended(&end, now);
    }

    pub fn rollback(&mut self, end: P, now: Instant) {
        if let Some(r) = self.stage.rollback(end.clone()) {
            self.release(r, &end, now);
        }
        self.ended(&end, now);
    }

    pub fn xa_start(&mut self, xid: &str) {
        self.stage.xa_start(xid);
    }

    pub fn xa_prepare(&mut self, xid: &str, end: P, now: Instant) {
        self.stage.xa_prepare(xid, end.clone(), now);
        self.ended(&end, now);
    }

    pub fn xa_end(&mut self, xid: &str, commit: bool, end: P, now: Instant) {
        match self.stage.xa_end(xid, commit, end.clone()) {
            stage::XaEnd::Released(r) => self.release(r, &end, now),
            stage::XaEnd::Unknown => self.events.push(CoreEvent::UnknownPrepare {
                xid: xid.to_string(),
                at: end.clone(),
            }),
            stage::XaEnd::RolledBack => {}
        }
        self.ended(&end, now);
    }

    /// The reader restarted at `to`: the open transaction is read again.
    pub fn rewound(&mut self, to: P) {
        self.stage.rewound(to);
        for tg in self.targets.iter_mut().flatten() {
            tg.touched = false;
        }
    }

    /// Installs a seeded cascade cache into `t`'s seeding one (plan `pi`) and replays its log.
    pub fn install_cache(&mut self, t: TargetId, pi: usize, seeded: Cache) {
        let Some(slot) = self.slot(t) else { return };
        let log = self.targets[slot].as_mut().unwrap().caches[pi].install(seeded);
        let Self {
            targets,
            consumer,
            hops,
            stage,
            events,
            ..
        } = self;
        let mut touched = Vec::new();
        let mut feed = Feed {
            targets,
            origin: stage.boundary(),
            staged: None,
            touched: &mut touched,
            events,
        };
        consumer.install(slot, pi, log, hops, &mut feed);
        targets[slot].as_mut().unwrap().seed_pin = None;
    }

    /// The events since the last call.
    pub fn events(&mut self) -> Vec<CoreEvent<P>> {
        std::mem::take(&mut self.events)
    }

    /// A released staged transaction's ids go to the buffers, pinned at its origin.
    fn release(&mut self, r: stage::Release<P, U>, end: &P, now: Instant) {
        for (slot, sets, items) in r.staged.into_parts() {
            let Some(tg) = self.targets[slot].as_mut() else {
                continue;
            };
            tg.buffer.pin(&r.origin);
            for (i, set) in sets.into_iter().enumerate() {
                for k in set.to_keys() {
                    let new = tg.buffer.mark(SpaceId(i as u16), k, &r.origin);
                    tg.stats.ids_marked += 1;
                    tg.stats.ids_absorbed += u64::from(!new);
                }
            }
            for i in items {
                let new = tg.buffer.item(i, &r.origin);
                tg.stats.items_marked += 1;
                tg.stats.items_absorbed += u64::from(!new);
            }
            tg.buffer.released(end);
            tg.lane.released(now);
        }
    }

    /// After every boundary: touched targets release, starting targets may start, old prepares unpin.
    fn ended(&mut self, end: &P, now: Instant) {
        for slot in self.stage.take_touched() {
            if let Some(tg) = self.targets[slot].as_mut() {
                tg.buffer.released(end);
                tg.lane.released(now);
                tg.touched = false;
            }
        }
        let boundary = self.stage.boundary().clone();
        let (start, keep): (Vec<usize>, Vec<usize>) = self.starting.drain(..).partition(|&s| {
            self.targets[s]
                .as_ref()
                .is_some_and(|tg| tg.spec.start <= boundary)
        });
        for s in start {
            if let Some(tg) = self.targets[s].as_mut() {
                tg.started = true;
            }
        }
        self.starting = keep;
        for (xid, origin) in self.stage.unpin_older(now, self.cfg.max_prepared_age) {
            self.events.push(CoreEvent::PrepareTooOld { xid, origin });
        }
    }
}

impl<P: Ord + Clone + Debug, U: Hash + Eq + Clone + Debug, J> Core<P, U, J> {
    /// The jobs due at `now`: at most one per target.
    pub fn due(&mut self, now: Instant) -> Vec<Job<P, U, J>> {
        let mut out = Vec::new();
        for tg in self.targets.iter_mut().flatten() {
            if tg.held {
                continue;
            }
            let due = tg.lane.due(
                now,
                &tg.spec.schedule,
                tg.buffer.pending_len() > 0,
                tg.buffer.in_flight(),
            );
            let kind = match due {
                Some(lane::Due::Executor) => match tg.lane.pop() {
                    Some(j) => JobKind::Executor(j),
                    None => continue,
                },
                Some(lane::Due::Flush) => {
                    if !tg.buffer.in_flight() {
                        tg.buffer.begin();
                        tg.lane.reset_timers();
                    }
                    let Some(t) = tg
                        .buffer
                        .take(tg.spec.schedule.chunk_ids, tg.spec.schedule.chunk_items)
                    else {
                        continue;
                    };
                    for i in &t.items {
                        if let Item::Hop { hop, link } = i {
                            self.hops.taken(*hop, link);
                        }
                    }
                    tg.stats.jobs += 1;
                    tg.stats.ids_taken += t.ids.iter().map(|(_, s)| s.len() as u64).sum::<u64>();
                    tg.stats.items_taken += t.items.len() as u64;
                    tg.running = Some(Running {
                        id: JobId(self.next_job),
                        ids: t.ids.clone(),
                        items: t.items.clone(),
                        facts: t.facts.clone(),
                    });
                    JobKind::Flush(Chunk {
                        ids: t.ids,
                        items: t.items,
                        facts: t.facts,
                        origin: t.origin,
                        high: t.high,
                    })
                }
                None => continue,
            };
            let id = JobId(self.next_job);
            self.next_job += 1;
            tg.lane.start(id);
            self.jobs.insert(id, tg.id);
            out.push(Job {
                id,
                target: tg.id,
                kind,
            });
        }
        out
    }

    /// When `due` may first return a job without a new event.
    pub fn next_wake(&self) -> Option<Instant> {
        self.targets
            .iter()
            .flatten()
            .filter(|tg| !tg.held)
            .filter_map(|tg| {
                tg.lane
                    .next_wake(&tg.spec.schedule, tg.buffer.pending_len() > 0)
            })
            .min()
    }

    /// The earliest retry hold of a target with work (lazyview's flush deadline wakes for it; its own
    /// batch window paces everything else).
    pub fn next_retry(&self) -> Option<Instant> {
        self.targets
            .iter()
            .flatten()
            .filter(|tg| !tg.held && tg.buffer.pending_len() > 0)
            .filter_map(|tg| tg.lane.not_before())
            .min()
    }

    /// A caller's own job on `t`'s lane (lazy-wire-fire's reset batches).
    pub fn enqueue(&mut self, t: TargetId, job: J) {
        if let Some(tg) = self.slot(t).and_then(|s| self.targets[s].as_mut()) {
            tg.lane.enqueue(job);
        }
    }

    /// The result of `job`.
    pub fn complete(&mut self, job: JobId, r: JobResult<U>) -> Completed {
        let Some(target) = self.jobs.remove(&job) else {
            return Completed::Stale;
        };
        let Some(tg) = self
            .targets
            .get_mut(target.slot as usize)
            .and_then(|s| s.as_mut())
        else {
            return Completed::Stale; // removed: its chunk went with `Drained`
        };
        if tg.id != target {
            // replaced: the lane is the new incarnation's, the buffer kept the chunk
            tg.lane.finish(job, None);
            if let Some(copy) = tg.running.take() {
                debug_assert_eq!(copy.id, job, "the lane runs the job `due` started");
                if let Some(f) = copy.facts {
                    tg.buffer.retry_facts(f);
                }
                tg.buffer.done(copy.ids, copy.items);
                if !tg.buffer.in_flight() && tg.buffer.pending_len() > 0 {
                    // retried work: a new generation at once, so the lane is due again.
                    tg.buffer.begin();
                }
            }
            return Completed::Stale;
        }
        tg.lane.finish(job, r.not_before);
        let Some(copy) = tg.running.take() else {
            return Completed::Done; // an executor job
        };
        debug_assert_eq!(copy.id, job, "the lane runs the job `due` started");
        let JobResult {
            retry_all,
            retry,
            items_retry,
            hop_answers,
            resolved,
            not_before: _,
        } = r;
        if retry_all {
            if let Some(f) = copy.facts.clone() {
                tg.buffer.retry_facts(f);
            }
        } else if let Some(fs) = copy.facts.as_ref() {
            let back: Vec<(SpaceId, Key, Fact)> = fs
                .iter()
                .filter(|(s, k, _)| retry.iter().any(|(s2, set)| s2 == s && set.contains(k)))
                .cloned()
                .collect();
            tg.buffer.retry_facts(back);
        }
        let (ids, items) = if retry_all {
            (copy.ids.clone(), copy.items.clone())
        } else {
            (retry, items_retry)
        };
        for i in &copy.items {
            if let Item::Hop { hop, link } = i {
                if items.contains(i) {
                    self.hops.forget(*hop, link);
                } else {
                    let ids = hop_answers
                        .iter()
                        .find(|(h, _)| h == hop)
                        .map(|(_, pairs)| {
                            pairs
                                .iter()
                                .filter(|(l, _)| l == link)
                                .map(|(_, k)| k.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    self.hops.answer(*hop, link.clone(), ids);
                }
            }
        }
        let n_ids = ids.iter().map(|(_, s)| s.len() as u64).sum::<u64>();
        let n_items = items.len() as u64;
        tg.buffer.done(ids, items);
        if !tg.buffer.in_flight() && tg.buffer.pending_len() > 0 {
            // retried work: a new generation at once, so the lane is due again.
            tg.buffer.begin();
        }
        tg.stats.ids_retried += n_ids;
        tg.stats.items_retried += n_items;
        tg.stats.ids_resolved += resolved;
        Completed::Done
    }

    /// The oldest replay position `t` pins.
    pub fn pinned(&self, t: TargetId) -> Option<P> {
        let slot = self.slot(t)?;
        let tg = self.targets[slot].as_ref().unwrap();
        match (tg.buffer.pinned(), tg.seed_pin.as_ref()) {
            (Some(a), Some(b)) => Some(std::cmp::min(a, b).clone()),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        }
    }

    /// Where a restart must read from: the oldest pin of every target and prepare, else the boundary.
    pub fn floor(&self) -> P {
        let mut f = self.stage.boundary().clone();
        if let Some(p) = self.stage.pin() {
            if p < &f {
                f = p.clone();
            }
        }
        for tg in self.targets.iter().flatten() {
            for p in tg.buffer.pinned().into_iter().chain(tg.seed_pin.as_ref()) {
                if p < &f {
                    f = p.clone();
                }
            }
        }
        f
    }

    /// Ids and items `t` holds (pending, in the generation, in the running chunk).
    pub fn pending(&self, t: TargetId) -> usize {
        let Some(tg) = self.slot(t).and_then(|s| self.targets[s].as_ref()) else {
            return 0;
        };
        let mut n = tg.buffer.pending_len() + tg.buffer.flight_len();
        if let Some(r) = &tg.running {
            n += r.ids.iter().map(|(_, s)| s.len()).sum::<usize>() + r.items.len();
        }
        n
    }

    pub fn pending_any(&self) -> bool {
        self.targets.iter().flatten().any(|tg| {
            tg.buffer.pending_len() + tg.buffer.flight_len() > 0
                || tg
                    .running
                    .as_ref()
                    .is_some_and(|r| !r.ids.is_empty() || !r.items.is_empty())
        })
    }

    pub fn busy(&self, t: TargetId) -> bool {
        self.slot(t)
            .and_then(|s| self.targets[s].as_ref())
            .is_some_and(|tg| tg.lane.busy())
    }

    /// `t`'s counters since the last call.
    pub fn take_stats(&mut self, t: TargetId) -> TargetStats {
        match self.slot(t).and_then(|s| self.targets[s].as_mut()) {
            Some(tg) => std::mem::take(&mut tg.stats),
            None => TargetStats::default(),
        }
    }

    /// Estimated heap bytes of every buffer, running chunk and the hop cache.
    pub fn bytes(&self) -> usize {
        let mut b: usize = self
            .targets
            .iter()
            .flatten()
            .map(|tg| tg.buffer.bytes())
            .sum();
        for tg in self.targets.iter().flatten() {
            if let Some(r) = &tg.running {
                b += r.ids.iter().map(|(_, s)| s.bytes()).sum::<usize>() + r.items.len() * 64;
            }
        }
        b + self.hops.bytes()
    }

    /// `bytes() > cfg.max_bytes`: the caller stops reading until jobs bring it down (lazyview: it
    /// flushes; its drain queue then bounds the reader).
    pub fn over_bound(&self) -> bool {
        self.bytes() > self.cfg.max_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::catalog::{FkAction, ForeignKey, TableInfo};

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

    const VIEW_SIMPLE: &str = "SELECT i.id, i.kind_id FROM items i";

    fn items_ref() -> TableRef {
        ("db".into(), "items".into())
    }

    fn pending(core: &Core<u64>, t: TargetId) -> usize {
        core.targets[t.slot as usize]
            .as_ref()
            .unwrap()
            .buffer
            .pending_len()
    }

    fn pinned(core: &Core<u64>, t: TargetId) -> Option<u64> {
        core.targets[t.slot as usize]
            .as_ref()
            .unwrap()
            .buffer
            .pinned()
            .copied()
    }

    fn seed_pinned(core: &Core<u64>, t: TargetId) -> bool {
        core.targets[t.slot as usize]
            .as_ref()
            .unwrap()
            .seed_pin
            .is_some()
    }

    #[test]
    fn rows_reach_open_targets_only() {
        let mut core = core();
        let held = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        core.hold(held, true);
        let mut late_spec = spec(vec![testkit::plan(VIEW_SIMPLE)]);
        late_spec.start = 10;
        let late = core.add_target(late_spec).unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        let ins = testkit::row(&[Some(1), Some(7), Some(1)]);
        core.row(&src, None, Some(&ins));
        let rows = |core: &Core<u64>, t: TargetId| {
            core.targets[t.slot as usize].as_ref().unwrap().stats.rows
        };
        assert_eq!(
            (rows(&core, held), rows(&core, late)),
            (0, 0),
            "held and unstarted are closed"
        );
        core.commit(10, true, Instant::now());
        core.row(&src, None, Some(&ins));
        assert_eq!(
            (rows(&core, held), rows(&core, late)),
            (0, 1),
            "the started target is open"
        );
    }

    #[test]
    fn commit_releases_touched_targets() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        core.row(
            &src,
            None,
            Some(&testkit::row(&[Some(1), Some(7), Some(1)])),
        );
        let now = Instant::now();
        core.commit(1, true, now);
        let tg = core.targets[t.slot as usize].as_ref().unwrap();
        assert!(!tg.touched);
        assert_eq!(tg.buffer.pending_len(), 1, "the id is pending");
        assert_eq!(
            tg.lane.due(now, &Schedule::immediate(10, 10), true, false),
            Some(lane::Due::Flush),
            "the lane's timer is set"
        );
    }

    #[test]
    fn xa_ids_wait_for_xa_commit() {
        // proposal §9 case 7: an XA transaction's ids wait for its XA COMMIT, pinned at its origin.
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let _links = core
            .add_target(spec(vec![testkit::plan(
                "SELECT l.child_id FROM item_links l",
            )]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        let ls = core
            .source(&("db".into(), "item_links".into()), None)
            .unwrap();
        let now = Instant::now();
        core.xa_start("x");
        core.row(
            &src,
            None,
            Some(&testkit::row(&[Some(1), Some(7), Some(1)])),
        );
        core.xa_prepare("x", 5, now);
        for n in 0..1_000u64 {
            core.row(
                &ls,
                None,
                Some(&testkit::row(&[Some(2 + n as i64), Some(3)])),
            );
            core.commit(6 + n, true, now);
        }
        assert_eq!(pending(&core, t), 0, "the XA id is not in the buffer");
        core.xa_end("x", true, 2_000, now);
        assert_eq!(pending(&core, t), 1, "the XA COMMIT releases the id");
        assert_eq!(pinned(&core, t), Some(0), "pinned at the XA's origin");
        core.xa_start("z");
        core.row(
            &src,
            None,
            Some(&testkit::row(&[Some(2), Some(7), Some(1)])),
        );
        core.xa_prepare("z", 2_001, now);
        core.xa_end("z", false, 2_002, now);
        assert_eq!(pending(&core, t), 1, "a rolled back XA leaves nothing");
        core.xa_end("y", true, 2_003, now);
        assert!(
            core.events().contains(&CoreEvent::UnknownPrepare {
                xid: "y".into(),
                at: 2_003
            }),
            "an unknown xid is an event"
        );
    }

    #[test]
    fn row_at_uses_the_given_origin() {
        let mut core = Core::<u64, NoItem, NoJob>::new(50, CoreConfig::direct(8 << 20));
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        core.row_at(
            &3,
            &src,
            None,
            Some(&testkit::row(&[Some(1), Some(7), Some(1)])),
        );
        let tg = core.targets[t.slot as usize].as_ref().unwrap();
        assert_eq!(
            tg.buffer.pinned(),
            Some(&3),
            "the kept row pins at its own origin"
        );
    }

    #[test]
    fn stage_key_lands_in_a_labeled_space() {
        // §9 case 5: the caller's own text keys round-trip through the labeled space.
        let mut core = core();
        let mut s = spec(vec![testkit::plan(VIEW_SIMPLE)]);
        s.labels = vec!["tag_".into()];
        let t = core.add_target(s).unwrap();
        let tag = SpaceId(core.spaces(t).unwrap().len() as u16 - 1);
        for k in ["007", "A-1", "42"] {
            core.stage_key(t, tag, Key::from_text(k), Op::Update);
        }
        core.commit(1, true, Instant::now());
        let tg = core.targets[t.slot as usize].as_mut().unwrap();
        assert!(tg.buffer.begin());
        let taken = tg.buffer.take(10, 10).unwrap();
        let set = taken
            .ids
            .iter()
            .find(|(sp, _)| *sp == tag)
            .map(|(_, set)| set)
            .expect("the tag space");
        let mut got: Vec<String> = set
            .to_keys()
            .into_iter()
            .map(|k| {
                let mut s = String::new();
                k.to_text(&mut s);
                s
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec!["007".to_string(), "42".to_string(), "A-1".to_string()]
        );
    }

    #[test]
    fn a_short_parent_image_is_an_unknown_event() {
        let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols: vec![child_col],
            parent_cols: vec![parent_col.into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        // 6K's derived-cascade catalog: `attrs` cascades from `obj` and `kind`.
        let mut cat: HashMap<TableRef, Arc<TableInfo>> = [
            testkit::table("kind", &[("kid", "smallint unsigned")], &[0]),
            testkit::table("obj", &[("oid", "int unsigned")], &[0]),
            testkit::table("link", &[("gid", "int"), ("oid", "int unsigned")], &[0, 1]),
            testkit::table(
                "attrs",
                &[
                    ("id", "int"),
                    ("oid", "int unsigned"),
                    ("aid", "smallint unsigned"),
                    ("val", "int"),
                ],
                &[0],
            ),
        ]
        .into_iter()
        .collect();
        let mut attrs = (*cat[&("db".into(), "attrs".into())]).clone();
        attrs.cascades = vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")];
        cat.insert(("db".into(), "attrs".into()), Arc::new(attrs));
        let p = testkit::plan_in(
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c \
             JOIN attrs a ON a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, c.oid",
            &cat,
        );
        let mut core = core();
        let t = core.add_target(spec(vec![p])).unwrap();
        let kind = core
            .source(&("db".into(), "kind".into()), None)
            .expect("watched");
        core.row(&kind, Some(&testkit::row(&[])), None);
        assert!(
            core.events().contains(&CoreEvent::Unknown {
                target: t,
                why: "a cascade parent's row image lacks columns",
            }),
            "a short parent image is the caller's safety net"
        );
    }

    #[test]
    fn seeding_pins_until_install() {
        let fk = |child_col: usize| ForeignKey {
            parent: ("db".into(), "obj".into()),
            child_cols: vec![child_col],
            parent_cols: vec!["oid".into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        // 6L's mapped-cascade catalog: `link` and `attrs` cascade from `obj`.
        let mut cat: HashMap<TableRef, Arc<TableInfo>> = [
            testkit::table("grp", &[("gid", "int")], &[0]),
            testkit::table("obj", &[("oid", "int")], &[0]),
            testkit::table("link", &[("gid", "int"), ("oid", "int")], &[0, 1]),
            testkit::table(
                "attrs",
                &[
                    ("id", "bigint"),
                    ("oid", "int"),
                    ("aid", "smallint"),
                    ("val", "int"),
                ],
                &[0],
            ),
        ]
        .into_iter()
        .collect();
        for name in ["link", "attrs"] {
            let mut t = (*cat[&("db".into(), name.into())]).clone();
            t.cascades = vec![fk(1)];
            cat.insert(("db".into(), name.into()), Arc::new(t));
        }
        let p = testkit::plan_in(
            "SELECT g.gid, COUNT(*) AS n, MAX(a.val) AS x FROM grp g JOIN link c ON c.gid = g.gid \
             JOIN attrs a ON a.oid = c.oid AND a.aid = 7 GROUP BY g.gid",
            &cat,
        );
        let mut core = core();
        let t = core.add_target(spec(vec![p])).unwrap();
        core.set_cache(t, 0, Cache::seeding());
        let attrs = core
            .source(&("db".into(), "attrs".into()), None)
            .expect("watched");
        core.row(
            &attrs,
            None,
            Some(&testkit::row(&[Some(1), Some(10), Some(7), Some(5)])),
        );
        assert!(
            seed_pinned(&core, t),
            "a map-child row logged while the cache seeds pins the position"
        );
        core.install_cache(t, 0, Cache::default());
        assert!(
            !seed_pinned(&core, t),
            "installing the seeded cache clears the pin"
        );
    }

    use crate::datum::Datum;

    const VIEW_HOP: &str = "SELECT i.id, l.child_id FROM items i JOIN item_links l ON \
                            l.parent_id = i.kind_id";

    fn links_ref() -> TableRef {
        ("db".into(), "item_links".into())
    }

    fn flush_ids(j: &Job<u64, NoItem, NoJob>) -> usize {
        match &j.kind {
            JobKind::Flush(c) => c.ids.iter().map(|(_, s)| s.len()).sum(),
            JobKind::Executor(_) => usize::MAX,
        }
    }

    fn flush_items(j: &Job<u64, NoItem, NoJob>) -> Vec<Item<NoItem>> {
        match &j.kind {
            JobKind::Flush(c) => c.items.clone(),
            JobKind::Executor(_) => panic!("a flush job"),
        }
    }

    #[test]
    fn due_gives_one_job_per_target_and_bounded_chunks() {
        let mut core = core();
        let mut s = spec(vec![testkit::plan(VIEW_SIMPLE)]);
        s.schedule = Schedule::immediate(1000, 1000);
        let a = core.add_target(s.clone()).unwrap();
        let b = core.add_target(s).unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        for n in 1..=2_500i64 {
            core.row(
                &src,
                None,
                Some(&testkit::row(&[Some(n), Some(1), Some(1)])),
            );
        }
        let now = Instant::now();
        core.commit(1, true, now);
        let j1 = core.due(now);
        assert_eq!(j1.len(), 2, "one job per target");
        assert_eq!((j1[0].target, j1[1].target), (a, b));
        assert_eq!((flush_ids(&j1[0]), flush_ids(&j1[1])), (1000, 1000));
        assert!(core.due(now).is_empty(), "both lanes are busy");
        assert_eq!(core.complete(j1[0].id, JobResult::ok()), Completed::Done);
        let j2 = core.due(now);
        assert_eq!(j2.len(), 1, "the target with the running job gets nothing");
        assert_eq!((j2[0].target, flush_ids(&j2[0])), (a, 1000));
        assert_eq!(core.complete(j1[1].id, JobResult::ok()), Completed::Done);
        let j3 = core.due(now);
        assert_eq!((j3.len(), j3[0].target, flush_ids(&j3[0])), (1, b, 1000));
        assert_eq!(core.complete(j2[0].id, JobResult::ok()), Completed::Done);
        let j4 = core.due(now);
        assert_eq!((j4.len(), j4[0].target, flush_ids(&j4[0])), (1, a, 500));
        assert_eq!(core.complete(j3[0].id, JobResult::ok()), Completed::Done);
        let j5 = core.due(now);
        assert_eq!((j5.len(), j5[0].target, flush_ids(&j5[0])), (1, b, 500));
        assert_eq!(core.complete(j4[0].id, JobResult::ok()), Completed::Done);
        assert_eq!(core.complete(j5[0].id, JobResult::ok()), Completed::Done);
        assert!(core.due(now).is_empty(), "3 jobs per target in all");
        let st = core.take_stats(a);
        assert_eq!((st.jobs, st.ids_taken), (3, 2500));
    }

    #[test]
    fn retry_all_keeps_the_generation_pinned() {
        // proposal §9 case 6: a failed chunk keeps the generation's pin while its ids wait again.
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        core.commit(7, true, Instant::now());
        for n in 1..=3i64 {
            core.row(
                &src,
                None,
                Some(&testkit::row(&[Some(n), Some(1), Some(1)])),
            );
        }
        let now = Instant::now();
        core.commit(8, true, now);
        assert_eq!(core.pinned(t), Some(7), "the ids pin their origin");
        let jobs = core.due(now);
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            core.complete(jobs[0].id, JobResult::failed(None)),
            Completed::Done
        );
        assert_eq!(core.pinned(t), Some(7), "the retried chunk keeps the pin");
        assert_eq!(core.pending(t), 3, "the ids are pending again");
        let jobs = core.due(now);
        assert_eq!(jobs.len(), 1);
        assert_eq!(core.complete(jobs[0].id, JobResult::ok()), Completed::Done);
        assert_eq!(core.pinned(t), None, "nothing pins once all is done");
    }

    #[test]
    fn stale_results_put_the_chunk_back() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        for n in 1..=2i64 {
            core.row(
                &src,
                None,
                Some(&testkit::row(&[Some(n), Some(1), Some(1)])),
            );
        }
        let now = Instant::now();
        core.commit(1, true, now);
        let jobs = core.due(now);
        assert_eq!(jobs.len(), 1);
        let fresh = core
            .replace_target(t, spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        assert_eq!(core.complete(jobs[0].id, JobResult::ok()), Completed::Stale);
        assert_eq!(
            core.pending(fresh),
            2,
            "the chunk is pending for the new incarnation"
        );
    }

    #[test]
    fn hop_answers_fill_the_cache_and_empty_ones_too() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_HOP)]))
            .unwrap();
        let h = (0..16u32)
            .map(HopId)
            .find(|h| core.hops.plan(*h).is_some())
            .expect("the join has one hop");
        assert_eq!(core.hop_of(h).unwrap().0, t);
        let ls = core.source(&links_ref(), None).unwrap();
        core.row(&ls, None, Some(&testkit::row(&[Some(5), Some(3)])));
        core.row(&ls, None, Some(&testkit::row(&[Some(6), Some(4)])));
        let now = Instant::now();
        core.commit(1, true, now);
        let jobs = core.due(now);
        assert_eq!(jobs.len(), 1);
        let items = flush_items(&jobs[0]);
        assert_eq!(items.len(), 2, "one item per link value");
        assert!(
            items
                .iter()
                .all(|i| matches!(i, Item::Hop { hop, .. } if *hop == h)),
            "{items:?}"
        );
        let r = JobResult {
            hop_answers: vec![(h, vec![(Box::new([Datum::Int(3)]), Key::Int(10))])],
            ..JobResult::ok()
        };
        assert_eq!(core.complete(jobs[0].id, r), Completed::Done);
        assert_eq!(
            core.hops.get(h, &[Datum::Int(3)]),
            Some(&[Key::Int(10)][..])
        );
        assert_eq!(
            core.hops.get(h, &[Datum::Int(4)]),
            Some(&[][..]),
            "the taken link without a pair reached no id"
        );
    }

    #[test]
    fn a_failed_item_chunk_is_retried_alone() {
        // proposal §9 case 4 (S4): a failed hop chunk retries alone, not mixed with the rest.
        let mut core = core();
        let mut s = spec(vec![testkit::plan(VIEW_HOP)]);
        s.schedule = Schedule::immediate(100, 2);
        core.add_target(s).unwrap();
        let ls = core.source(&links_ref(), None).unwrap();
        for n in 0..4i64 {
            core.row(
                &ls,
                None,
                Some(&testkit::row(&[Some(10 + n), Some(100 + n)])),
            );
        }
        let now = Instant::now();
        core.commit(1, true, now);
        let j1 = core.due(now);
        assert_eq!(j1.len(), 1, "at most one job per target");
        let first = flush_items(&j1[0]);
        assert_eq!(first.len(), 2, "chunk_items 2");
        let r = JobResult {
            items_retry: first.clone(),
            ..JobResult::ok()
        };
        assert_eq!(core.complete(j1[0].id, r), Completed::Done);
        let j2 = core.due(now);
        assert_eq!(j2.len(), 1);
        let second = flush_items(&j2[0]);
        assert_eq!(core.complete(j2[0].id, JobResult::ok()), Completed::Done);
        let j3 = core.due(now);
        assert_eq!(j3.len(), 1);
        let third = flush_items(&j3[0]);
        assert_eq!(core.complete(j3[0].id, JobResult::ok()), Completed::Done);
        assert!(core.due(now).is_empty(), "everything is taken and done");
        let pairs = [&second, &third];
        assert!(
            second.iter().all(|i| !third.contains(i)),
            "the two chunks never mix: {second:?} {third:?}"
        );
        // The retried items return to the buffer's HashSet and `begin` drains it in hash order,
        // so the pair may come back reversed: compare the chunks as sets, same length and every
        // item of one in the other (chunk items are distinct, so this is set equality).
        assert!(
            pairs
                .iter()
                .any(|p| p.len() == first.len() && p.iter().all(|i| first.contains(i))),
            "the retried pair runs again alone: {second:?} {third:?}"
        );
        assert!(
            pairs.iter().any(|p| p.iter().all(|i| !first.contains(i))),
            "the other two run alone: {second:?} {third:?}"
        );
    }

    #[test]
    fn floor_is_the_oldest_pin() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        let now = Instant::now();
        core.commit(5, true, now);
        core.xa_start("x");
        core.xa_prepare("x", 9, now); // pins where it prepared, then the boundary is 9
        assert_eq!(core.stage.pin(), Some(&5), "the prepare pins at 5");
        core.row_at(
            &3,
            &src,
            None,
            Some(&testkit::row(&[Some(1), Some(1), Some(1)])),
        );
        assert_eq!(core.pinned(t), Some(3), "the target pins at 3");
        assert_eq!(core.floor(), 3);
        core.commit(12, false, now);
        assert_eq!(
            core.stage.boundary(),
            &9,
            "settle false: the boundary stays"
        );
        assert_eq!(core.floor(), 3);
    }

    #[test]
    fn held_target_keeps_its_position() {
        let mut core = core();
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        let src = core.source(&items_ref(), None).unwrap();
        let now = Instant::now();
        core.commit(4, true, now);
        core.row(
            &src,
            None,
            Some(&testkit::row(&[Some(1), Some(1), Some(1)])),
        );
        core.commit(5, true, now);
        core.hold(t, true);
        assert!(core.due(now).is_empty(), "a held target starts nothing");
        assert_eq!(core.floor(), 4, "it keeps its position pinned");
    }

    #[test]
    fn over_bound_counts_every_buffer() {
        let mut core = Core::<u64, NoItem, NoJob>::new(0, CoreConfig::direct(1_000));
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        assert!(!core.over_bound());
        assert!(!core.pending_any());
        let src = core.source(&items_ref(), None).unwrap();
        for n in 0..100i64 {
            core.row(
                &src,
                None,
                Some(&testkit::row(&[Some(n), Some(1), Some(1)])),
            );
        }
        core.commit(1, true, Instant::now());
        assert!(core.pending_any(), "the ids sit in the buffer");
        assert_eq!(core.pending(t), 100);
        assert!(core.over_bound(), "100 integer ids exceed the bound");
    }

    #[test]
    fn executor_jobs_run_on_the_lane() {
        let mut core: Core<u64, NoItem, u32> = Core::new(0, CoreConfig::direct(8 << 20));
        let t = core
            .add_target(spec(vec![testkit::plan(VIEW_SIMPLE)]))
            .unwrap();
        core.enqueue(t, 7);
        let jobs = core.due(Instant::now());
        assert_eq!(jobs.len(), 1);
        assert!(matches!(jobs[0].kind, JobKind::Executor(7)));
        assert_eq!(jobs[0].target, t);
        assert!(core.busy(t));
        assert_eq!(core.complete(jobs[0].id, JobResult::ok()), Completed::Done);
        assert!(!core.busy(t));
        assert!(core.due(Instant::now()).is_empty());
        assert_eq!(
            core.complete(jobs[0].id, JobResult::ok()),
            Completed::Stale,
            "a completed job is not taken twice"
        );
    }
}
