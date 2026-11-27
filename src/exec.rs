//! Executor (a), the host side of the dedup core in lazyview (not copy set; keymap §3): one core target per
//! live version of a view on the Dedup route. A core chunk becomes the input of the engine's own refresh
//! path (`ViewDirty`: units, partial keys, presence marks and holds); hop items become hop queries on the
//! source after the flush's fence (`Hop.select` plus the key filter on the partner columns). Routes are
//! static: a version is on the core iff `AccessPlan::of(plan).route` is `Dedup`.
//!
//! Cost: nothing per binlog row here (the engine calls the core itself); per flush, one hop query per hop
//! and chunk of missed link values, then the refresh their ids need.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use mysql_async::Conn;

use crate::access::{self, AccessPlan, Hop, Occurrence, Path, Route, Via};
use crate::accumulate::{UnitChunk, UnitSet, ViewDirty};
use crate::analyze::{MapPlan, ViewPlan};
use crate::binlog::Position;

use crate::cascade::Cache;
use crate::datum::{Datum, Key as Tuple};
use crate::dedup::{
    Chunk, Core, CoreConfig, HopId, Item, Key, NoItem, Schedule, SpaceId, TargetId, TargetNeeds,
    TargetSpec,
};
use crate::stats::Tally;

/// One live version on the core.
pub struct Entry {
    pub target: TargetId,
    pub plan: Arc<ViewPlan>,
}

/// What `sync` changed.
#[derive(Debug, Default)]
pub struct Synced {
    /// Definitions with a new core target (the caller moves the version's cascade cache in).
    pub added: Vec<u64>,
    /// Definitions whose target was replaced in place (a re-plan: DDL, TRUNCATE, replan, and
    /// every rebuild/swap). `replace_target` keeps the OLD target's cascade caches, so the
    /// caller moves the version's freshly built cache in for these too.
    pub replaced: Vec<u64>,
    /// Definitions whose target `sync` held back for removal (a refused plan whose queue still
    /// held rows only the core had seen) and `advance_replacements` has now removed.
    pub removed: Vec<u64>,
    /// Definitions whose core target `sync` (immediate) or `advance_replacements` (a refusal
    /// held until its queue drained) removed, with the cascade cache taken back out of it
    /// (`move_core_caches` had moved the version's one in): the caller restores it into the
    /// live version's `ViewDirty`, so executor (b) continues with the real map - an
    /// incremental view never falls back to a full rebuild (owner rule).
    pub lost_cache: Vec<(u64, Cache)>,
    /// Definitions the core refused, with the reason: they stay on the `RouteT` path.
    pub refused: Vec<(u64, String)>,
}

/// The core and its targets, by definition id.
pub struct Dedup {
    pub core: Core<Position>,
    entries: HashMap<u64, Entry>,
    schedule: Schedule,
    /// Targets whose cascade cache missed binlog events before this position (a restart gap).
    gaps: HashMap<u64, Position>,
    /// Consecutive job failures per definition (the backoff of `backoff`).
    failures: HashMap<u64, u32>,
    /// Definitions whose plan the core refused: they keep their `RouteT` routes and warn once,
    /// not on every `rebuild_routes` (retried each sync, warned the first time only).
    refused: std::collections::HashSet<u64>,
    /// Definitions whose live version's cache was still seeding when its target was added or
    /// replaced: the target is held (fed nothing) until `deliver_cache` moves the ready cache
    /// in, at the version's install/drain.
    waiting: std::collections::HashSet<u64>,
    /// Replans held back by `sync`: `replace_target` re-registers the target's hops (their ids
    /// are never reused), so items still buffered under the old plan would name dead hops. The
    /// replace runs when the target's queue has drained (`advance_replacements`).
    pending_replace: HashMap<u64, Arc<ViewPlan>>,
    /// Refused definitions whose target still holds buffered items: `remove_target` would hand
    /// those back drained and lost (their rows went to the core only), so the removal waits for
    /// the queue to drain too (`advance_replacements`).
    pending_remove: std::collections::HashSet<u64>,
    /// Rows fed to the core since its jobs last drained (guard R3): the engine's flush trigger
    /// reads this O(1) count per binlog event; `wake` is the earliest retry hold of a busy lane,
    /// recomputed by [`Self::drained`] once per flush, never per event.
    fed: u64,
    wake: Option<Instant>,
    /// Definitions held by `pending_replace` whose registered plan is the SAME shape as the
    /// patched one while the consumer skips occurrences by column count: the registered source
    /// was built for the other column layout, so core-fed rows would be silently dropped. The
    /// engine keeps the (b) cache authoritative for these until `move_core_caches` moves the
    /// patched cache in.
    shape_stale: std::collections::HashSet<u64>,
}

impl Dedup {
    /// A core at `boundary`: chunks of at most `chunk_ids` ids and `chunk_items` hop items, byte bound
    /// `max_bytes`, direct mode.
    pub fn new(
        boundary: Position,
        chunk_ids: usize,
        chunk_items: usize,
        max_bytes: usize,
    ) -> Dedup {
        Dedup {
            core: Core::new(boundary, CoreConfig::direct(max_bytes)),
            entries: HashMap::new(),
            schedule: Schedule::immediate(chunk_ids, chunk_items),
            gaps: HashMap::new(),
            failures: HashMap::new(),
            refused: std::collections::HashSet::new(),
            waiting: std::collections::HashSet::new(),
            pending_replace: HashMap::new(),
            pending_remove: std::collections::HashSet::new(),
            fed: 0,
            wake: None,
            shape_stale: std::collections::HashSet::new(),
        }
    }

    /// Whether `plan` runs on the core: its static route is Dedup.
    pub fn routed(plan: &ViewPlan) -> bool {
        AccessPlan::of(plan).route == Route::Dedup
    }

    /// Why `plan` must keep the viewplan executor although its static route is Dedup, if so:
    /// a `Via::Traced` cascade compiles to no consumer route (the core follows a parent's change
    /// with hop queries on the live source, which cannot see children an FK cascade already
    /// removed server side), and no cascade route at all has the core yet — it lacks #167's
    /// replay-window fixes (the whole-view refresh on a cascade-cache gap), so a mapped
    /// cascade's kept child rows could go stale (D-6T-cascades; cascade-gap-core lifts this).
    /// Meanwhile the viewplan executor marks them on the materialized table itself
    /// (`ViewDirty::marks`). Refusing it is the engine's policy, not the core's.
    fn core_refusal(plan: &ViewPlan) -> Option<String> {
        let access = AccessPlan::of(plan);
        if let Some(why) = access.cascades.iter().find_map(|c| match c.via {
            Via::Traced => Some(format!(
                "cascade `{}` -> `{}` is traced, the core cannot follow its parent's changes",
                c.parent.1, c.child.1
            )),
            _ => None,
        }) {
            return Some(why);
        }
        if let Some(c) = access.cascades.first() {
            return Some(format!(
                "cascade `{}` -> `{}` keeps the executor: the core does not have #167's \
                 replay-window fixes yet, so a gap in the cascade cache would leave kept rows stale",
                c.parent.1, c.child.1
            ));
        }
        // The hop answer cache cannot see changes that land in the same window as the link
        // values they answer (see tmp/out/lvs-step6/6t/hops-followup.md): hop plans keep the
        // executor, which joins the images against the source at every refresh.
        access.occurrences.iter().find_map(|o| match &o.path {
            Path::Hop(_) => Some(format!(
                "alias `{}` resolves its units through a hop, the core answers hops from a cache \
                 that a same-window partner change cannot invalidate",
                o.alias
            )),
            _ => None,
        })
    }

    /// Pairs the items of `a` off against those of `b` under `same`, in any order: a multiset
    /// match, where one item answers exactly one item, so a duplicate on one side needs its twin
    /// on the other. The order of these lists is the order the query WRITES its sources in, and
    /// the names in them are the names the query spells for them — neither is a fact the core
    /// reads, so the comparisons below pair items off instead of walking the two `Vec`s in step.
    fn same_unordered<T, U>(a: &[T], b: &[U], same: impl Fn(&T, &U) -> bool) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut rest: Vec<&U> = b.iter().collect();
        a.iter()
            .all(|x| match rest.iter().position(|y| same(x, y)) {
                Some(i) => {
                    rest.remove(i);
                    true
                }
                None => false,
            })
    }

    /// Whether the core compiles the same classifier from two occurrences: `Consumer::add` takes
    /// the read mask from `table` and `read_columns`, which rows are the alias's from `filters`,
    /// and the `Way` from `path`. It never reads `Occurrence::alias`, the name the query happens
    /// to spell for the table (the analyzer resolved it into the plan's ordinals), so `FROM items
    /// i` and `FROM items x` are the same classifier over the same rows.
    fn same_occurrence(x: &Occurrence, y: &Occurrence) -> bool {
        x.table == y.table
            && x.read_columns == y.read_columns
            && x.filters == y.filters
            && x.path == y.path
    }

    /// Whether both plans have the same unit as the core reads it: its expressions, in order (a
    /// unit is an ordered tuple, so a permutation is another unit), and whether one unit maps to
    /// at most one materialized row. `IdPart::output` is left out: the index of the projection
    /// column that happens to yield an expression, while the core marks units by value.
    fn same_unit(a: &access::TargetId, b: &access::TargetId) -> bool {
        a.unique == b.unique
            && a.parts.len() == b.parts.len()
            && a.parts.iter().zip(&b.parts).all(|(x, y)| x.expr == y.expr)
    }

    /// Whether the core's consumer classifies rows identically for the two plans: the same
    /// occurrences (each source table, the columns it reads, its constant filters, the path its
    /// changed rows take to the units), the same cascades, and the same unit. `Occurrence` is that
    /// classification itself: `Consumer::add` compiles an occurrence's read mask from
    /// `read_columns` and its table, its filters from `filters`, and its `Way` from `path`, and the
    /// map routes come from `cascades` and the child's stored columns.
    ///
    /// What a re-plan changes besides these — its name, hash, the order the sources are written
    /// in, how each source is aliased, which projection column yields a unit — leaves the rows it
    /// derives alone, so each list is paired off by these facts rather than position by position.
    fn same_consumer_reads(a: &AccessPlan, b: &AccessPlan) -> bool {
        Self::same_unit(&a.id, &b.id)
            && Self::same_unordered(&a.occurrences, &b.occurrences, Self::same_occurrence)
            && Self::same_unordered(&a.cascades, &b.cascades, |x, y| x == y)
    }

    /// Whether two cascade maps keep the same rows: the same key, the same stored columns, and
    /// the same filters of the child's aliases. Those lists are paired off too — `MapPlan::keeps`
    /// passes a row that answers the filters of SOME alias, so the order the child's aliases were
    /// written in decides nothing about what the cache keeps.
    fn same_map(x: &MapPlan, y: &MapPlan) -> bool {
        x.key == y.key
            && x.cols == y.cols
            && Self::same_unordered(&x.filters, &y.filters, |x, y| x == y)
    }

    /// Whether both plans were made against the same source columns: same aliases over the same
    /// tables with the same layouts, same cascades over the same parent and child with the same
    /// map of what the cascade cache keeps. A difference here means a DDL moved the layout under
    /// the plan (not the definition), which is what the TABLE_MAP handler's shape machinery
    /// (`core_hold_prefix`, `mark_shape_stale`) is for. The two lists are paired off, like the
    /// reads above: which source the query writes first, and what it calls it, moves no column.
    fn same_source_layouts(kept: &ViewPlan, plan: &ViewPlan) -> bool {
        Self::same_unordered(&kept.aliases, &plan.aliases, |x, y| x.table == y.table)
            && Self::same_unordered(&kept.cascades, &plan.cascades, |x, y| {
                x.parent == y.parent
                    && x.child == y.child
                    && x.child_table == y.child_table
                    && match (&x.map, &y.map) {
                        (Some(x), Some(y)) => Self::same_map(x, y),
                        (None, None) => true,
                        _ => false,
                    }
            })
    }

    /// Whether `sync` may hold a replace of `kept` by `plan` while the target's queue drains:
    /// meanwhile the REGISTERED consumer is the one deciding which rows of this view's tables are
    /// irrelevant, so holding is free only when both consumers decide the same way.
    ///
    /// A re-plan of the same definition (its hash: a DDL's new column positions, a re-analyze)
    /// keeps the rule as it was - the shape machinery above holds on it. A changed definition
    /// waits only over unchanged source columns and identical reads, and those are compared by
    /// what the core reads: in any written order, under any alias spelling. #385 was a definition
    /// change that started reading a column the previous definition did not read, and while the
    /// hold was in force a change to that column was skipped as irrelevant for a materialized
    /// table that already had it, with nothing left to re-derive the row.
    ///
    /// Cost: two `AccessPlan::of` calls per replace of a busy target, never per row.
    fn may_hold_replace(kept: &ViewPlan, plan: &ViewPlan) -> bool {
        let (a, b) = (AccessPlan::of(kept), AccessPlan::of(plan));
        a.hash == b.hash
            || (Self::same_source_layouts(kept, plan) && Self::same_consumer_reads(&a, &b))
    }

    /// Mirrors `live` (definition id, plan) onto core targets: a routed plan gets a target (added, or
    /// replaced in place when the plan object changed), and every target not listed is removed. A target
    /// starts at once (`start` = the smallest position): lazyview adds them between transactions.
    pub fn sync(&mut self, live: &[(u64, Arc<ViewPlan>)]) -> Synced {
        let mut out = Synced::default();
        let mut keep = std::collections::HashSet::new();
        for (id, plan) in live {
            if !Self::routed(plan) {
                continue;
            }
            if let Some(why) = Self::core_refusal(plan) {
                // Keep its id (an added-then-refused target is removed here, not by the stale
                // sweep, so the `refused` marker below survives the sweep's `retain`).
                keep.insert(*id);
                let had_entry = self.entries.contains_key(id);
                let hold = had_entry
                    && self.entries.get(id).is_some_and(|e| {
                        self.core.pending(e.target) > 0 || self.core.busy(e.target)
                    });
                if hold {
                    // `remove_target` hands buffered items back drained, and these rows went to
                    // the core only: keep the target until it has applied them, and keep the
                    // entry (its plan Arc no longer matches, so `rebuild_routes` builds the
                    // `RouteT` the executor needs meanwhile). `advance_replacements` removes it,
                    // taking its cascade cache out then (the draining jobs still expand from it).
                    self.pending_remove.insert(*id);
                } else if let Some(e) = self.entries.remove(id) {
                    // The version's cascade cache went into the target (`move_core_caches`):
                    // take it back out before dropping the target (the live version keeps its
                    // `plan`, executor (b) continues with the real map - no rebuild).
                    let cache = self
                        .core
                        .cache_mut(e.target, 0)
                        .map(std::mem::take)
                        .unwrap_or_default();
                    self.core.remove_target(e.target);
                    self.gaps.remove(id);
                    self.failures.remove(id);
                    self.waiting.remove(id);
                    self.pending_remove.remove(id);
                    out.lost_cache.push((*id, cache));
                }
                self.pending_replace.remove(id);
                if self.refused.insert(*id) {
                    out.refused.push((*id, why));
                }
                continue;
            }
            keep.insert(*id);
            self.pending_remove.remove(id);
            let spec = TargetSpec {
                plans: vec![plan.clone()],
                labels: vec![],
                start: Position {
                    file: String::new(),
                    pos: 0,
                },
                schedule: self.schedule,
                needs: TargetNeeds::default(),
            };
            let old = self.entries.get(id).map(|e| (e.target, e.plan.clone()));
            let mut add = true;
            if let Some((t, kept)) = &old {
                if Arc::ptr_eq(kept, plan) {
                    self.pending_replace.remove(id);
                    self.refused.remove(id);
                    continue;
                }
                if kept.unit_exprs == plan.unit_exprs && kept.partial.sets == plan.partial.sets {
                    // `replace_target` re-registers the hops (their ids are never reused):
                    // items still buffered under the old plan would name dead hops. Hold the
                    // replace until the target's queue has drained (`advance_replacements`) —
                    // and hold it only while that costs nothing. Until the replace runs, the
                    // REGISTERED consumer is the one deciding which rows of this view's tables
                    // are irrelevant, so a re-plan that reads differently must not wait:
                    // #385 was a definition change that started reading a column the previous
                    // definition did not read, and while the hold was in force a change to that
                    // column was skipped as irrelevant for a materialized table that already
                    // had it, with nothing left to re-derive the row.
                    let queued = self.core.pending(*t) > 0 || self.core.busy(*t);
                    if queued && Self::may_hold_replace(kept, plan) {
                        self.pending_replace.insert(*id, plan.clone());
                        self.refused.remove(id);
                        add = false;
                    } else {
                        match self.core.replace_target(*t, spec.clone()) {
                            Ok(t) => {
                                self.entries.insert(
                                    *id,
                                    Entry {
                                        target: t,
                                        plan: plan.clone(),
                                    },
                                );
                                self.pending_replace.remove(id);
                                self.refused.remove(id);
                                out.replaced.push(*id);
                                add = false;
                            }
                            Err(_) => {
                                self.core.remove_target(*t);
                            }
                        }
                    }
                } else {
                    self.core.remove_target(*t);
                }
            }
            if add {
                match self.core.add_target(spec) {
                    Ok(t) => {
                        self.entries.insert(
                            *id,
                            Entry {
                                target: t,
                                plan: plan.clone(),
                            },
                        );
                        out.added.push(*id);
                        self.refused.remove(id);
                        self.pending_replace.remove(id);
                    }
                    Err(why) => {
                        if self.refused.insert(*id) {
                            out.refused.push((*id, why));
                        }
                    }
                }
            }
        }
        let stale: Vec<u64> = self
            .entries
            .keys()
            .filter(|id| !keep.contains(id))
            .copied()
            .collect();
        for id in stale {
            let e = self.entries.remove(&id).unwrap();
            self.core.remove_target(e.target);
            self.gaps.remove(&id);
            self.failures.remove(&id);
            self.waiting.remove(&id);
            self.pending_replace.remove(&id);
            self.pending_remove.remove(&id);
        }
        self.refused.retain(|id| keep.contains(id));
        out
    }

    /// Replaces `id`'s target with `plan` in place, or rebuilds it (remove and add) when the
    /// core refuses the replace (different spaces). `false`: the core refused the plan itself.
    fn apply_replace(&mut self, id: u64, plan: &Arc<ViewPlan>) -> bool {
        let spec = TargetSpec {
            plans: vec![plan.clone()],
            labels: vec![],
            start: Position {
                file: String::new(),
                pos: 0,
            },
            schedule: self.schedule,
            needs: TargetNeeds::default(),
        };
        let t = match self.entries.get(&id) {
            Some(e) => e.target,
            None => {
                return match self.core.add_target(spec) {
                    Ok(t) => {
                        self.entries.insert(
                            id,
                            Entry {
                                target: t,
                                plan: plan.clone(),
                            },
                        );
                        true
                    }
                    Err(_) => false,
                }
            }
        };
        match self.core.replace_target(t, spec.clone()) {
            Ok(t) => {
                self.entries.insert(
                    id,
                    Entry {
                        target: t,
                        plan: plan.clone(),
                    },
                );
                true
            }
            Err(_) => {
                self.core.remove_target(t);
                match self.core.add_target(spec) {
                    Ok(t) => {
                        self.entries.insert(
                            id,
                            Entry {
                                target: t,
                                plan: plan.clone(),
                            },
                        );
                        true
                    }
                    Err(_) => false,
                }
            }
        }
    }

    /// The replans `sync` held back (a target with buffered items cannot be replaced: its items
    /// would name the old plan's dead hops), once their targets have drained; and the removals
    /// of refused targets `sync` held back (`remove_target` would drop their buffered items,
    /// which only the core had seen). Like `sync`, it reports the definitions whose target is
    /// new, replaced, or finally removed: the caller moves the version's cascade cache in for
    /// each new one, takes it back out of each removed one (`Synced::lost_cache`), and rebuilds
    /// routes when one is gone.
    pub fn advance_replacements(&mut self) -> Synced {
        let mut out = Synced::default();
        let todo: Vec<(u64, Arc<ViewPlan>)> = self
            .pending_replace
            .iter()
            .map(|(id, plan)| (*id, plan.clone()))
            .collect();
        for (id, plan) in todo {
            match self.entries.get(&id) {
                None => {
                    self.pending_replace.remove(&id);
                }
                Some(e) if self.core.pending(e.target) > 0 || self.core.busy(e.target) => {}
                Some(_) => {
                    self.pending_replace.remove(&id);
                    if self.apply_replace(id, &plan) {
                        out.replaced.push(id);
                    }
                }
            }
        }
        let gone: Vec<u64> = self.pending_remove.iter().copied().collect();
        for id in gone {
            match self.entries.get(&id) {
                None => {
                    self.pending_remove.remove(&id);
                }
                Some(e) if self.core.pending(e.target) > 0 || self.core.busy(e.target) => {}
                Some(e) => {
                    let t = e.target;
                    // The queue drained: take the version's cascade cache out before dropping
                    // the target (see `sync`), so executor (b) keeps the real map - no rebuild.
                    let cache = self
                        .core
                        .cache_mut(t, 0)
                        .map(std::mem::take)
                        .unwrap_or_default();
                    self.entries.remove(&id);
                    self.core.remove_target(t);
                    self.gaps.remove(&id);
                    self.failures.remove(&id);
                    self.waiting.remove(&id);
                    self.pending_remove.remove(&id);
                    out.removed.push(id);
                    out.lost_cache.push((id, cache));
                }
            }
        }
        out
    }

    pub fn entry(&self, def_id: u64) -> Option<&Entry> {
        self.entries.get(&def_id)
    }

    /// Whether the plan last synced for `def_id` was refused (by `core_refusal` or the core
    /// itself): its rows must keep the `RouteT` path even while a held-back target lingers.
    pub fn is_refused(&self, def_id: u64) -> bool {
        self.refused.contains(&def_id)
    }

    /// Whether a replace of `def_id`'s target waits for its queue to drain (`sync`): the plans
    /// are equal in shape (same unit expressions and partial sets), so the running target is
    /// still the authority - the version's rows keep going to the core meanwhile.
    pub fn replacing(&self, def_id: u64) -> bool {
        self.pending_replace.contains_key(&def_id)
    }

    /// Whether the core runs `plan` for `def_id`: pointer-equal to the registered plan, or an
    /// equal-shape replace held back by `sync` (`replacing`: `sync` holds one only when both
    /// consumers classify rows the same way, [`Self::may_hold_replace`], which is what makes the
    /// running target read the same way) that is not marked shape-stale - while marked, the
    /// registered source was built for another column layout and would silently drop the
    /// occurrences it skips by column count.
    pub fn plan_matches(&self, def_id: u64, plan: &Arc<ViewPlan>) -> bool {
        self.entries.get(&def_id).is_some_and(|e| {
            Arc::ptr_eq(&e.plan, plan)
                || (self.replacing(def_id) && !self.shape_stale.contains(&def_id))
        })
    }

    /// Whether `def_id`'s held replace is marked shape-stale (see [`Self::plan_matches`]).
    pub fn core_shape_stale(&self, def_id: u64) -> bool {
        self.shape_stale.contains(&def_id)
    }

    /// The engine saw a TABLE_MAP this version's registered source skips while an equal-shape
    /// replace is held back: core-fed rows would vanish. Returns whether the mark is new.
    pub(crate) fn mark_shape_stale(&mut self, def_id: u64) -> bool {
        self.shape_stale.insert(def_id)
    }

    /// The patched cache reached the core (`move_core_caches`): the source reads the live
    /// layout again and the (a) authority returns.
    pub(crate) fn clear_shape_stale(&mut self, def_id: u64) {
        self.shape_stale.remove(&def_id);
    }

    /// `feed_core` fed `n` rows to the core: its targets have work (O(1), guard R3).
    pub fn note_rows(&mut self, n: usize) {
        self.fed += n as u64;
    }

    /// When the engine's flush must wake for the core targets (guard R3: two field reads
    /// per binlog event, never a pass over the core): the earliest retry hold cached by
    /// [`Self::drained`]. Rows fed to the core need no extra wake here: `feed_core` marks the
    /// version dirty, and the batch-window deadline of executor (b) covers them - waking per
    /// transaction would refresh a unit once per binlog event. A feed left over by an
    /// interrupted flush wakes through [`Self::rows_pending`] in the engine.
    pub fn trigger(&self, _now: Instant) -> Option<Instant> {
        self.wake
    }

    /// Rows are buffered in the core and no flush has taken them yet.
    pub fn rows_pending(&self) -> bool {
        self.fed > 0
    }

    /// The flush took the rows (`flush_dedup` ended): reset the running count and cache the
    /// core's earliest retry hold (`next_retry` is O(targets): per flush, never per event).
    pub fn drained(&mut self) {
        self.fed = 0;
        self.wake = self.core.next_retry();
    }

    pub fn def_of(&self, t: TargetId) -> Option<u64> {
        self.entries
            .iter()
            .find(|(_, e)| e.target == t)
            .map(|(id, _)| *id)
    }

    /// Moves a live version's cascade cache into its core target (its restart gap, if any, with it).
    pub fn set_cache(&mut self, def_id: u64, mut cache: Cache) {
        let Some(t) = self.entries.get(&def_id).map(|e| e.target) else {
            return;
        };
        if let Some(until) = cache.gap_until.take() {
            self.gaps.insert(def_id, until);
        }
        self.core.set_cache(t, 0, cache);
    }

    /// A target added or replaced while its version's cache still seeds: held (fed nothing, no
    /// job started, position pinned) until [`Self::deliver_cache`] moves the ready cache in.
    pub fn hold_for_cache(&mut self, def_id: u64) {
        if let Some(t) = self.entries.get(&def_id).map(|e| e.target) {
            self.core.hold(t, true);
            self.waiting.insert(def_id);
        }
    }

    /// The definitions held by [`Self::hold_for_cache`], waiting for their version's install/drain.
    pub fn waiting_cache(&self) -> Vec<u64> {
        self.waiting.iter().copied().collect()
    }

    /// The version's install/drain completed: its ready cache goes to the target and the hold lifts.
    pub fn deliver_cache(&mut self, def_id: u64, cache: Cache) {
        if !self.waiting.remove(&def_id) {
            return;
        }
        if let Some(t) = self.entries.get(&def_id).map(|e| e.target) {
            self.core.hold(t, false);
        }
        self.set_cache(def_id, cache);
    }

    /// A transaction boundary at `at`: the core's, and the restart gaps that end there.
    pub fn boundary(&mut self, at: &Position, now: Instant) {
        self.core.commit(at.clone(), true, now);
        for (id, until) in &self.gaps {
            if let Some(t) = self.entries.get(id).map(|e| e.target) {
                if let Some(c) = self.core.cache_mut(t, 0) {
                    c.gap = at < until;
                }
            }
        }
        self.gaps.retain(|_, until| at < until);
    }

    pub fn pinned(&self, def_id: u64) -> Option<Position> {
        self.entries
            .get(&def_id)
            .and_then(|e| self.core.pinned(e.target))
    }

    pub fn pending(&self, def_id: u64) -> u64 {
        self.entries
            .get(&def_id)
            .map_or(0, |e| self.core.pending(e.target) as u64)
    }

    /// The core's counters of `def_id` since the last call, in `/stats` terms.
    pub fn tally(&mut self, def_id: u64) -> Tally {
        let Some(t) = self.entries.get(&def_id).map(|e| e.target) else {
            return Tally::default();
        };
        let s = self.core.take_stats(t);
        Tally {
            rows: s.rows,
            rows_skipped: s.rows_skipped,
            ids_marked: s.ids_marked,
            ids_absorbed: s.ids_absorbed,
            images_marked: s.items_marked,
            images_absorbed: s.items_absorbed,
            cascade_rows: s.cascade_rows,
            cascade_gaps: s.cascade_gaps,
            xa_unknown_commits: 0,
            // The core does not report a short image yet (`cascade-gap-core`'s host-side half).
            cascade_short_images: 0,
        }
    }

    /// A chunk as refresh input: space 0 → units, space k + 1 → partial set k, presence items → marks
    /// and holds. The hop items come back apart.
    pub fn to_dirty(
        plan: &ViewPlan,
        chunk: Chunk<Position, NoItem>,
    ) -> (ViewDirty, Vec<(HopId, Tuple)>) {
        let mut dirty = ViewDirty::for_aliases(plan.aliases.len());
        let mut hops = Vec::new();
        for (s, set) in chunk.ids {
            if s == SpaceId::FULL {
                dirty.units.extend(set.into());
            } else {
                let k = s.index() - 1;
                dirty
                    .partial
                    .resize_with(plan.partial.sets.len(), UnitSet::default);
                dirty.partial[k].extend(set.into());
            }
        }
        for item in chunk.items {
            match item {
                Item::Hop { hop, link } => hops.push((hop, link)),
                Item::Presence { index, value, hold } => match hold {
                    true => dirty.holds.push((index, value)),
                    false => dirty.marks.push((index, value)),
                },
                Item::User(n) => match n {},
            }
        }
        (dirty, hops)
    }

    /// One hop statement: `hop.select` and the key filter on the partner columns for `links`.
    /// Numeric links go as one statement. Any other link (text, date, JSON) goes as one branch
    /// per link, labeled with its index: the server matches under the column's collation and
    /// may answer with a different spelling than the item queried (`= 'ABC'` finds 'abc '),
    /// while `complete` groups the pairs by the item's link with Rust equality — an unlabeled
    /// row would name the answered spelling, leaving the item's entry cached empty.
    pub fn hop_sql(hop: &Hop, links: &[Tuple]) -> (String, Vec<mysql_async::Value>) {
        if links
            .iter()
            .all(|l| l.iter().all(|d| matches!(d, Datum::Int(_))))
        {
            let chunk = if links.iter().all(|l| matches!(&**l, [Datum::Int(_)])) {
                let mut v: Vec<i64> = links
                    .iter()
                    .map(|l| match &**l {
                        [Datum::Int(i)] => *i,
                        _ => unreachable!("checked above"),
                    })
                    .collect();
                v.sort_unstable();
                v.dedup();
                UnitChunk::Ints(v)
            } else {
                UnitChunk::Keys(links.to_vec())
            };
            let (f, params) = chunk.filter(&hop.partner);
            return (format!("{} AND {f}", hop.select), params);
        }
        let mut sql = String::new();
        let mut params = Vec::new();
        for (i, l) in links.iter().enumerate() {
            if i > 0 {
                sql.push_str(" UNION ALL ");
            }
            let (f, p) = UnitChunk::Keys(vec![l.clone()]).filter(&hop.partner);
            sql.push_str(&format!(
                "SELECT {i} AS `__lnk`, `t{i}`.* FROM ({} AND ({f})) AS `t{i}`",
                hop.select
            ));
            params.extend(p);
        }
        (sql, params)
    }

    /// Runs the hop statements for `links` on `conn`, at most `chunk` links per statement: the answers
    /// (per hop, (link, id) pairs) and the number of statements.
    pub async fn hop_answers(
        &self,
        conn: &mut Conn,
        links: &[(HopId, Tuple)],
        chunk: usize,
        fresh: bool,
    ) -> Result<(Vec<(HopId, Vec<(Tuple, Key)>)>, u64)> {
        let mut by_hop: std::collections::BTreeMap<HopId, Vec<Tuple>> = Default::default();
        for (h, l) in links {
            by_hop.entry(*h).or_default().push(l.clone());
        }
        let mut out = Vec::with_capacity(by_hop.len());
        let mut stmts = 0u64;
        for (h, ls) in by_hop {
            let (_, hop) = self
                .hop(h)
                .ok_or_else(|| anyhow::anyhow!("hop {:?} is gone", h))?;
            let n = hop.partner.len();
            let mut answers = Vec::new();
            for c in ls.chunks(chunk.max(1)) {
                let (sql, params) = Self::hop_sql(hop, c);
                let labeled = c
                    .iter()
                    .any(|l| !l.iter().all(|d| matches!(d, Datum::Int(_))));
                for row in
                    crate::mv::exec_source::<mysql_async::Row>(conn, sql, params, fresh).await?
                {
                    let vals: Vec<Datum> =
                        row.unwrap().into_iter().map(Datum::from_value).collect();
                    // A labeled statement puts the link's index in the chunk first: the pair
                    // carries the item's own link bytes (`complete` groups by those).
                    let (link, rest): (Tuple, &[Datum]) = if labeled {
                        let i = match vals.first() {
                            Some(Datum::Int(i)) if (0..c.len()).contains(&(*i as usize)) => {
                                *i as usize
                            }
                            _ => continue,
                        };
                        if vals.len() < 1 + n {
                            continue;
                        }
                        (c[i].clone(), &vals[1 + n..])
                    } else {
                        if vals.len() < n {
                            continue;
                        }
                        let (a, b) = vals.split_at(n);
                        (a.to_vec().into_boxed_slice(), b)
                    };
                    answers.push((link, Key::from_parts(rest.to_vec().into_boxed_slice())));
                }
                stmts += 1;
            }
            out.push((h, answers));
        }
        Ok((out, stmts))
    }

    /// Adds the answers' ids to `dirty` (the full id → units, a partial set's positions → its partial
    /// keys); how many were new.
    pub fn add_answers(
        &self,
        dirty: &mut ViewDirty,
        answers: &[(HopId, Vec<(Tuple, Key)>)],
    ) -> u64 {
        let mut added = 0u64;
        for (h, answers) in answers {
            let Some((plan, hop)) = self.hop(*h) else {
                continue;
            };
            if hop.positions.len() == plan.unit_exprs.len() {
                for (_, key) in answers {
                    added += u64::from(dirty.units.insert(key.parts()));
                }
            } else {
                let k = plan
                    .partial
                    .sets
                    .iter()
                    .position(|s| *s == hop.positions)
                    .expect("a hop gives the unit positions of a partial set (6A)");
                dirty
                    .partial
                    .resize_with(plan.partial.sets.len(), UnitSet::default);
                for (_, key) in answers {
                    added += u64::from(dirty.partial[k].insert(key.parts()));
                }
            }
        }
        added
    }

    /// The retry delay of `def_id`'s next job: after a failure `250 ms << n` (n consecutive failures,
    /// at most 30 s); a success resets it and gives zero. Executor (a) never re-plans for failures.
    pub fn backoff(&mut self, def_id: u64, failed: bool) -> std::time::Duration {
        if !failed {
            self.failures.remove(&def_id);
            return std::time::Duration::ZERO;
        }
        let n = self.failures.entry(def_id).or_insert(0);
        let delay = std::time::Duration::from_millis(250 << (*n).min(7))
            .min(std::time::Duration::from_secs(30));
        *n += 1;
        delay
    }

    /// The hop of `h`: its plan and access path.
    fn hop(&self, h: HopId) -> Option<(&ViewPlan, &Hop)> {
        let (t, pi, alias) = self.core.hop_of(h)?;
        let Path::Hop(hop) = &self.core.access(t)?[pi].occurrences[alias].path else {
            return None;
        };
        let id = self.def_of(t)?;
        Some((&self.entries[&id].plan, hop))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::test_catalog::typed;
    use crate::catalog::TableRef;
    use crate::dedup::{
        testkit::{plan, row},
        IdSet,
    };

    fn pos(p: u64) -> Position {
        Position {
            file: String::new(),
            pos: p,
        }
    }

    fn dedup() -> Dedup {
        Dedup::new(pos(0), 100, 100, 1 << 20)
    }

    fn table_ref(name: &str) -> TableRef {
        ("db".into(), name.into())
    }

    fn link(v: i64) -> Tuple {
        Box::new([Datum::Int(v)])
    }

    const ONE_HOP: &str = "SELECT i.id, l.child_id FROM items i JOIN item_links l ON \
                           l.parent_id = i.kind_id";
    /// Routed with no hop: its images carry the unit positions themselves.
    const DIRECT: &str = "SELECT i.id, i.kind_id FROM items i";
    const TWO_PARTIALS: &str = "SELECT l.parent_id, i.id, MAX(v1.val) AS a, MAX(v2.val) AS b \
                                FROM item_links l JOIN items i ON i.id = l.child_id JOIN \
                                item_values v1 ON v1.item_id = i.id AND v1.key_id = 7 JOIN \
                                item_values v2 ON v2.key_id = l.parent_id GROUP BY l.parent_id, \
                                i.id";
    /// `TWO_PARTIALS` with its two `item_values` joins written the other way round: the same
    /// reads in another written order (`AccessPlan::occurrences` follow the query).
    const JOINS_PERMUTED: &str = "SELECT l.parent_id, i.id, MAX(v1.val) AS a, MAX(v2.val) AS b \
                                  FROM item_links l JOIN items i ON i.id = l.child_id JOIN \
                                  item_values v2 ON v2.key_id = l.parent_id JOIN item_values v1 \
                                  ON v1.item_id = i.id AND v1.key_id = 7 GROUP BY l.parent_id, \
                                  i.id";
    /// `TWO_PARTIALS` with the two `item_values` aliases spelled `k1` and `k2`: the same reads
    /// under other names (`Occurrence::alias` is what the query happens to spell).
    const JOINS_RENAMED: &str = "SELECT l.parent_id, i.id, MAX(k1.val) AS a, MAX(k2.val) AS b \
                                 FROM item_links l JOIN items i ON i.id = l.child_id JOIN \
                                 item_values k1 ON k1.item_id = i.id AND k1.key_id = 7 JOIN \
                                 item_values k2 ON k2.key_id = l.parent_id GROUP BY l.parent_id, \
                                 i.id";
    /// `JOINS_PERMUTED` with one more column of `v2` in the projection: another written order AND
    /// another read, so the consumer must go through at once.
    const JOINS_PERMUTED_READS_MORE: &str = "SELECT l.parent_id, i.id, MAX(v1.val) AS a, \
                                             MAX(v2.val) AS b, MAX(v2.note) AS c \
                                             FROM item_links l JOIN items i ON i.id = l.child_id \
                                             JOIN item_values v2 ON v2.key_id = l.parent_id JOIN \
                                             item_values v1 ON v1.item_id = i.id AND v1.key_id = 7 \
                                             GROUP BY l.parent_id, i.id";

    #[test]
    fn routed_follows_the_access_plan() {
        assert!(Dedup::routed(&plan("SELECT i.id, i.kind_id FROM items i")));
        assert!(!Dedup::routed(&plan("SELECT COUNT(*) n FROM items")));
    }

    #[test]
    fn sync_adds_replaces_and_removes() {
        let mut d = dedup();
        let p = plan(DIRECT);
        let s = d.sync(&[(1, p.clone())]);
        assert_eq!(s.added, vec![1], "{:?}", s.refused);
        let t0 = d.entry(1).unwrap().target;
        let s = d.sync(&[(1, p.clone())]);
        assert!(s.added.is_empty() && s.refused.is_empty());
        assert_eq!(d.entry(1).unwrap().target, t0, "the same Arc is kept");
        let s = d.sync(&[(1, Arc::new(Arc::unwrap_or_clone(p.clone())))]);
        assert!(s.added.is_empty() && s.refused.is_empty());
        let t1 = d.entry(1).unwrap().target;
        assert_eq!(t1.slot, t0.slot);
        assert!(t1.incarnation > t0.incarnation, "replaced in place");
        d.sync(&[]);
        assert!(d.entry(1).is_none());
    }

    #[test]
    fn sync_re_adds_a_target_whose_id_changed() {
        let mut d = dedup();
        let old = d.sync(&[(1, plan(DIRECT))]);
        assert_eq!(old.added, vec![1]);
        let t0 = d.entry(1).unwrap().target;
        let s = d.sync(&[(1, plan(TWO_PARTIALS))]);
        assert_eq!(s.added, vec![1], "{:?}", s.refused);
        assert!(d.def_of(t0).is_none(), "the old target is gone");
        assert_ne!(d.entry(1).unwrap().target, t0);
    }

    /// Cascade fixtures (mirroring the analyze planner's cascade tests): `kind` -> `realm` and
    /// `obj_attr` -> (`obj`, `kind`) always cascade; `map_shape` also gives `link` its cascading
    /// FK to `obj` (and `note` to `obj`), the map-cascade catalog.
    fn fk_catalog(map_shape: bool) -> HashMap<TableRef, Arc<crate::catalog::TableInfo>> {
        use crate::catalog::{Column, FkAction, ForeignKey};
        let int = |n: &str, ct: &str| {
            Column::new(
                n,
                ct.split(' ').next().unwrap(),
                ct,
                None,
                None,
                None,
                false,
            )
        };
        let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols: vec![child_col],
            parent_cols: vec![parent_col.into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        let with_fk = |t: (TableRef, Arc<crate::catalog::TableInfo>), f: Vec<ForeignKey>| {
            let mut i = (*t.1).clone();
            i.cascades = f;
            (t.0, Arc::new(i))
        };
        let link = typed(
            "link",
            vec![
                int("gid", "int unsigned"),
                int("oid", "int unsigned"),
                int("pos", "int"),
            ],
            &[0, 1],
        );
        let note = typed(
            "note",
            vec![int("oid", "int unsigned"), int("w", "int")],
            &[],
        );
        [
            typed("realm", vec![int("rid", "int unsigned")], &[0]),
            with_fk(
                typed(
                    "kind",
                    vec![int("kid", "smallint unsigned"), int("rid", "int unsigned")],
                    &[0],
                ),
                vec![fk("realm", 1, "rid")],
            ),
            typed("grp", vec![int("gid", "int unsigned")], &[0]),
            typed("obj", vec![int("oid", "int unsigned")], &[0]),
            if map_shape {
                with_fk(link, vec![fk("obj", 1, "oid")])
            } else {
                link
            },
            with_fk(
                typed(
                    "obj_attr",
                    vec![
                        int("id", "bigint unsigned"),
                        int("oid", "int unsigned"),
                        int("aid", "smallint unsigned"),
                        int("val", "int"),
                    ],
                    &[0],
                ),
                vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")],
            ),
            if map_shape {
                with_fk(note, vec![fk("obj", 0, "oid")])
            } else {
                note
            },
        ]
        .into_iter()
        .collect()
    }

    fn plan_in(
        sql: &str,
        cat: &HashMap<TableRef, Arc<crate::catalog::TableInfo>>,
    ) -> Arc<ViewPlan> {
        let d = crate::analyze::parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        Arc::new(crate::analyzer::plan(&d, "db", cat).unwrap())
    }

    /// Closes the staged units (a transaction boundary) and runs the queued jobs to empty
    /// (everything done).
    fn drain(d: &mut Dedup, t: TargetId) {
        for _ in 0..64 {
            d.boundary(&pos(9), Instant::now());
            let jobs = d.core.due(Instant::now());
            if jobs.is_empty() {
                assert_eq!(d.core.pending(t), 0, "still queued");
                assert!(!d.core.busy(t), "still in flight");
                return;
            }
            for j in jobs {
                d.core.complete(j.id, crate::dedup::JobResult::ok());
            }
        }
        panic!("the queue never drained");
    }

    /// A re-plan while the target's queue holds items cannot go through (the buffered items
    /// belong to the plan being replaced): `sync` holds it, the entry
    /// keeps the old plan, and the sweep replaces once the queue has drained.
    #[test]
    fn sync_holds_a_replan_until_the_target_drains() {
        let mut d = dedup();
        let p = plan(DIRECT);
        d.sync(&[(1, p.clone())]);
        let t0 = d.entry(1).unwrap().target;
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core
            .row(&items, None, Some(&row(&[Some(3), Some(7), Some(0)])));
        assert!(
            d.core.pending(t0) > 0 || d.core.busy(t0),
            "the fed rows are work in flight"
        );

        let q = plan(DIRECT); // a fresh plan object: a re-plan of the same shape
        let s = d.sync(&[(1, q.clone())]);
        assert!(s.replaced.is_empty() && s.added.is_empty(), "{s:?}");
        assert_eq!(
            d.entry(1).unwrap().target,
            t0,
            "the old target serves meanwhile"
        );
        assert!(Arc::ptr_eq(&d.entry(1).unwrap().plan, &p));
        drain(&mut d, t0);

        let s = d.advance_replacements();
        assert_eq!(s.replaced, vec![1]);
        assert_ne!(
            d.entry(1).unwrap().target,
            t0,
            "the core got a fresh target"
        );
        assert!(Arc::ptr_eq(&d.entry(1).unwrap().plan, &q));
    }

    #[test]
    fn sync_replaces_a_target_when_idle() {
        let mut d = dedup();
        let p = plan(DIRECT);
        d.sync(&[(1, p.clone())]);
        let t0 = d.entry(1).unwrap().target;
        let q = plan(DIRECT);
        let s = d.sync(&[(1, q.clone())]);
        assert_eq!(s.replaced, vec![1], "{s:?}");
        assert_ne!(d.entry(1).unwrap().target, t0);
        assert!(Arc::ptr_eq(&d.entry(1).unwrap().plan, &q));
    }

    /// The classifier's rule, which #385 turned against a view: a row change that touches no
    /// column the plan reads is skipped as irrelevant and marks nothing. `items`.`owner_id` is
    /// column 2, which [`DIRECT`] does not read; the view that starts reading it must not keep
    /// this classifier (see `a_definition_change_replaces_the_consumer_while_the_queue_is_busy`).
    #[test]
    fn a_change_to_an_unread_column_is_skipped_as_irrelevant() {
        let mut d = dedup();
        d.sync(&[(1, plan(DIRECT))]);
        let t = d.entry(1).unwrap().target;
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core.take_stats(t);
        d.core.row(
            &items,
            Some(&row(&[Some(4), Some(7), Some(1)])),
            Some(&row(&[Some(4), Some(7), Some(9)])),
        );
        let s = d.core.take_stats(t);
        assert_eq!((s.rows, s.rows_skipped, s.ids_marked), (1, 1, 0), "{s:?}");
    }

    /// #385 (CI, `migration_definition_change_crawl_outlives_the_fixed_ready_floor`, mariadb118,
    /// seed 976365634362): two rows kept the `customer_id` they had before an `UPDATE` — the
    /// column the definition change ADDS. `sync` held the equal-shape replace while the target's
    /// queue was busy, so the OLD plan kept deciding which rows were irrelevant for a materialized
    /// table that already had the new column: the change was skipped as irrelevant and, no further
    /// change for that key ever arriving, nothing re-derived the row.
    ///
    /// A re-plan that reads differently therefore goes through at once, queue or no queue: the
    /// hold exists for the hop ids `replace_target` re-registers, not for the classifier.
    #[test]
    fn a_definition_change_replaces_the_consumer_while_the_queue_is_busy() {
        let mut d = dedup();
        let old = plan(DIRECT);
        d.sync(&[(1, old.clone())]);
        let t = d.entry(1).unwrap().target;
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core
            .row(&items, None, Some(&row(&[Some(3), Some(7), Some(0)])));
        assert!(
            d.core.pending(t) > 0 || d.core.busy(t),
            "the fed row is work in flight"
        );

        // The definition change: the view now also reads `owner_id`. Same unit, same spaces: the
        // shape `sync` would otherwise hold for the queue.
        let new = plan("SELECT i.id, i.kind_id, i.owner_id FROM items i");
        let s = d.sync(&[(1, new.clone())]);
        assert_eq!(
            s.replaced,
            vec![1],
            "the new definition classifies this view's rows from now on: {s:?}"
        );
        assert!(Arc::ptr_eq(&d.entry(1).unwrap().plan, &new));
        assert!(
            !d.replacing(1),
            "no replace is held back for a plan that reads differently"
        );

        // A change to the column the old definition did not read, for a key the buffer has never
        // seen: it marks its unit, so the row is re-derived. The replace retired `t`.
        let t = d.entry(1).unwrap().target;
        d.core.take_stats(t);
        d.core.row(
            &items,
            Some(&row(&[Some(4), Some(7), Some(1)])),
            Some(&row(&[Some(4), Some(7), Some(9)])),
        );
        let s = d.core.take_stats(t);
        assert_eq!(s.rows_skipped, 0, "skipped as irrelevant: {s:?}");
        assert!(
            s.ids_marked > 0,
            "the row was not marked for a refresh: {s:?}"
        );
    }

    /// The other half of the rule: a re-plan that reads the SAME way still waits for the queue
    /// (`sync_holds_a_replan_until_the_target_drains`), so `plan_matches`'s claim — a held
    /// replace is the plan the running target reads — holds.
    #[test]
    fn a_replan_that_reads_the_same_waits_for_the_queue() {
        let of = |sql: &str| AccessPlan::of(&plan(sql));
        assert!(Dedup::same_consumer_reads(
            &of(DIRECT),
            &of("SELECT i.id AS whatever, i.kind_id FROM items i")
        ));
        // The column the view starts (or stops) reading is the classifier, not just the output.
        assert!(!Dedup::same_consumer_reads(
            &of(DIRECT),
            &of("SELECT i.id, i.kind_id, i.owner_id FROM items i")
        ));
        // So is a constant filter: it decides which rows of an alias are its rows at all. (The
        // analyzer's `ConstFilter` carries equality and IN predicates, so that is what a filter
        // difference looks like to the consumer.)
        assert!(!Dedup::same_consumer_reads(
            &of(DIRECT),
            &of("SELECT i.id, i.kind_id FROM items i WHERE i.owner_id = 7")
        ));
        assert!(
            Dedup::may_hold_replace(
                &plan(DIRECT),
                &plan("SELECT i.id AS whatever, i.kind_id FROM items i")
            ),
            "a definition that classifies the rows the same way may wait"
        );
        assert!(
            !Dedup::may_hold_replace(
                &plan(DIRECT),
                &plan("SELECT i.id, i.kind_id, i.owner_id FROM items i")
            ),
            "one that starts reading a column may not (#385)"
        );
    }

    /// The written order of the FROM clause and the names the query spells for its sources are
    /// not facts the core reads: the same view with its two `item_values` joins the other way
    /// round, or with those two aliases spelled `k1`/`k2`, classifies rows exactly as before and
    /// so keeps waiting for the queue. What still goes through at once is a plan that READS
    /// differently - here the permuted query with one more column of `v2`, the #385 shape.
    #[test]
    fn a_permuted_join_or_a_renamed_alias_keeps_the_hold() {
        let of = |sql: &str| AccessPlan::of(&plan(sql));
        let (base, permuted) = (of(TWO_PARTIALS), of(JOINS_PERMUTED));
        assert_ne!(
            base.occurrences, permuted.occurrences,
            "each plan lists its occurrences in the order the query writes them: this is the \
             pair a positional comparison refused"
        );
        assert!(
            Dedup::same_consumer_reads(&base, &permuted),
            "the same sources read the same way, in another order: {base:?} vs {permuted:?}"
        );
        let (base, renamed) = (of(TWO_PARTIALS), of(JOINS_RENAMED));
        assert_ne!(
            renamed.occurrences, base.occurrences,
            "the two plans spell two aliases differently"
        );
        assert!(
            base.occurrences
                .iter()
                .zip(&renamed.occurrences)
                .all(|(x, y)| x.table == y.table
                    && x.read_columns == y.read_columns
                    && x.filters == y.filters
                    && x.path == y.path)
                && base
                    .occurrences
                    .iter()
                    .zip(&renamed.occurrences)
                    .any(|(x, y)| x.alias != y.alias),
            "the spelling is the only difference: {base:?} vs {renamed:?}"
        );
        assert!(
            Dedup::same_consumer_reads(&base, &renamed),
            "the same sources under other names: {base:?} vs {renamed:?}"
        );
        for (what, kept_sql, next_sql) in [
            ("a permuted join", TWO_PARTIALS, JOINS_PERMUTED),
            ("a renamed source alias", TWO_PARTIALS, JOINS_RENAMED),
            ("both at once", JOINS_PERMUTED, JOINS_RENAMED),
        ] {
            let (kept, next) = (plan(kept_sql), plan(next_sql));
            assert!(
                Dedup::same_source_layouts(&kept, &next),
                "{what}: no column moved"
            );
            assert!(
                Dedup::may_hold_replace(&kept, &next),
                "{what}: a definition that reads the same may wait for the queue"
            );
        }
        assert!(
            !Dedup::may_hold_replace(&plan(TWO_PARTIALS), &plan(JOINS_PERMUTED_READS_MORE)),
            "the permuted join that also starts reading a column replaces at once (#385)"
        );
    }

    /// The same rule where it costs something: the target's queue is busy, and a re-plan that
    /// only reordered its joins is still held (`sync_holds_a_replan_until_the_target_drains`
    /// pins the re-plan of one definition; this is the re-plan of a DIFFERENT one, whose hash
    /// therefore cannot answer for it), while the consumer classifies rows the old way
    /// meanwhile. That is free only because it reads the same columns.
    #[test]
    fn sync_holds_a_replan_that_only_reordered_its_joins() {
        let mut d = dedup();
        let p = plan(TWO_PARTIALS);
        d.sync(&[(1, p.clone())]);
        let t0 = d.entry(1).unwrap().target;
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core
            .row(&items, None, Some(&row(&[Some(3), Some(7), Some(0)])));
        assert!(
            d.core.pending(t0) > 0 || d.core.busy(t0),
            "the fed row is work in flight"
        );

        let q = plan(JOINS_PERMUTED);
        let s = d.sync(&[(1, q.clone())]);
        assert!(
            s.replaced.is_empty() && s.added.is_empty(),
            "a re-plan that reads the same in another order waits: {s:?}"
        );
        assert_eq!(
            d.entry(1).unwrap().target,
            t0,
            "the old target serves meanwhile"
        );
        assert!(Arc::ptr_eq(&d.entry(1).unwrap().plan, &p));
        drain(&mut d, t0);
        assert_eq!(d.advance_replacements().replaced, vec![1]);
        assert!(Arc::ptr_eq(&d.entry(1).unwrap().plan, &q));
    }

    /// The arm of `may_hold_replace` a DDL's re-plan never reaches (the same definition
    /// short-circuits on its hash): a CHANGED definition over a source whose layout moved. Two
    /// definitions that classify rows identically are not enough — while the replace waits, the
    /// rows still flowing are classified and cached over the layout the OLD plan was made
    /// against, which is what `mark_shape_stale` keeps the two writers from doing at once.
    #[test]
    fn a_changed_definition_over_a_moved_layout_is_not_held() {
        let col = |n: &str| crate::catalog::Column::new(n, "int", "int", None, None, None, false);
        // `items` as the three columns the view reads, with `note` appended (a DDL added a
        // column), or with a UNIQUE key over `owner_id` (a DDL added an index) - a layout the
        // plan's own reads never look at.
        let catalog = |extra: bool, key: bool| {
            let mut cols = vec![col("id"), col("kind_id"), col("owner_id")];
            if extra {
                cols.push(col("note"));
            }
            let (t, info) = typed("items", cols, &[0]);
            let mut info = (*info).clone();
            if key {
                info.unique_keys = vec![vec![2]];
            }
            [(t, Arc::new(info))].into_iter().collect::<HashMap<_, _>>()
        };
        // A different definition (its hash) of the same two columns.
        const CHANGED: &str = "SELECT i.id AS whatever, i.kind_id FROM items i";
        let plain = catalog(false, false);
        assert!(
            Dedup::may_hold_replace(&plan_in(DIRECT, &plain), &plan_in(CHANGED, &plain)),
            "two definitions over one layout, the same reads: they may wait"
        );

        let before = plan_in(DIRECT, &plain);
        let after = plan_in(CHANGED, &catalog(true, false));
        assert!(
            !Dedup::same_source_layouts(&before, &after),
            "the same columns are not the same table"
        );
        assert!(
            !Dedup::may_hold_replace(&before, &after),
            "a changed definition over an added column replaces at once"
        );

        // The case that pins the arm alone: a layout the reads cannot see at all, so the only
        // thing that can refuse the hold is the layout comparison.
        let after = plan_in(CHANGED, &catalog(false, true));
        assert!(
            Dedup::same_consumer_reads(&AccessPlan::of(&before), &AccessPlan::of(&after)),
            "the classifier is unchanged: what moved is a key the view never reads"
        );
        assert!(
            !Dedup::same_source_layouts(&before, &after),
            "a layout the reads cannot see has still moved"
        );
        assert!(
            !Dedup::may_hold_replace(&before, &after),
            "a DDL under a changed definition replaces at once: the queue would be delivered \
             over a map of the other layout"
        );
    }

    /// The other comparison of `same_source_layouts` no re-plan of one definition can reach: the
    /// map the cascade cache keeps of the child's rows. Two definitions that read the same over
    /// `fk_catalog(true)` wait for the queue; a map carrying something else (what a DDL on the
    /// child leaves it: its aliases pinned differently while the key and the stored columns
    /// stand) sends the replace through at once. The second plan's map is set directly because
    /// one child cannot have two layouts in two catalogs: the second catalog would move
    /// `child_table` too, which the alias comparison already refuses, and the test would no
    /// longer name which comparison answered.
    #[test]
    fn a_changed_definition_whose_cascade_map_moved_is_not_held() {
        let cat = fk_catalog(true);
        let before = plan_in(
            "SELECT o.oid, MAX(a.val) AS x FROM obj o \
             JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY o.oid",
            &cat,
        );
        let mut after = Arc::unwrap_or_clone(plan_in(
            "SELECT o.oid, MAX(a.val) AS whatever FROM obj o \
             JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY o.oid",
            &cat,
        ));
        let at = before
            .cascades
            .iter()
            .position(|c| c.map.is_some())
            .expect("fk_catalog(true) gives a mapped cascade");
        assert!(
            Dedup::may_hold_replace(&before, &Arc::new(after.clone())),
            "two definitions over one layout, the same reads: they may wait"
        );
        let pin = |col: usize, v: i64| crate::analyze::ConstFilter {
            col,
            values: vec![Datum::Int(v)],
        };
        after.cascades[at].map.as_mut().unwrap().filters = vec![vec![pin(1, 7)], vec![pin(1, 8)]];
        let pinned = Arc::new(after.clone());
        assert!(
            Dedup::same_consumer_reads(&AccessPlan::of(&before), &AccessPlan::of(&pinned)),
            "occurrences and cascade routes unchanged: only what the cache keeps moved"
        );
        assert!(
            !Dedup::same_source_layouts(&before, &pinned),
            "what the cascade cache keeps of its child is a layout fact"
        );
        assert!(
            !Dedup::may_hold_replace(&before, &pinned),
            "a changed definition over a moved map replaces at once"
        );
        // The per-alias filters are a SET: `MapPlan::keeps` passes a row that answers the filters
        // of SOME alias, so the order the child's aliases were written in keeps the hold.
        after.cascades[at].map.as_mut().unwrap().filters = vec![vec![pin(1, 8)], vec![pin(1, 7)]];
        let other_order = Arc::new(after.clone());
        assert!(
            Dedup::same_source_layouts(&pinned, &other_order),
            "the same pins in another written order"
        );
        assert!(
            Dedup::may_hold_replace(&pinned, &other_order),
            "the order of the map's per-alias lists decides nothing"
        );
    }

    /// A DDL, not a definition change: the same definition replanned over a table that gained a
    /// column. That replace still waits whatever it reads - the TABLE_MAP handler's shape
    /// machinery (`core_hold_prefix`, `mark_shape_stale`) delivers it once the queue has drained,
    /// while delivering it under a map of the other layout would leave those rows to neither
    /// writer. Only a changed definition goes through at once.
    #[test]
    fn a_ddl_replan_keeps_the_hold_although_the_layout_moved() {
        let col = |n: &str| crate::catalog::Column::new(n, "int", "int", None, None, None, false);
        let catalog = |extra: bool| {
            let mut cols = vec![col("id"), col("kind_id"), col("owner_id")];
            if extra {
                cols.push(col("note"));
            }
            [typed("items", cols, &[0])]
                .into_iter()
                .collect::<HashMap<_, _>>()
        };
        let before = plan_in(DIRECT, &catalog(false));
        let after = plan_in(DIRECT, &catalog(true));
        assert!(
            Dedup::may_hold_replace(&before, &after),
            "the same definition over a moved layout keeps the hold: the shape machinery delivers it"
        );
    }

    // The parent read through a partial alias: `obj`'s cascade is traced. MySQL logs no
    // FK-cascaded child rows, so the core could not find the units the parent's change moved
    // out from under it; such a plan stays on the viewplan executor (which marks the
    // materialized table itself), refused by plan property, never by table or view names.
    #[test]
    fn sync_refuses_a_traced_cascade() {
        let p = plan_in(
            "SELECT c.gid, o.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
             LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid IN (8, 9) GROUP BY c.gid, o.oid",
            &fk_catalog(false),
        );
        assert!(
            matches!(Dedup::core_refusal(&p), Some(why) if why.contains("traced")),
            "{:?}",
            AccessPlan::of(&p).cascades
        );
        let mut d = dedup();
        let s = d.sync(&[(7, p.clone())]);
        assert!(s.added.is_empty(), "{:?}", s.added);
        assert_eq!(s.refused.len(), 1, "{:?}", s.refused);
        assert_eq!(s.refused[0].0, 7);
        assert!(
            d.entry(7).is_none(),
            "no core target: executor (b) keeps it"
        );
        let s = d.sync(&[(7, p)]);
        assert!(
            s.refused.is_empty() && d.entry(7).is_none(),
            "{:?}, refused warns once",
            s.refused
        );
    }

    /// #167 guard (PR 7, `167-core-refusal-guard`): every cascade route shape stays on the
    /// viewplan executor until `05-cg-core` lands #167's replay-window fixes on the core.
    /// `core_refusal` refuses the traced shape by name and *any* other cascade through one shape-
    /// agnostic arm (`cascades.first()`), so mapped cascades, parent-side routes on a view whose
    /// child is also routed, and derived / direct / untraced routes alike keep executor (b) — the
    /// refusal is by plan property, never by table or view names. When the core gains the fixes,
    /// update exactly these expectations (the `05-cg-core` spec names this test).
    #[test]
    fn core_refusal_keeps_every_cascade_shape_on_the_executor() {
        // A map cascade whose parent is also read by the plan (a parent route on a view whose
        // child is routed) — the shape the live #167 finding hit (soak seed 0x37d9e7d0bd6339ee).
        let map = plan_in(
            "SELECT o.oid, MAX(a.val) AS x FROM obj o \
             JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY o.oid",
            &fk_catalog(true),
        );
        // The parent read through a partial alias: the cascade is traced.
        let traced = plan_in(
            "SELECT c.gid, o.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
             LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid IN (8, 9) GROUP BY c.gid, o.oid",
            &fk_catalog(false),
        );
        for (what, p, want_traced) in [("map", &map, false), ("traced", &traced, true)] {
            let access = AccessPlan::of(p);
            assert!(
                !access.cascades.is_empty(),
                "{what}: want a cascade route on the plan"
            );
            assert_eq!(
                access.cascades.iter().any(|c| matches!(c.via, Via::Traced)),
                want_traced,
                "{what}: traced route where it should not be: {access:?}"
            );
            if !want_traced {
                assert!(
                    access
                        .cascades
                        .iter()
                        .any(|c| matches!(c.via, Via::Map { .. })),
                    "{what}: want a map cascade: {access:?}"
                );
            }
            let why = Dedup::core_refusal(p).unwrap_or_else(|| {
                panic!("{what}: a cascade plan must keep the executor: {access:?}")
            });
            assert!(
                why.contains(if want_traced {
                    "traced"
                } else {
                    "replay-window"
                }),
                "{what}: the refusal must name why this cascade keeps the executor: {why}"
            );
        }
        // The second arm is shape-agnostic (`cascades.first()`), so it refuses every `Via` the
        // analyzer can emit — including `Derived`, `Direct` and `Untraced` — as long as the plan
        // carries a cascade; only a plan with no cascade at all is accepted. One plan per those
        // remaining kinds, each with NO traced route (so the answer cannot come from the first
        // arm) and each expected to name the replay-window reason.
        for (what, wanted, candidates) in [
            (
                "derived",
                (|v: &Via| matches!(v, Via::Derived { .. })) as fn(&Via) -> bool,
                &[
                    (false, "SELECT a.id, a.val FROM obj_attr a WHERE a.aid = 7"),
                    (
                        false,
                        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c \
                         JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, c.oid",
                    ),
                    (
                        false,
                        "SELECT g.gid, COUNT(*) AS n, MAX(a.val) AS x FROM grp g \
                         JOIN link c ON c.gid = g.gid JOIN obj_attr a ON a.oid = c.oid \
                         AND a.aid = 7 GROUP BY g.gid",
                    ),
                ][..],
            ),
            (
                "direct",
                (|v: &Via| matches!(v, Via::Direct { .. })) as fn(&Via) -> bool,
                &[
                    (
                        false,
                        "SELECT a.oid, MAX(a.val) AS x FROM obj_attr a GROUP BY a.oid",
                    ),
                    (
                        false,
                        "SELECT c.oid, COUNT(*) AS n, MAX(a.val) AS x FROM link c \
                         JOIN obj_attr a ON a.oid = c.oid GROUP BY c.oid",
                    ),
                    (
                        true,
                        "SELECT c.oid, COUNT(*) AS n FROM link c GROUP BY c.oid",
                    ),
                ][..],
            ),
            (
                "untraced",
                (|v: &Via| matches!(v, Via::Untraced { .. })) as fn(&Via) -> bool,
                &[
                    // `note` has no key: the cascade map cannot keep its rows, so the route is
                    // neither unit, traced, derived nor mapped — the shape with no route at all.
                    (
                        true,
                        "SELECT g.gid, COUNT(*) AS n, MAX(t.w) AS x FROM grp g \
                         JOIN link c ON c.gid = g.gid LEFT JOIN note t ON t.oid = c.oid \
                         GROUP BY g.gid",
                    ),
                    (
                        true,
                        "SELECT g.gid, COUNT(*) AS n, MAX(t.w) AS x FROM grp g \
                         JOIN link c ON c.gid = g.gid JOIN obj o ON o.oid = c.gid \
                         LEFT JOIN note t ON t.oid = o.oid GROUP BY g.gid",
                    ),
                ][..],
            ),
        ] {
            let (p, access) = plan_for_via(what, wanted, candidates);
            assert!(
                !access.cascades.is_empty(),
                "{what}: want a cascade route on the plan"
            );
            assert!(
                !access.cascades.iter().any(|c| matches!(c.via, Via::Traced)),
                "{what}: no traced route here, so only the shape-agnostic arm can answer: \
                 {access:?}"
            );
            let why = Dedup::core_refusal(&p).unwrap_or_else(|| {
                panic!("{what}: a cascade plan must keep the executor: {access:?}")
            });
            assert!(
                why.contains("replay-window"),
                "{what}: the shape-agnostic arm must name why this cascade keeps the executor: \
                 {why}"
            );
        }
        assert!(
            Dedup::core_refusal(&plan(DIRECT)).is_none(),
            "a cascade-free plan is accepted"
        );
    }

    /// The first formulation of `candidates` that puts a cascade of route kind `wanted` on a
    /// plan carrying no traced cascade at all, so `core_refusal` answers it through the
    /// shape-agnostic arm alone. The candidates are the analyzer's own fixtures for that shape
    /// (`(map_shape, sql)` over `fk_catalog`): a formulation the analyzer rejects, or that
    /// routes another way, is passed over, and a `wanted` none of them yields fails the test —
    /// the guard names the shape, so it is pinned, not skipped.
    fn plan_for_via(
        what: &str,
        wanted: fn(&Via) -> bool,
        candidates: &[(bool, &str)],
    ) -> (Arc<ViewPlan>, AccessPlan) {
        let mut tried: Vec<String> = Vec::new();
        for (map_shape, sql) in candidates {
            let built = crate::analyze::parse_definition(&format!("CREATE VIEW v AS {sql}"))
                .ok()
                .and_then(|d| crate::analyzer::plan(&d, "db", &fk_catalog(*map_shape)).ok());
            let Some(p) = built else {
                tried.push(format!("{sql}: no plan"));
                continue;
            };
            let p = Arc::new(p);
            let access = AccessPlan::of(&p);
            let holds = access.cascades.iter().any(|c| wanted(&c.via));
            let traced = access.cascades.iter().any(|c| matches!(c.via, Via::Traced));
            if !holds || traced {
                let vias: Vec<String> = access
                    .cascades
                    .iter()
                    .map(|c| format!("{:?}", c.via))
                    .collect();
                tried.push(format!("{sql}: vias {vias:?}"));
                continue;
            }
            return (p, access);
        }
        panic!("no {what} cascade among the fixtures: {tried:?}");
    }

    /// Hop plans keep the executor. Their units come from the core's hop answer cache, which a
    /// change to the partner table in the same window cannot invalidate (the three cases in
    /// `tmp/out/lvs-step6/6t/hops-followup.md`), so `sync` refuses them the way it refuses a
    /// traced cascade — once per plan — while a direct plan over the same tables is accepted.
    #[test]
    fn sync_refuses_a_hop_plan_and_accepts_a_direct_one() {
        let p = plan(ONE_HOP);
        assert!(
            matches!(Dedup::core_refusal(&p), Some(why) if why.contains("hop")),
            "{:?}",
            AccessPlan::of(&p).occurrences
        );
        assert!(Dedup::core_refusal(&plan(DIRECT)).is_none());
        let mut d = dedup();
        let s = d.sync(&[(4, p.clone()), (5, plan(DIRECT))]);
        assert_eq!(s.added, vec![5], "{:?}", s.refused);
        assert_eq!(s.refused.len(), 1, "{:?}", s.refused);
        assert_eq!(s.refused[0].0, 4);
        assert!(
            d.entry(4).is_none(),
            "no core target: executor (b) keeps the hop view"
        );
        assert!(d.entry(5).is_some());
        let s = d.sync(&[(4, p), (5, plan(DIRECT))]);
        assert!(
            s.refused.is_empty() && d.entry(4).is_none() && d.entry(5).is_some(),
            "{:?}, refused warns once",
            s.refused
        );
    }

    /// A plan refused while its target's queue still holds rows only the core had seen: the
    /// removal waits for the drain (removing early would drop those rows), the executor's
    /// routes take over meanwhile, and the sweep reports the target as removed.
    #[test]
    fn sync_holds_a_refused_target_until_it_drains() {
        let mut d = dedup();
        add_hop_plan(&mut d, 7, plan(ONE_HOP));
        let t0 = d.entry(7).unwrap().target;
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core
            .row(&items, None, Some(&row(&[Some(3), Some(7), Some(0)])));
        let p = plan_in(
            "SELECT c.gid, o.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
             LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid IN (8, 9) GROUP BY c.gid, o.oid",
            &fk_catalog(false),
        );
        let s = d.sync(&[(7, p.clone())]);
        assert_eq!(s.refused.len(), 1, "{:?}", s.refused);
        assert!(
            s.lost_cache.is_empty(),
            "the target drains first: it still needs its cascade cache to expand the queued rows"
        );
        assert!(
            d.entry(7).is_some(),
            "the target drains first: its rows are only in the core"
        );
        drain(&mut d, t0);
        let s = d.advance_replacements();
        assert_eq!(s.removed, vec![7]);
        assert_eq!(
            s.lost_cache.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![7],
            "the version's cascade cache is taken back out of the removed target"
        );
        assert!(d.entry(7).is_none(), "the target and entry are gone");
        let s = d.sync(&[(7, p)]);
        assert!(
            s.refused.is_empty() && d.entry(7).is_none(),
            "{:?}, refused warns once",
            s.refused
        );
    }

    // The untraced route over `link` maps the parent's change through the cascade cache, but
    // the core lacks #167's replay-window fixes (D-6T-cascades): the plan is refused by plan
    // property, warned once, no core target — while a direct plan over the plain catalog is
    // accepted. cascade-gap-core moves map cascades onto the core once its tests pass there.
    #[test]
    fn sync_refuses_a_map_cascade_and_accepts_a_direct_one() {
        let p = plan_in(
            "SELECT o.oid, MAX(a.val) AS x FROM obj o \
             JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY o.oid",
            &fk_catalog(true),
        );
        let cas = AccessPlan::of(&p).cascades;
        assert!(
            cas.iter().any(|c| matches!(c.via, Via::Map { .. })),
            "want a map cascade: {cas:?}"
        );
        assert!(
            matches!(Dedup::core_refusal(&p), Some(why) if why.contains("cascade")),
            "{cas:?}"
        );
        assert!(
            Dedup::core_refusal(&plan(DIRECT)).is_none(),
            "a direct plan is accepted"
        );
        let mut d = dedup();
        let s = d.sync(&[(7, p.clone()), (8, plan(DIRECT))]);
        assert_eq!(s.added, vec![8], "{:?}", s.refused);
        assert_eq!(s.refused.len(), 1, "{:?}", s.refused);
        assert_eq!(s.refused[0].0, 7);
        assert!(
            d.entry(7).is_none(),
            "no core target: executor (b) keeps the map-cascade view"
        );
        assert!(d.entry(8).is_some(), "the core took the direct plan");
        let s = d.sync(&[(7, p), (8, plan(DIRECT))]);
        assert!(
            s.refused.is_empty() && d.entry(7).is_none() && d.entry(8).is_some(),
            "{:?}, refused warns once",
            s.refused
        );
    }

    #[test]
    fn chunks_become_units_and_partial_keys() {
        let mut ids = IdSet::default();
        ids.insert_int(1);
        ids.insert_int(2);
        let mut partial = IdSet::default();
        partial.insert_int(7);
        let chunk = Chunk {
            ids: vec![(SpaceId::FULL, ids), (SpaceId::partial(0), partial)],
            items: vec![Item::Presence {
                index: 3,
                value: Datum::Int(9),
                hold: false,
            }],
            facts: None,
            origin: pos(0),
            high: pos(1),
        };
        let (dirty, hops) = Dedup::to_dirty(&plan(TWO_PARTIALS), chunk);
        assert_eq!(dirty.units.len(), 2);
        assert_eq!(dirty.partial[0].len(), 1);
        assert_eq!(dirty.marks.len(), 1);
        assert_eq!(&dirty.marks[0], &(3, Datum::Int(9)));
        assert!(dirty.holds.is_empty() && hops.is_empty());
    }

    #[test]
    fn hop_sql_appends_the_partner_filter() {
        let p = plan(ONE_HOP);
        let hop = match &AccessPlan::of(&p).occurrences[1].path {
            Path::Hop(h) => h.clone(),
            other => panic!("expected a hop, got {other:?}"),
        };
        let links: Vec<Tuple> = [1i64, 2, 3, 4, 9].into_iter().map(link).collect();
        let (sql, params) = Dedup::hop_sql(&hop, &links);
        let (f, fp) = UnitChunk::Ints(vec![1, 2, 3, 4, 9]).filter(&hop.partner);
        assert_eq!(sql, format!("{} AND {f}", hop.select));
        assert!(f.contains("BETWEEN 1 AND 4"), "{f}");
        assert!(params.is_empty() && fp.is_empty(), "{params:?} {fp:?}");
        // A two-part link renders `IN ((?,?))` with 2 parameters.
        let hop2 = Hop {
            partner: vec![r#"`i`.`kind_id`"#.into(), r#"`i`.`owner_id`"#.into()],
            ..hop.clone()
        };
        let two: Vec<Tuple> = vec![Box::new([Datum::Int(1), Datum::Int(2)])];
        let (sql2, params2) = Dedup::hop_sql(&hop2, &two);
        assert!(sql2.ends_with("IN ((?,?))"), "{sql2}");
        assert_eq!(params2.len(), 2);
        // A text link is answered per branch, labeled with the link's index: the column's
        // collation may answer a different spelling, and `complete` groups by the item's bytes.
        let hop3 = Hop {
            partner: vec!["`l`.`note`".into()],
            ..hop.clone()
        };
        let texts: Vec<Tuple> = vec![
            Box::new([Datum::Bytes(b"abc".to_vec())]),
            Box::new([Datum::Bytes(b"ABC".to_vec())]),
        ];
        let (sql3, params3) = Dedup::hop_sql(&hop3, &texts);
        assert_eq!(sql3.matches(" UNION ALL ").count(), 1, "{sql3}");
        assert!(
            sql3.contains("SELECT 0 AS `__lnk`") && sql3.contains("SELECT 1 AS `__lnk`"),
            "{sql3}"
        );
        assert_eq!(params3.len(), 2, "{params3:?}"); // one bound parameter per branch
                                                     // An all-numeric multi-column link keeps the single unlabeled statement.
        assert!(!sql2.contains("__lnk"), "{sql2}");
    }

    /// The only hop of the single synced target.
    fn the_hop(d: &Dedup) -> HopId {
        (0u32..)
            .map(HopId)
            .find(|h| d.hop(*h).is_some())
            .expect("the view has one hop")
    }

    /// A core target for a plan `sync` refuses because its access needs hop items (see
    /// `sync_refuses_a_hop_plan_and_accepts_a_direct_one`): the tests of the core's hop
    /// machinery need such a target, so they install it with the same call `sync` makes when
    /// it accepts a plan, without the plan check.
    fn add_hop_plan(d: &mut Dedup, id: u64, plan: Arc<ViewPlan>) -> TargetId {
        let t = d
            .core
            .add_target(TargetSpec {
                plans: vec![plan.clone()],
                labels: vec![],
                start: Position {
                    file: String::new(),
                    pos: 0,
                },
                schedule: d.schedule,
                needs: TargetNeeds::default(),
            })
            .expect("table names are unique");
        d.entries.insert(id, Entry { target: t, plan });
        t
    }

    #[test]
    fn answers_land_in_the_hop_space() {
        // A hop that reaches every unit position gives units.
        let mut d = dedup();
        add_hop_plan(&mut d, 1, plan(ONE_HOP));
        let h = the_hop(&d);
        let mut dirty = ViewDirty::for_aliases(1);
        let n = d.add_answers(
            &mut dirty,
            &[(
                h,
                vec![(link(9), Key::from_parts(Box::new([Datum::Int(7)])))],
            )],
        );
        assert_eq!(n, 1);
        assert_eq!(dirty.units.len(), 1);
        assert!(dirty.units.contains(&[Datum::Int(7)]));
        assert_eq!(
            d.add_answers(
                &mut dirty,
                &[(
                    h,
                    vec![(link(9), Key::from_parts(Box::new([Datum::Int(7)])))]
                )]
            ),
            0
        );
        // A hop that reaches part of the unit gives that partial set's keys.
        // The shape is `crate::access`'s `hops_reach_partial_positions`: two unit parts, the
        // link alias reaches only the first, and the id alias gives it as a partial set.
        let mut p = Arc::unwrap_or_clone(plan(ONE_HOP));
        p.aliases[0].direct_unit = None;
        p.aliases.push(crate::analyze::AliasPlan {
            name: "o".into(),
            table: crate::dedup::testkit::table(
                "owners",
                &[("id", "int"), ("name", "varchar")],
                &[0],
            )
            .1,
            referenced: vec![true; 2],
            membership: vec![true; 2],
            direct_unit: None,
            image_cols: vec![],
            subst_from: (String::new(), String::new()),
            pos: (2, None),
            semi: None,
            lookup_where: false,
        });
        p.unit_exprs.push("`o`.`id`".into());
        p.unit_refs.push(vec![(2, 0)]);
        p.edges.push(crate::analyze::Edge {
            a: (2, 0),
            b: (0, 2),
            only_with: Some(2),
        });
        p.partial = crate::analyze::PartialUnits {
            sets: vec![vec![0], vec![1]],
            aliases: vec![Some((0, vec![0])), None, Some((1, vec![0]))],
        };
        let p = Arc::new(p);
        assert!(Dedup::routed(&p), "{:?}", AccessPlan::of(&p).route);
        assert_eq!(p.unit_exprs.len(), 2, "{:?}", p.unit_exprs);
        assert_eq!(
            p.partial.sets,
            vec![vec![0], vec![1]],
            "{:?}",
            p.partial.sets
        );
        let mut d = dedup();
        add_hop_plan(&mut d, 1, p.clone());
        let h = the_hop(&d);
        let (plan, hop) = d.hop(h).unwrap();
        assert_eq!(hop.positions, vec![0], "{hop:?}");
        assert_eq!(plan.partial.sets[0], vec![0]);
        let mut dirty = ViewDirty::for_aliases(p.aliases.len());
        let n = d.add_answers(
            &mut dirty,
            &[(
                h,
                vec![(link(9), Key::from_parts(Box::new([Datum::Int(7)])))],
            )],
        );
        assert_eq!(n, 1);
        assert!(dirty.units.is_empty());
        assert_eq!(dirty.partial[0].len(), 1);
    }

    #[test]
    fn tally_maps_items_to_images() {
        let mut d = dedup();
        add_hop_plan(&mut d, 1, plan(ONE_HOP));
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core
            .row(&items, None, Some(&row(&[Some(3), Some(7), Some(0)])));
        let t = d.tally(1);
        assert_eq!(t.ids_marked, 1, "the unit id of the row");
        assert_eq!(t.images_marked, 0);
        let links = d
            .core
            .source(&table_ref("item_links"), Some(2))
            .expect("watched");
        d.core.row(&links, None, Some(&row(&[Some(4), Some(7)])));
        let t = d.tally(1);
        assert_eq!(t.images_marked, 1, "the missed hop is an item");
        assert_eq!(t.images_absorbed, 0);
        assert_eq!(d.tally(1), Tally::default(), "taken since the last call");
    }

    #[test]
    fn backoff_doubles_and_resets() {
        let mut d = dedup();
        assert_eq!(d.backoff(1, true), std::time::Duration::from_millis(250));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_millis(500));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_millis(1000));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(2));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(4));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(8));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(16));
        for _ in 0..10 {
            assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(30));
        }
        assert_eq!(d.backoff(1, false), std::time::Duration::ZERO);
        assert_eq!(
            d.backoff(1, true),
            std::time::Duration::from_millis(250),
            "reset"
        );
    }
}
