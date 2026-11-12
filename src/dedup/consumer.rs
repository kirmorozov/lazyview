//! The consumer of the dedup core (keymap §4): per source table, every occurrence of that table in
//! every target's plans, compiled from the plans' access paths. Copy set: std, mysql_async's row types,
//! `crate::{access, analyze, catalog, datum, intset}` and `super::*` only.
//!
//! Per row change and occurrence, in order: the read columns (an UPDATE that changes none of them is
//! skipped), the constant filters per image (`ConstFilter::passes`; a side failing them is not the
//! occurrence's row), then the ids from the images. Deletes on every path: the before-image gives the
//! old ids, the after-image the new ones; an update that keeps its id marks it once.
//!
//! Cost per row and occurrence: the read-column compare, a few integer compares for the filters, a
//! projection of the id columns and one hash insert per id; no row image is kept.

use std::collections::HashMap;
use std::sync::Arc;

use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;

use crate::access::{AccessPlan, Path, Route, Via};
use crate::analyze::{CascadeRoute, ConstFilter, ViewPlan};
use crate::cascade;
use crate::catalog::{FkAction, TableInfo, TableRef};
use crate::datum::{Datum, Key as Tuple};
use crate::intset;

use super::hop::{HopPlan, Hops};
use super::key::{HopId, Item, Key, Op, SpaceId, Spaces};

/// The value of column `c` of `row` (`None`: not in the image).
pub fn value_at(row: &BinlogRow, c: usize) -> Option<&BinlogValue<'_>> {
    row.as_ref(c)
}

/// Whether an UPDATE from `before` to `after` changes a column of `mask` (a partial JSON update
/// counts as a change). Moved from `accumulate` (same body).
pub fn changed(before: &BinlogRow, after: &BinlogRow, mask: &[bool]) -> bool {
    mask.iter().enumerate().any(|(c, &m)| {
        m && (value_at(before, c) != value_at(after, c)
            || matches!(value_at(after, c), Some(BinlogValue::JsonDiff(_))))
    })
}

/// Where the consumer puts what it derives: the core's buffers or its staging (src/dedup/mod.rs),
/// or a recording sink in tests.
pub trait Sink<U> {
    /// Whether target `t` takes rows now (added, started, not held).
    fn open(&self, t: usize) -> bool;
    /// A row change reached an occurrence of target `t`; `skipped`: it did not concern it.
    fn row(&mut self, t: usize, skipped: bool);
    /// Integer id `v` (on `intset::from_i64` order) of space `s` of target `t`.
    fn int(&mut self, t: usize, s: SpaceId, v: u64, op: Op, cascaded: bool);
    /// Tuple id `k` of space `s` of target `t`.
    fn tuple(&mut self, t: usize, s: SpaceId, k: Tuple, op: Op, cascaded: bool);
    /// A deferred item of target `t`.
    fn item(&mut self, t: usize, i: Item<U>);
    /// A note for target `t`.
    fn note(&mut self, t: usize, n: Note);
    /// A hop lookup of target `t`: a hit or a miss.
    fn hop(&mut self, t: usize, hit: bool);
    /// The cascade cache of plan `pi` of target `t`.
    fn cache(&mut self, t: usize, pi: usize) -> &mut crate::cascade::Cache;
}

/// Something a row showed that is not an id: counts, and changes whose ids are unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Note {
    /// A cascade parent's image lacks columns: the ids its cascade changes are unknown.
    ShortParentImage,
    /// A kept child row lacks a stored column: the ids its cascade changes are unknown.
    IncompleteKeptRow,
    /// The cascade cache missed binlog events: the kept rows under the parent's key may be stale.
    CascadeGap,
    /// `n` kept child rows were replayed for a parent change.
    CascadeRows(u64),
    /// A parent or child change was logged while the cache seeds: the caller must hold the
    /// position until `install`.
    Logged,
}

/// One cascade whose parent is the table, compiled.
#[derive(Debug, Clone)]
struct Cas {
    target: usize,
    pi: usize,
    plan: Arc<ViewPlan>,
    ci: usize,
    /// The space of each partial set of the plan.
    sets: Vec<SpaceId>,
    /// The route goes through the cascade cache of `(target, pi)` instead of the parent's row.
    map: bool,
}

/// One mapped cascade whose child is the table: the child's own row events keep the cache of
/// cascade `ci` of plan `pi` of target `target`.
#[derive(Debug, Clone)]
struct MapChild {
    target: usize,
    pi: usize,
    plan: Arc<ViewPlan>,
    ci: usize,
}

/// How an occurrence's rows reach its target's ids.
#[derive(Debug, Clone)]
enum Way {
    Direct { cols: Vec<usize> },
    Partial { space: SpaceId, cols: Vec<usize> },
    Hop { hop: HopId },
}

/// One occurrence, compiled.
#[derive(Debug, Clone)]
struct Occ {
    target: usize,
    /// The occurrence's index in its plan (what a hop's `chain` and `partner` name).
    alias: usize,
    table: Arc<TableInfo>,
    read: Vec<bool>,
    filters: Vec<ConstFilter>,
    way: Way,
    /// The hops whose chain holds this alias: its rows change what those reach.
    chains: Vec<HopId>,
}

/// Every route of one source table.
#[derive(Debug, Default)]
struct Watched {
    occs: Vec<Occ>,
    cascades: Vec<Cas>,
    maps: Vec<MapChild>,
}

/// A source table as one TABLE_MAP of it shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    idx: usize,
    /// Occurrences (indexes into the table's list) planned for another column count.
    skip: Vec<usize>,
    /// Cascades (indexes into the table's list) whose parent has another column count.
    skip_cas: Vec<usize>,
    /// Maps (indexes into the table's list) whose child has another column count.
    skip_maps: Vec<usize>,
}

/// The per-table occurrence lists of every target.
#[derive(Debug, Default)]
pub struct Consumer {
    by_table: HashMap<TableRef, usize>,
    watched: Vec<Watched>,
}

impl Consumer {
    /// Compiles plan `pi` of target `t` into the per-table lists, all or nothing. `Err` names what
    /// the core cannot follow (a route other than Dedup, or a path or cascade this consumer does not
    /// take yet).
    pub fn add(
        &mut self,
        t: usize,
        pi: usize,
        plan: &Arc<ViewPlan>,
        access: &AccessPlan,
        spaces: &Spaces,
        hops: &mut Hops,
    ) -> Result<(), String> {
        if !matches!(access.route, Route::Dedup) {
            let reason = match &access.route {
                Route::ViewPlan { reason } | Route::Full { reason } => reason.clone(),
                _ => String::new(),
            };
            return Err(format!("route {}: {}", access.route_name(), reason));
        }
        // All or nothing: nothing is inserted before every occurrence and cascade compiles.
        let mut compiled: Vec<(TableRef, Occ)> = Vec::with_capacity(access.occurrences.len());
        // Pass 1 compiles the occurrences; a hop's plan waits for its id, so a refusal that comes
        // later registers neither an occurrence nor a hop.
        let mut fresh: Vec<(usize, HopPlan)> = Vec::new();
        for (ai, occ) in access.occurrences.iter().enumerate() {
            let table = plan.aliases[ai].table.clone();
            let way = match &occ.path {
                Path::Direct { cols } => Way::Direct { cols: cols.clone() },
                Path::Partial {
                    positions, cols, ..
                } => Way::Partial {
                    space: spaces
                        .find(positions)
                        .ok_or_else(|| format!("`{}`: no space for {:?}", occ.alias, positions))?,
                    cols: cols.clone(),
                },
                Path::Hop(h) => {
                    // 6A: the positions are the full id or one partial set.
                    let space = spaces.find(&h.positions).ok_or_else(|| {
                        format!("`{}`: no space for {:?}", occ.alias, h.positions)
                    })?;
                    // Every partner column on one chain alias: that alias, its columns in link order.
                    let partner = match h.partner_cols.first() {
                        Some(&(a, _)) if h.partner_cols.iter().all(|&(pa, _)| pa == a) => {
                            Some((a, h.partner_cols.iter().map(|&(_, c)| c).collect()))
                        }
                        _ => None,
                    };
                    fresh.push((
                        ai,
                        HopPlan {
                            target: t,
                            plan: pi,
                            alias: ai,
                            space,
                            link_cols: h.link_cols.clone(),
                            chain: h.aliases.clone(),
                            partner,
                        },
                    ));
                    // Pass 2 gives it the hop's id.
                    Way::Hop {
                        hop: HopId(u32::MAX),
                    }
                }
                _ => return Err(format!("`{}`: {:?}", occ.alias, occ.path)),
            };
            let read = occ
                .read_columns
                .clone()
                .unwrap_or_else(|| vec![true; table.columns.len()]);
            compiled.push((
                occ.table.clone(),
                Occ {
                    target: t,
                    alias: ai,
                    table,
                    read,
                    filters: occ.filters.clone(),
                    way,
                    chains: Vec::new(),
                },
            ));
        }
        let mut compiled_cas: Vec<(TableRef, Cas)> = Vec::new();
        let mut compiled_maps: Vec<(TableRef, MapChild)> = Vec::new();
        for (ci, c) in access.cascades.iter().enumerate() {
            match &c.via {
                Via::Traced => {}
                Via::Direct { .. } | Via::Derived { .. } | Via::Map { .. } => {
                    let sets = plan
                        .partial
                        .sets
                        .iter()
                        .map(|positions| {
                            spaces.find(positions).ok_or_else(|| {
                                format!("`{}`: no space for {:?}", c.parent.1, positions)
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    let map = matches!(c.via, Via::Map { .. });
                    compiled_cas.push((
                        c.parent.clone(),
                        Cas {
                            target: t,
                            pi,
                            plan: plan.clone(),
                            ci,
                            sets,
                            map,
                        },
                    ));
                    // The child's own row events keep that cache.
                    if map {
                        compiled_maps.push((
                            plan.cascades[ci].child.clone(),
                            MapChild {
                                target: t,
                                pi,
                                plan: plan.clone(),
                                ci,
                            },
                        ));
                    }
                }
                Via::Untraced { reason } => return Err(format!("cascade untraced: {reason}")),
            }
        }
        // Pass 2 (no `Err` after this point): the hop ids, and every occurrence of this `(t, pi)`
        // (all of `compiled`) whose alias the chain holds marks that hop as one it invalidates.
        for (ai, p) in fresh {
            let hop = hops.add(p.clone());
            compiled[ai].1.way = Way::Hop { hop };
            for (_, o) in compiled.iter_mut() {
                if p.chain.contains(&o.alias) {
                    o.chains.push(hop);
                }
            }
        }
        for (table, occ) in compiled {
            let i = self.entry(table);
            self.watched[i].occs.push(occ);
        }
        for (table, cas) in compiled_cas {
            let i = self.entry(table);
            self.watched[i].cascades.push(cas);
        }
        for (table, m) in compiled_maps {
            let i = self.entry(table);
            self.watched[i].maps.push(m);
        }
        Ok(())
    }

    /// The index of `table`'s list, creating it when the table is new.
    fn entry(&mut self, table: TableRef) -> usize {
        match self.by_table.get(&table) {
            Some(&i) => i,
            None => {
                self.watched.push(Watched::default());
                let i = self.watched.len() - 1;
                self.by_table.insert(table, i);
                i
            }
        }
    }

    /// Drops every occurrence, cascade and map of target `t` (table indexes stay valid).
    pub fn remove(&mut self, t: usize) {
        for w in self.watched.iter_mut() {
            w.occs.retain(|o| o.target != t);
            w.cascades.retain(|c| c.target != t);
            w.maps.retain(|m| m.target != t);
        }
    }

    /// Whether some target reads `table`.
    pub fn watched(&self, table: &TableRef) -> bool {
        self.by_table.get(table).is_some_and(|&i| {
            let w = &self.watched[i];
            !w.occs.is_empty() || !w.cascades.is_empty() || !w.maps.is_empty()
        })
    }

    /// `table` as a TABLE_MAP with `columns` columns shows it (`None`: any shape): occurrences
    /// planned for another column count, cascades whose parent has one, and maps whose child
    /// has one, skip its rows.
    pub fn source(&self, table: &TableRef, columns: Option<usize>) -> Option<Source> {
        let idx = *self.by_table.get(table)?;
        let w = &self.watched[idx];
        let skip = w
            .occs
            .iter()
            .enumerate()
            .filter(|(_, o)| columns.is_some_and(|n| o.table.columns.len() != n))
            .map(|(i, _)| i)
            .collect();
        let skip_cas = w
            .cascades
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                columns.is_some_and(|n| c.plan.cascades[c.ci].parent.columns.len() != n)
            })
            .map(|(i, _)| i)
            .collect();
        let skip_maps = w
            .maps
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                columns.is_some_and(|n| m.plan.cascades[m.ci].child_table.columns.len() != n)
            })
            .map(|(i, _)| i)
            .collect();
        Some(Source {
            idx,
            skip,
            skip_cas,
            skip_maps,
        })
    }

    /// The targets with an occurrence, cascade or map that `s` skips, ascending, once each.
    pub fn mismatched(&self, s: &Source) -> Vec<usize> {
        let w = &self.watched[s.idx];
        let mut targets: Vec<usize> = s
            .skip
            .iter()
            .map(|&i| w.occs[i].target)
            .chain(s.skip_cas.iter().map(|&i| w.cascades[i].target))
            .chain(s.skip_maps.iter().map(|&i| w.maps[i].target))
            .collect();
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    /// One row change of `s`'s table.
    pub fn row<U, S: Sink<U>>(
        &self,
        s: &Source,
        before: Option<&BinlogRow>,
        after: Option<&BinlogRow>,
        hops: &mut Hops,
        sink: &mut S,
    ) {
        for (i, o) in self.watched[s.idx].occs.iter().enumerate() {
            if s.skip.contains(&i) || !sink.open(o.target) {
                continue;
            }
            if let (Some(b), Some(a)) = (before, after) {
                if !changed(b, a, &o.read) {
                    sink.row(o.target, true);
                    continue;
                }
            }
            // A side failing the occurrence's constant filters is not its row.
            let b = before.filter(|r| ConstFilter::passes(&o.filters, r, &o.table));
            let a = after.filter(|r| ConstFilter::passes(&o.filters, r, &o.table));
            if b.is_none() && a.is_none() {
                sink.row(o.target, true);
                continue;
            }
            sink.row(o.target, false);
            match &o.way {
                Way::Direct { cols } => ids(o, SpaceId::FULL, cols, false, b, a, sink),
                Way::Partial { space, cols } => ids(o, *space, cols, false, b, a, sink),
                Way::Hop { hop } => hop_ids(o, *hop, false, b, a, hops, sink),
            }
            // A row of a chain table changes what the hop's link values reach: its partner values'
            // entries go (or all of the hop's when the partner is another alias); answers in flight
            // for them are not cached.
            for &h in &o.chains {
                match hops.plan(h).and_then(|p| p.partner.clone()) {
                    Some((pa, cols)) if pa == o.alias => {
                        for r in b.into_iter().chain(a) {
                            hops.evict(h, &project(o, r, &cols));
                        }
                    }
                    _ => hops.clear(h),
                }
            }
        }
        for (i, cas) in self.watched[s.idx].cascades.iter().enumerate() {
            if s.skip_cas.contains(&i) || !sink.open(cas.target) {
                continue;
            }
            cascade(self, cas, before, after, hops, sink);
        }
        for (i, m) in self.watched[s.idx].maps.iter().enumerate() {
            if s.skip_maps.contains(&i) || !sink.open(m.target) {
                continue;
            }
            feed_map(m, before, after, sink);
        }
    }

    /// The change the storage engine made to a child row, replayed as a row change of `table`:
    /// like [`Consumer::row`] over that table's routes of target `t` alone (the section's replay),
    /// with no skip list, and every id marked `cascaded`.
    fn replay<U, S: Sink<U>>(
        &self,
        t: usize,
        table: &TableRef,
        before: Option<&BinlogRow>,
        after: Option<&BinlogRow>,
        hops: &mut Hops,
        sink: &mut S,
    ) {
        if !sink.open(t) {
            return;
        }
        let Some(&i) = self.by_table.get(table) else {
            return;
        };
        let w = &self.watched[i];
        for o in w.occs.iter().filter(|o| o.target == t) {
            if let (Some(b), Some(a)) = (before, after) {
                if !changed(b, a, &o.read) {
                    sink.row(t, true);
                    continue;
                }
            }
            let b = before.filter(|r| ConstFilter::passes(&o.filters, r, &o.table));
            let a = after.filter(|r| ConstFilter::passes(&o.filters, r, &o.table));
            if b.is_none() && a.is_none() {
                sink.row(t, true);
                continue;
            }
            sink.row(t, false);
            match &o.way {
                Way::Direct { cols } => ids(o, SpaceId::FULL, cols, true, b, a, sink),
                Way::Partial { space, cols } => ids(o, *space, cols, true, b, a, sink),
                Way::Hop { hop } => hop_ids(o, *hop, true, b, a, hops, sink),
            }
            for &h in &o.chains {
                match hops.plan(h).and_then(|p| p.partner.clone()) {
                    Some((pa, cols)) if pa == o.alias => {
                        for r in b.into_iter().chain(a) {
                            hops.evict(h, &project(o, r, &cols));
                        }
                    }
                    _ => hops.clear(h),
                }
            }
        }
        for cas in w.cascades.iter().filter(|c| c.target == t) {
            cascade(self, cas, before, after, hops, sink);
        }
        for m in w.maps.iter().filter(|m| m.target == t) {
            feed_map(m, before, after, sink);
        }
    }

    /// Replays the seeding log of plan `pi`'s cache of target `t` (from `Cache::install`), in
    /// binlog order: children straight into the cache, parents again with their kept rows
    /// expanded. A gap (a missed binlog range) is a note for the caller's safety net.
    pub fn install<U, S: Sink<U>>(
        &self,
        t: usize,
        pi: usize,
        log: Vec<crate::cascade::Logged>,
        hops: &mut Hops,
        sink: &mut S,
    ) {
        for entry in log {
            match entry {
                cascade::Logged::Child { route, old, new } => {
                    sink.cache(t, pi).child(route, old, new);
                }
                cascade::Logged::Parent { route, old, new } => {
                    if sink.cache(t, pi).gap {
                        sink.note(t, Note::CascadeGap);
                    }
                    let Some(rows) = sink.cache(t, pi).parent(route, &old, new.as_ref()) else {
                        continue;
                    };
                    // The target's own route of this cascade, for `expand`.
                    let Some(cas) = self
                        .watched
                        .iter()
                        .flat_map(|w| w.cascades.iter())
                        .find(|c| c.target == t && c.pi == pi && c.ci == route)
                    else {
                        continue;
                    };
                    expand(self, cas, &rows, new.as_ref(), hops, sink);
                }
            }
        }
    }
}

/// The ids of columns `cols` of the images (Direct and Partial paths) into space `space` of `o`'s
/// target. `cascaded`: the row is one the storage engine changed (6L5's replay), not a logged one.
fn ids<U, S: Sink<U>>(
    o: &Occ,
    space: SpaceId,
    cols: &[usize],
    cascaded: bool,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
    sink: &mut S,
) {
    let t = o.target;
    let give = |k: Tuple, op: Op, sink: &mut S| {
        let int = match &*k {
            [Datum::Int(i)] => Some(*i),
            _ => None,
        };
        match int {
            Some(i) => sink.int(t, space, intset::from_i64(i), op, cascaded),
            None => sink.tuple(t, space, k, op, cascaded),
        }
    };
    if let &[c] = cols {
        // One id column: the Datums decide, an integer never builds a tuple.
        let datum = |r: &BinlogRow| Datum::from_binlog(value_at(r, c), &o.table.columns[c]);
        let new = after.map(datum);
        let old = before.map(datum).filter(|d| new.as_ref() != Some(d));
        let same = before.is_some() && old.is_none();
        if let Some(d) = old {
            give(Box::new([d]), Op::Delete, sink);
        }
        if let Some(d) = new {
            give(
                Box::new([d]),
                if same { Op::Update } else { Op::Insert },
                sink,
            );
        }
        return;
    }
    let new = after.map(|r| project(o, r, cols));
    let old = before
        .map(|r| project(o, r, cols))
        .filter(|k| new.as_ref() != Some(k));
    let same = before.is_some() && old.is_none();
    if let Some(k) = old {
        give(k, Op::Delete, sink);
    }
    if let Some(k) = new {
        give(k, if same { Op::Update } else { Op::Insert }, sink);
    }
}

/// The id `k` of space `s` of target `t`: a one-part integer goes to the `int` hook,
/// anything else to `tuple`.
fn send<U, S: Sink<U>>(t: usize, s: SpaceId, k: Tuple, op: Op, cascaded: bool, sink: &mut S) {
    match Key::from_parts(k) {
        Key::Int(v) => sink.int(t, s, v, op, cascaded),
        Key::Tuple(k) => sink.tuple(t, s, k, op, cascaded),
    }
}

/// Records the effect of a parent-row change the storage engine cascades into the child
/// table `cas`'s target reads (ported from `accumulate::feed_cascade`, same decisions).
/// The consumer never rebuilds: a short parent image becomes a note for the caller's
/// safety net (guard R1 c).
fn cascade<U, S: Sink<U>>(
    cons: &Consumer,
    cas: &Cas,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
    hops: &mut Hops,
    sink: &mut S,
) {
    let c = &cas.plan.cascades[cas.ci];
    let Some(b) = before else { return };
    let action = match after {
        None => c.on_delete,
        Some(a) => {
            if !c
                .parent_cols
                .iter()
                .any(|&p| value_at(b, p) != value_at(a, p))
            {
                return;
            }
            c.on_update
        }
    };
    if action == FkAction::Restrict {
        return;
    }
    // A key no alias of the child pins: none of its child rows is read, before or after (the
    // new key of a CASCADE update, or NULL). A short image or an absent value may reach it.
    if let Some((pc, values)) = &c.reach {
        let reaches = |r: &BinlogRow| {
            r.len() != c.parent.columns.len()
                || value_at(r, *pc).is_none()
                || values.contains(&Datum::from_binlog(
                    value_at(r, *pc),
                    &c.parent.columns[*pc],
                ))
        };
        let moved = after.filter(|_| action == FkAction::Cascade);
        if !reaches(b) && !moved.is_some_and(reaches) {
            return;
        }
    }
    sink.row(cas.target, false);
    if cas.map {
        feed_mapped_cascade(cons, cas, action, b, after, hops, sink);
        return;
    }
    if c.derived {
        derived(cas, c, action, b, after, sink);
        return;
    }
    // `add` compiles only `Via::Direct` and `Via::Derived`, so this is the direct case:
    // the parent columns give each unit value.
    let Some(pcols) = &c.unit_from_parent else {
        return;
    };
    let unit = |row: &BinlogRow| -> Tuple {
        pcols
            .iter()
            .map(|&p| Datum::from_binlog(value_at(row, p), &c.parent.columns[p]))
            .collect()
    };
    let nulls = || pcols.iter().map(|_| Datum::Null).collect();
    let old = if action == FkAction::Cascade {
        Op::Delete
    } else {
        Op::Update
    };
    send(cas.target, SpaceId::FULL, unit(b), old, true, sink);
    if let Some(a) = after {
        match action {
            FkAction::Cascade => send(cas.target, SpaceId::FULL, unit(a), Op::Insert, true, sink),
            _ => send(cas.target, SpaceId::FULL, nulls(), Op::Update, true, sink),
        }
    } else if action == FkAction::SetNull {
        send(cas.target, SpaceId::FULL, nulls(), Op::Update, true, sink);
    }
}

/// A derived cascade (ported from `accumulate::feed_derived_cascade`): partial unit keys
/// from the parent's old key (and its new key, or NULLs, as the action sets the children's
/// FK), and for each pinned value equal to the old or new key, the presence item that gives
/// the units of those child rows. A parent row that is not a full image cannot be read:
/// the note sends it to the caller's safety net.
fn derived<U, S: Sink<U>>(
    cas: &Cas,
    c: &CascadeRoute,
    action: FkAction,
    b: &BinlogRow,
    after: Option<&BinlogRow>,
    sink: &mut S,
) {
    let full = |r: &BinlogRow| r.len() == c.parent.columns.len();
    // The children's FK after the change: the new key (CASCADE update) or NULL (SET NULL).
    let new_key = after.filter(|_| action == FkAction::Cascade);
    if !full(b) || new_key.is_some_and(|a| !full(a)) {
        sink.note(cas.target, Note::ShortParentImage);
        return;
    }
    let datum = |r: &BinlogRow, p: usize| Datum::from_binlog(value_at(r, p), &c.parent.columns[p]);
    for pin in &c.pinned {
        let pinned = |r: &BinlogRow| {
            let v = datum(r, pin.parent_col);
            pin.values
                .iter()
                .find(|(x, _)| *x == v)
                .map(|(x, k)| (*k, x.clone()))
        };
        if let Some((index, value)) = pinned(b) {
            sink.item(
                cas.target,
                Item::Presence {
                    index,
                    value,
                    hold: false,
                },
            );
        }
        if let Some((index, value)) = new_key.and_then(pinned) {
            sink.item(
                cas.target,
                Item::Presence {
                    index,
                    value,
                    hold: true,
                },
            );
        }
    }
    for (set, pcols) in &c.partial_from_parent {
        let key = |r: &BinlogRow| -> Tuple { pcols.iter().map(|&p| datum(r, p)).collect() };
        let new = match new_key {
            Some(a) => Some(key(a)),
            None if action == FkAction::SetNull => {
                Some(pcols.iter().map(|_| Datum::Null).collect())
            }
            None => None,
        };
        for k in std::iter::once(key(b)).chain(new) {
            send(cas.target, cas.sets[*set], k, Op::Update, true, sink);
        }
    }
}

/// A child row of a mapped cascade keeps its entry in the cache of `(target, plan)` (ported from
/// `accumulate::feed_map`, same decisions): an update of no stored column, and a row neither
/// image keeps, are nothing; while the cache seeds the change goes to its log instead, which is
/// what makes the caller hold the position until [`Consumer::install`]. No id.
fn feed_map<U, S: Sink<U>>(
    m: &MapChild,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
    sink: &mut S,
) {
    let c = &m.plan.cascades[m.ci];
    let Some(map) = &c.map else { return };
    if let (Some(b), Some(a)) = (before, after) {
        if !map.cols.iter().any(|&k| {
            value_at(b, k) != value_at(a, k)
                || matches!(value_at(a, k), Some(BinlogValue::JsonDiff(_)))
        }) {
            return;
        }
    }
    let old = before.and_then(|r| cascade::kept(c, r));
    let new = after.and_then(|r| cascade::kept(c, r));
    if old.is_none() && new.is_none() {
        return;
    }
    let old_key = old
        .as_ref()
        .map(|(k, _)| k.clone())
        .filter(|k| new.as_ref().is_none_or(|(nk, _)| nk != k));
    let seeding = sink.cache(m.target, m.pi).is_seeding();
    sink.cache(m.target, m.pi).child(m.ci, old_key, new);
    if seeding {
        sink.note(m.target, Note::Logged);
    }
}

/// A parent row of a mapped cascade (ported from `accumulate::feed_mapped_cascade`): the kept
/// children under the parent's old key come back and replay as the child row changes the storage
/// engine made; `None` from the cache means the change went to the seeding log, and a gap is a
/// note for the caller's safety net (the consumer never rebuilds).
fn feed_mapped_cascade<U, S: Sink<U>>(
    cons: &Consumer,
    cas: &Cas,
    action: FkAction,
    b: &BinlogRow,
    after: Option<&BinlogRow>,
    hops: &mut Hops,
    sink: &mut S,
) {
    let c = &cas.plan.cascades[cas.ci];
    let full = |r: &BinlogRow| r.len() == c.parent.columns.len();
    if !full(b) || after.is_some_and(|a| !full(a)) {
        sink.note(cas.target, Note::ShortParentImage);
        return;
    }
    let key = |r: &BinlogRow| -> Tuple {
        c.parent_cols
            .iter()
            .map(|&p| Datum::from_binlog(value_at(r, p), &c.parent.columns[p]))
            .collect()
    };
    // The children's FK after the change: the new key (a CASCADE update), NULLs (SET NULL), or
    // none (the children go with the parent).
    let new = match (after, action) {
        (Some(a), FkAction::Cascade) => Some(key(a)),
        (None, FkAction::Cascade) => None,
        _ => Some(c.parent_cols.iter().map(|_| Datum::Null).collect()),
    };
    let old = key(b);
    if sink.cache(cas.target, cas.pi).gap {
        sink.note(cas.target, Note::CascadeGap);
    }
    match sink
        .cache(cas.target, cas.pi)
        .parent(cas.ci, &old, new.as_ref())
    {
        None => sink.note(cas.target, Note::Logged),
        // No kept child row under `old` (none exists, or a gap): no id can change.
        Some(rows) if rows.is_empty() => {}
        Some(rows) => expand(cons, cas, &rows, new.as_ref(), hops, sink),
    }
}

/// The kept child rows of a parent change, expanded into child row changes at event time (ported
/// from `accumulate::expand`): a complete row replays through the consumer's own occurrences of
/// the child table (so it gets the ids a logged row gets), with its kept image before and, for a
/// key update, the same image carrying the new FK; an incomplete row (a stored column missing
/// from the image) is a note, not an id.
fn expand<U, S: Sink<U>>(
    cons: &Consumer,
    cas: &Cas,
    rows: &[cascade::KeptRow],
    new: Option<&Tuple>,
    hops: &mut Hops,
    sink: &mut S,
) {
    let c = &cas.plan.cascades[cas.ci];
    sink.note(cas.target, Note::CascadeRows(rows.len() as u64));
    for row in rows {
        if !row.complete {
            sink.note(cas.target, Note::IncompleteKeptRow);
            continue;
        }
        let before = cascade::kept_image(c, row, None);
        let after = new.map(|fk| cascade::kept_image(c, row, Some(fk)));
        cons.replay(
            cas.target,
            &c.child,
            Some(&before),
            after.as_ref(),
            hops,
            sink,
        );
    }
}

/// Columns `cols` of `r` as a key, the way `o`'s table declares them.
fn project(o: &Occ, r: &BinlogRow, cols: &[usize]) -> Tuple {
    cols.iter()
        .map(|&c| Datum::from_binlog(value_at(r, c), &o.table.columns[c]))
        .collect()
}

/// The ids the images' link values reach through hop `h`: a hit marks the cached ids, a miss defers
/// one item per link value. The hop's own rows never invalidate its cache (6A: its SELECT does not
/// read the occurrence's table), so only the chain tables' rows do, in `row`.
fn hop_ids<U, S: Sink<U>>(
    o: &Occ,
    h: HopId,
    cascaded: bool,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
    hops: &mut Hops,
    sink: &mut S,
) {
    let Some(p) = hops.plan(h) else { return };
    let (t, space) = (o.target, p.space);
    let link_cols = p.link_cols.clone();
    let link = |r: &BinlogRow| project(o, r, &link_cols);
    let new = after.map(link);
    let old = before.map(link).filter(|l| new.as_ref() != Some(l));
    for l in old.into_iter().chain(new) {
        match hops.get(h, &l) {
            Some(ids) => {
                sink.hop(t, true);
                for k in ids {
                    match k {
                        Key::Int(v) => sink.int(t, space, *v, Op::Update, cascaded),
                        Key::Tuple(k) => sink.tuple(t, space, k.clone(), Op::Update, cascaded),
                    }
                }
            }
            None => {
                sink.hop(t, false);
                sink.item(t, Item::Hop { hop: h, link: l });
            }
        }
    }
}

#[cfg(test)]
mod tests;
