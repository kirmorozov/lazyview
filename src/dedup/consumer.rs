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
}

/// Something a row showed that is not an id: counts, and changes whose ids are unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Note {
    /// A cascade parent's image lacks columns: the ids its cascade changes are unknown.
    ShortParentImage,
}

/// One cascade whose parent is the table, compiled.
#[derive(Debug, Clone)]
struct Cas {
    target: usize,
    plan: Arc<ViewPlan>,
    ci: usize,
    /// The space of each partial set of the plan.
    sets: Vec<SpaceId>,
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
}

/// A source table as one TABLE_MAP of it shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    idx: usize,
    /// Occurrences (indexes into the table's list) planned for another column count.
    skip: Vec<usize>,
    /// Cascades (indexes into the table's list) whose parent has another column count.
    skip_cas: Vec<usize>,
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
        for (ci, c) in access.cascades.iter().enumerate() {
            match &c.via {
                Via::Traced => {}
                Via::Direct { .. } | Via::Derived { .. } => {
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
                    compiled_cas.push((
                        c.parent.clone(),
                        Cas {
                            target: t,
                            plan: plan.clone(),
                            ci,
                            sets,
                        },
                    ));
                }
                Via::Map { .. } => return Err("map cascades need PR 6L".to_string()),
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

    /// Drops every occurrence and cascade of target `t` (table indexes stay valid).
    pub fn remove(&mut self, t: usize) {
        for w in self.watched.iter_mut() {
            w.occs.retain(|o| o.target != t);
            w.cascades.retain(|c| c.target != t);
        }
    }

    /// Whether some target reads `table`.
    pub fn watched(&self, table: &TableRef) -> bool {
        self.by_table.get(table).is_some_and(|&i| {
            let w = &self.watched[i];
            !w.occs.is_empty() || !w.cascades.is_empty()
        })
    }

    /// `table` as a TABLE_MAP with `columns` columns shows it (`None`: any shape): occurrences
    /// planned for another column count, and cascades whose parent has one, skip its rows.
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
        Some(Source {
            idx,
            skip,
            skip_cas,
        })
    }

    /// The targets with an occurrence or cascade that `s` skips, ascending, once each.
    pub fn mismatched(&self, s: &Source) -> Vec<usize> {
        let w = &self.watched[s.idx];
        let mut targets: Vec<usize> = s
            .skip
            .iter()
            .map(|&i| w.occs[i].target)
            .chain(s.skip_cas.iter().map(|&i| w.cascades[i].target))
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
                Way::Direct { cols } => ids(o, SpaceId::FULL, cols, b, a, sink),
                Way::Partial { space, cols } => ids(o, *space, cols, b, a, sink),
                Way::Hop { hop } => hop_ids(o, *hop, b, a, hops, sink),
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
            cascade(cas, before, after, sink);
        }
    }
}

/// The ids of columns `cols` of the images (Direct and Partial paths) into space `space` of `o`'s
/// target.
fn ids<U, S: Sink<U>>(
    o: &Occ,
    space: SpaceId,
    cols: &[usize],
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
            Some(i) => sink.int(t, space, intset::from_i64(i), op, false),
            None => sink.tuple(t, space, k, op, false),
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
    cas: &Cas,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
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
                        Key::Int(v) => sink.int(t, space, *v, Op::Update, false),
                        Key::Tuple(k) => sink.tuple(t, space, k.clone(), Op::Update, false),
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
mod tests {
    use super::*;
    use crate::catalog::{FkAction, ForeignKey};
    use crate::dedup::testkit::{
        catalog, catalog_fk, plan, plan_in, row, row_typed, table, Cell, Rec,
    };

    fn table_ref(name: &str) -> TableRef {
        ("db".into(), name.into())
    }

    /// Compiles `sql` as target 0.
    struct C {
        c: Consumer,
        hops: Hops,
    }

    fn compile(sql: &str) -> (C, AccessPlan) {
        let p = plan(sql);
        let ap = AccessPlan::of(&p);
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
        (C { c, hops }, ap)
    }

    /// Compiles every `(target, sql)` into one consumer, in the given order.
    fn compile_all(plans: &[(usize, &str)]) -> (C, Vec<AccessPlan>) {
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        let aps = plans
            .iter()
            .map(|(t, sql)| {
                let p = plan(sql);
                let ap = AccessPlan::of(&p);
                let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
                c.add(*t, 0, &p, &ap, &spaces, &mut hops).unwrap();
                ap
            })
            .collect();
        (C { c, hops }, aps)
    }

    /// The compiled path of the occurrence the definition aliases `alias`.
    fn path<'a>(ap: &'a AccessPlan, alias: &str) -> &'a Path {
        &ap.occurrences
            .iter()
            .find(|o| o.alias == alias)
            .unwrap_or_else(|| panic!("no occurrence aliased `{alias}`"))
            .path
    }

    const DIRECT: &str = "SELECT i.id, i.kind_id FROM items i";
    const FILTERED: &str =
        "SELECT i.id, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
                            AND v.key_id = 99 AND v.scope_id = 0";

    #[test]
    fn direct_ids_from_both_images() {
        let (mut x, _) = compile(DIRECT);
        let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
        let mut rec = Rec::default();
        // Insert [1, 7, 0].
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(1), Some(7), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(1, Op::Insert)]);
        // Update to [1, 8, 0]: it keeps its id, so it is marked once.
        x.c.row(
            &s,
            Some(&row(&[Some(1), Some(7), Some(0)])),
            Some(&row(&[Some(1), Some(8), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![(1, Op::Insert), (1, Op::Update)]
        );
        // Update [1, 8, 0] to [2, 8, 0]: the old id goes, the new one comes.
        x.c.row(
            &s,
            Some(&row(&[Some(1), Some(8), Some(0)])),
            Some(&row(&[Some(2), Some(8), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        // Delete [2, 8, 0].
        x.c.row(
            &s,
            Some(&row(&[Some(2), Some(8), Some(0)])),
            None,
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![
                (1, Op::Insert),
                (1, Op::Update),
                (1, Op::Delete),
                (2, Op::Insert),
                (2, Op::Delete),
            ]
        );
    }

    #[test]
    fn unread_column_update_marks_nothing() {
        let (mut x, _) = compile(FILTERED);
        let s =
            x.c.source(&table_ref("item_values"), Some(5))
                .expect("watched");
        let mut rec = Rec::default();
        x.c.row(
            &s,
            Some(&row(&[Some(4), Some(99), Some(0), Some(5), Some(1)])),
            Some(&row(&[Some(4), Some(99), Some(0), Some(5), Some(2)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, true)], "only `note` changed");
        assert!(rec.ids.is_empty(), "{rec:?}");
    }

    #[test]
    fn filters_drop_other_rows_and_rows_leaving_them() {
        let (mut x, _) = compile(FILTERED);
        let s =
            x.c.source(&table_ref("item_values"), Some(5))
                .expect("watched");
        let mut rec = Rec::default();
        // A row the filters never let in.
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, true)], "key_id 98 fails the filters");
        assert!(rec.ids.is_empty(), "{rec:?}");
        // A row leaving them: its id is gone.
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
            Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(5, Op::Delete)]);
        // A short image is not a full image: it passes the filters.
        x.c.row(&s, None, Some(&row(&[Some(5)])), &mut x.hops, &mut rec);
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![(5, Op::Delete), (5, Op::Insert)]
        );
        assert_eq!(rec.rows, vec![(0, true), (0, false), (0, false)]);
    }

    #[test]
    fn a_row_comes_back_to_the_filters_and_another_leaves_them() {
        // The two rows that cross the occurrence's filters: only the image that passes them is
        // the occurrence's row, so the side that fails them contributes no id - the arriving row
        // has no before-image left to compare with, so it is new to the occurrence, not kept.
        let (mut x, _) = compile(FILTERED);
        let s =
            x.c.source(&table_ref("item_values"), Some(5))
                .expect("watched");
        let mut rec = Rec::default();
        // Re-entry: key_id 98 (not the filters' row) becomes key_id 99 (it is).
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
            Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, false)], "the after-image is the row");
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![(5, Op::Insert)],
            "the id is new to the occurrence, not kept: {rec:?}"
        );
        // The reverse: key_id 99 becomes 98, so only the before-image is the row and its id is gone.
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
            Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![(5, Op::Insert), (5, Op::Delete)],
            "{rec:?}"
        );
        assert_eq!(rec.rows, vec![(0, false), (0, false)]);
    }

    #[test]
    fn partial_ids_go_to_their_space() {
        let p = plan(
            "SELECT l.parent_id, i.id, MAX(v.val) AS v FROM item_links l JOIN items i ON i.id \
             = l.child_id JOIN item_values v ON v.item_id = i.id AND v.key_id = 7 GROUP BY \
             l.parent_id, i.id",
        );
        let ap = AccessPlan::of(&p);
        let set = match &ap.occurrences[2].path {
            Path::Partial { set, .. } => *set,
            other => panic!("item_values is not a partial path: {other:?}"),
        };
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
        let s = c
            .source(&table_ref("item_values"), Some(5))
            .expect("watched");
        let mut rec = Rec::default();
        c.row(
            &s,
            None,
            Some(&row(&[Some(4), Some(7), Some(0), Some(9), Some(0)])),
            &mut hops,
            &mut rec,
        );
        assert_eq!(rec.ints(0, SpaceId::partial(set)), vec![(4, Op::Insert)]);
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    }

    /// The composite id of one table plus a column that table also reads.
    const TUPLE_AND_A_COLUMN: &str = "SELECT v.item_id, v.key_id, MAX(v.val) AS m FROM \
                                       item_values v GROUP BY v.item_id, v.key_id";

    #[test]
    fn a_tuple_id_that_keeps_its_parts_is_marked_once() {
        // The occurrence's id is two of its table's columns and it reads a third: an UPDATE of
        // that column alone is the tuple path's `same` branch, which no other test reaches (the
        // composite fixture below changes an id column, so its `same` is never true).
        let (mut x, ap) = compile(TUPLE_AND_A_COLUMN);
        assert!(
            matches!(&ap.occurrences[0].path, Path::Direct { cols } if cols == &[0, 1]),
            "{:?}",
            ap.occurrences[0].path
        );
        let s =
            x.c.source(&table_ref("item_values"), Some(5))
                .expect("watched");
        let mut rec = Rec::default();
        // Insert [5, 99, 0, 1, 0]: a new tuple.
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        // Update `val` only: both id columns stay, so the tuple is marked once, as an update.
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
            Some(&row(&[Some(5), Some(99), Some(0), Some(2), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        let id = vec![Datum::Int(5), Datum::Int(99)];
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![(id.clone(), Op::Insert), (id.clone(), Op::Update)],
            "an id that stays is not a delete plus an insert: {rec:?}"
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
        // An update that does move an id column still marks the old tuple gone and the new one in.
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(99), Some(0), Some(2), Some(0)])),
            Some(&row(&[Some(6), Some(99), Some(0), Some(2), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![
                (id.clone(), Op::Insert),
                (id.clone(), Op::Update),
                (id, Op::Delete),
                (vec![Datum::Int(6), Datum::Int(99)], Op::Insert),
            ],
            "{rec:?}"
        );
    }

    #[test]
    fn composite_ids_are_tuples() {
        // The view id is two columns: every id reaches the sink's tuple hook.
        let (mut x, _) = compile("SELECT l.child_id, l.parent_id FROM item_links l");
        let s =
            x.c.source(&table_ref("item_links"), Some(2))
                .expect("watched");
        let mut rec = Rec::default();
        // Insert [3, 4].
        x.c.row(
            &s,
            None,
            Some(&row_typed(&[Cell::Int(3), Cell::Int(4)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![(vec![Datum::Int(3), Datum::Int(4)], Op::Insert)]
        );
        // Update [3, 4] to [3, 5]: the old tuple goes, the new one comes.
        x.c.row(
            &s,
            Some(&row_typed(&[Cell::Int(3), Cell::Int(4)])),
            Some(&row_typed(&[Cell::Int(3), Cell::Int(5)])),
            &mut x.hops,
            &mut rec,
        );
        // Delete [3, 5].
        x.c.row(
            &s,
            Some(&row_typed(&[Cell::Int(3), Cell::Int(5)])),
            None,
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![
                (vec![Datum::Int(3), Datum::Int(4)], Op::Insert),
                (vec![Datum::Int(3), Datum::Int(4)], Op::Delete),
                (vec![Datum::Int(3), Datum::Int(5)], Op::Insert),
                (vec![Datum::Int(3), Datum::Int(5)], Op::Delete),
            ]
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    }

    #[test]
    fn a_string_id_is_a_tuple() {
        // One id column, not an integer: still a tuple, never an int.
        let (mut x, _) = compile("SELECT s.slug, s.kind_id FROM slugs s");
        let s = x.c.source(&table_ref("slugs"), Some(2)).expect("watched");
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row_typed(&[Cell::Text("abc"), Cell::Int(7)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![(vec![Datum::Bytes(b"abc".to_vec())], Op::Insert)]
        );
    }

    #[test]
    fn an_unsigned_id_above_i64_max_is_a_tuple() {
        // The split is on the decoded Datum: an UNSIGNED BIGINT above i64::MAX decodes to
        // Datum::UInt, so the sink's tuple hook fires; one that fits an i64 stays the int hook.
        const OVER: u64 = 1 << 63;
        let (mut x, _) = compile("SELECT c.id, c.kind_id FROM counters c");
        let s =
            x.c.source(&table_ref("counters"), Some(2))
                .expect("watched");
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row_typed(&[Cell::UInt(OVER), Cell::Int(7)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![(vec![Datum::UInt(OVER)], Op::Insert)]
        );
        x.c.row(
            &s,
            None,
            Some(&row_typed(&[Cell::UInt(5), Cell::Int(7)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(5, Op::Insert)]);
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![(vec![Datum::UInt(OVER)], Op::Insert)],
            "the above-i64::MAX id is still the only tuple"
        );
    }

    #[test]
    fn a_smallint_id_stays_the_int_hook() {
        // A narrower signed type is still one Datum::Int: the int hook, with its value. A
        // negative value catches a decode that mistook it for unsigned (65533 then).
        let (mut x, _) = compile("SELECT t.code, t.kind_id FROM tags t");
        let s = x.c.source(&table_ref("tags"), Some(2)).expect("watched");
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row_typed(&[Cell::Int(-3), Cell::Int(7)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(-3, Op::Insert)]);
        assert!(rec.tuples(0, SpaceId::FULL).is_empty(), "{rec:?}");
    }

    #[test]
    fn shape_mismatch_skips_the_occurrence() {
        let (mut x, _) = compile(DIRECT);
        let s = x.c.source(&table_ref("items"), Some(4)).expect("watched");
        assert_eq!(x.c.mismatched(&s), vec![0]);
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(1), Some(7), Some(0), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
        // Any shape reaches it.
        let s = x.c.source(&table_ref("items"), None).expect("watched");
        assert!(x.c.mismatched(&s).is_empty());
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(1), Some(7), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(1, Op::Insert)]);
    }

    #[test]
    fn closed_targets_get_nothing() {
        let (mut x, _) = compile(DIRECT);
        let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
        let mut rec = Rec::default();
        rec.closed.push(0);
        x.c.row(
            &s,
            Some(&row(&[Some(1), Some(7), Some(0)])),
            Some(&row(&[Some(1), Some(8), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
    }

    #[test]
    fn remove_drops_a_target() {
        let (mut x, _) = compile(DIRECT);
        x.c.remove(0);
        assert!(!x.c.watched(&table_ref("items")), "no occurrence is left");
        let mut rec = Rec::default();
        if let Some(s) = x.c.source(&table_ref("items"), Some(3)) {
            x.c.row(
                &s,
                None,
                Some(&row(&[Some(1), Some(7), Some(0)])),
                &mut x.hops,
                &mut rec,
            );
        }
        assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
    }

    #[test]
    fn two_targets_share_a_source_table() {
        const KEY_98: &str =
            "SELECT i.id, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
                            AND v.key_id = 98 AND v.scope_id = 0";
        // Target 1 first, then 0, then 1 again: its skipped targets come out unsorted with a
        // duplicate.
        let (mut x, _aps) = compile_all(&[(1, FILTERED), (0, DIRECT), (1, KEY_98)]);
        let items = table_ref("items");
        assert!(x.c.watched(&items), "both targets watch items");
        assert!(
            x.c.watched(&table_ref("item_values")),
            "target 1's joined table is watched too"
        );
        // One row change: both targets' occurrences of items see it, in occurrence order.
        let s = x.c.source(&items, Some(3)).expect("watched");
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(1), Some(7), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(1, false), (0, false), (1, false)]);
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(1, Op::Insert)]);
        assert_eq!(
            rec.ints(1, SpaceId::FULL),
            vec![(1, Op::Insert), (1, Op::Insert)]
        );
        // A TABLE_MAP of another shape: occurrences 1, 0 and 1 are all skipped, and the skip
        // entries come out ascending, once each.
        let s = x.c.source(&items, Some(4)).expect("watched");
        assert_eq!(x.c.mismatched(&s), vec![0, 1]);
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(1), Some(7), Some(0), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
        // remove(0) keeps every occurrence of target 1.
        x.c.remove(0);
        assert!(x.c.watched(&items), "target 1 still watches items");
        assert!(
            x.c.watched(&table_ref("item_values")),
            "target 1 still watches item_values"
        );
        let s = x.c.source(&items, Some(3)).expect("watched");
        let mut rec = Rec::default();
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(2), Some(7), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(1, false), (1, false)]);
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
        assert_eq!(
            rec.ints(1, SpaceId::FULL),
            vec![(2, Op::Insert), (2, Op::Insert)]
        );
        // Only when the last target goes does the table stop being watched.
        x.c.remove(1);
        assert!(!x.c.watched(&items), "no occurrence of items is left");
        assert!(
            !x.c.watched(&table_ref("item_values")),
            "no occurrence of item_values is left"
        );
    }

    /// Every table `ap` can make the consumer watch: its occurrences and both sides of its cascades.
    fn plan_tables(ap: &AccessPlan) -> Vec<TableRef> {
        let mut v: Vec<TableRef> = ap.occurrences.iter().map(|o| o.table.clone()).collect();
        for c in &ap.cascades {
            v.push(c.parent.clone());
            v.push(c.child.clone());
        }
        v.sort();
        v.dedup();
        v
    }

    /// [`catalog`] with a cascading FK onto `item_values` whose parent columns give every unit
    /// value, which is the analyzer's `Via::Direct`.
    fn cascading_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        let mut cat = catalog();
        let mut t = (*cat[&table_ref("item_values")]).clone();
        t.cascades = vec![ForeignKey {
            parent: table_ref("item_links"),
            child_cols: vec![0, 1],
            parent_cols: vec!["child_id".into(), "parent_id".into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        }];
        cat.insert(table_ref("item_values"), Arc::new(t));
        cat
    }

    /// `add` compiles everything before it inserts anything, so each refusal leaves every table of
    /// the plan unwatched. (Hop paths compile since 6J; their tests are the last group below.)
    #[test]
    fn a_refusal_watches_no_table_of_the_plan() {
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);

        // A route the consumer does not take: the analyzer's Recompute route, over two tables, so
        // an occurrence compiled before the refusal would show.
        let p = plan("SELECT MAX(v.val) AS m FROM items i JOIN item_values v ON v.item_id = i.id");
        let ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Recompute), "{ap}");
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        assert!(
            c.add(0, 0, &p, &ap, &spaces, &mut hops)
                .unwrap_err()
                .starts_with("route recompute"),
            "the route is the reason"
        );
        for t in plan_tables(&ap) {
            assert!(!c.watched(&t), "the route refusal watched {t:?}");
        }

        // A path the consumer does not take, on a plan whose route is Dedup: the analyzer gives
        // `Whole` only with the Full and Recompute routes, so the second occurrence carries it
        // here. The first one compiled, so a half-watch would leave `items` watched.
        let p = plan(FILTERED);
        let mut ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        assert_eq!(ap.occurrences.len(), 2, "items and item_values");
        ap.occurrences[1].path = Path::Whole;
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let err = c.add(1, 0, &p, &ap, &spaces, &mut hops).unwrap_err();
        assert_eq!(err, "`v`: Whole");
        for t in plan_tables(&ap) {
            assert!(!c.watched(&t), "the path refusal watched {t:?}");
        }

        // A cascade the consumer cannot follow even after 6K: `Via::Untraced`, forced onto a
        // plan the route still lets through (its own `Via::Direct` compiles now). Its parent
        // is a table no occurrence reads, so the refusal must not have left the child watched
        // either. (The map refusal is `map_cascades_are_refused_until_6l`.)
        let p = plan_in(TUPLE_AND_A_COLUMN, &cascading_catalog());
        let mut ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        assert!(
            matches!(ap.cascades[0].via, Via::Direct { .. }),
            "{:?}",
            ap.cascades
        );
        ap.cascades[0].via = Via::Untraced {
            reason: "no unit".into(),
        };
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let err = c.add(2, 0, &p, &ap, &spaces, &mut hops).unwrap_err();
        assert!(err.contains("untraced"), "{err}");
        for t in plan_tables(&ap) {
            assert!(!c.watched(&t), "the cascade refusal watched {t:?}");
        }
    }

    /// The same `item_values` twice, giving the two parts of the unit in different columns: `v1`
    /// the second part through its `item_id`, `v2` the first through its `key_id`.
    const TWO_PARTIALS: &str = "SELECT l.parent_id, i.id, MAX(v1.val) AS a, MAX(v2.val) AS b \
                                FROM item_links l JOIN items i ON i.id = l.child_id JOIN \
                                item_values v1 ON v1.item_id = i.id AND v1.key_id = 7 JOIN \
                                item_values v2 ON v2.key_id = l.parent_id GROUP BY l.parent_id, \
                                i.id";

    #[test]
    fn two_partial_sets_keep_their_ids_apart() {
        let p = plan(TWO_PARTIALS);
        let ap = AccessPlan::of(&p);
        let partial = |alias: &str| match path(&ap, alias) {
            Path::Partial {
                set,
                positions,
                cols,
            } => (*set, positions.clone(), cols.clone()),
            other => panic!("`{alias}` is not a partial path: {other:?}"),
        };
        // One consumer, two partial occurrences of one table, each under its own unit positions.
        let (set1, pos1, cols1) = partial("v1");
        let (set2, pos2, cols2) = partial("v2");
        assert_eq!((pos1.as_slice(), cols1.as_slice()), (&[1][..], &[0][..]));
        assert_eq!((pos2.as_slice(), cols2.as_slice()), (&[0][..], &[1][..]));
        let (sp1, sp2) = (SpaceId::partial(set1), SpaceId::partial(set2));
        assert_ne!(sp1, sp2, "different positions, different spaces");
        assert_ne!(sp1, SpaceId::FULL);
        assert_ne!(sp2, SpaceId::FULL);

        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        c.add(
            0,
            0,
            &p,
            &ap,
            &Spaces::of(p.unit_exprs.len(), &p.partial.sets),
            &mut hops,
        )
        .unwrap();
        let s = c
            .source(&table_ref("item_values"), Some(5))
            .expect("watched");
        // One row change of the shared table: each occurrence projects its own columns, into its
        // own space, and neither sees the other's id.
        let mut rec = Rec::default();
        c.row(
            &s,
            None,
            Some(&row(&[Some(4), Some(7), Some(0), Some(9), Some(0)])),
            &mut hops,
            &mut rec,
        );
        assert_eq!(
            rec.rows,
            vec![(0, false), (0, false)],
            "both occurrences took the row"
        );
        assert_eq!(rec.ints(0, sp1), vec![(4, Op::Insert)], "v1: its item_id");
        assert_eq!(rec.ints(0, sp2), vec![(7, Op::Insert)], "v2: its key_id");
        assert!(
            rec.tuples(0, sp1).is_empty() && rec.tuples(0, sp2).is_empty(),
            "{rec:?}"
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");

        // A target whose spaces hold no partial set: the compile refuses, and it refuses the whole
        // plan, not the occurrence it was on.
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        let err = c
            .add(
                0,
                0,
                &p,
                &ap,
                &Spaces::of(p.unit_exprs.len(), &[]),
                &mut hops,
            )
            .unwrap_err();
        assert!(err.contains("no space for"), "{err}");
        for t in plan_tables(&ap) {
            assert!(!c.watched(&t), "the space refusal watched {t:?}");
        }
    }

    /// The one-hop view of access.rs `one_hop_reaches_the_full_id`: `l` reaches the full id through
    /// the link, so its rows are looked up in the hop cache and `i`'s rows invalidate it.
    const ONE_HOP: &str =
        "SELECT i.id, l.child_id FROM items i JOIN item_links l ON l.parent_id = \
                           i.kind_id";

    /// The two-hop view of access.rs `two_hops_through_two_tables`: `o` reaches the id through `l`
    /// and `i`, so both of them are its chain and the partner is `l`.`child_id`.
    const TWO_HOPS: &str = "SELECT i.id, MAX(o.name) n FROM items i JOIN item_links l ON \
                            l.parent_id = i.kind_id JOIN owners o ON o.id = l.child_id GROUP BY \
                            i.id";

    /// A one-part link value, the way the hop cache keys an entry by it.
    fn link(v: i64) -> Tuple {
        Box::new([Datum::Int(v)])
    }

    /// Cached ids, as [`Hops::answer`] takes them.
    fn cached(vs: &[i64]) -> Vec<Key> {
        vs.iter().map(|&v| Key::from_datum(Datum::Int(v))).collect()
    }

    /// The hop of occurrence `alias` of the compiled target.
    fn hop_for(x: &C, alias: usize) -> HopId {
        (0..)
            .map(HopId)
            .find(|h| x.hops.plan(*h).is_some_and(|p| p.alias == alias))
            .expect("that occurrence is not a hop")
    }

    /// An answer for link value `v` of hop `h`, the way a job's completion gives it.
    fn cache(x: &mut C, h: HopId, v: i64, ids: Vec<Key>) {
        let l = link(v);
        x.hops.taken(h, &l);
        assert!(x.hops.answer(h, l, ids), "{v} is not in flight");
    }

    #[test]
    fn hop_hit_marks_cached_ids() {
        let (mut x, ap) = compile(ONE_HOP);
        assert!(
            matches!(&ap.occurrences[1].path, Path::Hop(_)),
            "{:?}",
            ap.occurrences[1].path
        );
        let h = hop_for(&x, 1);
        let p = x.hops.plan(h).expect("the hop of `l`");
        assert_eq!(
            (p.link_cols.as_slice(), p.chain.as_slice()),
            (&[1][..], &[0][..]),
            "link `l`.`parent_id`, the chain is `i`"
        );
        assert_eq!(
            p.partner,
            Some((0, vec![1])),
            "the partner is `i`.`kind_id`"
        );
        assert_eq!(p.space, SpaceId::FULL, "the hop reaches the full id");
        cache(&mut x, h, 3, cached(&[10, 11]));
        let s =
            x.c.source(&table_ref("item_links"), Some(2))
                .expect("watched");
        let mut rec = Rec::default();
        // Insert [5, 3]: its link value is cached, so its ids are marked and nothing defers.
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(5), Some(3)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![(10, Op::Update), (11, Op::Update)],
            "{rec:?}"
        );
        assert_eq!(rec.hops, vec![(0, true)], "one lookup, a hit");
        assert!(rec.items.is_empty(), "a hit defers nothing: {rec:?}");
    }

    #[test]
    fn hop_miss_defers_one_item_per_link_value() {
        let (mut x, _) = compile(ONE_HOP);
        let h = hop_for(&x, 1);
        let s =
            x.c.source(&table_ref("item_links"), Some(2))
                .expect("watched");
        let mut rec = Rec::default();
        // Update [5, 3] to [5, 4]: nothing is cached, so each link value defers its own item.
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(3)])),
            Some(&row(&[Some(5), Some(4)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.items,
            vec![
                (
                    0,
                    Item::Hop {
                        hop: h,
                        link: link(3)
                    }
                ),
                (
                    0,
                    Item::Hop {
                        hop: h,
                        link: link(4)
                    }
                ),
            ],
            "the old and the new link value, in that order"
        );
        assert_eq!(rec.hops, vec![(0, false), (0, false)], "both missed");
        assert!(rec.ids.is_empty(), "a miss marks nothing: {rec:?}");
    }

    #[test]
    fn delete_through_a_hop_uses_the_before_image() {
        let (mut x, _) = compile(ONE_HOP);
        let h = hop_for(&x, 1);
        cache(&mut x, h, 3, cached(&[10, 11]));
        let s =
            x.c.source(&table_ref("item_links"), Some(2))
                .expect("watched");
        let mut rec = Rec::default();
        // Delete [5, 3]: the before-image is the only one that gives the link value.
        x.c.row(
            &s,
            Some(&row(&[Some(5), Some(3)])),
            None,
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(
            rec.ints(0, SpaceId::FULL),
            vec![(10, Op::Update), (11, Op::Update)],
            "{rec:?}"
        );
        assert_eq!(rec.hops, vec![(0, true)]);
        assert!(rec.items.is_empty(), "{rec:?}");
    }

    #[test]
    fn partner_row_change_evicts_its_values() {
        let (mut x, _) = compile(ONE_HOP);
        let h = hop_for(&x, 1);
        for v in [3, 4, 5] {
            cache(&mut x, h, v, cached(&[100 + v]));
        }
        let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
        let mut rec = Rec::default();
        // An `items` row moves its `kind_id` from 3 to 4: that alias holds every partner column, so
        // both images' partner values lose their entries.
        x.c.row(
            &s,
            Some(&row(&[Some(1), Some(3), Some(9)])),
            Some(&row(&[Some(1), Some(4), Some(9)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(x.hops.get(h, &link(3)), None, "the old partner value");
        assert_eq!(x.hops.get(h, &link(4)), None, "the new partner value");
        assert_eq!(
            x.hops.get(h, &link(5)),
            Some(&cached(&[105])[..]),
            "another cached link stays"
        );
        assert!(rec.hops.is_empty(), "`i` is not a hop: {rec:?}");
    }

    #[test]
    fn other_chain_row_clears_the_hop() {
        let (mut x, ap) = compile(TWO_HOPS);
        assert!(
            matches!(&ap.occurrences[2].path, Path::Hop(h) if h.aliases == [0, 1]),
            "{:?}",
            ap.occurrences[2].path
        );
        let oh = hop_for(&x, 2);
        let p = x.hops.plan(oh).expect("the hop of `o`").clone();
        assert_eq!(p.chain, vec![0, 1], "the chain is `i` and `l`");
        assert_eq!(
            p.partner,
            Some((1, vec![0])),
            "the partner is `l`.`child_id`"
        );
        for v in [3, 4] {
            cache(&mut x, oh, v, cached(&[v]));
        }
        let mut rec = Rec::default();
        // An `items` row: a chain alias that is not the partner's, so all of `o`'s hop goes.
        let items = x.c.source(&table_ref("items"), Some(3)).expect("watched");
        x.c.row(
            &items,
            Some(&row(&[Some(1), Some(3), Some(9)])),
            Some(&row(&[Some(1), Some(4), Some(9)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(
            x.hops.is_empty(),
            "a row of another chain alias clears the hop: {p:?}"
        );
        for v in [3, 4, 7] {
            cache(&mut x, oh, v, cached(&[v]));
        }
        // An `item_links` row: the partner's own alias, so only its child_id values go.
        let links =
            x.c.source(&table_ref("item_links"), Some(2))
                .expect("watched");
        x.c.row(
            &links,
            Some(&row(&[Some(3), Some(9)])),
            Some(&row(&[Some(4), Some(9)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(x.hops.get(oh, &link(3)), None, "the old child_id");
        assert_eq!(x.hops.get(oh, &link(4)), None, "the new child_id");
        assert_eq!(
            x.hops.get(oh, &link(7)),
            Some(&cached(&[7])[..]),
            "an entry no partner value of this row names stays"
        );
    }

    #[test]
    fn unread_partner_update_keeps_the_cache() {
        let (mut x, _) = compile(ONE_HOP);
        let h = hop_for(&x, 1);
        cache(&mut x, h, 3, cached(&[10]));
        cache(&mut x, h, 4, cached(&[11]));
        let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
        let mut rec = Rec::default();
        // `owner_id` is no column the view reads: the occurrence does not take the row at all, so
        // the chain invalidation of its alias never runs.
        x.c.row(
            &s,
            Some(&row(&[Some(1), Some(3), Some(9)])),
            Some(&row(&[Some(1), Some(3), Some(10)])),
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, true)], "only an unread column changed");
        assert_eq!(x.hops.get(h, &link(3)), Some(&cached(&[10])[..]));
        assert_eq!(x.hops.get(h, &link(4)), Some(&cached(&[11])[..]));
    }

    /// Compiles the owner-count view over [`catalog_fk`] as target 0.
    fn compile_fk(on_delete: FkAction) -> (C, AccessPlan) {
        let p = plan_in(
            "SELECT i.owner_id, COUNT(*) n FROM items i GROUP BY i.owner_id",
            &catalog_fk(on_delete),
        );
        let ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
        (C { c, hops }, ap)
    }

    #[test]
    fn parent_delete_marks_the_fk_ids() {
        let (mut x, ap) = compile_fk(FkAction::Cascade);
        assert!(
            matches!(ap.cascades[0].via, Via::Direct { .. }),
            "STOP if not: {:?}",
            ap.cascades
        );
        let s = x.c.source(&table_ref("owners"), Some(2)).expect("watched");
        let mut rec = Rec::default();
        // A delete of owners [7, ...]: the cascade marks the FK value it took with it.
        x.c.row(
            &s,
            Some(&row(&[Some(7), Some(0)])),
            None,
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, false)]);
        assert_eq!(
            rec.ids,
            vec![(
                0,
                SpaceId::FULL,
                Key::Int(intset::from_i64(7)),
                Op::Delete,
                true
            )]
        );
        assert!(rec.items.is_empty() && rec.notes.is_empty(), "{rec:?}");
    }

    #[test]
    fn restrict_and_unchanged_keys_mark_nothing() {
        let (mut x, _) = compile_fk(FkAction::Restrict);
        let s = x.c.source(&table_ref("owners"), Some(2)).expect("watched");
        let mut rec = Rec::default();
        // A delete the engine restricts: the view saw nothing.
        x.c.row(
            &s,
            Some(&row(&[Some(7), Some(0)])),
            None,
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
        // An update of `name` only: no `parent_cols` value changed, so no cascade.
        x.c.row(
            &s,
            Some(&row(&[Some(7), Some(1)])),
            Some(&row(&[Some(7), Some(2)])),
            &mut x.hops,
            &mut rec,
        );
        assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
    }

    #[test]
    fn set_null_marks_the_null_key() {
        let (mut x, ap) = compile_fk(FkAction::SetNull);
        assert!(
            matches!(ap.cascades[0].via, Via::Direct { .. }),
            "{:?}",
            ap.cascades
        );
        let s = x.c.source(&table_ref("owners"), Some(2)).expect("watched");
        let mut rec = Rec::default();
        // A delete sets the children's FK to NULL: the old key and the NULL key are marked.
        x.c.row(
            &s,
            Some(&row(&[Some(7), Some(0)])),
            None,
            &mut x.hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, false)]);
        assert_eq!(rec.ints(0, SpaceId::FULL), vec![(7, Op::Update)]);
        assert_eq!(
            rec.tuples(0, SpaceId::FULL),
            vec![(vec![Datum::Null], Op::Update)]
        );
        assert!(
            rec.ids.iter().all(|(_, _, _, _, cascaded)| *cascaded),
            "{rec:?}"
        );
    }

    /// The catalog of `accumulate::derived_cascades_use_the_parent_key`: `attrs` with FKs onto
    /// `obj` and `kind`, whose (single-column) keys the view pins.
    fn derived_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols: vec![child_col],
            parent_cols: vec![parent_col.into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        let mut cat: HashMap<TableRef, Arc<TableInfo>> = [
            table("kind", &[("kid", "smallint unsigned")], &[0]),
            table("obj", &[("oid", "int unsigned")], &[0]),
            table("link", &[("gid", "int"), ("oid", "int unsigned")], &[0, 1]),
            table(
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
        let (_, attrs) = table(
            "attrs",
            &[
                ("id", "int"),
                ("oid", "int unsigned"),
                ("aid", "smallint unsigned"),
                ("val", "int"),
            ],
            &[0],
        );
        let mut attrs = (*attrs).clone();
        attrs.cascades = vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")];
        cat.insert(table_ref("attrs"), Arc::new(attrs));
        cat
    }

    #[test]
    fn derived_cascades() {
        let p = plan_in(
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c \
             JOIN attrs a ON a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, c.oid",
            &derived_catalog(),
        );
        let ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        let ci = |parent: &str| {
            ap.cascades
                .iter()
                .position(|c| c.parent.1 == parent)
                .unwrap()
        };
        let (kind, obj) = (ci("kind"), ci("obj"));
        assert!(
            matches!(ap.cascades[kind].via, Via::Derived { .. }),
            "{:?}",
            ap.cascades
        );
        assert!(
            matches!(ap.cascades[obj].via, Via::Derived { .. }),
            "{:?}",
            ap.cascades
        );
        let set = p.cascades[obj].partial_from_parent[0].0;
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
        let parent = |name: &str| c.source(&table_ref(name), Some(1)).expect("watched");
        let (kind_src, obj_src) = (parent("kind"), parent("obj"));

        // A key no alias reads: nothing to do.
        let mut rec = Rec::default();
        c.row(
            &kind_src,
            Some(&row(&[Some(50)])),
            None,
            &mut hops,
            &mut rec,
        );
        c.row(
            &kind_src,
            Some(&row(&[Some(50)])),
            Some(&row(&[Some(51)])),
            &mut hops,
            &mut rec,
        );
        assert!(
            rec.rows.is_empty() && rec.ids.is_empty() && rec.items.is_empty(),
            "{rec:?}"
        );

        // The pinned key: a presence item for the units its presence column names.
        let mut rec = Rec::default();
        c.row(&kind_src, Some(&row(&[Some(7)])), None, &mut hops, &mut rec);
        assert_eq!(rec.rows, vec![(0, false)]);
        assert_eq!(
            rec.items,
            vec![(
                0,
                Item::Presence {
                    index: 0,
                    value: Datum::Int(7),
                    hold: false,
                }
            )]
        );
        assert!(rec.ids.is_empty() && rec.notes.is_empty(), "{rec:?}");

        // A key update into the pinned value: a hold, not a mark.
        let mut rec = Rec::default();
        c.row(
            &kind_src,
            Some(&row(&[Some(50)])),
            Some(&row(&[Some(7)])),
            &mut hops,
            &mut rec,
        );
        assert_eq!(
            rec.items,
            vec![(
                0,
                Item::Presence {
                    index: 0,
                    value: Datum::Int(7),
                    hold: true,
                }
            )]
        );

        // Out of the pinned value, twice: the consumer sends one item per row (the core
        // dedups them per flush, as `push_once` did here).
        let mut rec = Rec::default();
        c.row(
            &kind_src,
            Some(&row(&[Some(7)])),
            Some(&row(&[Some(51)])),
            &mut hops,
            &mut rec,
        );
        c.row(&kind_src, Some(&row(&[Some(7)])), None, &mut hops, &mut rec);
        assert_eq!(
            rec.items,
            vec![
                (
                    0,
                    Item::Presence {
                        index: 0,
                        value: Datum::Int(7),
                        hold: false,
                    }
                ),
                (
                    0,
                    Item::Presence {
                        index: 0,
                        value: Datum::Int(7),
                        hold: false,
                    }
                ),
            ]
        );
        assert!(rec.notes.is_empty(), "{rec:?}");

        // The partial units from the parent's key: the deleted key, the old and the new key
        // of an update, all as updates of the partial set, all cascaded.
        let mut rec = Rec::default();
        c.row(&obj_src, Some(&row(&[Some(5)])), None, &mut hops, &mut rec);
        c.row(
            &obj_src,
            Some(&row(&[Some(6)])),
            Some(&row(&[Some(9)])),
            &mut hops,
            &mut rec,
        );
        assert_eq!(rec.rows, vec![(0, false), (0, false)]);
        assert_eq!(
            rec.ints(0, SpaceId::partial(set)),
            vec![(5, Op::Update), (6, Op::Update), (9, Op::Update)]
        );
        assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
        assert!(rec.notes.is_empty(), "{rec:?}");

        // A parent image that is not full cannot be read: a note, nothing else (the core
        // never rebuilds).
        let mut rec = Rec::default();
        c.row(&kind_src, Some(&row(&[])), None, &mut hops, &mut rec);
        assert_eq!(rec.notes, vec![(0, Note::ShortParentImage)]);
        assert!(rec.ids.is_empty() && rec.items.is_empty(), "{rec:?}");
    }

    #[test]
    fn map_cascades_are_refused_until_6l() {
        let p = plan_in(TUPLE_AND_A_COLUMN, &cascading_catalog());
        let mut ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        ap.cascades[0].via = Via::Map {
            key: vec![0],
            cols: vec![0, 1],
        };
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        let err = c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap_err();
        assert!(err.contains("6L"), "{err}");
        for t in plan_tables(&ap) {
            assert!(!c.watched(&t), "the map refusal watched {t:?}");
        }
    }
}
