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
use crate::analyze::{ConstFilter, ViewPlan};
use crate::catalog::{TableInfo, TableRef};
use crate::datum::{Datum, Key as Tuple};
use crate::intset;

use super::hop::Hops;
use super::key::{Item, Op, SpaceId, Spaces};

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
}

/// How an occurrence's rows reach its target's ids.
#[derive(Debug, Clone)]
enum Way {
    Direct { cols: Vec<usize> },
    Partial { space: SpaceId, cols: Vec<usize> },
}

/// One occurrence, compiled.
#[derive(Debug, Clone)]
struct Occ {
    target: usize,
    table: Arc<TableInfo>,
    read: Vec<bool>,
    filters: Vec<ConstFilter>,
    way: Way,
}

/// Every route of one source table.
#[derive(Debug, Default)]
struct Watched {
    occs: Vec<Occ>,
}

/// A source table as one TABLE_MAP of it shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    idx: usize,
    /// Occurrences (indexes into the table's list) planned for another column count.
    skip: Vec<usize>,
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
        _pi: usize,
        plan: &Arc<ViewPlan>,
        access: &AccessPlan,
        spaces: &Spaces,
        _hops: &mut Hops,
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
                Path::Hop(_) => return Err("hop paths need PR 6J".to_string()),
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
                    table,
                    read,
                    filters: occ.filters.clone(),
                    way,
                },
            ));
        }
        for c in &access.cascades {
            if !matches!(c.via, Via::Traced) {
                return Err("cascades need PR 6K / 6L".to_string());
            }
        }
        for (table, occ) in compiled {
            let i = match self.by_table.get(&table) {
                Some(&i) => i,
                None => {
                    self.watched.push(Watched::default());
                    let i = self.watched.len() - 1;
                    self.by_table.insert(table, i);
                    i
                }
            };
            self.watched[i].occs.push(occ);
        }
        Ok(())
    }

    /// Drops every occurrence of target `t` (table indexes stay valid).
    pub fn remove(&mut self, t: usize) {
        for w in self.watched.iter_mut() {
            w.occs.retain(|o| o.target != t);
        }
    }

    /// Whether some target reads `table`.
    pub fn watched(&self, table: &TableRef) -> bool {
        self.by_table
            .get(table)
            .is_some_and(|&i| !self.watched[i].occs.is_empty())
    }

    /// `table` as a TABLE_MAP with `columns` columns shows it (`None`: any shape): occurrences
    /// planned for another column count skip its rows.
    pub fn source(&self, table: &TableRef, columns: Option<usize>) -> Option<Source> {
        let idx = *self.by_table.get(table)?;
        let skip = self.watched[idx]
            .occs
            .iter()
            .enumerate()
            .filter(|(_, o)| columns.is_some_and(|n| o.table.columns.len() != n))
            .map(|(i, _)| i)
            .collect();
        Some(Source { idx, skip })
    }

    /// The targets with an occurrence that `s` skips, ascending, once each.
    pub fn mismatched(&self, s: &Source) -> Vec<usize> {
        let mut targets: Vec<usize> = s
            .skip
            .iter()
            .map(|&i| self.watched[s.idx].occs[i].target)
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
        _hops: &mut Hops,
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
            }
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
    let project = |r: &BinlogRow| -> Tuple {
        cols.iter()
            .map(|&c| Datum::from_binlog(value_at(r, c), &o.table.columns[c]))
            .collect()
    };
    let new = after.map(project);
    let old = before.map(project).filter(|k| new.as_ref() != Some(k));
    let same = before.is_some() && old.is_none();
    if let Some(k) = old {
        give(k, Op::Delete, sink);
    }
    if let Some(k) = new {
        give(k, if same { Op::Update } else { Op::Insert }, sink);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::testkit::{plan, row, Rec};

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
    fn hop_plans_are_refused_until_6j() {
        let p = plan(
            "SELECT i.id, l.child_id FROM items i LEFT JOIN item_links l ON l.parent_id = \
             i.kind_id",
        );
        let ap = AccessPlan::of(&p);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        assert!(
            matches!(ap.occurrences[1].path, Path::Hop(_)),
            "{:?}",
            ap.occurrences[1].path
        );
        let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
        let mut c = Consumer::default();
        let mut hops = Hops::new(1000);
        let err = c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap_err();
        assert!(err.contains("6J"), "{err}");
        assert!(!c.watched(&table_ref("items")), "all or nothing");
        assert!(!c.watched(&table_ref("item_links")), "all or nothing");
    }
}
