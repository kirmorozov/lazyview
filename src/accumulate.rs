//! Buffered, de-duplicated change sets per view (hot path: hash/bitmap inserts only).

use std::collections::HashSet;

use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use roaring::RoaringTreemap;

use crate::analyze::{AliasPlan, Mode, ViewPlan};
use crate::catalog::FkAction;
use crate::datum::{Datum, Key};

const SIGN: u64 = 1 << 63;

/// Unit keys. Single-column integer keys live in a roaring bitmap (order-preserving encoding of
/// i64), everything else in a hash set.
#[derive(Default, Debug, Clone)]
pub struct UnitSet {
    pub ints: RoaringTreemap,
    pub keys: HashSet<Key>,
}

impl UnitSet {
    pub fn insert(&mut self, key: Key) {
        if let [Datum::Int(i)] = &*key {
            self.ints.insert((*i as u64) ^ SIGN);
        } else {
            self.keys.insert(key);
        }
    }

    pub fn insert_int(&mut self, i: i64) {
        self.ints.insert((i as u64) ^ SIGN);
    }

    pub fn len(&self) -> usize {
        self.ints.len() as usize + self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ints.is_empty() && self.keys.is_empty()
    }

    pub fn extend(&mut self, other: UnitSet) {
        self.ints |= other.ints;
        self.keys.extend(other.keys);
    }

    pub fn contains(&self, key: &[Datum]) -> bool {
        match key {
            [Datum::Int(i)] => self.ints.contains((*i as u64) ^ SIGN),
            k => self.keys.contains(k),
        }
    }

    /// Splits into chunks of at most `n` keys: sorted integer chunks first, then other keys.
    pub fn chunks(&self, n: usize) -> Vec<UnitChunk> {
        let mut out = Vec::new();
        let mut cur = Vec::with_capacity(n.min(self.ints.len() as usize));
        for v in self.ints.iter() {
            cur.push((v ^ SIGN) as i64);
            if cur.len() == n {
                out.push(UnitChunk::Ints(std::mem::take(&mut cur)));
            }
        }
        if !cur.is_empty() {
            out.push(UnitChunk::Ints(cur));
        }
        let keys: Vec<Key> = self.keys.iter().cloned().collect();
        for c in keys.chunks(n) {
            out.push(UnitChunk::Keys(c.to_vec()));
        }
        out
    }
}

#[derive(Debug, Clone)]
pub enum UnitChunk {
    /// Sorted ascending.
    Ints(Vec<i64>),
    Keys(Vec<Key>),
}

impl UnitChunk {
    pub fn len(&self) -> usize {
        match self {
            UnitChunk::Ints(v) => v.len(),
            UnitChunk::Keys(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Predicate over `exprs` matching exactly the chunk's keys, plus its parameters.
    /// Integer runs become `BETWEEN` ranges and inline literals (no parameters).
    pub fn filter(&self, exprs: &[String]) -> (String, Vec<mysql_async::Value>) {
        match self {
            UnitChunk::Ints(v) => {
                let e = &exprs[0];
                let mut singles = Vec::new();
                let mut parts = Vec::new();
                let mut i = 0;
                while i < v.len() {
                    let mut j = i;
                    while j + 1 < v.len() && v[j + 1] == v[j] + 1 {
                        j += 1;
                    }
                    if j - i >= 3 {
                        parts.push(format!("{e} BETWEEN {} AND {}", v[i], v[j]));
                    } else {
                        singles.extend(v[i..=j].iter().map(|x| x.to_string()));
                    }
                    i = j + 1;
                }
                if !singles.is_empty() {
                    parts.push(format!("{e} IN ({})", singles.join(",")));
                }
                (or_join(parts), Vec::new())
            }
            UnitChunk::Keys(keys) => {
                let mut params = Vec::new();
                let mut parts = Vec::new();
                let (nulls, plain): (Vec<&Key>, Vec<&Key>) =
                    keys.iter().partition(|k| crate::datum::key_has_null(k));
                if !plain.is_empty() {
                    let tuple = |n: usize| {
                        if n == 1 {
                            "?".to_string()
                        } else {
                            format!("({})", vec!["?"; n].join(","))
                        }
                    };
                    let lhs = if exprs.len() == 1 {
                        exprs[0].clone()
                    } else {
                        format!("({})", exprs.join(", "))
                    };
                    let items: Vec<String> = plain.iter().map(|_| tuple(exprs.len())).collect();
                    parts.push(format!("{lhs} IN ({})", items.join(",")));
                    for k in &plain {
                        params.extend(k.iter().map(Datum::to_value));
                    }
                }
                for k in nulls {
                    let conj: Vec<String> = exprs.iter().map(|e| format!("{e} <=> ?")).collect();
                    parts.push(format!("({})", conj.join(" AND ")));
                    params.extend(k.iter().map(Datum::to_value));
                }
                (or_join(parts), params)
            }
        }
    }
}

fn or_join(parts: Vec<String>) -> String {
    match parts.len() {
        0 => "FALSE".to_string(),
        1 => parts.into_iter().next().unwrap(),
        _ => format!("({})", parts.join(" OR ")),
    }
}

/// Buffered changes for one view (one plan version).
#[derive(Default, Debug)]
pub struct ViewDirty {
    pub units: UnitSet,
    /// Per alias: row images (membership columns) whose units still need a lookup query.
    pub images: Vec<HashSet<Key>>,
    /// Full views: rebuild needed. Global views: recompute the single row.
    pub all: bool,
    /// Incremental views: affected units unknown (cascading FK); rebuild in the background.
    pub rebuild: bool,
}

impl ViewDirty {
    pub fn is_empty(&self) -> bool {
        !self.all
            && !self.rebuild
            && self.units.is_empty()
            && self.images.iter().all(HashSet::is_empty)
    }

    pub fn key_count(&self) -> usize {
        self.units.len() + self.images.iter().map(HashSet::len).sum::<usize>()
    }

    /// Adds everything buffered in `other` (e.g. to retry after a failed refresh).
    pub fn merge(&mut self, other: ViewDirty) {
        self.units.extend(other.units);
        if self.images.len() < other.images.len() {
            self.images.resize(other.images.len(), HashSet::new());
        }
        for (mine, theirs) in self.images.iter_mut().zip(other.images) {
            mine.extend(theirs);
        }
        self.all |= other.all;
        self.rebuild |= other.rebuild;
    }

    pub fn take(&mut self) -> ViewDirty {
        let n = self.images.len();
        let mut out = std::mem::take(self);
        self.images = vec![HashSet::new(); n];
        if out.images.len() < n {
            out.images.resize(n, HashSet::new());
        }
        out
    }
}

/// Outcome of feeding one row change into one view.
#[derive(Debug, PartialEq, Eq)]
pub enum Fed {
    Skipped,
    Recorded,
}

fn value_at(row: &BinlogRow, c: usize) -> Option<&BinlogValue<'_>> {
    row.as_ref(c)
}

fn changed(before: &BinlogRow, after: &BinlogRow, mask: &[bool]) -> bool {
    mask.iter().enumerate().any(|(c, &m)| {
        m && (value_at(before, c) != value_at(after, c)
            || matches!(value_at(after, c), Some(BinlogValue::JsonDiff(_))))
    })
}

fn project(row: &BinlogRow, a: &AliasPlan, cols: &[usize]) -> Key {
    cols.iter()
        .map(|&c| Datum::from_binlog(value_at(row, c), &a.table.columns[c]))
        .collect()
}

/// Records the effect of one row change of `alias` on `plan`.
pub fn feed(
    plan: &ViewPlan,
    alias: usize,
    dirty: &mut ViewDirty,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
) -> Fed {
    let a = &plan.aliases[alias];
    if let (Some(b), Some(af)) = (before, after) {
        if !changed(b, af, &a.referenced) {
            return Fed::Skipped;
        }
    }
    if !matches!(plan.mode, Mode::Spj | Mode::Grouped) {
        dirty.all = true;
        return Fed::Recorded;
    }
    if let Some(cols) = &a.direct_unit {
        for row in [before, after].into_iter().flatten() {
            dirty.units.insert(project(row, a, cols));
        }
        return Fed::Recorded;
    }
    if dirty.images.len() <= alias {
        dirty.images.resize(plan.aliases.len(), HashSet::new());
    }
    let membership_changed = match (before, after) {
        (Some(b), Some(af)) => changed(b, af, &a.membership),
        _ => true,
    };
    if let Some(af) = after {
        dirty.images[alias].insert(project(af, a, &a.image_cols));
    }
    if let Some(b) = before {
        if after.is_none() || membership_changed {
            dirty.images[alias].insert(project(b, a, &a.image_cols));
        }
    }
    Fed::Recorded
}

/// Records the effect of a parent-row change that the storage engine cascades into a child
/// table the view reads (cascade `ci` of the plan).
pub fn feed_cascade(
    plan: &ViewPlan,
    ci: usize,
    dirty: &mut ViewDirty,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
) -> Fed {
    let c = &plan.cascades[ci];
    let Some(b) = before else { return Fed::Skipped };
    let action = match after {
        None => c.on_delete,
        Some(a) => {
            let changed = c
                .parent_cols
                .iter()
                .any(|&p| value_at(b, p) != value_at(a, p));
            if !changed {
                return Fed::Skipped;
            }
            c.on_update
        }
    };
    if action == FkAction::Restrict {
        return Fed::Skipped;
    }
    if !matches!(plan.mode, Mode::Spj | Mode::Grouped) {
        dirty.all = true;
        return Fed::Recorded;
    }
    let Some(pcols) = &c.unit_from_parent else {
        dirty.rebuild = true;
        return Fed::Recorded;
    };
    let unit = |row: &BinlogRow| -> Key {
        pcols
            .iter()
            .map(|&p| Datum::from_binlog(value_at(row, p), &c.parent.columns[p]))
            .collect()
    };
    dirty.units.insert(unit(b));
    if let Some(a) = after {
        match action {
            FkAction::Cascade => dirty.units.insert(unit(a)),
            _ => dirty
                .units
                .insert(pcols.iter().map(|_| Datum::Null).collect()),
        }
    } else if action == FkAction::SetNull {
        dirty
            .units
            .insert(pcols.iter().map(|_| Datum::Null).collect());
    }
    Fed::Recorded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(v: &[i64]) -> UnitChunk {
        UnitChunk::Ints(v.to_vec())
    }

    #[test]
    fn roaring_dedup_and_order() {
        let mut s = UnitSet::default();
        for _ in 0..30 {
            s.insert(Box::new([Datum::Int(42)]));
        }
        s.insert(Box::new([Datum::Int(-5)]));
        s.insert(Box::new([Datum::Int(7)]));
        s.insert(Box::new([Datum::Bytes(b"x".to_vec())]));
        s.insert(Box::new([Datum::Bytes(b"x".to_vec())]));
        assert_eq!(s.len(), 4);
        let c = s.chunks(2);
        assert_eq!(c.len(), 3);
        assert!(matches!(&c[0], UnitChunk::Ints(v) if v == &vec![-5, 7]));
        assert!(matches!(&c[1], UnitChunk::Ints(v) if v == &vec![42]));
        assert!(s.contains(&[Datum::Int(-5)]));
    }

    #[test]
    fn int_filters_use_ranges() {
        let (f, p) = ints(&[1, 2, 3, 4, 5, 9, 11, 12]).filter(&["u".into()]);
        assert_eq!(f, "(u BETWEEN 1 AND 5 OR u IN (9,11,12))");
        assert!(p.is_empty());
        assert_eq!(ints(&[3]).filter(&["u".into()]).0, "u IN (3)");
    }

    #[test]
    fn key_filters_null_safe() {
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), Datum::Bytes(b"a".to_vec())]),
            Box::new([Datum::Null, Datum::Bytes(b"b".to_vec())]),
        ]);
        let (f, p) = k.filter(&["a".into(), "b".into()]);
        assert_eq!(f, "((a, b) IN ((?,?)) OR (a <=> ? AND b <=> ?))");
        assert_eq!(p.len(), 4);
    }
}
