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

/// Consumes a [`UnitSet`] chunk by chunk, materializing each chunk only when it is requested
/// (the chunk size may change from one chunk to the next). Order as in [`UnitSet::chunks`]:
/// sorted integer keys first, then the other keys.
pub struct ChunkCursor {
    ints: std::iter::Peekable<roaring::treemap::IntoIter>,
    keys: std::iter::Peekable<std::collections::hash_set::IntoIter<Key>>,
    left: usize,
}

impl ChunkCursor {
    pub fn new(units: UnitSet) -> ChunkCursor {
        let left = units.len();
        ChunkCursor {
            ints: units.ints.into_iter().peekable(),
            keys: units.keys.into_iter().peekable(),
            left,
        }
    }

    /// Units not yet handed out.
    pub fn remaining(&self) -> usize {
        self.left
    }

    /// The next chunk of at most `n` (≥ 1) keys; integer and other keys are never mixed.
    pub fn next_chunk(&mut self, n: usize) -> Option<UnitChunk> {
        let n = n.max(1);
        if self.ints.peek().is_some() {
            let v: Vec<i64> = self
                .ints
                .by_ref()
                .take(n)
                .map(|v| (v ^ SIGN) as i64)
                .collect();
            self.left -= v.len();
            return Some(UnitChunk::Ints(v));
        }
        if self.keys.peek().is_some() {
            let v: Vec<Key> = self.keys.by_ref().take(n).collect();
            self.left -= v.len();
            return Some(UnitChunk::Keys(v));
        }
        None
    }

    /// Everything not handed out yet, back as a set (carry-over of an interrupted run).
    pub fn into_rest(self) -> UnitSet {
        let mut s = UnitSet::default();
        for v in self.ints {
            s.ints.insert(v);
        }
        s.keys.extend(self.keys);
        s
    }
}

#[derive(Debug, Clone)]
pub enum UnitChunk {
    /// Sorted ascending.
    Ints(Vec<i64>),
    Keys(Vec<Key>),
}

/// How a unit column's keys must be bound in a filter (see [`UnitChunk::filter_typed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnitKind {
    /// Compared as the parameter is bound.
    #[default]
    Plain,
    /// TIMESTAMP: keys are local datetimes of the session's time zone.
    Timestamp,
    /// JSON (MySQL/Percona): keys are JSON texts, compared as JSON values.
    Json,
}

impl UnitChunk {
    /// Adds the chunk's keys to `set` (carry-over of a failed chunk).
    pub fn insert_into(&self, set: &mut UnitSet) {
        match self {
            UnitChunk::Ints(v) => {
                for &i in v {
                    set.insert_int(i);
                }
            }
            UnitChunk::Keys(v) => {
                for k in v {
                    set.insert(k.clone());
                }
            }
        }
    }

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
    /// Integer runs become `BETWEEN` ranges and inline literals (no parameters), NULL key
    /// components `IS NULL` literals. Every expression is parenthesized: a unit can be `NOT a`,
    /// `a OR b` or `a BETWEEN b AND c`.
    pub fn filter(&self, exprs: &[String]) -> (String, Vec<mysql_async::Value>) {
        self.filter_typed(exprs, &[])
    }

    /// [`Self::filter`] where `kinds[i]` (default [`UnitKind::Plain`]) tells how the keys of
    /// `exprs[i]` are bound:
    ///
    /// * [`UnitKind::Timestamp`]: the keys are local datetimes of the session's time zone,
    ///   matched by local time within an hour around the key: in a DST fall-back hour one local
    ///   time is two instants, and an index lookup of `ts = ?` finds only the first of them (a
    ///   scan compares local times and finds both).
    /// * [`UnitKind::Json`]: the keys are JSON texts, bound as `CAST(? AS JSON)`. MySQL groups
    ///   JSON-equal values (`{"a": 1}` with `{"a": 1.0}`) and compares a JSON column with a plain
    ///   string parameter as text, so a plain `doc IN (?)` never finds the group's rows. On
    ///   MariaDB JSON is LONGTEXT and never of this kind.
    pub fn filter_typed(
        &self,
        exprs: &[String],
        kinds: &[UnitKind],
    ) -> (String, Vec<mysql_async::Value>) {
        let exprs: Vec<String> = exprs.iter().map(|e| format!("({e})")).collect();
        let kind = |i: usize| kinds.get(i).copied().unwrap_or_default();
        let local = |i: usize, d: &Datum| {
            kind(i) == UnitKind::Timestamp
                && matches!(d, Datum::Date(y, m, ..) if *y != 0 || *m != 0)
        };
        // Parameter placeholder for a key of unit `i`.
        let param = |i: usize| match kind(i) {
            UnitKind::Json => "CAST(? AS JSON)",
            _ => "?",
        };
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
                // Keys with a NULL need a null-safe match. So do TIME keys: on MySQL an `IN` list
                // of two or more TIME(fsp > 0) parameters matches nothing.
                let (null_safe, plain): (Vec<&Key>, Vec<&Key>) = keys.iter().partition(|k| {
                    k.iter()
                        .enumerate()
                        .any(|(i, d)| matches!(d, Datum::Null | Datum::Time(..)) || local(i, d))
                });
                if !plain.is_empty() {
                    let holders: Vec<&str> = (0..exprs.len()).map(param).collect();
                    let tuple = if exprs.len() == 1 {
                        holders[0].to_string()
                    } else {
                        format!("({})", holders.join(","))
                    };
                    let lhs = if exprs.len() == 1 {
                        exprs[0].clone()
                    } else {
                        format!("({})", exprs.join(", "))
                    };
                    let items: Vec<String> = plain.iter().map(|_| tuple.clone()).collect();
                    parts.push(format!("{lhs} IN ({})", items.join(",")));
                    for k in &plain {
                        params.extend(k.iter().map(Datum::to_value));
                    }
                }
                for k in null_safe {
                    let mut conj = Vec::with_capacity(exprs.len());
                    for (i, (e, d)) in exprs.iter().zip(k.iter()).enumerate() {
                        if matches!(d, Datum::Null) {
                            // A literal, never `<=> ?` bound to NULL: MySQL-family servers run
                            // that prepared comparison on a NULLable UNIQUE index as a one-row
                            // lookup and miss the other NULL rows.
                            conj.push(format!("{e} IS NULL"));
                        } else if local(i, d) {
                            conj.push(format!(
                                "{e} BETWEEN ? - INTERVAL 1 HOUR AND ? + INTERVAL 1 HOUR \
                                 AND CAST({e} AS DATETIME(6)) = ?"
                            ));
                            params.extend([d.to_value(), d.to_value(), d.to_value()]);
                        } else {
                            conj.push(format!("{e} <=> {}", param(i)));
                            params.push(d.to_value());
                        }
                    }
                    parts.push(format!("({})", conj.join(" AND ")));
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
    /// An empty buffer for a plan with `aliases` aliases.
    pub fn for_aliases(aliases: usize) -> ViewDirty {
        ViewDirty {
            images: vec![HashSet::new(); aliases],
            ..Default::default()
        }
    }

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
    let new = after.map(|af| project(af, a, &a.image_cols));
    if let Some(b) = before {
        // Also when only the image changed: a lineage image is the primary key, and the
        // materialized rows still carry the old one.
        let old = project(b, a, &a.image_cols);
        if membership_changed || new.as_ref() != Some(&old) {
            dirty.images[alias].insert(old);
        }
    }
    if let Some(new) = new {
        dirty.images[alias].insert(new);
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

    /// A row image whose image columns change records its old image too, even when no
    /// membership column changed: with lineage the image is the primary key, and the old key is
    /// what finds the materialized rows (a key change followed by a delete of the new key).
    #[test]
    fn changed_images_keep_the_old_one() {
        use std::sync::Arc;

        use mysql_async::consts::ColumnType;
        use mysql_async::Value;

        use crate::analyze::{parse_definition, plan};
        use crate::catalog::{Column, TableInfo};

        let table = |name: &str, cols: &[(&str, &str)]| {
            let t = TableInfo {
                schema: "db".into(),
                name: name.into(),
                columns: cols.iter().map(|(n, ty)| Column::simple(n, ty)).collect(),
                pk: vec![0],
                unique_keys: vec![],
                cascades: vec![],
                is_view: false,
                view_bases: vec![],
            };
            (("db".to_string(), name.to_string()), Arc::new(t))
        };
        let tables: std::collections::HashMap<_, _> = [
            table(
                "r",
                &[("id", "int"), ("e", "varchar"), ("label", "varchar")],
            ),
            table("t", &[("id", "bigint"), ("e", "varchar"), ("v", "int")]),
        ]
        .into_iter()
        .collect();
        let p = plan(
            &parse_definition(
                "CREATE VIEW v AS SELECT r.id, r.label, t.id AS tid FROM r LEFT JOIN t ON t.e = r.e",
            )
            .unwrap(),
            "db",
            &tables,
        )
        .unwrap();
        let a = &p.aliases[1];
        assert!(a.direct_unit.is_none(), "t is resolved by lookups");
        let row = |id: i64, e: &str| {
            let cols: Arc<[mysql_async::Column]> = (0..3)
                .map(|_| mysql_async::Column::new(ColumnType::MYSQL_TYPE_LONG))
                .collect();
            BinlogRow::new(
                vec![
                    Some(BinlogValue::Value(Value::Int(id))),
                    Some(BinlogValue::Value(Value::Bytes(e.as_bytes().to_vec()))),
                    Some(BinlogValue::Value(Value::Int(1))),
                ],
                cols,
            )
        };
        let mut dirty = ViewDirty::for_aliases(p.aliases.len());
        let (before, after) = (row(-1, "x"), row(-3, "x"));
        assert_eq!(
            feed(&p, 1, &mut dirty, Some(&before), Some(&after)),
            Fed::Recorded
        );
        // Both images are buffered (one entry when they are equal).
        let images = |b: &BinlogRow, af: &BinlogRow| {
            [project(b, a, &a.image_cols), project(af, a, &a.image_cols)]
        };
        let [old, new] = images(&before, &after);
        assert!(dirty.images[1].contains(&old) && dirty.images[1].contains(&new));
        assert_eq!(dirty.images[1].len(), if old != new { 2 } else { 1 });
        // A membership column changes (the key stays): both images again.
        let mut dirty = ViewDirty::for_aliases(p.aliases.len());
        let moved = row(-1, "y");
        feed(&p, 1, &mut dirty, Some(&before), Some(&moved));
        let [old, new] = images(&before, &moved);
        assert!(dirty.images[1].contains(&old) && dirty.images[1].contains(&new));
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

    fn chunk_keys(c: &UnitChunk) -> Vec<Key> {
        match c {
            UnitChunk::Ints(v) => v.iter().map(|&i| Key::from([Datum::Int(i)])).collect(),
            UnitChunk::Keys(v) => v.clone(),
        }
    }

    /// Random unit sets chunk disjointly and completely for every chunk size sequence, and an
    /// interrupted cursor returns exactly the keys not handed out.
    #[test]
    fn cursor_chunks_disjoint_and_complete() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200 {
            let mut s = UnitSet::default();
            let n = (rnd() % 600) as usize;
            for _ in 0..n {
                match rnd() % 4 {
                    0 => s.insert(Box::new([Datum::Bytes(
                        format!("k{}", rnd() % 300).into_bytes(),
                    )])),
                    1 => s.insert(Box::new([Datum::Int((rnd() % 50) as i64), Datum::Null])),
                    _ => s.insert_int((rnd() % 1000) as i64 - 500),
                }
            }
            let all: HashSet<Key> = {
                let mut v = HashSet::new();
                for c in s.chunks(7) {
                    v.extend(chunk_keys(&c));
                }
                v
            };
            assert_eq!(all.len(), s.len());
            let total = s.len();
            let mut cur = ChunkCursor::new(s.clone());
            let mut seen: HashSet<Key> = HashSet::new();
            let stop_after = if round % 3 == 0 {
                Some((rnd() % 5) as usize)
            } else {
                None
            };
            let mut handed = 0;
            let mut i = 0;
            while let Some(c) = cur.next_chunk(1 + (rnd() % 40) as usize) {
                assert!(!c.is_empty());
                if let UnitChunk::Ints(v) = &c {
                    assert!(v.windows(2).all(|w| w[0] < w[1]), "sorted");
                }
                for k in chunk_keys(&c) {
                    assert!(seen.insert(k), "a unit is in exactly one chunk");
                }
                handed += c.len();
                assert_eq!(cur.remaining(), total - handed);
                i += 1;
                if stop_after == Some(i) {
                    break;
                }
            }
            let rest = cur.into_rest();
            for k in all.iter() {
                assert!(
                    seen.contains(k) != rest.contains(k),
                    "every unit is either handed out or left over, never both"
                );
            }
            assert_eq!(seen.len() + rest.len(), total);
            let mut back = UnitSet::default();
            for k in &seen {
                back.insert(k.clone());
            }
            back.extend(rest);
            assert_eq!(back.len(), total);
        }
    }

    #[test]
    fn int_filters_use_ranges() {
        let (f, p) = ints(&[1, 2, 3, 4, 5, 9, 11, 12]).filter(&["u".into()]);
        assert_eq!(f, "((u) BETWEEN 1 AND 5 OR (u) IN (9,11,12))");
        assert!(p.is_empty());
        assert_eq!(ints(&[3]).filter(&["u".into()]).0, "(u) IN (3)");
    }

    /// Unit expressions are operands: `NOT a IN (...)` would be `NOT (a IN (...))`, and
    /// `a AND b IN (...)` would be `a AND (b IN (...))`.
    #[test]
    fn filters_parenthesize_expressions() {
        let e = ["NOT a".to_string()];
        assert_eq!(ints(&[0, 1]).filter(&e).0, "(NOT a) IN (0,1)");
        assert_eq!(
            ints(&[1, 2, 3, 4]).filter(&["a OR b".into()]).0,
            "(a OR b) BETWEEN 1 AND 4"
        );
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), Datum::Bytes(b"x".to_vec())]),
            Box::new([Datum::Null, Datum::Bytes(b"y".to_vec())]),
        ]);
        let (f, _) = k.filter(&["a BETWEEN 1 AND 2".into(), "b AND c".into()]);
        assert_eq!(
            f,
            "(((a BETWEEN 1 AND 2), (b AND c)) IN ((?,?)) OR ((a BETWEEN 1 AND 2) IS NULL AND (b AND c) <=> ?))"
        );
    }

    /// TIME keys never go into an `IN` list (MySQL matches none of two or more TIME(fsp > 0)
    /// parameters there).
    #[test]
    fn time_keys_use_null_safe_equality() {
        let t = |s| Datum::Time(false, 0, 1, 2, s, 500_000);
        let k = UnitChunk::Keys(vec![
            Box::new([t(3)]),
            Box::new([t(4)]),
            Box::new([Datum::Bytes(b"z".to_vec())]),
        ]);
        let (f, p) = k.filter(&["tm".into()]);
        assert_eq!(f, "((tm) IN (?) OR ((tm) <=> ?) OR ((tm) <=> ?))");
        assert_eq!(p.len(), 3);
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), t(3)]),
            Box::new([Datum::Int(2), t(4)]),
        ]);
        let (f, p) = k.filter(&["a".into(), "tm".into()]);
        assert_eq!(
            f,
            "(((a) <=> ? AND (tm) <=> ?) OR ((a) <=> ? AND (tm) <=> ?))"
        );
        assert_eq!(p.len(), 4);
    }

    /// TIMESTAMP keys (local datetimes) match by local time within an hour of the key, so both
    /// instants of a DST fall-back hour are found with or without an index.
    #[test]
    fn timestamp_keys_match_local_time() {
        let dt = Datum::Date(2021, 10, 31, 2, 30, 0, 250_000);
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), dt.clone()]),
            Box::new([Datum::Int(2), Datum::Null]),
            Box::new([Datum::Int(3), Datum::Date(0, 0, 0, 0, 0, 0, 0)]),
        ]);
        let (f, p) = k.filter_typed(
            &["g".into(), "ts".into()],
            &[UnitKind::Plain, UnitKind::Timestamp],
        );
        assert_eq!(
            f,
            "(((g), (ts)) IN ((?,?)) OR ((g) <=> ? AND (ts) BETWEEN ? - INTERVAL 1 HOUR AND ? + INTERVAL 1 HOUR \
             AND CAST((ts) AS DATETIME(6)) = ?) OR ((g) <=> ? AND (ts) IS NULL))"
        );
        assert_eq!(p.len(), 2 + 4 + 1);
        assert_eq!(p[3], dt.to_value());
        // Without types (and for DATETIME columns) nothing changes.
        assert_eq!(
            k.filter(&["g".into(), "ts".into()]).0,
            "(((g), (ts)) IN ((?,?),(?,?)) OR ((g) <=> ? AND (ts) IS NULL))"
        );
    }

    #[test]
    fn key_filters_null_safe() {
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), Datum::Bytes(b"a".to_vec())]),
            Box::new([Datum::Null, Datum::Bytes(b"b".to_vec())]),
        ]);
        let (f, p) = k.filter(&["a".into(), "b".into()]);
        assert_eq!(f, "(((a), (b)) IN ((?,?)) OR ((a) IS NULL AND (b) <=> ?))");
        assert_eq!(p.len(), 3);
    }

    /// NULL components are `IS NULL` literals without a parameter (G6: a prepared `col <=> ?`
    /// bound to NULL finds one row of a NULLable UNIQUE index on MySQL-family servers); the
    /// parameters of the other components shift accordingly.
    #[test]
    fn null_components_are_literals() {
        let b = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
        let k = UnitChunk::Keys(vec![Box::new([Datum::Null])]);
        let (f, p) = k.filter(&["code".into()]);
        assert_eq!(f, "((code) IS NULL)");
        assert!(p.is_empty());
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Null, b("x")]),
            Box::new([Datum::Int(3), Datum::Null]),
            Box::new([Datum::Null, Datum::Null]),
            Box::new([Datum::Int(4), b("y")]),
        ]);
        let (f, p) = k.filter(&["a".into(), "b".into()]);
        assert_eq!(
            f,
            "(((a), (b)) IN ((?,?)) OR ((a) IS NULL AND (b) <=> ?) OR ((a) <=> ? AND (b) IS NULL) \
             OR ((a) IS NULL AND (b) IS NULL))"
        );
        assert_eq!(
            p,
            vec![
                Datum::Int(4).to_value(),
                b("y").to_value(),
                b("x").to_value(),
                Datum::Int(3).to_value()
            ]
        );
    }

    /// JSON keys are bound as JSON values (a plain string parameter compares as text and never
    /// finds a JSON-equal group), in the `IN` tuple as well as in the null-safe branch.
    #[test]
    fn json_keys_bind_as_json() {
        let j = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
        let k = UnitChunk::Keys(vec![
            Box::new([j(r#"{"a":1.0}"#)]),
            Box::new([j(r#"{"b":2}"#)]),
            Box::new([Datum::Null]),
        ]);
        let (f, p) = k.filter_typed(&["doc".into()], &[UnitKind::Json]);
        assert_eq!(
            f,
            "((doc) IN (CAST(? AS JSON),CAST(? AS JSON)) OR ((doc) IS NULL))"
        );
        assert_eq!(p.len(), 2);
        assert_eq!(p[0], j(r#"{"a":1.0}"#).to_value());
        // Composite units bind per position.
        let k = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), j("[1, 2]")]),
            Box::new([Datum::Int(2), j("[3]")]),
            Box::new([Datum::Int(3), Datum::Null]),
            Box::new([Datum::Null, j("4")]),
        ]);
        let (f, p) = k.filter_typed(
            &["g".into(), "doc".into()],
            &[UnitKind::Plain, UnitKind::Json],
        );
        assert_eq!(
            f,
            "(((g), (doc)) IN ((?,CAST(? AS JSON)),(?,CAST(? AS JSON))) \
             OR ((g) <=> ? AND (doc) IS NULL) OR ((g) IS NULL AND (doc) <=> CAST(? AS JSON)))"
        );
        assert_eq!(p.len(), 6);
        // Plain kinds (MariaDB, where JSON is LONGTEXT) are unchanged.
        assert_eq!(
            k.filter_typed(
                &["g".into(), "doc".into()],
                &[UnitKind::Plain, UnitKind::Plain]
            )
            .0,
            k.filter(&["g".into(), "doc".into()]).0
        );
    }
}
