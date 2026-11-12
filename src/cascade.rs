//! The in-memory cascade cache: the child rows of the cascades a view cannot follow from the
//! parent's row alone (`CascadeRoute::map`), kept in memory, IO-free (no table).
//!
//! MySQL and MariaDB do not log the child rows an `ON DELETE` / `ON UPDATE` `CASCADE` or `SET
//! NULL` changes. For such a route the cache keeps every child row the view can read (its key,
//! its FK value and the columns the child's routes take values from), maintained from the
//! child's own row events and seeded from the build's read of the source. A parent delete or key
//! update then takes the rows under the parent's old key and the engine replays each of them
//! through every route of the child table, as the change the storage engine made; a server that
//! logs the cascaded rows itself only marks the same ids twice.
//!
//! One cache per version, in the dirty buffer. Contents: [`KeptRow`], [`kept`], [`kept_image`],
//! [`seed_sql`], [`seed_row`] (the caller runs the query) and [`Cache`] (routes grown on use, the
//! seeding log, the restart gap and its once-a-minute warning).
//!
//! Cost: per row event of a cached child table (filter-passing rows), one hash update per route
//! and version (during a build: one log entry); per parent delete / key update, one hash lookup
//! plus the synthesized child events; per flush: nothing (no map read or write, no housekeeping).

use std::collections::{HashMap, HashSet};

use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;

use crate::analyze::{int_sql, quote_ident, CascadeRoute};
use crate::binlog::Position;
use crate::catalog::{ColKind, Column};
use crate::datum::{Datum, Key};

/// A child row as a cascade map keeps it: its FK value and, per stored column (`MapPlan::cols`),
/// the datum its image carried (`None`: not in the image). `complete`: every stored column was
/// in the image (the host's change feed decides it).
#[derive(Debug, Clone, PartialEq)]
pub struct KeptRow {
    pub fk: Key,
    pub values: Vec<Option<Datum>>,
    pub complete: bool,
}

/// Whether `c`'s map keeps the child row `r` of its binlog event, and if so as what: its
/// key, its FK value and the stored columns. `None`: the filters drop it or its FK is null.
pub fn kept(c: &CascadeRoute, r: &BinlogRow) -> Option<(Key, KeptRow)> {
    let m = c.map.as_ref()?;
    let t = &c.child_table;
    let datum = |r: &BinlogRow, k: usize| Datum::from_binlog(r.as_ref(k), &t.columns[k]);
    if !m.keeps(r, t) {
        return None;
    }
    let fk: Key = c.child_cols.iter().map(|&k| datum(r, k)).collect();
    if fk.iter().any(Datum::is_null) {
        return None;
    }
    let mut complete = r.len() == t.columns.len();
    let values = m
        .cols
        .iter()
        .map(|&k| match r.as_ref(k) {
            None | Some(BinlogValue::JsonDiff(_)) => {
                complete = false;
                None
            }
            v => Some(Datum::from_binlog(v, &t.columns[k])),
        })
        .collect();
    let key = m.key.iter().map(|&k| datum(r, k)).collect();
    Some((
        key,
        KeptRow {
            fk,
            values,
            complete,
        },
    ))
}

/// The child row `row` of cascade `c`'s map as a binlog image of the child table: its stored
/// columns, every other column absent. With `fk`: the same row after the cascade set its FK.
pub fn kept_image(c: &CascadeRoute, row: &KeptRow, fk: Option<&Key>) -> BinlogRow {
    use mysql_async::consts::ColumnType;
    let (t, m) = (&c.child_table, c.map.as_ref().expect("a mapped cascade"));
    let mut values: Vec<Option<BinlogValue<'static>>> = vec![None; t.columns.len()];
    for (&k, v) in m.cols.iter().zip(&row.values) {
        if let Some(d) = v {
            values[k] = Some(BinlogValue::Value(d.to_binlog(&t.columns[k])));
        }
    }
    if let Some(fk) = fk {
        for (&k, d) in c.child_cols.iter().zip(fk.iter()) {
            values[k] = Some(BinlogValue::Value(d.to_binlog(&t.columns[k])));
        }
    }
    let columns: Vec<mysql_async::Column> = t
        .columns
        .iter()
        .map(|_| mysql_async::Column::new(ColumnType::MYSQL_TYPE_NULL))
        .collect();
    BinlogRow::new(values, std::sync::Arc::from(columns))
}

/// A select expression for stored column `col` whose value [`seed_datum`] turns into the datum
/// a binlog image of it gives (`Datum::from_binlog`): TIMESTAMP as epoch seconds, BIT as an
/// unsigned integer; ENUM / SET labels, JSON text and the rest as read.
fn seed_expr(col: &Column) -> String {
    let q = quote_ident(&col.name);
    match col.kind {
        ColKind::Timestamp => format!("UNIX_TIMESTAMP({q})"),
        ColKind::Bit(_) => format!("CAST({q} AS UNSIGNED)"),
        _ => q,
    }
}

fn seed_datum(v: Value, col: &Column) -> Datum {
    match (&col.kind, v) {
        (_, Value::NULL) => Datum::Null,
        (ColKind::Timestamp, Value::Int(i)) => Datum::Bytes(i.to_string().into_bytes()),
        (ColKind::Timestamp, Value::UInt(u)) => Datum::Bytes(u.to_string().into_bytes()),
        (_, v) => Datum::from_value(v),
    }
}

/// The seed query for `c`'s map: the child key, the FK columns and the stored columns
/// (`seed_expr` form), restricted to the rows the filters keep. `None`: no map.
pub fn seed_sql(c: &CascadeRoute) -> Option<String> {
    let m = c.map.as_ref()?;
    let t = &c.child_table;
    let mut select: Vec<String> = m
        .key
        .iter()
        .map(|&k| quote_ident(&t.columns[k].name))
        .collect();
    select.extend(
        c.child_cols
            .iter()
            .map(|&k| quote_ident(&t.columns[k].name)),
    );
    select.extend(m.cols.iter().map(|&k| seed_expr(&t.columns[k])));
    let mut conds: Vec<String> = c
        .child_cols
        .iter()
        .map(|&k| format!("{} IS NOT NULL", quote_ident(&t.columns[k].name)))
        .collect();
    if !m.filters.is_empty() && m.filters.iter().all(|f| !f.is_empty()) {
        let any: Vec<String> = m
            .filters
            .iter()
            .map(|fs| {
                let all: Vec<String> = fs
                    .iter()
                    .map(|f| {
                        let vals: Vec<String> = f.values.iter().map(int_sql).collect();
                        format!(
                            "{} IN ({})",
                            quote_ident(&t.columns[f.col].name),
                            vals.join(", ")
                        )
                    })
                    .collect();
                format!("({})", all.join(" AND "))
            })
            .collect();
        conds.push(format!("({})", any.join(" OR ")));
    }
    Some(format!(
        "SELECT {} FROM {}.{} WHERE {}",
        select.join(", "),
        quote_ident(&t.schema),
        quote_ident(&t.name),
        conds.join(" AND ")
    ))
}

/// One seed row: `vals` in the `seed_sql` order (child key, FK columns, stored columns)
/// as the child key and the kept row (complete: every stored column was in the image).
pub fn seed_row(c: &CascadeRoute, vals: Vec<Value>) -> (Key, KeptRow) {
    let m = c.map.as_ref().expect("seed_sql answered");
    let t = &c.child_table;
    let (nk, nf) = (m.key.len(), c.child_cols.len());
    let key: Key = m
        .key
        .iter()
        .zip(&vals[..nk])
        .map(|(&k, v)| seed_datum(v.clone(), &t.columns[k]))
        .collect();
    let fk: Key = c
        .child_cols
        .iter()
        .zip(&vals[nk..nk + nf])
        .map(|(&k, v)| seed_datum(v.clone(), &t.columns[k]))
        .collect();
    let values = m
        .cols
        .iter()
        .zip(&vals[nk + nf..])
        .map(|(&k, v)| Some(seed_datum(v.clone(), &t.columns[k])))
        .collect();
    (
        key,
        KeptRow {
            fk,
            values,
            complete: true,
        },
    )
}

/// One cached cascade route: the kept child rows, by child key and by FK value.
#[derive(Debug, Default)]
pub struct Route {
    pub rows: HashMap<Key, KeptRow>,
    pub by_fk: HashMap<Key, HashSet<Key>>,
}

/// A child or cached-parent change logged while a build's seed is outstanding.
#[derive(Debug)]
pub enum Logged {
    Child {
        route: usize,
        old: Option<Key>,
        new: Option<(Key, KeptRow)>,
    },
    Parent {
        route: usize,
        old: Key,
        new: Option<Key>,
    },
}

/// The in-memory cascade cache of one version: routes by plan cascade index, grown on use.
#[derive(Debug, Default)]
pub struct Cache {
    pub routes: Vec<Route>,
    log: Option<Vec<Logged>>,
    pub gap_until: Option<Position>,
    pub gap: bool,
    pub warned_at: u64,
}

impl Cache {
    fn ensure_route(&mut self, ci: usize) {
        (self.routes.len()..=ci).for_each(|_| self.routes.push(Route::default()));
    }

    /// Keeps `row` under `key` of route `ci`, growing `routes` on first use; a kept key whose
    /// FK moved (feed_map passes old = None) leaves no stale entry in `by_fk`.
    pub fn put(&mut self, ci: usize, key: Key, row: KeptRow) {
        self.ensure_route(ci);
        let route = &mut self.routes[ci];
        let moved = route
            .rows
            .get(&key)
            .filter(|p| p.fk != row.fk)
            .map(|p| p.fk.clone());
        if let Some(old_fk) = moved {
            if let Some(bucket) = route.by_fk.get_mut(&old_fk) {
                bucket.remove(&key);
                if bucket.is_empty() {
                    route.by_fk.remove(&old_fk);
                }
            }
        }
        route
            .by_fk
            .entry(row.fk.clone())
            .or_default()
            .insert(key.clone());
        route.rows.insert(key, row);
    }
}
impl Cache {
    /// A child row change of route `ci` by child key: when ready it removes `old`, puts
    /// `new` and keeps `by_fk` in step; while seeding it logs.
    pub fn child(&mut self, ci: usize, old: Option<Key>, new: Option<(Key, KeptRow)>) {
        if let Some(log) = self.log.as_mut() {
            log.push(Logged::Child {
                route: ci,
                old,
                new,
            });
            return;
        }
        if let Some(old) = old {
            self.remove_row(ci, &old);
        }
        if let Some((key, row)) = new {
            self.put(ci, key, row);
        }
    }

    /// Removes `key` of route `ci` and its `by_fk` entry; a no-op when it is not kept.
    fn remove_row(&mut self, ci: usize, key: &Key) {
        if let Some(route) = self.routes.get_mut(ci) {
            if let Some(row) = route.rows.remove(key) {
                let fk = row.fk;
                if let Some(set) = route.by_fk.get_mut(&fk) {
                    set.remove(key);
                }
                if route.by_fk.get(&fk).is_some_and(HashSet::is_empty) {
                    route.by_fk.remove(&fk);
                }
            }
        }
    }

    /// Route `ci`'s kept rows and FK buckets (none: `ci` was never grown).
    pub fn rows(&self, ci: usize) -> Option<&Route> {
        self.routes.get(ci)
    }

    /// Test-only: the kept rows of route `ci` under FK value `fk` (empty when `ci` was
    /// never grown or nothing is kept under `fk`).
    #[cfg(test)]
    pub fn under(&self, ci: usize, fk: &Key) -> Vec<&KeptRow> {
        let Some(route) = self.routes.get(ci) else {
            return Vec::new();
        };
        route.rows.values().filter(|r| r.fk == *fk).collect()
    }
}
impl Cache {
    /// The cache of the Next version of a build: ready, empty, logging until `install`.
    pub fn seeding() -> Self {
        Self {
            routes: Vec::new(),
            log: Some(Vec::new()),
            gap_until: None,
            gap: false,
            warned_at: 0,
        }
    }

    /// Whether this is a build's cache whose seed is still outstanding (changes are logged).
    pub fn is_seeding(&self) -> bool {
        self.log.is_some()
    }

    /// The engine's action on route `ci`'s children under `old`: delete / SET NULL drops
    /// them, a CASCADE update re-puts them under `new`; their old images come back (empty
    /// when none is kept under `old`, or `ci` was never grown), or `None` while seeding (the
    /// change is logged for `drain`).
    pub fn parent(&mut self, ci: usize, old: &Key, new: Option<&Key>) -> Option<Vec<KeptRow>> {
        if let Some(log) = self.log.as_mut() {
            log.push(Logged::Parent {
                route: ci,
                old: old.clone(),
                new: new.cloned(),
            });
            return None;
        }
        let Some(route) = self.routes.get_mut(ci) else {
            return Some(Vec::new());
        };
        let keys: Vec<Key> = route
            .by_fk
            .remove(old)
            .map(|s| s.into_iter().collect())
            .unwrap_or_default();
        let mut old_images = Vec::with_capacity(keys.len());
        for key in &keys {
            if let Some(mut row) = route.rows.remove(key) {
                old_images.push(row.clone());
                if let Some(new) = new {
                    row.fk = new.clone();
                    route.rows.insert(key.clone(), row);
                    route
                        .by_fk
                        .entry(new.clone())
                        .or_default()
                        .insert(key.clone());
                }
            }
        }
        Some(old_images)
    }
}
impl Cache {
    /// Installs the build's seeded cache: re-puts each kept row so `rows` and `by_fk` stay in
    /// step, then ends the seeding log and returns it in binlog order to drain (child to apply,
    /// parent to expand).
    pub fn install(&mut self, seeded: Cache) -> Vec<Logged> {
        for (ci, route) in seeded.routes.into_iter().enumerate() {
            for (key, row) in route.rows {
                self.put(ci, key, row);
            }
        }
        self.log.take().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::{ConstFilter, MapPlan};
    use crate::catalog::{FkAction, TableInfo};
    use std::sync::Arc;

    /// A table in schema `db` with the given columns and `pk` over column 0.
    fn table(name: &str, cols: Vec<Column>) -> Arc<TableInfo> {
        Arc::new(TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols,
            pk: vec![0],
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        })
    }

    /// A mapped route on `items`: key `id`, FKs `p1`/`p2`, stored `ts` (timestamp) and `flag`
    /// (bit(4)), the given per-alias constant filters.
    fn route(filters: Vec<Vec<ConstFilter>>) -> CascadeRoute {
        CascadeRoute {
            parent: table("parents", vec![Column::simple("id", "int")]),
            parent_cols: vec![0],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
            unit_from_parent: None,
            child: ("db".into(), "items".into()),
            child_cols: vec![1, 2],
            traced: false,
            untraced: None,
            derived: false,
            partial_from_parent: vec![],
            pinned: vec![],
            reach: None,
            child_table: table(
                "items",
                vec![
                    Column::simple("id", "int"),
                    Column::simple("p1", "int"),
                    Column::simple("p2", "int"),
                    Column::simple("ts", "timestamp"),
                    Column::new("flag", "bit", "bit(4)", None, None, None, false),
                ],
            ),
            map: Some(MapPlan {
                key: vec![0],
                cols: vec![3, 4],
                filters,
            }),
        }
    }

    #[test]
    fn cache_follows_child_rows_and_cascades() {
        let row = |fk: u64| KeptRow {
            fk: vec![Datum::UInt(fk)].into(),
            values: vec![],
            complete: true,
        };
        let key = |i: u64| vec![Datum::UInt(i)].into();
        let mut c = Cache::default();
        for i in 0..3 {
            c.put(0, key(i), row(1));
        }
        c.child(0, Some(key(1)), Some((key(1), row(2))));
        c.child(0, Some(key(2)), None);
        let got = c.parent(0, &key(1), None).unwrap();
        assert_eq!(got, vec![row(1)]);
        assert!(!c.rows(0).unwrap().by_fk.contains_key(&key(1)));
        assert!(!c.rows(0).unwrap().rows.contains_key(&key(0)));
        let got = c.parent(0, &key(2), Some(&key(3))).unwrap();
        assert_eq!(got, vec![row(2)]);
        assert_eq!(c.rows(0).unwrap().rows.get(&key(1)).unwrap().fk, key(3));
        // A route never grown has nothing under any key; only a seeding cache answers None.
        assert_eq!(c.parent(7, &key(1), None), Some(Vec::new()));
        let mut s = Cache::seeding();
        s.child(0, Some(key(4)), Some((key(4), row(4))));
        s.parent(0, &key(5), None);
        let log = s.install(Cache::default());
        assert_eq!(log.len(), 2);
        assert!(matches!(&log[0], Logged::Child { .. }));
        assert!(matches!(&log[1], Logged::Parent { .. }));
    }

    #[test]
    fn same_key_fk_move_leaves_no_stale_bucket() {
        let row = |fk: u64| KeptRow {
            fk: vec![Datum::UInt(fk)].into(),
            values: vec![],
            complete: true,
        };
        let key = |i: u64| vec![Datum::UInt(i)].into();
        let mut c = Cache::default();
        c.put(0, key(1), row(1));
        c.child(0, None, Some((key(1), row(2))));
        assert!(!c.rows(0).unwrap().by_fk.contains_key(&key(1)));
        assert!(c.parent(0, &key(1), None).unwrap().is_empty());
        assert_eq!(c.parent(0, &key(2), None), Some(vec![row(2)]));
    }

    #[test]
    fn seed_expr_pins_the_type_wrappers() {
        // TIMESTAMP and BIT carry different binlog representations; the seed must wrap them.
        assert_eq!(
            seed_expr(&Column::simple("ts", "timestamp")),
            "UNIX_TIMESTAMP(`ts`)"
        );
        assert_eq!(
            seed_expr(&Column::new(
                "flag", "bit", "bit(4)", None, None, None, false
            )),
            "CAST(`flag` AS UNSIGNED)"
        );
        // Every other kind is selected as is.
        assert_eq!(seed_expr(&Column::simple("p1", "int")), "`p1`");
    }

    #[test]
    fn seed_sql_pins_the_exact_query() {
        // No alias filters: only the FK IS NOT NULL conditions.
        let c = route(vec![]);
        assert_eq!(
            seed_sql(&c),
            Some(
                "SELECT `id`, `p1`, `p2`, UNIX_TIMESTAMP(`ts`), CAST(`flag` AS UNSIGNED) \
                 FROM `db`.`items` WHERE `p1` IS NOT NULL AND `p2` IS NOT NULL"
                    .into()
            )
        );
        // Two alias groups: an OR of AND-ed IN lists over the filter columns.
        let c = route(vec![
            vec![ConstFilter {
                col: 3,
                values: vec![Datum::Int(2), Datum::UInt(7)],
            }],
            vec![ConstFilter {
                col: 1,
                values: vec![Datum::Int(5)],
            }],
        ]);
        assert_eq!(
            seed_sql(&c).unwrap(),
            "SELECT `id`, `p1`, `p2`, UNIX_TIMESTAMP(`ts`), CAST(`flag` AS UNSIGNED) \
             FROM `db`.`items` WHERE `p1` IS NOT NULL AND `p2` IS NOT NULL AND \
             ((`ts` IN (2, 7)) OR (`p1` IN (5)))"
        );
        // An unmapped route has no seed query.
        let mut c = route(vec![]);
        c.map = None;
        assert_eq!(seed_sql(&c), None);
    }

    #[test]
    fn seed_row_splits_the_fetched_row() {
        // vals in the seed_sql order: key `id`, FKs `p1`/`p2`, stored `ts`/`flag`.
        let c = route(vec![]);
        let vals = vec![
            Value::Int(10),
            Value::Int(100),
            Value::Int(200),
            Value::Int(1_234_567),
            Value::UInt(u64::MAX),
        ];
        let (key, row) = seed_row(&c, vals);
        assert_eq!(key, vec![Datum::Int(10)].into());
        assert_eq!(row.fk, vec![Datum::Int(100), Datum::Int(200)].into());
        // The timestamp comes back as epoch-second text; the bit as its integer value.
        assert_eq!(
            row.values,
            vec![
                Some(Datum::Bytes(b"1234567".to_vec())),
                Some(Datum::UInt(u64::MAX))
            ]
        );
        assert!(row.complete);
    }
}
