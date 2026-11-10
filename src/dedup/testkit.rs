//! Test helpers of the dedup core: a neutral catalog, row images and a recording sink.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::Value;

use crate::analyze::{parse_definition, ViewPlan};
use crate::catalog::{Column, TableInfo, TableRef};
use crate::datum::{Datum, Key as Tuple};
use crate::intset;

use super::consumer::Sink;
use super::key::{Item, Key, NoItem, Op, SpaceId};

/// A column of the declared type: `data_type` with a trailing `unsigned` moved into the
/// `column_type`, where [`crate::catalog::col_kind`] looks for it.
fn column(name: &str, ty: &str) -> Column {
    match ty.strip_suffix(" unsigned") {
        Some(dt) => Column::new(
            name,
            dt,
            ty,
            Some("utf8mb4"),
            Some("utf8mb4_general_ci"),
            None,
            false,
        ),
        None => Column::simple(name, ty),
    }
}

/// `db.<name>` with `cols` (name, declared type; `"bigint unsigned"` gives an unsigned column)
/// and primary key `pk`.
pub(crate) fn table(name: &str, cols: &[(&str, &str)], pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
    let t = TableInfo {
        schema: "db".into(),
        name: name.into(),
        columns: cols.iter().map(|(n, ty)| column(n, ty)).collect(),
        pk: pk.to_vec(),
        unique_keys: vec![],
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    };
    (("db".into(), name.into()), Arc::new(t))
}

/// items (id, kind_id, owner_id; PK id), kinds (id, name), owners (id, name), item_values (item_id,
/// key_id, scope_id, val, note; PK the first three), item_links (child_id, parent_id; PK both),
/// slugs (slug, kind_id; PK the VARCHAR slug), counters (id, kind_id; PK the UNSIGNED BIGINT id),
/// tags (code, kind_id; PK the SMALLINT code).
pub(crate) fn catalog() -> HashMap<TableRef, Arc<TableInfo>> {
    [
        table(
            "items",
            &[("id", "int"), ("kind_id", "int"), ("owner_id", "int")],
            &[0],
        ),
        table("kinds", &[("id", "int"), ("name", "varchar")], &[0]),
        table("owners", &[("id", "int"), ("name", "varchar")], &[0]),
        table(
            "item_values",
            &[
                ("item_id", "int"),
                ("key_id", "int"),
                ("scope_id", "int"),
                ("val", "int"),
                ("note", "int"),
            ],
            &[0, 1, 2],
        ),
        table(
            "item_links",
            &[("child_id", "int"), ("parent_id", "int")],
            &[0, 1],
        ),
        table("slugs", &[("slug", "varchar"), ("kind_id", "int")], &[0]),
        table(
            "counters",
            &[("id", "bigint unsigned"), ("kind_id", "int")],
            &[0],
        ),
        table("tags", &[("code", "smallint"), ("kind_id", "int")], &[0]),
    ]
    .into_iter()
    .collect()
}

/// The plan of `CREATE VIEW v AS <sql>` over `tables`, through the analyzer's front end.
pub(crate) fn plan_in(sql: &str, tables: &HashMap<TableRef, Arc<TableInfo>>) -> Arc<ViewPlan> {
    Arc::new(
        crate::analyzer::plan(
            &parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap(),
            "db",
            tables,
        )
        .unwrap(),
    )
}

/// [`plan_in`] over [`catalog`].
pub(crate) fn plan(sql: &str) -> Arc<ViewPlan> {
    plan_in(sql, &catalog())
}

/// A full image of integer columns (`None`: SQL NULL).
pub(crate) fn row(vals: &[Option<i64>]) -> BinlogRow {
    row_typed(
        &vals
            .iter()
            .map(|v| v.map_or(Cell::Null, Cell::Int))
            .collect::<Vec<_>>(),
    )
}

/// One cell of a row image: its value and the column type its binlog column declares.
pub(crate) enum Cell<'a> {
    Int(i64),
    /// An unsigned binlog value: its column carries `UNSIGNED_FLAG`, the way a wide UNSIGNED
    /// column's does.
    UInt(u64),
    /// A string column's value (`VARCHAR` / `VAR_STRING`).
    Text(&'a str),
    Null,
}

/// A full image whose columns are declared from the cells' types, the way a TABLE_MAP declares
/// the table's columns.
pub(crate) fn row_typed(cells: &[Cell<'_>]) -> BinlogRow {
    let cols: Arc<[mysql_async::Column]> = cells
        .iter()
        .map(|c| {
            let col = mysql_async::Column::new(match c {
                Cell::Int(_) | Cell::UInt(_) | Cell::Null => ColumnType::MYSQL_TYPE_LONGLONG,
                Cell::Text(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
            });
            match c {
                Cell::UInt(_) => col.with_flags(ColumnFlags::UNSIGNED_FLAG),
                _ => col,
            }
        })
        .collect();
    BinlogRow::new(
        cells
            .iter()
            .map(|c| {
                Some(BinlogValue::Value(match c {
                    Cell::Int(i) => Value::Int(*i),
                    Cell::UInt(u) => Value::UInt(*u),
                    Cell::Text(s) => Value::Bytes(s.as_bytes().to_vec()),
                    Cell::Null => Value::NULL,
                }))
            })
            .collect(),
        cols,
    )
}

/// Everything a consumer sent, in order.
#[derive(Debug, Default)]
pub(crate) struct Rec {
    /// Targets that are not open.
    pub closed: Vec<usize>,
    pub rows: Vec<(usize, bool)>,
    pub ids: Vec<(usize, SpaceId, Key, Op, bool)>,
    pub items: Vec<(usize, Item<NoItem>)>,
}

impl Rec {
    /// The integer ids sent to target `t` in space `s`, as i64, in order, with their ops.
    pub fn ints(&self, t: usize, s: SpaceId) -> Vec<(i64, Op)> {
        self.ids
            .iter()
            .filter(|(t2, s2, ..)| *t2 == t && *s2 == s)
            .filter_map(|(_, _, k, op, _)| match k {
                Key::Int(u) => Some((intset::to_i64(*u), *op)),
                Key::Tuple(_) => None,
            })
            .collect()
    }

    /// The tuple ids sent to target `t` in space `s`, as their datum parts, in order, with
    /// their ops. The counterpart of [`Rec::ints`]: neither view drops what the other sees.
    pub fn tuples(&self, t: usize, s: SpaceId) -> Vec<(Vec<Datum>, Op)> {
        self.ids
            .iter()
            .filter(|(t2, s2, ..)| *t2 == t && *s2 == s)
            .filter_map(|(_, _, k, op, _)| match k {
                Key::Int(_) => None,
                Key::Tuple(k) => Some((k.to_vec(), *op)),
            })
            .collect()
    }
}

impl Sink<NoItem> for Rec {
    fn open(&self, t: usize) -> bool {
        !self.closed.contains(&t)
    }

    fn row(&mut self, t: usize, skipped: bool) {
        self.rows.push((t, skipped));
    }

    fn int(&mut self, t: usize, s: SpaceId, v: u64, op: Op, cascaded: bool) {
        self.ids.push((t, s, Key::Int(v), op, cascaded));
    }

    fn tuple(&mut self, t: usize, s: SpaceId, k: Tuple, op: Op, cascaded: bool) {
        self.ids.push((t, s, Key::Tuple(k), op, cascaded));
    }

    fn item(&mut self, t: usize, i: Item<NoItem>) {
        self.items.push((t, i));
    }
}
