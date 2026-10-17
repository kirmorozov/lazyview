//! Materialized tables: layout (from a probe view on the source), shadow builds by streaming
//! non-locking reads, per-unit diff refreshes and atomic swaps.
//!
//! Sources are only ever read with plain SELECTs (never INSERT ... SELECT), so the service takes
//! no locks on source rows; all writes go to the target tables.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row, Value};

use crate::accumulate::UnitChunk;
use crate::analyze::{quote_ident as q, ViewPlan};
use crate::datum::{Datum, Key};
use crate::metrics::Metrics;

/// Maximum placeholders per statement (server limit is 65535).
const MAX_PARAMS: usize = 60_000;

#[derive(Debug, Clone, PartialEq)]
pub struct MvCol {
    pub name: String,
    pub column_type: String,
    pub data_type: String,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    /// Projection columns followed by hidden unit columns.
    pub cols: Vec<MvCol>,
    pub n_visible: usize,
    /// Unit indices materialized as hidden columns `_lv_u<i>`.
    pub hidden: Vec<usize>,
    /// For each unit expression, its column index in `cols`.
    pub unit_pos: Vec<usize>,
    pub primary: bool,
}

impl Layout {
    pub fn col_list(&self) -> String {
        self.cols.iter().map(|c| q(&c.name)).collect::<Vec<_>>().join(", ")
    }

    pub fn unit_cols(&self) -> Vec<String> {
        self.unit_pos.iter().map(|&p| q(&self.cols[p].name)).collect()
    }

    pub fn create_sql(&self, schema: &str, table: &str) -> String {
        let mut defs: Vec<String> = self
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut d = format!("{} {}", q(&c.name), c.column_type);
                if let (Some(cs), Some(co)) = (&c.charset, &c.collation) {
                    d.push_str(&format!(" CHARACTER SET {cs} COLLATE {co}"));
                }
                let in_pk = self.primary && self.unit_pos.contains(&i);
                d.push_str(if in_pk { " NOT NULL" } else { " NULL" });
                if i >= self.n_visible {
                    d.push_str(" INVISIBLE");
                }
                d
            })
            .collect();
        if self.primary {
            defs.push(format!("PRIMARY KEY ({})", self.unit_cols().join(", ")));
        } else if !self.unit_pos.is_empty() {
            let parts: Option<Vec<String>> = self
                .unit_pos
                .iter()
                .map(|&p| {
                    let c = &self.cols[p];
                    match c.data_type.as_str() {
                        "json" | "geometry" | "point" | "linestring" | "polygon" => None,
                        t if t.contains("text") || t.contains("blob") => Some(format!("{}(255)", q(&c.name))),
                        _ => Some(q(&c.name)),
                    }
                })
                .collect();
            if let Some(parts) = parts {
                defs.push(format!("KEY `_lv_unit` ({})", parts.join(", ")));
            }
        }
        format!(
            "CREATE TABLE {}.{} (\n  {}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            q(schema),
            q(table),
            defs.join(",\n  ")
        )
    }

    pub fn unit_key(&self, row: &[Datum]) -> Key {
        self.unit_pos.iter().map(|&p| row[p].clone()).collect()
    }
}

/// Name of the shadow table a new version is built into (≤ 64 chars).
pub fn shadow_name(name: &str) -> String {
    side_name(name, "__lv_new")
}

fn side_name(name: &str, suffix: &str) -> String {
    let n = format!("{name}{suffix}");
    if n.len() <= 64 {
        n
    } else {
        format!("_lv_{}{suffix}", crate::analyze::definition_hash(name))
    }
}

/// Session settings for every connection the service opens: non-locking reads.
pub async fn init_session(conn: &mut Conn) -> Result<()> {
    conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").await?;
    Ok(())
}

/// Derives the materialized layout by creating a temporary view on the source and reading its
/// column types from `information_schema`.
pub async fn probe(src: &mut Conn, src_schema: &str, plan: &ViewPlan) -> Result<Layout> {
    let hidden: Vec<usize> = (0..plan.unit_exprs.len())
        .filter(|&u| plan.unit_in_projection[u].is_none())
        .collect();
    let probe = format!("_lv_probe_{}", plan.hash);
    let select = plan.select_sql(&hidden, None);
    src.query_drop(format!("CREATE OR REPLACE VIEW {}.{} AS {select}", q(src_schema), q(&probe)))
        .await
        .with_context(|| format!("view SELECT is invalid: {select}"))?;
    let rows: Result<Vec<(String, String, String, String, Option<String>, Option<String>)>> = src
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(IS_NULLABLE AS CHAR), \
             CAST(CHARACTER_SET_NAME AS CHAR), CAST(COLLATION_NAME AS CHAR) \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (src_schema, &probe),
        )
        .await
        .map_err(Into::into);
    src.query_drop(format!("DROP VIEW IF EXISTS {}.{}", q(src_schema), q(&probe)))
        .await?;
    let cols: Vec<MvCol> = rows?
        .into_iter()
        .map(|(name, column_type, data_type, nullable, charset, collation)| MvCol {
            name,
            column_type,
            data_type: data_type.to_ascii_lowercase(),
            charset,
            collation,
            nullable: nullable.eq_ignore_ascii_case("YES"),
        })
        .collect();
    let n_visible = cols.len() - hidden.len();
    if n_visible == 0 {
        bail!("view has no columns");
    }
    let unit_pos: Vec<usize> = (0..plan.unit_exprs.len())
        .map(|u| match plan.unit_in_projection[u] {
            Some(p) => p,
            None => n_visible + hidden.iter().position(|&h| h == u).unwrap(),
        })
        .collect();
    let primary = plan.unit_unique
        && !unit_pos.is_empty()
        && unit_pos.iter().all(|&p| {
            let c = &cols[p];
            !c.nullable && !c.data_type.contains("text") && !c.data_type.contains("blob") && c.data_type != "json"
        });
    Ok(Layout { cols, n_visible, hidden, unit_pos, primary })
}

/// `BASE TABLE` / `VIEW` / None.
pub async fn table_kind(conn: &mut Conn, schema: &str, name: &str) -> Result<Option<String>> {
    Ok(conn
        .exec_first(
            "SELECT CAST(TABLE_TYPE AS CHAR) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?)
}

/// Visible + invisible column names and types of an existing table, to validate a layout on resume.
pub async fn table_columns(conn: &mut Conn, schema: &str, name: &str) -> Result<Vec<(String, String)>> {
    Ok(conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        .await?)
}

pub fn layout_matches(layout: &Layout, existing: &[(String, String)]) -> bool {
    layout.cols.len() == existing.len()
        && layout
            .cols
            .iter()
            .zip(existing)
            .all(|(c, (n, t))| c.name.eq_ignore_ascii_case(n) && c.column_type.eq_ignore_ascii_case(t))
}

pub async fn create_shadow(conn: &mut Conn, schema: &str, name: &str, layout: &Layout) -> Result<String> {
    let shadow = shadow_name(name);
    conn.query_drop(format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(&shadow))).await?;
    conn.query_drop(layout.create_sql(schema, &shadow)).await?;
    Ok(shadow)
}

/// Atomically replaces `name` with its shadow table (or renames the shadow when `name` is new).
pub async fn swap(conn: &mut Conn, schema: &str, name: &str) -> Result<()> {
    let shadow = shadow_name(name);
    let old = side_name(name, "__lv_old");
    let (s, n) = (q(schema), q(name));
    if table_kind(conn, schema, name).await?.is_some() {
        conn.query_drop(format!("DROP TABLE IF EXISTS {s}.{}", q(&old))).await?;
        conn.query_drop(format!(
            "RENAME TABLE {s}.{n} TO {s}.{}, {s}.{} TO {s}.{n}",
            q(&old),
            q(&shadow)
        ))
        .await?;
        conn.query_drop(format!("DROP TABLE IF EXISTS {s}.{}", q(&old))).await?;
    } else {
        conn.query_drop(format!("RENAME TABLE {s}.{} TO {s}.{n}", q(&shadow))).await?;
    }
    Ok(())
}

pub async fn drop_table(conn: &mut Conn, schema: &str, name: &str) -> Result<()> {
    conn.query_drop(format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(name))).await?;
    Ok(())
}

fn rows_per_insert(layout: &Layout, batch_rows: usize) -> usize {
    (MAX_PARAMS / layout.cols.len().max(1)).clamp(1, batch_rows.max(1))
}

/// Multi-row parameterized INSERTs.
pub async fn insert_rows<C: Queryable>(conn: &mut C, schema: &str, table: &str, layout: &Layout, rows: &[Vec<Value>], batch_rows: usize, metrics: &Metrics) -> Result<()> {
    let per = rows_per_insert(layout, batch_rows);
    let tuple = format!("({})", vec!["?"; layout.cols.len()].join(","));
    for chunk in rows.chunks(per) {
        let sql = format!(
            "INSERT INTO {}.{} ({}) VALUES {}",
            q(schema),
            q(table),
            layout.col_list(),
            vec![tuple.as_str(); chunk.len()].join(",")
        );
        let params: Vec<Value> = chunk.iter().flatten().cloned().collect();
        conn.exec_drop(sql, params).await?;
        Metrics::add(&metrics.mv_queries, 1);
        Metrics::add(&metrics.mv_rows_inserted, chunk.len() as u64);
    }
    Ok(())
}

pub struct BuildTarget<'a> {
    pub conn: &'a mut Conn,
    pub schema: String,
}

/// Fills each target's shadow table from one streaming, non-locking read of the source.
pub async fn fill(src: &mut Conn, plan: &ViewPlan, layout: &Layout, targets: &mut [BuildTarget<'_>], name: &str, batch_rows: usize, metrics: &Metrics) -> Result<u64> {
    let shadow = shadow_name(name);
    let sql = plan.select_sql(&layout.hidden, None);
    let per = rows_per_insert(layout, batch_rows);
    let mut res = src.exec_iter(sql, ()).await?;
    Metrics::add(&metrics.source_queries, 1);
    let mut buf: Vec<Vec<Value>> = Vec::with_capacity(per);
    let mut total = 0u64;
    let mut last_log = std::time::Instant::now();
    loop {
        let row: Option<Row> = res.next().await?;
        let done = row.is_none();
        if let Some(r) = row {
            buf.push(r.unwrap());
        }
        if buf.len() >= per || (done && !buf.is_empty()) {
            for t in targets.iter_mut() {
                insert_rows(t.conn, &t.schema, &shadow, layout, &buf, batch_rows, metrics).await?;
            }
            total += buf.len() as u64;
            buf.clear();
            if last_log.elapsed() >= std::time::Duration::from_secs(10) {
                tracing::info!(view = %name, rows = total, "building: rows copied so far");
                last_log = std::time::Instant::now();
            }
        }
        if done {
            break;
        }
    }
    Ok(total)
}

/// Current source rows for a chunk of units, grouped by unit (`None` = whole view).
pub async fn read_source(src: &mut Conn, plan: &ViewPlan, layout: &Layout, chunk: Option<&UnitChunk>, metrics: &Metrics) -> Result<Vec<Vec<Value>>> {
    let (sql, params) = match chunk {
        Some(c) => {
            let (f, p) = c.filter(&plan.unit_exprs);
            (plan.select_sql(&layout.hidden, Some(&f)), p)
        }
        None => (plan.select_sql(&layout.hidden, None), Vec::new()),
    };
    let rows: Vec<Row> = src.exec(sql, params).await?;
    Metrics::add(&metrics.source_queries, 1);
    Ok(rows.into_iter().map(Row::unwrap).collect())
}

fn to_datums(r: &[Value]) -> Vec<Datum> {
    r.iter().cloned().map(Datum::from_value).collect()
}

fn group(layout: &Layout, rows: Vec<(Vec<Datum>, Vec<Value>)>) -> HashMap<Key, Vec<(Vec<Datum>, Vec<Value>)>> {
    let mut m: HashMap<Key, Vec<_>> = HashMap::new();
    for r in rows {
        m.entry(layout.unit_key(&r.0)).or_default().push(r);
    }
    m
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Applied {
    pub units_changed: u64,
    pub deleted: u64,
    pub inserted: u64,
}

/// Brings `table` in line with `new_rows` for the units in `chunk` (all rows when `None`),
/// writing only units whose rows differ.
pub async fn apply(conn: &mut Conn, schema: &str, table: &str, layout: &Layout, chunk: Option<&UnitChunk>, new_rows: &[Vec<Value>], batch_rows: usize, metrics: &Metrics) -> Result<Applied> {
    let unit_cols = layout.unit_cols();
    let (filter, params) = match chunk {
        Some(c) => c.filter(&unit_cols),
        None => ("TRUE".to_string(), Vec::new()),
    };
    let old: Vec<Row> = conn
        .exec(
            format!("SELECT {} FROM {}.{} WHERE {filter}", layout.col_list(), q(schema), q(table)),
            params,
        )
        .await?;
    Metrics::add(&metrics.mv_queries, 1);
    let old = group(layout, old.into_iter().map(|r| { let v = r.unwrap(); (to_datums(&v), v) }).collect());
    let new = group(layout, new_rows.iter().map(|v| (to_datums(v), v.clone())).collect());

    let mut changed: Vec<Key> = Vec::new();
    for (k, rows) in &new {
        let same = old.get(k).map(|o| {
            let mut a: Vec<&Vec<Datum>> = o.iter().map(|r| &r.0).collect();
            let mut b: Vec<&Vec<Datum>> = rows.iter().map(|r| &r.0).collect();
            a.sort();
            b.sort();
            a == b
        });
        if same != Some(true) {
            changed.push(k.clone());
        }
    }
    for k in old.keys() {
        if !new.contains_key(k) {
            changed.push(k.clone());
        }
    }
    if changed.is_empty() {
        return Ok(Applied::default());
    }

    let mut out = Applied { units_changed: changed.len() as u64, ..Default::default() };
    let deleted_rows: u64 = changed.iter().map(|k| old.get(k).map(|v| v.len() as u64).unwrap_or(0)).sum();
    let inserts: Vec<Vec<Value>> = changed
        .iter()
        .filter_map(|k| new.get(k))
        .flatten()
        .map(|r| r.1.clone())
        .collect();
    let mut tx = conn.start_transaction(Default::default()).await?;
    if deleted_rows > 0 {
        let del_filter = if layout.unit_pos.is_empty() {
            ("TRUE".to_string(), Vec::new())
        } else {
            keys_chunk(changed.clone()).filter(&unit_cols)
        };
        tx.exec_drop(format!("DELETE FROM {}.{} WHERE {}", q(schema), q(table), del_filter.0), del_filter.1)
            .await?;
        Metrics::add(&metrics.mv_queries, 1);
        Metrics::add(&metrics.mv_rows_deleted, deleted_rows);
        out.deleted = deleted_rows;
    }
    insert_rows(&mut tx, schema, table, layout, &inserts, batch_rows, metrics).await?;
    tx.commit().await?;
    out.inserted = inserts.len() as u64;
    Metrics::add(&metrics.units_changed, out.units_changed);
    Ok(out)
}

/// Builds a chunk from arbitrary keys (sorted integers when possible).
pub fn keys_chunk(keys: Vec<Key>) -> UnitChunk {
    let ints: Option<Vec<i64>> = keys
        .iter()
        .map(|k| match &**k {
            [Datum::Int(i)] => Some(*i),
            _ => None,
        })
        .collect();
    match ints {
        Some(mut v) => {
            v.sort_unstable();
            v.dedup();
            UnitChunk::Ints(v)
        }
        None => UnitChunk::Keys(keys),
    }
}
