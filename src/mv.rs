//! Materialized tables: layout (from a probe view on the source), shadow builds by streaming
//! non-locking reads, per-unit diff refreshes and atomic swaps.
//!
//! Sources are only ever read with plain SELECTs (never INSERT ... SELECT), so the service takes
//! no locks on source rows; all writes go to the target tables.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{BinaryProtocol, Conn, QueryResult, Row, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::analyze::{quote_ident as q, Mode, ViewPlan};
use crate::datum::{Datum, Key};
use crate::metrics::Metrics;
use crate::target::Kind;

// The JSON canonical form is the dialect's answer (A4); the forwarder keeps the old path (the ADDED
// rule for the files in flight), `crate::dialect::mysql` is the owner.
pub use crate::dialect::mysql::json_canonical;

/// Maximum placeholders per statement (server limit is 65535). `Kind::Postgres` has a hard
/// ceiling of 65535; this cap is below it for both families.
pub(crate) const MAX_PARAMS: usize = 60_000;
/// Name of the unique index over the row identity (see [`Layout::row_key`]).
pub(crate) const ROW_INDEX: &str = "_lv_row";
/// Estimated bytes per write statement unless configured (`LAZYVIEW_WRITE_BATCH_BYTES`).
pub const DEFAULT_WRITE_BATCH_BYTES: usize = 4 << 20;

#[derive(Debug, Clone, PartialEq)]
pub struct MvCol {
    pub name: String,
    /// Name in the source SELECT (differs from `name` with a view column list).
    pub src_name: String,
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
    /// Lineage keys, in the order of the plan's: per key, the positions in `cols` of its hidden
    /// `_lv_a<alias>_pk<j>` columns (after the hidden unit columns).
    pub lineage: Vec<Vec<usize>>,
    /// TIMESTAMP columns as `(position in cols, SQL expression)`, read from the source as
    /// `UNIX_TIMESTAMP(<expression>)` and written in a `+00:00` session (see
    /// [`Layout::source_sql`]). Empty when the view has none or one cannot be read that way.
    pub epoch: Vec<(usize, String)>,
    /// Columns no two rows agree on (positions in `cols`): the unit columns of views with at most
    /// one row per unit, followed by the lineage columns for select/join views whose rows they
    /// identify (see [`ViewPlan::rows_identified`]). Empty when rows have no identity. Rows are
    /// upserted by it when it can be a unique index (see [`Layout::row_key`]).
    pub identity: Vec<usize>,
    /// Sets of unit positions refreshed by partial unit keys (the plan's `partial.sets`): each
    /// gets an index unless the unit index starts with it.
    pub partial: Vec<Vec<usize>>,
    /// Key bytes an index of this view's tables may use: the tightest of its targets' limits
    /// (`crate::target::min_key_budget`, T3.1), which is what every index of the view is sized
    /// with. `crate::engine` sets it when the layout is probed for a build; a layout that never
    /// saw a target (a probe on the source, a test) carries the MySQL family's 3072, which is
    /// what every build used before PostgreSQL targets existed.
    pub key_budget: u64,
}

impl Layout {
    pub fn col_list(&self) -> String {
        self.cols
            .iter()
            .map(|c| crate::dialect::mysql::quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn unit_cols(&self) -> Vec<String> {
        self.unit_pos
            .iter()
            .map(|&p| crate::dialect::mysql::quote_ident(&self.cols[p].name))
            .collect()
    }

    pub fn create_sql(&self, kind: Kind, schema: &str, table: &str) -> String {
        self.create_sql_marked(kind, schema, table, None)
    }

    /// The table DDL of a fresh build for the target family `kind`, with an optional ownership
    /// marker as the table comment (see [`crate::target::marker`]). The family is the target's, never decided
    /// here: the engine passes its target's kind (S4's staged seam, closed by T3).
    pub fn create_sql_marked(
        &self,
        kind: Kind,
        schema: &str,
        table: &str,
        marker: Option<&str>,
    ) -> String {
        crate::target::create_table_sql(kind, self, schema, table, marker)
    }

    /// The row key changed rows are upserted by (positions in `cols`; see [`Layout::identity`]),
    /// when the table has a unique index over it. None: changed units are deleted and their
    /// rows inserted again.
    pub fn row_key(&self) -> Option<&[usize]> {
        self.row_key_for(self.key_kind())
    }

    /// As [`Layout::row_key`], for the table or the write of the family `dialect`: its key parts
    /// are quoted there, while the widths stay the view's (one row key for all its targets).
    pub fn row_key_for(&self, dialect: Kind) -> Option<&[usize]> {
        crate::target::row_index(self, &crate::target::KeyPlan::for_layout(self, dialect))
            .map(|_| self.identity.as_slice())
    }

    /// The family a view-level index decision is made for: the most restrictive of its targets,
    /// read back from the key budget (see [`crate::target::Kind::from_key_budget`]).
    fn key_kind(&self) -> Kind {
        Kind::from_key_budget(self.key_budget)
    }

    /// How the values of `cols` are bound in a filter of a write (see [`write_diff`]): JSON as
    /// JSON values, TIMESTAMPs read in the session's time zone by local time (in a `+00:00`
    /// write session they are exact UTC datetimes).
    pub(crate) fn write_kinds(&self, cols: &[usize]) -> Vec<UnitKind> {
        cols.iter()
            .map(|&p| match self.cols[p].data_type.as_str() {
                "timestamp" if !self.epoch_timestamps() => UnitKind::Timestamp,
                "json" => UnitKind::Json,
                _ => UnitKind::Plain,
            })
            .collect()
    }

    /// Whether TIMESTAMP columns are read as epochs and written as UTC datetimes in a `+00:00`
    /// session, so that instants in a DST fall-back hour survive the round trip (read as local
    /// datetimes and written back in the same zone, the second 02:30 became the first).
    pub(crate) fn epoch_timestamps(&self) -> bool {
        !self.epoch.is_empty()
    }

    /// The SELECT that reads the source rows (filtered by a unit predicate): the plan's SELECT
    /// with the epoch columns of [`crate::dialect::mysql::source_select`] in front of its select
    /// list.
    pub fn source_sql(&self, plan: &ViewPlan, filter: Option<&str>) -> String {
        let sql = plan.select_sql(&self.hidden, filter);
        crate::dialect::mysql::source_select(&sql, &self.epoch)
    }

    /// Column list that reads a materialized table (TIMESTAMP columns as epochs,
    /// [`crate::dialect::mysql::read_list`]).
    fn read_list(&self) -> String {
        let names: Vec<String> = self.cols.iter().map(|c| c.name.clone()).collect();
        let epoch: Vec<usize> = self.epoch.iter().map(|(p, _)| *p).collect();
        crate::dialect::mysql::read_list(&names, &epoch)
    }

    /// Brings a row read with [`Layout::source_sql`] into the form of [`Layout::normalize`]: the
    /// leading epochs replace the TIMESTAMP values (the dialect's answer,
    /// [`crate::dialect::mysql::normalize_source_row`]).
    pub fn normalize_source(&self, row: Vec<Value>) -> Vec<Value> {
        let row = crate::dialect::mysql::normalize_source_row(
            self.epoch.iter().map(|(p, _)| *p),
            self.epoch.len(),
            self.cols.len(),
            self.epoch_timestamps(),
            row,
        );
        self.normalize(row)
    }

    /// Brings a row read with [`Layout::read_list`] (or [`Layout::normalize_source`]) into the
    /// form that is compared and written back: the dialect's [`crate::dialect::mysql::normalize_row`]
    /// with this layout's column types and TIMESTAMP columns, the epochs through
    /// [`crate::dialect::mysql::epoch_to_utc`].
    pub fn normalize(&self, mut row: Vec<Value>) -> Vec<Value> {
        crate::dialect::mysql::normalize_row(
            self.cols.iter().map(|c| c.data_type.as_str()),
            self.epoch.iter().map(|(p, _)| *p),
            &mut row,
            crate::dialect::mysql::epoch_to_utc,
        );
        row
    }

    /// Whether the table has an index on the unit columns (PRIMARY KEY or `_lv_unit`, see
    /// [`Layout::create_sql`]). Without one, unit DELETEs scan the table and concurrent writes of
    /// disjoint units would wait on each other's row locks.
    pub fn has_unit_index(&self) -> bool {
        let plan = crate::target::KeyPlan::for_layout(self, self.key_kind());
        crate::target::unit_index(self, &plan).is_some()
    }

    /// How each unit column's keys are bound in a filter (see [`UnitChunk::filter_typed`]), from
    /// the probed column type: TIMESTAMP keys by local time, JSON keys (MySQL/Percona; MariaDB's
    /// JSON is LONGTEXT) as JSON values.
    pub fn unit_kinds(&self) -> Vec<UnitKind> {
        self.unit_pos
            .iter()
            .map(|&p| match self.cols[p].data_type.as_str() {
                "timestamp" => UnitKind::Timestamp,
                "json" => UnitKind::Json,
                _ => UnitKind::Plain,
            })
            .collect()
    }

    pub fn unit_key(&self, row: &[Datum]) -> Key {
        self.unit_pos.iter().map(|&p| row[p].clone()).collect()
    }
}

/// `_lv_a<alias>_pk<j>` (a lineage column) → `(alias, j)`.
fn lineage_col(name: &str) -> Option<(usize, usize)> {
    let (alias, j) = name.strip_prefix("_lv_a")?.split_once("_pk")?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(alias) || !digits(j) {
        return None;
    }
    Some((alias.parse().ok()?, j.parse().ok()?))
}

pub(crate) fn lineage_alias(name: &str) -> Option<usize> {
    lineage_col(name).map(|(a, _)| a)
}

/// Positions of the lineage columns `cols[from..]`, grouped into keys (consecutive columns of
/// one alias, `_pk0` first). None if one of them is not a lineage column.
fn lineage_keys(cols: &[MvCol], from: usize) -> Option<Vec<Vec<usize>>> {
    let mut keys: Vec<(usize, Vec<usize>)> = Vec::new();
    for (p, c) in cols.iter().enumerate().skip(from) {
        let (alias, j) = lineage_col(&c.src_name)?;
        match keys.last_mut() {
            Some((a, key)) if *a == alias && key.len() == j => key.push(p),
            _ if j == 0 => keys.push((alias, vec![p])),
            _ => return None,
        }
    }
    Some(keys.into_iter().map(|(_, k)| k).collect())
}

/// The TIMESTAMP columns of a probed layout with the expressions that compute them (see
/// [`Layout::epoch`]); empty unless every one has a known expression. The rule is the
/// dialect's ([`crate::dialect::mysql::epoch_prefix`]): this adapts the probe's types to it
/// (the alias cut stays here until `dialect-call-sites` moves `item_expr`).
fn epoch_exprs(
    plan: &ViewPlan,
    cols: &[MvCol],
    n_visible: usize,
    hidden: &[usize],
) -> Vec<(usize, String)> {
    let ts: Vec<usize> = cols
        .iter()
        .enumerate()
        .filter(|(_, c)| c.data_type == "timestamp")
        .map(|(i, _)| i)
        .collect();
    let projection: Vec<String> = plan
        .projection
        .iter()
        .map(|s| crate::dialect::mysql::item_expr(s).to_string())
        .collect();
    crate::dialect::mysql::epoch_prefix(
        !matches!(plan.mode, Mode::Full(_)),
        &plan.select_sql(hidden, None),
        &projection,
        &plan.unit_exprs,
        &ts,
        n_visible,
        hidden,
    )
}

/// Session settings for every connection the service opens (the target module renders them:
/// [`crate::target::init_source_session`]).
pub async fn init_session(conn: &mut Conn, lock_wait_s: u64) -> Result<()> {
    crate::target::init_source_session(conn, lock_wait_s).await
}

/// The statements that give a MySQL-family connection the service's session settings (see
/// [`crate::target::source_session_setup`]).
pub fn session_setup(sql_mode: &str, lock_wait_s: u64) -> [String; 3] {
    crate::target::source_session_setup(sql_mode, lock_wait_s)
}

/// `mode` without the flags that reject values the source holds and the flags that change how
/// SQL text is parsed (see [`crate::target::permissive_sql_mode`]).
pub fn permissive_sql_mode(mode: &str) -> String {
    crate::target::permissive_sql_mode(mode)
}

/// Derives the materialized layout by creating a temporary view on the source and reading its
/// column types from `information_schema`.
pub async fn probe(src: &mut Conn, src_schema: &str, plan: &ViewPlan) -> Result<Layout> {
    let cols = probe_columns(src, src_schema, plan).await?;
    layout_from(plan, cols)
}

/// The server round trip of [`probe`]: create the temporary probe view, read its columns back
/// from `information_schema`, drop the view.
async fn probe_columns(src: &mut Conn, src_schema: &str, plan: &ViewPlan) -> Result<Vec<MvCol>> {
    let hidden: Vec<usize> = (0..plan.unit_exprs.len())
        .filter(|&u| plan.unit_in_projection[u].is_none())
        .collect();
    let probe = format!("_lv_probe_{}", plan.hash);
    let select = plan.select_sql(&hidden, None);
    src.query_drop(format!(
        "CREATE OR REPLACE VIEW {}.{} AS {select}",
        q(src_schema),
        q(&probe)
    ))
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
    src.query_drop(format!(
        "DROP VIEW IF EXISTS {}.{}",
        q(src_schema),
        q(&probe)
    ))
    .await?;
    let cols: Vec<MvCol> = rows?
        .into_iter()
        .map(
            |(name, column_type, data_type, nullable, charset, collation)| MvCol {
                src_name: name.clone(),
                name,
                column_type,
                data_type: data_type.to_ascii_lowercase(),
                charset,
                collation,
                nullable: nullable.eq_ignore_ascii_case("YES"),
            },
        )
        .collect();
    Ok(cols)
}

/// The pure layout computation of [`probe`]: derive the [`Layout`] from the columns
/// [`probe_columns`] read back, unit-testable without a server.
fn layout_from(plan: &ViewPlan, cols: Vec<MvCol>) -> Result<Layout> {
    let hidden: Vec<usize> = (0..plan.unit_exprs.len())
        .filter(|&u| plan.unit_in_projection[u].is_none())
        .collect();
    // Projection, hidden unit columns, then lineage columns (incremental plans).
    let n_visible = match plan.mode {
        Mode::Full(_) => cols.len().saturating_sub(hidden.len()),
        _ => plan.projection.len(),
    };
    if n_visible + hidden.len() > cols.len() {
        bail!(
            "probe of view `{}` has {} columns, expected at least {}",
            plan.name,
            cols.len(),
            n_visible + hidden.len()
        );
    }
    // (The presence columns of pinned cascade children close the list.)
    let lineage_end = cols.len().saturating_sub(plan.presence.len());
    if cols[lineage_end..]
        .iter()
        .zip(&plan.presence)
        .any(|(c, p)| c.src_name != p.name)
    {
        bail!(
            "probe of view `{}`: unexpected columns where the presence columns belong",
            plan.name
        );
    }
    let lineage =
        lineage_keys(&cols[..lineage_end], n_visible + hidden.len()).with_context(|| {
            format!(
                "probe of view `{}`: unexpected columns after the hidden unit columns",
                plan.name
            )
        })?;
    let epoch = epoch_exprs(plan, &cols, n_visible, &hidden);
    let mut cols = cols;
    if let Some(names) = &plan.output_names {
        // `CREATE VIEW v (a, b) AS ...`: the view's column names, applied positionally.
        for (c, n) in cols.iter_mut().zip(names) {
            c.name = n.clone();
        }
    }
    if n_visible == 0 {
        bail!("view has no columns");
    }
    let unit_pos: Vec<usize> = (0..plan.unit_exprs.len())
        .map(|u| match plan.unit_in_projection[u] {
            Some(p) => p,
            None => n_visible + hidden.iter().position(|&h| h == u).unwrap(),
        })
        .collect();
    // (A PRIMARY KEY needs NOT NULL, and MariaDB rejects INVISIBLE NOT NULL columns without a
    // DEFAULT: hidden unit columns get a plain index instead.)
    let primary = plan.unit_unique
        && !unit_pos.is_empty()
        && unit_pos.iter().all(|&p| p < n_visible)
        && unit_pos.iter().all(|&p| {
            let c = &cols[p];
            !c.nullable
                && !c.data_type.contains("text")
                && !c.data_type.contains("blob")
                && c.data_type != "json"
        });
    let identity: Vec<usize> = if plan.unit_unique {
        unit_pos.clone()
    } else if plan.rows_identified() {
        unit_pos
            .iter()
            .chain(lineage.iter().flatten())
            .copied()
            .collect()
    } else {
        Vec::new()
    };
    Ok(Layout {
        cols,
        n_visible,
        hidden,
        unit_pos,
        primary,
        lineage,
        epoch,
        identity,
        partial: plan.partial.sets.clone(),
        key_budget: crate::target::MY_KEY_BYTES,
    })
}

/// Session time zone in which TIMESTAMPs are written (see [`Layout::epoch_timestamps`]).
pub(crate) use crate::target::UTC_SESSION;

pub(crate) fn rows_per_insert(layout: &Layout, batch_rows: usize) -> usize {
    (MAX_PARAMS / layout.cols.len().max(1)).clamp(1, batch_rows.max(1))
}

/// How a target server takes upserts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upsert {
    /// `INSERT ... VALUES (...) AS new ON DUPLICATE KEY UPDATE c = new.c` (MySQL and Percona
    /// from 8.0.19, where `VALUES(c)` is deprecated).
    RowAlias,
    /// `... ON DUPLICATE KEY UPDATE c = VALUES(c)` (MariaDB, which has no row alias; older MySQL).
    Values,
    /// `INSERT ... ON CONFLICT (<row key>) DO UPDATE SET ...` (PostgreSQL; rendered by
    /// `src/target/postgres.rs`, PR T4).
    OnConflict,
}

/// Statement limits and upsert syntax of the writes to one server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOpts {
    /// Rows per INSERT (`LAZYVIEW_WRITE_BATCH_ROWS`), also bounded by the placeholder limit.
    pub batch_rows: usize,
    /// Estimated bytes per statement (`LAZYVIEW_WRITE_BATCH_BYTES`, at most half the server's
    /// `max_allowed_packet`). A larger row goes alone.
    pub batch_bytes: usize,
    pub upsert: Upsert,
    /// The target family this connection speaks (statement shapes, caps). Set once when the
    /// connection is made and carried by every write (see [`crate::target::Kind`]).
    pub kind: crate::target::Kind,
}

impl WriteOpts {
    /// For a server reporting `version` (`@@version`) and `max_packet`
    /// (`@@max_allowed_packet`); the target module decides the upsert form (see
    /// [`crate::target::for_server`]).
    pub fn for_server(
        kind: crate::target::Kind,
        version: &str,
        max_packet: u64,
        batch_rows: usize,
        batch_bytes: usize,
    ) -> WriteOpts {
        crate::target::for_server(kind, version, max_packet, batch_rows, batch_bytes)
    }
}

/// Estimated bytes a value takes in a statement.
pub(crate) fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Bytes(b) => b.len() + 9,
        Value::NULL => 1,
        _ => 13,
    }
}

pub(crate) fn datum_bytes(d: &Datum) -> usize {
    match d {
        Datum::Bytes(b) => b.len() + 9,
        _ => 13,
    }
}

/// Consecutive runs of `items` with at most `max_items` items and `max_bytes` estimated bytes
/// each (at least one item per run).
pub(crate) fn batches<T>(
    items: &[T],
    max_items: usize,
    max_bytes: usize,
    bytes: impl Fn(&T) -> usize,
) -> Vec<&[T]> {
    let mut out = Vec::new();
    let (mut start, mut size) = (0, 0);
    for (i, it) in items.iter().enumerate() {
        let b = bytes(it);
        if i > start && (i - start >= max_items.max(1) || size + b > max_bytes) {
            out.push(&items[start..i]);
            (start, size) = (i, 0);
        }
        size += b;
    }
    if start < items.len() {
        out.push(&items[start..]);
    }
    out
}

/// Multi-row parameterized INSERTs (upserts with `upsert`, see [`Layout::row_key`]), at most
/// `w.batch_rows` rows and about `w.batch_bytes` bytes each.
pub async fn insert_rows<C: Queryable>(
    conn: &mut C,
    schema: &str,
    table: &str,
    layout: &Layout,
    rows: &[Vec<Value>],
    w: &WriteOpts,
    upsert: bool,
    metrics: &Metrics,
) -> Result<()> {
    let per = rows_per_insert(layout, w.batch_rows);
    let text = 3 + 2 * layout.cols.len();
    let row_bytes = |r: &Vec<Value>| text + r.iter().map(value_bytes).sum::<usize>();
    for chunk in batches(rows, per, w.batch_bytes, row_bytes) {
        let sql = crate::target::insert_sql(
            w.kind,
            schema,
            table,
            layout,
            chunk.len(),
            upsert.then_some(w.upsert),
            upsert.then(|| layout.row_key_for(w.kind)).flatten(),
        );
        let params: Vec<Value> = chunk.iter().flatten().cloned().collect();
        conn.exec_drop(sql, params).await?;
        Metrics::add(&metrics.mv_queries, 1);
        Metrics::add(&metrics.mv_rows_inserted, chunk.len() as u64);
        if upsert {
            Metrics::add(&metrics.mv_rows_upserted, chunk.len() as u64);
        }
    }
    Ok(())
}

/// Deletes the rows whose row key (see [`Layout::row_key`]) is one of `keys`, in statements of
/// at most the placeholder limit and about `w.batch_bytes` bytes.
async fn delete_keys<C: Queryable>(
    conn: &mut C,
    schema: &str,
    table: &str,
    layout: &Layout,
    key: &[usize],
    keys: &[Key],
    kind: Kind,
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<()> {
    let cols: Vec<String> = key.iter().map(|&p| q(&layout.cols[p].name)).collect();
    let kinds = layout.write_kinds(key);
    let per = MAX_PARAMS / cols.len().max(1);
    let key_bytes = |k: &Key| 4 + k.iter().map(datum_bytes).sum::<usize>();
    for piece in batches(keys, per, w.batch_bytes, key_bytes) {
        let chunk = keys_chunk(piece);
        let (f, p) = crate::target::unit_filter(kind, &chunk, &cols, &kinds);
        conn.exec_drop(
            format!("DELETE FROM {}.{} WHERE {f}", q(schema), q(table)),
            p,
        )
        .await?;
        Metrics::add(&metrics.mv_queries, 1);
    }
    Ok(())
}

/// One target of a build: its connection by family (the `target::Conn` enum, no trait
/// object; T6), so one build fills MySQL and PostgreSQL shadows side by side from one read.
pub struct BuildTarget<'a> {
    pub conn: &'a mut crate::target::Conn,
    pub schema: String,
    pub write: WriteOpts,
}

/// Fills each target's shadow table from one streaming, non-locking read of the source.
pub async fn fill(
    src: &mut Conn,
    plan: &ViewPlan,
    layout: &Layout,
    targets: &mut [BuildTarget<'_>],
    name: &str,
    batch_rows: usize,
    metrics: &Metrics,
) -> Result<u64> {
    let shadow = crate::target::shadow_name(name);
    let sql = layout.source_sql(plan, None);
    // Read in pieces of a batch; each target splits them by its own limits.
    let per = rows_per_insert(layout, batch_rows);
    if layout.epoch_timestamps() {
        // The build's own connections: they only insert from here on. A PostgreSQL session
        // needs no UTC block: its connect-time `session_setup` (T1.4) already pins
        // `TimeZone='UTC'`, so the MySQL session dance is skipped for it (T4.7).
        for t in targets.iter_mut() {
            if let crate::target::Conn::Mysql(c) = &mut t.conn {
                c.query_drop(format!("SET {UTC_SESSION}")).await?;
            }
        }
    }
    let mut res = src.exec_iter(sql, ()).await?;
    Metrics::add(&metrics.source_queries, 1);
    let mut buf: Vec<Vec<Value>> = Vec::with_capacity(per);
    let mut total = 0u64;
    let mut last_log = std::time::Instant::now();
    loop {
        let row: Option<Row> = res.next().await?;
        let done = row.is_none();
        if let Some(r) = row {
            buf.push(layout.normalize_source(r.unwrap()));
        }
        if buf.len() >= per || (done && !buf.is_empty()) {
            for t in targets.iter_mut() {
                // Plain INSERTs into the shadow, in the target's own dialect (T4.7): MySQL's
                // `insert_rows` or the same statement bound by `PgConn::insert_rows` (T4.1).
                match &mut t.conn {
                    crate::target::Conn::Mysql(c) => {
                        insert_rows(
                            c, &t.schema, &shadow, layout, &buf, &t.write, false, metrics,
                        )
                        .await?
                    }
                    crate::target::Conn::Postgres(c) => {
                        c.insert_rows(&t.schema, &shadow, layout, &buf, &t.write, false, metrics)
                            .await?
                    }
                }
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

/// Current source rows for a chunk of units, grouped by unit (`None` = whole view). `fresh`:
/// see [`exec_source`].
pub async fn read_source(
    src: &mut Conn,
    plan: &ViewPlan,
    layout: &Layout,
    chunk: Option<&UnitChunk>,
    metrics: &Metrics,
    fresh: bool,
) -> Result<Vec<Vec<Value>>> {
    match chunk {
        Some(c) => {
            let (f, p) = c.filter_typed(&plan.unit_exprs, &layout.unit_kinds());
            read_source_filtered(src, plan, layout, Some(&f), p, metrics, fresh).await
        }
        None => read_source_filtered(src, plan, layout, None, Vec::new(), metrics, fresh).await,
    }
}

/// Runs a read of the sources. `fresh` (MariaDB): the statement is prepared for this execution
/// only and closed after it. MariaDB returns a stale result for a re-executed prepared statement
/// with `NOT IN (subquery)` once the subquery's result changes between empty and non-empty (it
/// keeps answering as before; docs/server-setup.md). Without `fresh` the statement is cached.
pub async fn exec_source<T>(
    src: &mut Conn,
    sql: String,
    params: Vec<Value>,
    fresh: bool,
) -> Result<Vec<T>>
where
    T: FromRow + Send + 'static,
{
    if !fresh {
        return Ok(src.exec(sql, params).await?);
    }
    let stmt = src.prep(sql).await?;
    let rows = src.exec(&stmt, params).await;
    let closed = src.close(stmt).await;
    let rows = rows?;
    closed?;
    Ok(rows)
}

/// Current source rows matching `filter` (any predicate over the view's FROM clause, with
/// `params`), in the form [`diff`] and [`insert_rows`] expect (see [`Layout::normalize_source`]).
pub async fn read_source_filtered(
    src: &mut Conn,
    plan: &ViewPlan,
    layout: &Layout,
    filter: Option<&str>,
    params: Vec<Value>,
    metrics: &Metrics,
    fresh: bool,
) -> Result<Vec<Vec<Value>>> {
    let sql = layout.source_sql(plan, filter);
    let rows: Vec<Row> = exec_source(src, sql, params, fresh).await?;
    Metrics::add(&metrics.source_queries, 1);
    Ok(rows
        .into_iter()
        .map(|r| layout.normalize_source(r.unwrap()))
        .collect())
}

/// How one unit column of [`units_sql`] comes back from the server, and what turns it into the
/// [`Datum`] the unit keys of [`read_source`]'s rows are made of (see [`to_datums`]): the two
/// must agree, or a chunk the cursor handed out would match nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitRead {
    /// As read.
    Raw,
    /// A TIMESTAMP unit: `UNIX_TIMESTAMP(<expression>)`, the epoch [`Layout::source_sql`] reads
    /// its epoch columns as, then the same UTC datetime of the `+00:00` session.
    Epoch,
    /// A JSON unit of the MySQL family (MariaDB's JSON is LONGTEXT, so it reads as
    /// [`UnitRead::Raw`]): `CAST(<expression> AS CHAR)`, the text [`to_datums`] canonicalizes.
    /// (`SELECT DISTINCT` cannot group a JSON column, and the server's JSON text is the text a
    /// plain read of the column gives.)
    Json,
}

/// Whether a view's rows are the set of units a cursor can walk, and its chunks can be written
/// unit by unit: the guard of both unit cursors ([`units_sql`] and [`target_units`]) and of the
/// existence read ([`vanished`]). `Err` for the views that have no such unit list: a
/// non-incremental plan (its SELECT is the user's text: there is no FROM clause of our own to
/// project, and its `dirty.all` is a rebuild), a plan without units, or a view whose rows are not
/// a set of units — a window function or a top-N. There the unit list is wider than the view, and
/// a chunk of it would delete rows the view keeps, so the caller keeps the read it has today.
fn streamable(plan: &ViewPlan, layout: &Layout) -> Result<()> {
    if !plan.is_incremental() {
        bail!(
            "view `{}`: the unit cursor reads an incremental plan's FROM clause",
            plan.name
        );
    }
    if plan.unit_exprs.is_empty() {
        bail!("view `{}`: the unit cursor needs a unit", plan.name);
    }
    if plan.window_sql.is_some() || plan.order_limit_sql.is_some() {
        bail!(
            "view `{}`: a window or a top-N view has no unit list to stream",
            plan.name
        );
    }
    if layout.unit_pos.len() != plan.unit_exprs.len() {
        bail!(
            "view `{}`: the layout has {} unit columns for {} unit expressions",
            plan.name,
            layout.unit_pos.len(),
            plan.unit_exprs.len()
        );
    }
    Ok(())
}

/// The source side of a units cursor: one projection item per unit expression, with how it comes
/// back from the server (see [`UnitRead`]). The caller has checked [`streamable`].
fn unit_cols(plan: &ViewPlan, layout: &Layout) -> (Vec<String>, Vec<UnitRead>) {
    let mut cols: Vec<String> = Vec::with_capacity(plan.unit_exprs.len());
    let mut read: Vec<UnitRead> = Vec::with_capacity(plan.unit_exprs.len());
    for (j, e) in plan.unit_exprs.iter().enumerate() {
        // (The epoch entry of the column, when it has one: the very expression `source_sql`
        // reads, so the key of a TIMESTAMP unit is the key a chunk read gives.)
        let item = match layout.epoch.iter().find(|(p, _)| *p == layout.unit_pos[j]) {
            Some((_, expr)) => {
                read.push(UnitRead::Epoch);
                format!("UNIX_TIMESTAMP({expr})")
            }
            None if layout.cols[layout.unit_pos[j]].data_type == "json" => {
                read.push(UnitRead::Json);
                format!("CAST({e} AS CHAR)")
            }
            None => {
                read.push(UnitRead::Raw);
                e.clone()
            }
        };
        cols.push(item);
    }
    (cols, read)
}

/// The target side of a units cursor: one projection item per unit *column* of the materialized
/// table, each read exactly as [`read_old`] reads it (see [`Layout::read_list`], which is where
/// `UNIX_TIMESTAMP` and the plain quoted name come from). That is the whole contract: a key the
/// target cursor hands out is the key of the rows `read_old` answers for it, and the same datum a
/// chunk read of the source gives for the same unit — which is what lets [`vanished`] compare the
/// two sides at all.
fn target_unit_cols(layout: &Layout) -> (Vec<String>, Vec<UnitRead>) {
    let epoch: Vec<usize> = layout.epoch.iter().map(|(p, _)| *p).collect();
    let mut cols: Vec<String> = Vec::with_capacity(layout.unit_pos.len());
    let mut read: Vec<UnitRead> = Vec::with_capacity(layout.unit_pos.len());
    for name in layout.unit_cols() {
        let p = layout.unit_pos[cols.len()];
        let item = if epoch.contains(&p) {
            read.push(UnitRead::Epoch);
            format!("UNIX_TIMESTAMP({name})")
        } else if layout.cols[p].data_type == "json" {
            read.push(UnitRead::Json);
            format!("CAST({name} AS CHAR)")
        } else {
            read.push(UnitRead::Raw);
            name
        };
        cols.push(item);
    }
    (cols, read)
}

/// The data type of each unit column, in the unit order: what [`unit_datums`] normalizes a cursor's
/// row by, the same names [`Layout::unit_kinds`] classifies. Both cursors read the same unit
/// columns, so one list serves the source's expressions and the target's columns.
fn unit_types(layout: &Layout) -> Vec<String> {
    layout
        .unit_pos
        .iter()
        .map(|&p| layout.cols[p].data_type.clone())
        .collect()
}

/// `SELECT DISTINCT <cols> FROM <from> [WHERE <where> [AND <extra>]] [GROUP BY <group>]
/// [HAVING <having>] ORDER BY <cols>`: the shape a units cursor streams and the existence read of
/// [`vanished`] asks. The order is by the projected expressions, which `SELECT DISTINCT` requires
/// of an ORDER BY item; it costs the server nothing when the units are an index's columns, because
/// the loose index scan answers the DISTINCT and the order in one pass. `extra` is a predicate
/// over the same FROM clause AND-ed onto the plan's own `WHERE` (the chunk filter).
fn distinct_units_sql(
    cols: &[String],
    from: &str,
    where_sql: Option<&str>,
    group_sql: Option<&str>,
    having_sql: Option<&str>,
    extra: Option<&str>,
) -> String {
    let mut sql = format!("SELECT DISTINCT {} FROM {}", cols.join(", "), from);
    match (where_sql, extra) {
        (Some(w), Some(e)) => sql.push_str(&format!(" WHERE ({w}) AND ({e})")),
        (Some(w), None) => sql.push_str(&format!(" WHERE {w}")),
        (None, Some(e)) => sql.push_str(&format!(" WHERE {e}")),
        (None, None) => {}
    }
    if let Some(g) = group_sql {
        sql.push_str(" GROUP BY ");
        sql.push_str(g);
    }
    if let Some(h) = having_sql {
        sql.push_str(" HAVING ");
        sql.push_str(h);
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&cols.join(", "));
    sql
}

/// The statement of [`source_units`]: the plan's own unit expressions over its own FROM clause,
/// with the plan's `WHERE`, `GROUP BY` and `HAVING`. That is all it is: the cursor answers "which
/// units are there", the per-unit chunk reads do the work, so refreshing a whole view is a
/// sequence of chunks and never one collected view (R-02).
fn units_sql(plan: &ViewPlan, layout: &Layout) -> Result<(String, Vec<UnitRead>)> {
    streamable(plan, layout)?;
    let (cols, read) = unit_cols(plan, layout);
    Ok((
        distinct_units_sql(
            &cols,
            &plan.from_sql,
            plan.where_sql.as_deref(),
            plan.group_sql.as_deref(),
            plan.having_sql.as_deref(),
            None,
        ),
        read,
    ))
}

/// The unit key of one row of a units cursor ([`units_sql`], [`target_units_sql`], the existence
/// read of [`vanished`]): the same [`Datum`]s [`to_datums`] gives for those columns of a
/// [`read_source`] or [`read_old`] row, so a chunk the cursor handed out matches the rows of its
/// unit on both sides. `types` are the unit columns' data types ([`unit_types`]): [`to_datums`]
/// sees them through [`crate::dialect::mysql::normalize_row`], which a cursor's row goes through
/// here too (a BIT comes back as the integer the row holds, a TIME as the text every server
/// accepts as its parameter), and the epoch and JSON entries then read as they do there.
fn unit_datums(read: &[UnitRead], types: &[String], vals: Vec<Value>) -> Key {
    let mut row = vals;
    crate::dialect::mysql::normalize_row(
        types.iter().map(String::as_str),
        read.iter()
            .enumerate()
            .filter(|(_, r)| **r == UnitRead::Epoch)
            .map(|(i, _)| i),
        &mut row,
        crate::dialect::mysql::epoch_to_utc,
    );
    read.iter()
        .zip(row)
        .map(|(r, v)| match r {
            // (The epoch is the UTC datetime `normalize_row` made of it, as in `to_datums`.)
            UnitRead::Epoch => Datum::from_value(v),
            UnitRead::Json => match v {
                Value::Bytes(b) => Datum::Bytes(json_canonical(&b)),
                v => Datum::from_value(v),
            },
            UnitRead::Raw => Datum::from_value(v),
        })
        .collect()
}

/// The units of a plan's source, streamed: one server cursor over [`units_sql`], handed out as
/// [`UnitChunk`]s of at most `chunk` keys in the unit's own order, the integer fast path of
/// [`keys_chunk`] included. The whole-view refresh of a unit-based view is then the per-unit
/// chunk path (`read_source` + `read_old` + `diff` + `write_diff`) walked over every unit of the
/// source, so its memory is a chunk's and not the view's (R-02).
///
/// The cursor *is* the connection's open result set: while it lives, that connection answers no
/// other query. A sweep therefore streams the units on a connection of its own and reads and
/// writes the chunks on the ones the refresh already uses. It is started once per whole refresh,
/// never per binlog event; it allocates one chunk of keys at a time; and its state is
/// engine memory only — a restart re-runs the sweep from the start, which is idempotent.
///
/// MySQL family only, as `Conn` is: a PostgreSQL source streams through its own client.
pub struct UnitStream<'a> {
    res: QueryResult<'a, 'static, BinaryProtocol>,
    /// How each column of the statement is read, in the plan's unit order (see [`units_sql`]).
    read: Vec<UnitRead>,
    /// The unit columns' data types, in the same order (see [`unit_datums`]).
    types: Vec<String>,
    /// Unit keys per chunk, clamped by [`source_units`] to what a chunk filter can bind.
    chunk: usize,
    /// Keys handed out so far.
    seen: usize,
    /// The cursor's last chunk has been handed out.
    done: bool,
}

impl<'a> UnitStream<'a> {
    /// The next chunk of units, `None` when the source has no more. A single integer unit comes
    /// back as [`UnitChunk::Ints`] (the fast path every chunk filter already takes), anything
    /// else as [`UnitChunk::Keys`].
    pub async fn next_chunk(&mut self) -> Result<Option<UnitChunk>> {
        Ok(self.next_keys().await?.map(|k| keys_chunk(&k)))
    }

    /// As [`Self::next_chunk`] with the keys as the cursor read them, for a caller that groups
    /// or compares them itself.
    pub async fn next_keys(&mut self) -> Result<Option<Vec<Key>>> {
        if self.done {
            return Ok(None);
        }
        let mut out: Vec<Key> = Vec::with_capacity(self.chunk);
        while out.len() < self.chunk {
            let Some(r) = self.res.next().await? else {
                self.done = true;
                break;
            };
            out.push(unit_datums(&self.read, &self.types, r.unwrap()));
        }
        if out.is_empty() {
            self.done = true;
            return Ok(None);
        }
        self.seen += out.len();
        Ok(Some(out))
    }

    /// How many units have been handed out (a sweep logs it, a test counts on it).
    pub fn units(&self) -> usize {
        self.seen
    }

    /// Whether the source has no more units for the cursor.
    pub fn is_exhausted(&self) -> bool {
        self.done
    }

    /// Closes the cursor, dropping whatever rows are left, so the connection is clean for the
    /// next query. A sweep that stops early (a failure, a budget spent) calls this; dropping the
    /// stream instead leaves the rows to be discarded by the connection's next command.
    pub async fn finish(self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        let Self { res, .. } = self;
        res.drop_result().await?;
        Ok(())
    }
}

/// Unit keys a chunk of a cursor may hold: the caller's `cfg.query_chunk`, at least one and at
/// most what a chunk filter can bind (one parameter per key column of every key in it, the bound
/// [`read_source`] binds under).
fn chunk_limit(n_units: usize, chunk: usize) -> usize {
    chunk.max(1).min(MAX_PARAMS / n_units.max(1))
}

/// The units of `plan`'s source rows as a streamed cursor (see [`UnitStream`]).
///
/// `chunk` is the caller's `cfg.query_chunk`: the unit keys one chunk holds, clamped to what a
/// chunk filter's parameters allow (the same bound [`read_source`] binds under). `Err` for a plan
/// the cursor cannot stream, which keeps the whole read it has today (see [`units_sql`]).
pub async fn source_units<'a>(
    src: &'a mut Conn,
    plan: &ViewPlan,
    layout: &Layout,
    chunk: usize,
    metrics: &Metrics,
) -> Result<UnitStream<'a>> {
    let (sql, read) = units_sql(plan, layout)?;
    let chunk = chunk_limit(plan.unit_exprs.len(), chunk);
    // (A plain statement, not a prepared one: the cursor runs once, and MariaDB's answer for a
    // re-executed prepared statement cannot apply to it — see [`exec_source`].)
    let res = src.exec_iter(sql, ()).await?;
    Metrics::add(&metrics.source_queries, 1);
    Ok(UnitStream {
        res,
        read,
        types: unit_types(layout),
        chunk,
        seen: 0,
        done: false,
    })
}

/// The statement of [`target_units`]: `SELECT DISTINCT <unit columns> FROM <schema>.<table>`,
/// ordered by the same expressions. It is the target's twin of [`units_sql`] with no plan in it:
/// the units the view *holds*, which is what the vanished-unit pass of a whole refresh walks
/// (R-02). The caller has checked [`streamable`], so the unit list is the view's own.
fn target_units_sql(layout: &Layout, schema: &str, table: &str) -> (String, Vec<UnitRead>) {
    let (cols, read) = target_unit_cols(layout);
    let sql = format!(
        "SELECT DISTINCT {} FROM {}.{} ORDER BY {}",
        cols.join(", "),
        q(schema),
        q(table),
        cols.join(", ")
    );
    (sql, read)
}

/// The units the materialized table `schema`.`table` holds, as a streamed cursor (see
/// [`UnitStream`]): the target's twin of [`source_units`], same chunking, same keys.
///
/// This is the second cursor of a whole-view refresh. The first ([`source_units`]) covers the
/// units the source has now; this one covers the units the *view* has, so the refresh also sees the
/// units that vanished from the source while the view was behind (#167's gap windows): for a chunk
/// of them [`vanished`] asks the source once, and the units it no longer answers for have their
/// rows deleted by the ordinary chunk path ([`read_old`] + [`diff`] against an empty [`Prepared`] +
/// [`write_diff`]). Nothing is held: as in [`source_units`], one chunk of keys exists at a time,
/// and the cursor is the connection's open result set — a sweep streams it on a connection of its
/// own and reads and writes the chunks on the ones the refresh already uses. Started once per whole
/// refresh, never per binlog event; its state is engine memory only, so a restart re-runs the sweep
/// from the start, which is idempotent.
///
/// `chunk` is the caller's `cfg.query_chunk` (lv-super's ruling 6: no new setting), clamped to what
/// a chunk filter's parameters allow — the same bound [`read_old`] and [`vanished`] bind under, so
/// a chunk this cursor hands out is a chunk both can use. `Err` for a view whose rows are not a set
/// of units, which keeps the whole read it has today (see [`streamable`]).
///
/// MySQL family targets, as `Conn` is: a PostgreSQL target is read by its own client
/// ([`crate::target::postgres::PgConn::read_old`]), and its cursor comes with the sweep's PostgreSQL
/// leg rather than as code nothing calls yet (lv-super's ruling 7).
pub async fn target_units<'a>(
    conn: &'a mut Conn,
    schema: &str,
    table: &str,
    plan: &ViewPlan,
    layout: &Layout,
    chunk: usize,
    metrics: &Metrics,
) -> Result<UnitStream<'a>> {
    streamable(plan, layout)?;
    let (sql, read) = target_units_sql(layout, schema, table);
    let chunk = chunk_limit(layout.unit_pos.len(), chunk);
    // (A plain statement, as in [`source_units`]: the cursor runs once per whole refresh.)
    let res = conn.exec_iter(sql, ()).await?;
    Metrics::add(&metrics.mv_queries, 1);
    Ok(UnitStream {
        res,
        read,
        types: unit_types(layout),
        chunk,
        seen: 0,
        done: false,
    })
}

/// What [`vanished`] concluded about one chunk of the target's units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Vanished {
    /// These of the chunk's units have no row left in the source: their target rows may go, and
    /// the ordinary chunk path deletes them ([`read_old`] + [`diff`] with an empty [`Prepared`]).
    Keys(Vec<Key>),
    /// The source answered for the chunk with a key that is not one of the chunk's own: under a
    /// case-insensitive collation the two are the same unit, and only the server can tell. Do not
    /// delete from this chunk: refresh it from the source instead ([`read_source`] +
    /// [`apply_prepared`]), which reaches the same end state and lets the server do the comparison
    /// (a unit the source really has keeps its rows, rewritten to the source's own text; a unit it
    /// has lost loses them in the diff). Returned for a chunk the equality fast path cannot settle
    /// — the integer chunks of an `Ints` cursor always can.
    Ambiguous,
}

/// Which units of one target chunk the source no longer has: ONE read of the source, the shape of
/// [`units_sql`] with the chunk's filter AND-ed onto the plan's `WHERE` — the keys only, never a
/// view row (`SELECT DISTINCT` of the unit expressions, so at most `chunk` rows come back, and the
/// loose index scan answers it as an index lookup per unit). `fresh`: see [`read_source`].
///
/// This is the pass that makes the target's cursor safe to delete from: the units the source still
/// answers for are left to their own chunk refresh, and only the units it is silent about are
/// deleted. The comparison is exact on purpose, and [`Vanished::Ambiguous`] is how the function
/// says "not by me": a chunk whose answers are the chunk's own keys matches key for key, so a key
/// with no answer has no source rows; an answer that is some other text (a case variant of a key,
/// under a CI collation) breaks that argument, and guessing from here would delete a live unit's
/// rows. The plan's cost note (lv-super's ruling 2): one such read per chunk of the target's units,
/// which is what a merge-join of the two cursors across servers would cost in state instead.
///
/// A unit whose rows are gone *and* whose text the source no longer spells the same way is answered
/// for by nothing, so it is deleted whether or not the chunk is ambiguous; the ambiguity only ever
/// withholds a delete, never causes one.
pub async fn vanished(
    src: &mut Conn,
    plan: &ViewPlan,
    layout: &Layout,
    chunk: &UnitChunk,
    metrics: &Metrics,
    fresh: bool,
) -> Result<Vanished> {
    streamable(plan, layout)?;
    let n_units = plan.unit_exprs.len();
    // (The chunk filter binds a parameter per key column, as `read_source` does. A chunk of its own
    // comes from [`target_units`], which clamps it to this budget; a caller-built one is refused.)
    if chunk.len().saturating_mul(n_units).saturating_add(n_units) > MAX_PARAMS {
        bail!(
            "view `{}`: a chunk of {} units of {n_units} columns binds more than the {} parameters \
             a statement holds; take the chunk from `target_units`",
            plan.name,
            chunk.len(),
            MAX_PARAMS
        );
    }
    let keys: Vec<Key> = match chunk {
        UnitChunk::Ints(v) => v.iter().map(|&i| Key::from([Datum::Int(i)])).collect(),
        UnitChunk::Keys(k) => k.to_vec(),
        // (A partial chunk constrains only some unit columns: it cannot say which *units* are
        // there, which is the one question this read answers.)
        UnitChunk::Partial(..) => {
            bail!(
                "view `{}`: the vanished-unit read needs a whole unit chunk, not a partial one",
                plan.name
            )
        }
    };
    let (cols, read) = unit_cols(plan, layout);
    let (filter, params) = chunk.filter_typed(&plan.unit_exprs, &layout.unit_kinds());
    let sql = distinct_units_sql(
        &cols,
        &plan.from_sql,
        plan.where_sql.as_deref(),
        plan.group_sql.as_deref(),
        plan.having_sql.as_deref(),
        Some(&filter),
    );
    let rows: Vec<Row> = exec_source(src, sql, params, fresh).await?;
    Metrics::add(&metrics.source_queries, 1);
    let types = unit_types(layout);
    let answers: HashSet<Key> = rows
        .into_iter()
        .map(|r| unit_datums(&read, &types, r.unwrap()))
        .collect();
    // (Every answer that is one of the chunk's own keys settles that key: it has rows. An answer
    // that is none of them is a key the server matched to a chunk key without spelling it the same
    // way — a collation variant — and no key of this chunk can be trusted to be absent then.)
    let asked: HashSet<&Key> = keys.iter().collect();
    if answers.iter().any(|a| !asked.contains(a)) {
        return Ok(Vanished::Ambiguous);
    }
    Ok(Vanished::Keys(
        keys.into_iter().filter(|k| !answers.contains(k)).collect(),
    ))
}

/// The datums a row is compared and grouped by in [`diff`]: JSON values (MySQL/Percona `json`
/// columns) as [`json_canonical`] text, everything else as read.
fn to_datums(layout: &Layout, r: &[Value]) -> Vec<Datum> {
    r.iter()
        .zip(&layout.cols)
        .map(|(v, c)| match v {
            Value::Bytes(b) if c.data_type == "json" => Datum::Bytes(json_canonical(b)),
            v => Datum::from_value(v.clone()),
        })
        .collect()
}

/// Rows of one or more units, grouped by unit key: `(datums, original values)` per row.
type Groups = HashMap<Key, Vec<(Vec<Datum>, Vec<Value>)>>;

fn group(layout: &Layout, rows: Vec<(Vec<Datum>, Vec<Value>)>) -> Groups {
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
    /// Rows written by INSERTs (upserts included).
    pub inserted: u64,
    /// Of `inserted`, rows that replace an old row with the same row key in place.
    pub updated: u64,
    /// Old rows compared and found equal (not written).
    pub unchanged: u64,
}

/// New (source) rows of a chunk, converted and grouped by unit once and shared by every target.
#[derive(Debug, Default)]
pub struct Prepared {
    groups: Groups,
    /// Number of rows / datums (to decide whether a diff runs off the async threads).
    pub rows: usize,
    pub datums: usize,
}

/// Converts and groups source rows (see [`read_source`]) for [`diff`].
pub fn prepare(layout: &Layout, rows: Vec<Vec<Value>>) -> Prepared {
    let n = rows.len();
    let datums = rows.iter().map(Vec::len).sum();
    Prepared {
        groups: group(
            layout,
            rows.into_iter()
                .map(|v| (to_datums(layout, &v), v))
                .collect(),
        ),
        rows: n,
        datums,
    }
}

/// What a target table needs so that the chunk's units match the new rows. Without a row key
/// (see [`Layout::row_key`]) the changed units are deleted and their new rows inserted. With one,
/// only the rows of changed units that differ are written: `delete_keys` first, then `inserts`
/// as upserts, so a vanished key that the server deems equal to a new one (case, trailing
/// spaces) is gone before the new row is written.
#[derive(Debug, Default)]
pub struct Diff {
    /// Units with a row that differs.
    pub changed: Vec<Key>,
    /// Written per unit (no row key).
    pub by_unit: bool,
    /// Row keys whose old rows are deleted: rows that vanished, and the old rows of a changed
    /// key with a NULL (a NULL never conflicts in a unique index, so it cannot be upserted).
    pub delete_keys: Vec<Key>,
    /// Old rows deleted.
    pub deleted_rows: u64,
    /// Of `inserts`, rows that replace the old row of their row key in place.
    pub updated_rows: u64,
    /// Old rows equal to their new row (not written).
    pub unchanged_rows: u64,
    /// New rows written: inserted (per unit) or upserted (by row key).
    pub inserts: Vec<Vec<Value>>,
    /// Rows compared (old + new), for statistics.
    pub compared: usize,
}

/// Current MV rows of the units in `chunk` (all rows when `None`).
pub async fn read_old(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    kind: Kind,
    chunk: Option<&UnitChunk>,
    metrics: &Metrics,
) -> Result<Vec<Vec<Value>>> {
    let unit_cols = layout.unit_cols();
    let (filter, params) = match chunk {
        Some(c) => crate::target::unit_filter(kind, c, &unit_cols, &layout.unit_kinds()),
        None => ("TRUE".to_string(), Vec::new()),
    };
    let old: Vec<Row> = conn
        .exec(
            format!(
                "SELECT {} FROM {}.{} WHERE {filter}",
                layout.read_list(),
                q(schema),
                q(table)
            ),
            params,
        )
        .await?;
    Metrics::add(&metrics.mv_queries, 1);
    Ok(old
        .into_iter()
        .map(|r| layout.normalize(r.unwrap()))
        .collect())
}

type Row2 = (Vec<Datum>, Vec<Value>);

/// Whether two sets of rows are equal as multisets.
fn same_rows(a: &[&Row2], b: &[&Row2]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a: Vec<&Vec<Datum>> = a.iter().map(|r| &r.0).collect();
    let mut b: Vec<&Vec<Datum>> = b.iter().map(|r| &r.0).collect();
    a.sort();
    b.sort();
    a == b
}

/// Compares old MV rows with the prepared new rows per unit, and within a changed unit per row
/// key when the layout has one (pure CPU work).
pub fn diff(layout: &Layout, old: Vec<Vec<Value>>, new: &Prepared) -> Diff {
    let compared = old.len() + new.rows;
    let old = group(
        layout,
        old.into_iter()
            .map(|v| (to_datums(layout, &v), v))
            .collect(),
    );
    let row_key = layout.row_key();
    let mut d = Diff {
        by_unit: row_key.is_none(),
        compared,
        ..Default::default()
    };
    let none: Vec<Row2> = Vec::new();
    // New rows in the order the writes take their locks (see [`write_tx_mysql`]).
    let mut ins: Vec<&Row2> = Vec::new();
    let units = new
        .groups
        .keys()
        .chain(old.keys().filter(|k| !new.groups.contains_key(*k)));
    for k in units {
        let o: Vec<&Row2> = old.get(k).unwrap_or(&none).iter().collect();
        let n: Vec<&Row2> = new.groups.get(k).unwrap_or(&none).iter().collect();
        if same_rows(&o, &n) {
            d.unchanged_rows += o.len() as u64;
            continue;
        }
        d.changed.push(k.clone());
        match row_key {
            None => {
                d.deleted_rows += o.len() as u64;
                ins.extend(n.iter().copied());
            }
            Some(key) => diff_rows(key, o, n, &mut d, &mut ins),
        }
    }
    // One order for every writer: two chunks that touch the same keys take their row locks in
    // the same order, so their statements queue instead of deadlocking (#331).
    // Unit (or row key) first, then the whole row: a total order, so the same on every writer even
    // when the view's row key is none for its most restrictive target (a keyless diff still goes
    // out in one order; a target that upserts uses the same identity columns, see `row_key_for`).
    let order = row_key.unwrap_or(&layout.unit_pos[..]);
    ins.sort_by(|a, b| {
        order
            .iter()
            .map(|&p| a.0[p].cmp(&b.0[p]))
            .find(|o| o.is_ne())
            .unwrap_or_else(|| a.0.cmp(&b.0))
    });
    d.inserts = ins.iter().map(|r| r.1.clone()).collect();
    d.delete_keys.sort_unstable();
    d.changed.sort_unstable();
    d
}

/// The rows of one changed unit, compared per row key (see [`diff`]).
fn diff_rows<'a>(
    key: &[usize],
    old: Vec<&Row2>,
    new: Vec<&'a Row2>,
    d: &mut Diff,
    ins: &mut Vec<&'a Row2>,
) {
    fn by_key<'a>(key: &[usize], rows: Vec<&'a Row2>) -> HashMap<Key, Vec<&'a Row2>> {
        let mut m: HashMap<Key, Vec<&Row2>> = HashMap::new();
        for r in rows {
            m.entry(key.iter().map(|&p| r.0[p].clone()).collect())
                .or_default()
                .push(r);
        }
        m
    }
    let mut old = by_key(key, old);
    for (k, n) in by_key(key, new) {
        let o = old.remove(&k).unwrap_or_default();
        if same_rows(&o, &n) {
            d.unchanged_rows += o.len() as u64;
            continue;
        }
        let null = crate::datum::key_has_null(&k);
        debug_assert!(
            null || n.len() <= 1,
            "internal: {} rows of the view share the row key {k:?}; the table's unique index \
             `{ROW_INDEX}` cannot hold them",
            n.len()
        );
        // An old row with the same key is replaced in place, unless the key has a NULL (or the
        // table holds it more than once): those are deleted and inserted again.
        if !o.is_empty() && (null || o.len() > 1) {
            d.delete_keys.push(k);
            d.deleted_rows += o.len() as u64;
        } else if !o.is_empty() {
            d.updated_rows += 1;
        }
        ins.extend(n.iter().copied());
    }
    for (k, o) in old {
        d.delete_keys.push(k);
        d.deleted_rows += o.len() as u64;
    }
}

/// Writes a diff as autocommit statements (see [`Diff`], [`write_tx_mysql`]). `began` is set
/// before the first statement (from then on a lost connection may still have applied some: see
/// the supervisor's zombie fence).
pub async fn write_diff(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    d: &Diff,
    w: &WriteOpts,
    metrics: &Metrics,
    began: &AtomicBool,
) -> Result<Applied> {
    if d.changed.is_empty() {
        return Ok(Applied {
            unchanged: d.unchanged_rows,
            ..Default::default()
        });
    }
    began.store(true, Ordering::SeqCst);
    let utc = layout.epoch_timestamps();
    if utc {
        // TIMESTAMPs (also as keys of the DELETEs) are UTC datetimes; the session's own time
        // zone is restored for the unit filters of the next read.
        conn.query_drop(format!("SET @lv_tz = @@SESSION.time_zone, {UTC_SESSION}"))
            .await?;
    }
    // The transaction's open/commit stays with the family (one dispatch per write, never per
    // row). The PostgreSQL flush is `postgres::PgConn::write_tx` (T4.5), which opens its own
    // transaction; this arm is unreachable because the connection here is a MySQL one and the
    // pool that hands a write a `PgConn` is PR T5.
    let r = match w.kind {
        Kind::Mysql => write_tx_mysql(conn, schema, table, layout, d, w, metrics).await,
        Kind::Postgres => unreachable!(
            "the PostgreSQL write transaction runs on the target's own connection (PgConn::write_tx)"
        ),
    };
    if utc {
        let restored = conn.query_drop("SET SESSION time_zone = @lv_tz").await;
        if r.is_ok() {
            restored?;
        }
    }
    r
}

/// The MySQL statements of [`write_diff`] (the family's own arm; the PostgreSQL arm is
/// `PgConn::write_tx`).
async fn write_tx_mysql(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    d: &Diff,
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<Applied> {
    let out = Applied {
        units_changed: d.changed.len() as u64,
        deleted: d.deleted_rows,
        inserted: d.inserts.len() as u64,
        updated: d.updated_rows,
        unchanged: d.unchanged_rows,
    };
    // No transaction (owner, #331): a transaction held every row lock of a chunk until its COMMIT,
    // so two chunks touching the same rows in different orders deadlocked (ERROR 1213). Each
    // statement below is its own autocommit and releases its locks at once; keys and rows are
    // sorted (see [`diff`]), so one statement never waits on another that waits on it. The
    // deletes run first: a key the diff replaces (a NULL in it, or the table holding it twice)
    // must not take the upsert's new row with it. A crash or a failure between statements leaves
    // the chunk half written, which is safe: the position is saved only after the whole chunk
    // succeeded, and the replay re-reads the target, so its diff is exactly what is still missing.
    let tx = conn;
    if d.by_unit && d.deleted_rows > 0 {
        let del_filter = if layout.unit_pos.is_empty() {
            ("TRUE".to_string(), Vec::new())
        } else {
            let chunk = keys_chunk(&d.changed);
            crate::target::unit_filter(
                w.kind,
                &chunk,
                &layout.unit_cols(),
                &layout.write_kinds(&layout.unit_pos),
            )
        };
        tx.exec_drop(
            format!(
                "DELETE FROM {}.{} WHERE {}",
                q(schema),
                q(table),
                del_filter.0
            ),
            del_filter.1,
        )
        .await?;
        Metrics::add(&metrics.mv_queries, 1);
    }
    if !d.by_unit {
        // `by_unit` is false only when the layout has a row key (see [`diff`]), so the caller
        // holds it already: `delete_keys` does not re-check it.
        let key = layout
            .row_key_for(w.kind)
            .expect("internal: rows of a by-row diff have no row key");
        delete_keys(
            tx,
            schema,
            table,
            layout,
            key,
            &d.delete_keys,
            w.kind,
            w,
            metrics,
        )
        .await?;
    }
    Metrics::add(&metrics.mv_rows_deleted, d.deleted_rows);
    insert_rows(
        tx, schema, table, layout, &d.inserts, w, !d.by_unit, metrics,
    )
    .await?;
    Metrics::add(&metrics.units_changed, out.units_changed);
    Ok(out)
}

/// Brings `table` in line with the already-converted new rows of a chunk, for the units in
/// `chunk` (all rows when `None`), writing only units whose rows differ. `new` comes from
/// [`prepare`] and is immutable ([`diff`] only reads it), so one chunk prepares once and every
/// target of the chunk shares it (see [`apply`] for rows that are not converted yet).
pub async fn apply_prepared(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    chunk: Option<&UnitChunk>,
    new: &Prepared,
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<Applied> {
    let old = read_old(conn, schema, table, layout, w.kind, chunk, metrics).await?;
    let d = diff(layout, old, new);
    write_diff(
        conn,
        schema,
        table,
        layout,
        &d,
        w,
        metrics,
        &AtomicBool::new(false),
    )
    .await
}

/// [`apply_prepared`] for new rows that still need converting: one copy of `new_rows` for
/// [`prepare`]. A caller with several targets of the same chunk prepares once and calls
/// [`apply_prepared`] per target instead.
pub async fn apply(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    chunk: Option<&UnitChunk>,
    new_rows: &[Vec<Value>],
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<Applied> {
    let new = prepare(layout, new_rows.to_vec());
    apply_prepared(conn, schema, table, layout, chunk, &new, w, metrics).await
}

/// [`apply_prepared`] on a target connection of either family (the unsupervised refresh, M-02):
/// the MySQL arm is [`apply_prepared`] itself; the PostgreSQL arm is the same read-diff-write on
/// the target's own connection - [`crate::target::postgres::PgConn::read_old`], [`diff`],
/// [`crate::target::postgres::PgConn::write_tx`] - the shape `supervisor::write_once` already
/// uses for one op. One dispatch per chunk, never per row; the diff and the batches are the same
/// allocations the MySQL arm makes, so a PostgreSQL chunk costs what a MySQL one does.
pub async fn apply_prepared_on(
    conn: &mut crate::target::Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    chunk: Option<&UnitChunk>,
    new: &Prepared,
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<Applied> {
    match conn {
        crate::target::Conn::Mysql(c) => {
            apply_prepared(c, schema, table, layout, chunk, new, w, metrics).await
        }
        crate::target::Conn::Postgres(c) => {
            let old = c.read_old(schema, table, layout, chunk, metrics).await?;
            let d = diff(layout, old, new);
            if d.changed.is_empty() {
                // Nothing differs: no statement at all, as `write_diff` does for the MySQL family.
                return Ok(Applied {
                    unchanged: d.unchanged_rows,
                    ..Default::default()
                });
            }
            c.write_tx(
                schema,
                table,
                layout,
                &d,
                w,
                metrics,
                &AtomicBool::new(false),
            )
            .await
        }
    }
}

/// Builds a chunk from arbitrary keys (sorted integers when possible).
pub(crate) fn keys_chunk(keys: &[Key]) -> UnitChunk {
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
        None => UnitChunk::Keys(keys.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::analyze::{parse_definition, plan};
    use crate::catalog::{Column, TableInfo};

    fn col(name: &str, data_type: &str, column_type: &str, charset: Option<&str>) -> MvCol {
        MvCol {
            name: name.into(),
            src_name: name.into(),
            column_type: column_type.into(),
            data_type: data_type.into(),
            charset: charset.map(Into::into),
            collation: None,
            nullable: true,
        }
    }

    fn layout(cols: Vec<MvCol>, n_visible: usize, unit_pos: Vec<usize>, primary: bool) -> Layout {
        Layout {
            hidden: (0..cols.len() - n_visible).collect(),
            cols,
            n_visible,
            unit_pos,
            primary,
            lineage: Vec::new(),
            epoch: Vec::new(),
            identity: Vec::new(),
            partial: Vec::new(),
            key_budget: crate::target::MY_KEY_BYTES,
        }
    }

    /// Partial unit sets get an index of their own unless the unit index starts with them.
    #[test]
    fn partial_sets_are_indexed() {
        let cols = vec![
            col("g", "int", "int", None),
            col("o", "int", "int", None),
            col("v", "int", "int", None),
        ];
        let mut l = layout(cols, 3, vec![0, 1], true);
        l.partial = vec![vec![1], vec![0]];
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(sql.contains("PRIMARY KEY (`g`, `o`)"), "{sql}");
        assert!(sql.contains("KEY `_lv_p0` (`o`)"), "{sql}");
        assert!(!sql.contains("_lv_p1"), "a prefix of the unit index: {sql}");
    }

    fn ev_plan(sql: &str) -> ViewPlan {
        let t = TableInfo {
            schema: "db".into(),
            name: "ev".into(),
            columns: vec![
                Column::simple("id", "int"),
                Column::simple("g", "int"),
                Column::new("ts", "timestamp", "timestamp(6)", None, None, None, false),
            ],
            pk: vec![0],
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        let tables: HashMap<_, _> = [(("db".to_string(), "ev".to_string()), Arc::new(t))]
            .into_iter()
            .collect();
        plan(&parse_definition(sql).unwrap(), "db", &tables).unwrap()
    }

    /// TIMESTAMP columns are read as `UNIX_TIMESTAMP` of their own expressions, in front of the
    /// plan's select list (never through a derived table), and the epochs replace the values.
    #[test]
    fn timestamp_columns_read_as_epochs() {
        let p =
            ev_plan("CREATE VIEW v AS SELECT g, MAX(ts) AS mx, COUNT(*) AS n FROM ev GROUP BY g");
        let cols = vec![
            col("g", "int", "int", None),
            col("mx", "timestamp", "timestamp(6)", None),
            col("n", "bigint", "bigint", None),
        ];
        let epoch = epoch_exprs(&p, &cols, 3, &[]);
        assert_eq!(
            epoch,
            vec![(
                1,
                crate::dialect::mysql::item_expr(&p.projection[1]).to_string()
            )]
        );
        assert!(epoch[0].1.starts_with("MAX(") && !epoch[0].1.contains(" AS "));
        let mut l = layout(cols, 3, vec![0], true);
        l.epoch = epoch;
        let plain = p.select_sql(&[], Some("(g) IN (1)"));
        let sql = l.source_sql(&p, Some("(g) IN (1)"));
        assert_eq!(
            sql,
            format!(
                "SELECT UNIX_TIMESTAMP({}) AS `_lv_ts0`, {}",
                l.epoch[0].1,
                plain.strip_prefix("SELECT ").unwrap()
            )
        );
        assert_eq!(
            l.read_list(),
            "`g`, UNIX_TIMESTAMP(`mx`), `n`",
            "materialized rows: epochs in place"
        );
        // 2021-10-31 01:30:00.25 UTC, the second 02:30:00.25 in Europe/Berlin.
        let row = vec![
            Value::Bytes(b"1635643800.250000".to_vec()),
            Value::Int(7),
            Value::Date(2021, 10, 31, 2, 30, 0, 250_000),
            Value::Int(2),
        ];
        assert_eq!(
            l.normalize_source(row),
            vec![
                Value::Int(7),
                Value::Date(2021, 10, 31, 1, 30, 0, 250_000),
                Value::Int(2)
            ]
        );
        assert_eq!(
            l.normalize(vec![Value::Int(7), Value::Int(1635640200), Value::NULL]),
            vec![
                Value::Int(7),
                Value::Date(2021, 10, 31, 0, 30, 0, 0),
                Value::NULL
            ]
        );
        assert_eq!(
            l.normalize(vec![Value::Int(7), Value::Int(0), Value::NULL])[1],
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            "zero TIMESTAMP"
        );

        // Hidden unit columns use the unit expression; select/join views the column itself.
        let p = ev_plan("CREATE VIEW v AS SELECT COUNT(*) AS n FROM ev GROUP BY ts");
        let cols = vec![
            col("n", "bigint", "bigint", None),
            col("_lv_u0", "timestamp", "timestamp(6)", None),
        ];
        assert_eq!(
            epoch_exprs(&p, &cols, 1, &[0]),
            vec![(1, p.unit_exprs[0].clone())]
        );
        let p = ev_plan("CREATE VIEW v AS SELECT id, ts FROM ev");
        let cols = vec![
            col("id", "int", "int", None),
            col("ts", "timestamp", "timestamp(6)", None),
        ];
        assert_eq!(
            epoch_exprs(&p, &cols, 2, &[]),
            vec![(1, p.projection[1].clone())]
        );
        // A TIMESTAMP lineage column has no known expression: no epochs at all.
        let cols = vec![
            col("id", "int", "int", None),
            col("ts", "timestamp", "timestamp(6)", None),
            col("_lv_a1_pk0", "timestamp", "timestamp", None),
        ];
        assert!(epoch_exprs(&p, &cols, 2, &[]).is_empty());
    }

    #[test]
    fn lineage_columns_and_indexes() {
        let mut cols = vec![
            col("id", "int", "int", None),
            col("v", "varchar", "varchar(10)", Some("utf8mb4")),
            col("_lv_u0", "int", "int", None),
            col("_lv_a1_pk0", "int", "int", None),
            col("_lv_a1_pk1", "varchar", "varchar(20)", Some("utf8mb4")),
            col("_lv_a2_pk0", "bigint", "bigint", None),
        ];
        assert_eq!(lineage_keys(&cols, 3), Some(vec![vec![3, 4], vec![5]]));
        assert_eq!(lineage_keys(&cols, 6), Some(vec![]));
        assert_eq!(
            lineage_keys(&cols, 2),
            None,
            "a hidden unit column is no lineage column"
        );
        assert_eq!(lineage_col("_lv_a12_pk3"), Some((12, 3)));
        assert_eq!(lineage_col("_lv_a_pk0"), None);
        assert_eq!(lineage_col("_lv_u0"), None);
        let mut l = layout(cols.clone(), 2, vec![0], true);
        l.hidden = vec![];
        l.unit_pos = vec![0];
        l.lineage = vec![vec![3, 4], vec![5]];
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(
            sql.contains("KEY `_lv_a1` (`_lv_a1_pk0`, `_lv_a1_pk1`)"),
            "{sql}"
        );
        assert!(sql.contains("KEY `_lv_a2` (`_lv_a2_pk0`)"), "{sql}");
        assert!(sql.contains("`_lv_a1_pk0` int NULL INVISIBLE"), "{sql}");
        // Out of order key columns are rejected.
        cols.swap(3, 4);
        assert_eq!(lineage_keys(&cols, 3), None);
    }

    /// Unit indexes within InnoDB's 3072-byte key limit.
    #[test]
    fn unit_index_fits_key_limits() {
        let l = layout(
            vec![
                col("k", "varchar", "varchar(768)", Some("utf8mb4")),
                col("v", "int", "int", None),
            ],
            2,
            vec![0],
            true,
        );
        assert!(l
            .create_sql(Kind::Mysql, "s", "t")
            .contains("PRIMARY KEY (`k`)"));
        let l = layout(
            vec![
                col("k", "varchar", "varchar(1000)", Some("utf8mb4")),
                col("v", "int", "int", None),
            ],
            2,
            vec![0],
            true,
        );
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(!sql.contains("PRIMARY KEY"), "{sql}");
        assert!(sql.contains("KEY `_lv_unit` (`k`(768))"), "{sql}");
        assert!(sql.contains("`k` varchar(1000) NULL"), "{sql}");
        let texts: Vec<MvCol> = (1..=4)
            .map(|i| col(&format!("t{i}"), "text", "text", Some("utf8mb4")))
            .collect();
        let l = layout(texts, 4, vec![0, 1, 2, 3], false);
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(
            sql.contains("KEY `_lv_unit` (`t1`(192), `t2`(192), `t3`(192), `t4`(192))"),
            "{sql}"
        );
        let l = layout(vec![col("j", "json", "json", None)], 1, vec![0], false);
        assert!(
            !l.create_sql(Kind::Mysql, "s", "t").contains("KEY"),
            "JSON units get no index"
        );
        assert!(!l.has_unit_index());
    }

    /// The diff compares JSON columns as stored: a DECIMAL computed into JSON is rewritten
    /// once, not on every refresh; JSON unit keys group by the stored value too.
    #[test]
    fn diff_compares_json_columns_as_stored() {
        let l = layout(
            vec![
                col("g", "int", "int", None),
                col("amts", "json", "json", None),
                col("k", "json", "json", None),
            ],
            3,
            vec![0, 2],
            true,
        );
        let row = |g: i64, amts: &str, k: &str| {
            vec![
                Value::Int(g),
                Value::Bytes(amts.as_bytes().to_vec()),
                Value::Bytes(k.as_bytes().to_vec()),
            ]
        };
        let source = vec![
            row(1, "[10.00, 20.50]", r#"{"x": 1.50}"#),
            row(2, "[3.25]", "[1, 2]"),
        ];
        let stored = vec![
            row(1, "[10.0, 20.5]", r#"{"x": 1.5}"#),
            row(2, "[3.25]", "[1, 2]"),
        ];
        let d = diff(&l, stored.clone(), &prepare(&l, source.clone()));
        assert!(d.changed.is_empty(), "rewrote equal units: {:?}", d.changed);
        // A real change of one unit rewrites that unit only.
        let mut changed = source.clone();
        changed[1] = row(2, "[3.26]", "[1, 2]");
        let d = diff(&l, stored.clone(), &prepare(&l, changed));
        assert_eq!(d.changed.len(), 1);
        assert_eq!(
            d.changed[0][1],
            Datum::Bytes(b"[1,2]".to_vec()),
            "the key binds as canonical JSON"
        );
        assert_eq!(
            d.inserts,
            vec![row(2, "[3.26]", "[1, 2]")],
            "inserts keep the source text"
        );
        // An integer becoming a double is a change of the stored value.
        let d = diff(
            &l,
            stored,
            &prepare(&l, vec![source[0].clone(), row(2, "[3.25]", "[1, 2.0]")]),
        );
        assert_eq!(d.changed.len(), 2, "old [1,2] and new [1,2.0] units");
    }

    /// Row keys: the unit's PRIMARY KEY, else `_lv_row` over the whole identity columns (it
    /// replaces `_lv_unit`, whose columns it starts with); none when a column cannot be indexed
    /// whole, the key is too long, or TIMESTAMPs are read in the session's time zone.
    #[test]
    fn row_keys_and_their_indexes() {
        let mut l = layout(
            vec![
                col("g", "int", "int", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        l.identity = vec![0];
        assert_eq!(l.row_key(), Some(&[0][..]));
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(
            sql.contains("PRIMARY KEY (`g`)") && !sql.contains("UNIQUE"),
            "{sql}"
        );
        // Select/join view: unit + lineage columns.
        let mut l = layout(
            vec![
                col("id", "int", "int", None),
                col("v", "varchar", "varchar(10)", Some("utf8mb4")),
                col("_lv_a1_pk0", "int", "int", None),
                col("_lv_a2_pk0", "varchar", "varchar(20)", Some("utf8mb4")),
            ],
            2,
            vec![0],
            false,
        );
        l.hidden = vec![];
        l.lineage = vec![vec![2], vec![3]];
        l.identity = vec![0, 2, 3];
        assert_eq!(l.row_key(), Some(&[0, 2, 3][..]));
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(
            sql.contains("UNIQUE KEY `_lv_row` (`id`, `_lv_a1_pk0`, `_lv_a2_pk0`)"),
            "{sql}"
        );
        assert!(
            !sql.contains("`_lv_unit`"),
            "_lv_row serves unit filters: {sql}"
        );
        assert!(sql.contains("KEY `_lv_a1` (`_lv_a1_pk0`)"), "{sql}");
        assert!(l.has_unit_index());
        // Nullable grouped keys (no PRIMARY KEY possible): `_lv_row` over the unit.
        let mut l = layout(
            vec![
                col("g", "varchar", "varchar(10)", Some("utf8mb4")),
                col("n", "int", "int", None),
            ],
            2,
            vec![0],
            false,
        );
        l.identity = vec![0];
        assert!(l
            .create_sql(Kind::Mysql, "s", "t")
            .contains("UNIQUE KEY `_lv_row` (`g`)"));
        // A column that can only be indexed by a prefix: no row key, `_lv_unit` stays.
        let mut l = layout(
            vec![
                col("k", "varchar", "varchar(1000)", Some("utf8mb4")),
                col("n", "int", "int", None),
            ],
            2,
            vec![0],
            false,
        );
        l.identity = vec![0];
        assert_eq!(l.row_key(), None);
        let sql = l.create_sql(Kind::Mysql, "s", "t");
        assert!(
            sql.contains("KEY `_lv_unit` (`k`(768))") && !sql.contains("UNIQUE"),
            "{sql}"
        );
        let mut l = layout(
            vec![
                col("t", "text", "text", Some("utf8mb4")),
                col("n", "int", "int", None),
            ],
            2,
            vec![0],
            false,
        );
        l.identity = vec![0];
        assert_eq!(l.row_key(), None);
        // TIMESTAMPs: only when read as epochs (exact UTC values).
        let mut l = layout(
            vec![
                col("ts", "timestamp", "timestamp(6)", None),
                col("n", "int", "int", None),
            ],
            2,
            vec![0],
            true,
        );
        l.identity = vec![0];
        assert_eq!(l.row_key(), None, "local TIMESTAMPs");
        assert!(l
            .create_sql(Kind::Mysql, "s", "t")
            .contains("PRIMARY KEY (`ts`)"));
        l.epoch = vec![(0, "ts".into())];
        assert_eq!(l.row_key(), Some(&[0][..]));
        // No identity: no row key.
        let l = layout(vec![col("v", "int", "int", None)], 1, vec![], false);
        assert_eq!(l.row_key(), None);
    }

    #[test]
    fn upsert_syntax_and_packet_limits() {
        let w = |v: &str, p: u64| {
            WriteOpts::for_server(Kind::Mysql, v, p, 500, DEFAULT_WRITE_BATCH_BYTES)
        };
        assert_eq!(w("8.4.11", 64 << 20).upsert, Upsert::RowAlias);
        assert_eq!(w("26.7.0", 64 << 20).upsert, Upsert::RowAlias);
        assert_eq!(w("8.0.19", 64 << 20).upsert, Upsert::RowAlias);
        assert_eq!(w("8.0.18-log", 64 << 20).upsert, Upsert::Values);
        assert_eq!(w("5.7.44", 64 << 20).upsert, Upsert::Values);
        assert_eq!(
            w("10.11.19-MariaDB-ubu2204-log", 16 << 20).upsert,
            Upsert::Values
        );
        assert_eq!(w("5.5.5-10.6.1-MariaDB", 16 << 20).upsert, Upsert::Values);
        assert_eq!(w("13.0.2-MariaDB", 16 << 20).upsert, Upsert::Values);
        assert_eq!(w("8.4.11", 64 << 20).batch_bytes, 4 << 20);
        assert_eq!(
            w("8.4.11", 1 << 20).batch_bytes,
            512 << 10,
            "half of max_allowed_packet"
        );
        assert_eq!(w("8.4.11", 1024).batch_bytes, 1024, "floor");
    }

    #[test]
    fn upsert_statements() {
        let l = layout(
            vec![
                col("g", "int", "int", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        assert_eq!(
            crate::target::insert_sql(Kind::Mysql, "s", "t", &l, 2, None, None),
            "INSERT INTO `s`.`t` (`g`, `n`) VALUES (?,?),(?,?)"
        );
        assert_eq!(
            crate::target::insert_sql(Kind::Mysql, "s", "t", &l, 1, Some(Upsert::RowAlias), None),
            "INSERT INTO `s`.`t` (`g`, `n`) VALUES (?,?) AS `_lv_new` \
             ON DUPLICATE KEY UPDATE `g` = `_lv_new`.`g`, `n` = `_lv_new`.`n`"
        );
        assert_eq!(
            crate::target::insert_sql(Kind::Mysql, "s", "t", &l, 1, Some(Upsert::Values), None),
            "INSERT INTO `s`.`t` (`g`, `n`) VALUES (?,?) \
             ON DUPLICATE KEY UPDATE `g` = VALUES(`g`), `n` = VALUES(`n`)"
        );
        assert!(
            crate::target::insert_sql(
                Kind::Mysql,
                "s",
                "_LV_NEW",
                &l,
                1,
                Some(Upsert::RowAlias),
                None
            )
            .contains("AS `_lv_new_`"),
            "the row alias differs from the table name"
        );
    }

    #[test]
    fn statement_batches_by_rows_and_bytes() {
        let sizes = [10usize, 10, 10, 50, 10, 200, 10];
        let lens = |b: Vec<&[usize]>| b.iter().map(|x| x.len()).collect::<Vec<_>>();
        assert_eq!(lens(batches(&sizes, 100, 1000, |&b| b)), vec![7]);
        assert_eq!(lens(batches(&sizes, 3, 1000, |&b| b)), vec![3, 3, 1]);
        assert_eq!(
            lens(batches(&sizes, 100, 60, |&b| b)),
            vec![3, 2, 1, 1],
            "a big item alone"
        );
        assert_eq!(lens(batches(&sizes, 100, 1, |&b| b)), vec![1; 7]);
        assert!(batches::<usize>(&[], 10, 10, |&b| b).is_empty());
        let total: usize = batches(&sizes, 2, 25, |&b| b).iter().map(|b| b.len()).sum();
        assert_eq!(total, sizes.len(), "every item exactly once");
    }

    /// The row-level diff of upserting layouts: unchanged rows and units are never written, a
    /// changed row is one upsert, vanished rows are deleted by key, keys with a NULL are deleted
    /// and inserted again, rows move between units and units grow and shrink.
    #[test]
    fn row_level_diffs() {
        // An SPJ layout: unit `id`, value `v`, lineage `_lv_a1_pk0` (NULL: LEFT JOIN unmatched).
        let mut l = layout(
            vec![
                col("id", "int", "int", None),
                col("v", "varchar", "varchar(10)", Some("utf8mb4")),
                col("_lv_a1_pk0", "int", "int", None),
            ],
            2,
            vec![0],
            false,
        );
        l.hidden = vec![];
        l.lineage = vec![vec![2]];
        l.identity = vec![0, 2];
        assert!(l.row_key().is_some());
        let r = |id: i64, v: &str, a: Option<i64>| {
            vec![
                Value::Int(id),
                Value::Bytes(v.as_bytes().to_vec()),
                a.map(Value::Int).unwrap_or(Value::NULL),
            ]
        };
        let k = |id: i64, a: Option<i64>| -> Key {
            Box::new([Datum::Int(id), a.map(Datum::Int).unwrap_or(Datum::Null)])
        };
        let old = vec![
            r(1, "a", Some(10)),
            r(1, "b", Some(11)),
            r(1, "c", Some(12)),
            r(2, "x", None),
            r(3, "y", Some(30)),
        ];
        let sorted = |mut v: Vec<Key>| {
            v.sort();
            v
        };
        // Nothing changed: nothing written.
        let d = diff(&l, old.clone(), &prepare(&l, old.clone()));
        assert!(d.changed.is_empty() && d.inserts.is_empty() && d.delete_keys.is_empty());
        // One row of a 3-row unit changes: one upsert, no delete, unit counted once.
        let mut new = old.clone();
        new[1] = r(1, "B", Some(11));
        let d = diff(&l, old.clone(), &prepare(&l, new));
        assert!(!d.by_unit);
        assert_eq!(d.changed.len(), 1);
        assert_eq!(d.inserts, vec![r(1, "B", Some(11))]);
        assert!(d.delete_keys.is_empty());
        assert_eq!(d.deleted_rows, 0);
        // Row kinds: the changed row replaces its old one in place, the other rows are equal
        // (the unit's two other rows, units 2 and 3).
        assert_eq!((d.updated_rows, d.unchanged_rows), (1, 4));
        // The unit shrinks (12 vanishes) and grows (13 appears); unit 3 vanishes entirely.
        let new = vec![
            r(1, "a", Some(10)),
            r(1, "b", Some(11)),
            r(1, "d", Some(13)),
            r(2, "x", None),
        ];
        let d = diff(&l, old.clone(), &prepare(&l, new));
        assert_eq!(d.changed.len(), 2);
        assert_eq!(d.inserts, vec![r(1, "d", Some(13))]);
        assert_eq!(sorted(d.delete_keys), vec![k(1, Some(12)), k(3, Some(30))]);
        assert_eq!(d.deleted_rows, 2);
        // A row with a NULL in its key changes: delete (null-safe) + insert.
        let mut new = old.clone();
        new[3] = r(2, "X", None);
        let d = diff(&l, old.clone(), &prepare(&l, new));
        assert_eq!(d.delete_keys, vec![k(2, None)]);
        assert_eq!(d.inserts, vec![r(2, "X", None)]);
        assert_eq!(d.deleted_rows, 1);
        // The unmatched row gets a match: its NULL-key row goes, the new key is upserted.
        let mut new = old.clone();
        new[3] = r(2, "x", Some(20));
        let d = diff(&l, old.clone(), &prepare(&l, new));
        assert_eq!(d.delete_keys, vec![k(2, None)]);
        assert_eq!(d.inserts, vec![r(2, "x", Some(20))]);
        // A row moves from unit 3 to unit 1 (another unit's lineage now): both units change.
        let mut new = old.clone();
        new[4] = r(1, "y", Some(30));
        let d = diff(&l, old.clone(), &prepare(&l, new));
        assert_eq!(d.changed.len(), 2);
        assert_eq!(d.delete_keys, vec![k(3, Some(30))]);
        assert_eq!(d.inserts, vec![r(1, "y", Some(30))]);
        // A collation-equal key with other bytes is a different key: the old one is deleted
        // first, then the new one upserted (see `Diff`).
        let mut l2 = layout(
            vec![
                col("g", "varchar", "varchar(10)", Some("utf8mb4")),
                col("n", "int", "int", None),
            ],
            2,
            vec![0],
            true,
        );
        l2.identity = vec![0];
        let g = |s: &str, n: i64| vec![Value::Bytes(s.as_bytes().to_vec()), Value::Int(n)];
        let d = diff(&l2, vec![g("a", 1)], &prepare(&l2, vec![g("A", 1)]));
        assert_eq!(
            d.delete_keys,
            vec![Key::from([Datum::Bytes(b"a".to_vec())])]
        );
        assert_eq!(d.inserts, vec![g("A", 1)]);
        // Rows sharing a key with a NULL are legal (NULLs never conflict) and inserted as they are.
        let d = diff(
            &l,
            vec![],
            &prepare(&l, vec![r(5, "a", None), r(5, "b", None)]),
        );
        assert_eq!(d.inserts.len(), 2);
        // Without a row key the changed units are rewritten whole, as before.
        let mut l3 = l.clone();
        l3.identity = vec![];
        let mut new = old.clone();
        new[1] = r(1, "B", Some(11));
        let d = diff(&l3, old.clone(), &prepare(&l3, new));
        assert!(d.by_unit && d.delete_keys.is_empty());
        assert_eq!((d.inserts.len(), d.deleted_rows), (3, 3));
    }

    /// #331: the write order does not depend on the order the rows were read in, also for a view
    /// whose most restrictive target (PostgreSQL) cannot hold the row key. The key budget is the
    /// view's, so no MySQL target upserts by a row key the diff does not know (`row_key_for` is
    /// `Some` for a family only when the view-wide `row_key` is): the diff is per unit for all
    /// of them, and its rows still come out in one total order.
    #[test]
    fn writes_are_ordered_in_a_mixed_family_view() {
        let mut l = layout(
            vec![
                col("u", "int", "int", None),
                col("s", "varchar", "varchar(700)", Some("utf8mb4")),
            ],
            2,
            vec![0],
            false,
        );
        l.hidden = vec![];
        l.identity = vec![0, 1];
        l.key_budget = crate::target::PG_KEY_BYTES;
        assert!(l.row_key().is_none(), "too wide for the PostgreSQL index");
        assert!(
            l.row_key_for(Kind::Mysql).is_none(),
            "the same budget and widths: no family upserts where the view has no row key"
        );
        let r = |u: i64, s: &str| vec![Value::Int(u), Value::Bytes(s.as_bytes().to_vec())];
        let rows: Vec<Vec<Value>> = (0..30)
            .map(|i| r(i % 3, &format!("v{:02}", (i * 7) % 30)))
            .collect();
        let mut rev = rows.clone();
        rev.reverse();
        let a = diff(&l, Vec::new(), &prepare(&l, rows));
        let b = diff(&l, Vec::new(), &prepare(&l, rev));
        assert!(a.by_unit);
        assert_eq!(a.inserts, b.inserts, "one order for every writer");
        assert!(a.inserts.windows(2).all(|w| {
            let k = |x: &Vec<Value>| match &x[0] {
                Value::Int(i) => *i,
                _ => unreachable!(),
            };
            k(&w[0]) <= k(&w[1])
        }));
    }

    /// The DDL entry of a MySQL-family target: `Layout` renders nothing of its own, the family
    /// is the caller's (the lead's closure of S4's staged seam).
    #[test]
    fn ddl_entry_renders_the_mysql_family() {
        let l = layout(vec![col("k", "int", "int", None)], 1, vec![], false);
        let sql = l.create_sql_marked(Kind::Mysql, "s", "t", Some("lazyview:v1 view=t def=1"));
        assert!(sql.starts_with("CREATE TABLE `s`.`t` ("), "{sql}");
        assert!(
            sql.contains("ENGINE=InnoDB") && sql.contains("COMMENT='lazyview:v1 view=t def=1'"),
            "{sql}"
        );
    }

    /// The same layout on a PostgreSQL target: the target's types, one statement per index, the
    /// marker as a separate `COMMENT ON TABLE`, and no MySQL clause anywhere.
    #[test]
    fn ddl_entry_renders_the_postgres_family() {
        let mut l = layout(
            vec![col("k", "bigint", "bigint unsigned", None)],
            1,
            vec![],
            false,
        );
        l.identity = vec![0];
        l.key_budget = crate::target::PG_KEY_BYTES;
        let sql = l.create_sql_marked(Kind::Postgres, "s", "t", Some("lazyview:v1 view=t def=1"));
        assert!(sql.starts_with("CREATE TABLE \"s\".\"t\" ("), "{sql}");
        assert!(sql.contains("\"k\" numeric(20,0) NULL"), "{sql}");
        assert!(
            sql.contains("CREATE UNIQUE INDEX \"t_lv_row\" ON \"s\".\"t\" (\"k\");"),
            "{sql}"
        );
        assert!(
            sql.ends_with("COMMENT ON TABLE \"s\".\"t\" IS 'lazyview:v1 view=t def=1';"),
            "{sql}"
        );
        for absent in ["ENGINE", "CHARSET", "INVISIBLE", "CHARACTER SET"] {
            assert!(!sql.contains(absent), "{absent}: {sql}");
        }
    }

    /// A plan over a table of the caller's columns (a JSON unit needs a `json` column).
    fn plan_over(name: &str, cols: Vec<Column>, sql: &str) -> ViewPlan {
        let t = TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols,
            pk: vec![0],
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        let tables: HashMap<_, _> = [(("db".to_string(), name.to_string()), Arc::new(t))]
            .into_iter()
            .collect();
        plan(&parse_definition(sql).unwrap(), "db", &tables).unwrap()
    }

    /// The cursor is the plan's own units over the plan's own FROM clause, in the unit's order:
    /// nothing else may enter the statement, or a chunk of the sweep reads another view's rows.
    #[test]
    fn the_units_cursor_streams_the_plan_s_own_units() {
        let p = ev_plan("CREATE VIEW v AS SELECT id, g FROM ev WHERE g > 0");
        assert!(p.is_incremental(), "{:?}", p.mode);
        let l = layout(
            vec![col("id", "int", "int", None), col("g", "int", "int", None)],
            2,
            vec![0],
            true,
        );
        let (sql, read) = units_sql(&p, &l).unwrap();
        assert_eq!(read, vec![UnitRead::Raw]);
        assert!(
            sql.starts_with(&format!(
                "SELECT DISTINCT {} FROM {}",
                p.unit_exprs[0], p.from_sql
            )),
            "{sql}"
        );
        assert!(
            sql.contains(&format!("WHERE {}", p.where_sql.clone().unwrap())),
            "the plan's own filter: {sql}"
        );
        // (The order is the select list: `SELECT DISTINCT` rejects an ORDER BY item it lacks.)
        assert!(
            sql.ends_with(&format!(" ORDER BY {}", p.unit_exprs[0])),
            "{sql}"
        );
        for absent in ["GROUP BY", "HAVING", "LIMIT"] {
            assert!(!sql.contains(absent), "{absent}: {sql}");
        }
    }

    /// A grouped view's units are the units of its grouped rows: the cursor keeps the plan's
    /// `GROUP BY` and `HAVING`, so a unit the view holds no rows for is not in the list.
    #[test]
    fn the_units_cursor_keeps_the_group_and_the_having() {
        let p = ev_plan(
            "CREATE VIEW v AS SELECT g, COUNT(*) AS n FROM ev GROUP BY g HAVING COUNT(*) > 2",
        );
        assert!(p.is_incremental(), "{:?}", p.mode);
        let l = layout(
            vec![
                col("g", "int", "int", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        let (sql, read) = units_sql(&p, &l).unwrap();
        assert_eq!(read, vec![UnitRead::Raw]);
        assert!(
            sql.starts_with(&format!("SELECT DISTINCT {} FROM", p.unit_exprs[0])),
            "{sql}"
        );
        assert!(sql.contains("GROUP BY"), "{sql}");
        assert!(sql.contains("HAVING COUNT(*) > 2"), "{sql}");
    }

    /// A TIMESTAMP unit is the epoch `Layout::source_sql` reads its epoch columns as, and the key
    /// is the one a chunk read of the same unit gives (a chunk filter binds those keys).
    #[test]
    fn the_units_cursor_reads_a_timestamp_unit_as_an_epoch() {
        let p = ev_plan("CREATE VIEW v AS SELECT ts, COUNT(*) AS n FROM ev GROUP BY ts");
        assert!(p.is_incremental(), "{:?}", p.mode);
        let mut l = layout(
            vec![
                col("ts", "timestamp", "timestamp(6)", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        l.epoch = vec![(0, p.unit_exprs[0].clone())];
        let (sql, read) = units_sql(&p, &l).unwrap();
        assert_eq!(read, vec![UnitRead::Epoch]);
        assert!(
            sql.contains(&format!("UNIX_TIMESTAMP({})", p.unit_exprs[0])),
            "{sql}"
        );
        let epoch = Value::Int(1_700_000_000);
        let row = vec![
            crate::dialect::mysql::epoch_to_utc(epoch.clone()),
            Value::Int(3),
        ];
        assert_eq!(
            unit_datums(&read, &unit_types(&l), vec![epoch]).to_vec(),
            to_datums(&l, &row)[..1].to_vec(),
            "the cursor's key is the key of a chunk read"
        );
    }

    /// A JSON unit of the MySQL family comes back as the text [`to_datums`] canonicalizes:
    /// `SELECT DISTINCT` cannot group a JSON column.
    #[test]
    fn the_units_cursor_casts_a_json_unit() {
        let p = plan_over(
            "items",
            vec![Column::simple("id", "int"), Column::simple("doc", "json")],
            "CREATE VIEW v AS SELECT doc, COUNT(*) AS n FROM items GROUP BY doc",
        );
        assert!(p.is_incremental(), "{:?}", p.mode);
        let l = layout(
            vec![
                col("doc", "json", "json", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        let (sql, read) = units_sql(&p, &l).unwrap();
        assert_eq!(read, vec![UnitRead::Json]);
        assert!(
            sql.contains(&format!("CAST({} AS CHAR)", p.unit_exprs[0])),
            "{sql}"
        );
        let doc = Value::Bytes(b"{\"b\": 1, \"a\": 2}".to_vec());
        assert_eq!(
            unit_datums(&read, &unit_types(&l), vec![doc.clone()]).to_vec(),
            to_datums(&l, &[doc])[..1].to_vec(),
            "the cursor's key is the key of a chunk read"
        );
    }

    /// The plans the cursor cannot stream: no unit to list, or a row set that is not a set of
    /// units. Their whole refresh stays the single read it is today.
    #[test]
    fn the_units_cursor_refuses_the_plans_it_cannot_stream() {
        // The GLOBAL view: one row, no unit.
        let p = ev_plan("CREATE VIEW v AS SELECT COUNT(*) AS n FROM ev");
        let l = layout(vec![col("n", "bigint", "bigint", None)], 1, vec![], true);
        assert!(units_sql(&p, &l).is_err(), "no unit: {:?}", p.mode);
        // A top-N: its rows are the first N, not every unit; a chunk of a wider unit list would
        // delete the rows the view keeps.
        let p = ev_plan("CREATE VIEW v AS SELECT id, g FROM ev ORDER BY g LIMIT 5");
        let l = layout(
            vec![col("id", "int", "int", None), col("g", "int", "int", None)],
            2,
            vec![0],
            true,
        );
        assert!(units_sql(&p, &l).is_err(), "top-N: {:?}", p.mode);
        // A layout that is not the plan's.
        let p = ev_plan("CREATE VIEW v AS SELECT id, g FROM ev");
        let l = layout(
            vec![col("id", "int", "int", None), col("g", "int", "int", None)],
            2,
            vec![0, 1],
            true,
        );
        assert!(units_sql(&p, &l).is_err(), "two units for one expression");
    }

    /// A chunk is a chunk filter: at least one key, at most what its parameters allow.
    #[test]
    fn chunk_limit_clamps_to_the_parameter_budget() {
        assert_eq!(chunk_limit(1, 0), 1);
        assert_eq!(chunk_limit(1, 500), 500);
        assert_eq!(chunk_limit(1, usize::MAX), MAX_PARAMS);
        assert_eq!(chunk_limit(4, usize::MAX), MAX_PARAMS / 4);
    }

    /// The target's cursor lists the view's own unit columns and nothing else: no plan `WHERE`
    /// (the target holds what the view holds), the unit's own order, the table quoted.
    #[test]
    fn the_target_cursor_lists_the_view_s_own_unit_columns() {
        let p = ev_plan("CREATE VIEW v AS SELECT id, g FROM ev WHERE g > 0");
        let l = layout(
            vec![col("id", "int", "int", None), col("g", "int", "int", None)],
            2,
            vec![0],
            true,
        );
        assert!(streamable(&p, &l).is_ok());
        let (sql, read) = target_units_sql(&l, "db", "v_mv");
        assert_eq!(read, vec![UnitRead::Raw]);
        let u = l.unit_cols().join(", ");
        assert_eq!(
            sql,
            format!(
                "SELECT DISTINCT {u} FROM {}.{} ORDER BY {u}",
                q("db"),
                q("v_mv")
            )
        );
        // (The plan's filter is not in it: this is the units the view holds, vanished ones included
        // — they are the reason the pass exists.)
        for absent in ["WHERE", "GROUP BY", "HAVING", "LIMIT"] {
            assert!(!sql.contains(absent), "{absent}: {sql}");
        }
        // A view whose rows are not a set of units has no target list either: a chunk of a wider
        // unit list would delete the rows the view keeps.
        let top = ev_plan("CREATE VIEW v AS SELECT id, g FROM ev ORDER BY g LIMIT 5");
        assert!(streamable(&top, &l).is_err(), "top-N");
        let g = ev_plan("CREATE VIEW v AS SELECT COUNT(*) AS n FROM ev");
        let lg = layout(vec![col("n", "bigint", "bigint", None)], 1, vec![], true);
        assert!(streamable(&g, &lg).is_err(), "no unit");
    }

    /// A TIMESTAMP unit of the target is read as an epoch and a JSON unit as text: the very
    /// expressions [`read_old`] reads its rows with, so a target cursor's key is the key of the
    /// rows `read_old` answers for it and the key a chunk read of the source gives as well.
    #[test]
    fn the_target_cursor_reads_a_unit_column_as_read_old_reads_it() {
        let mut l = layout(
            vec![
                col("g", "timestamp", "timestamp", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        l.epoch = vec![(0, "`g`".to_string())];
        let (sql, read) = target_units_sql(&l, "db", "v_mv");
        assert_eq!(read, vec![UnitRead::Epoch]);
        assert!(
            sql.contains(&format!("UNIX_TIMESTAMP({})", l.unit_cols()[0])),
            "{sql}"
        );
        let l = layout(
            vec![
                col("doc", "json", "json", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        let (sql, read) = target_units_sql(&l, "db", "v_mv");
        assert_eq!(read, vec![UnitRead::Json]);
        assert!(
            sql.contains(&format!("CAST({} AS CHAR)", l.unit_cols()[0])),
            "`SELECT DISTINCT` cannot group a JSON column: {sql}"
        );
    }

    /// The vanished-unit read is the units cursor with the chunk's filter AND-ed onto the plan's
    /// own `WHERE`: the plan's FROM clause and nothing else, its `GROUP BY` and `HAVING` kept (a
    /// unit the grouped result drops has no rows in the view), the keys only.
    #[test]
    fn the_vanished_read_is_the_units_cursor_plus_the_chunk_filter() {
        let p = ev_plan(
            "CREATE VIEW v AS SELECT g, COUNT(*) AS n FROM ev WHERE g > 0 GROUP BY g \
             HAVING COUNT(*) > 2",
        );
        assert!(p.is_incremental(), "{:?}", p.mode);
        let l = layout(
            vec![
                col("g", "int", "int", None),
                col("n", "bigint", "bigint", None),
            ],
            2,
            vec![0],
            true,
        );
        let (cols, read) = unit_cols(&p, &l);
        assert_eq!(read, vec![UnitRead::Raw]);
        let build = |c: &UnitChunk| {
            let (f, ps) = c.filter_typed(&p.unit_exprs, &l.unit_kinds());
            let sql = distinct_units_sql(
                &cols,
                &p.from_sql,
                p.where_sql.as_deref(),
                p.group_sql.as_deref(),
                p.having_sql.as_deref(),
                Some(&f),
            );
            (sql, f, ps)
        };
        // The integer fast path: literals, no parameters, and one bounded index lookup per unit.
        let ints = UnitChunk::Ints(vec![1, 2, 9]);
        let (sql, f, params) = build(&ints);
        assert!(
            sql.starts_with(&format!("SELECT DISTINCT {} FROM {}", cols[0], p.from_sql)),
            "{sql}"
        );
        assert!(
            sql.contains(&format!(
                "WHERE ({}) AND ({})",
                p.where_sql.clone().unwrap(),
                f
            )),
            "the plan's filter and the chunk's, neither alone: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "{sql}");
        assert!(sql.contains("HAVING COUNT(*) > 2"), "{sql}");
        assert!(
            sql.ends_with(&format!(" ORDER BY {}", cols[0])),
            "the order is the select list: {sql}"
        );
        assert!(params.is_empty(), "an Ints chunk binds nothing: {params:?}");
        // A keys chunk binds a parameter per column of every key; a NULL key is a literal, and a
        // key whose text the collation matches is the server's to judge, never ours.
        let keys = UnitChunk::Keys(vec![
            Key::from([Datum::Bytes(b"a".to_vec())]),
            Key::from([Datum::Null]),
        ]);
        let (sql, f, params) = build(&keys);
        assert!(
            sql.contains(&format!(
                "WHERE ({}) AND ({})",
                p.where_sql.clone().unwrap(),
                f
            )),
            "{sql}"
        );
        assert!(f.contains("IS NULL"), "the NULL key: {f}");
        assert_eq!(params.len(), 1, "the NULL is a literal: {params:?}");
    }
}
