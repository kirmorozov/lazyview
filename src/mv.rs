//! Materialized tables: layout (from a probe view on the source), shadow builds by streaming
//! non-locking reads, per-unit diff refreshes and atomic swaps.
//!
//! Sources are only ever read with plain SELECTs (never INSERT ... SELECT), so the service takes
//! no locks on source rows; all writes go to the target tables.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row, Value};

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
#[allow(clippy::too_many_arguments)]
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
                d.inserts.extend(n.iter().map(|r| r.1.clone()));
            }
            Some(key) => diff_rows(key, o, n, &mut d),
        }
    }
    d
}

/// The rows of one changed unit, compared per row key (see [`diff`]).
fn diff_rows(key: &[usize], old: Vec<&Row2>, new: Vec<&Row2>, d: &mut Diff) {
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
        d.inserts.extend(n.iter().map(|r| r.1.clone()));
    }
    for (k, o) in old {
        d.delete_keys.push(k);
        d.deleted_rows += o.len() as u64;
    }
}

/// Writes a diff in one transaction (see [`Diff`]). `began` is set once the transaction has
/// been started on the server (from then on a lost connection may still commit: see the
/// supervisor's zombie fence).
#[allow(clippy::too_many_arguments)]
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

/// The MySQL transaction of [`write_diff`] (the family's own arm; the PostgreSQL arm arrives
/// with its transaction in PR T4).
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
    let mut tx = conn.start_transaction(Default::default()).await?;
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
            &mut tx,
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
        &mut tx, schema, table, layout, &d.inserts, w, !d.by_unit, metrics,
    )
    .await?;
    tx.commit().await?;
    Metrics::add(&metrics.units_changed, out.units_changed);
    Ok(out)
}

/// Brings `table` in line with `new_rows` for the units in `chunk` (all rows when `None`),
/// writing only units whose rows differ.
#[allow(clippy::too_many_arguments)]
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
    let old = read_old(conn, schema, table, layout, w.kind, chunk, metrics).await?;
    let new = prepare(layout, new_rows.to_vec());
    let d = diff(layout, old, &new);
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
}
