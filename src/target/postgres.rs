//! PostgreSQL as a target: the statements of a `postgres://` target DSN. Every function here is
//! a stub until the PR that lands its real form (T1-T5); none is reachable before
//! [`crate::target::Kind::from_dsn`] accepts a PostgreSQL scheme.

use anyhow::Result;
use mysql_async::{Conn, Opts, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::mv::{Layout, WriteOpts};

/// `INSERT ... ON CONFLICT (row key) DO UPDATE` — PR T4 replaces this stub with the real
/// statement. Unreachable today: a PostgreSQL target DSN is rejected at [`Kind::from_dsn`].
///
/// [`Kind::from_dsn`]: crate::target::Kind::from_dsn
pub(crate) fn insert_sql(
    _schema: &str,
    _table: &str,
    _layout: &Layout,
    _n: usize,
    _key: Option<&[usize]>,
) -> String {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// The `$n` form of the unit filter — PR T4 replaces this stub (no `CAST(? AS JSON)`: a jsonb
/// column compares to an untyped parameter by jsonb equality). Unreachable today: a PostgreSQL
/// target DSN is rejected at [`Kind::from_dsn`].
///
/// [`Kind::from_dsn`]: crate::target::Kind::from_dsn
pub(crate) fn unit_filter(
    _chunk: &UnitChunk,
    _cols: &[String],
    _kinds: &[UnitKind],
) -> (String, Vec<Value>) {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `CREATE TABLE` + one `CREATE INDEX` per index + `COMMENT ON TABLE` — PR T3 lands the real
/// DDL (types from T2's map, schema-wide index names, the 63-byte cap). Unreachable today.
pub(crate) fn create_table_sql(
    _layout: &Layout,
    _schema: &str,
    _table: &str,
    _marker: Option<&str>,
) -> String {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `relkind` from `pg_class` — PR T3. Unreachable today.
pub(crate) async fn table_kind(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
) -> Result<Option<String>> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `format_type` from `pg_attribute` — PR T3. Unreachable today.
pub(crate) async fn table_columns(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
) -> Result<Vec<(String, String)>> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// Exact-name, case-insensitive-type comparison — PR T3. Unreachable today.
pub(crate) fn layout_matches(_layout: &Layout, _existing: &[(String, String)]) -> bool {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// Unique index by column list over `pg_index` — PR T3. Unreachable today.
pub(crate) async fn has_row_index(
    _conn: &mut Conn,
    _schema: &str,
    _table: &str,
    _layout: &Layout,
) -> Result<bool> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `obj_description` — PR T3. Unreachable today.
pub(crate) async fn table_marker(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
) -> Result<Option<String>> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// Drop and create the shadow — PR T3.4. Unreachable today.
pub(crate) async fn create_shadow(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
    _layout: &Layout,
    _marker: &str,
) -> Result<String> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// The transactional swap (rename + index renames + drop in one `simple_query`) — PR T4.6.
/// Unreachable today.
pub(crate) async fn swap(_conn: &mut Conn, _schema: &str, _name: &str) -> Result<()> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `DROP TABLE IF EXISTS` — PR T4. Unreachable today.
pub(crate) async fn drop_table(_conn: &mut Conn, _schema: &str, _name: &str) -> Result<()> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// The five `SET` statements of a PostgreSQL session — PR T1.4 lands them (`TimeZone='UTC'`,
/// `DateStyle='ISO'`, `bytea_output='hex'`, `extra_float_digits=1`, `lock_timeout`).
/// Unreachable today.
pub(crate) fn session_setup(_lock_wait_s: u64) -> Vec<String> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// Applies [`session_setup`] to a fresh connection — PR T1.2. Unreachable today.
pub(crate) async fn init_session(_conn: &mut Conn, _lock_wait_s: u64) -> Result<()> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `WriteOpts` from `server_version_num` (no packet cap, `Upsert::OnConflict`) — PR T1.5.
/// Unreachable today.
pub(crate) async fn write_opts(
    _conn: &mut Conn,
    _kind: crate::target::Kind,
    _batch_rows: usize,
    _batch_bytes: usize,
) -> Result<WriteOpts> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}

/// `SELECT pg_terminate_backend($1)` on a fresh connection and the `pg_stat_activity` poll —
/// PR T5.2 lands it. Unreachable today.
pub(crate) async fn fence(_opts: &Opts, _id: u32) -> Result<()> {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}
