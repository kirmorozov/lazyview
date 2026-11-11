//! PostgreSQL as a target: the statements and the connection of a `postgres://` target DSN.
//! The connect level (PR T1) is real: [`PgConn`], its session ([`session_setup`]) and its
//! [`WriteOpts`]. The statement functions below are stubs until the PR that lands their real
//! form (T2-T5); the write path of `crate::mv` and the engine reject a PostgreSQL target before
//! any of them is reached (their `Kind::Postgres` arms are unreachable in this PR).

use anyhow::Result;
use mysql_async::{Conn, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::mv::{Layout, Upsert, WriteOpts};

mod connect;

/// One PostgreSQL target connection: the driver's `Client`, the backend `pid` (read once at
/// connect with `SELECT pg_backend_pid()`, so the zombie fence of PR T5 kills exactly this
/// backend) and the DSN it came from.
pub struct PgConn {
    pub(crate) client: tokio_postgres::Client,
    /// Read once at connect; `pg_terminate_backend` (the zombie fence) uses it when PR T5
    /// lands the pool that hands an op's id to the fence.
    #[allow(dead_code)]
    pub(crate) pid: i32,
    pub(crate) dsn: String,
}

impl PgConn {
    /// [`WriteOpts`] from `server_version_num` (T1.5): no packet limit, so batching is bounded
    /// by rows and bytes only (the MySQL `max_packet/2` cap is skipped) and upserts are
    /// `ON CONFLICT (row key) DO UPDATE` (rendered by T4).
    pub(crate) async fn write_opts(
        &self,
        kind: crate::target::Kind,
        batch_rows: usize,
        batch_bytes: usize,
    ) -> Result<WriteOpts> {
        let version: i64 = self
            .client
            .query_one("SELECT current_setting('server_version_num')::int8", &[])
            .await?
            .get(0);
        tracing::debug!(
            dsn = %self.dsn,
            server_version_num = version,
            "connected to a PostgreSQL target"
        );
        Ok(WriteOpts {
            batch_rows: batch_rows.max(1),
            batch_bytes: batch_bytes.max(1024),
            upsert: Upsert::OnConflict,
            kind,
        })
    }
}

/// `INSERT ... ON CONFLICT (row key) DO UPDATE` — PR T4 replaces this stub with the real
/// statement. Unreachable in this PR: the engine rejects a PostgreSQL write before T4.
pub(crate) fn insert_sql(
    _schema: &str,
    _table: &str,
    _layout: &Layout,
    _n: usize,
    _key: Option<&[usize]>,
) -> String {
    unreachable!("PostgreSQL writes land in PR T4")
}

/// The `$n` form of the unit filter — PR T4 replaces this stub (no `CAST(? AS JSON)`: a jsonb
/// column compares to an untyped parameter by jsonb equality). Unreachable in this PR.
pub(crate) fn unit_filter(
    _chunk: &UnitChunk,
    _cols: &[String],
    _kinds: &[UnitKind],
) -> (String, Vec<Value>) {
    unreachable!("PostgreSQL writes land in PR T4")
}

/// `CREATE TABLE` + one `CREATE INDEX` per index + `COMMENT ON TABLE` — PR T3 lands the real
/// DDL (types from T2's map, schema-wide index names, the 63-byte cap). Unreachable today.
pub(crate) fn create_table_sql(
    _layout: &Layout,
    _schema: &str,
    _table: &str,
    _marker: Option<&str>,
) -> String {
    unreachable!("PostgreSQL DDL lands in PR T3")
}

/// `relkind` from `pg_class` — PR T3. Unreachable today.
pub(crate) async fn table_kind(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
) -> Result<Option<String>> {
    unreachable!("PostgreSQL metadata reads land in PR T3")
}

/// `format_type` from `pg_attribute` — PR T3. Unreachable today.
pub(crate) async fn table_columns(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
) -> Result<Vec<(String, String)>> {
    unreachable!("PostgreSQL metadata reads land in PR T3")
}

/// Exact-name, case-insensitive-type comparison — PR T3. Unreachable today.
pub(crate) fn layout_matches(_layout: &Layout, _existing: &[(String, String)]) -> bool {
    unreachable!("PostgreSQL metadata reads land in PR T3")
}

/// Unique index by column list over `pg_index` — PR T3. Unreachable today.
pub(crate) async fn has_row_index(
    _conn: &mut Conn,
    _schema: &str,
    _table: &str,
    _layout: &Layout,
) -> Result<bool> {
    unreachable!("PostgreSQL metadata reads land in PR T3")
}

/// `obj_description` — PR T3. Unreachable today.
pub(crate) async fn table_marker(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
) -> Result<Option<String>> {
    unreachable!("PostgreSQL metadata reads land in PR T3")
}

/// Drop and create the shadow — PR T3.4. Unreachable today.
pub(crate) async fn create_shadow(
    _conn: &mut Conn,
    _schema: &str,
    _name: &str,
    _layout: &Layout,
    _marker: &str,
) -> Result<String> {
    unreachable!("PostgreSQL DDL lands in PR T3")
}

/// The transactional swap (rename + index renames + drop in one `simple_query`) — PR T4.6.
/// Unreachable today.
pub(crate) async fn swap(_conn: &mut Conn, _schema: &str, _name: &str) -> Result<()> {
    unreachable!("PostgreSQL writes land in PR T4")
}

/// `DROP TABLE IF EXISTS` — PR T4. Unreachable today.
pub(crate) async fn drop_table(_conn: &mut Conn, _schema: &str, _name: &str) -> Result<()> {
    unreachable!("PostgreSQL writes land in PR T4")
}

/// Session settings of every PostgreSQL target connection: values are stored and read back in
/// the forms the writer and the reader of this module agree on (`TimeZone='UTC'` so a
/// `timestamptz` text is an exact UTC datetime, like MySQL's `+00:00` session —
/// src/mv.rs:1107; `DateStyle='ISO'` and `bytea_output='hex'` and `extra_float_digits=1` so
/// the text forms are stable even under an `ALTER ROLE ... SET`; `lock_timeout` bounds a
/// swap behind a reader, the analogue of `lock_wait_timeout` (src/mv.rs:1077).
pub(crate) fn session_setup(lock_wait_s: u64) -> Vec<String> {
    vec![
        "SET TimeZone = 'UTC'".into(),
        "SET DateStyle = 'ISO'".into(),
        "SET bytea_output = 'hex'".into(),
        "SET extra_float_digits = 1".into(),
        format!("SET lock_timeout = '{}s'", lock_wait_s.max(1)),
    ]
}

/// Applies [`session_setup`] to a fresh connection — the pool's `setup` of PR T5 runs the same
/// statements. The direct connection applies them inside [`PgConn`]::connect. Unreachable in
/// this PR through the MySQL-typed dispatch of `crate::target::init_session`.
pub(crate) async fn init_session(_conn: &mut Conn, _lock_wait_s: u64) -> Result<()> {
    unreachable!("the PostgreSQL pool lands in PR T5")
}

/// [`WriteOpts`] from `server_version_num` (no packet cap, `Upsert::OnConflict`). The engine's
/// connection setup calls the real inherent method on [`PgConn`]; this arm of the MySQL-typed
/// dispatch stays unreachable.
pub(crate) async fn write_opts(
    _conn: &mut Conn,
    _kind: crate::target::Kind,
    _batch_rows: usize,
    _batch_bytes: usize,
) -> Result<WriteOpts> {
    unreachable!("the engine reads PostgreSQL WriteOpts from the PgConn")
}

/// `SELECT pg_terminate_backend($1)` on a fresh connection and the `pg_stat_activity` poll —
/// PR T5.2 lands it. Unreachable today (the PostgreSQL pool is T5 too).
pub(crate) async fn fence(_opts: &crate::target::Opts, _id: u32) -> Result<()> {
    unreachable!("the PostgreSQL pool and fence land in PR T5")
}

#[cfg(test)]
mod tests {
    use super::session_setup;

    #[test]
    fn session_setup_lists_the_five_settings() {
        let s = session_setup(30);
        assert_eq!(s.len(), 5);
        assert_eq!(s[0], "SET TimeZone = 'UTC'");
        assert_eq!(s[1], "SET DateStyle = 'ISO'");
        assert_eq!(s[2], "SET bytea_output = 'hex'");
        assert_eq!(s[3], "SET extra_float_digits = 1");
        assert_eq!(s[4], "SET lock_timeout = '30s'");
        // The analogue of MySQL's `lock_wait_timeout` never waits zero seconds.
        assert_eq!(session_setup(0)[4], "SET lock_timeout = '1s'");
    }
}
