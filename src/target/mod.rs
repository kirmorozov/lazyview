//! How lazyview talks to one target instance: the family discriminator and the facts a
//! connection's statements are built from. Dispatched once per target connection: every
//! target-facing function in `crate::mv` matches on [`Kind`] at its top and calls one
//! function of the family's module. No trait objects, nothing on a per-row path.

use anyhow::{bail, Result};
use mysql_async::{Conn, Opts, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::mv::{Layout, Upsert, WriteOpts};

pub mod mysql;
pub mod postgres;

/// The family of one target instance. A source is always MySQL-family (`crate::config::Flavor`
/// is the source's product; this is the target's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// MySQL, MariaDB, Percona: the statements of [`crate::mv`] today.
    Mysql,
    /// PostgreSQL, from a `postgres://` target DSN (`src/target/postgres.rs`).
    Postgres,
}

impl Kind {
    /// The family a target DSN names. Only the scheme decides; the family module parses the
    /// rest. `postgres://` and `postgresql://` are a PostgreSQL target; `mysql://` and
    /// `mariadb://` a MySQL-family one; anything else is an error. A *source* DSN is never a
    /// PostgreSQL one (`LAZYVIEW_DSN` rejects it in `crate::config`).
    pub fn from_dsn(dsn: &str) -> Result<Kind> {
        let scheme = dsn
            .split_once("://")
            .map(|(s, _)| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        match scheme.as_str() {
            "mysql" | "mariadb" => Ok(Kind::Mysql),
            // Until src/target/postgres.rs lands, a PostgreSQL target cannot be reached.
            "postgres" | "postgresql" => {
                bail!("PostgreSQL targets are not supported yet (postgres:// DSN)")
            }
            _ => bail!(
                "invalid target DSN: scheme `{scheme}` is not mysql://, mariadb:// or postgres://"
            ),
        }
    }
}

/// Ownership marker prefix of the comment of every table lazyview creates (see [`marker`]).
pub const MARKER_PREFIX: &str = "lazyview:v1 ";

/// Ownership marker for the tables of view `view` built from definition `def_id`, stored as the
/// table comment: `lazyview:v1 view=<name> def=<id>`, the name percent-encoded (`%`, quotes,
/// backslashes, spaces and control characters), so the comment needs no escaping.
/// Family-independent text; only where it is stored differs (a MySQL table comment, a
/// PostgreSQL `COMMENT ON TABLE`).
pub fn marker(view: &str, def_id: u64) -> String {
    let mut name = String::new();
    for c in view.chars() {
        if matches!(c, '%' | '\'' | '"' | '\\' | ' ' | '`') || c.is_control() {
            let mut buf = [0u8; 4];
            for b in c.encode_utf8(&mut buf).bytes() {
                name.push_str(&format!("%{b:02X}"));
            }
        } else {
            name.push(c);
        }
    }
    format!("{MARKER_PREFIX}view={name} def={def_id}")
}

/// Name of the shadow table a new version is built into (≤ 64 chars).
pub fn shadow_name(name: &str) -> String {
    side_name(name, "__lv_new")
}

/// Name the replaced version is renamed to during a swap.
pub fn old_name(name: &str) -> String {
    side_name(name, "__lv_old")
}

/// `v__lv_new` / `v__lv_old` → `v` (other names unchanged).
pub fn base_name(name: &str) -> &str {
    name.strip_suffix("__lv_new")
        .or_else(|| name.strip_suffix("__lv_old"))
        .unwrap_or(name)
}

fn side_name(name: &str, suffix: &str) -> String {
    let n = format!("{name}{suffix}");
    if n.len() <= 64 {
        n
    } else {
        format!("_lv_{}{suffix}", crate::analyze::definition_hash(name))
    }
}

/// The multi-row INSERT of `n` rows into `table`, upserting by the row key with `upsert`.
/// One dispatch per statement (never per row): the family's own function renders it.
pub(crate) fn insert_sql(
    kind: Kind,
    schema: &str,
    table: &str,
    layout: &Layout,
    n: usize,
    upsert: Option<Upsert>,
    key: Option<&[usize]>,
) -> String {
    match kind {
        // key is unused: MySQL's forms conflict against any unique key.
        Kind::Mysql => mysql::insert_sql(schema, table, layout, n, upsert),
        Kind::Postgres => postgres::insert_sql(schema, table, layout, n, key),
    }
}

/// `(predicate, parameters)` matching exactly `chunk`'s keys on `cols` (a DELETE's or a read's
/// WHERE). Dispatched once per statement.
pub(crate) fn unit_filter(
    kind: Kind,
    chunk: &UnitChunk,
    cols: &[String],
    kinds: &[UnitKind],
) -> (String, Vec<Value>) {
    match kind {
        Kind::Mysql => mysql::unit_filter(chunk, cols, kinds),
        Kind::Postgres => postgres::unit_filter(chunk, cols, kinds),
    }
}

/// `CREATE TABLE` of `layout` for `table`, with an optional ownership marker. Dispatched
/// once per DDL statement.
pub(crate) fn create_table_sql(
    kind: Kind,
    layout: &Layout,
    schema: &str,
    table: &str,
    marker: Option<&str>,
) -> String {
    match kind {
        Kind::Mysql => mysql::create_sql_marked(layout, schema, table, marker),
        Kind::Postgres => postgres::create_table_sql(layout, schema, table, marker),
    }
}

/// `BASE TABLE` / `VIEW` / None. One dispatch per metadata read.
pub(crate) async fn table_kind(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<Option<String>> {
    match kind {
        Kind::Mysql => mysql::table_kind(conn, schema, name).await,
        Kind::Postgres => postgres::table_kind(conn, schema, name).await,
    }
}

/// Visible + invisible column names and types of an existing table.
pub(crate) async fn table_columns(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<Vec<(String, String)>> {
    match kind {
        Kind::Mysql => mysql::table_columns(conn, schema, name).await,
        Kind::Postgres => postgres::table_columns(conn, schema, name).await,
    }
}

/// Whether an existing table's columns are the layout's.
pub(crate) fn layout_matches(kind: Kind, layout: &Layout, existing: &[(String, String)]) -> bool {
    match kind {
        Kind::Mysql => mysql::layout_matches(layout, existing),
        Kind::Postgres => postgres::layout_matches(layout, existing),
    }
}

/// Whether `table` has the unique index its rows are upserted by (see [`Layout::row_key`]).
pub(crate) async fn has_row_index(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    table: &str,
    layout: &Layout,
) -> Result<bool> {
    match kind {
        Kind::Mysql => mysql::has_row_index(conn, schema, table, layout).await,
        Kind::Postgres => postgres::has_row_index(conn, schema, table, layout).await,
    }
}

/// The ownership marker (see [`marker`]) of an existing table, if its comment is one.
pub(crate) async fn table_marker(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<Option<String>> {
    match kind {
        Kind::Mysql => mysql::table_marker(conn, schema, name).await,
        Kind::Postgres => postgres::table_marker(conn, schema, name).await,
    }
}

/// Creates the shadow table of `name` with the ownership marker as its comment.
pub(crate) async fn create_shadow_marked(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: &str,
) -> Result<String> {
    match kind {
        Kind::Mysql => mysql::create_shadow_marked(conn, schema, name, layout, marker).await,
        Kind::Postgres => postgres::create_shadow(conn, schema, name, layout, marker).await,
    }
}

/// Atomically replaces `name` with its shadow table (or renames the shadow when `name` is new).
pub(crate) async fn swap(conn: &mut Conn, kind: Kind, schema: &str, name: &str) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::swap(conn, schema, name).await,
        Kind::Postgres => postgres::swap(conn, schema, name).await,
    }
}

pub(crate) async fn drop_table(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::drop_table(conn, schema, name).await,
        Kind::Postgres => postgres::drop_table(conn, schema, name).await,
    }
}

/// The statements that give a new pooled connection the family's session settings. Dispatched
/// once per pooled connection (never per statement).
pub(crate) fn session_setup(kind: Kind, sql_mode: &str, lock_wait_s: u64) -> Vec<String> {
    match kind {
        Kind::Mysql => Vec::from(mysql::session_setup(sql_mode, lock_wait_s)),
        Kind::Postgres => postgres::session_setup(lock_wait_s),
    }
}

/// Applies the family's session settings to a fresh direct connection. One dispatch per
/// connection setup.
pub(crate) async fn init_session(conn: &mut Conn, kind: Kind, lock_wait_s: u64) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::init_session(conn, lock_wait_s).await,
        Kind::Postgres => postgres::init_session(conn, lock_wait_s).await,
    }
}

/// [`WriteOpts`] of the server `conn` is connected to. One dispatch per connection setup.
pub(crate) async fn write_opts(
    conn: &mut Conn,
    kind: Kind,
    batch_rows: usize,
    batch_bytes: usize,
) -> Result<WriteOpts> {
    match kind {
        Kind::Mysql => mysql::write_opts(conn, kind, batch_rows, batch_bytes).await,
        Kind::Postgres => postgres::write_opts(conn, kind, batch_rows, batch_bytes).await,
    }
}

/// Kills write op `id` and waits until the server no longer lists it (the zombie fence). One
/// dispatch per fence, never per row.
pub(crate) async fn fence(kind: Kind, opts: &Opts, id: u32) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::kill_and_wait(opts, id).await,
        Kind::Postgres => postgres::fence(opts, id).await,
    }
}
