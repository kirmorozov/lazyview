//! PostgreSQL as a target: the statements and the connection of a `postgres://` target DSN.
//! The connect level (PR T1) is real: [`PgConn`], its session ([`session_setup`]) and its
//! [`WriteOpts`]; so are the type map and the text codec of PR T2 ([`pg_type`], [`param_text`],
//! [`cell_value`]), which decide what a source column becomes here and how a value crosses.
//! The statement functions below are stubs until the PR that lands their real
//! form (T3-T5); the write path of `crate::mv` and the engine reject a PostgreSQL target before
//! any of them is reached (their `Kind::Postgres` arms are unreachable in this PR).

use std::borrow::Cow;

use anyhow::Result;
use mysql_async::{Conn, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::mv::{Layout, MvCol, Upsert, WriteOpts};

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

// ---------------------------------------------------------------------------
// The type map and the text codec (PR T2). Pure functions: the source column's
// declared type decides the PostgreSQL type, and a value crosses in both
// directions as the text PostgreSQL prints for it, so the whole cross-engine
// path is analyzable and unit-testable without a server.
// ---------------------------------------------------------------------------

/// The PostgreSQL type of a source column: the table of issue #56, completed against the
/// families [`crate::target::mysql::KeyPart`] knows. Keyed on `data_type` + `column_type` only
/// (never on a column name): the same source type always gives the same target type, for every
/// view. The strings are exactly what `format_type()` reports, so `layout_matches` (T3) can
/// compare them with `pg_attribute` verbatim.
///
/// `tinyint(1)` is 0/1 bytes and not a boolean, unsigned integers go one step wider (D3),
/// `float`/`double` keep a float type and compare by exact bit pattern (D4), TIME is text so the
/// whole MySQL range (±838:59:59) survives (D1), every string type loses its MySQL collation and
/// becomes `text` (D2), and an unknown source type lands in `text` too (the `KeyPart::None`
/// fallback: lazyview stores what the source gives it).
#[allow(dead_code)] // T3's CREATE TABLE and T4's writer/reader call these; the tests here prove them
pub(crate) fn pg_type(col: &MvCol) -> String {
    let dt = col.data_type.to_ascii_lowercase();
    let ct = col.column_type.to_ascii_lowercase();
    let u = is_unsigned(&ct);
    match dt.as_str() {
        // `bool`/`boolean` are `tinyint(1)` aliases, `year` is a small integer.
        "tinyint" | "bool" | "boolean" | "year" => "smallint".into(),
        "smallint" => {
            if u {
                "integer".into()
            } else {
                "smallint".into()
            }
        }
        "mediumint" | "int" | "integer" | "middleint" => {
            // `mediumint unsigned` still fits an INT32; `int unsigned` does not.
            if u && dt == "int" {
                "bigint".into()
            } else {
                "integer".into()
            }
        }
        // No PostgreSQL type holds a u64, so `bigint unsigned` becomes the exact decimal.
        "bigint" => {
            if u {
                "numeric(20,0)".into()
            } else {
                "bigint".into()
            }
        }
        "decimal" | "numeric" => numeric_type(&ct),
        "float" => "real".into(),
        "double" | "real" => "double precision".into(),
        // BIT(n) is the integer it holds (issue #56); BIT(1) is the tinyint alias of row two of
        // the map, and more than 63 bits needs the exact decimal.
        "bit" => match paren_number(&ct) {
            Some(n) if n > 63 => "numeric".into(),
            Some(1) => "smallint".into(),
            _ => "bigint".into(),
        },
        "date" => "date".into(),
        // The fraction is carried through: a `datetime(6)`/`timestamp(3)` source keeps its
        // microseconds. `timestamp` becomes timestamptz because the session is UTC (T1.4), which
        // makes its text an exact UTC datetime.
        "datetime" => match paren_number(&ct) {
            Some(n) if n > 0 => format!("timestamp({n})"),
            _ => "timestamp".into(),
        },
        "timestamp" => match paren_number(&ct) {
            Some(n) if n > 0 => format!("timestamp({n}) with time zone"),
            _ => "timestamp with time zone".into(),
        },
        "time" => "text".into(),
        "json" | "jsonb" => "jsonb".into(),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => "bytea".into(),
        // char, varchar, the four TEXT types, enum, set - and anything lazyview does not know
        // (MariaDB's INET6, UUID, a spatial type): text, collation dropped (D2).
        _ => "text".into(),
    }
}

/// The multi-row INSERT's and the diff read's parameter text for one value (`None` = SQL NULL).
/// PostgreSQL sends text-format parameters, so this is what crosses the wire.
///
/// Integers are decimal, floats use Rust's `Display` (the shortest form that round-trips, which
/// `extra_float_digits = 1` makes PostgreSQL agree with bit for bit, D4), `Bytes` goes as itself
/// for `text`/`numeric`/`jsonb` and as `\x`+hex for `bytea` (the session's `bytea_output`), and
/// the zero date is written NULL (D5, see [`cell_value`]).
#[allow(dead_code)] // the writer of PR T4 calls this; the tests here prove it
pub(crate) fn param_text<'a>(v: &'a Value, col: &MvCol) -> Option<Cow<'a, str>> {
    match v {
        Value::NULL => None,
        Value::Int(i) => Some(Cow::Owned(i.to_string())),
        Value::UInt(u) => Some(Cow::Owned(u.to_string())),
        Value::Float(f) => Some(Cow::Owned(f.to_string())),
        Value::Double(f) => Some(Cow::Owned(f.to_string())),
        Value::Bytes(b) => match pg_type(col).as_str() {
            "bytea" => Some(Cow::Owned(format!("\\x{}", hex::encode(b)))),
            // A text column's bytes are UTF-8 already (its source column is a character type);
            // a stray non-UTF-8 byte, which MySQL tolerates in TEXT, goes through lossily and
            // PostgreSQL rejects it - the failure strict mode gives on the source.
            _ => Some(String::from_utf8_lossy(b)),
        },
        Value::Date(y, m, d, h, i, s, us) => {
            // D5: a target has no zero date, so it is written NULL and read back as one only in
            // a column the layout marks NOT NULL (see [`cell_value`]).
            if *y == 0 || *m == 0 || *d == 0 {
                return None;
            }
            // A MySQL DATE column carries no time, so the target's `date` reads the same text.
            if pg_type(col) == "date" {
                return Some(Cow::Owned(format!("{y:04}-{m:02}-{d:02}")));
            }
            // A zero fraction is omitted, as both servers print it for a column without one.
            let frac = if *us == 0 {
                String::new()
            } else {
                format!(".{us:06}")
            };
            Some(Cow::Owned(format!(
                "{y:04}-{m:02}-{d:02} {h:02}:{i:02}:{s:02}{frac}"
            )))
        }
        // D1: TIME is text, `[-]HH:MM:SS.uuuuuu` with `HH = days*24 + hours` - the canonical form
        // of [`crate::datum::time_param`], exact for MySQL's ±838:59:59.
        Value::Time(neg, d, h, i, s, us) => Some(Cow::Owned(format!(
            "{}{}:{i:02}:{s:02}.{us:06}",
            if *neg { "-" } else { "" },
            d * 24 + u32::from(*h)
        ))),
    }
}

/// The inverse of [`param_text`]: one cell of a diff read (the driver yields `None` for a SQL
/// NULL; a `&str` could not tell an empty `text` value from NULL) into the `Value` shape
/// [`crate::mv::Layout::normalize`] produces, so `diff` compares the two engines' rows with no
/// change of its own. A cell that does not parse stays `Bytes` - a surprise stays visible instead
/// of quietly becoming a NULL.
#[allow(dead_code)] // the diff read of PR T4 calls this; the tests here prove it
pub(crate) fn cell_value(text: Option<&str>, col: &MvCol, nullable: bool) -> Value {
    let Some(text) = text else {
        // D5: the writer sends NULL for the zero date, so in a NOT NULL date column a NULL can
        // only mean that date; everywhere else (and in a nullable column) it stays NULL.
        return if !nullable && is_date_type(&col.data_type) {
            Value::Date(0, 0, 0, 0, 0, 0, 0)
        } else {
            Value::NULL
        };
    };
    let dt = col.data_type.to_ascii_lowercase();
    if dt == "time" {
        // TIME is stored as text (D1): the only case the target type alone cannot tell apart.
        return parse_time_text(text).unwrap_or_else(|| bytes(text));
    }
    // BIT is `UInt` here as it is in `normalize` (`src/mv.rs`: the binlog gives it as big-endian
    // bytes), whatever the map made of the column: `smallint`, `bigint`, or the `numeric` of a
    // BIT wider than 63 bits. Keying this on the target type would lose the shape above 63 bits.
    if dt == "bit" {
        return text
            .parse::<u64>()
            .map_or_else(|_| bytes(text), Value::UInt);
    }
    match pg_type(col).as_str() {
        "smallint" | "integer" | "bigint" => int_cell(text, false),
        // `bigint unsigned`: u64, and `Int` where it fits so binlog- and query-sourced keys
        // compare equal (see [`crate::datum::Datum::uint`]).
        "numeric(20,0)" => int_cell(text, true),
        "real" | "double precision" => text
            .parse::<f64>()
            .map_or_else(|_| bytes(text), Value::Double),
        "bytea" => parse_bytea(text),
        "date" => parse_date_text(text).unwrap_or_else(|| bytes(text)),
        s if s.starts_with("timestamp") => parse_date_text(text).unwrap_or_else(|| bytes(text)),
        // numeric, text (char, varchar, TEXT, enum, set, the unknown fallback) and jsonb: the
        // bytes as PostgreSQL printed them, which is what `normalize` compares for those columns.
        _ => bytes(text),
    }
}

/// The name of one index of `table`: PostgreSQL index names are schema-wide, so they derive from
/// the table (`<table>_lv_row`, `<table>_lv_unit`, `<table>_lv_a<k>`, `<table>_lv_p<k>`) instead
/// of MySQL's inline `_lv_row` (`src/target/mysql.rs`), and the swap renames the shadow's to these
/// in its own transaction (D7). PostgreSQL truncates an identifier at 63 bytes *silently*, so a
/// longer name takes the same `definition_hash` fallback [`crate::target::side_name`] uses - it
/// always fits, because the hash is 16 hex characters and the suffix is a few characters.
#[allow(dead_code)] // the DDL of PR T3 names the indexes with this; the test here proves it
pub(crate) fn index_name(table: &str, suffix: &str) -> String {
    let n = format!("{table}_{suffix}");
    if n.len() <= 63 {
        n
    } else {
        format!("_lv_{}_{suffix}", crate::analyze::definition_hash(table))
    }
}

/// `unsigned` / `zerofill` are read outside the parentheses, so an enum or set label that says
/// `unsigned` cannot make an integer column wide.
fn is_unsigned(column_type: &str) -> bool {
    let mut depth = 0usize;
    let mut outside = String::new();
    for c in column_type.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => outside.push(c),
            _ => {}
        }
    }
    outside.contains("unsigned") || outside.contains("zerofill")
}

/// `decimal(20,4)` → `numeric(20,4)`: the scale is preserved, so the text round-trips. Without a
/// declared precision the target takes an unconstrained `numeric` (the source's default is one).
fn numeric_type(column_type: &str) -> String {
    let inside = paren_inner(column_type).unwrap_or_default();
    let mut parts = inside.split(',').map(str::trim);
    match (parts.next(), parts.next()) {
        (Some(p), Some(s)) if is_number(p) && is_number(s) => format!("numeric({p},{s})"),
        (Some(p), None) if is_number(p) => format!("numeric({p})"),
        _ => "numeric".into(),
    }
}

fn is_number(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// `datetime(6)` → `Some(6)`; `datetime`, `enum('a','b')` → `None` / the first number.
fn paren_number(column_type: &str) -> Option<u64> {
    let digits: String = paren_inner(column_type)?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn paren_inner(column_type: &str) -> Option<&str> {
    let open = column_type.find('(')? + 1;
    let close = column_type[open..].rfind(')')?;
    Some(&column_type[open..open + close])
}

/// The date family of the map (the columns D5's zero-date rule speaks of).
fn is_date_type(data_type: &str) -> bool {
    matches!(
        data_type.to_ascii_lowercase().as_str(),
        "date" | "datetime" | "timestamp"
    )
}

/// `Int` for the integer family; with `wide` (a `numeric(20,0)`), `UInt` above `i64::MAX`.
fn int_cell(text: &str, wide: bool) -> Value {
    if let Ok(u) = text.parse::<u64>() {
        return match i64::try_from(u) {
            // `Int` where it fits, so binlog- and query-sourced keys compare equal.
            Ok(i) => Value::Int(i),
            // A u64 above `i64::MAX`: `UInt` in the column the map made for it (`numeric(20,0)`),
            // elsewhere the bytes, which keep the surprise visible instead of losing the row.
            Err(_) if wide => Value::UInt(u),
            Err(_) => bytes(text),
        };
    }
    if let Ok(i) = text.parse::<i64>() {
        return Value::Int(i);
    }
    bytes(text)
}

fn bytes(text: &str) -> Value {
    Value::Bytes(text.as_bytes().to_vec())
}

/// `\x0102ff` - the text form of `bytea` under the session's `bytea_output = 'hex'` - to the raw
/// bytes. Anything else is kept as written, so a role that overrode the GUC shows up as a diff.
fn parse_bytea(text: &str) -> Value {
    let hexed = match text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))
    {
        Some(h) if !h.is_empty() && h.len() % 2 == 0 => h,
        _ => return bytes(text),
    };
    match hex::decode(hexed) {
        Ok(b) => Value::Bytes(b),
        Err(_) => bytes(text),
    }
}

/// The `DateStyle = 'ISO'` text of `date`, `timestamp` and `timestamp with time zone`
/// (`2024-02-29 23:59:58.000001+00`) as `Value::Date`. The session is UTC (T1.4), so the only
/// suffix is PostgreSQL's `+00` (or an ISO `Z`); stripping it is what makes the value exact.
fn parse_date_text(text: &str) -> Option<Value> {
    let (date, time) = match text.split_once([' ', 'T']) {
        Some((d, x)) => (d, Some(x)),
        None => (text, None),
    };
    let mut d = date.split('-');
    let (Some(y), Some(m), Some(day)) = (d.next(), d.next(), d.next()) else {
        return None;
    };
    let (h, i, s, us) = match time {
        None => (0, 0, 0, 0),
        Some(x) => {
            let x = x.split(['+', 'Z']).next()?;
            let (clock, frac) = match x.split_once('.') {
                Some((c, f)) => (c, Some(f)),
                None => (x, None),
            };
            let mut c = clock.split(':');
            let (Some(h), Some(i), Some(s)) = (c.next(), c.next(), c.next()) else {
                return None;
            };
            (
                h.parse().ok()?,
                i.parse().ok()?,
                s.parse().ok()?,
                match frac {
                    None => 0,
                    Some(f) => {
                        let mut six = f.to_owned();
                        while six.len() < 6 {
                            six.push('0');
                        }
                        six.truncate(6);
                        six.parse().ok()?
                    }
                },
            )
        }
    };
    Some(Value::Date(
        y.parse().ok()?,
        m.parse().ok()?,
        day.parse().ok()?,
        h,
        i,
        s,
        us,
    ))
}

/// The canonical TIME text `[-]HH:MM:SS.uuuuuu` (`HH = days*24 + hours`, D1) as `Value::Time`,
/// the shape [`crate::mv::Layout::normalize`] keeps TIME in.
fn parse_time_text(text: &str) -> Option<Value> {
    let (neg, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let mut p = rest.split(':');
    let (Some(h), Some(m), Some(sec)) = (p.next(), p.next(), p.next()) else {
        return None;
    };
    let hours: u32 = h.parse().ok()?;
    let s = sec.split('.').next()?.parse().ok()?;
    let us = match sec.split_once('.') {
        None => 0,
        Some((_, f)) => {
            let mut six = f.to_owned();
            while six.len() < 6 {
                six.push('0');
            }
            six.truncate(6);
            six.parse().ok()?
        }
    };
    Some(Value::Time(
        neg,
        hours / 24,
        (hours % 24) as u8,
        m.parse().ok()?,
        s,
        us,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::Datum;

    fn col(name: &str, data_type: &str, column_type: &str) -> MvCol {
        MvCol {
            name: name.into(),
            src_name: name.into(),
            column_type: column_type.into(),
            data_type: data_type.into(),
            charset: None,
            collation: None,
            nullable: true,
        }
    }

    /// The whole map, one row per source type the probe can report (issue #56's list plus the
    /// kinds §0.8 found missing: `time`, the signed family, `date`, `float`/`double`, the
    /// unknown-type fallback).
    #[test]
    fn type_map_covers_every_data_type() {
        let cases: &[(&str, &str, &str)] = &[
            ("tinyint", "tinyint(4)", "smallint"),
            ("tinyint", "tinyint(1)", "smallint"),
            ("tinyint", "tinyint unsigned", "smallint"),
            ("tinyint", "tinyint(1) unsigned", "smallint"),
            ("tinyint", "tinyint(1) zerofill", "smallint"),
            ("year", "year(4)", "smallint"),
            ("smallint", "smallint(6)", "smallint"),
            ("smallint", "smallint unsigned", "integer"),
            ("mediumint", "mediumint(9)", "integer"),
            ("mediumint", "mediumint unsigned", "integer"),
            ("int", "int(11)", "integer"),
            ("int", "int unsigned", "bigint"),
            ("bigint", "bigint(20)", "bigint"),
            ("bigint", "bigint unsigned", "numeric(20,0)"),
            ("decimal", "decimal(20,4)", "numeric(20,4)"),
            ("numeric", "numeric(10,2)", "numeric(10,2)"),
            ("decimal", "decimal", "numeric"),
            ("float", "float", "real"),
            ("double", "double", "double precision"),
            ("real", "double", "double precision"),
            ("bit", "bit(1)", "smallint"),
            ("bit", "bit(8)", "bigint"),
            // BIT(64) holds 2^64-1, above any signed bigint, so it is the exact decimal (n > 63).
            ("bit", "bit(64)", "numeric"),
            ("bit", "bit(65)", "numeric"),
            ("date", "date", "date"),
            ("datetime", "datetime", "timestamp"),
            ("datetime", "datetime(6)", "timestamp(6)"),
            ("timestamp", "timestamp", "timestamp with time zone"),
            ("timestamp", "timestamp(3)", "timestamp(3) with time zone"),
            ("time", "time", "text"),
            ("time", "time(6)", "text"),
            ("char", "char(10)", "text"),
            ("varchar", "varchar(255)", "text"),
            ("tinytext", "tinytext", "text"),
            ("text", "text", "text"),
            ("mediumtext", "mediumtext", "text"),
            ("longtext", "longtext", "text"),
            ("enum", "enum('a','b')", "text"),
            ("set", "set('a','b','c')", "text"),
            ("json", "json", "jsonb"),
            ("binary", "binary(4)", "bytea"),
            ("varbinary", "varbinary(16)", "bytea"),
            ("tinyblob", "tinyblob", "bytea"),
            ("blob", "blob", "bytea"),
            ("mediumblob", "mediumblob", "bytea"),
            ("longblob", "longblob", "bytea"),
            // Unknown to lazyview (KeyPart::None): text, like every other string.
            ("geometry", "geometry", "text"),
            ("inet6", "inet6", "text"),
            ("uuid", "uuid", "text"),
            ("", "", "text"),
        ];
        for (dt, ct, want) in cases {
            let t = pg_type(&col("c", dt, ct));
            assert_eq!(&t, want, "{dt} {ct}");
            // What T3 compares against `format_type()`: lowercase, no MySQL spelling left.
            assert!(
                t.chars()
                    .all(|c| c.is_lowercase() || c.is_ascii_digit() || " ,()".contains(c)),
                "{t} is not a format_type() form"
            );
        }
        // An enum label never makes a column wide: `unsigned` counts only outside the parens.
        assert_eq!(pg_type(&col("c", "int", "enum('unsigned')")), "integer");
    }

    /// Every form the writer sends as a text parameter.
    #[test]
    fn param_text_forms() {
        let i = col("c", "int", "int");
        assert_eq!(param_text(&Value::Int(-7), &i).as_deref(), Some("-7"));
        assert_eq!(
            param_text(&Value::UInt(18_446_744_073_709_551_615), &i).as_deref(),
            Some("18446744073709551615")
        );
        assert_eq!(param_text(&Value::NULL, &i), None);
        // Shortest round-trip, which `extra_float_digits = 1` makes PostgreSQL print too (D4).
        let d = col("c", "double", "double");
        assert_eq!(
            param_text(&Value::Double(-2.25), &d).as_deref(),
            Some("-2.25")
        );
        assert_eq!(param_text(&Value::Double(0.1), &d).as_deref(), Some("0.1"));
        assert_eq!(
            param_text(&Value::Float(1.5), &col("c", "float", "float")).as_deref(),
            Some("1.5")
        );
        // decimal, text and jsonb cross as their own bytes; bytea as \x+hex.
        assert_eq!(
            param_text(
                &Value::Bytes(b"-12.50".to_vec()),
                &col("c", "decimal", "decimal(10,2)")
            )
            .as_deref(),
            Some("-12.50")
        );
        assert_eq!(
            param_text(
                &Value::Bytes(b"x y".to_vec()),
                &col("c", "varchar", "varchar(10)")
            )
            .as_deref(),
            Some("x y")
        );
        assert_eq!(
            param_text(
                &Value::Bytes(br#"{"a":1}"#.to_vec()),
                &col("c", "json", "json")
            )
            .as_deref(),
            Some(r#"{"a":1}"#)
        );
        assert_eq!(
            param_text(
                &Value::Bytes(vec![1, 2, 0xff]),
                &col("c", "longblob", "longblob")
            )
            .as_deref(),
            Some("\\x0102ff")
        );
        // Datetimes: the ISO form, a zero fraction omitted, the zero date NULL (D5).
        let dt = col("c", "datetime", "datetime(6)");
        assert_eq!(
            param_text(&Value::Date(2024, 2, 29, 23, 59, 58, 1), &dt).as_deref(),
            Some("2024-02-29 23:59:58.000001")
        );
        assert_eq!(
            param_text(&Value::Date(2024, 2, 29, 23, 59, 58, 0), &dt).as_deref(),
            Some("2024-02-29 23:59:58")
        );
        assert_eq!(
            param_text(
                &Value::Date(2024, 2, 29, 0, 0, 0, 0),
                &col("c", "date", "date")
            )
            .as_deref(),
            Some("2024-02-29")
        );
        // TIME is text with the days folded into the hours (D1), the form `time_param` canonicalises.
        let tm = col("c", "time", "time(6)");
        assert_eq!(
            param_text(&Value::Time(true, 34, 22, 59, 58, 990_000), &tm).as_deref(),
            Some("-838:59:58.990000")
        );
        assert_eq!(
            param_text(&Value::Time(false, 0, 5, 6, 7, 0), &tm).as_deref(),
            Some("5:06:07.000000")
        );
    }

    /// Every form the diff read decodes back, in the shapes `Layout::normalize` produces.
    #[test]
    fn cell_value_forms() {
        assert_eq!(
            cell_value(Some("-7"), &col("c", "int", "int"), true),
            Value::Int(-7)
        );
        assert_eq!(
            cell_value(Some("4294967295"), &col("c", "int", "int unsigned"), true),
            Value::Int(4_294_967_295)
        );
        // numeric(20,0): Int where it fits, UInt above i64::MAX.
        let bu = col("c", "bigint", "bigint unsigned");
        assert_eq!(
            cell_value(Some("9223372036854775807"), &bu, true),
            Value::Int(i64::MAX)
        );
        assert_eq!(
            cell_value(Some("18446744073709551615"), &bu, true),
            Value::UInt(u64::MAX)
        );
        assert_eq!(
            cell_value(Some("-2.25"), &col("c", "double", "double"), true),
            Value::Double(-2.25)
        );
        // BIT comes back as UInt, as normalize leaves it.
        assert_eq!(
            cell_value(Some("2748"), &col("c", "bit", "bit(12)"), true),
            Value::UInt(0x0abc)
        );
        assert_eq!(
            cell_value(Some("-12.50"), &col("c", "decimal", "decimal(10,2)"), true),
            Value::Bytes(b"-12.50".to_vec())
        );
        assert_eq!(
            cell_value(Some("a,b"), &col("c", "set", "set('a','b')"), true),
            Value::Bytes(b"a,b".to_vec())
        );
        assert_eq!(
            cell_value(Some("\\x0102ff"), &col("c", "blob", "blob"), true),
            Value::Bytes(vec![1, 2, 0xff])
        );
        let dt = col("c", "datetime", "datetime(6)");
        assert_eq!(
            cell_value(Some("2024-02-29 23:59:58.000001"), &dt, true),
            Value::Date(2024, 2, 29, 23, 59, 58, 1)
        );
        assert_eq!(
            cell_value(
                Some("2024-02-29 23:59:58"),
                &col("c", "datetime", "datetime"),
                true
            ),
            Value::Date(2024, 2, 29, 23, 59, 58, 0)
        );
        assert_eq!(
            cell_value(Some("2024-02-29"), &col("c", "date", "date"), true),
            Value::Date(2024, 2, 29, 0, 0, 0, 0)
        );
        // timestamptz: the session is UTC, so the +00 suffix carries no information.
        assert_eq!(
            cell_value(
                Some("2024-02-29 23:59:58.000001+00"),
                &col("c", "timestamp", "timestamp(6)"),
                true
            ),
            Value::Date(2024, 2, 29, 23, 59, 58, 1)
        );
        assert_eq!(
            cell_value(
                Some("-838:59:58.990000"),
                &col("c", "time", "time(6)"),
                true
            ),
            Value::Time(true, 34, 22, 59, 58, 990_000)
        );
        // An empty text value is a value, not a NULL.
        assert_eq!(
            cell_value(Some(""), &col("c", "varchar", "varchar(10)"), true),
            Value::Bytes(Vec::new())
        );
        assert_eq!(
            cell_value(None, &col("c", "varchar", "varchar(10)"), true),
            Value::NULL
        );
        // A cell the map did not expect stays bytes: never a silent NULL.
        assert_eq!(
            cell_value(Some("nope"), &col("c", "int", "int"), true),
            Value::Bytes(b"nope".to_vec())
        );
    }

    /// D5: the zero date leaves as NULL and returns as the zero date in a NOT NULL date column.
    #[test]
    fn zero_date_is_null_and_comes_back_for_not_null() {
        let dt = col("c", "datetime", "datetime(6)");
        for d in [
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            Value::Date(0, 2, 29, 0, 0, 0, 0),
            Value::Date(2024, 0, 29, 0, 0, 0, 0),
        ] {
            assert_eq!(param_text(&d, &dt), None, "{d:?}");
        }
        let not_null = MvCol {
            nullable: false,
            ..col("c", "datetime", "datetime")
        };
        assert_eq!(
            cell_value(None, &not_null, false),
            Value::Date(0, 0, 0, 0, 0, 0, 0)
        );
        // In a nullable column the same NULL stays NULL, and so does one in a NOT NULL column of
        // any other family: the rule belongs to the date family alone.
        assert_eq!(cell_value(None, &dt, true), Value::NULL);
        let text_not_null = MvCol {
            nullable: false,
            ..col("c", "varchar", "varchar(10)")
        };
        assert_eq!(cell_value(None, &text_not_null, false), Value::NULL);
        // One round of the rule: a real row is unaffected, only the zero date takes it.
        assert_eq!(
            cell_value(
                param_text(&Value::Date(2024, 2, 29, 23, 59, 58, 0), &not_null).as_deref(),
                &not_null,
                false
            ),
            Value::Date(2024, 2, 29, 23, 59, 58, 0)
        );
    }

    /// `Datum -> Value -> param_text -> cell_value -> Value -> Datum` is the identity, for every
    /// kind (the table of values mirrors `src/datum.rs`'s binlog round trip; the zero date is
    /// [zero_date_is_null_and_comes_back_for_not_null]'s, where NULL is the point).
    #[test]
    fn round_trip_every_kind() {
        let cases: Vec<(MvCol, Vec<Datum>)> = vec![
            (col("c", "int", "int"), vec![Datum::Int(-7), Datum::Int(0)]),
            (
                col("c", "bigint", "bigint unsigned"),
                vec![
                    Datum::Int(5),
                    Datum::Int(-1),
                    Datum::uint(i64::MAX as u64 + 1),
                    Datum::uint(u64::MAX),
                ],
            ),
            (
                col("c", "tinyint", "tinyint unsigned"),
                vec![Datum::Int(-1)],
            ),
            (
                col("c", "enum", "enum('a','b','c')"),
                vec![Datum::Bytes(Vec::new()), Datum::Bytes(b"a".to_vec())],
            ),
            (
                col("c", "set", "set('a','b','c')"),
                vec![Datum::Bytes(b"a,c".to_vec())],
            ),
            (
                col("c", "timestamp", "timestamp"),
                vec![Datum::Date(2023, 11, 14, 22, 13, 20, 0)],
            ),
            (
                col("c", "timestamp", "timestamp(3)"),
                vec![Datum::Date(2023, 11, 14, 22, 13, 20, 125_000)],
            ),
            (col("c", "bit", "bit(12)"), vec![Datum::uint(0x0abc)]),
            (col("c", "bit", "bit(64)"), vec![Datum::uint(u64::MAX)]),
            (
                col("c", "binary", "binary(4)"),
                vec![Datum::Bytes(vec![1, 2, 0, 0])],
            ),
            (
                col("c", "varchar", "varchar(10)"),
                vec![Datum::Bytes(b"x y".to_vec()), Datum::Bytes(Vec::new())],
            ),
            (
                col("c", "json", "json"),
                vec![Datum::Bytes(br#"{"a":1}"#.to_vec())],
            ),
            (
                col("c", "float", "float"),
                vec![Datum::Float(1.5f64.to_bits())],
            ),
            (
                col("c", "double", "double"),
                vec![Datum::Float((-2.25f64).to_bits())],
            ),
            (
                col("c", "datetime", "datetime(6)"),
                vec![Datum::Date(2024, 2, 29, 23, 59, 58, 1)],
            ),
            (
                col("c", "date", "date"),
                vec![Datum::Date(2024, 2, 29, 0, 0, 0, 0)],
            ),
            (
                col("c", "time", "time(6)"),
                vec![
                    Datum::Time(true, 34, 22, 59, 58, 990_000),
                    Datum::Time(false, 0, 1, 2, 3, 4),
                ],
            ),
            (
                col("c", "decimal", "decimal(10,2)"),
                vec![Datum::Bytes(b"-12.50".to_vec())],
            ),
            (col("c", "int", "int"), vec![Datum::Null]),
        ];
        for (c, values) in cases {
            for d in values {
                let v = d.to_value();
                let text = param_text(&v, &c).map(|s| s.into_owned());
                let back = cell_value(text.as_deref(), &c, c.nullable);
                assert_eq!(
                    Datum::from_value(back),
                    d,
                    "{} {d:?} went as {text:?} and came back as {v:?}",
                    c.column_type
                );
            }
        }
    }

    /// §0.6: PostgreSQL truncates an identifier at 63 bytes silently, so an index name that
    /// would not fit takes the hash fallback instead of becoming unfindable.
    #[test]
    fn index_name_fits_63_bytes() {
        assert_eq!(index_name("items", "lv_row"), "items_lv_row");
        assert_eq!(index_name("items", "lv_unit"), "items_lv_unit");
        assert_eq!(index_name("items", "lv_a1"), "items_lv_a1");
        assert_eq!(index_name("items", "lv_p2"), "items_lv_p2");
        // The table name itself fits, plus the shortest suffix: still the plain name.
        let exact = "t".repeat(63 - "_lv_row".len());
        assert_eq!(index_name(&exact, "lv_row"), format!("{exact}_lv_row"));
        // One byte over: the hash form, which fits and is stable for the same table.
        let over = "t".repeat(63 - "_lv_row".len() + 1);
        let n = index_name(&over, "lv_row");
        assert!(n.len() <= 63, "{n}");
        assert!(n.starts_with("_lv_") && n.ends_with("_lv_row"), "{n}");
        assert_eq!(n, index_name(&over, "lv_row"));
        // Every suffix T3 uses, on a name far beyond the cap.
        let far = "x".repeat(300);
        for s in ["lv_row", "lv_unit", "lv_a0", "lv_p11", "lv_a10_pk0"] {
            let n = index_name(&far, s);
            assert!(n.len() <= 63, "{s}: {n}");
            assert!(n.ends_with(&format!("_{s}")), "{s}: {n}");
        }
    }

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
