//! The MySQL family as a target: the statements `crate::mv` issues, moving here as the seam
//! lands (PR S2 onward). Verbatim moves: the SQL strings are the ones `crate::mv` rendered.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::analyze::quote_ident as q;
use crate::mv::{Layout, MvCol, Upsert, WriteOpts, ROW_INDEX};
use crate::supervisor::{Timeout, CONNECT_TIMEOUT, FENCE_WAIT};
use crate::target::{old_name, shadow_name, Kind, MARKER_PREFIX};

/// Row alias of upserts (`INSERT ... AS <alias> ON DUPLICATE KEY UPDATE`).
const ROW_ALIAS: &str = "_lv_new";

/// A multi-row INSERT of `n` rows; with `upsert`, rows whose key exists replace that row.
pub(crate) fn insert_sql(
    schema: &str,
    table: &str,
    layout: &Layout,
    n: usize,
    upsert: Option<Upsert>,
) -> String {
    let tuple = format!("({})", vec!["?"; layout.cols.len()].join(","));
    let mut sql = format!(
        "INSERT INTO {}.{} ({}) VALUES {}",
        q(schema),
        q(table),
        layout.col_list(),
        vec![tuple.as_str(); n].join(",")
    );
    // Every column is set, the key columns too: a collation-equal key takes the new bytes.
    let set = |value: &dyn Fn(&str) -> String| {
        layout
            .cols
            .iter()
            .map(|c| format!("{} = {}", q(&c.name), value(&q(&c.name))))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match upsert {
        None => {}
        Some(Upsert::RowAlias) => {
            // (The row alias must differ from the table name.)
            let alias = if table.eq_ignore_ascii_case(ROW_ALIAS) {
                q(&format!("{ROW_ALIAS}_"))
            } else {
                q(ROW_ALIAS)
            };
            let updates = set(&|c| format!("{alias}.{c}"));
            sql.push_str(&format!(" AS {alias} ON DUPLICATE KEY UPDATE {updates}"));
        }
        Some(Upsert::Values) => {
            let updates = set(&|c| format!("VALUES({c})"));
            sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {updates}"));
        }
        // `ON CONFLICT` is PostgreSQL's (target/postgres.rs renders it, T4); a MySQL
        // connection never carries these opts (they come from `PgConn::write_opts`).
        #[allow(unreachable_patterns)]
        _ => unreachable!("ON CONFLICT is the PostgreSQL family's"),
    }
    sql
}

/// `(predicate, parameters)` matching exactly `chunk`'s keys on `cols` (the WHERE of a DELETE or
/// of a diff read): the MySQL family's shape, [`UnitChunk::filter_typed`] unchanged. The one
/// line is the dispatch point, not the logic (`src/accumulate.rs` stays as it is).
pub(crate) fn unit_filter(
    chunk: &UnitChunk,
    cols: &[String],
    kinds: &[UnitKind],
) -> (String, Vec<Value>) {
    chunk.filter_typed(cols, kinds)
}

/// InnoDB's limit on the length of an index key, in bytes.
const MAX_KEY_BYTES: u64 = 3072;
/// Maximum number of columns in an index.
const MAX_KEY_PARTS: usize = 16;
/// Index prefix of TEXT / BLOB unit columns, in characters (bytes for BLOB).
const TEXT_PREFIX: u64 = 255;

/// The unique index behind [`Layout::row_key`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RowIndex {
    /// The PRIMARY KEY on the unit.
    Primary,
    /// `_lv_row`, a UNIQUE KEY over these (whole) columns.
    Unique(String),
}

/// `CREATE TABLE` with an optional ownership marker as the table comment (see
/// [`crate::target::marker`]).
pub(crate) fn create_sql_marked(
    layout: &Layout,
    schema: &str,
    table: &str,
    marker: Option<&str>,
) -> String {
    let index = unit_index(layout);
    let row = row_index(layout);
    let primary = matches!(index, Some((true, _)));
    let mut defs: Vec<String> = layout
        .cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut d = format!("{} {}", q(&c.name), c.column_type);
            if let (Some(cs), Some(co)) = (&c.charset, &c.collation) {
                d.push_str(&format!(" CHARACTER SET {cs} COLLATE {co}"));
            }
            let in_pk = primary && layout.unit_pos.contains(&i);
            d.push_str(if in_pk { " NOT NULL" } else { " NULL" });
            if i >= layout.n_visible {
                d.push_str(" INVISIBLE");
            }
            d
        })
        .collect();
    match (index, &row) {
        (Some((true, parts)), _) => defs.push(format!("PRIMARY KEY ({parts})")),
        // `_lv_row` starts with the unit columns: it serves unit filters too.
        (Some((false, _)), Some(RowIndex::Unique(_))) => {}
        (Some((false, parts)), _) => defs.push(format!("KEY `_lv_unit` ({parts})")),
        (None, _) => {}
    }
    if let Some(RowIndex::Unique(parts)) = &row {
        defs.push(format!("UNIQUE KEY `{ROW_INDEX}` ({parts})"));
    }
    for (name, parts) in lineage_indexes(layout)
        .into_iter()
        .chain(partial_indexes(layout))
    {
        defs.push(format!("KEY {} ({parts})", q(&name)));
    }
    let comment = marker
        .map(|m| format!(" COMMENT='{}'", m.replace('\'', "''")))
        .unwrap_or_default();
    format!(
        "CREATE TABLE {}.{} (\n  {}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4{comment}",
        q(schema),
        q(table),
        defs.join(",\n  ")
    )
}

/// The unit index as `(primary, key parts)`: the PRIMARY KEY when the unit is unique and its
/// columns fit InnoDB's key limits, otherwise a non-unique KEY over as many leading unit
/// columns as fit (long strings as prefixes). None when not even the first unit column can
/// be indexed; unit DELETEs and INSERTs stay correct without an index.
pub(crate) fn unit_index(layout: &Layout) -> Option<(bool, String)> {
    let parts: Vec<(&MvCol, KeyPart)> = layout
        .unit_pos
        .iter()
        .map(|&p| (&layout.cols[p], key_part(&layout.cols[p])))
        .collect();
    if layout.primary && parts.len() <= MAX_KEY_PARTS {
        let full: Option<u64> = parts.iter().map(|(_, k)| k.full_bytes()).sum();
        if full.is_some_and(|b| b <= MAX_KEY_BYTES) {
            return Some((true, layout.unit_cols().join(", ")));
        }
    }
    let lead: Vec<(&MvCol, KeyPart)> = parts
        .into_iter()
        .take_while(|(_, k)| *k != KeyPart::None)
        .take(MAX_KEY_PARTS)
        .collect();
    (1..=lead.len())
        .rev()
        .find_map(|n| fit_key(&lead[..n]))
        .map(|k| (false, k))
}

/// The unique index over [`Layout::identity`]: the PRIMARY KEY when the identity is the unit
/// and that is the primary key, else `_lv_row` over the whole identity columns (a prefix
/// would let distinct rows collide). None when the rows have no identity, a column cannot be
/// indexed whole, the key exceeds InnoDB's limits, or a TIMESTAMP column is read in the
/// session's time zone (the two instants of a DST fall-back hour read as one value).
pub(crate) fn row_index(layout: &Layout) -> Option<RowIndex> {
    if layout.identity.is_empty()
        || (!layout.epoch_timestamps()
            && layout
                .identity
                .iter()
                .any(|&p| layout.cols[p].data_type == "timestamp"))
    {
        return None;
    }
    if layout.identity == layout.unit_pos && matches!(unit_index(layout), Some((true, _))) {
        return Some(RowIndex::Primary);
    }
    if layout.identity.len() > MAX_KEY_PARTS {
        return None;
    }
    let bytes: Option<u64> = layout
        .identity
        .iter()
        .map(|&p| key_part(&layout.cols[p]).full_bytes())
        .sum();
    if !bytes.is_some_and(|b| b <= MAX_KEY_BYTES) {
        return None;
    }
    Some(RowIndex::Unique(
        layout
            .identity
            .iter()
            .map(|&p| q(&layout.cols[p].name))
            .collect::<Vec<_>>()
            .join(", "),
    ))
}

/// One non-unique index per lineage key, `(name, key parts)` (none for a key that does not
/// fit InnoDB's limits).
pub(crate) fn lineage_indexes(layout: &Layout) -> Vec<(String, String)> {
    layout
        .lineage
        .iter()
        .filter_map(|key| {
            let alias = crate::mv::lineage_alias(&layout.cols[*key.first()?].name)?;
            let parts: Vec<(&MvCol, KeyPart)> = key
                .iter()
                .map(|&p| (&layout.cols[p], key_part(&layout.cols[p])))
                .collect();
            if parts.len() > MAX_KEY_PARTS || parts.iter().any(|(_, k)| *k == KeyPart::None) {
                return None;
            }
            Some((format!("_lv_a{alias}"), fit_key(&parts)?))
        })
        .collect()
}

/// One non-unique index `_lv_p<k>` per set of partial unit positions the unit index does
/// not start with: a partial refresh reads and replaces the rows matching those columns
/// alone (an index range instead of a scan).
pub(crate) fn partial_indexes(layout: &Layout) -> Vec<(String, String)> {
    let unit_index = unit_index(layout).is_some();
    layout
        .partial
        .iter()
        .enumerate()
        .filter(|(_, at)| !(unit_index && at.iter().copied().eq(0..at.len())))
        .filter_map(|(k, at)| {
            let parts: Vec<(&MvCol, KeyPart)> = at
                .iter()
                .map(|&u| {
                    (
                        &layout.cols[layout.unit_pos[u]],
                        key_part(&layout.cols[layout.unit_pos[u]]),
                    )
                })
                .take_while(|(_, k)| *k != KeyPart::None)
                .take(MAX_KEY_PARTS)
                .collect();
            if parts.is_empty() {
                return None;
            }
            Some((format!("_lv_p{k}"), fit_key(&parts)?))
        })
        .collect()
}

/// How a column can be part of an index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum KeyPart {
    /// Fixed size, in bytes (an upper bound).
    Fixed(u64),
    /// String of `len` characters (bytes for binary strings) of at most `per_char` bytes each.
    /// TEXT / BLOB columns (`must_prefix`) can only be indexed by a prefix.
    Str {
        len: u64,
        per_char: u64,
        must_prefix: bool,
    },
    /// Not indexable (JSON, spatial, unknown types).
    None,
}

impl KeyPart {
    /// Bytes the whole column takes in a key (None: it cannot be indexed whole).
    pub(crate) fn full_bytes(&self) -> Option<u64> {
        match *self {
            KeyPart::Fixed(b) => Some(b),
            KeyPart::Str {
                len,
                per_char,
                must_prefix: false,
            } => Some(len * per_char),
            _ => None,
        }
    }
}

/// How `col` can be part of an index.
pub(crate) fn key_part(col: &MvCol) -> KeyPart {
    let ct = col.column_type.to_ascii_lowercase();
    let n = paren_number(&ct);
    let per_char = mbmaxlen(col.charset.as_deref());
    let text = |len, per_char, must_prefix| KeyPart::Str {
        len,
        per_char,
        must_prefix,
    };
    match col.data_type.as_str() {
        "tinyint" | "year" => KeyPart::Fixed(1),
        "smallint" | "enum" => KeyPart::Fixed(2),
        "mediumint" | "date" => KeyPart::Fixed(3),
        "int" | "integer" | "float" | "inet4" => KeyPart::Fixed(4),
        "bigint" | "double" | "real" | "set" | "datetime" | "timestamp" | "time" => {
            KeyPart::Fixed(8)
        }
        "decimal" | "numeric" => KeyPart::Fixed(n.unwrap_or(10) / 9 * 4 + 8),
        "bit" => KeyPart::Fixed(n.unwrap_or(1).div_ceil(8)),
        "uuid" | "inet6" => KeyPart::Fixed(16),
        "char" | "varchar" => text(n.unwrap_or(255), per_char, false),
        "binary" | "varbinary" => text(n.unwrap_or(255), 1, false),
        "tinytext" | "text" | "mediumtext" | "longtext" => text(TEXT_PREFIX, per_char, true),
        "tinyblob" | "blob" | "mediumblob" | "longblob" => text(TEXT_PREFIX, 1, true),
        _ => KeyPart::None,
    }
}

/// Key parts for `cols` within [`MAX_KEY_BYTES`]: fixed-size columns whole, string columns
/// sharing the rest (each whole if it fits, else a prefix). None if a string would get no
/// character at all.
pub(crate) fn fit_key(cols: &[(&MvCol, KeyPart)]) -> Option<String> {
    // (Length bytes of variable-length strings do not count.)
    let fixed: u64 = cols
        .iter()
        .map(|(_, k)| match k {
            KeyPart::Fixed(b) => *b,
            _ => 0,
        })
        .sum();
    let mut left = MAX_KEY_BYTES.checked_sub(fixed)?;
    let mut strings: Vec<usize> = (0..cols.len())
        .filter(|&i| matches!(cols[i].1, KeyPart::Str { .. }))
        .collect();
    strings.sort_by_key(|&i| match cols[i].1 {
        KeyPart::Str { len, per_char, .. } => len * per_char,
        _ => 0,
    });
    // Smallest first, so that what short strings leave over goes to the longer ones.
    let mut chars = vec![0u64; cols.len()];
    for (k, &i) in strings.iter().enumerate() {
        let KeyPart::Str { len, per_char, .. } = cols[i].1 else {
            continue;
        };
        let share = left / (strings.len() - k) as u64;
        chars[i] = len.min(share / per_char);
        if chars[i] == 0 {
            return None;
        }
        left -= chars[i] * per_char;
    }
    let parts: Vec<String> = cols
        .iter()
        .zip(chars)
        .map(|((c, k), n)| match *k {
            KeyPart::Str {
                len, must_prefix, ..
            } if must_prefix || n < len => format!("{}({n})", q(&c.name)),
            _ => q(&c.name),
        })
        .collect();
    Some(parts.join(", "))
}

/// Maximum bytes per character of a character set (4 when unknown: the table default is utf8mb4).
fn mbmaxlen(charset: Option<&str>) -> u64 {
    match charset.map(str::to_ascii_lowercase).as_deref() {
        Some(
            "latin1" | "latin2" | "latin5" | "latin7" | "ascii" | "binary" | "dec8" | "cp850"
            | "hp8" | "koi8r" | "koi8u" | "swe7" | "hebrew" | "tis620" | "greek" | "cp1250"
            | "cp1251" | "cp1256" | "cp1257" | "armscii8" | "cp866" | "keybcs2" | "macce"
            | "macroman" | "cp852" | "geostd8",
        ) => 1,
        Some("big5" | "sjis" | "euckr" | "gb2312" | "gbk" | "cp932" | "ucs2") => 2,
        Some("utf8mb3" | "utf8" | "ujis" | "eucjpms") => 3,
        _ => 4,
    }
}

/// `varchar(300)` → 300, `decimal(10,2)` → 10.
fn paren_number(column_type: &str) -> Option<u64> {
    let start = column_type.find('(')? + 1;
    let digits: String = column_type[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// This layout for a target of another product: every collation the target does not know is
/// replaced by the target's default collation of the column's character set; a column whose
/// character set is unknown too loses both clauses (the table default applies).
pub(crate) async fn with_target_collations(layout: &Layout, conn: &mut Conn) -> Result<Layout> {
    let rows: Vec<(String, String, String)> = conn
        .query(
            "SELECT CAST(COLLATION_NAME AS CHAR), CAST(CHARACTER_SET_NAME AS CHAR), \
             CAST(IS_DEFAULT AS CHAR) FROM information_schema.COLLATIONS",
        )
        .await
        .context("reading the target's collations")?;
    let known: HashSet<String> = rows
        .iter()
        .map(|(c, _, _)| c.to_ascii_lowercase())
        .collect();
    let default: HashMap<String, String> = rows
        .iter()
        .filter(|(_, _, d)| d.eq_ignore_ascii_case("yes"))
        .map(|(c, cs, _)| (cs.to_ascii_lowercase(), c.clone()))
        .collect();
    let mut l = layout.clone();
    for c in &mut l.cols {
        let (Some(cs), Some(co)) = (&c.charset, &c.collation) else {
            continue;
        };
        if known.contains(&co.to_ascii_lowercase()) {
            continue;
        }
        match default.get(&cs.to_ascii_lowercase()) {
            Some(d) => c.collation = Some(d.clone()),
            None => {
                c.charset = None;
                c.collation = None;
            }
        }
    }
    Ok(l)
}

/// The ownership marker (see [`crate::target::marker`]) of an existing table, if its comment
/// is one.
pub(crate) async fn table_marker(
    conn: &mut Conn,
    schema: &str,
    name: &str,
) -> Result<Option<String>> {
    let comment: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(TABLE_COMMENT AS CHAR) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?;
    Ok(comment.flatten().filter(|c| c.starts_with(MARKER_PREFIX)))
}

/// Whether `table` has the unique index its rows are upserted by (see [`Layout::row_key`]);
/// true when the layout has none. Tables created before upserts lack `_lv_row`.
pub(crate) async fn has_row_index(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
) -> Result<bool> {
    let (index, cols) = match row_index(layout) {
        None => return Ok(true),
        Some(RowIndex::Primary) => ("PRIMARY", &layout.unit_pos),
        Some(RowIndex::Unique(_)) => (ROW_INDEX, &layout.identity),
    };
    let parts: Vec<(String, i64, Option<i64>)> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(NON_UNIQUE AS SIGNED), CAST(SUB_PART AS SIGNED) \
             FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             AND INDEX_NAME = ? ORDER BY SEQ_IN_INDEX",
            (schema, table, index),
        )
        .await?;
    Ok(parts.len() == cols.len()
        && parts
            .iter()
            .zip(cols.iter())
            .all(|((name, non_unique, sub), &p)| {
                *non_unique == 0 && sub.is_none() && name.eq_ignore_ascii_case(&layout.cols[p].name)
            }))
}

/// `BASE TABLE` / `VIEW` / None.
pub(crate) async fn table_kind(
    conn: &mut Conn,
    schema: &str,
    name: &str,
) -> Result<Option<String>> {
    Ok(conn
        .exec_first(
            "SELECT CAST(TABLE_TYPE AS CHAR) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?)
}

/// Visible + invisible column names and types of an existing table, to validate a layout on resume.
pub(crate) async fn table_columns(
    conn: &mut Conn,
    schema: &str,
    name: &str,
) -> Result<Vec<(String, String)>> {
    Ok(conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        .await?)
}

pub(crate) fn layout_matches(layout: &Layout, existing: &[(String, String)]) -> bool {
    layout.cols.len() == existing.len()
        && layout.cols.iter().zip(existing).all(|(c, (n, t))| {
            c.name.eq_ignore_ascii_case(n) && c.column_type.eq_ignore_ascii_case(t)
        })
}

/// Creates the shadow table with an ownership marker (see [`crate::target::marker`]) as the
/// table comment.
pub(crate) async fn create_shadow_marked(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: &str,
) -> Result<String> {
    create_shadow(conn, schema, name, layout, Some(marker)).await
}

async fn create_shadow(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: Option<&str>,
) -> Result<String> {
    let shadow = shadow_name(name);
    conn.query_drop(format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(&shadow)))
        .await?;
    match conn
        .query_drop(layout.create_sql_marked(schema, &shadow, marker))
        .await
    {
        Ok(()) => {}
        // A collation or character set of the source this target does not know (MariaDB 13's
        // default `utf8mb4_uca1400_ai_ci` on MySQL): the target's default collation of each
        // column's character set is the closest match.
        Err(mysql_async::Error::Server(e)) if e.code == 1273 || e.code == 1115 => {
            let adapted = with_target_collations(layout, conn).await?;
            tracing::warn!(
                table = %name,
                "{}; creating the table with the target's default collations instead",
                e.message
            );
            conn.query_drop(adapted.create_sql_marked(schema, &shadow, marker))
                .await?;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(shadow)
}

/// Atomically replaces `name` with its shadow table (or renames the shadow when `name` is new).
pub(crate) async fn swap(conn: &mut Conn, schema: &str, name: &str) -> Result<()> {
    let shadow = shadow_name(name);
    let old = old_name(name);
    let (s, n) = (q(schema), q(name));
    // Readers with open transactions on the old table hold its metadata lock; don't queue
    // behind them for long (the caller retries), and don't block new readers meanwhile.
    conn.query_drop("SET SESSION lock_wait_timeout = 5").await?;
    if table_kind(conn, schema, &shadow).await?.is_none() {
        // Already swapped (an earlier attempt was interrupted after the rename).
        return Ok(());
    }
    if table_kind(conn, schema, name).await?.is_some() {
        conn.query_drop(format!("DROP TABLE IF EXISTS {s}.{}", q(&old)))
            .await?;
        conn.query_drop(format!(
            "RENAME TABLE {s}.{n} TO {s}.{}, {s}.{} TO {s}.{n}",
            q(&old),
            q(&shadow)
        ))
        .await?;
        conn.query_drop(format!("DROP TABLE IF EXISTS {s}.{}", q(&old)))
            .await?;
    } else {
        conn.query_drop(format!("RENAME TABLE {s}.{} TO {s}.{n}", q(&shadow)))
            .await?;
    }
    Ok(())
}

pub(crate) async fn drop_table(conn: &mut Conn, schema: &str, name: &str) -> Result<()> {
    conn.query_drop(format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(name)))
        .await?;
    Ok(())
}

/// Session time zone in which TIMESTAMPs are written (see [`crate::mv::Layout::epoch_timestamps`]).
pub(crate) const UTC_SESSION: &str = "SESSION time_zone = '+00:00'";

/// Session settings for every connection the service opens: non-locking reads, an sql_mode
/// in which values are stored as they were read (zero dates, no strict-mode rejections), and a
/// bounded table-lock wait (`lock_wait_s`) so a session blocked by a `LOCK TABLES` fails with
/// 1205 instead of waiting out the server default (a year).
pub(crate) async fn init_session(conn: &mut Conn, lock_wait_s: u64) -> Result<()> {
    let mode: Option<String> = conn
        .query_first("SELECT CAST(@@SESSION.sql_mode AS CHAR)")
        .await?;
    let mode = mode.unwrap_or_default();
    let permissive = permissive_sql_mode(&mode);
    let [isolation, set_mode, lock_wait] = session_setup(&permissive, lock_wait_s);
    conn.query_drop(isolation).await?;
    if permissive != mode {
        conn.query_drop(set_mode).await?;
    }
    conn.query_drop(lock_wait).await?;
    Ok(())
}

/// The statements that give a connection the service's session settings with `sql_mode` (a mode
/// [`permissive_sql_mode`] returned) and a table-lock wait of at most `lock_wait_s` seconds:
/// [`init_session`] runs them on a direct connection, the supervisor's pools
/// (src/supervisor.rs) on every new pooled connection.
pub(crate) fn session_setup(sql_mode: &str, lock_wait_s: u64) -> [String; 3] {
    [
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED".to_string(),
        format!("SET SESSION sql_mode = '{}'", sql_mode.replace('\'', "")),
        format!("SET SESSION lock_wait_timeout = {lock_wait_s}"),
    ]
}

/// `mode` without the flags that reject values the source holds (strict mode, zero dates) and
/// without the flags that change how SQL text is parsed (`ANSI_QUOTES`, `PIPES_AS_CONCAT`,
/// `HIGH_NOT_PRECEDENCE`, `NO_BACKSLASH_ESCAPES`, `IGNORE_SPACE`): lazyview parses definitions
/// and writes its SQL in the default mode, so under `ANSI_QUOTES` a `"paid"` the analyzer
/// traced as a string would be a column for the server. The combination modes (`ANSI`,
/// `TRADITIONAL`, `ORACLE`, ...) imply some of them and go too: the server lists their member
/// flags individually, so nothing else is lost.
pub(crate) fn permissive_sql_mode(mode: &str) -> String {
    mode.split(',')
        .map(str::trim)
        .filter(|m| {
            !m.is_empty()
                && m.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                && !matches!(
                    m.to_ascii_uppercase().as_str(),
                    "STRICT_TRANS_TABLES"
                        | "STRICT_ALL_TABLES"
                        | "NO_ZERO_DATE"
                        | "NO_ZERO_IN_DATE"
                        | "ANSI_QUOTES"
                        | "PIPES_AS_CONCAT"
                        | "HIGH_NOT_PRECEDENCE"
                        | "NO_BACKSLASH_ESCAPES"
                        | "IGNORE_SPACE"
                        | "TRADITIONAL"
                        | "ANSI"
                        | "DB2"
                        | "MAXDB"
                        | "MSSQL"
                        | "ORACLE"
                        | "POSTGRESQL"
                        | "MYSQL323"
                        | "MYSQL40"
                )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// [`WriteOpts`] for the server `conn` is connected to.
pub(crate) async fn write_opts(
    conn: &mut Conn,
    kind: Kind,
    batch_rows: usize,
    batch_bytes: usize,
) -> Result<WriteOpts> {
    let (version, packet): (String, u64) = conn
        .query_first("SELECT CAST(@@version AS CHAR), @@max_allowed_packet")
        .await?
        .context("reading @@version and @@max_allowed_packet")?;
    Ok(for_server(kind, &version, packet, batch_rows, batch_bytes))
}

/// For a server reporting `version` (`@@version`) and `max_packet` (`@@max_allowed_packet`).
pub(crate) fn for_server(
    kind: Kind,
    version: &str,
    max_packet: u64,
    batch_rows: usize,
    batch_bytes: usize,
) -> WriteOpts {
    let num: Vec<u32> = version
        .split(|c: char| !c.is_ascii_digit())
        .take(3)
        .map(|p| p.parse().unwrap_or(0))
        .collect();
    let mysql_alias = num.len() == 3 && (num[0], num[1], num[2]) >= (8, 0, 19);
    let upsert = if version.to_ascii_lowercase().contains("mariadb") || !mysql_alias {
        Upsert::Values
    } else {
        Upsert::RowAlias
    };
    let cap = usize::try_from(max_packet / 2).unwrap_or(usize::MAX);
    WriteOpts {
        batch_rows: batch_rows.max(1),
        batch_bytes: batch_bytes.min(cap).max(1024),
        upsert,
        kind,
    }
}

/// Only 1094 (`ER_NO_SUCH_THREAD`) proves the killed thread is gone. Any other server error —
/// 1045/1044 (no `SUPER`/`CONNECTION_ADMIN`), 1290 (`--read-only`), 1142 (killed) — means the
/// KILL never ran, so the fence must fail and `Endpoint::fence` parks the id in `zombies`.
fn kill_is_gone(err: &mysql_async::Error) -> bool {
    const ER_NO_SUCH_THREAD: u16 = 1094;
    matches!(err, mysql_async::Error::Server(se) if se.code == ER_NO_SUCH_THREAD)
}

/// `KILL CONNECTION id` on a fresh connection, then waits until `SHOW PROCESSLIST` (which lists
/// the user's own threads without privileges) no longer has it.
pub(crate) async fn kill_and_wait(opts: &Opts, id: u32) -> Result<()> {
    let fut = async {
        let mut c = Conn::new(opts.clone()).await?;
        match c.query_drop(format!("KILL CONNECTION {id}")).await {
            Ok(()) => {}
            // Only "unknown thread id" proves the thread is gone; anything else means the KILL did not run.
            Err(e) if kill_is_gone(&e) => {}
            Err(e) => {
                return Err(
                    anyhow::Error::from(e).context(format!("killing target connection {id}"))
                )
            }
        }
        let deadline = tokio::time::Instant::now() + FENCE_WAIT;
        loop {
            let rows: Vec<mysql_async::Row> = c.query("SHOW PROCESSLIST").await?;
            let listed = rows.iter().any(|r| {
                r.get::<Option<u64>, _>(0)
                    .flatten()
                    .is_some_and(|x| x == u64::from(id))
            });
            if !listed {
                let _ = c.disconnect().await;
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = c.disconnect().await;
                return Err(anyhow!(
                    "connection {id} still listed {FENCE_WAIT:?} after KILL"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    match tokio::time::timeout(FENCE_WAIT + CONNECT_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(anyhow::Error::new(Timeout(FENCE_WAIT + CONNECT_TIMEOUT))),
    }
}

/// The connection options of a MySQL-family target DSN. The source-side parser stays in
/// [`crate::binlog`]: a target DSN is not a replica DSN.
pub(crate) fn opts(dsn: &str) -> Result<Opts> {
    crate::binlog::replica_opts(dsn)
}

/// The schema a target DSN names (its path).
pub(crate) fn schema_of(opts: &Opts, what: &str) -> Result<String> {
    opts.db_name().map(str::to_string).ok_or_else(|| {
        anyhow!("{what} DSN must name a database (mysql://user:pass@host:port/<schema>)")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_err(code: u16) -> mysql_async::Error {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: "denied".into(),
            state: "HY000".into(),
        })
    }

    /// Only 1094 means the killed thread is already gone; an error that means the KILL never
    /// ran must keep the fence failing. Fails before the fix: any server error read as
    /// "already gone", so 1045/1290/1142 returned `true`.
    #[test]
    fn kill_is_gone_only_takes_unknown_thread() {
        assert!(kill_is_gone(&server_err(1094)));
        for code in [1045u16, 1290, 1142] {
            assert!(
                !kill_is_gone(&server_err(code)),
                "{code} is not already gone"
            );
        }
        let io =
            mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::other("net down")));
        assert!(!kill_is_gone(&io));
    }
}
