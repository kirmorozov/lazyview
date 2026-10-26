//! Materialized tables: layout (from a probe view on the source), shadow builds by streaming
//! non-locking reads, per-unit diff refreshes and atomic swaps.
//!
//! Sources are only ever read with plain SELECTs (never INSERT ... SELECT), so the service takes
//! no locks on source rows; all writes go to the target tables.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row, Value};

use crate::accumulate::{UnitChunk, UnitKind};
use crate::analyze::{quote_ident as q, Mode, ViewPlan};
use crate::datum::{Datum, Key};
use crate::metrics::Metrics;

/// Maximum placeholders per statement (server limit is 65535).
const MAX_PARAMS: usize = 60_000;
/// InnoDB's limit on the length of an index key, in bytes.
const MAX_KEY_BYTES: u64 = 3072;
/// Maximum number of columns in an index.
const MAX_KEY_PARTS: usize = 16;
/// Index prefix of TEXT / BLOB unit columns, in characters (bytes for BLOB).
const TEXT_PREFIX: u64 = 255;
/// Ownership marker prefix of the comment of every table lazyview creates (see [`marker`]).
pub const MARKER_PREFIX: &str = "lazyview:v1 ";
/// Name of the unique index over the row identity (see [`Layout::row_key`]).
const ROW_INDEX: &str = "_lv_row";
/// Row alias of upserts (`INSERT ... AS <alias> ON DUPLICATE KEY UPDATE`).
const ROW_ALIAS: &str = "_lv_new";
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
}

/// The unique index behind [`Layout::row_key`].
#[derive(Debug, Clone, PartialEq)]
enum RowIndex {
    /// The PRIMARY KEY on the unit.
    Primary,
    /// `_lv_row`, a UNIQUE KEY over these (whole) columns.
    Unique(String),
}

impl Layout {
    pub fn col_list(&self) -> String {
        self.cols
            .iter()
            .map(|c| q(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn unit_cols(&self) -> Vec<String> {
        self.unit_pos
            .iter()
            .map(|&p| q(&self.cols[p].name))
            .collect()
    }

    pub fn create_sql(&self, schema: &str, table: &str) -> String {
        self.create_sql_marked(schema, table, None)
    }

    /// `CREATE TABLE` with an optional ownership marker as the table comment (see [`marker`]).
    pub fn create_sql_marked(&self, schema: &str, table: &str, marker: Option<&str>) -> String {
        let index = self.unit_index();
        let row = self.row_index();
        let primary = matches!(index, Some((true, _)));
        let mut defs: Vec<String> = self
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut d = format!("{} {}", q(&c.name), c.column_type);
                if let (Some(cs), Some(co)) = (&c.charset, &c.collation) {
                    d.push_str(&format!(" CHARACTER SET {cs} COLLATE {co}"));
                }
                let in_pk = primary && self.unit_pos.contains(&i);
                d.push_str(if in_pk { " NOT NULL" } else { " NULL" });
                if i >= self.n_visible {
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
        for (name, parts) in self
            .lineage_indexes()
            .into_iter()
            .chain(self.partial_indexes())
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
    fn unit_index(&self) -> Option<(bool, String)> {
        let parts: Vec<(&MvCol, KeyPart)> = self
            .unit_pos
            .iter()
            .map(|&p| (&self.cols[p], self.cols[p].key_part()))
            .collect();
        if self.primary && parts.len() <= MAX_KEY_PARTS {
            let full: Option<u64> = parts.iter().map(|(_, k)| k.full_bytes()).sum();
            if full.is_some_and(|b| b <= MAX_KEY_BYTES) {
                return Some((true, self.unit_cols().join(", ")));
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
    fn row_index(&self) -> Option<RowIndex> {
        if self.identity.is_empty()
            || (!self.epoch_timestamps()
                && self
                    .identity
                    .iter()
                    .any(|&p| self.cols[p].data_type == "timestamp"))
        {
            return None;
        }
        if self.identity == self.unit_pos && matches!(self.unit_index(), Some((true, _))) {
            return Some(RowIndex::Primary);
        }
        if self.identity.len() > MAX_KEY_PARTS {
            return None;
        }
        let bytes: Option<u64> = self
            .identity
            .iter()
            .map(|&p| self.cols[p].key_part().full_bytes())
            .sum();
        if !bytes.is_some_and(|b| b <= MAX_KEY_BYTES) {
            return None;
        }
        Some(RowIndex::Unique(
            self.identity
                .iter()
                .map(|&p| q(&self.cols[p].name))
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    /// The row key changed rows are upserted by (positions in `cols`; see [`Layout::identity`]),
    /// when the table has a unique index over it. None: changed units are deleted and their
    /// rows inserted again.
    pub fn row_key(&self) -> Option<&[usize]> {
        self.row_index().map(|_| self.identity.as_slice())
    }

    /// How the values of `cols` are bound in a filter of a write (see [`write_diff`]): JSON as
    /// JSON values, TIMESTAMPs read in the session's time zone by local time (in a `+00:00`
    /// write session they are exact UTC datetimes).
    fn write_kinds(&self, cols: &[usize]) -> Vec<UnitKind> {
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
    fn epoch_timestamps(&self) -> bool {
        !self.epoch.is_empty()
    }

    /// The SELECT that reads the source rows (filtered by a unit predicate): the plan's SELECT
    /// with `UNIX_TIMESTAMP(<expression>)` of each TIMESTAMP column in front of its select list.
    /// (Applied to the expressions themselves: over the column of a derived table MySQL converts
    /// through the session's local time and loses the DST fold.)
    pub fn source_sql(&self, plan: &ViewPlan, filter: Option<&str>) -> String {
        let sql = plan.select_sql(&self.hidden, filter);
        if !self.epoch_timestamps() {
            return sql;
        }
        // (Checked by `probe`: the SELECT of an incremental plan starts like this.)
        let Some(rest) = sql.strip_prefix("SELECT ") else {
            return sql;
        };
        let extra: Vec<String> = self
            .epoch
            .iter()
            .enumerate()
            .map(|(k, (_, e))| format!("UNIX_TIMESTAMP({e}) AS `_lv_ts{k}`"))
            .collect();
        format!("SELECT {}, {rest}", extra.join(", "))
    }

    /// Column list that reads a materialized table (TIMESTAMP columns as `UNIX_TIMESTAMP`).
    fn read_list(&self) -> String {
        self.cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if self.epoch.iter().any(|(p, _)| *p == i) {
                    format!("UNIX_TIMESTAMP({})", q(&c.name))
                } else {
                    q(&c.name)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Brings a row read with [`Layout::source_sql`] into the form of [`Layout::normalize`]:
    /// the leading epochs replace the TIMESTAMP values.
    pub fn normalize_source(&self, mut row: Vec<Value>) -> Vec<Value> {
        if self.epoch_timestamps() && row.len() == self.cols.len() + self.epoch.len() {
            let epochs: Vec<Value> = row.drain(..self.epoch.len()).collect();
            for ((p, _), e) in self.epoch.iter().zip(epochs) {
                row[*p] = e;
            }
        }
        self.normalize(row)
    }

    /// Brings a row read with [`Layout::read_list`] (or [`Layout::normalize_source`]) into the
    /// form that is compared and written back: BIT values as unsigned integers (bound as strings
    /// the server stores 0 for `b'11111111'`), TIME values in the form every server accepts as a
    /// parameter (see [`crate::datum::time_param`]), TIMESTAMP epochs as UTC datetimes.
    pub fn normalize(&self, mut row: Vec<Value>) -> Vec<Value> {
        for (c, v) in self.cols.iter().zip(row.iter_mut()) {
            match (c.data_type.as_str(), &*v) {
                ("bit", Value::Bytes(b)) => {
                    *v = Value::UInt(b.iter().fold(0u64, |acc, x| (acc << 8) | *x as u64));
                }
                // TIME values MySQL >= 9.0 rejects as binary parameters go as text (both the
                // rows read from the source and those read back from the table, so they
                // still compare equal).
                ("time", Value::Time(..)) => {
                    *v = crate::datum::time_param(std::mem::replace(v, Value::NULL));
                }
                _ => {}
            }
        }
        for (p, _) in &self.epoch {
            if let Some(v) = row.get_mut(*p) {
                *v = epoch_to_utc(std::mem::replace(v, Value::NULL));
            }
        }
        row
    }

    /// One non-unique index per lineage key, `(name, key parts)` (none for a key that does not
    /// fit InnoDB's limits).
    fn lineage_indexes(&self) -> Vec<(String, String)> {
        self.lineage
            .iter()
            .filter_map(|key| {
                let alias = lineage_alias(&self.cols[*key.first()?].name)?;
                let parts: Vec<(&MvCol, KeyPart)> = key
                    .iter()
                    .map(|&p| (&self.cols[p], self.cols[p].key_part()))
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
    fn partial_indexes(&self) -> Vec<(String, String)> {
        let unit_index = self.unit_index().is_some();
        self.partial
            .iter()
            .enumerate()
            .filter(|(_, at)| !(unit_index && at.iter().copied().eq(0..at.len())))
            .filter_map(|(k, at)| {
                let parts: Vec<(&MvCol, KeyPart)> = at
                    .iter()
                    .map(|&u| {
                        (
                            &self.cols[self.unit_pos[u]],
                            self.cols[self.unit_pos[u]].key_part(),
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

    /// Whether the table has an index on the unit columns (PRIMARY KEY or `_lv_unit`, see
    /// [`Layout::create_sql`]). Without one, unit DELETEs scan the table and concurrent writes of
    /// disjoint units would wait on each other's row locks.
    pub fn has_unit_index(&self) -> bool {
        self.unit_index().is_some()
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

    /// This layout for a target of another product: every collation the target does not know is
    /// replaced by the target's default collation of the column's character set; a column whose
    /// character set is unknown too loses both clauses (the table default applies).
    pub async fn with_target_collations(&self, conn: &mut Conn) -> Result<Layout> {
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
        let mut l = self.clone();
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
}

/// How a column can be part of an index.
#[derive(Debug, Clone, Copy, PartialEq)]
enum KeyPart {
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
    fn full_bytes(&self) -> Option<u64> {
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

impl MvCol {
    fn key_part(&self) -> KeyPart {
        let ct = self.column_type.to_ascii_lowercase();
        let n = paren_number(&ct);
        let per_char = mbmaxlen(self.charset.as_deref());
        let text = |len, per_char, must_prefix| KeyPart::Str {
            len,
            per_char,
            must_prefix,
        };
        match self.data_type.as_str() {
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
}

/// Key parts for `cols` within [`MAX_KEY_BYTES`]: fixed-size columns whole, string columns
/// sharing the rest (each whole if it fits, else a prefix). None if a string would get no
/// character at all.
fn fit_key(cols: &[(&MvCol, KeyPart)]) -> Option<String> {
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

/// `_lv_a<alias>_pk<j>` (a lineage column) → `(alias, j)`.
fn lineage_col(name: &str) -> Option<(usize, usize)> {
    let (alias, j) = name.strip_prefix("_lv_a")?.split_once("_pk")?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(alias) || !digits(j) {
        return None;
    }
    Some((alias.parse().ok()?, j.parse().ok()?))
}

fn lineage_alias(name: &str) -> Option<usize> {
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

/// The expression of a rendered select item: without its trailing `AS <alias>`, if any.
fn item_expr(item: &str) -> &str {
    let Some((expr, alias)) = item.rsplit_once(" AS ") else {
        return item;
    };
    let alias = alias.trim();
    let quoted = |o: char, c: char| {
        alias.len() >= 2
            && alias.starts_with(o)
            && alias.ends_with(c)
            && !alias[1..alias.len() - 1]
                .replace(&format!("{c}{c}"), "")
                .contains(c)
    };
    let bare = !alias.is_empty()
        && alias
            .chars()
            .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$');
    if quoted('`', '`') || quoted('"', '"') || bare {
        expr
    } else {
        item
    }
}

/// The TIMESTAMP columns of a probed layout with the expressions that compute them (see
/// [`Layout::epoch`]); empty unless every one has a known expression.
fn epoch_exprs(
    plan: &ViewPlan,
    cols: &[MvCol],
    n_visible: usize,
    hidden: &[usize],
) -> Vec<(usize, String)> {
    if matches!(plan.mode, Mode::Full(_)) || !plan.select_sql(hidden, None).starts_with("SELECT ") {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (i, c) in cols.iter().enumerate() {
        if c.data_type != "timestamp" {
            continue;
        }
        let expr = if i < n_visible {
            plan.projection.get(i).map(|s| item_expr(s).to_string())
        } else {
            hidden
                .get(i - n_visible)
                .and_then(|&u| plan.unit_exprs.get(u).cloned())
        };
        match expr {
            Some(e) => out.push((i, e)),
            None => return Vec::new(),
        }
    }
    out
}

/// A `UNIX_TIMESTAMP()` result (`<seconds>[.<fraction>]`) as the UTC datetime; 0 is the zero
/// TIMESTAMP. Other values are returned unchanged.
fn epoch_to_utc(v: Value) -> Value {
    let (secs, micros) = match &v {
        Value::Int(s) => (*s, 0),
        Value::UInt(s) => match i64::try_from(*s) {
            Ok(s) => (s, 0),
            Err(_) => return v,
        },
        Value::Bytes(b) => match parse_epoch(b) {
            Some(e) => e,
            None => return v,
        },
        _ => return v,
    };
    if secs == 0 && micros == 0 {
        return Value::Date(0, 0, 0, 0, 0, 0, 0);
    }
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let t = secs.rem_euclid(86_400);
    let Ok(y) = u16::try_from(y) else { return v };
    Value::Date(
        y,
        m,
        d,
        (t / 3600) as u8,
        (t % 3600 / 60) as u8,
        (t % 60) as u8,
        micros,
    )
}

fn parse_epoch(b: &[u8]) -> Option<(i64, u32)> {
    let s = std::str::from_utf8(b).ok()?;
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    if !frac.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let frac = format!("{:0<6}", &frac[..frac.len().min(6)]);
    Some((int.parse().ok()?, frac.parse().ok()?))
}

/// Proleptic Gregorian (year, month, day) of a day number counted from 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u8;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// Ownership marker for the tables of view `view` built from definition `def_id`, stored as the
/// table comment: `lazyview:v1 view=<name> def=<id>`, the name percent-encoded (`%`, quotes,
/// backslashes, spaces and control characters), so the comment needs no escaping.
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

/// The ownership marker (see [`marker`]) of an existing table, if its comment is one.
pub async fn table_marker(conn: &mut Conn, schema: &str, name: &str) -> Result<Option<String>> {
    let comment: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(TABLE_COMMENT AS CHAR) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?;
    Ok(comment.flatten().filter(|c| c.starts_with(MARKER_PREFIX)))
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

/// Session settings for every connection the service opens: non-locking reads, and an sql_mode
/// in which values are stored as they were read (zero dates, no strict-mode rejections).
pub async fn init_session(conn: &mut Conn) -> Result<()> {
    conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await?;
    let mode: Option<String> = conn
        .query_first("SELECT CAST(@@SESSION.sql_mode AS CHAR)")
        .await?;
    if let Some(mode) = mode {
        let permissive = permissive_sql_mode(&mode);
        if permissive != mode {
            conn.query_drop(format!("SET SESSION sql_mode = '{permissive}'"))
                .await?;
        }
    }
    Ok(())
}

/// `mode` without the flags that reject values the source holds (strict mode, zero dates) and
/// without the flags that change how SQL text is parsed (`ANSI_QUOTES`, `PIPES_AS_CONCAT`,
/// `HIGH_NOT_PRECEDENCE`, `NO_BACKSLASH_ESCAPES`, `IGNORE_SPACE`): lazyview parses definitions
/// and writes its SQL in the default mode, so under `ANSI_QUOTES` a `"paid"` the analyzer
/// traced as a string would be a column for the server. The combination modes (`ANSI`,
/// `TRADITIONAL`, `ORACLE`, ...) imply some of them and go too: the server lists their member
/// flags individually, so nothing else is lost.
fn permissive_sql_mode(mode: &str) -> String {
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

/// Derives the materialized layout by creating a temporary view on the source and reading its
/// column types from `information_schema`.
pub async fn probe(src: &mut Conn, src_schema: &str, plan: &ViewPlan) -> Result<Layout> {
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
    })
}

/// Whether `table` has the unique index its rows are upserted by (see [`Layout::row_key`]);
/// true when the layout has none. Tables created before upserts lack `_lv_row`.
pub async fn has_row_index(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
) -> Result<bool> {
    let (index, cols) = match layout.row_index() {
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
pub async fn table_kind(conn: &mut Conn, schema: &str, name: &str) -> Result<Option<String>> {
    Ok(conn
        .exec_first(
            "SELECT CAST(TABLE_TYPE AS CHAR) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?)
}

/// Visible + invisible column names and types of an existing table, to validate a layout on resume.
pub async fn table_columns(
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

pub fn layout_matches(layout: &Layout, existing: &[(String, String)]) -> bool {
    layout.cols.len() == existing.len()
        && layout.cols.iter().zip(existing).all(|(c, (n, t))| {
            c.name.eq_ignore_ascii_case(n) && c.column_type.eq_ignore_ascii_case(t)
        })
}

pub async fn create_shadow(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    layout: &Layout,
) -> Result<String> {
    create_shadow_with(conn, schema, name, layout, None).await
}

/// [`create_shadow`] with an ownership marker (see [`marker`]) as the table comment.
pub async fn create_shadow_marked(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: &str,
) -> Result<String> {
    create_shadow_with(conn, schema, name, layout, Some(marker)).await
}

async fn create_shadow_with(
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
            let adapted = layout.with_target_collations(conn).await?;
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
pub async fn swap(conn: &mut Conn, schema: &str, name: &str) -> Result<()> {
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

pub async fn drop_table(conn: &mut Conn, schema: &str, name: &str) -> Result<()> {
    conn.query_drop(format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(name)))
        .await?;
    Ok(())
}

/// Session time zone in which TIMESTAMPs are written (see [`Layout::epoch_timestamps`]).
const UTC_SESSION: &str = "SESSION time_zone = '+00:00'";

fn rows_per_insert(layout: &Layout, batch_rows: usize) -> usize {
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
}

impl WriteOpts {
    /// For a server reporting `version` (`@@version`) and `max_packet` (`@@max_allowed_packet`).
    pub fn for_server(
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
        }
    }
}

/// [`WriteOpts`] for the server `conn` is connected to.
pub async fn write_opts(
    conn: &mut Conn,
    batch_rows: usize,
    batch_bytes: usize,
) -> Result<WriteOpts> {
    let (version, packet): (String, u64) = conn
        .query_first("SELECT CAST(@@version AS CHAR), @@max_allowed_packet")
        .await?
        .context("reading @@version and @@max_allowed_packet")?;
    Ok(WriteOpts::for_server(
        &version,
        packet,
        batch_rows,
        batch_bytes,
    ))
}

/// Estimated bytes a value takes in a statement.
fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Bytes(b) => b.len() + 9,
        Value::NULL => 1,
        _ => 13,
    }
}

fn datum_bytes(d: &Datum) -> usize {
    match d {
        Datum::Bytes(b) => b.len() + 9,
        _ => 13,
    }
}

/// Consecutive runs of `items` with at most `max_items` items and `max_bytes` estimated bytes
/// each (at least one item per run).
fn batches<T>(
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

/// A multi-row INSERT of `n` rows; with `upsert`, rows whose key exists replace that row.
fn insert_sql(
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
    }
    sql
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
        let sql = insert_sql(
            schema,
            table,
            layout,
            chunk.len(),
            upsert.then_some(w.upsert),
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
    keys: &[Key],
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<()> {
    let Some(key) = layout.row_key() else {
        bail!("internal: rows of `{table}` have no row key");
    };
    let cols: Vec<String> = key.iter().map(|&p| q(&layout.cols[p].name)).collect();
    let kinds = layout.write_kinds(key);
    let per = MAX_PARAMS / cols.len().max(1);
    let key_bytes = |k: &Key| 4 + k.iter().map(datum_bytes).sum::<usize>();
    for piece in batches(keys, per, w.batch_bytes, key_bytes) {
        let (f, p) = keys_chunk(piece.to_vec()).filter_typed(&cols, &kinds);
        conn.exec_drop(
            format!("DELETE FROM {}.{} WHERE {f}", q(schema), q(table)),
            p,
        )
        .await?;
        Metrics::add(&metrics.mv_queries, 1);
    }
    Ok(())
}

pub struct BuildTarget<'a> {
    pub conn: &'a mut Conn,
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
    let shadow = shadow_name(name);
    let sql = layout.source_sql(plan, None);
    // Read in pieces of a batch; each target splits them by its own limits.
    let per = rows_per_insert(layout, batch_rows);
    if layout.epoch_timestamps() {
        // The build's own connections: they only insert from here on.
        for t in targets.iter_mut() {
            t.conn.query_drop(format!("SET {UTC_SESSION}")).await?;
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
                insert_rows(
                    t.conn, &t.schema, &shadow, layout, &buf, &t.write, false, metrics,
                )
                .await?;
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

/// A JSON text in the form the server stores it, for comparisons. MySQL parses JSON text into
/// its binary format: integers stay integers, every other number becomes a DOUBLE, object keys
/// are sorted and the last of duplicate keys wins. So a DECIMAL inside a JSON value the view
/// computes (`JSON_ARRAYAGG(amount)` gives `[10.00, 20.50]`) is stored as `[10.0, 20.5]`: no
/// client can write it back as a DECIMAL. Two texts get the same result here when the server
/// stores the same value for them (numbers parsed with correct rounding, like the server's
/// parser); the result is JSON-equal to the input, so it also binds as a `CAST(? AS JSON)`
/// unit key. Text that does not parse is kept as it is.
pub fn json_canonical(text: &[u8]) -> Vec<u8> {
    serde_json::from_slice::<serde_json::Value>(text)
        .ok()
        .and_then(|v| serde_json::to_vec(&v).ok())
        .unwrap_or_else(|| text.to_vec())
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
    /// Of those, rows upserted by their row key.
    pub upserted: u64,
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
    /// New rows written: inserted (per unit) or upserted (by row key).
    pub inserts: Vec<Vec<Value>>,
    /// Rows compared (old + new), for statistics.
    pub compared: usize,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty()
    }
}

/// Current MV rows of the units in `chunk` (all rows when `None`).
pub async fn read_old(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    layout: &Layout,
    chunk: Option<&UnitChunk>,
    metrics: &Metrics,
) -> Result<Vec<Vec<Value>>> {
    let unit_cols = layout.unit_cols();
    let (filter, params) = match chunk {
        Some(c) => c.filter_typed(&unit_cols, &layout.unit_kinds()),
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
/// key when the layout has one (pure CPU work). Fails if new rows share a row key without a
/// NULL: the identity the table's unique index is built on does not hold.
pub fn diff(layout: &Layout, old: Vec<Vec<Value>>, new: &Prepared) -> Result<Diff> {
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
            continue;
        }
        d.changed.push(k.clone());
        match row_key {
            None => {
                d.deleted_rows += o.len() as u64;
                d.inserts.extend(n.iter().map(|r| r.1.clone()));
            }
            Some(key) => diff_rows(key, o, n, &mut d)?,
        }
    }
    Ok(d)
}

/// The rows of one changed unit, compared per row key (see [`diff`]).
fn diff_rows(key: &[usize], old: Vec<&Row2>, new: Vec<&Row2>, d: &mut Diff) -> Result<()> {
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
            continue;
        }
        let null = crate::datum::key_has_null(&k);
        if n.len() > 1 && !null {
            bail!(
                "internal: {} rows of the view share the row key {k:?}; the table's unique index \
                 `{ROW_INDEX}` cannot hold them",
                n.len()
            );
        }
        // An old row with the same key is replaced in place, unless the key has a NULL (or the
        // table holds it more than once): those are deleted and inserted again.
        if !o.is_empty() && (null || o.len() > 1) {
            d.delete_keys.push(k);
            d.deleted_rows += o.len() as u64;
        }
        d.inserts.extend(n.iter().map(|r| r.1.clone()));
    }
    for (k, o) in old {
        d.delete_keys.push(k);
        d.deleted_rows += o.len() as u64;
    }
    Ok(())
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
        return Ok(Applied::default());
    }
    began.store(true, Ordering::SeqCst);
    let utc = layout.epoch_timestamps();
    if utc {
        // TIMESTAMPs (also as keys of the DELETEs) are UTC datetimes; the session's own time
        // zone is restored for the unit filters of the next read.
        conn.query_drop(format!("SET @lv_tz = @@SESSION.time_zone, {UTC_SESSION}"))
            .await?;
    }
    let r = write_tx(conn, schema, table, layout, d, w, metrics).await;
    if utc {
        let restored = conn.query_drop("SET SESSION time_zone = @lv_tz").await;
        if r.is_ok() {
            restored?;
        }
    }
    r
}

/// The transaction of [`write_diff`].
async fn write_tx(
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
        upserted: if d.by_unit { 0 } else { d.inserts.len() as u64 },
    };
    let mut tx = conn.start_transaction(Default::default()).await?;
    if d.by_unit && d.deleted_rows > 0 {
        let del_filter = if layout.unit_pos.is_empty() {
            ("TRUE".to_string(), Vec::new())
        } else {
            keys_chunk(d.changed.clone())
                .filter_typed(&layout.unit_cols(), &layout.write_kinds(&layout.unit_pos))
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
        delete_keys(&mut tx, schema, table, layout, &d.delete_keys, w, metrics).await?;
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
    let old = read_old(conn, schema, table, layout, chunk, metrics).await?;
    let new = prepare(layout, new_rows.to_vec());
    let d = diff(layout, old, &new)
        .with_context(|| format!("comparing the rows of `{schema}`.`{table}`"))?;
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
        let sql = l.create_sql("s", "t");
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

    #[test]
    fn select_item_expressions() {
        assert_eq!(item_expr("MAX(ts) AS mx"), "MAX(ts)");
        assert_eq!(item_expr("MAX(`e`.`ts`) AS `m x`"), "MAX(`e`.`ts`)");
        assert_eq!(item_expr("x AS `a``b`"), "x");
        assert_eq!(item_expr("x AS \"q\""), "x");
        assert_eq!(item_expr("CAST(x AS SIGNED)"), "CAST(x AS SIGNED)");
        assert_eq!(item_expr("'a AS b'"), "'a AS b'");
        assert_eq!(item_expr("`ev`.`ts`"), "`ev`.`ts`");
        assert_eq!(item_expr("x AS `a` + 1"), "x AS `a` + 1");
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
        assert_eq!(epoch, vec![(1, item_expr(&p.projection[1]).to_string())]);
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
        let sql = l.create_sql("s", "t");
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
        assert!(l.create_sql("s", "t").contains("PRIMARY KEY (`k`)"));
        let l = layout(
            vec![
                col("k", "varchar", "varchar(1000)", Some("utf8mb4")),
                col("v", "int", "int", None),
            ],
            2,
            vec![0],
            true,
        );
        let sql = l.create_sql("s", "t");
        assert!(!sql.contains("PRIMARY KEY"), "{sql}");
        assert!(sql.contains("KEY `_lv_unit` (`k`(768))"), "{sql}");
        assert!(sql.contains("`k` varchar(1000) NULL"), "{sql}");
        let texts: Vec<MvCol> = (1..=4)
            .map(|i| col(&format!("t{i}"), "text", "text", Some("utf8mb4")))
            .collect();
        let l = layout(texts, 4, vec![0, 1, 2, 3], false);
        let sql = l.create_sql("s", "t");
        assert!(
            sql.contains("KEY `_lv_unit` (`t1`(192), `t2`(192), `t3`(192), `t4`(192))"),
            "{sql}"
        );
        let l = layout(vec![col("j", "json", "json", None)], 1, vec![0], false);
        assert!(
            !l.create_sql("s", "t").contains("KEY"),
            "JSON units get no index"
        );
        assert!(!l.has_unit_index());
    }

    /// JSON texts compare as the server stores them: a DECIMAL's scale and the formatting are
    /// lost, integers and doubles stay apart.
    #[test]
    fn json_texts_compare_as_stored() {
        let c = |s: &str| String::from_utf8(json_canonical(s.as_bytes())).unwrap();
        // The text a view computes vs. what the MV table returns for it (measured, MySQL 8.4).
        for (computed, stored) in [
            ("[10.00, 20.50]", "[10.0, 20.5]"),
            (r#"{"amt": 1.50}"#, r#"{"amt": 1.5}"#),
            ("100.000", "100.0"),
            ("0.000000150", "0.00000015"),
            ("12345678901234567890.1234567890", "1.2345678901234567e19"),
            ("18446744073709551616", "1.8446744073709552e19"),
            (
                r#"{"b":1,"a":2,"aa":3,"a":4}"#,
                r#"{"a": 4, "b": 1, "aa": 3}"#,
            ),
            ("[0.30000000000000004, 1e20]", "[0.30000000000000004, 1e20]"),
        ] {
            assert_eq!(c(computed), c(stored), "{computed} vs {stored}");
            assert_eq!(c(&c(stored)), c(stored), "canonical text is stable");
        }
        // Integers and doubles are different values in the server's format (`1` vs `1.0`).
        assert_ne!(c("[1]"), c("[1.0]"));
        assert_ne!(c(r#"{"a": 10}"#), c(r#"{"a": 10.0}"#));
        assert_eq!(
            c("[18446744073709551615, -9223372036854775808]"),
            "[18446744073709551615,-9223372036854775808]"
        );
        assert_ne!(c("[10.00]"), c("[10.01]"));
        assert_ne!(c(r#"["10.00"]"#), c(r#"["10.0"]"#), "strings stay exact");
        assert_eq!(c("not json"), "not json");
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
        let d = diff(&l, stored.clone(), &prepare(&l, source.clone())).unwrap();
        assert!(d.is_empty(), "rewrote equal units: {:?}", d.changed);
        // A real change of one unit rewrites that unit only.
        let mut changed = source.clone();
        changed[1] = row(2, "[3.26]", "[1, 2]");
        let d = diff(&l, stored.clone(), &prepare(&l, changed)).unwrap();
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
        )
        .unwrap();
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
        let sql = l.create_sql("s", "t");
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
        let sql = l.create_sql("s", "t");
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
            .create_sql("s", "t")
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
        let sql = l.create_sql("s", "t");
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
        assert!(l.create_sql("s", "t").contains("PRIMARY KEY (`ts`)"));
        l.epoch = vec![(0, "ts".into())];
        assert_eq!(l.row_key(), Some(&[0][..]));
        // No identity: no row key.
        let l = layout(vec![col("v", "int", "int", None)], 1, vec![], false);
        assert_eq!(l.row_key(), None);
    }

    #[test]
    fn upsert_syntax_and_packet_limits() {
        let w = |v: &str, p: u64| WriteOpts::for_server(v, p, 500, DEFAULT_WRITE_BATCH_BYTES);
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
            insert_sql("s", "t", &l, 2, None),
            "INSERT INTO `s`.`t` (`g`, `n`) VALUES (?,?),(?,?)"
        );
        assert_eq!(
            insert_sql("s", "t", &l, 1, Some(Upsert::RowAlias)),
            "INSERT INTO `s`.`t` (`g`, `n`) VALUES (?,?) AS `_lv_new` \
             ON DUPLICATE KEY UPDATE `g` = `_lv_new`.`g`, `n` = `_lv_new`.`n`"
        );
        assert_eq!(
            insert_sql("s", "t", &l, 1, Some(Upsert::Values)),
            "INSERT INTO `s`.`t` (`g`, `n`) VALUES (?,?) \
             ON DUPLICATE KEY UPDATE `g` = VALUES(`g`), `n` = VALUES(`n`)"
        );
        assert!(
            insert_sql("s", "_LV_NEW", &l, 1, Some(Upsert::RowAlias)).contains("AS `_lv_new_`"),
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
        let d = diff(&l, old.clone(), &prepare(&l, old.clone())).unwrap();
        assert!(d.is_empty() && d.inserts.is_empty() && d.delete_keys.is_empty());
        // One row of a 3-row unit changes: one upsert, no delete, unit counted once.
        let mut new = old.clone();
        new[1] = r(1, "B", Some(11));
        let d = diff(&l, old.clone(), &prepare(&l, new)).unwrap();
        assert!(!d.by_unit);
        assert_eq!(d.changed.len(), 1);
        assert_eq!(d.inserts, vec![r(1, "B", Some(11))]);
        assert!(d.delete_keys.is_empty());
        assert_eq!(d.deleted_rows, 0);
        // The unit shrinks (12 vanishes) and grows (13 appears); unit 3 vanishes entirely.
        let new = vec![
            r(1, "a", Some(10)),
            r(1, "b", Some(11)),
            r(1, "d", Some(13)),
            r(2, "x", None),
        ];
        let d = diff(&l, old.clone(), &prepare(&l, new)).unwrap();
        assert_eq!(d.changed.len(), 2);
        assert_eq!(d.inserts, vec![r(1, "d", Some(13))]);
        assert_eq!(sorted(d.delete_keys), vec![k(1, Some(12)), k(3, Some(30))]);
        assert_eq!(d.deleted_rows, 2);
        // A row with a NULL in its key changes: delete (null-safe) + insert.
        let mut new = old.clone();
        new[3] = r(2, "X", None);
        let d = diff(&l, old.clone(), &prepare(&l, new)).unwrap();
        assert_eq!(d.delete_keys, vec![k(2, None)]);
        assert_eq!(d.inserts, vec![r(2, "X", None)]);
        assert_eq!(d.deleted_rows, 1);
        // The unmatched row gets a match: its NULL-key row goes, the new key is upserted.
        let mut new = old.clone();
        new[3] = r(2, "x", Some(20));
        let d = diff(&l, old.clone(), &prepare(&l, new)).unwrap();
        assert_eq!(d.delete_keys, vec![k(2, None)]);
        assert_eq!(d.inserts, vec![r(2, "x", Some(20))]);
        // A row moves from unit 3 to unit 1 (another unit's lineage now): both units change.
        let mut new = old.clone();
        new[4] = r(1, "y", Some(30));
        let d = diff(&l, old.clone(), &prepare(&l, new)).unwrap();
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
        let d = diff(&l2, vec![g("a", 1)], &prepare(&l2, vec![g("A", 1)])).unwrap();
        assert_eq!(
            d.delete_keys,
            vec![Key::from([Datum::Bytes(b"a".to_vec())])]
        );
        assert_eq!(d.inserts, vec![g("A", 1)]);
        // Rows sharing a key without a NULL contradict the unique index: an error, not data loss.
        let e = diff(
            &l,
            vec![],
            &prepare(&l, vec![r(1, "a", Some(10)), r(1, "b", Some(10))]),
        );
        assert!(e.unwrap_err().to_string().contains("share the row key"));
        // ... with a NULL they are legal (NULLs never conflict) and inserted as they are.
        let d = diff(
            &l,
            vec![],
            &prepare(&l, vec![r(5, "a", None), r(5, "b", None)]),
        )
        .unwrap();
        assert_eq!(d.inserts.len(), 2);
        // Without a row key the changed units are rewritten whole, as before.
        let mut l3 = l.clone();
        l3.identity = vec![];
        let mut new = old.clone();
        new[1] = r(1, "B", Some(11));
        let d = diff(&l3, old.clone(), &prepare(&l3, new)).unwrap();
        assert!(d.by_unit && d.delete_keys.is_empty());
        assert_eq!((d.inserts.len(), d.deleted_rows), (3, 3));
    }

    #[test]
    fn markers_and_sql_modes() {
        assert_eq!(marker("v", 7), "lazyview:v1 view=v def=7");
        assert_eq!(marker("a b'c%", 1), "lazyview:v1 view=a%20b%27c%25 def=1");
        let l = layout(vec![col("v", "int", "int", None)], 1, vec![], false);
        assert!(l
            .create_sql_marked("s", "t", Some("it's"))
            .ends_with("DEFAULT CHARSET=utf8mb4 COMMENT='it''s'"));
        assert_eq!(
            permissive_sql_mode(
                "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
            ),
            "ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
        );
        // Parser flags and the combination modes implying them go too (the server lists the
        // members of a combination mode individually, so `ANSI` keeps `REAL_AS_FLOAT`).
        assert_eq!(permissive_sql_mode("TRADITIONAL,ANSI_QUOTES"), "");
        assert_eq!(
            permissive_sql_mode(
                "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI,\
                 NO_BACKSLASH_ESCAPES,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION"
            ),
            "REAL_AS_FLOAT,ONLY_FULL_GROUP_BY,NO_ENGINE_SUBSTITUTION"
        );
    }
}
