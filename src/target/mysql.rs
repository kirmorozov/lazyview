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
use crate::target::{
    lineage_indexes, old_name, partial_indexes, row_index, shadow_name, unit_index, KeyPart,
    KeyPlan, Kind, RowIndex, MARKER_PREFIX,
};

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
        // `ON CONFLICT` is PostgreSQL's (target/postgres.rs renders it, T4); a MySQL
        // connection never carries these opts (they come from `PgConn::write_opts`).
        None | Some(Upsert::OnConflict) => {}
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

/// Index prefix of TEXT / BLOB unit columns, in characters (bytes for BLOB).
const TEXT_PREFIX: u64 = 255;

/// `CREATE TABLE` with an optional ownership marker as the table comment (see
/// [`crate::target::marker`]).
pub(crate) fn create_sql_marked(
    layout: &Layout,
    schema: &str,
    table: &str,
    marker: Option<&str>,
) -> String {
    let plan = KeyPlan::for_layout(layout, Kind::Mysql);
    let index = unit_index(layout, &plan);
    let row = row_index(layout, &plan);
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
    for (name, parts) in lineage_indexes(layout, &plan)
        .into_iter()
        .chain(partial_indexes(layout, &plan))
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
    let (index, cols) = match row_index(layout, &KeyPlan::for_layout(layout, Kind::Mysql)) {
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
/// Whether `e` says the target does not know a collation / character set (MariaDB 13's
/// default `utf8mb4_uca1400_ai_ci` on MySQL): the adapt branch below, never a retry.
fn collation_gap(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(se) if se.code == 1273 || se.code == 1115)
}

/// Creates the shadow table: the drop + create is one idempotent block through the shared
/// policy (R4.2); the 1273/1115 collation adapt keeps its own non-retry branch.
pub(crate) async fn create_shadow_marked(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: &str,
    metrics: &crate::metrics::Metrics,
) -> Result<String> {
    let shadow = shadow_name(name);
    // The statements are identical on every try: render once, clone per try.
    let drop_sql = format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(&shadow));
    let create_sql = layout.create_sql_marked(Kind::Mysql, schema, &shadow, Some(marker));
    let known = super::with_retry_mut(
        metrics,
        "creating the shadow table",
        crate::supervisor::Policy::STATEMENTS,
        conn,
        |conn| {
            let (drop_sql, create_sql) = (drop_sql.clone(), create_sql.clone());
            Box::pin(async move {
                conn.query_drop(drop_sql).await?;
                match conn.query_drop(create_sql).await {
                    Ok(()) => Ok(true),
                    Err(e) if collation_gap(&e) => Ok(false),
                    Err(e) => Err(e.into()),
                }
            })
        },
    )
    .await?;
    if known {
        return Ok(shadow);
    }
    // The target's default collation of each column's character set is the closest match.
    let adapted = with_target_collations(layout, conn).await?;
    tracing::warn!(
        table = %name,
        "the source collations are unknown to this target; creating the table with the target's default collations instead"
    );
    conn.query_drop(adapted.create_sql_marked(Kind::Mysql, schema, &shadow, Some(marker)))
        .await?;
    Ok(shadow)
}

/// Atomically replaces `name` with its shadow table (or renames the shadow when `name` is new).
/// Atomically replaces `name` with its shadow table: one idempotent block through the shared
/// policy (R4.2) — the `table_kind` checks below make a repeat a no-op ("already swapped"), so a
/// transient 1205 (a reader's metadata lock) is simply retried — and the lock wait comes from
/// the one knob, `lock_wait_s` (R4.3), not a second hardcoded 5 s.
pub(crate) async fn swap(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    lock_wait_s: u64,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    let shadow = shadow_name(name);
    let old = old_name(name);
    let (s, n) = (q(schema), q(name));
    let (old_q, shadow_q) = (q(&old), q(&shadow));
    // The statements are identical on every try: render them once, clone per try.
    let set_lwt = format!("SET SESSION lock_wait_timeout = {lock_wait_s}");
    let drop_old = format!("DROP TABLE IF EXISTS {s}.{old_q}");
    let swap_two = format!("RENAME TABLE {s}.{n} TO {s}.{old_q}, {s}.{shadow_q} TO {s}.{n}");
    let swap_one = format!("RENAME TABLE {s}.{shadow_q} TO {s}.{n}");
    let (schema_t, shadow_t, name_t) = (schema.to_string(), shadow.clone(), name.to_string());
    super::with_retry_mut(
        metrics,
        "swapping the shadow table",
        crate::supervisor::Policy::STATEMENTS,
        conn,
        |conn| {
            let (set_lwt, drop_old, swap_two, swap_one, schema_t, shadow_t, name_t) = (
                set_lwt.clone(),
                drop_old.clone(),
                swap_two.clone(),
                swap_one.clone(),
                schema_t.clone(),
                shadow_t.clone(),
                name_t.clone(),
            );
            Box::pin(async move {
                // Readers with open transactions on the old table hold its metadata lock; don't
                // queue behind them for long (the helper retries), and don't block new readers.
                conn.query_drop(set_lwt).await?;
                if table_kind(conn, &schema_t, &shadow_t).await?.is_none() {
                    // Already swapped (an earlier attempt was interrupted after the rename).
                    return Ok(());
                }
                if table_kind(conn, &schema_t, &name_t).await?.is_some() {
                    conn.query_drop(drop_old.clone()).await?;
                    conn.query_drop(swap_two).await?;
                    conn.query_drop(drop_old).await?;
                } else {
                    conn.query_drop(swap_one).await?;
                }
                Ok(())
            })
        },
    )
    .await
}

pub(crate) async fn drop_table(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    // DROP TABLE IF EXISTS is a repeat (R4.2).
    let sql = format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(name));
    super::with_retry_mut(
        metrics,
        "dropping a table",
        crate::supervisor::Policy::STATEMENTS,
        conn,
        |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                conn.query_drop(sql).await?;
                Ok(())
            })
        },
    )
    .await
}

/// Session time zone in which TIMESTAMPs are written (see [`crate::mv::Layout::epoch_timestamps`]).
pub(crate) const UTC_SESSION: &str = "SESSION time_zone = '+00:00'";

/// Session settings for every connection the service opens: non-locking reads, an sql_mode
/// in which values are stored as they were read (zero dates, no strict-mode rejections), and a
/// bounded table-lock wait (`lock_wait_s`) so a session blocked by a `LOCK TABLES` fails with
/// 1205 instead of waiting out the server default (a year).
pub(crate) async fn init_session(
    conn: &mut Conn,
    lock_wait_s: u64,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    // One idempotent block through the shared policy (R4.2): every read and SET here is a
    // repeat, a transient failure simply re-runs the whole session setup.
    super::with_retry_mut(
        metrics,
        "session setup",
        crate::supervisor::Policy::STATEMENTS,
        conn,
        |conn| {
            Box::pin(async move {
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
            })
        },
    )
    .await
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
    metrics: &crate::metrics::Metrics,
) -> Result<WriteOpts> {
    // The read is idempotent (R4.2).
    let (version, packet): (String, u64) = super::with_retry_mut(
        metrics,
        "reading the target's write options",
        crate::supervisor::Policy::STATEMENTS,
        conn,
        |conn| {
            Box::pin(async move {
                Ok(conn
                    .query_first::<(String, u64), _>(
                        "SELECT CAST(@@version AS CHAR), @@max_allowed_packet",
                    )
                    .await?)
            })
        },
    )
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
pub(crate) async fn kill_and_wait(
    opts: &Opts,
    id: u32,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    let fut = async {
        // The connect and the KILL are repeats (R4.2): KILL is idempotent and "unknown thread
        // id" already proves the thread is gone. The SHOW PROCESSLIST deadline poll below stays
        // a poll.
        let mut c = super::with_retry(
            metrics,
            "fencing a target connection",
            crate::supervisor::Policy::STATEMENTS,
            || async {
                let mut c = Conn::new(opts.clone()).await?;
                match c.query_drop(format!("KILL CONNECTION {id}")).await {
                    Ok(()) => {}
                    Err(e) if kill_is_gone(&e) => {}
                    Err(e) => {
                        return Err(anyhow::Error::from(e)
                            .context(format!("killing target connection {id}")))
                    }
                }
                Ok(c)
            },
        )
        .await?;
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

    /// The ownership marker text, the quoting of the table comment and the SQL modes the
    /// service drops when it reads a source (moved here with the items they test).
    #[test]
    fn markers_and_sql_modes() {
        assert_eq!(crate::target::marker("v", 7), "lazyview:v1 view=v def=7");
        assert_eq!(
            crate::target::marker("a b'c%", 1),
            "lazyview:v1 view=a%20b%27c%25 def=1"
        );
        let l = layout(vec![col("v", "int", "int")], 1, vec![], false);
        assert!(create_sql_marked(&l, "s", "t", Some("it's"))
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

    /// The three statements a source session gets (moved here with the item it tests).
    #[test]
    fn session_setup_statements() {
        assert_eq!(
            session_setup("ONLY_FULL_GROUP_BY,NO_ENGINE_SUBSTITUTION", 60),
            [
                "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED".to_string(),
                "SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,NO_ENGINE_SUBSTITUTION'".to_string(),
                "SET SESSION lock_wait_timeout = 60".to_string(),
            ]
        );
        // A quote cannot end the string literal early.
        assert_eq!(session_setup("A'B", 2)[1], "SET SESSION sql_mode = 'AB'");
        assert_eq!(session_setup("", 2)[2], "SET SESSION lock_wait_timeout = 2");
    }
}
