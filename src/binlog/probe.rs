use super::*;

use crate::binlog::connect::value_str;

pub(super) fn show(v: &str) -> &str {
    if v.is_empty() {
        "NULL"
    } else {
        v
    }
}

/// The two spellings of the binlog status statement, the one the server is expected to know
/// first (the other is tried on a parse error).
///
/// The flavor follows from the version the handshake reports: MySQL and Percona are 8.x, 9.x
/// and, since 26.7, calendar versions (`YY.M`); MariaDB is 10.x to 13.x (mysql_async strips its
/// `5.5.5-` prefix). `SHOW BINARY LOG STATUS` exists on MySQL >= 8.2 (`SHOW MASTER STATUS` was
/// removed in 8.4) and on MariaDB >= 13.0, which still accepts `SHOW MASTER STATUS` as well.
pub(super) fn status_statements(major: u16, minor: u16) -> [&'static str; 2] {
    const NEW: &str = "SHOW BINARY LOG STATUS";
    const OLD: &str = "SHOW MASTER STATUS";
    let new_first = match major {
        8 => minor >= 2,
        9 => true,
        // MariaDB; version numbers up to 25 cannot be a calendar MySQL version.
        10..=25 => false,
        _ => major >= 26,
    };
    if new_first {
        [NEW, OLD]
    } else {
        [OLD, NEW]
    }
}

/// Current end of the binlog (`SHOW BINARY LOG STATUS`, falling back to `SHOW MASTER STATUS`).
pub async fn current_position(conn: &mut Conn) -> Result<Position> {
    let (major, minor, _) = conn.server_version();
    let mut parse_errors = Vec::new();
    for q in status_statements(major, minor) {
        match conn.query_first::<Row, _>(q).await {
            Ok(Some(row)) => {
                let file = value_str(row.as_ref(0));
                let pos: u64 = value_str(row.as_ref(1))
                    .parse()
                    .with_context(|| format!("{q}: unexpected Position value"))?;
                if file.is_empty() {
                    bail!("{q} returned an empty file name");
                }
                return Ok(Position { file, pos });
            }
            Ok(None) => bail!(
                "{q} returned no rows: binary logging is disabled on the server (log_bin=OFF)"
            ),
            // Syntax not known to this server version: try the other spelling.
            Err(mysql_async::Error::Server(e)) if e.code == ER_PARSE_ERROR => {
                parse_errors.push(format!("{q}: {}", e.message))
            }
            Err(e) => {
                return Err(with_hint(
                    anyhow::Error::new(e).context(format!("{q} failed")),
                ))
            }
        }
    }
    bail!(
        "cannot read the current binlog position: {}",
        parse_errors.join("; ")
    )
}

/// The server's `binlog-do-db` / `binlog-ignore-db` filters: columns 2 and 3 (`Binlog_Do_DB`,
/// `Binlog_Ignore_DB`) of the same status statement [`current_position`] reads (comma-separated
/// lists; empty when the filter is unset).
pub async fn binlog_filters(conn: &mut Conn) -> Result<(String, String)> {
    let (major, minor, _) = conn.server_version();
    let mut parse_errors = Vec::new();
    for q in status_statements(major, minor) {
        match conn.query_first::<Row, _>(q).await {
            Ok(Some(row)) => {
                // Row: File, Position, Binlog_Do_DB, Binlog_Ignore_DB[, Executed_Gtid_Set].
                return Ok((value_str(row.as_ref(2)), value_str(row.as_ref(3))));
            }
            Ok(None) => bail!(
                "{q} returned no rows: binary logging is disabled on the server (log_bin=OFF)"
            ),
            // Syntax not known to this server version: try the other spelling.
            Err(mysql_async::Error::Server(e)) if e.code == ER_PARSE_ERROR => {
                parse_errors.push(format!("{q}: {}", e.message))
            }
            Err(e) => {
                return Err(with_hint(
                    anyhow::Error::new(e).context(format!("{q} failed")),
                ))
            }
        }
    }
    bail!(
        "cannot read the binlog filters: {}",
        parse_errors.join("; ")
    )
}

/// Whether the server's `binlog-do-db` / `binlog-ignore-db` filters keep row events of `schema`
/// out of the binlog (the filters name the table's own schema): `do_db` is non-empty and does
/// not list the schema, or `ignore_db` does. The columns are comma-separated lists, spaces
/// around the entries are ignored, and names match exactly, case-sensitively, as the server
/// compares database names on this platform.
pub fn schema_filtered(do_db: &str, ignore_db: &str, schema: &str) -> bool {
    fn names(list: &str) -> Vec<&str> {
        list.split(',')
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .collect()
    }
    let do_names = names(do_db);
    if !do_names.is_empty() && !do_names.contains(&schema) {
        return true;
    }
    names(ignore_db).contains(&schema)
}

/// Whether `file` is still listed by `SHOW BINARY LOGS`.
pub async fn binlog_exists(conn: &mut Conn, file: &str) -> Result<bool> {
    let rows: Vec<Row> = conn
        .query("SHOW BINARY LOGS")
        .await
        .map_err(|e| with_hint(anyhow::Error::new(e).context("SHOW BINARY LOGS failed")))?;
    Ok(rows.iter().any(|r| value_str(r.as_ref(0)) == file))
}

/// The requested binlog file/position is not (or no longer) available on the server
/// (typically purged by `binlog_expire_logs_seconds` / `PURGE BINARY LOGS`).
#[derive(Debug, thiserror::Error)]
#[error("binlog {from} is not available on the server (purged?): {message}")]
pub struct MissingBinlog {
    pub from: Position,
    pub message: String,
}

/// ER_MASTER_FATAL_ERROR_READING_BINLOG; also used for unrelated fatal dump errors, hence the
/// message check in [`missing_binlog_message`].
const ER_SOURCE_FATAL_ERROR_READING_BINLOG: u16 = 1236;

pub(super) fn missing_binlog_message(e: &mysql_async::ServerError) -> bool {
    if e.code != ER_SOURCE_FATAL_ERROR_READING_BINLOG {
        return false;
    }
    let m = e.message.to_ascii_lowercase();
    // MySQL / Percona / MariaDB wordings for "file not in the index" / "file gone from disk" /
    // "GTIDs purged".
    [
        "could not find first log file",
        "could not open log file",
        "not found (errcode",
        "no such file",
        "purged",
    ]
    .iter()
    .any(|p| m.contains(p))
}

/// Whether `err` (anywhere in its chain) means the requested binlog was purged / does not exist.
pub fn is_missing_binlog(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        e.downcast_ref::<MissingBinlog>().is_some()
            || matches!(e.downcast_ref::<mysql_async::Error>(), Some(mysql_async::Error::Server(se)) if missing_binlog_message(se))
            || e.downcast_ref::<mysql_async::ServerError>().is_some_and(missing_binlog_message)
    })
}

const ER_PARSE_ERROR: u16 = 1064;

/// Server error codes meaning "not allowed": ER_DBACCESS_DENIED_ERROR (1044),
/// ER_ACCESS_DENIED_ERROR (1045; also what COM_REGISTER_SLAVE answers without REPLICATION SLAVE),
/// ER_TABLEACCESS_DENIED_ERROR (1142), ER_SPECIFIC_ACCESS_DENIED_ERROR (1227),
/// ER_PROCACCESS_DENIED_ERROR (1370).
pub(super) const ACCESS_DENIED_CODES: [u16; 5] = [1044, 1045, 1142, 1227, 1370];

fn server_errors(err: &anyhow::Error) -> impl Iterator<Item = &mysql_async::ServerError> {
    err.chain()
        .filter_map(|e| match e.downcast_ref::<mysql_async::Error>() {
            Some(mysql_async::Error::Server(se)) => Some(se),
            _ => e.downcast_ref::<mysql_async::ServerError>(),
        })
}

/// Whether `err` is a privilege problem (wrong credentials, or a missing grant such as
/// REPLICATION SLAVE / REPLICATION CLIENT / BINLOG MONITOR).
pub fn is_access_denied(err: &anyhow::Error) -> bool {
    server_errors(err).any(|se| ACCESS_DENIED_CODES.contains(&se.code))
        || err
            .chain()
            .any(|e| e.to_string().contains("REPLICATION SLAVE privilege"))
}

/// Whether `err` means another replica connected with the same server_id (the server then drops
/// the older connection): MySQL/Percona 1236 "A replica with the same server_uuid/server_id …",
/// MariaDB 4052 (ER_SLAVE_SAME_ID) "A slave with the same server_uuid/server_id …".
pub fn is_duplicate_server_id(err: &anyhow::Error) -> bool {
    server_errors(err).any(is_duplicate_server_id_error)
}

pub(super) fn is_duplicate_server_id_error(se: &mysql_async::ServerError) -> bool {
    // MariaDB's ER_SLAVE_SAME_ID (4052) is unambiguous by code; 1236 is also used for unrelated
    // fatal dump errors, so only there the message has to say "same server_id" (and
    // `missing_binlog_message` owns the other half of 1236).
    if se.code == 4052 {
        return true;
    }
    let m = se.message.to_ascii_lowercase();
    se.code == ER_SOURCE_FATAL_ERROR_READING_BINLOG
        && (m.contains("same server_uuid/server_id") || m.contains("same server_id"))
}

/// Adds a "how to fix" line to privilege errors of replication statements.
pub(super) fn with_hint(err: anyhow::Error) -> anyhow::Error {
    let Some(se) = server_errors(&err).find(|se| ACCESS_DENIED_CODES.contains(&se.code)) else {
        return err;
    };
    let hint = if se.message.contains("BINLOG MONITOR") {
        "the account needs BINLOG MONITOR (MariaDB) and REPLICATION SLAVE: GRANT BINLOG \
         MONITOR, REPLICATION SLAVE ON *.* TO <user>"
    } else {
        "the account needs REPLICATION CLIENT and REPLICATION SLAVE: GRANT REPLICATION CLIENT, \
         REPLICATION SLAVE ON *.* TO <user>"
    };
    err.context(hint)
}
