use super::*;

use crate::binlog::connect::value_str;
use crate::source::{server_errors, ACCESS_DENIED_CODES};

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

const ER_PARSE_ERROR: u16 = 1064;

/// Transient: this connection, or the server, is going away — repeat an idempotent call.
/// Classification is by server error code and `io::ErrorKind` only. The one documented exception
/// is the TLS pre-handshake case, which has no code and stays in `engine::is_tls_error`.
pub fn is_transient(e: &anyhow::Error) -> bool {
    const TRANSIENT_CODES: [u16; 14] = [
        1040, 1053, 1203, 1205, 1213, 1290, 1317, 1927, 1969, 2006, 2013, 3024, 3169, 4031,
    ];
    fn transient_io(io: &std::io::Error) -> bool {
        matches!(
            io.kind(),
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::UnexpectedEof
        )
    }
    server_errors(e).any(|se| TRANSIENT_CODES.contains(&se.code))
        || e.chain().any(|c| {
            (match c.downcast_ref::<mysql_async::Error>() {
                Some(mysql_async::Error::Io(mysql_async::IoError::Io(io))) => transient_io(io),
                Some(mysql_async::Error::Driver(mysql_async::DriverError::ConnectionClosed)) => {
                    true
                }
                _ => false,
            }) || c.downcast_ref::<std::io::Error>().is_some_and(transient_io)
        })
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
