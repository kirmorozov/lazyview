use super::*;

use crate::binlog::connect::value_str;
use crate::source::{mysql::status_statements, server_errors, GtidState, ACCESS_DENIED_CODES};

pub(super) fn show(v: &str) -> &str {
    if v.is_empty() {
        "NULL"
    } else {
        v
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

/// The source's own GTID state, read once at connect for a resume (§ the resume-by-GTID plan):
/// the `@@GLOBAL` variables MySQL/Percona (`gtid_mode`, `gtid_executed`, `gtid_purged`) and MariaDB
/// (`gtid_binlog_state`, `gtid_purged`) report about what they have executed and purged. These are
/// plain `SELECT @@GLOBAL.x` reads through the same retry probe preflight uses: a variable the
/// server does not have (MariaDB has no `gtid_mode`, MySQL/Percona no `gtid_binlog_state`) comes
/// back `None`, and any other failure is an error — never a silent empty set, which would turn a
/// broken read into a false "this server covers me". Interpreting the text and asking the subset
/// question is the flavor's job (see [`crate::source::gtid_covers`]); this is only the network edge
/// that reads the server's words.
pub async fn gtid_state(conn: &mut Conn) -> Result<GtidState> {
    Ok(GtidState {
        mode: crate::binlog::probe_var_retry(conn, "gtid_mode").await?,
        executed: crate::binlog::probe_var_retry(conn, "gtid_executed").await?,
        purged: crate::binlog::probe_var_retry(conn, "gtid_purged").await?,
        binlog_state: crate::binlog::probe_var_retry(conn, "gtid_binlog_state").await?,
    })
}

const ER_PARSE_ERROR: u16 = 1064;

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
