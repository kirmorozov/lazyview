//! Binlog replication client: positions, server checks and a decoded event stream.
//!
//! # Connecting
//!
//! Plain `mysql_async::Opts::from_url(dsn)` works against every supported server (verified on
//! MySQL 8.4 / 26.7, Percona 8.4 / 9.7, MariaDB 10.11 / 13.0 and the 13.2 preview; earlier also
//! on the now EOL MySQL 8.0, Percona 8.0 and on MariaDB 11.4), including accounts using
//! `caching_sha2_password` (the MySQL 8.x default) **without TLS**: when the server asks for full
//! authentication over an insecure channel, mysql_async fetches the server's RSA public key and
//! sends the password RSA-encrypted (needs only the default
//! `caching_sha2_password_auto_generate_rsa_keys=ON` on the server). For `localhost` /
//! `127.0.0.1` mysql_async moves the connection to the server's unix socket (`prefer_socket`,
//! on by default), which also counts as secure; `?prefer_socket=false` keeps TCP. TLS is
//! optional: `?require_ssl=true&verify_ca=false` works with the servers' auto-generated
//! self-signed certificates (rustls with the `ring` provider); MariaDB 10.11 has no TLS unless
//! certificates are configured (`have_ssl=DISABLED`). See [`replica_opts`].
//!
//! # Positions
//!
//! Every event header carries `log_pos`, the offset *after* the event in the current file. The
//! reader tracks the file from ROTATE events (including the artificial one the server sends at
//! stream start, which on MariaDB carries a CRC32 that has to be stripped by hand) and the
//! offset from `log_pos`. Artificial events (fake ROTATE, the format description re-sent when
//! starting mid-file) do not move the position.
//!
//! * MySQL `binlog_transaction_compression=ON`: mysql_async yields the `TRANSACTION_PAYLOAD`
//!   event and then the decompressed inner events, whose `log_pos` is 0. They are not part of
//!   the file layout, so the position stays at the end of the payload event and the inner XID
//!   becomes a `Commit` that resumes right after the payload.
//! * MariaDB >= 11.4 (`binlog_legacy_event_pos=OFF`): events inside a transaction have
//!   `log_pos = 0`; only the commit event has a real one. The position advances by event size
//!   there, but because the server omits ANNOTATE_ROWS events to GTID-capable replicas (holes
//!   before TABLE_MAP events) it is only trusted until the first TABLE_MAP; see
//!   `Tracker::boundary_here`.
//! * MariaDB omits ANNOTATE_ROWS events (holes in the offsets) on every version; `log_pos` of
//!   the following event is still exact.
//! * MariaDB >= 12.3 splits a rows event larger than `binlog_row_event_fragment_threshold`
//!   (default 1 GiB) into `Partial_rows` events; they are reassembled into the rows event.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::StreamExt;
use mysql_async::binlog::events::{
    Event, EventData, FormatDescriptionEvent, GtidEvent, QueryEvent, TableMapEvent,
    TransactionPayloadEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::{BinlogVersion, EventType};
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Opts, OptsBuilder, Row, Value};

/// Product name and environment-variable prefix named in this module's messages.
static PRODUCT: OnceLock<(String, String)> = OnceLock::new();

/// Sets the product name and environment-variable prefix this module names in its messages
/// (defaults `lazyview` and `LAZYVIEW_`), for a program that embeds it. Only the first call takes
/// effect; it returns whether this one did. lazyview itself never calls it.
pub fn set_product(name: &str, env_prefix: &str) -> bool {
    PRODUCT
        .set((name.to_string(), env_prefix.to_string()))
        .is_ok()
}

/// `(name, env_prefix)`, see [`set_product`].
fn product() -> (&'static str, &'static str) {
    match PRODUCT.get() {
        Some((name, prefix)) => (name, prefix),
        None => ("lazyview", "LAZYVIEW_"),
    }
}

/// A binlog coordinate. `pos` is the offset of the next event to read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Position {
    pub file: String,
    pub pos: u64,
}

impl Position {
    /// Numeric suffix of the binlog file name (`binlog.000012` → 12).
    pub fn file_seq(&self) -> u64 {
        self.file
            .rsplit('.')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.file_seq(), self.pos).cmp(&(other.file_seq(), other.pos))
    }
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    MySql,
    MariaDb,
}

impl Flavor {
    /// Flavor from a `VERSION()` string (`10.11.14-MariaDB-log` → MariaDB; Percona is MySQL).
    pub fn from_version(version: &str) -> Flavor {
        if version.to_ascii_lowercase().contains("mariadb") {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub flavor: Flavor,
    pub version: String,
}

/// Connection options for a DSN, tuned for a replication client.
///
/// The DSN is parsed with `Opts::from_url` (so `require_ssl`, `verify_ca`, `prefer_socket`, …
/// URL parameters keep working). On top of that a TCP keepalive is enabled so a silently dropped
/// connection is detected by the OS as well (the reader also has its own idle timeout derived from
/// the heartbeat period). See the module docs for authentication notes.
pub fn replica_opts(dsn: &str) -> Result<Opts> {
    // `tls_ca` / `ssl-ca` / `ssl_ca` (lazyview extension, not a mysql_async URL parameter):
    // PEM file(s) with CA certificates to trust in addition to the built-in roots (e.g. the
    // AWS RDS / Azure / self-signed CA). Implies TLS with certificate verification.
    let (url, cas) = split_ca_params(dsn);
    let opts = Opts::from_url(&url).map_err(|e| anyhow!("invalid DSN: {e}"))?;
    let keepalive = opts.tcp_keepalive().or(Some(Duration::from_secs(30)));
    let mut b = OptsBuilder::from_opts(opts.clone()).tcp_keepalive(keepalive);
    if !cas.is_empty() {
        for c in &cas {
            if !std::path::Path::new(c).is_file() {
                bail!(
                    "TLS CA file `{c}` not found ({}TLS_CA / tls_ca DSN parameter)",
                    product().1
                );
            }
        }
        let ssl = opts
            .ssl_opts()
            .cloned()
            .unwrap_or_default()
            .with_root_certs(
                cas.into_iter()
                    .map(|c| std::path::PathBuf::from(c).into())
                    .collect(),
            );
        b = b.ssl_opts(Some(ssl));
    }
    Ok(b.into())
}

fn split_ca_params(dsn: &str) -> (String, Vec<String>) {
    let Some((base, query)) = dsn.split_once('?') else {
        return (dsn.to_string(), Vec::new());
    };
    let mut keep = Vec::new();
    let mut cas = Vec::new();
    for kv in query.split('&').filter(|kv| !kv.is_empty()) {
        match kv.split_once('=') {
            Some(("tls_ca" | "ssl-ca" | "ssl_ca", v)) => cas.extend(
                percent_decode(v)
                    .split(',')
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
            ),
            _ => keep.push(kv),
        }
    }
    let url = if keep.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", keep.join("&"))
    };
    (url, cas)
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn value_str(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::NULL) => String::new(),
        Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::UInt(u)) => u.to_string(),
        Some(other) => format!("{other:?}"),
    }
}

/// Checks `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`; returns a clear error
/// listing every violated requirement.
///
/// On MariaDB `log_bin_compress=OFF` is required as well: compressed events (types 165–171)
/// cannot be decoded by this reader. So is a file binlog: with `binlog_storage_engine=innodb`
/// (MariaDB >= 12.3) the server only serves binlog dumps that start from a GTID ("GTID starting
/// position is required on master with --binlog-storage-engine enabled"), and this reader
/// starts and resumes from a file and offset.
pub async fn server_info(conn: &mut Conn) -> Result<ServerInfo> {
    let row: Row = conn
        .query_first(
            "SELECT VERSION(), @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image",
        )
        .await
        .context("reading server version and binlog settings")?
        .ok_or_else(|| anyhow!("reading server variables returned no row"))?;
    let version = value_str(row.as_ref(0));
    let log_bin = value_str(row.as_ref(1));
    let format = value_str(row.as_ref(2));
    let image = value_str(row.as_ref(3));
    let flavor = Flavor::from_version(&version);
    let mut compress = String::new();
    let mut storage_engine = String::new();
    if flavor == Flavor::MariaDb {
        // Variables missing on older servers (binlog_storage_engine before 12.3): nothing to
        // check.
        let v: Option<Value> = conn
            .query_first("SELECT @@GLOBAL.log_bin_compress")
            .await
            .unwrap_or(None);
        compress = value_str(v.as_ref());
        let v: Option<Value> = conn
            .query_first("SELECT @@GLOBAL.binlog_storage_engine")
            .await
            .unwrap_or(None);
        storage_engine = value_str(v.as_ref());
    }
    let mut problems = requirement_problems(
        flavor,
        &log_bin,
        &format,
        &image,
        &compress,
        &storage_engine,
    );
    if flavor == Flavor::MySql {
        // Partial JSON row images carry diffs instead of values; lazyview needs full values.
        let opts: Option<Value> = conn
            .query_first("SELECT @@GLOBAL.binlog_row_value_options")
            .await
            .unwrap_or(None);
        if value_str(opts.as_ref())
            .to_ascii_uppercase()
            .contains("PARTIAL_JSON")
        {
            problems.push(
                "binlog_row_value_options is PARTIAL_JSON (must be empty); fix: SET PERSIST \
                 binlog_row_value_options='' (Amazon RDS/Aurora: set it in the DB parameter group)"
                    .into(),
            );
        }
    }
    if !problems.is_empty() {
        bail!(unmet_requirements(&version, &problems));
    }
    Ok(ServerInfo { flavor, version })
}

/// The error message of a server that does not meet the binlog requirements (`problems`, see
/// [`requirement_problems`]).
fn unmet_requirements(version: &str, problems: &[String]) -> String {
    format!(
        "server {version} does not meet {}'s binlog requirements:\n  - {}",
        product().0,
        problems.join("\n  - ")
    )
}

/// One message per violated requirement, each with the way to fix it on that flavor.
fn requirement_problems(
    flavor: Flavor,
    log_bin: &str,
    format: &str,
    image: &str,
    compress: &str,
    storage_engine: &str,
) -> Vec<String> {
    let on = |v: &str| matches!(v.to_ascii_uppercase().as_str(), "1" | "ON");
    let maria = flavor == Flavor::MariaDb;
    // Dynamic variables: set now and make it survive a restart.
    let set = |var: &str, val: &str, opt: &str| {
        if maria {
            format!(
                "SET GLOBAL {var}='{val}' and add {var}={val} to my.cnf [mysqld] \
                 (container: --{opt}={val}; Amazon RDS: set it in the DB parameter group)"
            )
        } else {
            format!(
                "SET PERSIST {var}='{val}' (or {var}={val} in my.cnf [mysqld]; container: \
                 --{opt}={val}; Amazon RDS/Aurora: set {var}={val} in the DB parameter group)"
            )
        }
    };
    let mut problems = Vec::new();
    if !on(log_bin) {
        let fix = if maria {
            "add log_bin=ON (or log_bin=<basename>) to my.cnf [mysqld] or start mariadbd with \
             --log-bin, then restart"
        } else {
            "binary logging is on by default in MySQL 8: remove --skip-log-bin / \
             --disable-log-bin (or start mysqld with --log-bin), then restart"
        };
        problems.push(format!(
            "log_bin is {} (must be ON); fix: {fix}",
            show(log_bin)
        ));
    }
    if !format.eq_ignore_ascii_case("ROW") {
        problems.push(format!(
            "binlog_format is {} (must be ROW); fix: {}; sessions already connected keep \
             the old format until they reconnect",
            show(format),
            set("binlog_format", "ROW", "binlog-format")
        ));
    }
    if !image.eq_ignore_ascii_case("FULL") {
        problems.push(format!(
            "binlog_row_image is {} (must be FULL); fix: {}; sessions already connected keep \
             the old setting until they reconnect",
            show(image),
            set("binlog_row_image", "FULL", "binlog-row-image")
        ));
    }
    if maria && on(compress) {
        problems.push(
            "log_bin_compress is ON (compressed binlog events are not supported); fix: SET \
             GLOBAL log_bin_compress=OFF and remove log_bin_compress from my.cnf (container: \
             --log-bin-compress=OFF); binlogs written while it was ON cannot be read"
                .into(),
        );
    }
    if maria && !storage_engine.is_empty() {
        problems.push(format!(
            "binlog_storage_engine is {storage_engine} (must be unset: a binlog kept in the \
             storage engine is only served to replicas that start from a GTID, {} reads \
             it by file and position); fix: remove binlog_storage_engine from my.cnf and restart",
            product().0
        ));
    }
    problems
}

fn show(v: &str) -> &str {
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
fn status_statements(major: u16, minor: u16) -> [&'static str; 2] {
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

fn missing_binlog_message(e: &mysql_async::ServerError) -> bool {
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
const ACCESS_DENIED_CODES: [u16; 5] = [1044, 1045, 1142, 1227, 1370];

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

fn is_duplicate_server_id_error(se: &mysql_async::ServerError) -> bool {
    let m = se.message.to_ascii_lowercase();
    matches!(se.code, ER_SOURCE_FATAL_ERROR_READING_BINLOG | 4052)
        && (m.contains("same server_uuid/server_id") || m.contains("same server_id"))
}

/// Adds a "how to fix" line to privilege errors of replication statements.
fn with_hint(err: anyhow::Error) -> anyhow::Error {
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

/// One decoded stream item. Row data stays undecoded until [`BinlogReader::rows`] is called, so
/// events for unwatched tables cost nothing beyond framing.
#[derive(Debug, Clone)]
pub enum Item {
    TableMap {
        table_id: u64,
        schema: String,
        table: String,
        columns: u64,
    },
    Rows {
        table_id: u64,
        kind: RowsKind,
        event: Event,
    },
    /// Transaction committed (XID or COMMIT query). `pos` is where to resume after it.
    ///
    /// Also ends a one-phase XA transaction (`XA COMMIT … ONE PHASE`); on MySQL/Percona that
    /// transaction starts with [`Item::XaStart`], on MariaDB it is logged as a plain one.
    Commit {
        pos: Position,
    },
    /// An XA transaction begins; its rows follow, and it ends with [`Item::XaPrepare`] (or
    /// `Commit` for one-phase commits). MySQL: `XA START` query; MariaDB: GTID event flagged
    /// as prepared XA (emitted right after its [`Item::Gtid`]).
    XaStart {
        xid: String,
    },
    /// `XA PREPARE`: the XA transaction's rows are durable but **not visible** until the
    /// matching [`Item::XaEnd`], which may come much later (other transactions in between).
    /// A resumable boundary like `Commit`.
    XaPrepare {
        xid: String,
        pos: Position,
    },
    /// `XA COMMIT xid` (`commit = true`) or `XA ROLLBACK xid` of a prepared XA transaction. A
    /// resumable boundary; carries no rows.
    XaEnd {
        xid: String,
        commit: bool,
        pos: Position,
    },
    /// Any non-transactional statement (DDL etc.), already committed. `pos` as above.
    ///
    /// Exception: `CREATE TABLE … SELECT` is logged as this statement followed by the new
    /// table's row events and a `Commit` in the same transaction; `pos` (right after the
    /// statement) then lies inside that transaction, and resuming there yields those rows and
    /// the `Commit`. When the exact end of a statement inside a transaction is unknown
    /// (MariaDB 11.4+, after a TABLE_MAP), `pos` is the previous boundary instead (`Commit`
    /// likewise), so resuming replays that transaction.
    Statement {
        schema: String,
        query: String,
        pos: Position,
    },
    /// GTID of the transaction (or statement) whose items follow. MySQL/Percona:
    /// `uuid:gno` (`uuid:tag:gno` for tagged GTIDs); MariaDB: `domain-server-seq`.
    Gtid {
        gtid: String,
    },
    Heartbeat,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsKind {
    Insert,
    Update,
    Delete,
}

// Raw event type codes (MariaDB's are not in mysql_common's `EventType`).
mod ty {
    pub const QUERY: u8 = 0x02;
    pub const ROTATE: u8 = 0x04;
    pub const FORMAT_DESCRIPTION: u8 = 0x0f;
    pub const XID: u8 = 0x10;
    pub const TABLE_MAP: u8 = 0x13;
    pub const PRE_GA_WRITE_ROWS: u8 = 0x14;
    pub const PRE_GA_DELETE_ROWS: u8 = 0x16;
    pub const WRITE_ROWS_V1: u8 = 0x17;
    pub const UPDATE_ROWS_V1: u8 = 0x18;
    pub const DELETE_ROWS_V1: u8 = 0x19;
    pub const INCIDENT: u8 = 0x1a;
    pub const HEARTBEAT: u8 = 0x1b;
    pub const WRITE_ROWS: u8 = 0x1e;
    pub const UPDATE_ROWS: u8 = 0x1f;
    pub const DELETE_ROWS: u8 = 0x20;
    pub const GTID: u8 = 0x21;
    pub const XA_PREPARE: u8 = 0x26;
    pub const PARTIAL_UPDATE_ROWS: u8 = 0x27;
    pub const TRANSACTION_PAYLOAD: u8 = 0x28;
    pub const HEARTBEAT_V2: u8 = 0x29;
    pub const GTID_TAGGED: u8 = 0x2a;
    // MariaDB
    pub const MARIA_GTID: u8 = 162;
    pub const MARIA_QUERY_COMPRESSED: u8 = 165;
    pub const MARIA_DELETE_ROWS_COMPRESSED: u8 = 171;
    pub const MARIA_PARTIAL_ROWS: u8 = 172;
}

/// Table id of the dummy rows event that only carries the end-of-statement flag.
const DUMMY_TABLE_ID: u64 = 0x00ff_ffff;

/// `LOG_EVENT_ARTIFICIAL_F`: event generated by the dump thread, not present in the file.
const ARTIFICIAL_FLAG: u16 = 0x20;

/// Expansion state of a `TRANSACTION_PAYLOAD_EVENT` (bytes of inner events still expected).
#[derive(Debug, Clone, Copy)]
struct Payload {
    remaining: u64,
}

/// A rows event being reassembled from MariaDB `Partial_rows` fragments.
#[derive(Debug)]
struct Partial {
    total: u32,
    /// Sequence number of the next fragment (they start at 1).
    next: u32,
    /// Length of the whole rows event, from the first fragment.
    len: u64,
    buf: Vec<u8>,
}

/// Position bookkeeping and event → [`Item`] mapping, independent of the connection.
struct Tracker {
    flavor: Flavor,
    /// Position after the last event read (the next event's offset) — possibly mid-transaction.
    pos: Position,
    /// False once `pos` may lag the real offset: inside a MariaDB >= 11.4 transaction (events
    /// with `log_pos = 0`) after a TABLE_MAP, before which the server may have omitted an
    /// ANNOTATE_ROWS event (it does so for replicas with GTID capability).
    pos_exact: bool,
    /// Position of the last `Commit` / `Statement` (or the start position).
    boundary: Position,
    payload: Option<Payload>,
    partial: Option<Partial>,
    /// Table maps by id, tagged with the file generation they were seen in. Entries survive one
    /// ROTATE (unlike mysql_common's map), so rows buffered across a rotate still decode.
    tables: HashMap<u64, (u64, TableMapEvent<'static>, Option<Time2Twin>)>,
    generation: u64,
    /// Second item produced by one event (MariaDB XA GTID → `Gtid` + `XaStart`).
    queued: Option<Item>,
}

impl Tracker {
    fn new(flavor: Flavor, from: &Position) -> Tracker {
        Tracker {
            flavor,
            pos: from.clone(),
            pos_exact: true,
            boundary: from.clone(),
            payload: None,
            partial: None,
            tables: HashMap::new(),
            generation: 0,
            queued: None,
        }
    }

    fn table_map(&self, table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.tables.get(&table_id).map(|(_, t, _)| t)
    }
}

pub struct BinlogReader {
    stream: BinlogStream,
    t: Tracker,
    server_id: u32,
    /// Without any event (heartbeats included) for this long the connection is presumed dead.
    idle_timeout: Option<Duration>,
    /// First event read by `connect` (the stream is validated there) when it is not a rotate.
    pending: Option<Event>,
}

impl BinlogReader {
    /// Opens a dedicated connection and starts streaming from `from` as replica `server_id`.
    ///
    /// The first event is awaited here, so a purged/missing binlog fails `connect` with an error
    /// for which [`is_missing_binlog`] is true. With a non-zero `heartbeat` the server sends a
    /// heartbeat after that much idle time, and [`next`](Self::next) fails if nothing at all
    /// arrives for `3 × heartbeat + 10 s`.
    pub async fn connect(
        opts: Opts,
        server_id: u32,
        flavor: Flavor,
        from: &Position,
        heartbeat: Duration,
    ) -> Result<Self> {
        if server_id == 0 {
            bail!("replica server_id must not be 0");
        }
        let mut conn = Conn::new(opts)
            .await
            .context("opening the binlog connection")?;
        if flavor == Flavor::MariaDb {
            // MARIA_SLAVE_CAPABILITY_GTID: we understand GTID / GTID_LIST / BINLOG_CHECKPOINT
            // events (otherwise the server rewrites them into dummy events).
            conn.query_drop("SET @mariadb_slave_capability = 4")
                .await
                .context("SET @mariadb_slave_capability")?;
        }
        if !heartbeat.is_zero() {
            // Nanoseconds; MySQL >= 8.0.26 reads source_*, older MySQL and MariaDB master_*.
            let ns = heartbeat.as_nanos().clamp(1_000_000, 4_294_967_000_000_000);
            conn.query_drop(format!(
                "SET @master_heartbeat_period = {ns}, @source_heartbeat_period = {ns}"
            ))
            .await
            .context("setting the replication heartbeat period")?;
        }
        let request = BinlogStreamRequest::new(server_id)
            .with_filename(from.file.as_bytes())
            .with_pos(from.pos);
        let stream = conn
            .get_binlog_stream(request)
            .await
            .map_err(|e| Self::map_error(e, from, server_id))
            .with_context(|| {
                format!("requesting the binlog stream from {from} as server_id {server_id}")
            })?;

        let idle_timeout = (!heartbeat.is_zero()).then(|| heartbeat * 3 + Duration::from_secs(10));
        let mut reader = BinlogReader {
            stream,
            t: Tracker::new(flavor, from),
            server_id,
            idle_timeout,
            pending: None,
        };
        // Validate the request: the server answers with a fake ROTATE or an error (e.g. 1236).
        let first = tokio::time::timeout(Duration::from_secs(30), reader.stream.next())
            .await
            .map_err(|_| anyhow!("no response from the server within 30s after requesting the binlog from {from}"))?;
        match first {
            None => bail!(
                "the server closed the binlog stream right after it was requested from {from}"
            ),
            Some(Err(e)) => return Err(Self::map_error(e, from, server_id)),
            Some(Ok(ev)) if ev.header().event_type_raw() == ty::ROTATE => {
                reader.t.map_event(ev)?;
            }
            Some(Ok(ev)) => reader.pending = Some(ev),
        }
        Ok(reader)
    }

    pub fn flavor(&self) -> Flavor {
        self.t.flavor
    }

    /// Position after the last event read. Only `Commit` / `Statement` positions are safe resume
    /// points; this one may be inside a transaction.
    pub fn position(&self) -> &Position {
        &self.t.pos
    }

    /// The table map currently known for `table_id` (column types / metadata).
    pub fn table_map(&self, table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.t.table_map(table_id)
    }

    /// Next item; `Ok(None)` when the server closed the stream.
    ///
    /// Cancel-safe: dropping the future before it completes loses no event.
    pub async fn next(&mut self) -> Result<Option<Item>> {
        if let Some(item) = self.t.queued.take() {
            return Ok(Some(item));
        }
        let ev = match self.pending.take() {
            Some(ev) => ev,
            None => {
                let next = self.stream.next();
                let res = match self.idle_timeout {
                    Some(t) => tokio::time::timeout(t, next).await.map_err(|_| {
                        anyhow!("no binlog event or heartbeat received for {}s at {}; connection presumed dead", t.as_secs(), self.t.pos)
                    })?,
                    None => next.await,
                };
                match res {
                    None => return Ok(None),
                    Some(Ok(ev)) => ev,
                    Some(Err(e)) => return Err(Self::map_error(e, &self.t.pos, self.server_id)),
                }
            }
        };
        self.t.map_event(ev).map(Some)
    }

    /// Decodes the rows of a `Rows` item: `(before, after)` images.
    ///
    /// Uses the table map seen in the stream for `table_id` (kept for the current and the
    /// previous binlog file). Value notes: without signedness metadata (MariaDB's default
    /// `binlog_row_metadata=NO_LOG`) unsigned integers come back as signed values of the column
    /// width; TIMESTAMP is `Bytes("<epoch>[.<usec>]")`, ENUM its index, SET its bitmap bytes;
    /// `PARTIAL_UPDATE_ROWS` after images may hold `BinlogValue::JsonDiff`.
    pub fn rows(
        &self,
        table_id: u64,
        event: &Event,
    ) -> Result<Vec<(Option<BinlogRow>, Option<BinlogRow>)>> {
        self.t.rows(table_id, event)
    }

    pub async fn close(self) -> Result<()> {
        match tokio::time::timeout(Duration::from_secs(5), self.stream.close()).await {
            Ok(r) => r.context("closing the binlog connection"),
            Err(_) => Ok(()), // the connection is dropped anyway
        }
    }

    fn map_error(e: mysql_async::Error, at: &Position, server_id: u32) -> anyhow::Error {
        match e {
            mysql_async::Error::Server(ref se) if missing_binlog_message(se) => {
                anyhow::Error::new(MissingBinlog {
                    from: at.clone(),
                    message: se.message.clone(),
                })
            }
            mysql_async::Error::Server(ref se) if is_duplicate_server_id_error(se) => {
                let msg = format!(
                    "another replica connected with the same server_id {server_id} and the server \
                     dropped this stream (at {at}); give every {} instance and every real \
                     replica a unique server_id ({}SERVER_ID)",
                    product().0,
                    product().1
                );
                anyhow::Error::new(e).context(msg)
            }
            mysql_async::Error::Server(ref se) if ACCESS_DENIED_CODES.contains(&se.code) => {
                // The login itself succeeded, so this is COM_REGISTER_SLAVE / COM_BINLOG_DUMP.
                anyhow::Error::new(e).context(
                    "registering as a replica was refused: the account needs the REPLICATION \
                     SLAVE privilege (GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO <user>)",
                )
            }
            e => anyhow::Error::new(e).context(format!("binlog stream error at {at}")),
        }
    }
}

impl Tracker {
    fn rows(
        &self,
        table_id: u64,
        event: &Event,
    ) -> Result<Vec<(Option<BinlogRow>, Option<BinlogRow>)>> {
        let (_, tme, twin) = self.tables.get(&table_id).ok_or_else(|| {
            anyhow!("cannot decode rows event: no TABLE_MAP seen for table id {table_id}")
        })?;
        let data = event
            .read_data()
            .with_context(|| format!("parsing rows event for {}", tme_name(tme)))?;
        let Some(EventData::RowsEvent(re)) = data else {
            bail!(
                "event type {} is not a rows event",
                event.header().event_type_raw()
            );
        };
        if re.table_id() != table_id {
            bail!(
                "rows event is for table id {}, not {table_id}",
                re.table_id()
            );
        }
        let mut rows = re
            .rows(twin.as_ref().map_or(tme, |t| &t.tme))
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("decoding rows of {} (table id {table_id})", tme_name(tme)))?;
        if let Some(t) = twin {
            for (before, after) in rows.iter_mut() {
                for row in [before, after].into_iter().flatten() {
                    t.fix(row);
                }
            }
        }
        Ok(rows)
    }
}

/// Decode twin of a table map with TIME(1) / TIME(2) columns. mysql_common 0.37 decodes their
/// negative values with a fraction wrongly (`frac -= 0x100` on a `u32` in
/// `my_time_packed_from_binary`: an overflow panic in debug builds, garbage in release builds).
/// In the twin those columns are 4-byte BIT columns (same width; like TIME2 neither numeric nor
/// character, so the optional metadata still lines up), whose raw bytes [`Time2Twin::fix`]
/// decodes as the server does.
#[derive(Debug, Clone)]
struct Time2Twin {
    tme: TableMapEvent<'static>,
    cols: Vec<usize>,
}

impl Time2Twin {
    fn fix(&self, row: &mut BinlogRow) {
        for &c in &self.cols {
            let v = match row.as_ref(c) {
                Some(BinlogValue::Value(Value::Bytes(b))) => match <[u8; 4]>::try_from(&b[..]) {
                    Ok(b) => time2_short_fraction(b),
                    Err(_) => continue,
                },
                _ => continue,
            };
            row.place(c, BinlogValue::Value(v));
        }
    }
}

/// The twin (see [`Time2Twin`]) of `tme` from event `ev`, if the table has TIME2 columns with 1
/// or 2 fractional digits.
fn time2_twin(ev: &Event, tme: &TableMapEvent<'_>) -> Option<Time2Twin> {
    let n = usize::try_from(tme.columns_count()).ok()?;
    let cols: Vec<usize> = (0..n)
        .filter(|&i| {
            matches!(
                tme.get_column_type(i),
                Ok(Some(ColumnType::MYSQL_TYPE_TIME2))
            ) && matches!(tme.get_column_metadata(i), Some([1 | 2]))
        })
        .collect();
    if cols.is_empty() {
        return None;
    }
    let twin = time2_twin_body(ev, tme, &cols);
    if twin.is_none() {
        tracing::warn!(
            table = %tme_name(tme),
            "cannot build the TIME(1)/TIME(2) decode twin; negative values with a fraction may be misread"
        );
    }
    Some(Time2Twin { tme: twin?, cols })
}

fn time2_twin_body(
    ev: &Event,
    tme: &TableMapEvent<'_>,
    cols: &[usize],
) -> Option<TableMapEvent<'static>> {
    // Body: table id + flags (post-header), schema and table name (length byte, name, NUL),
    // column count, column types, metadata block, NULL bitmap, optional metadata.
    let body = ev.data();
    let post = usize::from(
        ev.fde()
            .get_event_type_header_length(EventType::TABLE_MAP_EVENT),
    );
    let mut p = post;
    for _ in 0..2 {
        p += usize::from(*body.get(p)?) + 2;
    }
    let (count, len) = lenenc(body.get(p..)?)?;
    let n = usize::try_from(count).ok()?;
    let types_at = p + len;
    let (meta_len, len) = lenenc(body.get(types_at + n..)?)?;
    let meta_at = types_at + n + len;
    let meta_end = meta_at.checked_add(usize::try_from(meta_len).ok()?)?;
    let meta = body.get(meta_at..meta_end)?;
    let mut new_meta = Vec::with_capacity(meta.len() + cols.len());
    let mut off = 0;
    for i in 0..n {
        let m = tme.get_column_metadata(i)?;
        if meta.get(off..off + m.len())? != m {
            return None;
        }
        off += m.len();
        // BIT metadata: (bits % 8, bytes).
        new_meta.extend_from_slice(if cols.contains(&i) { &[0, 4] } else { m });
    }
    if off != meta.len() || post < 6 {
        return None;
    }
    let mut twin = Vec::with_capacity(19 + body.len() + cols.len() + 8);
    twin.extend_from_slice(&0u32.to_le_bytes()); // timestamp
    twin.push(ty::TABLE_MAP);
    twin.extend_from_slice(&ev.header().server_id().to_le_bytes());
    twin.extend_from_slice(&0u32.to_le_bytes()); // event size, below
    twin.extend_from_slice(&0u32.to_le_bytes()); // log_pos
    twin.extend_from_slice(&0u16.to_le_bytes()); // flags
                                                 // The default format description expects a 6-byte table id.
    twin.extend_from_slice(&tme.table_id().to_le_bytes()[..6]);
    twin.extend_from_slice(body.get(post - 2..post)?);
    twin.extend_from_slice(&body[post..types_at]);
    twin.extend(
        body[types_at..types_at + n]
            .iter()
            .enumerate()
            .map(|(i, &t)| {
                if cols.contains(&i) {
                    ColumnType::MYSQL_TYPE_BIT as u8
                } else {
                    t
                }
            }),
    );
    put_lenenc(&mut twin, new_meta.len() as u64);
    twin.extend_from_slice(&new_meta);
    twin.extend_from_slice(&body[meta_end..]);
    let size = u32::try_from(twin.len()).ok()?;
    twin[9..13].copy_from_slice(&size.to_le_bytes());
    let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
    let ev = Event::read(&fde, &twin[..]).ok()?;
    let tme: TableMapEvent<'_> = ev.read_event().ok()?;
    Some(tme.into_owned())
}

/// Length-encoded integer at the start of `b`: (value, bytes used).
fn lenenc(b: &[u8]) -> Option<(u64, usize)> {
    let le = |n: usize| -> Option<u64> {
        let mut v = [0u8; 8];
        v[..n].copy_from_slice(b.get(1..1 + n)?);
        Some(u64::from_le_bytes(v))
    };
    match *b.first()? {
        x @ 0..=0xfa => Some((u64::from(x), 1)),
        0xfc => Some((le(2)?, 3)),
        0xfd => Some((le(3)?, 4)),
        0xfe => Some((le(8)?, 9)),
        _ => None,
    }
}

fn put_lenenc(out: &mut Vec<u8>, v: u64) {
    match v {
        0..=0xfa => out.push(v as u8),
        0xfb..=0xffff => {
            out.push(0xfc);
            out.extend_from_slice(&(v as u16).to_le_bytes());
        }
        0x1_0000..=0xff_ffff => {
            out.push(0xfd);
            out.extend_from_slice(&v.to_le_bytes()[..3]);
        }
        _ => {
            out.push(0xfe);
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
}

/// A TIME2 value with 1 or 2 fractional digits from its 4 binlog bytes (MySQL's
/// `my_time_packed_from_binary` and `TIME_from_longlong_time_packed`).
fn time2_short_fraction(b: [u8; 4]) -> Value {
    let mut int = (i64::from(b[0]) << 16 | i64::from(b[1]) << 8 | i64::from(b[2])) - 0x80_0000;
    let mut frac = i64::from(b[3]);
    if int < 0 && frac != 0 {
        // Negative values store the fraction reversed: -(0x100 - frac) of the next second up.
        int += 1;
        frac -= 0x100;
    }
    let packed = (int << 24) + frac * 10_000;
    let abs = packed.unsigned_abs();
    let hms = abs >> 24;
    let hours = (hms >> 12) % (1 << 10);
    Value::Time(
        packed < 0,
        (hours / 24) as u32,
        (hours % 24) as u8,
        ((hms >> 6) % (1 << 6)) as u8,
        (hms % (1 << 6)) as u8,
        (abs % (1 << 24)) as u32,
    )
}

impl Tracker {
    /// Resume point after the current event, which ends a transaction or statement. When the
    /// exact offset is unknown (see `pos_exact`) the previous boundary is returned instead:
    /// resuming there replays this transaction, which is harmless, whereas a guessed offset
    /// could point into the middle of an event.
    fn boundary_here(&mut self) -> Position {
        if self.pos_exact {
            self.boundary = self.pos.clone();
        }
        self.boundary.clone()
    }

    /// `Item::Rows` for a rows event (`Item::Other` for the dummy end-of-statement one).
    fn rows_item(&self, ev: Event) -> Result<Item> {
        let et = ev.header().event_type_raw();
        let kind = match et {
            ty::WRITE_ROWS_V1 | ty::WRITE_ROWS => RowsKind::Insert,
            ty::DELETE_ROWS_V1 | ty::DELETE_ROWS => RowsKind::Delete,
            ty::UPDATE_ROWS_V1 | ty::UPDATE_ROWS | ty::PARTIAL_UPDATE_ROWS => RowsKind::Update,
            _ => bail!("event type {et} at {} is not a rows event", self.pos),
        };
        let table_id = match ev
            .read_data()
            .with_context(|| format!("parsing rows event at {}", self.pos))?
        {
            Some(EventData::RowsEvent(re)) => re.table_id(),
            _ => bail!(
                "event type {et} at {} did not parse as a rows event",
                self.pos
            ),
        };
        Ok(if table_id == DUMMY_TABLE_ID {
            Item::Other
        } else {
            Item::Rows {
                table_id,
                kind,
                event: ev,
            }
        })
    }

    /// Adds a MariaDB `Partial_rows` fragment (type 172, MariaDB >= 12.3); returns the rows event
    /// once its last fragment is in.
    ///
    /// Body: fragment count u32, sequence number u32 (from 1), flags u8; the first fragment then
    /// has the length of the whole rows event u64. The fragments follow each other and their
    /// payloads concatenate to the rows event (header and body, without its checksum, although
    /// its header counts one).
    fn partial_rows(&mut self, ev: &Event) -> Result<Option<Event>> {
        let data = ev.data();
        let at = &self.pos;
        let bad = || {
            anyhow!(
                "malformed MariaDB Partial_rows event at {at} ({} bytes)",
                data.len()
            )
        };
        let u32_at = |i: usize| {
            data.get(i..i + 4)
                .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")))
        };
        let total = u32_at(0).ok_or_else(bad)?;
        let seq = u32_at(4).ok_or_else(bad)?;
        let mut payload = data.get(9..).ok_or_else(bad)?;
        if seq == 1 {
            let len = payload
                .get(..8)
                .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")))
                .ok_or_else(bad)?;
            payload = &payload[8..];
            self.partial = Some(Partial {
                total,
                next: 1,
                len,
                buf: Vec::new(),
            });
        }
        let Some(p) = self
            .partial
            .as_mut()
            .filter(|p| p.next == seq && p.total == total)
        else {
            bail!("MariaDB Partial_rows fragment {seq} of {total} at {at} does not continue a rows event");
        };
        p.buf.extend_from_slice(payload);
        p.next += 1;
        if seq < total {
            return Ok(None);
        }
        let Partial { len, mut buf, .. } = self.partial.take().expect("checked above");
        if buf.len() as u64 != len || buf.len() < 19 {
            bail!(
                "MariaDB Partial_rows fragments ending at {at} hold {} bytes, the rows event has {len}",
                buf.len()
            );
        }
        let size = u32::try_from(buf.len()).context("reassembled rows event over 4 GiB")?;
        buf[9..13].copy_from_slice(&size.to_le_bytes());
        let mut footer = ev.fde().footer();
        footer.set_checksum_enabled(false);
        let fde = ev.fde().clone().with_footer(footer);
        let rows = Event::read(&fde, &buf[..])
            .with_context(|| format!("reading the rows event reassembled at {at}"))?;
        Ok(Some(rows))
    }

    /// Updates the position bookkeeping for `ev` and maps it to an [`Item`].
    fn map_event(&mut self, ev: Event) -> Result<Item> {
        let header = ev.header();
        let et = header.event_type_raw();
        let log_pos = u64::from(header.log_pos());

        let inner = match self.payload.as_mut() {
            Some(p) => {
                let size = u64::from(header.event_size());
                if log_pos != 0 && log_pos > self.pos.pos {
                    // An event from the file itself while inner events were still expected:
                    // mysql_async silently skips payloads it fails to decompress.
                    bail!(
                        "transaction payload ending at {} was not expanded ({} bytes of inner events missing)",
                        self.pos,
                        p.remaining
                    );
                }
                p.remaining = p.remaining.saturating_sub(size);
                if p.remaining == 0 {
                    self.payload = None;
                }
                true
            }
            None => false,
        };
        let artificial = header.flags_raw() & ARTIFICIAL_FLAG != 0;
        if !inner && !artificial && !matches!(et, ty::HEARTBEAT | ty::HEARTBEAT_V2) {
            if log_pos != 0 {
                self.pos.pos = log_pos;
                self.pos_exact = true;
            } else if !matches!(et, ty::FORMAT_DESCRIPTION | ty::ROTATE) {
                // MariaDB >= 11.4 (binlog_legacy_event_pos=OFF) writes log_pos = 0 for events
                // inside a transaction; they are contiguous in the file, so advance by size.
                // (A FORMAT_DESCRIPTION with log_pos 0 is the copy re-sent at stream start.)
                self.pos.pos += u64::from(header.event_size());
                if et == ty::TABLE_MAP {
                    self.pos_exact = false;
                }
            }
        }

        let item = match et {
            ty::ROTATE => {
                let real = log_pos != 0 && !artificial;
                let (file, pos) = rotate_target(&ev, !real)?;
                if real {
                    // Keep table maps of the file just finished, drop older ones.
                    let g = self.generation;
                    self.tables.retain(|_, (gen, _, _)| *gen >= g);
                    self.generation += 1;
                }
                self.pos = Position { file, pos: pos.max(4) };
                Item::Other
            }
            ty::TABLE_MAP => {
                let tme: TableMapEvent<'_> = ev.read_event().context("parsing TABLE_MAP event")?;
                let item = Item::TableMap {
                    table_id: tme.table_id(),
                    schema: tme.database_name().into_owned(),
                    table: tme.table_name().into_owned(),
                    columns: tme.columns_count(),
                };
                let twin = time2_twin(&ev, &tme);
                self.tables.insert(
                    tme.table_id(),
                    (self.generation, tme.into_owned(), twin),
                );
                item
            }
            ty::WRITE_ROWS_V1
            | ty::UPDATE_ROWS_V1
            | ty::DELETE_ROWS_V1
            | ty::WRITE_ROWS
            | ty::UPDATE_ROWS
            | ty::DELETE_ROWS
            | ty::PARTIAL_UPDATE_ROWS => self.rows_item(ev)?,
            ty::MARIA_PARTIAL_ROWS => match self.partial_rows(&ev)? {
                Some(rows) => self.rows_item(rows)?,
                None => Item::Other,
            },
            ty::PRE_GA_WRITE_ROWS..=ty::PRE_GA_DELETE_ROWS => {
                bail!("unsupported pre-GA rows event (type {et}) at {}", self.pos)
            }
            ty::XID => Item::Commit { pos: self.boundary_here() },
            ty::XA_PREPARE => match xa_prepare_event(ev.data())? {
                (true, _) => Item::Commit { pos: self.boundary_here() }, // XA COMMIT … ONE PHASE
                (false, xid) => Item::XaPrepare { xid, pos: self.boundary_here() },
            },
            ty::QUERY => {
                let qe: QueryEvent<'_> = ev.read_event().context("parsing QUERY event")?;
                let query = qe.query();
                match classify_query(&query) {
                    QueryKind::InTxn => Item::Other,
                    QueryKind::TxnEnd => Item::Commit { pos: self.boundary_here() },
                    QueryKind::XaStart(xid) => Item::XaStart { xid },
                    QueryKind::XaPrepare(xid) => Item::XaPrepare { xid, pos: self.boundary_here() },
                    QueryKind::XaEnd(xid, commit) => Item::XaEnd { xid, commit, pos: self.boundary_here() },
                    QueryKind::Statement => Item::Statement {
                        schema: qe.schema().into_owned(),
                        query: query.into_owned(),
                        pos: self.boundary_here(),
                    },
                }
            }
            ty::GTID | ty::GTID_TAGGED => {
                let ge: GtidEvent = ev.read_event().context("parsing GTID event")?;
                Item::Gtid { gtid: mysql_gtid(&ge) }
            }
            ty::MARIA_GTID => {
                let (gtid, xa) = mariadb_gtid(header.server_id(), ev.data())?;
                self.queued = xa.map(|xid| Item::XaStart { xid });
                Item::Gtid { gtid }
            }
            ty::HEARTBEAT | ty::HEARTBEAT_V2 => Item::Heartbeat,
            ty::TRANSACTION_PAYLOAD => {
                let tpe: TransactionPayloadEvent<'_> =
                    ev.read_event().context("parsing TRANSACTION_PAYLOAD event")?;
                // Uncompressed payloads (algorithm NONE) carry no uncompressed size.
                let size = match tpe.uncompressed_size() {
                    0 => tpe.payload_raw().len() as u64,
                    n => n,
                };
                if size > 0 {
                    self.payload = Some(Payload { remaining: size });
                }
                Item::Other
            }
            ty::MARIA_QUERY_COMPRESSED..=ty::MARIA_DELETE_ROWS_COMPRESSED => bail!(
                "MariaDB compressed binlog event (type {et}) at {} is not supported; \
                 set log_bin_compress=OFF on the server (events already written compressed cannot be read)",
                self.pos
            ),
            ty::INCIDENT => {
                tracing::warn!(at = %self.pos, "INCIDENT event in the binlog: the source reports possibly lost changes");
                Item::Other
            }
            // FORMAT_DESCRIPTION, PREVIOUS_GTIDS, ANONYMOUS_GTID, ROWS_QUERY, INTVAR, RAND,
            // USER_VAR, STOP, IGNORABLE, TRANSACTION_CONTEXT, VIEW_CHANGE, MariaDB ANNOTATE_ROWS
            // (160), BINLOG_CHECKPOINT (161), GTID_LIST (163), START_ENCRYPTION (164), …
            _ => Item::Other,
        };
        Ok(item)
    }
}

/// `(file, position)` a ROTATE event points to.
///
/// The artificial ROTATE that opens a stream arrives before any FORMAT_DESCRIPTION event, so
/// mysql_common cannot know whether it carries a checksum: MySQL sends it without one, MariaDB
/// with a CRC32. For artificial rotates a trailing CRC32 that matches the event is stripped.
fn rotate_target(ev: &Event, artificial: bool) -> Result<(String, u64)> {
    let data = ev.data();
    if data.len() < 8 {
        bail!("ROTATE event too short ({} bytes)", data.len());
    }
    let pos = u64::from_le_bytes(data[..8].try_into().expect("8 bytes"));
    let mut name = &data[8..];
    if artificial && name.len() > 4 {
        let (body, tail) = data.split_at(data.len() - 4);
        let h = ev.header();
        let mut buf = Vec::with_capacity(19 + body.len());
        buf.extend_from_slice(&h.timestamp().to_le_bytes());
        buf.push(h.event_type_raw());
        buf.extend_from_slice(&h.server_id().to_le_bytes());
        buf.extend_from_slice(&h.event_size().to_le_bytes());
        buf.extend_from_slice(&h.log_pos().to_le_bytes());
        buf.extend_from_slice(&h.flags_raw().to_le_bytes());
        buf.extend_from_slice(body);
        if crc32(&buf).to_le_bytes() == tail {
            name = &body[8..];
        }
    }
    let file = std::str::from_utf8(name).context("ROTATE event with a non-UTF-8 file name")?;
    if file.is_empty() {
        bail!("ROTATE event without a file name");
    }
    Ok((file.to_string(), pos))
}

/// CRC-32 (IEEE), as used by `binlog_checksum=CRC32`. Bitwise: only used for tiny events.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn tme_name(tme: &TableMapEvent<'_>) -> String {
    format!("`{}`.`{}`", tme.database_name(), tme.table_name())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryKind {
    /// BEGIN / SAVEPOINT / ROLLBACK TO / XA END: inside a transaction.
    InTxn,
    /// COMMIT / ROLLBACK / XA COMMIT … ONE PHASE: transaction boundary.
    TxnEnd,
    XaStart(String),
    XaPrepare(String),
    /// XA COMMIT (true) / XA ROLLBACK (false) of a prepared transaction.
    XaEnd(String, bool),
    Statement,
}

fn classify_query(q: &str) -> QueryKind {
    let t = q.trim_start();
    let (first, rest) = split_word(t);
    let first = first.to_ascii_uppercase();
    let (second, rest2) = split_word(rest);
    let second = second.to_ascii_uppercase();
    match (first.as_str(), second.as_str()) {
        ("BEGIN", _) | ("SAVEPOINT", _) | ("ROLLBACK", "TO") => QueryKind::InTxn,
        ("COMMIT", _) | ("ROLLBACK", _) => QueryKind::TxnEnd,
        ("XA", "START" | "BEGIN") => QueryKind::XaStart(xid_from_sql(rest2).0),
        ("XA", "END") => QueryKind::InTxn,
        ("XA", "PREPARE") => QueryKind::XaPrepare(xid_from_sql(rest2).0),
        ("XA", "COMMIT") => {
            let (xid, tail) = xid_from_sql(rest2);
            let tail = tail
                .split_ascii_whitespace()
                .map(str::to_ascii_uppercase)
                .collect::<Vec<_>>();
            if tail.first().map(String::as_str) == Some("ONE") {
                QueryKind::TxnEnd
            } else {
                QueryKind::XaEnd(xid, true)
            }
        }
        ("XA", "ROLLBACK") => QueryKind::XaEnd(xid_from_sql(rest2).0, false),
        _ => QueryKind::Statement,
    }
}

/// First whitespace/`;`-delimited word and the rest.
fn split_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    let end = s
        .find(|c: char| c.is_ascii_whitespace() || c == ';')
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// Canonical XA transaction id: `X'<gtrid hex>',X'<bqual hex>',<formatID>` (lowercase hex),
/// the form the servers log it in.
fn xid_string(format_id: i64, gtrid: &[u8], bqual: &[u8]) -> String {
    let mut s = String::with_capacity(16 + 2 * (gtrid.len() + bqual.len()));
    s.push_str("X'");
    for b in gtrid {
        let _ = write!(s, "{b:02x}");
    }
    s.push_str("',X'");
    for b in bqual {
        let _ = write!(s, "{b:02x}");
    }
    let _ = write!(s, "',{format_id}");
    s
}

/// Parses `gtrid [, bqual [, formatID]]` (string, `X'hex'` or `0xhex` literals) from the start of
/// `s`; returns the canonical xid and the remaining text. Unparsable input is kept verbatim.
fn xid_from_sql(s: &str) -> (String, &str) {
    fn literal(s: &str) -> Option<(Vec<u8>, &str)> {
        let s = s.trim_start();
        let b = s.as_bytes();
        let hex = |digits: &str| -> Option<Vec<u8>> {
            let d = if digits.len() % 2 == 1 {
                format!("0{digits}")
            } else {
                digits.to_string()
            };
            (0..d.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&d[i..i + 2], 16).ok())
                .collect()
        };
        match b.first()? {
            b'X' | b'x' if b.get(1) == Some(&b'\'') => {
                let end = s[2..].find('\'')? + 2;
                Some((hex(&s[2..end])?, &s[end + 1..]))
            }
            b'0' if matches!(b.get(1), Some(b'x' | b'X')) => {
                let end = s[2..]
                    .find(|c: char| !c.is_ascii_hexdigit())
                    .map_or(s.len(), |e| e + 2);
                Some((hex(&s[2..end])?, &s[end..]))
            }
            q @ (b'\'' | b'"') => {
                let mut out = Vec::new();
                let mut i = 1;
                while i < b.len() {
                    match b[i] {
                        c if c == *q && b.get(i + 1) == Some(q) => {
                            out.push(c);
                            i += 2;
                        }
                        c if c == *q => return Some((out, &s[i + 1..])),
                        b'\\' if i + 1 < b.len() => {
                            out.push(b[i + 1]);
                            i += 2;
                        }
                        c => {
                            out.push(c);
                            i += 1;
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
    let verbatim = || {
        let (w, rest) = split_word(s);
        (w.to_string(), rest)
    };
    let Some((gtrid, mut rest)) = literal(s) else {
        return verbatim();
    };
    let mut bqual = Vec::new();
    let mut format_id = 1i64;
    if let Some(r) = rest.trim_start().strip_prefix(',') {
        let Some((b, r)) = literal(r) else {
            return verbatim();
        };
        bqual = b;
        rest = r;
        if let Some(r) = rest.trim_start().strip_prefix(',') {
            let r = r.trim_start();
            let end = r
                .find(|c: char| !(c.is_ascii_digit() || c == '-'))
                .unwrap_or(r.len());
            let Ok(n) = r[..end].parse() else {
                return verbatim();
            };
            format_id = n;
            rest = &r[end..];
        }
    }
    (xid_string(format_id, &gtrid, &bqual), rest)
}

/// XA_PREPARE_LOG_EVENT body (MySQL and MariaDB): one_phase u8, formatID u32, gtrid_length u32,
/// bqual_length u32, gtrid + bqual. Returns `(one_phase, xid)`.
fn xa_prepare_event(data: &[u8]) -> Result<(bool, String)> {
    let bad = || anyhow!("malformed XA_PREPARE event ({} bytes)", data.len());
    let u32_at = |i: usize| {
        data.get(i..i + 4)
            .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")))
    };
    let one_phase = *data.first().ok_or_else(bad)? != 0;
    let format_id = u32_at(1).ok_or_else(bad)? as i32;
    let g = u32_at(5).ok_or_else(bad)? as usize;
    let q = u32_at(9).ok_or_else(bad)? as usize;
    let ids = data.get(13..13 + g + q).ok_or_else(bad)?;
    Ok((
        one_phase,
        xid_string(i64::from(format_id), &ids[..g], &ids[g..]),
    ))
}

fn format_uuid(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, x) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn mysql_gtid(ge: &GtidEvent) -> String {
    let uuid = format_uuid(&ge.sid());
    match ge.tag() {
        Some(tag) => format!("{uuid}:{tag}:{}", ge.gno()),
        None => format!("{uuid}:{}", ge.gno()),
    }
}

/// MariaDB GTID_EVENT body (little endian): seq_no u64, domain_id u32, flags2 u8,
/// [commit_id u64 if FL_GROUP_COMMIT_ID], [formatID u32, gtrid_len u8, bqual_len u8, ids if
/// FL_PREPARED_XA / FL_COMPLETED_XA], …. Returns the GTID and, for the GTID that opens a
/// prepared XA transaction, its xid.
fn mariadb_gtid(server_id: u32, data: &[u8]) -> Result<(String, Option<String>)> {
    const FL_GROUP_COMMIT_ID: u8 = 0x02;
    const FL_PREPARED_XA: u8 = 0x40;
    const FL_COMPLETED_XA: u8 = 0x80;
    if data.len() < 12 {
        bail!("MariaDB GTID event too short ({} bytes)", data.len());
    }
    let seq = u64::from_le_bytes(data[0..8].try_into().expect("8 bytes"));
    let domain = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
    let gtid = format!("{domain}-{server_id}-{seq}");
    let flags = data.get(12).copied().unwrap_or(0);
    if flags & (FL_PREPARED_XA | FL_COMPLETED_XA) == 0 {
        return Ok((gtid, None));
    }
    let mut i = 13
        + if flags & FL_GROUP_COMMIT_ID != 0 {
            8
        } else {
            0
        };
    let bad = || anyhow!("malformed XA id in MariaDB GTID event {gtid}");
    let format_id = data.get(i..i + 4).ok_or_else(bad)?;
    let format_id = i32::from_le_bytes(format_id.try_into().expect("4 bytes"));
    let (g, q) = (
        *data.get(i + 4).ok_or_else(bad)? as usize,
        *data.get(i + 5).ok_or_else(bad)? as usize,
    );
    i += 6;
    let ids = data.get(i..i + g + q).ok_or_else(bad)?;
    let xid = xid_string(i64::from(format_id), &ids[..g], &ids[g..]);
    // Only the prepare opens the transaction; the completing GTID precedes XA COMMIT/ROLLBACK.
    Ok((gtid, (flags & FL_PREPARED_XA != 0).then_some(xid)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_classification() {
        use QueryKind::*;
        let x = |s: &str| s.to_string();
        for (q, k) in [
            ("BEGIN", InTxn),
            ("begin", InTxn),
            ("COMMIT", TxnEnd),
            ("ROLLBACK", TxnEnd),
            ("ROLLBACK TO `sp1`", InTxn),
            ("SAVEPOINT `sp1`", InTxn),
            ("XA START X'31',X'',1", XaStart(x("X'31',X'',1"))),
            ("xa begin 'p1'", XaStart(x("X'7031',X'',1"))),
            ("XA START X'31',X'',1 JOIN", XaStart(x("X'31',X'',1"))),
            ("XA END X'31',X'',1", InTxn),
            ("XA PREPARE X'31',X'',1", XaPrepare(x("X'31',X'',1"))),
            (
                "XA COMMIT X'7231',X'622071',7",
                XaEnd(x("X'7231',X'622071',7"), true),
            ),
            (
                "XA ROLLBACK X'7231',X'622071',7",
                XaEnd(x("X'7231',X'622071',7"), false),
            ),
            ("XA COMMIT X'31',X'',1 ONE PHASE", TxnEnd),
            (
                "XA COMMIT 'a''b', \"c\" , 42",
                XaEnd(x("X'612762',X'63',42"), true),
            ),
            (
                "XA COMMIT 0xABcd,X'0F',-1",
                XaEnd(x("X'abcd',X'0f',-1"), true),
            ),
            ("XA COMMIT weird", XaEnd(x("weird"), true)),
            ("  COMMIT /* implicit */", TxnEnd),
            ("CREATE TABLE t (a int)", Statement),
            ("COMMITTED_THING", Statement),
            ("XACT", Statement),
            ("", Statement),
        ] {
            assert_eq!(classify_query(q), k, "{q:?}");
        }
    }

    #[test]
    fn xa_event_bodies() {
        // XA_PREPARE_LOG_EVENT as logged by MySQL 8 / MariaDB for XA PREPARE 'r1','b q',7
        let body = [
            0, 7, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, b'r', b'1', b'b', b' ', b'q',
        ];
        assert_eq!(
            xa_prepare_event(&body).unwrap(),
            (false, "X'7231',X'622071',7".to_string())
        );
        let mut one = body;
        one[0] = 1;
        assert!(xa_prepare_event(&one).unwrap().0);
        assert!(xa_prepare_event(&body[..15]).is_err());
        // MariaDB GTID opening a prepared XA transaction (captured from 10.11 / 11.4)
        let g = [
            0xbb, 0x0f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x4c, 1, 0, 0, 0, 2, 0, b'p', b'1', 1, 0xff,
        ];
        assert_eq!(
            mariadb_gtid(9, &g).unwrap(),
            ("0-9-4027".to_string(), Some("X'7031',X'',1".to_string()))
        );
        // … and the one completing it (FL_COMPLETED_XA): no XaStart
        let c = [
            0xbc, 0x0f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x8d, 1, 0, 0, 0, 2, 0, b'p', b'1',
        ];
        assert_eq!(mariadb_gtid(9, &c).unwrap(), ("0-9-4028".to_string(), None));
        // FL_GROUP_COMMIT_ID shifts the xid by 8 bytes
        let mut gc = g[..13].to_vec();
        gc[12] = 0x42;
        gc.extend_from_slice(&[9; 8]);
        gc.extend_from_slice(&[5, 0, 0, 0, 1, 1, b'a', b'b']);
        assert_eq!(
            mariadb_gtid(9, &gc).unwrap().1.as_deref(),
            Some("X'61',X'62',5")
        );
        assert!(mariadb_gtid(9, &gc[..20]).is_err());
    }

    #[test]
    fn gtid_formats() {
        let sid = [
            0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42,
            0x95, 0x62,
        ];
        assert_eq!(format_uuid(&sid), "3e11fa47-71ca-11e1-9e33-c80aa9429562");
        assert_eq!(
            mysql_gtid(&GtidEvent::new(sid, 23)),
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"
        );
        let mut body = 77u64.to_le_bytes().to_vec();
        body.extend_from_slice(&2u32.to_le_bytes());
        body.push(0);
        assert_eq!(
            mariadb_gtid(5, &body).unwrap(),
            ("2-5-77".to_string(), None)
        );
        assert!(mariadb_gtid(5, &body[..5]).is_err());
    }

    #[test]
    fn missing_binlog_detection() {
        let se = |code, msg: &str| mysql_async::ServerError {
            code,
            message: msg.into(),
            state: "HY000".into(),
        };
        let missing = se(
            1236,
            "Could not find first log file name in binary log index file",
        );
        assert!(is_missing_binlog(&anyhow::Error::new(
            mysql_async::Error::Server(missing.clone())
        )));
        assert!(is_missing_binlog(
            &anyhow::Error::new(mysql_async::Error::Server(missing)).context("outer")
        ));
        assert!(is_missing_binlog(&anyhow::Error::new(MissingBinlog {
            from: Position {
                file: "binlog.000001".into(),
                pos: 4
            },
            message: "x".into()
        })));
        let other = se(1236, "A replica with the same server_uuid/server_id as this replica has connected to the source");
        assert!(!is_missing_binlog(&anyhow::Error::new(
            mysql_async::Error::Server(other)
        )));
        assert!(!is_missing_binlog(&anyhow!(
            "Could not find first log file name"
        )));
    }

    // ---- Tracker: hand-built events (default FDE: no checksums, standard post-header sizes)

    fn header(et: u8, server_id: u32, size: usize, log_pos: u32, flags: u16) -> Vec<u8> {
        let mut b = Vec::with_capacity(size);
        b.extend_from_slice(&0u32.to_le_bytes());
        b.push(et);
        b.extend_from_slice(&server_id.to_le_bytes());
        b.extend_from_slice(&(size as u32).to_le_bytes());
        b.extend_from_slice(&log_pos.to_le_bytes());
        b.extend_from_slice(&flags.to_le_bytes());
        b
    }

    fn read(bytes: &[u8]) -> Event {
        use mysql_async::binlog::events::FormatDescriptionEvent;
        use mysql_async::binlog::BinlogVersion;
        Event::read(&FormatDescriptionEvent::new(BinlogVersion::Version4), bytes).unwrap()
    }

    fn ev(et: u8, log_pos: u32, body: &[u8]) -> Event {
        let mut b = header(et, 7, 19 + body.len(), log_pos, 0);
        b.extend_from_slice(body);
        read(&b)
    }

    fn rotate(pos: u64, name: &str, log_pos: u32, artificial: bool, crc: bool) -> Event {
        let mut body = pos.to_le_bytes().to_vec();
        body.extend_from_slice(name.as_bytes());
        let size = 19 + body.len() + if crc { 4 } else { 0 };
        let mut b = header(
            ty::ROTATE,
            7,
            size,
            log_pos,
            if artificial { ARTIFICIAL_FLAG } else { 0 },
        );
        b.extend_from_slice(&body);
        if crc {
            let c = crc32(&b);
            b.extend_from_slice(&c.to_le_bytes());
        }
        read(&b)
    }

    fn query(log_pos: u32, schema: &str, q: &str) -> Event {
        let mut b = Vec::new();
        b.extend_from_slice(&1u32.to_le_bytes()); // thread id
        b.extend_from_slice(&0u32.to_le_bytes()); // exec time
        b.push(schema.len() as u8);
        b.extend_from_slice(&0u16.to_le_bytes()); // error code
        b.extend_from_slice(&0u16.to_le_bytes()); // status vars length
        b.extend_from_slice(schema.as_bytes());
        b.push(0);
        b.extend_from_slice(q.as_bytes());
        ev(ty::QUERY, log_pos, &b)
    }

    fn xid(log_pos: u32) -> Event {
        ev(ty::XID, log_pos, &42u64.to_le_bytes())
    }

    /// TABLE_MAP for `schema.table (a INT)`.
    fn table_map(log_pos: u32, table_id: u64, schema: &str, table: &str) -> Event {
        let mut b = table_id.to_le_bytes()[..6].to_vec();
        b.extend_from_slice(&1u16.to_le_bytes());
        b.push(schema.len() as u8);
        b.extend_from_slice(schema.as_bytes());
        b.push(0);
        b.push(table.len() as u8);
        b.extend_from_slice(table.as_bytes());
        b.push(0);
        b.extend_from_slice(&[1, 0x03, 0, 0]); // 1 column, LONG, no metadata, null bitmap
        ev(ty::TABLE_MAP, log_pos, &b)
    }

    /// WRITE_ROWS v1 with one row `(a)` for a single INT column.
    fn write_rows(log_pos: u32, table_id: u64, a: i32) -> Event {
        let mut b = table_id.to_le_bytes()[..6].to_vec();
        b.extend_from_slice(&1u16.to_le_bytes()); // STMT_END
        b.extend_from_slice(&[1, 0x01, 0x00]); // 1 column, present, row null bitmap
        b.extend_from_slice(&a.to_le_bytes());
        ev(ty::WRITE_ROWS_V1, log_pos, &b)
    }

    fn maria_gtid(log_pos: u32, seq: u64) -> Event {
        let mut b = seq.to_le_bytes().to_vec();
        b.extend_from_slice(&0u32.to_le_bytes());
        b.push(0);
        ev(ty::MARIA_GTID, log_pos, &b)
    }

    fn payload(log_pos: u32, uncompressed: u64, compressed_len: usize) -> Event {
        // compression type ZSTD, uncompressed size, payload size, end mark, payload bytes
        let mut b = vec![
            2,
            1,
            0,
            3,
            1,
            uncompressed as u8,
            1,
            1,
            compressed_len as u8,
            0,
        ];
        b.extend(std::iter::repeat_n(0xAA, compressed_len));
        ev(ty::TRANSACTION_PAYLOAD, log_pos, &b)
    }

    fn size(e: &Event) -> u32 {
        e.header().event_size()
    }

    fn pos(file: &str, pos: u64) -> Position {
        Position {
            file: file.into(),
            pos,
        }
    }

    fn commit_pos(item: Item) -> Position {
        match item {
            Item::Commit { pos } => pos,
            other => panic!("expected Commit, got {other:?}"),
        }
    }

    fn stmt_pos(item: Item) -> Position {
        match item {
            Item::Statement { pos, .. } => pos,
            other => panic!("expected Statement, got {other:?}"),
        }
    }

    #[test]
    fn stream_start_rotates() {
        // MariaDB: artificial ROTATE with a CRC32 that mysql_common could not strip.
        let mut t = Tracker::new(Flavor::MariaDb, &pos("mariadb-bin.000003", 100));
        assert!(matches!(
            t.map_event(rotate(100, "mariadb-bin.000003", 0, true, true))
                .unwrap(),
            Item::Other
        ));
        assert_eq!(t.pos, pos("mariadb-bin.000003", 100));
        // MySQL: no checksum on the artificial ROTATE.
        let mut t = Tracker::new(Flavor::MySql, &pos("binlog.000002", 4));
        t.map_event(rotate(4, "binlog.000002", 0, true, false))
            .unwrap();
        assert_eq!(t.pos, pos("binlog.000002", 4));
    }

    /// TABLE_MAP for `schema.table (a INT, t TIME(fsp), u TIME(6), v VARCHAR(10))`, with
    /// SIGNEDNESS optional metadata for the INT column.
    fn table_map_times(log_pos: u32, table_id: u64, fsp: u8) -> Event {
        let mut b = table_id.to_le_bytes()[..6].to_vec();
        b.extend_from_slice(&1u16.to_le_bytes());
        for name in ["db", "tq"] {
            b.push(name.len() as u8);
            b.extend_from_slice(name.as_bytes());
            b.push(0);
        }
        b.push(4); // columns
        b.extend_from_slice(&[0x03, 0x13, 0x13, 0x0f]); // LONG, TIME2, TIME2, VARCHAR
        b.extend_from_slice(&[4, fsp, 6, 40, 0]); // metadata: fsp, fsp, max length (LE u16)
        b.push(0x0f); // all nullable
        b.extend_from_slice(&[1, 1, 0x00]); // SIGNEDNESS: one numeric column, signed
        ev(ty::TABLE_MAP, log_pos, &b)
    }

    /// WRITE_ROWS v1 with one row `(a, t, u, 'x')`; `t` and `u` as raw TIME2 bytes.
    fn write_rows_times(log_pos: u32, table_id: u64, a: i32, t: &[u8], u: &[u8]) -> Event {
        let mut b = table_id.to_le_bytes()[..6].to_vec();
        b.extend_from_slice(&1u16.to_le_bytes()); // STMT_END
        b.extend_from_slice(&[4, 0x0f, 0x00]); // 4 columns, all present, no NULLs
        b.extend_from_slice(&a.to_le_bytes());
        b.extend_from_slice(t);
        b.extend_from_slice(u);
        b.extend_from_slice(&[1, b'x']);
        ev(ty::WRITE_ROWS_V1, log_pos, &b)
    }

    /// Negative TIME(1) / TIME(2) values with a fraction (mysql_common 0.37 panics on them in
    /// debug builds) decode as the server stores them; other columns are unaffected.
    #[test]
    fn short_fraction_negative_times() {
        let t6 = [0x80, 0xc0, 0x00, 0x00, 0x00, 0x01]; // 12:00:00.000001
        let cases: &[(u8, [u8; 4], Value)] = &[
            // -00:00:01.50 (intpart -2, reversed fraction 0x100 - 50)
            (
                2,
                [0x7f, 0xff, 0xfe, 206],
                Value::Time(true, 0, 0, 0, 1, 500_000),
            ),
            // -00:00:00.01
            (
                2,
                [0x7f, 0xff, 0xff, 255],
                Value::Time(true, 0, 0, 0, 0, 10_000),
            ),
            // -838:59:58.99
            (
                2,
                [0x4b, 0x91, 0x05, 157],
                Value::Time(true, 34, 22, 59, 58, 990_000),
            ),
            // -00:00:01.00: no fraction, nothing reversed
            (2, [0x7f, 0xff, 0xff, 0], Value::Time(true, 0, 0, 0, 1, 0)),
            // 12:00:00.75
            (
                2,
                [0x80, 0xc0, 0x00, 75],
                Value::Time(false, 0, 12, 0, 0, 750_000),
            ),
            // TIME(1) -100:00:00.1 is stored like TIME(2) -100:00:00.10
            (
                1,
                [0x79, 0xbf, 0xff, 246],
                Value::Time(true, 4, 4, 0, 0, 100_000),
            ),
        ];
        for (k, (fsp, raw, want)) in cases.iter().enumerate() {
            let mut t = Tracker::new(Flavor::MySql, &pos("binlog.000001", 4));
            let id = 40 + k as u64;
            t.map_event(table_map_times(100, id, *fsp)).unwrap();
            let rows = match t
                .map_event(write_rows_times(200, id, -7, raw, &t6))
                .unwrap()
            {
                Item::Rows { event, .. } => event,
                other => panic!("{other:?}"),
            };
            let decoded = t.rows(id, &rows).unwrap();
            let row = decoded[0].1.as_ref().unwrap();
            assert_eq!(row.as_ref(0), Some(&BinlogValue::Value(Value::Int(-7))));
            assert_eq!(
                row.as_ref(1),
                Some(&BinlogValue::Value(want.clone())),
                "{raw:?}"
            );
            assert_eq!(
                row.as_ref(2),
                Some(&BinlogValue::Value(Value::Time(false, 0, 12, 0, 0, 1)))
            );
            assert_eq!(
                row.as_ref(3),
                Some(&BinlogValue::Value(Value::Bytes(b"x".to_vec())))
            );
        }
    }

    #[test]
    fn mariadb_events_without_log_pos() {
        // MariaDB 11.4: events inside a transaction have log_pos 0, the last one is real.
        let mut t = Tracker::new(Flavor::MariaDb, &pos("mariadb-bin.000001", 1000));
        let g = maria_gtid(1032, 77);
        assert_eq!(size(&g), 32);
        match t.map_event(g).unwrap() {
            Item::Gtid { gtid } => assert_eq!(gtid, "0-7-77"),
            other => panic!("{other:?}"),
        }
        // CREATE … SELECT: statement first (exact position: no TABLE_MAP yet) …
        let q = query(0, "db", "CREATE TABLE `db`.`c` (`a` int)");
        let after_q = 1032 + u64::from(size(&q));
        assert_eq!(
            stmt_pos(t.map_event(q).unwrap()),
            pos("mariadb-bin.000001", after_q)
        );
        // … then rows; an ANNOTATE_ROWS may have been omitted before the TABLE_MAP …
        assert!(matches!(
            t.map_event(table_map(0, 5, "db", "c")).unwrap(),
            Item::TableMap {
                table_id: 5,
                columns: 1,
                ..
            }
        ));
        let rows = match t.map_event(write_rows(0, 5, -3)).unwrap() {
            Item::Rows {
                table_id: 5,
                kind: RowsKind::Insert,
                event,
            } => event,
            other => panic!("{other:?}"),
        };
        let decoded = t.rows(5, &rows).unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].0.is_none() && decoded[0].1.is_some());
        // … so a statement now gets the previous boundary (replaying the transaction is safe).
        let d = query(0, "db", "DROP TEMPORARY TABLE IF EXISTS `tmp`");
        assert_eq!(
            stmt_pos(t.map_event(d).unwrap()),
            pos("mariadb-bin.000001", after_q)
        );
        // The commit carries a real log_pos again.
        assert_eq!(
            commit_pos(t.map_event(xid(1500)).unwrap()),
            pos("mariadb-bin.000001", 1500)
        );
        assert_eq!(
            stmt_pos(t.map_event(query(1600, "db", "DROP TABLE x")).unwrap()),
            pos("mariadb-bin.000001", 1600)
        );
    }

    #[test]
    fn transaction_payload_positions() {
        let mut t = Tracker::new(Flavor::MySql, &pos("binlog.000001", 4));
        let inner = [
            query(0, "db", "BEGIN"),
            table_map(0, 9, "db", "t"),
            write_rows(0, 9, 1),
            xid(0),
        ];
        let total: u64 = inner.iter().map(|e| u64::from(size(e))).sum();
        assert!(matches!(
            t.map_event(payload(900, total, 20)).unwrap(),
            Item::Other
        ));
        let items: Vec<Item> = inner.into_iter().map(|e| t.map_event(e).unwrap()).collect();
        assert!(matches!(items[0], Item::Other));
        assert!(matches!(items[2], Item::Rows { table_id: 9, .. }));
        assert_eq!(
            commit_pos(items.into_iter().last().unwrap()),
            pos("binlog.000001", 900)
        );
        assert!(t.payload.is_none());
        assert_eq!(
            stmt_pos(t.map_event(query(1000, "db", "DROP TABLE t")).unwrap()),
            pos("binlog.000001", 1000)
        );

        // A payload that mysql_async failed to expand: the next file event is an error.
        let mut t = Tracker::new(Flavor::MySql, &pos("binlog.000001", 4));
        t.map_event(payload(900, 120, 20)).unwrap();
        let err = t.map_event(xid(1000)).unwrap_err();
        assert!(err.to_string().contains("not expanded"), "{err}");
    }

    #[test]
    fn rotation_and_table_map_retention() {
        let mut t = Tracker::new(Flavor::MySql, &pos("binlog.000001", 4));
        t.map_event(table_map(150, 3, "db", "t")).unwrap();
        let rows = match t.map_event(write_rows(180, 3, 7)).unwrap() {
            Item::Rows { event, .. } => event,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            commit_pos(t.map_event(xid(200)).unwrap()),
            pos("binlog.000001", 200)
        );
        t.map_event(rotate(4, "binlog.000002", 250, false, false))
            .unwrap();
        assert_eq!(t.pos, pos("binlog.000002", 4));
        assert!(t.rows(3, &rows).is_ok(), "table maps survive one rotation");
        assert_eq!(
            commit_pos(t.map_event(xid(300)).unwrap()),
            pos("binlog.000002", 300)
        );
        t.map_event(rotate(4, "binlog.000003", 350, false, false))
            .unwrap();
        let err = t.rows(3, &rows).unwrap_err();
        assert!(err.to_string().contains("no TABLE_MAP"), "{err}");
        // Heartbeats do not move the position.
        assert!(matches!(
            t.map_event(ev(ty::HEARTBEAT, 9999, b"binlog.000003"))
                .unwrap(),
            Item::Heartbeat
        ));
        assert_eq!(t.pos, pos("binlog.000003", 4));
    }

    #[test]
    fn unsupported_and_ignored_events() {
        let mut t = Tracker::new(Flavor::MariaDb, &pos("mariadb-bin.000001", 4));
        for et in 165..=171u8 {
            let err = t.map_event(ev(et, 100, &[0; 8])).unwrap_err();
            assert!(err.to_string().contains("log_bin_compress"), "{err}");
        }
        for et in [160u8, 161, 163, 164, ty::INCIDENT] {
            assert!(
                matches!(t.map_event(ev(et, 0, &[0; 4])).unwrap(), Item::Other),
                "type {et}"
            );
        }
        // Dummy rows event (table id 0x00ffffff, end of statement) is not a Rows item.
        assert!(matches!(
            t.map_event(write_rows(0, DUMMY_TABLE_ID, 0)).unwrap(),
            Item::Other
        ));
    }

    /// MariaDB `Partial_rows` fragment `seq` of `total` (the first one carries `len`).
    fn partial_rows(total: u32, seq: u32, len: Option<u64>, chunk: &[u8]) -> Event {
        let mut b = total.to_le_bytes().to_vec();
        b.extend_from_slice(&seq.to_le_bytes());
        b.push(u8::from(len.is_some()));
        if let Some(len) = len {
            b.extend_from_slice(&len.to_le_bytes());
        }
        b.extend_from_slice(chunk);
        ev(ty::MARIA_PARTIAL_ROWS, 0, &b)
    }

    #[test]
    fn mariadb_partial_rows_reassembled() {
        let mut t = Tracker::new(Flavor::MariaDb, &pos("mariadb-bin.000001", 1000));
        t.map_event(table_map(0, 5, "db", "c")).unwrap();
        // A WRITE_ROWS v1 event as the server fragments it: header and body, no checksum, but
        // the header's size counts one.
        let mut body = 5u64.to_le_bytes()[..6].to_vec();
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&[1, 0x01, 0x00]);
        body.extend_from_slice(&(-3i32).to_le_bytes());
        let mut inner = header(ty::WRITE_ROWS_V1, 7, 19 + body.len() + 4, 0, 0);
        inner.extend_from_slice(&body);
        let chunks: Vec<&[u8]> = inner.chunks(9).collect();
        let n = chunks.len() as u32;
        assert!(n >= 3);
        let mut at = t.pos.pos;
        for (i, c) in chunks.iter().enumerate() {
            let seq = i as u32 + 1;
            let frag = partial_rows(n, seq, (seq == 1).then_some(inner.len() as u64), c);
            at += u64::from(size(&frag));
            let item = t.map_event(frag).unwrap();
            assert_eq!(
                t.pos.pos, at,
                "fragments advance the position by their size"
            );
            if seq < n {
                assert!(matches!(item, Item::Other), "{item:?}");
                continue;
            }
            let Item::Rows {
                table_id: 5,
                kind: RowsKind::Insert,
                event,
            } = item
            else {
                panic!("{item:?}");
            };
            let rows = t.rows(5, &event).unwrap();
            assert_eq!(rows.len(), 1);
            assert!(rows[0].0.is_none());
            let after = rows[0].1.as_ref().unwrap();
            assert!(
                matches!(after.as_ref(0), Some(BinlogValue::Value(Value::Int(-3)))),
                "{after:?}"
            );
        }
        // A fragment that does not continue a rows event, and a length that does not add up.
        let err = t.map_event(partial_rows(3, 2, None, &[0; 4])).unwrap_err();
        assert!(err.to_string().contains("does not continue"), "{err}");
        t.map_event(partial_rows(2, 1, Some(100), &inner[..10]))
            .unwrap();
        let err = t
            .map_event(partial_rows(2, 2, None, &inner[10..]))
            .unwrap_err();
        assert!(err.to_string().contains("the rows event has 100"), "{err}");
    }

    #[test]
    fn status_statement_order_by_version() {
        let new = "SHOW BINARY LOG STATUS";
        let old = "SHOW MASTER STATUS";
        for (major, minor) in [(8, 0), (8, 1), (10, 11), (11, 4), (12, 3), (13, 0), (13, 2)] {
            assert_eq!(status_statements(major, minor)[0], old, "{major}.{minor}");
        }
        for (major, minor) in [(8, 2), (8, 4), (9, 0), (9, 7), (26, 7), (26, 10), (27, 1)] {
            assert_eq!(status_statements(major, minor)[0], new, "{major}.{minor}");
        }
    }

    #[test]
    fn crc32_known_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn requirement_messages() {
        assert!(requirement_problems(Flavor::MySql, "1", "ROW", "FULL", "", "").is_empty());
        assert!(requirement_problems(Flavor::MariaDb, "ON", "row", "full", "OFF", "").is_empty());
        let my = requirement_problems(Flavor::MySql, "0", "MIXED", "MINIMAL", "", "");
        assert_eq!(my.len(), 3, "{my:#?}");
        assert!(my[0].starts_with("log_bin is 0 (must be ON)") && my[0].contains("--log-bin"));
        assert!(
            my[1].contains("binlog_format is MIXED")
                && my[1].contains("SET PERSIST binlog_format='ROW'")
        );
        assert!(my[1].contains("--binlog-format=ROW"));
        assert!(
            my[2].contains("SET PERSIST binlog_row_image='FULL'")
                && my[2].contains("--binlog-row-image=FULL")
        );
        let ma = requirement_problems(Flavor::MariaDb, "", "STATEMENT", "NOBLOB", "ON", "innodb");
        assert_eq!(ma.len(), 5, "{ma:#?}");
        assert!(ma[0].starts_with("log_bin is NULL") && ma[0].contains("log_bin=ON"));
        assert!(ma[1].contains("SET GLOBAL binlog_format='ROW'") && ma[1].contains("my.cnf"));
        assert!(ma[2].contains("SET GLOBAL binlog_row_image='FULL'"));
        assert!(ma[3].contains("SET GLOBAL log_bin_compress=OFF"));
        assert!(
            ma[4].starts_with("binlog_storage_engine is innodb (must be unset")
                && ma[4].contains("remove binlog_storage_engine")
        );
    }

    /// The messages name lazyview by default, byte for byte as before [`set_product`] existed
    /// (lazyview never calls it, and no test does: it is set once per process).
    #[test]
    fn default_product_names_in_messages() {
        assert_eq!(product(), ("lazyview", "LAZYVIEW_"));
        let err = replica_opts("mysql://u:p@127.0.0.1/db?tls_ca=/no/such/ca.pem").unwrap_err();
        assert_eq!(
            err.to_string(),
            "TLS CA file `/no/such/ca.pem` not found (LAZYVIEW_TLS_CA / tls_ca DSN parameter)"
        );
        assert_eq!(
            unmet_requirements("8.4.11", &["a".into(), "b".into()]),
            "server 8.4.11 does not meet lazyview's binlog requirements:\n  - a\n  - b"
        );
        assert_eq!(
            requirement_problems(Flavor::MariaDb, "ON", "row", "full", "OFF", "innodb"),
            vec![
                "binlog_storage_engine is innodb (must be unset: a binlog kept in the storage engine \
                 is only served to replicas that start from a GTID, lazyview reads it by file and \
                 position); fix: remove binlog_storage_engine from my.cnf and restart"
                    .to_string()
            ]
        );
        let dup = mysql_async::Error::Server(mysql_async::ServerError {
            code: 1236,
            message: "A replica with the same server_uuid/server_id as this replica has connected \
                      to the source"
                .into(),
            state: "HY000".into(),
        });
        let at = Position {
            file: "binlog.000002".into(),
            pos: 4,
        };
        assert_eq!(
            BinlogReader::map_error(dup, &at, 77).to_string(),
            "another replica connected with the same server_id 77 and the server dropped this \
             stream (at binlog.000002:4); give every lazyview instance and every real replica a \
             unique server_id (LAZYVIEW_SERVER_ID)"
        );
    }

    #[test]
    fn privilege_and_duplicate_id_detection() {
        let se = |code, msg: &str| mysql_async::ServerError {
            code,
            message: msg.into(),
            state: "HY000".into(),
        };
        let wrap = |e: mysql_async::ServerError| {
            anyhow::Error::new(mysql_async::Error::Server(e)).context("outer")
        };
        // as observed on the test servers
        let denied = [
            se(1045, "Access denied for user 'u'@'%' (using password: YES)"),
            se(1227, "Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation"),
            se(1227, "Access denied; you need (at least one of) the BINLOG MONITOR privilege(s) for this operation"),
            se(1044, "Access denied for user 'u'@'%' to database 'x'"),
            se(1142, "SELECT command denied to user 'u'@'localhost' for table 't'"),
            se(1370, "execute command denied to user 'u'@'%' for routine 'p'"),
        ];
        for e in denied {
            let err = wrap(e);
            assert!(is_access_denied(&err), "{err:#}");
            assert!(!is_duplicate_server_id(&err) && !is_missing_binlog(&err));
        }
        assert!(with_hint(wrap(se(1227, "… BINLOG MONITOR …")))
            .to_string()
            .contains("GRANT BINLOG MONITOR"));
        assert!(with_hint(wrap(se(1227, "… REPLICATION CLIENT …")))
            .to_string()
            .contains("GRANT REPLICATION CLIENT"));
        assert!(is_access_denied(&anyhow!("x").context(
            "the account needs the REPLICATION SLAVE privilege (…)"
        )));
        let dups = [
            se(1236, "A replica with the same server_uuid/server_id as this replica has connected to the source; the first event 'binlog.000009' at 4"),
            se(1236, "A slave with the same server_uuid/server_id as this slave has connected to the master"),
            se(4052, "A slave with the same server_uuid/server_id is already connected; the first event 'mariadb-bin.000010' at 4"),
        ];
        for e in dups {
            let err = wrap(e);
            assert!(is_duplicate_server_id(&err), "{err:#}");
            assert!(!is_access_denied(&err) && !is_missing_binlog(&err));
        }
        let missing = wrap(se(
            1236,
            "Could not find first log file name in binary log index file",
        ));
        assert!(!is_duplicate_server_id(&missing) && !is_access_denied(&missing));
        assert!(!is_access_denied(&wrap(se(
            1064,
            "You have an error in your SQL syntax"
        ))));
    }

    #[test]
    fn flavor_detection() {
        assert_eq!(
            Flavor::from_version("10.11.14-MariaDB-log"),
            Flavor::MariaDb
        );
        assert_eq!(Flavor::from_version("13.2.0-MariaDB-log"), Flavor::MariaDb);
        assert_eq!(Flavor::from_version("8.0.43-34"), Flavor::MySql);
        assert_eq!(Flavor::from_version("8.4.6"), Flavor::MySql);
        assert_eq!(Flavor::from_version("9.7.2-2"), Flavor::MySql);
        assert_eq!(Flavor::from_version("26.7.0"), Flavor::MySql);
    }
}

#[cfg(test)]
mod ca_tests {
    use super::*;

    #[test]
    fn ca_params_split_and_applied() {
        let (u, c) = split_ca_params(
            "mysql://u:p@h/d?require_ssl=true&tls_ca=%2Ftmp%2Fa.pem,%2Fb.pem&verify_ca=true",
        );
        assert_eq!(u, "mysql://u:p@h/d?require_ssl=true&verify_ca=true");
        assert_eq!(c, vec!["/tmp/a.pem", "/b.pem"]);
        assert_eq!(split_ca_params("mysql://h/d").1.len(), 0);
        assert!(replica_opts("mysql://h/d?ssl-ca=/definitely/missing.pem").is_err());
        let ca = std::env::current_dir().unwrap().join("Cargo.toml");
        let o = replica_opts(&format!(
            "mysql://h/d?require_ssl=true&tls_ca={}",
            ca.display()
        ))
        .unwrap();
        assert_eq!(o.ssl_opts().unwrap().root_certs().len(), 1);
    }
}
