//! The flavor-neutral side of a replication source: where it is (`Position`), what it is
//! (`Flavor`, `SourceCaps`, `ServerInfo`), what its stream yielded (`Item`, `RowsKind`) and what
//! an error from it means (`MissingBinlog`, `is_missing_binlog`, `is_access_denied`,
//! `is_duplicate_server_id`).
//!
//! These types name the source, not the wire protocol, so they live here rather than in
//! [`crate::binlog`]: the reader, the engine and the catalog all speak them, and only `binlog`
//! talks `mysql_async`. Flavor-specific code joins them in this module as it leaves `binlog`.
//!
//! Copy set: `std` + `anyhow` + `mysql_async` only, no `crate::` beyond the copy set, no
//! environment variables (a knob is a parameter the caller passes), no logging.

use std::cmp::Ordering;

use mysql_async::binlog::events::{BinlogEventHeader, Event};

pub(crate) mod maria;

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

    /// The capabilities of this flavor, detected once at connect: callers read a flag here
    /// instead of matching on the flavor or threading a `mariadb` bool through the call chain.
    pub fn detect(self) -> SourceCaps {
        SourceCaps {
            fresh_snapshot: self == Flavor::MariaDb,
            json_cast: self == Flavor::MySql,
            sql_generation_expressions: self == Flavor::MariaDb,
        }
    }
}

/// What this source can do, chosen from its [`Flavor`] once at connect. A `Copy` value the
/// engine holds beside the flavor, so no per-call flavor compare reaches the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceCaps {
    /// Source reads must be prepared for this execution instead of reused from the
    /// prepared-statement cache: MariaDB's cache keeps answering from the old plan after a DDL.
    pub fresh_snapshot: bool,
    /// The server accepts `CAST(? AS JSON)` as a parameter cast (MariaDB has no JSON cast type).
    pub json_cast: bool,
    /// `GENERATION_EXPRESSION` comes back as plain SQL (MariaDB) rather than string-escaped
    /// the way MySQL / Percona print it.
    pub sql_generation_expressions: bool,
}

impl From<bool> for SourceCaps {
    /// The legacy `mariadb: bool` spelling (`true`: MariaDB), kept so callers built before
    /// [`SourceCaps`] still compile; new callers pass the caps chosen at connect.
    fn from(mariadb: bool) -> SourceCaps {
        (if mariadb {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        })
        .detect()
    }
}

/// What one event whose header carries `log_pos = 0` does to the tracked file offset: the
/// value [`crate::binlog::stream`]'s `Tracker::advance` gets from [`position_rule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// The event is not part of the file layout: the position does not move.
    Keep,
    /// The event is contiguous in the file: add `size` to the position; `exact` = false means
    /// the offset stops being trustworthy as a resume point inside a transaction from here on
    /// (an event with a real `log_pos` trusts it again).
    Advance { size: u64, exact: bool },
}

/// The one place the per-flavor `log_pos = 0` position rule lives: `Tracker::advance` calls
/// this for such an event, after excluding the FORMAT_DESCRIPTION re-send and ROTATE (whose
/// zero `log_pos` never means "inside a transaction"). MySQL's arm is trivial — its
/// `log_pos = 0` events are the decompressed inner events of a `TRANSACTION_PAYLOAD`, which
/// `advance` consumes before the hook. MariaDB's rule lives in
/// [`maria::position_rule`](maria::position_rule).
pub(crate) fn position_rule(flavor: Flavor, header: BinlogEventHeader) -> Step {
    match flavor {
        Flavor::MySql => Step::Keep,
        Flavor::MariaDb => maria::position_rule(header),
    }
}

#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub flavor: Flavor,
    pub version: String,
}

/// One decoded stream item. Row data stays undecoded until
/// [`BinlogReader::rows`](crate::binlog::BinlogReader::rows) is called, so
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
    /// The source reports possibly lost changes (an `INCIDENT` event: it could not log the rows
    /// of a statement, e.g. an unsafe statement written in row format). The copy set only reads
    /// it; the caller logs it. `pos` is the position right after the event, not a boundary: the
    /// transaction continues.
    Incident {
        pos: Position,
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

pub(crate) fn missing_binlog_message(e: &mysql_async::ServerError) -> bool {
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

/// Server error codes meaning "not allowed": ER_DBACCESS_DENIED_ERROR (1044),
/// ER_ACCESS_DENIED_ERROR (1045; also what COM_REGISTER_SLAVE answers without REPLICATION SLAVE),
/// ER_TABLEACCESS_DENIED_ERROR (1142), ER_SPECIFIC_ACCESS_DENIED_ERROR (1227),
/// ER_PROCACCESS_DENIED_ERROR (1370).
pub(crate) const ACCESS_DENIED_CODES: [u16; 5] = [1044, 1045, 1142, 1227, 1370];

pub(crate) fn server_errors(
    err: &anyhow::Error,
) -> impl Iterator<Item = &mysql_async::ServerError> {
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

pub(crate) fn is_duplicate_server_id_error(se: &mysql_async::ServerError) -> bool {
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
