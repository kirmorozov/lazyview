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
//!
//! Host code stays out of this directory entirely (decision D5): nothing under `src/source/` may
//! use the host's own modules (`target`, `mv`, `engine`, `meta`, `supervisor`), nor *declare* a
//! module that does — a `mod` declaration is an import too, and an embedder copying these files
//! verbatim would not compile. Host-side PostgreSQL source code lives in `pgsrc`, declared from
//! `src/lib.rs`.

/// The MySQL-family side (MySQL and Percona run the same server): preflight, the status
/// statement spelling, GTID text, transaction payloads, the QUERY/XA classifications and the
/// TIME(1)/(2) decode twin. MariaDB reaches what it shares through it, never by copy, and
/// Percona reaches all of it through `percona.rs`.
pub mod mysql;

use std::cmp::Ordering;

use mysql_async::binlog::events::{BinlogEventHeader, Event};

pub(crate) mod maria;
pub(crate) mod percona;

/// The binlog coordinate a stream read hands back: `pos` is the offset of the next event to read.
/// It is the *wire* shape, known inside the source layer and to whoever opens the stream; what
/// the engine and the metadata tables hold is [`Cursor`], which says nothing about files.
///
/// Embedder seam: the fields, `Ord` and `Display` are read from outside this crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Position {
    pub file: String,
    pub pos: u64,
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        // Embedder seam: the binlog-side ordering, by the MySQL-family file-sequence rule that
        // [`mysql::position_key`] owns. The generic side orders [`Cursor`]s, whose rule is the
        // flavor's.
        mysql::position_key(self).cmp(&mysql::position_key(other))
    }
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)
    }
}

/// Where the source has got to, as everything outside the flavor modules knows it: an **opaque,
/// ordered piece of text**. The engine compares, logs and persists this; the metadata tables
/// store the [`Display`] text. What is behind it — a binlog file and offset for the MySQL family,
/// the same pair for MariaDB too (its offset restarts at every file, so its rule orders by the
/// file's place in the index as well), later an LSN or xid for PostgreSQL — is the flavor's
/// business: [`mysql::cursor_key`] and [`maria::cursor_key`] hold the ordering rules,
/// [`mysql::parse`] the spelling, and the 4-byte file-header floor that made a raw offset of a
/// fresh file meaningless (`pos.max(4)`) is [`mysql::floor_offset`]. The generic side gets
/// ordering, equality, `Display` and a parse — never a file name or an offset.
#[derive(Debug, Clone)]
pub struct Cursor {
    flavor: Flavor,
    at: Position,
}

impl Cursor {
    /// The cursor of a binlog coordinate, under `flavor`'s rules (a real file never starts
    /// before its 4-byte header).
    pub fn from_position(flavor: Flavor, at: &Position) -> Cursor {
        Cursor {
            flavor,
            at: match flavor {
                Flavor::MySql | Flavor::Percona => mysql::normalize(at),
                Flavor::MariaDb => maria::normalize(at),
            },
        }
    }

    /// The cursor of a file and offset read from the source's own words (a status variable, a
    /// row of the metadata tables).
    pub fn from_parts(flavor: Flavor, file: String, pos: u64) -> Cursor {
        Cursor::from_position(flavor, &Position { file, pos })
    }

    /// No position yet: the source was never read (an empty file is the one spelling of it).
    pub fn none(flavor: Flavor) -> Cursor {
        Cursor::from_parts(flavor, String::new(), 0)
    }

    /// Whether this is a position at all — the shape `!file.is_empty()` had before the cursor.
    pub fn is_some(&self) -> bool {
        !self.at.file.is_empty()
    }

    /// The binlog coordinate to open a stream from / to answer the server's own questions with.
    pub fn to_position(&self) -> Position {
        self.at.clone()
    }
}

impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> Ordering {
        // One engine has one source, so the flavors match in practice; the flavor first and the
        // pair last keep `Ord` total and consistent with `Eq` when they do not.
        let key = |c: &Cursor| match c.flavor {
            Flavor::MySql | Flavor::Percona => mysql::cursor_key(&c.at),
            Flavor::MariaDb => maria::cursor_key(&c.at),
        };
        (self.flavor as u8, key(self), &self.at.file, self.at.pos).cmp(&(
            other.flavor as u8,
            key(other),
            &other.at.file,
            other.at.pos,
        ))
    }
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        self.flavor == other.flavor && self.at == other.at
    }
}

impl Eq for Cursor {}

impl std::hash::Hash for Cursor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.flavor as u8, &self.at.file, self.at.pos).hash(state);
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The text the metadata tables hold; identical in shape to the position it replaces, so
        // logs and the saved position read the same across the format bump.
        write!(f, "{}:{}", self.at.file, self.at.pos)
    }
}

/// Whether the source still serves `cursor` — the question `binlog_exists(file)` asked when the
/// policy was keyed on a file name. Per flavor: the MySQL family asks its own log index, MariaDB
/// reaches the same code through `mysql.rs` (never a copy). A cursor the source no longer serves
/// is a lost slot: the caller's `on_missing_binlog` policy applies to it, and
/// [`SourceCaps::cursor_purgeable`] says whether losing one this way is something the source does
/// on its own. A failed question is an error, never a `false`.
pub async fn can_resume(conn: &mut mysql_async::Conn, cursor: &Cursor) -> anyhow::Result<bool> {
    let at = cursor.to_position();
    match cursor.flavor {
        Flavor::MySql | Flavor::Percona => mysql::can_resume(conn, &at).await,
        Flavor::MariaDb => maria::can_resume(conn, &at).await,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    MySql,
    MariaDb,
    /// Percona Server: the MySQL server's code, so every answer here is MySQL's and
    /// `percona.rs` only forwards to [`mysql`]. It is its own flavor so that the product name and
    /// whatever Percona starts doing differently have a file of their own (D1 row 1).
    Percona,
}

impl Flavor {
    /// Flavor from a `VERSION()` string (`10.11.14-MariaDB-log` → MariaDB). Percona's
    /// `VERSION()` is indistinguishable from MySQL's, so it takes [`from_version_comment`].
    ///
    /// [`from_version_comment`]: Flavor::from_version_comment
    pub fn from_version(version: &str) -> Flavor {
        if version.to_ascii_lowercase().contains("mariadb") {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        }
    }

    /// Flavor from `VERSION()` plus `@@GLOBAL.version_comment`: the same answer as
    /// [`from_version`], except that a MySQL-looking server whose comment names Percona
    /// (`Percona Server (GPL), Release …`) is [`Flavor::Percona`] — read by that flavor itself
    /// (`percona::is_this`). A caller that has no comment passes `""`.
    ///
    /// [`from_version`]: Flavor::from_version
    pub fn from_version_comment(version: &str, version_comment: &str) -> Flavor {
        let flavor = Self::from_version(version);
        if flavor == Flavor::MySql && percona::is_this(version_comment) {
            Flavor::Percona
        } else {
            flavor
        }
    }

    /// Whether this flavor runs the MySQL server's code, so the MySQL-family answers in
    /// [`mysql`] (and `crate::fence`, which only it calls) are its answers too.
    pub fn is_mysql_family(self) -> bool {
        matches!(self, Flavor::MySql | Flavor::Percona)
    }

    /// The capabilities of this flavor, detected once at connect: callers read a flag here
    /// instead of matching on the flavor or threading a `mariadb` bool through the call chain.
    pub fn detect(self) -> SourceCaps {
        SourceCaps {
            fresh_snapshot: self == Flavor::MariaDb,
            json_cast: self.is_mysql_family(),
            sql_generation_expressions: self == Flavor::MariaDb,
            cursor_purgeable: true,
        }
    }

    /// The cursor of the text the metadata tables hold (`file:pos`), or `None` when it is not a
    /// cursor of this flavor. The one way back from text: a cursor that does not parse is an
    /// error the caller raises, not a position to try.
    pub fn parse_cursor(self, text: &str) -> Option<Cursor> {
        let at = match self {
            Flavor::MySql | Flavor::Percona => mysql::parse(text)?,
            Flavor::MariaDb => maria::parse(text)?,
        };
        Some(Cursor::from_position(self, &at))
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
    /// The source can drop a resume point on its own (the MySQL family purges binlog files, a
    /// PostgreSQL slot loses WAL past its retain window), so a cursor [`can_resume`] no longer
    /// answers for is an expected event, not a broken cursor: `Config::on_missing_binlog` is
    /// what decides what a lost slot costs.
    pub cursor_purgeable: bool,
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

/// Why the source refuses an incremental plan that reads `referenced` columns of `table` (the
/// per-flavor planning rule, asked once per plan by the engine), or `None`. `version` is the
/// server's `VERSION()`. MariaDB's rule lives in [`maria::plan_refusal`].
pub(crate) fn plan_refusal(
    flavor: Flavor,
    version: &str,
    table: &crate::catalog::TableInfo,
    referenced: &[bool],
) -> Option<String> {
    match flavor {
        Flavor::MariaDb => maria::plan_refusal(version, table, referenced),
        Flavor::MySql | Flavor::Percona => None,
    }
}

/// The one place the per-flavor `log_pos = 0` position rule lives: `Tracker::advance` calls
/// this for such an event, after excluding the FORMAT_DESCRIPTION re-send and ROTATE (whose
/// zero `log_pos` never means "inside a transaction"). The MySQL family's arm is trivial — its
/// `log_pos = 0` events are the decompressed inner events of a `TRANSACTION_PAYLOAD`, which
/// `advance` consumes before the hook. MariaDB's rule lives in
/// [`maria::position_rule`](maria::position_rule).
pub(crate) fn position_rule(flavor: Flavor, header: BinlogEventHeader) -> Step {
    match flavor {
        Flavor::MySql | Flavor::Percona => Step::Keep,
        Flavor::MariaDb => maria::position_rule(header),
    }
}

/// The verdict of a visibility-fence wait (the engine's `fence`, per flavor). Binlog events ship before the
/// committing transaction ends, so the engine must not read the sources until the transactions
/// it read from the binlog are visible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FenceVerdict {
    /// Every transaction read up to the boundary is visible: read the sources now.
    Exact,
    /// The wait timed out: the last transaction is not visible yet, read the binlog again.
    NotYet,
    /// No fence is available on this server: the caller waits its fallback delay, which is
    /// not exact under load.
    #[default]
    Unknown,
}

/// The caches the commit-order seam reads and updates. The engine owns them (they lived there before the
/// source seam); the source only asks the server what a cache has not settled yet.
#[derive(Debug, Clone, Default)]
pub(crate) struct FenceState {
    /// `Some(true)`: the server reports the binlog position of a consistent snapshot;
    /// `Some(false)`: it does not (the wait is skipped); `None`: not tried on this connection.
    pub(crate) snapshot: Option<bool>,
    /// `Some(true)`: the server commits in binlog order and the fence table exists;
    /// `Some(false)`: the commit-order fence is ruled out; `None`: not probed on this connection.
    pub(crate) commit_order: Option<bool>,
    /// The boundary the last successful commit-order fence write covered: no new event since,
    /// no new write.
    pub(crate) committed: Option<Position>,
}

/// What the commit-order seam ([`mysql::commit_order`]) reports for the engine to log (the
/// copy set does not log) once per process.
#[derive(Debug, Default)]
pub(crate) struct FenceReport {
    /// A WARN to log once per process (the fence ruled out; the engine's `warn_fence`).
    pub(crate) fence_warn: Option<String>,
}

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
