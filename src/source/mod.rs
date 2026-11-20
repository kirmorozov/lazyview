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
///
/// A cursor can also carry the GTID state of the source it came from ([`Cursor::gtid`]): the
/// canonical set text the flavor module has consumed, appended to the text as `[g=…]` (§ the
/// resume-by-GTID plan). It is *carried*, never ordered or compared — the ordering coordinate
/// stays `file:pos` — so the `[g=…]` part is what lets a resume request be answered on another
/// server whose `file:pos` means nothing here. It is `None` on every cursor a non-GTID source
/// produces, so the text is byte-for-byte the pre-GTID `file:pos`.
#[derive(Debug, Clone)]
pub struct Cursor {
    flavor: Flavor,
    at: Position,
    gtid: Option<String>,
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
            gtid: None,
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

    /// The GTID state this cursor carries, as the flavor's canonical set text (MySQL/Percona:
    /// `uuid:interval,…`; MariaDB: a `domain-server-seq` list) — the transactions the reader has
    /// consumed, which is what a resume on another server is checked against. `None` on a cursor
    /// from a source that reports no GTIDs, and on every cursor built before a GTID source put
    /// one here. Ordering and equality ignore it (see [`Cursor`]).
    pub fn gtid(&self) -> Option<&str> {
        self.gtid.as_deref()
    }

    /// The same cursor now carrying `gtid` — the set a GTID dump request starts from. Additive
    /// copy-set API (PR A); the set is the flavor's to canonicalize and the engine's to keep
    /// merged (it never reaches the per-event path, only the cursor a stream is opened from and
    /// the status row it is saved to).
    pub fn with_gtid(mut self, gtid: Option<String>) -> Cursor {
        self.gtid = gtid;
        self
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
        // logs and the saved position read the same across the format bump. The optional `[g=…]`
        // part carries the GTID state where the source has it (flavor-owned grammar, parsed back
        // by the flavor's `parse`); its absence is byte-for-byte the pre-GTID text.
        match self.gtid.as_deref().filter(|g| !g.is_empty()) {
            Some(g) => write!(f, "{}:{}[g={}]", self.at.file, self.at.pos, g),
            None => write!(f, "{}:{}", self.at.file, self.at.pos),
        }
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

/// Whether the source on `conn` has executed **every** transaction named by `saved`'s GTID set —
/// the subset question a resume on another server asks before it trusts a cursor whose `file:pos`
/// is meaningless there (`saved ⊆ source.gtid_executed`, the safety bar of the resume-by-GTID
/// plan). A saved cursor with no GTID set is the upgrade case and covers trivially: there is
/// nothing to check, so the file/pos and identity rules answer for it. Per flavor the read is the
/// server's own words and the grammar is the flavor's: MySQL/Percona compare against
/// `@@GLOBAL.gtid_executed`, MariaDB against `@@GLOBAL.gtid_binlog_state`; the interval math is
/// shared (MariaDB reaches it through `mysql.rs`, never a copy). A failed question is an error,
/// never a silent `false`.
pub async fn gtid_covers(conn: &mut mysql_async::Conn, saved: &Cursor) -> anyhow::Result<bool> {
    match saved.flavor {
        Flavor::MySql | Flavor::Percona => mysql::gtid_covers(conn, saved.gtid.as_deref()).await,
        Flavor::MariaDb => maria::gtid_covers(conn, saved.gtid.as_deref()).await,
    }
}

/// The GTID state a source reports about itself, read once at connect (the network edge, in
/// [`crate::binlog`]) and interpreted by the flavor modules. `None` means the server has no such
/// variable, not that it is empty: MariaDB has no `gtid_mode`, MySQL/Percona no
/// `gtid_binlog_state`; `gtid_purged` exists on both. `executed` (MySQL) and `binlog_state`
/// (MariaDB) are what a resume's [`gtid_covers`] compares against; `purged` is the pre-check that
/// a server has not thrown away the transactions the saved cursor still wants, and `mode` says
/// whether GTIDs are on at all (`gtid_covers` on a `gtid_mode=OFF` MySQL sees an empty
/// `executed` and answers `false`, the clean stop).
#[derive(Debug, Clone, Default)]
pub struct GtidState {
    /// `@@GLOBAL.gtid_mode` (MySQL/Percona): `ON` / `OFF` / `ON_PERMISSIVE` …; `None` on MariaDB.
    pub mode: Option<String>,
    /// `@@GLOBAL.gtid_executed` (MySQL/Percona): the canonical `uuid:interval,…` set consumed.
    pub executed: Option<String>,
    /// `@@GLOBAL.gtid_purged`: the set the server no longer has in its binlog (both flavors).
    pub purged: Option<String>,
    /// `@@GLOBAL.gtid_binlog_state` (MariaDB): the `domain-server-seq` list written to the binlog.
    pub binlog_state: Option<String>,
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

    /// Whether the binlog position this flavor reports for a consistent snapshot
    /// (`maria::snapshot_position`) is a watermark of *committed* transactions: waiting for it to
    /// reach the position the engine read from the binlog then proves every transaction up to
    /// there is visible to a new reader. The flavor answers, because that is what its server code
    /// does with the position; the engine asks it instead of branching on product names.
    ///
    /// MariaDB's pair is a watermark. The MySQL family's is not: what its `binlog_snapshot_%`
    /// status variables report is the raw binlog write position, which has already passed
    /// transactions still in flight, and the snapshot taken in the same moment cannot read them
    /// (measured on Percona Server 8.4: `Binlog_snapshot_position` had advanced past an in-flight
    /// update whose row that snapshot still showed as the old value). Waiting on such a position
    /// approves the read of stale rows, so a flavor that answers `false` is fenced by the GTID
    /// wait when it has GTIDs and otherwise by the commit-order row (`mysql::commit_order`).
    /// Oracle Community answers `false` and reports no pair at all, which rules the snapshot read
    /// out by itself.
    pub fn snapshot_position_is_watermark(self) -> bool {
        match self {
            Flavor::MariaDb => maria::snapshot_position_is_watermark(),
            Flavor::MySql => mysql::snapshot_position_is_watermark(),
            Flavor::Percona => percona::snapshot_position_is_watermark(),
        }
    }

    /// The capabilities of this flavor, detected once at connect: callers read a flag here
    /// instead of matching on the flavor or threading a `mariadb` bool through the call chain.
    pub fn detect(self) -> SourceCaps {
        SourceCaps {
            fresh_snapshot: self == Flavor::MariaDb,
            json_cast: self.is_mysql_family(),
            sql_generation_expressions: self == Flavor::MariaDb,
            cursor_purgeable: true,
            // Never assumed from the flavor: a source has GTIDs only once the connect probe
            // (`crate::binlog::gtid_state`) reports the server runs with them, at which point the
            // caller sets this to `true`. Default `false` is the safe reading — nothing resumes by
            // GTID on evidence the server never gave (the plan's safety bar).
            gtid: false,
        }
    }

    /// The cursor of the text the metadata tables hold (`file:pos`), or `None` when it is not a
    /// cursor of this flavor. The one way back from text: a cursor that does not parse is an
    /// error the caller raises, not a position to try.
    pub fn parse_cursor(self, text: &str) -> Option<Cursor> {
        let (at, gtid) = match self {
            Flavor::MySql | Flavor::Percona => mysql::parse(text)?,
            Flavor::MariaDb => maria::parse(text)?,
        };
        Some(Cursor::from_position(self, &at).with_gtid(gtid))
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
    /// The source runs with GTIDs, so [`gtid_covers`] is the resume question and a cursor may
    /// carry a `[g=…]` set. Set from the connect probe ([`crate::binlog::gtid_state`], which
    /// reads `gtid_mode` on the MySQL family and `gtid_binlog_state` on MariaDB), not from the
    /// flavor: `gtid_mode=OFF` keeps the single-server file/pos mode (the plan's Q-O2 default).
    pub gtid: bool,
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

/// A binlog event this build cannot map (review A-02): the bytes arrived and the decoder refused
/// them, so reading again from the saved position fails on the same bytes until a fixed build
/// runs. That is the difference from a source that is unreachable or has lost the file
/// ([`MissingBinlog`]) — one waits for the source, the other waits for a deploy, and only the
/// first is a network fault. The owner's ruling (2026-11-16) keeps a decode failure that exits
/// the process fatal, so the fields below are what the one ERROR line has to name: kind, table,
/// position.
#[derive(Debug)]
pub struct Undecodable {
    /// The event kind with its raw code, e.g. `rows(UPDATE_ROWS_V1)(31)` or `TABLE_MAP(19)`.
    pub kind: String,
    /// The table the event names when one is known: `schema.table` when a TABLE_MAP was seen for
    /// the id, `table id <n>` when it was not — the second is the commonest reason a rows event
    /// cannot be decoded. `None` for a failure to map the event itself, where no table is named
    /// by the bytes at all.
    pub table: Option<String>,
    /// Where the event sits in the binlog file: the position the reader keeps coming back to.
    pub position: Position,
    /// What the decoder said.
    detail: String,
}

impl Undecodable {
    /// Tags an error of the mapper with what the event was. Called only where an event failed,
    /// never on the hot path; the message of `e` is kept whole in [`Undecodable::detail`].
    pub(crate) fn tag(
        e: anyhow::Error,
        kind: String,
        table: Option<String>,
        at: &Position,
    ) -> anyhow::Error {
        anyhow::Error::new(Self {
            kind,
            table,
            position: at.clone(),
            detail: format!("{e:#}"),
        })
    }
}

impl std::fmt::Display for Undecodable {
    /// The one line an operator of a crashing instance gets (the owner's ruling of 2026-11-16
    /// keeps a decode failure fatal): kind, position, table when the bytes name one, and what the
    /// decoder said. The engine logs it as it is, so no prefix of its own repeats these words.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.table {
            Some(t) => write!(
                f,
                "cannot decode {} at {}: table {}, {}",
                self.kind, self.position, t, self.detail
            ),
            None => write!(
                f,
                "cannot decode {} at {}: {}",
                self.kind, self.position, self.detail
            ),
        }
    }
}

impl std::error::Error for Undecodable {}

/// True when the stream failed because an event cannot be decoded rather than because the source
/// went away (A-02), so the caller backs off instead of reconnecting on the network schedule.
/// Two sources, both the decoder's own: an error the mapper tagged with [`Undecodable`], and a
/// dump packet that is not a readable event at all — the reader reports those as an `io::Error`
/// of the invalid-data class. A truncated read arrives as `UnexpectedEof`, which [`is_transient`]
/// owns: a cut connection is not a decoder bug.
pub fn is_undecodable(e: &anyhow::Error) -> bool {
    fn undecodable_io(io: &std::io::Error) -> bool {
        matches!(
            io.kind(),
            std::io::ErrorKind::InvalidData
                | std::io::ErrorKind::InvalidInput
                | std::io::ErrorKind::Unsupported
        )
    }
    e.chain().any(|c| {
        c.downcast_ref::<Undecodable>().is_some()
            || match c.downcast_ref::<mysql_async::Error>() {
                Some(mysql_async::Error::Io(mysql_async::IoError::Io(io))) => undecodable_io(io),
                _ => false,
            }
            || c.downcast_ref::<std::io::Error>()
                .is_some_and(undecodable_io)
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{maria, mysql, Cursor, Flavor};

    /// parse → cursor → `Display`: the text the metadata tables would hold again.
    fn round_trip(flavor: Flavor, text: &str) -> String {
        flavor.parse_cursor(text).expect("parses").to_string()
    }

    #[test]
    fn cursor_text_round_trips_pre_gtid() {
        // Every pre-GTID `file:pos` text is byte-for-byte unchanged through parse + Display.
        assert_eq!(
            round_trip(Flavor::MySql, "binlog.000012:1234"),
            "binlog.000012:1234"
        );
        assert_eq!(
            round_trip(Flavor::Percona, "binlog.000003:4"),
            "binlog.000003:4"
        );
        assert_eq!(
            round_trip(Flavor::MariaDb, "mariadb-bin.000003:42"),
            "mariadb-bin.000003:42"
        );
    }

    #[test]
    fn cursor_text_round_trips_with_gtid() {
        // The bracketed set survives verbatim — its own colons (uuid:1-5) never disturb the
        // file:pos split that precedes it.
        let mysql = "binlog.000012:1234[g=3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:10,c80aa942-1111-2222-3333-c80aa9429562:3]";
        assert_eq!(round_trip(Flavor::MySql, mysql), mysql);
        let maria = "mariadb-bin.000003:42[g=0-1-100,1-2-50-60]";
        assert_eq!(round_trip(Flavor::MariaDb, maria), maria);
    }

    #[test]
    fn cursor_carries_gtid_apart_from_ordering() {
        // The set is carried, not ordered: two cursors at the same file:pos are equal and tie,
        // whatever set one of them carries (the ordering coordinate stays file:pos).
        let a = Cursor::from_parts(Flavor::MySql, "binlog.000001".into(), 100);
        let b = a.clone().with_gtid(Some("u:1-5".into()));
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a.gtid(), None);
        assert_eq!(b.gtid(), Some("u:1-5"));
        assert_eq!(b.to_string(), "binlog.000001:100[g=u:1-5]");
        // An empty set is not written: the bare `file:pos` is kept.
        assert_eq!(
            a.clone().with_gtid(Some(String::new())).to_string(),
            "binlog.000001:100"
        );
    }

    #[test]
    fn gtid_subset_table() {
        let s = |t: &str| mysql::parse_gtid_set(t);
        // empty saved set is a subset of anything
        assert!(mysql::gtid_subset(&s(""), &s("u:1-5")));
        // range within a range
        assert!(mysql::gtid_subset(&s("u:1-5"), &s("u:1-10")));
        // a gap (4,5) the source does not cover
        assert!(!mysql::gtid_subset(&s("u:1-5"), &s("u:1-3:6-9")));
        // two adjacent intervals cover the range
        assert!(mysql::gtid_subset(&s("u:1-5"), &s("u:1-3:4-6")));
        // single-number interval equals itself
        assert!(mysql::gtid_subset(&s("u:7"), &s("u:7")));
        assert!(!mysql::gtid_subset(&s("u:8"), &s("u:7")));
        // multi-sid: a missing sid breaks it, an extra sid in the source does not
        assert!(!mysql::gtid_subset(&s("u1:1-5,u2:1-2"), &s("u1:1-10")));
        assert!(mysql::gtid_subset(
            &s("u1:1-5,u2:1-2"),
            &s("u1:1-10,u2:1-3")
        ));
        assert!(mysql::gtid_subset(&s("u1:1-5"), &s("u1:1-10,u9:1")));
        // sid is matched case-insensitively (the server prints uuids in upper case)
        assert!(mysql::gtid_subset(&s("U:1-5"), &s("u:1-5")));
        // the same uuid recurring across groups merges its intervals
        assert!(mysql::gtid_subset(&s("u:4-5"), &s("u:1-2,u:3-4,u:5")));
    }

    #[test]
    fn gtid_subset_mariadb_lists() {
        let s = maria::parse_gtid_set;
        // per (domain, server) coverage of a `domain-server-seq` list
        assert!(mysql::gtid_subset(&s("0-1-100"), &s("0-1-100")));
        assert!(mysql::gtid_subset(&s("0-1-50"), &s("0-1-100")));
        assert!(!mysql::gtid_subset(&s("0-1-101"), &s("0-1-100")));
        // An explicit window `d-s-lo-hi` is covered by a high-water mark at or past its end; the
        // single-seq form is a high-water mark (transactions 1..=seq), not a bare point.
        assert!(mysql::gtid_subset(&s("0-1-40-60"), &s("0-1-100")));
        assert!(!mysql::gtid_subset(&s("0-1-40-110"), &s("0-1-100")));
        // a different server is a different key
        assert!(!mysql::gtid_subset(&s("0-2-50"), &s("0-1-100")));
        // multiple domains must each be covered
        assert!(mysql::gtid_subset(
            &s("0-1-50,1-2-10"),
            &s("0-1-100,1-2-10")
        ));
        assert!(!mysql::gtid_subset(
            &s("0-1-50,1-2-11"),
            &s("0-1-100,1-2-10")
        ));
        // empty saved set
        assert!(mysql::gtid_subset(&s(""), &s("0-1-100")));
    }

    /// PR C: the `[g=…]` text → the list `SET @slave_connect_state` takes. One high-water mark
    /// per `domain-server` pair (what the server reads from a list), intervals merged first, and
    /// nothing usable is an error — never an empty list, which the server would answer with a
    /// dump from its oldest binlog.
    #[test]
    fn maria_dump_gtid_list() {
        let list = |t: &str| maria::dump_gtid_list(t).unwrap();
        assert_eq!(list("0-1-100"), "0-1-100");
        // an explicit window goes out as its end: the server's list is a high-water mark too
        assert_eq!(list("0-1-40-60"), "0-1-60");
        // two entries of one pair merge to their highest seq, and pairs keep a stable order
        assert_eq!(list("0-1-3,0-1-10-12"), "0-1-12");
        assert_eq!(list("1-2-5,0-7-9,0-10-1"), "0-10-1,0-7-9,1-2-5");
        // whitespace and a repeated pair are tolerated (the text comes from a saved cursor)
        assert_eq!(list(" 0-1-4 , 0-1-2 "), "0-1-4");
        // Nothing usable: refused, with words that name the GTID list.
        for bad in ["", "   ", "not-a-list", "0-1", "0-1-x", "0-1-0", "0-1-5-3"] {
            let e = match maria::dump_gtid_list(bad) {
                Ok(list) => panic!("{bad:?} accepted as {list:?}"),
                Err(e) => e,
            };
            assert!(
                format!("{e:#}").contains("GTID list"),
                "{bad:?} refused with the wrong words: {e:#}"
            );
        }
    }
}
