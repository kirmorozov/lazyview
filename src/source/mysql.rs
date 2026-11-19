//! The MySQL-family side of a replication source: what the server must be set to
//! ([`preflight`]), how to ask it for the binlog status ([`status_statements`]), its GTID text
//! ([`mysql_gtid`]), compressed transactions ([`Payload`]), the QUERY-event and XA
//! classifications ([`classify_query`], [`xa_prepare_event`]) and the TIME(1)/(2) decode twin
//! ([`Time2Twin`]). Percona shares every line (it runs the same server); MariaDB reaches the
//! parts it shares through this module (`xid_string`), never by copy.
//!
//! The commit-order fence lives in [`crate::fence`], reached through these lines; per issue #57
//! it must never be ported to PostgreSQL.
//!
//! Copy set: `std` + `anyhow` + `mysql_async` only, no `crate::` beyond the copy set, no
//! environment variables (a knob is a parameter the caller passes), no logging.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Write as _;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::binlog::events::{
    Event, FormatDescriptionEvent, GtidEvent, TableMapEvent, TransactionPayloadEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::{BinlogVersion, EventType};
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use mysql_async::Value;

use crate::catalog::{Column, FkAction, ForeignKey, Generated, TableInfo, TableRef};
use crate::source::SourceCaps;

/// The MySQL/Aurora commit-order fence. `src/fence.rs` is its own copy-set file; the MySQL
/// family reaches it through these lines (Percona through here), so `fence.rs` has one
/// flavor-side caller.
pub use crate::fence::{commit, ordered, probe, table};

/// The MySQL family's answer to `Flavor::snapshot_position_is_watermark`: `false`. What its
/// `binlog_snapshot_%` status variables report is the raw binlog write position, and events are
/// written before the transaction commits, so the position has already passed transactions still
/// in flight that the snapshot taken at the same moment cannot read; a wait on it approves a read
/// of stale rows. Oracle Community reports no pair at all, which answers the same way. The family
/// is fenced by [`gtid_wait`] when it has GTIDs and by [`commit_order`] otherwise (Percona Server
/// reaches both through this file, `percona::snapshot_position_is_watermark`).
pub(crate) fn snapshot_position_is_watermark() -> bool {
    false
}

/// The MySQL family's GTID visibility wait: `WAIT_FOR_EXECUTED_GTID_SET` for `gtid`, capped at
/// 5 s by the server itself. `Ok(Some(Some(0)))`: visible; `Ok(Some(Some(_)))`: timed out; the
/// caller owns the meaning (see [`crate::source::fence`]).
pub(crate) async fn gtid_wait(conn: &mut Conn, gtid: &str) -> Result<Option<Option<i64>>> {
    Ok(conn
        .exec_first(
            "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 5)",
            (gtid.to_string(),),
        )
        .await?)
}

/// What the commit-order fence step concluded (see [`commit_order`]).
pub(crate) enum CommitStep {
    /// The fence write landed: every transaction read up to the boundary is visible.
    Written,
    /// The commit-order fence is not available here: the caller falls back to its delay.
    RuledOut,
}

/// The commit-order fence step: probes the commit order once per connection ([`probe`], the
/// variable and Aurora's enhanced binlog through [`ordered`]) and then commits one row of our
/// own ([`commit`]), both through `src/fence.rs` — this file is that file's only caller
/// (Percona reaches it through here; per issue #57 it must never reach PostgreSQL). The caches
/// ([`crate::source::FenceState`]) belong to the engine; the WARN texts name the caller's
/// `fallback` wait. `Err` is a transient connection failure (the caller heals and retries); a
/// ruled-out fence caches why and answers [`CommitStep::RuledOut`].
pub(crate) async fn commit_order(
    conn: &mut Conn,
    boundary: &crate::source::Position,
    instance_id: &str,
    fallback: &str,
    state: &mut crate::source::FenceState,
    report: &mut crate::source::FenceReport,
    beat: &mut dyn FnMut(),
) -> Result<CommitStep> {
    if state.committed.as_ref() == Some(boundary) {
        return Ok(CommitStep::Written);
    }
    if state.commit_order.is_none() {
        // Once per connection.
        match probe(conn).await {
            Ok(true) => state.commit_order = Some(true),
            Ok(false) => {
                state.commit_order = Some(false);
                report.fence_warn = Some(format!(
                    "binlog_order_commits is OFF: lazyview cannot prove a change is visible before reading the sources and {fallback}, which is not exact under load; fix: SET PERSIST binlog_order_commits=ON (Amazon RDS/Aurora: the DB parameter group) or turn on GTIDs"
                ));
            }
            Err(e) => {
                if crate::source::is_transient(&e) {
                    return Err(e);
                }
                state.commit_order = Some(false);
                report.fence_warn = Some(format!(
                    "commit fence unavailable ({e:#}); {fallback}, which is not exact under load"
                ));
            }
        }
    }
    if state.commit_order != Some(true) {
        return Ok(CommitStep::RuledOut);
    }
    match commit(conn, instance_id).await {
        Ok(()) => {
            state.committed = Some(boundary.clone());
            beat();
            Ok(CommitStep::Written)
        }
        Err(e) => {
            if crate::source::is_transient(&e) {
                return Err(e);
            }
            state.commit_order = Some(false);
            report.fence_warn = Some(format!(
                "commit fence write to {} failed ({e:#}); {fallback}, which is not exact under load",
                table()
            ));
            Ok(CommitStep::RuledOut)
        }
    }
}

/// The MySQL-family preflight on the already-read value of `binlog_row_value_options` (the
/// probe is the caller's: it is the network edge). One problem message when the server runs
/// with PARTIAL_JSON, `None` when it does not. Partial JSON row images carry diffs instead of
/// values; lazyview needs full values.
pub(crate) fn preflight(row_value_options: Option<&str>) -> Option<String> {
    if row_value_options
        .unwrap_or_default()
        .to_ascii_uppercase()
        .contains("PARTIAL_JSON")
    {
        return Some(
            "binlog_row_value_options is PARTIAL_JSON (must be empty); fix: SET PERSIST \
             binlog_row_value_options='' (Amazon RDS/Aurora: set it in the DB parameter group)"
                .into(),
        );
    }
    None
}

/// The two spellings of the binlog status statement, the one the server is expected to know
/// first (the other is tried on a parse error).
///
/// The flavor follows from the version the handshake reports (the caller cannot tell the two
/// apart yet): MySQL and Percona are 8.x, 9.x and, since 26.7, calendar versions (`YY.M`);
/// MariaDB is 10.x to 13.x (mysql_async strips its `5.5.5-` prefix). `SHOW BINARY LOG STATUS`
/// exists on MySQL >= 8.2 (`SHOW MASTER STATUS` was removed in 8.4) and on MariaDB >= 13.0,
/// which still accepts `SHOW MASTER STATUS` as well.
pub(crate) fn status_statements(major: u16, minor: u16) -> [&'static str; 2] {
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

pub(crate) fn format_uuid(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, x) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// The GTID text of a MySQL/Percona `GtidEvent`: `uuid:gno` (`uuid:tag:gno` for tagged GTIDs).
pub(crate) fn mysql_gtid(ge: &GtidEvent) -> String {
    let uuid = format_uuid(&ge.sid());
    match ge.tag() {
        Some(tag) => format!("{uuid}:{tag}:{}", ge.gno()),
        None => format!("{uuid}:{}", ge.gno()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryKind {
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

/// Classify the MySQL-family QUERY-event statement text (XA in the stream is MySQL-shaped;
/// MariaDB marks XA on its GTID event instead).
pub(crate) fn classify_query(q: &str) -> QueryKind {
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
/// the form the servers log it in. Shared: MariaDB's GTID XA flags decode their id with this.
pub(crate) fn xid_string(format_id: i64, gtrid: &[u8], bqual: &[u8]) -> String {
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
pub(crate) fn xa_prepare_event(data: &[u8]) -> Result<(bool, String)> {
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

/// `TRANSACTION_PAYLOAD_EVENT`: the event `binlog_transaction_compression=ON` writes around the
/// (then decompressed by mysql_async) inner events of a transaction.
pub const TRANSACTION_PAYLOAD: u8 = 0x28;

/// Expansion state of a `TRANSACTION_PAYLOAD_EVENT` (bytes of inner events still expected).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Payload {
    pub(crate) remaining: u64,
}

impl Payload {
    /// The state a `TRANSACTION_PAYLOAD` event opens.
    pub(crate) fn from_event(tpe: &TransactionPayloadEvent<'_>) -> Payload {
        // Uncompressed payloads (algorithm NONE) carry no uncompressed size.
        let size = match tpe.uncompressed_size() {
            0 => tpe.payload_raw().len() as u64,
            n => n,
        };
        Payload { remaining: size }
    }

    /// Consumes `event_size` bytes of inner events; true when the payload is now fully expanded.
    pub(crate) fn consume(&mut self, event_size: u64) -> bool {
        self.remaining = self.remaining.saturating_sub(event_size);
        self.remaining == 0
    }
}

/// Decode twin of a table map with TIME(1) / TIME(2) columns. mysql_common 0.37 decodes their
/// negative values with a fraction wrongly (`frac -= 0x100` on a `u32` in
/// `my_time_packed_from_binary`: an overflow panic in debug builds, garbage in release builds).
/// In the twin those columns are 4-byte BIT columns (same width; like TIME2 neither numeric nor
/// character, so the optional metadata still lines up), whose raw bytes [`Time2Twin::fix`]
/// decodes as the server does. Applies to both flavors; the bug is in the decoder, not the
/// server, so this is shared code, not a flavor branch.
#[derive(Debug, Clone)]
pub(crate) struct Time2Twin {
    tme: TableMapEvent<'static>,
    cols: Vec<usize>,
}

impl Time2Twin {
    /// The twin table map to decode rows with: same widths, the TIME(1)/(2) columns typed BIT.
    pub(crate) fn tme(&self) -> &TableMapEvent<'static> {
        &self.tme
    }

    pub(crate) fn fix(&self, row: &mut BinlogRow) {
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
/// or 2 fractional digits. A table that has them but whose event body does not line up with the
/// parsed layout is an error rather than a missing twin: reading it without one misreads negative
/// values with a fraction, silently.
pub(crate) fn time2_twin(ev: &Event, tme: &TableMapEvent<'_>) -> Result<Option<Time2Twin>> {
    let Ok(n) = usize::try_from(tme.columns_count()) else {
        bail!(
            "the column count of {} does not fit this platform; its TIME(1)/TIME(2) columns \
             cannot be decoded",
            tme_name(tme)
        );
    };
    let cols: Vec<usize> = (0..n)
        .filter(|&i| {
            matches!(
                tme.get_column_type(i),
                Ok(Some(ColumnType::MYSQL_TYPE_TIME2))
            ) && matches!(tme.get_column_metadata(i), Some([1 | 2]))
        })
        .collect();
    if cols.is_empty() {
        return Ok(None);
    }
    let Some(twin) = time2_twin_body(ev, tme, &cols) else {
        bail!(
            "cannot build the TIME(1)/TIME(2) decode twin of {}; negative values with a fraction \
             would be misread",
            tme_name(tme)
        );
    };
    Ok(Some(Time2Twin { tme: twin, cols }))
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
    twin.push(EventType::TABLE_MAP_EVENT as u8);
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

pub(crate) fn tme_name(tme: &TableMapEvent<'_>) -> String {
    format!("`{}`.`{}`", tme.database_name(), tme.table_name())
}

// ---- The catalog loaders (F6): the MySQL-family spelling of the five `information_schema`
// reads. `catalog.rs` keeps the generic records and the cache; MariaDB reaches the parts it
// shares through these lines (its answers for the generation expression and the default
// collation are its own, in `maria.rs`), never by copy.

/// One `information_schema.COLUMNS` row: name, data type, column type, charset, collation,
/// extra, octet length, nullability, generation expression.
pub(crate) type ColRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    Option<u64>,
    String,
    Option<String>,
);

/// The `COLUMNS` rows of one table in `ORDINAL_POSITION` order (`None`: the table does not
/// exist). The read is shared; what is made of the rows is the flavor's.
pub(crate) async fn read_columns(
    conn: &mut Conn,
    schema: &str,
    name: &str,
) -> Result<Option<Vec<ColRow>>> {
    let rows: Vec<ColRow> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(COLUMN_TYPE AS CHAR), \
             CAST(CHARACTER_SET_NAME AS CHAR), CAST(COLLATION_NAME AS CHAR), CAST(EXTRA AS CHAR), \
             CHARACTER_OCTET_LENGTH, CAST(IS_NULLABLE AS CHAR), CAST(GENERATION_EXPRESSION AS CHAR) \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(rows))
}

/// The MySQL / Percona table metadata: the five `information_schema` reads in MySQL spelling,
/// string-escaped generation expressions ([`unescape_generation_expression`]) and a
/// `COLLATION(CAST(... AS CHAR CHARACTER SET ...))` probe for every charset the table uses.
pub(crate) async fn load_table(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    caps: SourceCaps,
    default_collations: &mut HashMap<String, Option<String>>,
) -> Result<Option<TableInfo>> {
    let Some(rows) = read_columns(conn, schema, name).await? else {
        return Ok(None);
    };
    for (_, _, _, cs, ..) in &rows {
        if let Some(cs) = cs.as_deref().map(str::to_ascii_lowercase) {
            if let Entry::Vacant(e) = default_collations.entry(cs) {
                let co = default_collation(conn, e.key()).await?;
                e.insert(co);
            }
        }
    }
    let columns = columns_from_rows(
        rows,
        caps,
        default_collations,
        unescape_generation_expression,
    );
    Ok(Some(finish_table(conn, schema, name, columns).await?))
}

/// Assemble the columns from the `COLUMNS` rows: `unescape` renders a generated column's
/// expression in the flavor's spelling; the charset, collation and dependency work is
/// flavor-neutral (the same on every server that answers these views).
pub(crate) fn columns_from_rows(
    rows: Vec<ColRow>,
    caps: SourceCaps,
    default_collations: &HashMap<String, Option<String>>,
    unescape: fn(&str) -> String,
) -> Vec<Column> {
    let mut columns: Vec<Column> = rows
        .into_iter()
        .map(
            |(name, data_type, column_type, cs, co, extra, octets, nullable, gen)| {
                let default = cs
                    .as_deref()
                    .and_then(|c| default_collations.get(&c.to_ascii_lowercase()))
                    .and_then(|d| d.as_deref());
                let extra_uc = extra.to_ascii_uppercase();
                let generated = gen
                    .filter(|g| !g.trim().is_empty() && extra_uc.contains("GENERATED"))
                    .map(|expr| Generated {
                        expr: unescape(&expr),
                        stored: extra_uc.contains("STORED"),
                        deps: None,
                    });
                Column {
                    invisible: extra_uc.contains("INVISIBLE"),
                    nullable: nullable.eq_ignore_ascii_case("YES"),
                    generated,
                    ..Column::new(
                        &name,
                        &data_type,
                        &column_type,
                        cs.as_deref(),
                        co.as_deref(),
                        octets,
                        caps,
                    )
                    .with_default_collation(default)
                }
            },
        )
        .collect();
    // Base columns of the generated columns (the expression names them, quoted).
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    for c in &mut columns {
        if let Some(g) = &mut c.generated {
            g.deps = generated_deps(&g.expr, &names);
        }
    }
    columns
}

/// The key, cascade and view-base reads of one table whose columns are already loaded, and the
/// `TableInfo` they complete.
pub(crate) async fn finish_table(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    columns: Vec<Column>,
) -> Result<TableInfo> {
    let (pk, unique_keys) = load_keys(conn, schema, name, &columns).await?;
    let cascades = load_foreign_keys(conn, schema, name, &columns).await?;
    let (is_view, view_bases) = load_view_bases(conn, schema, name).await?;
    Ok(TableInfo {
        schema: schema.to_string(),
        name: name.to_string(),
        columns,
        pk,
        unique_keys,
        cascades,
        is_view,
        view_bases,
    })
}

/// The PRIMARY key ordinals and the other keys that identify a row, from one
/// `information_schema.STATISTICS` read (the PK is the `PRIMARY` group; PK columns are
/// always whole and NOT NULL on every flavor, so the filters that reject a prefix,
/// functional or nullable column cannot drop one).
async fn load_keys(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    columns: &[Column],
) -> Result<(Vec<usize>, Vec<Vec<usize>>)> {
    type KeyRow = (String, u64, Option<String>, Option<u64>, String);
    let key_rows: Vec<KeyRow> = conn
        .exec(
            "SELECT CAST(INDEX_NAME AS CHAR), SEQ_IN_INDEX, CAST(COLUMN_NAME AS CHAR), SUB_PART, \
             CAST(COALESCE(NULLABLE, '') AS CHAR) \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (schema, name),
        )
        .await?;
    keys_from(name, &key_rows, columns)
}

/// Foreign keys of this (child) table that change child rows, grouped by constraint name from
/// `information_schema.KEY_COLUMN_USAGE` joined with `REFERENTIAL_CONSTRAINTS`.
async fn load_foreign_keys(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    columns: &[Column],
) -> Result<Vec<ForeignKey>> {
    let fk_rows: Vec<(String, String, String, String, String, String, String)> = conn
        .exec(
            "SELECT CAST(k.CONSTRAINT_NAME AS CHAR), CAST(k.COLUMN_NAME AS CHAR), CAST(k.REFERENCED_TABLE_SCHEMA AS CHAR), \
             CAST(k.REFERENCED_TABLE_NAME AS CHAR), CAST(k.REFERENCED_COLUMN_NAME AS CHAR), \
             CAST(r.DELETE_RULE AS CHAR), CAST(r.UPDATE_RULE AS CHAR) \
             FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r \
               ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND r.TABLE_NAME = k.TABLE_NAME \
             WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL \
             ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION",
            (schema, name),
        )
        .await?;
    let mut cascades: Vec<ForeignKey> = Vec::new();
    let mut last: Option<String> = None;
    for (cname, col, ps, pt, pc, del, upd) in fk_rows {
        let (on_delete, on_update) = (FkAction::parse(&del), FkAction::parse(&upd));
        if on_delete == FkAction::Restrict && on_update == FkAction::Restrict {
            continue;
        }
        let Some(ci) = columns.iter().position(|c| c.name == col) else {
            // The FK rows and COLUMNS come from this connection's `information_schema` for the
            // same (schema, table): a missing column is broken metadata, never a lost cascade.
            bail!("internal: FK `{cname}` of table `{name}` names column `{col}`, which the table does not have");
        };
        if last.as_deref() != Some(cname.as_str()) {
            cascades.push(ForeignKey {
                parent: (ps, pt),
                child_cols: Vec::new(),
                parent_cols: Vec::new(),
                on_delete,
                on_update,
            });
            last = Some(cname);
        }
        let fk = cascades.last_mut().unwrap();
        fk.child_cols.push(ci);
        fk.parent_cols.push(pc);
    }
    Ok(cascades)
}

/// Whether the table is a SQL VIEW and its base tables. They come from the stored definition
/// (VIEW_TABLE_USAGE is missing on MariaDB and MySQL < 8.0.13, so parse VIEWS.VIEW_DEFINITION
/// instead).
async fn load_view_bases(
    conn: &mut Conn,
    schema: &str,
    name: &str,
) -> Result<(bool, Vec<TableRef>)> {
    let view_def: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(VIEW_DEFINITION AS CHAR) FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?;
    Ok(match view_def {
        Some(def) => (
            true,
            def.as_deref()
                .map(|d| crate::analyze::select_tables(d, schema))
                .unwrap_or_default(),
        ),
        None => (false, Vec::new()),
    })
}

/// The PRIMARY key ordinals and the other UNIQUE keys usable as row keys: whole columns only
/// (no prefix `SUB_PART`, no functional part, which has no column name), every column NOT NULL
/// (a nullable unique column admits several NULL rows). Rows come ordered by index name and
/// sequence. An index column the table does not have is broken metadata: a loud error naming
/// table and column, never a silently shorter key (`TableInfo::row_key` feeds the dedup key).
pub(crate) fn keys_from(
    table: &str,
    rows: &[(String, u64, Option<String>, Option<u64>, String)],
    columns: &[Column],
) -> Result<(Vec<usize>, Vec<Vec<usize>>)> {
    let mut pk: Vec<usize> = Vec::new();
    let mut out: Vec<Vec<usize>> = Vec::new();
    let mut current: Option<(String, Option<Vec<usize>>)> = None;
    let flush = |cur: &mut Option<(String, Option<Vec<usize>>)>,
                 pk: &mut Vec<usize>,
                 out: &mut Vec<Vec<usize>>| {
        if let Some((index, Some(k))) = cur.take() {
            if !k.is_empty() {
                if index == "PRIMARY" {
                    *pk = k;
                } else {
                    out.push(k);
                }
            }
        }
    };
    for (index, _, col, sub_part, _) in rows {
        if current.as_ref().map(|(n, _)| n != index).unwrap_or(true) {
            flush(&mut current, &mut pk, &mut out);
            current = Some((index.clone(), Some(Vec::new())));
        }
        let Some((_, key)) = &mut current else {
            continue;
        };
        let ci = match col.as_deref() {
            Some(c) => match columns.iter().position(|x| x.name == c) {
                Some(ci) => Some(ci),
                None => bail!(
                    "internal: index `{index}` of table `{table}` names column `{c}`, which the table does not have"
                ),
            },
            None => None,
        };
        match (ci, sub_part) {
            (Some(ci), None) if !columns[ci].nullable => {
                if let Some(k) = key {
                    k.push(ci);
                }
            }
            _ if index == "PRIMARY" => bail!(
                "internal: table `{table}` has an unusable PRIMARY column ({}: functional, prefix or nullable)",
                col.as_deref().unwrap_or("(functional)")
            ),
            _ => *key = None,
        }
    }
    flush(&mut current, &mut pk, &mut out);
    Ok((pk, out))
}

/// MySQL's `GENERATION_EXPRESSION` is the expression escaped like a string literal's content
/// (`\'` for a quote, `\\` for a backslash): the SQL text it stands for. MariaDB's answer
/// ([`crate::source::maria::unescape_generation_expression`]) is the plain SQL itself.
pub(crate) fn unescape_generation_expression(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.extend(chars.next()),
            c => out.push(c),
        }
    }
    out
}

/// How a JSON value binds as a parameter on the MySQL family: the server needs the type, so
/// the answer is the cast (MariaDB's answer, a plain string, is in `maria.rs`).
pub(crate) fn json_param() -> String {
    "CAST(? AS JSON)".to_string()
}

/// Ordinals of the columns a generated column's expression reads (parsed with the MySQL
/// dialect; `None` if the text does not parse).
pub(crate) fn generated_deps(expr: &str, names: &[String]) -> Option<Vec<usize>> {
    use sqlparser::ast::{visit_expressions, Expr};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    use std::ops::ControlFlow;
    let e = Parser::new(&MySqlDialect {})
        .try_with_sql(expr)
        .ok()?
        .parse_expr()
        .ok()?;
    let mut deps = Vec::new();
    let _ = visit_expressions(&e, |x: &Expr| {
        let id = match x {
            Expr::Identifier(i) => Some(i),
            Expr::CompoundIdentifier(parts) => parts.last(),
            _ => None,
        };
        if let Some(i) = id {
            if let Some(c) = names.iter().position(|n| n.eq_ignore_ascii_case(&i.value)) {
                if !deps.contains(&c) {
                    deps.push(c);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Some(deps)
}

/// The collation `CAST(... AS CHAR CHARACTER SET charset)` yields on this server (its default for
/// the charset). None if unknown. The MariaDB answer, which its `character_set_collations` can
/// override, is the flavor's own [`crate::source::maria::default_collation`]. The question is asked
/// in the shape [`crate::catalog::TEXT_PARAM_PREFIX`] binds a string in, so the answer is the
/// collation a bound value of this charset actually carries; `CONVERT(... USING charset)` is the
/// conversion, not the reinterpretation, and the two can differ.
pub(crate) async fn default_collation(conn: &mut Conn, charset: &str) -> Result<Option<String>> {
    if charset.is_empty()
        || !charset
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Ok(None);
    }
    let v = conn
        .query_first::<Option<String>, _>(format!(
            "SELECT CAST(COLLATION(CAST(CAST('' AS BINARY) AS CHAR CHARACTER SET {charset})) AS CHAR)"
        ))
        .await
        .with_context(|| format!("default collation of character set {charset}"))?;
    Ok(v.flatten().map(|c| c.to_ascii_lowercase()))
}

// --- the cursor's rules: the ordering and the spelling the generic side must not know -------

/// Numeric suffix of the binlog file name (`binlog.000012` → 12): the file's place in the index.
pub(crate) fn file_seq(file: &str) -> u64 {
    file.rsplit('.')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// The MySQL-family order of two binlog coordinates: the file's place in the index, then the
/// offset inside it (an offset says nothing about another file).
pub(crate) fn position_key(p: &crate::source::Position) -> (u64, u64) {
    (file_seq(&p.file), p.pos)
}

/// The cursor's order for this flavor — the same rule, reached from `Cursor`'s `Ord` so the
/// ordering lives here and not on the generic side.
pub(crate) fn cursor_key(p: &crate::source::Position) -> (u64, u64) {
    position_key(p)
}

/// No event lives before a binlog file's 4-byte header, so offset 0 of a real file is not a
/// resume point: the `pos.max(4)` the position used to carry, as the flavor's rule.
pub(crate) fn floor_offset(pos: u64) -> u64 {
    pos.max(4)
}

/// A coordinate as this flavor stores it: no file is no position (offset 0 of nothing); a named
/// file never starts below its header.
pub(crate) fn normalize(p: &crate::source::Position) -> crate::source::Position {
    if p.file.is_empty() {
        return crate::source::Position {
            file: String::new(),
            pos: 0,
        };
    }
    crate::source::Position {
        file: p.file.clone(),
        pos: floor_offset(p.pos),
    }
}

/// Splits a cursor's text into its `file:pos` and the optional bracketed GTID set (§ the
/// resume-by-GTID plan): `file:pos[g=set]` → `("file:pos", Some("set"))`, `file:pos` →
/// `("file:pos", None)`. Splitting on `[g=` before touching `:` is what lets the set keep its own
/// colons (`uuid:1-5`) without confusing the `file:pos` split that follows. The set is handed back
/// verbatim (it is opaque to the generic side); the flavor's own parser is what reads it later.
fn split_gtid(text: &str) -> (&str, Option<&str>) {
    if let Some(body) = text.strip_suffix(']') {
        if let Some(i) = body.find("[g=") {
            return (&body[..i], Some(&body[i + 3..]));
        }
    }
    (text, None)
}

/// The one spelling of a cursor's text (`file:pos` and the optional `[g=…]`) and its parse: the
/// metadata tables' text comes back through here, so nothing outside the flavor modules splits it.
/// A text with no bracket (every pre-GTID cursor) parses to `(position, None)`, exactly as before.
pub(crate) fn parse(text: &str) -> Option<(crate::source::Position, Option<String>)> {
    let (pos_text, g) = split_gtid(text);
    let (file, pos) = pos_text.rsplit_once(':')?;
    if file.is_empty() {
        return None;
    }
    Some((
        crate::source::Position {
            file: file.to_string(),
            pos: pos.parse().ok()?,
        },
        g.map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    ))
}

/// A parsed GTID set: every source id (a MySQL/Percona `uuid`, or a MariaDB `domain-server` pair)
/// mapped to the sorted, disjoint intervals of transaction numbers executed from it. The two
/// flavors spell the text differently — each `parse_gtid_set` owns its grammar and fills this
/// shared shape — but the subset question is the same intervals either way, so MariaDB reaches
/// [`gtid_subset`] through here rather than copying it.
pub(crate) type GtidSet = std::collections::BTreeMap<String, Vec<(u64, u64)>>;

/// The MySQL/Percona set grammar: comma-separated `uuid:interval[:interval…]` groups (the same
/// uuid may recur across groups, so its intervals merge), each `interval` a single `n` or a range
/// `n-m`. Empty text, blank groups and blank intervals contribute nothing (a `uuid:` group is a
/// uuid with no transactions); a non-numeric interval is dropped, never guessed.
pub(crate) fn parse_gtid_set(text: &str) -> GtidSet {
    let mut set = GtidSet::new();
    for group in text.split(',') {
        let group = group.trim();
        if group.is_empty() {
            continue;
        }
        let mut parts = group.split(':');
        let sid = match parts.next() {
            Some(s) => s.trim().to_ascii_lowercase(),
            None => continue,
        };
        if sid.is_empty() {
            continue;
        }
        let entry = set.entry(sid).or_default();
        for iv in parts {
            if let Some((lo, hi)) = parse_interval(iv) {
                merge_interval(entry, lo, hi);
            }
        }
    }
    set
}

/// `n` → `(n, n)`, `n-m` → `(n, m)` (both inclusive); blank or non-numeric → `None`.
fn parse_interval(iv: &str) -> Option<(u64, u64)> {
    let iv = iv.trim();
    if iv.is_empty() {
        return None;
    }
    match iv.split_once('-') {
        Some((a, b)) => Some((a.trim().parse().ok()?, b.trim().parse().ok()?)),
        None => iv.parse::<u64>().ok().map(|n| (n, n)),
    }
}

/// Inserts the inclusive range `[lo, hi]` into a sorted, disjoint interval list, coalescing with
/// any range it overlaps or touches (so consecutive transactions stay one range).
pub(crate) fn merge_interval(v: &mut Vec<(u64, u64)>, lo: u64, hi: u64) {
    if lo > hi {
        return;
    }
    let (mut lo, mut hi) = (lo, hi);
    let mut res: Vec<(u64, u64)> = Vec::with_capacity(v.len() + 1);
    let mut inserted = false;
    for &(a, b) in v.iter() {
        if b.saturating_add(1) < lo {
            res.push((a, b)); // whole range is below and not adjacent
        } else if a > hi.saturating_add(1) {
            if !inserted {
                res.push((lo, hi));
                inserted = true;
            }
            res.push((a, b)); // whole range is above and not adjacent
        } else {
            lo = lo.min(a); // overlaps or is adjacent: absorb
            hi = hi.max(b);
        }
    }
    if !inserted {
        res.push((lo, hi));
    }
    *v = res;
}

/// Whether every transaction number in the inclusive range `[lo, hi]` lies in one of `src`'s
/// (sorted, disjoint) intervals — an interval comparison, so a range of billions costs a walk of
/// the intervals, not of the numbers. An empty range is covered.
fn covers_interval(src: &[(u64, u64)], lo: u64, hi: u64) -> bool {
    if lo > hi {
        return true;
    }
    let mut need = lo;
    for &(a, b) in src {
        if b < need {
            continue; // this interval is below what is still needed
        }
        if a > need {
            return false; // a gap before covering the next needed number
        }
        need = b.saturating_add(1);
        if need > hi {
            return true;
        }
    }
    need > hi
}

/// Whether `saved ⊆ source`: every source id `saved` names is present in `source` and every one of
/// its ranges lies in `source`'s ranges for that id. An empty `saved` is a subset of anything
/// (a source that has consumed nothing is covered by every server) — the empty-set row of the
/// subset table.
pub(crate) fn gtid_subset(saved: &GtidSet, source: &GtidSet) -> bool {
    saved.iter().all(|(sid, ivals)| {
        source
            .get(sid)
            .is_some_and(|src| ivals.iter().all(|&(lo, hi)| covers_interval(src, lo, hi)))
    })
}

/// The MySQL/Percona subset question for [`crate::source::gtid_covers`]: does this server's
/// `@@GLOBAL.gtid_executed` include `saved`? A cursor with no set covers trivially (see
/// [`crate::source::gtid_covers`]); a `gtid_mode=OFF` server reports an empty `executed`, so a real
/// set is not covered and the caller stops cleanly rather than resuming somewhere it cannot.
pub(crate) async fn gtid_covers(conn: &mut Conn, saved: Option<&str>) -> Result<bool> {
    let Some(saved) = saved.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(true);
    };
    let saved = parse_gtid_set(saved);
    let st = crate::binlog::gtid_state(conn).await?;
    let executed = st.executed.unwrap_or_default();
    Ok(gtid_subset(&saved, &parse_gtid_set(&executed)))
}

/// Whether the server still has the file of `at` in its log index — what a lost slot means here.
pub(crate) async fn can_resume(
    conn: &mut mysql_async::Conn,
    at: &crate::source::Position,
) -> anyhow::Result<bool> {
    crate::binlog::binlog_exists(conn, &at.file).await
}
