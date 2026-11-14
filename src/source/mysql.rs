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

use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use mysql_async::binlog::events::{
    Event, FormatDescriptionEvent, GtidEvent, TableMapEvent, TransactionPayloadEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::{BinlogVersion, EventType};
use mysql_async::consts::ColumnType;
use mysql_async::Value;

/// The MySQL/Aurora commit-order fence. `src/fence.rs` is its own copy-set file; the MySQL
/// family reaches it through these lines (Percona through here), so `fence.rs` has one
/// flavor-side caller.
pub use crate::fence::{commit, ordered, probe, table};

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
