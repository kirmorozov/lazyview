use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum QueryKind {
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

pub(super) fn classify_query(q: &str) -> QueryKind {
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
pub(super) fn xa_prepare_event(data: &[u8]) -> Result<(bool, String)> {
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

pub(super) fn format_uuid(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, x) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{x:02x}");
    }
    s
}

pub(super) fn mysql_gtid(ge: &GtidEvent) -> String {
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
pub(super) fn mariadb_gtid(server_id: u32, data: &[u8]) -> Result<(String, Option<String>)> {
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
