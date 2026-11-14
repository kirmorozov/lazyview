//! The MariaDB side of the source seam: what MariaDB probes at preflight, asks for on the
//! connect handshake, and puts on the wire where the generic side must not know the flavor —
//! its raw event codes, `Partial_rows` fragment reassembly (>= 12.3), the `domain-server-seq`
//! GTID with XA flags, and the `log_pos = 0` advance-by-size position rule (>= 11.4).
//!
//! The generic side reaches this file through the `(flavor, code)` dispatch arms in
//! [`crate::binlog::stream`] and [`crate::source::position_rule`]; `rotate_target`/`crc32`
//! deliberately stay generic (they self-detect, no flavor branch).
//!
//! Copy set: `std` + `anyhow` + `mysql_async` only, no `crate::` beyond the copy set, no
//! environment variables (a knob is a parameter the caller passes), no logging.

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::binlog::events::{BinlogEventHeader, Event};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use crate::source::{Position, Step};

/// MariaDB `GTID_EVENT`: `domain-server-seq`, with the XA flags [`gtid`] reads.
pub const MARIA_GTID: u8 = 162;
/// MariaDB `Query_compressed_event` .. `Delete_rows_compressed_event`: written while
/// `log_bin_compress=ON`; this reader rejects them ([`compressed_event_error`], and preflight
/// demands the variable be OFF).
pub const MARIA_QUERY_COMPRESSED: u8 = 165;
pub const MARIA_DELETE_ROWS_COMPRESSED: u8 = 171;
/// MariaDB `Partial_rows_event` (>= 12.3): a rows event above
/// `binlog_row_event_fragment_threshold` split into fragments; [`partial_rows`] reassembles.
pub const MARIA_PARTIAL_ROWS: u8 = 172;

/// The connect handshake MariaDB needs before a binlog stream:
/// `MARIA_SLAVE_CAPABILITY_GTID` — we understand GTID / GTID_LIST / BINLOG_CHECKPOINT
/// events (otherwise the server rewrites them into dummy events).
pub(crate) async fn prepare_connection(conn: &mut Conn) -> Result<()> {
    conn.query_drop("SET @mariadb_slave_capability = 4")
        .await
        .context("SET @mariadb_slave_capability")
}

/// MariaDB's preflight probes, as `(log_bin_compress, binlog_storage_engine)`: compression
/// would put events on the wire this reader cannot decode, and a binlog kept in the storage
/// engine is only served to replicas that start from a GTID (this one starts by file and
/// offset). The violated-requirement messages stay with the shared preflight in `binlog`.
/// Variables missing on older servers (binlog_storage_engine before 12.3) come back `None`:
/// nothing to check. A probe that only failed is an error, never "absent".
pub(crate) async fn probes(conn: &mut Conn) -> Result<(Option<String>, Option<String>)> {
    let compress = crate::binlog::probe_var_retry(conn, "log_bin_compress").await?;
    let storage_engine = crate::binlog::probe_var_retry(conn, "binlog_storage_engine").await?;
    Ok((compress, storage_engine))
}

/// A rows event being reassembled from MariaDB `Partial_rows` fragments.
#[derive(Debug)]
pub(crate) struct Partial {
    total: u32,
    /// Sequence number of the next fragment (they start at 1).
    next: u32,
    /// Length of the whole rows event, from the first fragment.
    len: u64,
    buf: Vec<u8>,
}

/// Adds a MariaDB `Partial_rows` fragment (type 172, MariaDB >= 12.3) to `partial`; returns the
/// rows event once its last fragment is in. `Tracker`'s `(flavor, code)` arm calls this.
///
/// Body: fragment count u32, sequence number u32 (from 1), flags u8; the first fragment then
/// has the length of the whole rows event u64. The fragments follow each other and their
/// payloads concatenate to the rows event (header and body, without its checksum, although
/// its header counts one).
pub(crate) fn partial_rows(
    partial: &mut Option<Partial>,
    ev: &Event,
    at: &Position,
) -> Result<Option<Event>> {
    let data = ev.data();
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
        *partial = Some(Partial {
            total,
            next: 1,
            len,
            buf: Vec::new(),
        });
    }
    let Some(p) = partial
        .as_mut()
        .filter(|p| p.next == seq && p.total == total)
    else {
        bail!(
            "MariaDB Partial_rows fragment {seq} of {total} at {at} does not continue a rows event"
        );
    };
    p.buf.extend_from_slice(payload);
    p.next += 1;
    if seq < total {
        return Ok(None);
    }
    let Partial { len, mut buf, .. } = partial.take().expect("checked above");
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

/// The error for a compressed MariaDB binlog event (types 165–171): preflight demanded
/// `log_bin_compress=OFF`, but events written while it was ON cannot be read. `Tracker`'s
/// other `(flavor, code)` arm reports it.
pub(crate) fn compressed_event_error(et: u8, at: &Position) -> anyhow::Error {
    anyhow!(
        "MariaDB compressed binlog event (type {et}) at {at} is not supported; \
         set log_bin_compress=OFF on the server (events already written compressed cannot be read)",
    )
}

/// MariaDB GTID_EVENT body (little endian): seq_no u64, domain_id u32, flags2 u8,
/// [commit_id u64 if FL_GROUP_COMMIT_ID], [formatID u32, gtrid_len u8, bqual_len u8, ids if
/// FL_PREPARED_XA / FL_COMPLETED_XA], …. Returns the GTID (`domain-server-seq`) and, for the
/// GTID that opens a prepared XA transaction, its xid.
pub(crate) fn gtid(server_id: u32, data: &[u8]) -> Result<(String, Option<String>)> {
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
    let xid = crate::binlog::xid_string(i64::from(format_id), &ids[..g], &ids[g..]);
    // Only the prepare opens the transaction; the completing GTID precedes XA COMMIT/ROLLBACK.
    Ok((gtid, (flags & FL_PREPARED_XA != 0).then_some(xid)))
}

/// The MariaDB arm of [`crate::source::position_rule`]: MariaDB >= 11.4
/// (`binlog_legacy_event_pos=OFF`) writes `log_pos = 0` for the events inside a transaction;
/// they are contiguous in the file, so the position advances by the event size. The offset is
/// trusted only until the first TABLE_MAP: the server omits ANNOTATE_ROWS events (holes in the
/// offsets) before it for replicas with GTID capability, so from there on `pos` may lag the
/// real offset until the next event with a real `log_pos`.
pub(crate) fn position_rule(header: BinlogEventHeader) -> Step {
    // The generic event code, as MariaDB numbers it too (`binlog`'s `ty::TABLE_MAP`).
    const TABLE_MAP: u8 = 0x13;
    Step::Advance {
        size: u64::from(header.event_size()),
        exact: header.event_type_raw() != TABLE_MAP,
    }
}
