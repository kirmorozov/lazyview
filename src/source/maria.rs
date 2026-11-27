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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::binlog::events::{BinlogEventHeader, Event};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use crate::catalog::TableInfo;
use crate::source::{mysql, Position, SourceCaps, Step};

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

/// The MariaDB arm of [`crate::source::plan_refusal`] (issue #7): from MariaDB 11.4 on, the
/// UPDATE and DELETE row images leave VIRTUAL generated columns out and the row reader finds
/// columns by table position, so a REAL column positioned after a VIRTUAL one would be read
/// from the wrong place: a plan that reads one is refused until #7 is fixed. The two cases
/// the row images do carry are not refused: the VIRTUAL column itself (the planner treats it
/// as an expression over real columns) and STORED columns (the server writes them into the
/// images).
pub(crate) fn plan_refusal(
    version: &str,
    table: &TableInfo,
    referenced: &[bool],
) -> Option<String> {
    if !at_least_11_4(version) {
        return None;
    }
    let first_virtual = table
        .columns
        .iter()
        .position(|c| c.generated.as_ref().is_some_and(|g| !g.stored))?;
    let read = (first_virtual + 1..table.columns.len()).find(|&i| {
        table.columns[i].generated.is_none() && referenced.get(i).copied().unwrap_or(false)
    })?;
    Some(format!(
        "view reads column `{}` of `{}`.`{}`, a real column after its VIRTUAL generated column `{}`, which MariaDB {version} leaves out of the row images (issue #7): the view is refused until #7 is fixed",
        table.columns[read].name, table.schema, table.name, table.columns[first_virtual].name
    ))
}

/// Whether a `VERSION()` string is MariaDB 11.4 or later (`11.4.2-MariaDB`, `10.11.14-MariaDB`;
/// the `5.5.5-` replication prefix is skipped).
fn at_least_11_4(version: &str) -> bool {
    let v = version.strip_prefix("5.5.5-").unwrap_or(version);
    let mut parts = v
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u32>().unwrap_or(0));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    (major, minor) >= (11, 4)
}

// ---- The catalog loader (F6): MariaDB's answer to the five `information_schema` reads. The
// reads themselves are shared with `mysql.rs` (called into, never copied); MariaDB's own are
// how the rows are answered: plain-SQL generation expressions, and the default collation of a
// charset under `character_set_collations`.

/// The MariaDB table metadata: the shared `information_schema` reads, with
/// [`unescape_generation_expression`] as the generated-expression spelling and
/// [`default_collation`] as the collation probe.
/// MariaDB's answer to `Flavor::snapshot_position_is_watermark`: `true`. The pair names the point
/// of the binlog stream the snapshot reads as of, so everything at or before it is committed and
/// readable by that snapshot — waiting for it to reach the position already read from the binlog
/// is an exact fence (`visibility_snapshot_fence_exact` proves it under the widened commit
/// window). The MySQL family reports the raw write position instead, which is no watermark
/// (`mysql::snapshot_position_is_watermark`).
pub(crate) fn snapshot_position_is_watermark() -> bool {
    true
}

/// The position the source session's consistent snapshot includes: one transaction read at
/// REPEATABLE READ, committed whatever the status read did. `fail_first`/`try_no` are the
/// engine's snapshot-failure test hook (decision 4): a transient, non-connection failure.
/// The fence of the flavors whose `Flavor::snapshot_position_is_watermark` answers `true`
/// (MariaDB, [`snapshot_position_is_watermark`] below); `Ok(None)`: the server reports no
/// position, so it rules itself out (e.g. MySQL Community).
pub(crate) async fn snapshot_position(
    conn: &mut Conn,
    fail_first: Option<usize>,
    try_no: usize,
) -> Result<Option<Position>> {
    if fail_first.is_some_and(|n| try_no <= n) {
        bail!("injected snapshot fence failure");
    }
    // The one transaction lazyview keeps on a source (lv-super ruling 2026-11-26): read-only, no
    // row locks, and the only way MariaDB exposes the position readers can see
    // (`binlog_snapshot_*`). This is a visibility wait before reads, not a replay anchor, so a
    // plain position read (the raw write position passes commits in flight) would allow stale
    // rows nothing later corrects: `tests/visibility.rs` `visibility_snapshot_fence_exact`.
    // The session reads with READ COMMITTED; the snapshot needs REPEATABLE READ.
    conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await?;
    // The commit must run whatever the status read did: an open snapshot would poison the session.
    let read = snapshot_status(conn).await;
    let committed = conn.query_drop("COMMIT").await;
    let pos = read?;
    committed.context("committing the snapshot fence transaction")?;
    Ok(pos)
}

/// The binlog coordinates the session's consistent-snapshot transaction includes, as the two
/// `binlog_snapshot_%` status variables report them: `None` when the server reports no position
/// (an empty file counts as none). The caller owns the transaction: this reads, it does not
/// open or close one. (Also the position read of the engine's cascade seed, which owns its own
/// transaction.)
pub(crate) async fn snapshot_status(conn: &mut Conn) -> Result<Option<Position>> {
    let rows: Vec<(String, String)> = conn
        .query("SHOW SESSION STATUS LIKE 'binlog_snapshot_%'")
        .await
        .context("reading the binlog snapshot position")?;
    let mut file = None;
    let mut pos = None;
    for (k, v) in rows {
        match k.to_ascii_lowercase().as_str() {
            "binlog_snapshot_file" => file = Some(v),
            "binlog_snapshot_position" => pos = v.trim().parse::<u64>().ok(),
            _ => {}
        }
    }
    Ok(match (file, pos) {
        (Some(file), Some(pos)) if !file.is_empty() => Some(Position { file, pos }),
        _ => None,
    })
}

pub(crate) async fn load_table(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    caps: SourceCaps,
    default_collations: &mut HashMap<String, Option<String>>,
) -> Result<Option<TableInfo>> {
    let Some(rows) = mysql::read_columns(conn, schema, name).await? else {
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
    let columns = mysql::columns_from_rows(
        rows,
        caps,
        default_collations,
        unescape_generation_expression,
    );
    Ok(Some(
        mysql::finish_table(conn, schema, name, columns).await?,
    ))
}

/// MariaDB prints `GENERATION_EXPRESSION` as plain SQL, not string-escaped the way MySQL /
/// Percona print it: the text it gets is the SQL text it stands for (the identity that undoes
/// [`mysql::unescape_generation_expression`]).
pub(crate) fn unescape_generation_expression(text: &str) -> String {
    text.to_string()
}

/// How a JSON value binds as a parameter on MariaDB: it has no JSON cast type, so the JSON
/// text binds as a plain string (the MySQL family's answer, the cast, is in `mysql.rs`).
pub(crate) fn json_param() -> String {
    "?".to_string()
}

/// The collation `CAST(... AS CHAR CHARACTER SET charset)` yields on a MariaDB server: its
/// `character_set_collations` variable (a charset -> collation map) overrides the charset
/// default, and asking the server carries that override into the answer. None if unknown. Asked in
/// the shape [`crate::catalog::TEXT_PARAM_PREFIX`] binds a string in, so the answer is the collation
/// a bound value of this charset really carries.
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

// --- the cursor's rules: MariaDB's own ordering, the shared MySQL spelling ------------------

/// MariaDB orders cursors by the file's place in the index, then the offset inside it: `pos` is
/// a byte offset that restarts at every file (the stream's own [`position_rule`] moves it as
/// events fill the file), so it cannot order two files, while the file number grows with the
/// LSN. The rule is the MySQL family's one, reached through `mysql.rs`, never a copy.
pub(crate) fn cursor_key(p: &crate::source::Position) -> (u64, u64) {
    crate::source::mysql::position_key(p)
}

/// The text is the MySQL family's (`file:pos` and the optional `[g=…]`): a call into `mysql.rs`
/// for the split, never a copy. The set inside `[g=…]` is returned verbatim and read back by
/// MariaDB's own [`parse_gtid_set`].
pub(crate) fn parse(text: &str) -> Option<(crate::source::Position, Option<String>)> {
    crate::source::mysql::parse(text)
}

/// MariaDB's gtid-list grammar: comma-separated `domain-server-seq` (a high-water mark: every
/// transaction `1..=seq` for that `domain-server` pair, because MariaDB numbers them sequentially
/// per pair and `gtid_binlog_state` reports the last one written) or `domain-server-seq1-seq2` (an
/// explicit window `seq1..=seq2`, the shape `mariadb-binlog -j` / `SET @slave_connect_state` take).
/// The key is the `domain-server` pair. A token that is not a well-formed list entry is dropped,
/// never guessed. This is MariaDB's quirk: the MySQL family reads a bare number as one gno
/// ([`mysql::parse_gtid_set`]), MariaDB reads it as a prefix from 1.
pub(crate) fn parse_gtid_set(text: &str) -> mysql::GtidSet {
    let mut set = mysql::GtidSet::new();
    for tok in text.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        let mut f = tok.split('-');
        let (Some(dom), Some(server), Some(seq)) = (f.next(), f.next(), f.next()) else {
            continue;
        };
        let (Ok(d), Ok(s)) = (dom.trim().parse::<u32>(), server.trim().parse::<u32>()) else {
            continue;
        };
        let (lo, hi) = match (
            seq.trim().parse::<u64>(),
            f.next().map(|r| r.trim().parse::<u64>()),
        ) {
            // a single `seq` is the high-water mark: transactions 1..=seq.
            (Ok(a), None) => (1, a),
            // `seq1-seq2` is the explicit window seq1..=seq2.
            (Ok(a), Some(Ok(b))) => (a, b),
            _ => continue,
        };
        let entry = set.entry(format!("{d}-{s}")).or_default();
        mysql::merge_interval(entry, lo, hi);
    }
    set
}

/// MariaDB's merge of one transaction's GTID (`domain-server-seq`, the text
/// [`gtid`] renders from the stream's event) into the consumed set: the engine's call once per
/// committed transaction. The key is the `domain-server` pair, the value one number — a known
/// pair costs no allocation (map lookup + the in-place fast path of `mysql::merge_interval`),
/// only a first-time pair allocates its key. A token that is not a well-formed
/// `digits-digits-digits` returns `false` and changes nothing (dropped, never guessed).
pub(crate) fn merge_gtid(set: &mut mysql::GtidSet, gtid: &str) -> bool {
    let mut f = gtid.trim().split('-');
    let (Some(dom), Some(server), Some(seq), None) = (f.next(), f.next(), f.next(), f.next())
    else {
        return false;
    };
    let (Ok(d), Ok(s), Ok(n)) = (
        dom.trim().parse::<u32>(),
        server.trim().parse::<u32>(),
        seq.trim().parse::<u64>(),
    ) else {
        return false;
    };
    let key = format!("{d}-{s}");
    match set.get_mut(key.as_str()) {
        Some(ivals) => mysql::merge_interval(ivals, n, n),
        None => {
            let mut ivals = Vec::with_capacity(1);
            mysql::merge_interval(&mut ivals, n, n);
            set.insert(key, ivals);
        }
    }
    true
}

/// The consumed set as MariaDB gtid-list text: each interval its explicit window
/// `domain-server-lo-hi` (a bare `domain-server-n` would mean the high-water mark `1..=n`,
/// and the engine's intervals need not start at 1 when the reader joined mid-stream).
/// The shape [`parse_gtid_set`] and `SET @slave_connect_state` both read. std only.
pub(crate) fn render_list(set: &mysql::GtidSet) -> String {
    let mut out = String::new();
    for (pair, ivals) in set {
        for (lo, hi) in ivals {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(pair);
            out.push('-');
            out.push_str(&lo.to_string());
            out.push('-');
            out.push_str(&hi.to_string());
        }
    }
    out
}

/// MariaDB's GTID dump: the `domain-server-seq` list of the start cursor's `[g=…]` text, in the
/// shape `SET @slave_connect_state` (and `mariadb-binlog -j`) take.
///
/// MariaDB has no GTID payload on the dump request — that is MySQL's `COM_BINLOG_DUMP_GTID`. A
/// replica asks for a GTID start by setting the two session variables [`prepare_dump`] sends and
/// then requesting an ordinary dump with **no file name**; the server starts the stream after the
/// list. So the request itself needs nothing flavor-shaped (no raw-stream access), and only this
/// list and those statements are MariaDB's.
///
/// The list carries one high-water mark per `domain-server` pair, which is what the server reads
/// from it (the same shape `@@GLOBAL.gtid_binlog_state` prints): `[g=…]` was parsed by
/// [`parse_gtid_set`], so a pair's transactions are `1..=seq` and the dump begins after `seq`.
///
/// A text with no usable transaction is an `Err`, never an empty list: an empty list asks the
/// server for the start of its oldest binlog, which is never what a resume meant (the safety bar:
/// never a silent wrong resume). The caller reports this as a cursor problem.
///
/// Cost note (owner rule): called once per binlog connection, where the start is decided; the
/// event path gains no byte, no allocation, no branch. std only — the copy set must not need a
/// GTID crate for one render.
pub(crate) fn dump_gtid_list(text: &str) -> Result<String> {
    let mut out = String::new();
    for (pair, ranges) in parse_gtid_set(text) {
        // The key is the `domain-server` of a well-formed entry (see [`parse_gtid_set`]), so the
        // rendered line is digits and dashes only — safe to put in the statement below.
        let hi = ranges.iter().map(|&(_, hi)| hi).max().unwrap_or(0);
        if hi == 0 {
            continue; // seq 0 names no transaction
        }
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(&pair);
        out.push('-');
        out.push_str(&hi.to_string());
    }
    if out.is_empty() {
        bail!("the saved GTID list {text:?} leaves no transaction to resume after");
    }
    Ok(out)
}

/// Whether MariaDB can locate a binlog dump on this cursor's set at all.
///
/// `SET @slave_connect_state` is not a set the server filters events out of: it is the state the
/// dump starts *after*, and MariaDB answers it only for a state that names **every**
/// `domain-server` pair the node has written. In `contains_all_slave_gtid` (`sql/sql_repl.cc`), a
/// pair that a binlog file's `Gtid_list_log_event` carries but the requested state does not name
/// means "this slave needs that domain from its very first transaction", so the server searches
/// ever-older files; when none of the files it still has begins that domain, the dump dies with
/// `Could not find GTID state requested by slave in any binlog files. Probably the slave state is
/// too old and required binlog files have been purged` — and `@slave_gtid_strict_mode` never gets
/// a say, because the request dies while the file is being chosen.
///
/// So a reader whose set does not name a pair this node has written cannot anchor on GTIDs at all
/// — a writer with its own `gtid_domain_id`, a restored or multi-source history, a node whose
/// `gtid_binlog_state` was reloaded by hand. The reader of this set is the one that has to know:
/// a state the node cannot locate keeps the dump on its file/pos anchor, because the position asks
/// for nothing the server has to recognize, so the resume still happens. `node_state` is
/// `@@GLOBAL.gtid_binlog_state` and `saved` the cursor's `[g=…]` text; a node that has written
/// nothing has no pair to name, so it answers true.
pub fn dump_state_covers(node_state: &str, saved: &str) -> bool {
    dump_state_unnamed(node_state, saved).is_none()
}

/// The pair [`dump_state_covers`] would refuse a dump over: the first `domain-server` pair this
/// node's state carries that `saved` does not name, or `None` when the state can anchor a dump.
///
/// The engine logs it when one of its conditions holds a run back to the file/pos anchor, so an
/// operator — and a test that has to say which rule fired — reads the pair out of the line instead
/// of guessing from `set_bytes=0`. It is what `SHOW BINLOG EVENTS`' `Gtid_list` row names for that
/// domain. Public because the rule is the engine's own: a test that expects one anchor or the other
/// asks the product, rather than reading `sql_repl.cc` and writing its own copy of the check.
pub fn dump_state_unnamed(node_state: &str, saved: &str) -> Option<String> {
    let want = parse_gtid_set(saved);
    let named = |pair: &str| {
        want.get(pair)
            .is_some_and(|ivals| ivals.iter().any(|&(_, hi)| hi > 0))
    };
    // Sorted, so the line an operator reads (and a test asserts on) names the same pair every time
    // the same two states are compared: the map the parse yields has no order of its own.
    let mut missing: Vec<String> = parse_gtid_set(node_state)
        .into_iter()
        .filter(|(pair, ivals)| ivals.iter().any(|&(_, hi)| hi > 0) && !named(pair))
        .map(|(pair, _)| pair)
        .collect();
    missing.sort();
    missing.into_iter().next()
}

/// The two session statements that make MariaDB answer the ordinary `COM_BINLOG_DUMP` of
/// [`crate::binlog::stream`] (no file name, the minimum position) from a GTID position:
/// `@slave_connect_state` is the list to resume after, and `@slave_gtid_strict_mode = 1` makes the
/// server refuse a list whose transactions its binlog held and no longer has — a purged resume
/// point becomes the typed position-gone error instead of a dump from the oldest binlog (the same
/// reason a real GTID replica sets it). What strict mode does *not* refuse is a `domain-server`
/// pair the node has never written: that question is answered before the request, by
/// [`crate::source::gtid_covers`], never by trusting the packet. The third statement a real
/// replica sends, `@slave_gtid_ignore_duplicates`, decides what a *replica* applies and is
/// not read by the dump thread, so it stays at its default here.
///
/// Cost note: two statements per binlog connection, on the connect path only.
pub(crate) async fn prepare_dump(conn: &mut Conn, list: &str) -> Result<()> {
    conn.query_drop(format!("SET @slave_connect_state = '{list}'"))
        .await
        .with_context(|| format!("SET @slave_connect_state = '{list}'"))?;
    conn.query_drop("SET @slave_gtid_strict_mode = 1")
        .await
        .context("SET @slave_gtid_strict_mode = 1")
}

/// The MariaDB subset question for [`crate::source::gtid_covers`]: does this node's
/// `@@GLOBAL.gtid_binlog_state` include `saved`? Compared in flavor code over the parsed lists
/// (MariaDB has no server-side subset function to hand the question to), reusing the shared
/// interval rule through `mysql.rs`. A cursor with no set covers trivially.
/// MariaDB's GTID visibility wait (read-endpoint plan PR G, sibling of
/// [`mysql::gtid_wait`]): `MASTER_GTID_WAIT(<domain-server-seq list>, <ceil_s>)` — the wait is
/// one statement per read batch, timed by the server itself, never a client loop. `Ok(Some(Some(0)))`:
/// visible; anything else means "not approved", the caller owns the meaning (fail-closed).
pub(crate) async fn gtid_wait(
    conn: &mut Conn,
    list: &str,
    ceil_s: u64,
) -> Result<Option<Option<i64>>> {
    Ok(conn
        .exec_first(
            "SELECT MASTER_GTID_WAIT(?, ?)",
            (list.to_string(), ceil_s as i64),
        )
        .await?)
}

/// The read-endpoint pre-flight of MariaDB (PR G §6), on the read connection, asked only when
/// `LAZYVIEW_READ_DSN` is set. Refuses, never warns: an unfenceable reader would serve stale
/// rows with no signal.
pub(crate) async fn read_preflight(conn: &mut Conn, _ceil_s: u64) -> Result<()> {
    // The wait itself, with timeout 0 (§6.3): function existence, grants and the GTID state all
    // answer through it in the server's own words. `0-0-0` in the node's own domain id is
    // trivially executed, so a healthy node answers 0.
    let r: Option<Option<i64>> = conn
        .exec_first(
            "SELECT MASTER_GTID_WAIT(CONCAT(@@server_id, '-0-0'), 0)",
            (),
        )
        .await
        .context("the read endpoint cannot run MASTER_GTID_WAIT (grants? GTIDs?)")?;
    match r {
        Some(Some(0)) => {}
        Some(Some(n)) => bail!(
            "the read endpoint is refused: MASTER_GTID_WAIT answered {n} for a position it \
             should already have — unset LAZYVIEW_READ_DSN"
        ),
        Some(None) | None => bail!(
            "the read endpoint is refused: MASTER_GTID_WAIT answered NULL (GTID state unreadable) \
             — a replica without GTIDs cannot be fenced exactly; unset LAZYVIEW_READ_DSN"
        ),
    }
    // Galera (§6.4): the session-causal guard must exist; `init_read_session` sets it per read
    // connection below.
    if mysql::var_opt(conn, "SELECT CAST(@@GLOBAL.wsrep_on AS CHAR)")
        .await?
        .as_deref()
        == Some("1")
        && mysql::var_opt(conn, "SELECT CAST(@@GLOBAL.wsrep_sync_wait AS CHAR)")
            .await?
            .is_none()
    {
        bail!(
            "the read endpoint is refused: a Galera node without wsrep_sync_wait cannot \
                   give session-causal reads"
        );
    }
    Ok(())
}

/// Prepares the read connection (PR G §3, Galera row): `wsrep_sync_wait=1` gives every read on
/// it session-causal (monotonic) reads, server-side. The read connection carries nothing but
/// read batches (PR G §2), so asking it once per connection is exactly the plan's "for the
/// duration of one read batch" — zero statements per batch, zero per event. The provider's own
/// bound is asked of `wsrep_sync_wait_timeout`; where the server does not know that variable the
/// provider default stays (the batch's GTID wait above remains the freshness bound — a stalled
/// applier hangs the read, it never answers stale).
pub(crate) async fn init_read_session(conn: &mut Conn, ceil_s: u64) -> Result<()> {
    if mysql::var_opt(conn, "SELECT CAST(@@GLOBAL.wsrep_on AS CHAR)")
        .await?
        .as_deref()
        == Some("1")
    {
        conn.query_drop("SET SESSION wsrep_sync_wait = 1").await?;
        // The bound is asked, not demanded: an older provider without the variable keeps its
        // own default (see the doc line above).
        let _ = conn
            .query_drop(format!("SET SESSION wsrep_sync_wait_timeout = {ceil_s}"))
            .await;
    }
    Ok(())
}

pub(crate) async fn gtid_covers(conn: &mut Conn, saved: Option<&str>) -> Result<bool> {
    let Some(saved) = saved.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(true);
    };
    let saved = parse_gtid_set(saved);
    let st = crate::binlog::gtid_state(conn).await?;
    let state = st.binlog_state.unwrap_or_default();
    Ok(mysql::gtid_subset(&saved, &parse_gtid_set(&state)))
}

/// The 4-byte header floor is the shared one too.
pub(crate) fn normalize(p: &crate::source::Position) -> crate::source::Position {
    crate::source::mysql::normalize(p)
}

/// MariaDB answers `SHOW BINARY LOGS` the same way, so it asks through `mysql.rs`.
pub(crate) async fn can_resume(
    conn: &mut mysql_async::Conn,
    at: &crate::source::Position,
) -> anyhow::Result<bool> {
    crate::source::mysql::can_resume(conn, at).await
}
