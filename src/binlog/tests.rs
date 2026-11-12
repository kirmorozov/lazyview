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
        0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95,
        0x62,
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
    let other = se(
        1236,
        "A replica with the same server_uuid/server_id as this replica has connected to the source",
    );
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
    for et in [160u8, 161, 163, 164] {
        assert!(
            matches!(t.map_event(ev(et, 0, &[0; 4])).unwrap(), Item::Other),
            "type {et}"
        );
    }
    // INCIDENT reaches the caller as its own item: the copy set logs nothing.
    let et = ty::INCIDENT;
    assert!(
        matches!(
            t.map_event(ev(et, 0, &[0; 4])).unwrap(),
            Item::Incident { .. }
        ),
        "type {et}"
    );
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
fn schema_filtered_by_binlog_filters() {
    // No filters: nothing is excluded.
    assert!(!schema_filtered("", "", "app"));
    assert!(!schema_filtered(" , ", " , ", "app"));
    // do-db lists only the schemas that are binlogged.
    assert!(!schema_filtered("app,other", "", "app"));
    assert!(schema_filtered("app,other", "", "sales"));
    // Spaces around the entries are ignored.
    assert!(!schema_filtered(" app , other ", "", "app"));
    assert!(!schema_filtered("app , other", "", "other"));
    assert!(schema_filtered("app , other", "", "sales"));
    // ignore-db excludes the schemas it lists.
    assert!(!schema_filtered("app", "other", "app"));
    assert!(schema_filtered("", "sales", "sales"));
    assert!(schema_filtered("app,sales", "sales", "sales"));
    // ignore-db wins over do-db for the schema it lists.
    assert!(schema_filtered("app", "app", "app"));
    // The server compares names exactly, case-sensitively.
    assert!(schema_filtered("APP", "", "app"));
    assert!(!schema_filtered("APP", "app", "APP"));
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
fn default_program_names_in_messages() {
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

#[test]
fn probe_absent_only_for_unknown_variable() {
    let se = |code| {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: "x".into(),
            state: "HY000".into(),
        })
    };
    // 1193 ER_UNKNOWN_SYSTEM_VARIABLE: the server says the variable does not exist.
    assert!(connect::probe_var_is_absent(&se(1193)));
    // 2013 lost connection: a blip, so it must fail preflight, never read as "absent".
    assert!(!connect::probe_var_is_absent(&se(2013)));
}
