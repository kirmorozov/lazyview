//! Integration tests for `lazyview::binlog` against real servers.
//!
//! Servers come from `LAZYVIEW_TEST_DSNS` through `common::servers()` (see
//! `docs/development.md`); when it is unset the tests print a notice and pass.
//! Every server is tested concurrently; each run uses fresh schemas and replica server ids, and
//! only items of its own schema are asserted (other sessions may write concurrently).
//!
//! Two tests of this binary really do run at once at `--test-threads=2`, so everything a
//! collection waits for or counts as its own — the marker table, the XA xids, the schema match —
//! is derived from that run's schema or from a per-run counter, never from a name the binary
//! shares (#272: a shared marker truncated a concurrent test's stream, shared xids admitted
//! another instance's XA groups as ours).

mod common;

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context, Result};
use lazyview::binlog::*;
use lazyview::source::{
    is_access_denied, is_duplicate_server_id, is_missing_binlog, Flavor, Item, Position, RowsKind,
};
use mysql_async::binlog::events::Event;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Row, Value};

// ---------------------------------------------------------------------------------------------
// harness

fn targets(test: &str) -> Vec<(String, String)> {
    match common::servers() {
        Some(servers) => servers
            .into_iter()
            .map(|s| (s.flavor, s.base_dsn))
            .collect(),
        None => {
            println!("{test}: LAZYVIEW_TEST_DSNS not set, skipping");
            vec![]
        }
    }
}

/// Runs `f` against every configured server concurrently and fails listing every failure.
async fn on_all_servers<F, Fut>(test: &str, f: F)
where
    F: Fn(String, String) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut handles = Vec::new();
    for (name, dsn) in targets(test) {
        let fut = f(name.clone(), dsn);
        handles.push((name, tokio::spawn(fut)));
    }
    let mut failures = Vec::new();
    for (name, h) in handles {
        match h.await {
            Ok(Ok(())) => println!("{test} [{name}]: ok"),
            Ok(Err(e)) => failures.push(format!("[{name}] {e:#}")),
            Err(e) => failures.push(format!("[{name}] panicked: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{test} failed:\n{}",
        failures.join("\n")
    );
}

fn seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        nanos ^ (u64::from(std::process::id()) << 20)
    })
}

/// Distinct replica server ids for this run (0x2xxx_xxxx, away from the test servers' own ids).
fn server_id() -> u32 {
    static NEXT: OnceLock<AtomicU32> = OnceLock::new();
    NEXT.get_or_init(|| AtomicU32::new(0x2000_0000 | (((seed() >> 8) as u32 & 0x0fff) << 16)))
        .fetch_add(1, Ordering::Relaxed)
}

/// XA xid material, unique per workload rather than per process. XA ids are server-global, so two
/// concurrent instances of one flow (a second test of this binary, or one scenario run twice) on
/// one server would otherwise use the same ones: the other instance's XA groups would pass for
/// ours, and `XA START` could be refused with XAER_DUPID (#272).
fn xa_nonce() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    // An odd multiplier is a bijection on the low 32 bits, so concurrent calls cannot repeat one.
    (seed()
        ^ N.fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15))
        & 0xffff_ffff
}

fn unique_schema(tag: &str) -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "lvbs_{tag}_{:x}_{}",
        seed() & 0xff_ffff_ffff,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

fn with_param(dsn: &str, p: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}{p}")
}

async fn connect(dsn: &str) -> Result<Conn> {
    // The harness's connect classifier (transient retries only, auth fails at once); the
    // replica session settings stay this file's own.
    let mut c = common::connect_opts(replica_opts(dsn)?)
        .await
        .context("connect")?;
    c.query_drop("SET time_zone = '+00:00'").await?;
    Ok(c)
}

// ---------------------------------------------------------------------------------------------
// stream collection

type Image = Option<Vec<Value>>;

#[derive(Debug, Clone, PartialEq)]
enum Ev {
    Rows {
        table: String,
        kind: RowsKind,
        rows: Vec<(Image, Image)>,
    },
    Stmt(String),
}

/// How a group ended.
#[derive(Debug, Clone, PartialEq)]
enum Close {
    Commit,
    Statement,
    XaPrepare(String),
    XaEnd(String, bool),
}

/// One transaction / statement of the test schema, as seen in the stream.
#[derive(Debug, Clone)]
struct Group {
    gtid: Option<String>,
    /// xid of an `XaStart` inside the group.
    xa_start: Option<String>,
    evs: Vec<Ev>,
    end: Position,
    kind: Close,
}

#[derive(Default)]
struct Seen {
    groups: Vec<Group>,
    /// (table, column count) of TABLE_MAP items of the test schema.
    maps: BTreeSet<(String, u64)>,
    /// First rows event of the test schema since the last observed rotation. Sampled by file, not
    /// by run: the tracker keeps a table map across one rotation, and another session's
    /// `FLUSH BINARY LOGS` on a shared server adds rotations at will (#272).
    sample: Option<Sample>,
    /// How often a rows event of the file just left was decoded again at the rotation (the
    /// table-map-retention check; `collect` guarantees it runs at least once per collection).
    retained: usize,
    /// Any rows event of the test schema, for the unknown-table-id check (which never reaches a
    /// table map, so its age does not matter).
    any_rows: Option<(u64, Event)>,
    /// The files the stream moved into, in order (first entry: where it started).
    files: Vec<String>,
    heartbeats: usize,
}

/// One rows event of the test schema with the rows as decoded when it was first seen, so it can
/// be decoded again once the stream has moved on: equal decodes show the TABLE_MAP is still known.
struct Sample {
    table_id: u64,
    table: String,
    event: Event,
    rows: Vec<(Image, Image)>,
}

/// JSON columns are compared semantically (MySQL binary JSON vs MariaDB text).
fn norm(table: &str, i: usize, v: Value) -> Value {
    match (table, i, v) {
        ("t", 9, Value::Bytes(b)) => {
            let j: serde_json::Value = serde_json::from_slice(&b).expect("valid JSON in column j");
            Value::Bytes(j.to_string().into_bytes())
        }
        (_, _, v) => v,
    }
}

fn image(table: &str, row: Option<BinlogRow>) -> Result<Image> {
    let Some(row) = row else { return Ok(None) };
    let vals = row
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, bv)| {
            Value::try_from(bv)
                .map(|v| norm(table, i, v))
                .map_err(|e| anyhow!("column {i}: {e}"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(vals))
}

/// `CREATE TABLE … SELECT` is logged with a server-generated column list; compare it by shape.
fn stmt_ev(schema: &str, query: String) -> Ev {
    if query.starts_with("CREATE TABLE") && query.contains(&format!("`{schema}`.`c`")) {
        Ev::Stmt("CTAS c".into())
    } else {
        Ev::Stmt(query)
    }
}

fn check_pos(pos: &Position, last: &mut Option<Position>) -> Result<()> {
    if let Some(prev) = last.as_ref() {
        ensure!(pos > prev, "positions not increasing: {prev} then {pos}");
    }
    *last = Some(pos.clone());
    Ok(())
}

/// Reads until a Statement of `schema` containing `marker` (its [`marker_table`] name, unique to
/// this collection: a name shared with another test would end it early), keeping only items of
/// `schema` (and XA transactions whose xid is in `xids`). Also checks invariants that hold for
/// every transaction in the stream (not only ours).
async fn collect(
    reader: &mut BinlogReader,
    schema: &str,
    xids: &[String],
    marker: &str,
) -> Result<Seen> {
    let deadline = Instant::now() + Duration::from_secs(60);
    // The schema as a quoted identifier: `contains(schema)` alone would also accept another
    // session's schema whose name merely has this one as a prefix (`lvbs_s_1` in `lvbs_s_11`).
    let quoted = format!("`{schema}`");
    let mut seen = Seen::default();
    let mut tables: HashMap<u64, (String, String)> = HashMap::new();
    let mut mapped_in_txn: BTreeSet<u64> = BTreeSet::new();
    let mut gtid: Option<String> = None;
    let mut xa_start: Option<String> = None;
    let mut cur: Vec<Ev> = Vec::new();
    let mut last: Option<Position> = None;
    let mut cur_file = reader.position().file.clone();
    seen.files.push(cur_file.clone());
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let item = tokio::time::timeout(left, reader.next())
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out waiting for marker {marker}; groups so far: {:#?}",
                    seen.groups
                )
            })??
            .ok_or_else(|| anyhow!("stream closed before marker"))?;
        // One comparison per event: a ROTATE — ours or another session's — moves the sampled
        // rows event into `prev_sample`, so the sample is always the first one of the file
        // before the current one (#272; the tracker keeps a table map across one rotation only).
        if reader.position().file != cur_file {
            // A rotation was just observed, so exactly one rotation has passed since the sample
            // was taken: its TABLE_MAP must still be known (the tracker keeps it across one), and
            // it must decode to what it decoded when it was seen — whatever other sessions did to
            // the server in between (#272: sampling at the start of the run let a concurrent
            // session's FLUSH BINARY LOGS push the sample out of the one-rotation promise).
            if let Some(sm) = seen.sample.take() {
                let late = reader
                    .rows(sm.table_id, &sm.event)
                    .with_context(|| {
                        format!("table map of {} lost across the rotation", sm.table_id)
                    })?
                    .into_iter()
                    .map(|(b, a)| Ok((image(&sm.table, b)?, image(&sm.table, a)?)))
                    .collect::<Result<Vec<_>>>()?;
                ensure!(
                    late == sm.rows,
                    "rows event of table id {} decodes differently after the rotation",
                    sm.table_id
                );
                ensure!(!late.is_empty(), "sample rows event decodes to no row");
                seen.retained += 1;
            }
            seen.sample = None;
            cur_file = reader.position().file.clone();
            seen.files.push(cur_file.clone());
        }
        // Closes the current group with `kind` at `pos`; kept when it is ours.
        let mut close = |kind: Close, pos: Position, ours: bool, cur: &mut Vec<Ev>| -> Result<()> {
            check_pos(&pos, &mut last)?;
            if ours || !cur.is_empty() {
                seen.groups.push(Group {
                    gtid: gtid.take(),
                    xa_start: xa_start.take(),
                    evs: std::mem::take(cur),
                    end: pos,
                    kind,
                });
            }
            ensure!(cur.is_empty());
            gtid = None;
            xa_start = None;
            mapped_in_txn.clear();
            Ok(())
        };
        match item {
            Item::Gtid { gtid: g } => {
                ensure!(
                    cur.is_empty(),
                    "GTID {g} inside an open transaction of the test schema"
                );
                mapped_in_txn.clear();
                xa_start = None;
                gtid = Some(g);
            }
            Item::XaStart { xid } => {
                ensure!(cur.is_empty(), "XaStart {xid} after rows");
                xa_start = Some(xid);
            }
            Item::TableMap {
                table_id,
                schema: s,
                table,
                columns,
            } => {
                if s == schema {
                    seen.maps.insert((table.clone(), columns));
                }
                tables.insert(table_id, (s, table));
                mapped_in_txn.insert(table_id);
            }
            Item::Rows {
                table_id,
                kind,
                event,
            } => {
                let (s, table) = tables
                    .get(&table_id)
                    .ok_or_else(|| anyhow!("rows for unmapped table id {table_id}"))?;
                ensure!(
                    mapped_in_txn.contains(&table_id),
                    "rows event without TABLE_MAP in the same transaction"
                );
                if s != schema {
                    continue;
                }
                let rows = reader
                    .rows(table_id, &event)?
                    .into_iter()
                    .map(|(b, a)| Ok((image(table, b)?, image(table, a)?)))
                    .collect::<Result<Vec<_>>>()?;
                if seen.any_rows.is_none() {
                    seen.any_rows = Some((table_id, event.clone()));
                }
                if seen.sample.is_none() {
                    // Only the first rows event of a file is kept: the clone is once per
                    // rotation, not per event.
                    seen.sample = Some(Sample {
                        table_id,
                        table: table.clone(),
                        event,
                        rows: rows.clone(),
                    });
                }
                cur.push(Ev::Rows {
                    table: table.clone(),
                    kind,
                    rows,
                });
            }
            Item::Commit { pos } => close(Close::Commit, pos, false, &mut cur)?,
            Item::XaPrepare { xid, pos } => {
                let ours = xids.contains(&xid);
                close(Close::XaPrepare(xid), pos, ours, &mut cur)?
            }
            Item::XaEnd { xid, commit, pos } => {
                ensure!(cur.is_empty(), "XaEnd {xid} after rows");
                let ours = xids.contains(&xid);
                close(Close::XaEnd(xid, commit), pos, ours, &mut cur)?
            }
            Item::Statement {
                schema: _,
                query,
                pos,
            } => {
                ensure!(
                    cur.is_empty(),
                    "statement {query:?} inside an open transaction"
                );
                let ours = query.contains(&quoted);
                // Only this test's own marker ends the collection: on a shared server another
                // session's marker used to stop it here, dropping the rest of our stream (#272).
                let done = ours && query.contains(marker);
                cur.extend(ours.then(|| stmt_ev(schema, query)));
                // A CREATE … SELECT continues with rows + Commit in the same GTID transaction.
                close(Close::Statement, pos, ours, &mut cur)?;
                if done {
                    return Ok(seen);
                }
            }
            Item::Heartbeat => seen.heartbeats += 1,
            Item::Incident { .. } => {}
            Item::Other => {}
        }
    }
}

// ---------------------------------------------------------------------------------------------
// the workload

struct Expect {
    /// Whether table maps carry signedness (binlog_row_metadata MINIMAL/FULL). MariaDB defaults
    /// to NO_LOG: unsigned integers then decode as signed values of the column's width.
    signedness: bool,
}

impl Expect {
    fn u32(&self, v: u32) -> Value {
        if self.signedness {
            Value::Int(i64::from(v))
        } else {
            Value::Int(i64::from(v as i32))
        }
    }
    fn u64(&self, v: u64) -> Value {
        if self.signedness {
            Value::UInt(v)
        } else {
            Value::Int(v as i64)
        }
    }
}

fn b(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

fn json(s: &str) -> Value {
    let j: serde_json::Value = serde_json::from_str(s).unwrap();
    Value::Bytes(j.to_string().into_bytes())
}

fn ints(v: &[Option<i64>]) -> Vec<Value> {
    v.iter()
        .map(|x| x.map(Value::Int).unwrap_or(Value::NULL))
        .collect()
}

fn rows_ev(table: &str, kind: RowsKind, rows: Vec<(Image, Image)>) -> Ev {
    Ev::Rows {
        table: table.into(),
        kind,
        rows,
    }
}

fn ins(v: &[Option<i64>]) -> (Image, Image) {
    (None, Some(ints(v)))
}

const T_DDL: &str = "(id INT UNSIGNED NOT NULL PRIMARY KEY, u INT UNSIGNED NULL, big BIGINT NULL, \
    ubig BIGINT UNSIGNED NULL, amount DECIMAL(20,6) NULL, dt DATETIME(6) NULL, ts TIMESTAMP(3) NULL, \
    e ENUM('red','green','blue') NULL, s SET('a','b','c') NULL, j JSON NULL, \
    vc VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL, b BLOB NULL) ENGINE=InnoDB";

const MARKER: &str = "zz_marker_done";

/// The marker table that ends one collection, named for the schema it ends. One shared name would
/// let any other session on a shared server end it (#272).
fn marker_table(schema: &str) -> String {
    let mut h = 0xcbf2_9ce4_8422_2325u64; // FNV-1a: stable for a given schema
    for b in schema.as_bytes() {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
    }
    format!("{MARKER}_{h:x}")
}

#[derive(Debug, Clone)]
struct ExpGroup {
    evs: Vec<Ev>,
    /// Starts with its own GTID (false: continuation of a CREATE … SELECT transaction).
    gtid: bool,
    xa_start: Option<String>,
    kind: Close,
}

/// The canonical xid `lazyview::binlog` reports.
fn xid_canon(gtrid: &[u8], bqual: &[u8], format_id: i64) -> String {
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    format!("X'{}',X'{}',{format_id}", hex(gtrid), hex(bqual))
}

struct Workload {
    schema: String,
    flavor: Flavor,
    start: Position,
    end: Position,
    groups: Vec<ExpGroup>,
    /// MariaDB: `@@last_gtid` after each group that has its own GTID.
    gtids: Vec<String>,
    /// Canonical ids of the workload's XA transactions.
    xids: Vec<String>,
    /// Name of the marker table that ends this workload's own collection.
    marker: String,
    server_uuid: String,
}

async fn run_workload(w: &mut Conn, w2: &mut Conn, tag: &str) -> Result<Workload> {
    let info = server_info(w).await?;
    let flavor = info.flavor;
    let maria = flavor == Flavor::MariaDb;
    let meta: Option<String> = w.query_first("SELECT @@GLOBAL.binlog_row_metadata").await?;
    let ex = Expect {
        signedness: matches!(meta.as_deref(), Some("MINIMAL" | "FULL")),
    };
    let server_uuid: String = if maria {
        String::new()
    } else {
        w.query_first("SELECT @@server_uuid")
            .await?
            .unwrap_or_default()
    };
    // Whether committed transactions carry a GTID: MariaDB always; MySQL / Percona follow the
    // server's gtid_mode (ON and ON_PERMISSIVE assign one, OFF and OFF_PERMISSIVE do not).
    let has_gtid = if maria {
        true
    } else {
        let mode: Option<String> = w.query_first("SELECT @@GLOBAL.gtid_mode").await?;
        mode.is_some_and(|m| {
            m.eq_ignore_ascii_case("ON") || m.eq_ignore_ascii_case("ON_PERMISSIVE")
        })
    };
    let s = unique_schema(tag);
    w.query_drop(format!("CREATE DATABASE `{s}`")).await?;
    w.query_drop(format!("CREATE TABLE `{s}`.t {T_DDL}"))
        .await?;
    w.query_drop(format!(
        "CREATE TABLE `{s}`.k (id INT NOT NULL PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB"
    ))
    .await?;
    w.query_drop(format!(
        "CREATE TABLE `{s}`.m (id INT NOT NULL PRIMARY KEY) ENGINE=MyISAM"
    ))
    .await?;

    let start = current_position(w).await?;
    let mut groups: Vec<ExpGroup> = Vec::new();
    let mut gtids: Vec<String> = Vec::new();
    // Records the expected items of one group (and MariaDB's @@last_gtid for it).
    macro_rules! group {
        ($evs:expr) => {{
            let evs: Vec<Ev> = $evs;
            let kind = if matches!(evs.as_slice(), [Ev::Stmt(_)]) {
                Close::Statement
            } else {
                Close::Commit
            };
            group!(evs, kind, None)
        }};
        ($evs:expr, $kind:expr, $xa:expr) => {{
            groups.push(ExpGroup {
                evs: $evs,
                gtid: has_gtid,
                xa_start: $xa,
                kind: $kind,
            });
            if maria {
                let g: Option<String> = w.query_first("SELECT @@last_gtid").await?;
                gtids.push(g.unwrap_or_default());
            }
        }};
    }

    let row1 = vec![
        ex.u32(1),
        ex.u32(4_294_967_295),
        Value::Int(i64::MIN),
        ex.u64(u64::MAX),
        b("12345678901234.123456"),
        Value::Date(2024, 2, 29, 13, 14, 15, 123_456),
        b("1709212455.789000"),    // TIMESTAMP: epoch seconds.fraction
        Value::Int(2),             // ENUM: index
        Value::Bytes(vec![0b101]), // SET: bitmap
        json(r#"{"k":[1,2,{"x":null}],"s":"é"}"#),
        b("héllo 🎉 wörld"),
        Value::Bytes(vec![0, 255, 16]),
    ];
    let mut row1u = row1.clone();
    row1u[4] = b("-0.000001");
    row1u[7] = Value::NULL;
    row1u[8] = Value::Bytes(vec![0]);
    row1u[9] = json(r#"{"k":[1,2,{"x":null}],"s":"é","n":5}"#);
    row1u[10] = b("updated ✓");
    let mut row2 = vec![Value::NULL; 12];
    row2[0] = ex.u32(2);

    // 0: two inserts in one transaction (all types, then all NULLs)
    w.query_drop("BEGIN").await?;
    w.query_drop(format!(
        "INSERT INTO `{s}`.t VALUES (1, 4294967295, -9223372036854775808, 18446744073709551615, \
         12345678901234.123456, '2024-02-29 13:14:15.123456', '2024-02-29 13:14:15.789', 'green', 'a,c', \
         '{{\"k\": [1, 2, {{\"x\": null}}], \"s\": \"é\"}}', 'héllo 🎉 wörld', X'00FF10')"
    ))
    .await?;
    w.query_drop(format!("INSERT INTO `{s}`.t (id) VALUES (2)"))
        .await?;
    w.query_drop("COMMIT").await?;
    group!(vec![
        rows_ev("t", RowsKind::Insert, vec![(None, Some(row1.clone()))]),
        rows_ev("t", RowsKind::Insert, vec![(None, Some(row2.clone()))]),
    ]);
    // 1: multi-row statement (autocommit)
    w.query_drop(format!(
        "INSERT INTO `{s}`.k VALUES (1, 10), (2, 20), (3, 30)"
    ))
    .await?;
    group!(vec![rows_ev(
        "k",
        RowsKind::Insert,
        vec![
            ins(&[Some(1), Some(10)]),
            ins(&[Some(2), Some(20)]),
            ins(&[Some(3), Some(30)])
        ]
    )]);
    // 2: update + delete on two tables
    w.query_drop("BEGIN").await?;
    w.query_drop(format!(
        "UPDATE `{s}`.t SET amount = -0.000001, e = NULL, s = '', j = JSON_SET(j, '$.n', 5), vc = 'updated ✓' WHERE id = 1"
    ))
    .await?;
    w.query_drop(format!("DELETE FROM `{s}`.k WHERE id = 2"))
        .await?;
    w.query_drop("COMMIT").await?;
    group!(vec![
        rows_ev(
            "t",
            RowsKind::Update,
            vec![(Some(row1.clone()), Some(row1u.clone()))]
        ),
        rows_ev(
            "k",
            RowsKind::Delete,
            vec![(Some(ints(&[Some(2), Some(20)])), None)]
        ),
    ]);
    // rolled back: must not appear at all
    w.query_drop("BEGIN").await?;
    w.query_drop(format!("INSERT INTO `{s}`.k VALUES (99, 99)"))
        .await?;
    w.query_drop(format!("UPDATE `{s}`.t SET vc = 'nope' WHERE id = 1"))
        .await?;
    w.query_drop(format!("DELETE FROM `{s}`.k WHERE id = 1"))
        .await?;
    w.query_drop("ROLLBACK").await?;
    // 3: DDL
    let alter = format!("ALTER TABLE `{s}`.k ADD COLUMN extra INT NULL");
    w.query_drop(&alter).await?;
    group!(vec![Ev::Stmt(alter)]);
    // 4: multi-row update
    w.query_drop(format!("UPDATE `{s}`.k SET n = n + 1"))
        .await?;
    group!(vec![rows_ev(
        "k",
        RowsKind::Update,
        vec![
            (
                Some(ints(&[Some(1), Some(10), None])),
                Some(ints(&[Some(1), Some(11), None]))
            ),
            (
                Some(ints(&[Some(3), Some(30), None])),
                Some(ints(&[Some(3), Some(31), None]))
            ),
        ],
    )]);
    // 5: delete of the all-NULL row
    w.query_drop(format!("DELETE FROM `{s}`.t WHERE id = 2"))
        .await?;
    group!(vec![rows_ev(
        "t",
        RowsKind::Delete,
        vec![(Some(row2.clone()), None)]
    )]);
    // 6: MySQL: compressed TRANSACTION_PAYLOAD (inner events have log_pos 0)
    if !maria {
        w.query_drop("SET SESSION binlog_transaction_compression = ON")
            .await?;
    }
    w.query_drop("BEGIN").await?;
    w.query_drop(format!("INSERT INTO `{s}`.k VALUES (4, 40, 7)"))
        .await?;
    w.query_drop(format!("UPDATE `{s}`.k SET n = 41 WHERE id = 4"))
        .await?;
    w.query_drop("COMMIT").await?;
    if !maria {
        w.query_drop("SET SESSION binlog_transaction_compression = OFF")
            .await?;
    }
    group!(vec![
        rows_ev(
            "k",
            RowsKind::Insert,
            vec![ins(&[Some(4), Some(40), Some(7)])]
        ),
        rows_ev(
            "k",
            RowsKind::Update,
            vec![(
                Some(ints(&[Some(4), Some(40), Some(7)])),
                Some(ints(&[Some(4), Some(41), Some(7)]))
            )],
        ),
    ]);
    // 7: non-transactional table: QUERY "COMMIT" instead of XID
    w.query_drop(format!("INSERT INTO `{s}`.m VALUES (1)"))
        .await?;
    group!(vec![rows_ev("m", RowsKind::Insert, vec![ins(&[Some(1)])])]);
    // 8 + 9: CREATE … SELECT: statement, then rows + Commit in the same transaction
    w.query_drop(format!(
        "CREATE TABLE `{s}`.c (PRIMARY KEY (id)) AS SELECT id, n FROM `{s}`.k ORDER BY id"
    ))
    .await?;
    group!(vec![Ev::Stmt("CTAS c".into())]);
    groups.push(ExpGroup {
        evs: vec![rows_ev(
            "c",
            RowsKind::Insert,
            vec![
                ins(&[Some(1), Some(11)]),
                ins(&[Some(3), Some(31)]),
                ins(&[Some(4), Some(41)]),
            ],
        )],
        gtid: false,
        xa_start: None,
        kind: Close::Commit,
    });
    // binlog rotation (not logged itself)
    w.query_drop("FLUSH BINARY LOGS").await?;
    // 10-13: two-phase XA commit, with another session's transaction between PREPARE and
    // COMMIT: XaStart, rows, XaPrepare … XaEnd(commit)
    let xt = format!("{:x}", xa_nonce());
    let g1 = format!("lvc{xt}");
    let x1 = xid_canon(g1.as_bytes(), b"", 1);
    w.query_drop(format!("XA START '{g1}'")).await?;
    w.query_drop(format!("INSERT INTO `{s}`.k VALUES (5, 50, NULL)"))
        .await?;
    w.query_drop(format!("XA END '{g1}'")).await?;
    w.query_drop(format!("XA PREPARE '{g1}'")).await?;
    group!(
        vec![rows_ev(
            "k",
            RowsKind::Insert,
            vec![ins(&[Some(5), Some(50), None])]
        )],
        Close::XaPrepare(x1.clone()),
        Some(x1.clone())
    );
    w2.query_drop(format!("INSERT INTO `{s}`.m VALUES (2)"))
        .await?;
    groups.push(ExpGroup {
        evs: vec![rows_ev("m", RowsKind::Insert, vec![ins(&[Some(2)])])],
        gtid: has_gtid,
        xa_start: None,
        kind: Close::Commit,
    });
    if maria {
        let g: Option<String> = w2.query_first("SELECT @@last_gtid").await?;
        gtids.push(g.unwrap_or_default());
    }
    w.query_drop(format!("XA COMMIT '{g1}'")).await?;
    group!(vec![], Close::XaEnd(x1.clone(), true), None);
    // 14-15: two-phase XA rollback (bqual and formatID given): the rows never become visible
    let g2 = format!("lvr{xt}");
    let x2 = xid_canon(g2.as_bytes(), b"b q", 7);
    let spec2 = format!("'{g2}','b q',7");
    w.query_drop(format!("XA START {spec2}")).await?;
    w.query_drop(format!("INSERT INTO `{s}`.k VALUES (6, 60, NULL)"))
        .await?;
    w.query_drop(format!("XA END {spec2}")).await?;
    w.query_drop(format!("XA PREPARE {spec2}")).await?;
    group!(
        vec![rows_ev(
            "k",
            RowsKind::Insert,
            vec![ins(&[Some(6), Some(60), None])]
        )],
        Close::XaPrepare(x2.clone()),
        Some(x2.clone())
    );
    w.query_drop(format!("XA ROLLBACK {spec2}")).await?;
    group!(vec![], Close::XaEnd(x2.clone(), false), None);
    // 16: one-phase XA commit (hex xid): a plain Commit; MySQL still reports XaStart first
    let g3 = format!("lvo{xt}");
    let x3 = xid_canon(g3.as_bytes(), b"", 1);
    let spec3 = format!(
        "X'{}'",
        g3.bytes().map(|b| format!("{b:02X}")).collect::<String>()
    );
    w.query_drop(format!("XA START {spec3}")).await?;
    w.query_drop(format!("INSERT INTO `{s}`.k VALUES (7, 70, NULL)"))
        .await?;
    w.query_drop(format!("XA END {spec3}")).await?;
    w.query_drop(format!("XA COMMIT {spec3} ONE PHASE")).await?;
    group!(
        vec![rows_ev(
            "k",
            RowsKind::Insert,
            vec![ins(&[Some(7), Some(70), None])]
        )],
        Close::Commit,
        (!maria).then(|| x3.clone())
    );
    // 11: marker DDL ends the collection; its name belongs to this schema alone.
    let mt = marker_table(&s);
    let marker = format!("CREATE TABLE `{s}`.{mt} (id INT)");
    w.query_drop(&marker).await?;
    group!(vec![Ev::Stmt(marker)]);

    let end = current_position(w).await?;
    Ok(Workload {
        schema: s,
        flavor,
        start,
        end,
        groups,
        gtids,
        xids: vec![x1, x2, x3],
        marker: mt,
        server_uuid,
    })
}

fn flat(groups: &[Group]) -> Vec<Ev> {
    groups.iter().flat_map(|g| g.evs.clone()).collect()
}

async fn check_gtids(w: &mut Conn, wl: &Workload, groups: &[Group]) -> Result<()> {
    let mut got = Vec::new();
    for (g, e) in groups.iter().zip(&wl.groups) {
        ensure!(
            g.gtid.is_some() == e.gtid,
            "GTID presence {:?} for group {g:?}",
            g.gtid
        );
        got.extend(g.gtid.clone());
    }
    match wl.flavor {
        Flavor::MariaDb => ensure!(
            got == wl.gtids,
            "GTIDs {got:?} != @@last_gtid values {:?}",
            wl.gtids
        ),
        Flavor::MySql | Flavor::Percona => {
            let mut prev = 0u64;
            for g in &got {
                let (uuid, gno) = g.rsplit_once(':').ok_or_else(|| anyhow!("bad GTID {g}"))?;
                ensure!(
                    uuid == wl.server_uuid,
                    "GTID {g} not from server {}",
                    wl.server_uuid
                );
                let gno: u64 = gno.parse()?;
                ensure!(gno > prev, "GTID numbers not increasing: {got:?}");
                prev = gno;
                let ok: Option<i64> = w
                    .query_first(format!("SELECT GTID_SUBSET('{g}', @@GLOBAL.gtid_executed)"))
                    .await?;
                ensure!(ok == Some(1), "GTID {g} not in gtid_executed");
            }
        }
    }
    Ok(())
}

async fn stream_scenario(dsn: String) -> Result<()> {
    let mut w = connect(&dsn).await?;
    let mut w2 = connect(&dsn).await?;
    let opts: Opts = replica_opts(&dsn)?;
    let wl = run_workload(&mut w, &mut w2, "s").await?;
    let s = wl.schema.clone();
    let want: Vec<Ev> = wl.groups.iter().flat_map(|g| g.evs.clone()).collect();

    let mut r = BinlogReader::connect(
        opts.clone(),
        server_id(),
        wl.flavor,
        &wl.start,
        Duration::from_secs(1),
    )
    .await?;
    ensure!(r.flavor() == wl.flavor);
    let seen = collect(&mut r, &s, &wl.xids, &wl.marker).await?;
    let got = flat(&seen.groups);
    ensure!(
        got == want,
        "stream mismatch\n got: {got:#?}\nwant: {want:#?}"
    );
    ensure!(
        seen.groups.len() == wl.groups.len(),
        "{} groups, want {}",
        seen.groups.len(),
        wl.groups.len()
    );
    for (i, (g, e)) in seen.groups.iter().zip(&wl.groups).enumerate() {
        ensure!(
            g.evs == e.evs,
            "group {i} boundaries differ: {:?} vs {:?}",
            g.evs,
            e.evs
        );
        ensure!(
            g.kind == e.kind,
            "group {i}: ended by {:?}, want {:?}",
            g.kind,
            e.kind
        );
        ensure!(
            g.xa_start == e.xa_start,
            "group {i}: XaStart {:?}, want {:?}",
            g.xa_start,
            e.xa_start
        );
        ensure!(
            g.end > wl.start && g.end <= wl.end,
            "group {i} position {} outside {}..{}",
            g.end,
            wl.start,
            wl.end
        );
    }
    let (first, last) = (&seen.groups[0].end, &seen.groups.last().unwrap().end);
    ensure!(
        common::file_seq(&last.file) > common::file_seq(&first.file),
        "no rotation seen between {first} and {last}"
    );
    check_gtids(&mut w, &wl, &seen.groups).await?;
    let want_maps: BTreeSet<(String, u64)> = [("t", 12), ("k", 2), ("k", 3), ("m", 1), ("c", 2)]
        .iter()
        .map(|(t, n)| (t.to_string(), *n))
        .collect();
    ensure!(
        seen.maps == want_maps,
        "table maps {:?} != {want_maps:?}",
        seen.maps
    );

    // `collect` decoded a rows event of the file it just left at every rotation it observed
    // (see the rotation arm there); at least one rotation must have been crossed with rows of
    // ours behind it — our own FLUSH BINARY LOGS guarantees that.
    ensure!(
        seen.retained > 0,
        "no rotation crossed with rows of {s} behind it: files {:?}, start {} .. {}",
        seen.files,
        wl.start,
        r.position()
    );
    // rows() without a table map fails cleanly.
    let (_tid, ev) = seen.any_rows.ok_or_else(|| anyhow!("no rows event seen"))?;
    let err = r
        .rows(0x00ab_cdef, &ev)
        .expect_err("decoding with an unknown table id must fail");
    ensure!(
        err.to_string().contains("TABLE_MAP"),
        "unexpected error: {err:#}"
    );

    // Idle stream: a heartbeat (1 s period) arrives whenever the server has nothing to send for
    // a second. Other sessions may keep a shared server busy (no idle second, so no heartbeat);
    // only a 3 s silence without a heartbeat is a failure.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut busy = 0usize;
    loop {
        match tokio::time::timeout(Duration::from_secs(3), r.next()).await {
            Err(_) => bail!("stream idle for 3s without a heartbeat (period 1s)"),
            Ok(item) => match item? {
                Some(Item::Heartbeat) => break,
                Some(Item::Commit { .. } | Item::Statement { .. }) => busy += 1,
                _ => {}
            },
        }
        if Instant::now() > deadline {
            ensure!(
                busy > 30,
                "no heartbeat within 30s although only {busy} transactions arrived"
            );
            println!("note: server busy ({busy} transactions in 30s), heartbeat not observable");
            break;
        }
    }
    r.close().await?;

    // MySQL family: the compressed transaction really was a Transaction_payload.
    if wl.flavor.is_mysql_family() {
        let evs: Vec<Row> = w
            .query(format!(
                "SHOW BINLOG EVENTS IN '{}' FROM {}",
                wl.start.file, wl.start.pos
            ))
            .await?;
        let payloads = evs
            .iter()
            .filter(|r| r.get::<String, _>(2).as_deref() == Some("Transaction_payload"))
            .count();
        ensure!(
            payloads >= 1,
            "no Transaction_payload event after {}",
            wl.start
        );
    }

    // Resuming from every group boundary yields exactly the remaining items.
    for (i, g) in seen.groups.iter().enumerate().take(seen.groups.len() - 1) {
        let mut r2 = BinlogReader::connect(
            opts.clone(),
            server_id(),
            wl.flavor,
            &g.end,
            Duration::from_secs(5),
        )
        .await?;
        let rest = collect(&mut r2, &s, &wl.xids, &wl.marker)
            .await
            .with_context(|| format!("resuming after group {i} at {}", g.end))?;
        r2.close().await?;
        let want = flat(&seen.groups[i + 1..]);
        ensure!(
            flat(&rest.groups) == want,
            "resume after group {i} at {}: got {:#?}\nwant {want:#?}",
            g.end,
            rest.groups
        );
        let ends: Vec<_> = rest.groups.iter().map(|g| g.end.clone()).collect();
        let want_ends: Vec<_> = seen.groups[i + 1..].iter().map(|g| g.end.clone()).collect();
        ensure!(
            ends == want_ends,
            "resume after group {i}: positions {ends:?} != {want_ends:?}"
        );
        let kinds: Vec<_> = rest
            .groups
            .iter()
            .map(|g| (g.kind.clone(), g.xa_start.clone()))
            .collect();
        let want_kinds: Vec<_> = seen.groups[i + 1..]
            .iter()
            .map(|g| (g.kind.clone(), g.xa_start.clone()))
            .collect();
        ensure!(
            kinds == want_kinds,
            "resume after group {i}: endings {kinds:?} != {want_kinds:?}"
        );
        let gt: Vec<_> = rest.groups.iter().map(|g| g.gtid.clone()).collect();
        let want_gt: Vec<_> = seen.groups[i + 1..]
            .iter()
            .map(|g| g.gtid.clone())
            .collect();
        ensure!(
            gt == want_gt,
            "resume after group {i}: gtids {gt:?} != {want_gt:?}"
        );
    }

    w.query_drop(format!("DROP DATABASE `{s}`")).await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// tests

/// Inserts / updates / deletes in transactions, a rolled back transaction, DDL, all common
/// types, multi-row statements, a compressed transaction (MySQL), a MyISAM write, CREATE …
/// SELECT, a binlog rotation and XA transactions (two-phase commit with another transaction in
/// between, two-phase rollback, one-phase); then exact items, rows, GTIDs and positions,
/// heartbeats, and resuming from every boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_items_rows_positions_and_resume() {
    on_all_servers("stream_items_rows_positions_and_resume", |_name, dsn| {
        stream_scenario(dsn)
    })
    .await;
}

/// The same scenario twice at once on one server, inside one test: the shape `--test-threads=2`
/// gives when two tests of this binary overlap (#272). Both instances share the server, so each
/// must collect exactly the stream of its own schema: a marker, an xid or a name shared with the
/// other instance truncates or corrupts the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_two_concurrent_scenarios_are_isolated() {
    on_all_servers(
        "stream_two_concurrent_scenarios_are_isolated",
        |_name, dsn| async move {
            let (a, b) = tokio::join!(stream_scenario(dsn.clone()), stream_scenario(dsn));
            a.context("scenario 1")?;
            b.context("scenario 2")?;
            Ok(())
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_server_checks_and_missing_binlog() {
    on_all_servers(
        "stream_server_checks_and_missing_binlog",
        |name, dsn| async move {
            let mut c = connect(&dsn).await?;
            let info = server_info(&mut c).await?;
            let want = if name.starts_with("mariadb") {
                Flavor::MariaDb
            } else if name.starts_with("percona") {
                // Percona's own flavor, named by its version_comment.
                Flavor::Percona
            } else {
                Flavor::MySql
            };
            ensure!(
                info.flavor == want,
                "flavor {:?} for {}",
                info.flavor,
                info.version
            );

            let pos = current_position(&mut c).await?;
            ensure!(
                pos.pos >= 4 && common::file_seq(&pos.file) > 0,
                "odd position {pos}"
            );
            ensure!(
                binlog_exists(&mut c, &pos.file).await?,
                "{} not listed",
                pos.file
            );
            let (base, _) = pos.file.rsplit_once('.').unwrap();
            let gone = Position {
                file: format!("{base}.999999"),
                pos: 4,
            };
            ensure!(
                !binlog_exists(&mut c, &gone.file).await?,
                "{} listed",
                gone.file
            );

            let err = match BinlogReader::connect(
                replica_opts(&dsn)?,
                server_id(),
                info.flavor,
                &gone,
                Duration::from_secs(5),
            )
            .await
            {
                Ok(_) => bail!("streaming from {gone} succeeded"),
                Err(e) => e,
            };
            ensure!(
                is_missing_binlog(&err),
                "not recognised as missing binlog: {err:#}"
            );
            ensure!(
                !is_access_denied(&err) && !is_duplicate_server_id(&err),
                "missing binlog misclassified: {err:#}"
            );

            // A position past the end of the file is a different (non-"missing") error.
            let past = Position {
                file: pos.file.clone(),
                pos: pos.pos + 1_000_000_000,
            };
            let res = async {
                let mut r = BinlogReader::connect(
                    replica_opts(&dsn)?,
                    server_id(),
                    info.flavor,
                    &past,
                    Duration::from_secs(5),
                )
                .await?;
                r.next().await
            }
            .await;
            match res {
                Ok(item) => bail!("streaming from {past} returned {item:?}"),
                Err(e) => ensure!(
                    !is_missing_binlog(&e),
                    "position past EOF reported as missing binlog: {e:#}"
                ),
            }
            Ok(())
        },
    )
    .await;
}

/// PR B: a start carrying `g=` opens a GTID dump on the MySQL family — file and position are
/// zeroed and the server starts after the set: rows already in the set are not delivered,
/// rows committed after it are. A blank start keeps the old file/pos path (the GTID-off
/// world), MariaDB keeps it regardless (its dump is the flavor module's own request, PR C),
/// and a text with no usable transaction is refused before the request — never answered with
/// a dump from the start of the oldest binlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_resume_by_gtid() {
    on_all_servers("stream_resume_by_gtid", |_name, dsn| async move {
        let mut c = connect(&dsn).await?;
        let info = server_info(&mut c).await?;
        let s = unique_schema("gtid");
        c.query_drop(format!("CREATE DATABASE `{s}`")).await?;
        c.query_drop(format!(
            "CREATE TABLE `{s}`.items (id INT PRIMARY KEY, v INT NOT NULL)"
        ))
        .await?;
        c.query_drop(format!("INSERT INTO `{s}`.items VALUES (1,1),(2,2)"))
            .await?;
        // The set as the start cursor would carry it: everything consumed so far.
        let saved: Option<String> = if info.flavor == Flavor::MariaDb {
            c.query_first("SELECT @@GLOBAL.gtid_binlog_state").await?
        } else {
            let mode: Option<String> = c.query_first("SELECT @@GLOBAL.gtid_mode").await?;
            match mode.unwrap_or_default().to_ascii_uppercase().as_str() {
                "ON" | "ON_PERMISSIVE" | "OFF_PERMISSIVE" => {
                    c.query_first("SELECT @@GLOBAL.gtid_executed").await?
                }
                // GTIDs off: no set exists, the assertion below is the old path on this server.
                _ => None,
            }
        };
        c.query_drop(format!("INSERT INTO `{s}`.items VALUES (3,3)"))
            .await?;
        c.query_drop(format!("INSERT INTO `{s}`.items VALUES (9,9)"))
            .await?; // the marker

        // On the GTID path nothing the cursor carries about file/pos may matter, so the start
        // names no file at all (the request's own position is the server's minimum, which a GTID
        // dump does not read). On the old path an empty file would be refused, so the start is
        // the head of the oldest binlog (fresh per run: no purge has happened) and must see every
        // row of this test.
        let row: Row = c
            .query_first("SHOW BINARY LOGS")
            .await?
            .ok_or_else(|| anyhow!("SHOW BINARY LOGS is empty"))?;
        let first: String = row.get(0).ok_or_else(|| anyhow!("no Log_name column"))?;
        let by_gtid = info.flavor != Flavor::MariaDb && saved.is_some();
        let from = if by_gtid {
            Position {
                file: String::new(),
                pos: 4,
            }
        } else {
            Position {
                file: first,
                pos: 4,
            }
        };
        let mut reader = BinlogReader::connect_gtid(
            replica_opts(&dsn)?,
            server_id(),
            info.flavor,
            &from,
            saved.as_deref(),
            Duration::from_secs(5),
        )
        .await?;
        let mut items: HashMap<u64, String> = HashMap::new();
        let mut ids: Vec<i64> = Vec::new();
        let mut cur_gtid: Option<String> = None;
        let mut our_gtids: Vec<String> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let next = tokio::time::timeout(left, reader.next())
                .await
                .map_err(|_| anyhow!("timed out waiting for the marker row; ids {ids:?}"))?;
            let item = next?.ok_or_else(|| anyhow!("the stream closed before the marker row"))?;
            match item {
                Item::Gtid { gtid } => cur_gtid = Some(gtid),
                Item::TableMap {
                    table_id,
                    schema,
                    table,
                    ..
                } if schema == s && table == "items" => {
                    items.insert(table_id, table);
                }
                Item::Rows {
                    table_id,
                    kind: RowsKind::Insert,
                    event,
                } if items.contains_key(&table_id) => {
                    if by_gtid {
                        // The dump delivers our transactions whole: the frozen `Item::Gtid`
                        // comes before the rows of its transaction, on this path too.
                        ensure!(cur_gtid.is_some(), "our rows arrived without a GTID event");
                        if let Some(g) = &cur_gtid {
                            if !our_gtids.contains(g) {
                                our_gtids.push(g.clone());
                            }
                        }
                    }
                    for (_, after) in reader.rows(table_id, &event)? {
                        let vals = image("items", after)?
                            .ok_or_else(|| anyhow!("insert without an after image"))?;
                        let id = match vals.first() {
                            Some(Value::Int(n)) => *n,
                            v => bail!("odd id image {v:?} for {s}.items"),
                        };
                        ids.push(id);
                    }
                    if ids.contains(&9) {
                        break;
                    }
                }
                _ => {}
            }
        }
        reader.close().await?;
        if by_gtid {
            ensure!(
                !ids.contains(&1) && !ids.contains(&2),
                "rows inside the saved set were streamed again: {ids:?}"
            );
            ensure!(
                ids.contains(&3) && ids.contains(&9),
                "rows after the saved set were not streamed: {ids:?}"
            );
        } else {
            // The old path from the head of the oldest binlog sees every row of this test.
            ensure!(
                [1, 2, 3, 9].iter().all(|i| ids.contains(i)),
                "file/pos start delivered {ids:?}"
            );
        }
        if by_gtid {
            // The two transactions after the set each arrive as a transaction: the frozen
            // `Item::Gtid` (shape owned by lazy-wire-fire) leads its own rows group.
            ensure!(
                our_gtids.len() >= 2,
                "the two transactions after the set did not each carry a GTID event: {our_gtids:?}"
            );
        }
        c.query_drop(format!("DROP DATABASE `{s}`")).await?;
        Ok(())
    })
    .await;
}

/// A GTID text with no usable transaction is refused on the client before any request
/// leaves: an empty GTID dump means "from my oldest binlog" to the server, and a silent
/// wrong resume is forbidden. A blank text is "no set at all" and runs the old file/pos path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_gtid_start_refuses_unusable_set() {
    on_all_servers(
        "stream_gtid_start_refuses_unusable_set",
        |_name, dsn| async move {
            let mut c = connect(&dsn).await?;
            let info = server_info(&mut c).await?;
            let pos = current_position(&mut c).await?;
            // A blank (or blank-after-trim) set is no set: the old path runs, unchanged.
            let r = BinlogReader::connect_gtid(
                replica_opts(&dsn)?,
                server_id(),
                info.flavor,
                &pos,
                Some("   "),
                Duration::from_secs(5),
            )
            .await?;
            r.close().await?;
            if info.flavor == Flavor::MariaDb {
                // MariaDB does not read the set yet (PR C owns its dump): a foreign-shaped text
                // must not raise the client-side GTID refusal there.
                match BinlogReader::connect_gtid(
                    replica_opts(&dsn)?,
                    server_id(),
                    info.flavor,
                    &pos,
                    Some("1-2-3"),
                    Duration::from_secs(5),
                )
                .await
                {
                    Ok(r) => r.close().await?,
                    Err(e) => ensure!(
                        !format!("{e:#}").contains("saved GTID set"),
                        "MariaDB spoke the client-side GTID refusal early: {e:#}"
                    ),
                }
                return Ok(());
            }
            for bad in [
                "not-a-uuid:1-2",
                "00000000-0000-0000-0000-000000000001:",
                "00000000-0000-0000-0000-000000000001:0",
            ] {
                let e = BinlogReader::connect_gtid(
                    replica_opts(&dsn)?,
                    server_id(),
                    info.flavor,
                    &pos,
                    Some(bad),
                    Duration::from_secs(5),
                )
                .await
                .err()
                .ok_or_else(|| anyhow!("{bad:?} was accepted"))?;
                ensure!(
                    format!("{e:#}").contains("GTID") && !is_missing_binlog(&e),
                    "{bad:?} refused with the wrong words: {e:#}"
                );
            }
            Ok(())
        },
    )
    .await;
}

/// Plain TCP (caching_sha2_password full authentication via RSA key exchange), the unix socket
/// mysql_async switches to for local hosts, and TLS where the server has certificates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_connection_modes() {
    on_all_servers("stream_connection_modes", |name, dsn| async move {
        let mut c = connect(&dsn).await?;
        let host: Option<String> = c
            .query_first(
                "SELECT host FROM information_schema.processlist WHERE id = CONNECTION_ID()",
            )
            .await?;
        // have_ssl is gone in MySQL 8.4 (TLS always available there); MariaDB 10.11 without
        // configured certificates reports DISABLED.
        let have_ssl: Option<(String, String)> = c
            .query_first("SHOW GLOBAL VARIABLES LIKE 'have_ssl'")
            .await?;
        let have_ssl = have_ssl.map(|r| r.1).unwrap_or_else(|| "YES".into());
        // Drop cached password hashes so the next TCP login takes caching_sha2's full path.
        c.query_drop("FLUSH PRIVILEGES").await?;
        drop(c);
        let mut tcp = connect(&with_param(&dsn, "prefer_socket=false"))
            .await
            .context("TCP without TLS")?;
        let tcp_host: Option<String> = tcp
            .query_first(
                "SELECT host FROM information_schema.processlist WHERE id = CONNECTION_ID()",
            )
            .await?;
        // A TCP login shows `address:port` (the address varies: loopback locally, the docker
        // bridge gateway on CI); a socket login shows plain `localhost`.
        ensure!(
            tcp_host.as_deref().is_some_and(|h| h != "localhost"
                && h.rsplit_once(':')
                    .is_some_and(|(_, p)| p.parse::<u16>().is_ok())),
            "not TCP: {tcp_host:?}"
        );
        let mut tls_note = "server has no TLS".to_string();
        if have_ssl == "YES" {
            let mut tls = connect(&with_param(
                &dsn,
                "prefer_socket=false&require_ssl=true&verify_ca=false",
            ))
            .await
            .context("TLS")?;
            let cipher: Option<(String, String)> = tls
                .query_first("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                .await?;
            let cipher = cipher.map(|c| c.1).unwrap_or_default();
            ensure!(!cipher.is_empty(), "TLS requested but no cipher in use");
            tls_note = format!("TLS {cipher}");
        }
        println!("[{name}] default host={host:?}, TCP ok, {tls_note}");
        Ok(())
    })
    .await;
}

/// A second reader with the same server_id makes the server drop the first one; privilege
/// problems (no REPLICATION CLIENT / BINLOG MONITOR / REPLICATION SLAVE, wrong password) are
/// recognisable and say how to fix them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_duplicate_server_id_and_privileges() {
    on_all_servers(
        "stream_duplicate_server_id_and_privileges",
        |_name, dsn| async move {
            let mut c = connect(&dsn).await?;
            let info = server_info(&mut c).await?;
            let pos = current_position(&mut c).await?;
            let hb = Duration::from_secs(1);

            // duplicate server_id: the older stream fails, the newer one keeps working
            let sid = server_id();
            let mut r1 =
                BinlogReader::connect(replica_opts(&dsn)?, sid, info.flavor, &pos, hb).await?;
            let mut r2 =
                BinlogReader::connect(replica_opts(&dsn)?, sid, info.flavor, &pos, hb).await?;
            let err = tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    match r1.next().await {
                        Ok(Some(_)) => continue,
                        Ok(None) => break anyhow!("stream closed without an error"),
                        Err(e) => break e,
                    }
                }
            })
            .await
            .context("the first reader was not dropped")?;
            ensure!(
                is_duplicate_server_id(&err),
                "not a duplicate server_id error: {err:#}"
            );
            ensure!(
                !is_missing_binlog(&err) && !is_access_denied(&err),
                "misclassified: {err:#}"
            );
            ensure!(
                format!("{err:#}").contains(&format!("same server_id {sid}")),
                "no hint: {err:#}"
            );
            let item = tokio::time::timeout(Duration::from_secs(10), r2.next()).await??;
            ensure!(item.is_some(), "second reader closed");
            r2.close().await?;

            // an account with SELECT only
            let user = format!("lvnp{:x}", sid & 0xff_ffff);
            c.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"))
                .await?;
            c.query_drop(format!("GRANT SELECT ON *.* TO '{user}'@'%'"))
                .await?;
            let udsn = dsn.replacen("lv:lv@", &format!("{user}:pw@"), 1);
            let res = async {
                let mut uc = connect(&udsn).await?;
                let e = current_position(&mut uc)
                    .await
                    .expect_err("position without privilege");
                ensure!(
                    is_access_denied(&e) && format!("{e:#}").contains("GRANT"),
                    "position: {e:#}"
                );
                let e = binlog_exists(&mut uc, &pos.file)
                    .await
                    .expect_err("binlogs without privilege");
                ensure!(
                    is_access_denied(&e) && format!("{e:#}").contains("GRANT"),
                    "binlogs: {e:#}"
                );
                let e = match BinlogReader::connect(
                    replica_opts(&udsn)?,
                    server_id(),
                    info.flavor,
                    &pos,
                    hb,
                )
                .await
                {
                    Ok(_) => bail!("streaming without REPLICATION SLAVE succeeded"),
                    Err(e) => e,
                };
                ensure!(is_access_denied(&e), "stream: {e:#}");
                ensure!(
                    format!("{e:#}").contains("REPLICATION SLAVE"),
                    "stream hint: {e:#}"
                );
                ensure!(
                    !is_missing_binlog(&e) && !is_duplicate_server_id(&e),
                    "stream: {e:#}"
                );
                let bad = dsn.replacen("lv:lv@", &format!("{user}:wrong@"), 1);
                let e = anyhow::Error::new(
                    Conn::new(replica_opts(&bad)?)
                        .await
                        .map(|_| ())
                        .expect_err("bad password"),
                );
                ensure!(is_access_denied(&e), "bad password: {e:#}");
                Ok(())
            }
            .await;
            c.query_drop(format!("DROP USER '{user}'@'%'")).await?;
            res
        },
    )
    .await;
}

// ---------------------------------------------------------------------------------------------
// MariaDB Partial_rows fragments

/// `(table, kind, [row image lengths])` per event: the rows hold blobs of tens of KiB, which a
/// mismatch must not dump.
fn shape(evs: &[Ev]) -> Vec<String> {
    let len = |i: &Image| {
        i.as_ref().map(|v| {
            v.iter()
                .map(|x| match x {
                    Value::Bytes(b) => b.len(),
                    _ => 1,
                })
                .sum::<usize>()
        })
    };
    evs.iter()
        .map(|e| match e {
            Ev::Rows { table, kind, rows } => format!(
                "{table} {kind:?} {:?}",
                rows.iter()
                    .map(|(b, a)| (len(b), len(a)))
                    .collect::<Vec<_>>()
            ),
            Ev::Stmt(q) => format!("stmt {q}"),
        })
        .collect()
}

/// Joins adjacent rows events of one table and kind: the server packs rows into events of at
/// most `binlog_row_event_max_size` (8 KiB), so a statement's rows may come in several events.
fn merged(evs: &[Ev]) -> Vec<Ev> {
    let mut out: Vec<Ev> = Vec::new();
    for e in evs {
        match (out.last_mut(), e) {
            (
                Some(Ev::Rows { table, kind, rows }),
                Ev::Rows {
                    table: t,
                    kind: k,
                    rows: r,
                },
            ) if table == t && kind == k => rows.extend(r.iter().cloned()),
            _ => out.push(e.clone()),
        }
    }
    out
}

/// Three statements whose rows events exceed the fragment threshold (one row of the first
/// stays under it), then the marker; the stream must hold the whole rows. `fragmented` is set
/// when the caller armed `binlog_row_event_fragment_threshold` (MariaDB >= 12.3): the server
/// must then really split the events into `Partial_rows`. Without it the same rows run
/// unfragmented (no threshold: MySQL, Percona, MariaDB < 12.3).
async fn partial_rows_workload(
    dsn: &str,
    w: &mut Conn,
    s: &str,
    start: &Position,
    flavor: Flavor,
    fragmented: bool,
) -> Result<()> {
    let row = |id: i64, n: usize, c: char| -> Image {
        Some(vec![Value::Int(id), Value::Bytes(vec![c as u8; n])])
    };
    let f = "f".to_string();
    let mut want = Vec::new();
    // One multi-row INSERT is one rows event: 100 B, 5 KiB and 70 KiB blobs.
    w.query_drop(format!(
        "INSERT INTO `{s}`.f VALUES (1, REPEAT('a', 100)), (2, REPEAT('b', 5000)), (3, REPEAT('c', 70000))"
    ))
    .await?;
    want.push(Ev::Rows {
        table: f.clone(),
        kind: RowsKind::Insert,
        rows: vec![
            (None, row(1, 100, 'a')),
            (None, row(2, 5000, 'b')),
            (None, row(3, 70000, 'c')),
        ],
    });
    // Two 70 KiB images in one event.
    w.query_drop(format!(
        "UPDATE `{s}`.f SET d = REPEAT('d', 70000) WHERE id = 3"
    ))
    .await?;
    want.push(Ev::Rows {
        table: f.clone(),
        kind: RowsKind::Update,
        rows: vec![(row(3, 70000, 'c'), row(3, 70000, 'd'))],
    });
    w.query_drop(format!("DELETE FROM `{s}`.f")).await?;
    want.push(Ev::Rows {
        table: f,
        kind: RowsKind::Delete,
        rows: vec![
            (row(1, 100, 'a'), None),
            (row(2, 5000, 'b'), None),
            (row(3, 70000, 'd'), None),
        ],
    });
    let mt = marker_table(s);
    let marker = format!("CREATE TABLE `{s}`.`{mt}` (id INT)");
    w.query_drop(&marker).await?;
    want.push(Ev::Stmt(marker));

    // Proof the server wrote the workload its own way: at the 1 KiB threshold about 150 KiB of
    // row images per fragment gives hundreds of `Partial_rows`; on a server without the
    // threshold no rows event may fragment at all.
    let events: Vec<Row> = w
        .query(format!(
            "SHOW BINLOG EVENTS IN '{}' FROM {}",
            start.file, start.pos
        ))
        .await?;
    let fragments = events
        .iter()
        .filter(|r| r.get::<String, _>(2).as_deref() == Some("Partial_rows"))
        .count();
    if fragmented {
        ensure!(
            fragments >= 200,
            "{fragments} Partial_rows events after {start}, the workload should produce about 290"
        );
    } else {
        ensure!(
            fragments == 0,
            "{fragments} Partial_rows events after {start}: this server has no \
             binlog_row_event_fragment_threshold, no rows event may fragment"
        );
    }

    let mut r = BinlogReader::connect(
        replica_opts(dsn)?,
        server_id(),
        flavor,
        start,
        Duration::from_secs(1),
    )
    .await?;
    let seen = collect(&mut r, s, &[], &mt).await?;
    let got = merged(&flat(&seen.groups));
    ensure!(
        got == want,
        "stream mismatch\n got: {:#?}\nwant: {:#?}",
        shape(&got),
        shape(&want)
    );
    ensure!(
        seen.groups.len() == want.len(),
        "{} groups, want {}",
        seen.groups.len(),
        want.len()
    );
    Ok(())
}

/// MariaDB >= 12.3 splits a rows event larger than `binlog_row_event_fragment_threshold` into
/// `Partial_rows` events (type 172), which the reader reassembles. With the threshold at its
/// minimum (1 KiB) every rows event of this workload is fragmented (up to 140 fragments).
/// Servers without the variable (MySQL, Percona, MariaDB < 12.3) run the same workload
/// unfragmented: the reader must reassemble exactly the same rows either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_mariadb_partial_rows_reassembled() {
    on_all_servers(
        "stream_mariadb_partial_rows_reassembled",
        |_name, dsn| async move {
            let mut w = connect(&dsn).await?;
            let info = server_info(&mut w).await?;
            let threshold: Option<u64> = if info.flavor == Flavor::MariaDb {
                w.query_first("SELECT @@GLOBAL.binlog_row_event_fragment_threshold")
                    .await
                    .unwrap_or(None)
            } else {
                None
            };
            let s = unique_schema("pr");
            w.query_drop(format!("CREATE DATABASE `{s}`")).await?;
            w.query_drop(format!(
            "CREATE TABLE `{s}`.f (id INT NOT NULL PRIMARY KEY, d LONGBLOB NOT NULL) ENGINE=InnoDB"
        ))
            .await?;
            let start = current_position(&mut w).await?;
            let res = match threshold {
                // The server's setting comes back whatever happened (the schema stays for a failure).
                Some(_) => {
                    let mut globals = common::Globals::set(
                        &mut w,
                        &dsn,
                        &[("binlog_row_event_fragment_threshold", "1024")],
                    )
                    .await?;
                    let res =
                        partial_rows_workload(&dsn, &mut w, &s, &start, info.flavor, true).await;
                    globals.restore(&mut w).await?;
                    res
                }
                // No fragment threshold on this server: the same rows run unfragmented.
                None => partial_rows_workload(&dsn, &mut w, &s, &start, info.flavor, false).await,
            };
            res?;
            w.query_drop(format!("DROP DATABASE `{s}`")).await?;
            Ok(())
        },
    )
    .await;
}
