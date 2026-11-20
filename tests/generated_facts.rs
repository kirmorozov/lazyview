//! Row-image facts for VIRTUAL / STORED generated columns — PR-1 of the VIRTUAL/STORED plan
//! (`tmp/out/lvs-virtual/plan/breakdown.md` §6 PR-1). It grounds #7 / C-01 and changes no
//! behaviour: PR-2 is the code change these facts justify.
//!
//! The plan measured everything except the row-image layout itself (`plan/facts.md` §F): that
//! probe decoded with `mysqlbinlog` / `mariadb-binlog`, which the server images do not ship, so
//! no `TMAP`/`ROW` line was ever printed. This test closes that gap with the repo's OWN reader:
//! it writes rows into neutral tables (`probe_items`, `probe_entities`, `probe_links`,
//! `probe_e1`, plus `probe_base` as the no-generated-columns control) whose columns mix real,
//! VIRTUAL and STORED generated columns, and decodes what the row images really hold per
//! position through `lazyview::binlog::BinlogReader` — `table_map` and `rows`, the same two
//! calls `src/accumulate.rs:474` (`value_at(row, c) = row.as_ref(c)`) reads a row through.
//!
//! Per neutral table and per `binlog_row_image` (FULL, MINIMAL, NOBLOB) it prints and asserts:
//! the TABLE_MAP column count and the row-image length, the image slots in order with each
//! decoded value and the catalog ordinal it lands on under today's `row.as_ref(c)`, and — for
//! every real column that sits after a VIRTUAL one — the image slot its value is really read
//! from (the misalignment #7 is made of).
//!
//! Gated like every other server test: the servers come from `LAZYVIEW_TEST_DSNS` through
//! `common::servers()` (unset ⇒ one visible `SKIP` line and a pass, which is why the proof runs
//! name a flavour). Every fact line goes to the real **stderr**, past the harness's stdout
//! capture, so the decoded image is in the log of a GREEN run:
//! `bash tmp/lvs-suite.sh generated_facts <out> mariadb114` (and `mysql84`).

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Context, Result};
use lazyview::binlog::{current_position, replica_opts, BinlogReader};
use lazyview::source::{Flavor, Item, RowsKind};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::prelude::*;
use mysql_async::{Conn, Row, Value};

/// One fact line, on the real stderr: libtest captures a passing test's stdout, so a
/// `println!` never reaches the log the proof is read from (the same reason
/// `common::skip_scenario` writes its `SKIP` line to stderr). The `TMAP` / `ROW` / `MAP` /
/// `STORED` / `FACT` lines of this test are its artifact.
macro_rules! fact {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

// ---------------------------------------------------------------------------------------------
// harness (the shape tests/binlog_stream.rs uses)

const TEST: &str = "generated_row_image_facts";

#[tokio::test]
async fn generated_row_image_facts() {
    let Some(servers) = common::servers() else {
        common::skip_scenario(
            TEST,
            "LAZYVIEW_TEST_DSNS is not set (run it through tmp/lvs-suite.sh <flavour>)",
        );
        return;
    };
    let mut handles = Vec::new();
    for server in servers {
        let (name, shown, dsn) = (
            server.flavor.clone(),
            server.flavor.clone(),
            server.base_dsn.clone(),
        );
        handles.push((
            name,
            tokio::spawn(async move { facts_on(&shown, &dsn).await }),
        ));
    }
    let mut failures = Vec::new();
    for (name, h) in handles {
        match h.await {
            Ok(Ok(())) => fact!("OK {TEST} [{name}]"),
            Ok(Err(e)) => failures.push(format!("[{name}] {e:#}")),
            Err(e) => failures.push(format!("[{name}] panicked: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{TEST} failed:\n{}",
        failures.join("\n")
    );
}

fn seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        nanos ^ (u64::from(std::process::id()) << 20)
    })
}

/// Distinct replica server ids (0x3xxx_xxxx, away from the test servers' own ids).
fn server_id() -> u32 {
    static NEXT: OnceLock<AtomicU32> = OnceLock::new();
    NEXT.get_or_init(|| AtomicU32::new(0x3000_0000 | (((seed() >> 8) as u32 & 0x0fff) << 16)))
        .fetch_add(1, Ordering::Relaxed)
}

fn unique_schema(tag: &str) -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "lvgf_{tag}_{:x}_{}",
        seed() & 0xff_ffff_ffff,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

async fn connect(dsn: &str) -> Result<Conn> {
    let mut c = common::connect_opts(replica_opts(dsn)?)
        .await
        .context("connect")?;
    c.query_drop("SET time_zone = '+00:00'").await?;
    Ok(c)
}

fn trunc(s: &str, n: usize) -> String {
    let s: String = s.chars().filter(|c| *c != '\n' && *c != '\r').collect();
    if s.chars().count() <= n {
        s
    } else {
        s.chars().take(n).collect::<String>() + "…"
    }
}

// ---------------------------------------------------------------------------------------------
// values, rendered the same from the binlog image and from a SELECT

/// NULL, compact JSON, integers as integers, other numbers normalized (`3.10` == `3.1`,
/// `1.5` == `1.5`), so a value can be compared whichever side it came from.
fn canon_str(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('{') || t.starts_with('[') {
        if let Ok(j) = serde_json::from_str::<serde_json::Value>(t) {
            return serde_json::to_string(&j).unwrap_or_else(|_| t.to_string());
        }
    }
    if let Ok(i) = t.parse::<i64>() {
        return i.to_string();
    }
    if let Ok(f) = t.parse::<f64>() {
        return format!("{f:?}");
    }
    s.to_string()
}

fn canon(v: &Value) -> String {
    match v {
        Value::NULL => NULL.to_string(),
        Value::Bytes(b) => canon_str(&String::from_utf8_lossy(b)),
        Value::Int(i) => canon_str(&i.to_string()),
        Value::UInt(u) => canon_str(&u.to_string()),
        Value::Float(f) => canon_str(&format!("{f}")),
        Value::Double(d) => canon_str(&format!("{d}")),
        other => canon_str(&format!("{other:?}")),
    }
}

/// The rendering of "no value": what `row.as_ref(c)` gives for a NULL column, for a slot past
/// the end of the image, and what `SELECT` gives for a NULL column.
const NULL: &str = "<null>";

/// One decoded row image: the values in image order, rendered by [`canon`]. A slot the decoder
/// could not convert is an error naming the exact failure — an image the repo reader cannot
/// decode is a finding, not something to gloss over.
fn decode(row: &BinlogRow, what: &str) -> Result<Vec<String>> {
    (0..row.len())
        .map(|i| match row.as_ref(i) {
            None => Ok(NULL.to_string()),
            Some(v) => Value::try_from(v.clone()).map(|v| canon(&v)).map_err(|e| {
                anyhow!(
                    "{what} slot {i}: the repo reader cannot decode {}: {e}",
                    trunc(&format!("{v:?}"), 60)
                )
            }),
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// the neutral tables

/// One table of the fact matrix. `ddl` creates it (CREATE first, then the ALTERs, so a VIRTUAL
/// column can sit BEFORE the real column its expression reads, which one CREATE cannot express).
/// `$I1`/`$I2` are the two ids and `$A1`… the values of [`Plan`]; `{S}` is the schema. Every
/// phase writes its own distinguishable rows, so an image always belongs to a row of this phase.
struct Table {
    name: &'static str,
    ddl: &'static [&'static str],
    insert: &'static str,
    update: &'static str,
    delete: &'static str,
    /// The columns the UPDATE writes: a MINIMAL/NOBLOB after image must hold their new values.
    changed: &'static [&'static str],
}

const CTL: &str = "probe_ctl";

const TABLES: &[Table] = &[
    Table {
        // Control: no generated column at all, so image position must equal catalog ordinal.
        name: "probe_base",
        ddl: &["CREATE TABLE {S}.probe_base (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, \
                b INT NOT NULL, txt TEXT NULL)"],
        insert: "INSERT INTO {S}.probe_base (id,a,b,txt) VALUES ($I1,$A1,$B1,'$X1'),($I2,$A2,$B2,NULL)",
        update: "UPDATE {S}.probe_base SET a=$NA, b=$NB, txt='$NX' WHERE id=$I1",
        delete: "DELETE FROM {S}.probe_base WHERE id=$I2",
        changed: &["a", "b", "txt"],
    },
    Table {
        // The plan's matrix: VIRTUAL before / between / after real columns, STORED mixed in, a
        // VIRTUAL over a VIRTUAL, and VIRTUAL/STORED over TEXT and over JSON.
        name: "probe_items",
        ddl: &["CREATE TABLE {S}.probe_items (\
                id INT NOT NULL PRIMARY KEY, \
                a INT NOT NULL, \
                v_sum INT GENERATED ALWAYS AS (a + 1) VIRTUAL, \
                b INT NOT NULL, \
                s_dbl INT GENERATED ALWAYS AS (b * 2) STORED, \
                t VARCHAR(32) CHARACTER SET utf8mb4 NOT NULL, \
                v_txt VARCHAR(64) CHARACTER SET utf8mb4 GENERATED ALWAYS AS (CONCAT(t, '-', a)) VIRTUAL, \
                dbl DOUBLE NOT NULL, \
                decm DECIMAL(10,2) NOT NULL, \
                js JSON NULL, \
                v_json VARCHAR(64) GENERATED ALWAYS AS (COALESCE(JSON_UNQUOTE(JSON_EXTRACT(js, '$.k')), '-')) VIRTUAL, \
                txt TEXT NULL, \
                v_txtlen INT GENERATED ALWAYS AS (CHAR_LENGTH(txt)) VIRTUAL, \
                v_nest INT GENERATED ALWAYS AS (v_sum + b) VIRTUAL, \
                s_txtlen INT GENERATED ALWAYS AS (CHAR_LENGTH(txt)) STORED)"],
        insert: "INSERT INTO {S}.probe_items (id,a,b,t,dbl,decm,js,txt) VALUES \
                 ($I1,$A1,$B1,'$T1',$D1,$M1,'$J1','$X1'),($I2,$A2,$B2,'$T2',$D2,$M2,'$J2',NULL)",
        update: "UPDATE {S}.probe_items SET a=$NA, b=$NB, txt='$NX' WHERE id=$I1",
        delete: "DELETE FROM {S}.probe_items WHERE id=$I2",
        changed: &["a", "b", "txt"],
    },
    Table {
        // A VIRTUAL column FIRST in the table, its expression over a base column defined later
        // (added by ALTER, the way #7 meets it in a live schema), plus a STORED one.
        name: "probe_entities",
        ddl: &[
            "CREATE TABLE {S}.probe_entities (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, \
             txt TEXT NULL)",
            "ALTER TABLE {S}.probe_entities ADD COLUMN v_first INT AS (a + 100) VIRTUAL FIRST",
            "ALTER TABLE {S}.probe_entities ADD COLUMN s_par INT AS (a * 5) STORED",
        ],
        insert: "INSERT INTO {S}.probe_entities (id,a,txt) VALUES ($I1,$A1,'$X1'),($I2,$A2,NULL)",
        update: "UPDATE {S}.probe_entities SET a=$NA, txt='$NX' WHERE id=$I1",
        delete: "DELETE FROM {S}.probe_entities WHERE id=$I2",
        changed: &["a", "txt"],
    },
    Table {
        // VIRTUAL at the front and the end, STORED in the middle, all added to a table that
        // already exists — the DDL shape `plan_refusal` refuses today.
        name: "probe_links",
        ddl: &[
            "CREATE TABLE {S}.probe_links (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, \
             b INT NOT NULL)",
            "ALTER TABLE {S}.probe_links ADD COLUMN v_first INT AS (b + a) VIRTUAL FIRST",
            "ALTER TABLE {S}.probe_links ADD COLUMN s_mid INT AS (b * 3) STORED AFTER a",
            "ALTER TABLE {S}.probe_links ADD COLUMN v_last VARCHAR(20) AS (CONCAT('L', b)) VIRTUAL",
        ],
        insert: "INSERT INTO {S}.probe_links (id,a,b) VALUES ($I1,$A1,$B1),($I2,$A2,$B2)",
        update: "UPDATE {S}.probe_links SET a=$NA, b=$NB WHERE id=$I1",
        delete: "DELETE FROM {S}.probe_links WHERE id=$I2",
        changed: &["a", "b"],
    },
    Table {
        // The smallest shape that shows the defect: one real column after a VIRTUAL one, and a
        // STORED pair over a TEXT column whose value only the server computes.
        name: "probe_e1",
        ddl: &["CREATE TABLE {S}.probe_e1 (\
                id INT NOT NULL PRIMARY KEY, \
                a INT NOT NULL, \
                v_sum INT GENERATED ALWAYS AS (a + 1) VIRTUAL, \
                b INT NOT NULL, \
                s_dbl INT GENERATED ALWAYS AS (b * 2) STORED, \
                txt TEXT NULL, \
                s_txtlen INT GENERATED ALWAYS AS (CHAR_LENGTH(txt)) STORED, \
                v_txtlen INT GENERATED ALWAYS AS (CHAR_LENGTH(txt)) VIRTUAL)"],
        insert: "INSERT INTO {S}.probe_e1 (id,a,b,txt) VALUES ($I1,$A1,$B1,'$X1'),($I2,$A2,$B2,NULL)",
        update: "UPDATE {S}.probe_e1 SET a=$NA, b=$NB, txt='$NX' WHERE id=$I1",
        delete: "DELETE FROM {S}.probe_e1 WHERE id=$I2",
        changed: &["a", "b", "txt"],
    },
];

/// The values one `binlog_row_image` phase writes. Every number carries the phase in it, so a
/// value never means two different rows and a VIRTUAL column's value never equals a real
/// column's value of the same row.
struct Plan {
    id1: i64,
    id2: i64,
    subs: Vec<(&'static str, String)>,
    /// The UPDATE's new values, by column name: the after image must hold each of them.
    new_values: Vec<(&'static str, String)>,
}

impl Plan {
    fn new(p: usize) -> Self {
        let pe = p as i64;
        let (id1, id2) = (100 * (pe + 1) + 1, 100 * (pe + 1) + 2);
        let (a1, a2) = (1000 * pe + 11, 1000 * pe + 12);
        let (b1, b2) = (1000 * pe + 210, 1000 * pe + 220);
        let (na, nb) = (1000 * pe + 511, 1000 * pe + 601);
        let mut subs: Vec<(&'static str, String)> = vec![
            ("$I1", id1.to_string()),
            ("$I2", id2.to_string()),
            ("$A1", a1.to_string()),
            ("$A2", a2.to_string()),
            ("$B1", b1.to_string()),
            ("$B2", b2.to_string()),
            ("$T1", format!("t{p}_1")),
            ("$T2", format!("üñ{p}")),
            ("$D1", (pe as f64 + 0.5).to_string()),
            ("$D2", (pe as f64 + 0.75).to_string()),
            ("$M1", format!("{}.1", p + 3)),
            ("$M2", format!("{}.2", p + 3)),
            ("$J1", format!("{{\"k\":\"v{p}_1\"}}")),
            ("$J2", format!("{{\"k\":\"v{p}_2\"}}")),
            ("$X1", format!("txt{p}_1")),
            ("$NA", na.to_string()),
            ("$NB", nb.to_string()),
            ("$NX", format!("upd{p}")),
        ];
        // Longest key first: no key here is a substring of another, but the order keeps that
        // assumption from ever mattering.
        subs.sort_unstable_by_key(|k| std::cmp::Reverse(k.0.len()));
        Self {
            id1,
            id2,
            subs,
            new_values: vec![
                ("a", canon_str(&na.to_string())),
                ("b", canon_str(&nb.to_string())),
                ("txt", canon_str(&format!("upd{p}"))),
            ],
        }
    }

    /// The `$`-value placeholders only: `{S}` (the schema) is filled by [`write`].
    fn fill(&self, sql: &str) -> String {
        let mut s = sql.to_string();
        for (k, v) in &self.subs {
            s = s.replace(k, v);
        }
        s
    }

    fn new_value(&self, col: &str) -> Result<&str> {
        self.new_values
            .iter()
            .find(|(c, _)| *c == col)
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| {
                anyhow!("this table's UPDATE changes column {col}, which Plan does not know")
            })
    }
}

// ---------------------------------------------------------------------------------------------
// the catalog shape and the SELECT snapshots

/// A column of a probe table, as the catalog sees it (`information_schema`, DDL order).
struct Col {
    name: String,
    virt: bool,
    stored: bool,
}

/// The catalog shape of one table: its columns in DDL order (`Vec` index == catalog ordinal).
struct Shape {
    cols: Vec<Col>,
}

impl Shape {
    /// The non-VIRTUAL columns in DDL order, each with its catalog ordinal.
    fn real(&self) -> Vec<(usize, &Col)> {
        self.cols
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.virt)
            .collect()
    }

    /// The columns the server stores: neither VIRTUAL nor STORED GENERATED.
    fn base(&self) -> Vec<(usize, &Col)> {
        self.cols
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.virt && !c.stored)
            .collect()
    }

    fn ordinal_of(&self, name: &str) -> Result<usize> {
        self.cols
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| anyhow!("no column {name}"))
    }

    fn has_virtual_before_real(&self) -> bool {
        let mut seen_virtual = false;
        for c in &self.cols {
            if c.virt {
                seen_virtual = true;
            } else if seen_virtual {
                return true;
            }
        }
        false
    }
}

/// One `SELECT *` row: its `id` and every column value in DDL order.
struct RowSnap {
    id: String,
    vals: Vec<String>,
}

async fn shape_of(w: &mut Conn, schema: &str, table: &str) -> Result<Shape> {
    let rows = w
        .query::<Row, String>(format!(
            "SELECT COLUMN_NAME, EXTRA FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
        ))
        .await
        .with_context(|| format!("reading the shape of {schema}.{table}"))?;
    ensure!(!rows.is_empty(), "{schema}.{table} has no columns");
    let cols = rows
        .iter()
        .map(|r| {
            let name: String = r
                .get::<String, usize>(0)
                .ok_or_else(|| anyhow!("{table}: no COLUMN_NAME"))?;
            let extra: String = r.get::<String, usize>(1).unwrap_or_default();
            Ok(Col {
                name,
                virt: extra.contains("VIRTUAL GENERATED"),
                stored: extra.contains("STORED GENERATED"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        cols.iter().filter(|c| c.name == "id").count() == 1,
        "{schema}.{table} has no single id column"
    );
    Ok(Shape { cols })
}

async fn snapshot(w: &mut Conn, schema: &str, table: &str, shape: &Shape) -> Result<Vec<RowSnap>> {
    let rows = w
        .query::<Row, String>(format!("SELECT * FROM `{schema}`.`{table}`"))
        .await
        .with_context(|| format!("SELECT * of {schema}.{table}"))?;
    let id_at = shape.ordinal_of("id")?;
    rows.iter()
        .map(|r| {
            ensure!(
                r.len() == shape.cols.len(),
                "{schema}.{table}: SELECT * gave {} columns, the catalog has {}",
                r.len(),
                shape.cols.len()
            );
            let vals: Vec<String> = (0..shape.cols.len())
                .map(|i| r.as_ref(i).map(canon).unwrap_or_else(|| NULL.to_string()))
                .collect();
            Ok(RowSnap {
                id: vals[id_at].clone(),
                vals,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// capture, with the repo's own reader

/// One decoded image of a phase, as the repo reader produced it.
struct Img {
    table: String,
    kind: RowsKind,
    side: &'static str,
    vals: Vec<String>,
}

/// Streams until the sentinel row of [`CTL`] arrives (binlog order means everything written
/// before it has been delivered), keeping only the images and TABLE_MAPs of our own schema.
/// Returns the images and the TABLE_MAP column counts per table.
async fn capture(
    reader: &mut BinlogReader,
    schema: &str,
    budget: Duration,
) -> Result<(Vec<Img>, HashMap<String, u64>)> {
    let mut ours: HashMap<u64, String> = HashMap::new();
    let mut tmaps = HashMap::new();
    let mut out = Vec::new();
    let end = Instant::now() + budget;
    loop {
        let left = end.saturating_duration_since(Instant::now());
        ensure!(
            !left.is_zero(),
            "no sentinel row of {CTL} within {budget:?}"
        );
        let item = tokio::time::timeout(left, reader.next())
            .await
            .map_err(|_| {
                anyhow!("no binlog event for {budget:?} (waiting for the {CTL} sentinel)")
            })?
            .context("streaming binlog events")?
            .ok_or_else(|| anyhow!("the stream closed before the {CTL} sentinel"))?;
        match item {
            Item::TableMap {
                table_id,
                schema: s,
                table,
                columns,
            } if s == schema => {
                ours.insert(table_id, table.clone());
                tmaps.insert(table.clone(), columns);
                fact!(
                    "TMAP id={table_id} table={schema}.{table} columns_count={columns} {}",
                    tme_facts(reader, table_id)
                );
            }
            Item::Rows {
                table_id,
                kind,
                event,
            } => {
                let Some(table) = ours.get(&table_id).cloned() else {
                    continue;
                };
                let what = format!("{}.{} {kind:?}", schema, table);
                let rows = reader
                    .rows(table_id, &event)
                    .with_context(|| format!("decoding the rows of {what}"))?;
                for (i, (b, a)) in rows.iter().enumerate() {
                    for (side, r) in [("B", b), ("A", a)] {
                        let Some(r) = r else { continue };
                        let vals = decode(r, &what)?;
                        fact!(
                            "ROW table={table} kind={kind:?} row={i} side={side} len={} vals={:?}",
                            vals.len(),
                            vals
                        );
                        out.push(Img {
                            table: table.clone(),
                            kind,
                            side,
                            vals,
                        });
                    }
                }
                if table == CTL {
                    return Ok((out, tmaps));
                }
            }
            _ => {}
        }
    }
}

/// What the TABLE_MAP itself says about the layout — PR-2 has to derive its map from this plus
/// the catalog, because MariaDB's default `binlog_row_metadata=NO_LOG` sends no names at all.
fn tme_facts(reader: &BinlogReader, table_id: u64) -> String {
    let Some(tme) = reader.table_map(table_id) else {
        return "tme=<not kept by the reader>".to_string();
    };
    let mut meta = Vec::new();
    for f in tme.iter_optional_meta() {
        match f {
            Ok(f) => meta.push(trunc(&format!("{f:?}"), 60)),
            Err(e) => meta.push(format!("<meta error {e}>")),
        }
    }
    let names = meta.iter().any(|m| m.contains("ColumnName"));
    format!(
        "optional_metadata_fields={} column_names_in_table_map={names}",
        meta.len()
    )
}

// ---------------------------------------------------------------------------------------------
// the checks

/// The `SELECT *` of every table of a phase at the two points an image can come from: `s1`
/// right after the INSERTs (the INSERT after-image and the UPDATE before-image) and `s2` right
/// after the UPDATEs (the UPDATE after-image and the DELETE before-image). Keyed by table.
struct Snaps {
    s1: HashMap<String, Vec<RowSnap>>,
    s2: HashMap<String, Vec<RowSnap>>,
}

struct Ctx<'a> {
    flavor: &'a str,
    image: &'a str,
    schema: &'a str,
    plan: &'a Plan,
    snaps: &'a Snaps,
    /// The TABLE_MAP column counts the stream itself carried.
    tmaps: HashMap<String, u64>,
}

impl Ctx<'_> {
    /// Which `SELECT` an image must agree with: an INSERT logs the values as written, an
    /// UPDATE's before image is the pre-update state, an UPDATE's after image and a DELETE's
    /// before image are the post-update state.
    fn snaps(&self, kind: &RowsKind, side: &str) -> &HashMap<String, Vec<RowSnap>> {
        match (kind, side) {
            (RowsKind::Insert, _) | (RowsKind::Update, "B") => &self.snaps.s1,
            _ => &self.snaps.s2,
        }
    }
}

impl Ctx<'_> {
    /// The two rows this phase wrote, as the candidate owners of an image. A method, because
    /// the snapshots it returns live behind `ctx`'s own borrow.
    fn candidates(&self, table: &str, kind: &RowsKind, side: &str) -> Result<Vec<&RowSnap>> {
        let all = self
            .snaps(kind, side)
            .get(table)
            .ok_or_else(|| anyhow!("no snapshot of {}.{}", self.schema, table))?;
        let mine = [
            canon_str(&self.plan.id1.to_string()),
            canon_str(&self.plan.id2.to_string()),
        ];
        let found: Vec<&RowSnap> = all.iter().filter(|r| mine.contains(&r.id)).collect();
        ensure!(
            found.len() == 2,
            "{}.{}: the phase's two rows are not both in the {side}-image snapshot",
            self.schema,
            table
        );
        Ok(found)
    }
}

/// The layouts a row image can follow. **Which one a flavour uses is what this test measures**
/// — `facts.md` §F assumed the second (VIRTUAL columns are not logged; "MySQL and MariaDB log
/// base columns only", as `src/catalog.rs:199-203` and `src/source/maria.rs:199-203` also say)
/// and never measured it. The mariadb114 run of this test found the VIRTUAL columns' values in
/// the INSERT image and neither VIRTUAL nor STORED values in the UPDATE image, so the layout is
/// measured per flavour, per image mode and per statement instead of assumed. A test that simply
/// asserted §F's assumption would have failed on MariaDB and passed on MySQL without either run
/// saying which of the three layouts is true where.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Model {
    /// Every catalog column in DDL order: image position == catalog ordinal.
    All,
    /// The non-VIRTUAL columns in DDL order: what `row.as_ref(catalog_ordinal)` assumes, and the
    /// layout that misaligns every real column sitting after a VIRTUAL one.
    NonVirtual,
    /// The columns the server stores — neither VIRTUAL nor STORED GENERATED: the layout that
    /// misaligns everything after the first generated column of either kind.
    Base,
}

impl Model {
    fn name(self) -> &'static str {
        match self {
            Model::All => "all-catalog-columns",
            Model::NonVirtual => "non-virtual-only",
            Model::Base => "base-columns-only",
        }
    }

    /// Whether this layout carries a catalog column.
    fn keeps(self, col: &Col) -> bool {
        match self {
            Model::All => true,
            Model::NonVirtual => !col.virt,
            Model::Base => !col.virt && !col.stored,
        }
    }

    /// The layouts worth trying for a table: one that drops nothing always, plus one for each
    /// kind of generated column the table actually has.
    fn tried(shape: &Shape) -> Vec<Model> {
        let mut v = vec![Model::All];
        if shape.cols.iter().any(|c| c.virt) {
            v.push(Model::NonVirtual);
        }
        if shape.cols.iter().any(|c| c.virt || c.stored) {
            v.push(Model::Base);
        }
        v
    }
}

/// One slot of a layout: the catalog column it holds and the value the model puts there.
struct Slot {
    ordinal: usize,
    name: String,
    value: String,
    virt: bool,
    stored: bool,
}

fn layout(shape: &Shape, model: Model, row: &RowSnap) -> Vec<Slot> {
    shape
        .cols
        .iter()
        .enumerate()
        .filter(|(_, c)| model.keeps(c))
        .map(|(o, c)| Slot {
            ordinal: o,
            name: c.name.clone(),
            value: row.vals[o].clone(),
            virt: c.virt,
            stored: c.stored,
        })
        .collect()
}

/// Why `vals` is not what the layout produces: `None` when it is. An exact image is compared
/// position by position; a MINIMAL/NOBLOB image is a subset, so it only has to be a subsequence
/// of the layout — which is all a subset can be asked.
fn mismatch(want: &[Slot], vals: &[String], exact: bool) -> Option<String> {
    let want: Vec<&str> = want.iter().map(|s| s.value.as_str()).collect();
    if exact {
        if want.len() != vals.len() {
            return Some(format!(
                "the layout puts {} values in the image, the image holds {}",
                want.len(),
                vals.len()
            ));
        }
        return want.iter().zip(vals).enumerate().find_map(|(i, (w, v))| {
            (w != v).then(|| format!("slot {i}: the layout says {w}, the image holds {v}"))
        });
    }
    let mut rest = vals.iter();
    let mut need: Option<&String> = rest.next();
    for w in &want {
        if need.map(String::as_str) == Some(*w) {
            need = rest.next();
        }
    }
    need.map(|v| format!("{v} never comes up in the layout's order"))
}

/// What one real column of an image looks like to today's indexing: its value lives at `slot`,
/// while `row.as_ref(ordinal)` reads slot `ordinal`.
struct Misread {
    col: String,
    ordinal: usize,
    slot: usize,
    /// What `row.as_ref(ordinal)` returns, rendered.
    reads: String,
    /// The value that column really has, and whether the wrong slot gives something else.
    right: String,
    observable: bool,
    stored: bool,
}

/// One image, as the repo reader decoded it and the layout test read it.
struct Read {
    kind: RowsKind,
    side: String,
    model: Model,
    /// The other layouts these values cannot tell apart from the chosen one (a table with no
    /// generated column fits all three: they put the same values in the same slots).
    also: Vec<&'static str>,
    exact: bool,
    len: usize,
    virt_in_image: usize,
    stored_in_image: usize,
    misreads: Vec<Misread>,
}

/// The per-position report and the assertions for one image.
fn check_img(ctx: &Ctx<'_>, img: &Img, shape: &Shape) -> Result<Read> {
    let tmap = ctx.tmaps.get(&img.table).copied().ok_or_else(|| {
        anyhow!(
            "{}.{}: no TABLE_MAP for it in the stream",
            ctx.schema,
            img.table
        )
    })?;
    ensure!(
        usize::try_from(tmap) == Ok(shape.cols.len()),
        "{}.{}: the TABLE_MAP says {tmap} columns, the catalog has {}",
        ctx.schema,
        img.table,
        shape.cols.len()
    );
    let cands = ctx.candidates(&img.table, &img.kind, img.side)?;
    // A FULL image is the whole row, so it is compared position by position, and so is an
    // INSERT's: the server writes the columns the statement wrote, in order. Which columns those
    // are turned out to depend on `binlog_row_image` — mariadb114's MINIMAL INSERT image leaves
    // the VIRTUAL columns out of a row its FULL INSERT image carried in full — so the layout is
    // measured per statement kind and per mode instead of assumed. A MINIMAL/NOBLOB update or
    // delete image is a subset of one layout, so it only has to follow its order.
    let exact = ctx.image == "FULL" || img.kind == RowsKind::Insert;
    let mut fits: Vec<(Model, &RowSnap, Vec<Slot>)> = Vec::new();
    let mut why: Vec<String> = Vec::new();
    for m in Model::tried(shape) {
        for &row in &cands {
            let lay = layout(shape, m, row);
            match mismatch(&lay, &img.vals, exact) {
                None => fits.push((m, row, lay)),
                Some(e) => why.push(format!("{} of row {}: {e}", m.name(), row.id)),
            }
        }
    }
    let seen = format!(
        "{}.{} {} {:?} {}-image {:?}",
        ctx.schema, img.table, ctx.image, img.kind, img.side, img.vals
    );
    ensure!(
        !fits.is_empty(),
        "{seen}: no layout produces it — {}",
        why.join(" | ")
    );
    let rows: HashSet<&str> = fits.iter().map(|(_, r, _)| r.id.as_str()).collect();
    ensure!(
        rows.len() == 1,
        "{seen}: the layouts fit two different rows {rows:?}"
    );
    // Layouts that put the same values in the same slots are one layout as far as the image can
    // show (a table with no generated column fits all three). Two layouts putting *different*
    // values in the image, both matching, would mean the test's values cannot tell them apart:
    // that is a hole in the test, not a fact about the server, so it fails loudly.
    let seq = |lay: &[Slot]| -> Vec<String> { lay.iter().map(|s| s.value.clone()).collect() };
    let want = seq(&fits[0].2);
    let other: Vec<&'static str> = fits
        .iter()
        .filter(|(_, _, lay)| seq(lay) != want)
        .map(|(m, _, _)| m.name())
        .collect();
    // Only an exact image can tell the layouts apart. A MINIMAL/NOBLOB update or delete image is
    // the key, or the key plus what changed, and such a subset is a subsequence of every layout:
    // it shows which columns are *absent* but not where the rest sit, so ambiguity there is a
    // property of the image rather than a hole in the test, and the FACT line says so.
    ensure!(
        !exact || other.is_empty(),
        "{seen}: {other:?} produce a different image that also fits, so these values cannot tell \
         the layouts apart"
    );
    let model = fits[0].0;
    let also: Vec<&'static str> = fits
        .iter()
        .map(|(m, _, _)| m.name())
        .filter(|n| *n != model.name())
        .collect();
    let row = fits[0].1;
    let lay = &fits[0].2;

    let mut misreads = Vec::new();
    if exact {
        // The artifact: one SLOT line per slot — its position, the column it holds, the value,
        // and the catalog ordinal today's `row.as_ref(c)` would read it with.
        for (k, s) in lay.iter().enumerate() {
            fact!(
                "SLOT image={} table={}.{} kind={:?} side={} model={} slot={} catalog_ordinal={} \
                 col={} value={} virtual={} stored={}",
                ctx.image,
                ctx.schema,
                img.table,
                img.kind,
                img.side,
                model.name(),
                k,
                s.ordinal,
                s.name,
                img.vals[k],
                s.virt,
                s.stored,
            );
            if s.stored {
                // The parity fact: the value the server computed, as the image holds it and as
                // the SELECT reports it, with the slot and the catalog ordinal it sits at.
                fact!(
                    "STORED image={} table={}.{} kind={:?} side={} col={} catalog_ordinal={} \
                     image_slot={} value_in_image={} value_by_select={} equal=yes",
                    ctx.image,
                    ctx.schema,
                    img.table,
                    img.kind,
                    img.side,
                    s.name,
                    s.ordinal,
                    k,
                    img.vals[k],
                    s.value,
                );
            }
            if !s.virt && s.ordinal != k {
                // What today's `row.as_ref(catalog_ordinal)` reads for this column: a slot that
                // holds another column's value, or nothing at all past the end of the image.
                let as_read = img
                    .vals
                    .get(s.ordinal)
                    .cloned()
                    .unwrap_or_else(|| NULL.to_string());
                let reads = if s.ordinal < img.vals.len() {
                    as_read.clone()
                } else {
                    format!(
                        "{NULL}(slot {} is past the end of a {}-value image)",
                        s.ordinal,
                        img.vals.len()
                    )
                };
                misreads.push(Misread {
                    col: s.name.clone(),
                    ordinal: s.ordinal,
                    slot: k,
                    right: s.value.clone(),
                    observable: as_read != s.value,
                    reads,
                    stored: s.stored,
                });
            }
        }
        // The image must hold exactly the columns the layout keeps — nothing of a column the
        // layout drops, and nothing missing from the ones it keeps.
        let expect = match model {
            Model::All => shape.cols.len(),
            Model::NonVirtual => shape.real().len(),
            Model::Base => shape.base().len(),
        };
        ensure!(
            img.vals.len() == expect,
            "{}.{} {} {:?} {}-image holds {} values, the {} layout keeps {expect} of the {} \
             catalog columns",
            ctx.schema,
            img.table,
            ctx.image,
            img.kind,
            img.side,
            img.vals.len(),
            model.name(),
            shape.cols.len()
        );
        if model != Model::All {
            // The misalignment #7 is about: a kept column that sits after a dropped one is read
            // from the wrong slot, and here that must actually be observable — if the neighbouring
            // slot happened to hold the same value, the test's values were not distinct enough.
            let shifted = lay
                .iter()
                .enumerate()
                .any(|(k, s)| !s.virt && s.ordinal != k);
            ensure!(
                shifted == !misreads.is_empty(),
                "{}: the {} layout shifts {} column(s) off their catalog ordinal, which must \
                 show as {} misaligned reads",
                img.table,
                model.name(),
                lay.iter()
                    .enumerate()
                    .filter(|(k, s)| s.ordinal != *k)
                    .count(),
                if shifted { "some" } else { "no" }
            );
            ensure!(
                !shifted || misreads.iter().any(|m| m.observable),
                "{}: the {} layout reads a column from the wrong slot, but the value there \
                 equals the right one — the test's values are not distinct enough to tell the \
                 layouts apart",
                img.table,
                model.name()
            );
        }
    } else {
        // A subset image: it must be the primary key plus the changed columns. Which layout it
        // follows cannot be told from a subset, and this test says so instead of guessing.
        if img.side == "B" {
            ensure!(
                img.vals.contains(&row.id),
                "{}.{} {} {}-image {:?} holds no PK value, and the before image of a \
                 MINIMAL/NOBLOB update or delete is the primary key",
                ctx.schema,
                img.table,
                ctx.image,
                img.side,
                img.vals
            );
        }
        if img.side == "A" && img.kind == RowsKind::Update {
            let t = TABLES
                .iter()
                .find(|t| t.name == img.table)
                .ok_or_else(|| anyhow!("unknown table {}", img.table))?;
            for col in t.changed {
                let want = ctx.plan.new_value(col)?;
                ensure!(
                    img.vals.iter().any(|v| v == want),
                    "{}.{} {} after-image {:?} does not hold the new value {want} of the \
                     modified column {col}",
                    ctx.schema,
                    img.table,
                    ctx.image,
                    img.vals
                );
            }
        }
    }
    Ok(Read {
        kind: img.kind,
        side: img.side.to_string(),
        model,
        also,
        exact,
        len: img.vals.len(),
        virt_in_image: lay.iter().filter(|s| s.virt).count(),
        stored_in_image: lay.iter().filter(|s| s.stored).count(),
        misreads,
    })
}

fn check_phase(ctx: &Ctx<'_>, imgs: &[Img], shapes: &HashMap<String, Shape>) -> Result<()> {
    for t in TABLES {
        let shape = shapes
            .get(t.name)
            .ok_or_else(|| anyhow!("no shape for {}", t.name))?;
        let mine: Vec<&Img> = imgs.iter().filter(|i| i.table == t.name).collect();
        ensure!(
            !mine.is_empty(),
            "{}.{} wrote rows but the stream held no image of it",
            ctx.schema,
            t.name
        );
        let mut reads = Vec::new();
        for img in &mine {
            reads.push(check_img(ctx, img, shape)?);
        }
        // One MAP line per (column, wrong slot) the images show: the misalignment #7 is made of,
        // stated as the slot today's `row.as_ref(ordinal)` reads and what it holds there.
        let mut seen: HashSet<(String, usize)> = HashSet::new();
        for r in reads.iter().filter(|r| r.exact) {
            for m in &r.misreads {
                if seen.insert((m.col.clone(), m.ordinal)) {
                    fact!(
                        "MAP image={} table={}.{} col={} catalog_ordinal={} image_slot={} \
                         row_as_ref_reads_slot_{}_gives={} right_value={} observable={} stored={}",
                        ctx.image,
                        ctx.schema,
                        t.name,
                        m.col,
                        m.ordinal,
                        m.slot,
                        m.ordinal,
                        m.reads,
                        m.right,
                        m.observable,
                        m.stored
                    );
                }
            }
        }
        // A subset image says what is absent, not where things sit: record the layouts its
        // values fit, which is all it can support.
        let mut told: HashSet<(String, String)> = HashSet::new();
        for r in reads.iter().filter(|r| !r.exact) {
            if told.insert((format!("{:?}", r.kind), r.side.clone())) {
                let mut fits: Vec<&str> = r.also.clone();
                fits.push(r.model.name());
                fits.sort_unstable();
                fact!(
                    "SUBSET image={} table={}.{} kind={:?} side={} values_fit_layouts={:?} \
                     position=unknown(subset image)",
                    ctx.image,
                    ctx.schema,
                    t.name,
                    r.kind,
                    r.side,
                    fits
                );
            }
        }
        let virt = shape.cols.iter().filter(|c| c.virt).count();
        let stored = shape.cols.iter().filter(|c| c.stored).count();
        let exact: Vec<&Read> = reads.iter().filter(|r| r.exact).collect();
        // The measured layout per statement and side: MariaDB was found to log the whole row for
        // an INSERT and the base columns only for an UPDATE, so this is a list, not one value.
        let mut layout: Vec<String> = exact
            .iter()
            .map(|r| {
                format!(
                    "{:?}/{}={}{}",
                    r.kind,
                    r.side,
                    r.model.name(),
                    if r.also.is_empty() {
                        String::new()
                    } else {
                        format!("(also:{})", r.also.join(","))
                    }
                )
            })
            .collect();
        layout.sort_unstable();
        layout.dedup();
        let mut lens: Vec<usize> = reads.iter().map(|r| r.len).collect();
        lens.sort_unstable();
        lens.dedup();
        let mut virt_in: Vec<usize> = exact.iter().map(|r| r.virt_in_image).collect();
        virt_in.sort_unstable();
        virt_in.dedup();
        let mut stored_in: Vec<usize> = exact.iter().map(|r| r.stored_in_image).collect();
        stored_in.sort_unstable();
        stored_in.dedup();
        let mut mis: Vec<&str> = reads
            .iter()
            .flat_map(|r| r.misreads.iter())
            .map(|m| m.col.as_str())
            .collect();
        mis.sort_unstable();
        mis.dedup();
        // Position equals ordinal only where the image carries every catalog column.
        let pos_eq = if exact.is_empty() {
            "unknown(subset image)"
        } else if exact.iter().all(|r| r.model == Model::All) {
            "yes"
        } else {
            "no"
        };
        fact!(
            "FACT image={} flavor={} table={}.{} catalog_cols={} virtual_cols={} stored_cols={} \
             non_virtual_cols={} base_cols={} real_column_after_virtual={} tmap_cols={} \
             layouts={:?} image_lens={:?} virtual_values_in_image={:?} \
             stored_values_in_image={:?} image_position_eq_catalog_ordinal={} \
             misaligned_columns={:?} misalignment_observable={}",
            ctx.image,
            ctx.flavor,
            ctx.schema,
            t.name,
            shape.cols.len(),
            virt,
            stored,
            shape.real().len(),
            shape.base().len(),
            shape.has_virtual_before_real(),
            ctx.tmaps.get(t.name).copied().unwrap_or(u64::MAX),
            layout,
            lens,
            virt_in,
            stored_in,
            pos_eq,
            mis,
            reads
                .iter()
                .any(|r| r.misreads.iter().any(|m| m.observable))
        );
        if virt == 0 && stored == 0 {
            ensure!(
                reads
                    .iter()
                    .all(|r| r.model == Model::All && r.misreads.is_empty()),
                "{}: the control table has no generated column, so every image must be the whole \
                 catalog row in order",
                t.name
            );
            continue;
        }
        // Every exact image of one kind and side must follow the same layout — that is the fact
        // PR-2 builds its map from, so it may not depend on which statement produced the event.
        for r in &exact {
            ensure!(
                exact
                    .iter()
                    .filter(|o| o.kind == r.kind && o.side == r.side)
                    .all(|o| o.model == r.model),
                "{}: its {:?} {}-images follow different layouts",
                t.name,
                r.kind,
                r.side
            );
        }
        for r in &exact {
            if r.model == Model::All {
                continue;
            }
            ensure!(
                !mis.is_empty(),
                "{}: the {} layout drops generated columns, so a real column must be read from \
                 the wrong slot",
                t.name,
                r.model.name()
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// the run

async fn facts_on(flavor_label: &str, dsn: &str) -> Result<()> {
    let mut w = connect(dsn).await?;
    let ver: String = w.query_first("SELECT VERSION()").await?.unwrap_or_default();
    let flavor = if ver.contains("MariaDB") {
        Flavor::MariaDb
    } else if ver.contains("Percona") {
        Flavor::Percona
    } else {
        Flavor::MySql
    };
    let meta: String = w
        .query_first("SELECT @@binlog_row_metadata")
        .await?
        .unwrap_or_default();
    let fmt: String = w
        .query_first("SELECT @@binlog_format")
        .await?
        .unwrap_or_default();
    fact!("VER flavor={flavor_label} version={ver} binlog_format={fmt} binlog_row_metadata={meta}");

    let schema = unique_schema("gf");
    w.query_drop(format!("CREATE DATABASE `{schema}`"))
        .await
        .with_context(|| format!("CREATE DATABASE {schema}"))?;
    let r = run_phases(&mut w, &schema, flavor, dsn, flavor_label).await;
    w.query_drop(format!("DROP DATABASE `{schema}`")).await.ok();
    r
}

async fn run_phases(
    w: &mut Conn,
    schema: &str,
    flavor: Flavor,
    dsn: &str,
    flavor_label: &str,
) -> Result<()> {
    w.query_drop(format!(
        "CREATE TABLE `{schema}`.`{CTL}` (id INT NOT NULL PRIMARY KEY, tag VARCHAR(32) NOT NULL)"
    ))
    .await
    .context("creating the sentinel table")?;
    let mut shapes = HashMap::new();
    for t in TABLES {
        for stmt in t.ddl {
            let sql = stmt.replace("{S}", &format!("`{schema}`"));
            w.query_drop(&sql)
                .await
                .with_context(|| format!("DDL of {schema}.{}: {}", t.name, trunc(&sql, 200)))?;
        }
        let s = shape_of(w, schema, t.name).await?;
        fact!(
            "DDL flavor={flavor_label} table={}.{} cols=[{}]",
            schema,
            t.name,
            s.cols
                .iter()
                .enumerate()
                .map(|(o, c)| format!(
                    "{o}:{}:{}",
                    c.name,
                    if c.virt {
                        "VIRTUAL"
                    } else if c.stored {
                        "STORED"
                    } else {
                        "real"
                    }
                ))
                .collect::<Vec<_>>()
                .join(" ")
        );
        shapes.insert(t.name.to_string(), s);
    }

    for (p, image) in ["FULL", "MINIMAL", "NOBLOB"].iter().enumerate() {
        w.query_drop(format!("SET SESSION binlog_row_image='{image}'"))
            .await
            .with_context(|| format!("{flavor_label}: SET SESSION binlog_row_image='{image}'"))?;
        let now: String = w
            .query_first("SELECT @@session.binlog_row_image")
            .await?
            .unwrap_or_default();
        ensure!(
            now == *image,
            "{flavor_label}: binlog_row_image is {now}, not {image}"
        );
        let plan = Plan::new(p);
        fact!(
            "PHASE flavor={flavor_label} image={image} ids={} {}",
            plan.id1,
            plan.id2
        );

        // The position and the reader are taken BEFORE the writes, so the whole phase lies
        // ahead of the reader; the sentinel row of CTL closes the capture.
        let start = current_position(w).await.context("current_position")?;
        let mut reader = BinlogReader::connect(
            replica_opts(dsn)?,
            server_id(),
            flavor,
            &start,
            Duration::from_millis(500),
        )
        .await
        .with_context(|| format!("{flavor_label}: connecting the repo reader at {start}"))?;

        // INSERTs, snapshot, UPDATEs, snapshot, DELETEs, sentinel: each image is compared
        // against the state it was written from.
        for t in TABLES {
            write(w, schema, &plan, "INSERT", t.insert).await?;
        }
        let mut s1 = HashMap::new();
        for t in TABLES {
            s1.insert(
                t.name.to_string(),
                snapshot(w, schema, t.name, shapes.get(t.name).unwrap()).await?,
            );
        }
        for t in TABLES {
            write(w, schema, &plan, "UPDATE", t.update).await?;
        }
        let mut s2 = HashMap::new();
        for t in TABLES {
            s2.insert(
                t.name.to_string(),
                snapshot(w, schema, t.name, shapes.get(t.name).unwrap()).await?,
            );
        }
        for t in TABLES {
            write(w, schema, &plan, "DELETE", t.delete).await?;
        }
        w.query_drop(format!(
            "INSERT INTO `{schema}`.`{CTL}` (id,tag) VALUES ({},'{}')",
            plan.id1, image
        ))
        .await
        .context("writing the sentinel row")?;

        let (imgs, tmaps) = capture(&mut reader, schema, Duration::from_secs(120)).await?;
        reader.close().await.ok();
        let ctx = Ctx {
            flavor: flavor_label,
            image,
            schema,
            plan: &plan,
            snaps: &Snaps { s1, s2 },
            tmaps,
        };
        check_phase(&ctx, &imgs, &shapes)
            .with_context(|| format!("{flavor_label}: image={image}"))?;
    }
    Ok(())
}

async fn write(w: &mut Conn, schema: &str, plan: &Plan, kind: &str, stmt: &str) -> Result<()> {
    let sql = plan.fill(stmt).replace("{S}", &format!("`{schema}`"));
    w.query_drop(&sql)
        .await
        .with_context(|| format!("{kind} into {schema}: {}", trunc(&sql, 200)))
}
