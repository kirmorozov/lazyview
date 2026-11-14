//! PostgreSQL as a *target* (issue #56, PR C1): the harness side. Servers come from
//! `LAZYVIEW_TEST_PG_DSNS` (never `LAZYVIEW_TEST_DSNS`, which is the source list), each test
//! schema is a fresh `lvt_<tag>_<6 letters>` (the prefix CI's grants and leftover cleanup key
//! on), and `fetch` renders every cell into the bytes the MySQL oracle's `fetch` produces,
//! from the PostgreSQL column type (the T2 map's forms) — one small table, no per-view rules:
//! integers/numeric/text as-is; `real`/`double precision` as the `f64` bit pattern (both sides
//! parsed, never compared as text); `date`/`timestamp` as-is; `timestamp with time zone` with
//! its constant `+00` stripped (session `TimeZone='UTC'`); `text` TIME as-is (equal to
//! `datum.rs`'s canonical form); `bytea` hex-decoded (session `bytea_output='hex'`); `jsonb`
//! reparsed with `serde_json` and re-serialized with sorted keys (PG sorts and dedups keys,
//! `mv::json_canonical` does not). `_lv_*` columns are visible on a PostgreSQL target (D6), so
//! `fetch` selects only the columns that do not start with `_lv_` — what a reader sees.
//! Reuses `tokio_postgres` (a lib dependency since T1): no second driver.

use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::{dsn_with_db, oracle};

/// One PostgreSQL server from `LAZYVIEW_TEST_PG_DSNS` (a target, never a source).
#[derive(Debug, Clone)]
pub struct Server {
    /// Flavor label from `LAZYVIEW_TEST_PG_DSNS`, e.g. `pg16`.
    pub flavor: String,
    /// DSN as given; its database is the admin database, test schemas live next to it.
    pub base_dsn: String,
}

/// A test's own schema on one PostgreSQL target (its database and schema share the `lvt_`
/// name, so the engine's `schema_of` path yields exactly `schema`).
#[derive(Debug, Clone)]
pub struct PgDb {
    pub flavor: String,
    pub schema: String,
    /// DSN pointing at this database (= schema) of the target.
    pub dsn: String,
}

impl PgDb {
    /// A client on this schema: a fresh connection with `search_path` set to it. The driver
    /// task runs until the client drops, so no backend outlives the test.
    pub async fn client(&self) -> Result<Client> {
        let c = connect(&self.dsn).await?;
        c.batch_execute(&format!("SET search_path TO {}", qid(&self.schema)))
            .await
            .with_context(|| format!("SET search_path on {}/{}", self.schema, "pg"))?;
        Ok(c)
    }
}

/// One PostgreSQL column the comparison sees (visible columns only, `_lv_*` dropped).
#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    /// `information_schema.columns.data_type`, e.g. `double precision`, `jsonb`.
    pub ty: String,
    pub not_null: bool,
}

/// Servers listed in `LAZYVIEW_TEST_PG_DSNS`, or `None` when unset/empty.
pub fn pg_servers() -> Option<Vec<Server>> {
    let raw = std::env::var("LAZYVIEW_TEST_PG_DSNS").ok()?;
    let mut out = Vec::new();
    for (i, part) in raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .enumerate()
    {
        let (flavor, dsn) = match part.split_once('=') {
            Some((n, d)) if !n.contains("://") => (n.trim().to_string(), d.trim().to_string()),
            _ => (format!("pg{i}"), part.to_string()),
        };
        out.push(Server {
            flavor,
            base_dsn: dsn,
        });
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// The servers for a PostgreSQL-target scenario: `Some` when published, `None` after printing
/// a SKIP line when not — unless `LAZYVIEW_TEST_REQUIRE_PG=1`, where a missing server fails.
pub fn servers_or_skip(test: &str) -> Option<Vec<Server>> {
    match pg_servers() {
        Some(s) => Some(s),
        None => {
            if std::env::var("LAZYVIEW_TEST_REQUIRE_PG").is_ok_and(|v| v.trim() == "1") {
                panic!(
                    "{test}: LAZYVIEW_TEST_REQUIRE_PG=1 but LAZYVIEW_TEST_PG_DSNS lists no server"
                );
            }
            println!(
                "SKIP {test}: no PostgreSQL target (LAZYVIEW_TEST_PG_DSNS unset; \
                 LAZYVIEW_TEST_REQUIRE_PG=1 fails instead)"
            );
            None
        }
    }
}

fn qid(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// One attempt: connect and spawn the driver task (joined when the client drops).
pub async fn connect(dsn: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connecting to PostgreSQL {dsn}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

fn registry() -> &'static Mutex<Vec<(String, String)>> {
    static R: OnceLock<Mutex<Vec<(String, String)>>> = OnceLock::new();
    R.get_or_init(Default::default)
}

/// A fresh schema on `server`: database and schema `lvt_<tag>_<6 letters>` (the engine's
/// `schema_of` path yields the schema), registered for [`drop_dbs`].
pub async fn new_pg_db(server: &Server, tag: &str) -> Result<PgDb> {
    let suffix: String = (0..6)
        .map(|_| {
            let n: u8 = rand::random();
            (b'a' + n % 26) as char
        })
        .collect();
    let name = format!("lvt_{tag}_{suffix}");
    let admin = connect(&server.base_dsn)
        .await
        .with_context(|| format!("[{}] PostgreSQL admin connection", server.flavor))?;
    admin
        .batch_execute(&format!("CREATE DATABASE {}", qid(&name)))
        .await
        .with_context(|| format!("CREATE DATABASE {name}"))?;
    drop(admin);
    let dsn = dsn_with_db(&server.base_dsn, &name);
    let client = connect(&dsn).await?;
    client
        .batch_execute(&format!("CREATE SCHEMA {}", qid(&name)))
        .await
        .with_context(|| format!("CREATE SCHEMA {name}"))?;
    drop(client);
    registry()
        .lock()
        .unwrap()
        .push((server.base_dsn.clone(), name.clone()));
    Ok(PgDb {
        flavor: server.flavor.clone(),
        schema: name,
        dsn,
    })
}

/// Drops this test's databases (`WITH (FORCE)`: the engine's backends go too). Only the
/// databases of `dbs`: concurrent scenario runs share the registry and must not drop each
/// other's schemas.
pub async fn drop_dbs(dbs: &[PgDb]) -> Result<()> {
    let mine: Vec<(String, String)> = {
        let mut reg = registry().lock().unwrap();
        let keep = |n: &String| !dbs.iter().any(|d| &d.schema == n);
        let out = reg.iter().filter(|(_, n)| !keep(n)).cloned().collect();
        reg.retain(|(_, n)| keep(n));
        out
    };
    let mut first = None;
    for (admin_dsn, name) in mine {
        let r = async {
            let client = connect(&admin_dsn).await?;
            client
                .batch_execute(&format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    qid(&name)
                ))
                .await
                .with_context(|| format!("DROP DATABASE {name}"))?;
            drop(client);
            Ok(())
        }
        .await;
        if r.is_err() && first.is_none() {
            first = r.err();
        }
    }
    first.map_or(Ok(()), Err)
}

/// The visible columns of `"schema"."table"` in declaration order (the `_lv_*` columns are
/// visible on a PostgreSQL target (D6) but not what a reader of the view sees).
pub async fn columns(client: &Client, schema: &str, table: &str) -> Result<Vec<Col>> {
    let rows = client
        .query(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            &[&schema.to_string(), &table.to_string()],
        )
        .await
        .with_context(|| format!("column types of {schema}.{table}"))?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            let name: String = r.get(0);
            if name.starts_with("_lv_") {
                return None;
            }
            Some(Col {
                name,
                ty: r.get(1),
                not_null: r.get::<_, String>(2) == "NO",
            })
        })
        .collect())
}

/// Whether `"schema"."table"` exists.
pub async fn table_exists(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let rows = client
        .query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            &[&schema.to_string(), &table.to_string()],
        )
        .await?;
    Ok(!rows.is_empty())
}

/// Whether `"schema"."table"` has a unique index — the row key its upserts name (T4's
/// `ON CONFLICT` target; the same fact `PgConn::has_row_index` checks on the engine side).
pub async fn has_row_index(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let rows = client
        .query(
            "SELECT count(*) FROM pg_index i \
             JOIN pg_class t ON t.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE n.nspname = $1 AND t.relname = $2 AND i.indisunique",
            &[&schema.to_string(), &table.to_string()],
        )
        .await
        .with_context(|| format!("unique index of {schema}.{table}"))?;
    Ok(rows.first().is_some_and(|r| r.get::<_, i64>(0) > 0))
}

/// Reads `"schema"."table"` (visible columns only) as the oracle's `Table`, every cell rendered
/// into the bytes the MySQL oracle's `fetch` produces for the column's type (module docs).
pub async fn fetch(client: &Client, schema: &str, table: &str) -> Result<oracle::Table> {
    let cols = columns(client, schema, table).await?;
    client.batch_execute("SET extra_float_digits = 1").await?;
    client.batch_execute("SET bytea_output = 'hex'").await?;
    client.batch_execute("SET TimeZone = 'UTC'").await?;
    let list = cols
        .iter()
        .map(|c| format!("{}::text", qid(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {list} FROM {}.{}", qid(schema), qid(table));
    let rows = client
        .query(sql.as_str(), &[])
        .await
        .with_context(|| format!("fetching {schema}.{table} from PostgreSQL"))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut cells = Vec::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            let txt: Option<String> = row.get(i);
            cells.push(match txt {
                None => None,
                Some(t) => Some(normalize_pg(&c.ty, &t)?),
            });
        }
        out.push(cells);
    }
    Ok(oracle::Table {
        columns: cols.iter().map(|c| c.name.clone()).collect(),
        rows: out,
    })
}

/// The PostgreSQL text of a cell (`extra_float_digits=1`, `bytea_output='hex'`, `TimeZone=UTC`)
/// rendered into the comparison bytes of its column type.
pub fn normalize_pg(ty: &str, text: &str) -> Result<Vec<u8>> {
    Ok(match ty {
        "real" | "double precision" => f64_bits(text)?,
        "timestamp with time zone" => {
            strip_frac_zeros(text.strip_suffix("+00").unwrap_or(text)).into_bytes()
        }
        "bytea" => hex_decode(text)?,
        "jsonb" => json_canonical(text)?.into_bytes(),
        t if t.starts_with("timestamp") => strip_frac_zeros(text).into_bytes(),
        _ => text.as_bytes().to_vec(),
    })
}

/// MySQL prints a `datetime(6)` with all six fraction digits (`...54.971160`), PostgreSQL
/// trims trailing ones (`...54.97116`) — the same value, so the fraction is compared with its
/// trailing zeros (and the dot) removed on both sides.
fn strip_frac_zeros(text: &str) -> String {
    match text.split_once('.') {
        Some((d, f)) => {
            let f = f.trim_end_matches('0');
            if f.is_empty() {
                d.to_string()
            } else {
                format!("{d}.{f}")
            }
        }
        None => text.to_string(),
    }
}

/// The MySQL oracle text of the same column rendered into the same comparison bytes (floats
/// and `jsonb` only; the other forms agree as bytes already).
pub fn normalize_src(ty: &str, cell: Option<Vec<u8>>) -> Result<Option<Vec<u8>>> {
    match (ty, cell) {
        ("real" | "double precision", Some(b)) => Ok(Some(f64_bits(&String::from_utf8_lossy(&b))?)),
        ("jsonb", Some(b)) => Ok(Some(
            json_canonical(&String::from_utf8_lossy(&b))?.into_bytes(),
        )),
        (t, Some(b)) if t.starts_with("timestamp") || t == "date" => Ok(Some(
            strip_frac_zeros(&String::from_utf8_lossy(&b)).into_bytes(),
        )),
        (_, c) => Ok(c),
    }
}

fn f64_bits(text: &str) -> Result<Vec<u8>> {
    let v: f64 = text
        .trim()
        .parse()
        .with_context(|| format!("not a float: {text:?}"))?;
    Ok(v.to_bits().to_le_bytes().to_vec())
}

fn hex_decode(text: &str) -> Result<Vec<u8>> {
    let hex = text.strip_prefix("\\x").unwrap_or(text);
    anyhow::ensure!(hex.len().is_multiple_of(2), "odd hex length in {text:?}");
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .with_context(|| format!("bad hex byte in {text:?}"))
        })
        .collect()
}

/// Reparse with `serde_json` and re-serialize with sorted keys (PG's jsonb sorts and dedups
/// keys, `mv::json_canonical` does not, so both sides go through here).
fn json_canonical(text: &str) -> Result<String> {
    let v: serde_json::Value =
        serde_json::from_str(text).with_context(|| format!("not JSON: {}", clip(text)))?;
    Ok(json_of(&v))
}

fn json_of(v: &serde_json::Value) -> String {
    use serde_json::Value as J;
    match v {
        J::Null => "null".to_string(),
        J::Bool(b) => b.to_string(),
        J::Number(n) => n.to_string(),
        J::String(s) => serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\"")),
        J::Array(a) => format!("[{}]", a.iter().map(json_of).collect::<Vec<_>>().join(",")),
        J::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            format!(
                "{{{}}}",
                keys.iter()
                    .map(|k| format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap_or_else(|_| format!("\"{k}\"")),
                        m.get(k.as_str())
                            .map(json_of)
                            .unwrap_or_else(|| "null".to_string())
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn clip(s: &str) -> String {
    let c = s.chars().take(60).collect::<String>();
    if s.chars().count() > 60 {
        format!("{c}…")
    } else {
        c
    }
}

fn is_zero_date(b: &[u8]) -> bool {
    b.starts_with(b"0000-00-00")
}

fn is_date_family(ty: &str) -> bool {
    ty == "date" || ty.starts_with("timestamp")
}

/// Lines both tables up for [`oracle::diff_tables`]: the source side through
/// [`normalize_src`], and the zero-date rule (D5) — in a NOT NULL date column a source zero
/// date compares as NULL on both sides (the writer sends NULL there); a *nullable* date column
/// that holds one is dropped from both tables and the row count is asserted instead (the test
/// comments on this).
pub fn align(
    cols: &[Col],
    expected: oracle::Table,
    actual: oracle::Table,
) -> (oracle::Table, oracle::Table, Option<String>) {
    let n = cols.len();
    let zero_in = |i: usize, rows: &[Vec<Option<Vec<u8>>>]| {
        rows.iter().any(|r| {
            r.get(i)
                .is_some_and(|c| c.as_deref().is_some_and(is_zero_date))
        })
    };
    let mut keep = vec![true; n];
    let mut note = None;
    for (i, c) in cols.iter().enumerate() {
        if is_date_family(&c.ty) && !c.not_null && zero_in(i, &expected.rows) {
            keep[i] = false;
            note = Some(format!(
                "  column `{}` is a nullable date column holding a source zero date: skipped, \
                 the row count is asserted instead (oracle {} rows, MV {} rows)\n",
                c.name,
                expected.rows.len(),
                actual.rows.len()
            ));
        }
    }
    if keep.iter().any(|k| !k) && expected.rows.len() != actual.rows.len() {
        return (
            expected.clone(),
            actual.clone(),
            Some(note.clone().unwrap_or_else(|| {
                format!(
                    "  row count with a skipped zero-date column: oracle {} vs MV {}\n",
                    expected.rows.len(),
                    actual.rows.len()
                )
            })),
        );
    }
    let map = |rows: &[Vec<Option<Vec<u8>>>]| -> Vec<Vec<Option<Vec<u8>>>> {
        rows.iter()
            .map(|r| {
                (0..n)
                    .filter(|i| keep[*i])
                    .map(|i| {
                        let cell = r.get(i).cloned().flatten();
                        if is_date_family(&cols[i].ty)
                            && cols[i].not_null
                            && cell.as_deref().is_some_and(is_zero_date)
                        {
                            return None;
                        }
                        let fallback = cell.clone();
                        normalize_src(&cols[i].ty, cell).unwrap_or(fallback)
                    })
                    .collect()
            })
            .collect()
    };
    (
        oracle::Table {
            columns: (0..n)
                .filter(|i| keep[*i])
                .map(|i| cols[i].name.clone())
                .collect(),
            rows: map(&expected.rows),
        },
        oracle::Table {
            columns: (0..n)
                .filter(|i| keep[*i])
                .map(|i| cols[i].name.clone())
                .collect(),
            rows: map(&actual.rows),
        },
        None,
    )
}
