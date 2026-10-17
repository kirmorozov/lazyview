//! The oracle: for every materialized view `name` a plain SQL view `_oracle_<name>` with the same
//! SELECT is created on the source; the MV's `SELECT *` must equal it as a row multiset.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, Value};

use super::{exec, ident, table_type, Db};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Expected to be maintained incrementally (SPJ / grouped / global aggregate).
    Incremental,
    /// Expected to need the full-refresh fallback (DISTINCT, UNION, LIMIT, subquery, no-PK SPJ).
    Full,
    /// Reads another MV (chained).
    Chained,
}

/// One materialized view under test.
#[derive(Debug, Clone)]
pub struct ViewSpec {
    pub name: String,
    /// The SELECT (no trailing `;`).
    pub select: String,
    /// Optional view column list (without parentheses), e.g. `a, b, c`.
    pub columns: Option<String>,
    /// Statement inserted into `_lazyview.definition`.
    pub definition: String,
    pub kind: Kind,
    /// `_lazyview.targets` (comma separated; `None` = source).
    pub targets: Option<String>,
}

impl ViewSpec {
    pub fn new(name: &str, kind: Kind, select: &str) -> ViewSpec {
        let select = normalize_ws(select);
        ViewSpec {
            name: name.to_string(),
            definition: format!("CREATE VIEW {name} AS {select}"),
            select,
            columns: None,
            kind,
            targets: None,
        }
    }

    /// Same view written with MySQL noise: `head` goes between CREATE and VIEW (e.g.
    /// `OR REPLACE ALGORITHM=MERGE DEFINER=CURRENT_USER SQL SECURITY INVOKER`), the name is
    /// backticked, an explicit column list is added and `tail` (e.g. `WITH CHECK OPTION`) appended.
    pub fn noisy(mut self, head: &str, columns: Option<&str>, tail: &str) -> ViewSpec {
        self.columns = columns.map(str::to_string);
        let cols = columns.map(|c| format!(" ({c})")).unwrap_or_default();
        self.definition = format!(
            "CREATE {head} VIEW `{}`{cols} AS {} {tail}",
            self.name, self.select
        )
        .trim()
        .to_string();
        self
    }

    pub fn with_targets(mut self, targets: &str) -> ViewSpec {
        self.targets = Some(targets.to_string());
        self
    }

    /// Target names this view is materialized on (`source` = the source instance).
    pub fn target_names(&self) -> Vec<String> {
        let list: Vec<String> = self
            .targets
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if list.is_empty() {
            vec!["source".to_string()]
        } else {
            list
        }
    }

    pub fn oracle_name(&self) -> String {
        format!("_oracle_{}", self.name)
    }

    pub fn oracle_ddl(&self) -> String {
        let cols = self
            .columns
            .as_ref()
            .map(|c| format!(" ({c})"))
            .unwrap_or_default();
        format!(
            "CREATE OR REPLACE VIEW {}{cols} AS {}",
            ident(&self.oracle_name()),
            self.select
        )
    }
}

pub fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A fetched result set (raw bytes; NULL = None).
#[derive(Debug, Clone)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

pub async fn fetch(conn: &mut Conn, sql: &str) -> Result<Table> {
    let mut res = conn
        .query_iter(sql)
        .await
        .with_context(|| format!("SQL failed: {sql}"))?;
    let columns: Vec<String> = res
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let raw: Vec<Row> = res.collect().await?;
    drop(res);
    let rows = raw
        .into_iter()
        .map(|r| {
            r.unwrap()
                .into_iter()
                .map(|v| match v {
                    Value::NULL => None,
                    Value::Bytes(b) => Some(b),
                    other => Some(other.as_sql(true).into_bytes()),
                })
                .collect()
        })
        .collect();
    Ok(Table { columns, rows })
}

pub fn fmt_cell(c: &Option<Vec<u8>>) -> String {
    match c {
        None => "NULL".into(),
        Some(b) => match std::str::from_utf8(b) {
            // Control characters (binary-ish values that happen to be valid UTF-8) are shown as hex.
            Ok(s) if !s.chars().any(|c| c.is_control() && c != '\n') => {
                format!("'{}'", s.replace('\n', "\\n"))
            }
            _ => format!("0x{}", hex::encode(b)),
        },
    }
}

pub fn fmt_row(r: &[Option<Vec<u8>>]) -> String {
    format!(
        "({})",
        r.iter().map(fmt_cell).collect::<Vec<_>>().join(", ")
    )
}

/// Compares two result sets as multisets; `None` if equal, else a readable diff.
pub fn diff_tables(expected: &Table, actual: &Table, max_rows: usize) -> Option<String> {
    let mut out = String::new();
    if expected.columns != actual.columns {
        let _ = writeln!(
            out,
            "  column mismatch: oracle {:?} vs MV {:?}",
            expected.columns, actual.columns
        );
        if expected.columns.len() != actual.columns.len() {
            return Some(out);
        }
    }
    let mut counts: BTreeMap<&Vec<Option<Vec<u8>>>, i64> = BTreeMap::new();
    for r in &expected.rows {
        *counts.entry(r).or_default() += 1;
    }
    for r in &actual.rows {
        *counts.entry(r).or_default() -= 1;
    }
    let mut missing: Vec<(&Vec<Option<Vec<u8>>>, i64)> = Vec::new();
    let mut extra: Vec<(&Vec<Option<Vec<u8>>>, i64)> = Vec::new();
    for (r, c) in counts {
        if c > 0 {
            missing.push((r, c));
        } else if c < 0 {
            extra.push((r, -c));
        }
    }
    if missing.is_empty() && extra.is_empty() {
        return if out.is_empty() { None } else { Some(out) };
    }
    let n_missing: i64 = missing.iter().map(|x| x.1).sum();
    let n_extra: i64 = extra.iter().map(|x| x.1).sum();
    let _ = writeln!(
        out,
        "  oracle has {} rows, MV has {}; {} row(s) missing from MV, {} unexpected row(s) in MV",
        expected.rows.len(),
        actual.rows.len(),
        n_missing,
        n_extra
    );
    let _ = writeln!(out, "  columns: ({})", expected.columns.join(", "));
    // Pair rows that agree on the first column ("same key, different values").
    let mut extra_by_key: HashMap<&Option<Vec<u8>>, Vec<usize>> = HashMap::new();
    for (i, (r, _)) in extra.iter().enumerate() {
        if let Some(k) = r.first() {
            extra_by_key.entry(k).or_default().push(i);
        }
    }
    let mut paired_extra = vec![false; extra.len()];
    let mut shown = 0;
    for (r, c) in &missing {
        if shown >= max_rows {
            let _ = writeln!(out, "  … more differences omitted");
            break;
        }
        let mult = if *c > 1 {
            format!(" ×{c}")
        } else {
            String::new()
        };
        let partner = r
            .first()
            .and_then(|k| extra_by_key.get_mut(k))
            .and_then(|v| {
                let pos = v.iter().position(|i| !paired_extra[*i])?;
                Some(v.remove(pos))
            });
        match partner {
            Some(i) => {
                paired_extra[i] = true;
                let _ = writeln!(out, "  ≠ expected {}{mult}", fmt_row(r));
                let _ = writeln!(out, "       MV has {}", fmt_row(extra[i].0));
            }
            None => {
                let _ = writeln!(out, "  - missing  {}{mult}", fmt_row(r));
            }
        }
        shown += 1;
    }
    for (i, (r, c)) in extra.iter().enumerate() {
        if paired_extra[i] {
            continue;
        }
        if shown >= max_rows {
            let _ = writeln!(out, "  … more differences omitted");
            break;
        }
        let mult = if *c > 1 {
            format!(" ×{c}")
        } else {
            String::new()
        };
        let _ = writeln!(out, "  + extra    {}{mult}", fmt_row(r));
        shown += 1;
    }
    Some(out)
}

/// Creates/refreshes the oracle view of `spec` on the source connection.
pub async fn install_oracle(src: &mut Conn, spec: &ViewSpec) -> Result<()> {
    exec(src, &spec.oracle_ddl())
        .await
        .with_context(|| format!("creating oracle view for {}", spec.name))
}

fn installed() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static S: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    S.get_or_init(Default::default)
}

/// Creates the oracle view once per (schema, definition): repeated DDL is slow and every DDL is
/// also binlog traffic the engine has to look at.
async fn ensure_oracle(src: &mut Conn, schema: &str, spec: &ViewSpec, force: bool) -> Result<()> {
    let key = format!("{schema}\0{}", spec.oracle_ddl());
    if !force && installed().lock().unwrap().contains(&key) {
        return Ok(());
    }
    install_oracle(src, spec).await?;
    installed().lock().unwrap().insert(key);
    Ok(())
}

/// Reads the oracle view, re-creating it once if the read fails (e.g. invalidated by DDL).
async fn fetch_oracle(src: &mut Conn, schema: &str, spec: &ViewSpec) -> Result<Table> {
    let sql = format!("SELECT * FROM {}", ident(&spec.oracle_name()));
    match fetch(src, &sql).await {
        Ok(t) => Ok(t),
        Err(_) => {
            ensure_oracle(src, schema, spec, true).await?;
            fetch(src, &sql).await
        }
    }
}

/// Compares one view: oracle (on `src`, schema `schema`) vs MV table (on `mv`, or on `src` when
/// `None`, then both reads happen inside one consistent snapshot).
pub async fn compare_view(
    src: &mut Conn,
    schema: &str,
    mv: Option<&mut Conn>,
    spec: &ViewSpec,
) -> Result<Option<String>> {
    ensure_oracle(src, schema, spec, false).await?;
    let mv_sql = format!("SELECT * FROM {}", ident(&spec.name));
    let (expected, actual) = match mv {
        None => {
            match table_type(src, &spec.name).await? {
                Some(t) if t == "BASE TABLE" => {}
                Some(t) => {
                    return Ok(Some(format!(
                        "  MV `{}` is a {t}, expected a BASE TABLE\n",
                        spec.name
                    )))
                }
                None => return Ok(Some(format!("  MV table `{}` does not exist\n", spec.name))),
            }
            exec(src, "START TRANSACTION WITH CONSISTENT SNAPSHOT").await?;
            let e = fetch_oracle(src, schema, spec).await;
            let a = fetch(src, &mv_sql).await;
            exec(src, "COMMIT").await?;
            (e?, a?)
        }
        Some(mvc) => {
            match table_type(mvc, &spec.name).await? {
                Some(t) if t == "BASE TABLE" => {}
                Some(t) => {
                    return Ok(Some(format!(
                        "  MV `{}` on target is a {t}, expected a BASE TABLE\n",
                        spec.name
                    )))
                }
                None => {
                    return Ok(Some(format!(
                        "  MV table `{}` does not exist on the target\n",
                        spec.name
                    )))
                }
            }
            let e = fetch_oracle(src, schema, spec).await?;
            let a = fetch(mvc, &mv_sql).await?;
            (e, a)
        }
    };
    Ok(diff_tables(&expected, &actual, 15))
}

/// Compares every view materialized on the source (targets other than `source` are ignored).
pub async fn check_views(src: &Db, specs: &[ViewSpec]) -> Result<()> {
    check_views_targets(src, &HashMap::new(), specs).await
}

/// Compares every (view, target) pair. `targets` maps target names to their schemas; the
/// `source` target is `src`. Collects all mismatches into one error.
pub async fn check_views_targets(
    src: &Db,
    targets: &HashMap<String, Db>,
    specs: &[ViewSpec],
) -> Result<()> {
    let mut sc = src.conn().await?;
    let mut tconns: HashMap<String, Conn> = HashMap::new();
    let mut failures = String::new();
    let mut n_bad = 0;
    for spec in specs {
        for t in spec.target_names() {
            let res = if t == "source" {
                compare_view(&mut sc, &src.name, None, spec).await?
            } else if let Some(db) = targets.get(&t) {
                if !tconns.contains_key(&t) {
                    tconns.insert(t.clone(), db.conn().await?);
                }
                compare_view(&mut sc, &src.name, tconns.get_mut(&t), spec).await?
            } else {
                continue;
            };
            if let Some(d) = res {
                n_bad += 1;
                let where_ = if t == "source" {
                    String::new()
                } else {
                    format!(" on target {t}")
                };
                let _ = write!(
                    failures,
                    "\nview `{}`{where_} ({:?}) differs from oracle:\n  definition: {}\n{d}",
                    spec.name, spec.kind, spec.definition
                );
            }
        }
    }
    for (_, c) in tconns {
        c.disconnect().await.ok();
    }
    sc.disconnect().await.ok();
    if n_bad > 0 {
        bail!(
            "{n_bad} view(s) differ from their oracle (schema {}):{failures}",
            src.name
        );
    }
    Ok(())
}

/// Order-independent content fingerprint of a table (row count + sorted-row hash), via SQL text.
pub async fn fingerprint(conn: &mut Conn, table: &str) -> Result<(usize, u64)> {
    let t = fetch(conn, &format!("SELECT * FROM {}", ident(table))).await?;
    let mut rows = t.rows;
    rows.sort();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for r in &rows {
        for c in r {
            match c {
                None => h = (h ^ 0xff).wrapping_mul(0x100_0000_01b3),
                Some(b) => {
                    for x in b {
                        h = (h ^ *x as u64).wrapping_mul(0x100_0000_01b3);
                    }
                    h = (h ^ 0xfe).wrapping_mul(0x100_0000_01b3);
                }
            }
        }
    }
    Ok((rows.len(), h))
}

/// Errors unless `cond` (used for small sanity assertions in scenarios).
pub fn ensure_eq<T: PartialEq + std::fmt::Debug>(what: &str, actual: T, expected: T) -> Result<()> {
    if actual != expected {
        return Err(anyhow!("{what}: expected {expected:?}, got {actual:?}"));
    }
    Ok(())
}

/// `CREATE TABLE` DDL with the column types of an existing table/view (every column NULLable),
/// built from information_schema — the same approach the engine uses for MV tables.
pub async fn table_ddl_like(conn: &mut Conn, source: &str, new_name: &str) -> Result<String> {
    let cols = super::rows(
        conn,
        &format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            super::q(source)
        ),
    )
    .await?;
    if cols.is_empty() {
        bail!("no columns for {source}");
    }
    let defs: Vec<String> = cols
        .iter()
        .map(|c| {
            let name = c[0].clone().unwrap_or_default();
            let ty = c[1].clone().unwrap_or_default();
            let cs = match (&c[2], &c[3]) {
                (Some(cs), Some(co)) => format!(" CHARACTER SET {cs} COLLATE {co}"),
                _ => String::new(),
            };
            format!("{} {ty}{cs} NULL DEFAULT NULL", ident(&name))
        })
        .collect();
    Ok(format!(
        "CREATE TABLE {} ({})",
        ident(new_name),
        defs.join(", ")
    ))
}

/// Materializes every spec by hand from its oracle (in order, so chained views find their
/// upstream "MV"): `fresh` creates the tables, otherwise their contents are replaced.
pub async fn hand_materialize(db: &Db, specs: &[ViewSpec], fresh: bool) -> Result<()> {
    let mut c = db.conn().await?;
    for s in specs {
        install_oracle(&mut c, s).await?;
        if fresh {
            let ddl = table_ddl_like(&mut c, &s.oracle_name(), &s.name).await?;
            exec(&mut c, &ddl).await?;
        } else {
            exec(&mut c, &format!("DELETE FROM {}", ident(&s.name))).await?;
        }
        // Copied through the client: MySQL 8 rejects some INSERT … SELECT MAX(timestamp) … GROUP BY
        // with a bogus "Incorrect datetime value '0000-00-00'" error.
        let t = fetch(
            &mut c,
            &format!("SELECT * FROM {}", ident(&s.oracle_name())),
        )
        .await?;
        insert_rows(&mut c, &s.name, &t).await?;
    }
    c.disconnect().await.ok();
    Ok(())
}

/// Literal for a text-protocol cell (UTF-8 → quoted string, otherwise a hex literal).
pub fn cell_literal(c: &Option<Vec<u8>>) -> String {
    match c {
        None => "NULL".into(),
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => super::q(s),
            Err(_) => format!("X'{}'", hex::encode(b)),
        },
    }
}

/// Inserts fetched rows into `table` with multi-row INSERTs.
pub async fn insert_rows(conn: &mut Conn, table: &str, t: &Table) -> Result<()> {
    for chunk in t.rows.chunks(200) {
        let vals: Vec<String> = chunk
            .iter()
            .map(|r| {
                format!(
                    "({})",
                    r.iter().map(cell_literal).collect::<Vec<_>>().join(", ")
                )
            })
            .collect();
        exec(
            conn,
            &format!("INSERT INTO {} VALUES {}", ident(table), vals.join(", ")),
        )
        .await?;
    }
    Ok(())
}
