use super::*;

// ---------------------------------------------------------------------------------------------
// The monkey-wrench rename test (LAZYVIEW_TEST_RENAME)
// ---------------------------------------------------------------------------------------------

/// `LAZYVIEW_TEST_RENAME=<seed>`: every scenario runs twice with the same workload seed, as
/// written and renamed (see [`compare_renamed`]).
pub fn rename_seed() -> Option<u64> {
    let v = std::env::var("LAZYVIEW_TEST_RENAME").ok()?;
    let v = v.trim();
    (!v.is_empty()).then(|| v.parse().expect("LAZYVIEW_TEST_RENAME must be a u64"))
}

/// `LAZYVIEW_TEST_RENAME_CASE=1`: renamed runs also write each column reference in a letter case
/// of its own. Off by default while findings RI-1 to RI-4 are open (the analyzer compares some
/// expressions as text, see `src/analyzer/tests_rename.rs`).
pub fn rename_case() -> bool {
    std::env::var("LAZYVIEW_TEST_RENAME_CASE").is_ok_and(|v| v.trim() == "1")
}

/// `LAZYVIEW_TEST_RENAME_METRICS=strict`: the renamed run must also have the plain run's engine
/// metrics (meaningful for quiesced, single-writer scenarios only: batching follows timing).
pub fn rename_metrics_strict() -> bool {
    std::env::var("LAZYVIEW_TEST_RENAME_METRICS").is_ok_and(|v| v.trim() == "strict")
}

pub type SharedNames = Arc<Mutex<rename::Names>>;

/// Schemas of renamed runs (by their real name) → the renaming of their statements.
pub(crate) fn renamed_schemas() -> &'static Mutex<HashMap<String, SharedNames>> {
    static S: std::sync::OnceLock<Mutex<HashMap<String, SharedNames>>> = std::sync::OnceLock::new();
    S.get_or_init(Default::default)
}

/// The renaming of the schema `conn` works in, in a renamed run.
pub fn names_of(conn: &Conn) -> Option<SharedNames> {
    let db = conn.opts().db_name()?;
    renamed_schemas().lock().unwrap().get(db).cloned()
}

/// A statement as sent (renamed in a renamed run) and how to read its results.
pub struct RenamedSql {
    pub sql: String,
    /// The renaming, when the results carry names (lazyview's metadata, system schemas).
    back: Option<SharedNames>,
    /// The renaming of the statement's run (column names of results map back).
    names: Option<SharedNames>,
}

impl RenamedSql {
    /// The statement for error messages (both forms when renamed).
    pub fn shown(&self, written: &str) -> String {
        if self.sql == written {
            clip(written, 600)
        } else {
            format!("{} (renamed: {})", clip(written, 600), clip(&self.sql, 600))
        }
    }

    /// A result cell as the test reads it.
    pub fn cell(&self, v: String) -> String {
        match &self.back {
            Some(n) => {
                let n = n.lock().unwrap();
                n.original(&v).unwrap_or_else(|| n.unrename_message(&v))
            }
            None => v,
        }
    }

    /// A raw result cell as the test reads it (only text cells carry names).
    pub fn cell_bytes(&self, b: Vec<u8>) -> Vec<u8> {
        match (&self.back, String::from_utf8(b)) {
            (Some(_), Ok(t)) => self.cell(t).into_bytes(),
            (_, Ok(t)) => t.into_bytes(),
            (_, Err(e)) => e.into_bytes(),
        }
    }

    /// A result column name as the test reads it.
    pub fn column(&self, c: String) -> String {
        match &self.names {
            Some(n) => n.lock().unwrap().original(&c).unwrap_or(c),
            None => c,
        }
    }
}

impl RenamedSql {
    /// A typed row as the test reads it (text cells of metadata results map back).
    fn row(&self, mut row: Row) -> Row {
        if self.back.is_some() {
            for i in 0..row.len() {
                if let Some(Value::Bytes(b)) = row.as_ref(i).cloned() {
                    row.place(i, Value::Bytes(self.cell_bytes(b)));
                }
            }
        }
        row
    }

    /// Statement parameters as sent: in statements on lazyview's metadata or a system schema,
    /// string parameters that are names are renamed like literals.
    fn params(&self, p: mysql_async::Params) -> mysql_async::Params {
        let (Some(n), mysql_async::Params::Positional(v)) = (&self.back, &p) else {
            return p;
        };
        let n = n.lock().unwrap();
        mysql_async::Params::Positional(
            v.iter()
                .map(|x| match x {
                    Value::Bytes(b) => match std::str::from_utf8(b).ok().and_then(|s| n.get(s)) {
                        Some(new) => Value::Bytes(new.into_bytes()),
                        None => x.clone(),
                    },
                    other => other.clone(),
                })
                .collect(),
        )
    }
}

fn typed<T: mysql_async::prelude::FromRow>(r: &RenamedSql, rows: Vec<Row>) -> Result<Vec<T>> {
    rows.into_iter()
        .map(|row| {
            mysql_async::from_row_opt(r.row(row))
                .map_err(|e| anyhow!("row conversion failed for {}: {e:?}", clip(&r.sql, 300)))
        })
        .collect()
}

/// `conn.query(sql)` with typed rows, through the monkey-wrench renaming.
pub async fn typed_rows<T: mysql_async::prelude::FromRow + Send + 'static>(
    conn: &mut Conn,
    sql: impl AsRef<str>,
) -> Result<Vec<T>> {
    let sql = sql.as_ref();
    let r = renamed_sql(conn, sql)?;
    let rows: Vec<Row> = conn
        .query(r.sql.as_str())
        .await
        .with_context(|| format!("SQL failed: {}", r.shown(sql)))?;
    typed(&r, rows)
}

/// `conn.query_first(sql)`, through the monkey-wrench renaming.
pub async fn typed_first<T: mysql_async::prelude::FromRow + Send + 'static>(
    conn: &mut Conn,
    sql: impl AsRef<str>,
) -> Result<Option<T>> {
    Ok(typed_rows(conn, sql).await?.into_iter().next())
}

/// `conn.exec(sql, params)` with typed rows, through the monkey-wrench renaming.
pub async fn typed_exec<T, P>(conn: &mut Conn, sql: impl AsRef<str>, params: P) -> Result<Vec<T>>
where
    T: mysql_async::prelude::FromRow + Send + 'static,
    P: Into<mysql_async::Params> + Send,
{
    let sql = sql.as_ref();
    let r = renamed_sql(conn, sql)?;
    let rows: Vec<Row> = conn
        .exec(r.sql.as_str(), r.params(params.into()))
        .await
        .with_context(|| format!("SQL failed: {}", r.shown(sql)))?;
    typed(&r, rows)
}

/// [`typed_exec`] with the results as the server returns them (names not mapped back): for
/// names a test hands to lazyview's own functions.
pub async fn typed_exec_as_sent<T, P>(
    conn: &mut Conn,
    sql: impl AsRef<str>,
    params: P,
) -> Result<Vec<T>>
where
    T: mysql_async::prelude::FromRow + Send + 'static,
    P: Into<mysql_async::Params> + Send,
{
    let sql = sql.as_ref();
    let mut r = renamed_sql(conn, sql)?;
    let params = r.params(params.into());
    r.back = None;
    let rows: Vec<Row> = conn
        .exec(r.sql.as_str(), params)
        .await
        .with_context(|| format!("SQL failed: {}", r.shown(sql)))?;
    typed(&r, rows)
}

/// The name `name` has in `db` (renamed in a renamed run): for names a test hands to lazyview's
/// own functions.
pub fn name_for(db: &Db, name: &str) -> String {
    db_names(db)
        .and_then(|n| n.lock().unwrap().get(name))
        .unwrap_or_else(|| name.to_string())
}

/// `conn.exec_first(sql, params)`, through the monkey-wrench renaming.
pub async fn typed_exec_first<T, P>(
    conn: &mut Conn,
    sql: impl AsRef<str>,
    params: P,
) -> Result<Option<T>>
where
    T: mysql_async::prelude::FromRow + Send + 'static,
    P: Into<mysql_async::Params> + Send,
{
    Ok(typed_exec(conn, sql, params).await?.into_iter().next())
}

/// `sql` as the renamed run of `conn`'s schema sends it (unchanged otherwise).
pub fn renamed_sql(conn: &Conn, sql: &str) -> Result<RenamedSql> {
    let Some(names) = names_of(conn) else {
        return Ok(RenamedSql {
            sql: sql.to_string(),
            back: None,
            names: None,
        });
    };
    let r = names
        .lock()
        .unwrap()
        .rename_sql(sql)
        .map_err(|e| anyhow!("monkey-wrench rename: {e}"))?;
    Ok(RenamedSql {
        sql: r.sql,
        back: r.metadata.then(|| names.clone()),
        names: Some(names),
    })
}

/// The renaming of `db` (a schema of a renamed run), if any.
pub fn db_names(db: &Db) -> Option<SharedNames> {
    renamed_schemas().lock().unwrap().get(&db.name).cloned()
}

/// `sql` as `db`'s connections send it (renamed in a renamed run): for definitions a test plans
/// in-process against `db`'s catalog.
pub fn rename_for(db: &Db, sql: &str) -> Result<String> {
    match db_names(db) {
        Some(n) => n
            .lock()
            .unwrap()
            .rename_sql(sql)
            .map(|r| r.sql)
            .map_err(|e| anyhow!("monkey-wrench rename: {e}")),
        None => Ok(sql.to_string()),
    }
}

/// A plan made in-process from a definition renamed by [`rename_for`], with the original names
/// (view, aliases, tables, reasons and rendered SQL).
pub fn unrename_plan(db: &Db, mut p: lazyview::analyze::ViewPlan) -> lazyview::analyze::ViewPlan {
    let Some(n) = db_names(db) else {
        return p;
    };
    let n = n.lock().unwrap();
    let name = |s: &str| n.original(s).unwrap_or_else(|| s.to_string());
    let sql = |s: &str| n.unrename_text(s);
    let table = |t: &Arc<lazyview::catalog::TableInfo>| {
        let mut t = (**t).clone();
        t.schema = name(&t.schema);
        t.name = name(&t.name);
        for c in &mut t.columns {
            c.name = name(&c.name);
        }
        Arc::new(t)
    };
    p.name = name(&p.name);
    if let lazyview::analyze::Mode::Full(r) = &p.mode {
        p.mode = lazyview::analyze::Mode::Full(n.unrename_raw(r));
    }
    p.tables = p.tables.iter().map(|(s, t)| (name(s), name(t))).collect();
    for a in &mut p.aliases {
        a.name = name(&a.name);
        a.table = table(&a.table);
        a.semi = a.semi.as_deref().map(sql);
        a.subst_from = (sql(&a.subst_from.0), sql(&a.subst_from.1));
    }
    for c in &mut p.cascades {
        c.parent = table(&c.parent);
        c.child = (name(&c.child.0), name(&c.child.1));
    }
    for v in [&mut p.projection, &mut p.unit_exprs] {
        for s in v.iter_mut() {
            *s = sql(s);
        }
    }
    for k in &mut p.lineage {
        for s in &mut k.exprs {
            *s = sql(s);
        }
    }
    p.from_sql = sql(&p.from_sql);
    p.full_select = sql(&p.full_select);
    for o in [
        &mut p.where_sql,
        &mut p.group_sql,
        &mut p.having_sql,
        &mut p.order_limit_sql,
        &mut p.window_sql,
        &mut p.lookup_where_sql,
    ] {
        if let Some(s) = o.as_mut() {
            *s = sql(s);
        }
    }
    p
}

/// `got` (read from `db`, names mapped back) equals `want` up to the letter case and quoting
/// of names (a renamed name may need quotes its original did not).
pub fn same_text(db: &Db, got: &str, want: &str) -> bool {
    match db_names(db) {
        Some(n) => {
            let n = n.lock().unwrap();
            n.canonical(got) == n.canonical(want)
        }
        None => got == want,
    }
}

/// [`same_text`] element by element.
pub fn same_texts(db: &Db, got: &[String], want: &[&str]) -> bool {
    got.len() == want.len() && got.iter().zip(want).all(|(g, w)| same_text(db, g, w))
}

/// What the plain and the renamed run of a scenario must have in common at its end.
#[derive(Debug, Clone)]
pub struct Observed {
    /// `_lazyview_state` rows: name | target | state | mode | detail | last_error.
    states: Vec<String>,
    /// The distinct plans the engines logged (`building view=… mode=… tracing=…`).
    plans: Vec<String>,
    /// Final metrics per engine, in start order.
    metrics: Vec<(String, Snapshot)>,
}

/// Reads the end state of a run (before its schemas are dropped), names mapped back.
pub(crate) async fn observed(ctx: &Ctx) -> Result<Observed> {
    let names = ctx.names.as_ref().map(|n| n.lock().unwrap().clone());
    let back = |t: &str| match &names {
        Some(n) => n.unrename_raw(t),
        None => t.to_string(),
    };
    let mut states = Vec::new();
    let dbs = ctx.dbs.lock().unwrap().clone();
    for db in &dbs {
        let mut c = db.conn().await?;
        if !table_exists(&mut c, "_lazyview_state").await? {
            continue;
        }
        // (Read as stored: mapped back here, with the truncated details marked.)
        let rs: Vec<Row> = c
            .query(
                "SELECT CAST(name AS CHAR), CAST(target AS CHAR), CAST(state AS CHAR), CAST(mode AS CHAR), \
                 CAST(detail AS CHAR), CAST(last_error AS CHAR) FROM _lazyview_state",
            )
            .await?;
        for r in rs {
            let cells: Vec<String> = r
                .unwrap()
                .into_iter()
                .map(|v| value_text(&v).unwrap_or_else(|| "NULL".into()))
                .collect();
            let name = names
                .as_ref()
                .and_then(|n| n.original(&cells[0]))
                .unwrap_or_else(|| cells[0].clone());
            let detail = if cells[4].chars().count() >= 255 {
                format!("{} [truncated]", back(&cells[4]))
            } else {
                back(&cells[4])
            };
            states.push(format!(
                "{name} | {} | {} | {} | {detail} | {}",
                cells[1],
                cells[2],
                cells[3],
                back(&cells[5])
            ));
        }
        c.disconnect().await.ok();
    }
    let mut plans = std::collections::BTreeSet::new();
    let engines = ctx.engines.lock().unwrap().clone();
    for (_, _, log) in &engines {
        let text = std::fs::read_to_string(log).unwrap_or_default();
        for line in text.lines() {
            if let Some(at) = line.find(" building view=") {
                // The plan, not the targets (builds on an unreachable target are retried).
                let mut l = line[at + 1..].to_string();
                if let (Some(t), Some(m)) = (l.find(" targets="), l.find(" mode=")) {
                    if t < m {
                        l.replace_range(t..m, "");
                    }
                }
                plans.insert(back(&l));
            }
        }
    }
    let metrics = engines
        .iter()
        .map(|(label, m, _)| (label.clone(), m.snapshot()))
        .collect();
    // Each run has schemas of its own: named by their position.
    let schemas = |t: String| {
        dbs.iter().enumerate().fold(t, |t, (i, db)| {
            t.replace(&ident(&db.name), &format!("schema{i}"))
                .replace(&db.name, &format!("schema{i}"))
        })
    };
    let mut states: Vec<String> = states.into_iter().map(schemas).collect();
    states.sort();
    Ok(Observed {
        states,
        plans: plans.into_iter().map(schemas).collect(),
        metrics,
    })
}

/// The nine engine counters compared between a plain and a renamed run (the comparison in this
/// file and the one in `tests/rename_invariance.rs`).
pub fn counters(s: &Snapshot) -> [(&'static str, u64); 9] {
    [
        ("units_refreshed", s.units_refreshed),
        ("lookup_queries", s.lookup_queries),
        ("source_queries", s.source_queries),
        ("mv_queries", s.mv_queries),
        ("mv_rows_inserted", s.mv_rows_inserted),
        ("mv_rows_upserted", s.mv_rows_upserted),
        ("mv_rows_deleted", s.mv_rows_deleted),
        ("full_rebuilds", s.full_rebuilds),
        ("lookback_overflows", s.lookback_overflows),
    ]
}

/// Detail texts compare equal when equal, or when both are truncated and agree up to the
/// shorter one (minus the name the cut may have split).
fn same_state(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let cut = |s: &str| s.strip_suffix(" [truncated]").map(str::to_string);
    let (Some(x), Some(y)) = (cut(a), cut(b)) else {
        return false;
    };
    let n = x.len().min(y.len()).saturating_sub(24);
    x.is_char_boundary(n) && y.is_char_boundary(n) && x[..n] == y[..n]
}

/// The monkey-wrench rename test, after both runs of a scenario passed: the renamed run must
/// have left the same view states and logged the same plans as the plain one (names mapped
/// back), and, with [`rename_metrics_strict`], the same engine metrics; the metric differences
/// are printed either way. (`LAZYVIEW_TEST_RENAME=<seed>`; both runs are oracle-exact, the
/// scenario's own checks ran in each.)
pub(crate) fn compare_renamed(
    ctx: &Ctx,
    names: &rename::Names,
    plain: Observed,
    renamed: Observed,
) -> std::result::Result<(), String> {
    let c = |v: &[String]| -> Vec<String> { v.iter().map(|s| names.canonical(s)).collect() };
    let mut diffs = Vec::new();
    let (ps, rs) = (c(&plain.states), c(&renamed.states));
    if ps.len() != rs.len() || ps.iter().zip(&rs).any(|(a, b)| !same_state(a, b)) {
        diffs.push(format!(
            "view states differ:\n    plain:   {ps:#?}\n    renamed: {rs:#?}"
        ));
    }
    let set = |v: Vec<String>| -> Vec<String> {
        let s: std::collections::BTreeSet<String> = v.into_iter().collect();
        s.into_iter().collect()
    };
    let (pp, rp) = (set(c(&plain.plans)), set(c(&renamed.plans)));
    if pp != rp {
        let only = |a: &[String], b: &[String]| -> Vec<String> {
            a.iter().filter(|x| !b.contains(x)).cloned().collect()
        };
        diffs.push(format!(
            "logged plans differ:\n    plain only:   {:#?}\n    renamed only: {:#?}",
            only(&pp, &rp),
            only(&rp, &pp)
        ));
    }
    let mut report = Vec::new();
    for (i, (label, p)) in plain.metrics.iter().enumerate() {
        let r = renamed
            .metrics
            .get(i)
            .map(|(_, s)| s.clone())
            .unwrap_or_default();
        for ((k, a), (_, b)) in counters(p).into_iter().zip(counters(&r)) {
            if a != b {
                report.push(format!("engine {i} ({label}) {k}: plain {a}, renamed {b}"));
            }
        }
    }
    if plain.metrics.len() != renamed.metrics.len() {
        report.push(format!(
            "engines started: plain {}, renamed {}",
            plain.metrics.len(),
            renamed.metrics.len()
        ));
    }
    ctx.log(if report.is_empty() {
        "monkey-wrench rename: metrics equal".to_string()
    } else {
        format!(
            "monkey-wrench rename: metrics differ (batching follows timing):\n  {}",
            report.join("\n  ")
        )
    });
    if rename_metrics_strict() && !report.is_empty() {
        diffs.push(format!("engine metrics differ:\n  {}", report.join("\n  ")));
    }
    if diffs.is_empty() {
        ctx.log("monkey-wrench rename: same states and plans as the plain run");
        return Ok(());
    }
    Err(format!(
        "[{} on {}] monkey-wrench rename: the renamed run differs from the plain run:\n  {}\n{}",
        ctx.test,
        ctx.label(),
        diffs.join("\n  "),
        names.describe()
    ))
}
