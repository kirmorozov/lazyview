//! CTEs, derived tables and subqueries through the query analyzer (docs/design-analyzer.md):
//! one or more views per taxonomy class over the e-commerce schema, each checked for its
//! strategy (`_lazyview_state.mode` / `detail`, and the analyzer's own plan against the server's
//! catalog) and against an oracle VIEW with the same definition after a seeded random workload.
//!
//! Until the analyzer is wired into planning (`analyze::plan`), the engine is given the
//! analyzer's flattened form of each definition while the oracle keeps the definition as
//! written: that checks on every server that the rewrite returns the same rows and column names.

mod common;

#[path = "cte/definitions.rs"]
mod definitions;
#[path = "cte/defs.rs"]
mod defs;
#[path = "cte/edge.rs"]
mod edge;
#[path = "cte/json_keys.rs"]
mod json_keys;
#[path = "cte/orders_schema.rs"]
mod orders_schema;
#[path = "cte/routes.rs"]
mod routes;
#[path = "cte/semi_joins.rs"]
mod semi_joins;
#[path = "cte/virtuals.rs"]
mod virtuals;

use defs::cases;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use common::workload::{names, run_batches, setup_ecommerce, Gen, Sizes, Workload};
use common::*;
use lazyview::analyze::{parse_definition, Mode, ViewPlan};
use lazyview::catalog::{Catalog, Column, TableInfo, TableRef};

struct Case {
    name: &'static str,
    select: &'static str,
    /// Expected `_lazyview_state.mode` / `detail`.
    mode: &'static str,
    detail: &'static str,
}

const fn case(
    name: &'static str,
    mode: &'static str,
    detail: &'static str,
    select: &'static str,
) -> Case {
    Case {
        name,
        select,
        mode,
        detail,
    }
}

fn mode_detail(m: &Mode) -> (String, String) {
    match m {
        Mode::Spj => ("incremental".into(), "spj".into()),
        Mode::Grouped => ("incremental".into(), "grouped".into()),
        Mode::Global => ("incremental".into(), "global".into()),
        Mode::Full(r) => ("full".into(), r.clone()),
    }
}

/// Whether `analyze::plan` already goes through the analyzer (a merged CTE is not Full).
fn wired() -> bool {
    let t = TableInfo {
        schema: "db".into(),
        name: "t0".into(),
        columns: vec![Column::simple("id", "int")],
        pk: vec![0],
        unique_keys: vec![],
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    };
    let tables: HashMap<TableRef, Arc<TableInfo>> =
        [(("db".to_string(), "t0".to_string()), Arc::new(t))].into();
    let d = parse_definition("CREATE VIEW v AS WITH c AS (SELECT id FROM t0) SELECT id FROM c")
        .unwrap();
    lazyview::analyze::plan(&d, "db", &tables)
        .map(|p| !matches!(p.mode, Mode::Full(_)))
        .unwrap_or(false)
}

/// The analyzer's plan of a definition, against the server's catalog.
async fn analyzer_plan(db: &Db, mariadb: bool, definition: &str) -> Result<ViewPlan> {
    let d = parse_definition(&rename_for(db, definition)?)?;
    let mut c = db.conn().await?;
    let mut cat = Catalog::default();
    let mut tables = HashMap::new();
    // Like the engine: the tables read, then the parents of their cascading foreign keys.
    let mut queue: Vec<(TableRef, usize)> =
        lazyview::analyzer::referenced_tables(&d.query, &db.name)
            .into_iter()
            .map(|t| (t, 0))
            .collect();
    while let Some((t, depth)) = queue.pop() {
        if tables.contains_key(&t) || depth > 8 {
            continue;
        }
        if let Some(info) = cat.get(&mut c, &t.0, &t.1, mariadb).await? {
            queue.extend(info.cascades.iter().map(|f| (f.parent.clone(), depth + 1)));
            tables.insert(t, info);
        }
    }
    c.disconnect().await.ok();
    Ok(unrename_plan(
        db,
        lazyview::analyzer::plan(&d, &db.name, &tables)?,
    ))
}

/// The flat SELECT of an incremental plan (no hidden columns).
fn flat_sql(p: &ViewPlan) -> String {
    let mut sql = format!("SELECT {} FROM {}", p.projection.join(", "), p.from_sql);
    if let Some(w) = &p.where_sql {
        sql.push_str(&format!(" WHERE {w}"));
    }
    if let Some(g) = &p.group_sql {
        sql.push_str(&format!(" GROUP BY {g}"));
    }
    if let Some(h) = &p.having_sql {
        sql.push_str(&format!(" HAVING {h}"));
    }
    sql
}

async fn states(c: &mut mysql_async::Conn) -> Result<HashMap<String, (String, String)>> {
    let rs = rows(
        c,
        "SELECT name, COALESCE(mode, ''), COALESCE(detail, '') FROM _lazyview_state",
    )
    .await?;
    Ok(rs
        .into_iter()
        .map(|r| {
            (
                r[0].clone().unwrap_or_default(),
                (
                    r[1].clone().unwrap_or_default(),
                    r[2].clone().unwrap_or_default(),
                ),
            )
        })
        .collect())
}

/// The view specs of `cases`: asserts the analyzer's strategy for each (against the server's
/// catalog) and returns, per view, the (mode, detail) the engine must report.
async fn prepare(
    db: &Db,
    mariadb: bool,
    cases: Vec<Case>,
) -> Result<(Vec<ViewSpec>, HashMap<String, (String, String)>)> {
    let wired = wired();
    let mut specs = Vec::new();
    let mut engine_expect = HashMap::new();
    let mut bad = String::new();
    for c in cases {
        let kind = if c.mode == "full" {
            Kind::Full
        } else {
            Kind::Incremental
        };
        let mut spec = ViewSpec::new(c.name, kind, c.select);
        let plan = analyzer_plan(db, mariadb, &spec.definition)
            .await
            .with_context(|| format!("analyzer plan of {}", c.name))?;
        let got = mode_detail(&plan.mode);
        if got.0 != c.mode || !same_text(db, &got.1, c.detail) {
            bad.push_str(&format!(
                "\n  {}: analyzer chose {got:?}, expected ({}, {})",
                c.name, c.mode, c.detail
            ));
        }
        let expect = if wired {
            (c.mode.to_string(), c.detail.to_string())
        } else if matches!(plan.mode, Mode::Full(_)) {
            // As written: the engine's current planner names its own reason.
            ("full".to_string(), String::new())
        } else {
            spec.definition = format!("CREATE VIEW {} AS {}", c.name, flat_sql(&plan));
            let semi = plan.aliases.iter().any(|a| a.pos.0 == usize::MAX);
            if semi {
                // The current planner does not take subqueries apart.
                ("full".to_string(), "subquery".to_string())
            } else {
                got
            }
        };
        engine_expect.insert(c.name.to_string(), expect);
        specs.push(spec);
    }
    if !bad.is_empty() {
        bail!("analyzer strategies differ:{bad}");
    }
    Ok((specs, engine_expect))
}

/// Starts an engine over `specs` and checks the strategies it reports.
async fn start_checked(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    expect: &HashMap<String, (String, String)>,
    window_ms: &str,
) -> Result<Engine> {
    if !wired() {
        ctx.log("analyzer not wired into planning yet: the engine materializes the flattened definitions");
    }
    let (eng, _) = start_with_views(
        ctx,
        db,
        specs,
        &[
            ("LAZYVIEW_BATCH_WINDOW_MS", window_ms),
            ("LAZYVIEW_FULL_REFRESH_MS", "0"),
        ],
    )
    .await?;
    let mut c = db.conn().await?;
    let st = states(&mut c).await?;
    c.disconnect().await.ok();
    let mut bad = String::new();
    for (name, (mode, detail)) in expect {
        let got = st.get(name).cloned().unwrap_or_default();
        if got.0 != *mode || (!detail.is_empty() && !same_text(db, &got.1, detail)) {
            bad.push_str(&format!(
                "\n  {name}: engine reports {got:?}, expected ({mode}, {detail})"
            ));
        }
    }
    if !bad.is_empty() {
        bail!("engine strategies differ:{bad}");
    }
    Ok(eng)
}

// ---------------------------------------------------------------------------- one driver for all
//
// Every suite below is the same run over its own tables, cases and steps, so the run exists once:
// [`run_suite`] with a [`Suite`] entry.

/// One step of a suite: a label for the failure context, then the statements to run before the
/// next oracle check.
type Steps = Vec<(String, Vec<String>)>;

/// The view cases of a suite. [`prepare`] checks every case's strategy against the analyzer; the
/// suites that also check their plans against the server's catalog carry a route with each case.
enum Cases {
    /// the strategies are the whole plan check: no extra analyzer pass.
    Plain(Vec<Case>),
    /// each case also carries the route `ViewPlan::tracing` must report.
    Traced(Vec<(Case, &'static str)>),
}

impl Cases {
    /// Each case with its expected route, `None` where no route is checked.
    fn with_routes(&self) -> Vec<(&Case, Option<&'static str>)> {
        match self {
            Cases::Plain(cs) => cs.iter().map(|c| (c, None)).collect(),
            Cases::Traced(cs) => cs.iter().map(|(c, route)| (c, Some(*route))).collect(),
        }
    }

    /// The cases to build views and oracles from.
    fn into_cases(self) -> Vec<Case> {
        match self {
            Cases::Plain(cs) => cs,
            Cases::Traced(cs) => cs.into_iter().map(|(c, _)| c).collect(),
        }
    }
}

/// The cases of a suite, for the server family of the run.
type CasesFn = fn(bool) -> Cases;
/// The steps of a suite.
type StepsFn = fn() -> Steps;
/// The complaint about a case whose plan is not what the suite expects, `None` when it is.
type PlanCheck = fn(&Db, &Case, &ViewPlan) -> Option<String>;

/// The assertions a suite makes on each case's plan before the engine starts, beyond the strategy
/// check [`prepare`] does for every suite.
struct Plans {
    /// names these checks in the failure message.
    what: &'static str,
    check: PlanCheck,
}

/// A cte suite: everything [`run_suite`] needs besides the schema label and the batch window.
struct Suite<'a> {
    /// the tag of the schema: `cte_<tag>_<label>`.
    tag: &'a str,
    /// creates and seeds the suite's tables.
    ddl: &'a [&'a str],
    cases: CasesFn,
    steps: StepsFn,
    /// the plan assertions of [`Plans`]; `None` = the strategies are the whole plan check.
    plans: Option<Plans>,
    /// when set: building every case's plan must finish inside this budget.
    plan_budget: Option<Duration>,
}

/// The steps of a suite from literal `(label, statement)` pairs, one statement per step.
fn steps(src: &[(&str, &str)]) -> Steps {
    src.iter()
        .map(|(what, stmt)| (what.to_string(), vec![stmt.to_string()]))
        .collect()
}

/// The steps of a suite from literal `(label, statements)` pairs: a step runs all of its
/// statements before the next check.
fn step_groups(src: &[(&str, &[&str])]) -> Steps {
    src.iter()
        .map(|(what, stmts)| {
            (
                what.to_string(),
                stmts.iter().map(|stmt| stmt.to_string()).collect(),
            )
        })
        .collect()
}

/// The driver every cte suite shares: create and seed the tables, check the plans the suite asks
/// for, start the engine over its cases with the batch window `window_ms`, and verify every view
/// against its oracle after each step. `label` names the schema and the log lines.
async fn run_suite(ctx: &Ctx, s: &Suite<'_>, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_{}_{label}", s.tag)).await?;
    let mut c = db.conn().await?;
    for stmt in s.ddl {
        exec(&mut c, stmt).await?;
    }
    let mariadb = ctx.server().is_mariadb();
    let cases = (s.cases)(mariadb);
    if let Some(plans) = &s.plans {
        let mut bad = String::new();
        for (case, route) in cases.with_routes() {
            let def = format!("CREATE VIEW {} AS {}", case.name, case.select);
            let plan = analyzer_plan(&db, mariadb, &def).await?;
            let reported = plan.tracing();
            if let Some(want) = route.filter(|want| reported != *want) {
                bad.push_str(&format!(
                    "\n  {}: routes {}, expected {want}",
                    case.name, reported
                ));
            }
            if let Some(diff) = (plans.check)(&db, case, &plan) {
                bad.push_str(&diff);
            }
        }
        if !bad.is_empty() {
            bail!("{} differ:{bad}", plans.what);
        }
    }
    let started = Instant::now();
    let (specs, expect) = prepare(&db, mariadb, cases.into_cases()).await?;
    if let Some(budget) = s.plan_budget {
        anyhow::ensure!(
            started.elapsed() < budget,
            "planning took {:?}",
            started.elapsed()
        );
    }
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    for (what, stmts) in (s.steps)() {
        for stmt in stmts {
            exec(&mut c, &stmt).await.with_context(|| what.clone())?;
        }
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

async fn run(ctx: &Ctx, label: &str, seed: u64, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_{label}")).await?;
    let ids = setup_ecommerce(&db, seed, Sizes::scaled()).await?;
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    let mut c = db.conn().await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    let mut wl = Workload::new(seed, ids.clone());
    for phase in 1..=3 {
        let n = run_batches(&mut c, &mut wl, scaled(50))
            .await
            .with_context(|| format!("{label} phase {phase} workload"))?;
        verify(
            ctx,
            &mut eng,
            &db,
            &specs,
            &format!("{label}: phase {phase} ({n} txns)"),
        )
        .await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    ctx.log(format!(
        "{label}: ops {}; metrics {}",
        wl.stats_line(),
        fmt_snapshot(&eng.snapshot())
    ));
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

// --------------------------------------------------------------------------------- edge cases
