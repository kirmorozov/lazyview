//! The query analyzer (see docs/design-analyzer.md): definitions with CTEs, derived tables and
//! subqueries are resolved with MySQL's scoping rules and normalized — CTEs inlined per
//! reference, derived tables merged, DISTINCT turned into GROUP BY, semi-joins on unique keys
//! turned into joins — into flat blocks that the block planner ([`crate::analyze::plan`])
//! maintains exactly as if they had been written inline. What cannot be normalized gets a Full
//! plan whose reason names the construct.

mod ast;
mod build;
mod internal;
mod ir;
mod joins;
mod lower;
mod names;
mod normalize;
mod scope;
mod source;
#[cfg(test)]
mod tests;
mod union;

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use sqlparser::ast::{Query, SetExpr, TableFactor};

use crate::analyze::{self, Mode, ParsedDef, ViewPlan};

/// The block planner's reason for a group key on the NULL side of a LEFT JOIN.
const NULL_SIDE_KEY: &str = "group key on the NULL side of a LEFT JOIN";
use crate::catalog::{TableInfo, TableRef};
use ir::{Rel, Source};

pub use internal::InternalView;
pub use scope::referenced_tables;
pub use union::Branches;

/// What the analyzer may ask of the engine.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The engine materializes internal views ([`InternalView`]) on request.
    pub internal_views: bool,
    /// The definition is an internal view itself (its reserved `_lv_int_` name is allowed).
    pub internal: bool,
    /// The engine maintains UNION ALL views as branch plans ([`Branches`]).
    pub branch_plans: bool,
}

// (Short-lived: boxing the plan would only burden the callers.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Outcome {
    Plan(ViewPlan),
    /// Materialize these first (each `sql` is a definition planned like any other, on target
    /// `source`), then analyze again with their tables in `tables`. May be returned again with
    /// further views that read the first ones.
    NeedsInternal(Vec<InternalView>),
    /// A UNION ALL maintained per branch in one table.
    Branches(Branches),
}

/// Plans a definition (drop-in for [`analyze::plan`]): `tables` must hold every table that
/// [`referenced_tables`] lists.
pub fn plan(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<ViewPlan> {
    match analyze(def, default_schema, tables, &Options::default())? {
        Outcome::Plan(p) => Ok(p),
        Outcome::NeedsInternal(_) | Outcome::Branches(_) => {
            unreachable!("internal views and branch plans are not enabled")
        }
    }
}

/// Most base-table references one join block of the inlined (flat) form may have (each UNION
/// branch is a block). The servers allow 61 tables in a join; beyond this budget a
/// multiply-referenced CTE belongs in an internal view instead of being copied.
pub(crate) const INLINE_BUDGET: usize = 32;

/// Largest refresh SELECT a plan may render (the budgets above bound the analyzer's work; this
/// is the last guard before a statement the engine would send on every refresh).
const MAX_PLANNED_SQL: usize = 1 << 20;

/// Plans a definition, isolating subtrees into internal views where `opts` allows it.
pub fn analyze(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
    opts: &Options,
) -> Result<Outcome> {
    let out = analyze_block(def, default_schema, tables, opts)?;
    if let Outcome::Plan(p) = &out {
        if !matches!(p.mode, Mode::Full(_)) {
            let n = p.select_sql(&[], None).len();
            if n > MAX_PLANNED_SQL {
                let why = format!(
                    "the planned SELECT is {n} bytes (limit {MAX_PLANNED_SQL}): too large to run on every refresh"
                );
                return lower::full_plan(def, default_schema, tables, &why).map(Outcome::Plan);
            }
        }
    }
    Ok(out)
}

fn analyze_block(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
    opts: &Options,
) -> Result<Outcome> {
    if opts.internal && def.name.starts_with("_lv_int_") {
        // The block planner reserves `_lv_` names for lazyview: plan under a stand-in name.
        let mut d = def.clone();
        d.name = format!("lv{}", &def.name[3..]);
        let inner = Options {
            internal: false,
            ..opts.clone()
        };
        return Ok(match analyze(&d, default_schema, tables, &inner)? {
            Outcome::Plan(mut p) => {
                p.name = def.name.clone();
                Outcome::Plan(p)
            }
            other => other,
        });
    }
    let plan_as_written = || analyze::plan_block(def, default_schema, tables).map(Outcome::Plan);
    let full = |why: &str| lower::full_plan(def, default_schema, tables, why).map(Outcome::Plan);
    let set_op = matches!(def.query.body.as_ref(), SetExpr::SetOperation { .. });
    if !opts.internal_views && !(opts.branch_plans && set_op) && !needs_analysis(&def.query) {
        // (A group key on the NULL side goes on to name what would trace it.)
        let p = analyze::plan_block(def, default_schema, tables)?;
        if !matches!(&p.mode, Mode::Full(r) if r == NULL_SIDE_KEY) {
            return Ok(Outcome::Plan(p));
        }
    }
    // Spans must point into the text the item names are taken from.
    let Some((mut query, tokens)) = source::parse_query(&def.query_sql) else {
        return plan_as_written();
    };
    // Every CTE reference is inlined as a copy: bound the size of each flat join block before
    // building it (a chain of CTEs each referencing the previous one twice is 2^n copies).
    let size = scope::inlined_size(&query, &scope::Scope::default());
    if size.refs > INLINE_BUDGET {
        let what = match (size.ctes, size.derived) {
            (true, true) => "the WITH clause and derived tables",
            (true, false) => "the WITH clause",
            _ => "derived tables",
        };
        let place = match size.branch {
            Some(b) => format!("UNION branch {b}"),
            None => "one SELECT".into(),
        };
        let culprit = match &size.worst {
            Some((name, n)) if size.ctes => format!(": CTE `{name}` is referenced {n} times"),
            _ => String::new(),
        };
        return full(&format!(
            "inlining {what} reads {} table references in {place} (budget {INLINE_BUDGET}){culprit} (needs internal views)",
            size.refs
        ));
    }
    // `*` over USING / NATURAL joins is expanded as written (before a RIGHT JOIN is turned
    // around: the preserved side gives the shared columns), so the column list and order are
    // the server's. (The spans of the expanded items are none: they carry no source text.)
    let mut star_expanded = false;
    if let SetExpr::Select(s) = query.body.as_mut() {
        star_expanded = analyze::expand_star_over_using(s, default_schema, tables)?;
    }
    let joins_changed = joins::normalize(&mut query, default_schema, tables) || star_expanded;
    let text = source::Text::new(&def.query_sql, tokens);
    let bld = build::Builder {
        schema: default_schema,
        tables,
        text: &text,
        next_semi: Cell::new(0),
        depth: Cell::new(0),
    };
    if opts.branch_plans && set_op {
        match union::branches(def, &query, &bld, default_schema, tables)? {
            Some(Ok(b)) => return Ok(Outcome::Branches(b)),
            Some(Err(why)) => return full(&why),
            None => {}
        }
    }
    let mut b = match bld.query(&query, &scope::Scope::default())? {
        // (UNION ALL is traceable per branch: the engine does not run branch plans yet.)
        Rel::Opaque { why } if why.starts_with("UNION ALL") && !opts.branch_plans => {
            return full(&format!("{why} (needs branch plans)"))
        }
        Rel::Opaque { why } => return full(&why),
        Rel::Block(b) => *b,
    };
    b.rewritten |= joins_changed;
    // ORDER BY … LIMIT (top-N) is planned by the block planner; LIMIT alone has no defined rows.
    if b.limit && (!b.ordered || b.tail.is_none()) {
        return full(if b.ordered {
            "LIMIT"
        } else {
            "LIMIT without ORDER BY"
        });
    }
    if let Some(it) = b
        .items
        .iter()
        .find(|i| matches!(i.source, Source::Derived(_)))
    {
        if !opts.internal_views {
            let why = normalize::unmerged_reason(&b, it);
            return full(&if internal::isolatable(&b, it) {
                format!("{why} (needs internal views)")
            } else {
                why
            });
        }
        match internal::isolate(&mut b, default_schema, tables) {
            Ok(missing) if !missing.is_empty() => return Ok(Outcome::NeedsInternal(missing)),
            Ok(_) => {}
            Err(why) => return full(&why),
        }
    }
    // The names in an ORDER BY subquery resolve in its own FROM first: the analyzer leaves it as
    // written, which is only valid over the FROM and WHERE as written.
    if b.rewritten && normalize::tail_has_subquery(&b) {
        return full(
            "subquery in the ORDER BY of a top-N view whose other subqueries, DISTINCT or joins are rewritten",
        );
    }
    // (A WITH clause whose CTEs are all merged or unreferenced is dropped.)
    let planned: std::result::Result<ViewPlan, String> = if !b.rewritten && query.with.is_none() {
        let p = analyze::plan_block(def, default_schema, tables)?;
        match &p.mode {
            Mode::Full(r) if r == NULL_SIDE_KEY || r == "subquery" => Err(r.clone()),
            _ => Ok(p),
        }
    } else {
        if !def.columns.is_empty() && def.columns.len() != b.outputs.len() {
            anyhow::bail!(
                "view column list has {} names but SELECT returns {} columns",
                def.columns.len(),
                b.outputs.len()
            );
        }
        loop {
            match lower::plan_block(def, &b, default_schema, tables)? {
                Ok(p) => break Ok(p),
                // A semi-join whose table cannot be traced to the units: over an internal view
                // of its distinct keys it is an inner join.
                Err(lower::Refused::Semi(id, why)) if opts.internal_views => {
                    match internal::semi_view(&mut b, id, default_schema, tables) {
                        Ok(None) => continue,
                        Ok(Some(iv)) => return Ok(Outcome::NeedsInternal(vec![iv])),
                        Err(()) => return full(&why),
                    }
                }
                Err(r) => break Err(r.reason()),
            }
        }
    };
    let plan = match planned {
        Ok(p) => p,
        // A group key on the NULL side is traced through the tuple-lineage view.
        Err(why) if why == NULL_SIDE_KEY => {
            return match internal::tuple_view(&b, default_schema, tables) {
                Some((iv, None)) if opts.internal_views => Ok(Outcome::NeedsInternal(vec![iv])),
                Some((_, Some(v))) if opts.internal_views => {
                    match lower::plan_block(def, &v, default_schema, tables)? {
                        Ok(p) => Ok(Outcome::Plan(p)),
                        Err(r) => full(&r.reason()),
                    }
                }
                Some(_) => full(&format!("{why} (needs internal views)")),
                None => full(&why),
            };
        }
        // A subquery that stayed one: the reason the semi-join extraction recorded.
        Err(why) if why == "subquery" && !b.semi_blockers.is_empty() => {
            return full(&b.semi_blockers.join("; "));
        }
        Err(why) => return full(&why),
    };
    // Grouped over two or more lookup tables: the tuple-lineage view where the engine has
    // internal views (the engine's remembered before-images trace them meanwhile).
    if opts.internal_views && internal::needs_tuple_view(&plan) {
        match internal::tuple_view(&b, default_schema, tables) {
            Some((iv, None)) => return Ok(Outcome::NeedsInternal(vec![iv])),
            Some((_, Some(v))) => {
                return match lower::plan_block(def, &v, default_schema, tables)? {
                    Ok(p) => Ok(Outcome::Plan(p)),
                    Err(r) => full(&r.reason()),
                }
            }
            None => {}
        }
    }
    Ok(Outcome::Plan(plan))
}

/// Whether the statement uses anything the analyzer rewrites (otherwise the block planner sees
/// it unchanged).
fn needs_analysis(q: &Query) -> bool {
    if q.with.is_some() {
        return true;
    }
    let SetExpr::Select(s) = q.body.as_ref() else {
        return false;
    };
    s.distinct.is_some()
        || joins::has_rewritable(s)
        || ast::factors(&s.from)
            .iter()
            .any(|(_, f)| matches!(f, TableFactor::Derived { .. }))
        || ast::exprs_of(s).into_iter().any(ast::has_subquery)
}
