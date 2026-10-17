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

use crate::analyze::{self, ParsedDef, ViewPlan};
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

/// Plans a definition, isolating subtrees into internal views where `opts` allows it.
pub fn analyze(
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
    let plan_as_written = || analyze::plan(def, default_schema, tables).map(Outcome::Plan);
    let full = |why: &str| lower::full_plan(def, default_schema, tables, why).map(Outcome::Plan);
    let set_op = matches!(def.query.body.as_ref(), SetExpr::SetOperation { .. });
    if !opts.internal_views && !(opts.branch_plans && set_op) && !needs_analysis(&def.query) {
        return plan_as_written();
    }
    // Spans must point into the text the item names are taken from.
    let Some((mut query, tokens)) = source::parse_query(&def.query_sql) else {
        return plan_as_written();
    };
    let joins_changed = joins::normalize(&mut query, default_schema, tables);
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
        Rel::Opaque { why } => return full(&why),
        Rel::Block(b) => *b,
    };
    b.rewritten |= joins_changed;
    if b.limit {
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
            return full(&normalize::unmerged_reason(&b, it));
        }
        match internal::isolate(&mut b, default_schema, tables) {
            Ok(missing) if !missing.is_empty() => return Ok(Outcome::NeedsInternal(missing)),
            Ok(_) => {}
            Err(why) => return full(&why),
        }
    }
    // (A WITH clause whose CTEs are all merged or unreferenced is dropped.)
    let plan = if !b.rewritten && query.with.is_none() {
        analyze::plan(def, default_schema, tables)?
    } else {
        if !def.columns.is_empty() && def.columns.len() != b.outputs.len() {
            anyhow::bail!(
                "view column list has {} names but SELECT returns {} columns",
                def.columns.len(),
                b.outputs.len()
            );
        }
        match lower::plan_block(def, &b, default_schema, tables)? {
            Ok(p) => p,
            Err(why) => return full(&why),
        }
    };
    if opts.internal_views && internal::needs_tuple_view(&plan) {
        match internal::tuple_view(&b, default_schema, tables) {
            Some((iv, None)) => return Ok(Outcome::NeedsInternal(vec![iv])),
            Some((_, Some(v))) => {
                return match lower::plan_block(def, &v, default_schema, tables)? {
                    Ok(p) => Ok(Outcome::Plan(p)),
                    Err(why) => full(&why),
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
