//! The query analyzer (see docs/design-analyzer.md): definitions with CTEs, derived tables and
//! subqueries are resolved with MySQL's scoping rules and normalized — CTEs inlined per
//! reference, derived tables merged, DISTINCT turned into GROUP BY, semi-joins on unique keys
//! turned into joins — into flat blocks that the block planner ([`crate::analyze::plan`])
//! maintains exactly as if they had been written inline. What cannot be normalized gets a Full
//! plan whose reason names the construct.

mod ast;
mod build;
mod ir;
mod lower;
mod names;
mod normalize;
mod scope;
mod source;
#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use sqlparser::ast::{Query, SetExpr, TableFactor};

use crate::analyze::{self, ParsedDef, ViewPlan};
use crate::catalog::{TableInfo, TableRef};
use ir::{Block, Rel, Source};

pub use scope::referenced_tables;

/// Plans a definition (drop-in for [`analyze::plan`]): `tables` must hold every table that
/// [`referenced_tables`] lists.
pub fn plan(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<ViewPlan> {
    if !needs_analysis(&def.query) {
        return analyze::plan(def, default_schema, tables);
    }
    // Spans must point into the text the item names are taken from.
    let Some((query, tokens)) = source::parse_query(&def.query_sql) else {
        return analyze::plan(def, default_schema, tables);
    };
    let text = source::Text::new(&def.query_sql, tokens);
    let bld = build::Builder {
        schema: default_schema,
        tables,
        text: &text,
        next_semi: Cell::new(0),
        depth: Cell::new(0),
    };
    match bld.query(&query, &scope::Scope::default())? {
        Rel::Opaque { why } => lower::full_plan(def, default_schema, tables, &why),
        Rel::Block(b) => {
            if let Some(why) = top_reason(&b) {
                return lower::full_plan(def, default_schema, tables, &why);
            }
            // (A WITH clause whose CTEs are all merged or unreferenced is dropped.)
            if !b.rewritten && query.with.is_none() {
                return analyze::plan(def, default_schema, tables);
            }
            if !def.columns.is_empty() && def.columns.len() != b.outputs.len() {
                anyhow::bail!(
                    "view column list has {} names but SELECT returns {} columns",
                    def.columns.len(),
                    b.outputs.len()
                );
            }
            match lower::plan_block(def, &b, default_schema, tables)? {
                Ok(p) => Ok(p),
                Err(why) => lower::full_plan(def, default_schema, tables, &why),
            }
        }
    }
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
        || ast::factors(&s.from)
            .iter()
            .any(|(_, f)| matches!(f, TableFactor::Derived { .. }))
        || ast::exprs_of(s).into_iter().any(ast::has_subquery)
}

/// Why the top block cannot be planned as one flat block.
fn top_reason(b: &Block) -> Option<String> {
    if b.limit {
        return Some(if b.ordered {
            "LIMIT".into()
        } else {
            "LIMIT without ORDER BY".into()
        });
    }
    b.items
        .iter()
        .find(|i| matches!(i.source, Source::Derived(_)))
        .map(|it| normalize::unmerged_reason(b, it))
}
