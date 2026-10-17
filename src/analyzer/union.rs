//! UNION ALL as branch plans: every branch is normalized and planned on its own (the
//! statement's CTEs are visible in all of them). The engine keeps one table whose rows carry
//! their branch number (`_lv_branch`); a change of a branch's source is traced and refreshed
//! with that branch's plan only, so a source row feeding several branches is traced in each and
//! duplicates keep their multiplicity. Column names come from the first branch, column types
//! from the whole union (the engine probes it).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use sqlparser::ast::{Query, SelectItem, SetExpr, SetOperator, SetQuantifier};

use super::ast::qident;
use super::build::Builder;
use super::ir::{Block, Rel, Source};
use super::lower;
use super::names::finish_view_names;
use super::normalize::{qualify, query_of};
use super::scope::Scope;
use crate::analyze::{self, ParsedDef, ViewPlan};
use crate::catalog::{TableInfo, TableRef};

/// The branch plans of a UNION ALL view.
#[derive(Debug, Clone)]
pub struct Branches {
    /// The statement the table's columns (names and types) are probed from: the union as
    /// written, with the view's column list applied.
    pub probe_sql: String,
    /// One plan per branch; branch `b` owns the rows with `_lv_branch = b`.
    pub plans: Vec<ViewPlan>,
}

/// The branches of a chain of UNION ALL (parenthesized operands flattened), or None when another
/// set operation is involved.
fn leaves<'q>(s: &'q SetExpr, out: &mut Vec<&'q SetExpr>) -> bool {
    match s {
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left,
            right,
        } => leaves(left, out) && leaves(right, out),
        SetExpr::Query(q)
            if q.with.is_none() && q.order_by.is_none() && q.limit_clause.is_none() =>
        {
            leaves(&q.body, out)
        }
        SetExpr::Select(_) | SetExpr::Query(_) => {
            out.push(s);
            true
        }
        _ => false,
    }
}

/// Branch plans for `query` (a set operation). `Ok(None)`: not a UNION ALL chain;
/// `Ok(Some(Err(reason)))`: a branch cannot be planned on its own.
pub(crate) fn branches(
    def: &ParsedDef,
    query: &Query,
    bld: &Builder,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<Option<std::result::Result<Branches, String>>> {
    let mut parts = Vec::new();
    if !leaves(&query.body, &mut parts) || query.limit_clause.is_some() || query.fetch.is_some() {
        return Ok(None);
    }
    let scope = match &query.with {
        Some(w) => Scope::default().with_clause(w),
        None => Scope::default(),
    };
    let mut blocks: Vec<Block> = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let rel = match part {
            SetExpr::Select(s) => bld.branch(s, &scope)?,
            SetExpr::Query(q) => bld.nested(q, &scope)?,
            _ => unreachable!(),
        };
        let b = match rel {
            Rel::Block(b) => *b,
            Rel::Opaque { why } => {
                return Ok(Some(Err(format!("{why} in UNION ALL branch {}", i + 1))))
            }
        };
        if b.limit {
            return Ok(Some(Err(format!("LIMIT in UNION ALL branch {}", i + 1))));
        }
        if let Some(it) = b
            .items
            .iter()
            .find(|i| matches!(i.source, Source::Derived(_)))
        {
            return Ok(Some(Err(super::normalize::unmerged_reason(&b, it))));
        }
        if let Some(first) = blocks.first() {
            if first.outputs.len() != b.outputs.len() {
                bail!("the SELECTs of a UNION have a different number of columns");
            }
        }
        blocks.push(b);
    }
    // The first branch names the columns; the others are aliased to its names.
    let mut names: Vec<_> = blocks[0].outputs.iter().map(|o| o.name.clone()).collect();
    if let Some(&i) = finish_view_names(&mut names).first() {
        return Ok(Some(Err(format!(
            "unaliased column `{}` of a UNION ALL needs an alias",
            blocks[0].outputs[i].name.text
        ))));
    }
    let probe_sql = analyze::plan_block(def, schema, tables)?.full_select;
    let mut plans = Vec::new();
    for (i, mut b) in blocks.into_iter().enumerate() {
        if i > 0 {
            qualify(&mut b)?;
            for (o, n) in b.outputs.iter_mut().zip(&names) {
                o.name = n.clone();
                o.original = None;
            }
            b.rewritten = true;
        }
        // A branch's Full plan rebuilds the branch alone: its own SELECT, aliased.
        let mut s = b.select.clone();
        s.projection = b
            .outputs
            .iter()
            .zip(&names)
            .map(|(o, n)| SelectItem::ExprWithAlias {
                expr: o.expr.clone(),
                alias: qident(&n.text),
            })
            .collect();
        let branch_def = ParsedDef {
            query_sql: query_of(s).to_string(),
            ..def.clone()
        };
        b.rewritten = true;
        match lower::plan_block(&branch_def, &b, schema, tables)? {
            Ok(p) => plans.push(p),
            Err(r) => {
                return Ok(Some(Err(format!(
                    "{} in UNION ALL branch {}",
                    r.reason(),
                    i + 1
                ))))
            }
        }
    }
    Ok(Some(Ok(Branches { probe_sql, plans })))
}
