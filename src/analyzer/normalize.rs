//! Semantics-preserving rewrites of one block whose children are already normalized:
//!
//! 1. semi-/anti-join conjuncts (`[NOT] EXISTS`, `x IN (SELECT …)`) are taken out of WHERE;
//! 2. derived relations (CTE references, derived tables) that only select, project and join are
//!    **merged**: their FROM joins the block's FROM, their WHERE goes to the WHERE (or to the ON of
//!    their join), references to their columns become their (qualified) expressions;
//! 3. a sole aggregated derived relation under a filter/project block is **merged up**;
//! 4. DISTINCT becomes GROUP BY over the output columns;
//! 5. semi-joins whose subquery returns a unique key become inner joins.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use anyhow::{anyhow, bail, Result};
use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, BinaryOperator, Distinct, Expr, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator,
    ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
    UnaryOperator, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::ast::{
    and_all, conjuncts, exprs_of, factor_alias, factors, for_each_expr_mut, function_name,
    has_column_ref, has_subquery, join_constraint, join_constraint_mut, join_kind, kind_at, nested,
    qcol, qident, table_factor, JoinKind,
};
use super::build::{star_with_using, Builder, Res, Resolver};
use super::ir::{Block, Item, Origin, Rel, Semi, SemiTable, Source};
use super::names::ident_name;
use super::scope::Scope;
use crate::analyze::null_propagating;
use crate::catalog::{Column, TypeFamily};

pub(crate) const AGGREGATES: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "GROUP_CONCAT",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

/// Functions whose result is not NULL when an argument is NULL.
const SEMI_MARKER: &str = "__lv_semi_";
const OUTER_PLACEHOLDER: &str = "__lv_outer_";

pub(crate) fn block(bld: &Builder, b: &mut Block, scope: &Scope) -> Result<()> {
    scalars(bld, b, scope)?;
    extract_semis(bld, b, scope)?;
    merge(b)?;
    merge_up(b)?;
    pull_up(b)?;
    distinct_to_group(b);
    semis_to_joins(b)?;
    Ok(())
}

// ------------------------------------------------------------------------------------ facts

/// Whether `e` contains an aggregate function call (window functions excluded).
fn has_aggregate(e: &Expr) -> bool {
    let mut found = false;
    let _ = visit_expressions(e, |x: &Expr| {
        if let Expr::Function(f) = x {
            if f.over.is_none() && AGGREGATES.contains(&function_name(f).as_str()) {
                found = true;
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    found
}

fn has_window(e: &Expr) -> bool {
    let mut found = false;
    let _ = visit_expressions(e, |x: &Expr| {
        if matches!(x, Expr::Function(f) if f.over.is_some()) {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

fn group_exprs(s: &Select) -> Option<&Vec<Expr>> {
    match &s.group_by {
        GroupByExpr::Expressions(es, mods) if mods.is_empty() => Some(es),
        _ => None,
    }
}

/// GROUP BY, HAVING or aggregates (a block with one row per group).
pub(crate) fn aggregated(b: &Block) -> bool {
    !matches!(group_exprs(&b.select), Some(es) if es.is_empty())
        || b.select.having.is_some()
        || b.outputs.iter().any(|o| has_aggregate(&o.expr))
}

fn windowed(b: &Block) -> bool {
    !b.select.named_window.is_empty() || b.outputs.iter().any(|o| has_window(&o.expr))
}

fn block_has_subquery(b: &Block) -> bool {
    exprs_of(&b.select).into_iter().any(has_subquery)
        || b.outputs.iter().any(|o| has_subquery(&o.expr))
        || tail_has_subquery(b)
}

/// The ORDER BY kept with a LIMIT holds a subquery (its names are not rewritten: they resolve in
/// the subquery's own FROM first).
pub(crate) fn tail_has_subquery(b: &Block) -> bool {
    match b
        .tail
        .as_ref()
        .and_then(|t| t.order_by.as_ref())
        .map(|o| &o.kind)
    {
        Some(sqlparser::ast::OrderByKind::Expressions(es)) => {
            es.iter().any(|e| has_subquery(&e.expr))
        }
        _ => false,
    }
}

/// The construct that keeps a derived block from being merged into its parent as a plain
/// select/project/join (None: it can be merged).
pub(crate) fn blocker(b: &Block) -> Option<String> {
    if b.select.from.is_empty() {
        return Some("no FROM clause".into());
    }
    if let Some(it) = b
        .items
        .iter()
        .find(|i| matches!(i.source, Source::Derived(_)))
    {
        return Some(unmerged_reason(b, it));
    }
    if b.limit {
        return Some(if b.ordered {
            "ORDER BY … LIMIT".into()
        } else {
            "LIMIT without ORDER BY".into()
        });
    }
    if windowed(b) {
        return Some("window function".into());
    }
    if b.select.distinct.is_some() || b.converted_distinct() {
        return Some("DISTINCT".into());
    }
    if !matches!(&b.select.group_by, GroupByExpr::Expressions(es, mods) if es.is_empty() && mods.is_empty())
    {
        return Some("GROUP BY".into());
    }
    if b.select.having.is_some() {
        return Some("HAVING".into());
    }
    if b.outputs.iter().any(|o| has_aggregate(&o.expr)) {
        return Some("aggregate function".into());
    }
    if block_has_subquery(b) {
        return Some("subquery".into());
    }
    None
}

impl Block {
    fn converted_distinct(&self) -> bool {
        self.distinct_was
    }
}

/// Why item `it` of `b` stays a separate relation (it was not merged).
pub(crate) fn unmerged_reason(b: &Block, it: &Item) -> String {
    let what = it.origin.describe(&it.alias);
    if star_with_using(&b.select) {
        return "SELECT * over a USING join".into();
    }
    if tail_has_subquery(b) {
        return format!("subquery in the ORDER BY of a top-N view over {what}");
    }
    let Source::Derived(rel) = &it.source else {
        return format!("{what} could not be merged");
    };
    let inner = match rel {
        Rel::Opaque { why } => return why.clone(),
        Rel::Block(inner) => inner,
    };
    if let Some(why) = blocker(inner) {
        if aggregated(inner) || why == "DISTINCT" {
            return if b.items.len() > 1 {
                format!("{why} in {what} joined to other tables")
            } else {
                format!("{why} in {what} under an aggregate, DISTINCT, window or LIMIT")
            };
        }
        return format!("{why} in {what}");
    }
    match kind_at(&b.select.from, it.pos) {
        JoinKind::Left if !single_table(inner) => {
            format!("{what} joins several tables on the NULL side of a LEFT JOIN")
        }
        JoinKind::Left => format!(
            "{what} on the NULL side of a LEFT JOIN computes a column that is not NULL for missing rows"
        ),
        JoinKind::Other => match &it.pos {
            (t, Some(j)) => match join_constraint(&b.select.from[*t].joins[*j].join_operator) {
                Some(JoinConstraint::Natural) => "NATURAL join".into(),
                _ => "RIGHT/FULL/other join".into(),
            },
            _ => "RIGHT/FULL/other join".into(),
        },
        _ if block_has_subquery(b) => "subquery".into(),
        _ => format!("{what} could not be merged"),
    }
}

fn single_table(b: &Block) -> bool {
    b.select.from.len() == 1 && b.select.from[0].joins.is_empty() && b.items.len() == 1
}

// ------------------------------------------------------------------------------- rewriting

fn parse_expr(sql: &str) -> Result<Expr> {
    Parser::new(&MySqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| anyhow!("internal: cannot parse {sql}: {e}"))
}

/// Replaces GROUP BY ordinals and select-alias references in GROUP BY / HAVING by the
/// expressions they stand for (FROM columns take precedence, as on the server).
fn resolve_aliases(b: &mut Block, r: &Resolver) -> Result<()> {
    let outputs: Vec<(String, Expr)> = b
        .outputs
        .iter()
        .map(|o| (o.name.text.to_lowercase(), o.expr.clone()))
        .collect();
    let alias = |id: &Ident| {
        outputs
            .iter()
            .find(|(n, _)| *n == ident_name(id).to_lowercase())
            .map(|(_, e)| nested(e.clone()))
    };
    if let GroupByExpr::Expressions(es, _) = &mut b.select.group_by {
        for e in es.iter_mut() {
            let new = match &*e {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => {
                        let i: usize = n.parse().map_err(|_| anyhow!("bad GROUP BY {n}"))?;
                        Some(
                            outputs
                                .get(i.wrapping_sub(1))
                                .map(|(_, e)| e.clone())
                                .ok_or_else(|| anyhow!("GROUP BY {i} out of range"))?,
                        )
                    }
                    _ => None,
                },
                Expr::Identifier(id) if r.resolve(e) == Res::Unknown => alias(id),
                _ => None,
            };
            if let Some(n) = new {
                *e = n;
            }
        }
    }
    if let Some(h) = &mut b.select.having {
        let _ = visit_expressions_mut(h, |x: &mut Expr| {
            if let Expr::Identifier(id) = &*x {
                if r.resolve(x) == Res::Unknown {
                    if let Some(n) = alias(id) {
                        *x = n;
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    Ok(())
}

/// Qualifies the column references of `e` with their item aliases and replaces those in
/// `subst`. Errors on ambiguous references; leaves unknown identifiers alone.
fn rewrite_expr(
    e: &mut Expr,
    r: &Resolver,
    aliases: &[String],
    subst: &HashMap<(usize, usize), Expr>,
) -> Result<()> {
    let mut err = None;
    let _ = visit_expressions_mut(e, |x: &mut Expr| {
        if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            match r.resolve(x) {
                Res::Col(i, c) => {
                    *x = match subst.get(&(i, c)) {
                        Some(s) => nested(s.clone()),
                        None => {
                            let col = match &*x {
                                Expr::Identifier(id) => id.clone(),
                                Expr::CompoundIdentifier(p) => p.last().unwrap().clone(),
                                _ => unreachable!(),
                            };
                            qcol(&aliases[i], &col)
                        }
                    };
                }
                Res::Ambiguous(n) => {
                    err.get_or_insert_with(|| anyhow!("column `{n}` is ambiguous"));
                }
                Res::Unknown => {}
            }
        }
        ControlFlow::<()>::Continue(())
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Qualifies and substitutes every expression of the block (see [`rewrite_expr`]), with the
/// resolver of the block as it was before any FROM change. Outputs whose text changed lose their
/// "as written" form.
fn rewrite(b: &mut Block, r: &Resolver, subst: &HashMap<(usize, usize), Expr>) -> Result<()> {
    resolve_aliases(b, r)?;
    let aliases: Vec<String> = b.items.iter().map(|i| i.alias.clone()).collect();
    for o in &mut b.outputs {
        let before = o.expr.to_string();
        rewrite_expr(&mut o.expr, r, &aliases, subst)?;
        if o.expr.to_string() != before {
            o.original = None;
        }
    }
    let mut res = Ok(());
    for_each_expr_mut(&mut b.select, |e| {
        if res.is_ok() {
            res = rewrite_expr(e, r, &aliases, subst);
        }
    });
    res?;
    // ORDER BY (kept with a LIMIT) prefers select aliases, then FROM columns.
    let names: Vec<String> = b
        .outputs
        .iter()
        .map(|o| o.name.text.to_lowercase())
        .collect();
    if let Some(sqlparser::ast::OrderByKind::Expressions(es)) = b
        .tail
        .as_mut()
        .and_then(|t| t.order_by.as_mut())
        .map(|o| &mut o.kind)
    {
        for e in es.iter_mut() {
            let alias = matches!(&e.expr, Expr::Identifier(id) if names.contains(&ident_name(id).to_lowercase()));
            if !alias && !matches!(&e.expr, Expr::Value(_)) && !has_subquery(&e.expr) {
                rewrite_expr(&mut e.expr, r, &aliases, subst)?;
            }
        }
    }
    b.qualified = true;
    b.rewritten = true;
    Ok(())
}

/// Qualifies every column reference of a block that is not qualified yet.
pub(crate) fn qualify(b: &mut Block) -> Result<()> {
    if !b.qualified {
        let r = Resolver::new(b);
        rewrite(b, &r, &HashMap::new())?;
    }
    Ok(())
}

/// Replaces `alias.col` qualifiers per `renames` (lower-case old alias → new alias).
fn rename_refs(e: &mut Expr, renames: &HashMap<String, String>) {
    let _ = visit_expressions_mut(e, |x: &mut Expr| {
        if let Expr::CompoundIdentifier(parts) = x {
            if parts.len() == 2 {
                if let Some(n) = renames.get(&ident_name(&parts[0]).to_lowercase()) {
                    parts[0] = qident(n);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Renames the items of a qualified block so that none collides with `taken` (lower-case);
/// adds the new names to `taken`.
fn rename_items(b: &mut Block, prefix: &str, taken: &mut HashSet<String>) {
    let mut renames = HashMap::new();
    for it in &mut b.items {
        let mut name = it.alias.clone();
        if taken.contains(&name.to_lowercase()) {
            let base = format!("{prefix}__{}", it.alias);
            name = base.clone();
            let mut n = 2;
            while taken.contains(&name.to_lowercase()) || name.chars().count() > 64 {
                name = if base.chars().count() > 60 {
                    format!("_lv_t{n}")
                } else {
                    format!("{base}{n}")
                };
                n += 1;
            }
            renames.insert(it.alias.to_lowercase(), name.clone());
        }
        taken.insert(name.to_lowercase());
        it.alias = name;
    }
    if renames.is_empty() {
        return;
    }
    // FROM factors of renamed items get the new alias.
    let new_alias: Vec<(super::ir::Pos, String)> =
        b.items.iter().map(|i| (i.pos, i.alias.clone())).collect();
    for (pos, alias) in new_alias {
        let f = match pos {
            (t, None) => &mut b.select.from[t].relation,
            (t, Some(j)) => &mut b.select.from[t].joins[j].relation,
        };
        set_factor_alias(f, &alias);
    }
    for o in &mut b.outputs {
        rename_refs(&mut o.expr, &renames);
    }
    for_each_expr_mut(&mut b.select, |e| rename_refs(e, &renames));
}

fn set_factor_alias(f: &mut TableFactor, alias: &str) {
    if let TableFactor::Table { alias: a, .. } | TableFactor::Derived { alias: a, .. } = f {
        let cols = a.as_ref().map(|a| a.columns.clone()).unwrap_or_default();
        *a = Some(sqlparser::ast::TableAlias {
            explicit: true,
            name: qident(alias),
            columns: cols,
            at: None,
        });
    }
}

/// The name that qualifies a factor's columns.
fn factor_name(f: &TableFactor) -> Option<String> {
    if let Some(a) = factor_alias(f) {
        return Some(ident_name(&a.name));
    }
    match f {
        TableFactor::Table { name, .. } => match name.0.last() {
            Some(ObjectNamePart::Identifier(i)) => Some(ident_name(i)),
            _ => None,
        },
        _ => None,
    }
}

/// Recomputes item positions from the FROM clause (aliases are unique within a block).
fn relocate(b: &mut Block) -> Result<()> {
    let found: Vec<(super::ir::Pos, String)> = factors(&b.select.from)
        .into_iter()
        .map(|(p, f)| (p, factor_name(f).unwrap_or_default().to_lowercase()))
        .collect();
    let mut items = std::mem::take(&mut b.items);
    for it in &mut items {
        it.pos = found
            .iter()
            .find(|(_, n)| *n == it.alias.to_lowercase())
            .map(|(p, _)| *p)
            .ok_or_else(|| anyhow!("internal: FROM item `{}` lost while merging", it.alias))?;
    }
    items.sort_by_key(|i| i.pos);
    b.items = items;
    Ok(())
}

// ------------------------------------------------------------------------------------- merge

/// Converts `USING (c, …)` of every join into `ON left.c = right.c AND …` (left: the first
/// earlier item of the same FROM entry having the column).
fn using_to_on(b: &mut Block, r: &Resolver) {
    let items = b.items.clone();
    for (t, twj) in b.select.from.iter_mut().enumerate() {
        for (j, join) in twj.joins.iter_mut().enumerate() {
            let Some(c) = join_constraint_mut(&mut join.join_operator) else {
                continue;
            };
            let JoinConstraint::Using(cols) = c else {
                continue;
            };
            let Some(right) = items.iter().position(|i| i.pos == (t, Some(j))) else {
                continue;
            };
            let mut eqs = Vec::new();
            for col in cols.iter() {
                let Some(ObjectNamePart::Identifier(id)) = col.0.last() else {
                    continue;
                };
                let left = items.iter().position(|i| {
                    i.pos.0 == t
                        && i.pos < (t, Some(j))
                        && r.resolve(&Expr::CompoundIdentifier(vec![
                            qident(&i.alias),
                            id.clone(),
                        ])) != Res::Unknown
                });
                if let Some(left) = left {
                    eqs.push(Expr::BinaryOp {
                        left: Box::new(qcol(&items[left].alias, id)),
                        op: BinaryOperator::Eq,
                        right: Box::new(qcol(&items[right].alias, id)),
                    });
                }
            }
            if let Some(on) = and_all(eqs) {
                *c = JoinConstraint::On(on);
            }
        }
    }
}

/// Whether a derived block may be merged at a position of this kind.
fn mergeable_at(inner: &Block, kind: JoinKind) -> bool {
    match kind {
        JoinKind::Head | JoinKind::Inner => true,
        JoinKind::Left => {
            single_table(inner)
                && (inner
                    .outputs
                    .iter()
                    .all(|o| null_propagating(&o.expr) && has_column_ref(&o.expr))
                    || inner.items[0]
                        .table()
                        .is_some_and(|t| t.row_key().is_some()))
        }
        JoinKind::Other => false,
    }
}

/// One chain `r1 joins1 CROSS JOIN r2 joins2 …` from FROM entries.
fn chain(entries: Vec<TableWithJoins>) -> TableWithJoins {
    let mut it = entries.into_iter();
    let mut first = it.next().expect("non-empty FROM");
    for e in it {
        first.joins.push(Join {
            relation: e.relation,
            global: false,
            join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
        });
        first.joins.extend(e.joins);
    }
    first
}

fn merge(b: &mut Block) -> Result<()> {
    let picks: Vec<usize> = b
        .items
        .iter()
        .enumerate()
        .filter(|(_, it)| match &it.source {
            Source::Derived(Rel::Block(inner)) => {
                blocker(inner).is_none() && mergeable_at(inner, kind_at(&b.select.from, it.pos))
            }
            _ => false,
        })
        .map(|(i, _)| i)
        .collect();
    if picks.is_empty() || block_has_subquery(b) || star_with_using(&b.select) {
        return Ok(());
    }
    merge_picks(b, &picks).map(|_| ())
}

/// Merges the derived items `picks` into `b` (see [`merge`]); returns what is left of their
/// blocks (aliases renamed; FROM and WHERE moved out).
fn merge_picks(b: &mut Block, picks: &[usize]) -> Result<Vec<Block>> {
    let r = Resolver::new(b);
    using_to_on(b, &r);
    // Aliases that stay: the other items'.
    let mut taken: HashSet<String> = b
        .items
        .iter()
        .enumerate()
        .filter(|(i, _)| !picks.contains(i))
        .map(|(_, it)| it.alias.to_lowercase())
        .collect();
    let mut inners: HashMap<usize, Block> = HashMap::new();
    let mut subst: HashMap<(usize, usize), Expr> = HashMap::new();
    for &i in picks {
        let it = &mut b.items[i];
        let Source::Derived(Rel::Block(inner)) = std::mem::replace(
            &mut it.source,
            Source::Derived(Rel::Opaque { why: String::new() }),
        ) else {
            unreachable!()
        };
        let mut inner = *inner;
        qualify(&mut inner)?;
        rename_items(&mut inner, &it.alias, &mut taken);
        let null_side = kind_at(&b.select.from, it.pos) == JoinKind::Left;
        for (c, o) in inner.outputs.iter().enumerate() {
            let e = if null_side && !(null_propagating(&o.expr) && has_column_ref(&o.expr)) {
                let t = &inner.items[0];
                // (A key column is NOT NULL in every real row: NULL only when extended.)
                let pk = t
                    .table()
                    .and_then(|t| t.row_key().map(|k| t.columns[k[0]].name.clone()))
                    .unwrap();
                parse_expr(&format!(
                    "IF({} IS NULL, NULL, {})",
                    qcol(&t.alias, &qident(&pk)),
                    o.expr
                ))?
            } else {
                o.expr.clone()
            };
            subst.insert((i, c), e);
        }
        inners.insert(i, inner);
    }
    rewrite(b, &r, &subst)?;
    // Splice the FROM clauses, last position first so that earlier positions stay valid.
    let mut order = picks.to_vec();
    order.sort_by_key(|&i| std::cmp::Reverse(b.items[i].pos));
    let mut extra_where = Vec::new();
    for i in order {
        let inner = inners.get_mut(&i).unwrap();
        let ifrom = std::mem::take(&mut inner.select.from);
        let iwhere = inner.select.selection.take();
        match b.items[i].pos {
            (t, None) => {
                let rest = std::mem::take(&mut b.select.from[t].joins);
                if rest.is_empty() {
                    b.select.from.splice(t..=t, ifrom);
                } else {
                    let mut c = chain(ifrom);
                    c.joins.extend(rest);
                    b.select.from[t] = c;
                }
                extra_where.extend(iwhere);
            }
            (t, Some(j)) => {
                let join = &mut b.select.from[t].joins[j];
                let cond = match join_constraint_mut(&mut join.join_operator) {
                    Some(c) => match std::mem::replace(c, JoinConstraint::None) {
                        JoinConstraint::On(e) => Some(e),
                        JoinConstraint::None => None,
                        other => bail!("internal: unexpected join constraint {other:?}"),
                    },
                    None => None,
                };
                let single = ifrom.len() == 1 && ifrom[0].joins.is_empty();
                if single {
                    let on = and_all(cond.into_iter().chain(iwhere).collect());
                    join.relation = ifrom.into_iter().next().unwrap().relation;
                    if let Some(c) = join_constraint_mut(&mut join.join_operator) {
                        *c = on.map(JoinConstraint::On).unwrap_or(JoinConstraint::None);
                    }
                } else {
                    let c = chain(ifrom);
                    let mut joins = vec![Join {
                        relation: c.relation,
                        global: false,
                        join_operator: JoinOperator::Join(JoinConstraint::None),
                    }];
                    joins.extend(c.joins);
                    b.select.from[t].joins.splice(j..=j, joins);
                    extra_where.extend(cond);
                    extra_where.extend(iwhere);
                }
            }
        }
    }
    if !extra_where.is_empty() {
        let mut all: Vec<Expr> = b
            .select
            .selection
            .take()
            .iter()
            .flat_map(conjuncts)
            .collect();
        all.extend(extra_where);
        b.select.selection = and_all(all);
    }
    // The merged blocks' items replace the derived items.
    let mut items: Vec<Item> = b
        .items
        .drain(..)
        .enumerate()
        .filter(|(i, _)| !picks.contains(i))
        .map(|(_, it)| it)
        .collect();
    let mut rest = Vec::new();
    let mut picked: Vec<(usize, Block)> = inners.into_iter().collect();
    picked.sort_by_key(|(i, _)| *i);
    for (_, mut inner) in picked {
        items.extend(std::mem::take(&mut inner.items));
        b.semis.append(&mut inner.semis);
        rest.push(inner);
    }
    b.items = items;
    relocate(b)?;
    Ok(rest)
}

// ---------------------------------------------------------------------------------- merge-up

/// `SELECT f(c) FROM (SELECT g, agg(x) c FROM … GROUP BY g) d WHERE p` → one grouped block:
/// conjuncts of `p` over group keys only go to WHERE, the others to HAVING.
fn merge_up(b: &mut Block) -> Result<()> {
    if b.items.len() != 1 || b.select.from.len() != 1 || !b.select.from[0].joins.is_empty() {
        return Ok(());
    }
    let Source::Derived(Rel::Block(inner)) = &b.items[0].source else {
        return Ok(());
    };
    let inner_ok = simple_aggregate(inner);
    if !inner_ok
        || aggregated(b)
        || windowed(b)
        || b.limit
        || b.select.distinct.is_some()
        || !b.semis.is_empty()
        || block_has_subquery(b)
    {
        return Ok(());
    }
    let r = Resolver::new(b);
    let Source::Derived(Rel::Block(inner)) = std::mem::replace(
        &mut b.items[0].source,
        Source::Derived(Rel::Opaque { why: String::new() }),
    ) else {
        unreachable!()
    };
    let mut inner = *inner;
    qualify(&mut inner)?;
    let keys: HashSet<String> = group_exprs(&inner.select)
        .map(|es| es.iter().map(|e| nested(e.clone()).to_string()).collect())
        .unwrap_or_default();
    let is_key: Vec<bool> = inner
        .outputs
        .iter()
        .map(|o| keys.contains(&nested(o.expr.clone()).to_string()))
        .collect();
    let subst: HashMap<(usize, usize), Expr> = inner
        .outputs
        .iter()
        .enumerate()
        .map(|(c, o)| ((0, c), o.expr.clone()))
        .collect();
    let aliases = vec![b.items[0].alias.clone()];
    let (mut to_where, mut to_having) = (Vec::new(), Vec::new());
    for c in b.select.selection.take().iter().flat_map(conjuncts) {
        let mut over_keys = true;
        let _ = visit_expressions(&c, |x: &Expr| {
            if let Res::Col(_, col) = r.resolve(x) {
                over_keys &= is_key[col];
            }
            ControlFlow::<()>::Continue(())
        });
        let mut c = c;
        rewrite_expr(&mut c, &r, &aliases, &subst)?;
        if over_keys {
            to_where.push(c);
        } else {
            to_having.push(c);
        }
    }
    let mut outputs = std::mem::take(&mut b.outputs);
    for o in &mut outputs {
        let before = o.expr.to_string();
        rewrite_expr(&mut o.expr, &r, &aliases, &subst)?;
        if o.expr.to_string() != before {
            o.original = None;
        }
    }
    let mut w: Vec<Expr> = inner
        .select
        .selection
        .take()
        .iter()
        .flat_map(conjuncts)
        .collect();
    w.extend(to_where);
    inner.select.selection = and_all(w);
    let mut h: Vec<Expr> = inner
        .select
        .having
        .take()
        .iter()
        .flat_map(conjuncts)
        .collect();
    h.extend(to_having);
    inner.select.having = and_all(h);
    inner.outputs = outputs;
    inner.ordered = b.ordered;
    inner.rewritten = true;
    *b = inner;
    Ok(())
}

/// A block that aggregates its tables in one level (GROUP BY / aggregates over base tables).
fn simple_aggregate(inner: &Block) -> bool {
    aggregated(inner)
        && !windowed(inner)
        && !inner.limit
        && !block_has_subquery(inner)
        && !inner.select.from.is_empty()
        && inner
            .items
            .iter()
            .all(|i| matches!(i.source, Source::Table(..)))
        && inner.select.distinct.is_none()
        && group_exprs(&inner.select).is_some()
}

/// Aggregation pulled up through key-preserving inner joins (Yan & Larson's "eager/lazy
/// aggregation"): `SELECT … FROM t JOIN c ON c.pk = t.g` with `t = SELECT g, agg(x) … GROUP BY g`
/// becomes one grouped block over `t`'s tables joined to `c`, grouped by `g` and the columns of
/// `c` it uses. Each group of `t` joins at most one row of every other table, because the whole
/// primary key of that table is bound (by equalities of the same type family) to group keys or
/// to columns of tables bound before; every base row of the group joins that same row. So joining
/// before aggregating neither multiplies nor drops rows within a group, and conditions on the
/// aggregates move to HAVING.
fn pull_up(b: &mut Block) -> Result<()> {
    if aggregated(b)
        || windowed(b)
        || b.limit
        || b.select.distinct.is_some()
        || !b.semis.is_empty()
        || block_has_subquery(b)
        || star_with_using(&b.select)
        || b.items.len() < 2
    {
        return Ok(());
    }
    let derived: Vec<usize> = (0..b.items.len())
        .filter(|&i| matches!(b.items[i].source, Source::Derived(_)))
        .collect();
    let [d] = derived.as_slice() else {
        return Ok(());
    };
    let d = *d;
    let Source::Derived(Rel::Block(inner)) = &b.items[d].source else {
        return Ok(());
    };
    if !simple_aggregate(inner)
        || b.items.iter().any(|it| {
            !matches!(
                kind_at(&b.select.from, it.pos),
                JoinKind::Head | JoinKind::Inner
            )
        })
        || b.select.from.iter().flat_map(|t| &t.joins).any(|j| {
            !matches!(
                join_constraint(&j.join_operator),
                Some(JoinConstraint::On(_) | JoinConstraint::None) | None
            )
        })
    {
        return Ok(());
    }
    // The inner outputs: group keys (with their base column, when plain) and aggregates.
    let mut q = (**inner).clone();
    qualify(&mut q)?;
    let keys: HashSet<String> = group_exprs(&q.select)
        .map(|es| es.iter().map(|e| nested(e.clone()).to_string()).collect())
        .unwrap_or_default();
    let qr = Resolver::new(&q);
    let column_of = |e: &Expr, r: &Resolver, items: &[Item]| -> Option<crate::catalog::Column> {
        match r.resolve(e) {
            Res::Col(i, c) => items[i].table().map(|t| t.columns[c].clone()),
            _ => None,
        }
    };
    let out_key: Vec<bool> = q
        .outputs
        .iter()
        .map(|o| keys.contains(&nested(o.expr.clone()).to_string()))
        .collect();
    let out_agg: Vec<bool> = q.outputs.iter().map(|o| has_aggregate(&o.expr)).collect();
    let out_col: Vec<Option<crate::catalog::Column>> = q
        .outputs
        .iter()
        .map(|o| column_of(&o.expr, &qr, &q.items))
        .collect();
    let r = Resolver::new(b);
    // Every reference to the derived item must be a group key or an aggregate.
    let refs_ok = |e: &Expr| {
        let mut ok = true;
        let _ = visit_expressions(e, |x: &Expr| {
            if let Res::Col(i, c) = r.resolve(x) {
                ok &= i != d || out_key[c] || out_agg[c];
            }
            ControlFlow::<()>::Continue(())
        });
        ok
    };
    if !b.outputs.iter().all(|o| refs_ok(&o.expr)) || !exprs_of(&b.select).into_iter().all(refs_ok)
    {
        return Ok(());
    }
    // Conjuncts of WHERE and of the inner joins' ON.
    let mut conds: Vec<Expr> = b.select.selection.iter().flat_map(conjuncts).collect();
    for twj in &b.select.from {
        for j in &twj.joins {
            if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                conds.extend(conjuncts(e));
            }
        }
    }
    let refs_agg = |e: &Expr| {
        let mut agg = false;
        let _ = visit_expressions(e, |x: &Expr| {
            if let Res::Col(i, c) = r.resolve(x) {
                agg |= i == d && out_agg[c];
            }
            ControlFlow::<()>::Continue(())
        });
        agg
    };
    // Key preservation, to a fixpoint: a table is bound when each of its primary-key columns
    // equals a group key of the derived item or a column of a bound table ([`same_key_type`]).
    let mut bound: HashSet<usize> = [d].into_iter().collect();
    loop {
        let mut grew = false;
        for (ti, it) in b.items.iter().enumerate() {
            let Some(t) = it.table() else { continue };
            if bound.contains(&ti) {
                continue;
            }
            let covered = t.keys().any(|key| {
                key.iter().all(|&pc| {
                    conds.iter().filter(|c| !refs_agg(c)).any(|c| {
                        let Expr::BinaryOp {
                            left,
                            op: BinaryOperator::Eq,
                            right,
                        } = c
                        else {
                            return false;
                        };
                        [(left, right), (right, left)].iter().any(|(a, z)| {
                            if r.resolve(a) != Res::Col(ti, pc) {
                                return false;
                            }
                            let other = match r.resolve(z) {
                                Res::Col(i, c) if i == d && out_key[c] => out_col[c].clone(),
                                Res::Col(i, c) if i != ti && bound.contains(&i) => {
                                    b.items[i].table().map(|t| t.columns[c].clone())
                                }
                                _ => None,
                            };
                            other.is_some_and(|o| same_key_type(&o, &t.columns[pc]))
                        })
                    })
                })
            });
            if covered {
                bound.insert(ti);
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    if bound.len() != b.items.len() {
        return Ok(());
    }
    // Conditions on aggregates move to HAVING; the other tables' columns used after
    // aggregation join the GROUP BY (they are constant within a group).
    let mut having = Vec::new();
    let keep = |e: Expr, having: &mut Vec<Expr>| -> Option<Expr> {
        if refs_agg(&e) {
            having.push(e);
            None
        } else {
            Some(e)
        }
    };
    let w: Vec<Expr> = b
        .select
        .selection
        .take()
        .into_iter()
        .flat_map(|e| conjuncts(&e))
        .filter_map(|c| keep(c, &mut having))
        .collect();
    b.select.selection = and_all(w);
    for twj in &mut b.select.from {
        for j in &mut twj.joins {
            if let Some(c) = join_constraint_mut(&mut j.join_operator) {
                if let JoinConstraint::On(e) = c {
                    let kept: Vec<Expr> = conjuncts(e)
                        .into_iter()
                        .filter_map(|c| keep(c, &mut having))
                        .collect();
                    *c = and_all(kept)
                        .map(JoinConstraint::On)
                        .unwrap_or(JoinConstraint::None);
                }
            }
        }
    }
    let mut extra_keys: Vec<Expr> = Vec::new();
    let mut seen = HashSet::new();
    for e in b.outputs.iter().map(|o| &o.expr).chain(having.iter()) {
        let _ = visit_expressions(e, |x: &Expr| {
            if let Res::Col(i, c) = r.resolve(x) {
                if i != d {
                    let it = &b.items[i];
                    let col = qcol(&it.alias, &qident(&it.table().unwrap().columns[c].name));
                    if seen.insert(col.to_string()) {
                        extra_keys.push(col);
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    b.select.having = and_all(having);
    b.select.group_by = GroupByExpr::Expressions(extra_keys, vec![]);
    let mut rest = merge_picks(b, &[d])?;
    let inner = rest.pop().unwrap();
    let mut keys = group_exprs(&inner.select).cloned().unwrap_or_default();
    if let Some(es) = group_exprs(&b.select) {
        keys.extend(es.iter().cloned());
    }
    b.select.group_by = GroupByExpr::Expressions(keys, vec![]);
    let mut h: Vec<Expr> = inner.select.having.iter().flat_map(conjuncts).collect();
    h.extend(b.select.having.take().iter().flat_map(conjuncts));
    b.select.having = and_all(h);
    b.rewritten = true;
    Ok(())
}

// ------------------------------------------------------------------------------- DISTINCT

/// `SELECT DISTINCT e1, … FROM …` → `… GROUP BY e1, …` (same equality: NULLs equal,
/// collation-aware). A DISTINCT over one-row-per-group results is dropped.
fn distinct_to_group(b: &mut Block) {
    if !matches!(b.select.distinct, Some(Distinct::Distinct)) || windowed(b) {
        return;
    }
    let keys: Vec<Expr> = b
        .outputs
        .iter()
        .map(|o| o.expr.clone())
        .filter(has_column_ref)
        .collect();
    match group_exprs(&b.select) {
        Some(es) if es.is_empty() && b.select.having.is_none() => {
            if b.outputs.iter().any(|o| has_aggregate(&o.expr)) {
                // A single aggregate row is distinct already.
            } else if keys.is_empty() {
                // Only constants: one row when there is any.
                b.select.having = parse_expr("COUNT(*) > 0").ok();
            } else {
                b.select.group_by = GroupByExpr::Expressions(keys, vec![]);
            }
        }
        Some(es) => {
            let out: HashSet<String> = b
                .outputs
                .iter()
                .map(|o| nested(o.expr.clone()).to_string())
                .collect();
            let resolved_ordinals = es.iter().all(|e| match e {
                Expr::Value(v) => matches!(v.value, Value::Number(..)),
                e => out.contains(&nested(e.clone()).to_string()),
            });
            if !resolved_ordinals {
                return;
            }
        }
        None => return,
    }
    b.select.distinct = None;
    b.distinct_was = true;
    b.rewritten = true;
}

// ---------------------------------------------------------------------- scalar subqueries

/// Correlated scalar subqueries over one table, bound on its whole primary key (at most one row
/// matches), become LEFT JOINs: `(SELECT e FROM s WHERE s.pk = x AND p)` turns into
/// `LEFT JOIN s ON s.pk = x AND p` and `e` — guarded as `IF(s.pk IS NULL, NULL, e)` unless `e` is
/// NULL-propagating, since a scalar subquery without a row is NULL.
fn scalars(bld: &Builder, b: &mut Block, scope: &Scope) -> Result<()> {
    if !b.outputs.iter().any(|o| has_subquery(&o.expr))
        && !b.select.selection.as_ref().is_some_and(has_subquery)
    {
        return Ok(());
    }
    struct V<'a, 'q> {
        bld: &'a Builder<'a>,
        scope: &'a Scope<'q>,
        outer: &'a Resolver,
        taken: &'a mut HashSet<String>,
        joins: &'a mut Vec<(Join, Item)>,
        depth: usize,
        err: Option<anyhow::Error>,
        /// Where the subqueries being visited sit, and why those that stay subqueries do.
        place: &'static str,
        blockers: Vec<String>,
    }
    impl sqlparser::ast::VisitorMut for V<'_, '_> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
            if self.depth == 0 {
                if let Expr::Subquery(q) = &*e {
                    match scalar(self.bld, self.outer, q, self.scope, self.taken) {
                        Ok(Ok((x, join, item))) => {
                            *e = x;
                            self.joins.push((join, item));
                        }
                        Ok(Err(Some(why))) => {
                            self.blockers.push(format!("{why} in {}", self.place))
                        }
                        Ok(Err(None)) => {}
                        Err(err) => {
                            self.err = Some(err);
                            return ControlFlow::Break(());
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let outer = Resolver::new(b);
    let mut taken: HashSet<String> = b.items.iter().map(|i| i.alias.to_lowercase()).collect();
    let mut joins = Vec::new();
    let mut v = V {
        bld,
        scope,
        outer: &outer,
        taken: &mut taken,
        joins: &mut joins,
        depth: 0,
        err: None,
        place: "the SELECT list",
        blockers: Vec::new(),
    };
    use sqlparser::ast::VisitMut;
    for o in &mut b.outputs {
        let before = o.expr.to_string();
        let _ = o.expr.visit(&mut v);
        if o.expr.to_string() != before {
            o.original = None;
        }
    }
    v.place = "the WHERE clause";
    if let Some(w) = &mut b.select.selection {
        let _ = w.visit(&mut v);
    }
    if let Some(e) = v.err.take() {
        return Err(e);
    }
    b.semi_blockers.append(&mut v.blockers);
    if joins.is_empty() {
        return Ok(());
    }
    // One FROM chain (comma joins become CROSS JOINs), then the LEFT JOINs at its end.
    if b.select.from.len() > 1 {
        let from = std::mem::take(&mut b.select.from);
        b.select.from = vec![chain(from)];
    }
    for (join, item) in joins {
        b.select.from[0].joins.push(join);
        b.items.push(item);
    }
    b.rewritten = true;
    relocate(b)
}

/// The LEFT JOIN replacing one scalar subquery, if it is a unique lookup (see [`scalars`]);
/// else why it stays a subquery, where that has a precise reason.
type ScalarJoin = std::result::Result<(Expr, Join, Item), Option<String>>;

fn scalar(
    bld: &Builder,
    outer: &Resolver,
    q: &Query,
    scope: &Scope,
    taken: &mut HashSet<String>,
) -> Result<ScalarJoin> {
    let Rel::Block(sb) = bld.nested(q, scope)? else {
        return Ok(Err(None));
    };
    let sb = *sb;
    // C6: an aggregate over the rows a correlation selects is one row per outer key, an internal
    // grouped view joined on that key; uncorrelated, one value for every row.
    if aggregated(&sb) && !windowed(&sb) && !sb.limit {
        let inner = Resolver::new(&sb);
        let mut correlated = false;
        let _ = visit_expressions(&sb.select, |x: &Expr| {
            if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && inner.resolve(x) == Res::Unknown
                && matches!(outer.resolve(x), Res::Col(..))
            {
                correlated = true;
                return ControlFlow::Break(());
            }
            ControlFlow::<()>::Continue(())
        });
        return Ok(Err(Some(if correlated {
            "correlated aggregate subquery".to_string()
        } else {
            "uncorrelated aggregate subquery".to_string()
        })));
    }
    let simple = single_table(&sb)
        && matches!(sb.items[0].source, Source::Table(..))
        && sb.outputs.len() == 1
        && !sb.limit
        && !aggregated(&sb)
        && !windowed(&sb)
        && sb.select.distinct.is_none()
        && sb.semis.is_empty()
        && !block_has_subquery(&sb);
    if !simple {
        return Ok(Err(None));
    }
    let Source::Table(tref, info) = sb.items[0].source.clone() else {
        unreachable!()
    };
    if info.row_key().is_none() {
        return Ok(Err(None));
    }
    let inner = Resolver::new(&sb);
    // Inner, outer or unknown identifiers of an expression.
    let scan = |e: &Expr| {
        let (mut inner_refs, mut outer_refs, mut unknown) = (Vec::new(), 0, false);
        let _ = visit_expressions(e, |x: &Expr| {
            if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                match inner.resolve(x) {
                    Res::Col(_, c) => inner_refs.push(c),
                    Res::Ambiguous(_) => unknown = true,
                    Res::Unknown => match outer.resolve(x) {
                        Res::Col(..) => outer_refs += 1,
                        _ => unknown = true,
                    },
                }
            }
            ControlFlow::<()>::Continue(())
        });
        (inner_refs, outer_refs, unknown)
    };
    let conds: Vec<Expr> = sb.select.selection.iter().flat_map(conjuncts).collect();
    let mut bound = HashSet::new();
    for c in &conds {
        if scan(c).2 {
            return Ok(Err(None));
        }
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = c
        {
            for (a, z) in [(left, right), (right, left)] {
                if let Res::Col(_, col) = inner.resolve(a) {
                    let (zi, zo, _) = scan(z);
                    if zi.is_empty() && zo > 0 && key_exact(&info.columns[col], outer, z) {
                        bound.insert(col);
                    }
                }
            }
        }
    }
    // Each outer row matches at most one row: a whole key (the PK or a UNIQUE NOT NULL key) is
    // bound to outer columns that compare like it (G1).
    let Some(key) = info
        .keys()
        .find(|k| k.iter().all(|c| bound.contains(c)))
        .map(<[usize]>::to_vec)
    else {
        return Ok(Err(None));
    };
    if scan(&sb.outputs[0].expr).2 {
        return Ok(Err(None));
    }
    let mut alias = sb.items[0].alias.clone();
    if taken.contains(&alias.to_lowercase()) {
        let base = alias.clone();
        let mut n = 2;
        while taken.contains(&format!("{base}_{n}").to_lowercase()) {
            n += 1;
        }
        alias = format!("{base}_{n}");
    }
    taken.insert(alias.to_lowercase());
    // Qualify the inner columns with the alias; outer references stay as written.
    let localize = |mut e: Expr| {
        let _ = visit_expressions_mut(&mut e, |x: &mut Expr| {
            if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                if let Res::Col(_, c) = inner.resolve(x) {
                    *x = qcol(&alias, &qident(&info.columns[c].name));
                }
            }
            ControlFlow::<()>::Continue(())
        });
        e
    };
    // The value is NULL for an outer row without a match only if it propagates NULL from a
    // column of the subquery's own table; a value over outer columns alone (or constants)
    // keeps its value through the NULL-extended row and needs the guard (A6).
    let reads_inner = !scan(&sb.outputs[0].expr).0.is_empty();
    let value = localize(sb.outputs[0].expr.clone());
    let value = if reads_inner && null_propagating(&value) {
        value
    } else {
        parse_expr(&format!(
            "IF({} IS NULL, NULL, {value})",
            qcol(&alias, &qident(&info.columns[key[0]].name))
        ))?
    };
    let on = and_all(conds.into_iter().map(localize).collect());
    let name = ObjectName(vec![
        ObjectNamePart::Identifier(qident(&tref.0)),
        ObjectNamePart::Identifier(qident(&tref.1)),
    ]);
    let join = Join {
        relation: table_factor(name, Some(&alias)),
        global: false,
        join_operator: JoinOperator::Left(
            on.map(JoinConstraint::On).unwrap_or(JoinConstraint::None),
        ),
    };
    let item = Item {
        alias,
        pos: (0, None),
        origin: Origin::Table,
        source: Source::Table(tref, info),
    };
    Ok(Ok((nested(value), join, item)))
}

// ------------------------------------------------------------------------------ semi-joins

fn placeholder(n: usize) -> Expr {
    Expr::Identifier(Ident::new(format!("{OUTER_PLACEHOLDER}{n}__")))
}

fn placeholder_index(e: &Expr) -> Option<usize> {
    match e {
        Expr::Identifier(id) if id.quote_style.is_none() => id
            .value
            .strip_prefix(OUTER_PLACEHOLDER)?
            .strip_suffix("__")?
            .parse()
            .ok(),
        _ => None,
    }
}

/// Replaces placeholders by the given outer expressions.
pub(crate) fn fill_placeholders(e: &mut Expr, args: &[Expr]) {
    let _ = visit_expressions_mut(e, |x: &mut Expr| {
        if let Some(i) = placeholder_index(x) {
            if let Some(a) = args.get(i) {
                *x = nested(a.clone());
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

pub(crate) fn marker_name(id: usize) -> String {
    format!("{SEMI_MARKER}{id}__")
}

/// The arguments of the marker of semi-join `id` in `e`, if present.
pub(crate) fn marker_args(e: &Expr, id: usize) -> Option<Vec<Expr>> {
    let name = marker_name(id);
    let mut out = None;
    let _ = visit_expressions(e, |x: &Expr| {
        if let Expr::Function(f) = x {
            if f.name.to_string() == name {
                if let FunctionArguments::List(l) = &f.args {
                    out = Some(
                        l.args
                            .iter()
                            .filter_map(|a| match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                                _ => None,
                            })
                            .collect(),
                    );
                }
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    out
}

pub(crate) fn query_of(select: Select) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(select))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }
}

/// Whether column `c` of `item` (in FROM `from`) is never NULL: NOT NULL in its table, which is
/// the head or inner-joined (not on the NULL side of a LEFT JOIN: a merge onto a NULL side moves
/// this WHERE into an ON, refused later) and not followed by a RIGHT / FULL / other join in its
/// FROM entry (which would NULL-extend it); for a derived table or CTE, the column its output names
/// in the inner block, recursively (not through aggregation, whose groups may add NULLs).
fn never_null(from: &[TableWithJoins], item: &Item, c: usize) -> bool {
    let (t, j) = item.pos;
    let later = &from[t].joins[j.map_or(0, |j| j + 1)..];
    if !matches!(kind_at(from, item.pos), JoinKind::Head | JoinKind::Inner)
        || later
            .iter()
            .any(|x| matches!(join_kind(x), JoinKind::Other))
    {
        return false;
    }
    match &item.source {
        Source::Table(_, t) => !t.columns[c].nullable,
        Source::Derived(Rel::Block(inner)) => {
            !aggregated(inner)
                && inner.outputs.get(c).is_some_and(|o| {
                    match Resolver::new(inner).resolve(&o.expr) {
                        Res::Col(j, d) => never_null(&inner.select.from, &inner.items[j], d),
                        _ => false,
                    }
                })
        }
        Source::Derived(_) => false,
    }
}

/// Takes semi-/anti-join conjuncts over one table out of WHERE (see [`Semi`]).
fn extract_semis(bld: &Builder, b: &mut Block, scope: &Scope) -> Result<()> {
    let Some(w) = &b.select.selection else {
        return Ok(());
    };
    let parts = conjuncts(w);
    if !parts
        .iter()
        .any(|c| matches!(c, Expr::Exists { .. } | Expr::InSubquery { .. }))
    {
        return Ok(());
    }
    let outer = Resolver::new(b);
    let (items, from) = (&b.items, &b.select.from);
    let not_null = |e: &Expr| match outer.resolve(e) {
        Res::Col(i, c) => never_null(from, &items[i], c),
        _ => false,
    };
    let mut kept = Vec::new();
    let mut found = false;
    let mut blockers = Vec::new();
    for c in parts {
        match semi(bld, &outer, &not_null, &c, scope, &mut blockers)? {
            Some(s) => {
                kept.push(s.1);
                b.semis.push(s.0);
                found = true;
            }
            None => kept.push(c),
        }
    }
    b.semi_blockers.extend(blockers);
    if found {
        b.select.selection = and_all(kept);
        b.rewritten = true;
    }
    Ok(())
}

/// Why a subquery block cannot be a semi-join alias (precise fallback reason).
fn semi_blocker(kind: &str, sb: &Block) -> String {
    if sb.items.is_empty() {
        return format!("{kind} without a FROM clause");
    }
    if let Some(it) = sb
        .items
        .iter()
        .find(|i| !matches!(i.source, Source::Table(..)))
    {
        return format!("{kind} over {}", it.origin.describe(&it.alias));
    }
    if sb.limit {
        return format!("{kind} with LIMIT");
    }
    if windowed(sb) {
        return format!("{kind} with a window function");
    }
    if !sb.semis.is_empty() || block_has_subquery(sb) {
        return format!("{kind} containing a subquery");
    }
    format!("{kind} with an aggregate")
}

/// The ON conditions of a subquery's FROM when its tables are joined by inner joins only (each
/// becomes a conjunct of its WHERE); the reason otherwise.
fn inner_join_conditions(kind: &str, sb: &Block) -> std::result::Result<Vec<Expr>, String> {
    let mut on = Vec::new();
    for j in sb.select.from.iter().flat_map(|t| &t.joins) {
        match &j.join_operator {
            JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                match c {
                    JoinConstraint::On(e) => on.push(e.clone()),
                    JoinConstraint::None => {}
                    _ => {
                        return Err(format!(
                            "{kind} with a join USING columns of several tables"
                        ))
                    }
                }
            }
            // A lookup of the NULL side would have to drop the conditions that keep
            // NULL-extended rows (as the block planner does for the view's own FROM).
            _ => return Err(format!("{kind} with an outer join")),
        }
    }
    Ok(on)
}

/// A semi-join for conjunct `c`, with the marker that replaces it. A subquery that stays one
/// leaves its reason in `blockers`.
fn semi(
    bld: &Builder,
    outer: &Resolver,
    outer_not_null: &dyn Fn(&Expr) -> bool,
    c: &Expr,
    scope: &Scope,
    blockers: &mut Vec<String>,
) -> Result<Option<(Semi, Expr)>> {
    let (lhs, subquery, negated) = match c {
        Expr::Exists { subquery, negated } => (None, subquery, *negated),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => (Some(expr), subquery, *negated),
        _ => return Ok(None),
    };
    let kind = match (lhs.is_some(), negated) {
        (true, false) => "IN subquery",
        (true, true) => "NOT IN subquery",
        (false, false) => "EXISTS subquery",
        (false, true) => "NOT EXISTS subquery",
    };
    let mut not_in = lhs.is_some() && negated;
    if not_in && matches!(lhs.map(|e| &**e), Some(Expr::Tuple(_))) {
        blockers.push("NOT IN over a row constructor".into());
        return Ok(None);
    }
    let Rel::Block(sb) = bld.nested(subquery, scope)? else {
        blockers.push(format!("{kind} over a set operation or VALUES"));
        return Ok(None);
    };
    let mut sb = *sb;
    let simple = !sb.items.is_empty()
        && sb
            .items
            .iter()
            .all(|i| matches!(i.source, Source::Table(..)))
        && !sb.limit
        && !windowed(&sb)
        && sb.semis.is_empty()
        && sb.select.having.is_none()
        && !sb.outputs.iter().any(|o| has_aggregate(&o.expr))
        && !block_has_subquery(&sb);
    if !simple {
        blockers.push(semi_blocker(kind, &sb));
        return Ok(None);
    }
    // Several tables: joined by inner joins, whose ON conditions are WHERE conjuncts.
    let several = !single_table(&sb);
    if several {
        let r = Resolver::new(&sb);
        using_to_on(&mut sb, &r);
    }
    let mut on = match inner_join_conditions(kind, &sb) {
        Ok(on) => on,
        Err(why) => {
            blockers.push(why);
            return Ok(None);
        }
    };
    let mut tables: Vec<SemiTable> = sb
        .items
        .iter()
        .map(|it| match &it.source {
            Source::Table(t, info) => SemiTable {
                table: t.clone(),
                info: info.clone(),
                alias: it.alias.clone(),
                referenced: vec![false; info.columns.len()],
            },
            Source::Derived(_) => unreachable!(),
        })
        .collect();
    let inner = Resolver::new(&sb);
    let mut where_ = sb.select.selection.take();
    let mut proj: Vec<Expr> = sb.outputs.iter().map(|o| o.expr.clone()).collect();
    // VIRTUAL generated columns of the subquery's tables have no value in the row images its
    // lookups substitute: their expressions, converted to their types, stand in (B2).
    let mut virtual_failed = None;
    for e in on
        .iter_mut()
        .chain(where_.iter_mut())
        .chain(proj.iter_mut())
    {
        let _ = visit_expressions_mut(e, |x: &mut Expr| {
            if let Res::Col(i, col) = inner.resolve(x) {
                let t = &tables[i];
                if t.info.columns[col]
                    .generated
                    .as_ref()
                    .is_some_and(|g| !g.stored)
                {
                    match crate::analyze::virtual_expansion(&t.alias, &t.info, col) {
                        Ok(v) => *x = v,
                        Err(r) => {
                            virtual_failed.get_or_insert(r);
                        }
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    if let Some(why) = virtual_failed {
        blockers.push(format!("{kind} reading {why}"));
        return Ok(None);
    }
    // Inner columns are qualified with their alias, outer references become placeholders.
    let aliases: Vec<String> = tables.iter().map(|t| t.alias.clone()).collect();
    let mut outer_exprs: Vec<Expr> = Vec::new();
    let mut ok = true;
    let mut referenced: Vec<Vec<bool>> = tables.iter().map(|t| t.referenced.clone()).collect();
    let mut localize =
        |e: &mut Expr, outer_exprs: &mut Vec<Expr>, referenced: &mut Vec<Vec<bool>>| {
            let _ = visit_expressions_mut(e, |x: &mut Expr| {
                if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    && placeholder_index(x).is_none()
                {
                    match inner.resolve(x) {
                        Res::Col(i, col) => {
                            referenced[i][col] = true;
                            let id = match &*x {
                                Expr::Identifier(id) => id.clone(),
                                Expr::CompoundIdentifier(p) => p.last().unwrap().clone(),
                                _ => unreachable!(),
                            };
                            *x = qcol(&aliases[i], &id);
                        }
                        Res::Ambiguous(_) => ok = false,
                        Res::Unknown => match outer.resolve(x) {
                            Res::Col(..) => {
                                outer_exprs.push(x.clone());
                                *x = placeholder(outer_exprs.len() - 1);
                            }
                            _ => ok = false,
                        },
                    }
                }
                ControlFlow::<()>::Continue(())
            });
        };
    for e in &mut on {
        localize(e, &mut outer_exprs, &mut referenced);
    }
    if let Some(w) = &mut where_ {
        localize(w, &mut outer_exprs, &mut referenced);
    }
    for p in &mut proj {
        localize(p, &mut outer_exprs, &mut referenced);
    }
    if !ok {
        return Ok(None);
    }
    for (t, r) in tables.iter_mut().zip(referenced) {
        t.referenced = r;
    }
    let mut correlation = Vec::new();
    let mut join_conditions = Vec::new();
    let mut lookup_conditions = Vec::new();
    let lhs_parts: Vec<Expr> = match lhs {
        Some(e) => match &**e {
            Expr::Tuple(es) => es.clone(),
            other => vec![other.clone()],
        },
        None => Vec::new(),
    };
    let lhs_ph: Vec<Expr> = lhs_parts
        .iter()
        .map(|e| {
            outer_exprs.push(e.clone());
            placeholder(outer_exprs.len() - 1)
        })
        .collect();
    // `x NOT IN (SELECT k …)` with x and k never NULL is `NOT EXISTS (… AND k = x)`: no NULL
    // makes every row relevant, and the correlation can give direct units.
    let not_null_key = |p: &Expr| match inner.resolve(p) {
        Res::Col(t, c) => !tables[t].info.columns[c].nullable,
        _ => false,
    };
    let exists_form = not_in
        && lhs_parts.len() == 1
        && proj.len() == 1
        && outer_not_null(&lhs_parts[0])
        && not_null_key(&proj[0]);
    if exists_form {
        not_in = false;
    }
    if lhs.is_some() {
        if proj.len() != lhs_ph.len() {
            return Ok(None);
        }
        for (p, ph) in proj.iter().zip(&lhs_ph) {
            // (An expression relates rows by its value too; only a column can be a key.)
            if let Res::Col(t, col) = inner.resolve(p) {
                correlation.push((t, col, placeholder_index(ph).unwrap()));
            }
            let eq = Expr::BinaryOp {
                left: Box::new(p.clone()),
                op: BinaryOperator::Eq,
                right: Box::new(ph.clone()),
            };
            lookup_conditions.push(if not_in {
                // `x NOT IN K` also changes for every x when a NULL enters or leaves K, and for
                // a NULL x whenever K becomes empty or not.
                parse_expr(&format!("({eq} OR {ph} IS NULL OR {p} IS NULL)"))?
            } else {
                eq.clone()
            });
            join_conditions.push(eq);
        }
    }
    let conds: Vec<Expr> = on
        .iter()
        .flat_map(conjuncts)
        .chain(where_.iter().flat_map(conjuncts))
        .collect();
    for cj in conds {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = &cj
        {
            for (a, z) in [(left, right), (right, left)] {
                if let (Res::Col(t, col), Some(k)) = (inner.resolve(a), placeholder_index(z)) {
                    correlation.push((t, col, k));
                }
            }
        }
        lookup_conditions.push(cj.clone());
        join_conditions.push(cj);
    }
    if not_in {
        correlation.clear();
    }
    let mut sel = sb.select.clone();
    sel.distinct = None;
    if several {
        // `t1 AS a, t2 AS b WHERE <ON conditions> AND <WHERE>`: inner joins associate.
        sel.from = tables
            .iter()
            .map(|t| TableWithJoins {
                relation: table_factor(
                    ObjectName(vec![
                        ObjectNamePart::Identifier(qident(&t.table.0)),
                        ObjectNamePart::Identifier(qident(&t.table.1)),
                    ]),
                    Some(&t.alias),
                ),
                joins: vec![],
            })
            .collect();
        sel.selection = and_all(on.into_iter().chain(where_).collect());
    } else {
        sel.selection = where_;
    }
    if exists_form {
        let eq = Expr::BinaryOp {
            left: Box::new(proj[0].clone()),
            op: BinaryOperator::Eq,
            right: Box::new(lhs_ph[0].clone()),
        };
        sel.selection = and_all(sel.selection.take().into_iter().chain([eq]).collect());
        sel.projection = vec![SelectItem::UnnamedExpr(parse_expr("1")?)];
    } else if lhs.is_some() {
        sel.projection = proj.into_iter().map(SelectItem::UnnamedExpr).collect();
    }
    let predicate = match lhs {
        Some(_) if exists_form => Expr::Exists {
            subquery: Box::new(query_of(sel)),
            negated: true,
        },
        Some(_) => Expr::InSubquery {
            expr: Box::new(if lhs_ph.len() == 1 {
                lhs_ph[0].clone()
            } else {
                Expr::Tuple(lhs_ph.clone())
            }),
            subquery: Box::new(query_of(sel)),
            negated: not_in,
        },
        None => Expr::Exists {
            subquery: Box::new(query_of(sel)),
            negated,
        },
    };
    let id = bld.next_semi.get();
    bld.next_semi.set(id + 1);
    let args: Vec<String> = outer_exprs.iter().map(|e| e.to_string()).collect();
    let marker = parse_expr(&format!("{}({})", marker_name(id), args.join(", ")))?;
    Ok(Some((
        Semi {
            id,
            negated,
            tables,
            predicate,
            join_conditions,
            lookup_conditions,
            not_in,
            correlation,
        },
        marker,
    )))
}

/// `x NOT IN (SELECT k FROM S WHERE p)` (the predicate of a NOT IN semi-join) as its exact form
/// in a filter context (WHERE / ON: NULL and FALSE both drop the row): `NOT EXISTS (S where p)
/// OR (x IS NOT NULL AND NOT EXISTS (… AND k = x) AND NOT EXISTS (… AND k IS NULL))`. MariaDB
/// 10.11 / 13.0 re-use a stale NOT IN result when a prepared statement is re-executed after the
/// subquery flips between empty and non-empty (F5); NOT EXISTS is not affected.
pub(crate) fn not_in_as_not_exists(pred: &Expr) -> Option<Expr> {
    let Expr::InSubquery {
        expr,
        subquery,
        negated: true,
    } = pred
    else {
        return None;
    };
    let SetExpr::Select(sel) = subquery.body.as_ref() else {
        return None;
    };
    let [SelectItem::UnnamedExpr(k)] = sel.projection.as_slice() else {
        return None;
    };
    let x = nested((**expr).clone());
    let k = nested(k.clone());
    let one = parse_expr("1").ok()?;
    let not_exists = |extra: Option<Expr>| {
        let mut s = (**sel).clone();
        s.projection = vec![SelectItem::UnnamedExpr(one.clone())];
        s.selection = and_all(s.selection.take().into_iter().chain(extra).collect());
        Expr::Exists {
            subquery: Box::new(query_of(s)),
            negated: true,
        }
    };
    let and = |a: Expr, b: Expr| Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::And,
        right: Box::new(b),
    };
    let none = not_exists(None);
    let no_match = not_exists(Some(Expr::BinaryOp {
        left: Box::new(k.clone()),
        op: BinaryOperator::Eq,
        right: Box::new(x.clone()),
    }));
    let no_null = not_exists(Some(Expr::IsNull(Box::new(k))));
    Some(Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new(none),
        op: BinaryOperator::Or,
        right: Box::new(Expr::Nested(Box::new(and(
            and(Expr::IsNotNull(Box::new(x)), no_match),
            no_null,
        )))),
    })))
}

/// `s` with the aliases of its subquery's tables renamed away from `taken` (the lower-case
/// aliases of the block it belongs to, after every merge and rewrite): the outer expressions
/// filled into its predicate and lookup conditions must not be captured by a subquery table of
/// the same name (F3: `d.customer_id` merged into `o.customer_id` against the subquery's own
/// `o`; also an unqualified outer column qualified with an alias the subquery declares).
pub(crate) fn semi_renamed(s: &Semi, taken: &HashSet<String>) -> Semi {
    let mut used: HashSet<String> = taken.clone();
    used.extend(s.tables.iter().map(|t| t.alias.to_lowercase()));
    let mut out = s.clone();
    let mut renames: HashMap<String, String> = HashMap::new();
    for t in &mut out.tables {
        if !taken.contains(&t.alias.to_lowercase()) {
            continue;
        }
        let mut n = 1;
        let fresh = loop {
            let a = if t.alias.chars().count() <= 58 {
                format!("{}__m{n}", t.alias)
            } else {
                // (`_lv_` names are reserved for lazyview: no user alias collides.)
                format!("_lv_m{n}")
            };
            if !used.contains(&a.to_lowercase()) {
                break a;
            }
            n += 1;
        };
        used.insert(fresh.to_lowercase());
        renames.insert(t.alias.to_lowercase(), fresh.clone());
        t.alias = fresh;
    }
    if renames.is_empty() {
        return out;
    }
    rename_refs(&mut out.predicate, &renames);
    if let Expr::InSubquery { subquery, .. } | Expr::Exists { subquery, .. } = &mut out.predicate {
        if let SetExpr::Select(sel) = subquery.body.as_mut() {
            for twj in &mut sel.from {
                for f in std::iter::once(&mut twj.relation)
                    .chain(twj.joins.iter_mut().map(|j| &mut j.relation))
                {
                    if let Some(new) = factor_name(f).and_then(|n| renames.get(&n.to_lowercase())) {
                        let new = new.clone();
                        set_factor_alias(f, &new);
                    }
                }
            }
        }
    }
    for c in out
        .join_conditions
        .iter_mut()
        .chain(out.lookup_conditions.iter_mut())
    {
        rename_refs(c, &renames);
    }
    out
}

/// Whether `key_col = e` compares like the key, so that distinct key values never equal the
/// same value of `e` (G1): `e` is a column of the same key type (see [`same_key_type`]), or an
/// integer expression against an integer key.
fn key_exact(key_col: &Column, r: &Resolver, e: &Expr) -> bool {
    match r.column(e) {
        Some(c) => same_key_type(key_col, c),
        None => key_col.family() == TypeFamily::Integer && integer_valued(r, e),
    }
}

/// Whether an equality of columns `a` and `b` is injective on either one as a key: the same type
/// family (for strings the same charset and collation: no number against a string, no
/// `COLLATE`) and no TIMESTAMP (G4: under a time zone with DST, two instants of the fall-back
/// hour equal one local time).
fn same_key_type(a: &Column, b: &Column) -> bool {
    a.family() == b.family() && a.data_type != "timestamp" && b.data_type != "timestamp"
}

/// Whether `e` is an integer: integer columns and literals under `+ - * DIV %`.
fn integer_valued(r: &Resolver, e: &Expr) -> bool {
    match e {
        Expr::Nested(x) => integer_valued(r, x),
        Expr::Value(v) => matches!(&v.value, Value::Number(n, _) if n.parse::<i64>().is_ok()),
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => integer_valued(r, expr),
        Expr::BinaryOp {
            left,
            op:
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::MyIntegerDivide
                | BinaryOperator::Modulo,
            right,
        } => integer_valued(r, left) && integer_valued(r, right),
        _ => r
            .column(e)
            .is_some_and(|c| c.family() == TypeFamily::Integer),
    }
}

/// Whether the semi-join is over one table and its correlation covers a whole key of it (the
/// primary key or a UNIQUE NOT NULL key) with key-exact equalities: each outer row matches at
/// most one row of the table, the semi-join is an inner join.
fn unique(b: &Block, s: &Semi) -> bool {
    let [t] = s.tables.as_slice() else {
        return false;
    };
    let Some(args) = b
        .select
        .selection
        .as_ref()
        .and_then(|w| marker_args(w, s.id))
    else {
        return false;
    };
    let r = Resolver::new(b);
    !s.negated
        && t.info.keys().any(|key| {
            key.iter().all(|&c| {
                s.correlation.iter().any(|&(_, sc, k)| {
                    sc == c
                        && args
                            .get(k)
                            .is_some_and(|a| key_exact(&t.info.columns[c], &r, a))
                })
            })
        })
}

fn semis_to_joins(b: &mut Block) -> Result<()> {
    let joinable: HashSet<usize> = b
        .semis
        .iter()
        .filter(|s| unique(b, s))
        .map(|s| s.id)
        .collect();
    if joinable.is_empty() {
        return Ok(());
    }
    qualify(b)?;
    let mut taken: HashSet<String> = b.items.iter().map(|i| i.alias.to_lowercase()).collect();
    let semis = std::mem::take(&mut b.semis);
    let mut conds_all = Vec::new();
    for s in semis {
        if !joinable.contains(&s.id) {
            b.semis.push(s);
            continue;
        }
        let args = b
            .select
            .selection
            .as_ref()
            .and_then(|w| marker_args(w, s.id))
            .ok_or_else(|| anyhow!("internal: semi-join marker missing"))?;
        let t = s.tables.into_iter().next().unwrap();
        let mut alias = t.alias.clone();
        if taken.contains(&alias.to_lowercase()) {
            let mut n = 2;
            while taken.contains(&format!("{}_{n}", t.alias).to_lowercase()) {
                n += 1;
            }
            alias = format!("{}_{n}", t.alias);
        }
        taken.insert(alias.to_lowercase());
        let renames: HashMap<String, String> = [(t.alias.to_lowercase(), alias.clone())]
            .into_iter()
            .collect();
        for mut c in s.join_conditions {
            rename_refs(&mut c, &renames);
            fill_placeholders(&mut c, &args);
            conds_all.push(c);
        }
        // Drop the marker conjunct.
        let name = marker_name(s.id);
        let kept: Vec<Expr> = b
            .select
            .selection
            .take()
            .iter()
            .flat_map(conjuncts)
            .filter(|c| !matches!(c, Expr::Function(f) if f.name.to_string() == name))
            .collect();
        b.select.selection = and_all(kept);
        let name = ObjectName(vec![
            ObjectNamePart::Identifier(qident(&t.table.0)),
            ObjectNamePart::Identifier(qident(&t.table.1)),
        ]);
        b.select.from.push(TableWithJoins {
            relation: table_factor(name, Some(&alias)),
            joins: vec![],
        });
        b.items.push(Item {
            alias,
            pos: (b.select.from.len() - 1, None),
            origin: Origin::Table,
            source: Source::Table(t.table, t.info),
        });
    }
    let mut all: Vec<Expr> = b
        .select
        .selection
        .take()
        .iter()
        .flat_map(conjuncts)
        .collect();
    all.extend(conds_all);
    b.select.selection = and_all(all);
    b.rewritten = true;
    Ok(())
}
