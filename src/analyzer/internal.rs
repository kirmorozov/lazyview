//! Internal views (`_lv_int_<hash16>`): subtrees the analyzer isolates into views of their own.
//! The engine materializes them on the source like any view (incrementally), and the parent reads
//! their tables as a chained view. Two kinds:
//!
//! * an aggregated / DISTINCT CTE or derived table joined to other tables on anything but their
//!   key (otherwise it is pulled up, see `normalize::pull_up`): the internal view is its block;
//! * the **tuple-lineage view** `J` of a grouped block whose units cannot be traced through the
//!   join (two or more lookup-resolved tables, or a group key on the NULL side of a LEFT JOIN):
//!   `J` materializes the join (every column the grouped block uses, conditions on NULL sides
//!   left to the parent) with lineage per table, and the grouped view aggregates `J`, whose rows
//!   carry the group key: every change of `J` maps straight to its groups.
//!
//! Names hash the schema and the view's SQL, so identical subtrees of different definitions
//! share one internal view.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, Expr, GroupByExpr, ObjectName, ObjectNamePart,
    SelectItem, TableWithJoins,
};

use super::ast::{and_all, conjuncts, kind_at, qcol, qident, table_factor, JoinKind};
use super::build::{Res, Resolver};
use super::ir::{Block, Item, Origin, Rel, Source};
use super::names::finish_view_names;
use super::normalize::{aggregated, qualify, query_of};
use crate::analyze::{Mode, ViewPlan};
use crate::catalog::{TableInfo, TableRef};

/// An internal view the plan of a definition needs.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalView {
    /// `_lv_int_<hash16>`: also the name of its table in the lazyview schema.
    pub name: String,
    /// Its definition (`CREATE VIEW <name> AS SELECT …`), planned like any definition.
    pub sql: String,
    /// Why it exists, for `_lazyview_overview` (e.g. ``GROUP BY in CTE `t` ``).
    pub purpose: String,
    /// Columns forming a unique key of its rows (GROUP BY keys), if any.
    pub key: Vec<String>,
}

fn name_for(schema: &str, select_sql: &str) -> String {
    let h = hex::encode(Sha256::digest(format!("{schema}\0{select_sql}").as_bytes()));
    format!("_lv_int_{}", &h[..16])
}

fn view(schema: &str, select_sql: String, purpose: String, key: Vec<String>) -> InternalView {
    let name = name_for(schema, &select_sql);
    InternalView {
        sql: format!("CREATE VIEW {} AS {select_sql}", qident(&name)),
        name,
        purpose,
        key,
    }
}

/// The table factor `schema`.`name` AS `alias`.
fn table_ref_factor(t: &TableRef, alias: &str) -> sqlparser::ast::TableFactor {
    table_factor(
        ObjectName(vec![
            ObjectNamePart::Identifier(qident(&t.0)),
            ObjectNamePart::Identifier(qident(&t.1)),
        ]),
        Some(alias),
    )
}

/// The SELECT of a qualified block with every output named explicitly; `Err` when a name
/// would differ between servers or is not a valid column name.
fn render(b: &Block) -> Result<String, String> {
    let mut names: Vec<_> = b.outputs.iter().map(|o| o.name.clone()).collect();
    let bad = finish_view_names(&mut names);
    if let Some(&i) = bad.first() {
        return Err(format!(
            "unaliased column `{}` needs an alias",
            b.outputs[i].name.text
        ));
    }
    if names
        .iter()
        .zip(&b.outputs)
        .any(|(n, o)| n.text != o.name.text)
    {
        return Err("a column name longer than 64 characters needs an alias".into());
    }
    let mut s = b.select.clone();
    s.projection = b
        .outputs
        .iter()
        .map(|o| SelectItem::ExprWithAlias {
            expr: o.expr.clone(),
            alias: qident(&o.name.text),
        })
        .collect();
    let mut q = query_of(s);
    if let Some(tail) = &b.tail {
        // ORDER BY items naming an output become its position (outputs are renamed above).
        let mut order_by = tail.order_by.clone();
        if let Some(sqlparser::ast::OrderByKind::Expressions(es)) =
            order_by.as_mut().map(|o| &mut o.kind)
        {
            for e in es.iter_mut() {
                let pos = b.outputs.iter().position(|o| {
                    o.expr == e.expr
                        || matches!(&e.expr, Expr::Identifier(id)
                            if o.name.text.eq_ignore_ascii_case(&super::names::ident_name(id)))
                });
                if let Some(p) = pos {
                    e.expr = Expr::value(sqlparser::ast::Value::Number((p + 1).to_string(), false));
                }
            }
        }
        q.order_by = order_by;
        q.limit_clause = tail.limit_clause.clone();
        q.fetch = tail.fetch.clone();
    }
    Ok(q.to_string())
}

/// Whether an unmerged derived item of `b` can be an internal view: an aggregate, window or
/// top-N block over tables, joined inner or LEFT.
pub(crate) fn isolatable(b: &Block, it: &Item) -> bool {
    let Source::Derived(Rel::Block(inner)) = &it.source else {
        return false;
    };
    inner
        .items
        .iter()
        .all(|it| matches!(it.source, Source::Table(..)))
        && super::normalize::blocker(inner).is_some_and(|w| {
            matches!(
                w.as_str(),
                "GROUP BY"
                    | "HAVING"
                    | "aggregate function"
                    | "DISTINCT"
                    | "window function"
                    | "ORDER BY … LIMIT"
            )
        })
        && inner.semis.is_empty()
        && (!inner.limit || inner.tail.is_some())
        && matches!(
            kind_at(&b.select.from, it.pos),
            JoinKind::Head | JoinKind::Inner | JoinKind::Left
        )
}

/// Replaces the unmerged aggregated derived items of `b` by their internal views' tables, where
/// those exist in `tables`. Returns the internal views still missing, or the reason an item
/// cannot be isolated.
pub(crate) fn isolate(
    b: &mut Block,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<Vec<InternalView>, String> {
    let mut missing = Vec::new();
    for i in 0..b.items.len() {
        let Source::Derived(rel) = &b.items[i].source else {
            continue;
        };
        let Rel::Block(inner) = rel else {
            return Err(super::normalize::unmerged_reason(b, &b.items[i]));
        };
        if !isolatable(b, &b.items[i]) {
            return Err(super::normalize::unmerged_reason(b, &b.items[i]));
        }
        let mut inner = (**inner).clone();
        qualify(&mut inner).map_err(|e| e.to_string())?;
        let sql = render(&inner)?;
        let keys: HashSet<String> = match &inner.select.group_by {
            GroupByExpr::Expressions(es, _) => es.iter().map(|e| e.to_string()).collect(),
            _ => HashSet::new(),
        };
        let key: Vec<String> = inner
            .outputs
            .iter()
            .filter(|o| keys.contains(&o.expr.to_string()))
            .map(|o| o.name.text.clone())
            .collect();
        let what = b.items[i].origin.describe(&b.items[i].alias);
        let why = super::normalize::blocker(&inner).unwrap_or_default();
        let iv = view(schema, sql, format!("{why} in {what}"), key);
        let tref = (schema.to_string(), iv.name.clone());
        match tables.get(&tref) {
            Some(info) => {
                let mut info = (**info).clone();
                // The key is unique even where the table could not get a PRIMARY KEY (nullable
                // group columns): only the anchor's key is used, as its unit.
                if info.pk.is_empty() && b.items[i].pos == (0, None) {
                    info.pk = iv.key.iter().filter_map(|k| info.col_index(k)).collect();
                }
                let alias = b.items[i].alias.clone();
                let f = table_ref_factor(&tref, &alias);
                match b.items[i].pos {
                    (t, None) => b.select.from[t].relation = f,
                    (t, Some(j)) => b.select.from[t].joins[j].relation = f,
                }
                b.items[i].source = Source::Table(tref, Arc::new(info));
                b.items[i].origin = Origin::Table;
                b.rewritten = true;
            }
            None => missing.push(iv),
        }
    }
    Ok(missing)
}

/// Whether a grouped plan needs the tuple-lineage view (see the module comment).
pub(crate) fn needs_tuple_view(plan: &ViewPlan) -> bool {
    match &plan.mode {
        Mode::Grouped => {
            plan.aliases
                .iter()
                .filter(|a| a.direct_unit.is_none())
                .count()
                >= 2
        }
        Mode::Full(r) => r == super::NULL_SIDE_KEY,
        _ => false,
    }
}

/// The tuple-lineage view `J` of a flat grouped block, and the block rewritten to aggregate it:
/// `None` when it does not apply (not a grouped block over tables, anchor without key).
pub(crate) fn tuple_view(
    b: &Block,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Option<(InternalView, Option<Block>)> {
    if !aggregated(b)
        || b.items.is_empty()
        || !b
            .items
            .iter()
            .all(|i| matches!(i.source, Source::Table(..)))
        || !b.semis.is_empty()
        || b.items[0].table().is_none_or(|t| t.pk.is_empty())
    {
        return None;
    }
    let mut b = b.clone();
    qualify(&mut b).ok()?;
    let r = Resolver::new(&b);
    let null_side: Vec<bool> = b
        .items
        .iter()
        .map(|it| kind_at(&b.select.from, it.pos) == JoinKind::Left)
        .collect();
    let refs = |e: &Expr, out: &mut Vec<(usize, usize)>| {
        let _ = visit_expressions(e, |x: &Expr| {
            if let Res::Col(i, c) = r.resolve(x) {
                if !out.contains(&(i, c)) {
                    out.push((i, c));
                }
            }
            ControlFlow::<()>::Continue(())
        });
    };
    // WHERE conjuncts over preserved tables only stay in J; the others are evaluated on J's rows.
    let (mut j_where, mut v_where) = (Vec::new(), Vec::new());
    for c in b.select.selection.iter().flat_map(conjuncts) {
        let mut used = Vec::new();
        refs(&c, &mut used);
        if used.iter().any(|(i, _)| null_side[*i]) {
            v_where.push(c);
        } else {
            j_where.push(c);
        }
    }
    let mut cols: Vec<(usize, usize)> = Vec::new();
    for o in &b.outputs {
        refs(&o.expr, &mut cols);
    }
    if let GroupByExpr::Expressions(es, _) = &b.select.group_by {
        for e in es {
            refs(e, &mut cols);
        }
    }
    for e in b.select.having.iter().chain(v_where.iter()) {
        refs(e, &mut cols);
    }
    if cols.is_empty() {
        return None;
    }
    let col_name = |k: usize| format!("_lv_c{k}");
    let mut js = b.select.clone();
    js.projection = cols
        .iter()
        .enumerate()
        .map(|(k, (i, c))| SelectItem::ExprWithAlias {
            expr: qcol(
                &b.items[*i].alias,
                &qident(&b.items[*i].table().unwrap().columns[*c].name),
            ),
            alias: qident(&col_name(k)),
        })
        .collect();
    js.selection = and_all(j_where);
    js.group_by = GroupByExpr::Expressions(vec![], vec![]);
    js.having = None;
    js.distinct = None;
    let j_sql = query_of(js).to_string();
    let iv = view(
        schema,
        j_sql,
        "tuple lineage of a grouped view".into(),
        Vec::new(),
    );
    let tref = (schema.to_string(), iv.name.clone());
    let Some(info) = tables.get(&tref) else {
        return Some((iv, None));
    };
    // The grouped block over J: every column reference becomes J's column.
    let alias = "_lv_j";
    let mut v = b.clone();
    let to_j = |e: &mut Expr| {
        let _ = visit_expressions_mut(e, |x: &mut Expr| {
            if let Res::Col(i, c) = r.resolve(x) {
                if let Some(k) = cols.iter().position(|p| *p == (i, c)) {
                    *x = qcol(alias, &qident(&col_name(k)));
                }
            }
            ControlFlow::<()>::Continue(())
        });
    };
    for o in &mut v.outputs {
        to_j(&mut o.expr);
        o.original = None;
    }
    if let GroupByExpr::Expressions(es, _) = &mut v.select.group_by {
        es.iter_mut().for_each(&to_j);
    }
    if let Some(h) = &mut v.select.having {
        to_j(h);
    }
    let mut vw = v_where;
    vw.iter_mut().for_each(&to_j);
    v.select.selection = and_all(vw);
    v.select.from = vec![TableWithJoins {
        relation: table_ref_factor(&tref, alias),
        joins: vec![],
    }];
    v.items = vec![Item {
        alias: alias.into(),
        pos: (0, None),
        origin: Origin::Table,
        source: Source::Table(tref, info.clone()),
    }];
    v.rewritten = true;
    Some((iv, Some(v)))
}

/// Replaces semi-join `id` of `b` (`x IN (SELECT k FROM s WHERE p)`, `EXISTS (… s.k = x AND p)`)
/// by an inner join with the internal view of its distinct keys, `SELECT k FROM s WHERE p GROUP
/// BY k`, when that table exists: its rows are unique on `k`, so the semi-join is a join. Returns
/// the internal view while its table is missing; `Err` when the semi-join does not have that
/// shape (negated, or conditions on the outer query besides the key equalities).
pub(crate) fn semi_view(
    b: &mut Block,
    id: usize,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<Option<InternalView>, ()> {
    let k = b.semis.iter().position(|s| s.id == id).ok_or(())?;
    let s = b.semis[k].clone();
    // (Over several tables the distinct keys come from a join: not this shape.)
    let ([t], false) = (s.tables.as_slice(), s.negated) else {
        return Err(());
    };
    let placeholder = |e: &Expr| super::normalize::placeholder_of(e);
    // Key equalities `s.col = <outer>`, and the subquery's own conditions.
    let (mut keys, mut local): (Vec<(usize, usize)>, Vec<Expr>) = (Vec::new(), Vec::new());
    for c in &s.join_conditions {
        let pair = match c {
            Expr::BinaryOp {
                left,
                op: sqlparser::ast::BinaryOperator::Eq,
                right,
            } => [(left, right), (right, left)]
                .into_iter()
                .find_map(|(a, z)| {
                    let col = match &**a {
                        Expr::CompoundIdentifier(p) if p.len() == 2 => {
                            t.info.col_index(&super::names::ident_name(&p[1]))
                        }
                        _ => None,
                    }?;
                    Some((col, placeholder(z)?))
                }),
            _ => None,
        };
        match pair {
            Some(p) => keys.push(p),
            None if super::normalize::has_placeholder(c) => return Err(()),
            None => local.push(c.clone()),
        }
    }
    let mut cols: Vec<usize> = keys.iter().map(|(c, _)| *c).collect();
    cols.sort_unstable();
    cols.dedup();
    if cols.is_empty() {
        return Err(());
    }
    let col = |c: usize| qcol(&t.alias, &qident(&t.info.columns[c].name));
    let sel = format!(
        "SELECT {} FROM {}.{} AS {}{} GROUP BY {}",
        cols.iter()
            .map(|&c| format!("{} AS {}", col(c), qident(&t.info.columns[c].name)))
            .collect::<Vec<_>>()
            .join(", "),
        qident(&t.table.0),
        qident(&t.table.1),
        qident(&t.alias),
        and_all(local)
            .map(|w| format!(" WHERE {w}"))
            .unwrap_or_default(),
        cols.iter()
            .map(|&c| col(c).to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let key: Vec<String> = cols
        .iter()
        .map(|&c| t.info.columns[c].name.clone())
        .collect();
    let iv = view(
        schema,
        sel,
        format!("distinct keys of a subquery over `{}`", t.info.name),
        key,
    );
    let tref = (schema.to_string(), iv.name.clone());
    let Some(info) = tables.get(&tref) else {
        return Ok(Some(iv));
    };
    qualify(b).map_err(|_| ())?;
    let args = b
        .select
        .selection
        .as_ref()
        .and_then(|w| super::normalize::marker_args(w, id))
        .ok_or(())?;
    let mut alias = format!("{}_keys", t.alias);
    while b.items.iter().any(|i| i.alias.eq_ignore_ascii_case(&alias)) {
        alias.push('_');
    }
    let marker = super::normalize::marker_name(id);
    let mut conds: Vec<Expr> = b
        .select
        .selection
        .take()
        .iter()
        .flat_map(conjuncts)
        .filter(|c| !matches!(c, Expr::Function(f) if f.name.to_string() == marker))
        .collect();
    for (c, ph) in keys {
        conds.push(Expr::BinaryOp {
            left: Box::new(qcol(&alias, &qident(&t.info.columns[c].name))),
            op: sqlparser::ast::BinaryOperator::Eq,
            right: Box::new(super::ast::nested(args.get(ph).ok_or(())?.clone())),
        });
    }
    b.select.selection = and_all(conds);
    b.select.from.push(TableWithJoins {
        relation: table_ref_factor(&tref, &alias),
        joins: vec![],
    });
    b.items.push(Item {
        alias,
        pos: (b.select.from.len() - 1, None),
        origin: Origin::Table,
        source: Source::Table(tref, info.clone()),
    });
    b.semis.remove(k);
    b.rewritten = true;
    Ok(None)
}
