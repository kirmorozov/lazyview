//! Lowering: a normalized flat block becomes a synthetic definition that the block planner
//! (`analyze::plan`) turns into the executor's [`ViewPlan`]; semi-join aliases are attached
//! afterwards. Full plans keep the statement exactly as written.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use sqlparser::ast::{Expr, SelectItem};
use sqlparser::tokenizer::Token;

use super::ast::{and_all, join_constraint, qident};
use super::ir::{Block, Semi};
use super::names::{finish_view_names, ident_name};
use super::normalize::{
    fill_placeholders, marker_args, marker_name, not_in_as_not_exists, query_of, semi_renamed,
};
use super::source::tokenize;
use crate::analyze::{self, AliasPlan, Mode, ParsedDef, ViewPlan};
use crate::catalog::{TableInfo, TableRef};

/// Why a flat block was not planned incrementally as rewritten.
pub(crate) enum Refused {
    Reason(String),
}

impl Refused {
    pub(crate) fn reason(self) -> String {
        match self {
            Refused::Reason(r) => r,
        }
    }
}

/// Plans a flat block. `Ok(Err(_))`: it cannot be maintained incrementally as rewritten (the
/// caller falls back to a Full plan of the original statement, or isolates a semi-join).
pub(crate) fn plan_block(
    def: &ParsedDef,
    b: &Block,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<std::result::Result<ViewPlan, Refused>> {
    if b.outputs.is_empty() {
        return Ok(Err(Refused::Reason(
            "internal analyzer error: block without output columns".into(),
        )));
    }
    let mut names: Vec<_> = b.outputs.iter().map(|o| o.name.clone()).collect();
    for i in finish_view_names(&mut names) {
        if b.outputs[i].original.is_none() {
            return Ok(Err(Refused::Reason(format!(
                "unaliased column `{}` needs an alias (its server-generated name differs between versions)",
                b.outputs[i].name.text
            ))));
        }
    }
    let mut select = b.select.clone();
    let mut item_sql = Vec::new();
    select.projection = b
        .outputs
        .iter()
        .zip(&names)
        .map(|(o, n)| match &o.original {
            Some((item, text)) => {
                item_sql.push(text.clone());
                item.clone()
            }
            None => {
                item_sql.push(None);
                SelectItem::ExprWithAlias {
                    expr: o.expr.clone(),
                    alias: qident(&n.text),
                }
            }
        })
        .collect();
    let mut query = query_of(select);
    if let Some(tail) = &b.tail {
        query.order_by = tail.order_by.clone();
        query.limit_clause = tail.limit_clause.clone();
        query.fetch = tail.fetch.clone();
    }
    let flat = ParsedDef {
        schema: def.schema.clone(),
        name: def.name.clone(),
        columns: def.columns.clone(),
        query,
        hash: def.hash.clone(),
        query_sql: def.query_sql.clone(),
        item_sql,
    };
    let mut plan = match analyze::plan_block(&flat, schema, tables) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(view = %def.name, "analyzer: the merged form does not plan: {e:#}");
            return Ok(Err(Refused::Reason(format!(
                "internal analyzer error: {e}"
            ))));
        }
    };
    // A Full plan watches what the statement as written reads (semi-join subqueries included).
    if let Mode::Full(r) = &plan.mode {
        return Ok(Err(Refused::Reason(r.clone())));
    }
    if let Err(why) = attach_semis(&mut plan, b, tables) {
        return Ok(Err(why));
    }
    Ok(Ok(plan))
}

/// The semi-joins left in the block: their predicates replace the markers in the plan's WHERE,
/// and their tables become aliases whose changes map straight to the view's units (or, for a
/// single-row aggregate, mark it for recomputation). The lookup template of a table of a
/// subquery over several tables names the others by [`analyze::semi_table_marker`]: a lookup
/// that substitutes several of them evaluates one subquery over all their rows.
fn attach_semis(
    plan: &mut ViewPlan,
    b: &Block,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> std::result::Result<(), Refused> {
    if b.semis.is_empty() {
        return Ok(());
    }
    let missing = || Refused::Reason("internal analyzer error: semi-join marker missing".into());
    // A semi-join of a derived table merged onto the NULL side of a LEFT JOIN sits in its ON.
    let in_on = |id: usize| {
        b.select.from.iter().flat_map(|t| &t.joins).any(|j| {
            match join_constraint(&j.join_operator) {
                Some(sqlparser::ast::JoinConstraint::On(e)) => marker_args(e, id).is_some(),
                _ => false,
            }
        })
    };
    if let Some(s) = b.semis.iter().find(|s| in_on(s.id)) {
        return Err(Refused::Reason(format!(
            "{} in a derived table or CTE on the NULL side of a LEFT JOIN",
            semi_kind(s)
        )));
    }
    let Some(w) = &b.select.selection else {
        return Err(missing());
    };
    let mut where_sql = plan.where_sql.clone().unwrap_or_default();
    // Lookups of the subquery tables evaluate their predicates on the substituted rows.
    let mut lookup_where = plan
        .lookup_where_sql
        .clone()
        .unwrap_or_else(|| where_sql.clone());
    // The subquery's own aliases must not capture the outer expressions filled in (F3).
    let taken: HashSet<String> = b.items.iter().map(|i| i.alias.to_lowercase()).collect();
    for s in &b.semis {
        let s = &semi_renamed(s, &taken);
        let args = marker_args(w, s.id).ok_or_else(missing)?;
        // (The marker is a conjunct of the WHERE: a filter context, where NOT IN has an exact NOT
        // EXISTS form.)
        let mut pred = if s.not_in {
            not_in_as_not_exists(&s.predicate).ok_or_else(missing)?
        } else {
            s.predicate.clone()
        };
        fill_placeholders(&mut pred, &args);
        where_sql = replace_call(&where_sql, &marker_name(s.id), &format!("({pred})"))
            .ok_or_else(missing)?;
        lookup_where =
            replace_call(&lookup_where, &marker_name(s.id), "TRUE").ok_or_else(missing)?;
        if matches!(plan.mode, Mode::Full(_)) {
            continue;
        }
        let mut conds = s.lookup_conditions.clone();
        conds.iter_mut().for_each(|c| fill_placeholders(c, &args));
        let conds = and_all(conds)
            .map(|c| format!(" WHERE {c}"))
            .unwrap_or_default();
        // The subquery's tables become the aliases `first..`.
        let first = plan.aliases.len();
        for (ti, t) in s.tables.iter().enumerate() {
            let from: Vec<String> = s
                .tables
                .iter()
                .enumerate()
                .map(|(tj, u)| {
                    let rows = if tj == ti {
                        analyze::SUBST_MARKER.to_string()
                    } else {
                        analyze::semi_table_marker(first + tj)
                    };
                    format!("{rows} AS {}", qident(&u.alias))
                })
                .collect();
            let exists = format!("EXISTS (SELECT 1 FROM {}{conds})", from.join(", "));
            // Direct units where the correlation equals the unit, else forward lookups.
            let direct = match &plan.mode {
                Mode::Spj | Mode::Grouped if !s.not_in => direct_columns(plan, s, ti, &args).ok(),
                _ => None,
            };
            // A child of a cascading foreign key changes without binlog rows (C3): its parents'
            // deletes / key updates route to the units their image gives (the subquery is
            // correlated on the unit through the FK columns); otherwise the view is Full.
            if !t.info.cascades.is_empty() {
                let child = (t.info.schema.clone(), t.info.name.clone());
                for mut r in analyze::cascade_routes(&[(t.info.clone(), direct.clone())], tables) {
                    // (A cascade that continues out of the child reaches rows no image gives.)
                    let continues = r.child != child
                        || plan
                            .cascades
                            .iter()
                            .any(|c| (c.parent.schema.clone(), c.parent.name.clone()) == child);
                    if continues {
                        r.unit_from_parent = None;
                    }
                    if r.unit_from_parent.is_none() {
                        return Err(Refused::Reason(format!(
                            "{} over `{}`: the units a cascade from `{}` changes are not known",
                            semi_kind(s),
                            t.info.name,
                            r.parent.name
                        )));
                    }
                    plan.cascades.push(r);
                }
            }
            let image_cols = (0..t.referenced.len())
                .filter(|&c| t.referenced[c])
                .collect();
            plan.aliases.push(AliasPlan {
                name: t.alias.clone(),
                table: t.info.clone(),
                referenced: t.referenced.clone(),
                membership: t.referenced.clone(),
                direct_unit: direct,
                image_cols,
                // Not in FROM: lookups use `semi`.
                subst_from: (String::new(), String::new()),
                pos: (usize::MAX, None),
                semi: Some(exists),
                lookup_where: true,
            });
            if !plan.tables.contains(&t.table) {
                plan.tables.push(t.table.clone());
                plan.table_columns.push(None);
            }
        }
    }
    plan.where_sql = Some(where_sql);
    plan.lookup_where_sql = Some(lookup_where);
    Ok(())
}

fn semi_kind(s: &Semi) -> &'static str {
    match (&s.predicate, s.negated) {
        (Expr::InSubquery { .. }, _) => "IN subquery",
        (_, true) => "NOT EXISTS subquery",
        _ => "EXISTS subquery",
    }
}

/// For each unit expression, the column of the subquery's table `ti` equal to it: a correlation
/// `t.k = outer` whose outer side is a column that equals that unit on every row of its alias.
fn direct_columns(
    plan: &ViewPlan,
    s: &Semi,
    ti: usize,
    args: &[Expr],
) -> std::result::Result<Vec<usize>, String> {
    let resolve = |mut e: &Expr| -> Option<(usize, usize)> {
        while let Expr::Nested(inner) = e {
            e = inner;
        }
        let (tbl, col) = match e {
            Expr::Identifier(id) => (None, ident_name(id)),
            Expr::CompoundIdentifier(p) if p.len() >= 2 => (
                Some(ident_name(&p[p.len() - 2])),
                ident_name(&p[p.len() - 1]),
            ),
            _ => return None,
        };
        plan.aliases.iter().enumerate().find_map(|(a, ap)| {
            if tbl
                .as_ref()
                .is_some_and(|t| !ap.name.eq_ignore_ascii_case(t))
            {
                return None;
            }
            ap.table.col_index(&col).map(|c| (a, c))
        })
    };
    let info = &s.tables[ti].info;
    (0..plan.unit_exprs.len())
        .map(|u| {
            s.correlation.iter().find_map(|&(t, sc, k)| {
                if t != ti {
                    return None;
                }
                let (a, c) = resolve(args.get(k)?)?;
                let ap = &plan.aliases[a];
                let tc = &ap.table.columns[c];
                let sc_col = &info.columns[sc];
                (ap.direct_unit.as_ref()?.get(u) == Some(&c)
                    && tc.exact_key()
                    && sc_col.exact_key()
                    && tc.family() == sc_col.family())
                .then_some(sc)
            })
        })
        .collect::<Option<Vec<usize>>>()
        .ok_or_else(|| {
            format!(
                "{} over `{}` is not correlated on the view's unit key",
                semi_kind(s),
                info.name
            )
        })
}

/// Replaces the call `name(…)` in `sql` (token-aware) by `replacement`.
fn replace_call(sql: &str, name: &str, replacement: &str) -> Option<String> {
    let tokens = tokenize(sql)?;
    let chars: Vec<(usize, char)> = sql.char_indices().collect();
    // Character index at which each line starts (a string literal may contain newlines, C1).
    let mut line_starts = vec![0usize];
    for (i, (_, c)) in chars.iter().enumerate() {
        if *c == '\n' {
            line_starts.push(i + 1);
        }
    }
    let offset = |loc: sqlparser::tokenizer::Location| -> Option<usize> {
        let line = line_starts.get(loc.line.checked_sub(1)? as usize)?;
        let at = line + loc.column.checked_sub(1)? as usize;
        Some(chars.get(at).map(|(i, _)| *i).unwrap_or(sql.len()))
    };
    let start = tokens
        .iter()
        .position(|t| matches!(&t.token, Token::Word(w) if w.value == name))?;
    let mut depth = 0usize;
    for t in &tokens[start + 1..] {
        match t.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    let a = offset(tokens[start].span.start)?;
                    let z = offset(t.span.end)?;
                    return Some(format!("{}{replacement}{}", &sql[..a], &sql[z..]));
                }
            }
            Token::Whitespace(_) => {}
            _ if depth == 0 => return None,
            _ => {}
        }
    }
    None
}

/// A Full plan for the statement as written, with `reason` and the tables resolved with CTE
/// scoping.
pub(crate) fn full_plan(
    def: &ParsedDef,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
    reason: &str,
) -> Result<ViewPlan> {
    let mut p = analyze::plan_block(def, schema, tables)?;
    if matches!(&p.mode, Mode::Full(r) if r.starts_with("reads SQL view")) {
        return Ok(p);
    }
    p.mode = Mode::Full(reason.to_string());
    let scoped = super::referenced_tables(&def.query, schema);
    if p.tables != scoped {
        p.table_columns = vec![None; scoped.len()];
        p.tables = scoped;
    }
    Ok(p)
}
