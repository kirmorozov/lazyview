//! AST → IR: FROM items resolved through lexical scopes (tables, CTE references, derived tables),
//! projection names, column resolution. Every block is normalized right after it is built, so a
//! parent always sees normalized children (bottom-up).

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use sqlparser::ast::{
    Distinct, Expr, JoinConstraint, ObjectNamePart, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier, TableFactor,
};

use super::ast::{factors, join_constraint, object_table_ref, qident};
use super::ir::{Block, Item, Origin, Output, Pos, Rel, Source};
use super::names::{ident_name, item_name, Name, NameKind};
use super::normalize;
use super::scope::Scope;
use super::source::Text;
use crate::catalog::{TableInfo, TableRef};

pub(crate) struct Builder<'a> {
    pub schema: &'a str,
    pub tables: &'a HashMap<TableRef, Arc<TableInfo>>,
    pub text: &'a Text<'a>,
    /// Numbers semi-join markers uniquely across the statement.
    pub next_semi: Cell<usize>,
    /// Nesting depth of the relation being built (0: the view's own query).
    pub depth: Cell<usize>,
}

impl<'a> Builder<'a> {
    pub(crate) fn query<'q>(&self, q: &'q Query, scope: &Scope<'q>) -> Result<Rel> {
        let scope = match &q.with {
            Some(w) => scope.with_clause(w),
            None => scope.clone(),
        };
        let limit = q.limit_clause.is_some() || q.fetch.is_some();
        let ordered = q.order_by.is_some();
        match q.body.as_ref() {
            SetExpr::Select(s) => {
                // ORDER BY … LIMIT: kept with the block (only the clauses after the body are
                // used), so the normalizer rewrites it with the block (merged FROM items).
                let tail = limit.then(|| {
                    let mut tail = q.clone();
                    tail.with = None;
                    Box::new(tail)
                });
                self.select(s, ordered, limit, tail, &scope)
            }
            SetExpr::Query(inner) => Ok(match self.query(inner, &scope)? {
                Rel::Block(mut b) => {
                    b.limit |= limit;
                    b.ordered |= ordered;
                    Rel::Block(b)
                }
                other => other,
            }),
            SetExpr::SetOperation {
                op, set_quantifier, ..
            } => Ok(Rel::Opaque {
                why: set_operation(op, set_quantifier),
            }),
            SetExpr::Values(_) => Ok(Rel::Opaque {
                why: "VALUES".into(),
            }),
            _ => Ok(Rel::Opaque {
                why: "unsupported query".into(),
            }),
        }
    }

    /// Builds one branch of a set operation at the top of the statement.
    pub(crate) fn branch<'q>(&self, s: &'q Select, scope: &Scope<'q>) -> Result<Rel> {
        self.select(s, false, false, None, scope)
    }

    /// Builds a relation nested in the current one (a derived table, CTE body or subquery).
    pub(crate) fn nested<'q>(&self, q: &'q Query, scope: &Scope<'q>) -> Result<Rel> {
        self.depth.set(self.depth.get() + 1);
        let r = self.query(q, scope);
        self.depth.set(self.depth.get() - 1);
        r
    }

    fn select<'q>(
        &self,
        s: &'q Select,
        ordered: bool,
        limit: bool,
        tail: Option<Box<Query>>,
        scope: &Scope<'q>,
    ) -> Result<Rel> {
        let mut items = Vec::new();
        for (pos, f) in factors(&s.from) {
            match self.item(f, pos, scope)? {
                Ok(item) => items.push(item),
                Err(why) => return Ok(Rel::Opaque { why }),
            }
        }
        // A derived relation that is opaque makes this block opaque too (its columns are unknown).
        if let Some(why) = items.iter().find_map(|i| match &i.source {
            Source::Derived(Rel::Opaque { why }) => Some(why.clone()),
            _ => None,
        }) {
            return Ok(Rel::Opaque { why });
        }
        // `*` over USING / NATURAL joins coalesces columns (the server lists the shared
        // columns once, first): its columns are not the per-table expansion, so such a block
        // is not planned (A4: an empty or duplicated projection made the view fail).
        if star_with_using(s) {
            return Ok(Rel::Opaque {
                why: crate::analyze::STAR_USING.into(),
            });
        }
        let texts = self.text.items(s);
        let outputs = outputs(s, &items, &texts)?;
        let mut select = s.clone();
        if matches!(select.distinct, Some(Distinct::All)) {
            select.distinct = None;
        }
        let mut b = Block {
            rewritten: select.distinct != s.distinct,
            select,
            items,
            outputs,
            ordered,
            limit,
            semis: Vec::new(),
            semi_blockers: Vec::new(),
            qualified: false,
            distinct_was: false,
            tail,
        };
        normalize::block(self, &mut b, scope)?;
        Ok(Rel::Block(Box::new(b)))
    }

    /// Resolves one FROM factor. `Ok(Err(reason))`: a FROM item the analyzer cannot look into.
    fn item<'q>(
        &self,
        f: &'q TableFactor,
        pos: Pos,
        scope: &Scope<'q>,
    ) -> Result<std::result::Result<Item, String>> {
        match f {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                if let Some(cte) = scope.cte_for(name) {
                    let alias = alias
                        .as_ref()
                        .map(|a| ident_name(&a.name))
                        .unwrap_or_else(|| cte.name.clone());
                    if cte.recursive {
                        return Ok(Err(format!("recursive CTE `{}`", cte.name)));
                    }
                    let what = format!("CTE `{}`", cte.name);
                    let rel =
                        rename_columns(self.nested(cte.query, &cte.body)?, &cte.columns, &what)?;
                    let rel = match rel {
                        Rel::Opaque { why } => Rel::Opaque {
                            why: format!("{why} in {what}"),
                        },
                        b => b,
                    };
                    return Ok(Ok(Item {
                        alias,
                        pos,
                        origin: Origin::Cte(cte.name.clone()),
                        source: Source::Derived(rel),
                    }));
                }
                let t = object_table_ref(name, self.schema)
                    .ok_or_else(|| anyhow!("unsupported table name {name}"))?;
                let info = self
                    .tables
                    .get(&t)
                    .ok_or_else(|| anyhow!("table `{}`.`{}` does not exist", t.0, t.1))?
                    .clone();
                let alias = alias
                    .as_ref()
                    .map(|a| ident_name(&a.name))
                    .unwrap_or_else(|| t.1.clone());
                Ok(Ok(Item {
                    alias,
                    pos,
                    origin: Origin::Table,
                    source: Source::Table(t, info),
                }))
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
                ..
            } => {
                let name = alias
                    .as_ref()
                    .map(|a| ident_name(&a.name))
                    .ok_or_else(|| anyhow!("every derived table must have its own alias"))?;
                if *lateral {
                    return Ok(Err(format!("LATERAL derived table `{name}`")));
                }
                let what = format!("derived table `{name}`");
                let cols: Vec<String> = alias
                    .iter()
                    .flat_map(|a| a.columns.iter().map(|c| ident_name(&c.name)))
                    .collect();
                let rel = match rename_columns(self.nested(subquery, scope)?, &cols, &what)? {
                    Rel::Opaque { why } => Rel::Opaque {
                        why: format!("{why} in {what}"),
                    },
                    b => b,
                };
                Ok(Ok(Item {
                    alias: name,
                    pos,
                    origin: Origin::Derived,
                    source: Source::Derived(rel),
                }))
            }
            // (Chains of inner joins in parentheses are flattened before building: what is
            // left holds an outer join or hangs on one.)
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => Ok(Err(format!(
                "parenthesized join ({table_with_joins}) that cannot be flattened into the FROM chain (needs internal views)"
            ))),
            _ => Ok(Err("unsupported FROM item".into())),
        }
    }
}

/// `*` together with USING / NATURAL joins (their columns are coalesced in `*`).
pub(crate) fn star_with_using(s: &Select) -> bool {
    s.projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)))
        && s.from.iter().flat_map(|t| &t.joins).any(|j| {
            matches!(
                join_constraint(&j.join_operator),
                Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
            )
        })
}

fn set_operation(op: &SetOperator, q: &SetQuantifier) -> String {
    let all = matches!(q, SetQuantifier::All | SetQuantifier::AllByName);
    match (op, all) {
        (SetOperator::Union, true) => "UNION ALL".into(),
        (SetOperator::Union, false) => "UNION".into(),
        (SetOperator::Intersect, _) => "INTERSECT".into(),
        (SetOperator::Except | SetOperator::Minus, _) => "EXCEPT".into(),
    }
}

/// Applies a column list (`WITH t (a, b)`, `(…) AS d (a, b)`) to a relation's outputs.
fn rename_columns(rel: Rel, cols: &[String], what: &str) -> Result<Rel> {
    if cols.is_empty() {
        return Ok(rel);
    }
    match rel {
        Rel::Block(mut b) => {
            if b.outputs.len() != cols.len() {
                bail!(
                    "{what} has a column list of {} names but its SELECT returns {} columns",
                    cols.len(),
                    b.outputs.len()
                );
            }
            for (o, c) in b.outputs.iter_mut().zip(cols) {
                o.name = Name::new(c.clone(), NameKind::Alias);
            }
            Ok(Rel::Block(b))
        }
        other => Ok(other),
    }
}

/// The projection with wildcards expanded (in FROM order, visible columns), and names.
fn outputs(s: &Select, items: &[Item], texts: &[Option<String>]) -> Result<Vec<Output>> {
    let mut out = Vec::new();
    let expand = |item: &Item, out: &mut Vec<Output>| match &item.source {
        Source::Table(_, t) => {
            for c in t.columns.iter().filter(|c| !c.invisible) {
                out.push(Output {
                    name: Name::new(c.name.clone(), NameKind::Column),
                    expr: Expr::CompoundIdentifier(vec![qident(&item.alias), qident(&c.name)]),
                    original: None,
                });
            }
        }
        Source::Derived(Rel::Block(b)) => {
            for o in &b.outputs {
                out.push(Output {
                    name: o.name.clone(),
                    expr: Expr::CompoundIdentifier(vec![qident(&item.alias), qident(&o.name.text)]),
                    original: None,
                });
            }
        }
        Source::Derived(Rel::Opaque { .. }) => {}
    };
    for (i, item) in s.projection.iter().enumerate() {
        let text = texts.get(i).cloned().flatten();
        match item {
            SelectItem::UnnamedExpr(e) => out.push(Output {
                name: item_name(e, None, text.as_deref()),
                expr: e.clone(),
                original: Some((item.clone(), text)),
            }),
            SelectItem::ExprWithAlias { expr, alias } => out.push(Output {
                name: item_name(expr, Some(alias), None),
                expr: expr.clone(),
                original: Some((item.clone(), None)),
            }),
            SelectItem::Wildcard(_) => items.iter().for_each(|it| expand(it, &mut out)),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(on), _) => {
                let q = match on.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => ident_name(i),
                    _ => bail!("unsupported wildcard {on}.*"),
                };
                let it = items
                    .iter()
                    .find(|it| it.alias.eq_ignore_ascii_case(&q))
                    .ok_or_else(|| anyhow!("unknown table {q}.*"))?;
                expand(it, &mut out);
            }
            SelectItem::QualifiedWildcard(..) => bail!("unsupported wildcard"),
            _ => bail!("unsupported projection item {item}"),
        }
    }
    Ok(out)
}

/// Column resolution in one block (the rules of the block planner, plus ambiguity checks for
/// derived columns): `col` → the first item having it (several only through `USING`), `t.col`
/// → item `t`, `db.t.col` → table item `t` of schema `db`.
pub(crate) struct Resolver {
    cols: Vec<Option<Vec<String>>>,
    aliases: Vec<String>,
    schemas: Vec<Option<String>>,
    /// Items joined with `USING (…)`: their coalesced column names.
    using: Vec<Vec<String>>,
    derived: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Res {
    Col(usize, usize),
    Unknown,
    Ambiguous(String),
}

impl Resolver {
    pub(crate) fn new(b: &Block) -> Resolver {
        let using = b
            .items
            .iter()
            .map(|it| match it.pos {
                (t, Some(j)) => match join_constraint(&b.select.from[t].joins[j].join_operator) {
                    Some(JoinConstraint::Using(cols)) => cols
                        .iter()
                        .filter_map(|c| match c.0.last() {
                            Some(ObjectNamePart::Identifier(i)) => Some(ident_name(i)),
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            })
            .collect();
        Resolver {
            cols: b.items.iter().map(Item::columns).collect(),
            aliases: b.items.iter().map(|i| i.alias.clone()).collect(),
            schemas: b
                .items
                .iter()
                .map(|i| match &i.source {
                    Source::Table(t, _) => Some(t.0.clone()),
                    _ => None,
                })
                .collect(),
            using,
            derived: b
                .items
                .iter()
                .map(|i| matches!(i.source, Source::Derived(_)))
                .collect(),
        }
    }

    fn col_of(&self, item: usize, name: &str) -> Option<usize> {
        self.cols[item]
            .as_ref()?
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
    }

    pub(crate) fn resolve(&self, e: &Expr) -> Res {
        match e {
            Expr::Identifier(id) => {
                let name = ident_name(id);
                let hits: Vec<(usize, usize)> = (0..self.cols.len())
                    .filter_map(|i| self.col_of(i, &name).map(|c| (i, c)))
                    .collect();
                match hits.as_slice() {
                    [] => Res::Unknown,
                    [one] => Res::Col(one.0, one.1),
                    [first, rest @ ..] => {
                        let coalesced = rest.iter().all(|(i, _)| {
                            self.using[*i].iter().any(|u| u.eq_ignore_ascii_case(&name))
                        });
                        if coalesced || hits.iter().all(|(i, _)| !self.derived[*i]) {
                            // Base tables only: the block planner's rule (first match).
                            Res::Col(first.0, first.1)
                        } else {
                            Res::Ambiguous(name)
                        }
                    }
                }
            }
            Expr::CompoundIdentifier(parts) if (2..=3).contains(&parts.len()) => {
                let col = ident_name(&parts[parts.len() - 1]);
                let tbl = ident_name(&parts[parts.len() - 2]);
                let schema = (parts.len() == 3).then(|| ident_name(&parts[0]));
                let item = (0..self.cols.len()).find(|&i| {
                    self.aliases[i].eq_ignore_ascii_case(&tbl)
                        && match &schema {
                            Some(s) => self.schemas[i]
                                .as_ref()
                                .is_some_and(|x| x.eq_ignore_ascii_case(s)),
                            None => true,
                        }
                });
                match item.and_then(|i| self.col_of(i, &col).map(|c| (i, c))) {
                    Some((i, c)) => Res::Col(i, c),
                    None => Res::Unknown,
                }
            }
            Expr::Nested(inner) => self.resolve(inner),
            _ => Res::Unknown,
        }
    }
}
