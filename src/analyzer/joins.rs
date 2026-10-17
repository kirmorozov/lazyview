//! Join syntax normalized before anything else (in every SELECT of the statement):
//!
//! * `STRAIGHT_JOIN` is an inner join with a fixed join order: `JOIN`;
//! * a leading `a RIGHT JOIN b ON c` is `b LEFT JOIN a ON c` (the preserved side first, so it
//!   becomes the anchor);
//! * `a NATURAL [LEFT|RIGHT] JOIN b` over two tables is a join `USING` their common columns;
//! * parenthesized inner joins `(a JOIN b ON p) JOIN c ON q` are flattened (inner joins
//!   associate; the parenthesized join's own condition moves to WHERE when it cannot stay put).

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use sqlparser::ast::{
    Expr, Join, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Query, Select,
    TableFactor, TableWithJoins, VisitMut, VisitorMut,
};

use super::ast::{and_all, conjuncts, join_constraint, object_table_ref, single_name};
use crate::catalog::{TableInfo, TableRef};

/// Rewrites the joins of every SELECT in `q`; whether anything changed.
pub(crate) fn normalize(
    q: &mut Query,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> bool {
    struct V<'a> {
        schema: &'a str,
        tables: &'a HashMap<TableRef, Arc<TableInfo>>,
        changed: bool,
    }
    impl VisitorMut for V<'_> {
        type Break = ();
        fn pre_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
            self.changed |= select(s, self.schema, self.tables);
            ControlFlow::Continue(())
        }
    }
    let mut v = V {
        schema,
        tables,
        changed: false,
    };
    let _ = q.visit(&mut v);
    v.changed
}

/// Whether a statement has join syntax [`normalize`] rewrites.
pub(crate) fn has_rewritable(s: &Select) -> bool {
    s.from.iter().any(|t| {
        matches!(t.relation, TableFactor::NestedJoin { alias: None, .. })
            || t.joins.iter().any(|j| {
                matches!(j.relation, TableFactor::NestedJoin { alias: None, .. })
                    || matches!(
                        j.join_operator,
                        JoinOperator::StraightJoin(_)
                            | JoinOperator::Right(_)
                            | JoinOperator::RightOuter(_)
                    )
                    || matches!(
                        join_constraint(&j.join_operator),
                        Some(JoinConstraint::Natural)
                    )
            })
    })
}

fn select(s: &mut Select, schema: &str, tables: &HashMap<TableRef, Arc<TableInfo>>) -> bool {
    let mut changed = false;
    let mut extra_where = Vec::new();
    let from = std::mem::take(&mut s.from);
    for twj in from {
        let mut twj = flatten(twj, &mut extra_where, &mut changed);
        for j in &mut twj.joins {
            if let JoinOperator::StraightJoin(c) = &j.join_operator {
                j.join_operator = JoinOperator::Join(c.clone());
                changed = true;
            }
        }
        if !twj.joins.is_empty() && natural_to_using(&mut twj, schema, tables) {
            changed = true;
        }
        if let Some(first) = twj.joins.first() {
            if matches!(
                first.join_operator,
                JoinOperator::Right(_) | JoinOperator::RightOuter(_)
            ) {
                let first = twj.joins.remove(0);
                let c = match first.join_operator {
                    JoinOperator::Right(c) | JoinOperator::RightOuter(c) => c,
                    _ => unreachable!(),
                };
                let a = std::mem::replace(&mut twj.relation, first.relation);
                twj.joins.insert(
                    0,
                    Join {
                        relation: a,
                        global: false,
                        join_operator: JoinOperator::Left(c),
                    },
                );
                changed = true;
            }
        }
        s.from.push(twj);
    }
    if !extra_where.is_empty() {
        let mut all: Vec<Expr> = s.selection.take().iter().flat_map(conjuncts).collect();
        all.extend(extra_where);
        s.selection = and_all(all);
    }
    changed
}

/// Splices parenthesized inner joins (without alias) into the chain.
fn flatten(twj: TableWithJoins, extra_where: &mut Vec<Expr>, changed: &mut bool) -> TableWithJoins {
    let inner_only = |t: &TableWithJoins| {
        t.joins.iter().all(|j| {
            matches!(
                j.join_operator,
                JoinOperator::Join(JoinConstraint::On(_) | JoinConstraint::None)
                    | JoinOperator::Inner(JoinConstraint::On(_) | JoinConstraint::None)
                    | JoinOperator::CrossJoin(JoinConstraint::None)
            )
        })
    };
    let mut out = match twj.relation {
        TableFactor::NestedJoin {
            table_with_joins,
            alias: None,
        } if inner_only(&table_with_joins) => {
            *changed = true;
            flatten(*table_with_joins, extra_where, changed)
        }
        relation => TableWithJoins {
            relation,
            joins: Vec::new(),
        },
    };
    for j in twj.joins {
        let inner_join = matches!(
            j.join_operator,
            JoinOperator::Join(JoinConstraint::On(_) | JoinConstraint::None)
                | JoinOperator::Inner(JoinConstraint::On(_) | JoinConstraint::None)
                | JoinOperator::CrossJoin(JoinConstraint::None)
        );
        match j.relation {
            TableFactor::NestedJoin {
                table_with_joins,
                alias: None,
            } if inner_join && inner_only(&table_with_joins) => {
                *changed = true;
                let inner = flatten(*table_with_joins, extra_where, changed);
                if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                    extra_where.push(e.clone());
                }
                out.joins.push(Join {
                    relation: inner.relation,
                    global: false,
                    join_operator: JoinOperator::Join(JoinConstraint::None),
                });
                out.joins.extend(inner.joins);
            }
            relation => out.joins.push(Join {
                relation,
                global: j.global,
                join_operator: j.join_operator,
            }),
        }
    }
    out
}

/// `a NATURAL JOIN b` (first join, two tables) → `a JOIN b USING (common columns)`.
fn natural_to_using(
    twj: &mut TableWithJoins,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> bool {
    let first = &twj.joins[0];
    if !matches!(
        join_constraint(&first.join_operator),
        Some(JoinConstraint::Natural)
    ) {
        return false;
    }
    let table_of = |f: &TableFactor| match f {
        TableFactor::Table {
            name, args: None, ..
        } if single_name(name).is_some() || name.0.len() == 2 => {
            object_table_ref(name, schema).and_then(|t| tables.get(&t).cloned())
        }
        _ => None,
    };
    let (Some(a), Some(b)) = (table_of(&twj.relation), table_of(&first.relation)) else {
        return false;
    };
    let common: Vec<ObjectName> = a
        .columns
        .iter()
        .filter(|c| {
            !c.invisible
                && b.col_index(&c.name)
                    .is_some_and(|i| !b.columns[i].invisible)
        })
        .map(|c| {
            ObjectName(vec![ObjectNamePart::Identifier(super::ast::qident(
                &c.name,
            ))])
        })
        .collect();
    let using = if common.is_empty() {
        JoinConstraint::None
    } else {
        JoinConstraint::Using(common)
    };
    let op = match &first.join_operator {
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => JoinOperator::Left(using),
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => JoinOperator::Right(using),
        _ => JoinOperator::Join(using),
    };
    twj.joins[0].join_operator = op;
    true
}
