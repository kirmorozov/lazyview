//! Small sqlparser AST helpers shared by the analyzer passes.

use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions, BinaryOperator, Expr, GroupByExpr, Ident, Join, JoinConstraint,
    JoinOperator, NamedWindowExpr, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    TableAlias, TableFactor, TableWithJoins, Visit, Visitor,
};

use super::ir::Pos;
use super::names::ident_name;
use crate::catalog::TableRef;

/// A backtick-quoted identifier.
pub(crate) fn qident(id: &str) -> Ident {
    Ident::with_quote('`', id.replace('`', "``"))
}

/// `` `alias`.col `` (the column identifier kept as written).
pub(crate) fn qcol(alias: &str, col: &Ident) -> Expr {
    Expr::CompoundIdentifier(vec![qident(alias), col.clone()])
}

/// Wraps `e` in parentheses unless it is atomic.
pub(crate) fn nested(e: Expr) -> Expr {
    match e {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::Nested(_)
        | Expr::Function(_) => e,
        other => Expr::Nested(Box::new(other)),
    }
}

/// Top-level AND conjuncts of `e` (parentheses unwrapped).
pub(crate) fn conjuncts(e: &Expr) -> Vec<Expr> {
    fn walk(e: &Expr, out: &mut Vec<Expr>) {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                walk(left, out);
                walk(right, out);
            }
            Expr::Nested(inner)
                if matches!(
                    **inner,
                    Expr::BinaryOp {
                        op: BinaryOperator::And,
                        ..
                    }
                ) =>
            {
                walk(inner, out)
            }
            _ => out.push(e.clone()),
        }
    }
    let mut out = Vec::new();
    walk(e, &mut out);
    out
}

/// `a AND b AND …` (each operand parenthesized unless atomic); None when empty.
pub(crate) fn and_all(parts: Vec<Expr>) -> Option<Expr> {
    parts.into_iter().map(nested).reduce(|a, b| Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::And,
        right: Box::new(b),
    })
}

pub(crate) fn object_table_ref(name: &ObjectName, default_schema: &str) -> Option<TableRef> {
    let idents: Vec<String> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(ident_name(i)),
            _ => None,
        })
        .collect::<Option<_>>()?;
    match idents.as_slice() {
        [t] => Some((default_schema.to_string(), t.clone())),
        [s, t] => Some((s.clone(), t.clone())),
        _ => None,
    }
}

/// The single-part name of a table factor (`FROM t`), which may be a CTE reference.
pub(crate) fn single_name(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(i)] => Some(ident_name(i)),
        _ => None,
    }
}

/// A plain table factor `name AS alias`.
pub(crate) fn table_factor(name: ObjectName, alias: Option<&str>) -> TableFactor {
    TableFactor::Table {
        name,
        alias: alias.map(|a| TableAlias {
            explicit: true,
            name: qident(a),
            columns: vec![],
            at: None,
        }),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub(crate) fn factor_alias(f: &TableFactor) -> Option<&TableAlias> {
    match f {
        TableFactor::Table { alias, .. }
        | TableFactor::Derived { alias, .. }
        | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
        _ => None,
    }
}

pub(crate) fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::AsOf { constraint: c, .. } => Some(c),
        _ => None,
    }
}

pub(crate) fn join_constraint_mut(op: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::AsOf { constraint: c, .. } => Some(c),
        _ => None,
    }
}

/// How a FROM factor is joined to what precedes it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum JoinKind {
    /// First factor of a FROM list entry (also comma joins).
    Head,
    Inner,
    Left,
    Other,
}

pub(crate) fn join_kind(j: &Join) -> JoinKind {
    match &j.join_operator {
        JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c)
            if !matches!(c, JoinConstraint::Natural) =>
        {
            JoinKind::Inner
        }
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c)
            if !matches!(c, JoinConstraint::Natural) =>
        {
            JoinKind::Left
        }
        _ => JoinKind::Other,
    }
}

pub(crate) fn kind_at(from: &[TableWithJoins], pos: Pos) -> JoinKind {
    match pos {
        (_, None) => JoinKind::Head,
        (t, Some(j)) => join_kind(&from[t].joins[j]),
    }
}

/// Every FROM factor with its position, in order.
pub(crate) fn factors(from: &[TableWithJoins]) -> Vec<(Pos, &TableFactor)> {
    let mut out = Vec::new();
    for (t, twj) in from.iter().enumerate() {
        out.push(((t, None), &twj.relation));
        for (j, join) in twj.joins.iter().enumerate() {
            out.push(((t, Some(j)), &join.relation));
        }
    }
    out
}

/// Applies `f` to every expression root of the SELECT outside its FROM factors: projection,
/// join conditions, WHERE, GROUP BY, HAVING, named windows.
pub(crate) fn for_each_expr_mut(s: &mut Select, mut f: impl FnMut(&mut Expr)) {
    for item in &mut s.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => f(e),
            _ => {}
        }
    }
    for twj in &mut s.from {
        for j in &mut twj.joins {
            if let Some(JoinConstraint::On(e)) = join_constraint_mut(&mut j.join_operator) {
                f(e);
            }
        }
    }
    if let Some(e) = &mut s.selection {
        f(e);
    }
    if let GroupByExpr::Expressions(es, _) = &mut s.group_by {
        es.iter_mut().for_each(&mut f);
    }
    if let Some(e) = &mut s.having {
        f(e);
    }
    for w in &mut s.named_window {
        if let NamedWindowExpr::WindowSpec(spec) = &mut w.1 {
            spec.partition_by.iter_mut().for_each(&mut f);
            for o in &mut spec.order_by {
                f(&mut o.expr);
            }
        }
    }
}

/// The expression roots of [`for_each_expr_mut`], read-only.
pub(crate) fn exprs_of(s: &Select) -> Vec<&Expr> {
    let mut out = Vec::new();
    for item in &s.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => out.push(e),
            _ => {}
        }
    }
    for twj in &s.from {
        for j in &twj.joins {
            if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                out.push(e);
            }
        }
    }
    out.extend(s.selection.iter());
    if let GroupByExpr::Expressions(es, _) = &s.group_by {
        out.extend(es.iter());
    }
    out.extend(s.having.iter());
    for w in &s.named_window {
        if let NamedWindowExpr::WindowSpec(spec) = &w.1 {
            out.extend(spec.partition_by.iter());
            out.extend(spec.order_by.iter().map(|o| &o.expr));
        }
    }
    out
}

/// Whether the expression contains a subquery.
pub(crate) fn has_subquery(e: &Expr) -> bool {
    struct V(bool);
    impl Visitor for V {
        type Break = ();
        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.0 = true;
            ControlFlow::Break(())
        }
    }
    let mut v = V(false);
    let _ = e.visit(&mut v);
    v.0
}

/// The outermost subqueries of an expression (cloned).
pub(crate) fn subqueries(e: &Expr) -> Vec<Query> {
    struct V {
        depth: usize,
        out: Vec<Query>,
    }
    impl Visitor for V {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            if self.depth == 0 {
                self.out.push(q.clone());
            }
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
    }
    let mut v = V {
        depth: 0,
        out: Vec::new(),
    };
    let _ = e.visit(&mut v);
    v.out
}

/// Whether any column reference occurs in `e`.
pub(crate) fn has_column_ref(e: &Expr) -> bool {
    let mut found = false;
    let _ = visit_expressions(e, |x: &Expr| {
        if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

/// Upper-case name of a function call.
pub(crate) fn function_name(f: &sqlparser::ast::Function) -> String {
    match f.name.0.last() {
        Some(ObjectNamePart::Identifier(i)) => ident_name(i).to_ascii_uppercase(),
        _ => String::new(),
    }
}
