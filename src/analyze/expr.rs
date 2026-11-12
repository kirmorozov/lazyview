use super::*;
use super::{parse::*, plan::*, routes::*, vocab::*};

pub(crate) use crate::analyzer::{function_name, has_subquery};

/// Column reference resolved against the FROM clause.
pub(crate) type ColRef = (usize, usize);

pub(crate) struct Scope<'a> {
    pub(crate) aliases: &'a [(String, Arc<TableInfo>)],
}

impl Scope<'_> {
    pub(crate) fn resolve(&self, e: &Expr) -> Option<ColRef> {
        match e {
            Expr::Identifier(id) => self.find_unqualified(&ident_name(id)),
            Expr::CompoundIdentifier(parts) => {
                let col = ident_name(parts.last()?);
                let tbl = ident_name(parts.get(parts.len().checked_sub(2)?)?);
                let schema = if parts.len() >= 3 {
                    Some(ident_name(&parts[parts.len() - 3]))
                } else {
                    None
                };
                let a = self.aliases.iter().position(|(n, t)| {
                    n.eq_ignore_ascii_case(&tbl)
                        && schema
                            .as_ref()
                            .map(|s| t.schema.eq_ignore_ascii_case(s))
                            .unwrap_or(true)
                })?;
                Some((a, self.aliases[a].1.col_index(&col)?))
            }
            Expr::Nested(inner) => self.resolve(inner),
            _ => None,
        }
    }

    pub(crate) fn same_family(&self, a: ColRef, b: ColRef) -> bool {
        self.aliases[a.0].1.columns[a.1].family() == self.aliases[b.0].1.columns[b.1].family()
    }

    /// `alias.column` of a column reference, for messages.
    pub(crate) fn name(&self, c: ColRef) -> String {
        let (n, t) = &self.aliases[c.0];
        format!("{n}.{}", t.columns[c.1].name)
    }

    fn find_unqualified(&self, col: &str) -> Option<ColRef> {
        self.aliases
            .iter()
            .enumerate()
            .find_map(|(i, (_, t))| t.col_index(col).map(|c| (i, c)))
    }
}

pub(crate) struct Usage<'s> {
    pub(crate) scope: &'s Scope<'s>,
    pub(crate) select_aliases: Vec<String>,
    pub(crate) referenced: Vec<Vec<bool>>,
    pub(crate) membership: Vec<Vec<bool>>,
    pub(crate) unresolved: Option<String>,
}

impl Usage<'_> {
    /// Marks the columns of the FROM clause that `root` names, leaving other names alone.
    pub(crate) fn mark_known(&mut self, root: &Expr) {
        let unresolved = self.unresolved.take();
        self.mark(root, false, true);
        self.unresolved = unresolved;
    }

    pub(crate) fn mark<V: Visit>(
        &mut self,
        root: &V,
        set_membership: bool,
        allow_select_alias: bool,
    ) {
        let _ = visit_expressions(root, |e: &Expr| {
            if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                match self.scope.resolve(e) {
                    Some((a, c)) => {
                        self.referenced[a][c] = true;
                        if set_membership {
                            self.membership[a][c] = true;
                        }
                        // A generated column changes with its base columns (a VIRTUAL one
                        // has no image of its own): they count as read too.
                        let t = &self.scope.aliases[a].1;
                        if let Some(g) = &t.columns[c].generated {
                            let deps: Vec<usize> = match &g.deps {
                                Some(d) => d.clone(),
                                None => (0..t.columns.len()).collect(),
                            };
                            for d in deps {
                                self.referenced[a][d] = true;
                                if set_membership {
                                    self.membership[a][d] = true;
                                }
                            }
                        }
                    }
                    None => {
                        let is_alias = matches!(e, Expr::Identifier(id)
                            if self.select_aliases.iter().any(|a| a.eq_ignore_ascii_case(&ident_name(id))));
                        if !(allow_select_alias && is_alias) {
                            self.unresolved.get_or_insert_with(|| e.to_string());
                        }
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
}

/// The tables the subqueries of an ORDER BY read, each with the columns those subqueries may
/// read: every column they name (whatever the qualifier), all of them under `*` or a USING /
/// NATURAL join, and the base columns of the generated ones.
pub(crate) fn order_subquery_tables(
    o: &OrderBy,
    schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<(Arc<TableInfo>, Vec<bool>)> {
    struct Names {
        names: BTreeSet<String>,
        all: bool,
    }
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            fn set(s: &SetExpr, all: &mut bool) {
                match s {
                    SetExpr::Select(sel) => {
                        *all |= sel.projection.iter().any(|p| {
                            matches!(
                                p,
                                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                            )
                        }) || sel.from.iter().flat_map(|t| &t.joins).any(|j| {
                            matches!(
                                join_constraint(&j.join_operator),
                                Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
                            )
                        });
                    }
                    SetExpr::SetOperation { left, right, .. } => {
                        set(left, all);
                        set(right, all);
                    }
                    SetExpr::Query(q) => set(&q.body, all),
                    _ => {}
                }
            }
            set(&q.body, &mut self.all);
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            let id = match e {
                Expr::Identifier(id) => Some(id),
                Expr::CompoundIdentifier(parts) => parts.last(),
                _ => None,
            };
            if let Some(id) = id {
                self.names.insert(ident_name(id).to_lowercase());
            }
            ControlFlow::Continue(())
        }
    }
    let mut out: Vec<(Arc<TableInfo>, Vec<bool>)> = Vec::new();
    let OrderByKind::Expressions(es) = &o.kind else {
        return out;
    };
    for e in es {
        let _ = visit_expressions(&e.expr, |x: &Expr| {
            let q = match x {
                Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => q,
                Expr::InSubquery { subquery, .. } => subquery,
                _ => return ControlFlow::<()>::Continue(()),
            };
            let mut n = Names {
                names: BTreeSet::new(),
                all: false,
            };
            let _ = q.visit(&mut n);
            for t in referenced_tables(q, schema) {
                // Caller contract (`plan_block`: `tables` holds metadata for every table the
                // query reads): a gap is a bug, not a silently skipped table.
                let info = &tables[&t];
                let mut cols: Vec<bool> = info
                    .columns
                    .iter()
                    .map(|c| n.all || n.names.contains(&c.name.to_lowercase()))
                    .collect();
                for c in 0..cols.len() {
                    if let (true, Some(g)) = (cols[c], &info.columns[c].generated) {
                        match &g.deps {
                            Some(d) => d.iter().for_each(|&d| cols[d] = true),
                            None => cols.iter_mut().for_each(|x| *x = true),
                        }
                    }
                }
                match out.iter_mut().find(|(i, _)| Arc::ptr_eq(i, info)) {
                    Some((_, r)) => r.iter_mut().zip(&cols).for_each(|(a, b)| *a |= *b),
                    None => out.push((info.clone(), cols)),
                }
            }
            ControlFlow::Continue(())
        });
    }
    out
}

/// Union-find over (alias, column).
pub(crate) struct Classes {
    parent: HashMap<ColRef, ColRef>,
}

impl Classes {
    pub(crate) fn new() -> Self {
        Classes {
            parent: HashMap::new(),
        }
    }
    pub(crate) fn find(&mut self, x: ColRef) -> ColRef {
        let p = *self.parent.get(&x).unwrap_or(&x);
        if p == x {
            return x;
        }
        let r = self.find(p);
        self.parent.insert(x, r);
        r
    }
    pub(crate) fn union(&mut self, a: ColRef, b: ColRef) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }
}

pub(crate) fn conjuncts<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(inner) => conjuncts(inner, out),
        _ => out.push(e),
    }
}

/// The column equalities of a conjunct. A one-element `x IN (y)` is `x = y`.
pub(crate) fn equalities(e: &Expr, scope: &Scope) -> Vec<(ColRef, ColRef)> {
    let mut cs = Vec::new();
    conjuncts(e, &mut cs);
    cs.into_iter()
        .filter_map(|c| match c {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => Some((scope.resolve(left)?, scope.resolve(right)?)),
            Expr::InList {
                expr,
                list,
                negated: false,
            } if list.len() == 1 => Some((scope.resolve(expr)?, scope.resolve(&list[0])?)),
            _ => None,
        })
        .filter(|(a, b)| scope.same_family(*a, *b))
        .collect()
}

fn function_args(f: &sqlparser::ast::Function) -> Option<Vec<&Expr>> {
    match &f.args {
        FunctionArguments::None => Some(Vec::new()),
        FunctionArguments::Subquery(_) => None,
        FunctionArguments::List(l) => l
            .args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                }
                | FunctionArg::ExprNamed {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Some(e),
                FunctionArg::Unnamed(_) => Some(&NULL_EXPR),
                _ => None,
            })
            .collect(),
    }
}

static NULL_EXPR: Expr = Expr::Value(sqlparser::ast::ValueWithSpan {
    value: Value::Null,
    span: sqlparser::tokenizer::Span::empty(),
});

/// The expression is NULL whenever a column it references is NULL (conservative): columns,
/// literals, strict operators and the strict builtins of `NULL_STRICT` over such expressions.
/// `||` is not among the operators: the servers read it as OR in the default sql_mode (the
/// definition is refused at parse time); `<=>`, AND / OR / XOR, CASE, IS NULL, IN and every
/// unlisted function count as able to yield a value for NULL.
pub(crate) fn null_propagating(e: &Expr) -> bool {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
        Expr::Nested(x) => null_propagating(x),
        Expr::UnaryOp { op, expr } => {
            matches!(
                op,
                UnaryOperator::Minus | UnaryOperator::Plus | UnaryOperator::Not
            ) && null_propagating(expr)
        }
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
                    | BinaryOperator::MyIntegerDivide
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::BitwiseAnd
                    | BinaryOperator::BitwiseOr
                    | BinaryOperator::BitwiseXor
                    | BinaryOperator::PGBitwiseShiftLeft
                    | BinaryOperator::PGBitwiseShiftRight
                    | BinaryOperator::Arrow
                    | BinaryOperator::LongArrow
            ) && null_propagating(left)
                && null_propagating(right)
        }
        Expr::Cast { expr, .. } | Expr::Convert { expr, .. } | Expr::Collate { expr, .. } => {
            null_propagating(expr)
        }
        Expr::Extract { expr, .. } => null_propagating(expr),
        Expr::Function(f) => {
            f.over.is_none()
                && NULL_STRICT.contains(&function_name(f).as_str())
                && function_args(f)
                    .map(|a| a.into_iter().all(null_propagating))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// The predicate is never TRUE when a column it references is NULL (so it removes the
/// NULL-extended rows of a LEFT JOIN instead of accepting them).
pub(crate) fn null_rejecting(e: &Expr) -> bool {
    match e {
        Expr::Nested(x) => null_rejecting(x),
        Expr::BinaryOp {
            left,
            op:
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq,
            right,
        } => null_propagating(left) && null_propagating(right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => null_rejecting(left) || null_rejecting(right),
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            null_propagating(expr) && null_propagating(pattern)
        }
        Expr::InList { expr, list, .. } => {
            null_propagating(expr) && list.iter().all(null_propagating)
        }
        Expr::Between {
            expr, low, high, ..
        } => null_propagating(expr) && null_propagating(low) && null_propagating(high),
        Expr::IsNotNull(x) => null_propagating(x),
        _ => false,
    }
}

/// Constant filters of each FROM alias: the conjuncts of the WHERE clause and of inner joins'
/// ON that compare one column of it with constants ([`const_filter`]), those of its own LEFT
/// JOIN condition, and the same values for every column equal to a filtered one on the rows the
/// alias is part of (`base_edges` and its own `own_edges`, as for direct units). Each conjunct is
/// NULL-rejecting and reads that alias alone, so a row failing it is never the alias's row in a
/// row of the query: under an inner join's ON or the WHERE every joined row holding it is
/// dropped, as is the NULL-extended row it could displace (in the flat FROM an inner join's ON
/// filters like the WHERE, wherever it is written); under its own LEFT JOIN condition it matches
/// nothing, and the outer row is NULL-extended as it would be without it. Another LEFT JOIN's
/// condition is not this alias's (a LEFT JOIN keeps its preserved rows whatever it says). A
/// column equal to a filtered one holds a value only where the filtered column is not NULL, so
/// where that alias's row is real and passes its filters.
pub(crate) fn const_filters(
    select: &Select,
    joins: &[(usize, bool, JoinConstraint)],
    scope: &Scope,
    base_edges: &[(ColRef, ColRef)],
    own_edges: &[Vec<(ColRef, ColRef)>],
) -> Vec<Vec<ConstFilter>> {
    let mut out = vec![Vec::new(); scope.aliases.len()];
    let ons = joins.iter().filter_map(|(a, left, c)| match c {
        JoinConstraint::On(e) => Some((left.then_some(*a), e)),
        _ => None,
    });
    for (only, e) in ons.chain(select.selection.iter().map(|w| (None, w))) {
        let mut cs = Vec::new();
        conjuncts(e, &mut cs);
        for ((a, col), values) in cs.into_iter().filter_map(|c| const_filter(c, scope)) {
            let f = ConstFilter { col, values };
            if only.is_none_or(|o| o == a) && !out[a].contains(&f) {
                out[a].push(f);
            }
        }
    }
    let found = out.clone();
    for (ai, (_, t)) in scope.aliases.iter().enumerate() {
        let mut cls = Classes::new();
        for (x, y) in base_edges.iter().chain(&own_edges[ai]) {
            cls.union(*x, *y);
        }
        for c in (0..t.columns.len()).filter(|&c| filter_column(&t.columns[c])) {
            let rc = cls.find((ai, c));
            for (x, fs) in found.iter().enumerate() {
                for f in fs {
                    let g = ConstFilter {
                        col: c,
                        values: f.values.clone(),
                    };
                    if (x, f.col) != (ai, c) && cls.find((x, f.col)) == rc && !out[ai].contains(&g)
                    {
                        out[ai].push(g);
                    }
                }
            }
        }
    }
    out
}

/// Whether a column's binlog values compare exactly with integer literals (see [`const_filter`]).
pub(crate) fn filter_column(column: &crate::catalog::Column) -> bool {
    column.family() == TypeFamily::Integer
        && matches!(column.kind, ColKind::Plain | ColKind::Unsigned(_))
        && column.exact_key()
}

/// `col = literal`, `literal = col`, `col <=> literal` or `col IN (literal, …)` over an integer
/// column (signed or unsigned, not VIRTUAL) with integer literals: the column and the values it
/// must hold. Row images carry such a column's value as the integer it is (as for unit keys)
/// and SQL compares integers exactly, so a binlog value passes exactly when the condition holds
/// for the row; NULL fails both (`<=>` with a non-NULL literal is FALSE). Other kinds convert
/// (strings by collation and pad rules, temporal, decimal and inexact values): never filters.
fn const_filter(e: &Expr, scope: &Scope) -> Option<(ColRef, Vec<Datum>)> {
    let (col, lits) = match e {
        Expr::Nested(x) => return const_filter(x, scope),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq | BinaryOperator::Spaceship,
            right,
        } => match scope.resolve(left) {
            Some(_) => (left.as_ref(), vec![right.as_ref()]),
            None => (right.as_ref(), vec![left.as_ref()]),
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => (expr.as_ref(), list.iter().collect()),
        _ => return None,
    };
    let c = scope.resolve(col)?;
    if !filter_column(&scope.aliases[c.0].1.columns[c.1]) {
        return None;
    }
    let values = lits
        .into_iter()
        .map(int_literal)
        .collect::<Option<Vec<_>>>()?;
    Some((c, values))
}

/// An integer literal (`5`, `-1`, `TRUE`) as the datum of an integer column holding it.
fn int_literal(e: &Expr) -> Option<Datum> {
    fn value(e: &Expr) -> Option<i128> {
        match e {
            Expr::Nested(x) => value(x),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => value(expr).map(|v| -v),
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr,
            } => value(expr),
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => {
                    n.parse().ok()
                }
                Value::Boolean(b) => Some(i128::from(*b)),
                _ => None,
            },
            _ => None,
        }
    }
    let v = value(e)?;
    match (i64::try_from(v), u64::try_from(v)) {
        (Ok(i), _) => Some(Datum::Int(i)),
        (_, Ok(u)) => Some(Datum::UInt(u)),
        _ => None,
    }
}

/// Partial direct units: an alias traced neither by direct units nor by lineage whose row still
/// equals some unit expressions (plain columns of exact kinds) on every row it contributes to,
/// through the same equality classes as `direct_unit` (inner-join / WHERE equalities plus its
/// own LEFT JOIN condition). A changed row of it can only affect units with those values: its
/// images give partial unit keys, and the refresh recomputes every unit matching one, with no
/// lookup query and no part in the lookup combinations. (Select/join views keep lineage where
/// it applies; grouped views are the main users.)
pub(crate) fn partial_units(
    alias_plans: &[AliasPlan],
    lineage: &[LineageKey],
    unit_cols: &[Option<ColRef>],
    scope: &Scope,
    base_edges: &[(ColRef, ColRef)],
    own_edges: &[Vec<(ColRef, ColRef)>],
) -> PartialUnits {
    let mut out = PartialUnits::default();
    for (ai, a) in alias_plans.iter().enumerate() {
        let mut found = None;
        if a.direct_unit.is_none() && !lineage.iter().any(|k| k.alias == ai) {
            let mut cls = Classes::new();
            for (x, y) in base_edges.iter().chain(&own_edges[ai]) {
                cls.union(*x, *y);
            }
            let (mut at, mut cols) = (Vec::new(), Vec::new());
            for (u, uc) in unit_cols.iter().enumerate() {
                let Some(uc) = *uc else { continue };
                if !scope.aliases[uc.0].1.columns[uc.1].exact_key() {
                    continue;
                }
                let ru = cls.find(uc);
                // (Not a generated column: an image need not carry its value.)
                let col = (0..a.table.columns.len()).find(|&c| {
                    let col = &a.table.columns[c];
                    col.exact_key()
                        && col.generated.is_none()
                        && (uc == (ai, c) || cls.find((ai, c)) == ru)
                });
                if let Some(c) = col {
                    at.push(u);
                    cols.push(c);
                }
            }
            if !at.is_empty() {
                let set = match out.sets.iter().position(|s| *s == at) {
                    Some(i) => i,
                    None => {
                        out.sets.push(at);
                        out.sets.len() - 1
                    }
                };
                found = Some((set, cols));
            }
        }
        out.aliases.push(found);
    }
    if out.sets.is_empty() {
        out.aliases.clear();
    }
    out
}

/// A parent key column whose binlog value is its SQL value (as for unit keys), not generated.
pub(crate) fn filter_or_key_column(column: &crate::catalog::Column) -> bool {
    column.exact_key() && column.generated.is_none()
}

/// Column references in an expression (unresolvable identifiers are ignored).
pub(crate) fn col_refs(e: &Expr, scope: &Scope) -> Vec<ColRef> {
    let mut out = Vec::new();
    let _ = visit_expressions(e, |x: &Expr| {
        if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            if let Some(c) = scope.resolve(x) {
                out.push(c);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    out
}

/// Functions returning JSON values.
const JSON_PRODUCING: &[&str] = &[
    "JSON_EXTRACT",
    "JSON_OBJECT",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "JSON_SET",
    "JSON_INSERT",
    "JSON_REPLACE",
    "JSON_REMOVE",
    "JSON_MERGE",
    "JSON_MERGE_PATCH",
    "JSON_MERGE_PRESERVE",
    "JSON_KEYS",
    "JSON_ARRAY_APPEND",
    "JSON_ARRAY_INSERT",
    "JSON_QUOTE",
    "JSON_QUERY",
];

/// Whether a unit (group) expression is a JSON value computed by a function, `->` or a CAST.
pub(crate) fn json_computed(e: &Expr) -> bool {
    match e {
        Expr::Nested(x) => json_computed(x),
        Expr::Function(f) => JSON_PRODUCING.contains(&function_name(f).as_str()),
        Expr::Cast { data_type, .. }
        | Expr::Convert {
            data_type: Some(data_type),
            ..
        } => data_type.to_string().eq_ignore_ascii_case("JSON"),
        Expr::BinaryOp {
            op: BinaryOperator::Arrow,
            ..
        } => true,
        _ => false,
    }
}

/// Whether a unit expression's value has the JSON type: a JSON column (`json_col`), a value
/// computed as JSON ([`json_computed`]), or a choice one of whose values has it (`IFNULL`,
/// `COALESCE`, `NULLIF`, `IF`, `CASE`). Text and scalars over JSON (`->>`, JSON_UNQUOTE,
/// JSON_VALUE, JSON_LENGTH, `CAST(… AS CHAR)`) are not.
pub(crate) fn json_valued(e: &Expr, json_col: &dyn Fn(&Expr) -> bool) -> bool {
    let any = |xs: &[&Expr]| xs.iter().any(|x| json_valued(x, json_col));
    match e {
        Expr::Nested(x) => json_valued(x, json_col),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => json_col(e),
        Expr::Function(f) => match (function_name(f).as_str(), function_args(f)) {
            ("IFNULL" | "COALESCE", Some(a)) => any(&a),
            ("NULLIF", Some(a)) => any(&a[..a.len().min(1)]),
            ("IF", Some(a)) => any(&a[a.len().min(1)..]),
            _ => json_computed(e),
        },
        Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            conditions.iter().any(|w| json_valued(&w.result, json_col))
                || else_result
                    .as_deref()
                    .is_some_and(|x| json_valued(x, json_col))
        }
        _ => json_computed(e),
    }
}

/// Reasons the query cannot be maintained incrementally, whether it aggregates, and the
/// PARTITION BY expressions of each window function call.
pub(crate) fn scan_exprs(select: &Select) -> (Option<String>, bool, Vec<Vec<Expr>>) {
    let mut reason = None;
    let mut has_agg = false;
    let mut windows = Vec::new();
    let named = |name: &Ident| {
        select.named_window.iter().find_map(|w| match &w.1 {
            NamedWindowExpr::WindowSpec(spec) if w.0.value.eq_ignore_ascii_case(&name.value) => {
                Some(spec.partition_by.clone())
            }
            _ => None,
        })
    };
    let mut check = |e: &Expr| {
        match e {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                reason.get_or_insert_with(|| "subquery".to_string());
            }
            Expr::MatchAgainst { .. } => {
                reason.get_or_insert_with(|| "MATCH ... AGAINST".to_string());
            }
            Expr::Function(f) => {
                let n = function_name(f);
                if let Some(over) = &f.over {
                    // A window inheriting a named one takes its PARTITION BY (none if unknown).
                    windows.push(match over {
                        WindowType::WindowSpec(spec) => match &spec.window_name {
                            Some(w) => named(w).unwrap_or_default(),
                            None => spec.partition_by.clone(),
                        },
                        WindowType::NamedWindow(w) => named(w).unwrap_or_default(),
                    });
                } else if AGGREGATES.contains(&n.as_str()) {
                    has_agg = true;
                } else if NONDETERMINISTIC.contains(&n.as_str()) {
                    reason.get_or_insert_with(|| format!("non-deterministic function {n}"));
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    };
    let _ = visit_expressions(&select.projection, &mut check);
    let _ = visit_expressions(&select.selection, &mut check);
    let _ = visit_expressions(&select.having, &mut check);
    let _ = visit_expressions(&select.group_by, &mut check);
    let _ = visit_expressions(&select.from, &mut check);
    (reason, has_agg, windows)
}

/// The group expressions that determine all the others: one is dropped when its columns follow
/// from the columns of the remaining plain-column keys through equalities (inner joins, WHERE)
/// and primary keys (a table's whole primary key determines its row; a LEFT JOINed table whose
/// whole primary key equals determined columns is determined too, matched or NULL-extended).
pub(crate) fn reduce_unit(
    keys: Vec<Expr>,
    scope: &Scope,
    base_edges: &[(ColRef, ColRef)],
    own_edges: &[Vec<(ColRef, ColRef)>],
) -> Vec<Expr> {
    let closure = |known: &[ColRef]| {
        let mut d: HashSet<ColRef> = known.iter().copied().collect();
        loop {
            let before = d.len();
            for (x, y) in base_edges {
                if d.contains(x) {
                    d.insert(*y);
                }
                if d.contains(y) {
                    d.insert(*x);
                }
            }
            for (a, (_, t)) in scope.aliases.iter().enumerate() {
                let bound = |c: usize| {
                    d.contains(&(a, c))
                        || own_edges[a].iter().any(|(x, y)| {
                            (*x == (a, c) && y.0 != a && d.contains(y))
                                || (*y == (a, c) && x.0 != a && d.contains(x))
                        })
                };
                // Any whole key (primary or UNIQUE NOT NULL) determines the row.
                if t.keys().any(|k| k.iter().all(|&c| bound(c))) {
                    d.extend((0..t.columns.len()).map(|c| (a, c)));
                }
            }
            if d.len() == before {
                return d;
            }
        }
    };
    let mut keep = vec![true; keys.len()];
    for i in (0..keys.len()).rev() {
        let known: Vec<ColRef> = (0..keys.len())
            .filter(|&j| j != i && keep[j])
            .filter_map(|j| scope.resolve(&keys[j]))
            .collect();
        let refs = col_refs(&keys[i], scope);
        let d = closure(&known);
        if !refs.is_empty() && refs.iter().all(|r| d.contains(r)) {
            keep[i] = false;
        }
    }
    keys.into_iter()
        .zip(keep)
        .filter_map(|(k, kept)| kept.then_some(k))
        .collect()
}

/// The expressions of the output columns `def.id_outputs` names (see `ParsedDef::id_outputs`): a name
/// is a `CREATE VIEW` column-list name, an output alias, or the column name of an unaliased column
/// reference (case-insensitive). Grouped plans take group keys only, windowed plans shared partition
/// keys only.
pub(crate) fn chosen_id(
    def: &ParsedDef,
    proj: &[(&Expr, Option<String>)],
    keys: Option<&[Expr]>,
    scope: &Scope,
) -> Result<Vec<Expr>> {
    let mut chosen = Vec::new();
    for n in &def.id_outputs {
        let index: Option<usize> = if !def.columns.is_empty() {
            def.columns.iter().position(|c| c.eq_ignore_ascii_case(n))
        } else {
            let mut hits = Vec::new();
            for (i, (e, a)) in proj.iter().enumerate() {
                let named = match a {
                    Some(a) => a.eq_ignore_ascii_case(n),
                    None => match e {
                        Expr::Identifier(id) => ident_name(id).eq_ignore_ascii_case(n),
                        Expr::CompoundIdentifier(parts) => parts
                            .last()
                            .is_some_and(|id| ident_name(id).eq_ignore_ascii_case(n)),
                        _ => false,
                    },
                };
                if named {
                    hits.push(i);
                }
            }
            match hits.len() {
                0 => None,
                1 => Some(hits[0]),
                k => bail!("id output `{n}` names {k} output columns"),
            }
        };
        let i = match index {
            Some(i) => i,
            None => bail!("id output `{n}` is not an output column of the SELECT"),
        };
        let e = proj[i].0;
        if let Some(ks) = keys {
            let is_key = ks.iter().any(|k| {
                expr_key(e) == expr_key(k)
                    || (scope.resolve(e).is_some() && scope.resolve(e) == scope.resolve(k))
            });
            if !is_key {
                bail!("id output `{n}` is not a GROUP BY or shared PARTITION BY key");
            }
        }
        chosen.push(e.clone());
    }
    Ok(chosen)
}

/// Whether anything in `t` carries a `COLLATE` of its own: a `COLLATE` clause anywhere in the
/// definition (projection, WHERE, joins, GROUP BY, HAVING, ORDER BY, subqueries) makes the server
/// compare with an EXPLICIT collation, which an equally EXPLICIT parameter collation cannot meet.
pub(crate) fn has_collate(t: &impl Visit) -> bool {
    let mut found = false;
    let mut find = |e: &Expr| {
        if matches!(e, Expr::Collate { .. }) {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    };
    let _ = visit_expressions(t, &mut find);
    found
}
