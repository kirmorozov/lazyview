use super::*;
use super::{expr::*, parse::*, vocab::*};

/// What an expression's value is, as far as converting it to a column type goes.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ValueKind {
    /// Integer or DECIMAL.
    Num,
    Dbl,
    Str,
    Date,
    DateTime,
    Time,
    Null,
    Unknown,
}

impl std::fmt::Display for ValueKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ValueKind::Num => "an exact number",
            ValueKind::Dbl => "a floating-point number",
            ValueKind::Str => "a string",
            ValueKind::Date => "a date",
            ValueKind::DateTime => "a datetime",
            ValueKind::Time => "a time",
            ValueKind::Null => "NULL",
            ValueKind::Unknown => "a value of unknown type",
        })
    }
}

fn column_value_kind(c: &crate::catalog::Column) -> ValueKind {
    match c.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
        | "numeric" => ValueKind::Num,
        "float" | "double" | "real" => ValueKind::Dbl,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => ValueKind::Str,
        "date" => ValueKind::Date,
        "datetime" => ValueKind::DateTime,
        "time" => ValueKind::Time,
        _ => ValueKind::Unknown,
    }
}

/// The kind of value of a generation expression over the columns of `t` (named unqualified).
fn value_kind(e: &Expr, t: &TableInfo) -> ValueKind {
    use ValueKind::*;
    let numeric = |k: ValueKind| match k {
        Num | Dbl | Null => k,
        // Strings take part in arithmetic as doubles.
        Str => Dbl,
        _ => Unknown,
    };
    let common = |ks: Vec<ValueKind>| {
        let mut it = ks.into_iter().filter(|k| *k != Null);
        match it.next() {
            None => Null,
            Some(first) => {
                if it.all(|k| k == first) {
                    first
                } else {
                    Unknown
                }
            }
        }
    };
    let args = |f: &sqlparser::ast::Function| -> Vec<Expr> {
        match &f.args {
            FunctionArguments::List(l) => l
                .args
                .iter()
                .filter_map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    };
    match e {
        Expr::Nested(x) => value_kind(x, t),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let name = match e {
                Expr::Identifier(i) => ident_name(i),
                Expr::CompoundIdentifier(p) => p.last().map(ident_name).unwrap_or_default(),
                _ => unreachable!(),
            };
            t.col_index(&name)
                .map(|c| column_value_kind(&t.columns[c]))
                .unwrap_or(Unknown)
        }
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) if n.contains(['e', 'E']) => Dbl,
            Value::Number(..) | Value::Boolean(_) => Num,
            Value::SingleQuotedString(_) | Value::DoubleQuotedString(_) => Str,
            Value::Null => Null,
            _ => Unknown,
        },
        Expr::Prefixed { value, .. } => match value_kind(value, t) {
            Str => Str,
            _ => Unknown,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => numeric(value_kind(expr, t)),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => Num,
        Expr::BinaryOp { left, op, right } => {
            let (l, r) = (value_kind(left, t), value_kind(right, t));
            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => match (numeric(l), numeric(r)) {
                    (Unknown, _) | (_, Unknown) => Unknown,
                    (Dbl, _) | (_, Dbl) => Dbl,
                    (Null, Null) => Null,
                    _ => Num,
                },
                BinaryOperator::MyIntegerDivide => match (numeric(l), numeric(r)) {
                    (Unknown, _) | (_, Unknown) => Unknown,
                    _ => Num,
                },
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Spaceship
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Xor => Num,
                _ => Unknown,
            }
        }
        Expr::Function(f) => {
            let a = args(f);
            let kinds = |xs: &[Expr]| xs.iter().map(|x| value_kind(x, t)).collect::<Vec<_>>();
            match function_name(f).as_str() {
                "CONCAT" | "CONCAT_WS" | "LOWER" | "LCASE" | "UPPER" | "UCASE" | "SUBSTRING"
                | "SUBSTR" | "LEFT" | "RIGHT" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "LPAD"
                | "RPAD" | "REVERSE" | "JSON_UNQUOTE" | "JSON_VALUE" => Str,
                "ABS" | "ROUND" | "TRUNCATE" | "FLOOR" | "CEIL" | "CEILING" => a
                    .first()
                    .map(|x| numeric(value_kind(x, t)))
                    .unwrap_or(Unknown),
                "SIGN" => Num,
                "IFNULL" | "COALESCE" | "GREATEST" | "LEAST" => common(kinds(&a)),
                "IF" if a.len() == 3 => common(kinds(&a[1..])),
                "DATE" => Date,
                _ => Unknown,
            }
        }
        Expr::Case {
            conditions,
            else_result,
            ..
        } => common(
            conditions
                .iter()
                .map(|w| value_kind(&w.result, t))
                .chain(else_result.iter().map(|x| value_kind(x, t)))
                .collect(),
        ),
        Expr::Cast { data_type, .. } => {
            let ty = data_type.to_string().to_ascii_uppercase();
            if ["SIGNED", "UNSIGNED", "DECIMAL", "INT"]
                .iter()
                .any(|p| ty.starts_with(p))
            {
                Num
            } else if ["DOUBLE", "FLOAT", "REAL"]
                .iter()
                .any(|p| ty.starts_with(p))
            {
                Dbl
            } else if ty.starts_with("CHAR") || ty.starts_with("VARCHAR") {
                Str
            } else if ty.starts_with("DATETIME") {
                DateTime
            } else if ty.starts_with("DATE") {
                Date
            } else if ty.starts_with("TIME") && !ty.starts_with("TIMESTAMP") {
                Time
            } else {
                Unknown
            }
        }
        _ => Unknown,
    }
}

/// How the server converts a value of `kind` (`?`) to `c`'s declared type when it reads a
/// VIRTUAL column, as SQL; `None` where no exact SQL form is known. Measured on MySQL 8.4 / 26.7 /
/// 26.10, Percona 8.4 / 9.7 and MariaDB 10.11 / 13.0 / 13.2 (B1, F1, F2): integer columns round
/// a DOUBLE half to even (as `CAST(… AS SIGNED)` does) and an exact number or a string half away
/// from zero (through `DECIMAL(65,0)`), and clip to their range (before the CAST, which wraps an
/// unsigned value >= 2^63 and raises ERROR 1690 for an out-of-range function result on MySQL);
/// DECIMAL(p,s) and DOUBLE are their CAST; DATE from a DATETIME rounds (MySQL) or truncates
/// (MariaDB) the time like `CAST(… AS DATETIME)`; strings take the length, charset and collation
/// (CHAR drops trailing spaces). Not exact: FLOAT (MySQL refuses out-of-range CASTs), BIGINT
/// UNSIGNED from a DOUBLE (MySQL raises 1690 inside any clip), a number into a string (`i / 3`
/// prints 9 decimals in the column on MySQL 8.4 and MariaDB, 4 on MySQL 9, 4 by CONVERT
/// everywhere), a temporal value into a string short enough to cut it, DATE from a string (MySQL
/// stores zero dates), TEXT / TINYTEXT byte limits, other types.
fn virtual_conversion(c: &crate::catalog::Column, kind: ValueKind) -> Option<String> {
    use ValueKind::*;
    let ct = &c.column_type;
    let unsigned = ct.contains("unsigned");
    let type_args: Vec<u64> = ct
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(a, _)| a.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_default();
    let range = |bits: u32| -> (i128, i128) {
        if unsigned {
            (0, (1i128 << bits) - 1)
        } else {
            (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
        }
    };
    let text = |cs: &str, co: &str, len: Option<u64>, trim: bool| {
        let cast = match len {
            Some(n) => format!("LEFT(CONVERT(? USING {cs}), {n})"),
            None => format!("CONVERT(? USING {cs})"),
        };
        if trim {
            format!("TRIM(TRAILING ' ' FROM {cast}) COLLATE {co}")
        } else {
            format!("{cast} COLLATE {co}")
        }
    };
    Some(match c.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
            let bits = match c.data_type.as_str() {
                "tinyint" => 8,
                "smallint" => 16,
                "mediumint" => 24,
                "bigint" => 64,
                _ => 32,
            };
            let (min, max) = range(bits);
            match kind {
                // Clipped in the DECIMAL domain, then cast: DECIMAL(65,0) rounds half away from
                // zero and parses a string like the column, and the CAST never sees a value out
                // of its range (CAST(… AS SIGNED) of an unsigned value >= 2^63 wraps, F1).
                Num | Str | Null => format!(
                    "CAST(LEAST(GREATEST(CAST(? AS DECIMAL(65,0)), {min}), {max}) AS {})",
                    if unsigned && bits == 64 {
                        "UNSIGNED"
                    } else {
                        "SIGNED"
                    }
                ),
                // A DOUBLE rounds half to even as CAST does; it is clipped first because MySQL
                // raises ERROR 1690 for a function result out of BIGINT range (F2).
                Dbl if bits < 64 => format!("CAST(LEAST(GREATEST(?, {min}), {max}) AS SIGNED)"),
                Dbl if !unsigned => "IF(? >= 9.223372036854775807e18, 9223372036854775807, \
                     CAST(GREATEST(?, -9.223372036854775808e18) AS SIGNED))"
                    .into(),
                _ => return None,
            }
        }
        "decimal" | "numeric" if matches!(kind, Num | Dbl | Str | Null) => {
            let (p, s) = match type_args.as_slice() {
                [p, s] => (*p, *s),
                [p] => (*p, 0),
                _ => (10, 0),
            };
            let cast = format!("CAST(? AS DECIMAL({p},{s}))");
            if unsigned {
                format!("GREATEST({cast}, 0)")
            } else {
                cast
            }
        }
        "double" | "real"
            if !unsigned && type_args.is_empty() && matches!(kind, Num | Dbl | Str | Null) =>
        {
            "CAST(? AS DOUBLE)".into()
        }
        "date" => match kind {
            Date | Null => "CAST(? AS DATE)".into(),
            DateTime => "CAST(CAST(? AS DATETIME) AS DATE)".into(),
            _ => return None,
        },
        "datetime" if matches!(kind, Date | DateTime | Null) => {
            format!("CAST(? AS DATETIME({}))", type_args.first().unwrap_or(&0))
        }
        "time" if matches!(kind, DateTime | Time | Null) => {
            format!("CAST(? AS TIME({}))", type_args.first().unwrap_or(&0))
        }
        "char" | "varchar" => {
            let (Some(cs), Some(co), Some(&n)) = (&c.charset, &c.collation, type_args.first())
            else {
                return None;
            };
            let fits = match kind {
                Str | Null => true,
                Date | DateTime | Time => n >= 26,
                Num | Dbl | Unknown => false,
            };
            if !fits {
                return None;
            }
            text(cs, co, Some(n), c.data_type == "char")
        }
        "mediumtext" | "longtext" if !matches!(kind, Num | Dbl | Unknown) => {
            let (Some(cs), Some(co)) = (&c.charset, &c.collation) else {
                return None;
            };
            text(cs, co, None, false)
        }
        _ => return None,
    })
}

/// Most base-column references the inlined form of one VIRTUAL column may have.
const VIRTUAL_BUDGET: usize = 256;

/// What a reference to VIRTUAL column `c` of `t` (named `alias` in the query) stands for: its
/// generation expression over the alias's columns, nested VIRTUAL columns expanded the same
/// way, converted to the declared type ([`virtual_conversion`]). `Err`: why it cannot be.
pub(crate) fn virtual_expansion(
    alias: &str,
    t: &TableInfo,
    c: usize,
) -> std::result::Result<Expr, String> {
    // Nested VIRTUAL columns are copied per reference (fan-out^depth, F4): the size is measured
    // first, memoized per column, so an expansion past the budget is refused before it is built.
    fn size(
        t: &TableInfo,
        c: usize,
        memo: &mut HashMap<usize, Option<usize>>,
        depth: usize,
    ) -> Option<usize> {
        if let Some(&n) = memo.get(&c) {
            return n;
        }
        let g = t.columns[c].generated.as_ref()?;
        let n = if depth > 16 {
            None
        } else {
            parse_sql(&g.expr, |p| p.parse_expr()).and_then(|e| {
                let mut n = 0usize;
                let mut ok = true;
                let _ = visit_expressions(&e, |x: &Expr| {
                    let name = match x {
                        Expr::Identifier(i) => Some(ident_name(i)),
                        Expr::CompoundIdentifier(parts) => parts.last().map(ident_name),
                        _ => None,
                    };
                    if let Some(i) = name.and_then(|n| t.col_index(&n)) {
                        let nested = t.columns[i].generated.as_ref().is_some_and(|g| !g.stored);
                        match nested.then(|| size(t, i, memo, depth + 1)) {
                            Some(Some(k)) => n = n.saturating_add(k),
                            Some(None) => ok = false,
                            None => n = n.saturating_add(1),
                        }
                    }
                    ControlFlow::<()>::Continue(())
                });
                ok.then_some(n)
            })
        };
        memo.insert(c, n);
        n
    }
    if let Some(n) = size(t, c, &mut HashMap::new(), 0) {
        if n > VIRTUAL_BUDGET {
            return Err(format!(
                "VIRTUAL generated column `{}`.`{}` expands to {n} base-column references (budget {VIRTUAL_BUDGET}): its nested VIRTUAL columns are copied per reference",
                t.name, t.columns[c].name
            ));
        }
    }
    fn expand(
        alias: &str,
        t: &TableInfo,
        c: usize,
        depth: usize,
    ) -> std::result::Result<Expr, String> {
        let col = &t.columns[c];
        let g = col.generated.as_ref().expect("generated column");
        let unreadable = || {
            format!(
                "VIRTUAL generated column `{}`.`{}` with an expression the parser cannot read: {}",
                t.name, col.name, g.expr
            )
        };
        if g.deps.is_none() || depth > 16 {
            return Err(unreadable());
        }
        let mut e = parse_sql(&g.expr, |p| p.parse_expr()).ok_or_else(unreadable)?;
        let kind = value_kind(&e, t);
        let conversion = virtual_conversion(col, kind).ok_or_else(|| {
            format!(
                "VIRTUAL generated column `{}`.`{}` of type {}: no exact SQL conversion of its expression ({kind}) to that type",
                t.name, col.name, col.column_type
            )
        })?;
        let mut failed = None;
        let _ = visit_expressions_mut(&mut e, |x: &mut Expr| {
            let name = match &*x {
                Expr::Identifier(i) => Some(ident_name(i)),
                Expr::CompoundIdentifier(parts) => parts.last().map(ident_name),
                _ => None,
            };
            if let Some(i) = name.and_then(|n| t.col_index(&n)) {
                let nested_virtual = t.columns[i].generated.as_ref().is_some_and(|g| !g.stored);
                *x = if nested_virtual {
                    match expand(alias, t, i, depth + 1) {
                        Ok(v) => v,
                        Err(r) => {
                            failed.get_or_insert(r);
                            return ControlFlow::Break(());
                        }
                    }
                } else {
                    Expr::CompoundIdentifier(vec![qident(alias), qident(&t.columns[i].name)])
                };
            }
            ControlFlow::Continue(())
        });
        if let Some(r) = failed {
            return Err(r);
        }
        let value = match &e {
            Expr::Nested(_) => e.to_string(),
            _ => format!("({e})"),
        };
        let sql = conversion.replace('?', &value);
        let e = parse_sql(&sql, |p| p.parse_expr()).ok_or_else(unreadable)?;
        Ok(Expr::Nested(Box::new(e)))
    }
    expand(alias, t, c, 0)
}

/// Replaces every reference to a VIRTUAL generated column in the SELECT (and in the top-N
/// ORDER BY) by [`virtual_expansion`]; a bare projection item keeps the column's name. `Some`:
/// why one cannot be. Names inside subqueries of the ORDER BY are left alone (their own FROM
/// comes first).
pub(crate) fn inline_virtual_columns(
    select: &mut Select,
    order_by: Option<&mut OrderBy>,
    scope: &Scope,
) -> Result<Option<String>> {
    let virtual_of = |e: &Expr| -> Option<(usize, usize)> {
        let (a, c) = scope.resolve(e)?;
        let col = &scope.aliases[a].1.columns[c];
        col.generated.as_ref().filter(|g| !g.stored).map(|_| (a, c))
    };
    let expansion =
        |a: usize, c: usize| virtual_expansion(&scope.aliases[a].0, &scope.aliases[a].1, c);
    // (Checked as written: the inlined strings carry a COLLATE of their own.)
    let mut collate = false;
    let mut find_collate = |x: &Expr| {
        if matches!(x, Expr::Collate { .. }) {
            collate = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    };
    let _ = visit_expressions(&*select, &mut find_collate);
    if let Some(o) = &order_by {
        let _ = visit_expressions(&**o, &mut find_collate);
    }
    let mut failed: Option<String> = None;
    let mut text = None;
    let mut note = |a: usize, c: usize| {
        let col = &scope.aliases[a].1.columns[c];
        if col.charset.is_some() {
            text.get_or_insert_with(|| format!("`{}`.`{}`", scope.aliases[a].1.name, col.name));
        }
    };
    // Projection: a bare column keeps its name.
    for item in &mut select.projection {
        if let SelectItem::UnnamedExpr(e) = item {
            if let Some((a, c)) = virtual_of(e) {
                note(a, c);
                match expansion(a, c) {
                    Ok(x) => {
                        let name = scope.aliases[a].1.columns[c].name.clone();
                        *item = SelectItem::ExprWithAlias {
                            expr: x,
                            alias: qident(&name),
                        };
                    }
                    Err(r) => {
                        failed.get_or_insert(r);
                    }
                };
            }
        }
    }
    let mut replace = |e: &mut Expr| {
        if let Some((a, c)) = virtual_of(e) {
            note(a, c);
            match expansion(a, c) {
                Ok(x) => *e = x,
                Err(r) => {
                    failed.get_or_insert(r);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    };
    let _ = visit_expressions_mut(select, &mut replace);
    if let Some(OrderByKind::Expressions(es)) = order_by.map(|o| &mut o.kind) {
        for e in es.iter_mut().filter(|e| !has_subquery(&e.expr)) {
            let _ = visit_expressions_mut(&mut e.expr, &mut replace);
        }
    }
    if failed.is_some() {
        return Ok(failed);
    }
    // The inlined string carries an EXPLICIT collation where the column had an IMPLICIT one: next
    // to another explicit COLLATE the server would refuse the mix (or pick another collation).
    if let (Some(col), true) = (text, collate) {
        return Ok(Some(format!(
            "VIRTUAL generated column {col} next to an explicit COLLATE (its inlined form has an explicit collation)"
        )));
    }
    Ok(None)
}

/// `db.t.col` -> `t`.`col` for columns of the FROM clause: lookups replace a table by derived
/// rows, which can only be referenced through the alias.
pub(crate) fn strip_column_schemas(select: &mut Select, scope: &Scope) {
    let _ = visit_expressions_mut(select, |e: &mut Expr| {
        let resolved = match &*e {
            Expr::CompoundIdentifier(parts) if parts.len() == 3 => {
                scope.resolve(e).map(|(a, _)| (a, parts[2].clone()))
            }
            _ => None,
        };
        if let Some((a, col)) = resolved {
            *e = Expr::CompoundIdentifier(vec![qident(&scope.aliases[a].0), col]);
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Statement for full rebuilds: the SELECT as written, with the view's column list applied by a
/// CTE column list. That names columns by position like `CREATE VIEW v (a, b)` does (duplicate
/// names in the SELECT included); renaming the items instead would break ORDER BY / HAVING
/// references to their own aliases.
pub(crate) fn full_select_sql(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<String> {
    if def.columns.is_empty() {
        return Ok(def.query_sql.clone());
    }
    let ctes = cte_names(&def.query);
    if let Some(n) =
        leftmost_select(&def.query).and_then(|s| select_width(s, default_schema, tables, &ctes))
    {
        if n != def.columns.len() {
            bail!(
                "view column list has {} names but SELECT returns {n} columns",
                def.columns.len()
            );
        }
    }
    let names: Vec<String> = def.columns.iter().map(|c| quote_ident(c)).collect();
    Ok(format!(
        "WITH `_lv_cols` ({}) AS ({}) SELECT * FROM `_lv_cols`",
        names.join(", "),
        def.query_sql
    ))
}

/// The first SELECT of a query body (it names the columns of a set operation).
fn leftmost_select(q: &Query) -> Option<&Select> {
    let mut body = q.body.as_ref();
    loop {
        match body {
            SetExpr::Select(s) => return Some(s),
            SetExpr::SetOperation { left, .. } => body = left,
            SetExpr::Query(q) => body = q.body.as_ref(),
            _ => return None,
        }
    }
}

/// Number of columns a SELECT returns, if its wildcards can be expanded from the catalog.
fn select_width(
    s: &Select,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
    ctes: &[String],
) -> Option<usize> {
    // FROM tables by the name the query uses; None if a wildcard cannot be counted from them.
    let from: Option<Vec<(String, &TableInfo)>> = s
        .from
        .iter()
        .flat_map(|t| std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)))
        .map(|f| match f {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } if !(name.0.len() == 1
                && object_table_ref(name, default_schema)
                    .is_some_and(|t| ctes.iter().any(|c| c.eq_ignore_ascii_case(&t.1)))) =>
            {
                let t = object_table_ref(name, default_schema)?;
                let info = tables.get(&t)?;
                let n = alias.as_ref().map(|a| ident_name(&a.name)).unwrap_or(t.1);
                Some((n, info.as_ref()))
            }
            _ => None,
        })
        .collect();
    // USING / NATURAL joins merge columns in `*`.
    let merged = s.from.iter().flat_map(|t| &t.joins).any(|j| {
        matches!(
            &j.join_operator,
            JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c) | JoinOperator::Right(c) | JoinOperator::RightOuter(c)
                if matches!(c, JoinConstraint::Using(_) | JoinConstraint::Natural)
        )
    });
    let visible = |t: &TableInfo| t.columns.iter().filter(|c| !c.invisible).count();
    s.projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) if !merged => {
                Some(from.as_ref()?.iter().map(|(_, t)| visible(t)).sum())
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(on), _) => {
                let q = match on.0.last()? {
                    ObjectNamePart::Identifier(i) => ident_name(i),
                    _ => return None,
                };
                from.as_ref()?
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&q))
                    .map(|(_, t)| visible(t))
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => None,
            _ => Some(1),
        })
        .sum()
}

/// Base tables behind the referenced tables, expanding SQL views recursively (views present in
/// `tables` are expanded; unknown names are kept so the caller can load them).
pub(crate) fn expand_views(
    refs: &[TableRef],
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<TableRef> {
    let mut out = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut stack: Vec<TableRef> = refs.to_vec();
    while let Some(t) = stack.pop() {
        if !seen.insert(t.clone()) {
            continue;
        }
        match tables.get(&t) {
            Some(info) if info.is_view => stack.extend(info.view_bases.iter().cloned()),
            _ => {
                out.insert(t);
            }
        }
    }
    out.into_iter().collect()
}
