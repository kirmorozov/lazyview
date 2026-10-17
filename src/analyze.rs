//! Turns a `CREATE VIEW` statement into a [`ViewPlan`]: what to watch, how a changed row maps to
//! the materialized rows it affects ("units"), and pre-rendered SQL for every refresh query.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    visit_expressions, visit_relations, BinaryOperator, Expr, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableAlias, TableFactor, TableWithJoins, Value, Visit,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use crate::catalog::{FkAction, TableInfo, TableRef};

/// Placeholder table name used to render "FROM with one table replaced by literal rows".
const SUBST_MARKER: &str = "__lv_subst_marker__";

const AGGREGATES: &[&str] = &[
    "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "BIT_AND", "BIT_OR", "BIT_XOR", "STD",
    "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE", "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

const NONDETERMINISTIC: &[&str] = &[
    "NOW", "SYSDATE", "CURDATE", "CURTIME", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "LOCALTIME", "LOCALTIMESTAMP", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "UNIX_TIMESTAMP",
    "RAND", "UUID", "UUID_SHORT", "CONNECTION_ID", "USER", "CURRENT_USER", "SESSION_USER",
    "SYSTEM_USER", "LAST_INSERT_ID", "FOUND_ROWS", "ROW_COUNT", "SLEEP", "BENCHMARK",
];

/// The statement as written in `_lazyview.definition`.
#[derive(Debug, Clone)]
pub struct ParsedDef {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<String>,
    pub query: Query,
    pub hash: String,
}

pub fn definition_hash(def: &str) -> String {
    hex::encode(Sha256::digest(def.trim().as_bytes()))[..16].to_string()
}

/// Parses `CREATE [OR REPLACE] [ALGORITHM=..] [DEFINER=..] [SQL SECURITY ..] VIEW name [(cols)] AS
/// SELECT ... [WITH [CASCADED|LOCAL] CHECK OPTION]`.
pub fn parse_definition(def: &str) -> Result<ParsedDef> {
    let dialect = MySqlDialect {};
    let tokens = Tokenizer::new(&dialect, def)
        .tokenize()
        .map_err(|e| anyhow!("tokenize: {e}"))?;
    let words: Vec<&Token> = tokens
        .iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    let kw = |t: &Token, k: &str| matches!(t, Token::Word(Word { value, quote_style: None, .. }) if value.eq_ignore_ascii_case(k));
    if words.first().map(|t| kw(t, "CREATE")) != Some(true) {
        bail!("definition must start with CREATE ... VIEW");
    }
    let view_at = words
        .iter()
        .position(|t| kw(t, "VIEW"))
        .ok_or_else(|| anyhow!("definition must be a CREATE VIEW statement"))?;
    let mut i = view_at + 1;
    let mut parts = Vec::new();
    loop {
        match words.get(i) {
            Some(Token::Word(w)) => parts.push(w.value.clone()),
            _ => bail!("expected view name after VIEW"),
        }
        i += 1;
        if matches!(words.get(i), Some(Token::Period)) {
            i += 1;
            continue;
        }
        break;
    }
    let mut columns = Vec::new();
    if matches!(words.get(i), Some(Token::LParen)) {
        i += 1;
        loop {
            match words.get(i) {
                Some(Token::Word(w)) => columns.push(w.value.clone()),
                Some(Token::Comma) => {}
                Some(Token::RParen) => {
                    i += 1;
                    break;
                }
                _ => bail!("bad column list"),
            }
            i += 1;
        }
    }
    if !words.get(i).map(|t| kw(t, "AS")).unwrap_or(false) {
        bail!("expected AS after view name");
    }
    // Re-render the remaining tokens and hand them to the query parser.
    let rest: Vec<Token> = {
        let skip = words[..=i].len();
        let mut seen = 0;
        tokens
            .iter()
            .skip_while(|t| {
                if seen >= skip {
                    return false;
                }
                if !matches!(t, Token::Whitespace(_)) {
                    seen += 1;
                }
                true
            })
            .cloned()
            .collect()
    };
    let mut parser = Parser::new(&dialect).with_tokens(rest);
    let query = parser.parse_query().context("parse SELECT")?;
    // Allow a trailing WITH ... CHECK OPTION and/or semicolon; reject anything else.
    loop {
        match parser.next_token().token {
            Token::EOF => break,
            Token::SemiColon => {}
            Token::Word(w)
                if ["WITH", "CASCADED", "LOCAL", "CHECK", "OPTION"]
                    .iter()
                    .any(|k| w.value.eq_ignore_ascii_case(k)) => {}
            t => bail!("unexpected token after SELECT: {t}"),
        }
    }
    let name = parts.pop().unwrap();
    Ok(ParsedDef {
        schema: parts.pop(),
        name,
        columns,
        query: *query,
        hash: definition_hash(def),
    })
}

/// Every table a statement reads, including inside subqueries (used for full-refresh views and
/// before the catalog is consulted).
pub fn referenced_tables(query: &Query, default_schema: &str) -> Vec<TableRef> {
    let mut out = BTreeSet::new();
    let _ = visit_relations(query, |rel: &ObjectName| {
        if let Some(t) = object_table_ref(rel, default_schema) {
            out.insert(t);
        }
        ControlFlow::<()>::Continue(())
    });
    out.into_iter().collect()
}

fn object_table_ref(name: &ObjectName, default_schema: &str) -> Option<TableRef> {
    let idents: Vec<&str> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.as_str()),
            _ => None,
        })
        .collect::<Option<_>>()?;
    match idents.as_slice() {
        [t] => Some((default_schema.to_string(), t.to_string())),
        [s, t] => Some((s.to_string(), t.to_string())),
        _ => None,
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Spj => f.write_str("incremental (select/join)"),
            Mode::Grouped => f.write_str("incremental (grouped)"),
            Mode::Global => f.write_str("incremental (single-row aggregate)"),
            Mode::Full(r) => write!(f, "full rebuild ({r})"),
        }
    }
}

/// Classification of a view.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Select/project/join; unit = anchor (first table) primary key.
    Spj,
    /// GROUP BY; unit = group expressions.
    Grouped,
    /// Aggregates without GROUP BY: a single row recomputed on any change.
    Global,
    /// Unsupported for incremental maintenance; rebuilt on any change.
    Full(String),
}

/// Per table occurrence (alias) in the FROM clause.
#[derive(Debug, Clone)]
pub struct AliasPlan {
    /// Name the query uses to qualify this table's columns.
    pub name: String,
    pub table: Arc<TableInfo>,
    /// Columns used anywhere in the query.
    pub referenced: Vec<bool>,
    /// Columns that decide which unit a row contributes to (ON / WHERE / GROUP BY).
    pub membership: Vec<bool>,
    /// For each unit expression, the column of this table that equals it on every row this table
    /// contributes to. When set, units come straight from binlog images (no lookup query).
    pub direct_unit: Option<Vec<usize>>,
    /// Columns carried in literal row images for lookups (membership columns, ordinal order).
    pub image_cols: Vec<usize>,
    /// FROM clause with this alias replaced by `SUBST_MARKER`, split around it.
    pub subst_from: (String, String),
}

/// A parent table whose deletes/updates change rows of a table the view reads through a
/// cascading foreign key (such changes never reach the binlog).
#[derive(Debug, Clone)]
pub struct CascadeRoute {
    pub parent: Arc<TableInfo>,
    /// Ordinals of the referenced parent columns.
    pub parent_cols: Vec<usize>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    /// Parent column giving each unit value (the child's unit columns are the FK columns).
    /// `None`: affected units are unknown, the view is rebuilt.
    pub unit_from_parent: Option<Vec<usize>>,
}

#[derive(Debug, Clone)]
pub struct ViewPlan {
    pub schema: String,
    pub name: String,
    pub hash: String,
    pub mode: Mode,
    pub tables: Vec<TableRef>,
    pub aliases: Vec<AliasPlan>,
    /// Rendered projection items (with `AS` aliases where given).
    pub projection: Vec<String>,
    pub from_sql: String,
    pub where_sql: Option<String>,
    pub group_sql: Option<String>,
    pub having_sql: Option<String>,
    /// Unit expressions over base tables (empty for Global / Full).
    pub unit_exprs: Vec<String>,
    /// Projection index that already yields each unit expression, if any.
    pub unit_in_projection: Vec<Option<usize>>,
    /// Statement used for full rebuilds of `Full` views.
    pub full_select: String,
    /// Whether one unit maps to at most one materialized row.
    pub unit_unique: bool,
    pub cascades: Vec<CascadeRoute>,
}

impl ViewPlan {
    pub fn is_incremental(&self) -> bool {
        matches!(self.mode, Mode::Spj | Mode::Grouped)
    }

    /// SELECT producing materialized rows (projection + hidden unit columns), optionally
    /// restricted by `filter` (a predicate over `unit_exprs`).
    pub fn select_sql(&self, hidden_units: &[usize], filter: Option<&str>) -> String {
        if let Mode::Full(_) = self.mode {
            return self.full_select.clone();
        }
        let mut cols = self.projection.clone();
        for &u in hidden_units {
            cols.push(format!("{} AS `_lv_u{u}`", self.unit_exprs[u]));
        }
        let mut sql = format!("SELECT {} FROM {}", cols.join(", "), self.from_sql);
        push_where(&mut sql, self.where_sql.as_deref(), filter);
        if let Some(g) = &self.group_sql {
            sql.push_str(" GROUP BY ");
            sql.push_str(g);
        }
        if let Some(h) = &self.having_sql {
            sql.push_str(" HAVING ");
            sql.push_str(h);
        }
        sql
    }

    /// `SELECT DISTINCT <units> FROM <from with alias replaced by derived rows> WHERE ...`
    pub fn lookup_sql(&self, alias: usize, derived: &str) -> String {
        let a = &self.aliases[alias];
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}{}{}",
            self.unit_exprs.join(", "),
            a.subst_from.0,
            derived,
            a.subst_from.1
        );
        push_where(&mut sql, self.where_sql.as_deref(), None);
        sql
    }
}

fn push_where(sql: &mut String, where_sql: Option<&str>, filter: Option<&str>) {
    match (where_sql, filter) {
        (Some(w), Some(f)) => {
            sql.push_str(&format!(" WHERE ({w}) AND ({f})"));
        }
        (Some(w), None) => sql.push_str(&format!(" WHERE {w}")),
        (None, Some(f)) => sql.push_str(&format!(" WHERE {f}")),
        (None, None) => {}
    }
}

/// Column reference resolved against the FROM clause.
type ColRef = (usize, usize);

struct Scope<'a> {
    aliases: &'a [(String, Arc<TableInfo>)],
}

impl Scope<'_> {
    fn resolve(&self, e: &Expr) -> Option<ColRef> {
        match e {
            Expr::Identifier(id) => self.find_unqualified(&id.value),
            Expr::CompoundIdentifier(parts) => {
                let col = &parts.last()?.value;
                let tbl = &parts.get(parts.len().checked_sub(2)?)?.value;
                let schema = if parts.len() >= 3 {
                    Some(&parts[parts.len() - 3].value)
                } else {
                    None
                };
                let a = self.aliases.iter().position(|(n, t)| {
                    n.eq_ignore_ascii_case(tbl)
                        && schema.map(|s| t.schema.eq_ignore_ascii_case(s)).unwrap_or(true)
                })?;
                Some((a, self.aliases[a].1.col_index(col)?))
            }
            Expr::Nested(inner) => self.resolve(inner),
            _ => None,
        }
    }

    fn find_unqualified(&self, col: &str) -> Option<ColRef> {
        self.aliases
            .iter()
            .enumerate()
            .find_map(|(i, (_, t))| t.col_index(col).map(|c| (i, c)))
    }
}

struct Usage<'s> {
    scope: &'s Scope<'s>,
    select_aliases: Vec<String>,
    referenced: Vec<Vec<bool>>,
    membership: Vec<Vec<bool>>,
    unresolved: Option<String>,
}

impl Usage<'_> {
    fn mark<V: Visit>(&mut self, root: &V, set_membership: bool, allow_select_alias: bool) {
        let _ = visit_expressions(root, |e: &Expr| {
            if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                match self.scope.resolve(e) {
                    Some((a, c)) => {
                        self.referenced[a][c] = true;
                        if set_membership {
                            self.membership[a][c] = true;
                        }
                    }
                    None => {
                        let is_alias = matches!(e, Expr::Identifier(id)
                            if self.select_aliases.iter().any(|a| a.eq_ignore_ascii_case(&id.value)));
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

/// Union-find over (alias, column).
struct Classes {
    parent: HashMap<ColRef, ColRef>,
}

impl Classes {
    fn new() -> Self {
        Classes { parent: HashMap::new() }
    }
    fn find(&mut self, x: ColRef) -> ColRef {
        let p = *self.parent.get(&x).unwrap_or(&x);
        if p == x {
            return x;
        }
        let r = self.find(p);
        self.parent.insert(x, r);
        r
    }
    fn union(&mut self, a: ColRef, b: ColRef) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }
}

fn conjuncts<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(inner) => conjuncts(inner, out),
        _ => out.push(e),
    }
}

fn equalities(e: &Expr, scope: &Scope) -> Vec<(ColRef, ColRef)> {
    let mut cs = Vec::new();
    conjuncts(e, &mut cs);
    cs.into_iter()
        .filter_map(|c| match c {
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                Some((scope.resolve(left)?, scope.resolve(right)?))
            }
            _ => None,
        })
        .collect()
}

fn function_name(f: &sqlparser::ast::Function) -> String {
    match f.name.0.last() {
        Some(ObjectNamePart::Identifier(i)) => i.value.to_ascii_uppercase(),
        _ => String::new(),
    }
}

/// Reasons the query cannot be maintained incrementally, plus whether it aggregates.
fn scan_exprs(select: &Select) -> (Option<String>, bool) {
    let mut reason = None;
    let mut has_agg = false;
    let mut check = |e: &Expr| {
        match e {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                reason.get_or_insert_with(|| "subquery".to_string());
            }
            Expr::Function(f) => {
                let n = function_name(f);
                if f.over.is_some() {
                    reason.get_or_insert_with(|| "window function".to_string());
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
    (reason, has_agg)
}

fn quote(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

fn qident(id: &str) -> Ident {
    Ident::with_quote('`', id)
}

/// Builds the plan. `tables` must contain metadata for every table the query reads.
pub fn plan(def: &ParsedDef, default_schema: &str, tables: &HashMap<TableRef, Arc<TableInfo>>) -> Result<ViewPlan> {
    let schema = def.schema.clone().unwrap_or_else(|| default_schema.to_string());
    let all_tables = referenced_tables(&def.query, default_schema);
    for t in &all_tables {
        if !tables.contains_key(t) {
            bail!("table `{}`.`{}` does not exist", t.0, t.1);
        }
    }
    let mut query = def.query.clone();
    query.order_by = None;

    let full = |reason: &str, query: &Query| ViewPlan {
        schema: schema.clone(),
        name: def.name.clone(),
        hash: def.hash.clone(),
        mode: Mode::Full(reason.to_string()),
        tables: all_tables.clone(),
        aliases: Vec::new(),
        projection: Vec::new(),
        from_sql: String::new(),
        where_sql: None,
        group_sql: None,
        having_sql: None,
        unit_exprs: Vec::new(),
        unit_in_projection: Vec::new(),
        full_select: query.to_string(),
        unit_unique: false,
        cascades: cascade_routes(&all_tables.iter().filter_map(|t| tables.get(t).map(|i| (i.clone(), None))).collect::<Vec<_>>(), tables),
    };

    if query.with.is_some() {
        return Ok(full("WITH clause", &query));
    }
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return Ok(full("LIMIT", &query));
    }
    let select: &mut Select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => return Ok(full("set operation (UNION/INTERSECT/EXCEPT)", &def.query)),
    };
    if select.distinct.is_some() {
        return Ok(full("DISTINCT", &query));
    }
    if select.from.is_empty() {
        return Ok(full("no FROM clause", &query));
    }

    // FROM: tables and joins we know how to maintain.
    let mut aliases: Vec<(String, Arc<TableInfo>)> = Vec::new();
    let mut alias_pos: Vec<(usize, Option<usize>)> = Vec::new();
    // (alias index of the joined table, is_left_join, constraint)
    let mut joins: Vec<(usize, bool, JoinConstraint)> = Vec::new();
    let mut unsupported = None;
    let add_factor = |f: &TableFactor, pos: (usize, Option<usize>), aliases: &mut Vec<(String, Arc<TableInfo>)>, alias_pos: &mut Vec<(usize, Option<usize>)>| -> Option<String> {
        match f {
            TableFactor::Table { name, alias, args: None, .. } => {
                let t = object_table_ref(name, default_schema)?;
                let info = tables.get(&t)?.clone();
                let n = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| t.1.clone());
                aliases.push((n, info));
                alias_pos.push(pos);
                None
            }
            TableFactor::Derived { .. } => Some("derived table in FROM".into()),
            _ => Some("unsupported FROM item".into()),
        }
    };
    for (ti, twj) in select.from.iter().enumerate() {
        if let Some(r) = add_factor(&twj.relation, (ti, None), &mut aliases, &mut alias_pos) {
            unsupported.get_or_insert(r);
        }
        for (ji, j) in twj.joins.iter().enumerate() {
            let (left, c) = match &j.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => (false, c.clone()),
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (true, c.clone()),
                _ => {
                    unsupported.get_or_insert("RIGHT/FULL/other join".into());
                    continue;
                }
            };
            if matches!(c, JoinConstraint::Natural) {
                unsupported.get_or_insert("NATURAL join".into());
            }
            if let Some(r) = add_factor(&j.relation, (ti, Some(ji)), &mut aliases, &mut alias_pos) {
                unsupported.get_or_insert(r);
            } else {
                joins.push((aliases.len() - 1, left, c));
            }
        }
    }
    if let Some(r) = unsupported {
        return Ok(full(&r, &query));
    }

    // Expand wildcards so the projection order is explicit.
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard(_) => {
                for (n, t) in &aliases {
                    for c in t.columns.iter().filter(|c| !c.invisible) {
                        projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![qident(n), qident(&c.name)])));
                    }
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(on), _) => {
                let q = match on.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => i.value.clone(),
                    _ => bail!("bad qualified wildcard"),
                };
                let (n, t) = aliases
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&q))
                    .ok_or_else(|| anyhow!("unknown table {q}.*"))?;
                for c in t.columns.iter().filter(|c| !c.invisible) {
                    projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![qident(n), qident(&c.name)])));
                }
            }
            SelectItem::QualifiedWildcard(..) => bail!("unsupported wildcard"),
            other => projection.push(other),
        }
    }
    if !def.columns.is_empty() {
        if def.columns.len() != projection.len() {
            bail!("view column list has {} names but SELECT returns {} columns", def.columns.len(), projection.len());
        }
        projection = projection
            .into_iter()
            .zip(&def.columns)
            .map(|(item, name)| match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    SelectItem::ExprWithAlias { expr, alias: qident(name) }
                }
                other => other,
            })
            .collect();
    }
    select.projection = projection;

    let (reason, has_agg) = scan_exprs(select);
    if let Some(r) = reason {
        return Ok(full(&r, &query));
    }

    let scope = Scope { aliases: &aliases };
    let n_alias = aliases.len();
    let referenced: Vec<Vec<bool>> = aliases.iter().map(|(_, t)| vec![false; t.columns.len()]).collect();
    let membership = referenced.clone();

    let proj_exprs: Vec<(&Expr, Option<&str>)> = select
        .projection
        .iter()
        .map(|i| match i {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => unreachable!(),
        })
        .collect();

    // Column usage. Unresolvable identifiers in GROUP BY / HAVING may be select aliases.
    let mut usage = Usage {
        scope: &scope,
        select_aliases: proj_exprs.iter().filter_map(|(_, a)| a.map(str::to_string)).collect(),
        referenced,
        membership,
        unresolved: None,
    };
    usage.mark(&select.projection, false, false);
    usage.mark(&select.selection, true, false);
    for (_, _, c) in &joins {
        if let JoinConstraint::On(e) = c {
            usage.mark(e, true, false);
        }
    }
    usage.mark(&select.having, false, true);

    // USING (col): the column of the joined table and the first earlier table having it.
    let mut using_pairs: Vec<(usize, ColRef, ColRef)> = Vec::new();
    for (ja, _, c) in &joins {
        if let JoinConstraint::Using(cols) = c {
            for on in cols {
                let col = match on.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => i.value.clone(),
                    _ => bail!("bad USING column"),
                };
                let rc = aliases[*ja].1.col_index(&col).ok_or_else(|| anyhow!("USING column {col} not found"))?;
                let (la, lc) = (0..*ja)
                    .find_map(|a| aliases[a].1.col_index(&col).map(|c| (a, c)))
                    .ok_or_else(|| anyhow!("USING column {col} not found"))?;
                for (a, c) in [(*ja, rc), (la, lc)] {
                    usage.referenced[a][c] = true;
                    usage.membership[a][c] = true;
                }
                using_pairs.push((*ja, (*ja, rc), (la, lc)));
            }
        }
    }

    // GROUP BY: resolve ordinals and select aliases to the underlying expressions.
    let group_exprs: Vec<Expr> = match &select.group_by {
        GroupByExpr::Expressions(es, mods) if mods.is_empty() => es.clone(),
        GroupByExpr::Expressions(..) => return Ok(full("GROUP BY modifiers (ROLLUP)", &query)),
        GroupByExpr::All(_) => return Ok(full("GROUP BY ALL", &query)),
    };
    let mut resolved_groups = Vec::new();
    for g in &group_exprs {
        let r = match g {
            Expr::Value(v) if matches!(v.value, Value::Number(..)) => {
                let n: usize = v.value.to_string().parse().context("GROUP BY ordinal")?;
                proj_exprs.get(n.wrapping_sub(1)).map(|(e, _)| (*e).clone()).ok_or_else(|| anyhow!("GROUP BY {n} out of range"))?
            }
            Expr::Identifier(id) if scope.resolve(g).is_none() => proj_exprs
                .iter()
                .find(|(_, a)| a.map(|a| a.eq_ignore_ascii_case(&id.value)).unwrap_or(false))
                .map(|(e, _)| (*e).clone())
                .ok_or_else(|| anyhow!("cannot resolve GROUP BY {id}"))?,
            other => other.clone(),
        };
        resolved_groups.push(r);
    }
    for g in &resolved_groups {
        usage.mark(g, true, false);
    }
    if let Some(u) = usage.unresolved {
        bail!("cannot resolve column {u}");
    }
    let Usage { mut referenced, mut membership, .. } = usage;

    let mode = if !resolved_groups.is_empty() {
        Mode::Grouped
    } else if has_agg {
        Mode::Global
    } else {
        Mode::Spj
    };

    // Units.
    let unit_refs: Vec<Expr> = match mode {
        Mode::Spj => {
            let (n, t) = &aliases[0];
            if t.pk.is_empty() {
                return Ok(full(&format!("first table `{}` has no primary key", t.name), &query));
            }
            t.pk
                .iter()
                .map(|&c| {
                    referenced[0][c] = true;
                    membership[0][c] = true;
                    Expr::CompoundIdentifier(vec![qident(n), qident(&t.columns[c].name)])
                })
                .collect()
        }
        Mode::Grouped => resolved_groups.clone(),
        _ => Vec::new(),
    };
    let unit_cols: Vec<Option<ColRef>> = unit_refs.iter().map(|e| scope.resolve(e)).collect();
    let unit_exprs: Vec<String> = unit_refs.iter().map(|e| e.to_string()).collect();
    let unit_in_projection: Vec<Option<usize>> = unit_refs
        .iter()
        .zip(&unit_cols)
        .map(|(u, uc)| {
            proj_exprs.iter().position(|(e, _)| match uc {
                Some(uc) => scope.resolve(e) == Some(*uc),
                None => e.to_string() == u.to_string(),
            })
        })
        .collect();

    // Equality classes valid for rows that involve a given alias.
    let mut base_edges = Vec::new();
    if let Some(w) = &select.selection {
        base_edges.extend(equalities(w, &scope));
    }
    let mut own_edges: Vec<Vec<(ColRef, ColRef)>> = vec![Vec::new(); n_alias];
    for (ja, left, c) in &joins {
        let eqs = match c {
            JoinConstraint::On(e) => equalities(e, &scope),
            _ => Vec::new(),
        };
        if *left {
            // Valid only where the joined table is non-null and only if it participates.
            own_edges[*ja].extend(eqs.into_iter().filter(|(x, y)| x.0 == *ja || y.0 == *ja));
        } else {
            base_edges.extend(eqs);
        }
    }
    for (ja, l, r) in &using_pairs {
        let left_join = joins.iter().any(|(a, left, _)| a == ja && *left);
        if left_join {
            own_edges[*ja].push((*l, *r));
        } else {
            base_edges.push((*l, *r));
        }
    }

    let from_sql = select
        .from
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mut alias_plans = Vec::new();
    for (ai, (n, t)) in aliases.iter().enumerate() {
        let direct_unit = if unit_cols.is_empty() || unit_cols.iter().any(|u| u.is_none()) {
            None
        } else {
            let mut cls = Classes::new();
            for (x, y) in base_edges.iter().chain(&own_edges[ai]) {
                cls.union(*x, *y);
            }
            unit_cols
                .iter()
                .map(|u| {
                    let u = u.unwrap();
                    if !aliases[u.0].1.columns[u.1].exact_key() {
                        return None;
                    }
                    if u.0 == ai {
                        return Some(u.1);
                    }
                    let ru = cls.find(u);
                    (0..t.columns.len()).find(|&c| t.columns[c].exact_key() && cls.find((ai, c)) == ru)
                })
                .collect::<Option<Vec<usize>>>()
        };
        let image_cols: Vec<usize> = (0..t.columns.len()).filter(|&c| membership[ai][c]).collect();
        let subst_from = render_substituted_from(&select.from, alias_pos[ai], n)?;
        alias_plans.push(AliasPlan {
            name: n.clone(),
            table: t.clone(),
            referenced: referenced[ai].clone(),
            membership: membership[ai].clone(),
            direct_unit,
            image_cols,
            subst_from,
        });
    }

    let unit_unique = match mode {
        Mode::Spj => n_alias == 1,
        Mode::Grouped => true,
        _ => false,
    };
    Ok(ViewPlan {
        schema,
        name: def.name.clone(),
        hash: def.hash.clone(),
        mode,
        tables: all_tables.clone(),
                projection: select.projection.iter().map(|p| p.to_string()).collect(),
        from_sql,
        where_sql: select.selection.as_ref().map(|e| e.to_string()),
        group_sql: if group_exprs.is_empty() {
            None
        } else {
            Some(group_exprs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "))
        },
        having_sql: select.having.as_ref().map(|e| e.to_string()),
        unit_exprs,
        unit_in_projection,
        full_select: query.to_string(),
        unit_unique,
        cascades: cascade_routes(
            &alias_plans.iter().map(|a| (a.table.clone(), a.direct_unit.clone())).collect::<Vec<_>>(),
            tables,
        ),
        aliases: alias_plans,
    })
}

/// Cascading-FK parents (and, conservatively, their ancestors) of the given child tables.
fn cascade_routes(children: &[(Arc<TableInfo>, Option<Vec<usize>>)], tables: &HashMap<TableRef, Arc<TableInfo>>) -> Vec<CascadeRoute> {
    let mut out = Vec::new();
    let mut stack: Vec<(Arc<TableInfo>, Option<Vec<usize>>, usize)> = children.iter().map(|(t, d)| (t.clone(), d.clone(), 0)).collect();
    let mut seen: BTreeSet<(TableRef, TableRef)> = BTreeSet::new();
    while let Some((child, direct, depth)) = stack.pop() {
        for fk in &child.cascades {
            let Some(parent) = tables.get(&fk.parent) else { continue };
            if !seen.insert(((child.schema.clone(), child.name.clone()), fk.parent.clone())) && direct.is_none() {
                continue;
            }
            let parent_cols: Option<Vec<usize>> = fk.parent_cols.iter().map(|n| parent.col_index(n)).collect();
            let Some(parent_cols) = parent_cols else { continue };
            // Units follow from the parent image only if every unit column of the child is an
            // FK column with an exact representation on both sides.
            let unit_from_parent = direct.as_ref().and_then(|cols| {
                cols.iter()
                    .map(|c| {
                        let j = fk.child_cols.iter().position(|x| x == c)?;
                        let pc = parent_cols[j];
                        (child.columns[*c].exact_key() && parent.columns[pc].exact_key()).then_some(pc)
                    })
                    .collect::<Option<Vec<usize>>>()
            });
            out.push(CascadeRoute {
                parent: parent.clone(),
                parent_cols,
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                unit_from_parent,
            });
            if depth < 8 {
                // A cascade into the parent cascades further into the child: unknown units.
                stack.push((parent.clone(), None, depth + 1));
            }
        }
    }
    out
}

fn render_substituted_from(from: &[TableWithJoins], pos: (usize, Option<usize>), alias: &str) -> Result<(String, String)> {
    let mut from = from.to_vec();
    let marker = TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(SUBST_MARKER))]),
        alias: Some(TableAlias { explicit: true, name: qident(alias), columns: vec![], at: None }),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    };
    match pos {
        (t, None) => from[t].relation = marker,
        (t, Some(j)) => from[t].joins[j].relation = marker,
    }
    let s = from.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ");
    let (a, b) = s
        .split_once(SUBST_MARKER)
        .ok_or_else(|| anyhow!("internal: substitution marker missing"))?;
    Ok((a.to_string(), b.to_string()))
}

pub fn quote_ident(id: &str) -> String {
    quote(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Column;

    fn table(name: &str, cols: &[(&str, &str)], pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
        let t = TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols
                .iter()
                .map(|(n, ty)| Column::simple(n, ty))
                .collect(),
            pk: pk.to_vec(),
            cascades: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    }

    fn catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        [
            table("orders", &[("id", "int"), ("customer_id", "int"), ("status", "varchar"), ("total", "decimal"), ("note", "text")], &[0]),
            table("customers", &[("id", "int"), ("name", "varchar"), ("region_id", "int")], &[0]),
            table("regions", &[("id", "int"), ("name", "varchar")], &[0]),
            table("order_items", &[("order_id", "int"), ("line", "int"), ("qty", "int"), ("price", "decimal")], &[0, 1]),
            table("logs", &[("msg", "varchar")], &[]),
        ]
        .into_iter()
        .collect()
    }

    fn p(sql: &str) -> ViewPlan {
        let d = parse_definition(sql).unwrap();
        plan(&d, "db", &catalog()).unwrap()
    }

    #[test]
    fn parse_noise() {
        let d = parse_definition(
            "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `db`.`v1` (a, b) AS SELECT id, status FROM orders WITH CASCADED CHECK OPTION;",
        )
        .unwrap();
        assert_eq!(d.name, "v1");
        assert_eq!(d.schema.as_deref(), Some("db"));
        assert_eq!(d.columns, vec!["a", "b"]);
        assert!(parse_definition("SELECT 1").is_err());
        assert!(parse_definition("CREATE TABLE t (a int)").is_err());
        assert!(parse_definition("CREATE VIEW v AS SELECT 1 garbage garbage").is_err());
    }

    #[test]
    fn single_table_spj() {
        let v = p("CREATE VIEW v AS SELECT id, status FROM orders WHERE total > 10");
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.unit_exprs, vec!["`orders`.`id`"]);
        assert_eq!(v.unit_in_projection, vec![Some(0)]);
        assert!(v.unit_unique);
        let a = &v.aliases[0];
        assert_eq!(a.direct_unit, Some(vec![0]));
        assert_eq!(a.referenced, vec![true, false, true, true, false]);
        assert_eq!(a.membership, vec![true, false, false, true, false]);
    }

    #[test]
    fn join_direct_units_via_equalities() {
        let v = p("CREATE VIEW v AS SELECT o.id, c.name, r.name AS region, i.qty \
                   FROM orders o JOIN customers c ON c.id = o.customer_id \
                   LEFT JOIN regions r ON r.id = c.region_id \
                   LEFT JOIN order_items i ON i.order_id = o.id");
        assert_eq!(v.mode, Mode::Spj);
        assert!(!v.unit_unique);
        let by = |n: &str| v.aliases.iter().find(|a| a.name == n).unwrap();
        assert_eq!(by("o").direct_unit, Some(vec![0]));
        assert_eq!(by("i").direct_unit, Some(vec![0]), "items map to unit via i.order_id = o.id");
        assert_eq!(by("c").direct_unit, None, "customers need a lookup");
        assert_eq!(by("r").direct_unit, None);
        assert_eq!(by("c").subst_from.0, "orders o JOIN ");
        let look = v.lookup_sql(1, "(SELECT ? AS `id`)");
        assert!(look.starts_with("SELECT DISTINCT `o`.`id` FROM orders o JOIN (SELECT ? AS `id`) AS `c` ON"), "{look}");
    }

    #[test]
    fn grouped() {
        let v = p("CREATE VIEW v AS SELECT o.customer_id AS cust, COUNT(*) n, SUM(i.qty * i.price) amt \
                   FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY cust HAVING n > 1");
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(v.unit_exprs, vec!["o.customer_id"]);
        assert_eq!(v.unit_in_projection, vec![Some(0)]);
        assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
        assert_eq!(v.aliases[1].direct_unit, None);
        assert!(v.aliases[1].membership[0], "join column is membership");
        assert!(!v.aliases[1].membership[2], "qty only aggregated");
        assert!(v.aliases[1].referenced[2]);
        let s = v.select_sql(&[], Some("o.customer_id IN (1)"));
        assert!(s.ends_with("WHERE o.customer_id IN (1) GROUP BY cust HAVING n > 1"), "{s}");

        let v = p("CREATE VIEW v AS SELECT DATE(o.note) d, COUNT(*) FROM orders o GROUP BY 1");
        assert_eq!(v.unit_exprs, vec!["DATE(o.note)"]);
        assert_eq!(v.aliases[0].direct_unit, None);
    }

    #[test]
    fn global_and_full() {
        assert_eq!(p("CREATE VIEW v AS SELECT COUNT(*) FROM orders").mode, Mode::Global);
        for sql in [
            "CREATE VIEW v AS SELECT DISTINCT status FROM orders",
            "CREATE VIEW v AS SELECT id FROM orders LIMIT 5",
            "CREATE VIEW v AS SELECT id FROM orders UNION SELECT id FROM customers",
            "CREATE VIEW v AS SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
            "CREATE VIEW v AS SELECT msg FROM logs",
            "CREATE VIEW v AS SELECT id, NOW() FROM orders",
            "CREATE VIEW v AS SELECT o.id FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
        ] {
            let v = p(sql);
            assert!(matches!(v.mode, Mode::Full(_)), "{sql}: {:?}", v.mode);
            assert!(!v.tables.is_empty());
        }
        let v = p("CREATE VIEW v AS SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)");
        assert_eq!(v.tables.len(), 2);
    }

    #[test]
    fn wildcards_and_columns() {
        let v = p("CREATE VIEW v (a, b, c) AS SELECT r.*, 1 FROM regions r");
        assert_eq!(v.projection, vec!["`r`.`id` AS `a`", "`r`.`name` AS `b`", "1 AS `c`"]);
        assert!(parse_definition("CREATE VIEW v AS SELECT * FROM nope")
            .and_then(|d| plan(&d, "db", &catalog()))
            .is_err());
        assert!(parse_definition("CREATE VIEW v AS SELECT bogus FROM orders")
            .and_then(|d| plan(&d, "db", &catalog()))
            .is_err());
    }

    #[test]
    fn composite_and_using() {
        let v = p("CREATE VIEW v AS SELECT i.order_id, i.line, o.status FROM order_items i JOIN orders o ON o.id = i.order_id");
        assert_eq!(v.unit_exprs.len(), 2);
        assert_eq!(v.aliases[1].direct_unit, None, "orders map to only part of the composite unit");
        let v = p("CREATE VIEW v AS SELECT o.id, o.status FROM orders o JOIN customers c ON c.id = o.customer_id");
        assert_eq!(v.aliases[1].direct_unit, None);
        let v = p("CREATE VIEW v AS SELECT r.id, c.name FROM regions r JOIN customers c ON c.region_id = r.id");
        assert_eq!(v.aliases[1].direct_unit, Some(vec![2]));
    }
}
