//! Turns a `CREATE VIEW` statement into a [`ViewPlan`]: what to watch, how a changed row maps to
//! the materialized rows it affects ("units"), and pre-rendered SQL for every refresh query.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, visit_relations, BinaryOperator, DataType, Expr,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableAlias, TableFactor, TableWithJoins,
    UnaryOperator, Value, Visit,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Word};

use crate::catalog::{ColKind, FkAction, TableInfo, TableRef};

/// Placeholder table name used to render "FROM with one table replaced by literal rows".
const SUBST_MARKER: &str = "__lv_subst_marker__";

const AGGREGATES: &[&str] = &[
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

const NONDETERMINISTIC: &[&str] = &[
    "NOW",
    "SYSDATE",
    "CURDATE",
    "CURTIME",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "UNIX_TIMESTAMP",
    "RAND",
    "UUID",
    "UUID_SHORT",
    "CONNECTION_ID",
    "USER",
    "CURRENT_USER",
    "SESSION_USER",
    "SYSTEM_USER",
    "LAST_INSERT_ID",
    "FOUND_ROWS",
    "ROW_COUNT",
    "SLEEP",
    "BENCHMARK",
];

/// lazyview's own tables: never valid as view sources or view names.
const METADATA_TABLES: &[&str] = &[
    "_lazyview",
    "_lazyview_state",
    "_lazyview_status",
    "_lazyview_overview",
];

/// Functions whose result is not NULL when an argument is NULL; a predicate using them may
/// accept the NULL-extended rows of a LEFT JOIN.
const NULL_TOLERANT: &[&str] = &[
    "COALESCE",
    "IFNULL",
    "IF",
    "NULLIF",
    "ISNULL",
    "CONCAT_WS",
    "NVL",
    "IIF",
    "ELT",
    "FIELD",
    "MAKE_SET",
    "JSON_ARRAY",
    "JSON_OBJECT",
    "JSON_QUOTE",
    "QUOTE",
];

/// Functions returning JSON values (JSON group keys never match their own refresh filter).
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
];

/// SELECT modifiers that may precede the first projection item.
const SELECT_MODIFIERS: &[&str] = &[
    "ALL",
    "DISTINCT",
    "DISTINCTROW",
    "HIGH_PRIORITY",
    "STRAIGHT_JOIN",
    "SQL_SMALL_RESULT",
    "SQL_BIG_RESULT",
    "SQL_BUFFER_RESULT",
    "SQL_NO_CACHE",
    "SQL_CACHE",
    "SQL_CALC_FOUND_ROWS",
];

/// Reserved words that end a projection list.
const PROJECTION_END: &[&str] = &[
    "FROM",
    "INTO",
    "WHERE",
    "GROUP",
    "HAVING",
    "WINDOW",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "FOR",
    "LOCK",
];

/// The statement as written in `_lazyview.definition`.
#[derive(Debug, Clone)]
pub struct ParsedDef {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<String>,
    pub query: Query,
    pub hash: String,
    /// The SELECT exactly as written (escapes, spacing and comments included): statements run as
    /// written get the same column names from the server as the real view.
    pub query_sql: String,
    /// Source text of each projection item of a plain top-level SELECT (None where it cannot be
    /// recovered exactly; empty for other query shapes).
    pub item_sql: Vec<Option<String>>,
}

pub fn definition_hash(def: &str) -> String {
    hex::encode(Sha256::digest(def.trim().as_bytes()))[..16].to_string()
}

/// Parses `CREATE [OR REPLACE] [ALGORITHM=..] [DEFINER=..] [SQL SECURITY ..] VIEW name [(cols)] AS
/// SELECT ... [WITH [CASCADED|LOCAL] CHECK OPTION]`.
pub fn parse_definition(def: &str) -> Result<ParsedDef> {
    let dialect = MySqlDialect {};
    let tokens = tokenize(def)?;
    // Indexes of the non-whitespace tokens.
    let words: Vec<usize> = (0..tokens.len())
        .filter(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
        .collect();
    let word = |i: usize| words.get(i).map(|&w| &tokens[w].token);
    let kw = |t: &Token, k: &str| matches!(t, Token::Word(Word { value, quote_style: None, .. }) if value.eq_ignore_ascii_case(k));
    if word(0).map(|t| kw(t, "CREATE")) != Some(true) {
        bail!("definition must start with CREATE ... VIEW");
    }
    let view_at = (0..words.len())
        .position(|i| kw(&tokens[words[i]].token, "VIEW"))
        .ok_or_else(|| anyhow!("definition must be a CREATE VIEW statement"))?;
    let mut i = view_at + 1;
    let mut parts = Vec::new();
    loop {
        match word(i) {
            Some(Token::Word(w)) => parts.push(unquote(&w.value, w.quote_style)),
            _ => bail!("expected view name after VIEW"),
        }
        i += 1;
        if matches!(word(i), Some(Token::Period)) {
            i += 1;
            continue;
        }
        break;
    }
    let mut columns = Vec::new();
    if matches!(word(i), Some(Token::LParen)) {
        i += 1;
        loop {
            match word(i) {
                Some(Token::Word(w)) => columns.push(unquote(&w.value, w.quote_style)),
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
    if !word(i).map(|t| kw(t, "AS")).unwrap_or(false) {
        bail!("expected AS after view name");
    }
    // Hand the remaining tokens (with their locations in `def`) to the query parser.
    let rest = &tokens[words[i] + 1..];
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(rest.to_vec());
    let query = parser.parse_query().context("parse SELECT")?;
    let tail = parser.peek_token().span.start;
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
    let offsets = Offsets::new(def);
    // The SELECT as written: its first token up to the last one before the trailing clauses.
    let end = rest
        .iter()
        .position(|t| tail.line != 0 && t.span.start == tail)
        .unwrap_or(rest.len());
    let body: Vec<&TokenWithSpan> = rest[..end]
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();
    let query_sql = body
        .first()
        .zip(body.last())
        .and_then(|(a, b)| offsets.text(a, b))
        .filter(|sql| {
            parse_sql(sql, |p| p.parse_query()).map(|q| q.to_string()) == Some(query.to_string())
        })
        .map(str::to_string)
        .unwrap_or_else(|| query.to_string());
    let item_sql = match query.body.as_ref() {
        SetExpr::Select(sel) => select_item_sql(&tokens, sel, &offsets),
        _ => Vec::new(),
    };
    let name = parts.pop().unwrap();
    Ok(ParsedDef {
        schema: parts.pop(),
        name,
        columns,
        query: *query,
        hash: definition_hash(def),
        query_sql,
        item_sql,
    })
}

/// Definitions are tokenized without unescaping: string literals keep their escapes (`'a\'b'`,
/// `'x\_y'`), so rendered SQL means what was written, and quoted identifiers keep doubled quote
/// characters (see [`unquote`]).
fn tokenize(sql: &str) -> Result<Vec<TokenWithSpan>> {
    Tokenizer::new(&MySqlDialect {}, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|e| anyhow!("tokenize: {e}"))
}

/// Parses all of `sql` with `f`; None if it fails or leaves tokens over.
fn parse_sql<T>(sql: &str, f: impl FnOnce(&mut Parser) -> Result<T, ParserError>) -> Option<T> {
    let mut p = Parser::new(&MySqlDialect {}).with_tokens_with_locations(tokenize(sql).ok()?);
    let v = f(&mut p).ok()?;
    (p.peek_token().token == Token::EOF).then_some(v)
}

/// The name a (raw-tokenized) identifier stands for: a doubled quote character inside a quoted
/// identifier is one character of the name.
fn unquote(value: &str, quote: Option<char>) -> String {
    match quote {
        Some(q) => value.replace(&format!("{q}{q}"), &q.to_string()),
        None => value.to_string(),
    }
}

fn ident_name(i: &Ident) -> String {
    unquote(&i.value, i.quote_style)
}

/// Maps tokenizer locations (1-based lines and character columns) to byte offsets of a string.
struct Offsets<'a> {
    sql: &'a str,
    line_starts: Vec<usize>,
}

impl<'a> Offsets<'a> {
    fn new(sql: &'a str) -> Self {
        let line_starts = std::iter::once(0)
            .chain(sql.match_indices('\n').map(|(i, _)| i + 1))
            .collect();
        Offsets { sql, line_starts }
    }

    fn at(&self, loc: Location) -> Option<usize> {
        let line = usize::try_from(loc.line).ok()?.checked_sub(1)?;
        let col = usize::try_from(loc.column).ok()?.checked_sub(1)?;
        let start = *self.line_starts.get(line)?;
        self.sql[start..]
            .char_indices()
            .map(|(i, _)| start + i)
            .chain(std::iter::once(self.sql.len()))
            .nth(col)
    }

    /// Source text from the start of token `a` to the end of token `b`.
    fn text(&self, a: &TokenWithSpan, b: &TokenWithSpan) -> Option<&'a str> {
        self.sql.get(self.at(a.span.start)?..self.at(b.span.end)?)
    }
}

/// Source text of each projection item of `select`, kept only where it parses back to the same
/// item.
fn select_item_sql(
    tokens: &[TokenWithSpan],
    select: &Select,
    offsets: &Offsets,
) -> Vec<Option<String>> {
    match split_projection(tokens, select.select_token.0.span.start, offsets) {
        Some(texts) if texts.len() == select.projection.len() => texts
            .into_iter()
            .zip(&select.projection)
            .map(|(text, item)| {
                let parsed = parse_sql(text, |p| p.parse_select_item())?;
                (parsed.to_string() == item.to_string()).then(|| text.to_string())
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Splits the projection list after the `SELECT` keyword at `select_at` at its top-level commas.
fn split_projection<'a>(
    tokens: &[TokenWithSpan],
    select_at: Location,
    offsets: &Offsets<'a>,
) -> Option<Vec<&'a str>> {
    let is_word = |t: &Token, list: &[&str]| matches!(t, Token::Word(w) if w.quote_style.is_none() && list.iter().any(|k| w.value.eq_ignore_ascii_case(k)));
    let mut i = tokens.iter().position(|t| t.span.start == select_at)? + 1;
    while tokens.get(i).is_some_and(|t| {
        matches!(t.token, Token::Whitespace(_)) || is_word(&t.token, SELECT_MODIFIERS)
    }) {
        i += 1;
    }
    let mut items = Vec::new();
    let (mut depth, mut first, mut last) = (0usize, None, None);
    loop {
        let t = tokens.get(i);
        let at_end = match t.map(|t| &t.token) {
            None | Some(Token::EOF | Token::SemiColon) => true,
            Some(Token::Comma | Token::RParen) => depth == 0,
            Some(tok) => depth == 0 && is_word(tok, PROJECTION_END),
        };
        if at_end {
            items.push(offsets.text(first?, last?)?);
            if !matches!(t.map(|t| &t.token), Some(Token::Comma)) {
                return Some(items);
            }
            (first, last) = (None, None);
        } else {
            match t.map(|t| &t.token) {
                Some(Token::LParen) => depth += 1,
                Some(Token::RParen) => depth -= 1,
                _ => {}
            }
            if !matches!(t.map(|t| &t.token), Some(Token::Whitespace(_))) {
                first = first.or(t);
                last = t;
            }
        }
        i += 1;
    }
}

/// Every table a statement reads, including inside subqueries (used for full-refresh views and
/// before the catalog is consulted).
pub fn referenced_tables(query: &Query, default_schema: &str) -> Vec<TableRef> {
    let ctes = cte_names(query);
    let mut out = BTreeSet::new();
    let _ = visit_relations(query, |rel: &ObjectName| {
        let single = rel.0.len() == 1;
        if let Some(t) = object_table_ref(rel, default_schema) {
            if !(single && ctes.iter().any(|c| c.eq_ignore_ascii_case(&t.1))) {
                out.insert(t);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    out.into_iter().collect()
}

/// Names defined by `WITH` clauses anywhere in the query (they are not tables).
fn cte_names(query: &Query) -> Vec<String> {
    fn walk(q: &Query, out: &mut Vec<String>) {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                out.push(ident_name(&c.alias.name));
                walk(&c.query, out);
            }
        }
        walk_set(&q.body, out);
    }
    fn walk_set(s: &SetExpr, out: &mut Vec<String>) {
        match s {
            SetExpr::Query(q) => walk(q, out),
            SetExpr::SetOperation { left, right, .. } => {
                walk_set(left, out);
                walk_set(right, out);
            }
            SetExpr::Select(sel) => {
                let _ = visit_expressions(&**sel, |e: &Expr| {
                    match e {
                        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => walk(q, out),
                        Expr::InSubquery { subquery, .. } => walk(subquery, out),
                        _ => {}
                    }
                    ControlFlow::<()>::Continue(())
                });
                for twj in &sel.from {
                    for f in
                        std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation))
                    {
                        if let TableFactor::Derived { subquery, .. } = f {
                            walk(subquery, out);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(query, &mut out);
    out
}

/// Tables read by a stored SELECT (e.g. a SQL view's definition); empty if it does not parse.
pub fn select_tables(select_sql: &str, default_schema: &str) -> Vec<TableRef> {
    match parse_sql(select_sql, |p| p.parse_query()) {
        Some(q) => referenced_tables(&q, default_schema),
        None => Vec::new(),
    }
}

/// Every function called anywhere in the query, as `(schema, name)` (schema only when
/// qualified). The caller rejects stored functions and UDFs: their reads are invisible.
pub fn function_calls(query: &Query) -> Vec<(Option<String>, String)> {
    let mut out = BTreeSet::new();
    let _ = visit_expressions(query, |e: &Expr| {
        if let Expr::Function(f) = e {
            let parts: Vec<String> = f
                .name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(ident_name(i)),
                    _ => None,
                })
                .collect();
            match parts.as_slice() {
                [n] => {
                    out.insert((None, n.clone()));
                }
                [s, n] => {
                    out.insert((Some(s.clone()), n.clone()));
                }
                _ => {}
            }
        }
        ControlFlow::<()>::Continue(())
    });
    out.into_iter().collect()
}

fn object_table_ref(name: &ObjectName, default_schema: &str) -> Option<TableRef> {
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
    /// Column names from `CREATE VIEW v (a, b, ...)`, applied positionally to the materialized
    /// table (the SELECT itself keeps its own aliases so GROUP BY / HAVING resolve as written).
    pub output_names: Option<Vec<String>>,
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

    fn same_family(&self, a: ColRef, b: ColRef) -> bool {
        self.aliases[a.0].1.columns[a.1].family() == self.aliases[b.0].1.columns[b.1].family()
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

/// Union-find over (alias, column).
struct Classes {
    parent: HashMap<ColRef, ColRef>,
}

impl Classes {
    fn new() -> Self {
        Classes {
            parent: HashMap::new(),
        }
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

fn equalities(e: &Expr, scope: &Scope) -> Vec<(ColRef, ColRef)> {
    let mut cs = Vec::new();
    conjuncts(e, &mut cs);
    cs.into_iter()
        .filter_map(|c| match c {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => Some((scope.resolve(left)?, scope.resolve(right)?)),
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

/// The expression is NULL whenever a column it references is NULL (conservative).
fn null_propagating(e: &Expr) -> bool {
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
            !matches!(
                op,
                BinaryOperator::And
                    | BinaryOperator::Or
                    | BinaryOperator::Xor
                    | BinaryOperator::Spaceship
            ) && null_propagating(left)
                && null_propagating(right)
        }
        Expr::Cast { expr, .. } => null_propagating(expr),
        Expr::Function(f) => {
            f.over.is_none()
                && !NULL_TOLERANT.contains(&function_name(f).as_str())
                && function_args(f)
                    .map(|a| a.into_iter().all(null_propagating))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// The predicate is never TRUE when a column it references is NULL (so it removes the
/// NULL-extended rows of a LEFT JOIN instead of accepting them).
fn null_rejecting(e: &Expr) -> bool {
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

/// Column references in an expression (unresolvable identifiers are ignored).
fn col_refs(e: &Expr, scope: &Scope) -> Vec<ColRef> {
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

/// Whether a unit (group) expression yields JSON values.
fn json_valued(e: &Expr, scope: &Scope) -> bool {
    match e {
        Expr::Nested(x) => json_valued(x, scope),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => scope
            .resolve(e)
            .map(|(a, c)| scope.aliases[a].1.columns[c].kind == ColKind::Json)
            .unwrap_or(false),
        Expr::Function(f) => JSON_PRODUCING.contains(&function_name(f).as_str()),
        Expr::Cast { data_type, .. } => matches!(data_type, DataType::JSON),
        Expr::BinaryOp {
            op: BinaryOperator::Arrow,
            ..
        } => true,
        _ => false,
    }
}

fn function_name(f: &sqlparser::ast::Function) -> String {
    match f.name.0.last() {
        Some(ObjectNamePart::Identifier(i)) => ident_name(i).to_ascii_uppercase(),
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
            Expr::MatchAgainst { .. } => {
                reason.get_or_insert_with(|| "MATCH ... AGAINST".to_string());
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
pub fn plan(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<ViewPlan> {
    let schema = def
        .schema
        .clone()
        .unwrap_or_else(|| default_schema.to_string());
    let lname = def.name.to_ascii_lowercase();
    if lname.ends_with("__lv_new")
        || lname.ends_with("__lv_old")
        || lname.starts_with("_lv_")
        || METADATA_TABLES.contains(&lname.as_str())
    {
        bail!(
            "view name `{}` is reserved by lazyview (names starting with `_lv_`, ending with `__lv_new`/`__lv_old`, or lazyview's metadata tables)",
            def.name
        );
    }
    let all_tables = referenced_tables(&def.query, default_schema);
    for t in &all_tables {
        if METADATA_TABLES.iter().any(|m| m.eq_ignore_ascii_case(&t.1)) {
            bail!("lazyview metadata table `{}` cannot be a view source", t.1);
        }
        if !tables.contains_key(t) {
            bail!("table `{}`.`{}` does not exist", t.0, t.1);
        }
    }
    let output_names = (!def.columns.is_empty()).then(|| def.columns.clone());
    // Full rebuilds run the statement exactly as written (ORDER BY matters with LIMIT).
    let full_select = full_select_sql(def, default_schema, tables)?;
    let full_with = |reason: &str, watched: Vec<TableRef>| ViewPlan {
        schema: schema.clone(),
        name: def.name.clone(),
        hash: def.hash.clone(),
        mode: Mode::Full(reason.to_string()),
        cascades: cascade_routes(
            &watched
                .iter()
                .filter_map(|t| tables.get(t).map(|i| (i.clone(), None)))
                .collect::<Vec<_>>(),
            tables,
        ),
        tables: watched,
        aliases: Vec::new(),
        projection: Vec::new(),
        from_sql: String::new(),
        where_sql: None,
        group_sql: None,
        having_sql: None,
        unit_exprs: Vec::new(),
        unit_in_projection: Vec::new(),
        full_select: full_select.clone(),
        unit_unique: false,
        output_names: output_names.clone(),
    };
    let full = |reason: &str| full_with(reason, all_tables.clone());

    // A SQL view has no binlog rows of its own: watch its base tables, rebuild on change.
    if let Some(v) = all_tables.iter().find(|t| tables[*t].is_view) {
        return Ok(full_with(
            &format!("reads SQL view `{}`", v.1),
            expand_views(&all_tables, tables),
        ));
    }

    let mut query = def.query.clone();
    query.order_by = None;
    if query.with.is_some() {
        return Ok(full("WITH clause"));
    }
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return Ok(full("LIMIT"));
    }
    let select: &mut Select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => return Ok(full("set operation (UNION/INTERSECT/EXCEPT)")),
    };
    if select.distinct.is_some() {
        return Ok(full("DISTINCT"));
    }
    if select.from.is_empty() {
        return Ok(full("no FROM clause"));
    }

    // FROM: tables and joins we know how to maintain.
    let mut aliases: Vec<(String, Arc<TableInfo>)> = Vec::new();
    let mut alias_pos: Vec<(usize, Option<usize>)> = Vec::new();
    // (alias index of the joined table, is_left_join, constraint)
    let mut joins: Vec<(usize, bool, JoinConstraint)> = Vec::new();
    let mut unsupported = None;
    let add_factor = |f: &TableFactor,
                      pos: (usize, Option<usize>),
                      aliases: &mut Vec<(String, Arc<TableInfo>)>,
                      alias_pos: &mut Vec<(usize, Option<usize>)>|
     -> Option<String> {
        match f {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let t = object_table_ref(name, default_schema)?;
                let info = tables.get(&t)?.clone();
                let n = alias
                    .as_ref()
                    .map(|a| ident_name(&a.name))
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
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    (false, c.clone())
                }
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
        return Ok(full(&r));
    }

    // Expand wildcards so the projection order is explicit.
    let mut projection = Vec::new();
    // Source text of each projection item (none for wildcard expansions).
    let mut item_sql: Vec<Option<&str>> = Vec::new();
    for (ii, item) in std::mem::take(&mut select.projection)
        .into_iter()
        .enumerate()
    {
        match item {
            SelectItem::Wildcard(_) => {
                for (n, t) in &aliases {
                    for c in t.columns.iter().filter(|c| !c.invisible) {
                        projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                            qident(n),
                            qident(&c.name),
                        ])));
                        item_sql.push(None);
                    }
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(on), _) => {
                let q = match on.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => ident_name(i),
                    _ => bail!("bad qualified wildcard"),
                };
                let (n, t) = aliases
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&q))
                    .ok_or_else(|| anyhow!("unknown table {q}.*"))?;
                for c in t.columns.iter().filter(|c| !c.invisible) {
                    projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                        qident(n),
                        qident(&c.name),
                    ])));
                    item_sql.push(None);
                }
            }
            SelectItem::QualifiedWildcard(..) => bail!("unsupported wildcard"),
            other => {
                projection.push(other);
                item_sql.push(def.item_sql.get(ii).and_then(|s| s.as_deref()));
            }
        }
    }
    if !def.columns.is_empty() && def.columns.len() != projection.len() {
        bail!(
            "view column list has {} names but SELECT returns {} columns",
            def.columns.len(),
            projection.len()
        );
    }
    select.projection = projection;

    let (reason, has_agg) = scan_exprs(select);
    if let Some(r) = reason {
        return Ok(full(&r));
    }

    let scope = Scope { aliases: &aliases };
    strip_column_schemas(select, &scope);
    let n_alias = aliases.len();
    let referenced: Vec<Vec<bool>> = aliases
        .iter()
        .map(|(_, t)| vec![false; t.columns.len()])
        .collect();
    let membership = referenced.clone();

    let proj_exprs: Vec<(&Expr, Option<String>)> = select
        .projection
        .iter()
        .map(|i| match i {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(ident_name(alias))),
            _ => unreachable!(),
        })
        .collect();

    // Column usage. Unresolvable identifiers in GROUP BY / HAVING may be select aliases.
    let mut usage = Usage {
        scope: &scope,
        select_aliases: proj_exprs.iter().filter_map(|(_, a)| a.clone()).collect(),
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
                    Some(ObjectNamePart::Identifier(i)) => ident_name(i),
                    _ => bail!("bad USING column"),
                };
                let rc = aliases[*ja]
                    .1
                    .col_index(&col)
                    .ok_or_else(|| anyhow!("USING column {col} not found"))?;
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
        GroupByExpr::Expressions(..) => return Ok(full("GROUP BY modifiers (ROLLUP)")),
        GroupByExpr::All(_) => return Ok(full("GROUP BY ALL")),
    };
    let mut resolved_groups = Vec::new();
    for g in &group_exprs {
        let r = match g {
            Expr::Value(v) if matches!(v.value, Value::Number(..)) => {
                let n: usize = v.value.to_string().parse().context("GROUP BY ordinal")?;
                proj_exprs
                    .get(n.wrapping_sub(1))
                    .map(|(e, _)| (*e).clone())
                    .ok_or_else(|| anyhow!("GROUP BY {n} out of range"))?
            }
            Expr::Identifier(id) if scope.resolve(g).is_none() => proj_exprs
                .iter()
                .find(|(_, a)| {
                    a.as_ref()
                        .map(|a| a.eq_ignore_ascii_case(&ident_name(id)))
                        .unwrap_or(false)
                })
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
    let Usage {
        mut referenced,
        mut membership,
        ..
    } = usage;

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
                return Ok(full(&format!(
                    "first table `{}` has no primary key",
                    t.name
                )));
            }
            t.pk.iter()
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
    // Unit keys and lookup images of opaque types would be compared as different values.
    if matches!(mode, Mode::Spj | Mode::Grouped) {
        for (a, (_, t)) in aliases.iter().enumerate() {
            if let Some(c) = (0..t.columns.len())
                .find(|&c| membership[a][c] && t.columns[c].kind == ColKind::Opaque)
            {
                return Ok(full(&format!(
                    "{} column `{}`.`{}` decides which rows match, but its binlog value cannot be compared with its SQL value",
                    t.columns[c].data_type.to_ascii_uppercase(),
                    t.name,
                    t.columns[c].name
                )));
            }
        }
    }
    // Units and WHERE are evaluated on literal row images in lookups; that is only sound when the
    // NULL-extended rows of LEFT JOINs cannot change them.
    let null_side: Vec<usize> = joins
        .iter()
        .filter(|(_, left, _)| *left)
        .map(|(a, _, _)| *a)
        .collect();
    let on_null_side = |e: &Expr| {
        col_refs(e, &scope)
            .iter()
            .any(|(a, _)| null_side.contains(a))
    };
    if unit_refs.iter().any(&on_null_side) {
        return Ok(full("group key on the NULL side of a LEFT JOIN"));
    }
    if let Some(w) = &select.selection {
        let mut cs = Vec::new();
        conjuncts(w, &mut cs);
        if cs
            .into_iter()
            .any(|c| on_null_side(c) && !null_rejecting(c))
        {
            return Ok(full(
                "WHERE condition that keeps NULL-extended rows of a LEFT JOIN",
            ));
        }
    }
    if unit_refs.iter().any(|u| json_valued(u, &scope)) {
        return Ok(full("JSON group key"));
    }
    let unit_cols: Vec<Option<ColRef>> = unit_refs.iter().map(|e| scope.resolve(e)).collect();
    if unit_cols
        .iter()
        .flatten()
        .any(|(a, c)| matches!(aliases[*a].1.columns[*c].kind, ColKind::Bit(_)))
    {
        return Ok(full("BIT unit key"));
    }
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
                    (0..t.columns.len())
                        .find(|&c| t.columns[c].exact_key() && cls.find((ai, c)) == ru)
                })
                .collect::<Option<Vec<usize>>>()
        };
        let image_cols: Vec<usize> = (0..t.columns.len())
            .filter(|&c| membership[ai][c])
            .collect();
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

    if let Some(a) = alias_plans
        .iter()
        .find(|a| a.image_cols.is_empty() && a.direct_unit.is_none())
    {
        if matches!(mode, Mode::Spj | Mode::Grouped) {
            return Ok(full(&format!(
                "table `{}` contributes to every row (no join condition)",
                a.table.name
            )));
        }
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
        projection: select
            .projection
            .iter()
            .zip(&item_sql)
            .map(|(p, src)| match (p, src) {
                // Unaliased expressions as written: the server names the column after the text
                // (or `Name_exp_<n>`, or a unique variant of a duplicate), as for the real view.
                (SelectItem::UnnamedExpr(e), Some(src))
                    if !matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) =>
                {
                    src.to_string()
                }
                _ => p.to_string(),
            })
            .collect(),
        from_sql,
        where_sql: select.selection.as_ref().map(|e| e.to_string()),
        group_sql: if group_exprs.is_empty() {
            None
        } else {
            Some(
                group_exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        },
        having_sql: select.having.as_ref().map(|e| e.to_string()),
        unit_exprs,
        unit_in_projection,
        full_select: query.to_string(),
        unit_unique,
        cascades: cascade_routes(
            &alias_plans
                .iter()
                .map(|a| (a.table.clone(), a.direct_unit.clone()))
                .collect::<Vec<_>>(),
            tables,
        ),
        aliases: alias_plans,
        output_names,
    })
}

/// `db.t.col` -> `t`.`col` for columns of the FROM clause: lookups replace a table by derived
/// rows, which can only be referenced through the alias.
fn strip_column_schemas(select: &mut Select, scope: &Scope) {
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
fn full_select_sql(
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
    let names: Vec<String> = def.columns.iter().map(|c| quote(c)).collect();
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
fn expand_views(refs: &[TableRef], tables: &HashMap<TableRef, Arc<TableInfo>>) -> Vec<TableRef> {
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

/// Cascading-FK parents (and, conservatively, their ancestors) of the given child tables.
fn cascade_routes(
    children: &[(Arc<TableInfo>, Option<Vec<usize>>)],
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<CascadeRoute> {
    let mut out = Vec::new();
    let mut stack: Vec<(Arc<TableInfo>, Option<Vec<usize>>, usize)> = children
        .iter()
        .map(|(t, d)| (t.clone(), d.clone(), 0))
        .collect();
    let mut seen: BTreeSet<(TableRef, TableRef)> = BTreeSet::new();
    while let Some((child, direct, depth)) = stack.pop() {
        for fk in &child.cascades {
            let Some(parent) = tables.get(&fk.parent) else {
                continue;
            };
            if !seen.insert((
                (child.schema.clone(), child.name.clone()),
                fk.parent.clone(),
            )) && direct.is_none()
            {
                continue;
            }
            let parent_cols: Option<Vec<usize>> =
                fk.parent_cols.iter().map(|n| parent.col_index(n)).collect();
            let Some(parent_cols) = parent_cols else {
                continue;
            };
            // Units follow from the parent image only if every unit column of the child is an
            // FK column with an exact representation on both sides.
            let unit_from_parent = direct.as_ref().and_then(|cols| {
                cols.iter()
                    .map(|c| {
                        let j = fk.child_cols.iter().position(|x| x == c)?;
                        let pc = parent_cols[j];
                        (child.columns[*c].exact_key() && parent.columns[pc].exact_key())
                            .then_some(pc)
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

fn render_substituted_from(
    from: &[TableWithJoins],
    pos: (usize, Option<usize>),
    alias: &str,
) -> Result<(String, String)> {
    let mut from = from.to_vec();
    let marker = TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(SUBST_MARKER))]),
        alias: Some(TableAlias {
            explicit: true,
            name: qident(alias),
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
    };
    match pos {
        (t, None) => from[t].relation = marker,
        (t, Some(j)) => {
            let join = &mut from[t].joins[j];
            join.relation = marker;
            // Only rows that actually match the image are affected by it; the refresh of those
            // units recomputes any NULL extension.
            if let JoinOperator::Left(c) | JoinOperator::LeftOuter(c) = &join.join_operator {
                join.join_operator = JoinOperator::Join(c.clone());
            }
        }
    }
    let s = from
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(", ");
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
            columns: cols.iter().map(|(n, ty)| Column::simple(n, ty)).collect(),
            pk: pk.to_vec(),
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    }

    fn typed(name: &str, cols: Vec<Column>, pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
        let t = TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols,
            pk: pk.to_vec(),
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    }

    fn full_reason(sql: &str) -> String {
        match p(sql).mode {
            Mode::Full(r) => r,
            m => panic!("{sql}: expected Full, got {m:?}"),
        }
    }

    fn catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        [
            table(
                "orders",
                &[
                    ("id", "int"),
                    ("customer_id", "int"),
                    ("status", "varchar"),
                    ("total", "decimal"),
                    ("note", "text"),
                ],
                &[0],
            ),
            table(
                "customers",
                &[("id", "int"), ("name", "varchar"), ("region_id", "int")],
                &[0],
            ),
            table("regions", &[("id", "int"), ("name", "varchar")], &[0]),
            table(
                "order_items",
                &[
                    ("order_id", "int"),
                    ("line", "int"),
                    ("qty", "int"),
                    ("price", "decimal"),
                ],
                &[0, 1],
            ),
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
        assert_eq!(d.query_sql, "SELECT id, status FROM orders");
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
        let v = p(
            "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region, i.qty \
                   FROM orders o JOIN customers c ON c.id = o.customer_id \
                   LEFT JOIN regions r ON r.id = c.region_id \
                   LEFT JOIN order_items i ON i.order_id = o.id",
        );
        assert_eq!(v.mode, Mode::Spj);
        assert!(!v.unit_unique);
        let by = |n: &str| v.aliases.iter().find(|a| a.name == n).unwrap();
        assert_eq!(by("o").direct_unit, Some(vec![0]));
        assert_eq!(
            by("i").direct_unit,
            Some(vec![0]),
            "items map to unit via i.order_id = o.id"
        );
        assert_eq!(by("c").direct_unit, None, "customers need a lookup");
        assert_eq!(by("r").direct_unit, None);
        assert_eq!(by("c").subst_from.0, "orders o JOIN ");
        let look = v.lookup_sql(1, "(SELECT ? AS `id`)");
        assert!(
            look.starts_with(
                "SELECT DISTINCT `o`.`id` FROM orders o JOIN (SELECT ? AS `id`) AS `c` ON"
            ),
            "{look}"
        );
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
        assert!(
            s.ends_with("WHERE o.customer_id IN (1) GROUP BY cust HAVING n > 1"),
            "{s}"
        );

        let v = p("CREATE VIEW v AS SELECT DATE(o.note) d, COUNT(*) FROM orders o GROUP BY 1");
        assert_eq!(v.unit_exprs, vec!["DATE(o.note)"]);
        assert_eq!(v.aliases[0].direct_unit, None);
    }

    #[test]
    fn global_and_full() {
        assert_eq!(
            p("CREATE VIEW v AS SELECT COUNT(*) FROM orders").mode,
            Mode::Global
        );
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
        assert_eq!(v.projection, vec!["`r`.`id`", "`r`.`name`", "1"]);
        assert_eq!(
            v.output_names,
            Some(vec!["a".to_string(), "b".into(), "c".into()])
        );
        assert!(
            parse_definition("CREATE VIEW v (a) AS SELECT id, name FROM regions")
                .and_then(|d| plan(&d, "db", &catalog()))
                .is_err(),
            "column list length must match"
        );
        assert!(parse_definition("CREATE VIEW v AS SELECT * FROM nope")
            .and_then(|d| plan(&d, "db", &catalog()))
            .is_err());
        assert!(
            parse_definition("CREATE VIEW v AS SELECT bogus FROM orders")
                .and_then(|d| plan(&d, "db", &catalog()))
                .is_err()
        );
    }

    #[test]
    fn composite_and_using() {
        let v = p("CREATE VIEW v AS SELECT i.order_id, i.line, o.status FROM order_items i JOIN orders o ON o.id = i.order_id");
        assert_eq!(v.unit_exprs.len(), 2);
        assert_eq!(
            v.aliases[1].direct_unit, None,
            "orders map to only part of the composite unit"
        );
        let v = p("CREATE VIEW v AS SELECT o.id, o.status FROM orders o JOIN customers c ON c.id = o.customer_id");
        assert_eq!(v.aliases[1].direct_unit, None);
        let v = p("CREATE VIEW v AS SELECT r.id, c.name FROM regions r JOIN customers c ON c.region_id = r.id");
        assert_eq!(v.aliases[1].direct_unit, Some(vec![2]));
    }

    #[test]
    fn column_list_keeps_select_aliases_for_having() {
        // HAVING n refers to the SELECT alias `n`, not to the view column name.
        let v = p("CREATE VIEW v (cust, cnt) AS SELECT customer_id, COUNT(*) n FROM orders GROUP BY customer_id HAVING n > 1");
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(v.projection[1], "COUNT(*) AS n");
        assert!(v.select_sql(&[], None).ends_with("HAVING n > 1"));
        assert_eq!(v.output_names.as_ref().unwrap()[1], "cnt");
    }

    #[test]
    fn order_by_kept_for_full_and_ctes_are_not_tables() {
        let v = p("CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 3");
        assert!(matches!(v.mode, Mode::Full(_)));
        assert!(
            v.full_select.contains("ORDER BY total DESC LIMIT 3"),
            "{}",
            v.full_select
        );
        // ORDER BY without LIMIT is irrelevant for a table: dropped on the incremental path.
        let v = p("CREATE VIEW v AS SELECT id FROM orders ORDER BY total");
        assert_eq!(v.mode, Mode::Spj);
        assert!(!v.select_sql(&[], None).contains("ORDER BY"));
        let d = parse_definition(
            "CREATE VIEW v AS WITH big AS (SELECT id FROM orders WHERE total > 5) SELECT id FROM big",
        )
        .unwrap();
        assert_eq!(
            referenced_tables(&d.query, "db"),
            vec![("db".to_string(), "orders".to_string())]
        );
        assert!(matches!(
            plan(&d, "db", &catalog()).unwrap().mode,
            Mode::Full(_)
        ));
    }

    #[test]
    fn left_join_null_side() {
        // Group key from the NULL-supplying side: NULL-extended group is invisible to lookups.
        let r = full_reason("CREATE VIEW v AS SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id");
        assert!(r.contains("NULL side"), "{r}");
        let r = full_reason("CREATE VIEW v AS SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY n");
        assert!(r.contains("NULL side"), "{r}");
        // Anti-join: keeps NULL-extended rows.
        let r = full_reason("CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.id IS NULL");
        assert!(r.contains("NULL-extended"), "{r}");
        let r = full_reason("CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE IFNULL(c.name, 'x') = 'x'");
        assert!(r.contains("NULL-extended"), "{r}");
        let r = full_reason("CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name = 'a' OR o.total > 1");
        assert!(r.contains("NULL-extended"), "{r}");
        // Null-rejecting conditions stay incremental.
        let v = p("CREATE VIEW v AS SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name LIKE 'a%' AND UPPER(c.name) <> 'B' AND c.region_id IN (1, 2)");
        assert_eq!(v.mode, Mode::Spj);
        // The substituted LEFT-joined table is joined INNER in its own lookup.
        let c = v.aliases.iter().position(|a| a.name == "c").unwrap();
        assert_eq!(v.aliases[c].subst_from.0, "orders o JOIN ");
        let look = v.lookup_sql(c, "(SELECT 1 AS `id`)");
        assert!(!look.contains("LEFT JOIN (SELECT"), "{look}");
        // HAVING is evaluated on refresh, not in lookups: fine on the NULL side.
        let v = p("CREATE VIEW v AS SELECT o.id, COUNT(i.line) n FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY o.id HAVING COUNT(i.line) = 0");
        assert_eq!(v.mode, Mode::Grouped);
    }

    #[test]
    fn equalities_only_within_a_type_family() {
        let mut cat = catalog();
        let (k, t) = typed(
            "codes",
            vec![
                Column::simple("code", "varchar"),
                Column::simple("v", "int"),
            ],
            &[0],
        );
        cat.insert(k, t);
        let (k, t) = typed(
            "refs",
            vec![
                Column::simple("id", "int"),
                Column::new(
                    "code",
                    "varchar",
                    "varchar(9)",
                    Some("latin1"),
                    Some("latin1_swedish_ci"),
                    Some(9),
                    false,
                ),
                Column::simple("code_int", "int"),
            ],
            &[0],
        );
        cat.insert(k, t);
        let pl = |sql: &str| plan(&parse_definition(sql).unwrap(), "db", &cat).unwrap();
        // int = varchar: implicit conversion, not transitive -> no direct unit.
        let v = pl(
            "CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code_int = c.code",
        );
        assert_eq!(v.aliases[1].direct_unit, None);
        // different collations -> no direct unit.
        let v =
            pl("CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code = c.code");
        assert_eq!(v.aliases[1].direct_unit, None);
        // same family -> direct.
        let v =
            pl("CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code_int = c.v");
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.aliases[0].direct_unit, Some(vec![0]));
    }

    #[test]
    fn unsupported_constructs_fall_back() {
        assert!(full_reason(
            "CREATE VIEW v AS SELECT id FROM orders WHERE MATCH(note) AGAINST ('x')"
        )
        .contains("MATCH"));
        assert!(full_reason(
            "CREATE VIEW v AS SELECT o.id, c.name FROM orders o CROSS JOIN regions c"
        )
        .contains("every row"));
        assert!(
            full_reason("CREATE VIEW v AS SELECT o.id, r.name FROM orders o JOIN regions r")
                .contains("every row")
        );
    }

    #[test]
    fn json_and_bit_unit_keys() {
        let mut cat = catalog();
        let (k, t) = typed(
            "docs",
            vec![
                Column::simple("id", "int"),
                Column::new("doc", "json", "json", None, None, None, false),
                Column::new("flags", "bit", "bit(8)", None, None, None, false),
            ],
            &[0],
        );
        cat.insert(k, t);
        let pl = |sql: &str| {
            plan(&parse_definition(sql).unwrap(), "db", &cat)
                .unwrap()
                .mode
        };
        for sql in [
            "CREATE VIEW v AS SELECT doc, COUNT(*) FROM docs GROUP BY doc",
            "CREATE VIEW v AS SELECT JSON_EXTRACT(doc, '$.a') k, COUNT(*) FROM docs GROUP BY k",
            "CREATE VIEW v AS SELECT doc->'$.a' k, COUNT(*) FROM docs GROUP BY k",
        ] {
            assert_eq!(pl(sql), Mode::Full("JSON group key".into()), "{sql}");
        }
        // ->> yields text: fine.
        assert_eq!(
            pl("CREATE VIEW v AS SELECT doc->>'$.a' k, COUNT(*) FROM docs GROUP BY k"),
            Mode::Grouped
        );
        assert_eq!(
            pl("CREATE VIEW v AS SELECT flags, COUNT(*) FROM docs GROUP BY flags"),
            Mode::Full("BIT unit key".into())
        );
    }

    #[test]
    fn sql_views_reserved_names_and_metadata() {
        let mut cat = catalog();
        let mut info = (*cat[&("db".to_string(), "orders".to_string())]).clone();
        info.name = "paid_orders".into();
        info.pk.clear();
        info.is_view = true;
        info.view_bases = vec![
            ("db".into(), "orders".into()),
            ("db".into(), "nested_v".into()),
        ];
        cat.insert(("db".into(), "paid_orders".into()), Arc::new(info));
        let mut nested = (*cat[&("db".to_string(), "regions".to_string())]).clone();
        nested.name = "nested_v".into();
        nested.is_view = true;
        nested.view_bases = vec![("db".into(), "regions".into())];
        cat.insert(("db".into(), "nested_v".into()), Arc::new(nested));
        let v = plan(&parse_definition("CREATE VIEW v AS SELECT p.id, c.name FROM paid_orders p JOIN customers c ON c.id = p.customer_id").unwrap(), "db", &cat).unwrap();
        assert!(
            matches!(&v.mode, Mode::Full(r) if r.contains("paid_orders")),
            "{:?}",
            v.mode
        );
        assert_eq!(
            v.tables,
            vec![
                ("db".to_string(), "customers".to_string()),
                ("db".into(), "orders".into()),
                ("db".into(), "regions".into())
            ]
        );
        for bad in [
            "CREATE VIEW x__lv_new AS SELECT id FROM orders",
            "CREATE VIEW x__LV_OLD AS SELECT id FROM orders",
            "CREATE VIEW _lv_probe AS SELECT id FROM orders",
            "CREATE VIEW _lazyview_state AS SELECT id FROM orders",
        ] {
            assert!(
                plan(&parse_definition(bad).unwrap(), "db", &cat).is_err(),
                "{bad}"
            );
        }
        let mut cat2 = catalog();
        let (k, t) = table("_lazyview_state", &[("name", "varchar")], &[0]);
        cat2.insert(k, t);
        let e = plan(
            &parse_definition("CREATE VIEW v AS SELECT name FROM _lazyview_state").unwrap(),
            "db",
            &cat2,
        )
        .unwrap_err();
        assert!(e.to_string().contains("metadata"), "{e}");
        assert_eq!(
            select_tables(
                "select `db`.`orders`.`id` AS `id` from `db`.`orders` join x on 1",
                "db"
            ),
            vec![
                ("db".to_string(), "orders".to_string()),
                ("db".into(), "x".into())
            ]
        );
        assert!(select_tables("not sql", "db").is_empty());
    }

    #[test]
    fn function_calls_listed() {
        let d = parse_definition("CREATE VIEW v AS SELECT id, my_fn(total), app.calc(id) FROM orders WHERE UPPER(status) = 'X'").unwrap();
        let f = function_calls(&d.query);
        assert!(f.contains(&(None, "my_fn".to_string())));
        assert!(f.contains(&(Some("app".to_string()), "calc".to_string())));
        assert!(f.contains(&(None, "UPPER".to_string())));
    }

    #[test]
    fn literal_escapes_and_quoted_identifiers_survive() {
        let v = p(
            r"CREATE VIEW v AS SELECT id, status FROM orders WHERE status = 'a\'b' OR status LIKE 'x\_y' OR note = '\\n'",
        );
        let w = v.where_sql.as_deref().unwrap();
        for lit in [r"'a\'b'", r"'x\_y'", r"'\\n'"] {
            assert!(w.contains(lit), "{lit} in {w}");
        }
        let v =
            p(r"CREATE VIEW v AS SELECT CONCAT(status, '\\n') k, COUNT(*) FROM orders GROUP BY k");
        assert_eq!(v.unit_exprs, vec![r"CONCAT(status, '\\n')"]);
        // Full rebuilds run the text as written.
        let d = parse_definition(
            "CREATE VIEW v AS\n  SELECT DISTINCT CONCAT(status, '\\\\n')  FROM orders WHERE note = 'it''s' -- tail\n WITH CHECK OPTION;",
        )
        .unwrap();
        assert_eq!(
            d.query_sql,
            "SELECT DISTINCT CONCAT(status, '\\\\n')  FROM orders WHERE note = 'it''s'"
        );
        // Doubled backticks: names resolve unquoted and render quoted again.
        let mut cat = catalog();
        let (k, t) = table("we`ird", &[("i`d", "int"), ("v", "int")], &[0]);
        cat.insert(k, t);
        let d = parse_definition(
            "CREATE VIEW `v``1` (`a``b`, c) AS SELECT `i``d`, w.v FROM `we``ird` w WHERE w.`i``d` > 1",
        )
        .unwrap();
        assert_eq!(d.name, "v`1");
        assert_eq!(d.columns, vec!["a`b", "c"]);
        assert_eq!(
            referenced_tables(&d.query, "db"),
            vec![("db".to_string(), "we`ird".to_string())]
        );
        let v = plan(&d, "db", &cat).unwrap();
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.unit_exprs, vec!["`w`.`i``d`"]);
        assert_eq!(v.aliases[0].referenced, vec![true, true]);
        assert_eq!(v.aliases[0].membership, vec![true, false]);
        assert_eq!(
            select_tables("select `x`.`i``d` from `db`.`we``ird` `x`", "db"),
            vec![("db".to_string(), "we`ird".to_string())]
        );
    }

    #[test]
    fn opaque_key_columns_fall_back() {
        let mut cat = catalog();
        let opaque = |n: &str, ty: &str| Column::new(n, ty, ty, None, None, Some(16), true);
        let (k, t) = typed(
            "devices",
            vec![
                opaque("id", "uuid"),
                opaque("ip", "inet6"),
                Column::simple("name", "varchar"),
                opaque("pos", "point"),
            ],
            &[0],
        );
        cat.insert(k, t);
        let (k, t) = typed(
            "hits",
            vec![
                Column::simple("n", "int"),
                opaque("ip", "inet6"),
                opaque("v4", "inet4"),
            ],
            &[0],
        );
        cat.insert(k, t);
        let pl = |sql: &str| {
            plan(&parse_definition(sql).unwrap(), "db", &cat)
                .unwrap()
                .mode
        };
        for (sql, want) in [
            (
                "CREATE VIEW v AS SELECT id, name FROM devices",
                "UUID column `devices`.`id`",
            ),
            (
                "CREATE VIEW v AS SELECT ip, COUNT(*) FROM hits GROUP BY ip",
                "INET6 column `hits`.`ip`",
            ),
            (
                "CREATE VIEW v AS SELECT h.n, d.name FROM hits h JOIN devices d ON d.ip = h.ip",
                "INET6 column `hits`.`ip`",
            ),
            (
                "CREATE VIEW v AS SELECT n FROM hits WHERE v4 > '10.0.0.0'",
                "INET4 column `hits`.`v4`",
            ),
        ] {
            let m = pl(sql);
            assert!(
                matches!(&m, Mode::Full(r) if r.contains(want)),
                "{sql}: {m:?}"
            );
        }
        // Only projected (values come from queries), or no units at all: unaffected.
        assert_eq!(pl("CREATE VIEW v AS SELECT n, ip, v4 FROM hits"), Mode::Spj);
        assert_eq!(
            pl("CREATE VIEW v AS SELECT COUNT(*) FROM hits WHERE ip IS NOT NULL"),
            Mode::Global
        );
    }

    #[test]
    fn schema_qualified_columns_use_the_alias() {
        let v = p(
            "CREATE VIEW v AS SELECT db.orders.id, db.customers.name, db.orders.total + 1 \
                   FROM db.orders JOIN db.customers ON db.customers.id = db.orders.customer_id \
                   WHERE db.customers.name <> 'x' AND db.orders.total > 0",
        );
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.unit_exprs, vec!["`orders`.`id`"]);
        assert_eq!(
            v.where_sql.as_deref(),
            Some("`customers`.name <> 'x' AND `orders`.total > 0")
        );
        assert_eq!(v.projection[..2], ["`orders`.id", "`customers`.name"]);
        // Valid as written in the full SELECT (it is not substituted).
        assert_eq!(v.projection[2], "db.orders.total + 1");
        let look = v.lookup_sql(1, "(SELECT ? AS `id`, ? AS `name`)");
        assert_eq!(
            look,
            "SELECT DISTINCT `orders`.`id` FROM db.orders JOIN (SELECT ? AS `id`, ? AS `name`) AS `customers` \
             ON `customers`.id = `orders`.customer_id WHERE `customers`.name <> 'x' AND `orders`.total > 0"
        );
        let look = v.lookup_sql(0, "(SELECT ? AS `id`)");
        assert!(!look.contains("db.orders."), "{look}");
        let v = p("CREATE VIEW v AS SELECT db.orders.customer_id c, COUNT(*) FROM orders GROUP BY db.orders.customer_id");
        assert_eq!(v.unit_exprs, vec!["`orders`.customer_id"]);
        assert_eq!(v.group_sql.as_deref(), Some("`orders`.customer_id"));
    }

    #[test]
    fn column_list_for_full_rebuilds() {
        let v = p("CREATE VIEW v (st) AS SELECT DISTINCT status FROM orders");
        assert_eq!(
            v.full_select,
            "WITH `_lv_cols` (`st`) AS (SELECT DISTINCT status FROM orders) SELECT * FROM `_lv_cols`"
        );
        // Set operations take names from the first SELECT; ORDER BY keeps its own aliases.
        let v = p("CREATE VIEW v (x, y) AS SELECT id, status AS s FROM orders UNION ALL SELECT id, name FROM customers ORDER BY s LIMIT 2");
        assert_eq!(
            v.full_select,
            "WITH `_lv_cols` (`x`, `y`) AS (SELECT id, status AS s FROM orders UNION ALL \
             SELECT id, name FROM customers ORDER BY s LIMIT 2) SELECT * FROM `_lv_cols`"
        );
        let pl = |sql: &str| parse_definition(sql).and_then(|d| plan(&d, "db", &catalog()));
        for ok in [
            "CREATE VIEW v (a, b) AS SELECT DISTINCT * FROM regions",
            "CREATE VIEW v (a, b, c) AS SELECT DISTINCT r.*, c.id FROM regions r JOIN customers c ON c.region_id = r.id",
            // Not countable here (derived table, USING): the server checks the count.
            "CREATE VIEW v (a) AS SELECT * FROM (SELECT id, name FROM regions) d",
            "CREATE VIEW v (a, b, c, d) AS SELECT DISTINCT * FROM regions JOIN customers USING (id)",
        ] {
            let v = pl(ok).unwrap();
            assert!(matches!(v.mode, Mode::Full(_)), "{ok}");
            assert!(v.full_select.starts_with("WITH `_lv_cols` ("), "{ok}");
        }
        for bad in [
            "CREATE VIEW v (a) AS SELECT DISTINCT id, status FROM orders",
            "CREATE VIEW v (a) AS SELECT * FROM regions UNION SELECT id, name FROM customers",
            "CREATE VIEW v (a, b, c) AS SELECT DISTINCT * FROM regions",
        ] {
            let e = pl(bad).unwrap_err().to_string();
            assert!(e.contains("column list has"), "{bad}: {e}");
        }
        // Incremental plans keep their SELECT and are renamed positionally.
        let v = pl("CREATE VIEW v (a, b) AS SELECT id, name FROM regions").unwrap();
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.output_names, Some(vec!["a".to_string(), "b".into()]));
    }

    #[test]
    fn unaliased_expressions_keep_their_source_text() {
        let v = p("CREATE VIEW v AS SELECT id, CONCAT(status,'-',note), CASE WHEN total > 1 THEN 'y' ELSE 'n' END, \
                   id+customer_id, IF(total>1,'y','n'), `status`, o.note, (total), 'it''s', total AS t, \
                   total /* c */ + 1, total\n  + 2, CONCAT(note, 'é—x'), \
                   CONCAT(status, '-', note, '-', status, '-', note, '-', status, '-', note, '-', status) FROM orders o");
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(
            v.projection,
            vec![
                "id",
                "CONCAT(status,'-',note)",
                "CASE WHEN total > 1 THEN 'y' ELSE 'n' END",
                "id+customer_id",
                "IF(total>1,'y','n')",
                "`status`",
                "o.note",
                "(total)",
                "'it''s'",
                "total AS t",
                "total /* c */ + 1",
                "total\n  + 2",
                "CONCAT(note, 'é—x')",
                "CONCAT(status, '-', note, '-', status, '-', note, '-', status, '-', note, '-', status)",
            ]
        );
        let v = p("CREATE VIEW v AS SELECT SQL_NO_CACHE customer_id, COUNT( * ), count(*), SUM(total*2) FROM orders GROUP BY customer_id");
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(
            v.projection,
            vec!["customer_id", "COUNT( * )", "count(*)", "SUM(total*2)"]
        );
        assert_eq!(v.unit_in_projection, vec![Some(0)]);
        let v = p("CREATE VIEW v AS SELECT r.*, LENGTH( name ) FROM regions r");
        assert_eq!(
            v.projection,
            vec!["`r`.`id`", "`r`.`name`", "LENGTH( name )"]
        );
        let v = p("CREATE VIEW v AS SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders");
        assert_eq!(
            v.full_select,
            "SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders"
        );
    }
}
