//! Turns a `CREATE VIEW` statement into a [`ViewPlan`]: what to watch, how a changed row maps to
//! the materialized rows it affects ("units"), and pre-rendered SQL for every refresh query.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, BinaryOperator, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator, NamedWindowExpr,
    ObjectName, ObjectNamePart, OrderBy, OrderByKind, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableAlias, TableFactor, TableWithJoins,
    UnaryOperator, Value, Visit, Visitor, WindowType,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Word};

use crate::catalog::{ColKind, FkAction, TableInfo, TableRef, TypeFamily};
use crate::datum::Datum;

/// Placeholder table name used to render "FROM with one table replaced by literal rows".
pub(crate) const SUBST_MARKER: &str = "__lv_subst_marker__";

/// In the lookup template of a table of a semi-join subquery over several tables
/// ([`AliasPlan::semi`]), the place of the subquery's table that is alias `alias` of the plan:
/// that table, or its rows when a lookup substitutes it too.
pub(crate) fn semi_table_marker(alias: usize) -> String {
    format!("__lv_semi_table_{alias}__")
}

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
    // Sequences, advisory locks, files: a different value or side effect per evaluation.
    "NEXTVAL",
    "LASTVAL",
    "SETVAL",
    "NEXT_VALUE",
    "PREVIOUS_VALUE",
    "GET_LOCK",
    "RELEASE_LOCK",
    "RELEASE_ALL_LOCKS",
    "IS_FREE_LOCK",
    "IS_USED_LOCK",
    "LOAD_FILE",
    "RANDOM_BYTES",
    "SYS_GUID",
    "MASTER_POS_WAIT",
    "SOURCE_POS_WAIT",
    "WAIT_FOR_EXECUTED_GTID_SET",
    "MASTER_GTID_WAIT",
];

/// Why `SELECT *` over a USING / NATURAL join is not planned: the server lists each shared
/// column once (first), which is not the per-table expansion of `*`.
pub(crate) const STAR_USING: &str =
    "SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns";

/// lazyview's own tables: never valid as view sources or view names.
const METADATA_TABLES: &[&str] = &[
    "_lazyview",
    "_lazyview_state",
    "_lazyview_status",
    "_lazyview_overview",
];

/// Builtins that are NULL whenever one of their arguments is NULL (strict). Any function not
/// listed is taken as able to return a value for NULL arguments (`IF`, `COALESCE`, `CHAR`,
/// `CHARSET`, `COERCIBILITY`, MariaDB `NVL2` / `DECODE_ORACLE`, aggregates, ...), so an
/// expression over the NULL side of a LEFT JOIN gets its guard and a predicate using it keeps
/// NULL-extended rows.
const NULL_STRICT: &[&str] = &[
    // arithmetic
    "ABS",
    "CEIL",
    "CEILING",
    "FLOOR",
    "ROUND",
    "TRUNCATE",
    "MOD",
    "POW",
    "POWER",
    "SQRT",
    "EXP",
    "LN",
    "LOG",
    "LOG2",
    "LOG10",
    "SIGN",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "ATAN2",
    "COT",
    "DEGREES",
    "RADIANS",
    "CRC32",
    "CONV",
    "BIN",
    "OCT",
    "HEX",
    "UNHEX",
    "GREATEST",
    "LEAST",
    "DIV",
    // strings
    "UPPER",
    "UCASE",
    "LOWER",
    "LCASE",
    "LENGTH",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "OCTET_LENGTH",
    "BIT_LENGTH",
    "CONCAT",
    "SUBSTRING",
    "SUBSTR",
    "MID",
    "LEFT",
    "RIGHT",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "REVERSE",
    "REPEAT",
    "LPAD",
    "RPAD",
    "INSTR",
    "LOCATE",
    "POSITION",
    "SUBSTRING_INDEX",
    "ASCII",
    "ORD",
    "SPACE",
    "STRCMP",
    "SOUNDEX",
    "INSERT",
    "MD5",
    "SHA",
    "SHA1",
    "TO_BASE64",
    "FROM_BASE64",
    "COMPRESS",
    "UNCOMPRESS",
    "WEIGHT_STRING",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REGEXP_INSTR",
    "REGEXP_LIKE",
    "BINARY",
    "CONVERT",
    "CAST",
    // date and time
    "DATE",
    "TIME",
    "YEAR",
    "MONTH",
    "DAY",
    "DAYOFMONTH",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "WEEKDAY",
    "QUARTER",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MICROSECOND",
    "DAYNAME",
    "MONTHNAME",
    "LAST_DAY",
    "TO_DAYS",
    "TO_SECONDS",
    "FROM_DAYS",
    "FROM_UNIXTIME",
    "UNIX_TIMESTAMP",
    "STR_TO_DATE",
    "DATE_FORMAT",
    "TIME_FORMAT",
    "DATE_ADD",
    "DATE_SUB",
    "ADDDATE",
    "SUBDATE",
    "ADDTIME",
    "SUBTIME",
    "DATEDIFF",
    "TIMEDIFF",
    "TIMESTAMPDIFF",
    "TIMESTAMPADD",
    "TIMESTAMP",
    "MAKEDATE",
    "MAKETIME",
    "SEC_TO_TIME",
    "TIME_TO_SEC",
    "PERIOD_ADD",
    "PERIOD_DIFF",
    "CONVERT_TZ",
    "EXTRACT",
    "GET_FORMAT",
    // network, uuid, json
    "INET_ATON",
    "INET_NTOA",
    "INET6_ATON",
    "INET6_NTOA",
    "UUID_TO_BIN",
    "BIN_TO_UUID",
    "JSON_EXTRACT",
    "JSON_UNQUOTE",
    "JSON_LENGTH",
    "JSON_DEPTH",
    "JSON_TYPE",
    "JSON_VALID",
    "JSON_KEYS",
    "JSON_PRETTY",
    "JSON_STORAGE_SIZE",
    "JSON_VALUE",
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
    // (Comments are whitespace tokens: a definition of only comments is empty too.)
    if words.is_empty() {
        bail!("the definition is empty: {SINGLE_SELECT}");
    }
    if word(0).map(|t| kw(t, "CREATE")) != Some(true) {
        // A query reaches here only from a `_lazyview` row without a name (G3).
        if word(0).is_some_and(|t| {
            ["SELECT", "WITH", "VALUES", "TABLE"]
                .iter()
                .any(|k| kw(t, k))
                || matches!(t, Token::LParen)
        }) {
            bail!("the definition needs a name: set `_lazyview.name`, or write CREATE VIEW name AS SELECT …");
        }
        bail!("{SINGLE_SELECT}; found `{}` …", tokens[words[0]].token);
    }
    let view_at = (0..words.len())
        .position(|i| kw(&tokens[words[i]].token, "VIEW"))
        .ok_or_else(|| anyhow!("{SINGLE_SELECT}; found CREATE without VIEW"))?;
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
    match word(i + 1) {
        None => bail!("the definition is empty after AS: {SINGLE_SELECT}"),
        Some(t)
            if ["SELECT", "WITH", "VALUES", "TABLE"]
                .iter()
                .all(|k| !kw(t, k))
                && !matches!(t, Token::LParen) =>
        {
            bail!("{SINGLE_SELECT}; found `{t}` … after AS")
        }
        _ => {}
    }
    let statements = 1
        + (0..words.len())
            .filter(|&k| {
                matches!(word(k), Some(Token::SemiColon))
                    && word(k + 1).is_some_and(|t| !matches!(t, Token::SemiColon))
            })
            .count();
    if statements > 1 {
        bail!("the definition contains {statements} statements: {SINGLE_SELECT}");
    }
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(rest.to_vec());
    let query = parser.parse_query().context("parse SELECT")?;
    // The servers refuse variables in a view (ERROR 1351) and their values change without a
    // source change.
    let mut variable = None;
    let _ = visit_expressions(&query, |e: &Expr| {
        let id = match e {
            Expr::Identifier(i) => Some(i),
            Expr::CompoundIdentifier(parts) => parts.first(),
            _ => None,
        };
        if id.is_some_and(|i| i.quote_style.is_none() && i.value.starts_with('@')) {
            variable = Some(e.to_string());
            return ControlFlow::Break(());
        }
        ControlFlow::<()>::Continue(())
    });
    if let Some(v) = variable {
        bail!(
            "a view cannot read user or system variables (`{v}`): the servers refuse them in views"
        );
    }
    // The servers read `||` as OR unless sql_mode has PIPES_AS_CONCAT; the parser reads it as
    // concatenation. Neither reading can be trusted: ask for the explicit form.
    let mut pipes = false;
    let _ = visit_expressions(&query, |e: &Expr| {
        if matches!(
            e,
            Expr::BinaryOp {
                op: BinaryOperator::StringConcat,
                ..
            }
        ) {
            pipes = true;
        }
        ControlFlow::<()>::Continue(())
    });
    if pipes {
        bail!("`||` is OR on the server (sql_mode without PIPES_AS_CONCAT) but concatenation to the parser: write OR or CONCAT()");
    }
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
            t => bail!("unexpected `{t}` after the end of the SELECT statement"),
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

/// What a definition must be, for errors.
const SINGLE_SELECT: &str =
    "the definition must be a single SELECT statement (or CREATE VIEW name AS SELECT …)";

/// Definitions are tokenized without unescaping: string literals keep their escapes (`'a\'b'`,
/// `'x\_y'`), so rendered SQL means what was written, and quoted identifiers keep doubled quote
/// characters (see [`unquote`]).
/// Whether `def` is a `CREATE …` statement: its first token that is not whitespace or a comment
/// is CREATE, as [`parse_definition`] reads it (G2: `-- note` / `/* */` / `#` before it).
pub fn is_create_statement(def: &str) -> bool {
    match tokenize(def) {
        Ok(tokens) => tokens
            .iter()
            .find(|t| !matches!(t.token, Token::Whitespace(_)))
            .is_some_and(|t| {
                matches!(&t.token, Token::Word(w)
                    if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("CREATE"))
            }),
        // Unreadable text: the first word decides (parse_definition reports the error).
        Err(_) => def
            .split_whitespace()
            .next()
            .is_some_and(|w| w.eq_ignore_ascii_case("CREATE")),
    }
}

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
    crate::analyzer::referenced_tables(query, default_schema)
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
    /// Position in the FROM clause: (table index, join index).
    pub pos: (usize, Option<usize>),
    /// The table of a semi-/anti-join subquery (not in FROM): `EXISTS (SELECT 1 FROM <rows> AS
    /// alias WHERE …)` relating its rows to the view's rows, with `SUBST_MARKER` where the rows
    /// go (and [`semi_table_marker`]s for the subquery's other tables). Its lookups add this to
    /// the view's FROM and [`ViewPlan::lookup_where_sql`].
    pub semi: Option<String>,
    /// Lookups of this alias use [`ViewPlan::lookup_where_sql`] (semi-join tables, and NULL-side
    /// tables whose WHERE conjuncts keep NULL-extended rows).
    pub lookup_where: bool,
}

/// A constant filter of an alias (see `const_filters`): a row of its table is that alias's row
/// in some row of the query only if column `col` holds one of `values`, as decoded from the
/// binlog (`Datum::from_binlog`).
#[derive(Debug, Clone, PartialEq)]
pub struct ConstFilter {
    pub col: usize,
    pub values: Vec<Datum>,
}

impl ConstFilter {
    /// Whether `row`, a binlog image of `table`, can be a row of the alias pinned by `filters`:
    /// a few comparisons of decoded integers, no allocation. A row that is not a full image,
    /// or lacks the value, passes.
    pub fn passes(
        filters: &[ConstFilter],
        row: &mysql_async::binlog::row::BinlogRow,
        table: &TableInfo,
    ) -> bool {
        filters.is_empty()
            || row.len() != table.columns.len()
            || filters.iter().all(|f| match row.as_ref(f.col) {
                None => true,
                v => f
                    .values
                    .contains(&Datum::from_binlog(v, &table.columns[f.col])),
            })
    }
}

/// A child alias of a cascade pinned by its constant filters to some values of the FK (see
/// [`CascadeRoute::pinned`]): the parent column and, per value, its presence column.
#[derive(Debug, Clone, PartialEq)]
pub struct Pinned {
    pub parent_col: usize,
    pub values: Vec<(Datum, usize)>,
}

/// A hidden materialized column telling which units hold rows of a pinned cascade child alias
/// (see `cascade_derivations`): a grouped view counts the alias's rows with one FK value per
/// group (`counts`), a select/join view carries the alias's FK value. The units a cascade from
/// a parent key changes are the materialized units it marks for that key ([`Self::marks`]),
/// plus, for a key update into a pinned value, the units holding such rows now
/// ([`Self::holds`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Presence {
    /// Materialized column name, `_lv_c<k>`.
    pub name: String,
    /// The SELECT expression computing it.
    pub expr: String,
    /// The alias's FK column, as an expression of the view's FROM clause.
    pub fk: String,
    /// Grouped views: the FK value it counts.
    pub counts: Option<Datum>,
}

impl Presence {
    /// Predicate on the materialized table: the units holding the alias's rows with FK `value`.
    pub fn marks(&self, value: &Datum) -> String {
        match self.counts {
            Some(_) => format!("{} > 0", quote_ident(&self.name)),
            None => format!("{} = {}", quote_ident(&self.name), int_sql(value)),
        }
    }

    /// Predicate on the sources: the rows of the alias with FK `value`.
    pub fn holds(&self, value: &Datum) -> String {
        format!("{} = {}", self.fk, int_sql(value))
    }
}

/// An integer datum (constant filters hold only integers) as an SQL literal.
fn int_sql(d: &Datum) -> String {
    match d {
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        other => unreachable!("non-integer filter value {other:?}"),
    }
}

/// Partial direct units (see `partial_units`): aliases whose rows give part of the unit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PartialUnits {
    /// The distinct sets of unit positions (ascending) some alias gives.
    pub sets: Vec<Vec<usize>>,
    /// Per alias of the FROM clause (aliases past the end have none): the set it gives and, per
    /// position of the set, its column equal to that unit expression.
    pub aliases: Vec<Option<(usize, Vec<usize>)>>,
}

/// Lineage of a lookup-resolved alias (select/join views): hidden, indexed materialized columns
/// carrying the primary key of that alias's row in each materialized row. A changed row of the
/// table maps to the materialized rows it is part of by an index lookup on the materialized
/// table, never by joining its old image with the current state of the other tables (which may
/// have changed too, in a transaction not read from the binlog yet).
#[derive(Debug, Clone, PartialEq)]
pub struct LineageKey {
    pub alias: usize,
    /// Materialized column names, `_lv_a<alias>_pk<j>`, one per primary-key column.
    pub cols: Vec<String>,
    /// The primary-key columns as source expressions (`c`.`id`).
    pub exprs: Vec<String>,
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
    /// `None`: affected units are unknown, the view is rebuilt (unless `traced`).
    pub unit_from_parent: Option<Vec<usize>>,
    /// The child table and its FK column ordinals (constraint order, matching `parent_cols`).
    pub child: TableRef,
    pub child_cols: Vec<usize>,
    /// The view reads the parent through a lineage or direct-unit alias that every alias of the
    /// child is joined to on the whole FK, in a shape where a child row's presence in a
    /// materialized row implies that alias carries its parent (`cascade_joined`), and the same
    /// holds down every further cascade out of the child: the parent's own rows (routed to that
    /// alias) find every materialized row the cascade changes, so the parent needs no route of
    /// its own.
    pub traced: bool,
    /// Why the route is neither unit-derived, traced nor `derived` (the view rebuilds on the
    /// parent's deletes / key updates).
    pub untraced: Option<String>,
    /// The parent's row gives the units the cascade can change, alias by alias of the child
    /// (`cascade_derivations`), never a rebuild: `partial_from_parent` gives partial unit keys
    /// (set of `ViewPlan::partial`, the parent column for each position); each entry of
    /// `pinned` is an alias the constant filters pin to some values of the (single-column) FK:
    /// a parent key among them names the materialized units holding that alias's rows through
    /// a presence column (`ViewPlan::presence`), any other key reaches nothing.
    pub derived: bool,
    pub partial_from_parent: Vec<(usize, Vec<usize>)>,
    pub pinned: Vec<Pinned>,
    /// Every alias of the child pins its (single-column) FK with constant filters: the parent
    /// column and the values they allow. A parent key outside them is the FK of no alias's
    /// row, before or after the change, so the cascade changes nothing the view reads,
    /// whatever the route (`cascade_reach`).
    pub reach: Option<(usize, Vec<Datum>)>,
}

#[derive(Debug, Clone)]
pub struct ViewPlan {
    pub schema: String,
    pub name: String,
    pub hash: String,
    pub mode: Mode,
    pub tables: Vec<TableRef>,
    /// Full-refresh plans: per entry of `tables`, the columns the statement may read (`None`:
    /// any). An UPDATE that changes none of them cannot change the view.
    pub table_columns: Vec<Option<Vec<bool>>>,
    pub aliases: Vec<AliasPlan>,
    /// Rendered projection items (with `AS` aliases where given).
    pub projection: Vec<String>,
    pub from_sql: String,
    /// The FROM clause (incremental plans), for lookups that replace several tables at once.
    pub from_items: Vec<TableWithJoins>,
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
    /// Lineage keys of the lookup-resolved aliases that have a primary key (select/join views).
    /// Their images carry only the primary key (`image_cols`).
    pub lineage: Vec<LineageKey>,
    pub cascades: Vec<CascadeRoute>,
    /// Column names from `CREATE VIEW v (a, b, ...)`, applied positionally to the materialized
    /// table (the SELECT itself keeps its own aliases so GROUP BY / HAVING resolve as written).
    pub output_names: Option<Vec<String>>,
    /// Top-N views (single-unit, recomputed with one bounded query per batch): the
    /// `ORDER BY … LIMIT n` that ends their SELECT.
    pub order_limit_sql: Option<String>,
    /// Named windows (`WINDOW w AS (…)`), rendered after HAVING: the projection's `OVER w`
    /// references need them (L2).
    pub window_sql: Option<String>,
    /// WHERE of the lookups of the aliases marked `lookup_where`: `where_sql` without the
    /// subquery predicates (evaluated on the substituted rows instead) and without conjuncts that
    /// keep NULL-extended rows.
    pub lookup_where_sql: Option<String>,
    /// Per alias of the FROM clause: its constant filters (aliases past the end have none).
    pub alias_filters: Vec<Vec<ConstFilter>>,
    /// Aliases traced by partial units instead of lookups.
    pub partial: PartialUnits,
    /// Hidden presence columns of pinned cascade children (after the lineage columns).
    pub presence: Vec<Presence>,
}

impl ViewPlan {
    pub fn is_incremental(&self) -> bool {
        matches!(self.mode, Mode::Spj | Mode::Grouped)
    }

    /// SELECT producing materialized rows (projection, hidden unit columns, then the lineage
    /// columns), optionally restricted by `filter` (a predicate over `unit_exprs`).
    pub fn select_sql(&self, hidden_units: &[usize], filter: Option<&str>) -> String {
        if let Mode::Full(_) = self.mode {
            return self.full_select.clone();
        }
        let mut cols = self.projection.clone();
        for &u in hidden_units {
            cols.push(format!("{} AS `_lv_u{u}`", self.unit_exprs[u]));
        }
        for k in &self.lineage {
            for (e, c) in k.exprs.iter().zip(&k.cols) {
                cols.push(format!("{e} AS {}", quote_ident(c)));
            }
        }
        for p in &self.presence {
            cols.push(format!("{} AS {}", p.expr, quote_ident(&p.name)));
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
        if let Some(w) = &self.window_sql {
            sql.push(' ');
            sql.push_str(w);
        }
        if let Some(o) = &self.order_limit_sql {
            sql.push(' ');
            sql.push_str(o);
        }
        sql
    }

    /// How a changed row of each table maps to the materialized rows it affects, for logs:
    /// `alias:unit` (straight from the row image), `alias:lineage` (old rows by the hidden key
    /// columns, new rows by key), `alias:partial` (part of the unit from the row image, every unit
    /// matching it refreshed), `alias:lookup` (images joined with the current state) or, for
    /// full-refresh views, `rebuild`; parents of cascading foreign keys as `^parent:unit|rebuild`.
    pub fn tracing(&self) -> String {
        if !self.is_incremental() && !matches!(self.mode, Mode::Global) {
            return "rebuild".into();
        }
        let mut parts: Vec<String> = self
            .aliases
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let how = if matches!(self.mode, Mode::Global) {
                    "recompute"
                } else if a.direct_unit.is_some() {
                    "unit"
                } else if self.lineage_of(i).is_some() {
                    "lineage"
                } else if self.partial_of(i).is_some() {
                    "partial"
                } else {
                    "lookup"
                };
                format!("{}:{how}", a.name)
            })
            .collect();
        parts.extend(self.cascades.iter().map(|c| {
            let how = if c.unit_from_parent.is_some() {
                "unit"
            } else if c.traced {
                "traced"
            } else if c.derived {
                "derived"
            } else {
                "rebuild"
            };
            format!("^{}:{how}", c.parent.name)
        }));
        parts.join(",")
    }

    /// Number of lineage columns (they follow the hidden unit columns).
    pub fn lineage_width(&self) -> usize {
        self.lineage.iter().map(|k| k.cols.len()).sum()
    }

    /// The lineage key of `alias`, if it has one.
    pub fn lineage_of(&self, alias: usize) -> Option<&LineageKey> {
        self.lineage.iter().find(|k| k.alias == alias)
    }

    /// Whether the unit and the lineage keys together identify each materialized row of a
    /// select/join view: the first table has a row key (its primary key or a UNIQUE NOT NULL
    /// key, which is then the unit) and every other table of the FROM clause carries its row
    /// key as a lineage key. A row of such a view is one combination of rows of these tables
    /// (with NULLs for the unmatched side of a LEFT JOIN), so no two rows agree on all of them.
    /// Tables of semi- and anti-join subqueries and of ORDER BY subqueries contribute no rows
    /// of their own. A first table without a row key (equal rows share their unit), a table
    /// mapped to units straight from its images (no lineage columns), one without a row key
    /// (lineage by the whole row: duplicates) and any other table without lineage leave the
    /// rows unidentified. Grouped views and single-table views are identified by the unit
    /// alone (`unit_unique`).
    pub fn rows_identified(&self) -> bool {
        let keyed = |a: usize| self.aliases[a].table.row_key().map(<[usize]>::len);
        self.mode == Mode::Spj
            && !self.unit_exprs.is_empty()
            && !self.aliases.is_empty()
            && keyed(0).is_some()
            && (1..self.aliases.len()).all(|a| {
                let ap = &self.aliases[a];
                ap.semi.is_some()
                    || ap.pos.0 == usize::MAX
                    || keyed(a)
                        .is_some_and(|n| self.lineage_of(a).is_some_and(|k| k.cols.len() == n))
            })
    }

    /// The constant filters of `alias`: a row failing one is never that alias's row.
    pub fn filters_of(&self, alias: usize) -> &[ConstFilter] {
        self.alias_filters.get(alias).map_or(&[], Vec::as_slice)
    }

    /// The partial unit of `alias`: the index of its set in `partial.sets` and its column for
    /// each position of the set.
    pub fn partial_of(&self, alias: usize) -> Option<(usize, &[usize])> {
        let (set, cols) = self.partial.aliases.get(alias)?.as_ref()?;
        Some((*set, cols))
    }

    /// `SELECT DISTINCT <units> FROM ... WHERE ... AND <filter>`: the units of the current rows
    /// that `filter` (over a lineage alias's primary key) selects.
    pub fn key_lookup_sql(&self, filter: &str) -> String {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            self.unit_exprs.join(", "),
            self.from_sql
        );
        push_where(&mut sql, self.where_sql.as_deref(), Some(filter));
        sql
    }

    /// The semi-join template of `alias` with its rows `own` and, for the subquery's other
    /// tables, the rows `others` gives (`None`: the table itself).
    fn semi_exists(
        &self,
        alias: usize,
        own: &str,
        mut others: impl FnMut(usize) -> Option<String>,
    ) -> String {
        let mut exists = self.aliases[alias].semi.clone().unwrap_or_default();
        for (j, a) in self.aliases.iter().enumerate() {
            let marker = semi_table_marker(j);
            if exists.contains(&marker) {
                let rows = others(j).unwrap_or_else(|| {
                    format!(
                        "{}.{}",
                        quote_ident(&a.table.schema),
                        quote_ident(&a.table.name)
                    )
                });
                exists = exists.replacen(&marker, &rows, 1);
            }
        }
        exists.replacen(SUBST_MARKER, own, 1)
    }

    /// `SELECT DISTINCT <units> FROM <from with alias replaced by derived rows> WHERE ...`
    pub fn lookup_sql(&self, alias: usize, derived: &str) -> String {
        let a = &self.aliases[alias];
        if a.semi.is_some() {
            let mut sql = format!(
                "SELECT DISTINCT {} FROM {}",
                self.unit_exprs.join(", "),
                self.from_sql
            );
            let exists = self.semi_exists(alias, derived, |_| None);
            push_where(&mut sql, self.lookup_where_sql.as_deref(), Some(&exists));
            return sql;
        }
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}{}{}",
            self.unit_exprs.join(", "),
            a.subst_from.0,
            derived,
            a.subst_from.1
        );
        let where_sql = if a.lookup_where {
            self.lookup_where_sql.as_deref()
        } else {
            self.where_sql.as_deref()
        };
        push_where(&mut sql, where_sql, None);
        sql
    }

    /// As [`Self::lookup_sql`] with several aliases replaced by derived rows at once
    /// (`(alias, derived table)`). Also returns the order (indexes into `subst`) in which the
    /// derived tables appear in the statement, which is the order of their parameters.
    pub fn lookup_sql_multi(&self, subst: &[(usize, String)]) -> Result<(String, Vec<usize>)> {
        let mut from = self.from_items.clone();
        let marker = |k: usize| format!("__lv_subst_{k}__");
        // Tables of the FROM clause are replaced in place; semi-join tables join as EXISTS, one
        // per subquery: tables of one subquery substituted together share it.
        let mut exists = Vec::new();
        let mut done = vec![false; subst.len()];
        for (k, (a, _)) in subst.iter().enumerate() {
            let ap = &self.aliases[*a];
            match &ap.semi {
                Some(_) if done[k] => {}
                Some(_) => exists.push(self.semi_exists(*a, &marker(k), |j| {
                    let kj = subst.iter().position(|(b, _)| *b == j)?;
                    done[kj] = true;
                    Some(marker(kj))
                })),
                None => substitute_factor(&mut from, ap.pos, &ap.name, &marker(k)),
            }
        }
        let from_sql = from
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {from_sql}",
            self.unit_exprs.join(", ")
        );
        let weak = subst.iter().any(|(a, _)| self.aliases[*a].lookup_where);
        let where_sql = if weak || !exists.is_empty() {
            self.lookup_where_sql.as_deref()
        } else {
            self.where_sql.as_deref()
        };
        let filter = (!exists.is_empty()).then(|| exists.join(" AND "));
        push_where(&mut sql, where_sql, filter.as_deref());
        let mut order: Vec<(usize, usize)> = Vec::new();
        for k in 0..subst.len() {
            let at = sql
                .find(&marker(k))
                .ok_or_else(|| anyhow!("internal: substitution marker missing"))?;
            order.push((at, k));
        }
        order.sort_unstable();
        for (k, (_, derived)) in subst.iter().enumerate() {
            sql = sql.replacen(&marker(k), derived, 1);
        }
        Ok((sql, order.into_iter().map(|(_, k)| k).collect()))
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

    /// `alias.column` of a column reference, for messages.
    fn name(&self, c: ColRef) -> String {
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

struct Usage<'s> {
    scope: &'s Scope<'s>,
    select_aliases: Vec<String>,
    referenced: Vec<Vec<bool>>,
    membership: Vec<Vec<bool>>,
    unresolved: Option<String>,
}

impl Usage<'_> {
    /// Marks the columns of the FROM clause that `root` names, leaving other names alone.
    fn mark_known(&mut self, root: &Expr) {
        let unresolved = self.unresolved.take();
        self.mark(root, false, true);
        self.unresolved = unresolved;
    }

    fn mark<V: Visit>(&mut self, root: &V, set_membership: bool, allow_select_alias: bool) {
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

fn has_subquery(e: &Expr) -> bool {
    let mut found = false;
    let _ = visit_expressions(e, |x: &Expr| {
        if matches!(
            x,
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
        ) {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

/// The tables the subqueries of an ORDER BY read, each with the columns those subqueries may
/// read: every column they name (whatever the qualifier), all of them under `*` or a USING /
/// NATURAL join, and the base columns of the generated ones.
fn order_subquery_tables(
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
                let Some(info) = tables.get(&t) else {
                    continue;
                };
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
fn const_filters(
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
fn filter_column(column: &crate::catalog::Column) -> bool {
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
fn partial_units(
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

/// What [`cascade_derivations`] reads of the plan.
struct PinContext<'a> {
    aliases: &'a [AliasPlan],
    partial: &'a PartialUnits,
    filters: &'a [Vec<ConstFilter>],
    /// A GROUP BY view (presence counts per group); otherwise a select/join view.
    grouped: bool,
    having: bool,
    null_side: &'a [usize],
}

/// For a cascade the parent's row neither gives units for nor traces through its own alias:
/// whether it still gives the units the cascade can change, alias by alias of the child (an
/// alias with a unit route of its own needs nothing here), so that the route never rebuilds:
///
/// * an alias whose partial unit columns are all FK columns gets partial keys from the
///   parent's key (`partial_from_parent`);
/// * an alias pinned by its constant filters to some values of a single-column FK can only
///   hold child rows with those values: a parent key among them changes exactly the units
///   holding such rows, which a hidden presence column marks in the materialized table
///   ([`Presence`]); any other key reaches nothing. Exact only where those rows cannot keep a
///   unit out of the table: a grouped view without HAVING (or with the alias inner-joined and
///   pinned to one value: removing its rows removes the whole group), a select/join view
///   whose WHERE keeps no NULL-extended rows of the alias.
///
/// When every alias is covered the route is `derived`; otherwise it stays untraced.
/// (Only where the cascade continues into no untraced level: the caller checks.)
fn cascade_derivations(c: &mut CascadeRoute, ctx: &PinContext, presence: &mut Vec<Presence>) {
    let to_parent = |col: usize| -> Option<usize> {
        let j = c.child_cols.iter().position(|&x| x == col)?;
        let pc = c.parent_cols[j];
        filter_or_key_column(&c.parent.columns[pc]).then_some(pc)
    };
    let (mut parts, mut pins, mut added) = (Vec::new(), Vec::new(), Vec::new());
    for (ai, a) in ctx.aliases.iter().enumerate() {
        if (a.table.schema.as_str(), a.table.name.as_str())
            != (c.child.0.as_str(), c.child.1.as_str())
        {
            continue;
        }
        // A unit alias whose unit columns are all FK columns has a unit route of its own.
        if a.direct_unit.as_ref().is_some_and(|cols| {
            cols.iter()
                .all(|&col| a.table.columns[col].exact_key() && to_parent(col).is_some())
        }) {
            continue;
        }
        let part = ctx
            .partial
            .aliases
            .get(ai)
            .and_then(Option::as_ref)
            .and_then(|(set, cols)| {
                let pcols: Option<Vec<usize>> = cols.iter().map(|&col| to_parent(col)).collect();
                Some((*set, pcols?))
            });
        if let Some(p) = part {
            parts.push(p);
            continue;
        }
        let ([fk], [pc]) = (c.child_cols.as_slice(), c.parent_cols.as_slice()) else {
            return;
        };
        let Some(f) = ctx
            .filters
            .get(ai)
            .into_iter()
            .flatten()
            .find(|f| f.col == *fk)
        else {
            return;
        };
        let exact = if ctx.grouped {
            !ctx.having || (!ctx.null_side.contains(&ai) && f.values.len() == 1)
        } else {
            !a.lookup_where
        };
        if !exact || !filter_column(&c.parent.columns[*pc]) {
            return;
        }
        let fk_sql = format!(
            "{}.{}",
            quote_ident(&a.name),
            quote_ident(&a.table.columns[*fk].name)
        );
        let mut values: Vec<(Datum, usize)> = Vec::new();
        for (j, v) in f.values.iter().enumerate() {
            // Grouped: one count per value; select/join: the FK value itself, once.
            if !ctx.grouped && j > 0 {
                values.push((v.clone(), values[0].1));
                continue;
            }
            let k = presence.len() + added.len();
            added.push(Presence {
                name: format!("_lv_c{k}"),
                expr: if ctx.grouped {
                    format!("COUNT(CASE WHEN {fk_sql} = {} THEN 1 END)", int_sql(v))
                } else {
                    fk_sql.clone()
                },
                fk: fk_sql.clone(),
                counts: ctx.grouped.then(|| v.clone()),
            });
            values.push((v.clone(), k));
        }
        pins.push(Pinned {
            parent_col: *pc,
            values,
        });
    }
    if parts.is_empty() && pins.is_empty() {
        return;
    }
    presence.extend(added);
    c.derived = true;
    c.untraced = None;
    c.partial_from_parent = parts;
    c.pinned = pins;
}

/// Where every alias of a cascade's child pins its single-column FK with constant filters, the
/// parent column and the values they allow ([`CascadeRoute::reach`]): child rows with another
/// FK value are no alias's rows, before the cascade (old key) and after it (new key, or NULL),
/// and a cascade continuing out of them (`continues`: joined to an alias of this child) reaches
/// no materialized row either. The route's reason names the keys that still reach the view.
fn cascade_reach(
    c: &mut CascadeRoute,
    aliases: &[AliasPlan],
    filters: &[Vec<ConstFilter>],
    continues: bool,
) {
    let ([fk], [pc]) = (c.child_cols.as_slice(), c.parent_cols.as_slice()) else {
        return;
    };
    let (parent, mut child) = (&c.parent.columns[*pc], None);
    let mut values: Vec<Datum> = Vec::new();
    for (ai, a) in aliases.iter().enumerate() {
        if (a.table.schema.as_str(), a.table.name.as_str())
            != (c.child.0.as_str(), c.child.1.as_str())
        {
            continue;
        }
        let Some(f) = filters.get(ai).into_iter().flatten().find(|f| f.col == *fk) else {
            return;
        };
        child = Some(&a.table.columns[*fk]);
        for v in &f.values {
            if !values.contains(v) {
                values.push(v.clone());
            }
        }
    }
    // Both sides decode to the same integer kind (FK columns share their sign).
    let unsigned = |col: &crate::catalog::Column| matches!(col.kind, ColKind::Unsigned(_));
    let Some(child) = child else { return };
    if !continues || !filter_column(parent) || unsigned(parent) != unsigned(child) {
        return;
    }
    if let Some(r) = &mut c.untraced {
        let list: Vec<String> = values.iter().map(int_sql).collect();
        r.push_str(&format!(
            " (only `{}`.`{}` IN ({}) reaches it)",
            c.parent.name,
            parent.name,
            list.join(", ")
        ));
    }
    c.reach = Some((*pc, values));
}

/// A parent key column whose binlog value is its SQL value (as for unit keys), not generated.
fn filter_or_key_column(column: &crate::catalog::Column) -> bool {
    column.exact_key() && column.generated.is_none()
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
fn json_computed(e: &Expr) -> bool {
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
fn json_valued(e: &Expr, json_col: &dyn Fn(&Expr) -> bool) -> bool {
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

fn function_name(f: &sqlparser::ast::Function) -> String {
    match f.name.0.last() {
        Some(ObjectNamePart::Identifier(i)) => ident_name(i).to_ascii_uppercase(),
        _ => String::new(),
    }
}

/// Reasons the query cannot be maintained incrementally, whether it aggregates, and the
/// PARTITION BY expressions of each window function call.
fn scan_exprs(select: &Select) -> (Option<String>, bool, Vec<Vec<Expr>>) {
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
fn reduce_unit(
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

fn quote(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

fn qident(id: &str) -> Ident {
    Ident::with_quote('`', id)
}

/// Plans a definition: the query analyzer resolves CTEs, derived tables and subqueries and
/// normalizes them into flat blocks it plans with [`plan_block`] (see `crate::analyzer`).
/// `tables` must contain metadata for every table [`referenced_tables`] lists.
pub fn plan(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<ViewPlan> {
    crate::analyzer::plan(def, default_schema, tables)
}

/// The block planner: plans one flat block (no CTE references or derived tables left).
/// `tables` must contain metadata for every table the query reads.
pub fn plan_block(
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
    let full_with = |reason: &str, watched: Vec<TableRef>, columns| ViewPlan {
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
        table_columns: columns,
        aliases: Vec::new(),
        projection: Vec::new(),
        from_sql: String::new(),
        from_items: Vec::new(),
        where_sql: None,
        group_sql: None,
        having_sql: None,
        unit_exprs: Vec::new(),
        unit_in_projection: Vec::new(),
        full_select: full_select.clone(),
        order_limit_sql: None,
        window_sql: None,
        lookup_where_sql: None,
        alias_filters: Vec::new(),
        partial: PartialUnits::default(),
        presence: Vec::new(),
        unit_unique: false,
        lineage: Vec::new(),
        output_names: output_names.clone(),
    };
    let full = |reason: &str| {
        let columns = read_columns(&def.query, &all_tables, tables);
        full_with(reason, all_tables.clone(), columns)
    };

    // A SQL view has no binlog rows of its own: watch its base tables, rebuild on change.
    if let Some(v) = all_tables.iter().find(|t| tables[*t].is_view) {
        // Which base columns the SQL view reads is not known here: any change counts.
        let bases = expand_views(&all_tables, tables);
        let columns = vec![None; bases.len()];
        return Ok(full_with(
            &format!("reads SQL view `{}`", v.1),
            bases,
            columns,
        ));
    }

    let mut query = def.query.clone();
    let mut order_by = query.order_by.take();
    if query.with.is_some() {
        return Ok(full("WITH clause"));
    }
    // ORDER BY … LIMIT n: the first n rows are one unit, recomputed with one bounded query per
    // batch. Without ORDER BY the rows are not a function of the data: nothing to converge to.
    let top_n_limit = match (&order_by, query.limit_clause.take(), query.fetch.is_some()) {
        (_, None, false) => None,
        (Some(_), Some(l), false) => Some(l),
        _ => return Ok(full("LIMIT without ORDER BY")),
    };
    let select: &mut Select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        // C4: name what blocks a top-N over a set operation (refreshed in full: tracing it
        // would need an internal view of the union, shelved).
        SetExpr::SetOperation {
            op, set_quantifier, ..
        } if top_n_limit.is_some() => {
            let all = matches!(
                set_quantifier,
                sqlparser::ast::SetQuantifier::All | sqlparser::ast::SetQuantifier::AllByName
            );
            let op = match op {
                sqlparser::ast::SetOperator::Union if all => "UNION ALL",
                sqlparser::ast::SetOperator::Union => "UNION",
                sqlparser::ast::SetOperator::Intersect => "INTERSECT",
                _ => "EXCEPT",
            };
            return Ok(full(&format!("ORDER BY … LIMIT over a {op}")));
        }
        _ => return Ok(full("set operation (UNION/INTERSECT/EXCEPT)")),
    };
    if select.distinct.is_some() {
        return Ok(full("DISTINCT"));
    }
    if select.from.is_empty() {
        return Ok(full("no FROM clause"));
    }
    // `*` over USING / NATURAL joins lists the shared columns once (A4).
    let expanded_star = expand_star_over_using(select, default_schema, tables)?;
    let star_items: Vec<Option<&str>> = if expanded_star {
        // Expanded items have no source text of their own.
        Vec::new()
    } else {
        def.item_sql.iter().map(|s| s.as_deref()).collect()
    };

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
    if select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)))
        && joins
            .iter()
            .any(|(_, _, c)| matches!(c, JoinConstraint::Using(_) | JoinConstraint::Natural))
    {
        return Ok(full(STAR_USING));
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
                item_sql.push(star_items.get(ii).copied().flatten());
            }
        }
    }
    if projection.is_empty() {
        bail!("the SELECT has no columns");
    }
    // Output names the server will give (aliases and plain columns; other expressions get
    // server-generated names that `names.rs` keeps unique): a duplicate fails CREATE TABLE,
    // so it fails here, by name (L3). A column list renames everything positionally.
    if def.columns.is_empty() {
        let mut seen: Vec<String> = Vec::new();
        for item in &projection {
            let name = match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(ident_name(alias)),
                SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(ident_name(id)),
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    parts.last().map(ident_name)
                }
                _ => None,
            };
            if let Some(n) = name {
                if seen.iter().any(|s| s.eq_ignore_ascii_case(&n)) {
                    bail!(
                        "duplicate column name `{n}` in the SELECT (the server refuses such a view): alias one of them"
                    );
                }
                seen.push(n);
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

    let scope = Scope { aliases: &aliases };
    strip_column_schemas(select, &scope);
    // VIRTUAL generated columns have no value in the row images (lookups substitute literal
    // rows): every reference becomes the column's own expression over its base columns, before
    // anything (window partitions, units) is derived from the expressions.
    // An unaliased expression over a VIRTUAL column is named after its text by the server; once
    // inlined it carries that name as an alias (a GROUP BY sees the same inlined text, which
    // MySQL's ONLY_FULL_GROUP_BY compares).
    for (item, src) in select.projection.iter_mut().zip(item_sql.iter_mut()) {
        let SelectItem::UnnamedExpr(e) = item else {
            continue;
        };
        if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            continue;
        }
        let mut reads_virtual = false;
        let _ = visit_expressions(&*e, |x: &Expr| {
            if let Some((a, c)) = scope.resolve(x) {
                if scope.aliases[a].1.columns[c]
                    .generated
                    .as_ref()
                    .is_some_and(|g| !g.stored)
                {
                    reads_virtual = true;
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        });
        if !reads_virtual {
            continue;
        }
        match src.filter(|t| t.chars().count() <= 64 && !t.ends_with(' ') && !t.contains('\n')) {
            Some(t) => {
                *item = SelectItem::ExprWithAlias {
                    expr: e.clone(),
                    alias: qident(t),
                };
                *src = None;
            }
            None => {
                return Ok(full(&format!(
                    "unaliased column `{e}` reads a VIRTUAL generated column: give it an alias"
                )))
            }
        }
    }
    let top_n_order = if top_n_limit.is_some() {
        order_by.as_mut()
    } else {
        None
    };
    if let Some(reason) = inline_virtual_columns(select, top_n_order, &scope)? {
        return Ok(full(&reason));
    }
    // Top-N rows are chosen by the ORDER BY: its columns (also of joined aliases, also as
    // aggregate inputs) are read columns, whether or not they are projected (L1).
    let order_usage = order_by.clone();
    let top_n = top_n_limit.map(|l| format!("{}{l}", order_by.as_ref().unwrap()));
    let (reason, has_agg, windows) = scan_exprs(select);
    if let Some(r) = reason {
        return Ok(full(&r));
    }
    // The ORDER BY of a top-N view decides which rows it holds: a value that changes without a
    // source change (RAND(), NOW(), …; also inside its subqueries) leaves nothing to trace.
    if let (Some(o), Some(_)) = (&order_by, &top_n) {
        let mut why = None;
        let _ = visit_expressions(o, |e: &Expr| {
            // (Not a filter context: no exact NOT EXISTS form, and MariaDB re-uses a stale NOT
            // IN result when the refresh statement is re-executed, F5.)
            if matches!(e, Expr::InSubquery { negated: true, .. }) {
                why = Some(
                    "NOT IN subquery in the ORDER BY of a top-N view (MariaDB re-executes it with a stale result): write NOT EXISTS"
                        .to_string(),
                );
                return ControlFlow::Break(());
            }
            if let Expr::Function(f) = e {
                let n = function_name(f);
                if f.over.is_none() && NONDETERMINISTIC.contains(&n.as_str()) {
                    why = Some(format!(
                        "non-deterministic function {n} in the ORDER BY of a top-N view"
                    ));
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        });
        if let Some(r) = why {
            return Ok(full(&r));
        }
    }
    // A window's values depend on its whole partition: the unit is the PARTITION BY expressions
    // every window shares (filtering on them keeps whole partitions); without any, one unit.
    let windowed = !windows.is_empty();
    let partition: Vec<Expr> = match windows.split_first() {
        Some((first, rest)) => first
            .iter()
            .filter(|e| {
                rest.iter()
                    .all(|w| w.iter().any(|x| x.to_string() == e.to_string()))
            })
            .cloned()
            .collect(),
        None => Vec::new(),
    };

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
    usage.mark(&select.named_window, false, false);
    // (ORDER BY without LIMIT does not decide which rows the view holds.)
    if let (Some(o), Some(_)) = (&order_usage, &top_n) {
        if let OrderByKind::Expressions(es) = &o.kind {
            for e in es {
                if has_subquery(&e.expr) {
                    // In a subquery a name resolves in its own FROM first: only the view's
                    // columns it names are marked (a name of both marks one column too many,
                    // never too few); its tables are traced below.
                    usage.mark_known(&e.expr);
                } else {
                    usage.mark(&e.expr, false, true);
                }
            }
        }
    }
    for e in &partition {
        usage.mark(e, true, false);
    }

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

    let mode = if top_n.is_some()
        || windowed && (partition.is_empty() || has_agg || !resolved_groups.is_empty())
    {
        Mode::Global
    } else if windowed || !resolved_groups.is_empty() {
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
            // Without a row key (primary key or a UNIQUE NOT NULL key), equal rows are one
            // unit: the referenced columns whose binlog values compare exactly with their SQL
            // values.
            let key: Vec<usize> = match t.row_key() {
                Some(k) => k.to_vec(),
                None => (0..t.columns.len())
                    .filter(|&c| referenced[0][c] && t.columns[c].exact_key())
                    .collect(),
            };
            if key.is_empty() {
                return Ok(full(&format!(
                    "first table `{}` has no primary key",
                    t.name
                )));
            }
            key.iter()
                .map(|&c| {
                    referenced[0][c] = true;
                    membership[0][c] = true;
                    Expr::CompoundIdentifier(vec![qident(n), qident(&t.columns[c].name)])
                })
                .collect()
        }
        Mode::Grouped if windowed => partition.clone(),
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

    // A group key that the rest of the key determines adds nothing to the unit (DISTINCT over a
    // table's primary key and its other columns, GROUP BY o.id, c.name with c joined on its key).
    let unit_refs = if mode == Mode::Grouped && !windowed {
        reduce_unit(unit_refs, &scope, &base_edges, &own_edges)
    } else {
        unit_refs
    };
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
    // A WHERE conjunct that keeps NULL-extended rows (`c.id IS NULL`, COALESCE, OR) cannot be
    // evaluated on a substituted row of its NULL-side table: lookups of that table leave it out
    // (a superset of the affected units) and the refresh evaluates it. Such a table is traced
    // by lookups for both images: rows it stops hiding were never materialized (no lineage).
    let mut weak: Vec<usize> = Vec::new();
    let mut lookup_where: Option<Vec<String>> = None;
    if let Some(w) = &select.selection {
        let mut cs = Vec::new();
        conjuncts(w, &mut cs);
        if cs.iter().any(|c| on_null_side(c) && !null_rejecting(c)) {
            let mut kept = Vec::new();
            for c in cs {
                if on_null_side(c) && !null_rejecting(c) {
                    for (a, _) in col_refs(c, &scope) {
                        if null_side.contains(&a) && !weak.contains(&a) {
                            weak.push(a);
                        }
                    }
                } else {
                    kept.push(format!("({c})"));
                }
            }
            lookup_where = Some(kept);
        }
    }
    // JSON unit keys: a JSON column is a plain unit (the engine binds it as `CAST(? AS JSON)`,
    // typed by the materialized column, on MySQL; MariaDB JSON is text). A JSON value computed by
    // a function is not: MariaDB compares it as JSON (`JSON_EXTRACT(d, '$.a') = '"x"'` is false
    // for "x") while its unit filter binds the text, so its groups would be refreshed empty.
    if let Some(u) = unit_refs.iter().find(|u| json_computed(u)) {
        return Ok(full(&format!(
            "group key `{u}` is a JSON value computed by a function (MariaDB compares it as JSON, its unit filter as text): group by the column, or by its text with ->> / JSON_UNQUOTE"
        )));
    }
    // A JSON value in a composite unit (G5): MySQL compares JSON by value in `doc = ?` and in a
    // one-row `(g, doc) IN ((?, ?))`, but by representation in a row IN list of two or more
    // tuples, the engine's filter for several units, so a refresh would miss JSON-equal rows.
    // (MariaDB JSON is LONGTEXT: its columns are not of type json.)
    if unit_refs.len() > 1 {
        let json_col = |x: &Expr| {
            scope
                .resolve(x)
                .is_some_and(|(a, c)| aliases[a].1.columns[c].data_type == "json")
        };
        if let Some(u) = unit_refs.iter().find(|u| json_valued(u, &json_col)) {
            return Ok(full(&format!(
                "JSON value `{}` in a composite group key: MySQL compares JSON by representation in multi-row IN lists",
                u.to_string().replace('`', "")
            )));
        }
    }
    // (BIT unit keys: binlog values become unsigned integers and are bound as such; both sides
    // of every comparison see the same number.)
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
            pos: alias_pos[ai],
            semi: None,
            lookup_where: weak.contains(&ai),
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
    // Lineage (select/join views): a lookup-resolved table is traced to the materialized rows
    // its rows are part of through hidden key columns: its primary key or, without one, the
    // whole row (equal duplicates belong to the same materialized rows). Its images carry only
    // that key, and an UPDATE of a key column must refresh them (the hidden columns change).
    // Only keys whose binlog values compare exactly with their SQL values qualify; others keep
    // looking up images joined with the current state.
    let mut lineage = Vec::new();
    if mode == Mode::Spj {
        for (ai, a) in alias_plans.iter_mut().enumerate() {
            let key: Vec<usize> = match a.table.row_key() {
                Some(k) => k.to_vec(),
                None => (0..a.table.columns.len()).collect(),
            };
            if a.direct_unit.is_some()
                || a.lookup_where
                || key.is_empty()
                || !key.iter().all(|&c| a.table.columns[c].exact_key())
            {
                continue;
            }
            for &c in &key {
                a.referenced[c] = true;
            }
            lineage.push(LineageKey {
                alias: ai,
                cols: (0..key.len()).map(|j| format!("_lv_a{ai}_pk{j}")).collect(),
                exprs: key
                    .iter()
                    .map(|&c| {
                        Expr::CompoundIdentifier(vec![
                            qident(&a.name),
                            qident(&a.table.columns[c].name),
                        ])
                        .to_string()
                    })
                    .collect(),
            });
            a.image_cols = key;
        }
    }
    let alias_filters = const_filters(select, &joins, &scope, &base_edges, &own_edges);
    let partial = partial_units(
        &alias_plans,
        &lineage,
        &unit_cols,
        &scope,
        &base_edges,
        &own_edges,
    );
    let unit_unique = match mode {
        Mode::Spj => n_alias == 1 && aliases[0].1.row_key().is_some(),
        Mode::Grouped => !windowed,
        _ => false,
    };
    // Tables read by subqueries in the ORDER BY of a top-N view: a change of a column they name
    // recomputes it, like a change of its own tables (cascades into them too).
    let order_tables = match (&order_usage, &top_n) {
        (Some(o), Some(_)) => order_subquery_tables(o, default_schema, tables),
        _ => Vec::new(),
    };
    let mut presence: Vec<Presence> = Vec::new();
    let mut cascades = cascade_routes(
        &alias_plans
            .iter()
            .map(|a| (a.table.clone(), a.direct_unit.clone()))
            .chain(order_tables.iter().map(|(t, _)| (t.clone(), None)))
            .collect::<Vec<_>>(),
        tables,
    );
    // Cascades the parent's own rows trace: the view reads the parent through an alias whose
    // changed rows map to units directly or by lineage, joined on the whole FK (= the parent's
    // primary key) to every alias of the child so that a child row's presence implies that
    // alias carries its parent (`cascade_joined`). The child rows a cascade changes are exactly
    // those with the changed parent row, so every materialized row holding one is found through
    // it. A cascade that continues into the child's own children is found the same way only if
    // every further level is joined like that too.
    let table_ref = |t: &TableInfo| (t.schema.clone(), t.name.clone());
    let joined: Vec<Result<(), String>> = cascades
        .iter()
        .map(|c| {
            let mut keyed = c.parent_cols.clone();
            keyed.sort_unstable();
            let whole_key = c.parent.keys().any(|k| {
                let mut k = k.to_vec();
                k.sort_unstable();
                k == keyed
            });
            if !whole_key {
                return Err(format!(
                    "the foreign key of `{}` does not reference a whole key of `{}`",
                    c.child.1, c.parent.name
                ));
            }
            let parent = table_ref(&c.parent);
            let parents: Vec<usize> = (0..n_alias)
                .filter(|&x| {
                    table_ref(&alias_plans[x].table) == parent
                        && (alias_plans[x].direct_unit.is_some()
                            || lineage.iter().any(|k| k.alias == x)
                            || partial.aliases.get(x).is_some_and(Option::is_some))
                })
                .collect();
            let children: Vec<usize> = (0..n_alias)
                .filter(|&y| table_ref(&alias_plans[y].table) == c.child)
                .collect();
            if children.is_empty() {
                return Err(format!("the view does not read `{}`", c.child.1));
            }
            if parents.is_empty() {
                return Err(format!(
                    "the view does not read `{}` through a unit, partial or lineage alias",
                    c.parent.name
                ));
            }
            let mut why = String::new();
            for &y in &children {
                let mut ok = false;
                for &x in &parents {
                    match cascade_joined(
                        x,
                        y,
                        &c.parent_cols,
                        &c.child_cols,
                        &scope,
                        &base_edges,
                        &own_edges,
                        &null_side,
                        &joins,
                        &using_pairs,
                    ) {
                        Ok(()) => {
                            ok = true;
                            break;
                        }
                        Err(r) => why = r,
                    }
                }
                if !ok {
                    return Err(why);
                }
            }
            Ok(())
        })
        .collect();
    // Where the cascade continues (the child's own cascading children, recursively), the rows
    // it changes there are found through this route only if every further level is joined like
    // that: `traced` needs the structural condition here and down the chain, and a unit route
    // (units from the parent's image) needs it down the chain as well. A cycle (a
    // self-referential key, mutual cascades) reaches rows no image gives: never traced.
    fn continues_ok(
        i: usize,
        cascades: &[CascadeRoute],
        joined: &[Result<(), String>],
        path: &mut Vec<usize>,
    ) -> Result<(), (usize, String)> {
        path.push(i);
        let child = &cascades[i].child;
        let r = (0..cascades.len())
            .filter(|&j| cascades[j].parent.schema == child.0 && cascades[j].parent.name == child.1)
            .try_for_each(|j| {
                if path.contains(&j) {
                    return Err((
                        j,
                        format!(
                            "the cascade from `{}` into `{}` is part of a cycle",
                            cascades[j].parent.name, cascades[j].child.1
                        ),
                    ));
                }
                joined[j].clone().map_err(|r| (j, r))?;
                continues_ok(j, cascades, joined, path)
            });
        path.pop();
        r
    }
    let mut continuing = Vec::with_capacity(cascades.len());
    for i in 0..cascades.len() {
        let continues = continues_ok(i, &cascades, &joined, &mut Vec::new()).map_err(|(j, r)| {
            format!(
                "the cascade continues from `{}` into `{}`, which is not traced: {r}",
                cascades[j].parent.name, cascades[j].child.1
            )
        });
        let c = &mut cascades[i];
        if c.unit_from_parent.is_some() {
            // The parent's image gives this level's units; the next level may lie elsewhere.
            if let Err(r) = &continues {
                c.unit_from_parent = None;
                c.untraced = Some(r.clone());
            }
        } else {
            match joined[i].clone().and(continues.clone()) {
                Ok(()) => c.traced = true,
                Err(r) => c.untraced = Some(r),
            }
        }
        if c.untraced.is_some() && continues.is_ok() && matches!(mode, Mode::Spj | Mode::Grouped) {
            cascade_derivations(
                c,
                &PinContext {
                    aliases: &alias_plans,
                    partial: &partial,
                    filters: &alias_filters,
                    grouped: mode == Mode::Grouped && !resolved_groups.is_empty() && !windowed,
                    having: select.having.is_some(),
                    null_side: &null_side,
                },
                &mut presence,
            );
        }
        continuing.push(continues.is_ok());
    }
    for (t, cols) in order_tables {
        alias_plans.push(AliasPlan {
            name: t.name.clone(),
            table: t,
            referenced: cols.clone(),
            membership: cols,
            direct_unit: None,
            image_cols: Vec::new(),
            // Not in FROM: never looked up (one unit, recomputed).
            subst_from: (String::new(), String::new()),
            pos: (usize::MAX, None),
            semi: None,
            lookup_where: false,
        });
    }
    // (After every alias is known: the ORDER BY tables above have no filters.)
    for (c, &continues) in cascades.iter_mut().zip(&continuing) {
        cascade_reach(c, &alias_plans, &alias_filters, continues);
    }
    let window_sql = if select.named_window.is_empty() {
        None
    } else {
        Some(format!(
            "WINDOW {}",
            select
                .named_window
                .iter()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    };
    Ok(ViewPlan {
        schema,
        name: def.name.clone(),
        hash: def.hash.clone(),
        mode,
        table_columns: vec![None; all_tables.len()],
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
        from_items: select.from.clone(),
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
        order_limit_sql: top_n,
        window_sql,
        lookup_where_sql: lookup_where
            .map(|w| w.join(" AND "))
            .filter(|w| !w.is_empty()),
        alias_filters,
        partial,
        presence,
        unit_unique,
        lineage,
        cascades,
        aliases: alias_plans,
        output_names,
    })
}

/// Expands an unqualified `*` over USING / NATURAL joins the way the servers do: the join's
/// shared columns once, first, then the remaining columns of the left side, then those of the
/// right table; a RIGHT JOIN is the mirrored LEFT JOIN (shared columns and the rest from the
/// right table first); applied join by join along the chain. Only for a FROM of one chain of base tables with
/// inner / LEFT / RIGHT joins; anything else is left to the caller's fallback. Whether `*` was
/// expanded.
pub(crate) fn expand_star_over_using(
    select: &mut Select,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<bool> {
    if !select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)))
        || select.from.len() != 1
        || !select.from[0].joins.iter().any(|j| {
            matches!(
                join_constraint(&j.join_operator),
                Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
            )
        })
    {
        return Ok(false);
    }
    let twj = &select.from[0];
    let factor = |f: &TableFactor| -> Option<(String, Arc<TableInfo>)> {
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
                Some((n, info))
            }
            _ => None,
        }
    };
    let Some((alias0, t0)) = factor(&twj.relation) else {
        return Ok(false);
    };
    // (alias, column name) in the server's `*` order so far.
    let mut cols: Vec<(String, String)> = t0
        .columns
        .iter()
        .filter(|c| !c.invisible)
        .map(|c| (alias0.clone(), c.name.clone()))
        .collect();
    for j in &twj.joins {
        let Some((alias, t)) = factor(&j.relation) else {
            return Ok(false);
        };
        let right_preserved = match &j.join_operator {
            JoinOperator::Join(_) | JoinOperator::Inner(_) | JoinOperator::CrossJoin(_) => false,
            JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => false,
            JoinOperator::Right(_) | JoinOperator::RightOuter(_) => true,
            _ => return Ok(false),
        };
        let visible: Vec<String> = t
            .columns
            .iter()
            .filter(|c| !c.invisible)
            .map(|c| c.name.clone())
            .collect();
        let keys: Vec<String> = match join_constraint(&j.join_operator) {
            Some(JoinConstraint::Using(names)) => names
                .iter()
                .map(|n| match n.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => Ok(ident_name(i)),
                    _ => bail!("bad USING column"),
                })
                .collect::<Result<_>>()?,
            Some(JoinConstraint::Natural) => cols
                .iter()
                .map(|(_, c)| c.clone())
                .filter(|c| visible.iter().any(|v| v.eq_ignore_ascii_case(c)))
                .collect(),
            _ => Vec::new(),
        };
        let mut next: Vec<(String, String)> = Vec::new();
        for k in &keys {
            let left = cols
                .iter()
                .find(|(_, c)| c.eq_ignore_ascii_case(k))
                .ok_or_else(|| anyhow!("USING column {k} not found"))?;
            let right = visible
                .iter()
                .find(|c| c.eq_ignore_ascii_case(k))
                .ok_or_else(|| anyhow!("USING column {k} not found"))?;
            next.push(if right_preserved {
                (alias.clone(), right.clone())
            } else {
                left.clone()
            });
        }
        let is_key = |c: &str| keys.iter().any(|k| k.eq_ignore_ascii_case(c));
        let left_rest = cols.iter().filter(|(_, c)| !is_key(c)).cloned();
        let right_rest = visible
            .iter()
            .filter(|c| !is_key(c))
            .map(|c| (alias.clone(), c.clone()));
        // A RIGHT JOIN expands as the mirrored LEFT JOIN: the right table's columns first.
        if right_preserved {
            next.extend(right_rest);
            next.extend(left_rest);
        } else {
            next.extend(left_rest);
            next.extend(right_rest);
        }
        cols = next;
    }
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard(_) => {
                for (a, c) in &cols {
                    projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                        qident(a),
                        qident(c),
                    ])));
                }
            }
            other => projection.push(other),
        }
    }
    select.projection = projection;
    Ok(true)
}

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
fn inline_virtual_columns(
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

/// Whether every materialized row in which alias `y` (the child of a cascading FK) holds a row
/// also holds, in alias `x` (the parent), that row's parent: then the parent's own row finds the
/// rows a cascade changes. `Err` names why not.
///
/// Equalities valid whenever `y`'s row is present are the WHERE / inner-join ones (true in every
/// row, both sides non-NULL) and those of `y`'s own LEFT JOIN condition; `x`'s own LEFT JOIN
/// condition is not among them (`kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1`
/// keeps a kid whose parent is inactive, NULL-extended). A NULL-side parent is found anyway
/// when its join condition is exactly the foreign-key equality: the constraint guarantees the
/// parent row exists, so the condition matches it whenever the child's key is non-NULL (a child
/// with a NULL key is not cascaded into).
#[allow(clippy::too_many_arguments)]
fn cascade_joined(
    x: usize,
    y: usize,
    parent_cols: &[usize],
    child_cols: &[usize],
    scope: &Scope,
    base_edges: &[(ColRef, ColRef)],
    own_edges: &[Vec<(ColRef, ColRef)>],
    null_side: &[usize],
    joins: &[(usize, bool, JoinConstraint)],
    using_pairs: &[(usize, ColRef, ColRef)],
) -> Result<(), String> {
    let (xn, yn) = (&scope.aliases[x].0, &scope.aliases[y].0);
    let mut cls = Classes::new();
    for (a, b) in base_edges.iter().chain(&own_edges[y]) {
        cls.union(*a, *b);
    }
    let pairs = || child_cols.iter().copied().zip(parent_cols.iter().copied());
    if pairs().all(|(cc, pc)| cls.find((y, cc)) == cls.find((x, pc))) {
        return Ok(());
    }
    if !null_side.contains(&x) {
        return Err(format!("`{xn}` is not joined to `{yn}` on the foreign key"));
    }
    // The parent on the NULL side: its own condition must be the key equality and nothing else.
    let mut eqs: Vec<(ColRef, ColRef)> = Vec::new();
    match joins.iter().find(|(a, _, _)| *a == x).map(|(_, _, c)| c) {
        Some(JoinConstraint::On(e)) => {
            let mut cs = Vec::new();
            conjuncts(e, &mut cs);
            for c in cs {
                let eq = match c {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        right,
                    } => scope
                        .resolve(left)
                        .zip(scope.resolve(right))
                        .filter(|(a, b)| scope.same_family(*a, *b)),
                    _ => None,
                };
                match eq {
                    Some(eq) => eqs.push(eq),
                    None => {
                        return Err(format!(
                            "`{xn}` is on the NULL side of a LEFT JOIN whose condition is not \
                             the foreign-key equality alone: `{c}` leaves a `{yn}` row whose \
                             parent fails it without its parent"
                        ))
                    }
                }
            }
        }
        Some(JoinConstraint::Using(_)) => {
            eqs.extend(
                using_pairs
                    .iter()
                    .filter(|(a, _, _)| *a == x)
                    .map(|(_, l, r)| (*l, *r)),
            );
        }
        _ => return Err(format!("`{xn}` is not joined to `{yn}` on the foreign key")),
    }
    let mut covered = vec![false; child_cols.len()];
    for (a, b) in &eqs {
        let other = if a.0 == x { b } else { a };
        let j = pairs().position(|(cc, pc)| {
            (*a == (x, pc) || *b == (x, pc)) && cls.find((y, cc)) == cls.find(*other)
        });
        match j {
            Some(j) => covered[j] = true,
            None => {
                return Err(format!(
                    "`{xn}` is on the NULL side of a LEFT JOIN whose condition is not the \
                     foreign-key equality alone: `{}` = `{}` restricts `{xn}` further",
                    scope.name(*a),
                    scope.name(*b)
                ))
            }
        }
    }
    if covered.iter().all(|&c| c) {
        Ok(())
    } else {
        Err(format!(
            "`{xn}` is not joined to `{yn}` on the whole foreign key"
        ))
    }
}

/// Cascading-FK parents (and, conservatively, their ancestors) of the given child tables.
pub(crate) fn cascade_routes(
    children: &[(Arc<TableInfo>, Option<Vec<usize>>)],
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<CascadeRoute> {
    let mut out = Vec::new();
    let mut stack: Vec<(Arc<TableInfo>, Option<Vec<usize>>, usize)> = children
        .iter()
        .map(|(t, d)| (t.clone(), d.clone(), 0))
        .collect();
    let mut seen: BTreeSet<(TableRef, TableRef)> = BTreeSet::new();
    // Tables the view reads get their own routes (checked down the chain); the walk continues
    // past them only through parents the view does not read.
    let read: BTreeSet<TableRef> = children
        .iter()
        .map(|(t, _)| (t.schema.clone(), t.name.clone()))
        .collect();
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
                child: (child.schema.clone(), child.name.clone()),
                child_cols: fk.child_cols.clone(),
                traced: false,
                untraced: None,
                derived: false,
                partial_from_parent: Vec::new(),
                pinned: Vec::new(),
                reach: None,
            });
            if depth < 8 && !read.contains(&fk.parent) {
                // A cascade into the parent cascades further into the child: unknown units.
                stack.push((parent.clone(), None, depth + 1));
            }
        }
    }
    out
}

/// Columns of each table in `watched` that `query` may read, matched by name: every identifier
/// anywhere in the statement (columns, aliases, `USING` lists, ...) that names a column of the
/// table counts, so this is a superset. `None` (any column) where the statement reads columns
/// implicitly: `*`, NATURAL joins, FROM items other than tables and subqueries.
fn read_columns(
    query: &Query,
    watched: &[TableRef],
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<Option<Vec<bool>>> {
    #[derive(Default)]
    struct Names {
        names: HashSet<String>,
        any: bool,
    }
    impl Names {
        fn joins(&mut self, joins: &[Join]) {
            for j in joins {
                if !matches!(
                    join_constraint(&j.join_operator),
                    Some(JoinConstraint::On(_) | JoinConstraint::Using(_) | JoinConstraint::None)
                ) {
                    self.any = true;
                }
            }
        }
    }
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_ident(&mut self, i: &Ident) -> ControlFlow<()> {
            self.names.insert(ident_name(i).to_lowercase());
            ControlFlow::Continue(())
        }
        fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
            if s.projection.iter().any(|p| {
                matches!(
                    p,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                )
            }) {
                self.any = true;
            }
            for t in &s.from {
                self.joins(&t.joins);
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, f: &TableFactor) -> ControlFlow<()> {
            match f {
                TableFactor::Table { args: None, .. } | TableFactor::Derived { .. } => {}
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => self.joins(&table_with_joins.joins),
                _ => self.any = true,
            }
            ControlFlow::Continue(())
        }
    }
    let mut n = Names::default();
    let _ = query.visit(&mut n);
    watched
        .iter()
        .map(|t| {
            let info = tables.get(t).filter(|_| !n.any)?;
            Some(
                info.columns
                    .iter()
                    .map(|c| n.names.contains(&c.name.to_lowercase()))
                    .collect(),
            )
        })
        .collect()
}

fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
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

fn render_substituted_from(
    from: &[TableWithJoins],
    pos: (usize, Option<usize>),
    alias: &str,
) -> Result<(String, String)> {
    let mut from = from.to_vec();
    substitute_factor(&mut from, pos, alias, SUBST_MARKER);
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

/// Replaces the table at `pos` by a placeholder table `marker` (keeping the alias). Only rows
/// that actually match the substituted rows are affected by them, so a LEFT JOIN of that table
/// becomes an inner join (the refresh of the units recomputes any NULL extension).
fn substitute_factor(
    from: &mut [TableWithJoins],
    pos: (usize, Option<usize>),
    alias: &str,
    marker: &str,
) {
    let marker = TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(marker))]),
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
            if let JoinOperator::Left(c) | JoinOperator::LeftOuter(c) = &join.join_operator {
                join.join_operator = JoinOperator::Join(c.clone());
            }
        }
    }
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
            unique_keys: vec![],
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
            unique_keys: vec![],
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
        plan_block(&d, "db", &catalog()).unwrap()
    }

    /// Constant filters per FROM alias: `col = lit`, `lit = col`, `col <=> lit` and IN lists of
    /// integer literals over integer columns, from the WHERE, an inner join's ON or the alias's
    /// own LEFT JOIN condition, carried through equalities; nothing that converts, negates,
    /// combines or reads other tables.
    #[test]
    fn const_filters_per_alias() {
        use std::collections::BTreeMap;

        let int = |n: &str, ct: &str| {
            Column::new(
                n,
                ct.split(' ').next().unwrap(),
                ct,
                None,
                None,
                None,
                false,
            )
        };
        let mut virt = int("g", "int");
        virt.generated = Some(crate::catalog::Generated {
            expr: "(`oid` + 1)".into(),
            stored: false,
            deps: Some(vec![1]),
        });
        let mut stored = int("gs", "int");
        stored.generated = Some(crate::catalog::Generated {
            expr: "(`oid` * 2)".into(),
            stored: true,
            deps: Some(vec![1]),
        });
        let cat: HashMap<_, _> = [
            typed("obj", vec![int("id", "int")], &[0]),
            typed(
                "obj_attr",
                vec![
                    int("id", "int"),
                    int("oid", "int unsigned"),
                    int("aid", "smallint unsigned"),
                    int("scope", "smallint"),
                    int("big", "bigint unsigned"),
                    Column::simple("code", "varchar"),
                    Column::new(
                        "bin",
                        "varbinary",
                        "varbinary(8)",
                        None,
                        None,
                        Some(8),
                        false,
                    ),
                    Column::simple("price", "decimal"),
                    Column::simple("ts", "datetime"),
                    Column::simple("yr", "year"),
                    Column::simple("flag", "bit"),
                    Column::new(
                        "kind",
                        "enum",
                        "enum('a','b')",
                        Some("utf8mb4"),
                        Some("utf8mb4_general_ci"),
                        None,
                        false,
                    ),
                    virt,
                    stored,
                ],
                &[0],
            ),
        ]
        .into_iter()
        .collect();
        // Per alias name: its filters as `column=values`, sorted.
        let filters = |sql: &str| -> BTreeMap<String, Vec<String>> {
            let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
            let v = crate::analyzer::plan(&d, "db", &cat).unwrap();
            assert!(
                v.is_incremental() || v.mode == Mode::Global,
                "{sql}: {:?}",
                v.mode
            );
            (0..v.aliases.len())
                .map(|a| {
                    let t = &v.aliases[a].table;
                    let mut f: Vec<String> = v
                        .filters_of(a)
                        .iter()
                        .map(|f| format!("{}={:?}", t.columns[f.col].name, f.values))
                        .collect();
                    f.sort();
                    (v.aliases[a].name.clone(), f)
                })
                .collect()
        };
        let none = |sql: &str| {
            let f = filters(sql);
            assert!(f.values().all(Vec::is_empty), "{sql}: {f:?}");
        };
        let map = |entries: &[(&str, &[&str])]| -> BTreeMap<String, Vec<String>> {
            entries
                .iter()
                .map(|(a, f)| (a.to_string(), f.iter().map(|s| s.to_string()).collect()))
                .collect()
        };
        let from = "FROM obj o JOIN obj_attr a ON a.oid = o.id";
        // Inner ON, LEFT JOIN ON, both operand orders, `<=>`, IN lists, the WHERE; a constant
        // reaches the columns equal to the filtered one (o.id -> a.oid, b.oid).
        assert_eq!(
            filters(&format!(
                "SELECT o.id, a.scope, b.scope AS s {from} AND a.aid = 7 AND 3 = a.scope \
                 LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid IN (7, 9) AND b.scope <=> 4 \
                 WHERE a.big = 18446744073709551615 AND o.id = -1 AND (b.scope = +4) AND a.gs = 4"
            )),
            map(&[
                ("o", &["id=[Int(-1)]"]),
                (
                    "a",
                    &[
                        "aid=[Int(7)]",
                        "big=[UInt(18446744073709551615)]",
                        "gs=[Int(4)]",
                        "oid=[Int(-1)]",
                        "scope=[Int(3)]",
                    ]
                ),
                (
                    "b",
                    &["aid=[Int(7), Int(9)]", "oid=[Int(-1)]", "scope=[Int(4)]"]
                ),
            ])
        );
        // Grouped and global views alike; parentheses, signs and booleans are literals.
        assert_eq!(
            filters(&format!(
                "SELECT o.id, COUNT(*) AS n {from} AND ((a.aid) = (7)) AND a.scope = -(3) \
                 WHERE a.big IN (TRUE, 2) GROUP BY o.id"
            ))["a"],
            ["aid=[Int(7)]", "big=[Int(1), Int(2)]", "scope=[Int(-3)]"]
        );
        assert_eq!(
            filters(&format!("SELECT COUNT(*) AS n {from} WHERE a.aid = 7"))["a"],
            ["aid=[Int(7)]"]
        );
        // Equivalent formulations give the same filters per alias: join order, which ON or the
        // WHERE holds a condition, comma joins, `=` vs a one-element IN vs swapped sides, the
        // view wrapped in a CTE or a derived table with other output names.
        let base = "SELECT o.id, a.big AS x, b.big AS y FROM obj o \
                    JOIN obj_attr a ON a.oid = o.id AND a.aid = 7 \
                    LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid IN (8) AND b.scope = 2 \
                    WHERE a.scope = 2";
        let want = filters(base);
        assert_eq!(
            want,
            map(&[
                ("o", &[]),
                ("a", &["aid=[Int(7)]", "scope=[Int(2)]"]),
                ("b", &["aid=[Int(8)]", "scope=[Int(2)]"]),
            ])
        );
        for sql in [
            "SELECT o.id, a.big AS x, b.big AS y FROM obj_attr a \
             JOIN obj o ON o.id = a.oid AND 7 = a.aid AND a.scope IN (2) \
             LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid = 8 AND 2 = b.scope",
            "SELECT o.id, a.big AS x, b.big AS y FROM obj o, obj_attr a \
             LEFT JOIN obj_attr b ON b.oid = a.oid AND b.scope = 2 AND b.aid = 8 \
             WHERE a.oid = o.id AND a.scope = 2 AND a.aid = 7",
            "SELECT o.id, a.big AS x, b.big AS y FROM obj o \
             JOIN obj_attr a ON a.oid = o.id \
             LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid IN (8) AND b.scope = 2 \
             WHERE a.scope = 2 AND a.aid IN (7)",
            &format!("WITH w AS ({base}) SELECT id AS ident, x AS xx, y FROM w"),
            &format!("SELECT d.id AS ident, d.x FROM ({base}) d"),
            "SELECT o.id, a.big AS x, b.big AS y FROM obj o \
             JOIN (SELECT * FROM obj_attr WHERE aid = 7 AND scope = 2) a ON a.oid = o.id \
             LEFT JOIN (SELECT * FROM obj_attr WHERE scope = 2 AND aid = 8) b ON b.oid = o.id",
        ] {
            // Per alias; only where the normalizer renamed a merged derived table's aliases,
            // the filter sets regardless of names.
            let got = filters(sql);
            if got.keys().eq(want.keys()) {
                assert_eq!(got, want, "{sql}");
            } else {
                let sets = |m: &BTreeMap<String, Vec<String>>| {
                    let mut v: Vec<Vec<String>> = m.values().cloned().collect();
                    v.sort();
                    v
                };
                assert_eq!(sets(&got), sets(&want), "{sql}");
            }
        }
        // Constants carried through equalities of the alias's rows (inner ON / WHERE, its own
        // LEFT JOIN condition), not through another LEFT JOIN's.
        assert_eq!(
            filters(
                "SELECT o.id FROM obj o JOIN obj_attr a ON a.oid = o.id \
                 JOIN obj_attr b ON b.oid = o.id AND b.aid = a.aid \
                 LEFT JOIN obj_attr c ON c.oid = o.id AND c.scope = a.scope WHERE a.aid = 7 AND a.scope = 2"
            ),
            map(&[
                ("o", &[]),
                ("a", &["aid=[Int(7)]", "scope=[Int(2)]"]),
                ("b", &["aid=[Int(7)]"]),
                ("c", &["scope=[Int(2)]"]),
            ])
        );
        assert_eq!(
            filters("SELECT o.id FROM obj o JOIN obj_attr a ON a.oid = o.id AND o.id = 5"),
            map(&[("o", &["id=[Int(5)]"]), ("a", &["oid=[Int(5)]"])])
        );
        // Not filters: OR, NOT, IS NULL, functions, expressions, other columns or tables,
        // NULLs, non-integer literals, converting kinds.
        for cond in [
            "(a.aid = 7 OR a.aid = 9)",
            "NOT a.aid = 7",
            "a.aid NOT IN (7)",
            "a.aid <> 7",
            "a.aid IS NULL",
            "a.aid IS NOT NULL",
            "ABS(a.scope) = 1",
            "a.aid + 0 = 7",
            "a.aid = a.scope",
            "a.aid = o.id",
            "a.aid IN (7, NULL)",
            "a.aid <=> NULL",
            "a.aid = NULL",
            "a.aid = '7'",
            "a.aid = 7.0",
            "a.aid = 1e2",
            "a.aid = 0x61",
            "a.aid BETWEEN 1 AND 2",
            "a.aid > 6",
            "a.big = 99999999999999999999999",
            "a.code = 'x'",
            "a.bin = 'x'",
            "a.price = 5",
            "a.ts = 20200101",
            "a.yr = 2020",
            "a.flag = 1",
            "a.kind = 1",
            "a.g = 2",
        ] {
            none(&format!("SELECT o.id, a.scope {from} WHERE {cond}"));
            none(&format!("SELECT o.id, a.scope {from} AND {cond}"));
        }
        // A LEFT JOIN keeps its preserved rows whatever its condition says about them.
        none("SELECT o.id, a.scope FROM obj o LEFT JOIN obj_attr a ON a.oid = o.id AND o.id = 5");
        // A semi-join over a unique key becomes a join: its table's own conditions are its filters.
        assert_eq!(
            filters(&format!(
                "SELECT o.id, a.scope {from} WHERE a.aid IN (SELECT x.id FROM obj x WHERE x.id = 3)"
            ))["x"],
            ["id=[Int(3)]"]
        );
    }

    /// Partial direct units: an alias that is neither a unit nor a lineage alias but whose row
    /// equals some unit expressions (through inner-join, WHERE or its own LEFT JOIN equalities,
    /// same type family, exact kinds, any integer width or sign) gives that part of the unit, for
    /// every formulation of the same query; select/join views keep lineage.
    #[test]
    fn partial_units_per_alias() {
        use std::collections::BTreeMap;

        let int = |n: &str, ct: &str| {
            Column::new(
                n,
                ct.split(' ').next().unwrap(),
                ct,
                None,
                None,
                None,
                false,
            )
        };
        let cat: HashMap<_, _> = [
            typed(
                "link",
                vec![
                    int("gid", "int"),
                    int("oid", "mediumint unsigned"),
                    int("pos", "int"),
                ],
                &[0, 1],
            ),
            typed(
                "obj",
                vec![int("oid", "bigint"), Column::simple("label", "varchar")],
                &[0],
            ),
            typed(
                "obj_attr",
                vec![
                    int("id", "int"),
                    int("oid", "int unsigned"),
                    int("aid", "smallint unsigned"),
                    int("val", "int"),
                ],
                &[0],
            ),
            typed(
                "grp_attr",
                vec![
                    int("id", "int"),
                    int("gid", "smallint unsigned"),
                    int("aid", "tinyint"),
                    int("val", "int"),
                ],
                &[0],
            ),
            typed(
                "gen_attr",
                vec![int("id", "int"), int("src", "int unsigned"), {
                    let mut g = int("oid", "int unsigned");
                    g.generated = Some(crate::catalog::Generated {
                        expr: "`src`".into(),
                        stored: true,
                        deps: Some(vec![1]),
                    });
                    g
                }],
                &[0],
            ),
        ]
        .into_iter()
        .collect();
        let plan = |sql: &str| {
            let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
            crate::analyzer::plan(&d, "db", &cat).unwrap()
        };
        // Per alias name: unit / lineage / lookup / partial with the unit positions it gives.
        let routes = |sql: &str| -> BTreeMap<String, String> {
            let v = plan(sql);
            assert_eq!(v.mode, Mode::Grouped, "{sql}");
            (0..v.aliases.len())
                .map(|a| {
                    let how = if v.aliases[a].direct_unit.is_some() {
                        "unit".to_string()
                    } else if v.lineage_of(a).is_some() {
                        "lineage".to_string()
                    } else if let Some((set, _)) = v.partial_of(a) {
                        format!("partial{:?}", v.partial.sets[set])
                    } else {
                        "lookup".to_string()
                    };
                    (v.aliases[a].name.clone(), how)
                })
                .collect()
        };
        let map = |entries: &[(&str, &str)]| -> BTreeMap<String, String> {
            entries
                .iter()
                .map(|(a, h)| (a.to_string(), h.to_string()))
                .collect()
        };
        // Each alias gives the part of (group, object) its row carries: the object's attribute
        // and the object itself the second, the group's attribute (own LEFT JOIN condition) the
        // first; a table joined on a non-key value does not.
        let v = plan(
            "SELECT c.gid, o.oid, MAX(a.val) AS s, MAX(ga.val) AS t, MAX(x.val) AS x FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
             LEFT JOIN grp_attr ga ON ga.gid = c.gid AND ga.aid = 4 \
             LEFT JOIN obj_attr x ON x.val = a.val GROUP BY c.gid, o.oid",
        );
        assert_eq!(
            v.tracing(),
            "c:unit,o:partial,a:partial,ga:partial,x:lookup"
        );
        assert_eq!(v.partial.sets, vec![vec![1], vec![0]]);
        assert_eq!(v.partial_of(1), Some((0, &[0usize][..])));
        assert_eq!(v.partial_of(2), Some((0, &[1usize][..])));
        assert_eq!(v.partial_of(3), Some((1, &[1usize][..])));
        assert_eq!(v.partial_of(4), None);
        // The same routes for every formulation: join order, the carrying equality in the
        // WHERE or the ON, comma joins, the joins in a CTE or a derived table with other
        // output names, the group column not selected (a hidden unit column).
        let want = map(&[("c", "unit"), ("o", "partial[1]"), ("a", "partial[1]")]);
        for sql in [
            "SELECT c.gid, o.oid, MAX(a.val) AS s FROM link c JOIN obj o ON o.oid = c.oid \
             JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY c.gid, o.oid",
            "SELECT c.gid, o.oid, MAX(a.val) AS s FROM obj_attr a JOIN obj o ON o.oid = a.oid \
             JOIN link c ON c.oid = o.oid WHERE a.aid = 7 GROUP BY c.gid, o.oid",
            "SELECT c.gid, o.oid, MAX(a.val) AS s FROM link c JOIN obj o JOIN obj_attr a \
             ON a.aid = 7 WHERE o.oid = c.oid AND a.oid = o.oid GROUP BY c.gid, o.oid",
            "SELECT c.gid, o.oid, MAX(a.val) AS s FROM link c, obj o, obj_attr a \
             WHERE o.oid = c.oid AND a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, o.oid",
            "WITH w AS (SELECT c.gid AS g, o.oid AS ob, a.val AS v FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7) \
             SELECT g AS grp, ob AS object, MAX(v) AS s FROM w GROUP BY g, ob",
            "SELECT d.g, d.ob, MAX(d.v) AS s FROM (SELECT c.gid AS g, o.oid AS ob, a.val AS v \
             FROM link c JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid \
             WHERE a.aid = 7) d GROUP BY d.g, d.ob",
            "SELECT o.oid AS object, MAX(a.val) AS s FROM link c JOIN obj o ON o.oid = c.oid \
             JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY c.gid, o.oid",
        ] {
            assert_eq!(routes(sql), want, "{sql}");
        }
        // A generated column (STORED too) is not read from images: no partial part from it.
        assert_eq!(
            routes(
                "SELECT c.gid, o.oid, COUNT(*) AS n FROM link c JOIN obj o ON o.oid = c.oid \
                 JOIN gen_attr g ON g.oid = o.oid GROUP BY c.gid, o.oid"
            ),
            map(&[("c", "unit"), ("o", "partial[1]"), ("g", "lookup")])
        );
        // An expression group key gives no partial position; the plain key next to it does.
        assert_eq!(
            routes(
                "SELECT c.gid + 0 AS g, o.oid, MAX(a.val) AS s, MAX(ga.val) AS t FROM link c \
                 JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
                 LEFT JOIN grp_attr ga ON ga.gid = c.gid GROUP BY c.gid + 0, o.oid"
            ),
            map(&[
                ("c", "partial[1]"),
                ("o", "partial[1]"),
                ("a", "partial[1]"),
                ("ga", "lookup"),
            ])
        );
        // Another type family does not carry the unit; a full match is a unit alias.
        assert_eq!(
            plan(
                "SELECT c.gid, o.oid, COUNT(*) AS n FROM link c JOIN obj o ON o.oid = c.oid \
                 JOIN obj_attr a ON a.oid = o.label GROUP BY c.gid, o.oid"
            )
            .tracing(),
            "c:unit,o:partial,a:lookup"
        );
        let v = plan(
            "SELECT a.oid, a.aid, COUNT(*) AS n FROM obj_attr a \
             JOIN obj_attr b ON b.oid = a.oid AND b.aid = a.aid GROUP BY a.oid, a.aid",
        );
        assert_eq!(v.tracing(), "a:unit,b:unit");
        assert!(v.partial.sets.is_empty() && v.partial.aliases.is_empty());
        // Select/join: lineage stays; single-column units have no partial part.
        assert_eq!(
            plan("SELECT c.gid, c.oid, a.val FROM link c JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7")
                .tracing(),
            "c:unit,a:lineage"
        );
        assert_eq!(
            plan(
                "SELECT o.oid, MAX(a.val) AS v FROM obj o JOIN obj_attr a ON a.val = o.oid + 1 \
                 GROUP BY o.oid"
            )
            .tracing(),
            "o:unit,a:lookup"
        );
    }

    /// Cascades the parent's row still bounds: a parent read through a partial alias traces
    /// them; a child alias whose partial unit columns are FK columns gets partial keys from the
    /// parent's key; a child alias pinned by constant filters on FK columns only needs the
    /// parent rows that pass them; anything else, and chains through tables the view does not
    /// read, still rebuild.
    #[test]
    fn cascades_derived_from_partial_ids_and_filters() {
        use crate::catalog::{FkAction, ForeignKey};
        let int = |n: &str, ct: &str| {
            Column::new(
                n,
                ct.split(' ').next().unwrap(),
                ct,
                None,
                None,
                None,
                false,
            )
        };
        let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols: vec![child_col],
            parent_cols: vec![parent_col.into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
            let mut i = (*t.1).clone();
            i.cascades = f;
            (t.0, Arc::new(i))
        };
        let cat: HashMap<_, _> = [
            typed("realm", vec![int("rid", "int unsigned")], &[0]),
            with_fk(
                typed(
                    "kind",
                    vec![int("kid", "smallint unsigned"), int("rid", "int unsigned")],
                    &[0],
                ),
                vec![fk("realm", 1, "rid")],
            ),
            typed(
                "obj",
                vec![int("oid", "int unsigned"), int("w", "int")],
                &[0],
            ),
            typed(
                "link",
                vec![
                    int("gid", "int"),
                    int("oid", "int unsigned"),
                    int("pos", "int"),
                ],
                &[0, 1],
            ),
            with_fk(
                typed(
                    "obj_attr",
                    vec![
                        int("id", "int"),
                        int("oid", "int unsigned"),
                        int("aid", "smallint unsigned"),
                        int("val", "int"),
                    ],
                    &[0],
                ),
                vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")],
            ),
        ]
        .into_iter()
        .collect();
        let plan = |sql: &str| {
            let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
            crate::analyzer::plan(&d, "db", &cat).unwrap()
        };
        let route = |v: &ViewPlan, parent: &str| -> CascadeRoute {
            v.cascades
                .iter()
                .find(|c| c.parent.name == parent)
                .unwrap_or_else(|| panic!("{parent}: {:?}", v.cascades))
                .clone()
        };
        // The parent read through a partial alias: traced. The value-key parent is bounded
        // by the aliases' filters; its own parent (a chain) still rebuilds.
        let v = plan(
            "SELECT c.gid, o.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
             LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid IN (8, 9) GROUP BY c.gid, o.oid",
        );
        assert!(route(&v, "obj").traced, "{:?}", v.cascades);
        let k = route(&v, "kind");
        assert!(k.derived && k.untraced.is_none(), "{k:?}");
        assert!(k.partial_from_parent.is_empty());
        let kid = k.parent.col_index("kid").unwrap();
        // One count per pinned value (grouped view), named by the plan.
        assert_eq!(
            k.pinned,
            vec![
                Pinned {
                    parent_col: kid,
                    values: vec![(Datum::Int(7), 0)]
                },
                Pinned {
                    parent_col: kid,
                    values: vec![(Datum::Int(8), 1), (Datum::Int(9), 2)]
                },
            ]
        );
        assert_eq!(
            v.presence
                .iter()
                .map(|p| p.expr.clone())
                .collect::<Vec<_>>(),
            [
                "COUNT(CASE WHEN `a`.`aid` = 7 THEN 1 END)",
                "COUNT(CASE WHEN `b`.`aid` = 8 THEN 1 END)",
                "COUNT(CASE WHEN `b`.`aid` = 9 THEN 1 END)"
            ]
        );
        assert_eq!(v.presence[1].marks(&Datum::Int(8)), "`_lv_c1` > 0");
        assert_eq!(v.presence[2].holds(&Datum::Int(9)), "`b`.`aid` = 9");
        assert!(v
            .select_sql(&[], None)
            .contains("COUNT(CASE WHEN `a`.`aid` = 7 THEN 1 END) AS `_lv_c0`"));
        assert!(!route(&v, "realm").derived && route(&v, "realm").untraced.is_some());
        assert!(v.tracing().contains("^obj:traced") && v.tracing().contains("^kind:derived"));
        // Every alias of the child pins the FK: the keys that can reach the view at all. The
        // chain through `kind` (a table the view does not read) has no bound.
        assert_eq!(
            k.reach,
            Some((kid, vec![Datum::Int(7), Datum::Int(8), Datum::Int(9)]))
        );
        assert_eq!(route(&v, "realm").reach, None);
        // The parent not read: the child alias's partial unit is its FK column, so the
        // parent's key gives partial keys.
        let v = plan(
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c JOIN obj_attr a ON a.oid = c.oid \
             AND a.aid = 7 GROUP BY c.gid, c.oid",
        );
        let o = route(&v, "obj");
        assert!(o.derived, "{o:?}");
        assert_eq!(
            o.partial_from_parent,
            vec![(0, vec![o.parent.col_index("oid").unwrap()])]
        );
        assert_eq!(v.partial.sets, vec![vec![1]]);
        // An unpinned child alias without a partial unit on the FK: still a rebuild route.
        let v = plan(
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c JOIN obj_attr a ON a.val = c.pos \
             GROUP BY c.gid, c.oid",
        );
        assert!(route(&v, "kind").untraced.is_some() && !route(&v, "kind").derived);
        assert!(route(&v, "obj").untraced.is_some() && !route(&v, "obj").derived);
        assert_eq!(route(&v, "kind").reach, None);
        // A HAVING over a NULL-side pinned alias: removing its rows could let a group the table
        // does not hold appear, so presence cannot find it: the route stays untraced, but only
        // the pinned key reaches the view, and the reason says so.
        let v = plan(
            "SELECT c.gid, o.oid, MAX(b.val) AS y FROM link c JOIN obj o ON o.oid = c.oid \
             LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid = 8 GROUP BY c.gid, o.oid \
             HAVING MAX(b.val) IS NULL",
        );
        let k = route(&v, "kind");
        assert!(k.untraced.is_some() && !k.derived);
        assert!(v.presence.is_empty());
        assert_eq!(k.reach, Some((kid, vec![Datum::Int(8)])));
        assert!(
            k.untraced
                .as_deref()
                .is_some_and(|r| r.ends_with(" (only `kind`.`kid` IN (8) reaches it)")),
            "{k:?}"
        );
        // One alias of the child pinned, another not: no bound.
        let v = plan(
            "SELECT c.gid, c.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
             JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 LEFT JOIN obj_attr b ON b.oid = c.oid \
             GROUP BY c.gid, c.oid",
        );
        assert_eq!(route(&v, "kind").reach, None);
        // A pinned child whose own cascade continues into an alias not joined to it on that
        // FK: the rows a key outside the pins removes there may be read, so no bound; joined
        // on the FK, the bound holds.
        let v = plan(
            "SELECT a.id, a.val, k.kid FROM obj_attr a JOIN kind k ON k.kid = a.val AND k.rid = 3",
        );
        assert_eq!(route(&v, "realm").reach, None);
        let v = plan(
            "SELECT a.id, a.val, k.kid FROM obj_attr a JOIN kind k ON k.kid = a.aid AND k.rid = 3",
        );
        let r = route(&v, "realm");
        assert_eq!(r.reach, Some((0, vec![Datum::Int(3)])), "{r:?}");
        // A unit alias whose unit is not the FK: covered only by its filters on the FK.
        let v = plan("SELECT a.id, a.val FROM obj_attr a WHERE a.aid = 7");
        assert!(v.aliases[0].direct_unit.is_some());
        let k = route(&v, "kind");
        assert!(k.derived && k.pinned.len() == 1, "{k:?}");
        // Select/join: the FK value itself, selected by value.
        assert_eq!(v.presence.len(), 1);
        assert_eq!(v.presence[0].expr, "`a`.`aid`");
        assert_eq!(v.presence[0].marks(&Datum::Int(7)), "`_lv_c0` = 7");
        assert!(route(&v, "obj").untraced.is_some() && !route(&v, "obj").derived);
    }

    /// Derived cascade routes do not depend on how the query is written: the same routes, pins
    /// and partial keys for permuted joins, the FK equality in the WHERE or the ON (comma joins
    /// too), the pin as `=`, a one-element IN or with swapped sides, the view wrapped in a CTE or
    /// a derived table with renamed outputs, and the target id not selected. A pin on a
    /// composite FK is not followed (presence needs one FK column): untraced. The keys that can
    /// reach the view (`reach`) are the same per formulation too, also on an untraced route.
    #[test]
    fn derived_cascades_per_formulation() {
        use crate::catalog::{FkAction, ForeignKey};
        let int = |n: &str, ct: &str| {
            Column::new(
                n,
                ct.split(' ').next().unwrap(),
                ct,
                None,
                None,
                None,
                false,
            )
        };
        let fk = |parent: &str, child_cols: Vec<usize>, parent_cols: &[&str]| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols,
            parent_cols: parent_cols.iter().map(|c| c.to_string()).collect(),
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
            let mut i = (*t.1).clone();
            i.cascades = f;
            (t.0, Arc::new(i))
        };
        let cat: HashMap<_, _> = [
            typed("kind", vec![int("kid", "smallint unsigned")], &[0]),
            typed("pair", vec![int("p1", "int"), int("p2", "int")], &[0, 1]),
            typed("obj", vec![int("oid", "int unsigned")], &[0]),
            typed(
                "link",
                vec![int("gid", "int"), int("oid", "int unsigned")],
                &[0, 1],
            ),
            with_fk(
                typed(
                    "obj_attr",
                    vec![
                        int("id", "int"),
                        int("oid", "int unsigned"),
                        int("aid", "smallint unsigned"),
                        int("p1", "int"),
                        int("p2", "int"),
                        int("val", "int"),
                    ],
                    &[0],
                ),
                vec![
                    fk("obj", vec![1], &["oid"]),
                    fk("kind", vec![2], &["kid"]),
                    fk("pair", vec![3, 4], &["p1", "p2"]),
                ],
            ),
        ]
        .into_iter()
        .collect();
        // Per parent: how its cascade is followed (pins as their values, presence order apart).
        let routes = |sql: &str| -> BTreeMap<String, String> {
            let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
            let v = crate::analyzer::plan(&d, "db", &cat).unwrap();
            assert_eq!(v.mode, Mode::Grouped, "{sql}");
            v.cascades
                .iter()
                .map(|c| {
                    let how = if c.unit_from_parent.is_some() {
                        "unit".to_string()
                    } else if c.traced {
                        "traced".to_string()
                    } else if c.derived {
                        let pins: Vec<Vec<Datum>> = c
                            .pinned
                            .iter()
                            .map(|p| p.values.iter().map(|(d, _)| d.clone()).collect())
                            .collect();
                        format!(
                            "derived partial={:?} pins={pins:?}",
                            c.partial_from_parent
                                .iter()
                                .map(|(s, _)| v.partial.sets[*s].clone())
                                .collect::<Vec<_>>()
                        )
                    } else {
                        "rebuild".to_string()
                    };
                    let reach = c.reach.as_ref().map(|(_, v)| v);
                    (c.parent.name.clone(), format!("{how} reach={reach:?}"))
                })
                .collect()
        };
        use std::collections::BTreeMap;
        let base = "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c \
                    JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, c.oid";
        let want = routes(base);
        assert_eq!(want["obj"], "derived partial=[[1]] pins=[] reach=None");
        assert_eq!(
            want["kind"],
            "derived partial=[] pins=[[Int(7)]] reach=Some([Int(7)])"
        );
        assert_eq!(want["pair"], "rebuild reach=None");
        // Untraced (a HAVING over the NULL-side pinned alias) but bounded by the pin.
        let bounded = "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
                       ON b.oid = c.oid AND b.aid = 8 GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL";
        let want_bounded = routes(bounded);
        assert_eq!(want_bounded["kind"], "rebuild reach=Some([Int(8)])");
        for sql in [
            "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
             ON 8 = b.aid AND c.oid = b.oid GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
            "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
             ON b.oid = c.oid AND b.aid IN (8) GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
            "SELECT MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b ON b.oid = c.oid \
             AND b.aid = 8 GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
            "SELECT d.g, d.ob, MAX(d.v) AS y FROM (SELECT c.gid AS g, c.oid AS ob, b.val AS v \
             FROM link c LEFT JOIN obj_attr b ON b.oid = c.oid AND b.aid = 8) d \
             GROUP BY d.g, d.ob HAVING MAX(d.v) IS NULL",
        ] {
            assert_eq!(routes(sql), want_bounded, "{sql}");
        }
        for sql in [
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM obj_attr a JOIN link c ON c.oid = a.oid \
             WHERE a.aid = 7 GROUP BY c.gid, c.oid",
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c, obj_attr a \
             WHERE a.oid = c.oid AND a.aid IN (7) GROUP BY c.gid, c.oid",
            "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c JOIN obj_attr a \
             ON 7 = a.aid AND c.oid = a.oid GROUP BY c.gid, c.oid",
            "WITH w AS (SELECT c.gid AS g, c.oid AS ob, a.val AS v FROM link c \
             JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7) \
             SELECT g AS grp, ob AS object, MAX(v) AS x FROM w GROUP BY g, ob",
            "SELECT d.g, d.ob, MAX(d.v) AS x FROM (SELECT c.gid AS g, c.oid AS ob, a.val AS v \
             FROM link c JOIN obj_attr a ON a.oid = c.oid WHERE a.aid = 7) d GROUP BY d.g, d.ob",
            "SELECT MAX(a.val) AS x FROM link c JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 \
             GROUP BY c.gid, c.oid",
        ] {
            assert_eq!(routes(sql), want, "{sql}");
        }
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
        // L-06 / L-07: what is wrong, in the user's terms (a plan error is final: no retries).
        let err = |sql: &str| format!("{:#}", parse_definition(sql).unwrap_err());
        let one =
            "the definition must be a single SELECT statement (or CREATE VIEW name AS SELECT …)";
        assert_eq!(err(""), format!("the definition is empty: {one}"));
        assert_eq!(
            err("  -- a comment\n/* only */ "),
            format!("the definition is empty: {one}")
        );
        assert_eq!(
            err("CREATE VIEW `v` AS "),
            format!("the definition is empty after AS: {one}")
        );
        assert_eq!(
            err("CREATE VIEW `lz_insert` AS INSERT INTO orders VALUES (9,'z',1)"),
            format!("{one}; found `INSERT` … after AS")
        );
        for (sql, word) in [
            ("CREATE VIEW `v` AS UPDATE orders SET a = 1", "UPDATE"),
            ("CREATE VIEW `v` AS DELETE FROM orders", "DELETE"),
            ("CREATE VIEW `v` AS CREATE TABLE t (a INT)", "CREATE"),
            ("CREATE VIEW `v` AS DROP TABLE orders", "DROP"),
        ] {
            assert_eq!(
                err(sql),
                format!("{one}; found `{word}` … after AS"),
                "{sql}"
            );
        }
        assert_eq!(
            err("INSERT INTO orders VALUES (1)"),
            format!("{one}; found `INSERT` …")
        );
        // G3: a query without CREATE VIEW comes from a row without a name.
        for sql in [
            "SELECT num, COUNT(*) AS n FROM c GROUP BY num",
            "WITH t AS (SELECT 1 AS a) SELECT a FROM t",
            "(SELECT 1)",
        ] {
            assert_eq!(
                err(sql),
                "the definition needs a name: set `_lazyview.name`, or write CREATE VIEW name AS SELECT …",
                "{sql}"
            );
        }
        assert_eq!(
            err("CREATE TABLE t (a int)"),
            format!("{one}; found CREATE without VIEW")
        );
        assert_eq!(
            err("CREATE VIEW `v` AS SELECT id FROM orders; SELECT id FROM orders"),
            format!("the definition contains 2 statements: {one}")
        );
        assert_eq!(
            err("CREATE VIEW `v` AS SELECT 1 FROM t; SELECT 2 FROM t;\n SELECT 3 FROM t;"),
            format!("the definition contains 3 statements: {one}")
        );
        assert_eq!(
            err("CREATE VIEW v AS SELECT id, @@session.sql_mode AS m FROM t"),
            "a view cannot read user or system variables (`@@session.sql_mode`): the servers refuse them in views"
        );
        assert!(err("CREATE VIEW v AS SELECT id, @x AS x FROM t").contains("`@x`"));
        assert_eq!(
            err("CREATE VIEW v AS SELECT id FROM t LIMIT 2 ROWS EXAMINED 100"),
            "unexpected `ROWS` after the end of the SELECT statement"
        );
        // Accepted: CRLF line ends, a trailing semicolon (or two), comments, a string with `;`.
        for sql in [
            "CREATE VIEW v AS\r\nSELECT id\r\nFROM t\r\nWHERE id > 1;\r\n",
            "CREATE VIEW v AS SELECT id FROM t;;",
            "CREATE VIEW v AS SELECT id, 'a;b' AS s FROM t -- trailing; comment",
            "CREATE VIEW v AS SELECT `@x` FROM t",
        ] {
            assert!(parse_definition(sql).is_ok(), "{sql}");
        }
        assert!(parse_definition("CREATE TABLE t (a int)").is_err());
        assert!(parse_definition("CREATE VIEW v AS SELECT 1 garbage garbage").is_err());
    }

    /// Full-refresh views watch only the columns their statement names (E1a: an update of
    /// `orders.note` must not rebuild a DISTINCT view over `orders.status`).
    #[test]
    fn full_views_read_columns() {
        let cols = |sql: &str| {
            let v = p(sql);
            assert!(matches!(v.mode, Mode::Full(_)), "{sql}: {:?}", v.mode);
            v.tables
                .iter()
                .zip(&v.table_columns)
                .map(|(t, c)| (t.1.clone(), c.clone()))
                .collect::<Vec<_>>()
        };
        let (t, f) = (true, false);
        assert_eq!(
            cols("CREATE VIEW v AS SELECT DISTINCT status FROM orders"),
            vec![("orders".into(), Some(vec![f, f, t, f, f]))]
        );
        // Subqueries, ORDER BY, USING and aliases all count; names match any watched table.
        assert_eq!(
            cols(
                "CREATE VIEW v AS SELECT c.name FROM customers c JOIN regions USING (id) \
                 WHERE c.id IN (SELECT customer_id FROM orders WHERE total > 5) ORDER BY region_id LIMIT 3"
            ),
            vec![
                ("customers".into(), Some(vec![t, t, t])),
                ("orders".into(), Some(vec![t, t, f, t, f])),
                ("regions".into(), Some(vec![t, t])),
            ]
        );
        // Implicit column reads: any column counts.
        for sql in [
            "CREATE VIEW v AS SELECT DISTINCT * FROM orders",
            "CREATE VIEW v AS SELECT DISTINCT o.* FROM orders o",
            "CREATE VIEW v AS SELECT DISTINCT id FROM orders NATURAL JOIN order_items",
            "CREATE VIEW v AS SELECT id FROM orders UNION SELECT * FROM (SELECT id FROM regions) x",
        ] {
            assert!(cols(sql).iter().all(|(_, c)| c.is_none()), "{sql}");
        }
    }

    /// E6: select/join views trace lookup tables through hidden lineage key columns (their
    /// primary key per materialized row); their images carry only that key.
    #[test]
    fn spj_lineage_keys() {
        let v = p(
            "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
             JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
        );
        assert_eq!(
            v.lineage,
            vec![
                LineageKey {
                    alias: 1,
                    cols: vec!["_lv_a1_pk0".into()],
                    exprs: vec!["`c`.`id`".into()],
                },
                LineageKey {
                    alias: 2,
                    cols: vec!["_lv_a2_pk0".into()],
                    exprs: vec!["`r`.`id`".into()],
                },
            ]
        );
        assert_eq!(
            (v.aliases[1].image_cols.clone(), v.lineage_width()),
            (vec![0], 2)
        );
        assert_eq!(v.tracing(), "o:unit,c:lineage,r:lineage");
        assert!(v.select_sql(&[], None).ends_with(
            "`c`.`id` AS `_lv_a1_pk0`, `r`.`id` AS `_lv_a2_pk0` FROM orders o \
             JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id"
        ));
        assert_eq!(
            v.key_lookup_sql("`c`.`id` IN (1, 2)"),
            "SELECT DISTINCT `o`.`id` FROM orders o JOIN customers c ON c.id = o.customer_id \
             LEFT JOIN regions r ON r.id = c.region_id WHERE `c`.`id` IN (1, 2)"
        );
        // Composite keys; a key that the query does not read still counts as read (a key-only
        // UPDATE changes the hidden columns).
        let v = p(
            "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.customer_id",
        );
        assert_eq!(v.lineage[0].cols, vec!["_lv_a1_pk0", "_lv_a1_pk1"]);
        assert_eq!(v.lineage[0].exprs, vec!["`i`.`order_id`", "`i`.`line`"]);
        assert_eq!(v.aliases[1].image_cols, vec![0, 1]);
        assert!(v.aliases[1].referenced[1], "order_items.line");
        // Without a primary key the whole row is the key.
        let v =
            p("CREATE VIEW v AS SELECT o.id, l.msg FROM orders o JOIN logs l ON l.msg = o.status");
        assert_eq!(v.lineage[0].exprs, vec!["`l`.`msg`"]);
        assert_eq!(v.aliases[1].image_cols, vec![0]);
        // Tables whose rows map to units directly, grouped views and single tables: none.
        for sql in [
            "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
            "CREATE VIEW v AS SELECT c.name, COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name",
            "CREATE VIEW v AS SELECT id, status FROM orders",
        ] {
            assert!(p(sql).lineage.is_empty(), "{sql}");
        }
    }

    /// Rows of a select/join view are identified by the unit plus the lineage keys only when
    /// every other table carries its primary key as lineage (upserts by that identity).
    #[test]
    fn spj_rows_identified_by_unit_and_lineage() {
        for sql in [
            "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
             JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
            "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.customer_id",
            "CREATE VIEW v AS SELECT id, status FROM orders",
        ] {
            assert!(p(sql).rows_identified(), "{sql}");
        }
        for sql in [
            // order_items maps to units directly: no lineage, several rows per unit.
            "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
            // logs has no primary key: whole-row lineage, equal rows are duplicates.
            "CREATE VIEW v AS SELECT o.id, l.msg FROM orders o JOIN logs l ON l.msg = o.status",
            "CREATE VIEW v AS SELECT c.name, COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name",
            "CREATE VIEW v AS SELECT COUNT(*) AS n FROM orders",
            // A first table without a row key: equal rows share their unit.
            "CREATE VIEW v AS SELECT msg FROM logs",
        ] {
            assert!(!p(sql).rows_identified(), "{sql}");
        }
    }

    /// E1c: a cascading FK whose parent the view joins on the FK (through a lineage or
    /// direct-unit alias) is traced by the parent's own rows; one whose parent the view does not
    /// read (or joins otherwise) still rebuilds.
    #[test]
    fn cascades_traced_by_the_parent_alias() {
        use crate::catalog::{FkAction, ForeignKey};
        let fk = |parent: &str, child_cols: Vec<usize>| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols,
            parent_cols: vec!["id".into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        let mut cat = catalog();
        let with_fk = |t: (TableRef, Arc<TableInfo>), f: ForeignKey| {
            let mut i = (*t.1).clone();
            i.cascades = vec![f];
            (t.0, Arc::new(i))
        };
        cat.extend([
            table("grps", &[("id", "int"), ("name", "varchar")], &[0]),
            with_fk(
                table(
                    "pars",
                    &[("id", "int"), ("grp_id", "int"), ("label", "varchar")],
                    &[0],
                ),
                fk("grps", vec![1]),
            ),
            with_fk(
                table(
                    "kids",
                    &[("par_id", "int"), ("seq", "int"), ("qty", "int")],
                    &[0, 1],
                ),
                fk("pars", vec![0]),
            ),
        ]);
        let routes = |sql: &str| {
            let d = parse_definition(sql).unwrap();
            let v = plan_block(&d, "db", &cat).unwrap();
            let mut r: Vec<(String, bool)> = v
                .cascades
                .iter()
                .map(|c| (c.parent.name.clone(), c.traced))
                .collect();
            r.sort();
            r
        };
        assert_eq!(
            routes(
                "CREATE VIEW v AS SELECT k.par_id, k.seq, p.label, g.name FROM kids k \
                 JOIN pars p ON p.id = k.par_id LEFT JOIN grps g ON g.id = p.grp_id"
            ),
            vec![("grps".into(), true), ("pars".into(), true)]
        );
        // The parent is not read, or not joined on the FK: rebuilt.
        assert_eq!(
            routes("CREATE VIEW v AS SELECT par_id, seq, qty FROM kids"),
            vec![("grps".into(), false), ("pars".into(), false)]
        );
        assert_eq!(
            routes(
                "CREATE VIEW v AS SELECT k.par_id, k.seq, p.label FROM kids k JOIN pars p ON p.grp_id = k.qty"
            ),
            vec![("grps".into(), false), ("pars".into(), false)]
        );
    }

    /// A1: a cascade is traced only where a child row's presence in a materialized row implies
    /// the parent alias carries its parent. A parent on the NULL side of a LEFT JOIN with a
    /// condition beyond the key equality (or joined through another alias) keeps child rows
    /// whose parent fails it, NULL-extended: their cascading deletes would go unseen. The
    /// condition must hold down every further cascade; cycles are never traced.
    #[test]
    fn cascades_traced_only_where_the_child_row_implies_its_parent() {
        use crate::catalog::{FkAction, ForeignKey};
        let fk = |parent: &str, child_cols: Vec<usize>| ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols,
            parent_cols: vec!["id".into()],
            on_delete: FkAction::Cascade,
            on_update: FkAction::Cascade,
        };
        let mut cat = catalog();
        let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
            let mut i = (*t.1).clone();
            i.cascades = f;
            (t.0, Arc::new(i))
        };
        cat.extend([
            table("grps", &[("id", "int"), ("name", "varchar")], &[0]),
            with_fk(
                table(
                    "pars",
                    &[
                        ("id", "int"),
                        ("grp_id", "int"),
                        ("active", "tinyint"),
                        ("name", "varchar"),
                    ],
                    &[0],
                ),
                vec![fk("grps", vec![1])],
            ),
            with_fk(
                table(
                    "kids",
                    &[("id", "int"), ("par_id", "int"), ("qty", "int")],
                    &[0],
                ),
                vec![fk("pars", vec![1])],
            ),
            table("ords", &[("id", "int"), ("kid_id", "int")], &[0]),
            with_fk(
                table(
                    "nodes",
                    &[("id", "int"), ("parent_id", "int"), ("name", "varchar")],
                    &[0],
                ),
                vec![fk("nodes", vec![1])],
            ),
            with_fk(
                table(
                    "gkids",
                    &[("id", "int"), ("kid_id", "int"), ("v", "int")],
                    &[0],
                ),
                vec![fk("kids", vec![1])],
            ),
            with_fk(
                table("ma", &[("id", "int"), ("b_id", "int")], &[0]),
                vec![fk("mb", vec![1])],
            ),
            with_fk(
                table("mb", &[("id", "int"), ("a_id", "int")], &[0]),
                vec![fk("ma", vec![1])],
            ),
        ]);
        let routes = |sql: &str| {
            let d = parse_definition(sql).unwrap();
            let v = plan_block(&d, "db", &cat).unwrap();
            assert!(v.is_incremental(), "{sql}: {}", v.mode);
            let mut r: Vec<(String, String, bool, String)> = v
                .cascades
                .iter()
                .map(|c| {
                    // Unit-derived routes need no tracing; the others are traced or say why not.
                    let unit = c.unit_from_parent.is_some();
                    assert_eq!(c.traced, !unit && c.untraced.is_none(), "{sql}: {c:?}");
                    (
                        c.parent.name.clone(),
                        c.child.1.clone(),
                        c.traced || unit,
                        c.untraced.clone().unwrap_or_default(),
                    )
                })
                .collect();
            r.sort();
            r
        };
        // Every route of a parent the view reads is traced (one it does not read rebuilds).
        let traced = |sql: &str| {
            let rs = routes(sql);
            let read: Vec<&str> = ["grps", "pars", "nodes"]
                .into_iter()
                .filter(|t| sql.contains(&format!(" {t} ")))
                .collect();
            assert!(!read.is_empty());
            for r in rs.iter().filter(|r| read.contains(&r.0.as_str())) {
                assert!(r.2, "{sql}: {r:?}");
            }
        };
        let untraced = |sql: &str, parent: &str, why: &str| {
            let rs = routes(sql);
            let r = rs.iter().find(|r| r.0 == parent).unwrap();
            assert!(!r.2 && r.3.contains(why), "{sql}: {rs:?}");
        };
        // The parent on the NULL side, joined on exactly the key: the constraint makes the
        // join match whenever the key is non-NULL (an inner-join condition elsewhere is fine).
        traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id");
        traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON k.par_id = p.id WHERE k.qty > 1");
        traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON (p.id = k.par_id)");
        // The parent on the NULL side with a condition beyond the key (A1): untraced.
        let a1 = "whose condition is not the foreign-key equality alone";
        untraced(
            "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1",
            "pars",
            a1,
        );
        untraced(
            "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND k.qty > 1",
            "pars",
            a1,
        );
        untraced(
            "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.id > 1",
            "pars",
            a1,
        );
        // The same through a NULL-side child (A1, second shape).
        untraced(
            "CREATE VIEW v AS SELECT o.id, k.qty, p.name FROM ords o LEFT JOIN kids k ON k.id = o.kid_id \
             LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1",
            "pars",
            a1,
        );
        traced(
            "CREATE VIEW v AS SELECT o.id, k.qty, p.name FROM ords o LEFT JOIN kids k ON k.id = o.kid_id \
             LEFT JOIN pars p ON p.id = k.par_id",
        );
        // Inner joins: a child row is materialized only with its parent, whatever else the
        // condition asks; so is a child on the NULL side of its parent.
        traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k JOIN pars p ON p.id = k.par_id AND p.active = 1");
        traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k, pars p WHERE p.id = k.par_id AND p.active = 1");
        traced("CREATE VIEW v AS SELECT p.id, p.name, k.qty FROM pars p LEFT JOIN kids k ON k.par_id = p.id AND k.qty > 2");
        // A NULL-side parent whose condition goes through a third alias is not the key
        // equality (the constraint says nothing about `o.pid`).
        untraced(
            "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k JOIN ords o ON o.kid_id = k.id \
             LEFT JOIN pars p ON p.id = o.id",
            "pars",
            "`p.id` = `o.id` restricts `p` further",
        );
        // Two aliases of the parent: one joined on the key is enough.
        traced(
            "CREATE VIEW v AS SELECT k.id, p.name, q.name AS other FROM kids k LEFT JOIN pars p ON p.id = k.par_id \
             LEFT JOIN pars q ON q.id = k.qty",
        );
        // Chains: groups → parents → kids. Traced only when every level is.
        traced(
            "CREATE VIEW v AS SELECT k.id, p.name, g.name AS grp FROM kids k JOIN pars p ON p.id = k.par_id \
             LEFT JOIN grps g ON g.id = p.grp_id",
        );
        untraced(
            "CREATE VIEW v AS SELECT k.id, p.name, g.name AS grp FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1 \
             LEFT JOIN grps g ON g.id = p.grp_id",
            "grps",
            "continues from `pars` into `kids`, which is not traced",
        );
        untraced(
            "CREATE VIEW v AS SELECT p.id, p.name, g.name AS grp FROM pars p LEFT JOIN grps g ON g.id = p.grp_id AND g.name <> ''",
            "grps",
            a1,
        );
        // A self-referential cascade reaches every descendant: never traced (the alias read
        // as the parent is itself a child nobody is joined to as its parent).
        untraced(
            "CREATE VIEW v AS SELECT c.id, c.name, p.name AS parent FROM nodes c JOIN nodes p ON p.id = c.parent_id",
            "nodes",
            "`p` is not joined to `p` on the foreign key",
        );
        // A unit route (the parent's image gives the units) covers the next level only where
        // its rows are joined to the child's: here grandkids follow their kid, there they do
        // not (a deleted kid's grandkids sit in other units).
        {
            let d = parse_definition(
                "CREATE VIEW v AS SELECT k.par_id, COUNT(*) AS n FROM kids k \
                 JOIN gkids g ON g.kid_id = k.id AND g.v > 0 GROUP BY k.par_id",
            )
            .unwrap();
            let v = plan_block(&d, "db", &cat).unwrap();
            assert_eq!(
                v.tracing(),
                "k:unit,g:lookup,^kids:traced,^pars:unit,^grps:rebuild"
            );
            let d = parse_definition(
                "CREATE VIEW v AS SELECT k.par_id, COUNT(*) AS n FROM kids k \
                 JOIN gkids g ON g.v = k.qty GROUP BY k.par_id",
            )
            .unwrap();
            let v = plan_block(&d, "db", &cat).unwrap();
            assert_eq!(
                v.tracing(),
                "k:unit,g:lookup,^kids:rebuild,^pars:rebuild,^grps:rebuild"
            );
            let r = v.cascades.iter().find(|c| c.parent.name == "pars").unwrap();
            assert!(
                r.untraced.as_deref().is_some_and(|w| {
                    w.contains("continues from `kids` into `gkids`")
                        && w.contains("`k` is not joined to `g` on the foreign key")
                }),
                "{r:?}"
            );
        }
        // A3: a self-referential cascade reaches every descendant, also with the unit on the
        // key column (the parent's image gives one level): never unit-derived, never traced.
        {
            let d = parse_definition(
                "CREATE VIEW v AS SELECT n.parent_id, COUNT(*) AS n FROM nodes n GROUP BY n.parent_id",
            )
            .unwrap();
            let v = plan_block(&d, "db", &cat).unwrap();
            assert_eq!(v.tracing(), "n:unit,^nodes:rebuild");
            assert!(v.cascades[0]
                .untraced
                .as_deref()
                .is_some_and(|w| w.contains("part of a cycle")));
        }
        // Mutual cascades, each level joined on its key: a cycle, never traced.
        let rs = routes(
            "CREATE VIEW v AS SELECT a.id, b.id AS b FROM ma a JOIN mb b ON b.id = a.b_id AND a.id = b.a_id",
        );
        assert!(
            rs.iter().all(|r| !r.2 && r.3.contains("part of a cycle")),
            "{rs:?}"
        );
        // The parent is read but weakly (a WHERE conjunct keeps its NULL-extended rows): no
        // lineage, so nothing to trace through.
        untraced(
            "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id \
             WHERE p.id IS NULL OR p.active = 1",
            "pars",
            "does not read `pars` through a unit, partial or lineage alias",
        );
    }

    /// A2: only strict builtins and operators propagate NULL; everything else (CHAR, CHARSET,
    /// COERCIBILITY, NVL2, DECODE_ORACLE, aggregates, CASE, `<=>`, logical operators) may yield
    /// a value for NULL arguments, so NULL-side expressions over them are guarded and
    /// predicates over them keep NULL-extended rows.
    #[test]
    fn null_propagation_is_an_allow_list() {
        let e = |sql: &str| parse_sql(sql, |p| p.parse_expr()).unwrap();
        for sql in [
            "c.x",
            "c.x + 1",
            "-c.x",
            "c.x = 'a'",
            "UPPER(c.x)",
            "CONCAT(c.x, 'a')",
            "DATE_FORMAT(c.at, '%Y')",
            "CAST(c.x AS CHAR)",
            "ROUND(c.x / 3, 2)",
            "GREATEST(c.x, c.y)",
            "c.doc->'$.a'",
        ] {
            assert!(null_propagating(&e(sql)), "{sql}");
        }
        for sql in [
            "CHAR(c.x)",
            "CHARSET(c.x)",
            "COERCIBILITY(c.x)",
            "COLLATION(c.x)",
            "NVL2(c.x, 'a', 'b')",
            "DECODE_ORACLE(c.x, NULL, 'x', 'y')",
            "IFNULL(c.x, 0)",
            "COALESCE(c.x, 0)",
            "IF(c.x > 1, 1, 0)",
            "CASE WHEN c.x IS NULL THEN 1 END",
            "c.x <=> 1",
            "c.x IS NULL",
            "c.x > 1 AND c.y > 1",
            "c.x > 1 OR c.y > 1",
            "COUNT(c.x)",
            "IS_IPV4(c.x)",
            "INTERVAL_FN(c.x)",
            "QUOTE(c.x)",
            "JSON_ARRAY(c.x)",
            // A7: a NULL mode / locale / length argument selects a default.
            "WEEK(c.at, c.x)",
            "YEARWEEK(c.at, c.x)",
            "FORMAT(c.x, 2, c.l)",
            "SHA2(c.x, c.l)",
        ] {
            assert!(!null_propagating(&e(sql)), "{sql}");
        }
        // `||` is OR to the server and concatenation to the parser: refused as written.
        let err = parse_definition("CREATE VIEW v AS SELECT (a > 1) || 1 AS v FROM t")
            .unwrap_err()
            .to_string();
        assert!(err.contains("`||` is OR on the server"), "{err}");
        assert!(parse_definition("CREATE VIEW v AS SELECT a OR 1 AS v FROM t").is_ok());
    }

    /// A4: `SELECT *` over USING / NATURAL joins is expanded as the server does it: the shared
    /// columns once, first, from the preserved side, then the remaining columns of the left
    /// side, then of the right table, join by join; shapes the expansion does not cover are
    /// Full with a reason naming the fix.
    #[test]
    fn star_over_using_joins_expands_like_the_server() {
        let cat: HashMap<TableRef, Arc<TableInfo>> = [
            table("t_a", &[("k", "int"), ("x", "int")], &[0]),
            table("t_b", &[("k", "int"), ("y", "int")], &[0]),
            table("t_c", &[("y", "int"), ("z", "int")], &[0]),
        ]
        .into();
        let plan = |sql: &str| {
            let d = parse_definition(sql).unwrap();
            crate::analyzer::plan(&d, "db", &cat).unwrap()
        };
        for (sql, cols) in [
            (
                "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k)",
                vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
            ),
            (
                "CREATE VIEW v AS SELECT * FROM t_a LEFT JOIN t_b USING (k)",
                vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
            ),
            (
                "CREATE VIEW v AS SELECT * FROM t_a NATURAL JOIN t_b",
                vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
            ),
            // RIGHT JOIN: the mirrored LEFT JOIN, the right table's columns first.
            (
                "CREATE VIEW v AS SELECT * FROM t_a RIGHT JOIN t_b USING (k)",
                vec!["`t_b`.`k`", "`t_b`.`y`", "`t_a`.`x`"],
            ),
            // A chain: the second join's shared columns move to the front.
            (
                "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k) JOIN t_c USING (y)",
                vec!["`t_b`.`y`", "`t_a`.`k`", "`t_a`.`x`", "`t_c`.`z`"],
            ),
            (
                "CREATE VIEW v AS SELECT t_a.x AS ax, * FROM t_a JOIN t_b USING (k)",
                vec!["t_a.x AS ax", "`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
            ),
        ] {
            let p = plan(sql);
            assert_eq!(p.mode, Mode::Spj, "{sql}: {}", p.mode);
            assert_eq!(p.projection, cols, "{sql}");
        }
        // The block planner alone (the path without rewrites) expands the same way.
        let d = parse_definition("CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k)").unwrap();
        let p = plan_block(&d, "db", &cat).unwrap();
        assert_eq!(p.mode, Mode::Spj);
        assert_eq!(p.projection, vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"]);
        // Not covered (a derived table in the chain): Full, naming the fix.
        let p = plan("CREATE VIEW v AS SELECT * FROM t_a JOIN (SELECT k, y FROM t_b) d USING (k)");
        assert_eq!(p.mode, Mode::Full(STAR_USING.into()));
        assert_eq!(p.tables.len(), 2);
        for sql in [
            "CREATE VIEW v AS SELECT t_a.*, y FROM t_a JOIN t_b USING (k)",
            "CREATE VIEW v AS SELECT k, x, y FROM t_a LEFT JOIN t_b USING (k)",
        ] {
            let d = parse_definition(sql).unwrap();
            let p = crate::analyzer::plan(&d, "db", &cat).unwrap();
            assert_eq!(p.mode, Mode::Spj, "{sql}: {}", p.mode);
        }
    }

    /// L1: a top-N view's rows are chosen by its ORDER BY, so columns that appear only there
    /// (plain, in expressions, as aggregate inputs, of joined aliases) are read columns: an
    /// UPDATE of such a column must reach the recompute.
    #[test]
    fn order_by_columns_of_top_n_views_are_referenced() {
        let col = |v: &ViewPlan, alias: &str, c: &str| -> bool {
            let a = v.aliases.iter().find(|a| a.name == alias).unwrap();
            a.referenced[a.table.col_index(c).unwrap()]
        };
        for sql in [
            "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 2",
            "CREATE VIEW v AS SELECT id FROM orders ORDER BY total * 2 DESC LIMIT 2",
            "CREATE VIEW v AS SELECT id FROM orders ORDER BY -total LIMIT 2",
            "CREATE VIEW v AS SELECT id FROM orders ORDER BY ABS(total) DESC, id LIMIT 2",
            "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 1 OFFSET 1",
            "CREATE VIEW v AS SELECT customer_id FROM orders GROUP BY customer_id ORDER BY SUM(total) DESC LIMIT 2",
        ] {
            let v = p(sql);
            assert_eq!(v.mode, Mode::Global, "{sql}");
            assert!(col(&v, "orders", "total"), "{sql}");
            assert!(!col(&v, "orders", "note"), "{sql}");
        }
        let v = p(
            "CREATE VIEW v AS SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id \
             ORDER BY c.name DESC, o.id LIMIT 2",
        );
        assert!(col(&v, "c", "name") && !col(&v, "o", "total"));
        // An alias or ordinal of a projected expression adds nothing new.
        let v = p("CREATE VIEW v AS SELECT id, total * 2 AS t FROM orders ORDER BY t DESC LIMIT 2");
        assert!(col(&v, "orders", "total"));
        let v = p("CREATE VIEW v AS SELECT id, status FROM orders ORDER BY 2 DESC LIMIT 2");
        assert!(col(&v, "orders", "status") && !col(&v, "orders", "total"));
        // Windows: PARTITION BY / ORDER BY columns inside OVER are read columns too.
        let v = p(
            "CREATE VIEW v AS SELECT id, SUM(total) OVER (PARTITION BY customer_id ORDER BY status) AS s FROM orders",
        );
        assert!(col(&v, "orders", "customer_id") && col(&v, "orders", "status"));
    }

    /// L2: a named WINDOW clause stays part of the refresh SELECT (the projection's `OVER w`
    /// needs it), for windows used by several functions and windows refining another one.
    #[test]
    fn named_windows_are_kept_in_the_select() {
        let v = p(
            "CREATE VIEW v AS SELECT id, customer_id, ROW_NUMBER() OVER w AS rn, SUM(total) OVER w AS run \
             FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)",
        );
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(
            v.window_sql.as_deref(),
            Some("WINDOW w AS (PARTITION BY customer_id ORDER BY id)")
        );
        let sql = v.select_sql(&[0], None);
        assert!(
            sql.ends_with(" FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)"),
            "{sql}"
        );
        let v = p(
            "CREATE VIEW v AS SELECT id, SUM(total) OVER (w ORDER BY id) AS run FROM orders \
             WINDOW w AS (PARTITION BY customer_id)",
        );
        assert_eq!(v.mode, Mode::Grouped);
        assert!(v
            .select_sql(&[0], None)
            .ends_with(" WINDOW w AS (PARTITION BY customer_id)"));
        let v = p("CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER w AS rn FROM orders WINDOW w AS (ORDER BY id)");
        assert_eq!(v.mode, Mode::Global);
        assert!(v
            .select_sql(&[], None)
            .ends_with(" WINDOW w AS (ORDER BY id)"));
        assert!(p("CREATE VIEW v AS SELECT id, status FROM orders")
            .window_sql
            .is_none());
    }

    /// L3: duplicate output names (two plain columns or aliases with the same name, `*` over
    /// tables sharing names) and an empty projection are refused when planning, by name,
    /// instead of failing at CREATE TABLE and retrying forever; a column list renames.
    #[test]
    fn duplicate_output_names_are_refused() {
        let cat: HashMap<TableRef, Arc<TableInfo>> = [
            table("t_a", &[("k", "int"), ("x", "int")], &[0]),
            table("t_c", &[("k", "int"), ("z", "int"), ("x", "int")], &[0]),
        ]
        .into();
        let err = |sql: &str| {
            let d = parse_definition(sql).unwrap();
            let e = crate::analyzer::plan(&d, "db", &cat)
                .unwrap_err()
                .to_string();
            assert_eq!(
                plan_block(&d, "db", &cat).unwrap_err().to_string(),
                e,
                "{sql}"
            );
            e
        };
        assert!(
            err("CREATE VIEW v AS SELECT a.k, c.k FROM t_a a JOIN t_c c ON c.k = a.k")
                .contains("duplicate column name `k`")
        );
        assert!(
            err("CREATE VIEW v AS SELECT a.k, c.z AS K FROM t_a a JOIN t_c c ON c.k = a.k")
                .contains("duplicate column name `K`")
        );
        assert!(
            err("CREATE VIEW v AS SELECT * FROM t_a a JOIN t_c c ON c.k = a.k")
                .contains("duplicate column name `k`")
        );
        for sql in [
            "CREATE VIEW v AS SELECT a.k, c.k AS ck FROM t_a a JOIN t_c c ON c.k = a.k",
            "CREATE VIEW v (k1, k2) AS SELECT a.k, c.k FROM t_a a JOIN t_c c ON c.k = a.k",
            "CREATE VIEW v AS SELECT * FROM t_a JOIN t_c USING (k, x)",
        ] {
            let d = parse_definition(sql).unwrap();
            assert!(crate::analyzer::plan(&d, "db", &cat).is_ok(), "{sql}");
        }
    }

    /// Server schema: UNIQUE NOT NULL keys identify rows like the primary key (unit of a
    /// PK-less anchor, lineage, FD reduction of a DISTINCT, the parent key of a cascade), and
    /// a generated column is read through its base columns (a VIRTUAL one never a key).
    #[test]
    fn unique_keys_and_generated_columns() {
        use crate::catalog::Generated;
        let mut cat = catalog();
        let mut t = TableInfo {
            schema: "db".into(),
            name: "skus".into(),
            columns: vec![
                Column::simple("code", "varchar"),
                Column::simple("a", "int"),
                Column::simple("b", "int"),
                Column::simple("g", "int"),
                Column::simple("s", "int"),
                Column::simple("note", "text"),
            ],
            pk: vec![],
            unique_keys: vec![vec![0], vec![1, 2]],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        for c in &mut t.columns {
            c.nullable = c.name != "code" && c.name != "a" && c.name != "b";
        }
        t.columns[3].generated = Some(Generated {
            expr: "(`a` + `b`)".into(),
            stored: false,
            deps: Some(vec![1, 2]),
        });
        t.columns[4].generated = Some(Generated {
            expr: "(`a` * 2)".into(),
            stored: true,
            deps: Some(vec![1]),
        });
        cat.insert(("db".into(), "skus".into()), Arc::new(t));
        let plan = |sql: &str, cat: &HashMap<TableRef, Arc<TableInfo>>| {
            let d = parse_definition(sql).unwrap();
            crate::analyzer::plan(&d, "db", cat).unwrap()
        };
        // PK-less anchor with a unique key: the key is the unit, not the referenced columns.
        let v = plan("CREATE VIEW v AS SELECT note, a FROM skus", &cat);
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.unit_exprs, vec!["`skus`.`code`"]);
        assert!(v.unit_unique);
        // Lineage of a joined PK-less table through its unique key.
        let v = plan(
            "CREATE VIEW v AS SELECT o.id, s.note FROM orders o JOIN skus s ON s.code = o.status",
            &cat,
        );
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.lineage_of(1).unwrap().cols, vec!["_lv_a1_pk0"]);
        assert_eq!(v.lineage_of(1).unwrap().exprs, vec!["`s`.`code`"]);
        // DISTINCT over a unique key and other columns reduces to the key.
        let v = plan(
            "CREATE VIEW v AS SELECT DISTINCT code, note, s FROM skus",
            &cat,
        );
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(v.unit_exprs, vec!["code"]);
        let v = plan(
            "CREATE VIEW v AS SELECT DISTINCT a, b, note FROM skus",
            &cat,
        );
        assert_eq!(v.unit_exprs, vec!["a", "b"]);
        // A VIRTUAL generated column is inlined as its expression converted to its type (no
        // image value exists, B1); a STORED one is an ordinary column that reads its base
        // column too.
        let v = plan("CREATE VIEW v AS SELECT g, s FROM skus WHERE g > 1", &cat);
        assert_eq!(
            v.projection,
            vec![
                "(CAST(LEAST(GREATEST(CAST((`skus`.`a` + `skus`.`b`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED)) AS `g`",
                "s"
            ]
        );
        assert_eq!(
            v.where_sql.as_deref(),
            Some("(CAST(LEAST(GREATEST(CAST((`skus`.`a` + `skus`.`b`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED)) > 1")
        );
        let a = &v.aliases[0];
        assert!(a.referenced[1] && a.referenced[2] && a.referenced[4] && !a.referenced[3]);
        assert!(a.membership[1] && a.membership[2]);
        let v = plan("CREATE VIEW v AS SELECT DISTINCT g FROM skus", &cat);
        assert_eq!(v.unit_exprs, vec!["(CAST(LEAST(GREATEST(CAST((`skus`.`a` + `skus`.`b`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED))"]);
        let mut keyless = (*cat[&("db".to_string(), "skus".to_string())]).clone();
        keyless.unique_keys.clear();
        cat.insert(("db".into(), "skus".into()), Arc::new(keyless));
        let v = plan("CREATE VIEW v AS SELECT g, s FROM skus", &cat);
        assert_eq!(v.unit_exprs, vec!["`skus`.`a`", "`skus`.`b`", "`skus`.`s`"]);
        let mut unreadable = (*cat[&("db".to_string(), "skus".to_string())]).clone();
        unreadable.columns[3].generated.as_mut().unwrap().deps = None;
        cat.insert(("db".into(), "skus".into()), Arc::new(unreadable));
        let v = plan("CREATE VIEW v AS SELECT g FROM skus", &cat);
        assert!(
            matches!(&v.mode, Mode::Full(r) if r.starts_with("VIRTUAL generated column `skus`.`g`")),
            "{}",
            v.mode
        );
    }

    /// E6: lookups that replace several tables by their row images at once, with parameters in
    /// the order the derived tables appear.
    #[test]
    fn multi_alias_lookup_sql() {
        let v = p(
            "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
             JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
        );
        let (c, r) = (1, 2);
        assert!(v.aliases[c].direct_unit.is_none() && v.aliases[r].direct_unit.is_none());
        let (sql, order) = v
            .lookup_sql_multi(&[(r, "(R)".into()), (c, "(C)".into())])
            .unwrap();
        assert_eq!(
            sql,
            "SELECT DISTINCT `o`.`id` FROM orders o JOIN (C) AS `c` ON c.id = o.customer_id \
             JOIN (R) AS `r` ON r.id = c.region_id"
        );
        assert_eq!(order, vec![1, 0]);
        // One alias: the same statement as the single-table lookup.
        let (one, _) = v.lookup_sql_multi(&[(r, "(R)".into())]).unwrap();
        assert_eq!(one, v.lookup_sql(r, "(R)"));
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

        // The group key reduced to what determines it: customers' key, equal to orders' column.
        let v = p("CREATE VIEW v AS SELECT c.id, c.name, COUNT(*) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.id, c.name");
        assert_eq!(v.unit_exprs, vec!["c.id"]);
        assert_eq!(
            v.aliases[0].direct_unit,
            Some(vec![1]),
            "orders map to the unit directly"
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
                .and_then(|d| plan_block(&d, "db", &catalog()))
                .is_err(),
            "column list length must match"
        );
        assert!(parse_definition("CREATE VIEW v AS SELECT * FROM nope")
            .and_then(|d| plan_block(&d, "db", &catalog()))
            .is_err());
        assert!(
            parse_definition("CREATE VIEW v AS SELECT bogus FROM orders")
                .and_then(|d| plan_block(&d, "db", &catalog()))
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
        // Top-N: one unit, recomputed by the bounded query itself.
        let v = p("CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 3");
        assert_eq!(v.mode, Mode::Global);
        assert!(v.unit_exprs.is_empty() && v.lineage.is_empty());
        assert!(
            v.select_sql(&[], None)
                .ends_with("FROM orders ORDER BY total DESC LIMIT 3"),
            "{}",
            v.select_sql(&[], None)
        );
        assert_eq!(
            full_reason("CREATE VIEW v AS SELECT id FROM orders LIMIT 3"),
            "LIMIT without ORDER BY"
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
            plan_block(&d, "db", &catalog()).unwrap().mode,
            Mode::Full(_)
        ));
    }

    /// Window functions: the unit is the PARTITION BY all windows share (whole partitions are
    /// recomputed); without one, the view is one unit.
    #[test]
    fn window_partitions_are_units() {
        let v = p("CREATE VIEW v AS SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn, \
                   SUM(total) OVER w AS run FROM orders WINDOW w AS (PARTITION BY customer_id, status ORDER BY id)");
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(v.unit_exprs, vec!["customer_id"]);
        assert!(!v.unit_unique);
        assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
        assert!(
            v.aliases[0].referenced[2],
            "status, used by the named window"
        );
        assert!(v
            .select_sql(&[], Some("customer_id IN (1)"))
            .contains("WHERE customer_id IN (1)"));
        assert_eq!(
            p("CREATE VIEW v AS SELECT id, RANK() OVER (ORDER BY total DESC) AS r FROM orders")
                .mode,
            Mode::Global
        );
        assert_eq!(
            p("CREATE VIEW v AS SELECT customer_id, SUM(total), RANK() OVER (ORDER BY SUM(total)) FROM orders GROUP BY customer_id").mode,
            Mode::Global
        );
    }

    /// A first table without primary key: equal rows are one unit (its referenced columns).
    #[test]
    fn keyless_anchor_units() {
        let v = p("CREATE VIEW v AS SELECT msg, LENGTH(msg) AS n FROM logs WHERE msg <> ''");
        assert_eq!(v.mode, Mode::Spj);
        assert_eq!(v.unit_exprs, vec!["`logs`.`msg`"]);
        assert!(!v.unit_unique);
        assert_eq!(v.aliases[0].direct_unit, Some(vec![0]));
    }

    #[test]
    fn left_join_null_side() {
        // Group key from the NULL-supplying side: NULL-extended group is invisible to lookups.
        let r = full_reason("CREATE VIEW v AS SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id");
        assert!(r.contains("NULL side"), "{r}");
        let r = full_reason("CREATE VIEW v AS SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY n");
        assert!(r.contains("NULL side"), "{r}");
        // Anti-join (WHERE keeps NULL-extended rows): the NULL-side table's lookups leave those
        // conjuncts out and it is traced by lookups only (no lineage).
        let v = p("CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.id IS NULL AND o.total > 0");
        assert_eq!(v.mode, Mode::Spj);
        assert!(v.aliases[1].lookup_where && v.lineage.is_empty());
        assert_eq!(
            v.lookup_sql(1, "(R)"),
            "SELECT DISTINCT `o`.`id` FROM orders o JOIN (R) AS `c` ON c.id = o.customer_id WHERE (o.total > 0)"
        );
        assert!(v
            .select_sql(&[], None)
            .ends_with("WHERE c.id IS NULL AND o.total > 0"));
        for sql in [
            "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE IFNULL(c.name, 'x') = 'x'",
            "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name = 'a' OR o.total > 1",
        ] {
            let v = p(sql);
            assert_eq!(v.mode, Mode::Spj, "{sql}");
            assert_eq!(v.lookup_sql(1, "(R)"), "SELECT DISTINCT `o`.`id` FROM orders o JOIN (R) AS `c` ON c.id = o.customer_id");
        }
        // A NULL-side group key that the rest of the key determines is not part of the unit.
        let v = p("CREATE VIEW v AS SELECT o.id, c.name, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY o.id, c.name");
        assert_eq!(v.mode, Mode::Grouped);
        assert_eq!(v.unit_exprs, vec!["o.id"]);
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
        let pl = |sql: &str| plan_block(&parse_definition(sql).unwrap(), "db", &cat).unwrap();
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
            plan_block(&parse_definition(sql).unwrap(), "db", &cat)
                .unwrap()
                .mode
        };
        // A JSON column is a plain unit (the engine binds it typed, `CAST(? AS JSON)`).
        assert_eq!(
            pl("CREATE VIEW v AS SELECT doc, COUNT(*) FROM docs GROUP BY doc"),
            Mode::Grouped
        );
        // A JSON value computed by a function: MariaDB compares it as JSON, the filter as text.
        for (sql, key) in [
            (
                "CREATE VIEW v AS SELECT JSON_EXTRACT(doc, '$.a') k, COUNT(*) FROM docs GROUP BY k",
                "JSON_EXTRACT(doc, '$.a')",
            ),
            (
                "CREATE VIEW v AS SELECT doc->'$.a' k, COUNT(*) FROM docs GROUP BY k",
                "doc -> '$.a'",
            ),
        ] {
            assert_eq!(
                pl(sql),
                Mode::Full(format!("group key `{key}` is a JSON value computed by a function (MariaDB compares it as JSON, its unit filter as text): group by the column, or by its text with ->> / JSON_UNQUOTE")),
                "{sql}"
            );
        }
        // ->> yields text: fine.
        assert_eq!(
            pl("CREATE VIEW v AS SELECT doc->>'$.a' k, COUNT(*) FROM docs GROUP BY k"),
            Mode::Grouped
        );
        assert_eq!(
            pl("CREATE VIEW v AS SELECT flags, COUNT(*) FROM docs GROUP BY flags"),
            Mode::Grouped
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
        let v = plan_block(&parse_definition("CREATE VIEW v AS SELECT p.id, c.name FROM paid_orders p JOIN customers c ON c.id = p.customer_id").unwrap(), "db", &cat).unwrap();
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
                plan_block(&parse_definition(bad).unwrap(), "db", &cat).is_err(),
                "{bad}"
            );
        }
        let mut cat2 = catalog();
        let (k, t) = table("_lazyview_state", &[("name", "varchar")], &[0]);
        cat2.insert(k, t);
        let e = plan_block(
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
        let v = plan_block(&d, "db", &cat).unwrap();
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
            plan_block(&parse_definition(sql).unwrap(), "db", &cat)
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
        let pl = |sql: &str| parse_definition(sql).and_then(|d| plan_block(&d, "db", &catalog()));
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
