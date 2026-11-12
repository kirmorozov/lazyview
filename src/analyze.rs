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

use crate::analyzer::expr_key;
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

/// Tables an embedding program owns (see [`set_reserved`]): exact names and name prefixes, lowercase.
static RESERVED: std::sync::OnceLock<(Vec<String>, Vec<String>)> = std::sync::OnceLock::new();

/// Sets the tables an embedding program owns: exact `names` and name `prefixes`, compared
/// case-insensitively, refused as view sources like lazyview's metadata tables. Only the first call
/// takes effect; it returns whether this one did. lazyview itself never calls it.
pub fn set_reserved(names: &[&str], prefixes: &[&str]) -> bool {
    let low = |v: &[&str]| v.iter().map(|s| s.to_ascii_lowercase()).collect::<Vec<_>>();
    RESERVED.set((low(names), low(prefixes))).is_ok()
}

/// Whether `table` is an embedding program's table (see [`set_reserved`]).
fn reserved(table: &str) -> bool {
    let t = table.to_ascii_lowercase();
    RESERVED.get().is_some_and(|(names, prefixes)| {
        names.contains(&t) || prefixes.iter().any(|p| t.starts_with(p.as_str()))
    })
}

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
    /// The output columns the caller names as the target id (`IdSpec::Outputs`), in order; empty: the
    /// view's own unit. lazyview never sets it.
    pub id_outputs: Vec<String>,
}

pub fn definition_hash(def: &str) -> String {
    hex::encode(Sha256::digest(def.trim().as_bytes()))[..16].to_string()
}

/// Parses `CREATE [OR REPLACE] [ALGORITHM=..] [DEFINER=..] [SQL SECURITY ..] VIEW name [(cols)] AS
/// SELECT ... [WITH [CASCADED|LOCAL] CHECK OPTION]`.
pub fn parse_definition(def: &str) -> Result<ParsedDef> {
    // MySQL and MariaDB execute the content of `/*! ... */`, `/*!NNNNN ... */` and `/*M! ... */`: sqlparser expands
    // the first two unconditionally (whatever the version gate says) and ignores the third, so what lazyview plans
    // could differ from what the server runs. Refuse them anywhere in the text (also inside a string literal: no
    // quote-aware scan that could disagree with the server's escape rules).
    if def.contains("/*!") || def.to_ascii_uppercase().contains("/*M!") {
        bail!("the definition contains an executable comment (`/*!` or `/*M!`): the server would run its content; write the SQL without it");
    }

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
        id_outputs: Vec::new(),
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

/// A bare SELECT (an embedding program's query) parsed as the definition of view `name`: the same
/// checks and the same plan as `CREATE VIEW name AS <sql>`; `hash` is the definition hash of `sql`.
pub fn parse_select(sql: &str, name: &str) -> Result<ParsedDef> {
    if is_create_statement(sql) {
        bail!("expected a SELECT statement, found CREATE");
    }
    let mut def = parse_definition(&format!("CREATE VIEW {} AS {sql}", quote_ident(name)))?;
    def.hash = definition_hash(sql);
    Ok(def)
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
/// identifier is one character of the name. The owner of both readings of a name, shared with
/// `crate::analyzer` (see [`ident_name`]).
pub(crate) fn unquote(value: &str, quote: Option<char>) -> String {
    match quote {
        Some(q) => value.replace(&format!("{q}{q}"), &q.to_string()),
        None => value.to_string(),
    }
}

/// The name an identifier of a parsed AST stands for.
pub(crate) fn ident_name(i: &Ident) -> String {
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
pub(crate) fn int_sql(d: &Datum) -> String {
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
    /// The child table itself (its columns, keys and cascades).
    pub child_table: Arc<TableInfo>,
    /// An untraced route of a select/join or grouped view keeps the child's rows in the
    /// in-memory cascade cache (`crate::cascade`): a parent change replays them as the child row
    /// changes the storage engine made, through every route of the child table
    /// (`cascade_maps`). `None`: traced, unit, derived, another mode, or a child without a key
    /// (named in `untraced`).
    pub map: Option<MapPlan>,
}

/// What the cascade map keeps of a child's rows (see [`CascadeRoute::map`]).
#[derive(Debug, Clone, PartialEq)]
pub struct MapPlan {
    /// The child's key columns: its primary key, else its shortest NOT NULL unique key.
    pub key: Vec<usize>,
    /// The stored columns (ascending): every column a route of the child table takes values
    /// from (its aliases' images, units, partial units and filters; the parent columns of the
    /// cascades out of it; the FK columns of the cascades into it) and the key. A replayed
    /// update differs from its old row in FK columns only, so what else the aliases read
    /// (`AliasPlan::referenced`) compares equal on both sides without being stored.
    pub cols: Vec<usize>,
    /// Per alias of the child: its constant filters. A row is kept when it passes those of some
    /// alias (rows of no alias never reach the view). No alias (a level the view does not
    /// read): every row is kept.
    pub filters: Vec<Vec<ConstFilter>>,
}

impl MapPlan {
    /// Whether the map keeps `row` (a binlog image of the child).
    pub fn keeps(&self, row: &mysql_async::binlog::row::BinlogRow, table: &TableInfo) -> bool {
        self.filters.is_empty()
            || self
                .filters
                .iter()
                .any(|f| ConstFilter::passes(f, row, table))
    }
}

/// An equality between two alias columns, valid on every view row when `only_with` is `None`
/// (a WHERE conjunct or an inner-join ON) and, when `Some(j)`, only on the rows where the
/// alias `j` participates (its own LEFT-join ON). The hop paths of `crate::access` walk them.
#[derive(Debug, Clone)]
pub struct Edge {
    pub a: (usize, usize),
    pub b: (usize, usize),
    pub only_with: Option<usize>,
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
    /// The equality edges between alias columns ([`Edge`]).
    pub edges: Vec<Edge>,
    /// Per reduced unit expression: the alias columns it reads.
    pub unit_refs: Vec<Vec<(usize, usize)>>,
    /// Whether the definition spells a `COLLATE` of its own anywhere ([`has_collate`], the
    /// statement as written and as the lookups see it). The lookup of an incremental view then
    /// binds its string parameters without one: a `COLLATE` clause is EXPLICIT, and the view's own
    /// would meet the parameter's in an error ("Illegal mix of collations", 1267). See the lookup
    /// in `engine`.
    pub has_explicit_collate: bool,
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
            } else if c.map.is_some() {
                "map"
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

/// The column equalities of a conjunct. A one-element `x IN (y)` is `x = y`.
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

/// The cascade map of every route that stays untraced (see [`CascadeRoute::map`]): what each
/// child table's rows must keep so that replaying them through the child's routes reads only
/// stored columns. A child without a key cannot be kept: its route stays a rebuild route, and
/// its reason says so.
fn cascade_maps(
    cascades: &mut [CascadeRoute],
    aliases: &[AliasPlan],
    filters: &[Vec<ConstFilter>],
    partial: &PartialUnits,
) {
    let same = |t: &TableInfo, r: &TableRef| t.schema == r.0 && t.name == r.1;
    let untraced: Vec<usize> = (0..cascades.len())
        .filter(|&i| {
            let c = &cascades[i];
            c.untraced.is_some() && !c.traced && !c.derived && c.unit_from_parent.is_none()
        })
        .collect();
    for &i in &untraced {
        let child = cascades[i].child_table.clone();
        let child_ref = (child.schema.clone(), child.name.clone());
        let why = match child.row_key() {
            None => Some("has no primary or NOT NULL unique key".to_string()),
            Some(_) => {
                let fk = &cascades[i].child_cols;
                if fk.iter().any(|&k| !filter_or_key_column(&child.columns[k])) {
                    Some("has a foreign key compared by collation or type conversion".to_string())
                } else {
                    None
                }
            }
        };
        if let Some(why) = why {
            if let Some(r) = &mut cascades[i].untraced {
                r.push_str(&format!(
                    " (`{}` {why}: the cascade map cannot keep its rows)",
                    child.name
                ));
            }
            continue;
        }
        let key = child.row_key().expect("checked").to_vec();
        let mut cols: BTreeSet<usize> = key.iter().copied().collect();
        let mut alias_filters = Vec::new();
        for (ai, a) in aliases.iter().enumerate() {
            if !same(&a.table, &child_ref) {
                continue;
            }
            let mut read: Vec<usize> = a.image_cols.clone();
            read.extend(a.direct_unit.iter().flatten());
            if let Some(Some((_, pc))) = partial.aliases.get(ai) {
                read.extend(pc);
            }
            let f = filters.get(ai).cloned().unwrap_or_default();
            read.extend(f.iter().map(|f| f.col));
            cols.extend(read);
            alias_filters.push(f);
        }
        for c in cascades.iter() {
            if same(&c.parent, &child_ref) {
                cols.extend(&c.parent_cols);
            }
            if c.child == child_ref {
                cols.extend(&c.child_cols);
            }
        }
        cascades[i].map = Some(MapPlan {
            key,
            cols: cols.into_iter().collect(),
            filters: alias_filters,
        });
    }
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

/// The expressions of the output columns `def.id_outputs` names (see `ParsedDef::id_outputs`): a name
/// is a `CREATE VIEW` column-list name, an output alias, or the column name of an unaliased column
/// reference (case-insensitive). Grouped plans take group keys only, windowed plans shared partition
/// keys only.
fn chosen_id(
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

/// A backtick-quoted identifier for the AST. The stored value keeps the doubled backticks a name
/// needs inside quotes, exactly as the tokenizer leaves them, so a generated identifier and a
/// parsed one of the same name hold the same value (see [`unquote`], [`expr_key`]).
pub(crate) fn qident(id: &str) -> Ident {
    Ident::with_quote('`', id.replace('`', "``"))
}

/// Whether anything in `t` carries a `COLLATE` of its own: a `COLLATE` clause anywhere in the
/// definition (projection, WHERE, joins, GROUP BY, HAVING, ORDER BY, subqueries) makes the server
/// compare with an EXPLICIT collation, which an equally EXPLICIT parameter collation cannot meet.
fn has_collate(t: &impl Visit) -> bool {
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
        if reserved(&t.1) {
            bail!("reserved table `{}` cannot be a view source", t.1);
        }
        if !tables.contains_key(t) {
            bail!("table `{}`.`{}` does not exist", t.0, t.1);
        }
    }
    let output_names = (!def.columns.is_empty()).then(|| def.columns.clone());
    // The definition as written; the incremental plans below add what the inlined VIRTUAL columns
    // carry. Never a reason to reject, only to bind string parameters the other way.
    let has_explicit_collate = has_collate(&def.query);
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
        edges: Vec::new(),
        unit_refs: Vec::new(),
        has_explicit_collate,
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
                    .all(|w| w.iter().any(|x| expr_key(x) == expr_key(e)))
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
    // The caller's id (`ParsedDef::id_outputs`) replaces the unit; a single-row aggregate has none.
    let chosen = if def.id_outputs.is_empty() {
        None
    } else if matches!(mode, Mode::Global) {
        bail!("a single-row aggregate has no id outputs");
    } else {
        let keys = match mode {
            Mode::Grouped if windowed => Some(partition.as_slice()),
            Mode::Grouped => Some(resolved_groups.as_slice()),
            _ => None,
        };
        Some(chosen_id(def, &proj_exprs, keys, &scope)?)
    };
    let unit_refs: Vec<Expr> = match mode {
        _ if chosen.is_some() => {
            let c = chosen.clone().unwrap();
            for e in &c {
                for (a, col) in col_refs(e, &scope) {
                    referenced[a][col] = true;
                    membership[a][col] = true;
                }
            }
            c
        }
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

    // A group or partition key that the rest of the key determines adds nothing to the unit (the
    // partitions are the same sets).
    let unit_refs = if mode == Mode::Grouped && chosen.is_none() {
        reduce_unit(unit_refs, &scope, &base_edges, &own_edges)
    } else {
        unit_refs
    };
    // The plan's equality edges (the WHERE / inner ON, plus each LEFT-joined alias's own ON)
    // and the column refs of the reduced units: the hop paths of `crate::access` walk them.
    let edges: Vec<Edge> = base_edges
        .iter()
        .map(|&(a, b)| Edge {
            a,
            b,
            only_with: None,
        })
        .chain(own_edges.iter().enumerate().flat_map(|(ja, es)| {
            es.iter().map(move |&(a, b)| Edge {
                a,
                b,
                only_with: Some(ja),
            })
        }))
        .collect();
    let unit_refs_cols: Vec<Vec<ColRef>> = unit_refs.iter().map(|e| col_refs(e, &scope)).collect();
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
                None => expr_key(e) == expr_key(u),
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
    // A chosen id is the caller's: not reduced, and not known to be unique.
    let unit_unique = chosen.is_none()
        && match mode {
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
    if matches!(mode, Mode::Spj | Mode::Grouped) {
        cascade_maps(&mut cascades, &alias_plans, &alias_filters, &partial);
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
    // The statement as the lookups see it: a VIRTUAL column's inlined form carries a `COLLATE` of
    // its own too, so this only adds to what the definition as written already had.
    let has_explicit_collate = has_explicit_collate
        || has_collate(&*select)
        || order_usage.as_ref().is_some_and(has_collate);
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
        edges,
        unit_refs: unit_refs_cols,
        has_explicit_collate,
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
                    Expr::InList {
                        expr: left,
                        list,
                        negated: false,
                    } if list.len() == 1 => scope
                        .resolve(left)
                        .zip(scope.resolve(&list[0]))
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
                child_table: child.clone(),
                map: None,
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

/// The `JoinConstraint` of a join operator, `None` for the operators that carry none. One owner,
/// also read by `crate::analyzer` (its `ast` module re-exports it).
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

/// A backtick-quoted identifier: the name `` a`b `` renders as `` `a``b` ``.
pub fn quote_ident(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

#[cfg(test)]
mod tests;
