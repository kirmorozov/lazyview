use super::vocab::*;
use super::*;

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

pub(crate) fn tokenize(sql: &str) -> Result<Vec<TokenWithSpan>> {
    Tokenizer::new(&MySqlDialect {}, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|e| anyhow!("tokenize: {e}"))
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
pub(crate) fn cte_names(query: &Query) -> Vec<String> {
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
