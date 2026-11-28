use super::vocab::*;
use super::*;

pub(crate) use crate::analyzer::object_table_ref;

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
    /// #278 (D1): the view's follow-up statements, parsed from the same `definition` cell after
    /// the `CREATE VIEW` (a declaration AST per statement, in the order written). This is where
    /// the declaration survives the parse: name, kind and columns in order (the parse result's own
    /// rendering is what the RED-TEST list probes). The last field on purpose: the probe reads the
    /// declaration *after* the query, as the script is written.
    pub decls: Vec<Decl>,
}

/// The hash of the view's own statement (everything but its follow-up declarations): a definition
/// whose declarations changed and nothing else is reconciled, not rebuilt (#278, D6). A script of
/// one statement hashes exactly as [`definition_hash`] does, so the state rows of every view that
/// declares nothing keep the hash they had before the declarations existed.
pub fn body_hash(def: &str) -> Result<String> {
    let statements = super::decls::split_statements(def)?;
    match statements.len() {
        0 => Ok(definition_hash("")),
        1 => Ok(definition_hash(def)),
        _ => Ok(definition_hash(&statements[0])),
    }
}

/// The hash of a definition's follow-up statements alone (#278): what a view's slot compares
/// against to notice that a declaration was added, changed or removed — the trigger of the
/// reconcile pass, which is not a rebuild.
pub fn decls_hash(def: &str) -> Result<String> {
    let statements = super::decls::split_statements(def)?;
    Ok(definition_hash(&statements[1..].join("; ")))
}

/// The view's own name, when its OWN statement parses and only a follow-up declaration is refused
/// (#278, E8/E3/E10). The name is written in the head, before anything the declaration grammar
/// rejects, so a refused script still names its view: a refusal that cannot be written on the
/// view's own `_lazyview_state` row is a refusal nobody can find. `None` when the view statement
/// itself is the problem — the case that has always fallen back to `_lazyview.name`.
pub fn definition_name(def: &str) -> Option<String> {
    let first = super::decls::split_statements(def)
        .ok()?
        .into_iter()
        .next()?;
    parse_definition(&first).ok().map(|p| p.name)
}

pub fn definition_hash(def: &str) -> String {
    hex::encode(Sha256::digest(def.trim().as_bytes()))[..16].to_string()
}

/// Parses `CREATE [OR REPLACE] [ALGORITHM=..] [DEFINER=..] [SQL SECURITY ..] VIEW name [(cols)] AS
/// SELECT ... [WITH [CASCADED|LOCAL] CHECK OPTION]`.
pub fn parse_definition(def: &str) -> Result<ParsedDef> {
    // The parse profile (its refusals, the tokenizer, the parser) is the dialect's answer, and
    // both families now have one: the definitions parsed here are still MySQL-family, because a
    // `postgres://` source is refused at connect until phase S5 starts the PostgreSQL reader.
    let tokens = crate::dialect::tokenize_view(crate::dialect::Family::Mysql, def)?;
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
    // Hand the remaining tokens (with their locations in `def`) to the query parser: up to the
    // `;` that ends the view's own statement, so a follow-up declaration is never parsed as part
    // of the SELECT.
    // The view's own statement ends at its top-level `;` (a `;` in parentheses or a string is not
    // top-level; the tokenizer knows both). Everything after it is the script's declarations.
    let mut head_end = tokens.len();
    let mut depth = 0i32;
    for (j, tk) in tokens.iter().enumerate().skip(words[i] + 1) {
        match tk.token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::SemiColon if depth <= 0 => {
                head_end = j;
                break;
            }
            _ => {}
        }
    }
    let rest = &tokens[words[i] + 1..head_end];
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
    // #278 (D1): more than the view's own statement is allowed, but only as the free-standing
    // follow-up statements of the script (`CREATE [UNIQUE|FULLTEXT] INDEX … ON <view>(cols)`,
    // D4's generated-column statement): they are declarations, not a second query. The view's own
    // statement is the first one; the query parser never sees past its terminating `;`.
    if statements > 1 && head_end == tokens.len() {
        // A `;` the splitter does not consider top-level (it opens a paren): not a script.
        bail!("the definition contains {statements} statements: {SINGLE_SELECT}");
    }
    let mut parser = crate::dialect::new_parser(crate::dialect::Family::Mysql, rest.to_vec())?;
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
    let text = crate::analyzer::Text::new(def, tokens.clone());
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
        .and_then(|(a, b)| text.text(a, b))
        .filter(|sql| {
            parse_sql(sql, |p| p.parse_query()).map(|q| q.to_string()) == Some(query.to_string())
        })
        .map(str::to_string)
        .unwrap_or_else(|| query.to_string());
    let item_sql = match query.body.as_ref() {
        SetExpr::Select(sel) => text.items(sel),
        _ => Vec::new(),
    };
    let name = parts.pop().unwrap();
    // #278 (D1): the rest of the script is the view's follow-up statements — the declarations.
    // Each is refused here, at parse, if it is not one of the two forms the grammar allows (E8).
    let mut decls: Vec<Decl> = Vec::new();
    for s in super::decls::split_statements(def)?
        .into_iter()
        .skip(1)
        .filter(|s| !super::decls::is_empty_statement(s))
    {
        // A follow-up that does not even begin like a declaration — a second SELECT, an INSERT, a
        // DROP — is not a definition script at all. Its refusal is the one this parser has always
        // given for a multi-statement definition (the count and the single-SELECT rule), not a
        // declaration's own message: what the declaration grammar refuses is a statement that
        // *looks* like a declaration and is not one (E8).
        let up = s.trim_start().to_ascii_uppercase();
        if !(up.starts_with("CREATE ") || up.starts_with("ALTER ")) {
            bail!("the definition contains {statements} statements: {SINGLE_SELECT}");
        }
        decls.push(
            super::decls::parse_decl(&s)
                .with_context(|| format!("the follow-up statement `{s}`"))?,
        );
    }
    let cols = super::decls::view_columns(&query, &decls);
    super::decls::check(&name, &cols, &decls)?;
    Ok(ParsedDef {
        schema: parts.pop(),
        name,
        columns,
        query: *query,
        hash: definition_hash(def),
        query_sql,
        item_sql,
        id_outputs: Vec::new(),
        decls,
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

/// Tokens of `sql` with locations, without unescaping (the analyzer's tokenizer, see
/// [`crate::analyzer::source`]).
pub(crate) fn tokenize(sql: &str) -> Result<Vec<TokenWithSpan>> {
    crate::analyzer::tokenize(sql).ok_or_else(|| anyhow!("tokenize: the text does not tokenize"))
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
                        Expr::In {
                            target: InTarget::Subquery(q),
                            ..
                        } => walk(q, out),
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
