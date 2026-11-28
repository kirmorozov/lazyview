//! #278 — the follow-up statements of a view definition (owner rulings D1, D4).
//!
//! A definition is one `CREATE VIEW` plus its **free-standing follow-up statements**, written as
//! someone would type them by hand (D1: "follow up statements, as if someone would write them
//! manually"), split at top-level `;`. The grammar is ADD-ONLY:
//!
//! ```sql
//! CREATE [UNIQUE|FULLTEXT] INDEX <name> ON <view> (col, ...)
//! ALTER TABLE <view> ADD COLUMN <name> <type> AS (expr) VIRTUAL|STORED   -- D4, parsed here
//! ```
//!
//! There is no removal statement and no update statement (D1): the engine converges the view
//! table's index set to the declared set itself (D7/D9). Anything else — `DROP INDEX`, a prefix
//! length, an expression (functional) part, a `USING` method, `WITH` options, `SPATIAL`, a
//! partial-index `WHERE`, or tokens after the closing `)` — is refused here, at PARSE, on every
//! flavor (E8: "MySQL 8.0 *would* accept the functional index while MariaDB would not", so one
//! definition must mean one thing everywhere). The refusal names the statement, the offending
//! construct and the supported subset.

use super::*;

/// `CREATE [UNIQUE|FULLTEXT] INDEX <name> ON <table> (cols)` — the AST part (c) of every
/// expectations.md case asserts: name, kind, and the columns **in the declared order**.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDecl {
    pub name: String,
    /// The table (view) the statement names; checked against the view's own name.
    pub table: String,
    pub cols: Vec<String>,
    pub unique: bool,
    pub fulltext: bool,
    /// The statement as written: the error taxonomy quotes it, and the parse result must keep it.
    pub stmt: String,
}

/// D4's generated-column declaration, parsed on this seam and rendered by the separate PR line
/// (`breakdown.md` P9). Parsing it here is what lets `CREATE INDEX … (sig)` over a declared
/// generated column pass the column check while E3's undeclared one is refused.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDecl {
    pub table: String,
    pub name: String,
    /// The type as written (`VARCHAR(32)`), verbatim.
    pub col_type: String,
    /// The generation expression as written, without the surrounding parentheses.
    pub expr: String,
    pub stored: bool,
    pub stmt: String,
}

/// One follow-up statement of the script.
#[derive(Debug, Clone, PartialEq)]
pub enum Decl {
    Index(IndexDecl),
    Column(ColumnDecl),
}

impl Decl {
    /// The statement as written.
    pub fn stmt(&self) -> &str {
        match self {
            Decl::Index(d) => &d.stmt,
            Decl::Column(d) => &d.stmt,
        }
    }

    /// The table (view) the statement names.
    pub fn table(&self) -> &str {
        match self {
            Decl::Index(d) => &d.table,
            Decl::Column(d) => &d.table,
        }
    }
}

/// The subset the user grammar accepts, quoted in every refusal. The plain `CREATE INDEX` spelling
/// comes first and verbatim: E8 (c) asks that a refusal name the statement type it refuses
/// (`last_error must name "CREATE INDEX"`), and a refusal that only ever spells the bracketed form
/// names a string the user never wrote.
const SUPPORTED: &str = "the supported subset is CREATE INDEX name ON <view> (col, ...) — with \
                        UNIQUE or FULLTEXT after CREATE — and ALTER TABLE <view> ADD COLUMN name \
                        <type> AS (expr) VIRTUAL|STORED";

/// The refusal of a follow-up statement: the statement, the offending construct, the subset.
fn refusal(stmt: &str, what: &str) -> anyhow::Error {
    anyhow!("unsupported statement `{stmt}`: {what}. {SUPPORTED}")
}

/// The statements of a definition script: split at top-level `;` (a `;` inside parentheses or a
/// string literal is not top-level: the tokenizer already knows both), with comments and blank
/// statements dropped. The first is the view itself; the rest are the declarations.
pub(crate) fn split_statements(def: &str) -> Result<Vec<String>> {
    let tokens = crate::analyze::parse::tokenize(def)?;
    let text = crate::analyzer::Text::new(def, tokens.clone());
    let mut out = Vec::new();
    let mut start: Option<&TokenWithSpan> = None;
    let mut depth = 0i32;
    for t in &tokens {
        match t.token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::SemiColon if depth <= 0 => {
                if let Some(a) = start.take() {
                    if let Some(b) = prev_significant(&tokens, t) {
                        if let Some(s) = text.text(a, b) {
                            let s = s.trim();
                            if !s.is_empty() {
                                out.push(s.to_string());
                            }
                        }
                    }
                }
                continue;
            }
            _ => {}
        }
        if start.is_none() && !matches!(t.token, Token::Whitespace(_)) {
            start = Some(t);
        }
    }
    if let Some(a) = start {
        if let Some(b) = tokens
            .iter()
            .rev()
            .find(|t| !matches!(t.token, Token::Whitespace(_)))
        {
            if let Some(s) = text.text(a, b) {
                let s = s.trim();
                if !s.is_empty() {
                    out.push(s.to_string());
                }
            }
        }
    }
    Ok(out)
}

/// The last token before `t` that is not whitespace (the end of a statement's text).
fn prev_significant<'a>(
    tokens: &'a [TokenWithSpan],
    t: &TokenWithSpan,
) -> Option<&'a TokenWithSpan> {
    let end = tokens.iter().position(|x| std::ptr::eq(x, t))?;
    tokens[..end]
        .iter()
        .rev()
        .find(|x| !matches!(x.token, Token::Whitespace(_)))
}

/// Whether a split statement carries nothing (only comments or whitespace): not a declaration.
pub(crate) fn is_empty_statement(s: &str) -> bool {
    s.trim().is_empty()
}

/// A definition's follow-up statements as declarations, in the order written: the engine's view
/// of what a slot must have on its targets (#278). Statements the grammar refuses never reach a
/// slot (the definition parser refuses it first), so a statement that fails to parse here
/// carries no declaration and is skipped.
pub fn parsed_decls(def: &str) -> Vec<Decl> {
    split_statements(def)
        .unwrap_or_default()
        .into_iter()
        .skip(1)
        .filter(|s| !is_empty_statement(s))
        .filter_map(|s| parse_decl(&s).ok())
        .collect()
}

/// One follow-up statement (without its trailing `;`) as an AST, or the refusal that names the
/// construct it will not accept.
pub(crate) fn parse_decl(stmt: &str) -> Result<Decl> {
    let tokens = crate::analyze::parse::tokenize(stmt)?;
    let mut c = Cur { t: &tokens, i: 0 };
    c.ws();
    if c.kw("CREATE") {
        c.i += 1;
        parse_index(&mut c, stmt)
    } else if c.kw("ALTER") {
        c.i += 1;
        parse_column(&mut c, stmt)
    } else {
        let w = c.peek_word().unwrap_or_default();
        Err(refusal(
            stmt,
            &format!("a follow-up statement must declare an index or a generated column, not start with `{w}`"),
        ))
    }
}

/// `CREATE [UNIQUE|FULLTEXT] INDEX [IF NOT EXISTS] <name> ON <table> (cols)`.
fn parse_index(c: &mut Cur, stmt: &str) -> Result<Decl> {
    let mut unique = false;
    let mut fulltext = false;
    loop {
        c.ws();
        if c.kw("UNIQUE") {
            unique = true;
        } else if c.kw("FULLTEXT") {
            fulltext = true;
        } else if c.kw("SPATIAL") {
            return Err(refusal(
                stmt,
                "SPATIAL indexes are not supported (only a secondary or a FULLTEXT index)",
            ));
        } else {
            break;
        }
        c.i += 1;
    }
    if !c.kw("INDEX") && !c.kw("KEY") {
        return Err(refusal(stmt, "expected INDEX after CREATE"));
    }
    c.i += 1;
    c.ws();
    if c.kw("IF") {
        c.i += 1;
        c.ws();
        if c.kw("NOT") {
            c.i += 1;
            c.ws();
        }
        if !c.kw("EXISTS") {
            return Err(refusal(stmt, "expected EXISTS after IF NOT"));
        }
        c.i += 1;
    }
    let name = c.ident(stmt, "an index name")?;
    c.ws();
    if !c.kw("ON") {
        return Err(refusal(stmt, "expected ON before the table of the index"));
    }
    c.i += 1;
    let table = c.qualified(stmt)?;
    c.ws();
    if !c.eat(Token::LParen) {
        return Err(refusal(
            stmt,
            "expected the column list `(...)` of the index",
        ));
    }
    let mut cols: Vec<String> = Vec::new();
    loop {
        c.ws();
        // The list closes with `)`. Otherwise, from the second column on, the separator is exactly
        // one `,` — and a column must follow it: `(total,)` and `(a,, b)` are refused, not ignored
        // (E8 (c)).
        if c.eat(Token::RParen) {
            break;
        }
        if !cols.is_empty() && !c.eat(Token::Comma) {
            return Err(refusal(
                stmt,
                "expected `,` between the columns of the index, or the `)` that closes the list",
            ));
        }
        c.ws();
        if c.eat(Token::RParen) {
            return Err(refusal(
                stmt,
                "the column list has an empty entry (a stray `,` before the `)`)",
            ));
        }
        if c.eat(Token::LParen) {
            return Err(refusal(
                stmt,
                "an expression (functional) index part is not supported: only plain column names",
            ));
        }
        let col = c.ident(stmt, "a column name")?;
        c.ws();
        if c.eat(Token::LParen) {
            return Err(refusal(
                stmt,
                "a prefix length (a column written `col(n)`) is not supported: the key is indexed whole",
            ));
        }
        if c.kw("ASC") || c.kw("DESC") {
            return Err(refusal(
                stmt,
                "per-column sort order (ASC/DESC) is not supported",
            ));
        }
        cols.push(col);
    }
    if cols.is_empty() {
        return Err(refusal(stmt, "the column list of the index is empty"));
    }
    // Nothing may follow the statement: the same rule `parse_sql` enforces for a SELECT
    // (src/analyze.rs:52-58 — a statement that does not end at EOF is an error, not a shrug).
    while c.i < c.t.len() {
        let t = &c.t[c.i].token;
        if matches!(t, Token::Whitespace(_)) || matches!(t, Token::SemiColon) {
            c.i += 1;
            continue;
        }
        let what = match t {
            Token::Word(w) if w.value.eq_ignore_ascii_case("USING") => {
                "a USING index method clause is not supported (the engine chooses the method)"
            }
            Token::Word(w) if w.value.eq_ignore_ascii_case("WITH") => {
                "WITH storage options are not supported"
            }
            Token::Word(w) if w.value.eq_ignore_ascii_case("WHERE") => {
                "a partial-index WHERE clause is not supported"
            }
            Token::Word(w) if w.value.eq_ignore_ascii_case("COMMENT") => {
                "an index COMMENT is not supported"
            }
            other => &format!("trailing garbage after the closing `)`: `{other}`"),
        };
        return Err(refusal(stmt, what));
    }
    Ok(Decl::Index(IndexDecl {
        name,
        table,
        cols,
        unique,
        fulltext,
        stmt: stmt.to_string(),
    }))
}

/// `ALTER TABLE <view> ADD [COLUMN] <name> <type> AS (expr) VIRTUAL|STORED` (D4).
fn parse_column(c: &mut Cur, stmt: &str) -> Result<Decl> {
    c.ws();
    if !c.kw("TABLE") {
        return Err(refusal(
            stmt,
            "only ALTER TABLE … ADD COLUMN (a generated column) is supported",
        ));
    }
    c.i += 1;
    let table = c.qualified(stmt)?;
    c.ws();
    if !c.kw("ADD") {
        return Err(refusal(
            stmt,
            "only ADD is supported on ALTER TABLE (there is no removal statement in the grammar)",
        ));
    }
    c.i += 1;
    c.ws();
    if c.kw("COLUMN") {
        c.i += 1;
    } else if !c.peek_wordish() {
        return Err(refusal(stmt, "expected COLUMN and a column name after ADD"));
    }
    let name = c.ident(stmt, "a column name")?;
    // The type: everything up to `AS`, which cannot contain top-level parentheses of its own
    // (`DECIMAL(10,2)` may); the expression is the balanced parenthesized part that follows.
    let mut col_type = String::new();
    loop {
        c.ws();
        match c.t.get(c.i) {
            None => {
                return Err(refusal(
                    stmt,
                    "expected AS (expression) after the type of the generated column",
                ))
            }
            Some(_) if c.kw("AS") => break,
            Some(t) if matches!(t.token, Token::Comma) => {
                return Err(refusal(
                    stmt,
                    "a generated column is declared by one statement of its own",
                ))
            }
            Some(t) => {
                if !col_type.is_empty() && needs_space(&col_type, &t.token.to_string()) {
                    col_type.push(' ');
                }
                col_type.push_str(&t.token.to_string());
                c.i += 1;
            }
        }
    }
    c.i += 1; // AS
    c.ws();
    let generated = if c.kw("GENERATED") {
        c.i += 1;
        c.ws();
        if c.kw("ALWAYS") {
            c.i += 1;
            c.ws();
        }
        if !c.kw("AS") {
            return Err(refusal(
                stmt,
                "expected ALWAYS AS in the generated column definition",
            ));
        }
        c.i += 1;
        c.ws();
        true
    } else {
        false
    };
    if !generated && col_type.trim().is_empty() {
        return Err(refusal(stmt, "the generated column has no type"));
    }
    if !c.eat(Token::LParen) {
        return Err(refusal(
            stmt,
            "expected the parenthesized generation expression of the column",
        ));
    }
    let open = c.i - 1;
    let mut depth = 1usize;
    let close = loop {
        match c.t.get(c.i) {
            None => {
                return Err(refusal(
                    stmt,
                    "the generation expression of the column is not closed",
                ))
            }
            Some(t) if matches!(t.token, Token::LParen) => depth += 1,
            Some(t) if matches!(t.token, Token::RParen) => {
                depth -= 1;
                if depth == 0 {
                    break c.i;
                }
            }
            _ => {}
        }
        c.i += 1;
    };
    let text = crate::analyzer::Text::new(stmt, c.t.to_vec());
    let a = &c.t[open + 1];
    let b = &c.t[close.saturating_sub(1).max(open + 1)];
    let expr = text
        .text(a, b)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| refusal(stmt, "the generation expression of the column is empty"))?
        .to_string();
    c.i = close + 1;
    // VIRTUAL | STORED (PERSISTENT is MariaDB's spelling of STORED); the default is VIRTUAL.
    let mut stored = false;
    let mut kind_seen = false;
    while c.i < c.t.len() {
        if matches!(c.t[c.i].token, Token::Whitespace(_)) {
            c.i += 1;
            continue;
        }
        if c.kw("VIRTUAL") {
            kind_seen = true;
            c.i += 1;
            continue;
        }
        if c.kw("STORED") || c.kw("PERSISTENT") {
            stored = true;
            kind_seen = true;
            c.i += 1;
            continue;
        }
        if c.kw("NOT") {
            c.i += 1;
            c.ws();
            if c.kw("NULL") {
                c.i += 1;
                continue;
            }
            c.i -= 1;
        }
        break;
    }
    while c.i < c.t.len() {
        if matches!(c.t[c.i].token, Token::Whitespace(_))
            || matches!(c.t[c.i].token, Token::SemiColon)
        {
            c.i += 1;
            continue;
        }
        return Err(refusal(
            stmt,
            &format!(
                "trailing garbage after the generated column: `{}`",
                c.t[c.i].token
            ),
        ));
    }
    let _ = kind_seen;
    Ok(Decl::Column(ColumnDecl {
        table,
        name,
        col_type: col_type.trim().to_string(),
        expr,
        stored,
        stmt: stmt.to_string(),
    }))
}

/// Whether a space is needed between two rendered tokens of a column type.
fn needs_space(before: &str, after: &str) -> bool {
    !matches!(
        (before.chars().last(), after.chars().next()),
        (Some('('), _) | (_, Some('(')) | (_, Some(')')) | (_, Some(',')) | (_, Some('.'))
    )
}

/// The cross-checks a parsed script must pass before it reaches the planner: the declaration
/// names its own view, stays out of the engine's `_lv_*` namespace, and only indexes columns the
/// view actually defines (E3, E10). `cols` is empty when the view's columns cannot be named
/// statically (`SELECT *`, an expression without an alias): the check then cannot be honest and
/// is skipped — the engine re-checks it against the layout of the real build.
pub(crate) fn check(view: &str, cols: &[String], decls: &[Decl]) -> Result<()> {
    for d in decls {
        if !d.table().eq_ignore_ascii_case(view) {
            bail!(
                "{} names table `{}`, but the definition creates view `{view}`: a follow-up \
                 statement must name the view itself",
                d.stmt(),
                d.table(),
            );
        }
        let Decl::Index(i) = d else { continue };
        if i.name.starts_with("_lv_") {
            return Err(refusal(
                &i.stmt,
                &format!(
                    "the index name `{}` is in the engine's reserved `_lv_*` namespace (its keys \
                     are the engine's own)",
                    i.name
                ),
            ));
        }
        for col in &i.cols {
            if col.starts_with("_lv_") {
                return Err(refusal(
                    &i.stmt,
                    &format!(
                        "the column `{col}` is in the engine's reserved `_lv_*` namespace \
                         (its columns are invisible and not the user's to index)"
                    ),
                ));
            }
            if !cols.is_empty() && !cols.iter().any(|v| v.eq_ignore_ascii_case(col)) {
                return Err(refusal(
                    &i.stmt,
                    &format!(
                        "the view `{view}` does not define column `{col}`; its columns are ({}) \
                         (a generated column must be declared with its own ALTER TABLE … ADD \
                         COLUMN statement first)",
                        cols.join(", "),
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The columns the view defines, as far as they can be named statically: an alias, or a bare
/// column name. Empty says "cannot be named statically" (`SELECT *`, an unaliased expression), in
/// which case [`check`] cannot be honest and the engine's layout check is the one that answers.
/// The columns D4 declares count as defined ones (an index may be declared over a generated
/// column in the same script).
pub(crate) fn view_columns(query: &Query, decls: &[Decl]) -> Vec<String> {
    let mut out = Vec::new();
    let SetExpr::Select(sel) = query.body.as_ref() else {
        return Vec::new();
    };
    for item in &sel.projection {
        match item {
            SelectItem::ExprWithAlias { alias, .. } => out.push(ident_name(alias)),
            SelectItem::UnnamedExpr(Expr::Identifier(i)) => out.push(ident_name(i)),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                out.push(ident_name(parts.last().expect("non-empty")));
            }
            _ => return Vec::new(),
        }
    }
    for d in decls {
        if let Decl::Column(c) = d {
            out.push(c.name.clone());
        }
    }
    out
}

/// The token cursor over one statement.
struct Cur<'a> {
    t: &'a [TokenWithSpan],
    i: usize,
}

impl Cur<'_> {
    fn ws(&mut self) {
        while self.i < self.t.len() && matches!(self.t[self.i].token, Token::Whitespace(_)) {
            self.i += 1;
        }
    }

    fn kw(&self, k: &str) -> bool {
        self.t.get(self.i).is_some_and(|t| {
            matches!(&t.token, Token::Word(w)
                if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(k))
        })
    }

    fn eat(&mut self, tok: Token) -> bool {
        self.ws();
        if self.t.get(self.i).is_some_and(|t| t.token == tok) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn peek_word(&self) -> Option<String> {
        self.t.get(self.i).map(|t| match &t.token {
            Token::Word(w) => w.value.clone(),
            other => other.to_string(),
        })
    }

    /// Whether the next token could be an identifier or a keyword of a type (not punctuation).
    fn peek_wordish(&self) -> bool {
        self.t.get(self.i).is_some_and(|t| {
            matches!(
                t.token,
                Token::Word(_) | Token::SingleQuotedString(_) | Token::DoubleQuotedString(_)
            )
        })
    }

    /// One (possibly quoted) identifier.
    fn ident(&mut self, stmt: &str, what: &str) -> Result<String> {
        self.ws();
        match self.t.get(self.i) {
            Some(t) => match &t.token {
                Token::Word(w) => {
                    self.i += 1;
                    Ok(unquote(&w.value, w.quote_style))
                }
                Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => {
                    self.i += 1;
                    Ok(s.clone())
                }
                other => Err(refusal(stmt, &format!("expected {what}, found `{other}`"))),
            },
            None => Err(refusal(
                stmt,
                &format!("expected {what}, the statement ends"),
            )),
        }
    }

    /// A possibly schema-qualified name, rendered `schema.table` or `table`.
    fn qualified(&mut self, stmt: &str) -> Result<String> {
        let mut name = self.ident(stmt, "a table name")?;
        loop {
            self.ws();
            if self.t.get(self.i).is_some_and(|t| t.token == Token::Period) {
                self.i += 1;
                name.push('.');
                name.push_str(&self.ident(stmt, "a table name")?);
                continue;
            }
            break;
        }
        Ok(name)
    }
}

/// A token's text as an identifier: backticks and quotes dropped.
fn unquote(value: &str, quote: Option<char>) -> String {
    match quote {
        None => value.to_string(),
        Some(q @ ('"' | '`' | '\'')) => value.replace(&format!("{q}{q}"), &q.to_string()),
        Some(_) => value.to_string(),
    }
}
