//! Which tables a non-row binlog statement (DDL) may affect.
//!
//! With `binlog_format=ROW` every data change arrives as row events; QUERY events only carry DDL
//! and administrative statements. [`statement_tables`] tells the engine which tables such a
//! statement may have changed (structure or content) so their catalog entries can be invalidated
//! and dependent views rebuilt. It is deliberately cheap (sqlparser *tokenizer* only, no parsing)
//! and conservative: when a statement form is recognised every table it names is reported, even
//! when the change turns out to be a no-op.

use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

/// Tables possibly affected by the non-row statement `query`, executed with default database
/// `default_schema`. Unqualified names resolve to `default_schema`; names keep their spelling.
///
/// * `ALTER TABLE t …` → `t`, plus rename / exchange targets (`RENAME [TO|AS] t2`,
///   `EXCHANGE PARTITION p WITH TABLE t2`, MariaDB `CONVERT TABLE t2 TO PARTITION …`,
///   `CONVERT PARTITION p TO TABLE t2`)
/// * `DROP [TEMPORARY] TABLE [IF EXISTS] a, b` → `a`, `b`
/// * `RENAME TABLE a TO b, c TO d` → `a`, `b`, `c`, `d`
/// * `TRUNCATE [TABLE] t`, `REPAIR TABLE a, b` (may drop corrupt rows) → those tables
/// * `CREATE [OR REPLACE] [TEMPORARY] TABLE t …` (incl. `LIKE` / `AS SELECT`) → `t` only
/// * `CREATE … INDEX i ON t`, `DROP INDEX i ON t` → `t`
/// * `CREATE [OR REPLACE] / ALTER / DROP VIEW v` → `v` (views may be view sources)
/// * `DROP DATABASE x` / `CREATE OR REPLACE DATABASE x` → `("x", "*")` (every table of `x`)
///
/// [`StatementEffects::rows_kept`] is true only when every statement keeps every row of the
/// tables it names: `ALTER TABLE` (without `RENAME TO/AS`), `CREATE … INDEX` and
/// `DROP INDEX`. It is false for anything else, and for statements that cannot be analysed.
///
/// Empty for DML and for statements that cannot change table data or structure: `CREATE
/// DATABASE`, `ALTER DATABASE`, `OPTIMIZE` / `ANALYZE` / `CHECK TABLE` (physical only), users,
/// grants, routines, triggers, events, `SET`, `FLUSH`, ….
///
/// Comments are ignored, except executable comments (`/*! … */`, `/*!50001 … */`, MariaDB
/// `/*M! … */`) whose content is treated as SQL. A statement the tokenizer rejects yields
/// `(default_schema, "*")` when it starts like DDL (it cannot be analysed, so assume the worst).
pub fn statement_tables(query: &str, default_schema: &str) -> Vec<(String, String)> {
    statement_effects(query, default_schema).tables
}

/// What a non-row statement may affect.
#[derive(Debug, Default, PartialEq)]
pub struct StatementEffects {
    /// The tables it may have changed, as [`statement_tables`].
    pub tables: Vec<(String, String)>,
    /// The base tables it removes: those of `DROP TABLE[S] [IF EXISTS] a, b`. Not `DROP TEMPORARY
    /// TABLE` (it never removes a base table), `DROP INDEX i ON t` or `DROP VIEW`.
    pub dropped: Vec<(String, String)>,
    /// Every row of each table in `tables` is still there after the statement: ALTER TABLE
    /// (without RENAME), CREATE INDEX, DROP INDEX. False for anything else (RENAME TABLE,
    /// ALTER TABLE ... RENAME TO/AS, TRUNCATE, REPAIR, CREATE [OR REPLACE] TABLE, DROP, database
    /// statements, views, statements that cannot be parsed).
    pub rows_kept: bool,
}

/// [`statement_tables`] plus the tables the statement removes (see [`StatementEffects`]).
pub fn statement_effects(query: &str, default_schema: &str) -> StatementEffects {
    let toks = match lex(query) {
        Some(t) => t,
        None => {
            return StatementEffects {
                tables: fallback(query, default_schema),
                dropped: Vec::new(),
                rows_kept: false,
            }
        }
    };
    let mut p = Parser {
        t: &toks,
        i: 0,
        schema: default_schema,
        out: Vec::new(),
        dropped: Vec::new(),
        rows_kept: true,
        kept_stmt: false,
    };
    loop {
        let start = p.i;
        let more = p.statement();
        if !more {
            break;
        }
        // Next `;`-separated statement (QUERY events normally hold exactly one).
        p.i = start.max(p.i);
        let mut depth = 0i32;
        while let Some(t) = p.t.get(p.i) {
            match t {
                Tok::LParen => depth += 1,
                Tok::RParen => depth -= 1,
                Tok::Semi if depth <= 0 => break,
                _ => {}
            }
            p.i += 1;
        }
        if p.i >= p.t.len() {
            break;
        }
        p.i += 1; // skip `;`
    }
    let dedup = |v: Vec<(String, String)>| {
        let mut out: Vec<(String, String)> = Vec::with_capacity(v.len());
        for t in v {
            if !out.contains(&t) {
                out.push(t);
            }
        }
        out
    };
    StatementEffects {
        tables: dedup(p.out),
        dropped: dedup(p.dropped),
        rows_kept: p.rows_kept,
    }
}

/// Significant tokens: whitespace and comments dropped, executable comments expanded.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// Unquoted word (keyword or identifier).
    Word(String),
    /// Backtick- or double-quoted identifier.
    Quoted(String),
    Dot,
    Comma,
    LParen,
    RParen,
    Semi,
    Eq,
    Other,
}

fn lex(sql: &str) -> Option<Vec<Tok>> {
    let mut out = Vec::new();
    lex_into(sql, &mut out, 0)?;
    Some(out)
}

fn lex_into(sql: &str, out: &mut Vec<Tok>, depth: u8) -> Option<()> {
    let dialect = MySqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    for t in tokens {
        let tok = match t {
            Token::Whitespace(Whitespace::MultiLineComment(c)) => {
                // MySQL `/*!…*/` hints are expanded by the tokenizer already; MariaDB's
                // `/*M!…*/` (and `!` in case that ever changes) are expanded here.
                let body = c.strip_prefix("M!").or_else(|| c.strip_prefix('!'));
                if let (Some(body), true) = (body, depth < 4) {
                    lex_into(
                        body.trim_start_matches(|ch: char| ch.is_ascii_digit()),
                        out,
                        depth + 1,
                    )?;
                }
                continue;
            }
            Token::Whitespace(_) | Token::EOF => continue,
            Token::Word(w) if w.quote_style.is_some() => Tok::Quoted(w.value),
            Token::Word(w) => Tok::Word(w.value),
            Token::DoubleQuotedString(s) => Tok::Quoted(s), // ANSI_QUOTES identifiers
            Token::Period => Tok::Dot,
            Token::Comma => Tok::Comma,
            Token::LParen => Tok::LParen,
            Token::RParen => Tok::RParen,
            Token::SemiColon => Tok::Semi,
            Token::Eq => Tok::Eq,
            _ => Tok::Other,
        };
        out.push(tok);
    }
    Some(())
}

/// The tokenizer failed (should not happen for SQL the server executed): if it looks like DDL,
/// report the whole default schema.
fn fallback(query: &str, default_schema: &str) -> Vec<(String, String)> {
    let mut rest = query.trim_start();
    // Skip leading plain comments.
    while let Some(r) = rest.strip_prefix("/*") {
        if r.starts_with('!') || r.starts_with("M!") {
            rest = r
                .trim_start_matches(['!', 'M'])
                .trim_start_matches(|c: char| c.is_ascii_digit());
            break;
        }
        rest = r
            .split_once("*/")
            .map(|(_, r)| r)
            .unwrap_or("")
            .trim_start();
    }
    let first: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    let ddl = ["ALTER", "CREATE", "DROP", "RENAME", "TRUNCATE", "REPAIR"];
    if ddl.iter().any(|k| first.eq_ignore_ascii_case(k)) {
        vec![(default_schema.to_string(), "*".to_string())]
    } else {
        Vec::new()
    }
}

struct Parser<'a> {
    t: &'a [Tok],
    i: usize,
    schema: &'a str,
    out: Vec<(String, String)>,
    /// Base tables removed (`DROP TABLE`), also in `out`.
    dropped: Vec<(String, String)>,
    /// True while every parsed statement keeps every row of the tables it names
    /// ([`StatementEffects::rows_kept`]).
    rows_kept: bool,
    /// Whether the statement being parsed keeps every row. Reset to false before each statement,
    /// so a statement form that does not set it counts as removing rows.
    kept_stmt: bool,
}

impl Parser<'_> {
    fn peek_kw(&self, off: usize, kw: &str) -> bool {
        matches!(self.t.get(self.i + off), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
    }

    fn at_kw(&self, kw: &str) -> bool {
        self.peek_kw(0, kw)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        let hit = self.at_kw(kw);
        if hit {
            self.i += 1;
        }
        hit
    }

    fn eat_any(&mut self, kws: &[&str]) -> bool {
        kws.iter().any(|k| self.eat_kw(k))
    }

    /// `IF EXISTS` / `IF NOT EXISTS`.
    fn eat_if_exists(&mut self) {
        if self.at_kw("IF") {
            if self.peek_kw(1, "EXISTS") {
                self.i += 2;
            } else if self.peek_kw(1, "NOT") && self.peek_kw(2, "EXISTS") {
                self.i += 3;
            }
        }
    }

    fn ident(&mut self) -> Option<String> {
        let s = match self.t.get(self.i)? {
            Tok::Word(w) | Tok::Quoted(w) => w.clone(),
            _ => return None,
        };
        self.i += 1;
        Some(s)
    }

    /// `name` or `schema.name`.
    fn name(&mut self) -> Option<(String, String)> {
        let first = self.ident()?;
        if self.t.get(self.i) == Some(&Tok::Dot) {
            self.i += 1;
            let second = self.ident()?;
            return Some((first, second));
        }
        Some((self.schema.to_string(), first))
    }

    fn push_name(&mut self) -> bool {
        match self.name() {
            Some(n) => {
                self.out.push(n);
                true
            }
            None => false,
        }
    }

    /// Comma separated table list (`DROP TABLE a, b`, `REPAIR TABLE a, b`).
    fn push_name_list(&mut self) {
        while self.push_name() {
            if self.t.get(self.i) == Some(&Tok::Comma) {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    /// Skips `ALGORITHM = x`, `DEFINER = user`, `SQL SECURITY x` and similar prefixes of
    /// `CREATE` / `ALTER`; returns once none matches.
    fn skip_object_attributes(&mut self) {
        loop {
            if self.at_kw("ALGORITHM") || self.at_kw("DEFINER") {
                let definer = self.at_kw("DEFINER");
                self.i += 1;
                if self.t.get(self.i) == Some(&Tok::Eq) {
                    self.i += 1;
                }
                if definer {
                    self.skip_user();
                } else {
                    self.i += 1;
                }
            } else if self.at_kw("SQL") && self.peek_kw(1, "SECURITY") {
                self.i += 3;
            } else if self.eat_any(&[
                "TEMPORARY",
                "UNIQUE",
                "FULLTEXT",
                "SPATIAL",
                "ONLINE",
                "OFFLINE",
                "IGNORE",
                "AGGREGATE",
            ]) {
            } else if self.at_kw("OR") && self.peek_kw(1, "REPLACE") {
                self.i += 2;
            } else {
                return;
            }
        }
    }

    /// `user`, `'user'@'host'`, `` `u`@`h` ``, `u@h`, `CURRENT_USER[()]`.
    fn skip_user(&mut self) {
        if self.i < self.t.len() {
            self.i += 1;
        }
        if self.t.get(self.i) == Some(&Tok::LParen) && self.t.get(self.i + 1) == Some(&Tok::RParen)
        {
            self.i += 2;
            return;
        }
        // `@` is an `Other` token; the host part follows it.
        if self.t.get(self.i) == Some(&Tok::Other) {
            self.i += 2;
        }
    }

    /// Parses one statement starting at `self.i`; returns whether later `;`-separated statements
    /// should be looked at (false for routine / trigger / event definitions whose bodies hold
    /// statements that are not executed now).
    fn statement(&mut self) -> bool {
        let Some(Tok::Word(first)) = self.t.get(self.i) else {
            self.rows_kept = false; // not a statement this parser understands
            return true;
        };
        let first = first.to_ascii_uppercase();
        self.i += 1;
        self.kept_stmt = false;
        let more = match first.as_str() {
            "ALTER" => {
                self.alter();
                true
            }
            "CREATE" => self.create(),
            "DROP" => {
                self.drop();
                true
            }
            "RENAME" => {
                self.rename();
                true
            }
            "TRUNCATE" => {
                // Removes the rows without row events.
                self.eat_kw("TABLE");
                self.push_name();
                true
            }
            "REPAIR" => {
                // May drop corrupt rows.
                self.eat_any(&["NO_WRITE_TO_BINLOG", "LOCAL"]);
                if self.eat_any(&["TABLE", "TABLES"]) {
                    self.push_name_list();
                }
                true
            }
            _ => true,
        };
        self.rows_kept &= self.kept_stmt;
        more
    }

    fn alter(&mut self) {
        self.skip_object_attributes();
        if self.eat_kw("VIEW") {
            self.push_name();
            return;
        }
        if !self.eat_kw("TABLE") {
            return; // DATABASE, USER, EVENT, PROCEDURE, SERVER, TABLESPACE, SEQUENCE, …
        }
        self.eat_if_exists();
        if !self.push_name() {
            return;
        }
        self.kept_stmt = true;
        // Scan the alter specifications (outside parentheses) for other tables and for
        // specifications that replace or remove rows.
        let mut depth = 0i32;
        while let Some(t) = self.t.get(self.i) {
            match t {
                Tok::LParen => depth += 1,
                Tok::RParen => depth -= 1,
                Tok::Semi if depth <= 0 => return,
                Tok::Word(_) if depth == 0 => {
                    if self.eat_kw("RENAME") {
                        if self.at_kw("COLUMN")
                            || self.at_kw("INDEX")
                            || self.at_kw("KEY")
                            || self.at_kw("CONSTRAINT")
                        {
                            continue;
                        }
                        // RENAME [TO|AS] t2 replaces the table's rows by another table's.
                        self.kept_stmt = false;
                        self.eat_any(&["TO", "AS"]);
                        self.push_name();
                        continue;
                    }
                    if (self.at_kw("WITH") || self.at_kw("CONVERT") || self.at_kw("TO"))
                        && self.peek_kw(1, "TABLE")
                    {
                        // Table exchange / conversion: the rows come from the other table.
                        self.kept_stmt = false;
                        self.i += 2;
                        self.push_name();
                        continue;
                    }
                    // Partition removal or exchange, tablespace discard or import, TRUNCATE
                    // PARTITION: rows are removed or replaced.
                    if ["PARTITION", "EXCHANGE", "TRUNCATE", "DISCARD", "IMPORT"]
                        .iter()
                        .any(|k| self.at_kw(k))
                    {
                        self.kept_stmt = false;
                    }
                }
                _ => {}
            }
            self.i += 1;
        }
    }

    fn create(&mut self) -> bool {
        let replace = self.at_kw("OR") && self.peek_kw(1, "REPLACE");
        self.skip_object_attributes();
        if self.eat_kw("TABLE") || self.eat_kw("VIEW") {
            self.eat_if_exists();
            self.push_name();
        } else if self.eat_kw("INDEX") {
            self.index_target();
            self.kept_stmt = true;
        } else if self.at_kw("DATABASE") || self.at_kw("SCHEMA") {
            self.i += 1;
            if replace {
                self.eat_if_exists();
                if let Some(db) = self.ident() {
                    self.out.push((db, "*".into()));
                }
            }
        } else if self.at_kw("PROCEDURE")
            || self.at_kw("FUNCTION")
            || self.at_kw("TRIGGER")
            || self.at_kw("EVENT")
            || self.at_kw("PACKAGE")
        {
            return false;
        }
        true
    }

    fn drop(&mut self) {
        let temporary = self.eat_kw("TEMPORARY");
        if self.eat_any(&["TABLE", "TABLES"]) {
            self.eat_if_exists();
            let from = self.out.len();
            self.push_name_list();
            if !temporary {
                let names = self.out[from..].to_vec();
                self.dropped.extend(names);
            }
        } else if self.eat_kw("VIEW") {
            self.eat_if_exists();
            self.push_name_list();
        } else if self.eat_any(&["DATABASE", "SCHEMA"]) {
            self.eat_if_exists();
            if let Some(db) = self.ident() {
                self.out.push((db, "*".into()));
            }
        } else if self.eat_kw("INDEX") {
            self.index_target();
            self.kept_stmt = true;
        }
    }

    /// `… INDEX [IF [NOT] EXISTS] idx [USING …] ON t …`: the table after the first top-level `ON`.
    fn index_target(&mut self) {
        while let Some(t) = self.t.get(self.i) {
            match t {
                Tok::LParen | Tok::Semi => return,
                Tok::Word(w) if w.eq_ignore_ascii_case("ON") => {
                    self.i += 1;
                    self.push_name();
                    return;
                }
                _ => self.i += 1,
            }
        }
    }

    /// `RENAME TABLE[S] [IF EXISTS] a [WAIT n | NOWAIT] TO b [, c TO d] …`
    fn rename(&mut self) {
        if !self.eat_any(&["TABLE", "TABLES"]) {
            return; // RENAME USER
        }
        self.eat_if_exists();
        loop {
            if !self.push_name() {
                return;
            }
            // old name … TO new name
            while let Some(t) = self.t.get(self.i) {
                match t {
                    Tok::Word(w) if w.eq_ignore_ascii_case("TO") => break,
                    Tok::Comma | Tok::Semi => return,
                    _ => self.i += 1,
                }
            }
            if !self.eat_kw("TO") || !self.push_name() {
                return;
            }
            match self.t.get(self.i) {
                Some(Tok::Comma) => self.i += 1,
                _ => return,
            }
        }
    }
}

#[cfg(test)]
mod tests;
