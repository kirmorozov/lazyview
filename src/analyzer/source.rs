//! The definition's SELECT as written: re-parsed so that every node's span points into it, which
//! gives the source text of projection items at any nesting level (the server names unaliased
//! columns after that text).

use std::collections::HashMap;

use sqlparser::ast::{Query, Select};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

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

/// Tokens of the SELECT text with locations (no unescaping: literals keep their escapes).
pub(crate) fn tokenize(sql: &str) -> Option<Vec<TokenWithSpan>> {
    Tokenizer::new(&MySqlDialect {}, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .ok()
}

/// Parses `sql` as one query (nothing may follow it).
pub(crate) fn parse_query(sql: &str) -> Option<(Query, Vec<TokenWithSpan>)> {
    let tokens = tokenize(sql)?;
    let mut p = Parser::new(&MySqlDialect {}).with_tokens_with_locations(tokens.clone());
    let q = p.parse_query().ok()?;
    while p.peek_token().token == Token::SemiColon {
        p.next_token();
    }
    (p.peek_token().token == Token::EOF).then_some((*q, tokens))
}

/// Source text lookups for one parsed SELECT text.
pub(crate) struct Text<'a> {
    sql: &'a str,
    tokens: Vec<TokenWithSpan>,
    line_starts: Vec<usize>,
    /// Item texts per SELECT, keyed by the location of its `SELECT` keyword.
    cache: std::cell::RefCell<HashMap<(u64, u64), Vec<Option<String>>>>,
}

impl<'a> Text<'a> {
    pub(crate) fn new(sql: &'a str, tokens: Vec<TokenWithSpan>) -> Text<'a> {
        let line_starts = std::iter::once(0)
            .chain(sql.match_indices('\n').map(|(i, _)| i + 1))
            .collect();
        Text {
            sql,
            tokens,
            line_starts,
            cache: Default::default(),
        }
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

    fn text(&self, a: &TokenWithSpan, b: &TokenWithSpan) -> Option<&'a str> {
        self.sql.get(self.at(a.span.start)?..self.at(b.span.end)?)
    }

    /// Source text of each projection item of `select` (None where it cannot be recovered
    /// exactly, e.g. for statements the analyzer built itself).
    pub(crate) fn items(&self, select: &Select) -> Vec<Option<String>> {
        let at = select.select_token.0.span.start;
        let key = (at.line, at.column);
        if let Some(v) = self.cache.borrow().get(&key) {
            return v.clone();
        }
        let v = match self.split_projection(at) {
            Some(texts) if texts.len() == select.projection.len() => texts
                .into_iter()
                .zip(&select.projection)
                .map(|(text, item)| {
                    let mut p =
                        Parser::new(&MySqlDialect {}).with_tokens_with_locations(tokenize(text)?);
                    let parsed = p.parse_select_item().ok()?;
                    (p.peek_token().token == Token::EOF && parsed.to_string() == item.to_string())
                        .then(|| text.to_string())
                })
                .collect(),
            _ => vec![None; select.projection.len()],
        };
        self.cache.borrow_mut().insert(key, v.clone());
        v
    }

    /// Splits the projection list after the `SELECT` keyword at `select_at` at its top-level
    /// commas.
    fn split_projection(&self, select_at: Location) -> Option<Vec<&'a str>> {
        if select_at.line == 0 {
            return None;
        }
        let tokens = &self.tokens;
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
                items.push(self.text(first?, last?)?);
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
}
