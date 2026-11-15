//! The PostgreSQL family's answers on the dialect seam: the parse profile (how a definition's
//! text is tokenized and handed to the query parser, on sqlparser's `PostgreSqlDialect`, with
//! the refusal of the constructs only this family's server has), and how one identifier reads
//! once the server's case rule has been applied — the answers the analyzer's case rule and its
//! name resolution ask. The function vocabularies, the type kinds and the JSON renderings come
//! with `pgsrc-fn-vocab`, the scoping and the remaining refusal rules with `pgsrc-group-scope`.
//! `std`, `sqlparser` and `anyhow` only — no host code.
//!
//! Copy set: lazy-wire-fire copies this file together with `src/dialect/mod.rs` (its parent, the
//! [`Family`](super::Family) discriminator) and `src/dialect/mysql.rs` verbatim, as one set, so
//! the answers of both families compile standalone.

use anyhow::anyhow;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

/// The PostgreSQL-family parse profile, one instance shared by [`tokenize_view`] and
/// [`new_parser`].
static PROFILE: PostgreSqlDialect = PostgreSqlDialect {};

/// Tokens of a definition with locations on the PostgreSQL profile (no unescaping: literals keep
/// their escapes). Where the MySQL family refuses `/*! … */` and `/*M! … */` because its servers
/// run their content ([`super::mysql::tokenize_view`]), this family reads them as ordinary
/// comments: what lazyview plans is what the server runs, so there is nothing to refuse. A
/// double-quoted name is an identifier here, where the MySQL profile reads the same text as a
/// string; the constructs this family alone has and lazyview does not accept yet are refused
/// here, so no definition of them can reach the plan.
pub(crate) fn tokenize_view(sql: &str) -> anyhow::Result<Vec<TokenWithSpan>> {
    let tokens = Tokenizer::new(&PROFILE, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|_| anyhow!("tokenize: the text does not tokenize"))?;
    if let Some(what) = unsupported_construct(&tokens) {
        return Err(anyhow!(
            "the definition uses `{what}`, which only a PostgreSQL server has and lazyview does not accept yet: write the view without it"
        ));
    }
    Ok(tokens)
}

/// A query parser over tokens with locations on the PostgreSQL-family profile: the
/// `Parser::new(&PostgreSqlDialect {})` of the definition parser, so the profile has one home.
pub(crate) fn new_parser(tokens: Vec<TokenWithSpan>) -> Parser<'static> {
    Parser::new(&PROFILE).with_tokens_with_locations(tokens)
}

/// The construct, if any, that only this family's server has and lazyview does not accept yet,
/// named by the token that shows it so the refusal can name it. A quoted identifier is never the
/// construct (`"lateral"` is an ordinary column name; the bare word is this family's reserved
/// word, so the scan refuses nothing a server would take); the analyzer's own refusals cover the
/// constructs both families have, and `pgsrc-group-scope` brings the rest of this family's rules.
fn unsupported_construct(tokens: &[TokenWithSpan]) -> Option<&'static str> {
    let word = |t: &TokenWithSpan, k: &str| {
        matches!(&t.token, Token::Word(w)
            if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(k))
    };
    // Comments are whitespace tokens, so the next word is the next thing the server reads.
    let words: Vec<&TokenWithSpan> = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();
    words.iter().enumerate().find_map(|(i, t)| {
        if word(t, "LATERAL") {
            Some("LATERAL")
        } else if word(t, "DISTINCT") && words.get(i + 1).is_some_and(|n| word(n, "ON")) {
            Some("DISTINCT ON")
        } else {
            None
        }
    })
}

/// The key form of one identifier, for the plan-time comparisons of
/// [`crate::analyzer::expr_key`]: the value and quote style to render it with. The server folds
/// an unquoted name to lower as it resolves it, so the key folds it too; a double-quoted name is
/// exactly the name that was asked for, so the key keeps its case and keeps the quotes — `"Foo"`
/// is another column than `Foo` and must not key the same. The parts of a compound identifier
/// follow the same rule: this family folds a table or schema name as it folds a column name.
pub(crate) fn ident_key(value: &str, quote_style: Option<char>) -> (String, Option<char>) {
    match quote_style {
        Some(q) => (value.to_string(), Some(q)),
        None => (value.to_lowercase(), None),
    }
}

/// The name this family's server gives a column: an unquoted name comes back folded to lower, as
/// the resolver folds it; a double-quoted one comes back exactly as it was written.
pub(crate) fn fold_name(name: String, quoted: bool) -> String {
    if quoted {
        name
    } else {
        name.to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::Family;
    use sqlparser::ast::{Expr, Ident, SelectItem, Value};

    /// The first select item of `sql`, read on `family`'s parse profile.
    fn first_item(sql: &str, family: Family) -> SelectItem {
        let tokens = crate::dialect::tokenize_view(family, sql).unwrap();
        crate::dialect::new_parser(family, tokens)
            .unwrap()
            .parse_select_item()
            .unwrap()
    }

    /// The profile is the PostgreSQL one: what its server reads is what lazyview plans.
    #[test]
    fn parse_profile_is_the_postgresql_one() {
        // The executable comments are this family's ordinary comments (the MySQL profile refuses
        // them because its servers run them).
        let def = "CREATE VIEW entities AS SELECT id /*! 1 */ /*M! NNNNN x */ FROM items";
        assert!(crate::dialect::mysql::tokenize_view(def).is_err());
        assert!(tokenize_view(def).is_ok());
        // A double-quoted name is an identifier here, a string on the MySQL profile.
        assert_eq!(
            first_item("\"Id\"", Family::Postgres),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::with_quote('"', "Id"))),
        );
        assert_eq!(
            first_item("\"Id\"", Family::Mysql),
            SelectItem::UnnamedExpr(Expr::Value(Value::DoubleQuotedString("Id".into()).into())),
        );
        // The two profiles are the only difference: a statement both servers read the same way
        // gives the same item either way, so no MySQL plan can shift with the profile added.
        let plain = "SELECT id FROM items WHERE id > 1";
        assert_eq!(
            first_item(plain, Family::Postgres),
            first_item(plain, Family::Mysql)
        );
    }

    /// A construct only a PostgreSQL server has is refused, and the refusal names it; the MySQL
    /// profile, which accepts the same text, plans it as it always did.
    #[test]
    fn pg_only_constructs_are_refused() {
        for (sql, what) in [
            ("SELECT DISTINCT ON (id) id FROM items", "DISTINCT ON"),
            (
                "SELECT id FROM items NATURAL JOIN LATERAL (SELECT 1) AS links",
                "LATERAL",
            ),
        ] {
            let err = tokenize_view(sql).unwrap_err().to_string();
            assert!(err.contains(what), "{sql}: {err}");
            assert!(err.contains("does not accept"), "{sql}: {err}");
        }
        // Quoted, it is an ordinary name; a bare `DISTINCT` is not the construct. (`lateral`
        // unquoted is this family's reserved word, so refusing it refuses nothing real.)
        assert!(tokenize_view("SELECT \"lateral\", id FROM items").is_ok());
        assert!(tokenize_view("SELECT id AS \"lateral\" FROM items").is_ok());
        assert!(tokenize_view("SELECT DISTINCT id FROM items").is_ok());
        assert!(crate::dialect::tokenize_view(
            Family::Mysql,
            "SELECT id FROM items NATURAL JOIN LATERAL (SELECT 1) AS links"
        )
        .is_ok());
    }
}
