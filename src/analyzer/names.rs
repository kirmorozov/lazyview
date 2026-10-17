//! Column names as the server gives them to a view's columns (measured on MySQL 8.0/8.4,
//! Percona 8.0/8.4, MariaDB 10.11/11.4):
//!
//! * `expr AS name` → `name`; a column reference, also parenthesized, → the identifier as written;
//! * any other unaliased expression → its source text (string literals: their value);
//! * a generated name longer than 64 characters or ending in a space → `Name_exp_<position>`;
//! * flavor-dependent: a comment inside the text (kept by 8.4, stripped by 8.0 and MariaDB) and
//!   duplicates of generated names (`Name_exp_<x>` vs `My_exp_<x>`).

use sqlparser::ast::{Expr, Ident, Value};
use sqlparser::tokenizer::{Token, Whitespace};

use super::source::tokenize;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NameKind {
    /// Given with `AS`.
    Alias,
    /// Named after the referenced column.
    Column,
    /// Generated from the source text; `certain` is false where servers disagree.
    Generated { certain: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Name {
    pub text: String,
    pub kind: NameKind,
}

impl Name {
    pub(crate) fn new(text: impl Into<String>, kind: NameKind) -> Name {
        Name {
            text: text.into(),
            kind,
        }
    }

    pub(crate) fn certain(&self) -> bool {
        !matches!(self.kind, NameKind::Generated { certain: false })
    }

    pub(crate) fn generated(&self) -> bool {
        matches!(self.kind, NameKind::Generated { .. })
    }
}

/// The name a raw-tokenized identifier stands for (doubled quote characters are one character).
pub(crate) fn ident_name(i: &Ident) -> String {
    match i.quote_style {
        Some(q) => i.value.replace(&format!("{q}{q}"), &q.to_string()),
        None => i.value.clone(),
    }
}

/// Name of a projection item: `alias` (from `AS`), else derived from `expr` and its source text.
pub(crate) fn item_name(expr: &Expr, alias: Option<&Ident>, text: Option<&str>) -> Name {
    if let Some(a) = alias {
        return Name::new(ident_name(a), NameKind::Alias);
    }
    let mut e = expr;
    while let Expr::Nested(inner) = e {
        e = inner;
    }
    match e {
        Expr::Identifier(id) => return Name::new(ident_name(id), NameKind::Column),
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            return Name::new(ident_name(parts.last().unwrap()), NameKind::Column)
        }
        _ => {}
    }
    if !std::ptr::eq(e, expr) {
        // `(x + 1)`: the text includes the parentheses; keep it but do not guess.
        return match text {
            Some(t) => Name::new(t, NameKind::Generated { certain: false }),
            None => Name::new(expr.to_string(), NameKind::Generated { certain: false }),
        };
    }
    if let Expr::Value(v) = e {
        match &v.value {
            // Named after the value: certain only without backslash escapes (their meaning
            // depends on NO_BACKSLASH_ESCAPES) and when the text is the plain literal.
            Value::SingleQuotedString(s) => {
                let plain = !s.contains('\\') && text.is_some_and(|t| t.starts_with('\''));
                return Name::new(s.replace("''", "'"), NameKind::Generated { certain: plain });
            }
            Value::Number(..) | Value::Null | Value::Boolean(_) => {}
            // Other literals (introducers, hex, bit, double-quoted strings): not measured.
            _ => {
                return Name::new(
                    text.map(str::to_string).unwrap_or_else(|| expr.to_string()),
                    NameKind::Generated { certain: false },
                )
            }
        }
    }
    match text {
        Some(t) => Name::new(
            t,
            NameKind::Generated {
                certain: !has_comment(t),
            },
        ),
        None => Name::new(expr.to_string(), NameKind::Generated { certain: false }),
    }
}

/// Whether SQL text contains a comment token.
pub(crate) fn has_comment(sql: &str) -> bool {
    match tokenize(sql) {
        Some(tokens) => tokens.iter().any(|t| {
            matches!(
                t.token,
                Token::Whitespace(
                    Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)
                )
            )
        }),
        None => true,
    }
}

/// Applies the view-level rules to the names of a view's columns, in order: generated names
/// that are not valid column names become `Name_exp_<position>`. Returns the columns whose final
/// name differs between server flavors (an uncertain name, or a duplicate involving a generated
/// name).
pub(crate) fn finish_view_names(names: &mut [Name]) -> Vec<usize> {
    for (i, n) in names.iter_mut().enumerate() {
        if n.generated() && !valid_column_name(&n.text) {
            *n = Name::new(format!("Name_exp_{}", i + 1), NameKind::Alias);
        }
    }
    let mut out = Vec::new();
    for i in 0..names.len() {
        if !names[i].certain() {
            out.push(i);
        }
        for j in 0..i {
            if names[i].text.to_lowercase() == names[j].text.to_lowercase()
                && (names[i].generated() || names[j].generated())
            {
                out.extend([j, i]);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// `check_column_name`: not empty, at most 64 characters, not ending in a space.
fn valid_column_name(name: &str) -> bool {
    !name.is_empty() && name.chars().count() <= 64 && !name.ends_with(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::SelectItem;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn name_of(item_sql: &str) -> Name {
        let item = Parser::new(&MySqlDialect {})
            .with_tokens_with_locations(tokenize(item_sql).unwrap())
            .parse_select_item()
            .unwrap();
        match &item {
            SelectItem::UnnamedExpr(e) => item_name(e, None, Some(item_sql)),
            SelectItem::ExprWithAlias { expr, alias } => item_name(expr, Some(alias), None),
            _ => panic!("wildcard"),
        }
    }

    /// The measured server behaviour (see the module comment).
    #[test]
    fn names_as_the_server_gives_them() {
        let g = |t: &str| Name::new(t, NameKind::Generated { certain: true });
        assert_eq!(name_of("dbl + 1"), g("dbl + 1"));
        assert_eq!(name_of("dbl   +   1"), g("dbl   +   1"));
        assert_eq!(name_of("ID"), Name::new("ID", NameKind::Column));
        assert_eq!(name_of("t.DBL"), Name::new("DBL", NameKind::Column));
        assert_eq!(name_of("(dbl)"), Name::new("dbl", NameKind::Column));
        assert_eq!(name_of("`we``ird`"), Name::new("we`ird", NameKind::Column));
        assert_eq!(name_of("total*2 AS x"), Name::new("x", NameKind::Alias));
        assert_eq!(name_of("'it''s'"), g("it's"));
        assert!(!name_of(r"'a\'b'").certain());
        assert!(!name_of("total /* c */ + 1").certain());
        assert_eq!(name_of("-1"), g("-1"));
        let mut v = vec![
            name_of("dbl+1"),
            name_of(&format!("CONCAT(nm, '{}')", "a".repeat(60))),
            name_of("'abc '"),
        ];
        assert!(finish_view_names(&mut v).is_empty());
        assert_eq!(v[1].text, "Name_exp_2");
        assert_eq!(v[2].text, "Name_exp_3");
        let mut dup = vec![name_of("dbl+1"), name_of("dbl+1")];
        assert_eq!(finish_view_names(&mut dup), vec![0, 1]);
        let mut cols = vec![name_of("a.id"), name_of("b.ID")];
        assert!(
            finish_view_names(&mut cols).is_empty(),
            "the server rejects these itself"
        );
    }
}
