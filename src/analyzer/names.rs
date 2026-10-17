//! Column names as the server gives them to a view's columns (measured on MySQL 8.0/8.4,
//! Percona 8.0/8.4, MariaDB 10.11/11.4):
//!
//! * `expr AS name` → `name`; a column reference, also parenthesized, → the identifier as written;
//! * any other unaliased expression → its source text (string literals: their value);
//! * a generated name longer than 64 characters or ending in a space → `Name_exp_<position>`;
//! * flavor-dependent: a comment inside the text (kept by 8.4, stripped by 8.0 and MariaDB) and
//!   duplicates of generated names (`Name_exp_<x>` vs `My_exp_<x>`).

use sqlparser::ast::{Expr, Ident, UnaryOperator, Value};
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
    // Unary plus disappears in the parser: `+total` is named `total`.
    while let Expr::UnaryOp {
        op: UnaryOperator::Plus,
        expr: inner,
    } = e
    {
        if !matches!(**inner, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            return uncertain(expr, text);
        }
        e = inner;
    }
    match e {
        Expr::Identifier(id) => return Name::new(ident_name(id), NameKind::Column),
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            return Name::new(ident_name(parts.last().unwrap()), NameKind::Column)
        }
        _ => {}
    }
    if let (Expr::Value(v), Some(t)) = (e, text) {
        // String literals are named after their value: certain when the text is exactly that
        // one literal without backslash escapes (adjacent literals `'a' 'b'` are named `a` by
        // MySQL, `ab` by MariaDB; escapes depend on NO_BACKSLASH_ESCAPES).
        let literal = |prefix: &str, q: char, s: &str| {
            let plain = !s.contains('\\') && t == format!("{prefix}{q}{s}{q}");
            Name::new(
                s.replace(&format!("{q}{q}"), &q.to_string()),
                NameKind::Generated { certain: plain },
            )
        };
        match &v.value {
            Value::SingleQuotedString(s) => return literal("", '\'', s),
            Value::DoubleQuotedString(s) => return literal("", '"', s),
            Value::NationalStringLiteral(s) => return literal("N", '\'', s),
            _ => {}
        }
    }
    match text {
        // Numbers, NULL, TRUE/FALSE, hex and bit literals and every other expression: the text
        // as written, parentheses included.
        Some(t) => Name::new(
            t,
            NameKind::Generated {
                certain: !has_comment(t),
            },
        ),
        None => uncertain(expr, text),
    }
}

fn uncertain(expr: &Expr, text: Option<&str>) -> Name {
    Name::new(
        text.map(str::to_string).unwrap_or_else(|| expr.to_string()),
        NameKind::Generated { certain: false },
    )
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
        assert_eq!(name_of("(total + 1)"), g("(total + 1)"));
        assert_eq!(name_of("((id))"), Name::new("id", NameKind::Column));
        assert_eq!(name_of("+total"), Name::new("total", NameKind::Column));
        assert_eq!(name_of("-total"), g("-total"));
        assert_eq!(name_of("NOT id"), g("NOT id"));
        assert_eq!(name_of("TRUE"), g("TRUE"));
        assert_eq!(name_of("0x41"), g("0x41"));
        assert_eq!(name_of("X'41'"), g("X'41'"));
        assert_eq!(name_of("N'n'"), g("n"));
        assert_eq!(name_of("\"dq\""), g("dq"));
        assert!(!name_of("'a' 'b'").certain());
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
