//! The MySQL family's answers on the dialect seam: the identifier quoting, moved verbatim from
//! `crate::analyze::vocab`. `std` and `sqlparser` only — no host code, so the file travels with
//! the copy set.

use sqlparser::ast::Ident;

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

/// A backtick-quoted identifier for the AST. The stored value keeps the doubled backticks a name
/// needs inside quotes, exactly as the tokenizer leaves them, so a generated identifier and a
/// parsed one of the same name hold the same value (see [`unquote`], the analyzer's `expr_key`).
pub(crate) fn qident(id: &str) -> Ident {
    Ident::with_quote('`', id.replace('`', "``"))
}

/// A backtick-quoted identifier: the name `` a`b `` renders as `` `a``b` ``.
pub(crate) fn quote_ident(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}
