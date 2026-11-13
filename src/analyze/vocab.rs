use super::*;

pub(crate) use crate::analyzer::AGGREGATES;

/// Placeholder table name used to render "FROM with one table replaced by literal rows".
pub(crate) const SUBST_MARKER: &str = "__lv_subst_marker__";

/// In the lookup template of a table of a semi-join subquery over several tables
/// ([`AliasPlan::semi`]), the place of the subquery's table that is alias `alias` of the plan:
/// that table, or its rows when a lookup substitutes it too.
pub(crate) fn semi_table_marker(alias: usize) -> String {
    format!("__lv_semi_table_{alias}__")
}

// The function vocabularies (NONDETERMINISTIC, NULL_STRICT, STAR_USING) live in the dialect
// (`crate::dialect::mysql`), which travels with the copy set; these are the analyzer's old
// paths, kept for the call sites `dialect-call-sites` closes.
/// The MySQL family's nondeterministic builtins; the answer is the dialect's.
pub(crate) use crate::dialect::mysql::NONDETERMINISTIC;

/// Why `SELECT *` over a USING / NATURAL join is not planned; the answer is the dialect's.
pub(crate) use crate::dialect::mysql::STAR_USING;

/// lazyview's own tables: never valid as view sources or view names.
pub(crate) const METADATA_TABLES: &[&str] = &[
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
pub(crate) fn reserved(table: &str) -> bool {
    let t = table.to_ascii_lowercase();
    RESERVED.get().is_some_and(|(names, prefixes)| {
        names.contains(&t) || prefixes.iter().any(|p| t.starts_with(p.as_str()))
    })
}

// The four quoting answers live in the dialect (`crate::dialect::mysql`), which travels with
// the copy set (src/dialect/mod.rs + src/dialect/mysql.rs, no host code); these are the
// analyzer's old paths, kept so its call sites keep working until `dialect-call-sites`
// closes them.

/// The name a (raw-tokenized) identifier stands for; the answer is the dialect's.
pub(crate) fn unquote(value: &str, quote: Option<char>) -> String {
    crate::dialect::mysql::unquote(value, quote)
}

/// The name an identifier of a parsed AST stands for; the answer is the dialect's.
pub(crate) fn ident_name(i: &Ident) -> String {
    crate::dialect::mysql::ident_name(i)
}

/// A backtick-quoted identifier for the AST; the answer is the dialect's.
pub(crate) fn qident(id: &str) -> Ident {
    crate::dialect::mysql::qident(id)
}

/// A backtick-quoted identifier; the answer is the dialect's.
pub fn quote_ident(id: &str) -> String {
    crate::dialect::mysql::quote_ident(id)
}
