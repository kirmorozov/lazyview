//! The MySQL family's answers on the dialect seam: the identifier quoting, the function
//! vocabularies, the parameter and coercion renderings and the parse profile (how a definition's
//! text is tokenized and handed to the query parser, and how a rendered select item loses its
//! alias), moved verbatim from `crate::analyze::vocab`, `crate::analyze::columns`,
//! `crate::analyze::expr`, `crate::analyze::parse` and `crate::mv`. `std`, `sqlparser` and
//! `anyhow` only — no host code.
//!
//! Copy set: lazy-wire-fire copies this file together with `src/dialect/mod.rs` (its parent,
//! the [`Family`](super::Family) discriminator) verbatim, as a pair, so the quoting answers and
//! the function name lists the copy-set forwarders in `crate::analyze::vocab` delegate to
//! compile standalone.

use anyhow::anyhow;
use sqlparser::ast::Ident;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{TokenWithSpan, Tokenizer};

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

// The parameter and coercion renderings: what a bound value is written as belongs to the family,
// so a caller that renders a parameter or a character-set coercion asks these instead of writing
// the shape itself (the write side's JSON unit keys and a source reader's parameter column are such
// callers). `pub`, unlike the answers above with an in-crate caller only, because these two are
// asked from outside this crate as well. The PostgreSQL forms (`$n`, a JSON text with no cast) come
// with the PostgreSQL dialect.

/// How a bound parameter is written: `?`. PostgreSQL numbers its parameters (`$1`, `$2`, …) instead.
pub fn placeholder() -> &'static str {
    "?"
}

/// How a JSON value is bound: the server needs the type, so it is `CAST(? AS JSON)`. A JSON text
/// needs no cast on PostgreSQL, which is why the question belongs to the dialect.
pub fn json_param() -> &'static str {
    "CAST(? AS JSON)"
}

/// How a bound `?` is converted to the character set `cs`, cut to `n` characters when the column
/// has a length: `CONVERT(? USING cs)`, inside `LEFT(…, n)` in that case. The VIRTUAL-column
/// reader (`crate::analyze::columns`) asks this; PostgreSQL has no `CONVERT … USING` form.
pub(crate) fn charset_coercion(cs: &str, n: Option<u64>) -> String {
    match n {
        Some(n) => format!("LEFT(CONVERT(? USING {cs}), {n})"),
        None => format!("CONVERT(? USING {cs})"),
    }
}

// The function vocabularies, moved verbatim from `crate::analyze` (`vocab.rs`, `columns.rs`,
// `expr.rs`): the name groups are the dialect's answer, the reading of a group stays with the
// analyzer (`ValueKind` is `crate::analyze::columns`' own type). PostgreSQL's lists arrive with
// `pgsrc-fn-vocab`; a changed entry on either side is a guard finding.
pub(crate) const NONDETERMINISTIC: &[&str] = &[
    "NOW",
    "SYSDATE",
    "CURDATE",
    "CURTIME",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "UNIX_TIMESTAMP",
    "RAND",
    "UUID",
    "UUID_SHORT",
    "CONNECTION_ID",
    "USER",
    "CURRENT_USER",
    "SESSION_USER",
    "SYSTEM_USER",
    "LAST_INSERT_ID",
    "FOUND_ROWS",
    "ROW_COUNT",
    "SLEEP",
    "BENCHMARK",
    // Sequences, advisory locks, files: a different value or side effect per evaluation.
    "NEXTVAL",
    "LASTVAL",
    "SETVAL",
    "NEXT_VALUE",
    "PREVIOUS_VALUE",
    "GET_LOCK",
    "RELEASE_LOCK",
    "RELEASE_ALL_LOCKS",
    "IS_FREE_LOCK",
    "IS_USED_LOCK",
    "LOAD_FILE",
    "RANDOM_BYTES",
    "SYS_GUID",
    "MASTER_POS_WAIT",
    "SOURCE_POS_WAIT",
    "WAIT_FOR_EXECUTED_GTID_SET",
    "MASTER_GTID_WAIT",
];

/// Builtins that are NULL whenever one of their arguments is NULL (strict). Any function not
/// listed is taken as able to return a value for NULL arguments (`IF`, `COALESCE`, `CHAR`,
/// `CHARSET`, `COERCIBILITY`, MariaDB `NVL2` / `DECODE_ORACLE`, aggregates, ...), so an
/// expression over the NULL side of a LEFT JOIN gets its guard and a predicate using it keeps
/// NULL-extended rows.
pub(crate) const NULL_STRICT: &[&str] = &[
    // arithmetic
    "ABS",
    "CEIL",
    "CEILING",
    "FLOOR",
    "ROUND",
    "TRUNCATE",
    "MOD",
    "POW",
    "POWER",
    "SQRT",
    "EXP",
    "LN",
    "LOG",
    "LOG2",
    "LOG10",
    "SIGN",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "ATAN2",
    "COT",
    "DEGREES",
    "RADIANS",
    "CRC32",
    "CONV",
    "BIN",
    "OCT",
    "HEX",
    "UNHEX",
    "GREATEST",
    "LEAST",
    "DIV",
    // strings
    "UPPER",
    "UCASE",
    "LOWER",
    "LCASE",
    "LENGTH",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "OCTET_LENGTH",
    "BIT_LENGTH",
    "CONCAT",
    "SUBSTRING",
    "SUBSTR",
    "MID",
    "LEFT",
    "RIGHT",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "REVERSE",
    "REPEAT",
    "LPAD",
    "RPAD",
    "INSTR",
    "LOCATE",
    "POSITION",
    "SUBSTRING_INDEX",
    "ASCII",
    "ORD",
    "SPACE",
    "STRCMP",
    "SOUNDEX",
    "INSERT",
    "MD5",
    "SHA",
    "SHA1",
    "TO_BASE64",
    "FROM_BASE64",
    "COMPRESS",
    "UNCOMPRESS",
    "WEIGHT_STRING",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REGEXP_INSTR",
    "REGEXP_LIKE",
    "BINARY",
    "CONVERT",
    "CAST",
    // date and time
    "DATE",
    "TIME",
    "YEAR",
    "MONTH",
    "DAY",
    "DAYOFMONTH",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "WEEKDAY",
    "QUARTER",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MICROSECOND",
    "DAYNAME",
    "MONTHNAME",
    "LAST_DAY",
    "TO_DAYS",
    "TO_SECONDS",
    "FROM_DAYS",
    "FROM_UNIXTIME",
    "UNIX_TIMESTAMP",
    "STR_TO_DATE",
    "DATE_FORMAT",
    "TIME_FORMAT",
    "DATE_ADD",
    "DATE_SUB",
    "ADDDATE",
    "SUBDATE",
    "ADDTIME",
    "SUBTIME",
    "DATEDIFF",
    "TIMEDIFF",
    "TIMESTAMPDIFF",
    "TIMESTAMPADD",
    "TIMESTAMP",
    "MAKEDATE",
    "MAKETIME",
    "SEC_TO_TIME",
    "TIME_TO_SEC",
    "PERIOD_ADD",
    "PERIOD_DIFF",
    "CONVERT_TZ",
    "EXTRACT",
    "GET_FORMAT",
    // network, uuid, json
    "INET_ATON",
    "INET_NTOA",
    "INET6_ATON",
    "INET6_NTOA",
    "UUID_TO_BIN",
    "BIN_TO_UUID",
    "JSON_EXTRACT",
    "JSON_UNQUOTE",
    "JSON_LENGTH",
    "JSON_DEPTH",
    "JSON_TYPE",
    "JSON_VALID",
    "JSON_KEYS",
    "JSON_PRETTY",
    "JSON_STORAGE_SIZE",
    "JSON_VALUE",
];

/// Why `SELECT *` over a USING / NATURAL join is not planned: the server lists each shared
/// column once (first), which is not the per-table expansion of `*`.
pub(crate) const STAR_USING: &str =
    "SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns";

/// The builtins whose value is text whatever their arguments are.
pub(crate) const STR_FUNCTIONS: &[&str] = &[
    "CONCAT",
    "CONCAT_WS",
    "LOWER",
    "LCASE",
    "UPPER",
    "UCASE",
    "SUBSTRING",
    "SUBSTR",
    "LEFT",
    "RIGHT",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "LPAD",
    "RPAD",
    "REVERSE",
    "JSON_UNQUOTE",
    "JSON_VALUE",
];

/// The builtins whose value is numeric: the kind of the first argument, kept numeric.
pub(crate) const NUMERIC_FUNCTIONS: &[&str] =
    &["ABS", "ROUND", "TRUNCATE", "FLOOR", "CEIL", "CEILING"];

/// The builtins whose value is a number whatever the arguments' kinds are.
pub(crate) const NUM_FUNCTIONS: &[&str] = &["SIGN"];

/// The builtins whose value has the kind common to all their arguments.
pub(crate) const COMMON_KIND_FUNCTIONS: &[&str] = &["IFNULL", "COALESCE", "GREATEST", "LEAST"];

/// The conditional builtin: the kind common to its result arguments, the two after the
/// condition (an `IF` of any other arity has a value of unknown kind).
pub(crate) const CONDITIONAL_FUNCTIONS: &[&str] = &["IF"];

/// The builtins whose value is a date.
pub(crate) const DATE_FUNCTIONS: &[&str] = &["DATE"];

/// Functions returning JSON values.
pub(crate) const JSON_PRODUCING: &[&str] = &[
    "JSON_EXTRACT",
    "JSON_OBJECT",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "JSON_SET",
    "JSON_INSERT",
    "JSON_REPLACE",
    "JSON_REMOVE",
    "JSON_MERGE",
    "JSON_MERGE_PATCH",
    "JSON_MERGE_PRESERVE",
    "JSON_KEYS",
    "JSON_ARRAY_APPEND",
    "JSON_ARRAY_INSERT",
    "JSON_QUOTE",
    "JSON_QUERY",
];

/// The functions whose value is JSON when all their arguments are (`IFNULL`, `COALESCE`).
pub(crate) const JSON_ALL_ARG_FUNCTIONS: &[&str] = &["IFNULL", "COALESCE"];

/// The functions whose value is JSON when their first argument is (`NULLIF`).
pub(crate) const JSON_FIRST_ARG_FUNCTIONS: &[&str] = &["NULLIF"];

/// The functions whose value is JSON when an argument after the first one is (`IF`, whose
/// first argument is the condition).
pub(crate) const JSON_TAIL_ARG_FUNCTIONS: &[&str] = &["IF"];

// The parse profile, moved verbatim from `crate::analyze::parse::parse_definition`: how the
// definition text is read belongs to the family. `crate::dialect` dispatches on it; the
// PostgreSQL profile (sqlparser's `PostgreSqlDialect`, no MySQL comment expansion) arrives with
// `pgsrc-parse-profile`.

/// The MySQL-family parse profile, one instance shared by [`tokenize_view`] and [`new_parser`].
static PROFILE: MySqlDialect = MySqlDialect {};

/// Tokens of a definition with locations (no unescaping: literals keep their escapes). MySQL and
/// MariaDB execute the content of `/*! ... */`, `/*!NNNNN ... */` and `/*M! ... */`: sqlparser
/// expands the first two unconditionally (whatever the version gate says) and ignores the third,
/// so what lazyview plans could differ from what the server runs. Refuse them anywhere in the
/// text (also inside a string literal: no quote-aware scan that could disagree with the server's
/// escape rules).
pub(crate) fn tokenize_view(sql: &str) -> anyhow::Result<Vec<TokenWithSpan>> {
    if sql.contains("/*!") || sql.to_ascii_uppercase().contains("/*M!") {
        return Err(anyhow!(
            "the definition contains an executable comment (`/*!` or `/*M!`): the server would run its content; write the SQL without it"
        ));
    }
    Tokenizer::new(&PROFILE, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|_| anyhow!("tokenize: the text does not tokenize"))
}

/// A query parser over tokens with locations on the MySQL-family profile: the
/// `Parser::new(&MySqlDialect {})` of the definition parser, so the profile has one home.
pub(crate) fn new_parser(tokens: Vec<TokenWithSpan>) -> Parser<'static> {
    Parser::new(&PROFILE).with_tokens_with_locations(tokens)
}

/// The expression of a rendered select item: without its trailing `AS <alias>`, if any. The
/// backtick/double-quote/bare-alias rule is the family's answer (moved verbatim from
/// `crate::mv`); PostgreSQL's alias forms arrive with `pgsrc-parse-profile`.
pub(crate) fn item_expr(item: &str) -> &str {
    let Some((expr, alias)) = item.rsplit_once(" AS ") else {
        return item;
    };
    let alias = alias.trim();
    let quoted = |o: char, c: char| {
        alias.len() >= 2
            && alias.starts_with(o)
            && alias.ends_with(c)
            && !alias[1..alias.len() - 1]
                .replace(&format!("{c}{c}"), "")
                .contains(c)
    };
    let bare = !alias.is_empty()
        && alias
            .chars()
            .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$');
    if quoted('`', '`') || quoted('"', '"') || bare {
        expr
    } else {
        item
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The renderings a caller on the seam gets: pinned to the exact SQL the servers accept, so a
    /// moved literal cannot quietly rewrite a statement.
    #[test]
    fn parameter_and_coercion_renderings() {
        assert_eq!(placeholder(), "?");
        assert_eq!(json_param(), "CAST(? AS JSON)");
        assert_eq!(
            charset_coercion("utf8mb4", None),
            "CONVERT(? USING utf8mb4)"
        );
        assert_eq!(
            charset_coercion("utf8mb4", Some(12)),
            "LEFT(CONVERT(? USING utf8mb4), 12)"
        );
    }

    /// The expression of a rendered select item: the trailing alias comes off only where it
    /// really is one (moved verbatim with [`item_expr`] from `crate::mv`).
    #[test]
    fn select_item_expressions() {
        assert_eq!(item_expr("MAX(ts) AS mx"), "MAX(ts)");
        assert_eq!(item_expr("MAX(`e`.`ts`) AS `m x`"), "MAX(`e`.`ts`)");
        assert_eq!(item_expr("x AS `a``b`"), "x");
        assert_eq!(item_expr("x AS \"q\""), "x");
        assert_eq!(item_expr("CAST(x AS SIGNED)"), "CAST(x AS SIGNED)");
        assert_eq!(item_expr("'a AS b'"), "'a AS b'");
        assert_eq!(item_expr("`ev`.`ts`"), "`ev`.`ts`");
        assert_eq!(item_expr("x AS `a` + 1"), "x AS `a` + 1");
    }
}
