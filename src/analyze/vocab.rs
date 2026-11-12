use super::*;

/// Placeholder table name used to render "FROM with one table replaced by literal rows".
pub(crate) const SUBST_MARKER: &str = "__lv_subst_marker__";

/// In the lookup template of a table of a semi-join subquery over several tables
/// ([`AliasPlan::semi`]), the place of the subquery's table that is alias `alias` of the plan:
/// that table, or its rows when a lookup substitutes it too.
pub(crate) fn semi_table_marker(alias: usize) -> String {
    format!("__lv_semi_table_{alias}__")
}

pub(crate) const AGGREGATES: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "GROUP_CONCAT",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

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

/// Why `SELECT *` over a USING / NATURAL join is not planned: the server lists each shared
/// column once (first), which is not the per-table expansion of `*`.
pub(crate) const STAR_USING: &str =
    "SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns";

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

/// SELECT modifiers that may precede the first projection item.
pub(crate) const SELECT_MODIFIERS: &[&str] = &[
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
pub(crate) const PROJECTION_END: &[&str] = &[
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
/// parsed one of the same name hold the same value (see [`unquote`], [`expr_key`]).
pub(crate) fn qident(id: &str) -> Ident {
    Ident::with_quote('`', id.replace('`', "``"))
}

/// A backtick-quoted identifier: the name `` a`b `` renders as `` `a``b` ``.
pub fn quote_ident(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}
