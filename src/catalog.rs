//! Table metadata from `information_schema`, needed to resolve columns and decode binlog rows.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use mysql_async::prelude::*;
use mysql_async::Conn;

/// How a binlog value of this column must be normalized / bound back as a parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum ColKind {
    Plain,
    /// Unsigned integer of the given width in bits.
    Unsigned(u32),
    Enum(Vec<String>),
    Set(Vec<String>),
    /// TIMESTAMP: binlog carries epoch seconds, bound as `FROM_UNIXTIME(?)`.
    Timestamp,
    Binary,
    /// BIT(n): binlog carries big-endian bytes; normalized to an unsigned integer.
    Bit(u32),
    Json,
    /// Values whose binlog and query representations differ (YEAR, FLOAT, DOUBLE);
    /// never used to derive unit keys directly.
    Inexact,
    /// Types whose binlog bytes cannot be bound back as a parameter at all (MariaDB UUID, INET4,
    /// INET6, spatial types, VECTOR, anything unknown): the server compares them as a different
    /// value, so they can be neither unit keys nor lookup images.
    Opaque,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub kind: ColKind,
    /// Typed placeholder for a binlog value of this column, so literal rows keep the column's
    /// type and collation (and joins against them can use indexes).
    pub param: String,
    /// Whether binlog values of this column equal what the server returns for it in queries.
    pub exact: bool,
    /// INVISIBLE column (excluded from `SELECT *`).
    pub invisible: bool,
    /// Lower-case `DATA_TYPE`.
    pub data_type: String,
    /// Lower-case `COLUMN_TYPE` (length, precision, `unsigned`).
    pub column_type: String,
    pub charset: Option<String>,
    pub collation: Option<String>,
    /// `CHARACTER_OCTET_LENGTH` (declared byte length of fixed-length BINARY(n)).
    pub octet_len: Option<u64>,
    /// `IS_NULLABLE` (a NOT NULL column never holds NULL; NULL-extended rows of a LEFT JOIN
    /// still do).
    pub nullable: bool,
    /// Generated column (`GENERATION_EXPRESSION`): its value follows other columns.
    pub generated: Option<Generated>,
}

/// A generated column's definition.
#[derive(Debug, Clone)]
pub struct Generated {
    pub expr: String,
    /// STORED (in the row and its binlog images) rather than VIRTUAL (computed on read).
    pub stored: bool,
    /// Ordinals of the base columns the expression reads; `None` when the expression could
    /// not be parsed (any column may be read).
    pub deps: Option<Vec<usize>>,
}

/// Coarse type families: equality between columns of different families involves implicit
/// conversion and is not transitive, so it must not merge unit-equality classes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeFamily {
    Integer,
    Decimal,
    Float,
    /// Character data with its charset and collation (equal only within the same collation).
    Text(Option<String>, Option<String>),
    Binary,
    Date,
    DateTime,
    Time,
    Year,
    Bit,
    Json,
    Other(String),
}

impl Column {
    pub fn new(
        name: &str,
        data_type: &str,
        column_type: &str,
        charset: Option<&str>,
        collation: Option<&str>,
        octet_len: Option<u64>,
        mariadb: bool,
    ) -> Column {
        let kind = col_kind(data_type, column_type);
        let dt = data_type.to_ascii_lowercase();
        let utf8_like = charset
            .map(|c| {
                matches!(
                    c.to_ascii_lowercase().as_str(),
                    "utf8mb4" | "utf8mb3" | "utf8" | "ascii"
                )
            })
            .unwrap_or(true);
        let param = match (&kind, dt.as_str()) {
            (ColKind::Unsigned(_), _) => "CAST(? AS UNSIGNED)".to_string(),
            (_, "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint") => {
                "CAST(? AS SIGNED)".to_string()
            }
            (_, "decimal" | "numeric") => "CAST(? AS DECIMAL(65,30))".to_string(),
            (_, "date") => "CAST(? AS DATE)".to_string(),
            (_, "datetime") => "CAST(? AS DATETIME(6))".to_string(),
            (_, "time") => "CAST(? AS TIME(6))".to_string(),
            (ColKind::Timestamp, _) => "FROM_UNIXTIME(?)".to_string(),
            (ColKind::Bit(_), _) => "CAST(? AS UNSIGNED)".to_string(),
            // Fixed-length BINARY(n): keep the declared length so padded values compare equal.
            (ColKind::Binary, "binary") => match octet_len {
                Some(n) => format!("CAST(? AS BINARY({n}))"),
                None => "CAST(? AS BINARY)".to_string(),
            },
            (ColKind::Binary, _) => "CAST(? AS BINARY)".to_string(),
            (ColKind::Json, _) if !mariadb => "CAST(? AS JSON)".to_string(),
            (
                ColKind::Plain,
                "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext",
            ) => match (charset, collation) {
                (Some(cs), Some(co)) => text_param(cs, Some(co)),
                _ => "?".to_string(),
            },
            _ => "?".to_string(),
        };
        let exact = match kind {
            ColKind::Timestamp | ColKind::Json | ColKind::Inexact | ColKind::Opaque => false,
            ColKind::Plain => utf8_like,
            _ => true,
        };
        Column {
            name: name.to_string(),
            kind,
            param,
            exact,
            invisible: false,
            data_type: dt,
            column_type: column_type.to_ascii_lowercase(),
            charset: charset.map(|c| c.to_ascii_lowercase()),
            collation: collation.map(|c| c.to_ascii_lowercase()),
            octet_len,
            nullable: true,
            generated: None,
        }
    }

    pub fn family(&self) -> TypeFamily {
        match self.data_type.as_str() {
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
                TypeFamily::Integer
            }
            "decimal" | "numeric" => TypeFamily::Decimal,
            "float" | "double" | "real" => TypeFamily::Float,
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum"
            | "set" => TypeFamily::Text(self.charset.clone(), self.collation.clone()),
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                TypeFamily::Binary
            }
            "date" => TypeFamily::Date,
            "datetime" | "timestamp" => TypeFamily::DateTime,
            "time" => TypeFamily::Time,
            "year" => TypeFamily::Year,
            "bit" => TypeFamily::Bit,
            "json" => TypeFamily::Json,
            other => TypeFamily::Other(other.to_string()),
        }
    }

    /// Test helper: a column with default charset handling.
    pub fn simple(name: &str, data_type: &str) -> Column {
        Column::new(
            name,
            data_type,
            data_type,
            Some("utf8mb4"),
            Some("utf8mb4_general_ci"),
            None,
            false,
        )
    }

    /// Whether binlog values of this column can stand for its SQL values in unit and lineage
    /// keys. A VIRTUAL generated column has no value in the row images (MySQL and MariaDB log
    /// base columns only), so it never can.
    pub fn exact_key(&self) -> bool {
        self.exact && !self.generated.as_ref().is_some_and(|g| !g.stored)
    }

    /// Drops the `COLLATE` of a string param when the column has `default`, the collation that
    /// `CONVERT(... USING charset)` yields on the server. The param then has the column's own
    /// IMPLICIT coercibility; an EXPLICIT one clashes with any `COLLATE` in the view definition
    /// ("Illegal mix of collations").
    pub fn with_default_collation(mut self, default: Option<&str>) -> Column {
        if let (Some(cs), Some(co), Some(d)) = (&self.charset, &self.collation, default) {
            if co.eq_ignore_ascii_case(d) && self.param.starts_with("CONVERT(") {
                self.param = text_param(cs, None);
            }
        }
        self
    }
}

/// Typed placeholder for a string value: its bytes in the column's charset (and collation).
fn text_param(charset: &str, collation: Option<&str>) -> String {
    match collation {
        Some(co) => format!("CONVERT(CAST(? AS BINARY) USING {charset}) COLLATE {co}"),
        None => format!("CONVERT(CAST(? AS BINARY) USING {charset})"),
    }
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    /// Ordinals of primary key columns (empty: no primary key).
    pub pk: Vec<usize>,
    /// Other keys that identify a row: UNIQUE indexes over NOT NULL columns, whole columns
    /// only (no prefix or functional parts), in index-name order.
    pub unique_keys: Vec<Vec<usize>>,
    /// Foreign keys of this (child) table whose ON DELETE / ON UPDATE action changes child rows.
    /// Those changes are made by the storage engine and are NOT written to the binlog.
    pub cascades: Vec<ForeignKey>,
    /// A SQL VIEW rather than a base table (it has no binlog rows of its own).
    pub is_view: bool,
    /// For views: the tables named in the view's definition (may include other views).
    pub view_bases: Vec<TableRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkAction {
    Restrict,
    Cascade,
    SetNull,
}

impl FkAction {
    fn parse(rule: &str) -> FkAction {
        match rule.to_ascii_uppercase().as_str() {
            "CASCADE" => FkAction::Cascade,
            "SET NULL" | "SET DEFAULT" => FkAction::SetNull,
            _ => FkAction::Restrict,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub parent: TableRef,
    /// Child column ordinals, in constraint order.
    pub child_cols: Vec<usize>,
    /// Referenced parent column names, in constraint order.
    pub parent_cols: Vec<String>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

impl TableInfo {
    /// Every key that identifies a row: the primary key first, then the unique keys.
    pub fn keys(&self) -> impl Iterator<Item = &[usize]> + '_ {
        std::iter::once(self.pk.as_slice())
            .filter(|k| !k.is_empty())
            .chain(self.unique_keys.iter().map(Vec::as_slice))
    }

    /// The shortest key that identifies a row (the primary key when there is one).
    pub fn row_key(&self) -> Option<&[usize]> {
        if !self.pk.is_empty() {
            return Some(&self.pk);
        }
        self.unique_keys
            .iter()
            .min_by_key(|k| k.len())
            .map(Vec::as_slice)
    }

    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }
}

pub type TableRef = (String, String);

/// Per-process cache of table metadata. Invalidated on DDL.
#[derive(Default)]
pub struct Catalog {
    tables: HashMap<TableRef, Arc<TableInfo>>,
    /// Charset -> collation of `CONVERT(... USING charset)` (None: unknown).
    default_collations: HashMap<String, Option<String>>,
}

impl Catalog {
    pub fn invalidate(&mut self, t: &TableRef) {
        self.tables.remove(t);
    }

    pub fn clear(&mut self) {
        self.tables.clear();
        self.default_collations.clear();
    }

    pub async fn get(
        &mut self,
        conn: &mut Conn,
        schema: &str,
        name: &str,
        mariadb: bool,
    ) -> Result<Option<Arc<TableInfo>>> {
        let key = (schema.to_string(), name.to_string());
        if let Some(t) = self.tables.get(&key) {
            return Ok(Some(t.clone()));
        }
        let Some(t) = load_table(conn, schema, name, mariadb, &mut self.default_collations).await?
        else {
            return Ok(None);
        };
        let t = Arc::new(t);
        self.tables.insert(key, t.clone());
        Ok(Some(t))
    }
}

pub async fn load_table(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    mariadb: bool,
    default_collations: &mut HashMap<String, Option<String>>,
) -> Result<Option<TableInfo>> {
    type ColRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<u64>,
        String,
        Option<String>,
    );
    let rows: Vec<ColRow> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(COLUMN_TYPE AS CHAR), \
             CAST(CHARACTER_SET_NAME AS CHAR), CAST(COLLATION_NAME AS CHAR), CAST(EXTRA AS CHAR), \
             CHARACTER_OCTET_LENGTH, CAST(IS_NULLABLE AS CHAR), CAST(GENERATION_EXPRESSION AS CHAR) \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let pk_names: Vec<String> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR) FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        .await?;
    for (_, _, _, cs, ..) in &rows {
        if let Some(cs) = cs.as_deref().map(str::to_ascii_lowercase) {
            if let Entry::Vacant(e) = default_collations.entry(cs) {
                let co = default_collation(conn, e.key()).await;
                e.insert(co);
            }
        }
    }
    let mut columns: Vec<Column> = rows
        .into_iter()
        .map(
            |(name, data_type, column_type, cs, co, extra, octets, nullable, gen)| {
                let default = cs
                    .as_deref()
                    .and_then(|c| default_collations.get(&c.to_ascii_lowercase()))
                    .and_then(|d| d.as_deref());
                let extra_uc = extra.to_ascii_uppercase();
                let generated = gen
                    .filter(|g| !g.trim().is_empty() && extra_uc.contains("GENERATED"))
                    .map(|expr| Generated {
                        // MySQL / Percona print it string-escaped (`_utf8mb4\'$.a\'`), MariaDB as SQL.
                        expr: if mariadb {
                            expr
                        } else {
                            unescape_generation_expression(&expr)
                        },
                        stored: extra_uc.contains("STORED"),
                        deps: None,
                    });
                Column {
                    invisible: extra_uc.contains("INVISIBLE"),
                    nullable: nullable.eq_ignore_ascii_case("YES"),
                    generated,
                    ..Column::new(
                        &name,
                        &data_type,
                        &column_type,
                        cs.as_deref(),
                        co.as_deref(),
                        octets,
                        mariadb,
                    )
                    .with_default_collation(default)
                }
            },
        )
        .collect();
    // Base columns of the generated columns (the expression names them, quoted).
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    for c in &mut columns {
        if let Some(g) = &mut c.generated {
            g.deps = generated_deps(&g.expr, &names);
        }
    }
    type KeyRow = (String, u64, Option<String>, Option<u64>, String);
    let key_rows: Vec<KeyRow> = conn
        .exec(
            "SELECT CAST(INDEX_NAME AS CHAR), SEQ_IN_INDEX, CAST(COLUMN_NAME AS CHAR), SUB_PART, \
             CAST(COALESCE(NULLABLE, '') AS CHAR) \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY' \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (schema, name),
        )
        .await?;
    let unique_keys = unique_keys_from(&key_rows, &columns);
    let pk = pk_names
        .iter()
        .filter_map(|n| columns.iter().position(|c| &c.name == n))
        .collect();
    let fk_rows: Vec<(String, String, String, String, String, String, String)> = conn
        .exec(
            "SELECT CAST(k.CONSTRAINT_NAME AS CHAR), CAST(k.COLUMN_NAME AS CHAR), CAST(k.REFERENCED_TABLE_SCHEMA AS CHAR), \
             CAST(k.REFERENCED_TABLE_NAME AS CHAR), CAST(k.REFERENCED_COLUMN_NAME AS CHAR), \
             CAST(r.DELETE_RULE AS CHAR), CAST(r.UPDATE_RULE AS CHAR) \
             FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r \
               ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND r.TABLE_NAME = k.TABLE_NAME \
             WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL \
             ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION",
            (schema, name),
        )
        .await?;
    let mut cascades: Vec<ForeignKey> = Vec::new();
    let mut last: Option<String> = None;
    for (cname, col, ps, pt, pc, del, upd) in fk_rows {
        let (on_delete, on_update) = (FkAction::parse(&del), FkAction::parse(&upd));
        if on_delete == FkAction::Restrict && on_update == FkAction::Restrict {
            continue;
        }
        let Some(ci) = columns.iter().position(|c| c.name == col) else {
            continue;
        };
        if last.as_deref() != Some(cname.as_str()) {
            cascades.push(ForeignKey {
                parent: (ps, pt),
                child_cols: Vec::new(),
                parent_cols: Vec::new(),
                on_delete,
                on_update,
            });
            last = Some(cname);
        }
        let fk = cascades.last_mut().unwrap();
        fk.child_cols.push(ci);
        fk.parent_cols.push(pc);
    }
    // Views: their base tables come from the stored definition (VIEW_TABLE_USAGE is missing on
    // MariaDB and MySQL < 8.0.13, so parse VIEWS.VIEW_DEFINITION instead).
    let view_def: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(VIEW_DEFINITION AS CHAR) FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?;
    let (is_view, view_bases) = match view_def {
        Some(def) => (
            true,
            def.as_deref()
                .map(|d| crate::analyze::select_tables(d, schema))
                .unwrap_or_default(),
        ),
        None => (false, Vec::new()),
    };
    Ok(Some(TableInfo {
        schema: schema.to_string(),
        name: name.to_string(),
        columns,
        pk,
        unique_keys,
        cascades,
        is_view,
        view_bases,
    }))
}

/// UNIQUE keys usable as row keys: whole columns only (no prefix `SUB_PART`, no functional
/// part, which has no column name), every column NOT NULL (a nullable unique column admits
/// several NULL rows). Rows come ordered by index name and sequence.
fn unique_keys_from(
    rows: &[(String, u64, Option<String>, Option<u64>, String)],
    columns: &[Column],
) -> Vec<Vec<usize>> {
    let mut out: Vec<Vec<usize>> = Vec::new();
    let mut current: Option<(String, Option<Vec<usize>>)> = None;
    let flush = |cur: &mut Option<(String, Option<Vec<usize>>)>, out: &mut Vec<Vec<usize>>| {
        if let Some((_, Some(k))) = cur.take() {
            if !k.is_empty() && !out.contains(&k) {
                out.push(k);
            }
        }
    };
    for (index, _, col, sub_part, _) in rows {
        if current.as_ref().map(|(n, _)| n != index).unwrap_or(true) {
            flush(&mut current, &mut out);
            current = Some((index.clone(), Some(Vec::new())));
        }
        let Some((_, key)) = &mut current else {
            continue;
        };
        let ci = col
            .as_deref()
            .and_then(|c| columns.iter().position(|x| x.name == c));
        match (ci, sub_part) {
            (Some(ci), None) if !columns[ci].nullable => {
                if let Some(k) = key {
                    k.push(ci);
                }
            }
            _ => *key = None,
        }
    }
    flush(&mut current, &mut out);
    out
}

/// Ordinals of the columns a generated column's expression reads (parsed with the MySQL
/// dialect; `None` if the text does not parse).
/// MySQL's `GENERATION_EXPRESSION` is the expression escaped like a string literal's content
/// (`\'` for a quote, `\\` for a backslash): the SQL text it stands for.
fn unescape_generation_expression(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.extend(chars.next()),
            c => out.push(c),
        }
    }
    out
}

fn generated_deps(expr: &str, names: &[String]) -> Option<Vec<usize>> {
    use sqlparser::ast::{visit_expressions, Expr};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    use std::ops::ControlFlow;
    let e = Parser::new(&MySqlDialect {})
        .try_with_sql(expr)
        .ok()?
        .parse_expr()
        .ok()?;
    let mut deps = Vec::new();
    let _ = visit_expressions(&e, |x: &Expr| {
        let id = match x {
            Expr::Identifier(i) => Some(i),
            Expr::CompoundIdentifier(parts) => parts.last(),
            _ => None,
        };
        if let Some(i) = id {
            if let Some(c) = names.iter().position(|n| n.eq_ignore_ascii_case(&i.value)) {
                if !deps.contains(&c) {
                    deps.push(c);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Some(deps)
}

/// The collation `CONVERT(... USING charset)` yields on this server (its default for the
/// charset, which MariaDB's `character_set_collations` can override). None if unknown.
async fn default_collation(conn: &mut Conn, charset: &str) -> Option<String> {
    if charset.is_empty()
        || !charset
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return None;
    }
    conn.query_first::<Option<String>, _>(format!(
        "SELECT CAST(COLLATION(CONVERT(CAST('' AS BINARY) USING {charset})) AS CHAR)"
    ))
    .await
    .ok()
    .flatten()
    .flatten()
    .map(|c| c.to_ascii_lowercase())
}

pub fn col_kind(data_type: &str, column_type: &str) -> ColKind {
    let dt = data_type.to_ascii_lowercase();
    let ct = column_type.to_ascii_lowercase();
    match dt.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
            if ct.contains("unsigned") =>
        {
            ColKind::Unsigned(match dt.as_str() {
                "tinyint" => 8,
                "smallint" => 16,
                "mediumint" => 24,
                "int" | "integer" => 32,
                _ => 64,
            })
        }
        "enum" => ColKind::Enum(parse_labels(column_type)),
        "set" => ColKind::Set(parse_labels(column_type)),
        "timestamp" => ColKind::Timestamp,
        "bit" => ColKind::Bit(paren_number(&ct).unwrap_or(1)),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => ColKind::Binary,
        "json" => ColKind::Json,
        "year" | "float" | "double" | "real" => ColKind::Inexact,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
        | "numeric" | "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
        | "date" | "datetime" | "time" => ColKind::Plain,
        _ => ColKind::Opaque,
    }
}

/// `bit(12)` → 12.
fn paren_number(column_type: &str) -> Option<u32> {
    let start = column_type.find('(')? + 1;
    let end = column_type[start..].find(')')? + start;
    column_type[start..end].trim().parse().ok()
}

/// Parses `enum('a','b''c')` into its labels.
fn parse_labels(column_type: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = column_type.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut s = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    s.push('\'');
                }
                Some('\'') | None => break,
                Some('\\') => {
                    if let Some(n) = chars.next() {
                        s.push(n);
                    }
                }
                Some(c) => s.push(c),
            }
        }
        out.push(s);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_keys_and_generated_dependencies() {
        let mut cols = vec![
            Column::simple("id", "int"),
            Column::simple("code", "varchar"),
            Column::simple("email", "varchar"),
            Column::simple("a", "int"),
            Column::simple("b", "int"),
        ];
        for c in &mut cols {
            c.nullable = c.name == "email";
        }
        let row = |i: &str, n: u64, c: Option<&str>, sub: Option<u64>| {
            (i.to_string(), n, c.map(str::to_string), sub, String::new())
        };
        let rows = vec![
            row("u_ab", 1, Some("a"), None),
            row("u_ab", 2, Some("b"), None),
            row("u_code", 1, Some("code"), None),
            row("u_email", 1, Some("email"), None),
            row("u_func", 1, None, None),
            row("u_pref", 1, Some("code"), Some(3)),
            row("u_pref", 2, Some("a"), None),
        ];
        assert_eq!(unique_keys_from(&rows, &cols), vec![vec![3, 4], vec![1]]);
        let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
        assert_eq!(generated_deps("(`a` + `b`)", &names), Some(vec![3, 4]));
        assert_eq!(generated_deps("`a` * 2", &names), Some(vec![3]));
        assert_eq!(
            generated_deps("concat(`code`,_utf8mb4'x')", &names),
            Some(vec![1])
        );
        assert_eq!(generated_deps("((", &names), None);
        // MySQL / Percona print the expression string-escaped (B3; measured on 8.4).
        let mysql = r#"concat(`code`,_latin1\'it\\\'s\',_latin1\'\\\\x\',_latin1\'q"d\')"#;
        assert_eq!(
            unescape_generation_expression(mysql),
            r#"concat(`code`,_latin1'it\'s',_latin1'\\x',_latin1'q"d')"#
        );
        assert_eq!(
            generated_deps(&unescape_generation_expression(mysql), &names),
            Some(vec![1])
        );
        assert_eq!(
            generated_deps(
                &unescape_generation_expression(r"json_unquote(json_extract(`a`,_utf8mb4\'$.a\'))"),
                &names
            ),
            Some(vec![3])
        );
        let mut g = Column::simple("g", "int");
        g.generated = Some(Generated {
            expr: "a".into(),
            stored: false,
            deps: Some(vec![3]),
        });
        assert!(!g.exact_key());
        g.generated.as_mut().unwrap().stored = true;
        assert!(g.exact_key());
    }

    #[test]
    fn labels() {
        assert_eq!(
            parse_labels("enum('a','b''c','d,e')"),
            vec!["a", "b'c", "d,e"]
        );
        assert_eq!(col_kind("int", "int(10) unsigned"), ColKind::Unsigned(32));
        assert_eq!(col_kind("INT", "int"), ColKind::Plain);
        assert_eq!(col_kind("timestamp", "timestamp(6)"), ColKind::Timestamp);
        assert_eq!(col_kind("bit", "bit(12)"), ColKind::Bit(12));
        assert_eq!(col_kind("bit", "bit"), ColKind::Bit(1));
    }

    #[test]
    fn params_and_families() {
        let b = Column::new("b", "binary", "binary(4)", None, None, Some(4), false);
        assert_eq!(b.param, "CAST(? AS BINARY(4))");
        let vb = Column::new("b", "varbinary", "varbinary(4)", None, None, Some(4), false);
        assert_eq!(vb.param, "CAST(? AS BINARY)");
        let bit = Column::new("f", "bit", "bit(3)", None, None, None, false);
        assert_eq!(bit.param, "CAST(? AS UNSIGNED)");
        assert_eq!(bit.family(), TypeFamily::Bit);
        let a = Column::new(
            "a",
            "varchar",
            "varchar(5)",
            Some("utf8mb4"),
            Some("utf8mb4_bin"),
            Some(20),
            false,
        );
        let c = Column::new(
            "c",
            "varchar",
            "varchar(5)",
            Some("utf8mb4"),
            Some("utf8mb4_general_ci"),
            Some(20),
            false,
        );
        assert_ne!(
            a.family(),
            c.family(),
            "different collations are different families"
        );
        let i = Column::simple("i", "int");
        let u = Column::new("u", "bigint", "bigint unsigned", None, None, None, false);
        assert_eq!(i.family(), u.family(), "signedness does not matter");
        assert_ne!(i.family(), a.family());
    }

    #[test]
    fn opaque_types_and_default_collations() {
        for (dt, ct) in [
            ("uuid", "uuid"),
            ("inet4", "inet4"),
            ("inet6", "inet6"),
            ("geometry", "geometry"),
            ("point", "point"),
            ("vector", "vector(3)"),
        ] {
            let c = Column::new("c", dt, ct, None, None, Some(16), true);
            assert_eq!(c.kind, ColKind::Opaque, "{dt}");
            assert!(!c.exact_key(), "{dt}");
            assert_eq!(c.param, "?");
        }
        for dt in ["int", "varchar", "decimal", "date", "datetime", "time"] {
            assert!(Column::simple("c", dt).exact_key(), "{dt}");
        }
        let s = Column::new(
            "s",
            "varchar",
            "varchar(5)",
            Some("utf8mb4"),
            Some("utf8mb4_0900_ai_ci"),
            Some(20),
            false,
        );
        assert_eq!(
            s.param,
            "CONVERT(CAST(? AS BINARY) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci"
        );
        assert_eq!(
            s.clone()
                .with_default_collation(Some("utf8mb4_0900_ai_ci"))
                .param,
            "CONVERT(CAST(? AS BINARY) USING utf8mb4)"
        );
        assert_eq!(
            s.clone()
                .with_default_collation(Some("utf8mb4_general_ci"))
                .param,
            s.param,
            "a non-default collation stays explicit"
        );
        assert_eq!(s.clone().with_default_collation(None).param, s.param);
        let i = Column::simple("i", "int");
        assert_eq!(i.clone().with_default_collation(Some("x")).param, i.param);
        let e = Column::new(
            "e",
            "enum",
            "enum('a')",
            Some("utf8mb4"),
            Some("utf8mb4_general_ci"),
            Some(4),
            false,
        );
        assert_eq!(
            e.with_default_collation(Some("utf8mb4_general_ci")).param,
            "?"
        );
    }
}
