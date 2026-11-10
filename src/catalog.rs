//! Table metadata from `information_schema`, needed to resolve columns and decode binlog rows.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
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
    let Some(columns) = load_columns(conn, schema, name, mariadb, default_collations).await? else {
        return Ok(None);
    };
    let pk_names = load_pk(conn, schema, name).await?;
    let unique_keys = load_unique_keys(conn, schema, name, &columns).await?;
    let pk = pk_names
        .iter()
        .filter_map(|n| columns.iter().position(|c| &c.name == n))
        .collect();
    let cascades = load_foreign_keys(conn, schema, name, &columns).await?;
    let (is_view, view_bases) = load_view_bases(conn, schema, name).await?;
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

/// Columns from `information_schema.COLUMNS` in `ORDINAL_POSITION` order
/// (`None`: the table does not exist), remembering each charset's collation of
/// `CONVERT(... USING charset)` in `default_collations`.
async fn load_columns(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    mariadb: bool,
    default_collations: &mut HashMap<String, Option<String>>,
) -> Result<Option<Vec<Column>>> {
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
    for (_, _, _, cs, ..) in &rows {
        if let Some(cs) = cs.as_deref().map(str::to_ascii_lowercase) {
            if let Entry::Vacant(e) = default_collations.entry(cs) {
                let co = default_collation(conn, e.key()).await?;
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
    Ok(Some(columns))
}

/// PRIMARY key column names in `ORDINAL_POSITION` order.
async fn load_pk(conn: &mut Conn, schema: &str, name: &str) -> Result<Vec<String>> {
    let pk_names: Vec<String> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR) FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        .await?;
    Ok(pk_names)
}

/// Other keys that identify a row, from `information_schema.STATISTICS`.
async fn load_unique_keys(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    columns: &[Column],
) -> Result<Vec<Vec<usize>>> {
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
    Ok(unique_keys_from(&key_rows, columns))
}

/// Foreign keys of this (child) table that change child rows, grouped by constraint name from
/// `information_schema.KEY_COLUMN_USAGE` joined with `REFERENTIAL_CONSTRAINTS`.
async fn load_foreign_keys(
    conn: &mut Conn,
    schema: &str,
    name: &str,
    columns: &[Column],
) -> Result<Vec<ForeignKey>> {
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
    Ok(cascades)
}

/// Whether the table is a SQL VIEW and its base tables. They come from the stored definition
/// (VIEW_TABLE_USAGE is missing on MariaDB and MySQL < 8.0.13, so parse VIEWS.VIEW_DEFINITION
/// instead).
async fn load_view_bases(
    conn: &mut Conn,
    schema: &str,
    name: &str,
) -> Result<(bool, Vec<TableRef>)> {
    let view_def: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(VIEW_DEFINITION AS CHAR) FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, name),
        )
        .await?;
    Ok(match view_def {
        Some(def) => (
            true,
            def.as_deref()
                .map(|d| crate::analyze::select_tables(d, schema))
                .unwrap_or_default(),
        ),
        None => (false, Vec::new()),
    })
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
async fn default_collation(conn: &mut Conn, charset: &str) -> Result<Option<String>> {
    if charset.is_empty()
        || !charset
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Ok(None);
    }
    let v = conn
        .query_first::<Option<String>, _>(format!(
            "SELECT CAST(COLLATION(CONVERT(CAST('' AS BINARY) USING {charset})) AS CHAR)"
        ))
        .await
        .with_context(|| format!("default collation of character set {charset}"))?;
    Ok(v.flatten().map(|c| c.to_ascii_lowercase()))
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

/// Shared fixtures for the planner tests, so the same catalog is not pasted once per module:
/// `table` and `typed` build one table, `catalog` is what the block planner's own tests
/// plan against, `wide_catalog` adds the columns a plan has to see by type (typed, opaque,
/// generated) and the view, `linked_catalog` is the small star schema `access` plans its
/// routes over, and `VIEW_CORPUS` holds the view statements both planners are checked against.
#[cfg(test)]
pub(crate) mod test_catalog {
    use super::{Column, TableInfo, TableRef};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// A table of hand-built columns, keyed by its `(schema, name)` reference.
    pub(crate) fn typed(name: &str, cols: Vec<Column>, pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
        let t = TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols,
            pk: pk.to_vec(),
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    }

    /// A table of `(name, data type)` columns; use `typed` where the type itself matters.
    pub(crate) fn table(
        name: &str,
        cols: &[(&str, &str)],
        pk: &[usize],
    ) -> (TableRef, Arc<TableInfo>) {
        typed(
            name,
            cols.iter().map(|(n, ty)| Column::simple(n, ty)).collect(),
            pk,
        )
    }

    /// The tables the block planner's tests plan against.
    pub(crate) fn catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        [
            table(
                "orders",
                &[
                    ("id", "int"),
                    ("customer_id", "int"),
                    ("status", "varchar"),
                    ("total", "decimal"),
                    ("note", "text"),
                ],
                &[0],
            ),
            table(
                "customers",
                &[("id", "int"), ("name", "varchar"), ("region_id", "int")],
                &[0],
            ),
            table("regions", &[("id", "int"), ("name", "varchar")], &[0]),
            table(
                "order_items",
                &[
                    ("order_id", "int"),
                    ("line", "int"),
                    ("qty", "int"),
                    ("price", "decimal"),
                ],
                &[0, 1],
            ),
            table("logs", &[("msg", "varchar")], &[]),
        ]
        .into_iter()
        .collect()
    }

    /// `catalog` plus the tables whose columns only a typed catalog can describe (opaque
    /// types, a collated `varchar`, JSON and BIT), a name with a backtick in it, and a view over
    /// `orders`: the union of what the analyzer's tests need.
    pub(crate) fn wide_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        let opaque = |n: &str, ty: &str| Column::new(n, ty, ty, None, None, Some(16), true);
        let mut cat: HashMap<TableRef, Arc<TableInfo>> = [
            typed(
                "codes",
                vec![
                    Column::simple("code", "varchar"),
                    Column::simple("v", "int"),
                ],
                &[0],
            ),
            typed(
                "refs",
                vec![
                    Column::simple("id", "int"),
                    Column::new(
                        "code",
                        "varchar",
                        "varchar(9)",
                        Some("latin1"),
                        Some("latin1_swedish_ci"),
                        Some(9),
                        false,
                    ),
                    Column::simple("code_int", "int"),
                ],
                &[0],
            ),
            typed(
                "docs",
                vec![
                    Column::simple("id", "int"),
                    Column::new("doc", "json", "json", None, None, None, false),
                    Column::new("flags", "bit", "bit(8)", None, None, None, false),
                ],
                &[0],
            ),
            typed(
                "devices",
                vec![
                    opaque("id", "uuid"),
                    opaque("ip", "inet6"),
                    Column::simple("name", "varchar"),
                    opaque("pos", "point"),
                ],
                &[0],
            ),
            typed(
                "hits",
                vec![
                    Column::simple("n", "int"),
                    opaque("ip", "inet6"),
                    opaque("v4", "inet4"),
                ],
                &[0],
            ),
            table("we`ird", &[("i`d", "int"), ("v", "int")], &[0]),
        ]
        .into_iter()
        .collect();
        cat.extend(catalog());
        let mut paid = (*cat[&("db".to_string(), "orders".to_string())]).clone();
        paid.name = "paid_orders".into();
        paid.pk.clear();
        paid.is_view = true;
        paid.view_bases = vec![("db".into(), "orders".into())];
        cat.insert(("db".into(), "paid_orders".into()), Arc::new(paid));
        cat
    }

    /// The star schema `access` derives its routes from.
    pub(crate) fn linked_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        [
            table(
                "items",
                &[("id", "int"), ("kind_id", "int"), ("owner_id", "int")],
                &[0],
            ),
            table("kinds", &[("id", "int"), ("name", "varchar")], &[0]),
            table(
                "item_values",
                &[("item_id", "int"), ("key_id", "int"), ("val", "int")],
                &[0, 1],
            ),
            table("owners", &[("id", "int"), ("name", "varchar")], &[0]),
            table(
                "item_links",
                &[("child_id", "int"), ("parent_id", "int")],
                &[0, 1],
            ),
        ]
        .into_iter()
        .collect()
    }

    /// The view statements of the planner tests, planned identically by both planners except for
    /// what the analyzer's differential test lists as improved. Both test modules iterate it, so
    /// a statement that moves stays in the corpus instead of silently leaving it.
    pub(crate) const VIEW_CORPUS: &[&str] = &[
    "CREATE VIEW v AS {sql}",
    "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `db`.`v1` (a, b) AS SELECT id, status FROM orders WITH CASCADED CHECK OPTION;",
    "CREATE VIEW `v` AS ",
    "CREATE VIEW `lz_insert` AS INSERT INTO orders VALUES (9,'z',1)",
    "CREATE VIEW `v` AS UPDATE orders SET a = 1",
    "CREATE VIEW `v` AS DELETE FROM orders",
    "CREATE VIEW `v` AS CREATE TABLE t (a INT)",
    "CREATE VIEW `v` AS DROP TABLE orders",
    "CREATE VIEW `v` AS SELECT id FROM orders; SELECT id FROM orders",
    "CREATE VIEW `v` AS SELECT 1 FROM t; SELECT 2 FROM t;\n SELECT 3 FROM t;",
    "CREATE VIEW v AS SELECT id, @@session.sql_mode AS m FROM t",
    "CREATE VIEW v AS SELECT id, @x AS x FROM t",
    "CREATE VIEW v AS SELECT id FROM t LIMIT 2 ROWS EXAMINED 100",
    "CREATE VIEW v AS\\r\nSELECT id\\r\nFROM t\\r\nWHERE id > 1;\\r\n",
    "CREATE VIEW v AS SELECT id FROM t;;",
    "CREATE VIEW v AS SELECT id, 'a;b' AS s FROM t -- trailing; comment",
    "CREATE VIEW v AS SELECT `@x` FROM t",
    "CREATE VIEW v AS SELECT 1 garbage garbage",
    "CREATE VIEW v AS SELECT DISTINCT status FROM orders",
    "CREATE VIEW v AS SELECT c.name FROM customers c JOIN regions USING (id) WHERE c.id IN (SELECT customer_id FROM orders WHERE total > 5) ORDER BY region_id LIMIT 3",
    "CREATE VIEW v AS SELECT DISTINCT * FROM orders",
    "CREATE VIEW v AS SELECT DISTINCT o.* FROM orders o",
    "CREATE VIEW v AS SELECT DISTINCT id FROM orders NATURAL JOIN order_items",
    "CREATE VIEW v AS SELECT id FROM orders UNION SELECT * FROM (SELECT id FROM regions) x",
    "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
    "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.customer_id",
    "CREATE VIEW v AS SELECT o.id, l.msg FROM orders o JOIN logs l ON l.msg = o.status",
    "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
    "CREATE VIEW v AS SELECT c.name, COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name",
    "CREATE VIEW v AS SELECT id, status FROM orders",
    "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
    "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.customer_id",
    "CREATE VIEW v AS SELECT id, status FROM orders",
    "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
    "CREATE VIEW v AS SELECT o.id, l.msg FROM orders o JOIN logs l ON l.msg = o.status",
    "CREATE VIEW v AS SELECT c.name, COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name",
    "CREATE VIEW v AS SELECT COUNT(*) AS n FROM orders",
    "CREATE VIEW v AS SELECT msg FROM logs",
    "CREATE VIEW v AS SELECT k.par_id, k.seq, p.label, g.name FROM kids k JOIN pars p ON p.id = k.par_id LEFT JOIN grps g ON g.id = p.grp_id",
    "CREATE VIEW v AS SELECT par_id, seq, qty FROM kids",
    "CREATE VIEW v AS SELECT k.par_id, k.seq, p.label FROM kids k JOIN pars p ON p.grp_id = k.qty",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON k.par_id = p.id WHERE k.qty > 1",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON (p.id = k.par_id)",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND k.qty > 1",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.id > 1",
    "CREATE VIEW v AS SELECT o.id, k.qty, p.name FROM ords o LEFT JOIN kids k ON k.id = o.kid_id LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1",
    "CREATE VIEW v AS SELECT o.id, k.qty, p.name FROM ords o LEFT JOIN kids k ON k.id = o.kid_id LEFT JOIN pars p ON p.id = k.par_id",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k JOIN pars p ON p.id = k.par_id AND p.active = 1",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k, pars p WHERE p.id = k.par_id AND p.active = 1",
    "CREATE VIEW v AS SELECT p.id, p.name, k.qty FROM pars p LEFT JOIN kids k ON k.par_id = p.id AND k.qty > 2",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k JOIN ords o ON o.kid_id = k.id LEFT JOIN pars p ON p.id = o.id",
    "CREATE VIEW v AS SELECT k.id, p.name, q.name AS other FROM kids k LEFT JOIN pars p ON p.id = k.par_id LEFT JOIN pars q ON q.id = k.qty",
    "CREATE VIEW v AS SELECT k.id, p.name, g.name AS grp FROM kids k JOIN pars p ON p.id = k.par_id LEFT JOIN grps g ON g.id = p.grp_id",
    "CREATE VIEW v AS SELECT k.id, p.name, g.name AS grp FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1 LEFT JOIN grps g ON g.id = p.grp_id",
    "CREATE VIEW v AS SELECT p.id, p.name, g.name AS grp FROM pars p LEFT JOIN grps g ON g.id = p.grp_id AND g.name <> ''",
    "CREATE VIEW v AS SELECT c.id, c.name, p.name AS parent FROM nodes c JOIN nodes p ON p.id = c.parent_id",
    "CREATE VIEW v AS SELECT k.par_id, COUNT(*) AS n FROM kids k JOIN gkids g ON g.kid_id = k.id AND g.v > 0 GROUP BY k.par_id",
    "CREATE VIEW v AS SELECT k.par_id, COUNT(*) AS n FROM kids k JOIN gkids g ON g.v = k.qty GROUP BY k.par_id",
    "CREATE VIEW v AS SELECT n.parent_id, COUNT(*) AS n FROM nodes n GROUP BY n.parent_id",
    "CREATE VIEW v AS SELECT a.id, b.id AS b FROM ma a JOIN mb b ON b.id = a.b_id AND a.id = b.a_id",
    "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id WHERE p.id IS NULL OR p.active = 1",
    "CREATE VIEW v AS SELECT (a > 1) || 1 AS v FROM t",
    "CREATE VIEW v AS SELECT a OR 1 AS v FROM t",
    "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT * FROM t_a LEFT JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT * FROM t_a NATURAL JOIN t_b",
    "CREATE VIEW v AS SELECT * FROM t_a RIGHT JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k) JOIN t_c USING (y)",
    "CREATE VIEW v AS SELECT t_a.x AS ax, * FROM t_a JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT * FROM t_a JOIN (SELECT k, y FROM t_b) d USING (k)",
    "CREATE VIEW v AS SELECT t_a.*, y FROM t_a JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT k, x, y FROM t_a LEFT JOIN t_b USING (k)",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 2",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY total * 2 DESC LIMIT 2",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY -total LIMIT 2",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY ABS(total) DESC, id LIMIT 2",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 1 OFFSET 1",
    "CREATE VIEW v AS SELECT customer_id FROM orders GROUP BY customer_id ORDER BY SUM(total) DESC LIMIT 2",
    "CREATE VIEW v AS SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY c.name DESC, o.id LIMIT 2",
    "CREATE VIEW v AS SELECT id, total * 2 AS t FROM orders ORDER BY t DESC LIMIT 2",
    "CREATE VIEW v AS SELECT id, status FROM orders ORDER BY 2 DESC LIMIT 2",
    "CREATE VIEW v AS SELECT id, SUM(total) OVER (PARTITION BY customer_id ORDER BY status) AS s FROM orders",
    "CREATE VIEW v AS SELECT id, customer_id, ROW_NUMBER() OVER w AS rn, SUM(total) OVER w AS run FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)",
    "CREATE VIEW v AS SELECT id, SUM(total) OVER (w ORDER BY id) AS run FROM orders WINDOW w AS (PARTITION BY customer_id)",
    "CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER w AS rn FROM orders WINDOW w AS (ORDER BY id)",
    "CREATE VIEW v AS SELECT id, status FROM orders",
    "CREATE VIEW v AS SELECT a.k, c.k FROM t_a a JOIN t_c c ON c.k = a.k",
    "CREATE VIEW v AS SELECT a.k, c.z AS K FROM t_a a JOIN t_c c ON c.k = a.k",
    "CREATE VIEW v AS SELECT * FROM t_a a JOIN t_c c ON c.k = a.k",
    "CREATE VIEW v AS SELECT a.k, c.k AS ck FROM t_a a JOIN t_c c ON c.k = a.k",
    "CREATE VIEW v (k1, k2) AS SELECT a.k, c.k FROM t_a a JOIN t_c c ON c.k = a.k",
    "CREATE VIEW v AS SELECT * FROM t_a JOIN t_c USING (k, x)",
    "CREATE VIEW v AS SELECT note, a FROM skus",
    "CREATE VIEW v AS SELECT o.id, s.note FROM orders o JOIN skus s ON s.code = o.status",
    "CREATE VIEW v AS SELECT DISTINCT code, note, s FROM skus",
    "CREATE VIEW v AS SELECT DISTINCT a, b, note FROM skus",
    "CREATE VIEW v AS SELECT g, s FROM skus WHERE g > 1",
    "CREATE VIEW v AS SELECT DISTINCT g FROM skus",
    "CREATE VIEW v AS SELECT g, s FROM skus",
    "CREATE VIEW v AS SELECT g FROM skus",
    "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
    "CREATE VIEW v AS SELECT id, status FROM orders WHERE total > 10",
    "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region, i.qty FROM orders o JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id LEFT JOIN order_items i ON i.order_id = o.id",
    "CREATE VIEW v AS SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id",
    "CREATE VIEW v AS SELECT o.id, c.name FROM orders o JOIN customers c ON c.id IN (o.customer_id)",
    "CREATE VIEW v AS SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id IN (c.id)",
    "CREATE VIEW v AS SELECT o.id, c.name FROM orders o, customers c WHERE c.id IN (o.customer_id)",
    "CREATE VIEW v AS SELECT o.id, c.name, COUNT(*) n FROM orders o JOIN customers c ON c.id IN (o.customer_id) GROUP BY o.id, c.name",
    "CREATE VIEW q AS {sql}",
    "CREATE VIEW x AS SELECT 1",
    "CREATE VIEW v (k, idcol) AS SELECT o.customer_id, o.id FROM orders o",
    "CREATE VIEW v AS SELECT id FROM {name}",
    "CREATE VIEW v AS SELECT o.id FROM orders o",
    "CREATE VIEW v AS SELECT o.customer_id AS cust, COUNT(*) n, SUM(i.qty * i.price) amt FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY cust HAVING n > 1",
    "CREATE VIEW v AS SELECT c.id, c.name, COUNT(*) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.id, c.name",
    "CREATE VIEW v AS SELECT DATE(o.note) d, COUNT(*) FROM orders o GROUP BY 1",
    "CREATE VIEW v AS SELECT COUNT(*) FROM orders",
    "CREATE VIEW v AS SELECT DISTINCT status FROM orders",
    "CREATE VIEW v AS SELECT id FROM orders LIMIT 5",
    "CREATE VIEW v AS SELECT id FROM orders UNION SELECT id FROM customers",
    "CREATE VIEW v AS SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
    "CREATE VIEW v AS SELECT id, NOW() FROM orders",
    "CREATE VIEW v AS SELECT o.id FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
    "CREATE VIEW v AS SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
    "CREATE VIEW v (a, b, c) AS SELECT r.*, 1 FROM regions r",
    "CREATE VIEW v (a) AS SELECT id, name FROM regions",
    "CREATE VIEW v AS SELECT * FROM nope",
    "CREATE VIEW v AS SELECT bogus FROM orders",
    "CREATE VIEW v AS SELECT i.order_id, i.line, o.status FROM order_items i JOIN orders o ON o.id = i.order_id",
    "CREATE VIEW v AS SELECT o.id, o.status FROM orders o JOIN customers c ON c.id = o.customer_id",
    "CREATE VIEW v AS SELECT r.id, c.name FROM regions r JOIN customers c ON c.region_id = r.id",
    "CREATE VIEW v (cust, cnt) AS SELECT customer_id, COUNT(*) n FROM orders GROUP BY customer_id HAVING n > 1",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 3",
    "CREATE VIEW v AS SELECT id FROM orders LIMIT 3",
    "CREATE VIEW v AS SELECT id FROM orders ORDER BY total",
    "CREATE VIEW v AS WITH big AS (SELECT id FROM orders WHERE total > 5) SELECT id FROM big",
    "CREATE VIEW v AS SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn, SUM(total) OVER w AS run FROM orders WINDOW w AS (PARTITION BY customer_id, status ORDER BY id)",
    "CREATE VIEW v AS SELECT id, RANK() OVER (ORDER BY total DESC) AS r FROM orders",
    "CREATE VIEW v AS SELECT customer_id, SUM(total), RANK() OVER (ORDER BY SUM(total)) FROM orders GROUP BY customer_id",
    "CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER (PARTITION BY id, status ORDER BY id) AS rn FROM orders",
    "CREATE VIEW v AS SELECT o.id, ROW_NUMBER() OVER (PARTITION BY o.id, c.name ORDER BY o.id) AS rn FROM orders o JOIN customers c ON c.id = o.customer_id",
    "CREATE VIEW v AS SELECT msg, LENGTH(msg) AS n FROM logs WHERE msg <> ''",
    "CREATE VIEW v AS SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id",
    "CREATE VIEW v AS SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY n",
    "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.id IS NULL AND o.total > 0",
    "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE IFNULL(c.name, 'x') = 'x'",
    "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name = 'a' OR o.total > 1",
    "CREATE VIEW v AS SELECT o.id, c.name, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY o.id, c.name",
    "CREATE VIEW v AS SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name LIKE 'a%' AND UPPER(c.name) <> 'B' AND c.region_id IN (1, 2)",
    "CREATE VIEW v AS SELECT o.id, COUNT(i.line) n FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY o.id HAVING COUNT(i.line) = 0",
    "CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code_int = c.code",
    "CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code = c.code",
    "CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code_int = c.v",
    "CREATE VIEW v AS SELECT id FROM orders WHERE MATCH(note) AGAINST ('x')",
    "CREATE VIEW v AS SELECT o.id, c.name FROM orders o CROSS JOIN regions c",
    "CREATE VIEW v AS SELECT o.id, r.name FROM orders o JOIN regions r",
    "CREATE VIEW v AS SELECT doc, COUNT(*) FROM docs GROUP BY doc",
    "CREATE VIEW v AS SELECT JSON_EXTRACT(doc, '$.a') k, COUNT(*) FROM docs GROUP BY k",
    "CREATE VIEW v AS SELECT doc->'$.a' k, COUNT(*) FROM docs GROUP BY k",
    "CREATE VIEW v AS SELECT doc->>'$.a' k, COUNT(*) FROM docs GROUP BY k",
    "CREATE VIEW v AS SELECT flags, COUNT(*) FROM docs GROUP BY flags",
    "CREATE VIEW v AS SELECT p.id, c.name FROM paid_orders p JOIN customers c ON c.id = p.customer_id",
    "CREATE VIEW x__lv_new AS SELECT id FROM orders",
    "CREATE VIEW x__LV_OLD AS SELECT id FROM orders",
    "CREATE VIEW _lv_probe AS SELECT id FROM orders",
    "CREATE VIEW _lazyview_state AS SELECT id FROM orders",
    "CREATE VIEW v AS SELECT name FROM _lazyview_state",
    "CREATE VIEW v AS SELECT id, my_fn(total), app.calc(id) FROM orders WHERE UPPER(status) = 'X'",
    "CREATE VIEW v AS SELECT id, status FROM orders WHERE status = 'a\\'b' OR status LIKE 'x\\_y' OR note = '\\\\n'",
    "CREATE VIEW v AS SELECT CONCAT(status, '\\\\n') k, COUNT(*) FROM orders GROUP BY k",
    "CREATE VIEW v AS\n  SELECT DISTINCT CONCAT(status, '\\\\n')  FROM orders WHERE note = 'it''s' -- tail\n WITH CHECK OPTION;",
    "CREATE VIEW `v``1` (`a``b`, c) AS SELECT `i``d`, w.v FROM `we``ird` w WHERE w.`i``d` > 1",
    "CREATE VIEW v AS SELECT id, name FROM devices",
    "CREATE VIEW v AS SELECT ip, COUNT(*) FROM hits GROUP BY ip",
    "CREATE VIEW v AS SELECT h.n, d.name FROM hits h JOIN devices d ON d.ip = h.ip",
    "CREATE VIEW v AS SELECT n FROM hits WHERE v4 > '10.0.0.0'",
    "CREATE VIEW v AS SELECT n, ip, v4 FROM hits",
    "CREATE VIEW v AS SELECT COUNT(*) FROM hits WHERE ip IS NOT NULL",
    "CREATE VIEW v AS SELECT db.orders.id, db.customers.name, db.orders.total + 1 FROM db.orders JOIN db.customers ON db.customers.id = db.orders.customer_id WHERE db.customers.name <> 'x' AND db.orders.total > 0",
    "CREATE VIEW v AS SELECT db.orders.customer_id c, COUNT(*) FROM orders GROUP BY db.orders.customer_id",
    "CREATE VIEW v (st) AS SELECT DISTINCT status FROM orders",
    "CREATE VIEW v (x, y) AS SELECT id, status AS s FROM orders UNION ALL SELECT id, name FROM customers ORDER BY s LIMIT 2",
    "CREATE VIEW v (a, b) AS SELECT DISTINCT * FROM regions",
    "CREATE VIEW v (a, b, c) AS SELECT DISTINCT r.*, c.id FROM regions r JOIN customers c ON c.region_id = r.id",
    "CREATE VIEW v (a) AS SELECT * FROM (SELECT id, name FROM regions) d",
    "CREATE VIEW v (a, b, c, d) AS SELECT DISTINCT * FROM regions JOIN customers USING (id)",
    "CREATE VIEW v (a) AS SELECT DISTINCT id, status FROM orders",
    "CREATE VIEW v (a) AS SELECT * FROM regions UNION SELECT id, name FROM customers",
    "CREATE VIEW v (a, b, c) AS SELECT DISTINCT * FROM regions",
    "CREATE VIEW v (a, b) AS SELECT id, name FROM regions",
    "CREATE VIEW v AS SELECT id, CONCAT(status,'-',note), CASE WHEN total > 1 THEN 'y' ELSE 'n' END, id+customer_id, IF(total>1,'y','n'), `status`, o.note, (total), 'it''s', total AS t, total /* c */ + 1, total\n  + 2, CONCAT(note, 'é—x'), CONCAT(status, '-', note, '-', status, '-', note, '-', status, '-', note, '-', status) FROM orders o",
    "CREATE VIEW v AS SELECT SQL_NO_CACHE customer_id, COUNT( * ), count(*), SUM(total*2) FROM orders GROUP BY customer_id",
    "CREATE VIEW v AS SELECT r.*, LENGTH( name ) FROM regions r",
    "CREATE VIEW v AS SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders",
    "CREATE VIEW v AS SELECT id /*! , 1 */ FROM items",
    "CREATE VIEW v AS SELECT id FROM items /*!50100 WHERE 1 */",
    "CREATE VIEW v AS SELECT id FROM items /*M! WHERE 1 */",
    "CREATE VIEW v AS SELECT id FROM items /*m!100500 WHERE 1 */",
    "CREATE VIEW v AS SELECT id FROM items WHERE note = '/*!x*/'",
    "CREATE VIEW v AS SELECT id /* a note */ FROM items",
    "CREATE VIEW v AS SELECT id FROM items -- a note",
    "CREATE VIEW v AS SELECT /*+ MAX_EXECUTION_TIME(1000) */ id FROM items",
    ];
}
