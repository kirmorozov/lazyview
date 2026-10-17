//! Table metadata from `information_schema`, needed to resolve columns and decode binlog rows.

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
    Json,
    /// Values whose binlog and query representations differ (YEAR, FLOAT, DOUBLE);
    /// never used to derive unit keys directly.
    Inexact,
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
}

impl Column {
    pub fn new(name: &str, data_type: &str, column_type: &str, charset: Option<&str>, collation: Option<&str>, mariadb: bool) -> Column {
        let kind = col_kind(data_type, column_type);
        let dt = data_type.to_ascii_lowercase();
        let utf8_like = charset
            .map(|c| matches!(c.to_ascii_lowercase().as_str(), "utf8mb4" | "utf8mb3" | "utf8" | "ascii"))
            .unwrap_or(true);
        let param = match (&kind, dt.as_str()) {
            (ColKind::Unsigned(_), _) => "CAST(? AS UNSIGNED)".to_string(),
            (_, "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint") => "CAST(? AS SIGNED)".to_string(),
            (_, "decimal" | "numeric") => "CAST(? AS DECIMAL(65,30))".to_string(),
            (_, "date") => "CAST(? AS DATE)".to_string(),
            (_, "datetime") => "CAST(? AS DATETIME(6))".to_string(),
            (_, "time") => "CAST(? AS TIME(6))".to_string(),
            (ColKind::Timestamp, _) => "FROM_UNIXTIME(?)".to_string(),
            (ColKind::Binary, _) => "CAST(? AS BINARY)".to_string(),
            (ColKind::Json, _) if !mariadb => "CAST(? AS JSON)".to_string(),
            (ColKind::Plain, "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext") => match (charset, collation) {
                (Some(cs), Some(co)) => format!("CONVERT(CAST(? AS BINARY) USING {cs}) COLLATE {co}"),
                _ => "?".to_string(),
            },
            _ => "?".to_string(),
        };
        let exact = match kind {
            ColKind::Timestamp | ColKind::Json | ColKind::Inexact => false,
            ColKind::Plain => utf8_like,
            _ => true,
        };
        Column { name: name.to_string(), kind, param, exact, invisible: false }
    }

    /// Test helper: a column with default charset handling.
    pub fn simple(name: &str, data_type: &str) -> Column {
        Column::new(name, data_type, data_type, Some("utf8mb4"), Some("utf8mb4_general_ci"), false)
    }

    pub fn exact_key(&self) -> bool {
        self.exact
    }
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    /// Ordinals of primary key columns (empty: no primary key).
    pub pk: Vec<usize>,
    /// Foreign keys of this (child) table whose ON DELETE / ON UPDATE action changes child rows.
    /// Those changes are made by the storage engine and are NOT written to the binlog.
    pub cascades: Vec<ForeignKey>,
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
}

impl Catalog {
    pub fn invalidate(&mut self, t: &TableRef) {
        self.tables.remove(t);
    }

    pub fn clear(&mut self) {
        self.tables.clear();
    }

    pub async fn get(&mut self, conn: &mut Conn, schema: &str, name: &str, mariadb: bool) -> Result<Option<Arc<TableInfo>>> {
        let key = (schema.to_string(), name.to_string());
        if let Some(t) = self.tables.get(&key) {
            return Ok(Some(t.clone()));
        }
        let Some(t) = load_table(conn, schema, name, mariadb).await? else {
            return Ok(None);
        };
        let t = Arc::new(t);
        self.tables.insert(key, t.clone());
        Ok(Some(t))
    }
}

pub async fn load_table(conn: &mut Conn, schema: &str, name: &str, mariadb: bool) -> Result<Option<TableInfo>> {
    let rows: Vec<(String, String, String, Option<String>, Option<String>, String)> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(COLUMN_TYPE AS CHAR), \
             CAST(CHARACTER_SET_NAME AS CHAR), CAST(COLLATION_NAME AS CHAR), CAST(EXTRA AS CHAR) \
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
    let columns: Vec<Column> = rows
        .into_iter()
        .map(|(name, data_type, column_type, cs, co, extra)| Column {
            invisible: extra.to_ascii_uppercase().contains("INVISIBLE"),
            ..Column::new(&name, &data_type, &column_type, cs.as_deref(), co.as_deref(), mariadb)
        })
        .collect();
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
        let Some(ci) = columns.iter().position(|c| c.name == col) else { continue };
        if last.as_deref() != Some(cname.as_str()) {
            cascades.push(ForeignKey { parent: (ps, pt), child_cols: Vec::new(), parent_cols: Vec::new(), on_delete, on_update });
            last = Some(cname);
        }
        let fk = cascades.last_mut().unwrap();
        fk.child_cols.push(ci);
        fk.parent_cols.push(pc);
    }
    Ok(Some(TableInfo {
        schema: schema.to_string(),
        name: name.to_string(),
        columns,
        pk,
        cascades,
    }))
}

pub fn col_kind(data_type: &str, column_type: &str) -> ColKind {
    let dt = data_type.to_ascii_lowercase();
    let ct = column_type.to_ascii_lowercase();
    match dt.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" if ct.contains("unsigned") => {
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
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => ColKind::Binary,
        "json" => ColKind::Json,
        "year" | "float" | "double" | "real" => ColKind::Inexact,
        _ => ColKind::Plain,
    }
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
    fn labels() {
        assert_eq!(parse_labels("enum('a','b''c','d,e')"), vec!["a", "b'c", "d,e"]);
        assert_eq!(col_kind("int", "int(10) unsigned"), ColKind::Unsigned(32));
        assert_eq!(col_kind("INT", "int"), ColKind::Plain);
        assert_eq!(col_kind("timestamp", "timestamp(6)"), ColKind::Timestamp);
    }
}
