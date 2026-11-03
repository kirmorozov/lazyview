//! Commit-order visibility fence for Oracle MySQL without GTIDs.
//!
//! With `binlog_order_commits=ON` the server commits transactions in binlog order, so one
//! autocommit write of our own (a row that always changes, so it is binlogged) returns only
//! after every transaction flushed before it is visible to readers. The caller owns the
//! connection and the instance id and decides what to do when the fence is unavailable; this
//! module only knows the table name, the server variables, and the SQL.
//!
//! Copy set (see `.claude/agents/generic-guard.md`): only `std`, `anyhow` and `mysql_async`; no
//! import from this crate, no `LAZYVIEW_*` environment variable, no logging.

use std::sync::OnceLock;

use anyhow::{bail, Result};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;

/// The fence table name (see [`set_table`]).
static TABLE: OnceLock<String> = OnceLock::new();

/// Sets the fence table name (default `_lazyview_fence`), for a program that embeds this module.
/// Only the first call takes effect; it returns whether this one did. lazyview itself never calls
/// it.
pub fn set_table(name: &str) -> bool {
    TABLE.set(name.to_string()).is_ok()
}

/// The fence table name, see [`set_table`].
pub fn table() -> &'static str {
    match TABLE.get() {
        Some(name) => name,
        None => "_lazyview_fence",
    }
}

/// Whether the server commits in binlog order: `binlog_order_commits` (`@@GLOBAL`) is `ON` or `1`
/// (any case), or Aurora's enhanced binlog is `ACTIVE` (`aurora_enhanced_binlog` status), which
/// orders commits whatever the variable says. `None` means the variable or status is absent.
pub fn ordered(var: Option<&str>, aurora_enhanced: Option<&str>) -> bool {
    let on = |v: &str| {
        let v = v.trim();
        v == "1" || v.eq_ignore_ascii_case("on")
    };
    var.is_some_and(on) || aurora_enhanced.is_some_and(|v| v.trim().eq_ignore_ascii_case("active"))
}

/// Backtick-quotes an identifier (a backtick inside it is doubled).
fn quote(t: &str) -> String {
    format!("`{}`", t.replace('`', "``"))
}

fn create_sql(t: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (`id` VARCHAR(64) NOT NULL PRIMARY KEY, \
         `n` BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        quote(t)
    )
}

fn commit_sql(t: &str) -> String {
    format!(
        "INSERT INTO {} (`id`, `n`) VALUES (?, 1) ON DUPLICATE KEY UPDATE `n` = `n` + 1",
        quote(t)
    )
}

/// Reads the server's commit-order settings on `conn` and creates the fence table when commits
/// are ordered. `Ok(false)`: not ordered, nothing created. The table lives in the default schema
/// of `conn`.
pub async fn probe(conn: &mut Conn) -> Result<bool> {
    let var: Option<Option<String>> = conn
        .query_first("SELECT @@GLOBAL.binlog_order_commits")
        .await?;
    let aurora: Option<(String, String)> = conn
        .query_first("SHOW STATUS LIKE 'aurora_enhanced_binlog'")
        .await?;
    let var = var.flatten();
    let aurora = aurora.map(|(_, v)| v);
    if !ordered(var.as_deref(), aurora.as_deref()) {
        return Ok(false);
    }
    conn.query_drop(create_sql(table())).await?;
    Ok(true)
}

/// Commits one row change for `id` (its own autocommit statement, so the binlog carries it).
/// Errors when the server reports no changed row, which the fence never expects.
pub async fn commit(conn: &mut Conn, id: &str) -> Result<()> {
    conn.exec_drop(commit_sql(table()), (id,)).await?;
    if conn.affected_rows() == 0 {
        bail!("fence write changed no row");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_reads_the_variable_and_aurora_status() {
        assert!(ordered(Some("1"), None));
        assert!(ordered(Some("ON"), None));
        assert!(ordered(Some("on"), None));
        assert!(!ordered(Some("0"), None));
        assert!(!ordered(Some("OFF"), None));
        assert!(!ordered(None, None));
        assert!(ordered(Some("OFF"), Some("ACTIVE")));
        assert!(ordered(None, Some("active")));
        assert!(!ordered(Some("OFF"), Some("INACTIVE")));
    }

    #[test]
    fn default_table_name() {
        assert_eq!(table(), "_lazyview_fence");
    }

    #[test]
    fn sql_text_quotes_the_table() {
        assert_eq!(
            create_sql("_lazyview_fence"),
            "CREATE TABLE IF NOT EXISTS `_lazyview_fence` (`id` VARCHAR(64) NOT NULL PRIMARY KEY, \
             `n` BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        assert_eq!(
            commit_sql("_lazyview_fence"),
            "INSERT INTO `_lazyview_fence` (`id`, `n`) VALUES (?, 1) \
             ON DUPLICATE KEY UPDATE `n` = `n` + 1"
        );
        assert_eq!(quote("a`b"), "`a``b`");
        assert!(commit_sql("a`b").starts_with("INSERT INTO `a``b` "));
    }
}
