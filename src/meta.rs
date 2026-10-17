//! Service metadata tables in the source schema: definitions, per-table state, binlog position.
//! Created and migrated (additively, idempotently) at startup.

use std::collections::HashMap;

use anyhow::Result;
use mysql_async::prelude::*;
use mysql_async::Conn;

use crate::binlog::Position;

pub const DEFS: &str = "_lazyview";
pub const STATE: &str = "_lazyview_state";
pub const STATUS: &str = "_lazyview_status";

/// Name usable in `_lazyview.targets` for the source instance itself.
pub const SOURCE_TARGET: &str = "source";

const CREATE_DEFS: &str = "CREATE TABLE IF NOT EXISTS `_lazyview` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `definition` MEDIUMTEXT NOT NULL,
  `targets` VARCHAR(255) NULL DEFAULT NULL,
  `enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

const CREATE_STATE: &str = "CREATE TABLE IF NOT EXISTS `_lazyview_state` (
  `name` VARCHAR(64) NOT NULL,
  `target` VARCHAR(64) NOT NULL,
  `definition_id` BIGINT UNSIGNED NOT NULL,
  `definition_hash` CHAR(16) NOT NULL,
  `mode` VARCHAR(16) NOT NULL,
  `detail` VARCHAR(255) NULL,
  `state` VARCHAR(16) NOT NULL,
  `managed` TINYINT(1) NOT NULL DEFAULT 0,
  `last_error` TEXT NULL,
  `refreshed_at` TIMESTAMP(3) NULL,
  `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`name`, `target`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

const CREATE_STATUS: &str = "CREATE TABLE IF NOT EXISTS `_lazyview_status` (
  `instance_id` VARCHAR(64) NOT NULL PRIMARY KEY,
  `binlog_file` VARCHAR(255) NOT NULL,
  `binlog_pos` BIGINT UNSIGNED NOT NULL,
  `epoch` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// Columns added after the first release: (table, column, definition). Applied if missing.
const MIGRATIONS: &[(&str, &str, &str)] = &[
    (DEFS, "targets", "VARCHAR(255) NULL DEFAULT NULL AFTER `definition`"),
    (DEFS, "enabled", "TINYINT(1) NOT NULL DEFAULT 1"),
    (STATE, "detail", "VARCHAR(255) NULL AFTER `mode`"),
    (STATE, "managed", "TINYINT(1) NOT NULL DEFAULT 0 AFTER `state`"),
    (STATUS, "epoch", "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `binlog_pos`"),
];

/// One-stop status: every definition with its per-target state.
const CREATE_OVERVIEW: &str = "CREATE OR REPLACE SQL SECURITY INVOKER VIEW `_lazyview_overview` AS
SELECT d.`id`, COALESCE(s.`name`, CONCAT('#', d.`id`)) AS `name`, s.`target`, d.`enabled`, s.`state`, s.`mode`,
       s.`detail`, s.`last_error`, s.`refreshed_at`, d.`updated_at` AS `definition_updated_at`, d.`definition`
FROM `_lazyview` d LEFT JOIN `_lazyview_state` s ON s.`definition_id` = d.`id`";

pub async fn ensure(conn: &mut Conn, schema: &str) -> Result<()> {
    for ddl in [CREATE_DEFS, CREATE_STATE, CREATE_STATUS] {
        conn.query_drop(ddl).await?;
    }
    for (table, col, def) in MIGRATIONS {
        let n: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (schema, *table, *col),
            )
            .await?;
        if n == Some(0) {
            conn.query_drop(format!("ALTER TABLE `{table}` ADD COLUMN `{col}` {def}")).await?;
        }
    }
    // Only (re)create the convenience view when it is missing, to avoid DDL on every start.
    let has: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_lazyview_overview'",
            (schema,),
        )
        .await?;
    if has == Some(0) {
        conn.query_drop(CREATE_OVERVIEW).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct DefRow {
    pub id: u64,
    pub definition: String,
    pub targets: Vec<String>,
    pub enabled: bool,
}

pub fn parse_targets(s: Option<&str>) -> Vec<String> {
    let mut out: Vec<String> = s
        .unwrap_or("")
        .split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect();
    if out.is_empty() {
        out.push(SOURCE_TARGET.to_string());
    }
    out.sort();
    out.dedup();
    out
}

pub async fn load_definitions(conn: &mut Conn) -> Result<Vec<DefRow>> {
    let rows: Vec<(u64, String, Option<String>, i64)> = conn
        .query("SELECT `id`, CAST(`definition` AS CHAR), CAST(`targets` AS CHAR), `enabled` FROM `_lazyview` ORDER BY `id`")
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, definition, targets, enabled)| DefRow {
            id,
            definition,
            targets: parse_targets(targets.as_deref()),
            enabled: enabled != 0,
        })
        .collect())
}

#[derive(Debug, Clone)]
pub struct StateRow {
    pub definition_id: u64,
    pub hash: String,
    pub state: String,
    /// The table was created by lazyview (and may be replaced or dropped by it).
    pub managed: bool,
}

pub async fn load_state(conn: &mut Conn) -> Result<HashMap<(String, String), StateRow>> {
    let rows: Vec<(String, String, u64, String, String, i64)> = conn
        .query("SELECT `name`, `target`, `definition_id`, `definition_hash`, `state`, `managed` FROM `_lazyview_state`")
        .await?;
    Ok(rows
        .into_iter()
        .map(|(n, t, id, hash, state, managed)| ((n, t), StateRow { definition_id: id, hash, state, managed: managed != 0 }))
        .collect())
}

pub struct StateUpdate<'a> {
    pub name: &'a str,
    pub target: &'a str,
    pub definition_id: u64,
    pub hash: &'a str,
    pub mode: &'a str,
    pub detail: Option<&'a str>,
    pub state: &'a str,
    pub error: Option<&'a str>,
    /// Marks the table as created by lazyview; never cleared by later updates.
    pub managed: bool,
}

pub async fn set_state(conn: &mut Conn, u: StateUpdate<'_>) -> Result<()> {
    let refreshed = u.state == "ready";
    conn.exec_drop(
        "INSERT INTO `_lazyview_state` (`name`, `target`, `definition_id`, `definition_hash`, `mode`, `detail`, `state`, `managed`, `last_error`, `refreshed_at`) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, IF(?, CURRENT_TIMESTAMP(3), NULL)) \
         ON DUPLICATE KEY UPDATE `definition_id` = VALUES(`definition_id`), `definition_hash` = VALUES(`definition_hash`), \
         `mode` = VALUES(`mode`), `detail` = VALUES(`detail`), `state` = VALUES(`state`), `managed` = GREATEST(`managed`, VALUES(`managed`)), \
         `last_error` = VALUES(`last_error`), `refreshed_at` = IF(?, CURRENT_TIMESTAMP(3), `refreshed_at`)",
        (u.name, u.target, u.definition_id, u.hash, u.mode, u.detail, u.state, u.managed, u.error, refreshed, refreshed),
    )
    .await?;
    Ok(())
}

/// Removes leftover error rows of a definition under other names (e.g. `#7` before it parsed).
pub async fn delete_stale_state(conn: &mut Conn, definition_id: u64, keep_name: &str) -> Result<()> {
    conn.exec_drop(
        "DELETE FROM `_lazyview_state` WHERE `definition_id` = ? AND `name` <> ? AND `managed` = 0",
        (definition_id, keep_name),
    )
    .await?;
    Ok(())
}

/// Bumps `refreshed_at` of views that were refreshed incrementally.
pub async fn touch_refreshed(conn: &mut Conn, names: &[String]) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let marks = vec!["?"; names.len()].join(",");
    conn.exec_drop(
        format!("UPDATE `_lazyview_state` SET `refreshed_at` = CURRENT_TIMESTAMP(3) WHERE `state` = 'ready' AND `name` IN ({marks})"),
        names.to_vec(),
    )
    .await?;
    Ok(())
}

pub async fn delete_state(conn: &mut Conn, name: &str, target: &str) -> Result<()> {
    conn.exec_drop("DELETE FROM `_lazyview_state` WHERE `name` = ? AND `target` = ?", (name, target))
        .await?;
    Ok(())
}

pub async fn load_position(conn: &mut Conn, instance: &str) -> Result<Option<Position>> {
    let row: Option<(String, u64)> = conn
        .exec_first("SELECT `binlog_file`, `binlog_pos` FROM `_lazyview_status` WHERE `instance_id` = ?", (instance,))
        .await?;
    Ok(row.filter(|(f, _)| !f.is_empty()).map(|(file, pos)| Position { file, pos }))
}

/// Saves the position if `epoch` is still current; fails when another instance took over.
pub async fn save_position(conn: &mut Conn, instance: &str, p: &Position, epoch: u64) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview_status` SET `binlog_file` = ?, `binlog_pos` = ? WHERE `instance_id` = ? AND `epoch` = ?",
        (&p.file, p.pos, instance, epoch),
    )
    .await?;
    if conn.affected_rows() == 0 {
        let cur: Option<u64> = conn
            .exec_first("SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?", (instance,))
            .await?;
        if cur != Some(epoch) {
            anyhow::bail!("another lazyview instance took over (epoch {cur:?}, ours {epoch}); stopping");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets() {
        assert_eq!(parse_targets(None), vec!["source"]);
        assert_eq!(parse_targets(Some(" ")), vec!["source"]);
        assert_eq!(parse_targets(Some("c, b,c")), vec!["b", "c"]);
    }
}
