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
  `name` VARCHAR(64) NULL DEFAULT NULL,
  `definition` MEDIUMTEXT NOT NULL,
  `targets` VARCHAR(255) NULL DEFAULT NULL,
  `enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `rebuild` TINYINT(1) NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uq_lazyview_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

const CREATE_STATE: &str = "CREATE TABLE IF NOT EXISTS `_lazyview_state` (
  `name` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
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
  `server_identity` VARCHAR(255) NULL,
  `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// Columns added after the first release: (table, column, definition). Applied if missing.
const MIGRATIONS: &[(&str, &str, &str)] = &[
    (DEFS, "name", "VARCHAR(64) NULL DEFAULT NULL AFTER `id`"),
    (
        DEFS,
        "targets",
        "VARCHAR(255) NULL DEFAULT NULL AFTER `definition`",
    ),
    (DEFS, "enabled", "TINYINT(1) NOT NULL DEFAULT 1"),
    (
        DEFS,
        "rebuild",
        "TINYINT(1) NOT NULL DEFAULT 0 AFTER `enabled`",
    ),
    (
        DEFS,
        "created_at",
        "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ),
    (
        DEFS,
        "updated_at",
        "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    ),
    (STATE, "detail", "VARCHAR(255) NULL AFTER `mode`"),
    (
        STATE,
        "managed",
        "TINYINT(1) NOT NULL DEFAULT 0 AFTER `state`",
    ),
    (
        STATUS,
        "epoch",
        "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `binlog_pos`",
    ),
    (STATUS, "server_identity", "VARCHAR(255) NULL AFTER `epoch`"),
];

/// One-stop status: every definition with its per-target state.
const CREATE_OVERVIEW: &str = "CREATE OR REPLACE SQL SECURITY INVOKER VIEW `_lazyview_overview` AS
SELECT d.`id`, COALESCE(d.`name`, s.`name`, CONCAT('#', d.`id`)) AS `name`, s.`target`, d.`enabled`, s.`state`, s.`mode`,
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
            conn.query_drop(format!("ALTER TABLE `{table}` ADD COLUMN `{col}` {def}"))
                .await?;
        }
    }
    // State rows are keyed by the view's table name, which the server compares case-sensitively
    // (lower_case_table_names=0): `Lz_Case` and `lz_case` must not share a row.
    let collation: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(COLLATION_NAME AS CHAR) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_lazyview_state' AND COLUMN_NAME = 'name'",
            (schema,),
        )
        .await?;
    if collation.flatten().as_deref() != Some("utf8mb4_bin") {
        conn.query_drop(
            "ALTER TABLE `_lazyview_state` MODIFY `name` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
        )
        .await?;
    }
    let idx: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_lazyview' AND INDEX_NAME = 'uq_lazyview_name'",
            (schema,),
        )
        .await?;
    if idx == Some(0) {
        conn.query_drop("ALTER TABLE `_lazyview` ADD UNIQUE KEY `uq_lazyview_name` (`name`)")
            .await?;
    }
    // Recreate the convenience view only when it is missing or outdated (avoids DDL per start).
    let current: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(VIEW_DEFINITION AS CHAR) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_lazyview_overview'",
            (schema,),
        )
        .await?;
    let up_to_date = current
        .flatten()
        .map(|d| d.contains("`d`.`name`") || d.contains("d.name"))
        .unwrap_or(false);
    if !up_to_date {
        // A convenience only: never fatal.
        if let Err(e) = conn.query_drop(CREATE_OVERVIEW).await {
            tracing::warn!("cannot create _lazyview_overview: {e}");
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct DefRow {
    pub id: u64,
    /// Optional view name; with a name, `definition` may be a bare SELECT.
    pub name: Option<String>,
    pub definition: String,
    pub targets: Vec<String>,
    pub enabled: bool,
    /// `rebuild = 1`: rebuild the view once (cleared by lazyview when the new table is live).
    pub rebuild: bool,
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

impl DefRow {
    /// The statement to plan: `definition` as written, or `CREATE VIEW <name> AS <definition>`
    /// when a name is given and the definition is a bare SELECT.
    pub fn effective_definition(&self) -> String {
        let d = self.definition.trim();
        let starts_create = d
            .split_whitespace()
            .next()
            .map(|w| w.eq_ignore_ascii_case("CREATE"))
            .unwrap_or(false);
        match &self.name {
            Some(n) if !starts_create => {
                format!("CREATE VIEW `{}` AS {d}", n.replace('`', "``"))
            }
            _ => d.to_string(),
        }
    }
}

/// Records the view name of a `CREATE VIEW` definition in `name` (filled in for rows inserted
/// without one, updated when the definition renames the view). Fails with ER 1062 when another
/// row uses the name.
pub async fn set_name(conn: &mut Conn, id: u64, name: &str) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview` SET `name` = ? WHERE `id` = ? AND NOT (`name` <=> ?)",
        (name, id, name),
    )
    .await?;
    Ok(())
}

/// Clears the `rebuild` request of definition `id` (its rebuilt table is live). Leaves
/// `updated_at` alone: the definition did not change.
pub async fn clear_rebuild(conn: &mut Conn, id: u64) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview` SET `rebuild` = 0, `updated_at` = `updated_at` WHERE `id` = ? AND `rebuild` <> 0",
        (id,),
    )
    .await?;
    Ok(())
}

/// The id and the name of the `_lazyview` row named `name` (the key ignores letter case).
pub async fn definition_named(conn: &mut Conn, name: &str) -> Result<Option<(u64, String)>> {
    Ok(conn
        .exec_first(
            "SELECT `id`, CAST(`name` AS CHAR) FROM `_lazyview` WHERE `name` = ?",
            (name,),
        )
        .await?)
}

pub async fn load_definitions(conn: &mut Conn) -> Result<Vec<DefRow>> {
    let rows: Vec<(u64, Option<String>, String, Option<String>, i64, i64)> = conn
        .query("SELECT `id`, CAST(`name` AS CHAR), CAST(`definition` AS CHAR), CAST(`targets` AS CHAR), `enabled`, `rebuild` FROM `_lazyview` ORDER BY `id`")
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, definition, targets, enabled, rebuild)| DefRow {
            id,
            name: name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty()),
            definition,
            targets: parse_targets(targets.as_deref()),
            enabled: enabled != 0,
            rebuild: rebuild != 0,
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
        .map(|(n, t, id, hash, state, managed)| {
            (
                (n, t),
                StateRow {
                    definition_id: id,
                    hash,
                    state,
                    managed: managed != 0,
                },
            )
        })
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

/// Puts every state row into `error` with `reason` (lazyview stops because the views can no longer
/// be kept up to date, see `Engine::mark_unusable`).
pub async fn mark_all_error(conn: &mut Conn, reason: &str) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview_state` SET `state` = 'error', `last_error` = ?",
        (reason,),
    )
    .await?;
    Ok(())
}

/// Removes leftover error rows of a definition under other names (e.g. `#7` before it parsed).
pub async fn delete_stale_state(
    conn: &mut Conn,
    definition_id: u64,
    keep_name: &str,
) -> Result<()> {
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

/// `(name, target)` of every state row.
pub async fn state_keys(conn: &mut Conn) -> Result<Vec<(String, String)>> {
    Ok(conn
        .query("SELECT CAST(`name` AS CHAR), CAST(`target` AS CHAR) FROM `_lazyview_state`")
        .await?)
}

pub async fn delete_state(conn: &mut Conn, name: &str, target: &str) -> Result<()> {
    conn.exec_drop(
        "DELETE FROM `_lazyview_state` WHERE `name` = ? AND `target` = ?",
        (name, target),
    )
    .await?;
    Ok(())
}

pub async fn load_position(conn: &mut Conn, instance: &str) -> Result<Option<Position>> {
    let row: Option<(String, u64)> = conn
        .exec_first(
            "SELECT `binlog_file`, `binlog_pos` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance,),
        )
        .await?;
    Ok(row
        .filter(|(f, _)| !f.is_empty())
        .map(|(file, pos)| Position { file, pos }))
}

/// Identity of the server the saved position belongs to (see `engine::server_identity`).
pub async fn load_identity(conn: &mut Conn, instance: &str) -> Result<Option<String>> {
    let row: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(`server_identity` AS CHAR) FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance,),
        )
        .await?;
    Ok(row.flatten())
}

pub async fn save_identity(conn: &mut Conn, instance: &str, identity: &str) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview_status` SET `server_identity` = ? WHERE `instance_id` = ?",
        (identity, instance),
    )
    .await?;
    Ok(())
}

/// Saves the position if `epoch` is still current; fails when another instance took over.
pub async fn save_position(
    conn: &mut Conn,
    instance: &str,
    p: &Position,
    epoch: u64,
) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview_status` SET `binlog_file` = ?, `binlog_pos` = ? WHERE `instance_id` = ? AND `epoch` = ?",
        (&p.file, p.pos, instance, epoch),
    )
    .await?;
    if conn.affected_rows() == 0 {
        let cur: Option<u64> = conn
            .exec_first(
                "SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?",
                (instance,),
            )
            .await?;
        if cur != Some(epoch) {
            anyhow::bail!(
                "another lazyview instance took over (epoch {cur:?}, ours {epoch}); stopping"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_definition() {
        let row = |name: Option<&str>, def: &str| DefRow {
            id: 1,
            name: name.map(str::to_string),
            definition: def.to_string(),
            targets: vec![],
            enabled: true,
            rebuild: false,
        };
        assert_eq!(
            row(Some("v"), " SELECT 1 ").effective_definition(),
            "CREATE VIEW `v` AS SELECT 1"
        );
        assert_eq!(
            row(None, "CREATE VIEW x AS SELECT 1").effective_definition(),
            "CREATE VIEW x AS SELECT 1"
        );
        assert_eq!(
            row(Some("x"), "create view x as select 1").effective_definition(),
            "create view x as select 1"
        );
    }

    #[test]
    fn targets() {
        assert_eq!(parse_targets(None), vec!["source"]);
        assert_eq!(parse_targets(Some(" ")), vec!["source"]);
        assert_eq!(parse_targets(Some("c, b,c")), vec!["b", "c"]);
    }
}
