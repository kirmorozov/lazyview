//! Service metadata tables in the source schema: definitions, per-table state, binlog position.
//! Created and migrated (additively, idempotently) at startup.

use std::collections::HashMap;

use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_async::Conn;
use mysql_async::Params;

use crate::source::{Cursor, Flavor};

pub const DEFS: &str = "_lazyview";
pub const STATE: &str = "_lazyview_state";
pub const STATUS: &str = "_lazyview_status";

/// Name usable in `_lazyview.targets` for the source instance itself.
pub const SOURCE_TARGET: &str = "source";

/// The largest value a `MEDIUMTEXT` column holds whole: 2^24-1 **bytes** (the width
/// [`CREATE_STATUS`] and the `MIGRATIONS` entry give `binlog_cursor`).
const MEDIUMTEXT_BYTES: usize = 16 * 1024 * 1024 - 1;

/// How far below [`MEDIUMTEXT_BYTES`] a cursor still counts as one the column can hold, so that
/// the refusal of [`save_position`] is the message the operator reads rather than the server's
/// own packet error. The value travels inside a statement (`UPDATE … SET binlog_cursor = ?`),
/// and a server caps a statement at its `max_allowed_packet` — 16 MiB is what MariaDB ships with,
/// so a cursor that wanted every last byte of the column could never be sent to such a server.
/// Half a MiB covers the statement around the value and keeps the whole UPDATE under 16 MiB.
const CURSOR_HEADROOM_BYTES: usize = 512 * 1024;

/// The longest cursor text `binlog_cursor` can hold whole, and so the longest one
/// [`save_position`] will hand the server. The MySQL family stores at most the column's bytes
/// and, with `sql_mode` without `STRICT_TRANS_TABLES`, **hands back the first ones of a longer
/// value and says nothing**: a cursor whose `[g=…]` set was cut comes back from [`load_position`]
/// as an unterminated set, which is no cursor at all, and the start refuses to run. That is
/// issue #347 — a 30-minute smoke's `gtid_executed` reached 1,332 intervals, the `TEXT` column
/// of the day kept the first 65,535 of 69,000 bytes, and the first `stop` event found the
/// restarted engine dying on `the saved position "…[g=…" is no cursor this source speaks`.
/// Over the bound a cursor is **refused** (an error, nothing stored), never shortened: the set is
/// what a resume by GTID is checked against on another server (the subset question of
/// [`crate::source::gtid_covers`]), so a save that dropped it would silently disable the resume
/// for exactly the long-lived clusters whose set outgrows a column. Widening the column to
/// `MEDIUMTEXT` buys the room the set really needs; the bound here is the wall behind it, and the
/// round-trip check of [`save_position`] is the one that says the server stored what we wrote.
pub const MAX_CURSOR_BYTES: usize = MEDIUMTEXT_BYTES - CURSOR_HEADROOM_BYTES;

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
  `binlog_cursor` MEDIUMTEXT NULL,
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
    (
        STATUS,
        "binlog_cursor",
        "MEDIUMTEXT NULL AFTER `binlog_pos`",
    ),
    (STATUS, "server_identity", "VARCHAR(255) NULL AFTER `epoch`"),
    (STATE, "route", "VARCHAR(16) NULL AFTER `detail`"),
    (STATE, "access", "TEXT NULL AFTER `route`"),
];

/// One-stop status: every definition with its per-target state.
const CREATE_OVERVIEW: &str = "CREATE OR REPLACE SQL SECURITY INVOKER VIEW `_lazyview_overview` AS
SELECT d.`id`, COALESCE(d.`name`, s.`name`, CONCAT('#', d.`id`)) AS `name`, s.`target`, d.`enabled`, s.`state`, s.`mode`,
       s.`detail`, s.`route`, s.`access`, s.`last_error`, s.`refreshed_at`, d.`updated_at` AS `definition_updated_at`, d.`definition`
FROM `_lazyview` d LEFT JOIN `_lazyview_state` s ON s.`definition_id` = d.`id`";

/// The commit-fence table (`src/fence.rs`) is lazyview's own table too: `setup` creates it and
/// the operations run only checks for it (`verify` below). The DDL text mirrors the private
/// `create_sql` in `src/fence.rs`; that file belongs to the copy set and must not change, so
/// the text is kept in sync here (unit test: `fence_create_sql_matches`).
fn create_fence() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS `{}` (`id` VARCHAR(64) NOT NULL PRIMARY KEY, \
         `n` BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        crate::fence::table().replace('`', "``")
    )
}

/// Tables and columns the operations run needs before it starts. Keep in sync with the
/// `CREATE_*` constants and `MIGRATIONS` above and with `create_fence` (enforced by the unit
/// tests below). The fence table's name is `fence::table()` — checked separately because it is
/// configurable.
const REQUIRED: &[(&str, &[&str])] = &[
    (
        DEFS,
        &[
            "id",
            "name",
            "definition",
            "targets",
            "enabled",
            "rebuild",
            "created_at",
            "updated_at",
        ],
    ),
    (
        STATE,
        &[
            "name",
            "target",
            "definition_id",
            "definition_hash",
            "mode",
            "detail",
            "route",
            "access",
            "state",
            "managed",
            "last_error",
            "refreshed_at",
            "updated_at",
        ],
    ),
    (
        STATUS,
        &[
            "instance_id",
            "binlog_file",
            "binlog_pos",
            "binlog_cursor",
            "epoch",
            "server_identity",
            "updated_at",
        ],
    ),
];

/// The operations run refuses to start when its own tables are missing or older than this
/// build: only `lazyview setup` (the setup account) creates and migrates them. Display names
/// the fix; `main` maps this error to exit code 2.
#[derive(Debug)]
pub struct SetupRequired(pub Vec<String>);

impl std::fmt::Display for SetupRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lazyview's own tables are not ready ({}). Fix: run `lazyview setup` with the \
             setup account, then start the service again",
            self.0.join("; ")
        )
    }
}

impl std::error::Error for SetupRequired {}

/// Read-only pre-flight for the operations run: checks through `information_schema` (SELECT
/// rights only, no DDL) that every table and column this build needs exists. A missing one is
/// a startup error naming the fix. The convenience view `_lazyview_overview` is not required
/// (it is a `setup` nicety).
pub async fn verify(conn: &mut Conn, schema: &str) -> Result<()> {
    let fence = crate::fence::table();
    let mut need: Vec<(&str, &[&str])> = REQUIRED.to_vec();
    need.push((fence, &["id", "n"]));
    let present: Vec<String> = conn
        .exec(
            "SELECT CAST(TABLE_NAME AS CHAR) FROM information_schema.TABLES WHERE \
             TABLE_SCHEMA = ? AND TABLE_NAME IN (?, ?, ?, ?)",
            (schema, DEFS, STATE, STATUS, fence),
        )
        .await?;
    let mut missing: Vec<String> = Vec::new();
    for (table, cols) in &need {
        if !present.iter().any(|t| t == table) {
            missing.push(format!("table `{table}` is missing"));
            continue;
        }
        let have: Vec<String> = conn
            .exec(
                "SELECT CAST(COLUMN_NAME AS CHAR) FROM information_schema.COLUMNS WHERE \
                 TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (schema, *table),
            )
            .await?;
        for col in *cols {
            if !have.iter().any(|c| c == col) {
                missing.push(format!("`{table}`.{col} is missing"));
            }
        }
    }
    // State rows are keyed case-sensitively (`Lz_Case` and `lz_case` are different views):
    // `ensure` fixes the collation, the operations run only checks it.
    if present.iter().any(|t| t == STATE) {
        let collation: Option<Option<String>> = conn
            .exec_first(
                "SELECT CAST(COLLATION_NAME AS CHAR) FROM information_schema.COLUMNS WHERE \
                 TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'name'",
                (schema, STATE),
            )
            .await?;
        if collation.flatten().as_deref() != Some("utf8mb4_bin") {
            missing.push(format!("`{STATE}`.name is not utf8mb4_bin"));
        }
    }
    // The width of the cursor column is a rule, not a nicety: a `TEXT` (or older) column is the
    // #347 truncation, and only the setup run's `ensure` widens it. Asking here names the fix at
    // startup (`SetupRequired`), instead of the operations run dying at its first oversized save.
    if present.iter().any(|t| t == STATUS) {
        let cursor_type: Option<Option<String>> = conn
            .exec_first(
                "SELECT CAST(DATA_TYPE AS CHAR) FROM information_schema.COLUMNS WHERE \
                 TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'binlog_cursor'",
                (schema, STATUS),
            )
            .await?;
        let cursor_type = cursor_type.flatten();
        if cursor_type.as_deref() != Some("mediumtext") {
            missing.push(format!(
                "`{STATUS}`.binlog_cursor is {cursor_type:?}, not mediumtext"
            ));
        }
    }
    if !missing.is_empty() {
        return Err(SetupRequired(missing).into());
    }
    Ok(())
}

pub async fn ensure(conn: &mut Conn, schema: &str) -> Result<()> {
    for ddl in [CREATE_DEFS, CREATE_STATE, CREATE_STATUS] {
        conn.query_drop(ddl).await?;
    }
    conn.query_drop(create_fence()).await?;
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
    // `binlog_cursor` widened to MEDIUMTEXT for the resume-by-GTID plan: the cursor text can carry
    // a whole GTID set (`file:pos[g=…]`), which a few thousand writers could push past VARCHAR(255)
    // and truncate into a wrong resume, and a long-lived cluster pushes past the 65,535 bytes of the
    // `TEXT` this column had until #356 (issue #347: 1,332 intervals of `gtid_executed`, the server
    // kept the first 64 KiB and said nothing). It stays NULL-able because a TEXT/MEDIUMTEXT column
    // takes no DEFAULT (the status row is first inserted without it, in `coord::claim`, and read
    // back as "no cursor yet"). The row is still written once per flush on the same cadence — the
    // payload only grows, from ~30 B to typically <1 KB — so this adds no new writes to the source,
    // and the cursor is metadata that belongs in the control store (`_lazyview_status`), not the
    // source. Idempotent: the ALTER runs only while the column is not already MEDIUMTEXT, so an
    // install that `ensure` created (or widened on an earlier run) does no DDL per start, and an old
    // install that stopped at VARCHAR(255) or `TEXT` is brought to the same shape by this one path —
    // the same `MIGRATIONS`/`ensure` path every earlier column change uses, on the metadata server
    // the status row lives on (`Config::test_meta_dsn`, `dsn` when there is no separate one).
    let cursor_type: Option<Option<String>> = conn
        .exec_first(
            "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_lazyview_status' AND COLUMN_NAME = 'binlog_cursor'",
            (schema,),
        )
        .await?;
    if cursor_type.flatten().as_deref() != Some("mediumtext") {
        conn.query_drop("ALTER TABLE `_lazyview_status` MODIFY `binlog_cursor` MEDIUMTEXT NULL")
            .await?;
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
        .map(|d| (d.contains("`d`.`name`") || d.contains("d.name")) && d.contains("route"))
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
    /// when a name is given and the definition is a bare SELECT. Comments before CREATE belong
    /// to the statement; a leading UTF-8 byte-order mark does not.
    pub fn effective_definition(&self) -> String {
        let d = self
            .definition
            .trim()
            .trim_start_matches('\u{feff}')
            .trim_start();
        match &self.name {
            Some(n) if !crate::analyze::is_create_statement(d) => {
                format!("CREATE VIEW {} AS {d}", crate::analyze::quote_ident(n))
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
    /// The access plan EXPLAIN text written with the state (NULL before the column existed).
    pub access: Option<String>,
}

pub async fn load_state(conn: &mut Conn) -> Result<HashMap<(String, String), StateRow>> {
    let rows: Vec<(String, String, u64, String, String, i64, Option<String>)> = conn
        .query("SELECT `name`, `target`, `definition_id`, `definition_hash`, `state`, `managed`, `access` FROM `_lazyview_state`")
        .await?;
    Ok(rows
        .into_iter()
        .map(|(n, t, id, hash, state, managed, access)| {
            (
                (n, t),
                StateRow {
                    definition_id: id,
                    hash,
                    state,
                    managed: managed != 0,
                    access,
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
    pub route: Option<&'a str>,
    pub access: Option<&'a str>,
    pub state: &'a str,
    pub error: Option<&'a str>,
    /// Marks the table as created by lazyview; never cleared by later updates.
    pub managed: bool,
}

pub async fn set_state(conn: &mut Conn, u: StateUpdate<'_>) -> Result<()> {
    let refreshed = u.state == "ready";
    conn.exec_drop(
        "INSERT INTO `_lazyview_state` (`name`, `target`, `definition_id`, `definition_hash`, `mode`, `detail`, `route`, `access`, `state`, `managed`, `last_error`, `refreshed_at`) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, IF(?, CURRENT_TIMESTAMP(3), NULL)) \
         ON DUPLICATE KEY UPDATE `definition_id` = VALUES(`definition_id`), `definition_hash` = VALUES(`definition_hash`), \
         `mode` = VALUES(`mode`), `detail` = VALUES(`detail`), `route` = VALUES(`route`), `access` = VALUES(`access`), \
         `state` = VALUES(`state`), `managed` = GREATEST(`managed`, VALUES(`managed`)), \
         `last_error` = VALUES(`last_error`), `refreshed_at` = IF(?, CURRENT_TIMESTAMP(3), `refreshed_at`), \
         `updated_at` = CURRENT_TIMESTAMP(3)",
        // A 13-element list: `Params` tuples top out at 12.
        Params::Positional(vec![
            u.name.into(),
            u.target.into(),
            u.definition_id.into(),
            u.hash.into(),
            u.mode.into(),
            u.detail.into(),
            u.route.into(),
            u.access.into(),
            u.state.into(),
            u.managed.into(),
            u.error.into(),
            refreshed.into(),
            refreshed.into(),
        ]),
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

/// Sets (`Some`) or clears (`None`) `last_error` of a `ready` state row without touching its state
/// or `refreshed_at` (a live view whose refreshes fail stays `ready` and keeps being retried).
pub async fn set_last_error(
    conn: &mut Conn,
    name: &str,
    target: &str,
    error: Option<&str>,
) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview_state` SET `last_error` = ? WHERE `name` = ? AND `target` = ? AND `state` = 'ready'",
        (error, name, target),
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

/// Bumps `refreshed_at` of views that were refreshed incrementally. `names` is never empty:
/// the only caller (the engine's flush loop) skips the batch when nothing was refreshed.
pub async fn touch_refreshed(conn: &mut Conn, names: &[String]) -> Result<()> {
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

/// The saved resume point. `binlog_cursor` holds the [`Cursor`]'s text; the two pre-cursor
/// columns are the one-time format bump's fallback (a row written before the cursor exists still
/// has its position there, and the flavor parses it back). Cursor text that does not parse is an
/// error, never a fresh start: `LAZYVIEW_ON_MISSING_BINLOG=fail` is what keeps a wrong one from
/// being quietly rebuilt over.
pub async fn load_position(
    conn: &mut Conn,
    instance: &str,
    flavor: Flavor,
) -> Result<Option<Cursor>> {
    let row: Option<(String, u64, Option<String>)> = conn
        .exec_first(
            "SELECT `binlog_file`, `binlog_pos`, `binlog_cursor` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance,),
        )
        .await?;
    let Some((file, pos, text)) = row else {
        return Ok(None);
    };
    // A NULL cursor (a status row claimed but never saved, or one from before the cursor existed)
    // is "no cursor yet" — the same reading the pre-cursor empty text had; the file/pos columns
    // below carry it.
    let text = text.unwrap_or_default();
    if !text.is_empty() {
        return flavor.parse_cursor(&text).map(Some).with_context(|| {
            format!("the saved position {text:?} is no cursor this source speaks")
        });
    }
    if file.is_empty() {
        return Ok(None);
    }
    Ok(Some(Cursor::from_parts(flavor, file, pos)))
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

/// The decision [`save_position`] makes before it hands the server anything: whether a cursor text
/// of `text_len` bytes fits `binlog_cursor` ([`MAX_CURSOR_BYTES`]). The cursor text is ASCII
/// (`file:pos[g=uuid:interval,…]`, see [`Cursor`]'s `Display`), so bytes and chars are the same and
/// the column's byte limit is the bound. Separate from the save so the wall the engine stops on is
/// visibly the one the column has (unit test: `cursor_column_is_the_mediumtext_the_bound_measures`).
fn cursor_fits(text_len: usize) -> bool {
    text_len <= MAX_CURSOR_BYTES
}

/// The error of a cursor the column cannot hold whole. It names the three things the operator
/// needs to act on: how big the cursor is, what the limit is, and why it grew that big — a
/// pathologically fragmented `gtid_executed`, one interval per gap in the server's GTID stream.
/// Shortening the cursor is not an answer [`save_position`] may take: the set is what a resume on
/// another server is checked against ([`crate::source::gtid_covers`]), so a save that dropped or
/// cut it would silently disable resume-by-GTID for exactly the long-lived clusters whose set
/// outgrows a column ([`MAX_CURSOR_BYTES`], issue #347).
fn cursor_too_long(text_len: usize, gtid: Option<&str>) -> anyhow::Error {
    let (set_bytes, intervals) = match gtid {
        Some(g) => (
            g.len(),
            g.split(',').filter(|s| !s.is_empty()).count().max(1),
        ),
        None => (0, 0),
    };
    anyhow::anyhow!(
        "refusing to save a cursor of {text_len} bytes: `binlog_cursor` holds at most \
         {MAX_CURSOR_BYTES} (a MEDIUMTEXT column), and half a cursor is no cursor — nothing was \
         stored, the row keeps the position it had. The cause is the consumed GTID set: {set_bytes} \
         bytes in {intervals} interval(s), which means the server's `gtid_executed` is \
         pathologically fragmented (a gap between nearly every transaction, from rolled back or \
         empty transactions, or a restored or multi-source history). This is the source's own set, \
         so `lazyview setup` is not the fix — the column is already MEDIUMTEXT: ask why the source \
         reports that many intervals, and consolidate its set"
    )
}

/// Saves the **cheap half** of the position: `binlog_file` and `binlog_pos` only, with no read-back.
///
/// The cursor column is deliberately left alone, so between two [`save_position`] calls the row can
/// hold a cursor *behind* these two columns. That is the shape the apply loop needs: what a cursor
/// costs to send is the size of its `[g=…]` set, and a slow or fragmented meta link must not be able
/// to throttle applying by that many bytes (the CI red of run 45446296665: `chaos_both_sides` on a
/// one-byte-per-packet link never caught up while every flush rewrote a tens-of-kilobytes set). What
/// a lag costs is a restart that re-reads binlog it has already applied — safe because re-applying a
/// change is idempotent (delete-before-insert, and the same rows in the same buckets), which is what
/// `lifecycle_resumes_from_a_cursor_behind_the_applied_position` proves rather than asserts. The
/// columns themselves stay the truth a start can fall back to (a NULL or absent cursor reads them),
/// and they are written with the same `WHERE epoch = ?` as the full save: a write that changed
/// nothing while the position did advance means another instance holds the row.
pub async fn save_position_at(
    conn: &mut Conn,
    instance: &str,
    at: &crate::binlog::Position,
    epoch: u64,
) -> Result<()> {
    conn.exec_drop(
        "UPDATE `_lazyview_status` SET `binlog_file` = ?, `binlog_pos` = ? WHERE `instance_id` = ? AND `epoch` = ?",
        (&at.file, at.pos, instance, epoch),
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

/// Writes the whole cursor — the position **and** its consumed set together — and is the only path
/// that changes `binlog_cursor`, so it is also the only one that needs the byte-exact read-back.
///
/// Saves the position if `epoch` is still current; fails when another instance took over.
///
/// Returns the cursor the row now holds, verified: [`save_position`] refuses a cursor the column
/// cannot hold whole ([`cursor_too_long`], nothing stored) and reads back what the server says it
/// kept, so no server setting — `sql_mode` without strict tables, or a column an old install left
/// narrower than MEDIUMTEXT — can hand back the first bytes of a long cursor in silence, which is
/// how #347 died. A save the server did shorten is an error too, and it leaves no half cursor
/// behind: the column is cleared to NULL, so the next start reads the `binlog_file`/`binlog_pos`
/// the same statement wrote instead of the truncated text it cannot parse. A refused or mismatched
/// save is an error, never a warning: the caller must stop, because a position the row does not
/// actually hold is worse than no save at all.
pub async fn save_position(
    conn: &mut Conn,
    instance: &str,
    c: &Cursor,
    epoch: u64,
) -> Result<Cursor> {
    // `binlog_cursor` is the value a start reads; the two pre-cursor columns stay filled through
    // the bump, so an older build (and the harness) still finds the position there.
    let at = c.to_position();
    let text = c.to_string();
    if !cursor_fits(text.len()) {
        return Err(cursor_too_long(text.len(), c.gtid()));
    }
    conn.exec_drop(
        "UPDATE `_lazyview_status` SET `binlog_file` = ?, `binlog_pos` = ?, `binlog_cursor` = ? WHERE `instance_id` = ? AND `epoch` = ?",
        (&at.file, at.pos, &text, instance, epoch),
    )
    .await?;
    // The byte-exact round-trip, on the same read as the fencing epoch. Length, not content, is
    // what a server that truncates can be caught with: it keeps the *first* bytes of an over-long
    // value, so the stored length is the one thing that cannot survive it, and `CHAR_LENGTH`
    // against `LENGTH` catches the other way a value can come back wrong (a charset mangling —
    // the cursor text is ASCII, so both must be `text.len()`). One extra read per save, which is
    // one per `LAZYVIEW_POSITION_PERSIST_MS` (5 s by default) on the flush path, never the event
    // path — the price of never believing a position the row does not hold.
    let row: Option<(Option<u64>, Option<u64>, u64)> = conn
        .exec_first(
            "SELECT LENGTH(`binlog_cursor`), CHAR_LENGTH(`binlog_cursor`), `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance,),
        )
        .await?;
    let (bytes, chars, cur) = row.context(format!(
        "the status row of instance {instance} is gone: the saved position cannot be verified"
    ))?;
    if cur != epoch {
        anyhow::bail!("another lazyview instance took over (epoch {cur}, ours {epoch}); stopping");
    }
    let want = text.len() as u64;
    if bytes != Some(want) || chars != Some(want) {
        // What the server kept is the *first bytes* of the value — #347's row, which the next
        // start cannot read. Nothing that reads as a cursor is left behind: the column goes back
        // to NULL (the same row, the same epoch guard), so a start from here takes the
        // `binlog_file`/`binlog_pos` the statement did write — a position this run really read —
        // instead of dying on `no cursor this source speaks`. The save still fails: the row does
        // not hold the cursor this run read, and that is what the caller must stop on.
        let cleared = conn
            .exec_drop(
                "UPDATE `_lazyview_status` SET `binlog_cursor` = NULL WHERE `instance_id` = ? AND `epoch` = ?",
                (instance, epoch),
            )
            .await
            .map_err(|e| format!("{e:#}"));
        let left = match cleared {
            Ok(()) => "The cursor column is cleared to NULL, so a start from this row reads the \
                       file and position it can trust."
                .to_string(),
            Err(e) => format!(
                "Clearing the cursor column failed ({e}): a start from this row reads a \
                 truncated cursor until `lazyview setup` widens the column."
            ),
        };
        anyhow::bail!(
            "the server kept {} of the {want} bytes of cursor it was given for instance \
             {instance} (and says they are {} characters): `_lazyview_status`.binlog_cursor does \
             not hold the position this run read — a truncated cursor is the #347 failure, whose \
             next start dies on `no cursor this source speaks`. The column must be MEDIUMTEXT \
             (up to {MAX_CURSOR_BYTES} bytes of cursor): run `lazyview setup` with the setup \
             account, and check the server's `sql_mode` and `max_allowed_packet` — lazyview's own \
             sessions run a sql_mode without `STRICT_TRANS_TABLES` (`dialect/mysql.rs`), so an \
             over-long value is truncated in silence rather than refused. {left}",
            bytes.unwrap_or(0),
            chars.unwrap_or(0),
        );
    }
    Ok(c.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Column names declared in a `CREATE TABLE` constant: lines whose first token is a
    /// backticked identifier.
    fn create_cols(create: &str) -> Vec<String> {
        create
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with('`'))
            .map(|l| {
                let l = &l[1..];
                l[..l.find('`').unwrap()].to_string()
            })
            .collect()
    }

    /// `REQUIRED` covers every column the `CREATE_*` constants and `MIGRATIONS` create, so the
    /// operations run's `verify` cannot pass on a schema `ensure` would consider migrated.
    #[test]
    fn required_covers_create_and_migrations() {
        let creates = [
            (DEFS, CREATE_DEFS),
            (STATE, CREATE_STATE),
            (STATUS, CREATE_STATUS),
        ];
        for (table, cols) in REQUIRED {
            let create = create_cols(creates.iter().find(|(t, _)| t == table).unwrap().1);
            for col in *cols {
                assert!(
                    MIGRATIONS.iter().any(|(t, c, _)| t == table && c == col)
                        || create.iter().any(|c| c == col),
                    "{table}.{col} not created by ensure"
                );
            }
            for (t, col, _) in MIGRATIONS {
                if t == table {
                    assert!(
                        cols.iter().any(|c| c == col),
                        "MIGRATIONS adds {table}.{col} but REQUIRED does not require it"
                    );
                }
            }
        }
    }

    /// `binlog_cursor` is the MEDIUMTEXT whose byte limit [`MAX_CURSOR_BYTES`] stays under (#347):
    /// the bound is only a bound while the column is the one `ensure` creates and migrates to — and
    /// it is only the right bound while it sits *past* the 65,535 bytes the old `TEXT` column
    /// stopped a set at, which is what #356's shed silently accepted.
    #[test]
    fn cursor_column_is_the_mediumtext_the_bound_measures() {
        assert!(
            CREATE_STATUS.contains("`binlog_cursor` MEDIUMTEXT NULL"),
            "CREATE_STATUS no longer creates a MEDIUMTEXT cursor: MAX_CURSOR_BYTES must follow it"
        );
        assert!(
            MIGRATIONS.iter().any(|(t, c, d)| *t == STATUS
                && *c == "binlog_cursor"
                && d.starts_with("MEDIUMTEXT")),
            "the migration that widened the cursor column is gone, or no longer widens it"
        );
        // Checked at compile time, so the bound cannot drift from the column unnoticed: under the
        // MEDIUMTEXT limit (at it, a cursor would reach the server and be truncated), and over the
        // TEXT limit — being wider than the column #347 died on is the whole point.
        const _: () = assert!(
            MAX_CURSOR_BYTES < MEDIUMTEXT_BYTES,
            "the bound must stay under what a MEDIUMTEXT column holds"
        );
        const _: () = assert!(
            MAX_CURSOR_BYTES > 65_535,
            "the bound must stay past the 65,535 bytes a TEXT column held"
        );
    }

    /// The decision [`save_position`] makes about the cursor text, without a server: everything the
    /// `TEXT` column of #347 could not hold is saved whole (a shed is not an option — the set is
    /// what [`crate::source::gtid_covers`] checks a resume against), and only a cursor past the
    /// MEDIUMTEXT bound is refused, with a message that names the size, the limit and the cause.
    #[test]
    fn only_a_cursor_past_the_mediumtext_bound_is_refused() {
        let at = Cursor::from_parts(Flavor::MySql, "binlog.000004".into(), 228_705_009);
        let set = "1ec44047-e72e-11f1-81ab-7ab4f2cf23bf:1-346,".repeat(4);
        assert!(
            cursor_fits(at.clone().with_gtid(Some(set)).to_string().len()),
            "an ordinary set must still ride the save"
        );
        // #347's cursor: one interval per gap, the shape that reached 65,535 bytes and came back cut
        // mid-UUID. It fits MEDIUMTEXT, so it rides the save whole.
        let fragmented = (0..2_000u32)
            .map(|i| {
                format!(
                    "1ec44047-e72e-11f1-81ab-7ab4f2cf23bf:{}-{}",
                    i * 3 + 1,
                    i * 3 + 2
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        assert!(
            fragmented.len() > 65_535,
            "the test set must be one the TEXT column could not hold ({} B)",
            fragmented.len()
        );
        let whole = at.clone().with_gtid(Some(fragmented.clone()));
        assert!(
            cursor_fits(whole.to_string().len()),
            "a set past the old TEXT limit must ride the save whole ({} B)",
            whole.to_string().len()
        );
        assert_eq!(whole.to_string(), format!("{at}[g={fragmented}]"));
        // Past the MEDIUMTEXT bound: refused, and the refusal names the three things to act on.
        let monster = "1ec44047-e72e-11f1-81ab-7ab4f2cf23bf:1-346,".repeat(400_000);
        let big = at.clone().with_gtid(Some(monster.clone()));
        let big_len = big.to_string().len();
        assert!(
            !cursor_fits(big_len),
            "a cursor the column cannot hold whole must never be sent to it"
        );
        let msg = format!("{:#}", cursor_too_long(big_len, big.gtid()));
        let size = big_len.to_string();
        let limit = MAX_CURSOR_BYTES.to_string();
        for names in [
            size.as_str(),
            limit.as_str(),
            "pathologically fragmented",
            "nothing was stored",
        ] {
            assert!(
                msg.contains(names),
                "the refusal {msg:?} does not name {names:?}"
            );
        }
        // The parser still refuses to guess a set closed: a truncated cursor — what the server
        // handed back in #347 — is no cursor, and `load_position` makes that an error.
        assert!(
            Flavor::MySql
                .parse_cursor(&format!("{at}[g={monster}"))
                .is_none(),
            "an unterminated [g=…] set must stay an error"
        );
        let parsed = Flavor::MySql
            .parse_cursor(&at.to_string())
            .expect("the pre-GTID text parses");
        assert_eq!(parsed.to_position(), at.to_position());
        assert_eq!(parsed.gtid(), None);
    }

    /// The fence DDL in `create_fence` mirrors `src/fence.rs`'s private `create_sql` (the file
    /// is copy-set); both texts stay in sync with what `verify` requires.
    #[test]
    fn fence_create_sql_matches() {
        let sql = create_fence();
        assert!(sql.starts_with(&format!(
            "CREATE TABLE IF NOT EXISTS `{}` (",
            crate::fence::table()
        )));
        assert!(sql.contains("`id` VARCHAR(64) NOT NULL PRIMARY KEY"));
        assert!(sql.contains("`n` BIGINT UNSIGNED NOT NULL"));
    }

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
        // G2: comments before CREATE (and mysqldump's versioned comments) belong to a CREATE
        // statement, which stays as written once the engine has recorded its name.
        for def in [
            "-- annotated view\nCREATE VIEW g2_cmt AS SELECT id, num FROM c",
            "/* block note */ CREATE VIEW g2_blk AS SELECT id FROM c",
            "#hash note\nCREATE VIEW g2_hash AS SELECT num FROM c",
            "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50001 VIEW `v` AS SELECT id FROM c */",
        ] {
            assert_eq!(row(Some("v"), def).effective_definition(), def, "{def}");
        }
        assert_eq!(
            row(Some("v"), "-- note\nSELECT 1").effective_definition(),
            "CREATE VIEW `v` AS -- note\nSELECT 1"
        );
        // A leading byte-order mark is dropped.
        assert_eq!(
            row(Some("v"), "\u{feff}CREATE VIEW v AS SELECT 1").effective_definition(),
            "CREATE VIEW v AS SELECT 1"
        );
        assert_eq!(
            row(Some("v"), "\u{feff} SELECT 1").effective_definition(),
            "CREATE VIEW `v` AS SELECT 1"
        );
    }

    #[test]
    fn targets() {
        assert_eq!(parse_targets(None), vec!["source"]);
        assert_eq!(parse_targets(Some(" ")), vec!["source"]);
        assert_eq!(parse_targets(Some("c, b,c")), vec!["b", "c"]);
    }
}
