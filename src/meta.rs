//! Service metadata tables in the source schema: definitions, per-table state, binlog position.
//! Created and migrated (additively, idempotently) by the setup run (`lazyview setup`).
//!
//! Two accounts, two schemas (owner 2026-11-15: "we need to separate status table from definition.
//! to limit write scope for the service"): `_lazyview` holds the definitions and is written by the
//! management user alone, so the database's audit log attributes every change of a definition to a
//! person. The service account reads it (`SELECT` only) and writes only lazyview's own status
//! tables — `_lazyview_state`, `_lazyview_status`, the commit fence. `LAZYVIEW_DEFINITIONS_DATABASE`
//! puts the definitions in a schema of their own; unset it is `LAZYVIEW_DATABASE` and the simple
//! setup is unchanged. The service never writes `_lazyview`: the view name is derived from the
//! definition (never stored back) and a rebuild request is acknowledged in
//! `_lazyview_state.rebuild_served`.

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
  `rebuild` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uq_lazyview_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// The per-view, per-target state, upserted by the service. `rebuild_served` holds the
/// `_lazyview.rebuild` value this view and target were last built from: any value above it is a
/// rebuild request, and this table is the only one the service writes (see `serve_rebuild`).
const CREATE_STATE: &str = "CREATE TABLE IF NOT EXISTS `_lazyview_state` (
  `name` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `target` VARCHAR(64) NOT NULL,
  `definition_id` BIGINT UNSIGNED NOT NULL,
  `definition_hash` CHAR(16) NOT NULL,
  `mode` VARCHAR(16) NOT NULL,
  `detail` VARCHAR(255) NULL,
  `state` VARCHAR(16) NOT NULL,
  `managed` TINYINT(1) NOT NULL DEFAULT 0,
  `rebuild_served` BIGINT UNSIGNED NOT NULL DEFAULT 0,
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
        // A counter, not a flag: the request is `UPDATE _lazyview SET rebuild = rebuild + 1` and
        // the service acknowledges it in `_lazyview_state.rebuild_served`. `ensure` also widens an
        // existing TINYINT (see the widening below).
        "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `enabled`",
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
    (
        STATE,
        "rebuild_served",
        "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `managed`",
    ),
];

/// The definitions table, always schema-qualified: in the advanced setup it lives in a schema of
/// its own where the service account has SELECT only, so a name that would resolve through the
/// connection's default schema could read a different table. `defs_schema` is
/// `LAZYVIEW_DEFINITIONS_DATABASE`, or `LAZYVIEW_DATABASE` when unset (the simple setup).
pub(crate) fn defs_table(defs_schema: &str) -> String {
    format!("`{}`.`{}`", defs_schema.replace('`', "``"), DEFS)
}

/// `CREATE_DEFS` with the table name qualified (its only occurrence is the created table).
fn create_defs(defs_schema: &str) -> String {
    CREATE_DEFS.replacen(&format!("`{DEFS}`"), &defs_table(defs_schema), 1)
}

/// One-stop status: every definition with its per-target state. The definitions table is
/// qualified: it may live in the definitions schema, the view itself is created in the service's
/// own schema next to the status tables.
fn create_overview(defs_schema: &str) -> String {
    format!(
        "CREATE OR REPLACE SQL SECURITY INVOKER VIEW `_lazyview_overview` AS
SELECT d.`id`, COALESCE(d.`name`, s.`name`, CONCAT('#', d.`id`)) AS `name`, s.`target`, d.`enabled`, s.`state`, s.`mode`,
       s.`detail`, s.`route`, s.`access`, s.`last_error`, s.`refreshed_at`, d.`updated_at` AS `definition_updated_at`, d.`definition`
FROM {} d LEFT JOIN `_lazyview_state` s ON s.`definition_id` = d.`id`",
        defs_table(defs_schema)
    )
}

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
            "rebuild_served",
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

/// Whether an error is the server refusing a statement for lack of a privilege (1044 database,
/// 1142 table, 1143 column): the account that ran it is not the setup account.
fn access_denied(e: &mysql_async::Error) -> bool {
    matches!(
        e,
        mysql_async::Error::Server(s) if matches!(s.code, 1044 | 1142 | 1143)
    )
}

/// The schema of a table this build needs: the definitions table is the management user's and may
/// live in a schema of its own; every other table is lazyview's own, in its schema.
fn table_schema<'a>(table: &str, schema: &'a str, defs_schema: &'a str) -> &'a str {
    if table == DEFS {
        defs_schema
    } else {
        schema
    }
}

/// Presence of one table, read through `information_schema` (SELECT rights only, no DDL).
async fn table_present(conn: &mut Conn, schema: &str, table: &str) -> Result<bool> {
    let n: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, table),
        )
        .await?;
    Ok(n.unwrap_or(0) > 0)
}

/// Runs one piece of lazyview's own DDL (the setup run only). An account without the right to run
/// it gets an error naming the setup run rather than the server's privilege error, and never a
/// silent skip: MySQL checks the CREATE privilege even when `IF NOT EXISTS` would make the
/// statement a no-op, so `ensure` asks `information_schema` first and runs the DDL only where a
/// table or a column is really missing.
async fn own_ddl(conn: &mut Conn, schema: &str, table: &str, sql: &str) -> Result<()> {
    match conn.query_drop(sql).await {
        Ok(()) => Ok(()),
        Err(e) if access_denied(&e) => Err(SetupRequired(vec![if table == DEFS {
            format!(
                "no right to create or alter {} — the definitions table belongs to the \
                 management user's schema: run `lazyview setup` with the setup account, which has \
                 CREATE, ALTER and INDEX there",
                defs_table(schema)
            )
        } else {
            format!("no right to create or alter `{schema}`.`{table}`")
        }])
        .into()),
        Err(e) => Err(e.into()),
    }
}

/// Read-only pre-flight for the operations run: checks through `information_schema` (SELECT
/// rights only, no DDL) that every table and column this build needs exists — the definitions
/// table in `defs_schema`, lazyview's own tables in `schema`. A missing one is a startup error
/// naming the fix. The convenience view `_lazyview_overview` is not required (it is a `setup`
/// nicety).
pub async fn verify(conn: &mut Conn, schema: &str, defs_schema: &str) -> Result<()> {
    let fence = crate::fence::table();
    let mut need: Vec<(&str, &str, &[&str])> = REQUIRED
        .iter()
        .map(|(table, cols)| (table_schema(table, schema, defs_schema), *table, *cols))
        .collect();
    need.push((schema, fence, &["id", "n"]));
    let mut missing: Vec<String> = Vec::new();
    for (sch, table, cols) in need {
        if !table_present(conn, sch, table).await? {
            missing.push(format!("table `{sch}`.`{table}` is missing"));
            continue;
        }
        let have: Vec<String> = conn
            .exec(
                "SELECT CAST(COLUMN_NAME AS CHAR) FROM information_schema.COLUMNS WHERE \
                 TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (sch, table),
            )
            .await?;
        for col in cols.iter() {
            if !have.iter().any(|c| c == col) {
                missing.push(format!("`{sch}`.`{table}`.{col} is missing"));
            }
        }
        if table == DEFS {
            // `rebuild` must be the counter the request grows (`SET rebuild = rebuild + 1`): a
            // TINYINT from before this build stops asking once it saturates.
            let ty: Option<Option<String>> = conn
                .exec_first(
                    "SELECT LOWER(DATA_TYPE) FROM information_schema.COLUMNS WHERE \
                     TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'rebuild'",
                    (sch, table),
                )
                .await?;
            if !ty.flatten().is_some_and(|t| t.starts_with("bigint")) {
                missing.push(format!(
                    "{}.rebuild is not a counter yet (widening migration)",
                    defs_table(sch)
                ));
            }
        }
    }
    // State rows are keyed case-sensitively (`Lz_Case` and `lz_case` are different views):
    // `ensure` fixes the collation, the operations run only checks it.
    if table_present(conn, schema, STATE).await? {
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
    if table_present(conn, schema, STATUS).await? {
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

/// Creates and migrates lazyview's own tables: the setup run (`lazyview setup`) is the only run
/// that calls it (the operations run checks instead — see [`verify`]). `_lazyview` is created in
/// `defs_schema`, where the setup account holds the DDL rights; the status tables, the commit
/// fence and the convenience view stay in `schema`, the service's own schema.
pub async fn ensure(conn: &mut Conn, schema: &str, defs_schema: &str) -> Result<()> {
    if !table_present(conn, defs_schema, DEFS).await? {
        own_ddl(conn, defs_schema, DEFS, &create_defs(defs_schema)).await?;
    }
    let fence = crate::fence::table();
    for (table, sql) in [
        (STATE, CREATE_STATE.to_string()),
        (STATUS, CREATE_STATUS.to_string()),
        (fence, create_fence()),
    ] {
        if !table_present(conn, schema, table).await? {
            own_ddl(conn, schema, table, &sql).await?;
        }
    }
    for (table, col, def) in MIGRATIONS {
        let sch = table_schema(table, schema, defs_schema);
        let n: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (sch, *table, *col),
            )
            .await?;
        if n == Some(0) {
            let sql = if *table == DEFS {
                format!("ALTER TABLE {} ADD COLUMN `{col}` {def}", defs_table(sch))
            } else {
                format!("ALTER TABLE `{table}` ADD COLUMN `{col}` {def}")
            };
            own_ddl(conn, sch, table, &sql).await?;
        }
    }
    // `rebuild` widened from the first release's TINYINT(1) to a counter: the request grows it
    // (`UPDATE _lazyview SET rebuild = rebuild + 1`, the management user's statement) and the
    // service acknowledges it in `_lazyview_state.rebuild_served`, so the value must keep growing
    // and never saturate. Additive only, and idempotent: the ALTER runs while the column is not a
    // BIGINT yet. A request left by the old version (`rebuild = 1`, never served) is still served
    // once, because a new `rebuild_served` row starts at 0. This is DDL on the definitions table:
    // it belongs to the setup account, and the operations run only checks it (`verify` above).
    let rebuild_type: Option<Option<String>> = conn
        .exec_first(
            "SELECT LOWER(DATA_TYPE) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'rebuild'",
            (defs_schema, DEFS),
        )
        .await?;
    if !rebuild_type
        .flatten()
        .is_some_and(|t| t.starts_with("bigint"))
    {
        own_ddl(
            conn,
            defs_schema,
            DEFS,
            &format!(
                "ALTER TABLE {} MODIFY `rebuild` BIGINT UNSIGNED NOT NULL DEFAULT 0",
                defs_table(defs_schema)
            ),
        )
        .await?;
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
            "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'uq_lazyview_name'",
            (defs_schema, DEFS),
        )
        .await?;
    if idx == Some(0) {
        own_ddl(
            conn,
            defs_schema,
            DEFS,
            &format!(
                "ALTER TABLE {} ADD UNIQUE KEY `uq_lazyview_name` (`name`)",
                defs_table(defs_schema)
            ),
        )
        .await?;
    }
    // Recreate the convenience view only when it is missing or outdated (avoids DDL per start).
    let current: Option<Option<String>> = conn
        .exec_first(
            "SELECT CAST(VIEW_DEFINITION AS CHAR) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_lazyview_overview'",
            (schema,),
        )
        .await?;
    // The definitions schema is named in the view text, so a view built for another one counts
    // as outdated.
    let up_to_date = current
        .flatten()
        .map(|d| {
            (d.contains("`d`.`name`") || d.contains("d.name"))
                && d.contains("route")
                && d.contains(&format!("`{}`", defs_schema.replace('`', "``")))
        })
        .unwrap_or(false);
    if !up_to_date {
        // A convenience only: never fatal.
        if let Err(e) = conn.query_drop(create_overview(defs_schema)).await {
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
    /// The rebuild counter: any value different from `_lazyview_state.rebuild_served` asks for one
    /// rebuild, which the service acknowledges there (never here). The request is
    /// `UPDATE _lazyview SET rebuild = rebuild + 1`, the management user's statement.
    pub rebuild: u64,
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

/// Acknowledges a rebuild request: records in `_lazyview_state` the `_lazyview.rebuild` value the
/// view's targets were built from (`serve_rebuild` is the whole acknowledgment — the service never
/// writes `_lazyview`, so every change of a definition stays the management user's and the
/// database's audit log attributes it).
///
/// The rows it acknowledges — this definition's rows under its **current** name and its
/// **current** targets — are exactly the rows `Engine::served_sofar` measures the served minimum
/// over at the next start: a row the definition left behind (the name before a rename, a target
/// dropped by a retarget) is outside both sets, so it can neither pin the view to a rebuild on
/// every start nor be a counted row this UPDATE fails to reach (guard R1 of #320). The
/// `definition_id` keeps the acknowledgment from touching another definition's row that happens
/// to carry this view's current name (a rename's leftover, until its table is reclaimed).
///
/// Cost: one UPDATE per view per served request, on the build-completion path — never per binlog
/// event. The `<> ?` guard keeps it from rewriting a row that already carries the value.
pub async fn serve_rebuild(
    conn: &mut Conn,
    definition_id: u64,
    name: &str,
    targets: &[String],
    served: u64,
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }
    let marks = vec!["?"; targets.len()].join(",");
    let mut params: Vec<mysql_async::Value> = Vec::with_capacity(targets.len() + 4);
    params.push(served.into());
    params.push(definition_id.into());
    params.push(name.into());
    params.extend(targets.iter().map(|t| t.as_str().into()));
    params.push(served.into());
    conn.exec_drop(
        format!(
            "UPDATE `_lazyview_state` SET `rebuild_served` = ? WHERE `definition_id` = ? AND `name` = ? AND `target` IN ({marks}) AND `rebuild_served` <> ?"
        ),
        Params::Positional(params),
    )
    .await?;
    Ok(())
}

pub async fn load_definitions(conn: &mut Conn, defs_schema: &str) -> Result<Vec<DefRow>> {
    let rows: Vec<(u64, Option<String>, String, Option<String>, i64, i64)> = conn
        .query(format!("SELECT `id`, CAST(`name` AS CHAR), CAST(`definition` AS CHAR), CAST(`targets` AS CHAR), `enabled`, `rebuild` FROM {} ORDER BY `id`", defs_table(defs_schema)))
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, definition, targets, enabled, rebuild)| DefRow {
            id,
            name: name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty()),
            definition,
            targets: parse_targets(targets.as_deref()),
            enabled: enabled != 0,
            rebuild: rebuild.max(0) as u64,
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
    /// The `_lazyview.rebuild` value this view and target were last built from: a definition whose
    /// counter differs from it asks for one rebuild (see [`serve_rebuild`], and
    /// `Engine::served_sofar` for the rows that decide at a start).
    pub rebuild_served: u64,
}

pub async fn load_state(conn: &mut Conn) -> Result<HashMap<(String, String), StateRow>> {
    let rows: Vec<(String, String, u64, String, String, i64, Option<String>, i64)> = conn
        .query("SELECT `name`, `target`, `definition_id`, `definition_hash`, `state`, `managed`, `access`, `rebuild_served` FROM `_lazyview_state`")
        .await?;
    Ok(rows
        .into_iter()
        .map(|(n, t, id, hash, state, managed, access, served)| {
            (
                (n, t),
                StateRow {
                    definition_id: id,
                    hash,
                    state,
                    managed: managed != 0,
                    access,
                    rebuild_served: served.max(0) as u64,
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
    /// The request value a row INSERTed here was built from, so a row written for a first
    /// build, a rename's new name or a retarget's new target does not ask for a rebuild at
    /// the next start. Never written for an existing row: `serve_rebuild` alone owns that
    /// column, and `Engine::served_sofar` reads it as the minimum over exactly the rows that
    /// UPDATE covers.
    pub rebuild_served: u64,
}

pub async fn set_state(conn: &mut Conn, u: StateUpdate<'_>) -> Result<()> {
    let refreshed = u.state == "ready";
    conn.exec_drop(
        "INSERT INTO `_lazyview_state` (`name`, `target`, `definition_id`, `definition_hash`, `mode`, `detail`, `route`, `access`, `state`, `managed`, `last_error`, `refreshed_at`, `rebuild_served`) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, IF(?, CURRENT_TIMESTAMP(3), NULL), ?) \
         ON DUPLICATE KEY UPDATE `definition_id` = VALUES(`definition_id`), `definition_hash` = VALUES(`definition_hash`), \
         `mode` = VALUES(`mode`), `detail` = VALUES(`detail`), `route` = VALUES(`route`), `access` = VALUES(`access`), \
         `state` = VALUES(`state`), `managed` = GREATEST(`managed`, VALUES(`managed`)), \
         `last_error` = VALUES(`last_error`), `refreshed_at` = IF(?, CURRENT_TIMESTAMP(3), `refreshed_at`), \
         `updated_at` = CURRENT_TIMESTAMP(3)",
        // A 14-element list: `Params` tuples top out at 12. `rebuild_served` is INSERT-only.
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
            u.rebuild_served.into(),
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
    Ok(load_position_row(conn, instance, flavor).await?.0)
}

/// Both halves of the resume point as the row actually holds them: the cursor [`load_position`]
/// reads, and the `binlog_file`/`binlog_pos` the same save is meant to have written — `('', 0)` when
/// there is no row, or nothing was ever saved in it (no real position has an empty file name, so that
/// value matches nothing).
///
/// They are read apart from each other because they can disagree, and the difference is what a save
/// has to notice. A takeover's row is inserted by `coord`'s `bump_epoch` with the two columns empty
/// and only the epoch filled; a run that seeds its "what the row holds" from the position it
/// *started* at instead of the one the columns hold concludes the columns are current and never
/// writes them — which leaves the row every operator's first query reads (`SELECT * FROM
/// _lazyview_status`) silent about where its run began, and leaves a row with no cursor text with no
/// position at all for the fallback above to read. Red on CI run 45670984231 as
/// `lifecycle_bootstrap_and_live_definitions` waiting 60 s for a `binlog_file <> ''` row.
pub async fn load_position_row(
    conn: &mut Conn,
    instance: &str,
    flavor: Flavor,
) -> Result<(Option<Cursor>, crate::binlog::Position)> {
    let row: Option<(String, u64, Option<String>)> = conn
        .exec_first(
            "SELECT `binlog_file`, `binlog_pos`, `binlog_cursor` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance,),
        )
        .await?;
    let nothing = crate::binlog::Position {
        file: String::new(),
        pos: 0,
    };
    let Some((file, pos, text)) = row else {
        return Ok((None, nothing));
    };
    // A NULL cursor (a status row claimed but never saved, or one from before the cursor existed)
    // is "no cursor yet" — the same reading the pre-cursor empty text had; the file/pos columns
    // below carry it.
    let text = text.unwrap_or_default();
    if !text.is_empty() {
        let cursor = flavor.parse_cursor(&text).with_context(|| {
            format!("the saved position {text:?} is no cursor this source speaks")
        })?;
        return Ok((Some(cursor), crate::binlog::Position { file, pos }));
    }
    if file.is_empty() {
        return Ok((None, nothing));
    }
    let at = crate::binlog::Position {
        file: file.clone(),
        pos,
    };
    Ok((Some(Cursor::from_parts(flavor, file, pos)), at))
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

/// Writes the whole cursor — the position **and** its consumed set, and with them the two
/// pre-cursor columns — in one statement, and reads back what the server kept.
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

/// The size of the statement [`save_position_pair`] puts on the wire, to within the packet header:
/// its fixed text plus the four parameters. The engine reads a measured duration of that save
/// against this number to know how fast the meta link is, and prices a cursor rewrite with the
/// answer ([`Cursor`] text is `file:pos[g=…]` ASCII, so its bytes and its characters are the same) —
/// which is why the estimate is written next to the statement it estimates, not guessed at the call
/// site.
pub fn cheap_save_bytes(at: &crate::binlog::Position, instance: &str, epoch: u64) -> usize {
    const FIXED: usize = "UPDATE `_lazyview_status` SET `binlog_file` = ?, `binlog_pos` = ? \
                          WHERE `instance_id` = ? AND `epoch` = ?"
        .len();
    FIXED + at.file.len() + at.pos.to_string().len() + instance.len() + epoch.to_string().len() + 16
}

/// Saves the **position** alone — `binlog_file`/`binlog_pos`, the same `WHERE epoch = ?` as the full
/// save — and leaves `binlog_cursor` exactly as the row holds it. This is the half a flush sends when
/// the consumed set has grown to a size the link cannot carry between two flushes
/// ([`crate::engine`] prices it): the row keeps the last set a save afforded, and its position moves,
/// so a restart from here reads the cursor the row holds (a position behind where this run got to) and
/// re-applies what it missed. Re-applying is idempotent, and that path is what
/// `lifecycle_resumes_from_a_cursor_behind_the_applied_position` proves rather than asserts.
///
/// The two columns are the pair a start falls back to when the cursor column is empty or unreadable
/// ([`load_position`]), so they stay the position this run really read; writing them apart from the
/// cursor never leaves a position *ahead* of a set (the direction that would let a resume skip
/// transactions), only a set behind a position, which is the safe direction. Fails when another
/// instance took the row over.
pub async fn save_position_pair(
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
        // Rendered once: this is the byte-exactness the save's read-back depends on, and a
        // `to_string()` per assertion is a copy of a set the test only wants to look at.
        let whole_text = whole.to_string();
        assert!(
            cursor_fits(whole_text.len()),
            "a set past the old TEXT limit must ride the save whole ({} B)",
            whole_text.len()
        );
        assert_eq!(whole_text, format!("{at}[g={fragmented}]"));
        // Past the MEDIUMTEXT bound: refused, and the refusal names the three things to act on.
        // The text is built from the same shape `Display` writes (asserted one line above), so the
        // refusal is measured without holding a second 17 MB copy of the set alive.
        let monster = "1ec44047-e72e-11f1-81ab-7ab4f2cf23bf:1-346,".repeat(400_000);
        let big_len = format!("{at}[g={monster}]").len();
        assert!(
            !cursor_fits(big_len),
            "a cursor the column cannot hold whole must never be sent to it"
        );
        let msg = format!("{:#}", cursor_too_long(big_len, Some(monster.as_str())));
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

    /// The definitions table is always named with its schema (the service account may have SELECT
    /// only there), the service's own tables stay in the connection's schema.
    #[test]
    fn definitions_table_is_schema_qualified() {
        assert_eq!(defs_table("ctl"), "`ctl`.`_lazyview`");
        assert_eq!(defs_table("a`b"), "`a``b`.`_lazyview`");
        let ddl = create_defs("ctl");
        assert!(
            ddl.starts_with("CREATE TABLE IF NOT EXISTS `ctl`.`_lazyview` ("),
            "{ddl}"
        );
        assert!(ddl.contains("`rebuild` BIGINT UNSIGNED NOT NULL DEFAULT 0"));
        let view = create_overview("ctl");
        assert!(
            view.contains("FROM `ctl`.`_lazyview` d LEFT JOIN `_lazyview_state` s"),
            "{view}"
        );
        assert!(!view.contains("`ctl`.`_lazyview_state`"));
        assert!(
            MIGRATIONS
                .iter()
                .any(|(t, c, _)| *t == STATE && *c == "rebuild_served"),
            "rebuild_served must arrive as an additive migration"
        );
    }

    #[test]
    fn effective_definition() {
        let row = |name: Option<&str>, def: &str| DefRow {
            id: 1,
            name: name.map(str::to_string),
            definition: def.to_string(),
            targets: vec![],
            enabled: true,
            rebuild: 0,
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
