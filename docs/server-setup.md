# Server setup

lazyview reads the source server's binlog as a replica. It needs row-based binary logging and an
account with replication privileges. At startup it checks the settings below. If any is wrong, it
exits with one message per problem, and each message says how to fix it on that server flavor.

## Requirements

| setting | required value | applies to |
|---|---|---|
| `log_bin` | `ON` | all |
| `binlog_format` | `ROW` | all |
| `binlog_row_image` | `FULL` | all |
| `binlog_row_value_options` | empty (not `PARTIAL_JSON`) | MySQL, Percona |
| `log_bin_compress` | `OFF` | MariaDB |

Recommended:

* **GTIDs** on MySQL and Percona (`gtid_mode=ON`, `enforce_gtid_consistency=ON`) save one write per
  refresh but are not needed for exact views. Before reading the sources after a change, lazyview
  makes sure what it read from the binlog is visible: with GTIDs it waits for that transaction
  (`WAIT_FOR_EXECUTED_GTID_SET`); MariaDB and Percona without GTIDs wait until the binlog position
  of a consistent snapshot (`Binlog_snapshot_file` / `_position`) reaches the position read; Oracle
  MySQL without GTIDs (also Amazon RDS / Aurora) commits one row of its own (`_lazyview_fence`, one
  row per instance): with `binlog_order_commits=ON` (the default) MySQL commits in binlog order, so
  every earlier transaction is visible when it returns. With `binlog_order_commits=OFF` lazyview
  warns and waits a fixed `LAZYVIEW_VISIBILITY_DELAY_MS` (20 ms), which is not exact under load.
* **binlog_order_commits=ON** (the default) on Oracle MySQL without GTIDs, so the visibility fence
  above is exact. With OFF lazyview starts, logs one warning and uses the fixed delay.
* **Binlog retention** longer than the longest lazyview outage you want to survive. If the saved
  position has been purged when lazyview comes back, it fails, or rebuilds every view with
  `LAZYVIEW_ON_MISSING_BINLOG=rebuild`. The settings are `binlog_expire_logs_seconds` (MySQL,
  Percona, MariaDB 10.6+) or `expire_logs_days`.

### binlog-do-db / binlog-ignore-db

The server's binlog filters must not exclude the schema lazyview works in (the
database named in `LAZYVIEW_DSN` / `LAZYVIEW_DATABASE`, where its metadata
tables and the `_lazyview_fence` table live) or the source DSN's default
schema when it differs: row events are filtered by the table's own schema, so
an excluded schema is invisible in the binlog. At startup lazyview reads
`Binlog_Do_DB` and `Binlog_Ignore_DB` from the same status statement that
gives the current binlog position (`SHOW BINARY LOG STATUS`, `SHOW MASTER
STATUS` on older servers) and warns when either schema is excluded; it cannot
detect the filters on the target servers it writes to. While lazyview's own
schema is excluded its metadata and visibility-fence writes are not binlogged,
so the visibility fence proves nothing; changes to tables in an excluded
source schema never reach lazyview at all. The filters are startup options:
changing them needs a server restart, then restart lazyview to clear the
warning.

### sql_mode

lazyview's own sessions reset the parts of the server's `sql_mode` that change how SQL text is
parsed: `ANSI_QUOTES`, `PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE`, `NO_BACKSLASH_ESCAPES` and
`IGNORE_SPACE` (and the combination modes `ANSI`, `TRADITIONAL`, `ORACLE`, ... that imply them;
their other members stay). View definitions are read, and the SQL lazyview runs is written, in the
default mode: `"paid"` is a string, `||` is `OR`, `NOT a = b` is `NOT (a = b)`. A global
`ANSI_QUOTES` therefore does not break a definition written for the default mode; a definition
that relies on `ANSI_QUOTES` (double-quoted identifiers) must use backticks instead. The strict and
zero-date flags are dropped as well so values are copied as stored (see
[internals](internals.md)). Other flags (`ONLY_FULL_GROUP_BY`, `NO_ENGINE_SUBSTITUTION`, ...) are
kept.

`binlog_transaction_compression=ON` on MySQL is supported. `binlog_format` and `binlog_row_image`
are dynamic, but sessions that are already connected keep the old value until they reconnect.

### sql_log_bin=0: changes that never reach the binlog

A session that sets `sql_log_bin=0` writes nothing to the binlog — and the binlog
is all lazyview reads. Every change made in such a session is invisible to it:
the view tables keep serving rows that no longer match the source, and the views
stay `ready`, because from lazyview's side nothing happened. Switching
`sql_log_bin` off takes a privileged account (`SUPER`, or `SYSTEM_VARIABLES_ADMIN`
on MySQL 8), so this is about who holds such accounts and about tools that run
with them (dumps and restores, bulk loaders, migrations). With `log_bin=OFF` on
the server lazyview refuses to start
([requirements](#requirements)); it cannot see a single session turning its own
writes off.

A pre-flight check can cover the static server settings of this page: `log_bin`
on, `binlog_format=ROW`, `binlog_row_image=FULL`, the binlog filters not
excluding a schema a view reads. What no check can cover is a session changing
its own settings while lazyview runs. A `preflight` command that runs the static
checks is planned; no version is promised for it.

## Supported servers

Every major line a vendor supports is tested on every change and required for a release
(**blocking**), the oldest included, plus the latest GA (Innovation or rolling) release of each
product: 12 servers, latest patch release of each line. Two more lines are **AWS-only**: the
community has retired them, but Amazon RDS and Aurora still run them, and lazyview claims both
services. The published pre-releases are tested as well, **advisory** only: tested, not
supported, and a failure never blocks a release or the `edge` image. The exact builds are pinned
in `docker-compose.test.yml` (CI and the local servers run the same images).

| Product | Line | Tested build | Tier | GA | Community support ends | Amazon RDS / Aurora |
|---|---|---|---|---|---|---|
| MySQL | 8.0 | 8.0.46 | blocking, AWS-only | 2018-04-19 | retired 2026-04-30 (final 8.0.46) | RDS Extended Support to 2029-07-31; Aurora MySQL 3 (8.0-compatible) standard to 2028-04-30, Extended to 2029-07-31 |
| MySQL | 8.4 LTS | 8.4.13 | blocking, LTS | 2024-04-30 | Premier 2029-04-30, Extended 2032-04 | RDS standard to 2029-07-31, Extended to 2032-07-31; Aurora MySQL 8.4 (GA 2026-05-21) to 2032-04 |
| MySQL | 9.7 LTS | 9.7.4 | blocking, LTS | 2026-04-21 | Premier 2031-04, Extended 2034-04 | RDS Database Preview only |
| MySQL | 26.10 Innovation | 26.10.0 | blocking, latest GA | 2026-10-20 | until the next Innovation release | not offered |
| Percona Server | 8.4 LTS | 8.4.11-11 | blocking, LTS | 2024-08-28 (8.4.0-1) | follows upstream 8.4 (2032-04) | not offered |
| Percona Server | 9.7 LTS | 9.7.2-2 | blocking, LTS (latest GA) | 2026 | follows upstream 9.7 (2034-04) | not offered |
| MariaDB | 10.6 | 10.6.28 | blocking, AWS-only | 2021-07-06 | retired 2026-07-06 (final 10.6.28) | RDS standard to 2026-12-31 |
| MariaDB | 10.11 LTS | 10.11.19 | blocking, LTS | 2023-02-16 | 2028-02-16 | RDS to 2028-02 |
| MariaDB | 11.4 LTS | 11.4.13 | blocking, LTS | 2024-05-29 | 2029-05-29 | RDS to 2029-05 |
| MariaDB | 11.8 LTS | 11.8.9 | blocking, LTS | 2025-06-04 | 2028-06-04 | RDS to 2028-06 |
| MariaDB | 12.3 LTS | 12.3.3 | blocking, LTS | 2026-05-28 | 2029-06-12 | RDS to 2029-06 |
| MariaDB | 13.0 rolling | 13.0.2 | blocking, latest GA | 2026-05 | with the next rolling GA (Q4 2026) | not offered |
| MariaDB | 13.1 | 13.1.1 RC | advisory, pre-release | n/a | unsupported | not offered |
| MariaDB | 13.2 | 13.2.0 Preview | advisory, pre-release | n/a | unsupported | not offered |

Percona publishes no Innovation line and no pre-release, so its latest GA is 9.7 LTS; no
registry has a MySQL pre-release (26.10 went GA, and no newer Early Access exists). When a new
GA arrives (MySQL 27.x, MariaDB 13.1, ...), the latest-GA pin moves to it and the advisory
entries move on to the next pre-release. The AWS-only lines are dropped when AWS retires them
(MariaDB 10.6 after 2026-12-31): one matrix entry in `ci.yml` and one service in
`docker-compose.test.yml` each. Percona Server 8.0 (final 8.0.46-37, no RDS edition) is
dropped; an opt-in tarball pin remains in `scripts/test-servers.sh`.

Sources: MySQL
[RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MySQL.Concepts.VersionMgmt.html) and
[Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraMySQLReleaseNotes/AuroraMySQL.release-calendars.html)
calendars ([Aurora MySQL 8.4 announcement](https://aws.amazon.com/about-aws/whats-new/2026/05/amazon-aurora-mysql/8-4/)),
[MySQL 9.7 LTS](https://infoq.com/news/2026/05/mysql-97-lts/),
[MySQL 26.10.0 release notes](https://dev.mysql.com/doc/relnotes/mysql/26.10/en/news-26-10-0.html),
MariaDB [maintenance policy](https://mariadb.org/about/#maintenance-policy) and
[RDS for MariaDB](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MariaDB.Concepts.VersionMgmt.html),
Percona [release notes](https://docs.percona.com/percona-server/latest/release-notes/release-notes-index.html).
Dates are as published on 2026-10-25; Percona 9.7's GA date is not in the sources and is left
at the year.

Amazon RDS for MySQL / MariaDB and Aurora MySQL are supported on the lines above that they run
(see below).

### Caveats per line

* **MySQL 8.0 and Percona/MySQL 8.4 versus 9.x and 26.x**: 8.0 has none of the innovation
  features and lazyview needs none of them. What differs: `SHOW MASTER STATUS` instead of `SHOW
  BINARY LOG STATUS` (chosen by server version); `binlog_row_value_options` exists since 8.0.3
  (`PARTIAL_JSON` is refused at startup, on 8.0 too); `binlog_transaction_compression` since
  8.0.20; the `utf8` alias means `utf8mb3`; accounts use `caching_sha2_password` by default
  (`mysql_native_password` is off by default in 8.4 and gone in 9.0, so create the lazyview
  account without naming a plugin).
* **MariaDB 10.6 and 10.11 versus 11.x and later**: `JSON` is an alias for `LONGTEXT` on every
  MariaDB, so JSON columns arrive as text. The `UUID` type exists from 10.7, `INET4` / `INET6` from
  10.10: 10.6 has none of them, the later lines send them as 16-byte / 4-byte binary values
  that lazyview decodes. 10.6 lacks the `utf8mb4_uca1400_*` collations (11.5 and later).
* **MySQL 9.x and 26.x**: `innodb_native_foreign_keys` is OFF by default, so foreign-key
  cascades run at the SQL layer and the cascaded child rows are logged in the binlog (before the
  parent's row event). lazyview handles both layouts: with native cascades (8.x) the children are
  traced through the parent. `SHOW MASTER STATUS` is gone (lazyview uses `SHOW BINARY LOG
  STATUS`) and `binlog_format` is deprecated but still required to be `ROW`. These servers
  reject a binary-protocol `TIME` parameter in the last hour of the range when it has a
  fraction (`838:00:00.5` to `838:59:59.999999`, either sign: "TIME value is out of range in
  'set_parameter_value'") although they store such values; lazyview sends those as text.
* **MariaDB 12.3 and later**: `binlog_storage_engine` must stay unset (a binlog stored in InnoDB
  is only served to replicas that start from a GTID; lazyview reads by file and position and
  refuses to start otherwise). Rows events larger than `binlog_row_event_fragment_threshold`
  (default 1 GiB) arrive as `Partial_rows` fragments; lazyview reassembles them, so the default is
  fine.
* **MariaDB 11.4 and later**: `binlog_legacy_event_pos=OFF` (events inside a transaction carry no
  position) is handled; see the module notes in `src/binlog.rs`.
* **MariaDB 11.5 and later** (13.x included): the default collation is `utf8mb4_uca1400_ai_ci`.
  Views on the same server follow the source's collation. A [target](views.md#targets-one-source-many-servers)
  of another flavor (MySQL, Percona, MariaDB before 10.10) does not know it: the target table
  then gets the target's default collation for the charset (logged as a warning); the bytes are
  the same, only comparisons on the target follow its rules.
* **MariaDB 11.4 and later**: a VIRTUAL generated column that is not the last column of a table is
  missing from the UPDATE / DELETE row images the server writes (known issue, tracked in
  [#7](https://github.com/kirmorozov/lazyview/issues/7)); views over such tables can be wrong
  there until it is fixed.
* **MariaDB, all versions**: `log_bin_compress` must be OFF.
* **MariaDB, all versions tested (10.11, 13.0, 13.2 at least)**: a prepared statement with
  `x NOT IN (subquery)` that is executed again after the subquery's result changed between empty
  and non-empty returns the result of the earlier execution (for example rows with a NULL `x` stay
  in). lazyview prepares every source read afresh on MariaDB, so its views are not affected; the
  cost is one extra round trip per read. Applications that reuse prepared statements should know
  about it (`tests/regressions.rs` `reg_not_in_prepared_statement_canary` shows whether a server
  still has it).

## MySQL 8.4 / 26.x and Percona Server 8.4 / 9.7

The defaults already meet the requirements (binary logging on, ROW, FULL). If binary logging was
turned off, remove `--skip-log-bin` / `--disable-log-bin`. A `my.cnf` that spells everything out,
GTIDs included:

```ini
[mysqld]
server_id                = 1
log_bin                  = binlog
binlog_format            = ROW
binlog_row_image         = FULL
gtid_mode                = ON
enforce_gtid_consistency = ON
```

Turning on `gtid_mode` on a running server with existing replicas takes several steps
(`OFF_PERMISSIVE`, `ON_PERMISSIVE`, then `ON`). Follow the MySQL manual for that.

## MariaDB 10.11 / 13.x

Binary logging is off by default. Add to `my.cnf`, then restart:

```ini
[mariadb]
log_bin          = mariadb-bin
binlog_format    = ROW
binlog_row_image = FULL
log_bin_compress = OFF
```

In a container, pass the same settings as flags: `--log-bin=mariadb-bin --binlog-format=ROW
--binlog-row-image=FULL --log-bin-compress=OFF`. Binlogs written while `log_bin_compress` was on
cannot be read. On 12.3 and later leave `binlog_storage_engine` unset (see the caveats above).

## Amazon RDS and Aurora

* **Binary logging.** On RDS for MySQL and MariaDB it is on when automated backups are enabled
  (backup retention of at least one day). On Aurora MySQL, setting `binlog_format` in the DB
  cluster parameter group turns it on.
* **Parameter group.** Set `binlog_format=ROW` and `binlog_row_image=FULL`, and leave
  `binlog_row_value_options` empty (Aurora: the DB cluster parameter group). Reboot the instance
  (or the writer) if a static parameter changed.
* **Binlog retention.** RDS purges binlogs within minutes by default. lazyview warns at startup if
  retention is under 12 hours. Run once, as the master user:

  ```sql
  CALL mysql.rds_set_configuration('binlog retention hours', 24);
  ```

* **GTIDs** (RDS for MySQL): set `gtid_mode=ON` and `enforce_gtid_consistency=ON` in the
  parameter group. lazyview logs this tip at startup when GTIDs are off.
* **Endpoints.** Connect to the instance endpoint or the Aurora cluster (writer) endpoint
  directly. RDS Proxy is not supported: lazyview connects as a replica (binlog dump) and holds a
  named lock for the whole session, and a pooling proxy does not pass either through.
* **TLS.** RDS and Aurora certificates come from Amazon's own CA. Use `LAZYVIEW_TLS_CA` with the
  RDS bundle, see [TLS for managed databases](configuration.md#managed-databases-rds-aurora-azure-cloud-sql).

## User and grants

lazyview runs in two runs ([the two runs](operations.md#the-two-runs)), and the rights follow the
run: the setup run is the only one that does DDL on lazyview's own tables. Two layouts: a
**simple setup** with one account that plays both roles, and an **advanced setup** with the three
accounts — **setup**, **control** and **consumption** — the recommended production layout. The
neutral names below are yours to choose: `app` is the application's schema, `lazyview` the schema
of lazyview's own (`LAZYVIEW_DATABASE`: its status tables, the view tables it builds on the
source, and `_lazyview` unless [the definitions have a schema of their
own](#definitions-in-a-schema-of-their-own)), `other_schema` any schema a view reads.

### Simple setup

Run once as an admin. One account does both runs; the cost of that is the last bullet below. The
setup run creates lazyview's own tables in a schema of lazyview's own — not in `app`, which it only
reads.

```sql
CREATE DATABASE IF NOT EXISTS lazyview;
CREATE USER 'lazyview'@'%' IDENTIFIED BY 'change-me';
-- lazyview's own schema: its tables (the setup run creates them), the view tables it builds there,
-- the `_lazyview_overview` view and the planner's probe views:
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON lazyview.* TO 'lazyview'@'%';
-- the application schema: the sources the views read (lazyview never writes here):
GRANT SELECT ON app.* TO 'lazyview'@'%';
-- every other schema a view reads:
GRANT SELECT ON other_schema.* TO 'lazyview'@'%';
-- the binlog stream (global on MySQL / MariaDB, never per schema):
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview'@'%';
```

* The schema must exist (`CREATE DATABASE lazyview`): `lazyview setup` creates lazyview's own tables
  in it, and the operations run only checks that they are there.
* `CREATE VIEW`, `SHOW VIEW` and `DROP` are used to work out the column types of a view (the
  planner's `_lv_probe_…` views) and to build the view tables, which on the source are created in
  this same schema.
* A view reads another schema with a qualified name (`app.orders`) — that is what the `SELECT` on
  `app` is for. An unqualified name means lazyview's own schema.
* On each [target](views.md#targets-one-source-many-servers), the target's user needs the schema
  `GRANT` above on the target's schema. Targets need no replication privileges.
* If a privilege is missing, lazyview's error message includes the `GRANT` statements to run.
* On MariaDB 10.5.2 and later, `REPLICATION CLIENT` is an alias of `BINLOG MONITOR`: `SHOW GRANTS`
  prints `GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.*` for the documented `GRANT`. Same
  privilege, nothing to change.
* The cost of the one account: it holds `CREATE`, `DROP` and `ALTER` on lazyview's own schema, and
  it is also the account that writes the definitions — disabling or deleting a row of `_lazyview`
  drops the view's table by default ([changes while running](views.md#changes-while-running)).
  From v0.2 the service writes no row of `_lazyview` at all (the name and the rebuild request are
  yours), so with one account the audit log cannot even tell the service's operator from the
  control plane. No extra grant is needed for that: the `SELECT` above is what the service uses
  there. Where the distinction matters, use the
  [advanced setup](#advanced-setup-recommended-for-production).

### Advanced setup (recommended for production)

Three accounts, one per job: the account that installs, the account that writes the definitions,
and the long-lived account that runs the service. The neutral names below are the accounts
themselves: `lazyview_setup`, `lazyview_control`, `lazyview_svc`.

| account | used | grants |
|---|---|---|
| setup | `lazyview setup` only — at install and at upgrade, never at run time | `CREATE`, `ALTER`, `INDEX`, `CREATE VIEW`, `DROP` on lazyview's own schema (its tables and the `_lazyview_overview` view), `INSERT` and `UPDATE` on `_lazyview_status` (the run takes the instance lock, whose fencing epoch lives there), `SELECT` on every schema a definition reads: validating a definition plans it, and a table the account cannot see reads as missing — and, with the definitions in a schema of their own, `SELECT`, `CREATE`, `ALTER` and `INDEX` there: it is the setup account that creates `_lazyview` and widens its `rebuild` counter |
| control | whoever writes the definitions (your control plane); the service never does | `SELECT` on lazyview's own schema; `INSERT`, `UPDATE`, `DELETE` on `_lazyview` — it alone changes definitions, enables and disables them, sets `targets` and asks for a rebuild (`rebuild = rebuild + 1`) |
| consumption | the operations run: the service | `SELECT` on `_lazyview` and, from v0.2, nothing else there; `SELECT`, `INSERT`, `UPDATE`, `DELETE` on the status tables; no DDL on lazyview's own tables; its DDL rights are for the view tables it builds; `REPLICATION SLAVE`, `REPLICATION CLIENT` on `*.*` of the servers whose binlog it reads |

The status tables are `_lazyview_state` (one row per view and target, upserted),
`_lazyview_status` (the saved position per instance) and `_lazyview_fence` (the
visibility-fence counter). From v0.2 those are the only tables the service writes:
the definitions table is read-only for it, so every change of a definition —
including a rebuild request, `UPDATE _lazyview SET rebuild = rebuild + 1` — is a
write of the control account, and the request is acknowledged in the service's own
`_lazyview_state.rebuild_served`
([rebuilding a view](views.md#rebuilding-a-view)). Until v0.2 the service also
wrote two columns of `_lazyview`: `name`, filled in from a view's `CREATE VIEW`
statement, and `rebuild`, cleared when a rebuild request was served; while you are
still on that release, keep the column grant the consumption script used to carry
(`GRANT UPDATE (name, rebuild) ON lazyview._lazyview TO 'lazyview_svc'@'%';`). The
v0.2 service needs neither.

The two runs are what makes the split exact. `lazyview setup`, run with the setup account, owns
all DDL on lazyview's own tables; the operations run issues none of it — it checks them through
`information_schema` with `SELECT` rights only and refuses to start (exit `2`) naming the setup run
when one is missing ([the two runs](operations.md#the-two-runs)). So the install and upgrade step
is one `lazyview setup` with the setup account's credentials, and nothing else at run time
(`LAZYVIEW_SYNC=false` is a deprecated alias of it, kept for one release). On Kubernetes the
`setup` initContainer runs it on every pod start; the advanced layout gives that container its own
Secret with the setup account ([Kubernetes](kubernetes.md#deploy)).

Today the control tables live on the source server in `LAZYVIEW_DATABASE`. A
control store off the source is planned; no version is promised for it. In the
advanced layout the definitions table `_lazyview` lives in a schema of its
own, apart from the schemas where the service creates and drops view tables:
MySQL grants `CREATE` and `DROP` per schema, so a service that may create its
view tables in the same schema could drop the definitions table too. From v0.2
the setting that places it there is `LAZYVIEW_DEFINITIONS_DATABASE`, and the
service account then holds `SELECT` in that schema and nothing else
([definitions in a schema of their own](#definitions-in-a-schema-of-their-own)).
The service's view tables on the source are still created in the schema of
lazyview's own (`LAZYVIEW_DATABASE`), which is why the consumption script below
carries the schema-wide DDL line. Once a view's tables are known ahead of time,
grant them per table instead:
`v_order_total`, `v_order_total__lv_new`, `v_order_total__lv_old`, and the
planner's `_lv_probe_%` views (`CREATE VIEW` and `DROP`).

Through a view the control user can read whatever the service account can
read: the view tables are ordinary tables, and the service built them from what
its own `SELECT` rights allowed. Grant `SELECT` on them deliberately. Note too
how much the control user holds: disabling or deleting a row of `_lazyview`
drops the view's table by default
([changes while running](views.md#changes-while-running)).

**Audit of definition changes.** `_lazyview` records no actor and no history. The
row-based binlog carries each change and its time, not the account that made it.
Attribution comes from the database's audit log (MySQL Enterprise Audit, MariaDB
Audit Plugin): in the advanced layout every change to `_lazyview` comes from the
control account, so the audit log names it for every definition change — the
account, not the person behind it; separating the two is a matter of one account
per person on your side.

**GRANT script — setup account, control database on the source.** Run once as an
admin; the account is then used only by `lazyview setup`, at install and at
upgrade.

```sql
CREATE DATABASE lazyview;                       -- the schema of lazyview's own

CREATE USER 'lazyview_setup'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT, CREATE, ALTER, INDEX, CREATE VIEW, DROP ON lazyview.* TO 'lazyview_setup'@'%';
GRANT INSERT, UPDATE ON lazyview._lazyview_status TO 'lazyview_setup'@'%';  -- the setup run takes the instance lock, which writes the fencing epoch there
GRANT SELECT ON app.* TO 'lazyview_setup'@'%';            -- the schemas its definitions read
GRANT SELECT ON other_schema.* TO 'lazyview_setup'@'%';
```

**GRANT script — control account, control database on the source.** The only
account that changes definitions; nothing runs as it.

```sql
CREATE USER 'lazyview_control'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT ON lazyview.* TO 'lazyview_control'@'%';      -- also reads the state tables and the overview
GRANT INSERT, UPDATE, DELETE ON lazyview._lazyview TO 'lazyview_control'@'%';
```

**GRANT script — consumption account, control database on the source.** The
service, the operations run: no DDL on lazyview's own tables.

```sql
CREATE USER 'lazyview_svc'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT ON lazyview._lazyview TO 'lazyview_svc'@'%';
-- from v0.2 the service writes no column of _lazyview, so it needs no UPDATE there; on the
-- release before v0.2 it also wrote `name` and cleared `rebuild`, and needs
-- GRANT UPDATE (name, rebuild) ON lazyview._lazyview TO 'lazyview_svc'@'%';  until then
GRANT SELECT, INSERT, UPDATE, DELETE ON lazyview._lazyview_state TO 'lazyview_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lazyview._lazyview_status TO 'lazyview_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lazyview._lazyview_fence TO 'lazyview_svc'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview_svc'@'%';
GRANT SELECT ON app.* TO 'lazyview_svc'@'%';              -- the source tables the views read
GRANT SELECT ON other_schema.* TO 'lazyview_svc'@'%';     -- every other schema a view reads
-- required by the code as it stands today, for a different reason than the tables above: the
-- service's view tables on the source are built in this same schema (they move out with the
-- definitions table); remove this line when that lands, and grant the view tables per table:
GRANT INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON lazyview.* TO 'lazyview_svc'@'%';

-- optional, for dashboards and humans:
CREATE USER 'lazyview_monitor'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT ON lazyview._lazyview_overview TO 'lazyview_monitor'@'%';
GRANT SELECT ON lazyview._lazyview TO 'lazyview_monitor'@'%';  -- the overview is SQL SECURITY
                                                                -- INVOKER and joins _lazyview
GRANT SELECT ON lazyview._lazyview_state TO 'lazyview_monitor'@'%';
GRANT SELECT ON lazyview._lazyview_status TO 'lazyview_monitor'@'%';
GRANT SELECT ON lazyview._lazyview_fence TO 'lazyview_monitor'@'%';
```

On MySQL and MariaDB the replication privileges (`REPLICATION SLAVE`, `REPLICATION CLIENT`) are
global, never per schema: a service that streams a separate control instance sees that instance's
whole binlog and filters it client-side. (On PostgreSQL a publication can be limited to the
control tables.)

#### Definitions in a schema of their own

From v0.2 `LAZYVIEW_DEFINITIONS_DATABASE` places `_lazyview` in a schema of its own —
`shop_views` below; unset it is the schema of `LAZYVIEW_DSN`, which is the simple setup
unchanged. This is the setting to use when the definitions are owned by a migration tool or a
DBA role and the runtime account should not be able to touch them: the service account holds
`SELECT` in that schema and nothing else, so it cannot write, widen or rename anything there. Its
own tables (`_lazyview_state`, `_lazyview_status`, the commit fence, `_lazyview_overview`) and its
view tables stay in `LAZYVIEW_DATABASE`. The definitions table is always addressed
schema-qualified (`shop_views`.`_lazyview`), so the service does not need that schema as its
connection schema and unqualified names in a definition still mean `LAZYVIEW_DATABASE`. Rebuild
requests are read through the same `SELECT` and acknowledged in the service's own
`_lazyview_state.rebuild_served`, so the definitions schema needs no write grant at all.

```sh
LAZYVIEW_DSN=mysql://lazyview_svc:change-me@db:3306/lazyview   # the service's own schema
LAZYVIEW_DEFINITIONS_DATABASE=shop_views                       # where _lazyview lives
```

Both runs read the variable, so give the setup run the same value: pointed at no definitions
schema, it creates `_lazyview` in the service's own schema (`ensure`, `src/meta.rs:353`) and the
operations run then reports the definitions table missing (`verify`, `src/meta.rs:286`). A service
that started with the setting logs ``definitions table `shop_views`.`_lazyview` (the service reads
it only)`` (`src/engine.rs:1796`).

And the definitions schema has to be binlogged: a definition you change there reaches the service
as a binlog event on that table (`src/engine.rs:5001`), so a `binlog-do-db` / `binlog-ignore-db`
that drops it leaves the service seeing the definitions only at its next restart — the startup
check says exactly that
(`binlog-do-db / binlog-ignore-db exclude the definitions schema {defs_schema}: … noticed only when
the service restarts; remove the filter or include {defs_schema}`, `src/engine.rs:232-243`, and see
[binlog-do-db / binlog-ignore-db](#binlog-do-db--binlog-ignore-db)).

**GRANT script — setup account, definitions in their own schema.** Run once as an admin. The
setup run is the only run that does DDL, and it owns the DDL on *both* schemas: lazyview's own
tables in `lazyview`, and the definitions table in `shop_views` — creating `_lazyview` where it is
missing and running the two migrations of v0.2, `rebuild` widened to `BIGINT UNSIGNED` and
`rebuild_served BIGINT UNSIGNED NOT NULL DEFAULT 0` added to `_lazyview_state`. The `SELECT` on
the definitions schema is what lets the run read the definitions to validate them and create
`_lazyview_overview` over them. There is no `DROP` there: lazyview drops nothing in the
definitions schema, and the management user's table should not be droppable by the installer.

```sql
CREATE DATABASE shop_views;                        -- the definitions' own schema
CREATE USER 'lazyview_setup'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT, CREATE, ALTER, INDEX, CREATE VIEW, DROP ON lazyview.* TO 'lazyview_setup'@'%';
GRANT INSERT, UPDATE ON lazyview._lazyview_status TO 'lazyview_setup'@'%';  -- the instance lock writes the fencing epoch there
GRANT SELECT, CREATE, ALTER, INDEX ON shop_views.* TO 'lazyview_setup'@'%'; -- creates _lazyview, adds its columns, widens rebuild
GRANT SELECT ON app.* TO 'lazyview_setup'@'%';            -- the schemas its definitions read
```

**GRANT script — consumption account, definitions in their own schema.** `SELECT` in the
definitions schema and nothing else there. With only those rights the service still reads the
definitions, serves a rebuild request and acknowledges it in its own schema. It runs no migration
there: an account without the right would meet `ERROR 1142` mid-migration, so the operations run
asks `information_schema` first and stops with a message naming the missing table or column and
the `lazyview setup` that adds it (exit `2`). On `lazyview.*` and on the source schemas it holds
exactly what the consumption script above grants:

```sql
CREATE USER 'lazyview_svc'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT ON shop_views.* TO 'lazyview_svc'@'%';          -- SELECT only: no write, no DDL
GRANT SELECT, INSERT, UPDATE, DELETE ON lazyview._lazyview_state TO 'lazyview_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lazyview._lazyview_status TO 'lazyview_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lazyview._lazyview_fence TO 'lazyview_svc'@'%';
GRANT INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON lazyview.* TO 'lazyview_svc'@'%';                        -- its own view tables
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview_svc'@'%';
GRANT SELECT ON app.* TO 'lazyview_svc'@'%';
```

**GRANT script — control account, definitions in their own schema.** Definitions, `enabled`,
`targets` and rebuild requests are all its writes, so the audit log names it for every change:

```sql
CREATE USER 'lazyview_control'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT ON lazyview.* TO 'lazyview_control'@'%';        -- also reads the state tables and the overview
GRANT SELECT, INSERT, UPDATE, DELETE ON shop_views._lazyview TO 'lazyview_control'@'%';
```

One thing to know about this layout: whoever reads `_lazyview_overview` needs the read right in
**both** schemas. The view is created `SQL SECURITY INVOKER` and joins the definitions table
(`create_overview`, `src/meta.rs:136`), so its reader is checked against both sides — add
`GRANT SELECT ON shop_views._lazyview TO 'lazyview_monitor'@'%';` to the monitor script above,
next to the `SELECT` on `lazyview._lazyview_overview`.

**GRANT script — separate control instance (planned).** When the control store
runs on an instance of its own instead of the source, the same three accounts
exist there: the setup and control scripts are unchanged, pointed at the control
server. The service account gains replication rights there only where
lazyview reads the control store as another source:

```sql
-- on the control instance, for the service account only:
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview_svc'@'%';
```

**GRANT script — target instance.** On each
[target](views.md#targets-one-source-many-servers), the target's user needs the
DDL and DML rights on the target schema that holds the view's tables, and no
replication privileges:

```sql
CREATE USER 'lazyview_target'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON app.* TO 'lazyview_target'@'%';            -- the target's schema
```

## Verify

As the lazyview user, on the source:

```sql
SHOW VARIABLES WHERE Variable_name IN
  ('log_bin', 'binlog_format', 'binlog_row_image', 'binlog_row_value_options',
   'log_bin_compress', 'binlog_order_commits', 'gtid_mode');
-- expect log_bin ON, binlog_format ROW, binlog_row_image FULL,
-- binlog_row_value_options empty (MySQL), log_bin_compress OFF (MariaDB),
-- binlog_order_commits ON (Oracle MySQL without GTIDs)
SHOW GRANTS;
SHOW BINARY LOGS;  -- works only with REPLICATION CLIENT; lists the retained binlog files
```

After lazyview has started, its replication connection shows up in the process list:

```sql
SELECT id, user, host, time FROM information_schema.PROCESSLIST
 WHERE command LIKE 'Binlog Dump%';
```
