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

* **GTIDs** on MySQL and Percona (`gtid_mode=ON`, `enforce_gtid_consistency=ON`). Before reading
  the sources after a change, lazyview then waits for that exact transaction to be visible
  (`WAIT_FOR_EXECUTED_GTID_SET`). Without GTIDs, and always on MariaDB, it waits a fixed
  `LAZYVIEW_VISIBILITY_DELAY_MS` (20 ms) instead.
* **Binlog retention** longer than the longest lazyview outage you want to survive. If the saved
  position has been purged when lazyview comes back, it fails, or rebuilds every view with
  `LAZYVIEW_ON_MISSING_BINLOG=rebuild`. The settings are `binlog_expire_logs_seconds` (MySQL,
  Percona, MariaDB 10.6+) or `expire_logs_days`.

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

Run once as an admin. `app` is the schema that holds `_lazyview` (`LAZYVIEW_DATABASE`).

```sql
CREATE USER 'lazyview'@'%' IDENTIFIED BY 'change-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON app.* TO 'lazyview'@'%';
-- every other schema a view reads:
GRANT SELECT ON other_schema.* TO 'lazyview'@'%';
```

* The schema must exist (`CREATE DATABASE app`). lazyview creates its metadata tables in it.
* `CREATE VIEW`, `SHOW VIEW` and `DROP` are used to work out the column types of a view.
* On each [target](views.md#targets-one-source-many-servers), the target's user needs the second
  `GRANT` on the target schema. Targets need no replication privileges.
* If a privilege is missing, lazyview's error message includes the `GRANT` statements to run.

## Verify

As the lazyview user, on the source:

```sql
SHOW VARIABLES WHERE Variable_name IN
  ('log_bin', 'binlog_format', 'binlog_row_image', 'binlog_row_value_options',
   'log_bin_compress', 'gtid_mode');
-- expect log_bin ON, binlog_format ROW, binlog_row_image FULL,
-- binlog_row_value_options empty (MySQL), log_bin_compress OFF (MariaDB)
SHOW GRANTS;
SHOW BINARY LOGS;  -- works only with REPLICATION CLIENT; lists the retained binlog files
```

After lazyview has started, its replication connection shows up in the process list:

```sql
SELECT id, user, host, time FROM information_schema.PROCESSLIST
 WHERE command LIKE 'Binlog Dump%';
```
