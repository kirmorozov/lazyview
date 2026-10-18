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

`binlog_transaction_compression=ON` on MySQL is supported. `binlog_format` and `binlog_row_image`
are dynamic, but sessions that are already connected keep the old value until they reconnect.

## Supported servers

Per product, two lines are tested on every change and required for a release (**blocking**): the
oldest line the vendor still supports and the latest GA line. The newest pre-release is tested
as well, **advisory** only (it never blocks). Lines the vendor has retired are dropped. The exact
builds are pinned in `docker-compose.test.yml` (CI and the local servers run the same images).

| Product | Line | Tested build | Tier | Vendor support ends |
|---|---|---|---|---|
| MySQL | 8.4 LTS | 8.4.11 | blocking, oldest LTS | Premier Apr 2029, Extended Apr 2032 |
| MySQL | 26.7 Innovation | 26.7.0 | blocking, latest GA | with the next Innovation release (26.10, due October 2026) |
| MySQL | 26.10 | 26.10.0 Early Access | advisory | pre-release, unsupported |
| Percona Server | 8.4 LTS | 8.4.11-11 | blocking, oldest LTS | active (upstream 8.4: Apr 2032) |
| Percona Server | 9.7 LTS | 9.7.2-2 | blocking, latest GA | active (upstream 9.7: Apr 2034) |
| MariaDB | 10.11 LTS | 10.11.19 | blocking, oldest LTS | 2028-02-16 |
| MariaDB | 13.0 rolling | 13.0.2 | blocking, latest GA | rolling: with the next rolling GA (Q4 2026) |
| MariaDB | 13.2 | 13.2.0 Preview | advisory | pre-release, unsupported |

Percona publishes no Innovation line and no pre-release, so its latest GA is 9.7 LTS. When MySQL
26.10 goes GA, the latest-GA pin moves from 26.7.0 to it (and the advisory entry to the next
Early Access); MariaDB's moves with each rolling GA (13.1, 13.2, ...).

Vendor-supported but **not tested**: MySQL 9.7 LTS (Apr 2031 / Apr 2034), MariaDB 11.4 LTS
(2029-05-29), 11.8 LTS (2028-06-04 community) and 12.3 LTS (2029-06-12). They sit between the
tested lines and lazyview runs on them, but a release does not check them.

**Dropped as end-of-life**: MySQL 8.0 (EOL April 2026, final 8.0.46), Percona Server 8.0 (EOL
2026-04-01, final community build 8.0.46-37) and MariaDB 10.6 (community EOL 2026-07-06, final
10.6.28). Earlier releases of lazyview were tested on MySQL 8.0 and Percona 8.0 and nothing in
the binlog protocol changed for them, but they get no further testing.

Also supported: Amazon RDS for MySQL / MariaDB and Aurora MySQL running one of the lines above
(see below).

### Caveats per line

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
* **MariaDB, all versions**: `log_bin_compress` must be OFF.

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
