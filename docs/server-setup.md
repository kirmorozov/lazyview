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

## MySQL and Percona Server 8.0 / 8.4

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

## MariaDB 10.11 / 11.4

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
cannot be read.

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
