# lazyview

Materialized views for MySQL, Percona Server and MariaDB. You store a view definition (a `SELECT`)
in a table. lazyview, a small Rust service that reads the binlog as a replica, materializes it as a
real InnoDB table (on the same server or on others) and keeps it up to date incrementally. Reading
a view is a plain table read, and the source tables are never locked.

## Quickstart

**1. Prepare the database** (once, as an admin). `app` is the schema that holds your tables,
lazyview's metadata tables and the views.

```sql
CREATE DATABASE IF NOT EXISTS app;
CREATE USER 'lazyview'@'%' IDENTIFIED BY 'change-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON app.* TO 'lazyview'@'%';
```

The binlog must be on, in ROW format with full row images. MySQL and Percona 8.x do that by
default. MariaDB, RDS and Aurora need some configuration, see [server setup](docs/server-setup.md).

**2. Run it** (database on the Docker host, health port: see [operations](docs/operations.md#running-with-docker)).

```sh
docker run -d --name lazyview --restart unless-stopped \
  -e LAZYVIEW_HOST=db.example.com \
  -e LAZYVIEW_USER=lazyview \
  -e LAZYVIEW_PASSWORD='change-me' \
  -e LAZYVIEW_DATABASE=app \
  ghcr.io/kirmorozov/lazyview:edge
```

`edge` is the latest build of `main`; once a release exists, pin a version (`:0.1.0`) or use
`latest`. To try it with a bundled MySQL 8.4, run
`docker compose -f docker-compose.example.yml up -d --build --wait` from a checkout, or set
`LAZYVIEW_IMAGE=ghcr.io/kirmorozov/lazyview:edge` to use the published image (see the header of
[docker-compose.example.yml](docker-compose.example.yml)). Kubernetes: [docs/kubernetes.md](docs/kubernetes.md).

**3. Define a view.** lazyview creates the `_lazyview` table on startup. Each row is one view, here
a `CREATE VIEW` statement (or `name` plus a plain `SELECT`).

```sql
CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(50), total DECIMAL(10,2));
INSERT INTO orders VALUES (1, 'ann', 10), (2, 'bob', 5), (3, 'ann', 7);

INSERT INTO _lazyview (definition) VALUES
 ('CREATE VIEW customer_totals AS
   SELECT customer, SUM(total) AS total, COUNT(*) AS n FROM orders GROUP BY customer');

SELECT * FROM _lazyview_overview;   -- what each view is doing
SELECT * FROM customer_totals;      -- a plain table, once built
```

`state` is `building`, `ready` or `error` (see `last_error`). `mode` is `incremental` or `full`,
and `detail` says how (`spj`, `grouped`, `global`) or why (for example `non-deterministic function
NOW`). The mode is known as soon as the definition is parsed, before the table is built. A view in
`error` keeps the mode of its plan; it shows `unknown` only when the definition could not be planned. Once every view has a table, the log says `ready: N view(s) live`
(`docker logs -f lazyview`).

Later, change the view (the old version stays readable until the new one is ready) or remove it
(drops the table):

```sql
UPDATE _lazyview SET definition = 'SELECT customer, COUNT(*) AS n FROM orders GROUP BY customer'
 WHERE name = 'customer_totals';
DELETE FROM _lazyview WHERE name = 'customer_totals';
```

## How it works

* The binlog says which rows changed; lazyview re-reads only the affected units (rows or groups)
  with plain, non-locking `SELECT`s and writes only what differs.
* Incremental by default: select/join views (also with `DISTINCT`, subqueries, CTEs and derived
  tables), `GROUP BY`, aggregates, window functions and top-N (`ORDER BY … LIMIT`) are all
  maintained from the binlog. A view is `full` only where a source change cannot be traced to the
  rows it affects (`NOW()` and other non-deterministic functions, `UNION`, a few more, see
  [what makes a view full](docs/views.md#what-makes-a-view-full)). A full view is rebuilt into a
  shadow table and swapped in, so changes reach it within about `LAZYVIEW_FULL_REFRESH_MS` (60 s)
  plus one build.
* Changed definitions and `ALTER`, `TRUNCATE` or `RENAME` of a source table are handled online: a
  new table is built next to the live one and swapped in with an atomic `RENAME TABLE`.
* After a crash or restart, lazyview resumes from the binlog position saved in `_lazyview_status`.

Details: [internals](docs/internals.md).

## Configuration

All settings are environment variables. The most used ones are listed here. See
[docs/configuration.md](docs/configuration.md) for all of them and for TLS.

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_HOST`, `LAZYVIEW_PORT` | —, `3306` | source server, the one whose binlog is read |
| `LAZYVIEW_USER`, `LAZYVIEW_PASSWORD` | `root`, — | credentials, used verbatim (no URL encoding) |
| `LAZYVIEW_DATABASE` | required with `LAZYVIEW_HOST` | schema holding `_lazyview` and the views |
| `LAZYVIEW_DSN` | — | instead of the five above: `mysql://user:pass@host:3306/app` |
| `LAZYVIEW_TLS`, `LAZYVIEW_TLS_CA` | `disabled`, — | `required`, `verify-ca` or `verify-full`; CA bundle for private or managed CAs |
| `LAZYVIEW_START_MODE` | `resume` | `resume`, `rebuild` (rebuild everything, then stream) or `once` (rebuild every view, exit non-zero if any fails) |
| `LAZYVIEW_FULL_REFRESH_MS` | `60000` | full-mode views are rebuilt at most once per interval |
| `LAZYVIEW_HEALTH_ADDR` | `0.0.0.0:8080` | `/livez`, `/readyz`, `/startupz`, `/metrics`; `off` disables |
| `RUST_LOG`, `LAZYVIEW_LOG_FORMAT` | `info`, `text` | log level; `json` for log shippers |

## Supported servers

MySQL 8.0, 8.4, 9.7 and 26.10, Percona Server 8.4 and 9.7, MariaDB 10.6, 10.11, 11.4, 11.8, 12.3
and 13.0: every major line a vendor supports (the oldest included) plus the latest GA, all tested
on every change, also on Amazon RDS and Aurora MySQL (MySQL 8.0 and MariaDB 10.6 are community-EOL
and kept because AWS still runs them). The published pre-releases (MariaDB 13.1 RC, 13.2 preview)
are tested as advisory: tested, not supported. Details and dates:
[Server setup](docs/server-setup.md#supported-servers).

## Documentation

* [Server setup](docs/server-setup.md): binlog settings per server, RDS and Aurora, user and grants.
* [Configuration](docs/configuration.md): every environment variable, TLS.
* [Writing views](docs/views.md): definitions, incremental and full mode, joins, targets, metadata.
* [Operations](docs/operations.md): Docker, health, metrics, logs, restarts, troubleshooting.
* [Kubernetes](docs/kubernetes.md): kustomize manifests, probes, rollouts.
* [Internals](docs/internals.md): units, de-duplication, diff writes, resume.
* [Development](docs/development.md): building, tests, the container image, CI.

License: [MIT](LICENSE).
