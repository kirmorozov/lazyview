# lazyview

Materialized views for **MySQL 8.0 / 8.4**, **Percona Server 8.0 / 8.4** and **MariaDB 10.11 / 11.4**,
kept up to date by a small Rust service that reads the binlog as a replica.

You describe a view with a plain `CREATE VIEW` statement in a table; lazyview materializes it as a
real InnoDB table (on the same instance or on other instances) and maintains it incrementally.

```sql
INSERT INTO _lazyview (definition, targets) VALUES
 ('CREATE VIEW order_totals AS
     SELECT o.id, o.created_at, c.name AS customer, SUM(i.qty * i.price) AS total
     FROM orders o JOIN customers c ON c.id = o.customer_id
     LEFT JOIN order_items i ON i.order_id = o.id
     GROUP BY o.id, o.created_at, c.name',
  'source,analytics');
```

## How it works

* **Parsing.** The statement is parsed (MySQL dialect) to find the source tables, joins, filters
  and grouping. Each view gets a *unit*: the key that identifies a group of materialized rows
  (the first table's primary key for select/join views, the GROUP BY key for aggregates).
* **Minimal work per change.** Each binlog row is decoded once and fanned out to every view reading
  that table. Updates that touch no column a view uses are dropped. Rows are mapped to unit keys
  straight from the binlog image when the unit is reachable through equality conditions
  (`i.order_id = o.id`); otherwise a single `SELECT DISTINCT unit` lookup per batch resolves them.
* **De-duplication.** Keys are buffered for `LAZYVIEW_BATCH_WINDOW_MS` (integer keys in roaring
  bitmaps, others in hash sets). 30 writes to order 42 across `orders`, `order_items` and
  `payments` become **one** refresh of unit 42.
* **Non-blocking.** Sources are only read with plain, non-locking `SELECT`s (READ COMMITTED);
  there is never an `INSERT … SELECT`. Fresh rows are diffed against the materialized rows per
  unit and only units that changed are written (multi-row INSERT / DELETE on the target).
* **Foreign-key cascades.** `ON DELETE/UPDATE CASCADE | SET NULL` changes are made by the storage
  engine and never reach the binlog; lazyview watches the parent tables too. When a view's unit is
  the FK column (e.g. grouped by `order_id`, or joined `i.order_id = o.id`), units come from the
  parent's binlog row; otherwise the view is rebuilt online after such a parent change (a warning
  is logged when the view is created).
* **Views that cannot be maintained incrementally** (UNION, DISTINCT, LIMIT, subqueries, window
  functions, non-deterministic functions, first table without a primary key) fall back to full
  rebuilds into a shadow table, swapped in atomically.

## Online changes and migrations

Every structural change is done online, with the old table readable and maintained until an
atomic `RENAME TABLE` swap:

| change | what happens |
|---|---|
| new definition | built in the background into `<name>__lv_new`, then renamed into place |
| `UPDATE _lazyview SET definition = …` | new version built next to the live one; changes during the build are buffered and applied; then swapped |
| new name on the same `_lazyview.id` | new table built; the old one dropped after the new one is live |
| `targets` changed | new targets built; removed targets dropped after the swap |
| `enabled = 0` / row deleted | table dropped (`LAZYVIEW_DROP_REMOVED=true`) |
| `ALTER` / `TRUNCATE` / `RENAME` of a source table | view re-planned and rebuilt online |

Readers never see a missing or half-built table. lazyview never replaces or drops a table it did not
create (`_lazyview_state.managed`).

## Targets: one source, many instances

`_lazyview.targets` is a comma-separated list of target names; `source` (the default when NULL)
is the instance whose binlog is read. Sources are read once per batch and the result is diffed
and written to each target independently.

```
LAZYVIEW_TARGETS=analytics,reporting
LAZYVIEW_TARGET_ANALYTICS_DSN=mysql://lv:secret@analytics-db:3306/marts
LAZYVIEW_TARGET_REPORTING_DSN=mysql://lv:secret@reporting-db:3306/reports
```

A view may read another lazyview table (chained views) only if the upstream view is materialized
on `source`, because only the source binlog is watched.

## Configuration

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_DSN` | required | `mysql://user:pass@host:3306/schema` — source; schema holding `_lazyview` |
| `LAZYVIEW_START_MODE` | `resume` | `resume` continue from `_lazyview_status`; `rebuild` rebuild everything then stream; `once` build everything and exit |
| `LAZYVIEW_SYNC` | `true` | `false`: create/migrate metadata and validate definitions, then exit |
| `LAZYVIEW_BATCH_WINDOW_MS` | `200` | de-duplication window |
| `LAZYVIEW_BATCH_MAX_KEYS` | `50000` | flush earlier when this many keys are buffered |
| `LAZYVIEW_QUERY_CHUNK` | `1000` | keys per refresh/lookup query |
| `LAZYVIEW_WRITE_BATCH_ROWS` | `500` | rows per multi-row INSERT |
| `LAZYVIEW_TARGETS`, `LAZYVIEW_TARGET_<NAME>_DSN` | — | additional target instances |
| `LAZYVIEW_SERVER_ID` | derived from instance id | replica server id (unique in the topology) |
| `LAZYVIEW_INSTANCE_ID` | `default` | key of the saved position in `_lazyview_status` |
| `LAZYVIEW_DROP_REMOVED` | `true` | drop tables of removed definitions/targets |
| `LAZYVIEW_ON_MISSING_BINLOG` | `fail` | `fail` or `rebuild` when the saved binlog file is purged |
| `LAZYVIEW_VISIBILITY_DELAY_MS` | `20` | wait before reading sources when no GTID fence is available (MariaDB, gtid_mode=OFF) |
| `LAZYVIEW_HEARTBEAT_MS` | `5000` | replication heartbeat |
| `LAZYVIEW_POSITION_PERSIST_MS` | `5000` | how often an idle position is saved |
| `RUST_LOG`, `LAZYVIEW_LOG_FORMAT` | `info`, `text` | logging (`json` available) |

### Server requirements

`log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL` (MariaDB: `log_bin_compress=OFF`).
The user needs `REPLICATION SLAVE, REPLICATION CLIENT`, `SELECT` on the sources and
`CREATE, DROP, ALTER, INSERT, DELETE, CREATE VIEW` on the lazyview schema (and on target schemas).

## Metadata tables (source schema)

* `_lazyview` — your definitions: `id`, `definition`, `targets`, `enabled`.
* `_lazyview_state` — per view and target: mode, `state` (`building` / `ready` / `error`),
  `last_error`, `refreshed_at`.
* `_lazyview_status` — saved binlog position per `LAZYVIEW_INSTANCE_ID` (resume after restart).

## Running

### Docker

Fill in host, user, password and database, then paste:

```sh
docker run -d --name lazyview --restart unless-stopped \
  -e LAZYVIEW_HOST=db.example.com \
  -e LAZYVIEW_USER=lazyview \
  -e LAZYVIEW_PASSWORD='change-me' \
  -e LAZYVIEW_DATABASE=app \
  ghcr.io/OWNER/lazyview:latest
```

* `OWNER` is the GitHub owner of this repository (CI publishes to `ghcr.io/<owner>/<repo>`).
  Or build it yourself: `make docker IMAGE=lazyview:latest` and run `lazyview:latest`.
* Values are used verbatim (no URL encoding). `LAZYVIEW_PORT` defaults to `3306`;
  `LAZYVIEW_DSN=mysql://user:pass@host:3306/db` works instead of the four variables.
* Database on the Docker host itself: add `--add-host=host.docker.internal:host-gateway` and use
  `LAZYVIEW_HOST=host.docker.internal`.
* Keep the password out of your shell history with `--env-file lazyview.env` (`NAME=value` lines).
* Add `-p 8080:8080` to reach `/metrics`, `/healthz`, `/readyz` from outside the container.

The same as a Compose service for an existing database:

```yaml
services:
  lazyview:
    image: ghcr.io/OWNER/lazyview:latest
    restart: unless-stopped
    environment:
      LAZYVIEW_HOST: db.example.com
      LAZYVIEW_USER: lazyview
      LAZYVIEW_PASSWORD: change-me
      LAZYVIEW_DATABASE: app
```

Or try everything at once: MySQL 8.4 (ROW binlog, replication grants) plus lazyview built from this
checkout. The header of `docker-compose.example.yml` shows how to define a first view.

```sh
docker compose -f docker-compose.example.yml up -d --build --wait
```

### Image

* Base `gcr.io/distroless/static-debian12:nonroot`, pinned by digest in the `Dockerfile`: no shell,
  no package manager; CA certificates (`/etc/ssl/certs/ca-certificates.crt`) and tzdata
  (`/usr/share/zoneinfo`) included. `/lazyview` is a static musl binary; `linux/amd64` and `linux/arm64`.
* Runs as `65532:65532` (`nonroot`) with `ENTRYPOINT ["/lazyview"]`. It writes no files, so
  `--read-only --cap-drop ALL --security-opt no-new-privileges` work.
* Tags: `X.Y.Z`, `X.Y`, `latest` (releases from `v*` git tags), `edge` (main), `sha-<commit>`.
  Each also exists with a `-debug` suffix (`1.2.3-debug`, `latest-debug`, `edge-debug`): the same
  binary on `static-debian12:debug-nonroot`, which adds busybox under `/busybox`, so
  `docker exec -it lazyview sh` works there (and only there).
* Pushed images carry provenance (`mode=max`) and SBOM attestations:
  `docker buildx imagetools inspect ghcr.io/OWNER/lazyview:latest --format '{{json .SBOM}}'`
  (or `'{{json .Provenance}}'`).

### Health

None of this needs a shell in the container:

* `HEALTHCHECK` is exec form `["/lazyview", "healthcheck"]`: the binary probes its own `/healthz`,
  so `docker ps` shows `healthy` / `unhealthy`. If you set `LAZYVIEW_HEALTH_ADDR=off`, also run
  with `--no-healthcheck`.
* HTTP on `LAZYVIEW_HEALTH_ADDR` (default `0.0.0.0:8080`): `/healthz` (engine loop alive),
  `/readyz` (streaming the binlog, all views built, none in error), `/metrics` (Prometheus).
  Kubernetes:

  ```yaml
  livenessProbe:  { httpGet: { path: /healthz, port: 8080 }, periodSeconds: 30 }
  readinessProbe: { httpGet: { path: /readyz,  port: 8080 }, periodSeconds: 10 }
  ```

* lazyview exits non-zero on fatal errors (`2` = configuration error), so a restart policy
  recovers: `docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} restarts={{.RestartCount}}' lazyview`.
* Logs: `docker logs -f lazyview` (`LAZYVIEW_LOG_FORMAT=json` for log shippers).
* From the database, without touching the container:

  ```sql
  SELECT name, target, state, last_error, refreshed_at FROM _lazyview_state WHERE state <> 'ready';
  SELECT * FROM _lazyview_status;  -- saved binlog position per LAZYVIEW_INSTANCE_ID
  SELECT id, user, host, time FROM information_schema.PROCESSLIST
   WHERE command LIKE 'Binlog Dump%';  -- lazyview's replication connection
  ```

### Building the image

* `make docker`: runtime image for the host arch (`lazyview:dev`). `make docker VARIANT=debug`
  builds the debug variant (`lazyview:dev-debug`); `make docker-all` builds both.
* `make docker PLATFORMS=linux/amd64,linux/arm64 PUSH=1 IMAGE=ghcr.io/OWNER/lazyview:tag` pushes a
  multi-arch image (without `PUSH=1`: an OCI archive in `tmp/out`).
* `make docker-smoke [VARIANT=debug]` runs the built image: `--version`, the configuration error
  without env, user and entrypoint, shell absent (runtime) or present (debug).
* `make image-digests` prints the current digests of the pinned base images, to refresh the pins.
* Plain `docker buildx build [--target debug] .` works too. The build stages cross-compile on the
  build platform with cargo-zigbuild (no QEMU); BuildKit cache mounts keep the crate downloads and a
  separate layer keeps the compiled dependencies.

## Development

* `make test` — unit tests.
* `scripts/test-servers.sh start` — start local MySQL/MariaDB/Percona servers from official
  tarballs (under `tmp/out`); `make test-integration` runs the integration suites against all of them.
* `docker compose -f docker-compose.test.yml up -d --wait` — the same servers as containers.
