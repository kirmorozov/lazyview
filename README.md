# lazyview

Materialized views for **MySQL 8.0 / 8.4**, **Percona Server 8.0 / 8.4** and **MariaDB 10.11 / 11.4**,
kept up to date by a small Rust service that reads the binlog as a replica.

You describe a view with a plain `CREATE VIEW` statement in a table; lazyview materializes it as a
real InnoDB table (on the same instance or on other instances) and maintains it incrementally.

```sql
-- by name, with a plain SELECT ...
INSERT INTO _lazyview (name, definition) VALUES
 ('order_totals',
  'SELECT o.id, o.created_at, c.name AS customer, SUM(i.qty * i.price) AS total
     FROM orders o JOIN customers c ON c.id = o.customer_id
     LEFT JOIN order_items i ON i.order_id = o.id
    GROUP BY o.id, o.created_at, c.name');

-- ... or as a complete CREATE VIEW statement (the name is filled in for you)
INSERT INTO _lazyview (definition) VALUES
 ('CREATE VIEW paid_orders AS SELECT id, total FROM orders WHERE status = ''paid''');

-- change or remove a view by name; the old version stays live until the new one is ready
UPDATE _lazyview SET definition = 'SELECT id, total, status FROM orders' WHERE name = 'paid_orders';
DELETE FROM _lazyview WHERE name = 'paid_orders';
```

Each row is one view; there can be any number of them. Names are unique (enforced by the table).

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

Connection — either the separate variables (values used verbatim, no URL encoding) or a DSN:

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_HOST`, `LAZYVIEW_PORT` | —, `3306` | source server (the one whose binlog is read) |
| `LAZYVIEW_USER`, `LAZYVIEW_PASSWORD` | `root`, — | credentials |
| `LAZYVIEW_DATABASE` | required with `LAZYVIEW_HOST` | schema holding `_lazyview` and the views |
| `LAZYVIEW_DSN` | — | alternative: `mysql://user:pass@host:3306/schema` (`mariadb://` accepted; URL-encode special characters) |
| `LAZYVIEW_TLS` | `disabled` | `disabled`, `required` (encrypt, don't verify), `verify-full` (see [TLS](#tls)) |
| `LAZYVIEW_TLS_CA` | — | PEM file of a private CA to trust (implies `verify-full`) |
| `LAZYVIEW_TARGET_<NAME>_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` / `_DSN` / `_TLS` / `_TLS_CA` | — | extra target instances, discovered automatically (or list names in `LAZYVIEW_TARGETS`) |

Behaviour:

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_START_MODE` | `resume` | `resume` continue from the saved position; `rebuild` rebuild everything then stream; `once` build everything and exit |
| `LAZYVIEW_SYNC` | `true` | `false`: create/migrate metadata, validate every definition, exit non-zero if any is invalid (CI check) |
| `LAZYVIEW_BATCH_WINDOW_MS` | `200` | de-duplication window |
| `LAZYVIEW_BATCH_MAX_KEYS` | `50000` | flush earlier when this many keys are buffered |
| `LAZYVIEW_QUERY_CHUNK` | `1000` | keys per refresh/lookup query |
| `LAZYVIEW_WRITE_BATCH_ROWS` | `500` | rows per multi-row INSERT |
| `LAZYVIEW_BUILD_CONCURRENCY` | `2` | view builds running at once |
| `LAZYVIEW_DROP_REMOVED` | `true` | drop tables of removed definitions/targets |
| `LAZYVIEW_ON_MISSING_BINLOG` | `fail` | `fail` or `rebuild` when the saved binlog file was purged |
| `LAZYVIEW_SERVER_ID` | random | replica server id; set it only if your topology needs fixed ids |
| `LAZYVIEW_INSTANCE_ID` | `default` | key of the saved position; one active instance per schema and id |
| `LAZYVIEW_VISIBILITY_DELAY_MS` | `20` | wait before reading sources when no GTID fence is available (MariaDB, `gtid_mode=OFF`) |
| `LAZYVIEW_HEARTBEAT_MS` | `5000` | replication heartbeat |
| `LAZYVIEW_POSITION_PERSIST_MS` | `5000` | how often an idle position is saved |
| `LAZYVIEW_HEALTH_ADDR` | `0.0.0.0:8080` | health/metrics endpoint, `off` to disable (see [Health](#health)) |
| `RUST_LOG`, `LAZYVIEW_LOG_FORMAT` | `info`, `text` | logging (`json` available); the default level logs lifecycle events and problems only |

A second lazyview started against the same schema (same `LAZYVIEW_INSTANCE_ID`) waits instead of
writing, and takes over if the first one stops. On unrecoverable errors lazyview exits non-zero;
run it with a restart policy and it resumes from the saved position.

### Server requirements

Binary log: `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`.
MySQL 8 / Percona have these by default. **MariaDB** needs them in `my.cnf` (then restart):

```ini
[mariadb]
log_bin          = mariadb-bin
binlog_format    = ROW
binlog_row_image = FULL
log_bin_compress = OFF
```

Database user (run once as an admin; lazyview prints these statements if a privilege is missing):

```sql
CREATE USER 'lazyview'@'%' IDENTIFIED BY 'change-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW ON `app`.* TO 'lazyview'@'%';
-- plus SELECT on any other schema your views read, and the second GRANT on each target schema
```

## Metadata tables (source schema)

* `_lazyview` — your definitions, one row per view: `id`, `name` (unique), `definition` (a
  `SELECT`, or a full `CREATE VIEW` statement), `targets`, `enabled`.
* `_lazyview_state` — per view and target: mode, `state` (`building` / `ready` / `error`),
  `last_error`, `refreshed_at`.
* `_lazyview_status` — saved binlog position per `LAZYVIEW_INSTANCE_ID` (resume after restart).
* `_lazyview_overview` — a view joining definitions and state: `SELECT * FROM _lazyview_overview`
  answers "what is each view doing and why did it fail".

## Running

### Docker

Once, as a database admin: create the user and grant what lazyview needs (`app` is the schema
that holds `_lazyview`). The binlog must be on in ROW format with full row images; MySQL /
Percona 8.x default to that, MariaDB and some managed services do not (see
[Server requirements](#server-requirements)).

```sql
CREATE USER 'lazyview'@'%' IDENTIFIED BY 'change-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazyview'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON app.* TO 'lazyview'@'%';
-- plus GRANT SELECT ON <schema>.* for every other schema your views read
SHOW VARIABLES WHERE Variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
-- expect binlog_format ROW, binlog_row_image FULL, log_bin ON
```

Then fill in host, user, password and database, and paste:

```sh
docker run -d --name lazyview --restart unless-stopped \
  -e LAZYVIEW_HOST=db.example.com \
  -e LAZYVIEW_USER=lazyview \
  -e LAZYVIEW_PASSWORD='change-me' \
  -e LAZYVIEW_DATABASE=app \
  ghcr.io/kirmorozov/lazyview:edge
```

* `edge` is the latest build of `main`. After the first release (`v*` git tag), pin a version
  instead (`ghcr.io/kirmorozov/lazyview:0.1.0`) or use `latest`. Or build the image yourself:
  `make docker IMAGE=lazyview:local` and run `lazyview:local`.
* Values are used verbatim (no URL encoding). `LAZYVIEW_PORT` defaults to `3306`;
  `LAZYVIEW_DSN=mysql://user:pass@host:3306/db` works instead of the four variables.
  `-e LAZYVIEW_TLS=required` encrypts the connection (see [TLS](#tls) below).
* Database on the Docker host itself: add `--add-host=host.docker.internal:host-gateway` and use
  `LAZYVIEW_HOST=host.docker.internal`.
* Keep the password out of your shell history with `--env-file lazyview.env`: `NAME=value` lines,
  taken literally (no quotes, no `$` expansion).
* Add `-p 8080:8080` to reach `/metrics`, `/livez`, `/readyz`, `/startupz` from outside the container.

The same as a Compose service for an existing database:

```yaml
services:
  lazyview:
    image: ghcr.io/kirmorozov/lazyview:edge
    restart: unless-stopped
    environment:
      LAZYVIEW_HOST: db.example.com
      LAZYVIEW_USER: lazyview
      LAZYVIEW_PASSWORD: change-me
      LAZYVIEW_DATABASE: app
```

Compose expands `$` in the file, so write a `$` in a value as `$$` (`s3cr$$t` for `s3cr$t`).

Or try everything at once: MySQL 8.4 (ROW binlog, replication grants) plus lazyview built from this
checkout. The header of `docker-compose.example.yml` shows how to define a first view. Needs
Docker Compose v2.23.1 or newer (inline `configs`).

```sh
docker compose -f docker-compose.example.yml up -d --build --wait
```

### Image

* `ghcr.io/kirmorozov/lazyview`, base `gcr.io/distroless/static-debian12:nonroot`, pinned by
  digest in the `Dockerfile`: no shell, no package manager; tzdata (`/usr/share/zoneinfo`) and the
  Debian CA bundle are included (lazyview does not use the bundle, see [TLS](#tls)). `/lazyview` is
  a static musl binary; one multi-arch image for `linux/amd64` and `linux/arm64`.
* Runs as `65532:65532` (`nonroot`) with `ENTRYPOINT ["/lazyview"]`. It writes no files, so
  `--read-only --cap-drop ALL --security-opt no-new-privileges` work. There is no shell to
  `docker exec` into; use `docker exec lazyview /lazyview healthcheck`, the HTTP endpoints and the
  database tables (see [Health](#health)).
* Tags:
  * `edge`: the latest push to `main`.
  * `sha-<commit>` (7 hex digits): every published build; the newest 20 are kept.
  * Releases (`vX.Y.Z` git tags): `X.Y.Z`, `X.Y`, `X` (not for `0.x`) and `latest`. Pre-release
    tags (`v1.2.0-rc.1`) publish only `1.2.0-rc.1`. `latest` does not exist before the first
    release. Release and `edge` images are never deleted.
* Every image is built and tested by CI before it is pushed: `--version`, exit code `2` without
  configuration, user `65532`, no shell, healthcheck (both architectures, arm64 under QEMU), plus an
  end-to-end run of `docker-compose.example.yml` against MySQL 8.4 (amd64). Releases additionally
  need the integration tests on all six servers.
* Pushed images carry BuildKit provenance (`mode=max`) and SBOM attestations, and a signed GitHub
  build-provenance attestation:

  ```sh
  docker buildx imagetools inspect ghcr.io/kirmorozov/lazyview:edge --format '{{json .SBOM}}'
  docker buildx imagetools inspect ghcr.io/kirmorozov/lazyview:edge --format '{{json .Provenance}}'
  gh attestation verify oci://ghcr.io/kirmorozov/lazyview:edge --owner kirmorozov
  ```

* Maintainers: GHCR creates the package as private on the first push. Make it public once
  (package page, Package settings, Change visibility, Public), otherwise anonymous `docker pull`
  fails with `unauthorized` and users need `docker login ghcr.io` with a `read:packages` token.

### TLS

`LAZYVIEW_TLS=disabled|required|verify-full` (per target `LAZYVIEW_TARGET_<NAME>_TLS`).

* `required` encrypts the connection without verifying the server certificate. Use it for servers
  with MySQL's auto-generated certificates (they carry no host name, so they can never be verified).
* `verify-full` checks the certificate chain and the host name. Public CAs are trusted out of the
  box (Mozilla roots compiled into the binary). For a private or managed CA (AWS RDS / Aurora,
  Azure, Cloud SQL, corporate) download its bundle and point `LAZYVIEW_TLS_CA` at it, e.g. mount
  `global-bundle.pem` and set `LAZYVIEW_TLS_CA=/certs/global-bundle.pem` (implies `verify-full`).
* `verify-ca` (chain only, no host name check) is accepted but currently behaves like
  `verify-full`: the MySQL driver's host-name override does not work with the current TLS
  library. Use `required` or a certificate that names the host.

The image's `/etc/ssl/certs` is not consulted; only the compiled-in roots and `LAZYVIEW_TLS_CA`.

### Health

None of this needs a shell in the container:

* `HEALTHCHECK` is exec form `["/lazyview", "healthcheck"]`: the binary probes its own `/livez`,
  so `docker ps` shows `healthy` / `unhealthy`. The same works by hand in the shell-free image:
  `docker exec lazyview /lazyview healthcheck && echo ok`. If you set `LAZYVIEW_HEALTH_ADDR=off`,
  also run with `--no-healthcheck`.
* HTTP on `LAZYVIEW_HEALTH_ADDR` (default `0.0.0.0:8080`):
  * `/livez` (alias `/healthz`): the engine loop is making progress. It fails only after
    `LAZYVIEW_LIVENESS_STALL_SECS` (120) without progress; database outages, long builds and
    binlog lag do not fail it.
  * `/readyz`: this instance is active, streams the binlog and every view has a live table.
    A hot standby (another instance holds the lock) is not ready unless you ask `/readyz?standby=ok`.
    Views in error are listed but do not fail it unless `LAZYVIEW_READY_REQUIRE_ALL=true`, so
    alert on `lazyview_views_error`.
  * `/startupz`: booted (hot standby, or the binlog stream connected once).
  * `/metrics`: Prometheus. `?verbose` lists the individual checks, `?format=json` returns JSON.
  * Kubernetes uses `/startupz`, `/livez` and `/readyz?standby=ok`; see [Kubernetes](#kubernetes).

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

* `make docker`: image for the host arch, loaded into the local docker as `lazyview:dev`
  (`IMAGE=` to change the tag).
* `make docker PLATFORMS=linux/amd64,linux/arm64 PUSH=1 IMAGE=ghcr.io/kirmorozov/lazyview:tag`
  pushes a multi-arch image (without `PUSH=1`: an OCI archive in `tmp/out`). Multi-platform builds
  need a docker-container buildx builder (Docker's default `docker` driver supports them only with
  the containerd image store), so the Makefile creates and uses one named `lazyview-multiarch`
  when the current builder is the `docker` driver. `BUILDER=<name>` selects another builder. With
  plain buildx, run `docker buildx create --use --driver docker-container` once.
* `make docker-smoke [IMAGE=...]` checks a loaded image: `--version`, the configuration error
  without env, user and entrypoint, healthcheck, no shell. CI runs the same target.
* `make image-digests` prints the current digests of the pinned base images, to refresh the pins
  (Dependabot also proposes base image updates weekly).
* Plain `docker buildx build .` works too. The build stages cross-compile on the build platform
  with cargo-zigbuild (no QEMU); BuildKit cache mounts keep the crate downloads and a separate layer
  keeps the compiled dependencies.
* CI (`.github/workflows/`): `ci.yml` runs lint, unit tests, the integration matrix and the image
  smoke / end-to-end tests on every push and pull request; `publish-image.yml` pushes `main` and
  `v*` tags to GHCR (pull requests never push); `ghcr-cleanup.yml` deletes untagged and old
  `sha-*` images after each publish and daily, without touching the platform and attestation
  manifests of kept multi-arch images.

## Kubernetes

Kustomize manifests live in `deploy/k8s/`:

* `base/`: namespace-agnostic. A Deployment with 2 replicas (one active, one hot standby), the
  ConfigMap `lazyview-config` (generated from `base/config.env`), the Service `lazyview-metrics`,
  a PodDisruptionBudget and a ServiceAccount without a token. Credentials come from a Secret
  `lazyview-db` that you create.
* `overlays/example/`: namespace `lazyview` (Pod Security `restricted`), an example Secret, and
  the image pin. Copy this directory to start.
* `components/prometheus/`: a ServiceMonitor plus a PrometheusRule (needs the Prometheus Operator).

```sh
cp -r deploy/k8s/overlays/example deploy/k8s/overlays/prod
# Set `images:` in kustomization.yaml and the values in secret.yaml. Or drop secret.yaml from the
# overlay and create the Secret yourself once the namespace exists:
#   kubectl -n lazyview create secret generic lazyview-db --from-literal=LAZYVIEW_HOST=db.example.com \
#     --from-literal=LAZYVIEW_USER=lazyview --from-literal=LAZYVIEW_PASSWORD='change-me' \
#     --from-literal=LAZYVIEW_DATABASE=app
kubectl apply -k deploy/k8s/overlays/prod
kubectl -n lazyview get pods        # both READY 1/1: one active, one hot standby
kubectl -n lazyview logs -l app.kubernetes.io/name=lazyview --prefix | grep -E 'standing by|took over|streaming'
```

The base references `ghcr.io/OWNER/lazyview:0.1.0`. Set your registry and tag (or a digest) with
`images:` in the overlay. The Secret is read when the container starts, so run
`kubectl -n lazyview rollout restart deployment/lazyview` after you change it.

### Probes

| endpoint | Deployment probe | fails (503) when | does not fail for |
|---|---|---|---|
| `/livez` (alias `/healthz`) | `livenessProbe` | the engine loop has made no progress for `LAZYVIEW_LIVENESS_STALL_SECS` (120): the process is wedged and a restart fixes it | long builds or flushes, database outages, binlog lag, hot standby, views in error |
| `/startupz` | `startupProbe` | boot not finished: waiting for the database, migrating metadata, acquiring the lock | — |
| `/readyz` | `readinessProbe` as `/readyz?standby=ok` | starting; hot standby (except with `?standby=ok`); binlog disconnected longer than `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (30); a view with no live table yet (first build); engine stalled; a view in error, but only with `LAZYVIEW_READY_REQUIRE_ALL=true` | a view rebuilding behind its live table, short reconnects |

* `?verbose` lists the individual checks (Kubernetes apiserver style). `?format=json` or
  `Accept: application/json` returns JSON. `?require_all=1` applies the strict view check for a
  single request. `HEAD` works everywhere.
* Views in error do not affect readiness by default. A broken definition is not a fault of the
  pod, and an unready pod would only block rollouts and node drains. The error shows up in the
  `/readyz` body, in `lazyview_views_error` (alert on it) and in `_lazyview_state`.
* Thresholds in the Deployment: startup 5 s × 180 (15 minutes to boot, including waiting for the
  database). Liveness 10 s × 3, so a restart comes about 150 s after the engine loop stalls.
  Readiness 5 s × 3.
* `lazyview healthcheck` (exec form, no shell) probes `/livez`. It works in the distroless image:
  `kubectl -n lazyview exec deploy/lazyview -- /lazyview healthcheck`.
  To look at the endpoints: `kubectl -n lazyview port-forward deploy/lazyview 8080` and then
  `curl 'localhost:8080/readyz?verbose'`.

### Active / standby, rollouts and drains

* Replicas with the same `LAZYVIEW_INSTANCE_ID` share a MySQL named lock on the source. One is
  active. The others are hot standbys: connected, metadata checked, polling the lock. A standby
  takes over a few seconds after the active instance stops (SIGTERM) or loses its connection.
* The readiness probe counts a healthy standby as ready (`/readyz?standby=ok`). Plain `/readyz`
  reports a standby as not ready, which is right for humans and monitoring but breaks a singleton
  workload in two ways. A `RollingUpdate` could never finish, because a new pod cannot become
  ready while an old pod holds the lock. And every PodDisruptionBudget would block node drains.
* Strategy `RollingUpdate` with `maxSurge: 1, maxUnavailable: 0`. The new pod becomes Ready as
  a standby (image, configuration and database access verified), then an old pod is terminated.
  If that pod was the active one, a standby takes over. A broken release never becomes Ready, so
  the running pods keep working. `Recreate` also works, but it stops everything first.
* PDB `maxUnavailable: 1` with `unhealthyPodEvictionPolicy: AlwaysAllow`. Drains take one pod at
  a time, and crash-looping pods never block them. Preferred pod anti-affinity keeps the active
  and standby pods on different nodes.
* `terminationGracePeriodSeconds: 60`. On SIGTERM lazyview stops reading the binlog, flushes, saves
  the position and releases the lock. Being killed later is also safe, because changes since the
  saved position are re-applied idempotently.
* `enableServiceLinks: false` is required. Otherwise a Service named `lazyview` would inject
  `LAZYVIEW_PORT=tcp://…`, and services named `lazyview-target-*` would inject
  `LAZYVIEW_TARGET_*` variables, all of which lazyview reads as configuration.
* Security: UID/GID 65532, read-only root filesystem, all capabilities dropped,
  `allowPrivilegeEscalation: false`, seccomp `RuntimeDefault`, no service account token. This
  meets the `restricted` Pod Security Standard.
* An unspecified `LAZYVIEW_HEALTH_ADDR` (`0.0.0.0:8080`, the default, or `[::]:8080`) listens on
  IPv4 and IPv6, so probes work in IPv6-only and dual-stack clusters.

### Metrics

`lazyview-metrics` also publishes pods that are not ready, so standbys and pods that are still
building get scraped too. Gauges on top of the counters: `lazyview_standby`, `lazyview_starting`,
`lazyview_streaming`, `lazyview_alive`, `lazyview_ready` (plain `/readyz`), `lazyview_views*`,
`lazyview_heartbeat_age_seconds`, `lazyview_binlog_disconnected_seconds`,
`lazyview_applied_position_age_seconds` (this one also grows while the source is idle) and
`lazyview_build_info`. `components/prometheus` alerts when no instance is streaming, when views
are in error, and when views have had no live table for 2 h.

Validate changed manifests offline:
`kustomize build deploy/k8s/overlays/example | kubeconform -strict -summary`.

## Development

* `make test` — unit tests.
* `scripts/test-servers.sh start` — start local MySQL/MariaDB/Percona servers from official
  tarballs (under `tmp/out`); `make test-integration` runs the integration suites against all of them.
* `docker compose -f docker-compose.test.yml up -d --wait` — the same servers as containers.
