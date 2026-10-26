# Operations

## Running with Docker

```sh
docker run -d --name lazyview --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -p 8080:8080 \
  --env-file lazyview.env \
  ghcr.io/kirmorozov/lazyview:edge
```

* `lazyview.env` holds `NAME=value` lines (`LAZYVIEW_HOST=…`, `LAZYVIEW_USER=…`,
  `LAZYVIEW_PASSWORD=…`, `LAZYVIEW_DATABASE=…`). This keeps the password out of your shell history.
  Docker takes the values literally: no quotes, no `$` expansion.
* The image runs as `65532:65532` and writes no files, so `--read-only`, `--cap-drop ALL` and
  `--security-opt no-new-privileges` work.
* `-p 8080:8080` publishes `/metrics`, `/livez`, `/readyz` and `/startupz` outside the container.
* For a database on the Docker host itself, add `--add-host=host.docker.internal:host-gateway`
  and set `LAZYVIEW_HOST=host.docker.internal`.
* The image has no shell, so there is nothing to `docker exec` into. Use
  `docker exec lazyview /lazyview healthcheck`, the HTTP endpoints, the logs and the
  [database tables](#status-from-the-database).
* Image tags: `edge` is the latest build of `main`, `sha-<commit>` is every published build, and
  releases are `X.Y.Z`, `X.Y`, `X` and `latest`. Details: [development](development.md#image).

As a Compose service for an existing database:

```yaml
services:
  lazyview:
    image: ghcr.io/kirmorozov/lazyview:edge
    restart: unless-stopped
    read_only: true
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
    environment:
      LAZYVIEW_HOST: db.example.com
      LAZYVIEW_USER: lazyview
      LAZYVIEW_PASSWORD: change-me
      LAZYVIEW_DATABASE: app
```

Compose expands `$` in the file, so write a `$` in a value as `$$` (`s3cr$$t` for `s3cr$t`).

To try everything at once, [docker-compose.example.yml](../docker-compose.example.yml) starts
MySQL 8.4 (ROW binlog, GTIDs, replication grants) plus lazyview built from the checkout. Its header
shows how to define a first view and how to use a published image instead. It needs Docker
Compose v2.23.1 or newer (inline `configs`).

```sh
docker compose -f docker-compose.example.yml up -d --build --wait
```

## Command line

| command | effect |
|---|---|
| `lazyview` | run the service (configured by environment variables) |
| `lazyview healthcheck` | exit `0` if the running service's `/livez` answers `200` within 3 s, else `1` |
| `lazyview --version` (`-V`, `version`) | print the version |
| `lazyview --help` (`-h`, `help`) | short usage |

Exit codes of the service: `0` after a clean stop (SIGTERM or Ctrl-C), after
`LAZYVIEW_START_MODE=once` and after a successful `LAZYVIEW_SYNC=false` check. `2` for a
configuration error (`configuration error: …`) or an unknown argument. `1` for any other fatal
error (`fatal: …`), including an invalid or schema-less `LAZYVIEW_DSN` and a missing
`LAZYVIEW_TLS_CA` file (see [configuration](configuration.md)). Run it with a restart
policy; it resumes from the saved position. To see the last exit and the restart count:

```sh
docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} restarts={{.RestartCount}}' lazyview
```

`lazyview healthcheck` reads `LAZYVIEW_HEALTH_ADDR` (default `127.0.0.1:8080`) and reaches an
unspecified address (`0.0.0.0`, `[::]`) through loopback. The image's `HEALTHCHECK` runs it in
exec form, so `docker ps` shows `healthy` or `unhealthy`. If you set `LAZYVIEW_HEALTH_ADDR=off`,
also run the container with `--no-healthcheck`.

## Health endpoints

Served on `LAZYVIEW_HEALTH_ADDR` (default `0.0.0.0:8080`). A failed bind is retried every 5 s and
never stops the engine.

| endpoint | `200` when | `503` when | never fails because of |
|---|---|---|---|
| `/livez` (alias `/healthz`) | the engine loop made progress within `LAZYVIEW_LIVENESS_STALL_SECS` (120), or `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` (900) while booting | the process is wedged; a restart fixes it | long builds or flushes, database outages, binlog lag, hot standby, views in error |
| `/startupz` | the engine loop makes progress (the same verdict as `/livez`, with the boot limit while booting); `?verbose` shows the boot phase (booting, hot standby, binlog stream connected) | only when the engine is stalled | waiting for the database, migrating metadata, acquiring the lock |
| `/readyz` | this instance is active, streams the binlog and every view has a live table | starting; hot standby (unless `?standby=ok`); binlog disconnected longer than `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (30); a view without a live table yet (first build; unless `?building=ok`); engine stalled; a view in error, only with `LAZYVIEW_READY_REQUIRE_ALL=true` | a view rebuilding behind its live table, short reconnects |
| `/metrics` | always (Prometheus text format) | — | — |

* `?verbose` lists the individual checks. `?format=json` or `Accept: application/json` returns
  JSON. `?require_all=1` (or `=0`) overrides `LAZYVIEW_READY_REQUIRE_ALL` for one request.
  `?standby=ok` counts a healthy hot standby as ready. `?building=ok` does not fail readiness for
  views still building their first version (the Kubernetes probe uses both). `HEAD` works
  everywhere.
* Views in error do not affect readiness by default. A broken definition is not a fault of the
  instance. The error shows up in the `/readyz` body, in `lazyview_views_error` (alert on it) and
  in `_lazyview_state`.

```sh
curl -s 'localhost:8080/readyz?verbose'
curl -s 'localhost:8080/readyz?format=json'
```

## Metrics

`/metrics` in Prometheus format.

Counters (`lazyview_<name>_total`): `events` (binlog events), `rows_seen`, `rows_skipped` (rows
no view needed), `flushes`, `units_refreshed`, `units_changed`, `lookup_queries`,
`source_queries`, `mv_queries`, `mv_rows_inserted` (rows written by `INSERT`, upserts
included), `mv_rows_upserted` (of those, rows upserted by their row key), `mv_rows_deleted`,
`full_rebuilds` (every table build: first builds, changed definitions and full-mode refreshes),
`definition_reloads`, `plans_timed_out` (definitions whose planning took longer than
`LAZYVIEW_PLAN_TIMEOUT_MS`), and for [concurrent refreshes](configuration.md#concurrent-refreshes)
`supervisor_overload_errors` (ops that failed with an overload error), `supervisor_retries`,
`supervisor_zombie_kills` (writes killed after a timeout inside their transaction) and
`supervisor_cpu_offloads` (diffs run off the async threads). The binlog keeps being read while a
flush runs, up to 100,000 events or 32 MiB queued: `flushes_overlapped` counts the flushes during
which it was read, `binlog_backpressure` those during which the queue was full (the stream then
waits).

Per connection pool of concurrent refreshes, labelled
`{pool="read|write", endpoint="source|<target>"}`: `lazyview_supervisor_limit`, `_max` (pool
size), `_inflight`, `_peak_inflight` and `_queued` (ops waiting for a permit).
`lazyview_inflight_rows`: rows read from the sources and not yet written to every target.

Gauges:

| gauge | meaning |
|---|---|
| `lazyview_views` | view definitions loaded |
| `lazyview_views_ready` | views with a live, error-free version |
| `lazyview_views_building` | views with a version building in the background |
| `lazyview_views_error` | views in error (see `_lazyview_state.last_error`) |
| `lazyview_streaming` | 1 while connected to the binlog stream |
| `lazyview_standby` | 1 while waiting for another instance's lock (hot standby) |
| `lazyview_starting` | 1 until the binlog stream is first connected |
| `lazyview_alive`, `lazyview_ready` | 1 while `/livez` and plain `/readyz` pass |
| `lazyview_heartbeat_age_seconds` | seconds since the engine loop last reported progress |
| `lazyview_liveness_limit_seconds` | heartbeat age at which `/livez` fails (`LAZYVIEW_LIVENESS_STALL_SECS`, or the boot limit while booting) |
| `lazyview_binlog_disconnected_seconds` | seconds an instance that streamed before has been without a connected binlog stream (0 while streaming, on standby and before the first connect) |
| `lazyview_applied_position_age_seconds` | seconds since the applied binlog position last changed; also grows while the source is idle. Present once a position has been applied |
| `lazyview_applied_binlog_pos{file}` | binlog position up to which every change has been applied |
| `lazyview_build_info{version}` | always 1 |

Alerts worth having: `lazyview_streaming == 0` for 10 minutes, `lazyview_views_error > 0`, and
views without a live table for a long time (`lazyview_views - lazyview_views_ready -
lazyview_views_error > 0` for 2 h). [docs/kubernetes.md](kubernetes.md#metrics) has these as a
PrometheusRule.

## Logs

Logs go to stdout (`docker logs -f lazyview`). `LAZYVIEW_LOG_FORMAT=json` writes one JSON object
per line. The default level `info` logs lifecycle events and problems only:

| message | meaning |
|---|---|
| ``connected to <version> at host:port, schema `app` `` | startup reached the source |
| `waiting for source database at host:port: …; retrying in …` | the database is unreachable; lazyview keeps retrying (backoff up to 30 s) |
| `another lazyview instance … is active on schema …; standing by` | hot standby (see [below](#duplicate-instances-and-hot-standby)) |
| `took over as active instance` | a standby became active |
| `building view=… targets=… mode=…` | a view is being built; `mode` is, for example, `incremental (grouped)` or `full rebuild (non-deterministic function NOW)` |
| `live view=… targets=…` | a newly built table was swapped in |
| `ready: N view(s) live` | every view has a table (`, M in error` if some failed) |
| `streaming binlog position=…` | reading the binlog from that position |
| `definition changed; building the new version online (current version stays live)` | an `UPDATE` of `_lazyview` was picked up |
| `source schema changed; rebuilding online` | a source table was altered |
| `no views defined yet; add one with: INSERT INTO …` | `_lazyview` is empty |

`RUST_LOG=lazyview=debug` adds a summary every 60 s (`last 60s: N binlog events, …`) and the
routine full-mode rebuilds.

## Status from the database

Without touching the container:

```sql
SELECT * FROM _lazyview_overview;                     -- every view: state, mode, last error
SELECT name, target, state, last_error, refreshed_at
  FROM _lazyview_state WHERE state <> 'ready';        -- what is building or broken
SELECT * FROM _lazyview_status;                       -- saved binlog position per LAZYVIEW_INSTANCE_ID
SELECT id, user, host, time FROM information_schema.PROCESSLIST
 WHERE command LIKE 'Binlog Dump%';                   -- lazyview's replication connection
```

## Start modes

| `LAZYVIEW_START_MODE` | behaviour |
|---|---|
| `resume` (default) | continue from the position saved in `_lazyview_status`. Tables whose definition is unchanged and whose columns still match are reused; the others are rebuilt. Without a saved position (first start) every view is built |
| `rebuild` | rebuild every view, then stream from the current binlog position |
| `once` | rebuild every view, also those already built, and exit: `0` if all succeeded, `1` if any failed |

`LAZYVIEW_SYNC=false` only creates or migrates the metadata tables and validates the definitions
(see [views](views.md#defining-a-view)).

## Restart and resume

* On SIGTERM (or Ctrl-C), lazyview stops reading the binlog, flushes buffered changes, saves the
  position and releases its lock.
* Being killed is safe as well. The changes since the saved position are applied again, and that
  gives the same result.
* While idle, the position is saved every `LAZYVIEW_POSITION_PERSIST_MS` (5 s).
* After a fatal error, lazyview exits non-zero. Lost connections are not fatal: the binlog stream
  reconnects with backoff, and the source connection is re-established.
* If the saved binlog file has been purged in the meantime, lazyview fails with `saved binlog
  position … is no longer available`. Set `LAZYVIEW_ON_MISSING_BINLOG=rebuild` (or start once
  with `LAZYVIEW_START_MODE=rebuild`) to rebuild instead. Keep binlogs long enough
  ([server setup](server-setup.md#requirements)).

## Duplicate instances and hot standby

Instances with the same `LAZYVIEW_INSTANCE_ID` on the same schema share a MySQL named lock
(`GET_LOCK`) on the source. Only the holder is active. A second instance logs `standing by`,
stays connected, and takes over a few seconds after the active one stops or loses its connection.
`/readyz` reports a standby as not ready unless asked with `?standby=ok`.

Every takeover increments `_lazyview_status.epoch`, and every position write checks it. An
instance that lost the lock without noticing stops at its next flush with `another lazyview
instance took over …; stopping`.

A single instance with a restart policy is enough for most setups. A standby only shortens the gap
after a failure. Do not run instances with *different* `LAZYVIEW_INSTANCE_ID`s against the same
schema: each would maintain the same views.

## Troubleshooting

| error or symptom | fix |
|---|---|
| `configuration error: no database configured` (exit 2) | set `LAZYVIEW_HOST`, `LAZYVIEW_USER`, `LAZYVIEW_PASSWORD`, `LAZYVIEW_DATABASE` or `LAZYVIEW_DSN` |
| `server … does not meet lazyview's binlog requirements` | each listed item says what to change; see [server setup](server-setup.md) |
| `access denied for user …: check LAZYVIEW_USER / LAZYVIEW_PASSWORD` | wrong credentials, or the user may not connect from this host |
| `the lazyview database user lacks privileges; as an admin run: GRANT …` | run the printed `GRANT`s |
| `registering as a replica was refused` | `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO …` |
| ``database `app` does not exist`` | `CREATE DATABASE app`, or fix `LAZYVIEW_DATABASE` |
| `TLS to source database at … failed` | set `LAZYVIEW_TLS_CA` to the server's CA bundle, or use `LAZYVIEW_TLS=required`; see [TLS](configuration.md#tls) |
| `fatal: TLS CA file … not found` (exit 1) | the path in `LAZYVIEW_TLS_CA` does not exist in the container (mount it) |
| `saved binlog position … is no longer available on the server` | binlogs were purged while lazyview was down; see [restart and resume](#restart-and-resume) |
| `another replica connected with the same server_id N` | give every lazyview instance and every real replica a unique `LAZYVIEW_SERVER_ID` |
| `Amazon RDS/Aurora binlog retention is …` (warning) | `CALL mysql.rds_set_configuration('binlog retention hours', 24);` |
| a view in `error`: ``name `x` already used by row #N`` | the view name of its `CREATE VIEW` statement belongs to row N; rename one of the two views |
| `another definition already materializes …` | two rows define the same view; remove one |
| `refusing to replace existing base table … that lazyview does not manage` | a table with that name exists; rename or drop it, or pick another view name |
| ``table `app`.`t` does not exist`` | create the table; the view is retried when it appears |
| `unknown target …` | configure `LAZYVIEW_TARGET_<NAME>_*`, or fix `_lazyview.targets` |
| ``chained views need the upstream view on target `source` `` | add `source` to the upstream view's `targets` |
| `view calls stored function …` | stored and loadable functions are not supported; inline the logic |
| a view is `full` but should be incremental | `detail` names the construct; see [views](views.md#what-makes-a-view-full). `SELECT name, detail FROM _lazyview_overview WHERE mode = 'full' AND state <> 'error'` lists them (a view in `error` keeps the mode of its plan) |
| a full-mode view lags | it is rebuilt at most once per `LAZYVIEW_FULL_REFRESH_MS`; lower it, or make the view incremental |
| the container is `unhealthy` with `LAZYVIEW_HEALTH_ADDR=off` | run it with `--no-healthcheck` |
| `/readyz` stays 503 on an otherwise fine instance | `curl 'localhost:8080/readyz?verbose'` names the failing check |

## Upgrading

1. Pin the new version (`ghcr.io/kirmorozov/lazyview:X.Y.Z`) and restart the container, or roll
   the Deployment ([Kubernetes](kubernetes.md#rollouts-drains-and-standby)).
2. At startup, lazyview adds missing columns to its metadata tables. This only adds columns and
   is safe to run repeatedly.
3. With `LAZYVIEW_START_MODE=resume` (the default) it continues from the saved position. Views
   whose definition and table layout are unchanged are reused; the rest are rebuilt online.
   Select/join views over several tables are rebuilt once when an upgrade adds hidden lineage
   columns to their layout (see [the materialized table](views.md#the-materialized-table)).
4. Views that are resumed keep their previous `_lazyview_state.mode` and `detail` (`detail` is
   NULL after the upgrade that introduced it) until they are next rebuilt.
   `LAZYVIEW_SYNC=false` with another `LAZYVIEW_INSTANCE_ID` prints the current classification of
   every definition.
