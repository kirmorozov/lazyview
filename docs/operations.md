# Operations

## The two runs

lazyview has exactly two kinds of runs, and each account only needs what its run does (the
accounts and their `GRANT`s: [server setup](server-setup.md#advanced-setup-recommended-for-production)):

| run | command | what it does | account |
|---|---|---|---|
| setup and migration | `lazyview setup` | creates and migrates lazyview's own tables (`_lazyview`, `_lazyview_state`, `_lazyview_status`, `_lazyview_fence`) in the configured schema, validates every enabled definition and logs its mode and plan; exits `0` when all is ready, `1` when a definition is invalid. Idempotent, and it builds no view tables and never moves the saved binlog position | setup (DDL on lazyview's own tables) |
| operations | `lazyview` (or `lazyview run`) | streams the binlog and maintains the views. It never issues DDL on lazyview's own tables: it checks them through `information_schema` with `SELECT` rights only, and a missing table or column is a startup error (exit `2`) naming the fix — run `lazyview setup` with the setup account | consumption (no DDL on lazyview's own tables) |

View tables are **not** lazyview's own tables: their DDL (build, swap, drop) stays in the
operations run, as always. `lazyview setup` is also a CI check: an invalid definition is exit `1`.
It takes the instance lock first, so next to a running instance give it another
`LAZYVIEW_INSTANCE_ID` (for example `ci`).

`LAZYVIEW_SYNC=false` is a **deprecated alias of `lazyview setup`** (one release): it logs a WARN
and behaves exactly like the setup run. Replace it in scripts and CI with `lazyview setup`, run
with the setup account.

## Running with Docker

```sh
docker run -d --name lazyview --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -p 8080:8080 \
  --env-file lazyview.env \
  ghcr.io/kirmorozov/lazyview:0.1.0
```

* `lazyview.env` holds `NAME=value` lines (`LAZYVIEW_HOST=…`, `LAZYVIEW_USER=…`,
  `LAZYVIEW_PASSWORD=…`, `LAZYVIEW_DATABASE=…`). This keeps the password out of your shell history.
  Docker takes the values literally: no quotes, no `$` expansion.
* Before the first start of the service, and once per upgrade, run the setup run with the same
  file ([the two runs](#the-two-runs)): `docker run --rm --env-file lazyview.env
  ghcr.io/kirmorozov/lazyview:0.1.0 setup`. It creates and migrates lazyview's own tables and
  validates the definitions; the operations run above creates and alters none of them and refuses
  to start until they exist.
* The image runs as `65532:65532` and writes no files, so `--read-only`, `--cap-drop ALL` and
  `--security-opt no-new-privileges` work.
* The endpoint binds `127.0.0.1:8080` inside the container, which is not the host: `-p 8080:8080`
  publishes `/metrics`, `/stats`, `/livez`, `/readyz` and `/startupz` only together with
  `LAZYVIEW_HEALTH_ADDR=0.0.0.0:8080` in `lazyview.env`. Before 0.1.0 the default was
  `0.0.0.0:8080`, so an upgrade that scraped `/metrics` from the host has to add that line once.
  The image's own `HEALTHCHECK` and `docker exec lazyview /lazyview healthcheck` need nothing: they
  run inside the container.
* A password can stay out of the environment: `LAZYVIEW_PASSWORD_FILE` names a file the container
  reads at startup. With `-v ./secrets:/run/secrets:ro`:

  ```sh
  echo 'change-me' > secrets/lazyview_password    # 0400, readable by uid 65532
  echo 'LAZYVIEW_PASSWORD_FILE=/run/secrets/lazyview_password' >> lazyview.env
  ```

  `LAZYVIEW_PASSWORD=file:///run/secrets/lazyview_password` does the same; a variable that holds a
  `file://`, `aws-secret://` or `plain://` prefix is a reference, everything else is the value
  ([Secrets](configuration.md#secrets)).
* For a database on the Docker host itself, add `--add-host=host.docker.internal:host-gateway`
  and set `LAZYVIEW_HOST=host.docker.internal`.
* The image has no shell, so there is nothing to `docker exec` into. Use
  `docker exec lazyview /lazyview healthcheck`, the HTTP endpoints, the logs and the
  [database tables](#status-from-the-database).
* Image tags: releases are `X.Y.Z`, `X.Y` and `latest` (for 0.x there is no bare `X` tag), `edge`
  is built from every push to `main` and may be unstable (use a release for production),
  `sha-<commit>` is every published build.
  Details: [development](development.md#image).

As Compose services for an existing database — the two runs, so the one-shot setup run comes
first and the service starts only once it exits `0`
([docker-compose.example.yml](../docker-compose.example.yml) is this, with a database of its own):

```yaml
services:
  lazyview-setup:                            # the setup run: once, at install and at upgrade
    image: ghcr.io/kirmorozov/lazyview:0.1.0
    command: ["setup"]
    restart: "no"
    healthcheck:
      disable: true                          # the image's HEALTHCHECK probes the service
    read_only: true
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
    environment: &db
      LAZYVIEW_HOST: db.example.com
      LAZYVIEW_USER: lazyview
      LAZYVIEW_PASSWORD: change-me
      LAZYVIEW_DATABASE: lazyview
  lazyview:                                  # the operations run
    image: ghcr.io/kirmorozov/lazyview:0.1.0
    restart: unless-stopped
    depends_on:
      lazyview-setup:
        condition: service_completed_successfully
    read_only: true
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
    environment:
      <<: *db
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
| `lazyview` (`lazyview run`) | run the service — the operations run (configured by environment variables); it never issues DDL on lazyview's own tables |
| `lazyview setup` | the setup and migration run: create and migrate lazyview's own tables, validate every enabled definition and log its mode and plan, then exit (`0` ready, `1` a definition is invalid). Run it as the setup account, once at install and once per upgrade |
| `lazyview healthcheck` | exit `0` if the running service's `/livez` answers `200` within 3 s, else `1` |
| `lazyview --version` (`-V`, `version`) | print the version |
| `lazyview --help` (`-h`, `help`) | short usage |

Exit codes of the service: `0` after a clean stop (SIGTERM or Ctrl-C), after
`LAZYVIEW_START_MODE=once` and after a successful `lazyview setup`. `2` for a
configuration error (`configuration error: …`), an unknown argument, and for a missing
table or column of lazyview's own (the error names `lazyview setup` with the setup account as
the fix). `1` for any other fatal
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

Served on `LAZYVIEW_HEALTH_ADDR` (default `127.0.0.1:8080`; `off` disables it). A container or a pod
that has to be scraped from outside sets `0.0.0.0:8080` explicitly. A failed bind is retried every
5 s and never stops the engine.

With `LAZYVIEW_HEALTH_TOKEN` (or `LAZYVIEW_HEALTH_TOKEN_FILE`, see
[Secrets](configuration.md#secrets)) set, `/metrics` and `/stats` answer only to
`Authorization: Bearer <token>`; a request without it gets `401` with
`WWW-Authenticate: Bearer realm="lazyview"`. The probes do not look at the token, so kubelet,
Docker's `HEALTHCHECK` and `lazyview healthcheck` keep working unchanged.

| endpoint | `200` when | `503` when | never fails because of |
|---|---|---|---|
| `/livez` (alias `/healthz`) | the engine loop made progress within `LAZYVIEW_LIVENESS_STALL_SECS` (120), or `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` (900) while booting | the process is wedged; a restart fixes it | long builds or flushes, database outages, binlog lag, hot standby, views in error |
| `/startupz` | the engine loop makes progress (the same verdict as `/livez`, with the boot limit while booting); `?verbose` shows the boot phase (booting, hot standby, binlog stream connected) | only when the engine is stalled | waiting for the database, migrating metadata, acquiring the lock |
| `/readyz` | this instance is active, streams the binlog and every view has a live table | starting; hot standby (unless `?standby=ok`); binlog disconnected longer than `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (30); a view without a live table yet (first build; unless `?building=ok`); engine stalled; a view in error, only with `LAZYVIEW_READY_REQUIRE_ALL=true` | a view rebuilding behind its live table, short reconnects |
| `/metrics` | always (Prometheus text format); with `LAZYVIEW_HEALTH_TOKEN` set, only for `Authorization: Bearer <token>` | — | — |
| `/stats` | always (JSON, [per-view update statistics](#per-view-update-statistics)); with `LAZYVIEW_HEALTH_TOKEN` set, only for `Authorization: Bearer <token>` | — | — |

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
curl -s -H "Authorization: Bearer $(cat /run/secrets/lazyview_health_token)" \
  'localhost:8080/metrics' | head
# without the header, when a token is set:
#   HTTP/1.1 401 Unauthorized
#   WWW-Authenticate: Bearer realm="lazyview"
```

## Metrics

`/metrics` in Prometheus format. With `LAZYVIEW_HEALTH_TOKEN` set, Prometheus needs the same token:

```yaml
scrape_configs:
  - job_name: lazyview
    metrics_path: /metrics
    authorization:
      # the file's content, trailing newline trimmed
      credentials_file: /etc/prometheus/lazyview_token
    static_configs:
      - targets: ["lazyview:8080"]
```

Keep the probes unauthenticated (they are, by design) and gate only what describes the workload:
`/metrics` and `/stats` name your views, their tables, error counts and row volumes.

Counters (`lazyview_<name>_total`): `events` (binlog events), `rows_seen`, `rows_skipped` (rows
no view needed), `flushes`, `units_refreshed`, `units_changed`, `lookup_queries`,
`source_queries`, `mv_queries`, `mv_rows_inserted` (rows written by `INSERT`, upserts
included), `mv_rows_upserted` (of those, rows upserted by their row key), `mv_rows_deleted`,
`full_rebuilds` (every table build: first builds, changed definitions and full-mode refreshes),
`definition_reloads`, `plans_timed_out` (definitions whose planning took longer than
`LAZYVIEW_PLAN_TIMEOUT_MS`), `stats_batches_dropped` (flush batches of per-view statistics dropped
because the aggregator lagged; see [per-view update statistics](#per-view-update-statistics)), and
for [concurrent refreshes](configuration.md#concurrent-refreshes)
`supervisor_overload_errors` (ops that failed with an overload error), `supervisor_retries`,
`supervisor_zombie_kills` (writes killed after a timeout inside their transaction) and
`supervisor_cpu_offloads` (diffs run off the async threads). The binlog keeps being read while a
flush runs, up to 100,000 events or 32 MiB queued: `flushes_overlapped` counts the flushes during
which it was read, `binlog_backpressure` those during which the queue was full (the stream then
waits). `binlog_backpressure` also counts the pauses while the changes buffered for building views
exceed their 256 MiB budget (logged as `binlog reading paused until a build is applied`): every
view lags meanwhile. `cascade_rows` sums over the views the child rows a mapped cascade changed,
replayed from the in-memory cascade cache. `short_row_images` counts the rows events lazyview
refused because an image left out a column a view reading that table needs by position
([a session that writes short row images](server-setup.md#a-session-that-writes-short-row-images)).

The leadership lock ([duplicate instances](#duplicate-instances-and-hot-standby)) keeps its own
counters: `leader_beat_failures` (beats of the lock's own session that were not answered: the
session is dropped, re-opened and the lock confirmed again, so a GC pause or a proxy blip shows here
and is not fatal), `leader_uncertain` (episodes of two periods without an answered beat, during
which every write gate was closed and the writes in flight ended through the zombie fence until the
lock was confirmed again — the instance is not lost, but each entry is a stall of its refreshes)
and `leader_fenced` (this instance was replaced: another session holds the lock or the fencing epoch
moved while its session was away).

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

Per view, labelled `{view="<schema>.<view>"}`: the counters of
[`/stats`](#per-view-update-statistics) as `lazyview_view_<name>_total` (`source_rows`,
`source_rows_skipped`, `ids_marked`, `ids_absorbed`, `images_marked`, `images_absorbed`,
`cascade_rows`, `cascade_gaps`, `entities_updated`,
`rows_inserted`, `rows_updated`, `rows_deleted`, `rows_unchanged`,
`whole_view_refreshes`, `builds`, `source_queries`, `lookup_queries`, `flushes`) and the gauges
`lazyview_view_ids_pending`, `lazyview_view_cascade_cached_rows` and
`lazyview_view_last_flush_timestamp_seconds`.

Alerts worth having: `lazyview_streaming == 0` for 10 minutes, `lazyview_views_error > 0`, and
views without a live table for a long time (`lazyview_views - lazyview_views_ready -
lazyview_views_error > 0` for 2 h). [docs/kubernetes.md](kubernetes.md#metrics) has these as a
PrometheusRule. An incremental view rebuilt as a whole (`increase(lazyview_view_whole_view_refreshes_total[1h]) > 0`
on a view whose `_lazyview_state.mode` is `incremental`) is worth a look too: row changes never
cause that, so its reason (in `/stats`) says what did. Of the lock's own counters,
`lazyview_leader_fenced_total` rising on an instance you did not restart says a second instance
took the lock, and `lazyview_leader_uncertain_total` rising steadily says the lock server keeps
going silent on it — each entry is a stall of that instance's refreshes. A
`lazyview_short_row_images_total` that grows is a writing session with `binlog_row_image` below
`FULL` ([server setup](server-setup.md#a-session-that-writes-short-row-images)).

## Per-view update statistics

`GET /stats` returns, per view, whether the work follows the change: an update of one row may
refresh rows in several views, but never a whole view. The numbers are kept in memory only: they
start at zero when the process starts (`since_unix`) and are never written to the database.

The numbers are best effort. Each flush hands them to a background task, which folds them and
publishes the snapshot `/stats` and `/metrics` serve, so they may lag the binlog by about a
second. When that task falls 64 flushes behind, a flush's batch is dropped instead of slowing the
engine and counted in `batches_dropped` (`/stats`) and `lazyview_stats_batches_dropped_total`
(`/metrics`). After a drop the counters are low and `balanced` may be false.

```sh
curl -s localhost:8080/stats | jq '.views["mydb.my_view"]'
```

```json
{ "since_unix": 1792990000, "now_unix": 1792990321, "batches_dropped": 0,
  "views": { "<schema>.<view>": { "mode": "incremental", "ids_marked": 12000, "...": "..." } } }
```

| field | meaning |
|---|---|
| `mode` | as `_lazyview_state.mode`: `incremental` or `full` |
| `source_rows`, `source_rows_skipped` | binlog row changes fed to the view (once per occurrence of their table in the definition), and of those the ones skipped as irrelevant (no column the view reads changed, or the row fails the view's constant filters) |
| `ids_marked` | target ids (the view's units: group keys, or the key a row of the view hangs off) marked for a refresh, before deduplication. An update that keeps its id marks it once; one that moves a row marks the old and the new id. Ids found by lookups of row images are marked when the flush resolves them. A partial unit key (a table that carries only part of the unit; every unit matching it is refreshed) counts as one id |
| `ids_absorbed` | marks of an id that was already pending (absorbed by deduplication) |
| `ids_pending` | ids buffered and not refreshed yet |
| `images_marked`, `images_absorbed` | row images of tables that do not carry the target id, buffered for a lookup at the flush, and of those the ones already buffered |
| `cascade_rows` | child rows replayed from the in-memory cascade cache for a parent delete or key update ([mapped cascade routes](views.md#foreign-key-cascades)) |
| `cascade_gaps` | parent changes replayed from before the cache's startup seed: counted and warned about at most once a minute, no recovery — the rows heal on their next change |
| `cascade_cached_rows` | child rows kept in the live version's in-memory cascade cache; the view holds one entry per kept row |
| `entities_updated` | target ids refreshed after deduplication (each id once per flush), summed over flushes |
| `rows_inserted`, `rows_updated`, `rows_deleted`, `rows_unchanged` | view rows written by refreshes (summed over targets): new, replaced in place, deleted, and compared but equal (not written) |
| `whole_view_refreshes`, `last_whole_view_refresh` | builds that replaced a live version of the view (a full-mode refresh, affected ids unknown, a changed definition or source schema, a target rebuilt after it was down), with the last one's `reason` and `unix` time |
| `builds` | first builds (no live version yet) |
| `source_queries`, `lookup_queries` | source reads and lookups the view's refreshes ran |
| `flushes`, `last_flush_unix` | refreshes of the view, and the last one's time |
| `balanced` | `ids_marked == ids_absorbed + entities_updated + ids_pending` |

Every mark either finds its id pending or adds it to the buffer, from which the next flush
refreshes it once, so `balanced` is true after every flush unless batches were dropped. A
whole-view refresh drops the ids it covers; after one, `ids_marked` may be larger. `ids_marked /
entities_updated` shows how much deduplication saves. For an incremental view
`whole_view_refreshes` stays 0: a value above 0 is an update storm, and
`last_whole_view_refresh.reason` says why. There are no recent-window fields; for a recent rate
use `/metrics`, e.g. `rate(lazyview_view_ids_marked_total[1m])` or
`rate(lazyview_view_entities_updated_total[1m])`.

`scripts/stats-check.sh [URL]` (curl + jq; URL defaults to `http://127.0.0.1:8080/stats`) prints
marked / absorbed / updated / pending / whole-view refreshes per view and exits 1 when an
incremental view was refreshed whole or a view's counters do not balance (2 when `/stats` cannot
be read); with `batches_dropped` above 0 it prints a note and does not fail on balance. Run it
after a load test to confirm deduplication works. Full-mode views are listed but never fail it.

Cost: per binlog row and view, a few additions to plain integers in the view's change buffer
(whose insert already says whether the id was new). Everything else is handed to a background
task once per flush (one non-blocking channel send, no lock on the engine's path), which folds it
and publishes a snapshot. Every statistic is a counter summed since start (or a gauge): no per-id
state, so the memory per view is constant. The
in-memory cascade cache adds one entry per kept child row of a view's mapped cascade routes:
`cascade_cached_rows` shows how many, so a view over a large child table the plan cannot trace
through the parent's row grows with that table's kept rows.

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
| `source schema changed; the view reads the same columns: new column positions, no rebuild` | a source table was altered without changing what the view reads |
| `source schema changed; rebuilding online` (with the difference in parentheses when re-planned on the spot) | a source table was altered in a way that changes what the view reads, or a change could not be checked in place |
| `no views defined yet; add one with: INSERT INTO …` | `_lazyview` is empty |
| `the cascade cache missed binlog events (gap); the kept rows under this parent key may be stale` (warning, at most once a minute per view) | a parent delete or key update was replayed from before the cache's startup seed; it is counted in `cascade_gaps` and the kept rows heal on their next change |

`RUST_LOG=lazyview=debug` adds a summary every 60 s (`last 60s: N binlog events, …`) and the
routine full-mode rebuilds.

## Status from the database

Without touching the container:

```sql
SELECT * FROM _lazyview_overview;                     -- every view: state, mode, last error
SELECT name, target, state, last_error, refreshed_at
  FROM _lazyview_state WHERE state <> 'ready';        -- what is building or broken
SELECT * FROM _lazyview_status;                       -- saved binlog position per LAZYVIEW_INSTANCE_ID
SELECT * FROM _lazyview_fence;                        -- Oracle MySQL without GTIDs: visibility-fence counter per LAZYVIEW_INSTANCE_ID (safe to drop; re-created)
SELECT id, user, host, time FROM information_schema.PROCESSLIST
 WHERE command LIKE 'Binlog Dump%';                   -- lazyview's replication connection
```

## Start modes

| `LAZYVIEW_START_MODE` | behaviour |
|---|---|
| `resume` (default) | continue from the position saved in `_lazyview_status`. Tables whose definition is unchanged and whose columns still match are reused; the others are rebuilt. Without a saved position (first start) every view is built |
| `rebuild` | rebuild every view, then stream from the current binlog position |
| `once` | rebuild every view, also those already built, and exit: `0` if all succeeded, `1` if any failed |

Neither mode creates or migrates lazyview's own tables: that is the job of
[`lazyview setup`](#the-two-runs), which also checks every definition without building anything
(see [defining a view](views.md#defining-a-view)).

## Restart and resume

* On SIGTERM (or Ctrl-C), lazyview stops reading the binlog, flushes buffered changes, saves the
  position and releases its lock.
* Being killed is safe as well. The changes since the saved position are applied again, and that
  gives the same result.
* While idle, the position is saved every `LAZYVIEW_POSITION_PERSIST_MS` (5 s). It also moves on
  while nothing is written: after a binlog rotation (`FLUSH BINARY LOGS`, a server restart) the
  saved and reported position follow the server into the new file at the next heartbeat
  (`LAZYVIEW_HEARTBEAT_MS`, 5 s).
* After a fatal error, lazyview exits non-zero. Lost connections are not fatal: the binlog stream
  reconnects with backoff, and the source connection is re-established. That includes a server
  shutting down or restarting (`Server shutdown in progress`) and sessions killed by an admin or a
  proxy; lazyview waits for the server and goes on.
* At startup every definition is planned again, views in error included: a view stays in
  `error` only if it still fails (with a fresh `last_error` and `updated_at`), and the state rows
  of definitions deleted while lazyview was stopped are removed.
* If the saved binlog file has been purged in the meantime, lazyview fails with `saved binlog
  position … is no longer available`, and before it exits every view's state row says so
  (`state = 'error'`, `last_error` starting with `stale: lazyview stopped:`), since the views no
  longer follow the source. Other reasons the position cannot be read (a position saved on
  another server, a replication error while the file still exists) stop lazyview too but leave
  the state rows as they are: once the cause is fixed, the next start resumes. Set
  `LAZYVIEW_ON_MISSING_BINLOG=rebuild` (or start once with `LAZYVIEW_START_MODE=rebuild`) to
  rebuild instead; the rebuild clears it. Keep binlogs long enough
  ([server setup](server-setup.md#requirements)).

## Duplicate instances and hot standby

Instances with the same `LAZYVIEW_INSTANCE_ID` on the same schema share a MySQL named lock
(`GET_LOCK`) on the source. Only the holder is active. A second instance logs `standing by`,
stays connected, and takes over a few seconds after the active one stops or loses its connection.
`/readyz` reports a standby as not ready unless asked with `?standby=ok`.

A named lock has no expiry: it is freed when its session ends, so the lock lives on a connection of
its own, beaten there by a task of its own — a `DO 1` and an ownership check every
`LAZYVIEW_LOCK_HEARTBEAT_MS` (5 s), never on the binlog loop. That session also sets its own
`wait_timeout` to `LAZYVIEW_LOCK_LEASE_MS` (20 s). While the process lives, its beats keep the
session; when the process is gone — killed hard, its node powered off, or cut off so that nothing
ever closes the connection — nobody beats, the server waits out the lease, ends the session, and
the lock dies with it. The lock is thus *released* by the server, with nothing on the lazyview side
having to notice anything, and the replacement streams after about one lease plus its own poll
(≤ 5 s) and its start. Lowering the source's `wait_timeout` or the database host's TCP keepalive is
no longer needed for that recovery; it stays harmless. Set the lease from your recovery objective:
it is the time a partitioned node still holds the lock, and the biggest part of how long a
replacement waits.

The other direction is the beats noticing: every statement the lock's session sends carries a
client-side deadline of one `LAZYVIEW_LOCK_HEARTBEAT_MS`, and two periods without an answered beat
close every write gate at once — flushes, the saved position, the state rows, the refresh writes,
and the target DDL (a rebuild's `RENAME TABLE` swap, dropping a shadow or an owned table, reclaiming
an orphan) — while the writes already in flight are ended through the zombie fence instead of being
left to run to `LAZYVIEW_TARGET_TIMEOUT_MS`. Checking a gate is one read of the lock task's own
state, so it is not work on the event path. The task then re-opens the session and asks for the
lock without waiting (`GET_LOCK(name, 0)`: the answer is immediate, or the server is not there). A
fencing epoch that moved while the session was away says this instance was replaced: it logs
`fenced: …; this instance is the replaced one and stops at its next check` at WARN and exits without
touching the state rows the replacement now owns; an unchanged epoch means the lock is merely busy,
and it stays gated and tries again on the next beat. A merely slow instance is not replaced: its
beats are answered by their own task, so a busy binlog loop, a long catch-up or a swap waiting its
`lock_wait_timeout` never loses the lock.

Every takeover increments `_lazyview_status.epoch`, and every position write checks it. An
instance that lost the lock without noticing stops at its next flush with `another lazyview
instance took over …; stopping`.

A single instance with a restart policy is enough for most setups. A standby only shortens the gap
after a failure. Do not run instances with *different* `LAZYVIEW_INSTANCE_ID`s against the same
schema: each would maintain the same views.

### One instance per target

One instance maintains the views of all its targets under one saved position: a
target that is down, or a view whose refresh keeps failing while it shows
`ready`, holds that position and so holds back everything else in the instance.
When a target that may be down must not hold back the others, run a separate
lazyview instance per target (per group of targets): each instance has its own
position, lock and failure domain, so a stuck target delays only its own
instance.

* Each instance needs its own `LAZYVIEW_DATABASE`. This is the warning above
  turned into a rule: different instances mean different schemas, never two
  instance ids on one schema.
* The costs: one more full binlog stream and connection set on the source per
  instance, and the instances wake each other with a position write at most
  every 5 s (`LAZYVIEW_POSITION_PERSIST_MS`).
* Give each instance the grants of its own schema as usual; with the
  [advanced setup](server-setup.md#advanced-setup-recommended-for-production),
  each instance's service account touches only its own control schema.

The pattern isolates target failures. Inside one instance it does not help
against a `ready` view whose refresh fails on every attempt: it still holds the
position for the views of its own instance.

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
| `saved binlog position … is no longer available on the server` (also in `last_error` of every view) | binlogs were purged while lazyview was down; see [restart and resume](#restart-and-resume) |
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
| a view is on the `viewplan` route (it is refreshed through the view plan, not by per-table keys) | `access` in `_lazyview_overview` names the occurrence (or cascade) and why, e.g. `` `g` (`groups`): lineage only: …`` or `cascade … via … untraced`; the plan log prints the same text at build time (`RUST_LOG=info`, the default) |
| a full-mode view lags | it is rebuilt at most once per `LAZYVIEW_FULL_REFRESH_MS`; lower it, or make the view incremental |
| the container is `unhealthy` with `LAZYVIEW_HEALTH_ADDR=off` | run it with `--no-healthcheck` |
| `/readyz` stays 503 on an otherwise fine instance | `curl 'localhost:8080/readyz?verbose'` names the failing check |

## Upgrading

1. Pin the new version (`ghcr.io/kirmorozov/lazyview:X.Y.Z`) and restart the container, or roll
   the Deployment ([Kubernetes](kubernetes.md#rollouts-drains-and-standby)).
2. Run `lazyview setup` with the new version **once, before the new operations run starts**
   ([the two runs](#the-two-runs)). The new image may need a table or a column the old schema
   lacks, and the operations run no longer migrates: it checks its own tables and refuses to
   start (exit `2`) until the setup run has created or migrated them. The setup run only adds
   columns and is safe to run repeatedly. On Kubernetes the `setup` initContainer does this by
   itself on every pod start, so `kubectl apply -k` stays a single copy-paste step
   ([Kubernetes](kubernetes.md#deploy)).
3. With `LAZYVIEW_START_MODE=resume` (the default) it continues from the saved position. Views
   whose definition and table layout are unchanged are reused; the rest are rebuilt online.
   Select/join views over several tables are rebuilt once when an upgrade adds hidden lineage
   columns to their layout (see [the materialized table](views.md#the-materialized-table)).
4. Views that are resumed keep their previous `_lazyview_state.mode` and `detail` (`detail` is
   NULL after the upgrade that introduced it) until they are next rebuilt.
   `lazyview setup` with another `LAZYVIEW_INSTANCE_ID` prints the current classification of
   every definition.
5. Before an upgrade you can preview which views plan differently: run `lazyview setup` of the new
   image with another `LAZYVIEW_INSTANCE_ID`. It prints each definition's access plan and, per
   (view, target), `access same`, `access changed (stored X, now Y)` or `access new (no state)`.
   A change is not an error — the new binary may plan better — but `access changed` lists the
   views whose routes or traced columns differ from what the running instance stored.
6. The health and metrics endpoint now binds `127.0.0.1:8080` by default (it used to listen on
   `0.0.0.0:8080`). A deployment that scrapes `/metrics` from another host must set
   `LAZYVIEW_HEALTH_ADDR=0.0.0.0:8080` explicitly — and once the endpoint is reachable beyond the
   host, set `LAZYVIEW_HEALTH_TOKEN` (or `LAZYVIEW_HEALTH_TOKEN_FILE`) so `/metrics` and `/stats`
   answer only to `Authorization: Bearer <token>`; the probes stay open. Details:
   [Health and logging](configuration.md#health-and-logging),
   [Secrets](configuration.md#secrets).
