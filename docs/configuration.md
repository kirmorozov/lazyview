# Configuration

lazyview is configured only through environment variables. Most invalid values are a
configuration error: lazyview logs `configuration error: …` and exits with code `2`. Exceptions:

* An unparseable `LAZYVIEW_DSN`, a DSN without a schema and a missing `LAZYVIEW_TLS_CA` file are
  reported when the engine starts, as `fatal: …` with exit code `1`.
* Health settings (`LAZYVIEW_LIVENESS_STALL_SECS`, `LAZYVIEW_LIVENESS_BOOT_STALL_SECS`,
  `LAZYVIEW_READY_*`): each invalid variable is named in a warning and falls back to its own
  default.
* `LAZYVIEW_LOG_FORMAT` other than `json` means `text`, and an invalid `RUST_LOG` falls back to
  `info`.

Value formats:

* Booleans: `1`, `true`, `yes`, `on` or `0`, `false`, `no`, `off` (any case).
* Durations: whole numbers in the unit the name says (`_MS` milliseconds, `_SECS` seconds).
* Enumerations (`LAZYVIEW_START_MODE`, `LAZYVIEW_TLS`, …): any case.

## Connection

The source is the server whose binlog lazyview reads. It also holds the metadata tables and is the
default target for views. Set either the separate variables or a DSN.

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_HOST` | — | source host name or IP address (IPv6 without brackets is fine) |
| `LAZYVIEW_PORT` | `3306` | source port |
| `LAZYVIEW_USER` | `root` | user name |
| `LAZYVIEW_PASSWORD` | none | password |
| `LAZYVIEW_DATABASE` | required with `LAZYVIEW_HOST` | the schema lazyview works in: it holds `_lazyview`, the other metadata tables and the views |
| `LAZYVIEW_DSN` | — | alternative to the five above: `mysql://user:pass@host:3306/app` (`mariadb://` is accepted too). Takes precedence when set |
| `LAZYVIEW_TLS` | `disabled` | `disabled`, `required`, `verify-ca`, `verify-full` (see [TLS](#tls)) |
| `LAZYVIEW_TLS_CA` | — | PEM file with the CA certificates to trust; implies `verify-full` when `LAZYVIEW_TLS` is not set |

* The separate variables are used verbatim. Special characters in the password need no escaping.
  In `LAZYVIEW_DSN`, URL-encode them (`p@ss` becomes `p%40ss`).
* Empty values count as unset.
* With neither `LAZYVIEW_HOST` nor `LAZYVIEW_DSN` set, lazyview exits with code `2` and
  `no database configured`.
* The DSN may carry extra driver parameters after `?`. For example, `?prefer_socket=false` keeps
  a connection to `localhost` / `127.0.0.1` on TCP. By default the driver switches such
  connections to the server's unix socket.

## Targets

Views can be materialized on other servers as well (see
[Targets](views.md#targets-one-source-many-servers)). Each target is configured like the source,
with the prefix `LAZYVIEW_TARGET_<NAME>_`:

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_TARGET_<NAME>_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_DATABASE` | —, `3306`, `root`, none, required | target connection; `_DATABASE` is the schema the views are created in |
| `LAZYVIEW_TARGET_<NAME>_DSN` | — | alternative to the separate variables |
| `LAZYVIEW_TARGET_<NAME>_TLS`, `_TLS_CA` | `LAZYVIEW_TLS`, `LAZYVIEW_TLS_CA` | TLS for this target; the source's settings apply when unset |
| `LAZYVIEW_TARGETS` | discovered | comma-separated target names. When unset, every `LAZYVIEW_TARGET_<NAME>_DSN` or `_HOST` variable defines a target |
| `LAZYVIEW_TARGET_TIMEOUT_MS` | `60000` | upper bound for one write round trip to a target (minimum `1000`), so an unresponsive target cannot stall the binlog loop |
| `LAZYVIEW_PLAN_TIMEOUT_MS` | `10000` | upper bound for planning one definition. Planning runs off the binlog loop; a definition that takes longer goes into error (``planning took longer than 10 s; simplify the definition or raise LAZYVIEW_PLAN_TIMEOUT_MS``) and is not planned again until it changes, `rebuild = 1` is set, or lazyview restarts |

Discovered targets get lower-case names: `LAZYVIEW_TARGET_ANALYTICS_HOST` defines `analytics`.
Names listed in `LAZYVIEW_TARGETS` are used as written; their variables use the name in upper
case with `-` replaced by `_` (`re-port` reads `LAZYVIEW_TARGET_RE_PORT_DSN`). `_lazyview.targets`
must use the same spelling. The name `source` always means the source server.

```sh
LAZYVIEW_TARGET_ANALYTICS_HOST=analytics-db
LAZYVIEW_TARGET_ANALYTICS_USER=lazyview
LAZYVIEW_TARGET_ANALYTICS_PASSWORD=change-me
LAZYVIEW_TARGET_ANALYTICS_DATABASE=marts
LAZYVIEW_TARGET_REPORTING_DSN=mysql://lazyview:change-me@reporting-db:3306/reports
```

## Behaviour

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_START_MODE` | `resume` | `resume`: continue from the saved binlog position and reuse existing tables. `rebuild`: rebuild every view, then stream from the current position. `once`: rebuild every view, also those already built, and exit (code `1` if any failed). See [start modes](operations.md#start-modes) |
| `LAZYVIEW_SYNC` | `true` | `false`: create or migrate the metadata tables, validate every enabled definition, log its mode, and exit. The exit code is `1` if any definition is invalid, so this works as a CI check. It takes the instance lock first: next to a running instance, give it another `LAZYVIEW_INSTANCE_ID` (for example `ci`) |
| `LAZYVIEW_FULL_REFRESH_MS` | `60000` | full-mode views are rebuilt at most once per interval. A change marks the view stale; the rebuild starts once the interval since the previous rebuild started has passed. `0` rebuilds as soon as possible. The same limit applies to incremental views that need a rebuild because the affected rows are unknown: a cascading foreign key (see [foreign keys](views.md#foreign-key-cascades)), or more than max(10 × `LAZYVIEW_BATCH_MAX_KEYS`, 100000) changed keys buffered during a build |
| `LAZYVIEW_DROP_REMOVED` | `true` | drop the tables of removed or disabled definitions and of removed targets. `false` keeps them (no longer maintained). Tables lazyview did not create are never dropped |
| `LAZYVIEW_ON_MISSING_BINLOG` | `fail` | the saved binlog file was purged from the server: `fail` exits with an error, `rebuild` rebuilds every view from the current position |
| `LAZYVIEW_INSTANCE_ID` | `default` | names the saved position in `_lazyview_status` and the lock that lets only one instance per schema and id be active (see [duplicate instances](operations.md#duplicate-instances-and-hot-standby)) |
| `LAZYVIEW_SERVER_ID` | random | replica server id, random in [2^30, 2^31) by default. Set it if your topology needs fixed ids. Must not be `0` and must be unique among the server's replicas |

## Tuning

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_BATCH_WINDOW_MS` | `200` | de-duplication window: how long changed units are collected before a refresh |
| `LAZYVIEW_BATCH_MAX_KEYS` | `50000` | flush earlier when this many unit keys are buffered |
| `LAZYVIEW_QUERY_CHUNK` | `1000` | unit keys per refresh or lookup query (must be greater than `0`) |
| `LAZYVIEW_WRITE_BATCH_ROWS` | `500` | rows per multi-row `INSERT` into a view table |
| `LAZYVIEW_WRITE_BATCH_BYTES` | `4194304` | estimated bytes per `INSERT` or `DELETE` statement into a view table (at least `1024`), capped at half of each server's `max_allowed_packet`. A row larger than that is written alone. Statements also stay under 60,000 placeholders |
| `LAZYVIEW_BUILD_CONCURRENCY` | `2` | view builds running at once; each holds one source connection plus one per target |
| `LAZYVIEW_VISIBILITY_DELAY_MS` | `20` | wait before reading the sources when no GTID fence is available (MariaDB, or `gtid_mode=OFF`). Binlog events can arrive before their transaction is visible to readers |
| `LAZYVIEW_HEARTBEAT_MS` | `5000` | replication heartbeat period (minimum `100`) |
| `LAZYVIEW_POSITION_PERSIST_MS` | `5000` | how often the binlog position is saved when nothing else is written |

### Reserved

`LAZYVIEW_STALE_AFTER_MS` is accepted (and validated) but has no effect yet (see
[design: refresh supervisor](design-supervisor.md), §6 I2).

## Concurrent refreshes

Refreshes read the dirty units of a view in chunks from the source and write each chunk to every
target, several chunks and targets at once, on persistent connection pools: one read pool on the
source and one write pool per target (the source included). Each unit is in exactly one chunk
of a refresh, and a refresh finishes every write before the next one of the view starts, so no
older write lands after a newer one. Each chunk is written to a target as one transaction: the
rows that vanished are deleted by key and the changed and new rows upserted, or, for a view
without a row key, the changed units are deleted and inserted again (see
[internals](internals.md)), in statements of `LAZYVIEW_WRITE_BATCH_ROWS` rows and
`LAZYVIEW_WRITE_BATCH_BYTES` bytes. Tables without an index on the unit get one write at a time
per target. Errors of an overloaded or unreachable server (too many connections, lock wait
timeout, deadlock, lost connection, `LAZYVIEW_TARGET_TIMEOUT_MS`) are retried up to 3 times with
backoff and never lead to a rebuild. A write that times out after its transaction began is
killed (`KILL CONNECTION`) and the target takes no further writes until the server no longer
lists it. See [design: refresh supervisor](design-supervisor.md).

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_SUPERVISOR` | `adaptive` | `adaptive` or `fixed`: concurrent refreshes (`adaptive` uses fixed limits until the adaptive limiter exists, and logs that at startup); `off`: one sequential refresh at a time on single connections (the engine before concurrent refreshes) |
| `LAZYVIEW_MAX_SOURCE_CONCURRENCY` | `4` | source reads at once (read pool size) |
| `LAZYVIEW_MAX_TARGET_CONCURRENCY` | `2` | writes at once per target, the source included (write pool size per target) |
| `LAZYVIEW_TARGET_<NAME>_MAX_CONCURRENCY` | | the same for one target (`SOURCE` for the source) |
| `LAZYVIEW_MAX_INFLIGHT_ROWS` | `200000` | rows read but not yet written to every target; no further chunk is read while more are in flight |
| `LAZYVIEW_CPU_WORKERS` | `min(4, max(1, cores/4))` | large diffs (4096 rows or 64k values) running off the async threads at once |

The defaults of the two pool sizes are interim values; they become 8 and 4 with the adaptive
limiter. Pooled connections take the same session settings as the others (READ COMMITTED, the
[sql_mode](server-setup.md#sql_mode) lazyview uses) and close after 30 s idle, so lazyview keeps
at most 4 + `LAZYVIEW_MAX_SOURCE_CONCURRENCY` + `LAZYVIEW_MAX_TARGET_CONCURRENCY` connections on
the source plus 1 + `LAZYVIEW_MAX_TARGET_CONCURRENCY` on each other target, besides the builds
(`LAZYVIEW_BUILD_CONCURRENCY`).

## Health and logging

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_HEALTH_ADDR` | `0.0.0.0:8080` | listen address of the health and metrics endpoint (also `/stats`, see [operations.md](operations.md#per-view-update-statistics)). `off` (or empty) disables it. An unspecified address (`0.0.0.0` or `[::]`) listens on IPv4 and IPv6 |
| `LAZYVIEW_LIVENESS_STALL_SECS` | `120` | `/livez` fails after this many seconds without engine progress. At least `30`, and at least 2 × the heartbeat in seconds + 10 (an idle source only wakes the engine with binlog heartbeats). With a warning, a value below 30 falls back to the default and a value below the heartbeat minimum is raised to it. The default also rises to that minimum when `LAZYVIEW_HEARTBEAT_MS` is above 55000 |
| `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` | `900` | `/livez` limit while the engine boots (before the first binlog connect, also after a standby takes over); never lower than `LAZYVIEW_LIVENESS_STALL_SECS` |
| `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` | `30` | `/readyz` tolerates a disconnected binlog stream this long |
| `LAZYVIEW_READY_REQUIRE_ALL` | `false` | `true`: a view in error makes `/readyz` fail |
| `RUST_LOG` | `info` | log filter, for example `debug` or `lazyview=debug,warn`. `info` logs lifecycle events and problems only |
| `LAZYVIEW_LOG_FORMAT` | `text` | `json` writes one JSON object per line |
| `NO_COLOR` | — | set to anything to disable colored output (colors are only used on a terminal anyway) |

The endpoints are described in [operations](operations.md#health-endpoints).

## TLS

`LAZYVIEW_TLS` sets the mode for the source and, unless a target sets `LAZYVIEW_TARGET_<NAME>_TLS`,
for every target.

| mode | aliases | effect |
|---|---|---|
| `disabled` | `off`, `false` | no TLS (the default) |
| `required` | `require`, `on`, `true` | encrypted, the server certificate is not checked |
| `verify-full` | `verify-identity`, `verify_identity` | the certificate chain and the host name are checked |
| `verify-ca` | `verify_ca` | accepted, but currently works like `verify-full` (see below) |

* `required` works with the certificates MySQL generates by itself. Those carry no host name, so
  they can never be verified.
* `verify-full` trusts the public CAs out of the box: the Mozilla root certificates are compiled
  into the binary. The container's `/etc/ssl/certs` is not used.
* For a private or managed CA (corporate, AWS RDS / Aurora, Azure, Cloud SQL), point
  `LAZYVIEW_TLS_CA` at its PEM bundle. Without an explicit
  `LAZYVIEW_TLS` this implies `verify-full`. A missing file is reported at startup.
* `verify-ca` (check the chain, skip the host name) does not work yet: the MySQL driver's host-name
  override has no effect with the current TLS library, so the host name is still checked. Use
  `required`, or a certificate that names the host you connect to.
* A failed handshake ends with `TLS to source database at host:port failed`, followed by these
  options.
* Without TLS, accounts that use `caching_sha2_password` (the MySQL 8 default) still work: the
  driver fetches the server's RSA public key and sends the password encrypted with it. This needs
  `caching_sha2_password_auto_generate_rsa_keys=ON`, the server default.
* MariaDB 10.11 offers no TLS unless certificates are configured on the server
  (`have_ssl=DISABLED`).

### Managed databases (RDS, Aurora, Azure, Cloud SQL)

Download the provider's CA bundle, mount it into the container and point `LAZYVIEW_TLS_CA` at it.
For Amazon RDS and Aurora:

```sh
curl -fsSLo global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
docker run -d --name lazyview --restart unless-stopped \
  -v "$PWD/global-bundle.pem:/certs/global-bundle.pem:ro" \
  -e LAZYVIEW_TLS_CA=/certs/global-bundle.pem \
  -e LAZYVIEW_HOST=mydb.abc123.eu-west-1.rds.amazonaws.com \
  -e LAZYVIEW_USER=lazyview \
  -e LAZYVIEW_PASSWORD='change-me' \
  -e LAZYVIEW_DATABASE=app \
  ghcr.io/kirmorozov/lazyview:edge
```

Connect to the instance or cluster endpoint whose name is in the certificate. The container runs
as uid 65532, so the file must be readable by that user.
