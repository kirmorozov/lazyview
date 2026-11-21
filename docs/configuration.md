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

## The two runs and their accounts

`lazyview setup` (the setup and migration run) and `lazyview` / `lazyview run` (the operations run)
read the same variables, and each run takes its own account ([the two runs](operations.md#the-two-runs)):

| account | used by | what it needs |
|---|---|---|
| setup | `lazyview setup`, at install and at upgrade | DDL on lazyview's own tables, plus the `SELECT` rights that let it plan the definitions |
| control | whoever writes the definitions (the service never does) | `SELECT`, `INSERT`, `UPDATE`, `DELETE` on `_lazyview` |
| consumption | the operations run | `SELECT` on `_lazyview`, DML on the status tables, `SELECT` on the sources its views read, DDL for its own view tables, and `REPLICATION SLAVE` / `REPLICATION CLIENT` where it reads the binlog |

The simple setup gives one account all of these; the [server setup](server-setup.md#advanced-setup-recommended-for-production)
is the production layout and carries one `GRANT` script per account and per layout (a control
database on the source, a separate control instance, a target instance). Nothing else in this page
differs between the runs: the setup run reads the connection variables exactly as the operations
run does, so pointing it at the setup account is a matter of the Secret or the env file you start
it with.

## Connection

The source is the server whose binlog lazyview reads. It also holds the metadata tables and is the
default target for views. Set either the separate variables or a DSN.

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_HOST` | — | source host name or IP address (IPv6 without brackets is fine) |
| `LAZYVIEW_PORT` | `3306` | source port |
| `LAZYVIEW_USER` | `root` | user name |
| `LAZYVIEW_PASSWORD` | none | password; may hold a [reference](#secrets) instead of the value |
| `LAZYVIEW_PASSWORD_FILE` | — | read the password from that file instead of the environment: the file's content, one trailing newline trimmed (see [Secrets](#secrets)) |
| `LAZYVIEW_DATABASE` | required with `LAZYVIEW_HOST` | the schema lazyview works in: it holds `_lazyview`, the other metadata tables and the views |
| `LAZYVIEW_DSN` | — | alternative to the five above: `mysql://user:pass@host:3306/app` (`mariadb://` is accepted too). Takes precedence when set |
| `LAZYVIEW_TLS` | `disabled` | `disabled`, `required`, `verify-ca`, `verify-full` (see [TLS](#tls)) |
| `LAZYVIEW_TLS_CA` | — | PEM file with the CA certificates to trust; implies `verify-full` when `LAZYVIEW_TLS` is not set |

* The separate variables are used verbatim. Special characters in the password need no escaping.
  In `LAZYVIEW_DSN`, URL-encode them (`p@ss` becomes `p%40ss`). One exception: a password written as
  `file://…` or `aws-secret://…` is a [reference](#secrets), not a value; a password that really
  starts that way is written `plain://p@ss`.
* Empty values count as unset.
* With neither `LAZYVIEW_HOST` nor `LAZYVIEW_DSN` set, lazyview exits with code `2` and
  `no database configured`.
* The DSN may carry extra driver parameters after `?`. For example, `?prefer_socket=false` keeps
  a connection to `localhost` / `127.0.0.1` on TCP. By default the driver switches such
  connections to the server's unix socket.

## Secrets

A password never has to be an environment value. Each password input accepts two variables, and the
password field of a DSN accepts a reference too:

| variable | meaning |
|---|---|
| `LAZYVIEW_PASSWORD` | the value itself, or a reference below |
| `LAZYVIEW_PASSWORD_FILE` | a file to read: its content is the value, or a reference below |
| `LAZYVIEW_DSN` password field | the same references, percent-encoded like any other DSN character |

Targets use the same pair with their own prefix (`LAZYVIEW_TARGET_<NAME>_PASSWORD[_FILE]`), and so
does the health token (`LAZYVIEW_HEALTH_TOKEN[_FILE]`, see [Health and logging](#health-and-logging)).

| reference | resolved to |
|---|---|
| `file:///run/secrets/lazyview_password` | the contents of that file, one trailing newline trimmed |
| `aws-secret://prod/lazyview` | AWS Secrets Manager `GetSecretValue`, the whole secret string |
| `aws-secret://prod/lazyview#password` | the `password` field of that JSON secret |
| `plain://file:///etc/pw` | the literal text after `plain://`: the escape hatch for a password that starts with a scheme |

* A file (or a `_FILE` variable) may itself hold a reference, so an indirection layer stays out of
  the environment. Up to eight levels; more is refused as a configuration error.
* Absolute paths only, the file must be a regular file, and at most 1 MiB.
* Setting both `LAZYVIEW_PASSWORD` and `LAZYVIEW_PASSWORD_FILE` is an error, not a precedence rule:
  one place to look, one place to fix.
* An unknown scheme (`vault://…`) is an error that lists the known ones. `aws-secret://` in a build
  without the cargo feature `aws-secrets` is an error naming the feature. In either case nothing is
  tried and the text is never used as a password.
* A resolved value is held in a buffer that is zeroized when it is last used, and it is never
  logged: an error names the variable and the secret id, never the value. `Debug` of the resolved
  type prints `Secret(***)`. (The environment's own copy of an unread reference, and the copy inside
  a DSN, are not ours to clear.)
* `aws-secret://` uses the default AWS credential chain: the environment, an EKS pod identity, an
  instance profile. No lazyview variable selects credentials, so none can be pointed at the wrong
  account. Build with `cargo build --features aws-secrets`; the published image does not include it.
* A secret file is read at startup, so a rotation needs a restart (`kill -HUP` is not watched):
  `kubectl rollout restart`, `docker restart`, `systemctl restart`.

```sh
# Kubernetes mounts a Secret as files; nothing else has to change
LAZYVIEW_PASSWORD_FILE=/run/secrets/lazyview_password
# Docker: -v ./secrets:/run/secrets:ro with a 0400 file owned by uid 65532
LAZYVIEW_PASSWORD=file:///run/secrets/lazyview_password   # a _FILE variable also takes the bare path
```

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
| `LAZYVIEW_PLAN_TIMEOUT_MS` | `10000` | upper bound for planning one definition. Planning runs off the binlog loop; a definition that takes longer goes into error (``planning took longer than 10 s; simplify the definition or raise LAZYVIEW_PLAN_TIMEOUT_MS``) and is not planned again until it changes, a rebuild is asked for (`rebuild = rebuild + 1`), or lazyview restarts |

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
| `LAZYVIEW_SYNC` | `true` | **Deprecated (one release): `false` is an alias of the `lazyview setup` command** — it logs a WARN and does what the setup run does: create or migrate lazyview's own tables, validate every enabled definition, log its mode and its access plan, and compare each plan's fingerprint with the one stored in `_lazyview_state.access`: per (view, target) it logs `access same`, `access changed (stored X, now Y)` or `access new (no state)`. A change is logged, not an error — the exit code is `1` only for invalid definitions, so this works as a CI check. It takes the instance lock first: next to a running instance, give it another `LAZYVIEW_INSTANCE_ID` (for example `ci`). Write `lazyview setup` (with the setup account) instead; see [the two runs](operations.md#the-two-runs) |
| `LAZYVIEW_FULL_REFRESH_MS` | `60000` | full-mode views are rebuilt at most once per interval. A change marks the view stale; the rebuild starts once the interval since the previous rebuild started has passed. `0` rebuilds as soon as possible. The same limit applies to incremental views that need a rebuild because the affected rows are unknown: a cascading foreign key (see [foreign keys](views.md#foreign-key-cascades)) |
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
| `LAZYVIEW_VISIBILITY_DELAY_MS` | `20` | fallback wait before reading the sources when no exact visibility check is available: Oracle MySQL with GTIDs off and `binlog_order_commits=OFF`, or when lazyview cannot write `_lazyview_fence` (both logged as a warning). Unused otherwise |
| `LAZYVIEW_HEARTBEAT_MS` | `5000` | replication heartbeat period (minimum `100`) |
| `LAZYVIEW_POSITION_PERSIST_MS` | `5000` | how often the binlog position is saved when nothing else is written |
| `LAZYVIEW_LOCK_HEARTBEAT_MS` | `5000` | period (minimum `1000`) of the leadership lock's own session: a `DO 1` and an ownership check on a connection of its own, from a task of its own — never on the binlog loop and never per binlog event. Also how fast a lost lock is noticed: two periods without an answered beat close every write gate. Nothing to do with `LAZYVIEW_HEARTBEAT_MS` above but the name (that one is the binlog heartbeat) |
| `LAZYVIEW_LOCK_LEASE_MS` | `20000` | the `wait_timeout` (minimum `2000`) the leadership lock's own session sets for itself: the server ends a session it has heard nothing from this long, and a MySQL named lock dies with its session. How long a lost node keeps the lock, so the main term of how long a replacement waits for it (see [duplicate instances](operations.md#duplicate-instances-and-hot-standby)). Must be at least `4 × LAZYVIEW_LOCK_HEARTBEAT_MS` — two periods to notice the beats have stopped and one more to confirm the lock on the way back have to end before the lease does, and startup refuses a combination that does not |

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
| `LAZYVIEW_HEALTH_ADDR` | `127.0.0.1:8080` | listen address of the health and metrics endpoint (also `/stats`, see [operations.md](operations.md#per-view-update-statistics)). `off` (or empty) disables it. Loopback by default, so a container or a pod that must be scraped from outside sets `0.0.0.0:8080` explicitly. An unspecified address (`0.0.0.0` or `[::]`) listens on IPv4 and IPv6 |
| `LAZYVIEW_HEALTH_TOKEN` | — | with it set, `/metrics` and `/stats` answer only to `Authorization: Bearer <token>`; `/livez`, `/healthz`, `/startupz`, `/readyz` and `/` stay open |
| `LAZYVIEW_HEALTH_TOKEN_FILE` | — | read the token from that file instead (see [Secrets](#secrets)) |
| `LAZYVIEW_LIVENESS_STALL_SECS` | `120` | `/livez` fails after this many seconds without engine progress. At least `30`, and at least 2 × the heartbeat in seconds + 10 (an idle source only wakes the engine with binlog heartbeats). With a warning, a value below 30 falls back to the default and a value below the heartbeat minimum is raised to it. The default also rises to that minimum when `LAZYVIEW_HEARTBEAT_MS` is above 55000 |
| `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` | `900` | `/livez` limit while the engine boots (before the first binlog connect, also after a standby takes over); never lower than `LAZYVIEW_LIVENESS_STALL_SECS` |
| `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` | `30` | `/readyz` tolerates a disconnected binlog stream this long |
| `LAZYVIEW_READY_REQUIRE_ALL` | `false` | `true`: a view in error makes `/readyz` fail |
| `RUST_LOG` | `info` | log filter, for example `debug` or `lazyview=debug,warn`. `info` logs lifecycle events and problems only |
| `LAZYVIEW_LOG_FORMAT` | `text` | `json` writes one JSON object per line |
| `NO_COLOR` | — | set to anything to disable colored output (colors are only used on a terminal anyway) |
| `LAZYVIEW_OTEL` | `off` | `otlp` exports spans to an OpenTelemetry collector over OTLP/gRPC; anything else (a typo included) stays off. See [OpenTelemetry](#opentelemetry) |

The endpoints are described in [operations](operations.md#health-endpoints).

* The token is compared without an early exit on the first differing byte, so a scraper cannot learn
  it one character at a time. A refusal is `401` with `WWW-Authenticate: Bearer realm="lazyview"`,
  and its body does not repeat the token. Set it when `/metrics` is reachable from outside the host:
  the metrics name your views, tables and error counts.

Before 0.1.0 the default was `0.0.0.0:8080`. An endpoint that describes the workload should not be
published by default, so it now binds loopback. Anything that scraped it from outside the container
or the pod must set `LAZYVIEW_HEALTH_ADDR=0.0.0.0:8080` (the manifests in `deploy/k8s` already do).

## OpenTelemetry

Tracing is opt-in and off by default: `LAZYVIEW_OTEL` is the only switch, and an ambient collector on
the network changes nothing.

| value | effect |
|---|---|
| unset, `off`, anything else | no tracer provider and no exporter are ever built. The instrumented paths are one atomic load each and allocate nothing — `tests/otel_off_free.rs` counts the allocations and fails if that stops being true |
| `otlp` | spans are exported to a collector over OTLP/gRPC |

A value that is neither (`LAZYVIEW_OTEL=1`, `otel`) is off, and a `WARN` says so: a misspelling must
not quietly disable telemetry, and must not fail a start either.

Everything else is the standard OpenTelemetry environment — the same names every language's SDK
reads, so one collector setup serves them all. lazyview invents no names of its own here.

| variable | default | meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | the collector. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` overrides it for traces alone |
| `OTEL_EXPORTER_OTLP_HEADERS` | — | `k1=v1,k2=v2` added to every export (an auth header for a managed collector). `OTEL_EXPORTER_OTLP_TRACES_HEADERS` for traces alone |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | **`grpc` only.** `http/protobuf` and `http/json` are refused with one `WARN` and tracing stays off: gRPC is the transport the embedder of the copy set already carries, and the HTTP one would add a second HTTP stack to a binary that has no other use for it |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | head sampling: `always_on`, `always_off`, `traceidratio`, `parentbased_traceidratio`, and the `parentbased_always_*` forms |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | the ratio for the two `*ratio` samplers, `0.0`–`1.0` |
| `OTEL_SERVICE_NAME` | `lazyview` | `service.name` of the resource; wins over the default |
| `OTEL_RESOURCE_ATTRIBUTES` | — | `k1=v1,k2=v2` added to the resource (`deployment.environment.name=prod`); keys lazyview sets itself keep the programmatic value, as the specification orders it |
| `OTEL_SDK_DISABLED` | — | `true` disables telemetry even with the switch on |
| `OTEL_TRACES_EXPORTER` | `otlp` | `none` disables spans alone (the per-signal switch, for a collector that carries the other signals); any other name but `otlp` is refused with a `WARN` |
| `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT`, `OTEL_SPAN_EVENT_COUNT_LIMIT`, `OTEL_SPAN_LINK_COUNT_LIMIT` | SDK defaults | the span limits, honoured as the specification names them |

The resource carries `service.name` (`lazyview`), `service.version`, `service.instance.id`
(`LAZYVIEW_INSTANCE_ID`) and `lazyview.source.server` (the source's server identity — a name, never a
DSN), plus `telemetry.sdk.*`.

### What is traced

Spans, one per unit of work, with the attributes a backend groups on. `lazyview.binlog.*` are opened
by the copy set ([`src/binlog/stream.rs`](https://github.com/kirmorozov/lazyview/blob/main/src/binlog/stream.rs));
the rest by the engine and the coordinator.

| span | one per | attributes |
|---|---|---|
| `lazyview.binlog.read_batch` | the events read between two flushes | `lazyview.source.position`, `lazyview.source.flavor`, `lazyview.events`, `lazyview.rows` |
| `lazyview.binlog.transaction` | one source transaction read | `lazyview.source.position`, `lazyview.source.gtid`, `lazyview.events`, `lazyview.rows` |
| `lazyview.view.flush` | one engine flush | `lazyview.epoch`, `lazyview.links`, `lazyview.outcome` |
| `lazyview.view.refresh` | one view brought up to date | `lazyview.view.id`, `lazyview.view.name`, `lazyview.mode`, `lazyview.keys`, `lazyview.targets`, `lazyview.forced`, `lazyview.links`, `lazyview.outcome` |
| `lazyview.target.write_batch` | one batch of statements to one target | `lazyview.target.name`, `lazyview.rows`, `lazyview.keys`, `lazyview.outcome` |
| `lazyview.position.persist` | one save of the replay position | `lazyview.epoch`, `lazyview.source.position`, `lazyview.outcome` |
| `lazyview.epoch.takeover` | one leadership takeover | `lazyview.epoch`, `lazyview.outcome` |

`lazyview.outcome` is one of `success`, `error`, `timeout`, so a dashboard can count failures without
matching on message text. Failures also set the span's status.

**No span carries anything a customer wrote.** Attribute values are names, ids, counts, positions and
outcomes: never a row value, never a key, never a statement, never a DSN or a credential. `Span::fail`
takes a `&'static str` error class (`"timeout"`, `"target"`, `"meta"`) rather than an error's text for
exactly that reason.

A flush and a refresh **link** the source transactions whose rows they merged, they are not parented
by them: `N` transactions merge into one delivery, and a parent–child relation would make a span of
one row event the ancestor of everything that row ever touched. A batch links at most
`otel::MAX_LINKS` (16) of them, which is why the parents live on the reader (one per transaction) and
in a capped sidecar in the engine rather than in the dedup core's pending-key entry.

Telemetry never breaks the pipeline: a bad `OTEL_*` value, an unsupported protocol or an unreachable
collector leaves tracing off with one `WARN`, and an export that fails afterwards is counted
(`/metrics`' `lazyview_otel_export_errors_total` will report it once metrics land) and `WARN`ed about
at most once a minute. The last spans are flushed on the exit path, bounded by 3 s so a collector that
has gone cannot hold a shutdown open.

### Why an embedder sees the same traces

`src/otel.rs`, `src/otel/**` and the binlog reader are copied verbatim by
[lazy-wire-fire](https://github.com/kirmorozov/lazy-wire-fire) (its copy set). The `traceparent` of a
source transaction is therefore stamped **inside the reader**, at read time, and handed out by
`BinlogReader::tx_parent()`: a copied reader stamps it too, and the delivery it later merges those
rows into can link back to the transaction — in the same process or another one. The module reads no
`LAZYVIEW_*` variable and imports nothing outside the copy set: the host's own settings arrive as
parameters of `otel::init`, which is how lazy-wire-fire feeds its own (`LAZYWF_OTEL`).

### Prometheus, and the other two signals

`/metrics` is untouched: it stays the pull-based Prometheus endpoint it always was (see
[health and logging](#health-and-logging)), and it does not depend on this switch. OTLP *metrics* (over
the same fields `/metrics` and `/stats` read) and the `tracing` → OTLP *logs* bridge arrive in their
own changes on this module's once-cell and resource; `OTEL_METRICS_EXPORTER` and `OTEL_LOGS_EXPORTER`
are read there. For Kubernetes, see [the collector side](kubernetes.md#opentelemetry).

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
* A connection whose TLS is off and whose server is not on this host logs one `WARN` at startup, per
  endpoint, naming the variable that turns TLS on:

  ```
  WARN source: db.example.com connects without TLS; set LAZYVIEW_TLS=required (or verify-full) to keep credentials and data off the wire
  ```

  (A target's line starts `target analytics: …` and names `LAZYVIEW_TARGET_ANALYTICS_TLS`.)
  Loopback and unix-socket endpoints are silent (nothing crosses the wire), and one line per endpoint
  per start, never per reconnect: an internal network is a legitimate choice, so this informs and
  changes nothing else. `LAZYVIEW_TEST_*` role DSNs are not warned about; they point at the same
  server as the source.

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
