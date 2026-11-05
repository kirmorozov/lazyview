# Similar projects and how lazyview differs

Several systems keep query results fresh as the underlying data changes. They differ in **where the result
lives** (in the source database, in a separate engine, in a cache in front of the database), **how it is kept
fresh** (recomputed on a schedule, maintained in the writer's transaction, maintained from a change log) and
**what you have to run**. lazyview is one point in that space: a small service that reads the MySQL / MariaDB
binlog as a replica and maintains plain tables, on the source server or on others. This page places it next to
the alternatives so you can pick the right tool.

Facts about other projects are as of 2026-11; check their documentation for the current state.

## Comparison

| project | what it is | how views are kept fresh | where results live | ops footprint | fit vs lazyview |
|---|---|---|---|---|---|
| **MySQL / MariaDB: scheduled refresh** (an `EVENT` or cron running `INSERT … SELECT` into a table, or `CREATE OR REPLACE TABLE … AS SELECT`) | the built-in way; MySQL has no materialized views | full recompute per run; freshness = the interval | same server | none beyond the scheduler | fine for small or rarely changing data; recompute cost grows with the table, not with the change, and readers see a stale or swapped table. lazyview's full-mode views are this with an online swap (`docs/views.md` "What makes a view full") |
| **MySQL / MariaDB: triggers** | `AFTER INSERT/UPDATE/DELETE` triggers that update a summary table | synchronously, inside the writing transaction | same server | hand-written per view, must be kept in step with the schema | exact and immediate, but every write pays for it; deadlocks and lock waits move into the application's transactions; cascaded foreign-key changes do not fire row triggers in InnoDB. lazyview never runs in the writer's transaction and needs no code per view |
| **pg_ivm** (PostgreSQL extension, `CREATE IMMV`) | incremental view maintenance inside Postgres | triggers, synchronously in the writing transaction | same database | install the extension (not available on every managed service) | Postgres only. Inner joins, some aggregates, DISTINCT, EXISTS, simple CTEs; outer joins supported since 1.13 (simple equijoins only, no aggregates or subqueries in an outer-join view); no window functions or `ORDER BY … LIMIT`. Transaction-consistent, which lazyview is not (it is asynchronous) |
| **Materialize** | a streaming database (Timely / Differential Dataflow) that ingests from Postgres and MySQL logical replication, Kafka, … | continuously, incrementally, with strong consistency guarantees inside Materialize | in Materialize (memory / its storage); results are read through its own Postgres-wire endpoint or sinked out | a second database system (hosted or self-managed), memory sized to the working set | wider SQL (recursive, streaming joins, temporal filters) and consistency; a second system to run and query. lazyview writes back into the database you already have |
| **RisingWave** | a streaming database (Postgres-compatible SQL) with materialized views over CDC sources (MySQL and Postgres CDC built in) and sinks | continuously, incrementally | in RisingWave's state store (object storage + cache); can sink results to Postgres, MySQL, Kafka, … | a cluster (compute, meta, object store), or its cloud service | suited where many streams are joined and results must be served at scale; heavier than needed for "keep a summary table current on my MySQL". lazyview has no separate state store and no cluster |
| **Feldera** | an incremental computation engine (DBSP) that evaluates SQL programs incrementally over input connectors (Debezium / Kafka, Postgres, …) and output connectors | continuously, incrementally | in Feldera; results are pushed to sinks or queried from its API | a service with connectors (self-hosted or cloud) | suited when the program is complex and throughput is the point; same "second system" trade-off as above |
| **ReadySet** | a caching layer in front of MySQL / Postgres that speaks the database's wire protocol and incrementally maintains the results of cached `SELECT`s from the replication stream | from the binlog / logical replication, incrementally, per cached query | in ReadySet's cache; served to the application in place of the database | a proxy in the query path, memory for cached results, replication access | the right tool to speed up an application's existing queries without changing them; results are served by ReadySet in place of the database, not stored as tables in your database. lazyview is for durable tables that any client reads with plain SQL |
| **Epsio** | an external engine that reads PostgreSQL logical replication, the MySQL binlog and SQL Server CDC and processes changes after commit | from the change log, incrementally | tables in the source database | a commercial product, self-hosted or hosted | the closest in shape to lazyview (asynchronous, writes tables back into the database). lazyview can write to several target servers from one binlog read (`docs/views.md` "Targets: one source, many servers") |
| **Debezium + a stream processor** (Flink SQL, Kafka Streams, ksqlDB) | CDC into Kafka, then a stream job computes the derived table and a sink connector writes it | from the binlog via Kafka, incrementally, as a stream job | wherever the sink writes (a database table, a topic) | Kafka, Connect, a stream-processing cluster, connectors; schema registry | the general-purpose, scalable path; a lot of infrastructure and operational surface for a few summary tables. lazyview is one process and no queue |
| **Cache-invalidation systems** (application caches keyed by entity, invalidated from CDC or from the application) | caches of query results that are dropped when a related row changes | invalidation, then recompute on the next read | a cache (Redis, Memcached, in-process) | the cache plus the invalidation plumbing | different problem: serve hot reads fast. Invalidation is a dedup problem too (many writes to one entity invalidate once), which is the part lazyview's per-unit buffer solves, but lazyview recomputes eagerly and stores a table; a cache recomputes lazily and stores nothing durable |

Not covered: database-native materialized views elsewhere (Oracle fast refresh with materialized view logs, SQL
Server indexed views, ClickHouse materialized views, which are insert-only triggers), because they are not
options for a MySQL / MariaDB source.

## What lazyview does differently (true today, per the code and docs)

* **Results are plain tables in your database, maintained from the binlog, with the source never locked.**
  lazyview connects as a replica, re-reads only the affected rows or groups with non-locking `SELECT`s and writes
  only what differs (`README.md` "How it works"; `docs/internals.md` "Non-blocking reads, diff writes"). There
  is no trigger, no proxy, no extension, no queue, and no second query engine: readers run `SELECT … FROM
  customer_totals`.
* **Incremental by default, for a wide SQL subset.** Select/join views (with DISTINCT, subqueries, CTEs and derived
  tables), GROUP BY with aggregates, window functions and top-N (`ORDER BY … LIMIT`) are maintained per unit from
  the binlog. A view is `full` (rebuilt into a shadow table and swapped in) only for constructs where a change
  cannot be traced to the rows it affects, and the reason is shown in `_lazyview_overview.detail`
  (`docs/views.md` "Incremental or full", "What makes a view full").
* **Per-unit refresh with de-duplication.** Changes are buffered per unit (a row key or a group key) for a short
  window and refreshed once: "30 writes to order 42 across orders, order_items and payments become one refresh of
  unit 42" (`docs/internals.md` "De-duplication"). `GET /stats` shows per view how many ids were marked, absorbed
  by de-duplication and refreshed, and that an incremental view was never refreshed whole (`docs/operations.md`
  "Per-view update statistics").
* **Exact visibility before reading.** Binlog events arrive before their transaction is visible to readers;
  lazyview waits for it with `WAIT_FOR_EXECUTED_GTID_SET`, the consistent-snapshot binlog position, or a
  commit-order fence of its own, and only falls back to a configured delay where none of these exists
  (`docs/internals.md` "Visibility fence"). Old rows are resolved by lineage columns, not by joining old images
  with the current state (`docs/internals.md` "Lineage, not the current state, for old rows").
* **Foreign-key cascades are followed** although MySQL and MariaDB do not log the child rows a cascading foreign
  key changes: through the parent's row where the plan can trace it, otherwise through an in-memory cascade cache
  (`docs/views.md` "Foreign-key cascades"; `docs/internals.md` "Cascade cache").
* **Online changes, many targets, resume.** A changed definition or an `ALTER` / `TRUNCATE` / `RENAME` of a source
  table builds a new table next to the live one and swaps it in with an atomic `RENAME TABLE`; a view can be
  written to several servers from one binlog read; after a crash or restart lazyview resumes from the saved
  position (`docs/internals.md` "Online changes", "Targets", "Crash, restart, resume"; `docs/views.md`
  "Targets: one source, many servers"). It is one static binary or a distroless container with
  health endpoints and Prometheus metrics (`docs/operations.md`, `docs/kubernetes.md`).

## When not to use lazyview

* **You need the view to be transaction-consistent with the source.** lazyview is asynchronous: a view lags the
  binlog by the batch window plus the refresh (`docs/internals.md` "De-duplication"). Use a plain `VIEW`, triggers,
  or (on Postgres) `pg_ivm`.
* **Your source is not MySQL, MariaDB or Percona Server.** Postgres and others are not supported today
  (`README.md` "Supported servers").
* **The definition is `full`-mode and expensive.** `UNION`, non-deterministic functions such as `NOW()`,
  recursive CTEs, `LIMIT` without `ORDER BY` and the other constructs listed in `docs/views.md` "What makes a view
  full" are rebuilt whole, at most once per `LAZYVIEW_FULL_REFRESH_MS`. If most of your views are of that kind, a
  scheduled refresh does the same with less machinery.
* **You cannot run the binlog in ROW format with full row images, or cannot grant replication privileges**
  (`docs/server-setup.md` "Requirements"). lazyview has nothing to read without them.
* **You need streaming joins over event streams, windows over time, or results at a scale beyond one database
  server.** That is what Materialize, RisingWave, Feldera or Flink are for.
* **You want to speed up existing application queries without changing them.** A caching proxy such as ReadySet
  fits that; lazyview materializes tables you query explicitly.
* **Views that depend on time passing.** lazyview refreshes only when a source table changes; a view filtering on
  `NOW()` is not updated while its tables are idle (`docs/views.md` "What makes a view full").
* **Keys lazyview cannot compare exactly.** Some column types cannot be unit keys or lookup images (for example,
  changes to non-UTF-8 text keys are not refreshed: see the issues labelled "known limitation", `README.md` "Known
  limitations").
