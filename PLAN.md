# lazyview — implementation plan

Materialized ("lazy") views for MySQL-family servers, kept up to date by a small Rust
service that connects as a replica and reads the binlog.

Targets: **MySQL 8.0 / 8.4**, **Percona Server 8.0 / 8.4**, **MariaDB 10.11 / 11.4** (current LTS lines).
Shipped as a multi-arch (`linux/amd64`, `linux/arm64`) static container.

---

## 1. User-facing model

```sql
-- created by the service if missing
CREATE TABLE _lazyview (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  definition MEDIUMTEXT NOT NULL,          -- full "CREATE VIEW name AS SELECT ..." statement
  enabled    TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO _lazyview (definition) VALUES
 ('CREATE VIEW order_totals AS
     SELECT o.id, o.created_at, c.name AS customer, SUM(i.qty*i.price) AS total
     FROM orders o JOIN customers c ON c.id = o.customer_id
     LEFT JOIN order_items i ON i.order_id = o.id
     GROUP BY o.id, o.created_at, c.name');
```

The service materializes `order_totals` as a real InnoDB table in the same schema and keeps it in
sync. Inserting / updating / deleting / disabling rows of `_lazyview` is picked up live from the
binlog (no restart). Removing a definition drops its table (`LAZYVIEW_DROP_REMOVED=true`).

Service-owned tables:

| table | purpose |
|---|---|
| `_lazyview_status` | one row per `instance_id`: `binlog_file`, `binlog_pos`, `server_uuid`, `updated_at`. Written **in the same transaction** as MV changes → crash-consistent resume. |
| `_lazyview_state`  | one row per view: `name`, `definition_hash`, `mode` (`incremental`/`full`), `state` (`building`/`ready`/`error`), `last_error`, `refreshed_at`. |

## 2. Configuration (ENV)

| var | default | meaning |
|---|---|---|
| `LAZYVIEW_DSN` | required | `mysql://user:pass@host:3306/schema` — schema holding `_lazyview` and the MVs |
| `LAZYVIEW_START_MODE` | `resume` | `resume`: continue from `_lazyview_status` (build only views without valid state). `rebuild`: rebuild every MV, then stream from now. `once`: build/refresh everything and exit (no streaming). |
| `LAZYVIEW_SYNC` | `true` | `false` = do not stream binlog (validate + create metadata only, then exit 0). Alias for quick disable. |
| `LAZYVIEW_BATCH_WINDOW_MS` | `200` | de-duplication buffer: changes are accumulated this long (closed at a transaction boundary) before being applied |
| `LAZYVIEW_BATCH_MAX_KEYS` | `50000` | flush earlier if this many distinct keys are buffered |
| `LAZYVIEW_QUERY_CHUNK` | `1000` | max keys per generated `IN (...)` |
| `LAZYVIEW_SERVER_ID` | derived from instance id | replica server_id (must be unique in the topology) |
| `LAZYVIEW_INSTANCE_ID` | `default` | row key in `_lazyview_status` (allows several services per schema) |
| `LAZYVIEW_DROP_REMOVED` | `true` | drop MV table when its definition is deleted |
| `LAZYVIEW_ON_MISSING_BINLOG` | `fail` | `fail` or `rebuild` when the saved binlog file was purged |
| `RUST_LOG` / `LAZYVIEW_LOG_FORMAT` | `info` / `text` | logging (`json` available) |

Server requirements (checked at startup, clear error otherwise): `log_bin=ON`,
`binlog_format=ROW`, `binlog_row_image=FULL`; user needs `REPLICATION SLAVE, REPLICATION CLIENT`,
`SELECT` on source tables and DDL/DML on the target schema.

## 3. Architecture

```
binlog stream ──► Decoder ──► TxnBuffer ──commit──► Accumulator (dedup, window) ──flush──► Applier ──► MySQL
 (mysql_async)    filters by    per-txn           HashSet<unit key> per view          1 txn: DELETE/INSERT
                  table id      changes           + lookup sets + full flags          per chunk + position
```

Modules: `config`, `parse` (statement → `ViewPlan`), `sqlgen` (all generated SQL), `catalog`
(information_schema: columns, PKs; cached per table, invalidated on DDL), `binlog` (stream,
positions, flavor quirks), `accumulate` (dedup), `apply` (refresh + build), `meta` (`_lazyview*`
tables), `engine` (orchestration), `main`.

Library crate + thin binary, so integration tests drive the engine in-process and read its
counters (`Metrics`: events seen/skipped, units refreshed, statements executed, full rebuilds).

## 4. Parsing & classification (`parse`)

1. Strip MySQL-only prefix noise before parsing: `OR REPLACE`, `ALGORITHM=…`, `DEFINER=…`,
   `SQL SECURITY …`, trailing `WITH [CASCADED|LOCAL] CHECK OPTION`, optional column list
   `VIEW v (a,b) AS`.
2. Parse the `SELECT` with `sqlparser` (`MySqlDialect`). Extract view name, FROM/JOIN tree,
   aliases, select items, WHERE, GROUP BY, HAVING.
3. Resolve every table to `(schema, table)`; fetch PK + columns from `information_schema`.
4. Classify:
   * **SPJ** — select/project/join (inner + left joins), no aggregates. **Unit** = anchor
     (first FROM table) primary key. Requires anchor PK.
   * **Grouped** — GROUP BY over expressions (select aliases resolved). **Unit** = group-by
     expressions. HAVING allowed.
   * **Global aggregate** — aggregates without GROUP BY: one row; unit = `()`.
   * **Full** — anything else (UNION, DISTINCT, LIMIT, window functions, subqueries in
     FROM/WHERE, non-deterministic functions such as `NOW()`, `RAND()`, tables w/o PK used in
     a way that needs lookups). Correct but expensive: debounced rebuild into a shadow table +
     atomic `RENAME TABLE` swap.
5. Per alias compute column usage sets (by column ordinal, for direct binlog image access):
   * `referenced` – any use in the query;
   * `membership` – used in ON / WHERE / GROUP BY (decides *which* unit a row belongs to);
   * `direct_unit` – if every unit expression is a plain column of this alias, units come straight
     from the row image (no query).

## 5. Minimal work per update (hot path)

Per binlog row event, work is O(row) hash operations, never a query:

1. `TABLE_MAP` for a table no view watches → table id marked ignored; its row events are skipped
   **without decoding rows**.
2. UPDATE whose before/after images are equal on all `referenced` columns of every alias that maps
   to this table → **dropped** (e.g. `last_seen_at` bumps).
3. If the alias has `direct_unit` → unit key(s) taken from before + after image → inserted into the
   view's `HashSet<UnitKey>` (dedup).
4. Otherwise the row is queued for a *lookup*:
   * `after` side: `HashSet<pk>` — resolved later with one `SELECT DISTINCT unit FROM … WHERE … AND a.pk IN (…)`.
   * `before` side (DELETE, or UPDATE that changed a `membership` column): `HashSet<image projected
     to referenced columns>` — resolved with the same query where alias `a` is replaced by a
     derived table of literal before-images (`(SELECT ? AS c1, … UNION ALL …) AS a`).
     An UPDATE that changes no membership column needs **only** the pk lookup (old units = new units).
5. Rows from a transaction are only moved into the accumulator at COMMIT/XID (rolled-back or
   partial transactions never leak).

### De-duplication

All sets are keyed by value, across every table and every transaction inside the window
(`LAZYVIEW_BATCH_WINDOW_MS`). 30 writes touching order `42` through `orders`, `order_items` and
`customers` collapse to **one** unit `42` → one `DELETE` + one `INSERT … SELECT` for that unit
(chunked together with other units). Lookup inputs are de-duplicated too (same pk / same image ⇒
one parameter), then lookup results are merged into the unit set.

### Flush (one DB transaction)

```
for each dirty view:
   units ← direct units ∪ lookup(after pks) ∪ lookup(before images)       -- deduped
   for chunk in units:
      DELETE FROM mv WHERE <unit cols> IN (chunk)
      INSERT INTO mv (cols…, hidden unit cols) <view SELECT with "AND unit IN (chunk)" injected>
UPDATE _lazyview_status SET binlog_file=?, binlog_pos=?   -- position of last committed txn in window
COMMIT
```
Full-mode views flagged dirty are rebuilt (shadow + swap) before the data transaction.

Unit filtering: non-NULL tuples use `(u1,u2) IN ((?,?),…)`; tuples containing NULL use
`(u1 <=> ? AND u2 <=> ?)`. Unit columns are indexed in the MV (PRIMARY KEY when unique and
NOT NULL: single-table SPJ on its PK, or grouped view on non-null group cols).

## 6. Materialized table layout

* Column types: create a temporary real VIEW `_lv_probe_<hash>` with the SELECT, read
  `information_schema.COLUMNS`, drop it. Works identically on MySQL/MariaDB/Percona, GTID-safe
  (no `CREATE TABLE … SELECT`).
* Unit expressions not projected by the view are added as `INVISIBLE` columns `_lv_u0…`
  (MySQL ≥ 8.0.23, MariaDB ≥ 10.3) so `SELECT *` on the MV equals the view.
* If the unit is already a projected plain column (e.g. `o.id`), it is reused — no extra column
  ("id persists through the statement").

## 7. Initial build / consistency

1. Read current binlog position (`SHOW BINARY LOG STATUS`, fallback `SHOW MASTER STATUS`).
2. Build MVs (shadow table, `INSERT … SELECT`, `RENAME` swap).
3. Stream from the position read in step 1.

Every refresh recomputes units from **current** base state, so re-applying changes that are
already reflected is harmless (idempotent) → snapshot/stream overlap is safe without locks.
A new definition seen in the stream is built synchronously at that point of the stream; the
same argument applies.

## 8. Binlog handling

* Register as replica (`COM_BINLOG_DUMP` via `mysql_async`), `server_id` from config.
* Flavor quirks: MariaDB `@mariadb_slave_capability=4`, `@master_binlog_checksum`; MariaDB v1
  row events and ANNOTATE/GTID events; MySQL 8.4 `SHOW BINARY LOG STATUS`.
* `ROTATE` updates file; position recorded at `XID` / `COMMIT` boundaries only.
* `QUERY` events: `TRUNCATE`, `ALTER`, `DROP`, `RENAME` on a watched table → invalidate catalog,
  mark dependent views for full rebuild. Changes to `_lazyview` → definitions reloaded at commit.
* Events on MV tables are ignored unless another view sources from that MV (chained views); a full
  rebuild of an MV marks its dependants for full rebuild.
* Reconnect with backoff from the last committed position on stream errors.

## 9. Packaging

* `Dockerfile`: multi-stage, `--platform=$BUILDPLATFORM` builder with `cargo-zigbuild` producing
  static musl binaries for `x86_64` / `aarch64` (no QEMU), final image `FROM scratch`
  (+ CA certs), non-root user. `rustls` TLS only, no OpenSSL.
* `docker buildx build --platform linux/amd64,linux/arm64`.
* `.github/workflows/ci.yml`: fmt, clippy, unit tests, integration matrix (service containers for
  all 6 servers), multi-arch build + push on tags.
* `docker-compose.test.yml` for local integration runs on a machine with Docker.

## 10. Test plan

### Unit (no DB)
* **parse**: name extraction with `OR REPLACE`, `ALGORITHM`, `DEFINER`, `SQL SECURITY`, column
  lists, backticks, schema-qualified names; classification table (≈30 statements → expected mode,
  unit, anchor, alias usage sets); unsupported constructs fall back to `full` with a reason.
* **sqlgen**: golden SQL for refresh, lookup (pk + substituted before image), NULL-safe unit
  filters, chunking, invisible column DDL.
* **accumulate**: 30 writes on one id across 3 tables ⇒ 1 unit; UPDATE on unreferenced column ⇒
  dropped; UPDATE not touching membership columns ⇒ no before-image lookup; rollback-free
  semantics (only committed txns move in); window / max-keys flush triggers; dedup of identical
  before-images.
* **config**: env parsing and validation.

### Integration (real servers; each test runs against every flavor in `LAZYVIEW_TEST_DSNS`)
Oracle: for each MV the test also creates a plain SQL `VIEW` with the same SELECT and compares
sorted full row sets (MV vs. real view) after the service catches up to the server's current
binlog position.

1. **bootstrap** – metadata tables created; server requirement checks report clear errors.
2. **shapes** – single-table filter, 3-table inner join, left join (child inserted later
   turns NULL row into joined row and back), self join, GROUP BY + HAVING, global COUNT/SUM,
   NULL group keys, select-alias GROUP BY, full-mode view (DISTINCT / LIMIT / subquery), chained
   view (MV over MV).
3. **randomized workload** – seeded RNG, thousands of mixed INSERT/UPDATE/DELETE across 4
   tables in multi-statement transactions (incl. ROLLBACKs, PK changes, moving rows between groups,
   deleting parents), then oracle comparison for every view. Repeated with several seeds and
   window sizes (0 ms and 500 ms).
4. **de-duplication** – 30 writes on the same id across 3 tables inside one window ⇒ metrics show
   exactly 1 refreshed unit and 2 data statements for that view.
5. **minimal work** – updates touching only unreferenced columns ⇒ 0 statements executed.
6. **resume** – stop engine, write more, restart in `resume` ⇒ catches up from stored position,
   0 full rebuilds, oracle equal. Crash-consistency: kill between flushes.
7. **start modes** – `rebuild` rebuilds all; `once` builds and exits; `LAZYVIEW_SYNC=false` exits.
8. **live definitions** – insert definition while streaming ⇒ MV appears; update definition ⇒
   rebuilt; disable/delete ⇒ dropped; invalid SQL ⇒ `_lazyview_state.state='error'` with message,
   other views unaffected.
9. **DDL** – TRUNCATE source ⇒ MV emptied; ALTER ADD COLUMN on source ⇒ still correct afterwards.
10. **types** – DECIMAL, DATETIME(6), JSON, utf8mb4 / case-insensitive collations, BLOB, unsigned
    BIGINT through before-image substitution.
11. **large transaction** – 20k-row transaction ⇒ chunked, correct.

Local runs (no Docker in the dev sandbox): official server tarballs started from
`tmp/out/db/<flavor>` by `scripts/test-servers.sh` on ports 33080–33115. CI: service containers.

---

## 11. Revisions (decided during implementation)

These supersede the corresponding parts above.

* **Non-blocking, no `INSERT … SELECT`.** Refreshes read the affected units from the sources with
  plain SELECTs (READ COMMITTED, no locks), read the same units from the MV, diff per unit and write
  only units whose rows changed (DELETE + multi-row parameterized INSERT on the target). Builds
  stream one non-locking SELECT and batch-insert into shadow tables.
* **Roaring bitmaps.** Single-column integer unit keys are buffered in a `RoaringTreemap`
  (order-preserving i64 encoding); flush filters use inline literals and `BETWEEN` for runs.
* **Direct unit derivation through equalities.** Union-find over `=` conjuncts of WHERE / inner ON
  (and a LEFT JOIN's own ON for its table) maps a table's column to the unit, so e.g.
  `order_items` changes yield unit `order_id` straight from the binlog image — no lookup query.
* **Lookups** substitute literal row images (projected to membership columns, de-duplicated) as a
  typed derived table (`CAST(? AS SIGNED)`, `CONVERT(CAST(? AS BINARY) USING cs) COLLATE co`, …)
  so joins from it use the real tables' indexes.
* **Fan-out.** One decoded row is routed to every (view, version, alias) reading the table.
* **Targets.** `_lazyview.targets` (comma list; `source` = the source instance, the default).
  Sources are read once per chunk; each target is diffed/written independently. Position is
  saved on the source after target writes (crash ⇒ idempotent replay). Chained views require the
  upstream view on `source`.
* **Online migration.** Per definition a *slot* with `live` and `next` versions; `next` builds in
  the background (own connections) while `live` keeps being maintained; changes during the build
  are buffered for `next`, applied to the shadow, then `RENAME TABLE` swaps atomically. Used for new
  / changed / renamed / retargeted definitions, source DDL and full-mode rebuilds.
* **Commit-visibility fence.** Binlog events reach replicas before the commit is visible; before
  source reads the engine runs `WAIT_FOR_EXECUTED_GTID_SET(last_gtid)` (MySQL/Percona with GTID)
  or sleeps `LAZYVIEW_VISIBILITY_DELAY_MS` (MariaDB / no GTID).
* **Ownership.** `_lazyview_state.managed` marks tables lazyview created; it never replaces or drops
  anything else.
* **Metadata migrations.** `meta::MIGRATIONS` adds missing columns idempotently at startup.

### Later revisions

* **Definitions by name.** `_lazyview.name` (UNIQUE); with a name the definition may be a bare
  SELECT; names of `CREATE VIEW` rows are backfilled; name/definition mismatch → error.
* **Full-refresh cadence.** `LAZYVIEW_FULL_REFRESH_MS` (default 60 s): full-mode views (and
  incremental views with unknown affected rows) are rebuilt at most once per interval. The mode
  and reason are decided at plan time and shown in `_lazyview_overview`.
* **Operational model.** One instance; fatal errors exit, the orchestrator restarts, the saved
  position resumes. A `GET_LOCK` guard + fencing epoch keep an accidental second copy from
  writing. No HA backends.
* **Robustness.** Replans keyed by definition id with exponential retry; a failed build keeps the
  live version maintained; swaps retry on metadata-lock timeouts; ownership recorded only after a
  successful swap; orphans cleaned at start; source connection self-heals; per-target timeouts.
* **Correctness additions.** FK cascades (parents watched; direct units or rebuild), XA (rows
  re-fed at XA COMMIT; position never past an open XA PREPARE), LEFT JOIN anti-join / COALESCE →
  full mode, SQL-view sources → full mode, stored functions rejected, cycles rejected.
* **Health.** `/livez` `/readyz` `/startupz` `/metrics` + `lazyview healthcheck` (distroless image
  has no shell). Kubernetes manifests in `deploy/k8s`.
* **Supervisor (in progress).** Parallel flushes (views × chunks × targets) on pools with an
  adaptive per-endpoint concurrency limit; see `docs/design-supervisor.md`.
