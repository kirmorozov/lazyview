# Writing views

## Defining a view

Each row of `_lazyview` (in the schema `LAZYVIEW_DATABASE`) is one view, and there can be any
number of them. lazyview picks up inserts, updates and deletes from the binlog; no restart is
needed.

```sql
-- a name and a plain SELECT
INSERT INTO _lazyview (name, definition) VALUES
 ('customer_totals', 'SELECT customer_id, SUM(total) AS total, COUNT(*) AS n FROM orders GROUP BY customer_id');

-- or a complete CREATE VIEW statement; `name` is filled in for you
INSERT INTO _lazyview (definition) VALUES
 ('CREATE VIEW paid_orders AS SELECT id, total FROM orders WHERE status = ''paid''');

-- change, disable, re-enable, remove
UPDATE _lazyview SET definition = 'SELECT id, total, status FROM orders' WHERE name = 'paid_orders';
UPDATE _lazyview SET enabled = 0 WHERE name = 'paid_orders';
UPDATE _lazyview SET enabled = 1 WHERE name = 'paid_orders';
DELETE FROM _lazyview WHERE name = 'paid_orders';
```

* `name` is unique (enforced by the table). In a `CREATE VIEW` statement the view name of the
  statement counts: `name` is filled in or updated to it, so changing the name in the statement
  renames the view online. If another row already has that name, the view goes into error:
  ``name `x` already used by row #N``. A bare SELECT takes its name from `name`.
* The `CREATE VIEW` form accepts what `SHOW CREATE VIEW` prints:
  `CREATE [OR REPLACE] [ALGORITHM=…] [DEFINER=…] [SQL SECURITY …] VIEW name [(columns)] AS SELECT …
  [WITH CHECK OPTION]`.
* A column list (`CREATE VIEW v (a, b, c) AS SELECT …`) renames the result columns in order. It
  must have as many names as the SELECT has columns.
* Unqualified table names refer to `LAZYVIEW_DATABASE`. Other schemas work when qualified
  (`other.customers`) and when the user may read them.
* The view is always created in `LAZYVIEW_DATABASE` (and in each target's schema). A
  schema-qualified view name is rejected.
* Reserved names: `_lazyview`, `_lazyview_state`, `_lazyview_status`, `_lazyview_overview`, names
  starting with `_lv_` and names ending in `__lv_new` or `__lv_old`. The metadata tables cannot be
  read by a view either.
* Not allowed: a view that reads itself or forms a cycle with other views, and calls to stored
  functions or loadable functions (UDFs). lazyview cannot see the tables those functions read, so
  it could not keep the view correct.
* To check definitions without changing anything (for example in CI), run lazyview with
  `LAZYVIEW_SYNC=false`. It logs ``definition #N: `name` ok — <mode> → <targets>`` for each
  definition and exits with code `1` if any is invalid.

A definition that cannot be planned does not stop the service. The view gets `state = 'error'`
with the reason in `last_error`, and the other views keep working. A view in error because a table
was missing is retried when the table appears.

## The materialized table

* A real InnoDB table with the view's name, in `LAZYVIEW_DATABASE` (and on each target). Column
  types are taken from the SELECT itself: lazyview creates a temporary view `_lv_probe_<hash>` on
  the source and reads its columns.
* When the unit (see below) identifies one row and is part of the SELECT, it becomes the
  `PRIMARY KEY`. A unit that is not selected is stored in hidden `INVISIBLE` columns `_lv_u0`,
  `_lv_u1`, … with an index. `SELECT *` does not return them.
* Select/join views also keep the lineage of their rows: for each joined table whose rows map to
  units only through a lookup, its primary key (or, for a table without one, the whole row) in
  hidden, indexed `INVISIBLE` columns `_lv_a<i>_pk<j>`. A change to such a row is traced to the
  materialized rows it was part of through them, also when its join partners changed at the same
  time.
* lazyview never replaces or drops a table it did not create. If a table of that name already
  exists, the view goes into error: `refusing to replace existing base table … that lazyview does
  not manage`. Ownership is recorded in `_lazyview_state.managed`.

### JSON values

On MySQL and Percona a `JSON` column of the materialized table holds the value the server
stores for the JSON text the view returns. The server parses that text: integers stay integers,
every other number becomes a `DOUBLE`, and object keys are sorted. JSON copied from a `JSON`
column of a source table is unchanged, because it was parsed the same way when it was stored.

A `DECIMAL` inside a JSON value the view computes is different. `JSON_ARRAYAGG(amount)`,
`JSON_OBJECTAGG(k, amount)`, `JSON_OBJECT('sum', SUM(amount))` and `CAST(amount AS JSON)` keep
the `DECIMAL` while the value stays inside the server, but no client can write it back as one.
The materialized table stores it as a `DOUBLE`:

| the view returns | the materialized table holds |
|---|---|
| `[10.00, 20.50]` | `[10.0, 20.5]` |
| `{"sum": 30.50}` | `{"sum": 30.5}` |
| `12345678901234567890.1234567890` | `1.2345678901234567e19` |

The values are numerically equal, and JSON comparisons (`=`, `JSON_CONTAINS`, `GROUP BY`) treat
them as equal, but the text differs (`->>` returns `10.0`). Precision beyond a `DOUBLE`'s 15-17
significant digits is lost. To keep the exact decimal text, select it as a string instead, for
example `GROUP_CONCAT(amount)` or `CAST(amount AS CHAR)` inside `JSON_ARRAYAGG`.

lazyview compares JSON columns by the value the server stores, so such a view does not rewrite
its rows on every refresh. On MariaDB `JSON` is a `LONGTEXT` alias: the text is stored exactly
as the view returns it, and it is compared as text.

## Incremental or full

Each definition is classified when it is parsed, before anything is built. The result is in
`_lazyview_state` and `_lazyview_overview`, in the `building` log line (`mode=…`) and in the
`LAZYVIEW_SYNC=false` output. The log uses longer names: `incremental (select/join)`,
`incremental (grouped)`, `incremental (single-row aggregate)` and `full rebuild (<reason>)`.

Right after the `INSERT`, `state`, `mode` and `detail` are NULL in `_lazyview_overview` until
lazyview picks the row up from the binlog (a fraction of a second). Resumed views keep the values
written by the version that built them (see [upgrading](operations.md#upgrading)).

```sql
SELECT name, target, state, mode, detail FROM _lazyview_overview;
```

| `mode` | `detail` | how it is maintained |
|---|---|---|
| `incremental` | `spj` | select / join. Unit: primary key of the first table in FROM. Only changed units are re-read and rewritten |
| `incremental` | `grouped` | GROUP BY. Unit: the group key. Only changed groups are recomputed |
| `incremental` | `global` | aggregates without GROUP BY (a single row). Recomputed with one query per batch when a source changes |
| `full` | the reason (see below) | rebuilt completely into a shadow table and swapped in, at most once per `LAZYVIEW_FULL_REFRESH_MS` |
| `unknown` | NULL | shown for every view in state `error`, also when the definition itself is valid (for example a failed build or a name clash). For a definition that parses, `LAZYVIEW_SYNC=false` still prints the mode, and so does the `building` log line if a build started |

### What makes a view full

These constructs switch a view to `full`. `detail` shows the text in the first column.

| `detail` | example |
|---|---|
| `DISTINCT` | `SELECT DISTINCT customer_id FROM orders` |
| `LIMIT` | `… ORDER BY total DESC LIMIT 10` (also `FETCH`) |
| `set operation (UNION/INTERSECT/EXCEPT)` | `SELECT … UNION SELECT …` |
| `WITH clause` | `WITH x AS (…) SELECT …` |
| `subquery` | `WHERE id IN (SELECT …)`, `EXISTS (…)`, a scalar subquery |
| `derived table in FROM` | `FROM (SELECT …) t` |
| `unsupported FROM item` | other FROM items, such as table functions or parenthesized joins |
| `RIGHT/FULL/other join` | `RIGHT JOIN`, `STRAIGHT_JOIN` and other join types |
| `NATURAL join` | `NATURAL JOIN` |
| `window function` | `ROW_NUMBER() OVER (…)` |
| `MATCH ... AGAINST` | full-text search |
| `non-deterministic function <NAME>` | `NOW`, `SYSDATE`, `CURDATE`, `CURTIME`, `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `UTC_DATE`, `UTC_TIME`, `UTC_TIMESTAMP`, `UNIX_TIMESTAMP`, `RAND`, `UUID`, `UUID_SHORT`, `CONNECTION_ID`, `USER`, `CURRENT_USER`, `SESSION_USER`, `SYSTEM_USER`, `LAST_INSERT_ID`, `FOUND_ROWS`, `ROW_COUNT`, `SLEEP`, `BENCHMARK` |
| `GROUP BY modifiers (ROLLUP)` | `GROUP BY a WITH ROLLUP` |
| `GROUP BY ALL` | `GROUP BY ALL` |
| `no FROM clause` | `SELECT 1` |
| ``first table `t` has no primary key`` | a select/join view whose first table has no primary key |
| ``table `t` contributes to every row (no join condition)`` | `FROM orders o JOIN regions r` without a condition, `CROSS JOIN` |
| `group key on the NULL side of a LEFT JOIN` | see [LEFT JOIN](#left-join) |
| `WHERE condition that keeps NULL-extended rows of a LEFT JOIN` | see [LEFT JOIN](#left-join) |
| `JSON group key` | `GROUP BY JSON_EXTRACT(doc, '$.type')`, or a `JSON` column |
| `BIT unit key` | grouping by, or a first-table primary key of, a `BIT` column |
| ``reads SQL view `v` `` | the definition reads a plain SQL view; lazyview watches the view's base tables |

How full mode behaves:

* Any change to a table the view reads marks it stale, except an `UPDATE` that leaves every
  column the statement names unchanged. The rebuild runs the definition exactly as written
  (`ORDER BY` included) into `<name>__lv_new`, then swaps it in. The old table stays readable
  the whole time; `state` is `building` meanwhile.
* At most one rebuild of a view runs at a time. Changes during it schedule one more rebuild after
  it; they never restart it. A rebuild that fails is retried with exponential backoff (up to 5
  minutes), or right away when a table it reads is altered, renamed or created.
* Rebuilds start at most once per `LAZYVIEW_FULL_REFRESH_MS` (default 60 s), measured from the
  start of the previous one. Under constant writes a full view is therefore up to about one
  interval plus one build behind. `0` rebuilds as soon as possible.
* Views are only refreshed when a source table changes, never because time passes. A view that
  filters on `NOW()` is not updated while its tables are idle.
* A rebuild runs the whole query, so keep full-mode views cheap or make them incremental. Often a
  rewrite helps: `SELECT DISTINCT customer_id FROM orders` and
  `SELECT customer_id FROM orders GROUP BY customer_id` return the same rows, but the second one
  is incremental.

## Joins

* Supported incrementally: `JOIN` / `INNER JOIN`, `LEFT [OUTER] JOIN`, comma joins and `CROSS JOIN`
  with the condition in `WHERE`, and `USING (…)`.
* For select/join views, the first table in FROM is the anchor: its primary key is the unit, so
  put the table you want one row per first.
* Equalities between columns (`i.order_id = o.id`) let lazyview map a changed row to its units
  straight from the binlog row, without a query. This needs columns of the same type family
  (integers with integers, decimals with decimals, strings with the same character set and
  collation). `TIMESTAMP`, `JSON`, `FLOAT`, `DOUBLE` and `YEAR` keys always use a lookup query.

### LEFT JOIN

The rows a LEFT JOIN adds with NULLs (NULL-extended rows) need care:

* A GROUP BY key must not come from the NULL side. `… LEFT JOIN order_items i … GROUP BY
  i.order_id` is full; group by `o.id` instead.
* A WHERE condition on a NULL-side column must reject NULLs: `=`, `<>`, `<`, `>`, `LIKE`,
  `IN (list)`, `BETWEEN` and `IS NOT NULL` are fine, also combined with `AND`. `IS NULL`,
  `IFNULL()`, `COALESCE()` and `OR` make the view full (write `c.name IN ('a', 'b')` rather than
  `c.name = 'a' OR c.name = 'b'`).
* An anti-join written with `WHERE i.order_id IS NULL` is therefore full. The same result as an
  incremental view:

  ```sql
  SELECT o.id, COUNT(i.order_id) AS n
    FROM orders o LEFT JOIN order_items i ON i.order_id = o.id
   GROUP BY o.id
  HAVING COUNT(i.order_id) = 0
  ```

  `HAVING` is evaluated on refresh, so NULL-side columns are fine there.

## Foreign-key cascades

Changes made by `ON DELETE` / `ON UPDATE` `CASCADE` or `SET NULL` are done by the storage engine
and never reach the binlog. lazyview therefore also watches the parent tables of every table a view
reads.

* If the view's unit is the foreign-key column (grouped by `order_id`, or joined
  `i.order_id = o.id`), the affected units come from the parent's binlog row.
* If the view joins the parent on the foreign key (`JOIN customers c ON c.id = a.customer_id`),
  the rows a cascade changes are found through the parent's row as well, also over several
  levels (groups → parents → children) when every level is joined.
* Through a `LEFT JOIN` with the parent on the NULL side, the parent is traced only when the join
  condition is the foreign-key equality and nothing else (`LEFT JOIN customers c ON c.id =
  a.customer_id`, or `USING`). With any other condition (`… ON c.id = a.customer_id AND
  c.active = 1`), a child row whose parent fails it stays in the view without its parent, so the
  parent's row does not lead to it: such a view is rebuilt as described below.
* Otherwise a delete or key update on the parent triggers an online rebuild of the view (at most
  once per `LAZYVIEW_FULL_REFRESH_MS`, never more than one at a time). lazyview logs a warning
  when such a view is created: `reads a child of a cascading foreign key whose units cannot be
  derived from the parent`. Group or join by the foreign-key column to avoid the rebuilds.

The same rate-limited online rebuild is used when too many changed keys were buffered to track
individually: more than max(10 × `LAZYVIEW_BATCH_MAX_KEYS`, 100000) during a build.

## XA transactions

Rows of an XA transaction are held back until `XA COMMIT`, including a commit from another session
after the preparing session disconnected, and `XA COMMIT … ONE PHASE`. `XA ROLLBACK` discards
them. Prepared but unresolved XA data never appears in a view. lazyview never saves a position
inside an open `XA PREPARE`. If it still sees an `XA COMMIT` for a transaction prepared before the
current run, it logs a warning and rebuilds all views.

## Chained views

A view may read another lazyview table, for example a daily summary over `order_totals`. Only the
source's binlog is read, so the upstream view must be materialized on target `source`. Otherwise
the definition fails with ``chained views need the upstream view on target `source` ``. When the
upstream view is rebuilt and swapped, its dependent views are rebuilt online too.

The upstream may be defined in the same transaction, or after the dependent view. Until the
upstream table is live, the dependent view has `state = 'waiting'` (``waiting for upstream view
`x` to be built``); it is built as soon as the upstream is swapped in.

A plain SQL view (`CREATE VIEW` run directly on the server) can also be a source. It has no binlog
rows of its own, so lazyview watches its base tables and maintains the view in full mode.

## Targets: one source, many servers

`_lazyview.targets` is a comma-separated list of target names. `NULL` or empty means `source`, the
server whose binlog is read. Targets are configured with `LAZYVIEW_TARGET_<NAME>_*` (see
[configuration](configuration.md#targets)).

```sql
INSERT INTO _lazyview (name, definition, targets) VALUES
 ('order_totals', 'SELECT o.id, SUM(i.qty * i.price) AS total FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY o.id',
  'source,analytics');
```

* Sources are read once per batch. The result is compared and written to each target
  separately, in the target's schema.
* An unknown target name puts the view in error: `unknown target`.
* A target that is down or slow does not hold up the others. A single write round trip is capped
  by `LAZYVIEW_TARGET_TIMEOUT_MS`.
* Changing `targets` builds the view on the new targets, then drops it from the removed ones.

## Changes while running

Every structural change is done online. The old table stays readable (and, unless noted,
maintained) until an atomic `RENAME TABLE` swap.

| change | what happens |
|---|---|
| new definition | built in the background into `<name>__lv_new`, then renamed into place |
| `UPDATE _lazyview SET definition = …` | the new version is built next to the live one; changes during the build are buffered and applied; then swapped |
| new `name` for the same `_lazyview.id`, or another view name in its `CREATE VIEW` statement | the new table is built; the old one is dropped after the new one is live. `name` follows the statement's view name |
| `targets` changed | built on the new targets; removed targets are dropped after the swap |
| `enabled = 0` or row deleted | the table is dropped (with `LAZYVIEW_DROP_REMOVED=true`, the default) |
| `ALTER TABLE`, `TRUNCATE` or `RENAME TABLE` of a source table, or an online schema change tool's table swap (`RENAME TABLE t TO t_old, t_new TO t`) | the view is re-planned and rebuilt online. The old table stays readable but is not maintained until the swap |
| `DROP TABLE` of a source table | the view goes into error (`table … does not exist`); the old table stays readable. It is rebuilt once the table exists again |
| an upstream lazyview table is swapped (chained view) | the dependent view is rebuilt online; the old version stays maintained |

If a re-plan fails (for example, the source table is gone), the view goes into error and is
retried with exponential backoff of up to 5 minutes.

## Metadata tables

lazyview creates these in `LAZYVIEW_DATABASE` at startup and adds missing columns after an upgrade.

`_lazyview`: your definitions.

| column | meaning |
|---|---|
| `id` | auto-increment key |
| `name` | view name, unique; filled in from `CREATE VIEW` when NULL |
| `definition` | a `SELECT` (with `name`) or a complete `CREATE VIEW` statement |
| `targets` | comma-separated target names; NULL means `source` |
| `enabled` | `1` (default) or `0` |
| `created_at`, `updated_at` | timestamps |

`_lazyview_state`: one row per view and target.

| column | meaning |
|---|---|
| `name`, `target` | primary key |
| `definition_id`, `definition_hash` | which definition the table was built from |
| `mode`, `detail` | `incremental` / `full` / `unknown` (every view in `error`), and the kind or reason (see [above](#incremental-or-full)) |
| `state` | `building`, `ready`, `waiting` (a chained view whose upstream view is not built yet) or `error` |
| `managed` | `1` once lazyview created the table; only such tables are ever replaced or dropped |
| `last_error` | the error message in state `error` |
| `refreshed_at` | last time the table was built or refreshed |
| `updated_at` | last change of this row |

`_lazyview_status`: the saved binlog position per `LAZYVIEW_INSTANCE_ID` (`binlog_file`,
`binlog_pos`) and the fencing `epoch`, which grows each time an instance takes over.

`_lazyview_overview`: a view that joins definitions and state: `id`, `name`, `target`,
`enabled`, `state`, `mode`, `detail`, `last_error`, `refreshed_at`, `definition_updated_at`,
`definition`. A definition that never got a state row shows up with NULL state.

```sql
SELECT * FROM _lazyview_overview;
SELECT name, target, last_error FROM _lazyview_overview WHERE state = 'error';
SELECT name, detail FROM _lazyview_overview WHERE mode = 'full';
```
