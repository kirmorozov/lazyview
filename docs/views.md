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

-- rebuild one view from scratch (online)
UPDATE _lazyview SET rebuild = 1 WHERE name = 'paid_orders';
```

* `name` is unique (enforced by the table). In a `CREATE VIEW` statement the view name of the
  statement counts: `name` is filled in or updated to it, so changing the name in the statement
  renames the view online. If another row already has that name, the view goes into error:
  ``name `x` already used by row #N``. A bare SELECT takes its name from `name`.
* `name` ignores letter case. A view whose name differs from another row's only in case
  (`lz_case` next to `Lz_Case`) goes into error, keeping its own name:
  ``name `lz_case` is already used by row #N as `Lz_Case`: … remove this row with DELETE FROM
  _lazyview WHERE id = M``. Delete it by `id`: `WHERE name = 'lz_case'` matches the other row.
* The `CREATE VIEW` form accepts what `SHOW CREATE VIEW` prints:
  `CREATE [OR REPLACE] [ALGORITHM=…] [DEFINER=…] [SQL SECURITY …] VIEW name [(columns)] AS SELECT …
  [WITH CHECK OPTION]`.
* A column list (`CREATE VIEW v (a, b, c) AS SELECT …`) renames the result columns in order. It
  must have as many names as the SELECT has columns.
* Unqualified table names refer to `LAZYVIEW_DATABASE`. Other schemas work when qualified
  (`other.customers`) and when the user may read them.
* The view is always created in `LAZYVIEW_DATABASE` (and in each target's schema). A view name
  qualified with that schema (`CREATE VIEW app.x AS …`) is accepted; a name qualified with any
  other schema is rejected: ``schema-qualified view name `other`.`x` is not supported``.
* Reserved names: `_lazyview`, `_lazyview_state`, `_lazyview_status`, `_lazyview_overview`, names
  starting with `_lv_` and names ending in `__lv_new` or `__lv_old`. The metadata tables cannot be
  read by a view either.
* Not allowed: a view that reads itself or forms a cycle with other views, and calls to stored
  functions or loadable functions (UDFs). A view named like a table it reads (`orders` over
  `orders`) is refused like any table lazyview does not manage: ``refusing to replace existing
  base table … that lazyview does not manage (the view reads it: give the view another name)``. lazyview cannot see the tables those functions read, so
  it could not keep the view correct.
* To check definitions without changing anything (for example in CI), run lazyview with
  `LAZYVIEW_SYNC=false`. It logs ``definition #N: `name` ok — <mode> → <targets>`` for each
  definition and exits with code `1` if any is invalid.

A definition that cannot be planned does not stop the service. The view gets `state = 'error'`
with the reason in `last_error`, and the other views keep working. Planning one definition may
take at most `LAZYVIEW_PLAN_TIMEOUT_MS` (10 s); past that it is in error until it changes. A view in error because a table
was missing is retried when the table appears. A live view whose definition is changed into one
that does not parse or plan keeps its last good table (readable, no longer maintained) until the
definition is fixed; the table is dropped only when the definition row is deleted or disabled.
At startup every definition is planned again, views in error included: a view stays in error
only if it still fails, with a fresh `last_error` and `updated_at`.

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
| `incremental` | `spj` | select / join (also with subqueries, CTEs, derived tables and `ORDER BY` without `LIMIT`). Unit: the primary key of the first table in FROM, a key reduced from it by unique keys, or the whole row of a table without a primary key. Only changed units are re-read; with a [row key](internals.md) only their rows that differ are written, otherwise the changed units are rewritten |
| `incremental` | `grouped` | `GROUP BY`, `DISTINCT` (unit: the row, or a source key that determines it) and window functions with `PARTITION BY` (unit: the partition). Only changed groups are recomputed |
| `incremental` | `global` | aggregates without `GROUP BY` (a single row), top-N (`ORDER BY … LIMIT n`) and windows without `PARTITION BY`. One unit, recomputed with one query per batch when a source changes |
| `full` | the reason (see below) | rebuilt completely into a shadow table and swapped in, at most once per `LAZYVIEW_FULL_REFRESH_MS` |
| `unknown` | NULL | the definition could not be planned (for example it reads a lazyview metadata table or names a schema-qualified view). `LAZYVIEW_SYNC=false` still reports such a definition as invalid |

A view in state `error` keeps the `mode` and `detail` of its plan, also when the definition is
fine and only the build failed (a missing table, a name clash, a server error), so `mode = 'full'`
also lists broken views. Filter with `state <> 'error'` (see [metadata tables](#metadata-tables)).

### What makes a view incremental

lazyview keeps a view incremental whenever a change to a source row can be traced to the output
rows it affects. That covers:

* select / join views over any number of tables (`JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `NATURAL`,
  `STRAIGHT_JOIN`, `USING`, comma joins), also over tables without a primary key;
* `GROUP BY` and aggregates (`COUNT(DISTINCT …)`, `GROUP_CONCAT`, `HAVING` included);
* `DISTINCT` (it groups by the selected columns, or by a source key that determines them);
* subqueries: `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` and comparisons against them (semi-joins and
  anti-joins), also over joined subqueries;
* CTEs (non-recursive) and derived tables, merged into the outer query;
* window functions (the unit is the `PARTITION BY` group; without it the whole window is
  recomputed) and top-N (`ORDER BY … LIMIT n`: the `ORDER BY` columns count as read columns, so
  changing one that is not selected is noticed too);
* generated columns: `STORED` ones are plain columns; a `VIRTUAL` one is replaced by its
  expression, with the column keeping its name;
* primary and unique keys, `NOT NULL` and column types come from the server's schema, so a key
  that is unique and not null lets lazyview trace a join without a lookup.

### What makes a view full

These constructs switch a view to `full`: no usable trace exists, or lazyview does not trace it
yet. `detail` shows the text in the first column.

| `detail` | example |
|---|---|
| `set operation (UNION/INTERSECT/EXCEPT)` | `SELECT … UNION SELECT …`, also `UNION ALL` today |
| `LIMIT without ORDER BY` | `… LIMIT 10` (which rows are returned is up to the server) |
| `recursive CTE `x`` | `WITH RECURSIVE x AS (…)` |
| `LATERAL derived table `x`` | `JOIN LATERAL (SELECT …) x` |
| `subquery` | a scalar subquery in the SELECT list |
| `unsupported FROM item` | other FROM items, such as table functions |
| `MATCH ... AGAINST` | full-text search |
| `non-deterministic function <NAME>` | `NOW`, `SYSDATE`, `CURDATE`, `CURTIME`, `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `UTC_DATE`, `UTC_TIME`, `UTC_TIMESTAMP`, `UNIX_TIMESTAMP`, `RAND`, `UUID`, `UUID_SHORT`, `CONNECTION_ID`, `USER`, `CURRENT_USER`, `SESSION_USER`, `SYSTEM_USER`, `LAST_INSERT_ID`, `FOUND_ROWS`, `ROW_COUNT`, `SLEEP`, `BENCHMARK` (also inside the `ORDER BY` of a top-N view) |
| `GROUP BY modifiers (ROLLUP)` | `GROUP BY a WITH ROLLUP` |
| `GROUP BY ALL` | `GROUP BY ALL` (MySQL and MariaDB reject it: the view is in `error`) |
| `no FROM clause` | `SELECT 1` |
| ``table `t` contributes to every row (no join condition)`` | `FROM orders o JOIN regions r` without a condition, `CROSS JOIN` |
| ``first table `t` has no primary key`` | a first table without a primary key and without any column lazyview can compare exactly |
| `SELECT * over a USING or NATURAL join …` | list the columns instead: the server coalesces the shared ones |
| ``unaliased column `…` reads a VIRTUAL generated column: give it an alias`` | a select item over a `VIRTUAL` column whose text is longer than 64 characters (or ends in a space or contains a newline) and has no `AS` alias |
| ``<TYPE> column `t`.`c` decides which rows match, but its binlog value cannot be compared with its SQL value`` | a filter or join on a column type whose binlog form differs from its SQL form |
| ``IN subquery over `t`, a child of a cascading foreign key`` | `WHERE id IN (SELECT parent_id FROM child)` where `child` is a child of a cascading foreign key (see [foreign-key cascades](#foreign-key-cascades)) |
| ``reads SQL view `v` `` | the definition reads a plain SQL view; lazyview watches the view's base tables |

**Planned, still full today.** These reasons name a missing piece of lazyview, not a limit of the
idea. The view is correct, only rebuilt instead of maintained:

| `detail` | example | waits for |
|---|---|---|
| `JSON group key (needs typed unit filters)` | `GROUP BY JSON_EXTRACT(doc, '$.type')`, or a `JSON` column | JSON-aware group keys |
| reasons ending in `(needs internal views)` | a group key on the NULL side of a `LEFT JOIN`; an aggregated or `DISTINCT` CTE joined to other tables; an aggregate or window inside a derived table on the NULL side of a `LEFT JOIN`; a parenthesized join that cannot be flattened | internal views (an invisible chained view) |
| reasons ending in `(needs branch plans)`, and `UNION ALL` | `SELECT … UNION ALL SELECT …` | branch plans for `UNION ALL` |

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
* A rebuild runs the whole query, so keep full-mode views cheap, or remove the construct that
  makes them full: for example filter on a column instead of `NOW()`.

## Joins

* Supported incrementally: `JOIN` / `INNER JOIN`, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`,
  `NATURAL` joins, `STRAIGHT_JOIN`, comma joins and `CROSS JOIN` with the condition in `WHERE`,
  and `USING (…)`. `SELECT *` over `USING` or `NATURAL` needs the columns listed.
* For select/join views, the first table in FROM is the anchor: its primary key is the unit (or a
  key reduced from it by unique keys), so put the table you want one row per first.
* Equalities between columns (`i.order_id = o.id`) let lazyview map a changed row to its units
  straight from the binlog row, without a query. This needs columns of the same type family
  (integers with integers, decimals with decimals, strings with the same character set and
  collation). `TIMESTAMP`, `JSON`, `FLOAT`, `DOUBLE` and `YEAR` keys always use a lookup query.
* A changed row only counts for a table alias whose constant conditions it meets: `col = 5`,
  `col IN (2, 3, 4)` or `col <=> 1` on an integer column, in the `WHERE`, in an inner join's `ON`
  or in that alias's own `LEFT JOIN` condition, also through equalities (`a.kind = b.kind AND
  b.kind = 5` pins both). A key / value table joined once per key (`JOIN item_attr w ON
  w.item_id = i.id AND w.attr = 5`, `JOIN item_attr h ON h.item_id = i.id AND h.attr = 6`)
  therefore sends each changed value row only to the aliases of its own key, so a transaction
  that changes several values of one item stays a small incremental refresh.
* In a grouped view, a table joined to part of the group key (in a view grouped by `g.id, i.id`,
  a value table joined on `w.item_id = i.id`) is traced without a lookup query: its changed row
  refreshes every group with that `i.id`. The materialized table gets an index on that part of
  the key when its own key does not start with it.
* A join without any condition between a table and the rest (`FROM orders o JOIN regions r`,
  `CROSS JOIN`) makes the view full: every row of that table belongs to every output row.

### LEFT JOIN

The rows a LEFT JOIN adds with NULLs (NULL-extended rows) are traced too, so these are all
incremental: conditions on the NULL side such as `IS NULL`, `IFNULL()`, `COALESCE()` and `OR`, and
the anti-join

```sql
SELECT o.id FROM orders o LEFT JOIN customers c ON c.name = o.customer WHERE c.id IS NULL
```

The exception is a `GROUP BY` key taken from the NULL side: `… LEFT JOIN order_items i … GROUP BY
i.order_id` is full (`group key on the NULL side of a LEFT JOIN (needs internal views)`; planned).
Group by `o.id` instead:

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
  when such a view is created (with the `reasons` that apply): `reads a child of a cascading foreign key whose units cannot be
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
`x` to be built``); it is built as soon as the upstream is swapped in. While the upstream view is
in error, the dependent view is in `error` too, with ``upstream view `x` is in error: <its
error>`` in `last_error` (also further down a chain of views). It goes back to `waiting` when the
upstream is built again, and is built once the upstream is live.

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
| `UPDATE _lazyview SET rebuild = 1` | the view is rebuilt online like a changed definition; `rebuild` goes back to `0` once the new table is live. Set while lazyview is stopped, the view is rebuilt at startup instead of resumed |
| `TRUNCATE` or `DROP TABLE` of a view's table on the source | the view is rebuilt online (lazyview never truncates its tables); after a `DROP` the table is missing until the rebuilt one is renamed into place |
| `ALTER TABLE`, `TRUNCATE` or `RENAME TABLE` of a source table, or an online schema change tool's table swap (`RENAME TABLE t TO t_old, t_new TO t`) | the view is re-planned and rebuilt online. The old table stays readable but is not maintained until the swap |
| `DROP TABLE` of a source table | the view goes into error (`table … does not exist`); the old table stays readable. It is rebuilt once the table exists again |
| an upstream lazyview table is swapped (chained view) | the dependent view is rebuilt online; the old version stays maintained |

If a re-plan fails (for example, the source table is gone), the view goes into error and is
retried with exponential backoff of up to 5 minutes.

Other hand edits of a view's table (`DELETE`, `UPDATE`, `INSERT`, or any change on a target) are
not noticed: lazyview keeps maintaining the table on top of them, so rows that are not changed
again stay wrong. Repair such a view with `rebuild = 1`.

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
| `rebuild` | set to `1` to rebuild the view once; lazyview sets it back to `0` when the new table is live |
| `created_at`, `updated_at` | timestamps |

`_lazyview_state`: one row per view and target.

| column | meaning |
|---|---|
| `name`, `target` | primary key |
| `definition_id`, `definition_hash` | which definition the table was built from |
| `mode`, `detail` | `incremental` / `full` / `unknown` (not planned), and the kind or reason (see [above](#incremental-or-full)); a view in `error` keeps those of its plan |
| `state` | `building`, `ready`, `waiting` (a chained view whose upstream view is not built yet) or `error` |
| `managed` | `1` once lazyview created the table; only such tables are ever replaced or dropped |
| `last_error` | the error message in state `error` |
| `refreshed_at` | last time the table was built or refreshed |
| `updated_at` | last change of this row |

A state row deleted by hand is written back as it was within moments (`refreshed_at` restarts).
The state rows are lazyview's record of which tables it built: do not edit them. The rows of a
definition deleted while lazyview was stopped are removed at the next start (with the table, if
lazyview built one).

`_lazyview_status`: the saved binlog position per `LAZYVIEW_INSTANCE_ID` (`binlog_file`,
`binlog_pos`) and the fencing `epoch`, which grows each time an instance takes over.

`_lazyview_overview`: a view that joins definitions and state: `id`, `name`, `target`,
`enabled`, `state`, `mode`, `detail`, `last_error`, `refreshed_at`, `definition_updated_at`,
`definition`. A definition that never got a state row shows up with NULL state.

```sql
SELECT * FROM _lazyview_overview;
SELECT name, target, last_error FROM _lazyview_overview WHERE state = 'error';
SELECT name, detail FROM _lazyview_overview WHERE mode = 'full' AND state <> 'error';
```
