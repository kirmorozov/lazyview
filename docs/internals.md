# How it works

* **Units.** Each definition is parsed (MySQL dialect) into its source tables, joins, filters and
  grouping. It gets a *unit*: the key of a group of materialized rows. That is the first table's
  primary key for select/join views and the GROUP BY key for aggregates (see
  [incremental or full](views.md#incremental-or-full)).
* **One binlog read, fanned out.** Each binlog row is decoded once and routed to every view that
  reads the table. Updates that touch no column a view uses are dropped. A row maps to its units
  straight from the row image when the unit is reachable through equalities
  (`i.order_id = o.id`). Otherwise one `SELECT DISTINCT` lookup per batch finds them.
* **Lineage, not the current state, for old rows.** The current state of the sources can be
  ahead of the binlog read so far, so an old row joined with the *current* rows of the other
  tables may find nothing (a customer and its region deleted one after the other). Select/join
  views therefore store the primary key (or the whole row) of each looked-up table in hidden
  indexed columns (`_lv_a<i>_pk<j>`): the old rows are found in the materialized table by key,
  the new rows by a key-restricted query on the sources. Grouped views still join images with
  the current state, which is exact for one such table. With several, every
  combination of them is looked up with their images, and each flush's images are remembered
  until the binlog has been read past the server position taken right after its lookups: every
  change those lookups could see is combined with them before they are dropped
  (`src/lookback.rs` has the argument). Too many remembered images rebuild that view online.
* **Visibility fence.** Binlog events arrive before their transaction is visible. Before reading
  the sources lazyview waits for it: `WAIT_FOR_EXECUTED_GTID_SET` (MySQL / Percona with GTIDs), or
  the binlog position of a consistent snapshot (MariaDB and Percona Server commit in binlog
  order), else `LAZYVIEW_VISIBILITY_DELAY_MS`.
* **De-duplication.** Unit keys are buffered for `LAZYVIEW_BATCH_WINDOW_MS` (200 ms), single-column
  integer keys in roaring bitmaps and others in hash sets. 30 writes to order 42 across `orders`,
  `order_items` and `payments` become one refresh of unit 42.
* **Non-blocking reads, diff writes.** Sources are read only with plain, non-locking `SELECT`s
  (READ COMMITTED), never with `INSERT … SELECT`. Fresh rows are compared with the materialized
  rows of each unit, and only the units that changed are written.
* **Values as stored.** lazyview's sessions drop `STRICT_*`, `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`
  from the server's `sql_mode`. Zero dates (`'0000-00-00'`, `'2020-00-00'`) are read, compared
  and written as stored, as MariaDB does in any mode. MySQL in strict mode would group
  `'2020-00-00'` with `'0000-00-00'`. TIMESTAMP columns are read as `UNIX_TIMESTAMP(…)` and
  written in a `+00:00` session, so instants in a DST fall-back hour keep their hour. Other
  expressions keep the server's time zone. BIT values are written as integers.
* **Full-refresh fallback.** Definitions that cannot be maintained per unit
  ([list](views.md#what-makes-a-view-full)) are rebuilt into a shadow table and swapped in. A
  change marks the view stale; the rebuild starts once `LAZYVIEW_FULL_REFRESH_MS` (60 s) has passed
  since the previous rebuild started, so a change shows up after at most about one interval plus
  one build.
* **Online changes.** New or changed definitions, renames, and an `ALTER`, `TRUNCATE` or `RENAME`
  of a source table all build a new table next to the live one. The new table then replaces the
  old one with an atomic `RENAME TABLE`. Readers never see a missing or half-built table.
* **Targets.** A view can also be materialized on other servers (`_lazyview.targets`). Sources are
  read once, and the result is written to each target.
* **Crash, restart, resume.** The binlog position is saved in `_lazyview_status`. On a fatal error
  lazyview exits non-zero. After a restart it resumes from the saved position and re-applies the
  changes since then, which is safe to do twice.
