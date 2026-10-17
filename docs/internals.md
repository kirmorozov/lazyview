# How it works

* **Units.** Each definition is parsed (MySQL dialect) into its source tables, joins, filters and
  grouping. It gets a *unit*: the key of a group of materialized rows. That is the first table's
  primary key for select/join views and the GROUP BY key for aggregates (see
  [incremental or full](views.md#incremental-or-full)).
* **One binlog read, fanned out.** Each binlog row is decoded once and routed to every view that
  reads the table. Updates that touch no column a view uses are dropped. A row maps to its units
  straight from the row image when the unit is reachable through equalities
  (`i.order_id = o.id`). Otherwise one `SELECT DISTINCT` lookup per batch finds them.
* **De-duplication.** Unit keys are buffered for `LAZYVIEW_BATCH_WINDOW_MS` (200 ms), single-column
  integer keys in roaring bitmaps and others in hash sets. 30 writes to order 42 across `orders`,
  `order_items` and `payments` become one refresh of unit 42.
* **Non-blocking reads, diff writes.** Sources are read only with plain, non-locking `SELECT`s
  (READ COMMITTED), never with `INSERT … SELECT`. Fresh rows are compared with the materialized
  rows of each unit, and only the units that changed are written.
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
