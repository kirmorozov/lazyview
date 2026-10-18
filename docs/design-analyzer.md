# Query analyzer: design

Status: phases A and B done and wired (`analyze::plan` delegates to `analyzer::plan`; the old
planner body is the block planner `analyze::plan_block`); JSON group keys are plain units since
the engine's typed unit filters; phase C (internal views, UNION ALL branch plans) waits for the engine, those
views are Full with a reason naming the missing piece (§3.3). Owner rule: **a view is maintained
incrementally unless it is mathematically impossible to trace a source change to the output
rows it affects.** Every Full fallback names the construct and states why no trace exists.

## 1. Tracing model

The executor keeps an MV partitioned into *units*. A refresh recomputes the rows of a set of units
from the current source state (one unit-filtered query) and writes the diff. Correctness = **delta
coverage**: for every source change, every unit whose rows differ between the old and the new
state is refreshed after the new state is visible. A change `(before, after)` of an alias is
traced to units by one of:

| trace | before side | after side |
|---|---|---|
| **direct unit** | the unit is (equal to) a column of the row: read it from the binlog image | same |
| **lineage** | `SELECT DISTINCT <unit> FROM mv WHERE (_lv_a<i>_pk…) IN (…)`: MV rows derived from that source row | forward query restricted by the row's key on the current state |
| **forward key query** | current state is enough when the traced attribute belongs to the outer row (semi/anti-joins, §3.4) | same |
| **all units** | the change can affect every output row (cross join, global predicate) | same |

Lineage columns (hidden, INVISIBLE, indexed `KEY _lv_a<i>`) carry the primary key of each
lookup-resolved alias, `_lv_a<i>_pk<j>`, after the hidden unit columns `_lv_u<k>` (shape shipped
by engine worker A: `ViewPlan.lineage: Vec<LineageKey { alias, cols, exprs }>`). Extensions:
aliases without a PK carry **value lineage** `_lv_a<i>_v<j>` (their referenced columns, exact
kinds only, NULL-safe matching); UNION ALL rows carry `_lv_branch` with globally numbered aliases;
lineage of NULL-extended rows is NULL for the missing side (filters must be NULL-safe, `<=>`).
Lineage is by key, so it survives intermediate updates inside a batch, PK changes (old and new
keys are both traced) and duplicates (one lineage value, many MV rows, refreshed as one unit).
The overview reports, per view and alias, `direct` / `lineage` / `forward` / `all`.

## 2. Architecture

```
ParsedDef ─build─► IR ─normalize─► IR' ─classify─► Strategy tree ─lower─► Outcome
                (scopes, names)  (rewrites)      (per subtree)        ViewPlan(s) + internal views
```

* `src/analyzer/ir.rs` — logical plan with resolved names: `Rel = Base(TableRef, TableInfo) |
  Block | SetOp{op, all, inputs} | Recursive | Values`. A `Block` is the canonical SQL pipeline
  `Join(kind, cond)* → Filter → Aggregate → Having → Window → Distinct → Order/Limit → Project`
  over FROM items; every item has a block-unique alias, an origin (`table`, `cte <name>`,
  `derived`, `subquery`), and output columns `(name, expr, NameKind)`. Expressions stay
  sqlparser `Expr`s; column references are resolved through a scope chain.
* `build.rs` — name resolution exactly as MySQL/MariaDB do it: a WITH clause opens a scope for
  its later CTEs and its body (a non-recursive CTE is *not* visible in its own body, so
  `WITH orders AS (SELECT … FROM orders)` reads the base table); qualified names never hit CTEs;
  derived tables see only outer CTEs; GROUP BY/HAVING resolve FROM columns before select
  aliases, ORDER BY the reverse; unreferenced CTEs are ignored (never evaluated by the server).
* `normalize.rs` — semantics-preserving rewrites, each with a precondition that yields a precise
  reason when it does not apply: CTE inlining (one copy per reference), derived-table merging,
  merge-up of a sole aggregated item, pull-up through key-preserving joins, DISTINCT→GROUP BY,
  semi-join→join or semi-join alias (markers), scalar subquery→LEFT JOIN, ORDER BY drop,
  wildcard expansion. Predicate pushdown falls out of merging (outer predicates land in the flat
  WHERE); for merge-up, group-key-only conjuncts go to WHERE, the rest to HAVING.
* `joins.rs` (RIGHT→LEFT, STRAIGHT_JOIN, NATURAL→USING, nested inner joins flattened), `scope.rs`
  (WITH scoping, `referenced_tables`), `names.rs` (§5), `source.rs` (spans into the text as
  written), `internal.rs` (§4), `union.rs` (branch plans), `ast.rs` (helpers).
* `lower.rs` — renders a flat block back to a sqlparser `Select` + synthetic `ParsedDef` and hands
  it to the **block planner** `analyze::plan_block` (units, equality classes, null-side rules,
  lineage, cascades, unit reduction by FDs, windows, top-N), then attaches semi-join aliases.
  There is exactly one implementation of those rules, so a merged CTE is planned *identically*
  to its hand-inlined form. Full plans are always built from the user's statement as written
  (`full_select`, watched tables from the scoped resolution, subquery tables included).

Contract: `analyzer::analyze(def, schema, tables, opts) -> Outcome` with `Outcome::Plan`,
`Outcome::NeedsInternal(Vec<InternalView>)` or `Outcome::Branches`; `analyzer::plan()` (what
`analyze::plan` calls) runs it with internal views and branch plans disabled and turns those
outcomes into Full plans whose reason ends in `(needs internal views)` / `(needs branch plans)`.
`ViewPlan` stays the executor contract; additions (`order_limit_sql`, `lookup_where_sql`,
`AliasPlan.semi`, `AliasPlan.lookup_where`) are backward compatible.

## 3. Taxonomy

### 3.1 CTEs and derived tables (merge family — no executor change)

| class | strategy | correctness |
|---|---|---|
| single-reference SPJ CTE / derived table (filter, projection, rename, inner/comma joins) | **merge**: inner FROM joins the outer FROM (aliases renamed on collision, `t__o`), inner WHERE into the outer WHERE (first/comma item) or the join's ON, outer refs replaced by the (parenthesized, fully qualified) inner expressions | σ/π/⋈ commute under bag semantics; no DISTINCT, aggregate, window, LIMIT or non-determinism inside |
| CTE referenced 2–3× (self-joins, different filters) | merge **per reference** (fresh aliases each) | a deterministic CTE denotes one bag; each reference is an independent copy |
| column lists `WITH t(a,b)`, `(…) AS d(a,b)` | rename positionally, then merge; count mismatch is an error (as on the server) | renaming only |
| chained CTEs | DAG in textual order (MySQL/MariaDB allow only earlier references), inlined bottom-up | composition of merges |
| nested WITH in derived tables / subqueries / CTE bodies | lexical scopes, merged inside-out | scoping only |
| CTE name shadowing a table | resolved lexically (see build) | scoping only |
| derived/CTE on the preserved side of LEFT JOIN | merge, inner WHERE stays in WHERE | σ on the preserved side commutes with ⟕ |
| single-table derived/CTE on the NULL side | merge, inner WHERE into the ON; outputs that are not NULL-propagating become `IF(alias.pk IS NULL, NULL, e)`. NULL-propagating = columns, literals, strict operators and the strict builtins of `analyze::NULL_STRICT` (an allow-list: `CHAR`, `CHARSET`, `COERCIBILITY`, `NVL2`, `DECODE_ORACLE`, `IF`/`COALESCE`, aggregates, `CASE`, `<=>`, AND/OR and every unlisted function have a value for NULL and are guarded; A2) | `L ⟕c σq R = L ⟕(c∧q) R`; a NULL-extended row has NULL pk, a real one never |
| multi-table derived on the NULL side | **internal view** (SPJ, lineage) joined as one table | `L ⟕ (R1⋈R2) ≠ (L⟕R1)⟕R2` (partial matches), so it cannot be flattened |
| ORDER BY without LIMIT inside | dropped | no effect on the bag |
| aggregated / DISTINCT CTE as the only FROM item of a filter/project block | **merge-up**: one Grouped/Global block; outer predicates on group keys → WHERE, others → HAVING | `σp(γG(Q)) = γG(σp Q)` for `p` over G; `π` over aggregates is HAVING/projection |
| aggregated / DISTINCT CTE joined on the other tables' keys | **pull-up**: one grouped block over the CTE's tables joined to the others, grouped by the CTE's keys plus the other tables' used columns; aggregate conditions → HAVING | each group joins ≤ 1 row per table (its whole PK equals group keys or bound columns, same type family), the same row for every base row of the group (Yan & Larson) |
| aggregated / DISTINCT CTE joined otherwise | **internal grouped view** `_lv_int_<h>` (key = GROUP BY columns, unique) + outer as a chained view; joins on the key give direct units | the internal view is exact; the outer is a view over a table |
| LATERAL derived table (MySQL ≥ 8.0.14) | phase D: per-outer-row units (unit = outer key) | — |

### 3.2 Block shapes

| class | strategy | units / lineage | why it is exact |
|---|---|---|---|
| DISTINCT | GROUP BY all output columns; if the tuple contains a source key that determines it (anchor PK, key-preserving joins) the unit is reduced to that key (FD); DISTINCT over a global aggregate or over a full GROUP BY key is dropped | unit = tuple or key; presence = group non-empty | DISTINCT and GROUP BY use the same equality (NULL = NULL, collation-aware); with ci collations the representative of equal values is server-chosen in both |
| GROUP BY with ≥ 2 lookup-resolved aliases, a NULL-side group key, or an inexact group key | **tuple-lineage internal view** `J = π(group exprs, agg args, lineage) σ FROM` + the grouped view over `J` | V's only alias is J; J's binlog rows carry the group key → direct units; J traces its aliases by lineage | γ over the materialized join equals γ over the join; J is maintained like any SPJ view |
| COUNT(DISTINCT), GROUP_CONCAT(… ORDER BY), HAVING over aggregates | already per group | group key | a refresh recomputes whole groups |
| UNION ALL (also in CTEs) | **branch plan**: each branch is planned by the block planner; rows tagged `_lv_branch`; one source row may feed several branches (all traced) | (branch, branch unit); per-branch lineage | ⊎ is per-branch; refreshing (b, u) rewrites exactly branch b's rows of u; multiplicities exact |
| UNION / mixed | MySQL precedence: a DISTINCT union absorbs every branch to its left → `DISTINCT(A⊎…⊎C) ⊎ D`; DISTINCT part = internal UNION ALL view + grouped view over it | tuple | UNION = δ(⊎) |
| INTERSECT / EXCEPT (8.0.31+, MariaDB) | internal UNION ALL view with branch counts + grouped view (`HAVING SUM(b=1)>0 AND SUM(b=2)>0` / `=0`) | tuple | set semantics by counting |
| IN / EXISTS with a unique subquery key (its PK, key-preserving join) | **semi-join → inner join**, merged | normal alias | ⋉ = ⋈ when each outer row matches ≤ 1 inner row; IN's NULL logic ≡ `=` |
| IN / EXISTS / NOT EXISTS whose correlation equals the unit | **semi-join alias**: predicate kept in the refresh, subquery table traced by direct units | unit | the predicate of unit u reads only subquery rows with key u |
| other IN / EXISTS with equality correlation `k = x` | inner join with the **internal view of its distinct keys** `SELECT k FROM s WHERE p GROUP BY k` (unique on k) | normal alias over the internal table | ⋉ over a unique key = ⋈ |
| other NOT EXISTS with equality correlation `k = x` | semi-join alias traced by a **forward key query** `… WHERE <outer WHERE without it> AND x IN (k values)` (phase B) | outer units with x = k | the old relevance of a subquery row depends only on outer rows with x = k now or before; those that changed are traced by their own images |
| correlated scalar subquery `(SELECT e FROM s WHERE s.pk = x …)` | **LEFT JOIN** `s ON s.pk = x …`, value `e` (guarded `IF(s.pk IS NULL, NULL, e)` unless `e` is NULL-propagating over at least one column of `s`; a value over outer columns or constants alone keeps its value in the NULL-extended row and is always guarded, A6) | normal alias | ≤ 1 row matches; no row gives NULL in both forms |
| NOT IN | as NOT EXISTS, plus units with `x IS NULL` on any change and **all units** when a NULL k enters/leaves | — | `x NOT IN K` is TRUE iff K = ∅ or (x, K have no NULL and x ∉ K) |
| uncorrelated EXISTS / scalar subquery in WHERE | all units on a change of the subquery's tables | — | the predicate is one value for all rows |
| window functions with PARTITION BY | **partitioned units**: unit = PARTITION BY columns common to all windows (named windows resolved), not unique; filter before windowing keeps whole partitions | partition key; per-alias lineage (rows are 1:1 with source tuples) | window values of a partition depend only on its rows |
| window without PARTITION BY | one unit (Global), **bounded recompute** (one query, diff; no swap) | — | the result is one partition |
| named windows `… OVER w … WINDOW w AS (…)` | resolved like inline windows (partition from the named spec, also through `OVER (w ORDER BY …)`); the WINDOW clause is carried on the plan (`window_sql`) and rendered after HAVING in every refresh SELECT (L2) | as above | syntax |
| top-N `ORDER BY … LIMIT n` | **bounded recompute**; the ORDER BY columns (plain, in expressions, aggregate inputs, of joined aliases) are read columns even when not projected, so an UPDATE of one reaches the recompute (L1): one unit (Global), the refresh query keeps `ORDER BY … LIMIT` (`order_limit_sql`). Phase D: an internal `top-(n+B)` buffer view, changes ranked strictly below its last key skipped | one unit | the refresh recomputes the whole top-n; with ties at the boundary the definition itself picks rows arbitrarily (tests use a total ORDER BY) |
| filter on a window result (top-N per partition) | window block as internal partitioned view + outer filter as chained view (unit = partition key) | partition key | composition |
| RIGHT JOIN at the start, STRAIGHT_JOIN, NATURAL JOIN, parenthesized inner joins | rewritten to LEFT JOIN / JOIN / USING (common columns) / one chain | normal | syntax; inner joins associate |
| `SELECT *` over USING / NATURAL joins of base tables (A4) | `*` expanded as the server does before any rewrite (`analyze::expand_star_over_using`): per join, the shared columns once, first, then the left side's remaining columns, then the right table's; a RIGHT JOIN is the mirrored LEFT JOIN (shared columns and the rest of the right table first); applied along the chain | normal | the servers define `*` exactly so (`COALESCE` of the shared columns equals the preserved side's value) |
| LEFT JOIN with WHERE keeping NULL-extended rows (`IS NULL`, `COALESCE`, `OR`) | lookups of the NULL-side alias drop the conjuncts that are not NULL-rejecting on it (superset), refresh re-evaluates them | normal | dropping conjuncts can only add units |
| child of a cascading / SET NULL foreign key (`CascadeRoute`) | **unit route** when the child's unit columns are the FK columns (units from the parent's image); otherwise **traced** when the view reads the parent through a unit or lineage alias `x` and every alias `y` of the child is joined so that *y's row present ⇒ x holds its parent*: the key equality follows from the WHERE / inner-join equalities plus y's own LEFT JOIN condition, or x is on the NULL side with a join condition that is exactly the FK equality (the constraint guarantees the match). A NULL-side parent with any further conjunct (`… AND p.active = 1`, A1), a parent joined through a third alias, or a weak (NULL-kept) parent alias keeps child rows without parent lineage: **rebuild route** with `CascadeRoute.untraced` naming the conjunct. Both unit and traced routes also need every further cascade out of the child (chains) to be joined that way; a cycle (self-referential key, A3; mutual keys) is never traced | parent's own route / rebuild | a cascaded child row sits only in rows found through its parent; the engine sees no event for it |
| table without PK as anchor | **value units**: unit = its referenced columns (exact kinds), duplicates counted by the refresh | value lineage | equal rows are interchangeable; the refresh rewrites the multiset |
| SQL view as a source | inlined like a derived table (needs `VIEW_DEFINITION` in the catalog) | as merged | a view is a named derived table |
| BIT unit keys | plain units (BIT values compare exactly through typed placeholders) | unit | measured on all six servers (`edge_bit_group`) |
| JSON unit keys (MySQL) | plain units: the engine binds JSON unit values as `CAST(? AS JSON)` (typed unit filters, `UnitKind::Json` from the probed MV column type; engine A, main 882ee55). MariaDB JSON is LONGTEXT: text units | unit | MySQL groups JSON-equal values and compares the bound JSON the same way; the MV keeps the server's representative text of each group |
| opaque (UUID, INET6) unit keys | internal view computing a SQL surrogate (`HEX(k)`, `CAST(k AS CHAR)`) as the downstream unit | surrogate | SQL computes both sides of the comparison |

### 3.3 Full fallbacks (impossibility arguments)

| construct | reason text | why no trace exists |
|---|---|---|
| non-deterministic function (`NOW`, `RAND`, `UUID`, `CURRENT_USER`, …) | `non-deterministic function NOW` | the output changes without any source change: there is no event to trace |
| LIMIT without ORDER BY (top level or in a CTE) | `LIMIT without ORDER BY` | the result is not a function of the data, so there is no target to converge to |
| WITH RECURSIVE (used) | ``recursive CTE `t` `` | an output row depends on an unbounded chain of source rows; no finite key relation maps a changed row to the closure rows it supports. A DRed delete-and-rederive operator would be needed (future) |
| MATCH … AGAINST in natural-language mode | `MATCH ... AGAINST` | relevance uses table-wide statistics: any row change can change every row's score (boolean mode is row-local and becomes incremental) |
| user / system variables | error (servers reject them in views) | — |
| `\|\|` | error: ``\`\|\|\` is OR on the server (sql_mode without PIPES_AS_CONCAT) but concatenation to the parser: write OR or CONCAT()`` | the two readings differ; the definition is refused as written |
| sequences, advisory locks, `LOAD_FILE`, `RANDOM_BYTES`, `SYS_GUID`, replication waits | `non-deterministic function NEXTVAL` | as for `NOW()`: a value or side effect per evaluation |
| CTE chain / derived tables whose inlined form exceeds `INLINE_BUDGET` (32 base-table references; A5) | ``inlining the WITH clause and derived tables reads 64 table references (budget 32): CTE `t5` is referenced 2 times (needs internal views)`` | each CTE reference is inlined as a copy (2^n for a chain of double references); the size is measured once per body before building (linear), so planning stays bounded. A multiply-referenced CTE beyond the budget belongs in an internal view (§4) |
| `SELECT *` over a USING / NATURAL join through a derived table or a comma join (A4, rest) | `SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns` | the chain expansion below covers only base tables in one join chain; elsewhere neither the per-table expansion (duplicate columns) nor an empty projection is a valid view |
| cross join with a changing table | stays `table … contributes to every row` (an *all units* trace: every output row contains that row) | the minimal affected set is the whole view |

Everything else is traceable; until its engine piece lands, the view is Full with a reason that
names the construct and ends in the missing piece:

| construct (today Full) | reason suffix | lands with |
|---|---|---|
| aggregated / DISTINCT / window / top-N CTE or derived table joined to other tables (no pull-up) | `(needs internal views)` | §4 |
| group key on the NULL side of a LEFT JOIN; NULL-side DISTINCT | `group key on the NULL side of a LEFT JOIN (needs internal views)` | tuple lineage J |
| semi-join not correlated on the unit / over a cascading-FK child | ``IN subquery over `s` is not correlated on the view's unit key`` (suffix still to add, see handoff) | distinct-keys view |
| UNION ALL | `UNION ALL … (needs branch plans)` | branch plans |
| UNION (distinct), INTERSECT, EXCEPT | construct name | §3.2 rows (phase C/D) |

### 3.4 Semi- and anti-joins in detail

`P(l) = [NOT] EXISTS(σ k=x(l) ∧ p S)`; `x IN (π_k σ_p S)` normalizes to the EXISTS form. A change
`s⁻→s⁺` of S changes `P` only for outer rows with `x ∈ {k(s⁻), k(s⁺)}`. Those rows are found on
the current outer state: an outer row that had that `x` before and not now changed itself and is
traced by its own images. The refresh re-evaluates `P` against the current state. Other aliases'
lookups may evaluate `P` on the current state: a unit whose `P` flipped is already traced through
S. With S's key unique (`k` = PK of S), `P` is an inner join and S an ordinary alias.

## 4. Internal views (`_lv_int_<hash16>`)

* **What**: a subtree isolated by the classifier (grouped/DISTINCT CTE joined to tables,
  tuple-lineage J, top-N buffer, partitioned window block, multi-table NULL-side derived table,
  DISTINCT part of a UNION, surrogate-key view). Its definition is a self-contained
  `SELECT` with explicit column names; it is planned by the same analyzer (it may need internal
  views itself: a DAG).
* **Name** `_lv_int_` + 16 hex of SHA-256(schema, normalized SQL): identical subtrees of different
  definitions share one table (refcount). `_lv_` names are reserved for users already.
* **Lifecycle**: created (on target `source` only) when a definition that needs it is planned;
  the parent waits (state `building`) until it is live, then is planned against its table as a
  chained view; dropped when no live or next version of any definition references it; rebuilt
  online on source DDL like any view (its swap rebuilds dependants online). Resume: internal
  views have `_lazyview_state` rows (`definition_id` NULL, `parent` = comma list of user views)
  and resume like user views.
* **Overview**: shown in `_lazyview_overview` with `kind = 'internal'` and `parent`; `detail`
  says why it exists (`intermediate of v_x: GROUP BY in CTE t`).
* **Engine contract**: `Outcome::NeedsInternal([InternalView { name, sql, purpose, key }])`;
  the engine ensures the slots, waits, then re-plans the parent with their `TableInfo`s (`key` =
  unique key columns when the MV could not get a PRIMARY KEY, e.g. a nullable group key).
  The tuple-lineage J replaces worker A's interim "remembered before-images" resolver for grouped
  views with ≥ 2 lookup aliases: the analyzer emits J whenever the block planner reports ≥ 2
  aliases without direct units in a grouped block.

## 5. Output column names

The MV must have the oracle VIEW's column names. Measured on all six servers: an alias gives its
name; a column reference (also parenthesized) gives the identifier *as written*; an unaliased
expression gives its source text; names longer than 64 characters or ending in a space become
`Name_exp_<position>`. Flavor-dependent: duplicates of generated names (`Name_exp_<x>` on MySQL /
Percona, `My_exp_<x>` on MariaDB) and comments inside the text (kept by 8.4, stripped by 8.0 and
MariaDB). Items the rewrite leaves alone keep their source text (the server names them); rewritten
items get an explicit alias computed by those rules; when the result would be flavor-dependent
the merge is refused with ``unaliased column `…` needs an alias``. Types come from probing the
flat SELECT; merged expressions are the same expressions, so types match.

## 6. MySQL vs MariaDB

CTEs in views: MySQL 8.0+, MariaDB 10.2+; same visibility rules (earlier CTEs only, no
self-visibility without RECURSIVE), WITH allowed in subqueries and derived tables. `derived_merge`,
`/*+ MERGE NO_MERGE */` and `ALGORITHM=` change execution only (full rebuilds keep them as written).
LATERAL is MySQL-only; INTERSECT/EXCEPT need MySQL 8.0.31+ or MariaDB 10.3+; derived condition
pushdown (MySQL 8.0.22+, MariaDB 10.2+) only affects rebuild speed. Naming differences: §5.

## 7. Tests

* Unit (`src/analyzer/tests.rs`): **differential** — every statement of the `analyze.rs` corpus:
  incremental plans must be identical (Debug-equal ViewPlan), Full plans identical or listed as an
  improvement with the expected new mode; **metamorphic** — CTE/derived form vs. hand-inlined
  form produce identical plans (modulo `full_select`/`hash`); **per class** — strategy and exact
  reason text, including every refusal.
* Integration (`tests/cte.rs`, `tests/shapes.rs`; all 6 flavors, `LAZYVIEW_FULL_REFRESH_MS=0`,
  several seeds): ≥ 12 CTE/derived views (one per class) plus the edge-case corpus (NULL join/
  group/DISTINCT keys, NOT IN with NULLs, NULL-extended rows feeding DISTINCT/GROUP BY, PK-less
  duplicates appearing/disappearing, key moves and key swaps in one transaction, delete+reinsert,
  self-joins through a CTE referenced 3×, ci/ai collations and PAD SPACE, emoji, DECIMAL scale,
  DATETIME(6), TIMESTAMP, ENUM order, BIT, BIGINT UNSIGNED max, ±0.0, nested CTEs, shadowing,
  shared CTE text across definitions, window ties and partition moves, top-N ties at the
  boundary, 30× hammering, big transactions, groups becoming empty, HAVING crossed both ways).
  Each view asserts mode/detail and MV == oracle (rows and column names).
* State: `cargo test --lib` 106 tests; the differential is strict (an incremental plan that
  changes, or a Full plan that changes without an `improvements()` entry naming the new mode,
  fails). `tests/cte.rs`: 45 main cases and 19 edge cases; the mode/detail asserted is the
  engine's own (from `_lazyview_overview` after start) and equals the analyzer's plan.

## 8. Rollout

* **A** (done; analyzer, no executor change): build/normalize/lower; merge family, merge-up,
  pull-up, DISTINCT→GROUP BY, semi-joins with unique keys or unit correlation, scalar lookups
  as LEFT JOINs, join syntax; analyzer side of internal views (`analyze()` with `Options`,
  `Outcome::NeedsInternal`: aggregated / window / top-N CTEs, tuple lineage, distinct keys of
  semi-joins) and of UNION ALL branch plans (`Outcome::Branches`); tests.
* **B** (done and wired, except JSON keys): `analyze::plan` / `referenced_tables` delegate;
  block-planner additions that need no engine change: partitioned windows, bounded recompute
  (top-N, windows without PARTITION BY), semi/anti-join forward-key lookups (EXISTS template
  per alias, `AliasPlan.semi`; NOT IN with its NULL branches), NULL-side lookup weakening (weak
  aliases filter with `lookup_where_sql` and carry no lineage), value units for PK-less
  anchors, BIT units, DISTINCT/GROUP BY unit reduction by FD closure (`reduce_unit`),
  RIGHT/NATURAL/STRAIGHT, JSON group keys (typed unit filters landed on main).
* **C** (engine, other owners; spec sent to the coordinator): internal views (§4), branch plans
  for UNION ALL (one slot, N plans sharing the table, `_lv_branch` in unit filters), NULL-safe
  lineage filters, `detail` from the plan, `VIEW_DEFINITION` in the catalog.
* **D**: top-N buffer, INTERSECT/EXCEPT, LATERAL, surrogate keys, DRed for recursion.

## 9. Server schema & metadata

What the analyzer and planner take from the server today, verified in the code, and what they
should take. Risk: **W** = wrong results possible, **M** = missed incremental opportunity only.

| item | today | risk | decision |
|---|---|---|---|
| columns: type, charset/collation, octet length, INVISIBLE | `catalog.rs` (`information_schema.COLUMNS`) → exact kinds, type families, typed placeholders | — | keep |
| PRIMARY KEY | `KEY_COLUMN_USAGE` `PRIMARY` → `TableInfo.pk` (lineage, semi→join, pull-up, unit reduction) | — | keep |
| cascading FKs, SQL-view sources | `REFERENTIAL_CONSTRAINTS`; `VIEWS.VIEW_DEFINITION` (SQL-view sources stay Full) | — | keep; inline SQL views later (phase D) |
| stored / loadable functions | `engine.rs` reads `ROUTINES`, `mysql.func` and rejects the view | — | keep (their reads are invisible) |
| output names and types | `mv::probe` creates a temporary VIEW of the planned SELECT and reads its `COLUMNS` | M | probe names come from the *rewritten* SELECT (our `names.rs` prediction); see canonical form |
| UNIQUE keys other than the PK | **loaded** (`TableInfo.unique_keys`: `STATISTICS NON_UNIQUE = 0`, whole columns only, every column NOT NULL; `TableInfo::keys()` / `row_key()`) and used as row keys: unit of a PK-less anchor, lineage of a PK-less lookup alias, FD closure in `reduce_unit` (DISTINCT / GROUP BY reduction), the parent key a cascading FK may reference | — | done (block planner); semi→join and pull-up in `normalize.rs` still use `pk` only: next |
| NOT NULL | **loaded** (`Column.nullable`), used for the UNIQUE-key rule above | M | NOT IN over NOT NULL columns on both sides = NOT EXISTS (drops the all-units trace for NULL keys): next. Never for NULL-side aliases (NULL-extended rows) |
| generated columns | **loaded** (`Column.generated { expr, stored, deps }`, base columns parsed from `GENERATION_EXPRESSION`; unparsable → all columns). The block planner **inlines every reference to a VIRTUAL column as its expression** over the alias-qualified base columns (`inline_virtual_columns`; a bare projection item keeps the column's name), so no plan ever reads a VIRTUAL value from a row image: measured on MariaDB 13.0 / 13.2, a unit lookup over `GROUP BY g` with `g` VIRTUAL took the image's value and missed the group (MySQL 8.4 / 26.7, Percona and MariaDB 10.11 happened to pass). An unreadable expression is Full with ``VIRTUAL generated column `t`.`g` with an expression the parser cannot read: …``. A referenced STORED column also marks its base columns referenced | — | done |
| canonical definition (probe `VIEW_DEFINITION`) | not read | **W** (resolution bugs undetected) | **validator, not input**: read it after the probe of the statement as written, parse it, and compare base tables, referenced columns per table and output names with the analyzer's resolution; a mismatch fails the definition with an error naming the difference. Not the input: its printing differs per flavor and version (`count(0)`, charset introducers, `(not(exists(…)))`, comments kept by 8.4), and Full plans, names and errors must stay the user's text |
| session sql_mode | `mv::init_session` keeps the server's mode minus strict / zero-date flags; the parser assumes the default mode | **W** | `ANSI_QUOTES` (`"x"` is a column to the server, a string to us: its changes are never traced), `PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE`, `NO_BACKSLASH_ESCAPES` (tokens), MariaDB `ORACLE`/`MSSQL`/… change the parse. Recommended: strip these flags in `init_session` like the strict ones (server and parser then agree by construction; documented in docs/views.md); otherwise read `@@SESSION.sql_mode` when planning and fail the definition naming the flag. `IGNORE_SPACE`, `ONLY_FULL_GROUP_BY`, `REAL_AS_FLOAT` do not change our parse. Already in the default mode sqlparser 0.63 parses `||` as concatenation (at multiplication precedence) where the servers read OR: refuse `||` with an error asking for `OR` / `CONCAT()` |
| `lower_case_table_names` | not read; tables compared as written, aliases partly case-insensitive (`lower.rs`) | **W** | 1/2: canonicalize schema/table names to the stored `TABLE_NAME` (else binlog events for `orders` never match a definition writing `Orders`); 0: compare tables and aliases exactly and refuse definitions whose aliases collide case-insensitively. Columns are always case-insensitive (correct today) |
| flavor-specific functions | one `MySqlDialect`, one non-deterministic list | **W** | add `NEXTVAL`/`LASTVAL`/`SETVAL`/`NEXT VALUE FOR` (MariaDB sequences), `LOAD_FILE`, `GET_LOCK`/`RELEASE_LOCK`/`IS_FREE_LOCK`/`IS_USED_LOCK` to the non-deterministic set; reject sequences as sources. MariaDB JSON is LONGTEXT (text units, correct) |
| session variables (`group_concat_max_len`, `div_precision_increment`, `time_zone`, `lc_time_names`, `default_week_format`) | the service's session values | — | they change values, not traces; MV and oracle agree when sessions agree; document |
| CHECK constraints, other FDs | not loaded | M | skip |
