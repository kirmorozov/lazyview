//! #278 — the declared index set of a managed view table (owner rulings D1, D6, D7, D9).
//!
//! A view may declare secondary indexes as free-standing follow-up statements
//! (`CREATE [UNIQUE|FULLTEXT] INDEX name ON <view> (cols)`, parsed in
//! [`crate::analyze::decls`]). The statements are a *desired state*, never a command: the engine
//! converges the table's index set to `E ∪ declared` — `E` being its own keys
//! (`PRIMARY`/`_lv_unit`/`_lv_row`/`_lv_a<k>`/`_lv_p<k>`, which no reconcile may touch, H6) — on
//! every pass, so a declaration added later, an index the operator dropped by hand, a hand-made
//! extra and a same-name-different-columns declaration all end in the same place (D9: there is no
//! report-only category).
//!
//! Two seams apply the same plan:
//!
//! * the **shadow** table of a build, before it is filled ([`apply_shadow`]): a freshly created,
//!   empty table takes plain `ALTER … ADD INDEX` statements, which is also how a FULLTEXT index
//!   arrives — MySQL-family FULLTEXT can never be built online (online-ddl-research.md item 1),
//!   and the shadow path is the only place it may be.
//! * the **live** table ([`converge`], `online = true`): every statement carries the family's
//!   online clause explicitly — `ALGORITHM=INPLACE, LOCK=NONE` on MySQL/Percona,
//!   `ALGORITHM=NOCOPY, LOCK=NONE` (then `INPLACE`) on MariaDB, `CREATE INDEX CONCURRENTLY` on
//!   PostgreSQL — because the clause is an assertion that the server refused to copy the table,
//!   not an optimization. A FULLTEXT that is missing from a live table is not attempted at all:
//!   the caller serves it by the shadow path.
//!
//! The session runs the DDL with a small `lock_wait_timeout`, so an idle transaction holding a
//! metadata lock costs one short failed statement and a retry on the engine's backoff rather
//! than a stalled flush loop (online-ddl-research.md item 5).

use anyhow::{anyhow, bail, ensure, Result};
use mysql_async::prelude::*;

use super::{q, Conn, KeyPlan, Kind, MAX_KEY_PARTS, MY_KEY_BYTES, PG_KEY_BYTES};
use crate::analyze::decls::IndexDecl;
use crate::metrics::Metrics;
use crate::mv::Layout;

/// How long one online statement may wait for a metadata lock before the pass gives up and
/// retries later (MDL, online-ddl-research.md item 5).
const MDL_WAIT_S: u64 = 2;

/// One declared index, in the shape it must have on the table. `parts` is the key as the target
/// family's DDL renders it, produced by [`super::fit_key`] and therefore already budget-checked.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Wanted {
    pub(crate) name: String,
    /// The declared columns, in the declared order: what `SEQ_IN_INDEX` must read back (H4).
    pub(crate) cols: Vec<String>,
    pub(crate) parts: String,
    pub(crate) unique: bool,
    pub(crate) fulltext: bool,
}

/// One index the target reports for the table, in the same shape, so the two sides compare.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Actual {
    pub(crate) name: String,
    pub(crate) cols: Vec<String>,
    pub(crate) unique: bool,
    pub(crate) fulltext: bool,
    /// PostgreSQL only: `pg_index.indisvalid = false`, the aftermath of a failed
    /// `CREATE INDEX CONCURRENTLY` that an `IF NOT EXISTS` would silently keep.
    pub(crate) invalid: bool,
}

impl Actual {
    fn matches(&self, w: &Wanted) -> bool {
        !self.invalid
            && self.name.eq_ignore_ascii_case(&w.name)
            && self.unique == w.unique
            && self.fulltext == w.fulltext
            && self.cols.len() == w.cols.len()
            && self
                .cols
                .iter()
                .zip(w.cols.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}

/// Whether `name` is one of lazyview's own indexes of the table `table`: the reconcile never drops
/// or renames one (H6 (c): "dropping or renaming any `_lv_*` index is a test failure"), and on
/// PostgreSQL the primary-key CONSTRAINT's index is the name the SERVER computes for that table,
/// `{table}_pkey`.
///
/// The `_pkey` arm is the EXACT computed name, never a suffix match: an index a user named
/// `orders_pkey_hist` is the user's, not the engine's, and is reconciled like any other declared
/// key (kept while it is declared and matches, dropped when it is not) — it must not be adopted as
/// engine property, which a suffix test would silently do (lv-super order msg_0b786f086d67, on the
/// generic guard's PASS at `c71109a`, 0 findings, msg_08dce71359a1).
pub(crate) fn is_engine_key(table: &str, name: &str) -> bool {
    name == "PRIMARY"
        || name.contains("_lv_")
        || name.eq_ignore_ascii_case(&format!("{table}_pkey"))
}

/// The columns of `layout` a user statement may index: the projection, not the hidden
/// `_lv_u*`/`_lv_a*` columns the engine appended.
fn visible<'a>(layout: &'a Layout, col: &str) -> Option<&'a crate::mv::MvCol> {
    layout
        .cols
        .iter()
        .take(layout.n_visible)
        .find(|c| c.name.eq_ignore_ascii_case(col))
}

/// Whether a MySQL-family column can carry a FULLTEXT index: InnoDB indexes only the character
/// and text types (E9: a FULLTEXT over `DECIMAL` is a named refusal, not a server error).
fn fulltext_capable(col: &crate::mv::MvCol) -> bool {
    matches!(
        col.data_type.to_ascii_lowercase().as_str(),
        "char"
            | "varchar"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
    )
}

/// The budget error of a declared key that does not fit (E11 (c)): it quotes the seam's own
/// numbers, names the index and the view, and says whose budget was too tight.
fn budget_error(view: &str, d: &IndexDecl, plan: &KeyPlan) -> anyhow::Error {
    let family = match plan.dialect {
        Kind::Mysql => "mysql",
        Kind::Postgres => "postgres",
    };
    anyhow!(
        "index `{}` of view `{view}` is over the key budget of target {family}: MY_KEY_BYTES \
         {MY_KEY_BYTES} / PG_KEY_BYTES {PG_KEY_BYTES} / MAX_KEY_PARTS {MAX_KEY_PARTS} ({} parts, \
         key ({})). Shorten a column or index fewer columns; lazyview never adds a prefix a \
         declaration did not write",
        d.name,
        d.cols.len(),
        d.cols.join(", "),
    )
}

/// How the declared indexes of ONE view are named on ONE target (#278, E7 (c) PG). A MySQL-family
/// server names indexes per table, so a declaration is created exactly as the user wrote it.
/// PostgreSQL names them per SCHEMA, and every view routed at a target shares that target's
/// schema, so a name two views both declare cannot exist twice as written — the raw 42P07 death
/// E7 (c) forbids. Only the engine can see both views, so it is the one that says which names must
/// carry the table prefix, and which physical names another view already owns.
#[derive(Debug, Clone, Default)]
pub(crate) struct Naming {
    /// Declared names (folded to lower case) this view shares with another view of the same
    /// schema: rendered `{view}_{name}`, the reading E7 (c) recommends (Q3) and the one
    /// `src/target/postgres.rs` already uses for the engine's own keys (`{table}_pkey`).
    pub(crate) shared: Vec<String>,
    /// Physical names another view of the same schema will use: a declaration may not take one.
    pub(crate) taken: Vec<String>,
}

impl Naming {
    fn shared_for(&self, name: &str) -> bool {
        let n = name.to_ascii_lowercase();
        self.shared.iter().any(|s| s == &n)
    }

    fn taken_for(&self, name: &str) -> bool {
        self.taken.iter().any(|t| t.eq_ignore_ascii_case(name))
    }
}

/// The declared set of one target family: every statement validated (its columns are columns of
/// the view, its kind exists on this family, its key fits this family's budget) and deduplicated
/// by name — E2: the same statement written twice is one index — while the same name with a
/// different shape is refused rather than applied in some arbitrary order.
pub(crate) fn desired(
    view: &str,
    decls: &[IndexDecl],
    layout: &Layout,
    kind: Kind,
    naming: &Naming,
) -> Result<Vec<Wanted>> {
    let plan = KeyPlan::for_layout(layout, kind);
    let mut want: Vec<Wanted> = Vec::with_capacity(decls.len());
    for d in decls {
        ensure!(!d.cols.is_empty(), "index `{}` declares no column", d.name);
        ensure!(
            !is_engine_key(view, &d.name),
            "index name `{}` is in lazyview's own index namespace (`PRIMARY`, `_lv_*`, \
             `{view}_pkey` — the name PostgreSQL gives this view's own primary-key index), which a \
             declaration may not use",
            d.name
        );
        let mut parts = Vec::with_capacity(d.cols.len());
        for c in &d.cols {
            let col =
                visible(layout, c).ok_or_else(|| {
                    anyhow!(
                    "index `{}` of view `{view}` is over `{c}`, which the view does not project; \
                     its columns are {}{}",
                    d.name,
                    layout
                        .cols
                        .iter()
                        .take(layout.n_visible)
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", "),
                    if d.fulltext { "" } else { "; a generated column must be declared too" }
                )
                })?;
            if d.fulltext && matches!(kind, Kind::Mysql) {
                ensure!(
                    fulltext_capable(col),
                    "index kind FULLTEXT is not supported on target mysql for `{view}.{}`: the \
                     column is `{}`, and InnoDB builds FULLTEXT indexes only over character and \
                     text columns; index a text column instead",
                    col.name,
                    col.column_type
                );
            }
            parts.push((col, whole_part(col, plan.part(col))));
        }
        if d.fulltext && matches!(kind, Kind::Postgres) {
            bail!(
                "index kind FULLTEXT is not supported on target postgres: PostgreSQL has no \
                 FULLTEXT index type; use a B-tree index for prefix matches or GIN/trigram \
                 (`CREATE INDEX … USING GIN`) for text search — the declaration of `{}` is refused",
                d.name
            );
        }
        let parts = fit_or_err(view, d, &parts, &plan)?;
        // The key parts are the family's rendering; the compared shape stays the declared columns.
        let cols = d.cols.clone();
        // PG: index names are schema-wide, so two views of one schema that both declare `i_total`
        // collide at the server — the raw 42P07 death E7 (c) forbids. The engine, the only party
        // that can see both views, says which names must carry the table prefix ([`Naming`]); those
        // are created as `{view}_{name}`, exactly as `postgres.rs` names the engine's own keys
        // (`{table}_pkey`), and a name this view alone declares keeps the spelling the user wrote,
        // so a view alone in its schema is indistinguishable from a MySQL-family one (H1, H2 (c) PG
        // read them back under the declared name). The comparison below is made on the physical
        // name, so one declaration is still one index (E2) under either spelling.
        let name = match kind {
            Kind::Postgres => {
                let name = if naming.shared_for(&d.name) {
                    format!("{view}_{}", d.name)
                } else {
                    d.name.clone()
                };
                ensure!(
                    name.len() <= 63,
                    "index `{}` of view `{view}` cannot be named on PostgreSQL: its physical name \
                     `{name}` is longer than PostgreSQL's 63-byte identifier limit, and a name the \
                     server truncates could never be recognised on the next pass. Shorten the index \
                     name; the view's columns are not affected.",
                    d.name
                );
                ensure!(
                    !naming.taken_for(&name),
                    "index `{}` of view `{view}` cannot be named `{name}` on PostgreSQL: another \
                     view of the same schema already owns that name (PostgreSQL names indexes per \
                     schema, not per table). Rename the index in one of the two declarations.",
                    d.name
                );
                name
            }
            _ => d.name.clone(),
        };
        if let Some(prev) = want.iter().find(|w| w.name.eq_ignore_ascii_case(&name)) {
            ensure!(
                prev.cols == cols && prev.unique == d.unique && prev.fulltext == d.fulltext,
                "index `{}` of view `{view}` is declared twice with different shapes: ({}) and \
                 ({}); one name must mean one index",
                d.name,
                prev.cols.join(", "),
                cols.join(", ")
            );
            continue; // E2: the same declaration written twice is one index, no warning needed.
        }
        want.push(Wanted {
            name,
            cols,
            parts,
            unique: d.unique,
            fulltext: d.fulltext,
        });
    }
    Ok(want)
}

/// The key of one declared index, or the budget error that names the seam's numbers.
fn fit_or_err(
    view: &str,
    d: &IndexDecl,
    parts: &[(&crate::mv::MvCol, super::KeyPart)],
    plan: &KeyPlan,
) -> Result<String> {
    super::fit_key(parts, plan).ok_or_else(|| budget_error(view, d, plan))
}

/// The whole-key form of a declared key part.
///
/// `KeyPart::must_prefix` means "a prefix is the only key this family can build for that column".
/// That is true of an unsized `TEXT`/`BLOB`, which declares no length and can never be indexed
/// whole, and it is what PostgreSQL's own widths say about EVERY text column — because PostgreSQL's
/// `text` type is itself unsized (`postgres.rs`'s `key_part`, whose comment explains that a view
/// with a PostgreSQL target is planned with THESE widths for both tables). A column that declares
/// its own length is a different thing: `varchar(64)` is indexed whole by both families, so the
/// flag is an artefact of the widths' provenance, not a limit on the user's key. Without this, the
/// strict seam refused every declared key containing a string column on any view with a PostgreSQL
/// target — H2 (c) PG (`u_status_grp` over `status VARCHAR(10)`) and H7 (c) (`u_id_name` over
/// `id, name`) died in the build with "over the key budget of target" on numbers nowhere near it.
///
/// A part whose column declares NO length keeps the flag, and the strict seam refuses it: that key
/// cannot be built without inventing a prefix, and E8 refuses the `name(4)` spelling that would ask
/// for one, so the honest answer is the named refusal.
fn whole_part(col: &crate::mv::MvCol, part: super::KeyPart) -> super::KeyPart {
    match part {
        super::KeyPart::Str {
            len,
            per_char,
            must_prefix: true,
        } if declared_length(&col.column_type).is_some() => super::KeyPart::Str {
            len,
            per_char,
            must_prefix: false,
        },
        p => p,
    }
}

/// The length a column type declares for itself: `varchar(64)` → `Some(64)`, an unsized `text` →
/// `None`. This is the line between a column that can be indexed whole and one that can only ever
/// be indexed by a prefix.
fn declared_length(column_type: &str) -> Option<u64> {
    let s = column_type.to_ascii_lowercase();
    let open = s.find('(')?;
    let close = open + s[open..].find(')')?;
    s[open + 1..close]
        .trim()
        .split(',')
        .next()?
        .trim()
        .parse()
        .ok()
}

/// What one pass must do: `(drops, adds)`, and whether a change cannot be made online on this
/// family (a missing FULLTEXT index on a MySQL-family table that is already live: the caller must
/// serve it by the shadow path, never by a write-blocking `ALTER`).
///
/// `table` is the table whose index set `have` reports, and it is what decides an engine key
/// ([`is_engine_key`]): everything else in `have` belongs to the user and is reconciled against
/// `want` — kept while it matches, dropped when it does not.
pub(crate) fn plan_changes(
    table: &str,
    want: &[Wanted],
    have: &[Actual],
    online: bool,
    family: Kind,
) -> (Vec<String>, Vec<Wanted>, bool) {
    let mut drops: Vec<String> = Vec::new();
    let mut adds: Vec<Wanted> = Vec::new();
    let mut shadow = false;
    for h in have {
        if is_engine_key(table, &h.name) {
            continue;
        }
        let w = want.iter().find(|w| w.name.eq_ignore_ascii_case(&h.name));
        match w {
            // Same name, right shape, valid: nothing to do (E2: a repeated pass re-applies
            // nothing). Anything else under this name is dropped and re-created (D7).
            Some(w) if h.matches(w) => {}
            Some(_) | None => drops.push(h.name.clone()),
        }
    }
    for w in want {
        if have.iter().any(|h| h.matches(w)) {
            continue;
        }
        if online && w.fulltext && matches!(family, Kind::Mysql) {
            // MySQL: "Permits Concurrent DML: No", error 1846 on `LOCK=NONE`, and the first
            // FULLTEXT rebuilds the hidden FTS_DOC_ID; MariaDB allows one FULLTEXT at a time and
            // no `LOCK=NONE` at all afterwards. So this is the shadow path's, never a live ALTER.
            shadow = true;
            continue;
        }
        adds.push(w.clone());
    }
    (drops, adds, shadow)
}

/// The statement that adds `w` to `t`. `ADD INDEX` (not `ADD KEY`) and the online clauses are
/// written as one token: they are asserted to have reached the server (H5 (c) reads them out of
/// `performance_schema`), and a form the digest rewrites would make the assertion unobservable.
fn add_sql(schema: &str, table: &str, w: &Wanted, clauses: &str) -> String {
    let kind = if w.fulltext {
        "FULLTEXT INDEX"
    } else if w.unique {
        "UNIQUE INDEX"
    } else {
        "INDEX"
    };
    format!(
        "ALTER TABLE {}.{q} ADD {kind} {name} ({parts}){clauses}",
        q(schema),
        q = q(table),
        name = q(&w.name),
        parts = w.parts,
    )
}

/// The statement that drops an index of `t`.
fn drop_sql(schema: &str, table: &str, name: &str, clauses: &str) -> String {
    format!(
        "ALTER TABLE {}.{t} DROP INDEX {n}{clauses}",
        q(schema),
        t = q(table),
        n = q(name),
    )
}

/// The online clauses of the MySQL family, per product (online-ddl-research.md item 2: the
/// clause is an assertion, never an optimization, and the two products spell it differently).
enum Product {
    Mysql,
    MariaDb,
}

/// Which MySQL-family product this target is: the target layer has no `Kind` per product yet
/// (P10), so the version string is asked of the server itself, where the answer is.
async fn product(conn: &mut mysql_async::Conn) -> Result<Product> {
    let v: String = conn
        .query_first("SELECT CAST(VERSION() AS CHAR)")
        .await?
        .unwrap_or_default();
    Ok(if v.to_ascii_lowercase().contains("mariadb") {
        Product::MariaDb
    } else {
        Product::Mysql
    })
}

/// The indexes of `schema`.`table` as the MySQL family reports them (`information_schema`
/// `STATISTICS`, the read `has_row_index` already uses — this one reads the whole set).
pub(crate) async fn mysql_index_set(
    conn: &mut mysql_async::Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<Actual>> {
    let rows: Vec<(String, Option<String>, i64, String, i64)> = conn
        .exec(
            "SELECT CAST(INDEX_NAME AS CHAR), CAST(COLUMN_NAME AS CHAR), \
                    CAST(NON_UNIQUE AS SIGNED), CAST(INDEX_TYPE AS CHAR), CAST(SEQ_IN_INDEX AS SIGNED) \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (schema, table),
        )
        .await?;
    Ok(group(rows))
}

/// Rows of `STATISTICS` grouped into the compared shape (also what `pg_index` is folded into).
fn group(rows: Vec<(String, Option<String>, i64, String, i64)>) -> Vec<Actual> {
    let mut out: Vec<Actual> = Vec::new();
    for (name, col, non_unique, kind, _seq) in rows {
        let fulltext = kind.eq_ignore_ascii_case("FULLTEXT");
        let unique = non_unique == 0;
        match out.iter_mut().find(|a| a.name == name) {
            Some(a) => {
                if let Some(c) = col {
                    a.cols.push(c);
                }
                a.unique &= unique;
                a.fulltext |= fulltext;
            }
            None => out.push(Actual {
                name,
                cols: col.into_iter().collect(),
                unique,
                fulltext,
                invalid: false,
            }),
        }
    }
    out
}

/// The policy of one declared-index statement: **one** attempt. The supervisor's `STATEMENTS`
/// policy (3 tries) is wrong for online DDL: each try waits the session's whole
/// `lock_wait_timeout` for a metadata lock, so a parked reader turned one pass into three
/// consecutive queues of pending exclusive MDL — measured as `slow flush … elapsed=9.37s` on a
/// 2 s wait, with the writer queued behind it. The retry belongs to the reconcile pass instead
/// (the engine's `MDL_PARK`), on the engine's clock, where the flush loop is not held behind it
/// and the table is not held exclusively while it sleeps.
const DDL_RETRY: crate::supervisor::Policy = crate::supervisor::Policy {
    attempts: 1,
    base: std::time::Duration::from_millis(50),
    cap: std::time::Duration::from_millis(50),
    jitter: false,
};

/// One MySQL-family statement, sent once: see [`DDL_RETRY`] for why the usual retry policy of an
/// idempotent target step is deliberately not used here.
async fn mysql_stmt(
    conn: &mut mysql_async::Conn,
    what: &str,
    sql: String,
    metrics: &Metrics,
) -> Result<()> {
    super::with_retry_mut(metrics, what, DDL_RETRY, conn, |conn| {
        let sql = sql.clone();
        Box::pin(async move { conn.query_drop(sql).await.map_err(Into::into) })
    })
    .await
}

/// The whole MySQL-family pass: `SET lock_wait_timeout`, the drops, then the adds, each statement
/// its own online DDL. MariaDB's `NOCOPY` is tried first and `INPLACE` is the fallback (a clause
/// the server refuses is an error, never a silent copy).
async fn mysql_apply(
    conn: &mut mysql_async::Conn,
    schema: &str,
    table: &str,
    drops: &[String],
    adds: &[Wanted],
    online: bool,
    metrics: &Metrics,
) -> Result<()> {
    if drops.is_empty() && adds.is_empty() {
        return Ok(());
    }
    let product = product(conn).await?;
    let session = format!("SET SESSION lock_wait_timeout = {MDL_WAIT_S}");
    mysql_stmt(conn, "setting the reconcile lock wait", session, metrics).await?;
    // The online clauses are assertions: `ALGORITHM`/`LOCK` are written so that a server that
    // would copy the table refuses with an error instead of blocking the writers (D6).
    let clauses = if online {
        match product {
            Product::Mysql => ", ALGORITHM=INPLACE, LOCK=NONE",
            Product::MariaDb => ", ALGORITHM=NOCOPY, LOCK=NONE",
        }
    } else {
        ""
    };
    for name in drops {
        let sql = drop_sql(schema, table, name, clauses);
        match mysql_stmt(conn, "dropping a stale index", sql.clone(), metrics).await {
            Ok(()) => {}
            // MariaDB has no NOCOPY for this shape: the same statement with INPLACE is the
            // documented answer, and it is still an online assertion.
            Err(e)
                if online
                    && matches!(product, Product::MariaDb)
                    && format!("{e:?}").to_ascii_lowercase().contains("algorithm") =>
            {
                mysql_stmt(
                    conn,
                    "dropping a stale index (INPLACE)",
                    drop_sql(schema, table, name, ", ALGORITHM=INPLACE, LOCK=NONE"),
                    metrics,
                )
                .await?;
            }
            Err(e) => return Err(e),
        }
    }
    for w in adds {
        let sql = add_sql(schema, table, w, clauses);
        match mysql_stmt(conn, "creating a declared index", sql.clone(), metrics).await {
            Ok(()) => {}
            Err(e)
                if online
                    && matches!(product, Product::MariaDb)
                    && format!("{e:?}").to_ascii_lowercase().contains("algorithm") =>
            {
                mysql_stmt(
                    conn,
                    "creating a declared index (INPLACE)",
                    add_sql(schema, table, w, ", ALGORITHM=INPLACE, LOCK=NONE"),
                    metrics,
                )
                .await?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Whether a failed statement is a metadata-lock wait the caller should retry quietly rather
/// than report (MDL, online-ddl-research.md item 5).
pub(crate) fn is_mdl_wait(e: &anyhow::Error) -> bool {
    super::is_lock_wait_timeout(e)
}

/// What one pass over a live table ended in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Outcome {
    /// The table already carried exactly `E ∪ declared`: nothing was sent to the target.
    Current,
    /// Indexes were created or dropped.
    Changed,
    /// A declared index cannot be built online on this family (a MySQL-family FULLTEXT on a live
    /// table): the caller must serve it by a rebuild, whose shadow path builds it.
    Shadow,
}

/// The declared half of the resume check (#278, E6 / H5): `true` when the table's index set is
/// exactly the engine's own keys plus `decls` — nothing hand-made, nothing mismatched. A `false`
/// answer sends the view to the build path, because a table carrying an index lazyview did not
/// create is not "the same table" and a rebuild is the only honest way to serve it; an `Err` is a
/// declaration the target cannot take at all, which the build reports as the build's own failure.
pub(crate) async fn shape_matches(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    table: &str,
    view: &str,
    layout: &Layout,
    decls: &[IndexDecl],
    naming: &Naming,
) -> Result<bool> {
    let want = desired(view, decls, layout, kind, naming)?;
    let drops = match conn {
        Conn::Mysql(c) => {
            let have = mysql_index_set(c, schema, table).await?;
            plan_changes(table, &want, &have, false, kind).0
        }
        Conn::Postgres(c) => {
            let have = c.index_set(schema, table).await?;
            plan_changes(table, &want, &have, false, kind).0
        }
    };
    Ok(drops.is_empty())
}

/// Converges the index set of the live table `schema`.`table` to `E ∪ declared` with the family's
/// online DDL. Idempotent: a table that already matches answers [`Outcome::Current`] without
/// sending any statement (E2 (c): "the statement is not re-applied once the shape matches").
pub(crate) async fn converge(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    table: &str,
    view: &str,
    layout: &Layout,
    decls: &[IndexDecl],
    metrics: &Metrics,
    naming: &Naming,
) -> Result<Outcome> {
    let want = desired(view, decls, layout, kind, naming)?;
    match conn {
        Conn::Mysql(c) => {
            let have = mysql_index_set(c, schema, table).await?;
            let (drops, adds, shadow) = plan_changes(table, &want, &have, true, kind);
            if drops.is_empty() && adds.is_empty() {
                return Ok(if shadow {
                    Outcome::Shadow
                } else {
                    Outcome::Current
                });
            }
            mysql_apply(c, schema, table, &drops, &adds, true, metrics).await?;
            Ok(if shadow {
                Outcome::Shadow
            } else {
                Outcome::Changed
            })
        }
        Conn::Postgres(c) => {
            let have = c.index_set(schema, table).await?;
            let (drops, adds, _) = plan_changes(table, &want, &have, true, kind);
            if drops.is_empty() && adds.is_empty() {
                return Ok(Outcome::Current);
            }
            c.converge_declarations(schema, table, &drops, &adds)
                .await?;
            Ok(Outcome::Changed)
        }
    }
}

/// Converges the index set of the **shadow** table of a build, before it is filled: the table is
/// empty and nobody reads it, so no online clause is needed and this is where a FULLTEXT index
/// belongs. A declaration the target cannot take fails the build here, with the view still
/// serving its previous version.
///
/// PostgreSQL is the one family that cannot always do this: index names are schema-wide there, and
/// the live table of the same view is still in that schema while its shadow is built (the swap
/// comes later), so creating the shadow's declared index under the name the live table's copy
/// already holds would die with 42P07 in the middle of a rebuild. A rebuild therefore leaves the
/// declarations to the online pass, which the swap arms at once and which the state row waits for
/// (`building` until the table carries them); a first build, with no live table to collide with,
/// creates them here like every other family.
pub(crate) async fn apply_shadow(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    table: &str,
    view: &str,
    layout: &Layout,
    decls: &[IndexDecl],
    metrics: &Metrics,
    naming: &Naming,
) -> Result<()> {
    if matches!(kind, Kind::Postgres) && conn.table_kind(schema, view).await?.is_some() {
        return Ok(());
    }
    let want = desired(view, decls, layout, kind, naming)?;
    if want.is_empty() {
        return Ok(());
    }
    match conn {
        Conn::Mysql(c) => {
            let have = mysql_index_set(c, schema, table).await?;
            let (drops, adds, _) = plan_changes(table, &want, &have, false, kind);
            mysql_apply(c, schema, table, &drops, &adds, false, metrics).await
        }
        Conn::Postgres(c) => {
            let have = c.index_set(schema, table).await?;
            let (drops, adds, _) = plan_changes(table, &want, &have, false, kind);
            c.converge_declarations(schema, table, &drops, &adds).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::MvCol;

    fn col(name: &str, column_type: &str) -> MvCol {
        MvCol {
            name: name.to_string(),
            src_name: name.to_string(),
            column_type: column_type.to_string(),
            data_type: column_type
                .split('(')
                .next()
                .unwrap_or(column_type)
                .to_string(),
            charset: None,
            collation: None,
            nullable: false,
        }
    }

    fn layout(cols: &[MvCol]) -> Layout {
        Layout {
            cols: cols.to_vec(),
            n_visible: cols.len(),
            hidden: Vec::new(),
            unit_pos: Vec::new(),
            primary: false,
            lineage: Vec::new(),
            epoch: Vec::new(),
            identity: Vec::new(),
            partial: Vec::new(),
            key_budget: MY_KEY_BYTES,
        }
    }

    fn decl(name: &str, cols: &[&str]) -> IndexDecl {
        IndexDecl {
            name: name.to_string(),
            table: "orders".to_string(),
            cols: cols.iter().map(|c| c.to_string()).collect(),
            unique: false,
            fulltext: false,
            stmt: format!("CREATE INDEX {name} ON orders ({})", cols.join(", ")),
        }
    }

    fn naming() -> Naming {
        Naming {
            shared: Vec::new(),
            taken: Vec::new(),
        }
    }

    /// The engine's own index set of one table, as the target reports it: MySQL's `PRIMARY`, the
    /// primary-key constraint's index PostgreSQL names for that table, and the engine's `_lv_*`
    /// keys.
    fn engine_keys(table: &str) -> Vec<Actual> {
        let names: Vec<String> = vec![
            "PRIMARY".to_string(),
            format!("{table}_pkey"),
            "_lv_unit".to_string(),
            "_lv_a0".to_string(),
        ];
        names
            .into_iter()
            .map(|name| Actual {
                cols: vec!["id".to_string()],
                unique: name == "PRIMARY" || name.ends_with("_pkey"),
                fulltext: false,
                invalid: false,
                name,
            })
            .collect()
    }

    /// #278 (lv-super order msg_0b786f086d67, on the generic guard's PASS at `c71109a`):
    /// engine ownership of a `_pkey` name is the EXACT name the server computes for this table
    /// (`{table}_pkey`), never a suffix. `orders_pkey_hist` is a user's index and the reconcile
    /// treats it as one.
    #[test]
    fn engine_key_is_the_exact_computed_pkey_not_a_suffix() {
        // What the engine owns on `orders`.
        assert!(is_engine_key("orders", "PRIMARY"), "MySQL's primary key");
        assert!(
            is_engine_key("orders", "orders_pkey"),
            "the name PostgreSQL gives the primary-key index of `{}`",
            "orders"
        );
        assert!(
            is_engine_key("orders", "ORDERS_PKEY"),
            "identifiers are compared case-insensitively: another spelling of the same index"
        );
        assert!(
            is_engine_key("orders", "orders__lv_new_pkey"),
            "the shadow table's own pkey carries `_lv_` and stays the engine's"
        );
        assert!(is_engine_key("orders", "_lv_unit"), "the engine's unit key");
        assert!(is_engine_key("orders", "_lv_row"), "the engine's row key");

        // What only LOOKS like one — every one of these is the user's.
        assert!(
            !is_engine_key("orders", "orders_pkey_hist"),
            "`orders_pkey_hist` is not the name PostgreSQL computes for `orders`; a suffix match \
             here would adopt a user's index as engine property and the reconcile would never \
             touch it again"
        );
        assert!(
            !is_engine_key("orders", "orders_pkeyx"),
            "one character more is a different name"
        );
        assert!(
            !is_engine_key("orders", "pkey"),
            "the bare word is not a `{}`-prefixed engine name", "orders"
        );
        assert!(
            !is_engine_key("orders", "items_pkey"),
            "another table's primary-key index is not an engine key OF `orders` — the engine never \
             claims another table's constraint"
        );
        assert!(!is_engine_key("orders", "i_total"), "an ordinary declared name");
    }

    /// The same rule seen from the declaration: a name ending in `_pkey` that is not this view's
    /// `{view}_pkey` is accepted as the user's own, and the reconcile keeps, creates and drops it
    /// by the declared set. The name that IS the engine's stays refused.
    #[test]
    fn a_pkey_suffixed_declaration_is_user_owned_and_reconciled() {
        let l = layout(&[col("id", "int"), col("total", "decimal(10,2)")]);
        // (a) declared: the user's spelling survives on both families (MySQL names per table; on
        // PostgreSQL this name is not shared, so `Naming` leaves it alone).
        for kind in [Kind::Mysql, Kind::Postgres] {
            let w = desired(
                "orders",
                &[decl("orders_pkey_hist", &["total"])],
                &l,
                kind,
                &naming(),
            )
            .unwrap_or_else(|e| {
                panic!("{kind:?}: a declaration named `orders_pkey_hist` is not in the engine's \
                        namespace and must be accepted as the user's own: {e:#}")
            });
            assert_eq!(w.len(), 1, "one declaration is one index");
            assert_eq!(
                w[0].name, "orders_pkey_hist",
                "{kind:?}: the name must stay exactly as the user wrote it"
            );
        }
        // (b) the name the engine DOES own on this view's table stays refused, so the tightening
        // did not open that door.
        let e = desired(
            "orders",
            &[decl("orders_pkey", &["total"])],
            &l,
            Kind::Postgres,
            &naming(),
        )
        .expect_err("the view's own `{view}_pkey` belongs to lazyview, not to a declaration");
        let msg = format!("{e:#}");
        assert!(
            msg.contains("orders_pkey") && msg.contains("namespace"),
            "the refusal must name the refused name and the namespace: {msg}"
        );

        // (c) reconciled per the declared set: kept while declared and matching, created when
        // missing, dropped when no longer declared — and an engine key is never in any of the three
        // lists. A suffix match would have made all three silently do nothing.
        let want = desired(
            "orders",
            &[decl("orders_pkey_hist", &["total"])],
            &l,
            Kind::Mysql,
            &naming(),
        )
        .unwrap();
        let mine = || {
            vec![Actual {
                name: "orders_pkey_hist".to_string(),
                cols: vec!["total".to_string()],
                unique: false,
                fulltext: false,
                invalid: false,
            }]
        };
        let mut present = engine_keys("orders");
        present.extend(mine());

        let (drops, adds, _) = plan_changes("orders", &want, &present, true, Kind::Mysql);
        assert!(
            drops.is_empty() && adds.is_empty(),
            "E2 (c): the table already carries E ∪ declared, so a repeated pass sends nothing; \
             drops={drops:?} adds={adds:?}"
        );

        let (drops, adds, _) = plan_changes("orders", &want, &engine_keys("orders"), true, Kind::Mysql);
        assert!(
            drops.is_empty(),
            "H6 (c): not one engine key may be dropped, whatever the user declared; drops={drops:?}"
        );
        assert_eq!(
            adds.iter().map(|w| w.name.as_str()).collect::<Vec<_>>(),
            vec!["orders_pkey_hist"],
            "the declared key whose name merely ends in `_pkey` must be CREATED like any other \
             declared key, not passed over as engine property"
        );

        let (drops, adds, _) = plan_changes("orders", &[], &present, true, Kind::Mysql);
        assert_eq!(
            drops,
            vec!["orders_pkey_hist".to_string()],
            "D9: a user index that is no longer declared is reconciled away — the same fate as any \
             other user index, which is exactly what `orders_pkey` and the `_lv_*` keys must never \
             share"
        );
        assert!(adds.is_empty());
        let (drops, _, _) = plan_changes("orders", &[], &engine_keys("orders"), true, Kind::Mysql);
        assert!(
            drops.is_empty(),
            "and with nothing declared, nothing of the engine's own set is dropped; drops={drops:?}"
        );
    }
}
