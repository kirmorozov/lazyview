//! #278 — user-declared secondary indexes (and generated columns) on materialized views: the
//! expected-behavior tests of `tmp/out/lvs-indexes/plan/expectations.md`, one `scenario` per case,
//! asserting part **(c)** (the resulting view-table DDL) against `information_schema.STATISTICS`
//! (MySQL family) and `pg_indexes`/`pg_index` (PostgreSQL target).
//!
//! ## These tests are INTENTIONALLY RED (owner ruling D8)
//!
//! The ladder is expectations → plan → **failing tests** → implementation
//! (`tmp/out/lvs-indexes/plan/breakdown.md` P1-P3b; the red list of test names is
//! `tests/indexes-known-red.txt`, the shape of `tests/chaos-known-red.txt:1-8`). They fail because
//! the feature is absent, never because of a test bug:
//!
//! * no follow-up-statement grammar: `parse_definition` demands one statement
//!   (`SINGLE_SELECT`, src/analyze/parse.rs:208), so a (a)-script with a `CREATE INDEX` never
//!   reaches the planner and no index is ever rendered (`create_sql_marked`
//!   src/target/mysql.rs:113-128, `create_table_sql`/`create_index`
//!   src/target/postgres.rs:686-762 render only the engine's own keys);
//! * nothing persists a declaration (src/meta.rs:20-30, 319-328);
//! * the engine has no target-side online-DDL mechanism at all (no `ALGORITHM=`/`LOCK=NONE`/
//!   `CONCURRENTLY` in `src/`, facts.md "Online DDL today"), and `try_resume` checks only the
//!   column list and `has_row_index` (src/engine.rs:2666-2675).
//!
//! A cifix session must **not** weaken, skip or delete any test here: the implementation PRs turn
//! them green. Every assertion message carries the expectations.md case id and its (a)/(b)/(c)
//! part, so a red run says which expectation is unmet.
//!
//! Binding inputs (read them before changing anything here): `plan/decisions.md` (D1-D9),
//! `plan/expectations.md`, `plan/breakdown.md`, `plan/facts.md`, and `plan/online-ddl-research.md`
//! (FULLTEXT is never online on MySQL-family — so the FULLTEXT-on-a-live-table cases assert
//! "no write-blocking observed" plus convergence by whichever path ran, never a live
//! `ALTER … ADD FULLTEXT`; secondary/unique online cases assert the per-flavor clause; the PG cases
//! cover the `INVALID` aftermath of a failed `CREATE INDEX CONCURRENTLY`). ClickHouse is not in
//! this ladder (D2, and no `src/target/clickhouse` exists).
//!
//! Corpora are the pinned ones of expectations.md (`A` = tests/regressions/availability.rs:4-6,
//! `B` = tests/targets.rs:447-450) with neutral names; nothing is invented.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use mysql_async::Conn;
use tokio_postgres::Client;

use common::engineproc;
use common::pg;
use common::*;

// ---------------------------------------------------------------------------------------------
// (b) the original db: the pinned corpora of expectations.md, verbatim
// ---------------------------------------------------------------------------------------------

/// Corpus `A` — `tests/regressions/availability.rs:4-6`.
const A: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, status VARCHAR(10) NOT NULL)",
    "INSERT INTO entities VALUES (10,1,'new'),(11,1,'paid'),(12,2,'new'),(13,3,'paid')",
];

/// Corpus `B` — `tests/targets.rs:447-450`.
const B: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
    "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL)",
    "INSERT INTO entities VALUES (1,'alpha'),(2,'beta'),(3,'gamma')",
    "INSERT INTO items VALUES (1,1,10.50),(2,1,20.25),(3,2,5.00),(4,3,7.75),(5,2,0.00)",
];

/// Corpus `B` widened by the `VARCHAR(2000)` column E11 needs ("corpus `B` widened by the extra
/// column"), so the declared key over `(name, total, entity_id)` overflows the key budget.
const BW: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
    "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL, \
     name VARCHAR(2000) NOT NULL DEFAULT 'x')",
    "INSERT INTO entities VALUES (1,'alpha'),(2,'beta'),(3,'gamma')",
    "INSERT INTO items VALUES (1,1,10.50,'a'),(2,1,20.25,'b'),(3,2,5.00,'c'),(4,3,7.75,'d'),(5,2,0.00,'e')",
];

// The (a) scripts, verbatim from expectations.md.
const SEL_TOTALS: &str =
    "SELECT entity_id, SUM(amount) AS total, COUNT(*) AS n FROM items GROUP BY entity_id";
const SEL_STATUS: &str = "SELECT id, grp, status FROM entities";
const SEL_NAMES: &str = "SELECT id, name FROM entities";
const SEL_LINKS: &str = "SELECT e.id AS id, e.name AS name, i.amount AS amount FROM entities e JOIN items i ON i.entity_id = e.id";

/// The virtual generated column of H8/E3 (D4: declared as an explicit follow-up statement; the
/// verb's exact spelling is Q1b/OPEN, this is D4's own wording).
const H8_COLUMN: &str = "ALTER TABLE entity_status ADD COLUMN sig VARCHAR(32) \
                         AS (CONCAT(status, '-', grp)) VIRTUAL";

// ---------------------------------------------------------------------------------------------
// The (c) notation of expectations.md: IDX(t) = E(t) ∪ declared, read off the target
// ---------------------------------------------------------------------------------------------

/// One declared index: `(name, columns in SEQ_IN_INDEX order, unique, fulltext)` — the AST the
/// grammar must produce (H1-H4) and the shape (c) asserts.
#[derive(Debug, Clone, Copy)]
struct Decl {
    name: &'static str,
    cols: &'static [&'static str],
    unique: bool,
    fulltext: bool,
}

const fn decl(name: &'static str, cols: &'static [&'static str]) -> Decl {
    Decl {
        name,
        cols,
        unique: false,
        fulltext: false,
    }
}
const fn unique(name: &'static str, cols: &'static [&'static str]) -> Decl {
    Decl {
        name,
        cols,
        unique: true,
        fulltext: false,
    }
}
const fn fulltext(name: &'static str, cols: &'static [&'static str]) -> Decl {
    Decl {
        name,
        cols,
        unique: false,
        fulltext: true,
    }
}

/// An index as the target reports it: `(name, columns in key order, unique, kind)`, `kind` being
/// `BTREE` / `FULLTEXT` (`INDEX_TYPE` on MySQL family, read off `indexdef` on PostgreSQL).
type Idx = (String, Vec<String>, bool, String);

/// `E(t)`: the engine's own index set — `PRIMARY KEY`/`KEY _lv_unit`, `UNIQUE KEY _lv_row`,
/// `KEY _lv_a<k>`, `KEY _lv_p<k>` (src/target/mod.rs:754-873; on PostgreSQL the physical names are
/// `<table>_lv_…`, src/target/postgres.rs:1417, and the primary key is `<table>_pkey`). H6 (c):
/// "dropping or renaming any `_lv_*` index is a test failure".
fn engine_key(name: &str) -> bool {
    name == "PRIMARY" || name.contains("_lv_") || name.ends_with("_pkey")
}

/// (c) `IDX(t) = E(t) ∪ declared set`, nothing else (convergence, not addition: the count matters).
fn expect_idx(case: &str, table: &str, got: &[Idx], declared: &[Decl]) -> Result<()> {
    for d in declared {
        let hit = got.iter().find(|(n, _, _, _)| n == d.name);
        let Some((_, cols, uniq, kind)) = hit else {
            bail!(
                "{case} (c): `{table}` has no index `{}` — IDX({table}) must be E({table}) ∪ \
                 {{ {} ({}) {} }}, nothing else; the table carries {got:?}",
                d.name,
                d.name,
                d.cols.join(", "),
                if d.fulltext {
                    "FULLTEXT"
                } else if d.unique {
                    "UNIQUE"
                } else {
                    "non-unique"
                },
            );
        };
        let same =
            cols.len() == d.cols.len() && cols.iter().zip(d.cols).all(|(a, b)| a.as_str() == *b);
        ensure!(
            same,
            "{case} (c): index `{}` is over ({}) as stored, expected ({}) in SEQ_IN_INDEX order \
             (H4: the other order fails the test)",
            d.name,
            cols.join(", "),
            d.cols.join(", "),
        );
        ensure!(
            *uniq == d.unique,
            "{case} (c): index `{}` has UNIQUE={uniq}, the declaration says {}",
            d.name,
            d.unique,
        );
        let want_kind = if d.fulltext { "FULLTEXT" } else { "BTREE" };
        ensure!(
            kind.eq_ignore_ascii_case(want_kind),
            "{case} (c): index `{}` is of kind `{kind}`, the declaration says `{want_kind}`",
            d.name,
        );
    }
    let extra: Vec<&String> = got
        .iter()
        .map(|(n, _, _, _)| n)
        .filter(|n| !engine_key(n) && !declared.iter().any(|d| d.name == **n))
        .collect();
    ensure!(
        extra.is_empty(),
        "{case} (c): `{table}` carries indexes that are neither the engine set E nor the declared \
         set: {extra:?} (part (c) says `nothing else` — convergence, not addition)"
    );
    Ok(())
}

/// The rows of `information_schema.STATISTICS` for `t` (name, columns in `SEQ_IN_INDEX` order,
/// unique, kind) — the query shape of tests/upsert.rs:103-123 plus `INDEX_TYPE` for FULLTEXT.
async fn myf_idx(c: &mut Conn, t: &str) -> Result<Vec<Idx>> {
    let rows = rows(
        c,
        &format!(
            "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, INDEX_TYPE, SEQ_IN_INDEX \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            q(t),
        ),
    )
    .await?;
    let mut acc: BTreeMap<String, (Vec<String>, bool, String)> = BTreeMap::new();
    for r in rows {
        let get = |i: usize| r.get(i).and_then(|x| x.clone()).unwrap_or_default();
        let (name, col, non_unique, kind) = (get(0), get(1), get(2) == "0", get(3));
        let e = acc.entry(name).or_insert_with(|| (Vec::new(), true, kind));
        e.0.push(col);
        if non_unique {
            e.1 = false;
        }
    }
    Ok(acc
        .into_iter()
        .map(|(name, (cols, uniq, kind))| (name, cols, uniq, kind))
        .collect())
}

/// The PG twin of [`myf_idx`] — `pg_indexes.indexdef` (no such helper existed in `tests/`,
/// facts.md: "a PG-side red test must add one"): name, columns, unique and kind, with
/// `pg_index.indisvalid` folded into the kind as `INVALID` so the aftermath of a failed
/// `CREATE INDEX CONCURRENTLY` (online-ddl-research.md item 3) is visible to an assertion.
async fn pg_idx(client: &Client, schema: &str, table: &str) -> Result<Vec<Idx>> {
    let rows = client
        .query(
            "SELECT c.relname, i.indexdef, COALESCE(ix.indisvalid, true) \
             FROM pg_indexes i \
             JOIN pg_class c ON c.relname = i.indexname \
             JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname \
             LEFT JOIN pg_index ix ON ix.indexrelid = c.oid \
             WHERE i.schemaname = $1 AND i.tablename = $2 ORDER BY c.relname",
            &[&schema, &table],
        )
        .await
        .with_context(|| format!("pg_indexes for {schema}.{table}"))?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.get(0);
        let def: String = r.get(1);
        let valid: bool = r.get(2);
        let lower = def.to_ascii_lowercase();
        let cols = match def.find('(') {
            Some(a) if a + 1 < def.len() => {
                let b = def.rfind(')').unwrap_or(def.len());
                if b <= a + 1 {
                    Vec::new()
                } else {
                    def[a + 1..b]
                        .split(',')
                        .map(|p| {
                            p.trim()
                                .trim_matches('"')
                                .split_whitespace()
                                .next()
                                .unwrap_or("")
                                .to_string()
                        })
                        .filter(|p| !p.is_empty())
                        .collect()
                }
            }
            _ => Vec::new(),
        };
        let kind = if !valid {
            "INVALID".to_string()
        } else if lower.contains("using fulltext") {
            "FULLTEXT".to_string()
        } else {
            match lower.find("using") {
                Some(i) => def
                    .get(i + 5..)
                    .and_then(|s| s.split_whitespace().next())
                    .unwrap_or("btree")
                    .to_ascii_uppercase(),
                None => "BTREE".to_string(),
            }
        };
        out.push((name, cols, lower.contains("unique"), kind));
    }
    Ok(out)
}

/// The names of the `INVALID` indexes of `t` on PostgreSQL (`pg_index.indisvalid = false`): what a
/// failed `CREATE INDEX CONCURRENTLY` leaves behind, and what `IF NOT EXISTS` silently skips
/// (online-ddl-research.md item 3; the reconciler's sweep duty).
async fn pg_invalid(client: &Client, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT ic.relname FROM pg_index i \
             JOIN pg_class ic ON ic.oid = i.indexrelid \
             JOIN pg_class t ON t.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE n.nspname = $1 AND t.relname = $2 AND NOT i.indisvalid ORDER BY ic.relname",
            &[&schema, &table],
        )
        .await
        .context("pg_index indisvalid sweep")?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// The `SHOW CREATE TABLE` of `t` (H1 (c): "On MYF: `KEY \`i_total\` (\`total\`)` in
/// `SHOW CREATE TABLE`"; H3 (c): `FULLTEXT KEY \`ft_name\` (\`name\`)`).
async fn myf_show_create(c: &mut Conn, t: &str) -> Result<String> {
    let r = rows(c, &format!("SHOW CREATE TABLE {}", ident(t))).await?;
    ensure!(
        !r.is_empty() && r[0].len() > 1,
        "`{t}` does not exist on the target"
    );
    Ok(r[0][1].clone().unwrap_or_default())
}

/// `CREATE_TIME` of `t`: the witness of H5 (c) "the **same** table (never a replacement)" — a
/// shadow rebuild would replace the table and move it.
async fn myf_create_time(c: &mut Conn, t: &str) -> Result<Option<String>> {
    let r = rows(
        c,
        &format!(
            "SELECT CAST(CREATE_TIME AS CHAR) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {}",
            q(t),
        ),
    )
    .await?;
    Ok(r.first().and_then(|x| x.first().cloned().flatten()))
}

/// The statement digests the server has really executed (performance_schema, on by default):
/// the observable of the per-flavor online clauses (online-ddl-research.md item 2 — "always emit
/// ALGORITHM/LOCK explicitly: they are ASSERTIONS"). `None` when performance_schema is not
/// available on this server at all: a missing instrumentation is not a failed expectation, so the
/// caller says so and checks the rest rather than blaming the engine.
async fn myf_digests(c: &mut Conn, like: &str) -> Result<Option<Vec<String>>> {
    let r = rows(
        c,
        &format!(
            "SELECT DIGEST_TEXT FROM performance_schema.events_statements_summary_by_digest \
             WHERE SCHEMA_NAME = DATABASE() AND DIGEST_TEXT LIKE {} ORDER BY LAST_SEEN",
            q(like),
        ),
    )
    .await;
    Ok(match r {
        Ok(rows) => Some(
            rows.into_iter()
                .filter_map(|x| x.first().cloned().flatten())
                .collect(),
        ),
        Err(_) => None,
    })
}

// ---------------------------------------------------------------------------------------------
// The harness for a case: (a) the definition as the user writes it, (b) the seeded source
// ---------------------------------------------------------------------------------------------

/// (a) form S of expectations.md: the `CREATE VIEW` and its free-standing follow-up statements in
/// one `definition` cell, split at top-level `;` (D1: "follow up statements, as if someone would
/// write them manually"). The storage of the statements (same cell vs a companion table, Q1) is a
/// plan decision, so every case here is storage-independent and passes under either.
fn script(view: &str, select: &str, decls: &[&str]) -> String {
    let mut s = format!("CREATE VIEW {view} AS {select}");
    for d in decls {
        s.push_str("; ");
        s.push_str(d);
    }
    s.push(';');
    s
}

/// One row of the case table: (a) the definition/statement table, (b) the original db, (c) what
/// the resulting view table must look like.
struct Case {
    /// expectations.md case id, in every failure message.
    id: &'static str,
    /// the schema/engine tag.
    tag: &'static str,
    /// (b) the pinned corpus.
    ddl: &'static [&'static str],
    view: &'static str,
    select: &'static str,
    /// (a) the follow-up statements, in order.
    decls: &'static [&'static str],
    /// (c) the declared set the view table must carry.
    declared: &'static [Decl],
    /// (c) when the case must be refused: `_lazyview_state.state` is `error` and `last_error`
    /// mentions every one of these; empty means the view must be `ready`.
    err: &'static [&'static str],
    /// (c) a first-time refused view has no table at all (E10).
    no_table: bool,
    /// the PostgreSQL leg runs too (D2/D5: postgres is a target; ClickHouse is not in this ladder).
    pg: bool,
}

/// (a)+(b): a fresh schema with the corpus, the (a) script in `_lazyview`, the engine started. It
/// deliberately does **not** wait for `ready`: every case asserts (c) itself, so the failure names
/// the expectation instead of a generic readiness timeout.
async fn open(
    ctx: &Ctx,
    tag: &str,
    ddl: &[&str],
    spec: &ViewSpec,
    ov: &[(&str, &str)],
) -> Result<(Db, Engine, Conn)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    exec_all(&mut c, ddl).await?;
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, std::slice::from_ref(spec)).await?;
    let eng = ctx.start_engine(&db, ov)?;
    Ok((db, eng, c))
}

/// A `ViewSpec` whose `definition` is the (a) script: the `CREATE VIEW` plus the follow-up
/// statements (the storage question — same cell vs a companion table, Q1 — is the plan's, and every
/// case here is storage-independent).
fn spec_of(c: &Case, targets: Option<&str>) -> ViewSpec {
    let mut s = ViewSpec::new(c.view, Kind::Incremental, c.select);
    s.definition = script(c.view, c.select, c.decls);
    match targets {
        Some(t) => s.with_targets(t),
        None => s,
    }
}

/// `["a","b"] == the strings of v`.
fn eq_strs(v: &[String], want: &[&str]) -> bool {
    v.len() == want.len() && v.iter().zip(want).all(|(a, b)| a.as_str() == *b)
}

/// (a) "the user appends the follow-up statement to the same row's definition" — the H5/E1/E4/E5/E6
/// trigger (whatever the Q1 storage turns out to be, the user's action is this UPDATE).
async fn append_statement(c: &mut Conn, view: &str, decl: &str) -> Result<()> {
    exec(
        c,
        &format!(
            "UPDATE _lazyview SET definition = CONCAT(definition, '; ', {}) WHERE name = {}",
            q(decl),
            q(view),
        ),
    )
    .await
}

/// The `_lazyview_state` row of `(view, target)`; the wait is bounded and a timeout is reported as
/// the expectation it is blocking.
async fn settle(
    eng: &mut Engine,
    c: &mut Conn,
    case: &str,
    view: &str,
    target: &str,
    want: &str,
    budget: Duration,
) -> Result<StateRow> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        eng.check_alive()?;
        let rows = view_state(c, view).await?;
        let mine: Vec<StateRow> = rows
            .into_iter()
            .filter(|s| s.target.as_deref().unwrap_or("source") == target)
            .collect();
        if let Some(row) = mine.iter().find(|s| s.state == want) {
            return Ok(row.clone());
        }
        // A view that is meant to be refused must not be sitting in `ready`, and vice versa.
        if want == "ready" {
            if let Some(e) = mine.iter().find(|s| s.state == "error") {
                bail!(
                    "{case} (c): the view is in `error`, so (c) can never hold: last_error = {:?}; \
                     (a) is the script that declares it, (b) the seeded corpus",
                    e.last_error,
                );
            }
        }
        if std::time::Instant::now() > deadline {
            bail!(
                "{case} (c): `{view}` on `{target}` never reached state `{want}` within {budget:?}; \
                 state rows: {mine:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// (c) on a MySQL-family target: the state row, then the resulting DDL — either the view is
/// `ready` and `IDX = E ∪ declared`, or the case is a refusal and `last_error` names what it must
/// name while the target's index set stays exactly `E`.
async fn check_myf(eng: &mut Engine, c: &mut Conn, case: &Case, target: &str) -> Result<()> {
    if case.err.is_empty() {
        settle(eng, c, case.id, case.view, target, "ready", t_ready()).await?;
        let got = myf_idx(c, case.view).await?;
        expect_idx(case.id, case.view, &got, case.declared)?;
        let ddl = myf_show_create(c, case.view).await?;
        for d in case.declared {
            let shape = if d.fulltext {
                format!("FULLTEXT KEY `{}`", d.name)
            } else if d.unique {
                format!("UNIQUE KEY `{}`", d.name)
            } else {
                format!("KEY `{}`", d.name)
            };
            ensure!(
                ddl.contains(&shape),
                "{} (c): SHOW CREATE TABLE must render {shape} ({}) — it renders:\n{ddl}",
                case.id,
                d.cols.join(", "),
            );
        }
        return Ok(());
    }
    let row = settle(
        eng,
        c,
        case.id,
        case.view,
        target,
        "error",
        Duration::from_secs(60),
    )
    .await?;
    let msg = row.last_error.clone().unwrap_or_default();
    for want in case.err {
        ensure!(
            msg.to_ascii_lowercase().contains(&want.to_ascii_lowercase()),
            "{} (c): last_error must name `{want}` (the expectation's own error taxonomy); it says: {msg:?}",
            case.id,
        );
    }
    if case.no_table {
        ensure!(
            !table_exists(c, case.view).await?,
            "{} (c): a first-time refused view must have no table at all, `{}` exists",
            case.id,
            case.view,
        );
        return Ok(());
    }
    let got = myf_idx(c, case.view).await?;
    for d in case.declared {
        ensure!(
            !got.iter().any(|(n, ..)| n == d.name),
            "{} (c): the refused declaration must never reach the target — `{}` exists on `{}` \
             while the statement was refused ({got:?})",
            case.id,
            d.name,
            case.view,
        );
    }
    Ok(())
}

/// (c) on a PostgreSQL target, by the side of the MySQL-family leg: the source stays MySQL, the
/// view is materialized with `targets = 'pg'` (the shape of tests/targets.rs:455-462), and the
/// check reads `pg_indexes`. `Ok(None)` (with a printed SKIP note) when no PostgreSQL target is
/// published for this run.
async fn with_pg<F, Fut>(
    ctx: &Arc<Ctx>,
    case: &'static Case,
    targets: Option<&str>,
    ov_extra: &[(&str, &str)],
    f: F,
) -> Result<()>
where
    F: FnOnce(Client, String, Engine, Conn) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let Some(server) = pg::pg_servers().and_then(|v| v.into_iter().next()) else {
        ctx.log(format!(
            "{}: PostgreSQL leg skipped (LAZYVIEW_TEST_PG_DSNS unset; CI's postgres-target job runs it)",
            case.id
        ));
        return Ok(());
    };
    let pgdb = pg::new_pg_db(&server, case.tag).await?;
    let spec = spec_of(case, targets);
    let env = target_env(&[("pg", &pgdb.dsn)]);
    let mut ov: Vec<(&str, &str)> = ov_refs(&env).iter().map(|(a, b)| (*a, *b)).collect();
    ov.extend_from_slice(ov_extra);
    let (_db, eng, sc) = open(ctx, case.tag, case.ddl, &spec, &ov).await?;
    let client = pg::connect(&pgdb.dsn).await?;
    let r = f(client, pgdb.schema.clone(), eng, sc).await;
    // Cleanup whatever the assertions said.
    let _ = pg::drop_dbs(std::slice::from_ref(&pgdb)).await;
    r
}

/// The MySQL-family leg plus (when the case says so and a server is published) the PostgreSQL leg.
async fn case_both(ctx: &Arc<Ctx>, case: &'static Case) -> Result<()> {
    let spec = spec_of(case, None);
    let (_db, mut eng, mut sc) = open(ctx, case.tag, case.ddl, &spec, &[]).await?;
    let r = check_myf(&mut eng, &mut sc, case, "source").await;
    eng.stop().await?;
    r?;
    if case.pg {
        with_pg(
            ctx,
            case,
            Some("pg"),
            &[],
            move |client, schema, mut eng, mut sc| async move {
                if case.err.is_empty() {
                    settle(
                        &mut eng,
                        &mut sc,
                        case.id,
                        case.view,
                        "pg",
                        "ready",
                        t_ready(),
                    )
                    .await?;
                    let got = pg_idx(&client, &schema, case.view).await?;
                    expect_idx(case.id, case.view, &got, case.declared)
                } else {
                    let row = settle(
                        &mut eng,
                        &mut sc,
                        case.id,
                        case.view,
                        "pg",
                        "error",
                        Duration::from_secs(60),
                    )
                    .await?;
                    let msg = row.last_error.clone().unwrap_or_default();
                    for want in case.err {
                        ensure!(
                            msg.to_ascii_lowercase()
                                .contains(&want.to_ascii_lowercase()),
                            "{} (c) PG: last_error must name `{want}`; it says: {msg:?}",
                            case.id,
                        );
                    }
                    Ok(())
                }
            },
        )
        .await
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// PART 1 — HAPPY PATH (H1-H8): expectations.md rows, verbatim (a)/(b)/(c)
// ---------------------------------------------------------------------------------------------

/// A locally built `Case` cannot borrow its fixture arrays: the fields are `&'static` (the shape of
/// a `const CASE_*` row), and a case literal inside a test is a few words of data — leaked once,
/// for the life of the test process.
fn own_decls(v: Vec<Decl>) -> &'static [Decl] {
    Box::leak(v.into_boxed_slice())
}
fn own_strs(v: Vec<&'static str>) -> &'static [&'static str] {
    Box::leak(v.into_boxed_slice())
}

const CASE_H1: Case = Case {
    id: "H1",
    tag: "idx_h1",
    ddl: B,
    view: "item_totals",
    select: SEL_TOTALS,
    decls: &["CREATE INDEX i_total ON item_totals (total)"],
    declared: &[decl("i_total", &["total"])],
    err: &[],
    no_table: false,
    pg: true,
};

/// H1 — one secondary index, declared at create time. `happy · MYF · PG`
/// (a) `CREATE VIEW item_totals AS …; CREATE INDEX i_total ON item_totals (total);`
/// (b) corpus `B`; (c) `IDX(item_totals) = E ∪ { i_total (total) non-unique }`, nothing else, and
/// `KEY \`i_total\` (\`total\`)` in `SHOW CREATE TABLE`; on PG the physical name is
/// `item_totals_i_total`. The table is built once, empty, with the index already present.
#[test]
fn idx_single_at_create() {
    scenario("idx_single_at_create", |ctx| async move {
        case_both(&ctx, &CASE_H1).await
    });
}

const CASE_H2: Case = Case {
    id: "H2",
    tag: "idx_h2",
    ddl: A,
    view: "entity_status",
    select: SEL_STATUS,
    decls: &["CREATE UNIQUE INDEX u_status_grp ON entity_status (status, grp)"],
    declared: &[unique("u_status_grp", &["status", "grp"])],
    err: &[],
    no_table: false,
    pg: true,
};

/// H2 — UNIQUE index, declared at create time. `happy · MYF · PG`
/// (a) `CREATE VIEW entity_status AS SELECT id, grp, status FROM entities;` + `CREATE UNIQUE INDEX
/// u_status_grp ON entity_status (status, grp);` (b) corpus `A`; (c) `IDX = E ∪ { u_status_grp
/// (status, grp) UNIQUE }`, nothing else — the data as seeded satisfies it, so the uniqueness must
/// not be violated by rows lazyview itself writes (checked through a binlog UPDATE below).
#[test]
fn idx_unique_at_create() {
    scenario("idx_unique_at_create", |ctx| async move {
        case_both(&ctx, &CASE_H2).await?;
        // (b) the seeded rows plus the churn the view keeps serving: the UNIQUE must still hold,
        // i.e. lazyview's own writes never break it.
        let spec = spec_of(&CASE_H2, None);
        let (_db, mut eng, mut sc) = open(&ctx, "idx_h2_live", CASE_H2.ddl, &spec, &[]).await?;
        check_myf(&mut eng, &mut sc, &CASE_H2, "source").await?;
        exec(&mut sc, "UPDATE entities SET status = 'sent' WHERE id = 13").await?;
        verify(
            &ctx,
            &mut eng,
            &_db,
            std::slice::from_ref(&spec),
            "H2 (b): after a status change",
        )
        .await?;
        let got = myf_idx(&mut sc, "entity_status").await?;
        expect_idx("H2", "entity_status", &got, CASE_H2.declared)?;
        eng.stop().await
    });
}

/// H3 — FULLTEXT index, on the flavors that have it.
/// `happy · MYF (mysql, percona, mariadb ≥ 10.6 — the suite's floor) · PG: see E9, an error there`
/// (a) `CREATE VIEW entity_names AS SELECT id, name FROM entities;` + `CREATE FULLTEXT INDEX
/// ft_name ON entity_names (name);` (b) corpus `B`; (c) `IDX = E ∪ { ft_name (name) FULLTEXT }`,
/// nothing else, rendered as `FULLTEXT KEY \`ft_name\` (\`name\`)` on InnoDB (today
/// `create_sql_marked` renders no FULLTEXT at all, src/target/mysql.rs:113-128). `MATCH … AGAINST`
/// is out of scope: existence, kind and column set only.
#[test]
fn idx_fulltext_where_supported() {
    scenario("idx_fulltext_where_supported", |ctx| async move {
        let (_v, mut eng, mut sc) = open(&ctx, "idx_h3", B, &spec_of(&CASE_H3, None), &[]).await?;
        // (b)/(c) flavor gate: FULLTEXT exists on the MySQL family (the suite's MariaDB floor is
        // 10.6, the first version with InnoDB FULLTEXT); the PG leg of FULLTEXT is E9's named error.
        let version = rows(
            &mut sc,
            "SELECT CONCAT(@@version, ' / ', @@version_comment)",
        )
        .await?;
        let version = version
            .first()
            .and_then(|r| r.first().cloned().flatten())
            .unwrap_or_default();
        let v = version.to_ascii_lowercase();
        ensure!(
            v.contains("mysql") || v.contains("mariadb") || v.contains("percona"),
            "H3 (flavors): the FULLTEXT case runs on the MySQL family only; this server says {version:?}",
        );
        check_myf(&mut eng, &mut sc, &CASE_H3, "source").await?;
        let ddl = myf_show_create(&mut sc, "entity_names").await?;
        ensure!(
            ddl.contains("FULLTEXT KEY `ft_name` (`name`)"),
            "H3 (c): SHOW CREATE TABLE must render `FULLTEXT KEY `ft_name` (`name`)` on InnoDB; \
             it renders:\n{ddl}"
        );
        ensure!(
            ddl.contains("ENGINE=InnoDB"),
            "H3 (c): the view table is an InnoDB table: {ddl}"
        );
        drop(v);
        eng.stop().await
    });
}

const CASE_H3: Case = Case {
    id: "H3",
    tag: "idx_h3",
    ddl: B,
    view: "entity_names",
    select: SEL_NAMES,
    decls: &["CREATE FULLTEXT INDEX ft_name ON entity_names (name)"],
    declared: &[fulltext("ft_name", &["name"])],
    err: &[],
    no_table: false,
    pg: false,
};

const CASE_H4: Case = Case {
    id: "H4",
    tag: "idx_h4",
    ddl: B,
    view: "item_totals",
    select: SEL_TOTALS,
    decls: &["CREATE INDEX i_tot_ent ON item_totals (total, entity_id)"],
    declared: &[decl("i_tot_ent", &["total", "entity_id"])],
    err: &[],
    no_table: false,
    pg: true,
};

/// H4 — multi-column index, order significant. `happy · MYF · PG`
/// (a) H1's view + `CREATE INDEX i_tot_ent ON item_totals (total, entity_id);` (b) corpus `B`;
/// (c) `IDX = E ∪ { i_tot_ent (total, entity_id) non-unique }` asserted in `SEQ_IN_INDEX` order, so
/// `(entity_id, total)` fails the test; PG: `item_totals_i_tot_ent (total, entity_id)`.
#[test]
fn idx_multi_column_order_matters() {
    scenario("idx_multi_column_order_matters", |ctx| async move {
        case_both(&ctx, &CASE_H4).await
    });
}

const CASE_H6: Case = Case {
    id: "H6",
    tag: "idx_h6",
    ddl: B,
    view: "links",
    select: SEL_LINKS,
    decls: &["CREATE INDEX i_amount ON links (amount)"],
    declared: &[decl("i_amount", &["amount"])],
    err: &[],
    no_table: false,
    pg: true,
};

/// H6 — declared indexes coexist with the engine's own set. `happy · MYF · PG`
/// (a) the join view `links` of corpus `B` + `CREATE INDEX i_amount ON links (amount);` (b) corpus
/// `B`; (c) the engine's own `PRIMARY KEY`/`_lv_unit`, `UNIQUE _lv_row`, `_lv_a<k>`, `_lv_p<k>` are
/// all still there **plus** `i_amount (amount)` — dropping or renaming any `_lv_*` index is a
/// failure (the D9-literal-reading guard, Q2) — and the `_lv_*` invisible columns stay invisible
/// (src/target/mysql.rs:107-109).
#[test]
fn idx_coexist_with_engine_keys() {
    scenario("idx_coexist_with_engine_keys", |ctx| async move {
        let (_db, mut eng, mut sc) = open(&ctx, "idx_h6", B, &spec_of(&CASE_H6, None), &[]).await?;
        check_myf(&mut eng, &mut sc, &CASE_H6, "source").await?;
        let got = myf_idx(&mut sc, "links").await?;
        // (c) the engine set is unchanged: the row key that the upsert path needs above all.
        ensure!(
            got.iter()
                .any(|(n, _, _, _)| n == "_lv_row" || n == "PRIMARY"),
            "H6 (c): the engine's own row key is gone — `E` must be unchanged: {got:?}",
        );
        ensure!(
            got.iter().any(|(n, _, _, _)| n.contains("_lv_")),
            "H6 (c): no `_lv_*` engine index is left on the view table: {got:?}",
        );
        let cols = rows(
            &mut sc,
            "SELECT COLUMN_NAME, EXTRA FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'links' AND COLUMN_NAME LIKE '\\_lv\\_%'",
        )
        .await?;
        for r in &cols {
            let extra = r.get(1).and_then(|x| x.clone()).unwrap_or_default();
            ensure!(
                extra.to_ascii_lowercase().contains("invisible"),
                "H6 (c): the engine column `{}` must stay INVISIBLE (src/target/mysql.rs:107-109), \
                 EXTRA is {extra:?}",
                r.first().and_then(|x| x.clone()).unwrap_or_default(),
            );
        }
        // …and a reader of the view still sees only what the view projects.
        let visible = rows(
            &mut sc,
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'links' \
             AND COLUMN_NAME NOT LIKE '\\_lv\\_%' ORDER BY ORDINAL_POSITION",
        )
        .await?;
        let visible: Vec<String> = visible
            .into_iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        ensure!(
            eq_strs(&visible, &["id", "name", "amount"]),
            "H6 (c): the view's own columns changed: {visible:?}",
        );
        eng.stop().await
    });
}

/// H5 — declared **later**, on a live view, reconciled online (D6, D9). `happy · MYF · PG`
/// (a) the row is created without the index and reaches `ready` with the rows verified; then the
/// user appends `CREATE INDEX i_total ON item_totals (total);` to the same row's definition.
/// (b) corpus `B`, with 3 further `items` rows through the binlog so the view is being fed while
/// the reconcile runs. (c) the **same** table (never a replacement) ends as `IDX = E ∪ { i_total
/// (total) }`, `SELECT` results and row count unchanged across the reconcile,
/// `_lazyview_state.state` stays `ready`, no full-rebuild counter movement (the metric gate of
/// tests/regressions/availability.rs:165-172). The reconcile is an online `ALTER …
/// ALGORITHM=INPLACE, LOCK=NONE` on MySQL/Percona, `ALGORITHM=NOCOPY|INPLACE, LOCK=NONE` on
/// MariaDB, `CREATE INDEX CONCURRENTLY` on PostgreSQL (online-ddl-research.md item 2 — the clause
/// is an assertion, not an optimization).
#[test]
fn idx_declared_later_online() {
    scenario("idx_declared_later_online", |ctx| async move {
        let spec = ViewSpec::new("item_totals", Kind::Incremental, SEL_TOTALS);
        let (db, mut eng, mut sc) = open(&ctx, "idx_h5", B, &spec, &[]).await?;
        settle(
            &mut eng,
            &mut sc,
            "H5",
            "item_totals",
            "source",
            "ready",
            t_ready(),
        )
        .await?;
        // (b) the view is being fed while the reconcile runs.
        exec(
            &mut sc,
            "INSERT INTO items VALUES (6,1,1.00),(7,2,2.00),(8,3,3.00)",
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&spec),
            "H5 (b): before the declaration",
        )
        .await?;
        let before = myf_idx(&mut sc, "item_totals").await?;
        let ct_before = myf_create_time(&mut sc, "item_totals").await?;
        let snap = eng.snapshot();

        // (a) the user appends the follow-up statement to the same row's definition.
        append_statement(
            &mut sc,
            "item_totals",
            "CREATE INDEX i_total ON item_totals (total)",
        )
        .await?;
        let case = Case {
            id: "H5",
            tag: "idx_h5_live",
            ddl: B,
            view: "item_totals",
            select: SEL_TOTALS,
            decls: own_strs(vec!["CREATE INDEX i_total ON item_totals (total)"]),
            declared: own_decls(vec![decl("i_total", &["total"])]),
            err: own_strs(vec![]),
            no_table: false,
            pg: false,
        };
        let case: &'static Case = Box::leak(Box::new(case));
        check_myf(&mut eng, &mut sc, case, "source").await?;

        // (c) the same table, never a replacement.
        let ct_after = myf_create_time(&mut sc, "item_totals").await?;
        ensure!(
            ct_before == ct_after,
            "H5 (c): the reconcile replaced the live table (CREATE_TIME {ct_before:?} → {ct_after:?}): \
             it must be an online ALTER of the same table",
        );
        // (c) row count and results unchanged, state still `ready`.
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&spec),
            "H5 (c): after the reconcile",
        )
        .await?;
        let row = settle(
            &mut eng,
            &mut sc,
            "H5",
            "item_totals",
            "source",
            "ready",
            Duration::from_secs(10),
        )
        .await?;
        ensure!(
            row.last_error.is_none(),
            "H5 (c): the reconciled view reports last_error {:?} while staying `ready`",
            row.last_error,
        );
        // (c) no full rebuild: the online path, not a repopulate.
        let d = eng.snapshot().since(&snap);
        ensure!(
            d.full_rebuilds == 0,
            "H5 (c): the reconcile rebuilt the view (full_rebuilds {}): the metric gate of \
             tests/regressions/availability.rs:165-172 says index DDL must not move it",
            d.full_rebuilds,
        );
        // (c) the per-flavor online clause really reached the server
        // (online-ddl-research.md item 2: emit ALGORITHM/LOCK explicitly — they are assertions).
        let is_mariadb = {
            let v = rows(&mut sc, "SELECT VERSION()").await?;
            v.first()
                .and_then(|r| r.first().cloned().flatten())
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("mariadb")
        };
        match myf_digests(&mut sc, "%ALTER%item_totals%ADD%INDEX%").await? {
            None => ctx.log(
                "H5: performance_schema is not available on this server, so the online-clause \
                 clause (ALGORITHM=/LOCK=NONE really reached the server) could not be observed",
            ),
            Some(alts) => {
                let joined = alts.join("\n").to_ascii_lowercase();
                ensure!(
                    alts.iter().any(|a| {
                        let a = a.to_ascii_lowercase();
                        a.contains("algorithm=nocopy") || a.contains("algorithm=inplace")
                    }) && joined.contains("lock=none"),
                    "H5 (c): no online clause reached the server{}: expected an \
                     `ALTER TABLE … ADD INDEX … ALGORITHM=INPLACE, LOCK=NONE` (MariaDB may answer \
                     NOCOPY); digests seen: {alts:?}",
                    if is_mariadb { " (MariaDB)" } else { "" },
                );
            }
        }
        // The engine set E is untouched by the reconcile.
        for (n, cols, uniq, kind) in &before {
            ensure!(
                myf_idx(&mut sc, "item_totals")
                    .await?
                    .iter()
                    .any(|(n2, c2, u2, k2)| n2 == n && c2 == cols && u2 == uniq && k2 == kind),
                "H5 (c): the reconcile changed the engine key `{n}`",
            );
        }
        eng.stop().await
    });
}

/// H7 — one view, several targets, one declared set (D2 destination-only).
/// `happy · MYF + PG (fan-out) · MYF + MYF (two schemas)`
/// (a) the `links` view of H6 with `targets = 'source,mysql2,pg'` + `CREATE INDEX i_amount ON
/// links (amount);` and `CREATE UNIQUE INDEX u_id_name ON links (id, name);` (b) corpus `B` on the
/// source; (c) every target's own table carries `E ∪ { i_amount (amount), u_id_name (id, name)
/// UNIQUE }`, the PG instance's physical names being `links_i_amount` / `links_u_id_name`
/// (src/target/postgres.rs:754-762); a target that is down does not block the others (the fan-out
/// rule tests/targets.rs already asserts).
#[test]
fn idx_fanout_all_targets_converge() {
    scenario_multi("idx_fanout_all_targets_converge", 2, |ctx| async move {
        let src = ctx.new_db("idx_h7_src").await?;
        let mut c = src.conn().await?;
        exec_all(&mut c, B).await?;
        ensure_lazyview_table(&mut c).await?;
        let mut spec =
            ViewSpec::new("links", Kind::Incremental, SEL_LINKS).with_targets("b,pg,down");
        spec.definition = script(
            "links",
            SEL_LINKS,
            &[
                "CREATE INDEX i_amount ON links (amount)",
                "CREATE UNIQUE INDEX u_id_name ON links (id, name)",
            ],
        );
        add_views(&mut c, std::slice::from_ref(&spec)).await?;
        let tb = ctx.new_db_on(1, "idx_h7_b").await?;
        // The PostgreSQL seat, when one is published; the `down` seat is a closed port either way.
        let (env, pgdb) = match pg::pg_servers().and_then(|v| v.into_iter().next()) {
            Some(server) => {
                let pgdb = pg::new_pg_db(&server, "idx_h7_pg").await?;
                (
                    target_env(&[
                        ("b", &tb.dsn),
                        ("pg", &pgdb.dsn),
                        ("down", "mysql://lv:lv@127.0.0.1:1/nowhere"),
                    ]),
                    Some(pgdb),
                )
            }
            None => {
                ctx.log("H7: PostgreSQL seat skipped (LAZYVIEW_TEST_PG_DSNS unset)");
                (
                    target_env(&[
                        ("b", &tb.dsn),
                        ("down", "mysql://lv:lv@127.0.0.1:1/nowhere"),
                    ]),
                    None,
                )
            }
        };
        let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
        // (c) the MySQL-family seat converges to the declared set, and the unreachable target does
        // not block it (the fan-out rule tests/targets.rs already asserts).
        settle(&mut eng, &mut c, "H7", "links", "b", "ready", t_ready()).await?;
        let mut cc = tb.conn().await?;
        let got = myf_idx(&mut cc, "links").await?;
        expect_idx("H7", "links", &got, H7_DECLARED)?;
        if let Some(e) = eng.exited() {
            bail!("H7 (c): the engine died while one target was unreachable: {e:?}");
        }
        drop(cc);
        // (c) the PostgreSQL seat of the same view: physical names `<view>_<name>`
        // (src/target/postgres.rs:754-762).
        if let Some(pgdb) = pgdb.as_ref() {
            eng.stop().await?;
            let mut c2 = src.conn().await?;
            exec(&mut c2, "DELETE FROM _lazyview WHERE name = 'links'").await?;
            let mut pg_spec = spec.clone();
            pg_spec.targets = Some("pg".to_string());
            add_views(&mut c2, std::slice::from_ref(&pg_spec)).await?;
            let mut eng2 = ctx.start_engine(&src, &ov_refs(&target_env(&[("pg", &pgdb.dsn)])))?;
            settle(&mut eng2, &mut c2, "H7", "links", "pg", "ready", t_ready()).await?;
            let client = pg::connect(&pgdb.dsn).await?;
            let rows = pg_idx(&client, &pgdb.schema, "links").await?;
            expect_idx("H7", "links", &rows, H7_DECLARED)?;
            for want in ["links_i_amount", "links_u_id_name"] {
                ensure!(
                    rows.iter().any(|(n, ..)| n == want),
                    "H7 (c) PG: the physical index `{want}` is missing: {rows:?}",
                );
            }
            drop(client);
            eng2.stop().await?;
            pg::drop_dbs(std::slice::from_ref(pgdb)).await.ok();
        } else {
            eng.stop().await?;
        }
        Ok(())
    });
}

const H7_DECLARED: &[Decl] = &[
    decl("i_amount", &["amount"]),
    unique("u_id_name", &["id", "name"]),
];

/// H8 — a virtual generated column declared the same way, then indexed (D4).
/// `happy · MYF (mysql ≥ 5.7, percona, mariadb ≥ 10.6) · PG: STORED only on ≤ 17`
/// (a) `CREATE VIEW entity_status AS SELECT id, grp, status FROM entities;` + `ALTER TABLE
/// entity_status ADD COLUMN sig VARCHAR(32) AS (CONCAT(status, '-', grp)) VIRTUAL;` + `CREATE INDEX
/// i_sig ON entity_status (sig);` (b) corpus `A`, then `UPDATE entities SET status = 'sent' WHERE
/// id = 13` through the binlog; (c) the view table has `sig` rendered as
/// `… GENERATED ALWAYS AS (concat(…)) VIRTUAL` (nothing of the kind is rendered today) and
/// `IDX = E ∪ { i_sig (sig) }`; `sig` is not a projection column, so `SELECT *` from the view keeps
/// returning only `id, grp, status`.
#[test]
fn idx_on_declared_virtual_column() {
    scenario("idx_on_declared_virtual_column", |ctx| async move {
        let (v, mut eng, mut sc) = open(&ctx, "idx_h8", A, &spec_of(&CASE_H8, None), &[]).await?;
        // (b) a binlog change while the view is live.
        exec(&mut sc, "UPDATE entities SET status = 'sent' WHERE id = 13").await?;
        settle(
            &mut eng,
            &mut sc,
            "H8",
            "entity_status",
            "source",
            "ready",
            t_ready(),
        )
        .await?;
        check_myf(&mut eng, &mut sc, &CASE_H8, "source").await?;
        // (c) the column is engine-appended and rendered as a generated one…
        let ddl = myf_show_create(&mut sc, "entity_status").await?;
        ensure!(
            ddl.contains("GENERATED ALWAYS AS") && ddl.to_ascii_lowercase().contains("virtual"),
            "H8 (c): `sig` must be rendered as `… GENERATED ALWAYS AS (concat(…)) VIRTUAL` \
             (src/target/mysql.rs:100-111 renders nothing of the kind today); SHOW CREATE TABLE:\n{ddl}",
        );
        // (c) …and is NOT a projection column: the view still returns id, grp, status only.
        let cols = rows(
            &mut sc,
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'entity_status' \
             AND EXTRA NOT LIKE '%INVISIBLE%' ORDER BY ORDINAL_POSITION",
        )
        .await?;
        let cols: Vec<String> = cols
            .into_iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        ensure!(
            eq_strs(&cols, &["id", "grp", "status"]),
            "H8 (c): SELECT * of the view must still return only id, grp, status; the visible \
             columns are {cols:?}",
        );
        drop(v);
        eng.stop().await
    });
}

const CASE_H8: Case = Case {
    id: "H8",
    tag: "idx_h8",
    ddl: A,
    view: "entity_status",
    select: SEL_STATUS,
    decls: &[H8_COLUMN, "CREATE INDEX i_sig ON entity_status (sig)"],
    declared: &[decl("i_sig", &["sig"])],
    err: &[],
    no_table: false,
    pg: false,
};

// ---------------------------------------------------------------------------------------------
// PART 2 — WEIRD EDGE CASES (E1-E12)
// ---------------------------------------------------------------------------------------------

/// Replaces the whole `definition` of a view (the user editing the row the way docs/views.md
/// documents: an UPDATE on `_lazyview`).
async fn set_definition(c: &mut Conn, view: &str, def: &str) -> Result<()> {
    exec(
        c,
        &format!(
            "UPDATE _lazyview SET definition = {} WHERE name = {}",
            q(def),
            q(view)
        ),
    )
    .await
}

const CASE_E1_NEW: &str = "CREATE INDEX i_total ON item_totals (n)";

/// E1 — an index of the same name already exists on the view table, with different columns.
/// `edge · MYF · PG`
/// (a) H1's script, but the declaration is `CREATE INDEX i_total ON item_totals (n);`; (b) corpus
/// `B`, and the live view table already carries `KEY i_total (total)` (an earlier declaration — the
/// real shape of the brief's "same name via create_table_like", corrected by facts.md: no
/// `CREATE TABLE … LIKE` exists); (c) converges to `IDX = E ∪ { i_total (n) }`, and no error 1061
/// (MYF, duplicate key name) / 42P07 (PG, relation exists) reaches the user — the engine may drop
/// and re-create freely (D7/D9: no ownership check, no report-only).
#[test]
fn idx_reconciles_same_name_different_columns() {
    scenario(
        "idx_reconciles_same_name_different_columns",
        |ctx| async move {
            let (_db, mut eng, mut sc) =
                open(&ctx, "idx_e1", B, &spec_of(&CASE_H1, None), &[]).await?;
            settle(
                &mut eng,
                &mut sc,
                "E1",
                "item_totals",
                "source",
                "ready",
                t_ready(),
            )
            .await?;
            // (b) the live view table already carries `KEY i_total (total)`.
            expect_idx(
                "E1",
                "item_totals",
                &myf_idx(&mut sc, "item_totals").await?,
                CASE_H1.declared,
            )?;
            // (a) the declaration now says `(n)`.
            set_definition(
                &mut sc,
                "item_totals",
                &script("item_totals", SEL_TOTALS, &[CASE_E1_NEW]),
            )
            .await?;
            let c = Case {
                id: "E1",
                tag: "idx_e1",
                ddl: B,
                view: "item_totals",
                select: SEL_TOTALS,
                decls: own_strs(vec![CASE_E1_NEW]),
                declared: own_decls(vec![decl("i_total", &["n"])]),
                err: own_strs(vec![]),
                no_table: false,
                pg: false,
            };
            let c: &'static Case = Box::leak(Box::new(c));
            check_myf(&mut eng, &mut sc, c, "source").await?;
            // (c) the duplicate-key error never reaches the user.
            let row = settle(
                &mut eng,
                &mut sc,
                "E1",
                "item_totals",
                "source",
                "ready",
                Duration::from_secs(10),
            )
            .await?;
            let err = row.last_error.clone().unwrap_or_default();
            ensure!(
                !err.contains("1061") && !err.to_ascii_lowercase().contains("duplicate key name"),
                "E1 (c): error 1061 reached the user: {err:?}",
            );
            eng.stop().await
        },
    );
}

/// E2 — the same index declared twice. `edge · MYF · PG`
/// (a) H1's script with the follow-up statement written twice; (b) corpus `B`; (c) exactly one
/// index `i_total (total)`, the view is `ready`, `last_error` NULL, no error in the log beyond at
/// most one warning, and repeated reconciles (restart, drift check) are idempotent — the statement
/// is not re-applied once the shape matches.
#[test]
fn idx_declared_twice_is_idempotent() {
    scenario("idx_declared_twice_is_idempotent", |ctx| async move {
        let case = Case {
            id: "E2",
            tag: "idx_e2",
            ddl: B,
            view: "item_totals",
            select: SEL_TOTALS,
            decls: own_strs(vec![
                "CREATE INDEX i_total ON item_totals (total)",
                "CREATE INDEX i_total ON item_totals (total)",
            ]),
            declared: own_decls(vec![decl("i_total", &["total"])]),
            err: own_strs(vec![]),
            no_table: false,
            pg: false,
        };
        let case: &'static Case = Box::leak(Box::new(case));
        let (_db, mut eng, mut sc) = open(&ctx, "idx_e2", B, &spec_of(case, None), &[]).await?;
        check_myf(&mut eng, &mut sc, case, "source").await?;
        // (c) exactly one `i_total`, whatever else the shape says.
        let got = myf_idx(&mut sc, "item_totals").await?;
        let n = got.iter().filter(|(name, ..)| name == "i_total").count();
        ensure!(
            n == 1,
            "E2 (c): {n} indexes are named `i_total`, exactly one is declared: {got:?}"
        );
        // (c) `last_error` NULL, and no error in the log beyond at most one warning.
        let row = settle(
            &mut eng,
            &mut sc,
            "E2",
            "item_totals",
            "source",
            "ready",
            Duration::from_secs(10),
        )
        .await?;
        ensure!(
            row.last_error.is_none(),
            "E2 (c): the duplicated declaration left last_error = {:?}",
            row.last_error,
        );
        let log = std::fs::read_to_string(&eng.log_path)?;
        let noisy: Vec<&str> = log
            .lines()
            .filter(|l| l.contains("i_total") && (l.contains("ERROR") || l.contains("WARN")))
            .collect();
        ensure!(
            noisy.iter().all(|l| !l.contains("ERROR")) && noisy.len() <= 1,
            "E2 (c): the duplicate declaration is more than a warning: {noisy:?}",
        );
        // (c) a repeated reconcile re-applies nothing: the shape already matches.
        let before = myf_create_time(&mut sc, "item_totals").await?;
        exec(
            &mut sc,
            "UPDATE _lazyview SET updated_at = CURRENT_TIMESTAMP WHERE name = 'item_totals'",
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        expect_idx(
            "E2",
            "item_totals",
            &myf_idx(&mut sc, "item_totals").await?,
            case.declared,
        )?;
        ensure!(
            myf_create_time(&mut sc, "item_totals").await? == before,
            "E2 (c): a repeated reconcile replaced the table",
        );
        eng.stop().await
    });
}

const CASE_E3: Case = Case {
    id: "E3",
    tag: "idx_e3",
    ddl: A,
    view: "entity_status",
    select: SEL_STATUS,
    decls: &["CREATE INDEX i_sig ON entity_status (sig)"],
    declared: &[decl("i_sig", &["sig"])],
    err: &["sig"],
    no_table: false,
    pg: true,
};

/// E3 — an index over a virtual generated column that was **not** declared. `edge · MYF · PG`
/// (a) H8's script with the `ADD COLUMN sig …` statement removed, leaving `CREATE INDEX i_sig ON
/// entity_status (sig);`; (b) corpus `A`; (c) no index appears, the view goes to `state = 'error'`
/// with a named error naming the column and the missing declaration, and the previous version keeps
/// serving rows. D4 requires the explicit column statement, so the engine must not invent the
/// column.
#[test]
fn idx_over_undeclared_column_errors() {
    scenario("idx_over_undeclared_column_errors", |ctx| async move {
        case_both(&ctx, &CASE_E3).await
    });
}

/// E4 — a declaration arrives while a shadow rebuild / backfill is in flight. `edge · MYF · PG`
/// (a) H1's script; while the build runs (`LAZYVIEW_TEST_BUILD_DELAY`, cf.
/// tests/regressions/availability.rs:24, builds.rs:17-19) the user appends `CREATE INDEX i_total ON
/// item_totals (total);`; (b) corpus `B` plus 200 `items` inserts so the backfill lasts; (c) after
/// the swap settles, exactly one live table named `item_totals` with `IDX = E ∪ { i_total (total) }`
/// and no `_lv_new`/`_lv_old` leftover carrying a different index set (both side tables gone, names
/// from src/target/mod.rs:304-318); whichever path served it, rows are never lost or duplicated
/// (oracle diff).
#[test]
fn idx_declared_during_inflight_build() {
    scenario("idx_declared_during_inflight_build", |ctx| async move {
        // (b) 200 more `items` rows so the backfill lasts.
        let vals: Vec<String> = (100..300).map(|i| format!("({i},1,{i}.50)")).collect();
        let extra = format!(
            "INSERT INTO items (id, entity_id, amount) VALUES {}",
            vals.join(",")
        );
        let mut all: Vec<&str> = B.to_vec();
        all.push(extra.as_str());
        // (a) the build is slowed by the test hook; the marker goes in the definition text
        // (src/config.rs:132 `LAZYVIEW_TEST_BUILD_DELAY=<marker>:<ms>`).
        let def = format!("/*long_build*/ CREATE VIEW item_totals AS {SEL_TOTALS};");
        let mut spec = ViewSpec::new("item_totals", Kind::Incremental, SEL_TOTALS);
        spec.definition = def;
        let (db, mut eng, mut sc) = open(
            &ctx,
            "idx_e4",
            &all,
            &spec,
            &[("LAZYVIEW_TEST_BUILD_DELAY", "/*long_build*/:20000")],
        )
        .await?;
        // (a) the user appends the declaration while that build is in flight.
        append_statement(
            &mut sc,
            "item_totals",
            "CREATE INDEX i_total ON item_totals (total)",
        )
        .await?;
        settle(
            &mut eng,
            &mut sc,
            "E4",
            "item_totals",
            "source",
            "ready",
            Duration::from_secs(120),
        )
        .await?;
        // (c) exactly one live table with the declared index…
        expect_idx(
            "E4",
            "item_totals",
            &myf_idx(&mut sc, "item_totals").await?,
            CASE_H1.declared,
        )?;
        // …and no shadow/leftover side table at all (src/target/mod.rs:304-318).
        let sides = rows(
            &mut sc,
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() \
             AND (TABLE_NAME LIKE 'item\\_totals\\_\\_lv\\_new' OR TABLE_NAME LIKE 'item\\_totals\\_\\_lv\\_old' \
                  OR TABLE_NAME LIKE '\\_lv\\_%')",
        )
        .await?;
        let sides: Vec<String> = sides
            .into_iter()
            .filter_map(|r| r.first().cloned().flatten())
            .collect();
        ensure!(
            sides.is_empty(),
            "E4 (c): side tables left behind by the interrupted swap: {sides:?}",
        );
        // (c) rows are never lost or duplicated: the oracle matches.
        let mut spec2 = ViewSpec::new("item_totals", Kind::Incremental, SEL_TOTALS);
        spec2.definition = script(
            "item_totals",
            SEL_TOTALS,
            &["CREATE INDEX i_total ON item_totals (total)"],
        );
        verify(
            &ctx,
            &mut eng,
            &db,
            std::slice::from_ref(&spec2),
            "E4 (c): after the swap settles",
        )
        .await?;
        eng.stop().await
    });
}

/// E5 — restart / resume in the middle of a reconcile. `edge · MYF · PG`
/// (a) H5's script: the view is live without the index, the declaration is appended, then the
/// process is killed (tests/common/engineproc.rs is the engine-as-process harness) **after** the
/// ALTER started and before the index exists; (b) corpus `B`; (c) on restart the engine resumes from
/// the binlog position (not a rebuild) **and** re-converges to `IDX = E ∪ { i_total (total) }`, and
/// the rows are the oracle's. Today `try_resume` checks only the column list and `has_row_index`
/// (src/engine.rs:2666-2675) and is blind to every secondary index — this expectation is what forces
/// the resume check to grow.
#[test]
fn idx_resume_after_interrupted_reconcile() {
    scenario("idx_resume_after_interrupted_reconcile", |ctx| async move {
        let spec = ViewSpec::new("item_totals", Kind::Incremental, SEL_TOTALS);
        let (db, mut eng, mut sc) = open(&ctx, "idx_e5", B, &spec, &[]).await?;
        settle(
            &mut eng,
            &mut sc,
            "E5",
            "item_totals",
            "source",
            "ready",
            t_ready(),
        )
        .await?;
        expect_idx(
            "E5",
            "item_totals",
            &myf_idx(&mut sc, "item_totals").await?,
            &[],
        )?;
        // (a) the declaration is appended, then the process is killed with the reconcile in flight.
        append_statement(
            &mut sc,
            "item_totals",
            "CREATE INDEX i_total ON item_totals (total)",
        )
        .await?;
        drop(sc);
        eng.stop().await?;
        let mut p =
            engineproc::EngineProc::start(&ctx, &db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])
                .await?;
        p.wait_ready(Duration::from_secs(120)).await?;
        let pid = p.pid();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        p.kill_now()?;
        engineproc::EngineProc::wait_pid_gone(pid, Duration::from_secs(30))?;
        drop(p);
        // (c) on restart the engine resumes and re-converges.
        let mut p2 =
            engineproc::EngineProc::start(&ctx, &db, &[("LAZYVIEW_POSITION_PERSIST_MS", "200")])
                .await?;
        p2.wait_ready(Duration::from_secs(120)).await?;
        let mut c2 = db.conn().await?;
        settle_proc(&mut c2, "item_totals", "ready", Duration::from_secs(120)).await?;
        // (c) the declared set is there again after the interrupted reconcile…
        expect_idx(
            "E5",
            "item_totals",
            &myf_idx(&mut c2, "item_totals").await?,
            CASE_H1.declared,
        )?;
        // (c) …and the engine resumed from the binlog position instead of rebuilding.
        let scraped = p2.scrape().await?;
        ensure!(
            scraped.counter("full_rebuilds") == 0,
            "E5 (c): the restart rebuilt the view instead of resuming from the binlog position \
             (full_rebuilds {}); log tail:\n{}",
            scraped.counter("full_rebuilds"),
            p2.log_tail(20),
        );
        // (c) the rows are the oracle's.
        let got = rows(
            &mut c2,
            "SELECT entity_id, total, n FROM item_totals ORDER BY entity_id",
        )
        .await?;
        let want = rows(
            &mut c2,
            "SELECT entity_id, SUM(amount), COUNT(*) FROM items GROUP BY entity_id ORDER BY entity_id",
        )
        .await?;
        ensure!(
            got == want,
            "E5 (c): after the restart the view does not match its oracle: {got:?} vs {want:?}",
        );
        p2.stop()?;
        Ok(())
    });
}

/// [`settle`] for the engine-as-process of E5 (no in-process `Engine` to keep alive).
async fn settle_proc(c: &mut Conn, view: &str, want: &str, budget: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        let rows = view_state(c, view).await?;
        if rows.iter().any(|s| s.state == want) {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!("{view} never reached `{want}` within {budget:?}; state rows: {rows:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// E6 — the operator edits the view table by hand (drift). `edge · MYF · PG`
/// (a) H1's script (declares `i_total (total)`); then by hand on the target: `DROP INDEX i_total ON
/// item_totals;` and `CREATE INDEX hand_made ON item_totals (n);`; (b) corpus `B`; (c) the engine
/// re-converges to `IDX = E ∪ { i_total (total) }`: the missing declared index comes back,
/// `hand_made` is gone, nothing is merely reported (D9 killed the report-only category). Triggers to
/// pin in the plan: after each swap, on definition change, and on resume — this row pins the resume
/// trigger; and the existing rule that the binlog seeing index DDL on a view table does not trigger
/// a rebuild must still hold (tests/regressions/availability.rs:132-175).
#[test]
fn idx_drift_reconverges_without_report_only() {
    scenario(
        "idx_drift_reconverges_without_report_only",
        |ctx| async move {
            let (db, mut eng, mut sc) =
                open(&ctx, "idx_e6", B, &spec_of(&CASE_H1, None), &[]).await?;
            check_myf(&mut eng, &mut sc, &CASE_H1, "source").await?;
            let snap = eng.snapshot();
            // (a) the operator's hand edits on the view table.
            exec(&mut sc, "DROP INDEX i_total ON item_totals").await?;
            exec(&mut sc, "CREATE INDEX hand_made ON item_totals (n)").await?;
            // (c) the hand-made index is not treated as a source change (no rebuild).
            settle(
                &mut eng,
                &mut sc,
                "E6",
                "item_totals",
                "source",
                "ready",
                Duration::from_secs(20),
            )
            .await?;
            let d = eng.snapshot().since(&snap);
            ensure!(
                d.full_rebuilds == 0,
                "E6 (c): index DDL on the view table triggered a rebuild ({}): the rule of \
             tests/regressions/availability.rs:132-175 must hold",
                d.full_rebuilds,
            );
            // (c) on the next reconcile (pinned here as the resume trigger) the set converges again.
            eng.stop().await?;
            let mut eng = ctx.start_engine(&db, &[])?;
            settle(
                &mut eng,
                &mut sc,
                "E6",
                "item_totals",
                "source",
                "ready",
                Duration::from_secs(120),
            )
            .await?;
            let got = myf_idx(&mut sc, "item_totals").await?;
            expect_idx("E6", "item_totals", &got, CASE_H1.declared)?;
            ensure!(
            !got.iter().any(|(n, ..)| n == "hand_made"),
            "E6 (c): the hand-made index survived the reconcile — the engine reported instead of \
             converging (D9 killed the report-only category): {got:?}",
        );
            eng.stop().await
        },
    );
}

const DECL_V_ITEMS: &str = "CREATE INDEX i_total ON v_items (total)";
const DECL_V_ENT: &str = "CREATE INDEX i_total ON v_ent (status)";

/// E7 — an index-name collision between two views on the same target database.
/// `edge · MYF (per-table names) · PG (schema-wide names)`
/// (a) `v_items` = `CREATE VIEW v_items AS SELECT entity_id, SUM(amount) AS total FROM items GROUP BY
/// entity_id;` + `CREATE INDEX i_total ON v_items (total);`, and `v_ent` = `CREATE VIEW v_ent AS
/// SELECT id, status FROM entities;` + `CREATE INDEX i_total ON v_ent (status);`, both on the same
/// instance/schema; (b) corpus `B`; (c) MYF: both tables keep `i_total` with their own columns, no
/// error; PG: names are schema-wide (the reason src/target/postgres.rs:721-723 prefixes the table
/// name), so the engine must either physically name them `v_items_i_total` / `v_ent_i_total` (the
/// recommended reading, Q3) or fail with a named error — silently letting the second build die with
/// 42P07 is a failure.
#[test]
fn idx_name_collision_two_views() {
    scenario("idx_name_collision_two_views", |ctx| async move {
        let src = ctx.new_db("idx_e7").await?;
        let mut c = src.conn().await?;
        exec_all(&mut c, B).await?;
        ensure_lazyview_table(&mut c).await?;
        let mut s1 = ViewSpec::new("v_items", Kind::Incremental, SEL_V_ITEMS);
        s1.definition = script("v_items", SEL_V_ITEMS, &[DECL_V_ITEMS]);
        let mut s2 = ViewSpec::new(
            "v_ent",
            Kind::Incremental,
            "SELECT id, status FROM entities",
        );
        s2.definition = script("v_ent", "SELECT id, status FROM entities", &[DECL_V_ENT]);
        add_views(&mut c, &[s1.clone(), s2.clone()]).await?;
        let mut eng = ctx.start_engine(&src, &[])?;
        // (c) MYF: index names are per-table, so both keep `i_total` with their own columns.
        settle(
            &mut eng,
            &mut c,
            "E7",
            "v_items",
            "source",
            "ready",
            t_ready(),
        )
        .await?;
        settle(
            &mut eng,
            &mut c,
            "E7",
            "v_ent",
            "source",
            "ready",
            t_ready(),
        )
        .await?;
        expect_idx(
            "E7",
            "v_items",
            &myf_idx(&mut c, "v_items").await?,
            &[decl("i_total", &["total"])],
        )?;
        expect_idx(
            "E7",
            "v_ent",
            &myf_idx(&mut c, "v_ent").await?,
            &[decl("i_total", &["status"])],
        )?;
        let log = std::fs::read_to_string(&eng.log_path)?;
        ensure!(
            !log.contains("1061") && !log.to_ascii_lowercase().contains("duplicate key name"),
            "E7 (c) MYF: a per-table index name collision surfaced as an error",
        );
        eng.stop().await?;
        // (c) PG: schema-wide names — physical prefixing (recommended, Q3) or a named error; a
        // silent 42P07 death of the second build is the failure this clause forbids.
        if let Some(server) = pg::pg_servers().and_then(|v| v.into_iter().next()) {
            let pgdb = pg::new_pg_db(&server, "idx_e7_pg").await?;
            let mut c2 = src.conn().await?;
            exec(
                &mut c2,
                "DELETE FROM _lazyview WHERE name IN ('v_items','v_ent')",
            )
            .await?;
            let mut p1 = s1.clone();
            let mut p2 = s2.clone();
            p1.targets = Some("pg".to_string());
            p2.targets = Some("pg".to_string());
            add_views(&mut c2, &[p1, p2]).await?;
            let env = target_env(&[("pg", &pgdb.dsn)]);
            let mut eng2 = ctx.start_engine(&src, &ov_refs(&env))?;
            let mut both_ready = true;
            for v in ["v_items", "v_ent"] {
                match settle(&mut eng2, &mut c2, "E7", v, "pg", "ready", t_ready()).await {
                    Ok(_) => {}
                    Err(_) => both_ready = false,
                }
            }
            let cl = pg::connect(&pgdb.dsn).await?;
            if both_ready {
                let a = pg_idx(&cl, &pgdb.schema, "v_items").await?;
                let b = pg_idx(&cl, &pgdb.schema, "v_ent").await?;
                ensure!(
                    a.iter().any(|(n, ..)| n == "v_items_i_total")
                        && b.iter().any(|(n, ..)| n == "v_ent_i_total"),
                    "E7 (c) PG: with both views ready, the physical names must be prefixed \
                     (`v_items_i_total` / `v_ent_i_total`, src/target/postgres.rs:721-723): {a:?} {b:?}",
                );
            } else {
                let errs = view_state(&mut c2, "v_ent").await?;
                let msg: String = errs
                    .iter()
                    .filter_map(|s| s.last_error.clone())
                    .collect::<Vec<_>>()
                    .join(" | ");
                ensure!(
                    !msg.contains("42P07") && !msg.to_ascii_lowercase().contains("already exists"),
                    "E7 (c) PG: the second view died with the raw 42P07 / `already exists` instead of \
                     a named error or a prefixed name: {msg:?}",
                );
            }
            drop(cl);
            eng2.stop().await?;
            pg::drop_dbs(std::slice::from_ref(&pgdb)).await.ok();
        } else {
            ctx.log("E7: PostgreSQL leg skipped (LAZYVIEW_TEST_PG_DSNS unset)");
        }
        Ok(())
    });
}

const SEL_V_ITEMS: &str = "SELECT entity_id, SUM(amount) AS total FROM items GROUP BY entity_id";

/// E8 — unsupported index forms must fail at PARSE with a clear error. `edge · MYF · PG`
/// (a) each of `((UPPER(total)))`, `(total(4))`, `USING HASH`, `WITH (fillfactor=70)`,
/// `CREATE SPATIAL INDEX`, `WHERE total > 0`, appended to H1's view in turn; (b) corpus `B` (no data
/// is written — they must never reach the target); (c) no `CREATE INDEX` is ever sent to the target
/// (the resulting DDL is exactly `E`: no partial index, no `_lv_`-prefixed surprise), and the view
/// goes to `state = 'error'` with an error that names the statement, the offending construct and the
/// supported subset. The rule is deterministic across flavors (MySQL 8.0 would take the functional
/// index, MariaDB would not): refuse all of them at parse on every flavor. Trailing garbage after the
/// statement is refused the way `parse_sql` refuses anything that does not end at EOF
/// (src/analyze.rs:52-58).
#[test]
fn idx_unsupported_forms_fail_at_parse() {
    scenario("idx_unsupported_forms_fail_at_parse", |ctx| async move {
        for decl in [
            "CREATE INDEX i_up ON item_totals ((UPPER(total)))",
            "CREATE INDEX i_pre ON item_totals (total(4))",
            "CREATE INDEX i_h ON item_totals (total) USING HASH",
            "CREATE INDEX i_o ON item_totals (total) WITH (fillfactor=70)",
            "CREATE SPATIAL INDEX i_sp ON item_totals (total)",
            "CREATE INDEX i_e ON item_totals (total) WHERE total > 0",
            "CREATE INDEX i_total ON item_totals (total) GARBAGE",
        ] {
            let name = decl
                .split_whitespace()
                .nth(2)
                .unwrap_or("i_bad")
                .to_string();
            let case = Box::leak(Box::new(Case {
                id: "E8",
                tag: "idx_e8",
                ddl: B,
                view: "item_totals",
                select: SEL_TOTALS,
                // the (a) script appends the `;` itself, so the statement goes in without it.
                decls: Box::leak(Box::new([then_strip_semicolon(decl)])),
                // (c) the target must show nothing of it: no declared index at all.
                declared: own_decls(vec![]),
                err: own_strs(vec!["CREATE INDEX"]),
                no_table: false,
                pg: true,
            }));
            case_both(&ctx, case).await?;
            // (c) "no partial index, no `_lv_`-prefixed surprise": whatever the refused statement
            // was, the target's index set stayed exactly the engine's own.
            let (_db, mut eng, mut sc) =
                open(&ctx, "idx_e8_check", B, &spec_of(case, None), &[]).await?;
            settle(
                &mut eng,
                &mut sc,
                "E8",
                "item_totals",
                "source",
                "error",
                Duration::from_secs(60),
            )
            .await?;
            let got = myf_idx(&mut sc, "item_totals").await?;
            let stray: Vec<&String> = got
                .iter()
                .map(|(n, ..)| n)
                .filter(|n| !engine_key(n))
                .collect();
            ensure!(
                stray.is_empty(),
                "E8 (c): `{decl}` leaked something to the target ({}): {stray:?}",
                name,
            );
            drop(_db);
            eng.stop().await?;
        }
        Ok(())
    });
}

/// `CREATE INDEX …;` without the trailing `;` (the (a) script adds it).
fn then_strip_semicolon(decl: &str) -> &'static str {
    let d = decl.strip_suffix(';').unwrap_or(decl);
    Box::leak(d.to_string().into_boxed_str())
}

fn decl_(name: &'static str) -> Decl {
    Decl {
        name,
        cols: &[],
        unique: false,
        fulltext: false,
    }
}

const CASE_E9_PG: Case = Case {
    id: "E9",
    tag: "idx_e9_pg",
    ddl: B,
    view: "entity_names",
    select: SEL_NAMES,
    decls: &["CREATE FULLTEXT INDEX ft_name ON entity_names (name)"],
    declared: &[fulltext("ft_name", &["name"])],
    err: &["FULLTEXT"],
    no_table: false,
    pg: true,
};

/// E9 — a flavor/engine without the feature: FULLTEXT. `edge · PG (no FULLTEXT index type exists) ·
/// MYF where the storage engine or product lacks it`
/// (a) H3's script declared for a PG target; and on MYF a FULLTEXT index over a column the family
/// will not index (`CREATE FULLTEXT INDEX ft_amt ON item_totals (total);` over `DECIMAL`); (b) corpus
/// `B`; (c) a **named, decided error** — one taxonomy entry per unsupported combination, e.g.
/// `index kind FULLTEXT is not supported on target <flavor>: <what to write instead>` — written to
/// `_lazyview_state.last_error` with `state = 'error'`, the live table and its existing indexes
/// untouched, the other targets unaffected (today `create_index` renders no method clause at all,
/// src/target/postgres.rs:754-762). Whether a capability gate is consulted per target product/version
/// is Q5/OPEN (facts.md: `Kind` has no MariaDB/Percona distinction); the unit half of this row is
/// `index_kind_supported_per_flavor` in src/ddl/tests.rs.
#[test]
fn idx_fulltext_refused_on_pg() {
    scenario("idx_fulltext_refused_on_pg", |ctx| async move {
        // The PostgreSQL leg: FULLTEXT is not an index type PostgreSQL has.
        let case = Box::leak(Box::new(Case {
            pg: true,
            ..CASE_E9_PG
        }));
        with_pg(&ctx, case, Some("pg"), &[], |client, schema, mut eng, mut sc| async move {
            let row = settle(&mut eng, &mut sc, "E9", "entity_names", "pg", "error", Duration::from_secs(60)).await?;
            let msg = row.last_error.clone().unwrap_or_default();
            ensure!(
                msg.to_ascii_lowercase().contains("fulltext"),
                "E9 (c) PG: last_error must name FULLTEXT as unsupported on this target; it says: {msg:?}",
            );
            let got = pg_idx(&client, &schema, "entity_names").await?;
            ensure!(
                got.iter().all(|(n, ..)| !n.contains("ft_name")),
                "E9 (c) PG: the refused FULLTEXT index reached the target: {got:?}",
            );
            Ok(())
        })
        .await?;
        // The MYF leg of the same row: FULLTEXT over a column the family will not index.
        let case2 = Case {
            id: "E9",
            tag: "idx_e9_myf",
            ddl: B,
            view: "item_totals",
            select: SEL_TOTALS,
            decls: own_strs(vec!["CREATE FULLTEXT INDEX ft_amt ON item_totals (total)"]),
            declared: own_decls(vec![fulltext("ft_amt", &["total"])]),
            err: own_strs(vec!["FULLTEXT"]),
            no_table: false,
            pg: false,
        };
        let case2: &'static Case = Box::leak(Box::new(case2));
        let (_db, mut eng, mut sc) =
            open(&ctx, "idx_e9_myf", B, &spec_of(case2, None), &[]).await?;
        check_myf(&mut eng, &mut sc, case2, "source").await?;
        eng.stop().await
    });
}

const CASE_E10: Case = Case {
    id: "E10",
    tag: "idx_e10",
    ddl: B,
    view: "item_totals",
    select: SEL_TOTALS,
    decls: &["CREATE INDEX i_nope ON item_totals (nope)"],
    declared: &[decl("i_nope", &["nope"])],
    err: &["nope"],
    no_table: true,
    pg: true,
};

/// E10 — the declared column is not a column of the view. `edge · MYF · PG`
/// (a) H1's script with `CREATE INDEX i_nope ON item_totals (nope);`; (b) corpus `B`; (c) it never
/// reaches the target: `IDX = E`, the view is `state = 'error'`, the error names `nope` and lists the
/// view's columns; the previously live version (if any) keeps serving, and a first-time view reports
/// the error and has no table, as a planning failure does today (src/engine.rs:3002's path).
#[test]
fn idx_unknown_column_errors() {
    scenario("idx_unknown_column_errors", |ctx| async move {
        case_both(&ctx, &CASE_E10).await
    });
}

/// E11 — a declared index too wide for the engine's key budget. `edge · MYF · PG`
/// (a) H1's view plus a wide text column: `CREATE INDEX i_wide ON item_totals (name, total,
/// entity_id);` where `name` is `VARCHAR(2000)` (so `(3072|2704) * 4` bytes overflow), and separately
/// a 17-column index; (b) corpus `B` widened by the extra column; (c) a named error from the existing
/// budget seam (`MY_KEY_BYTES 3072` / `PG_KEY_BYTES 2704` / `MAX_KEY_PARTS 16`, `fit_key` → `None`,
/// src/target/mod.rs:575-596, 695-752) that quotes the budget and the offending index; `IDX = E` on
/// **every** target the view has, including the one whose budget alone was too tight (`min_key_budget`
/// is the tightest over targets — the error must say which target); no partial index is created
/// anywhere.
#[test]
fn idx_over_budget_errors_with_reason() {
    scenario("idx_over_budget_errors_with_reason", |ctx| async move {
        for (tag, decl, name) in [
            (
                "idx_e11_wide",
                "CREATE INDEX i_wide ON item_totals (name, total, entity_id)",
                "i_wide",
            ),
            (
                "idx_e11_parts",
                "CREATE INDEX i_many ON item_totals (entity_id, total, n, name, name, name, name, \
                 name, name, name, name, name, name, name, name, name, name)",
                "i_many",
            ),
        ] {
            let case = Box::leak(Box::new(Case {
                id: "E11",
                tag,
                ddl: BW,
                view: "item_totals",
                select: "SELECT entity_id, SUM(amount) AS total, COUNT(*) AS n, MAX(name) AS name \
                         FROM items GROUP BY entity_id",
                decls: own_strs(vec![decl]),
                declared: own_decls(vec![decl_(name)]),
                err: own_strs(vec![name]),
                no_table: false,
                pg: true,
            }));
            // (c) on every target the view has, the declared index is refused, not created.
            case_both(&ctx, case).await?;
            let (_db, mut eng, mut sc) = open(&ctx, tag, BW, &spec_of(case, None), &[]).await?;
            // (c) the error quotes the budget and says which target's budget was the tightest.
            let row = settle(
                &mut eng,
                &mut sc,
                "E11",
                "item_totals",
                "source",
                "error",
                Duration::from_secs(60),
            )
            .await?;
            let msg = row.last_error.clone().unwrap_or_default();
            let quoted = ["3072", "2704", "16", "key", "budget"];
            ensure!(
                quoted.iter().any(|w| msg.to_ascii_lowercase().contains(w)),
                "E11 (c): {name}: the error must quote the key budget (MY_KEY_BYTES 3072 / \
                 PG_KEY_BYTES 2704 / MAX_KEY_PARTS 16); it says: {msg:?}",
            );
            ensure!(
                msg.to_ascii_lowercase().contains("target")
                    || msg.to_ascii_lowercase().contains("postgres")
                    || msg.to_ascii_lowercase().contains("mysql"),
                "E11 (c): {name}: the error must say which target's budget was too tight; it says: {msg:?}",
            );
            // (c) no partial index anywhere.
            let got = myf_idx(&mut sc, "item_totals").await?;
            ensure!(
                got.iter().all(|(n, ..)| n != name),
                "E11 (c): the over-budget index `{name}` was created anyway: {got:?}",
            );
            drop(_db);
            eng.stop().await?;
        }
        Ok(())
    });
}

/// E12 — a declared UNIQUE index the data does not satisfy. `edge · MYF · PG`
/// (a) H2's view but seeded so two rows share `(status, grp)`, the declaration appended **after** the
/// view is live; (b) corpus `A` plus `INSERT INTO entities VALUES (14, 1, 'new')` through the binlog
/// (a second `('new',1)`); (c) the server's refusal (MYF 1062 / PG 23505) is surfaced as the view's
/// `last_error` with `state = 'error'`; the live table keeps its current rows and its previous index
/// set, and the view is not rebuilt in a loop (the reconcile retries on a bounded schedule, cf.
/// `LAG_MAX_FAILURES` src/engine.rs:7601 and `supervisor::Policy`); the user fixing the definition
/// (or the data) converges it on the next pass.
#[test]
fn idx_unique_violated_reports_and_survives() {
    scenario(
        "idx_unique_violated_reports_and_survives",
        |ctx| async move {
            let spec = ViewSpec::new("entity_status", Kind::Incremental, SEL_STATUS);
            let (db, mut eng, mut sc) = open(&ctx, "idx_e12", A, &spec, &[]).await?;
            settle(
                &mut eng,
                &mut sc,
                "E12",
                "entity_status",
                "source",
                "ready",
                t_ready(),
            )
            .await?;
            // (b) a second ('new',1) through the binlog, so the UNIQUE the user is about to declare is
            // already violated by the data.
            exec(&mut sc, "INSERT INTO entities VALUES (14, 1, 'new')").await?;
            let before = myf_idx(&mut sc, "entity_status").await?;
            let rows_before = scalar_i64(&mut sc, "SELECT COUNT(*) FROM entity_status").await?;
            let snap = eng.snapshot();
            // (a) the UNIQUE declaration is appended after the view is live.
            append_statement(
                &mut sc,
                "entity_status",
                "CREATE UNIQUE INDEX u_status_grp ON entity_status (status, grp)",
            )
            .await?;
            let row = settle(
                &mut eng,
                &mut sc,
                "E12",
                "entity_status",
                "source",
                "error",
                Duration::from_secs(60),
            )
            .await?;
            let msg = row
                .last_error
                .clone()
                .unwrap_or_default()
                .to_ascii_lowercase();
            ensure!(
            msg.contains("duplicate") || msg.contains("1062") || msg.contains("unique") || msg.contains("23505"),
            "E12 (c): the server's refusal (MYF 1062 / PG 23505) must be surfaced as last_error; \
             it says: {:?}",
            row.last_error,
        );
            // (c) the live table keeps its rows and its previous index set.
            let after = myf_idx(&mut sc, "entity_status").await?;
            ensure!(
            after == before,
            "E12 (c): the refused UNIQUE changed the live table's index set: {before:?} → {after:?}",
        );
            ensure!(
                scalar_i64(&mut sc, "SELECT COUNT(*) FROM entity_status").await? == rows_before,
                "E12 (c): the refused UNIQUE lost rows from the live table",
            );
            // (c) no rebuild loop: the reconcile retries on a bounded schedule.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let d = eng.snapshot().since(&snap);
            ensure!(
                d.full_rebuilds <= 1,
                "E12 (c): the refused UNIQUE put the view into a rebuild loop (full_rebuilds {})",
                d.full_rebuilds,
            );
            // (c) the user fixes the data: the next pass converges.
            exec(&mut sc, "DELETE FROM entities WHERE id = 14").await?;
            let c = Case {
                id: "E12",
                tag: "idx_e12",
                ddl: A,
                view: "entity_status",
                select: SEL_STATUS,
                decls: own_strs(vec![
                    "CREATE UNIQUE INDEX u_status_grp ON entity_status (status, grp)",
                ]),
                declared: own_decls(vec![unique("u_status_grp", &["status", "grp"])]),
                err: own_strs(vec![]),
                no_table: false,
                pg: false,
            };
            let c: &'static Case = Box::leak(Box::new(c));
            settle(
                &mut eng,
                &mut sc,
                "E12",
                "entity_status",
                "source",
                "ready",
                Duration::from_secs(120),
            )
            .await?;
            check_myf(&mut eng, &mut sc, c, "source").await?;
            drop(db);
            eng.stop().await
        },
    );
}

// ---------------------------------------------------------------------------------------------
// The failure-mode seeds of plan/online-ddl-research.md (binding input): FULLTEXT is never online on
// MySQL-family, so these assert "no write-blocking observed" + convergence by whichever path ran —
// never a live `ALTER … ADD FULLTEXT`. (The 1799 seed needs `innodb_online_alter_log_max_size`, a
// server-level variable: it is NOT tested here and is flagged OPEN for the deputy.)
// ---------------------------------------------------------------------------------------------

const CASE_FT_LIVE: Case = Case {
    id: "FT-live",
    tag: "idx_ft_live",
    ddl: B,
    view: "entity_names",
    select: SEL_NAMES,
    decls: &["CREATE FULLTEXT INDEX ft_name ON entity_names (name)"],
    declared: &[fulltext("ft_name", &["name"])],
    err: &[],
    no_table: false,
    pg: false,
};

/// Research seed 1 — a FULLTEXT index declared against a **live, populated** view table.
/// `research · MYF` (mysql, percona, mariadb)
/// FULLTEXT can never be built online on the MySQL family (MySQL: "Permits Concurrent DML: No",
/// `LOCK=NONE` errors 1846 "Fulltext index creation requires a lock", and the first FULLTEXT
/// rebuilds for the hidden FTS_DOC_ID; MariaDB: a one-time INPLACE rebuild, one FULLTEXT at a time,
/// and once a table has a FULLTEXT no ALTER may run with `LOCK=NONE` at all). So the correct asserted
/// outcome is **no write-blocking observed** while the reconcile runs, with the index present when
/// the pass completes, whichever path served it (the shadow/swap path of breakdown.md P6/P8) — a test
/// that demanded a live `ALTER … ADD FULLTEXT … LOCK=NONE` would be asserting an impossibility.
#[test]
fn idx_fulltext_declared_later_never_blocks_writes() {
    scenario(
        "idx_fulltext_declared_later_never_blocks_writes",
        |ctx| async move {
            let spec = ViewSpec::new("entity_names", Kind::Incremental, SEL_NAMES);
            let (db, mut eng, mut sc) = open(&ctx, "idx_ft_live", B, &spec, &[]).await?;
            settle(
                &mut eng,
                &mut sc,
                "FT-live",
                "entity_names",
                "source",
                "ready",
                t_ready(),
            )
            .await?;
            // (b) the table is populated and being fed before the declaration arrives.
            exec(
                &mut sc,
                "INSERT INTO entities VALUES (4,'delta'),(5,'epsilon')",
            )
            .await?;
            settle(
                &mut eng,
                &mut sc,
                "FT-live",
                "entity_names",
                "source",
                "ready",
                Duration::from_secs(30),
            )
            .await?;
            let mut writer = db.conn().await?;
            let seq = 100i64;
            exec(
                &mut writer,
                &format!("INSERT INTO entities VALUES ({seq},'written-during-reconcile')"),
            )
            .await?;
            // (a) the FULLTEXT declaration arrives against the live, populated table.
            append_statement(
                &mut sc,
                "entity_names",
                "CREATE FULLTEXT INDEX ft_name ON entity_names (name)",
            )
            .await?;
            // (c) writes keep flowing while the reconcile runs: another row must be materialized in the
            // view while the index pass is (or was) in flight — a blocking ALTER would stall the flush
            // loop and this would time out.
            exec(
                &mut writer,
                &format!("INSERT INTO entities VALUES ({}, 'flow-1')", seq + 1),
            )
            .await?;
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            loop {
                let n: i64 = scalar_i64(
                    &mut sc,
                    "SELECT COUNT(*) FROM entity_names WHERE name = 'flow-1'",
                )
                .await?;
                if n == 1 {
                    break;
                }
                ensure!(
                    std::time::Instant::now() < deadline,
                    "FT-live (c): writes stopped flowing while the FULLTEXT reconcile ran \
                 (FULLTEXT cannot be built online on MySQL-family, so the engine must serve this \
                 declaration by the shadow/swap path, never by a write-blocking ALTER)",
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // (c) and the index ends up there, whichever path ran.
            settle(
                &mut eng,
                &mut sc,
                "FT-live",
                "entity_names",
                "source",
                "ready",
                Duration::from_secs(120),
            )
            .await?;
            let got = myf_idx(&mut sc, "entity_names").await?;
            expect_idx("FT-live", "entity_names", &got, CASE_FT_LIVE.declared)?;
            // (c) the 1846 failure mode is designed for, not surfaced.
            let row = settle(
                &mut eng,
                &mut sc,
                "FT-live",
                "entity_names",
                "source",
                "ready",
                Duration::from_secs(5),
            )
            .await?;
            let msg = row
                .last_error
                .clone()
                .unwrap_or_default()
                .to_ascii_lowercase();
            ensure!(
            !msg.contains("1846") && !msg.contains("requires a lock"),
            "FT-live (c): the reconcile surfaced MySQL's 1846 (`Fulltext index creation requires a \
             lock`) instead of taking the shadow path: {msg:?}",
        );
            drop(writer);
            eng.stop().await
        },
    );
}

/// Research seed 3 — the `INVALID` aftermath of a failed `CREATE INDEX CONCURRENTLY`.
/// `research · PG`
/// A failed concurrent build leaves an `INVALID` index that still costs write overhead, and
/// `CREATE INDEX CONCURRENTLY IF NOT EXISTS` sees that invalid index and **silently skips** it
/// (online-ddl-research.md item 3): retry-by-rerun is a trap, so the reconciler must sweep
/// (`pg_index.indisvalid`) + `DROP INDEX CONCURRENTLY` + re-create. This case plants the trap — a
/// stale `INVALID` index under the physical name of a later declaration (a failed unique build on
/// duplicate `id` values, the seed's own "unique violation seed") — and asserts the next pass ends
/// with a **valid** index.
#[test]
fn idx_pg_concurrently_invalid_swept() {
    scenario("idx_pg_concurrently_invalid_swept", |ctx| async move {
        let Some(server) = pg::pg_servers().and_then(|v| v.into_iter().next()) else {
            skip_scenario(
                "idx_pg_concurrently_invalid_swept",
                "no PostgreSQL target (LAZYVIEW_TEST_PG_DSNS unset; LAZYVIEW_TEST_REQUIRE_PG=1 fails instead)",
            );
            return Ok(());
        };
        let pgdb = pg::new_pg_db(&server, "idx_pg_inv").await?;
        let mut spec = ViewSpec::new("links", Kind::Incremental, SEL_LINKS).with_targets("pg");
        spec.definition = script("links", SEL_LINKS, &["CREATE INDEX i_dup ON links (id)"]);
        let (_src, mut eng, mut sc) = open(
            &ctx,
            "idx_pg_inv",
            B,
            &spec,
            &ov_refs(&target_env(&[("pg", &pgdb.dsn)])),
        )
        .await?;
        let cl = pg::connect(&pgdb.dsn).await?;
        settle(
            &mut eng,
            &mut sc,
            "PG-invalid",
            "links",
            "pg",
            "ready",
            t_ready(),
        )
        .await?;
        // (b) the aftermath of a failed concurrent build: a UNIQUE `CONCURRENTLY` build over the
        // duplicated `id` of the join, which the server refuses — leaving the INVALID index behind.
        let stale = format!(
            "CREATE UNIQUE INDEX CONCURRENTLY \"links_i_dup\" ON \"{}\".links (id)",
            pgdb.schema
        );
        let seeded = cl.batch_execute(&stale).await;
        let invalid = pg_invalid(&cl, &pgdb.schema, "links").await?;
        ensure!(
            seeded.is_err() && invalid.iter().any(|n| n == "links_i_dup"),
            "PG-invalid (b): seeding the INVALID aftermath failed (build said {seeded:?}, invalid now \
             {invalid:?}): the case cannot test the sweep",
        );
        // (c) the next reconcile pass must sweep the INVALID index and re-create the declared one
        // (an `IF NOT EXISTS` short-circuit leaves the view with an invalid index forever).
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let got = pg_idx(&cl, &pgdb.schema, "links").await?;
            let invalid = pg_invalid(&cl, &pgdb.schema, "links").await?;
            let hit = got.iter().find(|(n, ..)| n == "links_i_dup");
            if let Some((_, cols, uniq, kind)) = hit {
                if invalid.is_empty() && kind == "BTREE" && !uniq && eq_strs(cols, &["id"]) {
                    break;
                }
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "PG-invalid (c): the reconciler never swept the INVALID `links_i_dup` (a \
                 `CREATE INDEX CONCURRENTLY IF NOT EXISTS` would silently skip it): indexes {got:?}, \
                 still invalid {invalid:?}",
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        drop(cl);
        eng.stop().await?;
        pg::drop_dbs(std::slice::from_ref(&pgdb)).await.ok();
        Ok(())
    });
}

/// Research seed 5 — a metadata-lock wait must not stall the writer.
/// `research · MYF` (mysql, percona, mariadb)
/// "online" DDL still takes a brief exclusive MDL at start and end; a long or idle transaction
/// queues the ALTER and the pending X-MDL then blocks **all** subsequent DML on the table
/// (online-ddl-research.md item 5). The reconciler must run with a small session
/// `lock_wait_timeout`, treat MDL waits as retryable and never let the flush loop wait behind
/// itself. This case parks an idle transaction on the view table while the declaration arrives and
/// asserts the ingestion keeps flowing, the engine stays alive, and the set still converges.
#[test]
fn idx_mdl_wait_does_not_stall_writes() {
    scenario("idx_mdl_wait_does_not_stall_writes", |ctx| async move {
        let spec = ViewSpec::new("item_totals", Kind::Incremental, SEL_TOTALS);
        let (db, mut eng, mut sc) = open(&ctx, "idx_mdl", B, &spec, &[]).await?;
        settle(
            &mut eng,
            &mut sc,
            "MDL",
            "item_totals",
            "source",
            "ready",
            t_ready(),
        )
        .await?;
        // (b) an idle transaction holding a metadata lock on the view table.
        let mut blocker = db.conn().await?;
        exec(&mut blocker, "START TRANSACTION").await?;
        exec(&mut blocker, "SELECT COUNT(*) FROM item_totals").await?;
        // (a) the declaration arrives while that transaction is parked.
        append_statement(
            &mut sc,
            "item_totals",
            "CREATE INDEX i_total ON item_totals (total)",
        )
        .await?;
        // (c) the writer keeps flowing: the pending X-MDL must not become a write stall behind
        // which every later DML queues. The probe reads with a small `lock_wait_timeout` so that a
        // blocked queue is an assertion failure here, never a hang of the suite.
        exec(
            &mut sc,
            "INSERT INTO items VALUES (201,1,1.25),(202,2,2.50),(203,3,3.75)",
        )
        .await?;
        let mut probe = db.conn().await?;
        exec(&mut probe, "SET SESSION lock_wait_timeout = 5").await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let seen = scalar_i64(
                &mut probe,
                "SELECT COUNT(*) FROM item_totals WHERE entity_id = 3",
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "MDL (c): reading the view table stalled behind the reconcile's pending \
                     exclusive metadata lock ({e:#}) — the reconcile must retry the MDL wait with \
                     a small lock_wait_timeout, not leave the table (and the flush loop) queued \
                     behind it",
                )
            })?;
            if seen >= 2 {
                break;
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "MDL (c): writes stopped reaching the view while an idle transaction held a \
                 metadata lock on it — the reconcile must retry the MDL wait (small \
                 lock_wait_timeout), not stall the flush loop",
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        ensure!(
            eng.exited().is_none(),
            "MDL (c): the engine died on the metadata-lock wait"
        );
        exec(&mut blocker, "COMMIT").await?;
        drop(blocker);
        // (c) and once the lock is free, the declared set converges.
        settle(
            &mut eng,
            &mut sc,
            "MDL",
            "item_totals",
            "source",
            "ready",
            Duration::from_secs(120),
        )
        .await?;
        expect_idx(
            "MDL",
            "item_totals",
            &myf_idx(&mut sc, "item_totals").await?,
            CASE_H1.declared,
        )?;
        eng.stop().await
    });
}
