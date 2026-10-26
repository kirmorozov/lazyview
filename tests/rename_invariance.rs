//! The monkey-wrench rename test, execution level: one workload against two schemas that differ
//! only by a consistent renaming of every table, column, index, constraint, view and alias name
//! (opaque names plus `id` on a non-key column, `zz9` on a key column, `*_id` / `x_id` on plain
//! columns, `*_link` tables, reserved words and names with spaces, dots, dashes or backticks,
//! columns written in a different letter case at each occurrence). Each view runs on its own
//! pair of engines, plain and renamed in lockstep, every step quiesced: both must be
//! oracle-exact, with the same view state (names mapped back) and the same engine metrics,
//! step by step. The renamer itself is checked offline against every SQL statement the
//! integration suites write.

mod common;

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use common::rename::{rust_literals, Names};
use common::workload::Gen;
use common::*;
use lazyview::Snapshot;
use sqlparser::ast::{Ident, Visit, Visitor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// Neutral schema: entities in groups, a key-value table and a link table, both children
/// of `entity` through cascading foreign keys.
const DDL: &[&str] = &[
    "CREATE TABLE grp (
       id INT NOT NULL PRIMARY KEY,
       label VARCHAR(32) NOT NULL,
       weight INT NOT NULL DEFAULT 0
     )",
    "CREATE TABLE entity (
       id INT NOT NULL PRIMARY KEY,
       grp_id INT NULL,
       label VARCHAR(32) NULL,
       score INT NOT NULL DEFAULT 0,
       KEY k_entity_grp (grp_id)
     )",
    "CREATE TABLE attr_value (
       owner_id INT NOT NULL,
       slot INT NOT NULL,
       val VARCHAR(32) NULL,
       num INT NULL,
       PRIMARY KEY (owner_id, slot),
       KEY k_slot (slot),
       CONSTRAINT fk_attr_owner FOREIGN KEY (owner_id) REFERENCES entity (id)
         ON DELETE CASCADE ON UPDATE CASCADE
     )",
    "CREATE TABLE link (
       a_id INT NOT NULL,
       b_id INT NOT NULL,
       note VARCHAR(16) NULL,
       PRIMARY KEY (a_id, b_id),
       KEY k_link_b (b_id),
       CONSTRAINT fk_link_a FOREIGN KEY (a_id) REFERENCES entity (id)
         ON DELETE CASCADE ON UPDATE CASCADE
     )",
];

/// One view per shape: select / join, grouped, key-value self-joins with constant
/// filters, cascading children, semi-joins and CTEs.
fn views() -> Vec<ViewSpec> {
    vec![
        ViewSpec::new(
            "v_join",
            Kind::Incremental,
            "SELECT e.id, e.label, g.label AS grp_label, e.score FROM entity e
             JOIN grp g ON g.id = e.grp_id WHERE e.score > 10",
        ),
        ViewSpec::new(
            "v_left",
            Kind::Incremental,
            "SELECT e.id, e.score, g.label AS grp_label, g.weight FROM entity e
             LEFT JOIN grp g ON g.id = e.grp_id",
        ),
        ViewSpec::new(
            "v_grouped",
            Kind::Incremental,
            "SELECT e.grp_id, COUNT(*) AS n, SUM(e.score) AS total, MAX(e.label) AS top
             FROM entity e GROUP BY e.grp_id",
        ),
        ViewSpec::new(
            "v_eav",
            Kind::Incremental,
            "SELECT e.id, e.label, a1.val AS first_val, a2.num AS second_num FROM entity e
             JOIN attr_value a1 ON a1.owner_id = e.id AND a1.slot = 1
             LEFT JOIN attr_value a2 ON a2.owner_id = e.id AND a2.slot = 2
             WHERE e.score >= 0",
        ),
        ViewSpec::new(
            "v_eav_grouped",
            Kind::Incremental,
            "SELECT a.slot, COUNT(*) AS n, SUM(a.num) AS total FROM attr_value a
             JOIN entity e ON e.id = a.owner_id WHERE a.slot IN (1, 2, 3) GROUP BY a.slot",
        ),
        ViewSpec::new(
            "v_link",
            Kind::Incremental,
            "SELECT l.a_id, l.b_id, l.note, e.label FROM link l JOIN entity e ON e.id = l.b_id",
        ),
        ViewSpec::new(
            "v_semi",
            Kind::Incremental,
            "SELECT e.id, e.label FROM entity e
             WHERE EXISTS (SELECT 1 FROM link l WHERE l.a_id = e.id AND l.note = 'x')",
        ),
        ViewSpec::new(
            "v_cte",
            Kind::Incremental,
            "WITH big AS (SELECT id, grp_id FROM entity WHERE score > 50)
             SELECT g.id, g.label, COUNT(*) AS n FROM grp g JOIN big b ON b.grp_id = g.id
             GROUP BY g.id, g.label",
        ),
    ]
}

fn seed_rows(g: &mut Gen) -> Vec<String> {
    let mut v = Vec::new();
    for id in 1..=6 {
        v.push(format!(
            "INSERT INTO grp (id, label, weight) VALUES ({id}, 'g{id}', {})",
            g.range(0, 9)
        ));
    }
    for id in 1..=40 {
        v.push(format!(
            "INSERT INTO entity (id, grp_id, label, score) VALUES ({id}, {}, 'e{}', {})",
            g.range(1, 7),
            g.range(1, 30),
            g.range(0, 100)
        ));
        for slot in 1..=3 {
            if g.chance(0.7) {
                v.push(format!(
                    "INSERT INTO attr_value (owner_id, slot, val, num) VALUES ({id}, {slot}, 'v{}', {})",
                    g.range(1, 9),
                    g.range(0, 50)
                ));
            }
        }
    }
    for _ in 0..60 {
        v.push(format!(
            "INSERT IGNORE INTO link (a_id, b_id, note) VALUES ({}, {}, '{}')",
            g.range(1, 40),
            g.range(1, 40),
            if g.chance(0.5) { "x" } else { "y" }
        ));
    }
    v
}

/// One step of the workload: a transaction of one to three changes (inserts, updates, deletes,
/// key moves, cascading deletes and key updates).
fn step(g: &mut Gen, next_id: &mut i64) -> Vec<String> {
    let mut v = Vec::new();
    for _ in 0..g.range(1, 3) {
        let e = g.range(1, *next_id - 1);
        v.push(match g.range(0, 11) {
            0 => {
                *next_id += 1;
                format!(
                    "INSERT INTO entity (id, grp_id, label, score) VALUES ({}, {}, 'e{}', {})",
                    *next_id - 1,
                    g.range(1, 7),
                    g.range(1, 30),
                    g.range(0, 100)
                )
            }
            1 => format!("UPDATE entity SET score = {} WHERE id = {e}", g.range(0, 100)),
            2 => format!("UPDATE entity SET label = 'e{}' WHERE id = {e}", g.range(1, 30)),
            // A key move of the grouped views.
            3 => format!("UPDATE entity SET grp_id = {} WHERE id = {e}", g.range(1, 7)),
            // Cascades to attr_value and link (no binlog rows for the children).
            4 => format!("DELETE FROM entity WHERE id = {e}"),
            // A primary-key move, cascading to the children's foreign keys.
            5 => {
                *next_id += 1;
                format!("UPDATE entity SET id = {} WHERE id = {e}", *next_id - 1)
            }
            6 => format!(
                "INSERT INTO attr_value (owner_id, slot, val, num) SELECT id, {}, 'v{}', {} FROM entity \
                 WHERE id = {e} ON DUPLICATE KEY UPDATE num = num + 1",
                g.range(1, 3),
                g.range(1, 9),
                g.range(0, 50)
            ),
            7 => format!(
                "UPDATE attr_value SET val = 'v{}', num = {} WHERE owner_id = {e} AND slot = {}",
                g.range(1, 9),
                g.range(0, 50),
                g.range(1, 3)
            ),
            8 => format!(
                "DELETE FROM attr_value WHERE owner_id = {e} AND slot = {}",
                g.range(1, 3)
            ),
            9 => format!(
                "INSERT IGNORE INTO link (a_id, b_id, note) SELECT a.id, b.id, '{}' FROM entity a, entity b \
                 WHERE a.id = {e} AND b.id = {}",
                if g.chance(0.5) { "x" } else { "y" },
                g.range(1, *next_id - 1)
            ),
            10 => format!("DELETE FROM link WHERE a_id = {e} OR b_id = {e}"),
            _ => format!(
                "UPDATE grp SET label = 'g{}', weight = weight + 1 WHERE id = {}",
                g.range(1, 9),
                g.range(1, 6)
            ),
        });
    }
    v
}

/// The counters compared between the plain and the renamed run.
fn counters(s: &Snapshot) -> [(&'static str, u64); 9] {
    [
        ("units_refreshed", s.units_refreshed),
        ("lookup_queries", s.lookup_queries),
        ("source_queries", s.source_queries),
        ("mv_queries", s.mv_queries),
        ("mv_rows_inserted", s.mv_rows_inserted),
        ("mv_rows_upserted", s.mv_rows_upserted),
        ("mv_rows_deleted", s.mv_rows_deleted),
        ("full_rebuilds", s.full_rebuilds),
        ("lookback_overflows", s.lookback_overflows),
    ]
}

/// Caught up and idle: the next step starts a new batch.
async fn quiesce(engine: &mut Engine, db: &Db) -> Result<()> {
    let mut c = db.conn().await?;
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;
    engine.wait_caught_up(&mut c, t_catchup()).await?;
    c.disconnect().await.ok();
    Ok(())
}

/// The state row of the view (mode, detail; names mapped back on the renamed side).
async fn state_of(db: &Db, name: &str) -> Result<(String, String)> {
    let mut c = db.conn().await?;
    let r = rows(
        &mut c,
        &format!(
            "SELECT CAST(mode AS CHAR), CAST(detail AS CHAR) FROM _lazyview_state WHERE name = {}",
            q(name)
        ),
    )
    .await?;
    c.disconnect().await.ok();
    let Some(r) = r.into_iter().next() else {
        bail!("no state row for {name}");
    };
    Ok((
        r[0].clone().unwrap_or_default(),
        r[1].clone().unwrap_or_default(),
    ))
}

/// Runs one view on a plain and a renamed schema in lockstep (columns written in a letter case
/// of their own at each occurrence; `alternate`: lower and upper case in turn).
async fn pair(ctx: &Ctx, spec: &ViewSpec, alternate: bool) -> Result<()> {
    let mut n = Names::new(ctx.sub_seed(&format!("names:{}", spec.name)));
    n.vary_case = true;
    n.alternate_case = alternate;
    n.special_tables = !ctx.server().is_mariadb();
    let names = Arc::new(Mutex::new(n));
    let plain = ctx.new_db(&format!("ri_{}", spec.name)).await?;
    let renamed = ctx
        .new_db_renamed(&format!("ri_{}", spec.name), &names)
        .await?;
    let mut g = Gen::new(ctx.sub_seed(&format!("data:{}", spec.name)));
    let seed = seed_rows(&mut g);
    for db in [&plain, &renamed] {
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        exec_all(&mut c, &seed).await?;
        c.disconnect().await.ok();
    }
    let specs = std::slice::from_ref(spec);
    let (mut ea, _) = start_with_views(ctx, &plain, specs, &[]).await?;
    ctx.log(format!(
        "{}: {}",
        spec.name,
        names.lock().unwrap().describe()
    ));
    let (mut eb, _) = start_with_views(ctx, &renamed, specs, &[]).await?;
    let fail = |what: String| -> anyhow::Error {
        anyhow::anyhow!(
            "{}: {what}\n{}",
            spec.name,
            names.lock().unwrap().describe()
        )
    };
    quiesce(&mut ea, &plain).await?;
    quiesce(&mut eb, &renamed).await?;
    let (sa, sb) = (ea.snapshot(), eb.snapshot());
    if counters(&sa) != counters(&sb) {
        return Err(fail(format!(
            "the initial builds differ:\n  plain   {:?}\n  renamed {:?}",
            counters(&sa),
            counters(&sb)
        )));
    }
    let mut next_id = 41;
    let n_steps = scaled(24);
    let mut totals = [Snapshot::default(), Snapshot::default()];
    for k in 0..n_steps {
        let stmts = step(&mut g, &mut next_id);
        let mut deltas = Vec::new();
        for (db, eng) in [(&plain, &mut ea), (&renamed, &mut eb)] {
            let before = eng.snapshot();
            let mut c = db.conn().await?;
            exec(&mut c, "START TRANSACTION").await?;
            exec_all(&mut c, &stmts).await?;
            exec(&mut c, "COMMIT").await?;
            c.disconnect().await.ok();
            quiesce(eng, db).await?;
            deltas.push(eng.snapshot().since(&before));
        }
        if counters(&deltas[0]) != counters(&deltas[1]) {
            return Err(fail(format!(
                "step {k} ({stmts:?}) differs:\n  plain   {:?}\n  renamed {:?}",
                counters(&deltas[0]),
                counters(&deltas[1])
            )));
        }
        for (t, d) in totals.iter_mut().zip(&deltas) {
            *t = add(t, d);
        }
    }
    verify(ctx, &mut ea, &plain, specs, "plain")
        .await
        .context("plain run")?;
    verify(ctx, &mut eb, &renamed, specs, "renamed")
        .await
        .context("renamed run")?;
    let (ma, mb) = (
        state_of(&plain, &spec.name).await?,
        state_of(&renamed, &spec.name).await?,
    );
    let n = names.lock().unwrap().clone();
    let canon = |(m, d): &(String, String)| (m.clone(), n.canonical(d));
    if canon(&ma) != canon(&mb) {
        return Err(fail(format!(
            "_lazyview_state differs: plain {ma:?}, renamed {mb:?}"
        )));
    }
    ctx.log(format!(
        "{}: {} ({}) — {n_steps} steps, same metrics: {}",
        spec.name,
        ma.0,
        ma.1,
        fmt_snapshot(&totals[0])
    ));
    ea.stop().await?;
    eb.stop().await?;
    Ok(())
}

fn add(a: &Snapshot, b: &Snapshot) -> Snapshot {
    // (`since` is the only arithmetic Snapshot has: a + b = a - (0 - b) does not fit u64.)
    let mut s = a.clone();
    s.units_refreshed += b.units_refreshed;
    s.lookup_queries += b.lookup_queries;
    s.source_queries += b.source_queries;
    s.mv_queries += b.mv_queries;
    s.mv_rows_inserted += b.mv_rows_inserted;
    s.mv_rows_upserted += b.mv_rows_upserted;
    s.mv_rows_deleted += b.mv_rows_deleted;
    s.full_rebuilds += b.full_rebuilds;
    s.lookback_overflows += b.lookback_overflows;
    s
}

/// Views whose plan depends on the letter case of column names today (findings RI-1 to RI-4,
/// see `src/analyzer/tests_rename.rs`): windows sharing a PARTITION BY, DISTINCT over its own
/// GROUP BY, a filter on the group key of an aggregated CTE, an aggregated CTE joined on its key.
fn case_findings() -> Vec<ViewSpec> {
    vec![
        ViewSpec::new(
            "v_ri1_windows",
            Kind::Incremental,
            "SELECT id, grp_id, ROW_NUMBER() OVER (PARTITION BY grp_id ORDER BY id) AS rn,
                    COUNT(*) OVER (PARTITION BY grp_id) AS n FROM entity",
        ),
        ViewSpec::new(
            "v_ri2_distinct",
            Kind::Incremental,
            "SELECT DISTINCT grp_id, COUNT(*) AS n FROM entity GROUP BY grp_id",
        ),
        ViewSpec::new(
            "v_ri3_having",
            Kind::Incremental,
            "WITH t AS (SELECT grp_id, COUNT(*) AS n, SUM(score) AS s FROM entity GROUP BY grp_id)
             SELECT grp_id, s FROM t WHERE grp_id > 2 AND n > 1",
        ),
        ViewSpec::new(
            "v_ri4_key_join",
            Kind::Incremental,
            "WITH t AS (SELECT grp_id, SUM(score) AS s FROM entity GROUP BY grp_id)
             SELECT g.label, t.s FROM grp g JOIN t ON t.grp_id = g.id",
        ),
    ]
}

/// Plain and renamed: oracle-exact, the same view state and the same metrics at every step.
#[test]
fn same_execution_plain_and_renamed() {
    run_pairs("rename_invariance", views(), false);
}

/// The same for the views of findings RI-1 to RI-4 (their mode differs when the letter case of
/// a column differs between its occurrences).
#[test]
#[ignore = "RI-1..RI-4: the analyzer compares expressions as text (letter case of column names)"]
fn same_execution_with_case_varied_columns() {
    run_pairs("rename_invariance_case", case_findings(), true);
}

fn run_pairs(test: &str, specs: Vec<ViewSpec>, alternate: bool) {
    scenario(test, move |ctx| {
        let specs = specs.clone();
        async move {
            // Every view runs; the differences of all of them are reported together.
            let mut failed = Vec::new();
            for spec in skip_views(specs) {
                if let Err(e) = pair(&ctx, &spec, alternate).await {
                    ctx.log(format!("{e:#}"));
                    failed.push(format!("{e:#}"));
                }
            }
            if !failed.is_empty() {
                bail!("{} view(s) differ:\n{}", failed.len(), failed.join("\n"));
            }
            Ok(())
        }
    });
}

fn parses(sql: &str) -> bool {
    Parser::parse_sql(&MySqlDialect {}, sql).is_ok()
}

/// Identifiers of a statement.
fn idents(sql: &str) -> Vec<Ident> {
    struct All(Vec<Ident>);
    impl Visitor for All {
        type Break = ();
        fn pre_visit_ident(&mut self, i: &Ident) -> ControlFlow<()> {
            self.0.push(i.clone());
            ControlFlow::Continue(())
        }
    }
    let mut v = All(Vec::new());
    for s in Parser::parse_sql(&MySqlDialect {}, sql).unwrap_or_default() {
        let _ = s.visit(&mut v);
    }
    v.0
}

/// The renamer, offline, on every complete SQL statement written in the integration suites:
/// it renames (statements the parser cannot read name nothing that was renamed), the renamed
/// statement parses, and outside lazyview's metadata no defined name is left unrenamed and
/// mapping the names back gives the statement as written (up to the letter case and quoting of
/// names).
#[test]
fn renamer_round_trips_the_suites_statements() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .chain(std::fs::read_dir(dir.join("common")).unwrap())
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();
    let sqlish = |s: &str| {
        let u = s.trim_start().to_ascii_uppercase();
        [
            "CREATE TABLE",
            "CREATE VIEW",
            "CREATE OR REPLACE VIEW",
            "CREATE INDEX",
            "ALTER TABLE",
            "INSERT",
            "REPLACE",
            "UPDATE",
            "DELETE",
            "SELECT",
            "WITH",
        ]
        .iter()
        .any(|k| u.starts_with(k))
            && !s.contains('{')
            && parses(s)
    };
    let mut checked = 0;
    let mut problems = Vec::new();
    let all: Vec<(String, Vec<String>)> = files
        .iter()
        .map(|f| {
            let src = std::fs::read_to_string(f).unwrap();
            (
                f.file_name().unwrap().to_string_lossy().to_string(),
                rust_literals(&src)
                    .into_iter()
                    .filter(|s| sqlish(s))
                    .collect(),
            )
        })
        .collect();
    for seed in [1, 2, 3, 4] {
        let mut names = Names::new(seed);
        // Definitions first: a statement may use names another file defines.
        let ddl = |s: &&String| s.trim_start().to_ascii_uppercase().starts_with("CREATE");
        for (_, stmts) in &all {
            for s in stmts.iter().filter(ddl) {
                let _ = names.rename_sql(s);
            }
        }
        for (fname, stmts) in &all {
            for s in stmts {
                let r = match names.rename_sql(s) {
                    Ok(r) => r,
                    Err(e) => {
                        problems.push(format!("{fname} (seed {seed}): {e}"));
                        continue;
                    }
                };
                checked += 1;
                if !parses(&r.sql) {
                    problems.push(format!(
                        "{fname} (seed {seed}): renamed statement does not parse:\n  {s}\n  {}",
                        r.sql
                    ));
                    continue;
                }
                if !r.metadata {
                    let new: HashSet<String> = names
                        .pairs()
                        .iter()
                        .map(|(_, n, _)| n.to_ascii_lowercase())
                        .collect();
                    let left: Vec<String> = idents(&r.sql)
                        .into_iter()
                        .map(|i| i.value)
                        .filter(|v| names.is_original(v) && !new.contains(&v.to_ascii_lowercase()))
                        .collect();
                    if !left.is_empty() {
                        problems.push(format!(
                            "{fname} (seed {seed}): {left:?} left unrenamed in\n  {}",
                            r.sql
                        ));
                    }
                }
                // (Statements on lazyview's metadata keep its column names: `id` there is
                // `_lazyview.id`, not the column renamed to `id`.)
                let back = names.canonical(&names.unrename_text(&r.sql));
                if !r.metadata && back != names.canonical(s) {
                    problems.push(format!(
                        "{fname} (seed {seed}): names do not map back:\n  written: {}\n  back:    {}",
                        names.canonical(s),
                        back
                    ));
                }
            }
        }
        // View definitions as the suites write them, MySQL's optional clauses included.
        for t in common::workload::ecommerce_views()
            .into_iter()
            .chain(common::typed::views())
        {
            checked += 1;
            match names.rename_sql(&t.definition) {
                // (The parser does not read MySQL's CHECK OPTION clause.)
                Ok(r) if parses(&r.sql) || r.sql.contains("CHECK OPTION") => {}
                Ok(r) => problems.push(format!(
                    "definition of {}: does not parse: {}",
                    t.name, r.sql
                )),
                Err(e) => problems.push(format!("definition of {}: {e}", t.name)),
            }
        }
    }
    assert!(checked > 300, "only {checked} statements checked");
    assert!(
        problems.is_empty(),
        "{} problems in {checked} renamed statements (first 10):\n{}",
        problems.len(),
        problems
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
