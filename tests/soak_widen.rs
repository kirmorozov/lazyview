//! The soak's `SourceDdl` event, isolated: which views may do a whole-view refresh when
//! `entities.kind` is widened, and what the soak's rebuild rule (F5) is entitled to hold them to.
//!
//! The soak (`tests/soak.rs`, `LAZYVIEW_TEST_SOAK=long`) fails when an `incremental` view's
//! cumulative `whole_view_refreshes` (from `/stats`) rises above what the event that caused the DDL
//! recorded in its ledger (`soakcheck::check_once` for the rule, the `SourceDdl` arm of
//! `soakchaos::run_event` for the ledger). The f1 triage proved the engine right and that rule
//! wrong (`tmp/out/lvs-chaos/f1/report.md`, the CI soak smoke of 2026-11-16:
//! `'scheduled @+1560s': incremental view did a whole-view refresh: wc_refs_by_city@source = 1`).
//! These tests are the regression test for the rule's side, on the finding's own view set and the
//! event's own six statements:
//!
//! * [`idle_widening_refreshes_only_the_kind_readers`] pins the engine's side, which is the
//!   documented one (`docs/views.md:433-436`): a widening the kept rows survive leaves a table in
//!   place, so only the plans that read the widened column rebuild, and the chained views
//!   (`docs/views.md:436`: rebuilt when an upstream table *is* swapped) stay at zero;
//! * [`the_rebuild_rules_set_covers_what_the_engine_treats_as_affected`] pins the rule's side,
//!   which was not: the set a `SourceDdl` event may approve came from the `occ` lines of the access
//!   record alone, while the engine's affected set is the plan's tables **plus the parents of
//!   cascading foreign keys into them** (`src/engine.rs:3800-3807`). A view like `wc_refs_by_city`
//!   — its plan reads `entity_refs` only, whose FK cascades from `entities` — is therefore affected
//!   by the event's DDL and could never be approved by it: one legitimate whole refresh of it was
//!   an unavoidable FAIL. The rule now reads the access record's `cascade` lines too
//!   ([`common::soakchaos::plan_tables`]), and this test is what fails first if that is undone: the
//!   approvable set comes from the rule's own code, the affected set from the server's FK metadata.
//! * [`the_rule_exempts_only_reasons_the_engine_actually_reports`] pins the third fix, the reason
//!   exemption of `check_once`: the reasons it tolerates beyond the ledger are the engine's own
//!   strings, and the two that explain a rebuild an event *does* own stay failures, so the rule is
//!   not loosened beyond what `docs/views.md:433-436` documents.
//! * [`the_wave_credits_only_the_event_that_owns_the_view`] pins when those exemptions apply in
//!   time: the smoke re-run of 2026-11-16 failed on `w_chain_a@source = 1` (`last reason: new
//!   version: …`) 40 s after the burst, while the event's ledger had already closed its bars and a
//!   *scheduled* check was running beside the landing wave. A beyond-bar refresh is credited to the
//!   `SourceDdl` event that owns the view (`docs/views.md:433`, the version bump of a source-schema
//!   change) while its wave is open — and to nobody outside that window, not to a view the DDL does
//!   not touch, not after the tail, not for any other reason.

mod common;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{bail, ensure, Result};
use common::soakchaos::{occ_tables, plan_tables, stale_closure};
use common::soakcheck::{view_rows, Ledger, ViewRow, UNOWNED_REASONS, WAVE_REASONS};
use common::workload::names;
use common::*;

/// The tables of the soak's schema ([`common::soakwork::SOAK_DDL`]) that this event's views read,
/// verbatim: `entity_refs` cascades from `entities` (the cascade case) and `entities.kind` is
/// `VARCHAR(16) NULL`, the column the event widens.
const DDL: &[&str] = &[
    "CREATE TABLE entities (
       id INT NOT NULL PRIMARY KEY,
       grp INT NOT NULL,
       kind VARCHAR(16) NULL,
       amount DECIMAL(10,2) NULL,
       state VARCHAR(12) NULL,
       weight DOUBLE NULL,
       created_at DATETIME(6) NOT NULL,
       note VARCHAR(255) NULL,
       meta JSON NULL,
       spare_at TIMESTAMP(3) NULL DEFAULT NULL,
       KEY k_grp (grp)
     )",
    "CREATE TABLE items (
       id BIGINT NOT NULL PRIMARY KEY,
       entity_id INT NULL,
       grp INT NOT NULL,
       qty INT NOT NULL,
       price DECIMAL(10,2) NOT NULL,
       label VARCHAR(24) NULL,
       KEY k_entity (entity_id)
     )",
    "CREATE TABLE tags (
       item_id INT NOT NULL,
       tag VARCHAR(24) NOT NULL,
       n INT NOT NULL,
       PRIMARY KEY (item_id, tag)
     )",
    "CREATE TABLE `groups` (
       id INT NOT NULL PRIMARY KEY,
       region VARCHAR(8) NOT NULL,
       rate DECIMAL(5,3) NOT NULL
     )",
    "CREATE TABLE entity_refs (
       entity_id INT NOT NULL,
       seq INT NOT NULL,
       city VARCHAR(30) NOT NULL,
       PRIMARY KEY (entity_id, seq),
       CONSTRAINT fk_entity_refs_entity FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE
     )",
];

/// Deterministic seed: 20 entities, three `items` each (so `w_groups_by_region`'s
/// `HAVING COUNT(*) > 1` keeps them and their revenue clears `w_chain_a`'s 500), a tag on the two
/// items per entity with `qty > 3` (so `w_big_entities` is non-empty), four `groups`, and two
/// `entity_refs` per entity (so `wc_refs_by_city` is non-empty and cascades from `entities`).
fn seed() -> Vec<String> {
    let mut entities = Vec::new();
    let mut items = Vec::new();
    let mut tags = Vec::new();
    let mut refs = Vec::new();
    for e in 1..=20i64 {
        entities.push(format!(
            "({e}, {}, {}, {}.00, {}, {}.5, '2026-01-01 00:00:00.000000')",
            e % 4,
            ["'x'", "'y'", "'z'"][(e % 3) as usize],
            e * 7,
            ["'new'", "'open'", "'closed'"][(e % 3) as usize],
            e * 3
        ));
        for k in 1..=3i64 {
            let id = 1000 + e * 10 + k;
            items.push(format!(
                "({id}, {e}, {}, {}, {}.00, 'l{id}')",
                e % 4,
                k * 2,
                k * 100
            ));
            if k > 1 {
                tags.push(format!("({id}, 'hot', {k})"));
            }
        }
        refs.push(format!("({e}, 1, 'c1')"));
        refs.push(format!("({e}, 2, 'c2')"));
    }
    vec![
        format!(
            "INSERT INTO entities (id, grp, kind, amount, state, weight, created_at) VALUES {}",
            entities.join(", ")
        ),
        format!(
            "INSERT INTO items (id, entity_id, grp, qty, price, label) VALUES {}",
            items.join(", ")
        ),
        format!(
            "INSERT INTO tags (item_id, tag, n) VALUES {}",
            tags.join(", ")
        ),
        "INSERT INTO `groups` (id, region, rate) VALUES (0,'north',1.500),(1,'south',2.250),\
         (2,'east',0.750),(3,'west',3.125)"
            .to_string(),
        format!(
            "INSERT INTO entity_refs (entity_id, seq, city) VALUES {}",
            refs.join(", ")
        ),
    ]
}

/// The definitions of the finding, verbatim from the failing run's `_lazyview` (the same SELECTs
/// the soak builds, `tests/common/soakwork.rs`, here the five they named plus the two upstreams the
/// chains need). Of these only `w_big_entities` reads `entities.kind`: `w_groups_by_region` reads
/// `entities.grp`, `w_chain_b` reads `entities.state`, `wc_refs_by_city` reads `entity_refs`
/// (cascading from `entities`) and `w_chain_a` only the table of `w_groups_by_region`.
fn specs() -> Vec<ViewSpec> {
    vec![
        ViewSpec::new(
            "w_big_entities",
            Kind::Incremental,
            "SELECT e.id, e.kind FROM entities e \
             WHERE e.id IN (SELECT i.entity_id FROM items i JOIN tags t ON t.item_id = i.id \
                            WHERE i.qty > 3)",
        ),
        ViewSpec::new(
            "w_groups_by_region",
            Kind::Incremental,
            "SELECT i.entity_id, e.grp, COUNT(*) AS n, SUM(i.qty * i.price) AS revenue, \
             MIN(g.rate) AS min_rate, MAX(g.rate) AS max_rate, AVG(i.qty) AS avg_qty \
             FROM items i LEFT JOIN entities e ON e.id = i.entity_id \
             LEFT JOIN `groups` g ON g.id = e.grp GROUP BY i.entity_id, e.grp HAVING COUNT(*) > 1",
        ),
        ViewSpec::new(
            "w_chain_a",
            Kind::Chained,
            "SELECT entity_id, revenue, n FROM w_groups_by_region WHERE revenue > 500",
        ),
        ViewSpec::new(
            "w_chain_b",
            Kind::Chained,
            "SELECT e.state, COUNT(*) AS n_entities, SUM(c.revenue) AS revenue \
             FROM w_groups_by_region c JOIN entities e ON e.id = c.entity_id GROUP BY e.state",
        ),
        ViewSpec::new(
            "wc_refs_by_city",
            Kind::Incremental,
            "SELECT city, COUNT(*) AS n FROM entity_refs GROUP BY city",
        ),
    ]
}

/// The six statements of the `SourceDdl` event, in order (the `SourceDdl` arm of
/// `soakchaos::run_event`). The event's own widening is 16 → 32; a fresh schema starts at 16 so
/// this is that statement.
const BURST: &[&str] = &[
    "ALTER TABLE items ADD COLUMN spare INT NULL",
    "ALTER TABLE items DROP COLUMN spare",
    "ANALYZE TABLE items",
    "ALTER TABLE entities MODIFY COLUMN kind VARCHAR(32) NULL",
    "DROP INDEX k_kind ON entities",
    "CREATE INDEX k_kind ON entities(kind)",
];

/// The tables of the event's DDLs, as the event passes them to `plan_stale_of`.
const SEEDS: &[&str] = &["items", "entities"];

/// The schema, the views, and an engine whose cadence is far enough away that nothing but the
/// event's DDL can cause a whole refresh. Chained definitions go in through
/// [`start_with_views`], which adds them once their upstream is ready.
async fn setup(ctx: &Ctx, tag: &str, cadence_ms: &str) -> Result<(Db, Engine, Vec<ViewSpec>)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    exec_all(&mut c, DDL).await?;
    exec_all(&mut c, &seed()).await?;
    c.disconnect().await.ok();
    let specs = specs();
    let (mut eng, _) = start_with_views(
        ctx,
        &db,
        &specs,
        &[("LAZYVIEW_FULL_REFRESH_MS", cadence_ms)],
    )
    .await?;
    let mut c = db.conn().await?;
    wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
    c.disconnect().await.ok();
    verify(ctx, &mut eng, &db, &specs, "initial").await?;
    Ok((db, eng, specs))
}

/// The per-view `whole_view_refreshes` of `/stats`, keyed `view@target` the way F5 keys it.
async fn counters(eng: &Engine, db: &Db, specs: &[ViewSpec]) -> Result<HashMap<String, u64>> {
    Ok(view_rows(eng, db, specs)
        .await?
        .into_iter()
        .map(|v| (format!("{}@{}", v.name, v.target), v.whole_view_refreshes))
        .collect())
}

/// How many 1 s reads [`settle`] allows before it calls the bars unsettled, and how many
/// consecutive equal reads end it. Two reads can agree across the 1–2 s gap between two serial
/// rebuild completions, so the requirement is [`SETTLE_STABLE`] — the same lesson the soak's
/// ledger learned (`LEDGER_STABLE_READS`, `tests/common/soakchaos.rs`).
const SETTLE_READS: usize = 60;
const SETTLE_STABLE: usize = 4;

/// Re-read the `/stats` rows until [`SETTLE_STABLE`] consecutive reads agree — the way the
/// event's ledger closes now (`LEDGER_READS`, `tests/common/soakchaos.rs`): a rebuild wave can
/// land after `wait_ready` has returned, and only a bar that stopped moving is a bar the
/// assertions below can hold.
async fn settle(eng: &Engine, db: &Db, specs: &[ViewSpec]) -> Result<Vec<ViewRow>> {
    let mut prev: Option<HashMap<String, u64>> = None;
    let mut same = 0usize;
    for _ in 0..SETTLE_READS {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let rows = view_rows(eng, db, specs).await?;
        let now: HashMap<String, u64> = rows
            .iter()
            .map(|v| (format!("{}@{}", v.name, v.target), v.whole_view_refreshes))
            .collect();
        same = if prev.as_ref() == Some(&now) {
            same + 1
        } else {
            0
        };
        prev = Some(now);
        if same >= SETTLE_STABLE {
            return Ok(rows);
        }
    }
    bail!("the whole_view_refreshes bars were still moving after {SETTLE_READS} reads");
}

/// One `(name, target, access)` row per view, the way the soak's chaos side reads them
/// (`state_rows`, `tests/common/soakchaos.rs`).
async fn state_rows(db: &Db) -> Result<Vec<(String, String, String)>> {
    let mut c = db.conn().await?;
    let rows = rows(
        &mut c,
        "SELECT CAST(name AS CHAR), CAST(target AS CHAR), COALESCE(CAST(access AS CHAR), '') \
         FROM _lazyview_state ORDER BY name, target",
    )
    .await?;
    c.disconnect().await.ok();
    Ok(rows
        .into_iter()
        .filter_map(|r| match (r[0].clone(), r[1].clone(), r[2].clone()) {
            (Some(name), Some(target), Some(access)) => Some((name, target, access)),
            _ => None,
        })
        .collect())
}

/// `(view@target, view name, tables)` per spec, with `tables` from `of`: the shape both sides of
/// the comparison below use, so they differ only in where the table set comes from.
async fn refs_of<F>(
    db: &Db,
    specs: &[ViewSpec],
    of: F,
) -> Result<Vec<(String, String, HashSet<String>)>>
where
    F: Fn(&str) -> HashSet<String>,
{
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    Ok(state_rows(db)
        .await?
        .into_iter()
        .filter(|(name, _, _)| names.contains(&name.as_str()))
        .map(|(name, target, access)| (format!("{name}@{target}"), name, of(&access)))
        .collect())
}

/// The `view@target` keys a `SourceDdl` event of `seeds` may approve: the rule's own code —
/// [`plan_tables`] (the `occ` lines **and** the `cascade` lines) closed over chains by
/// [`stale_closure`], exactly what `plan_stale_of` builds and the event's ledger approves into.
async fn rule_set(db: &Db, specs: &[ViewSpec], seeds: &[&str]) -> Result<HashSet<String>> {
    Ok(stale_closure(
        &refs_of(db, specs, plan_tables).await?,
        seeds,
    ))
}

/// The `view@target` keys the engine itself treats as affected by a DDL of `seeds`
/// (`src/engine.rs:3800-3807`): the same closure over the plan's `occ` tables plus the parents of
/// the cascading FKs into them — but those parents come from the **server's FK metadata**, not from
/// the access record, so the two sides of the comparison are independent: a rule that stops reading
/// the `cascade` lines loses `wc_refs_by_city` while this side keeps it.
async fn engine_affected(db: &Db, specs: &[ViewSpec], seeds: &[&str]) -> Result<HashSet<String>> {
    let mut c = db.conn().await?;
    // The join keeps the query portable: MySQL and MariaDB both keep the cascade rules on
    // `REFERENTIAL_CONSTRAINTS` and the child→parent pair on `KEY_COLUMN_USAGE`.
    let fks = rows(
        &mut c,
        "SELECT DISTINCT kcu.TABLE_NAME, kcu.REFERENCED_TABLE_NAME \
         FROM information_schema.KEY_COLUMN_USAGE kcu \
         JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
           ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
         WHERE kcu.TABLE_SCHEMA = DATABASE() AND kcu.REFERENCED_TABLE_NAME IS NOT NULL \
           AND (rc.DELETE_RULE = 'CASCADE' OR rc.UPDATE_RULE = 'CASCADE')",
    )
    .await?;
    c.disconnect().await.ok();
    let cascades: Vec<(String, String)> = fks
        .into_iter()
        .filter_map(|r| match (r[0].clone(), r[1].clone()) {
            (Some(child), Some(parent)) => Some((child, parent)),
            _ => None,
        })
        .collect();
    ensure!(
        cascades.iter().any(|(child, parent)| child == "entity_refs"
            && parent == "entities"),
        "`entity_refs` no longer cascades from `entities`: the schema this finding is about changed"
    );
    let refs = refs_of(db, specs, |access| {
        let mut tables = occ_tables(access);
        for (child, parent) in &cascades {
            if tables.contains(child) {
                tables.insert(parent.clone());
            }
        }
        tables
    })
    .await?;
    Ok(stale_closure(&refs, seeds))
}

/// The documented side of the finding (`docs/views.md:433-436`): with the engine idle, the
/// widening that the kept rows survive rebuilds only the plans that read the widened column and
/// leaves every other view's table in place. The event approved the plans of this burst — table
/// granularity, `on_tables_changed` — but only the `kind` readers may actually rebuild, so the three
/// views the finding named (`w_chain_a`, `w_chain_b`, `wc_refs_by_city`) and their upstream
/// `w_groups_by_region` must sit at zero. A later statement of the burst can still catch a `kind`
/// reader mid-rebuild (a queued rebuild always builds), hence the `>= 1` there.
#[test]
fn idle_widening_refreshes_only_the_kind_readers() {
    scenario("soak_widen_idle", |ctx| async move {
        let (db, mut eng, specs) = setup(&ctx, "soak_widen_idle", "86400000").await?;
        let mut c = db.conn().await?;
        let base = counters(&eng, &db, &specs).await?;
        // One statement at a time, each fully settled before the next. The burst may not be
        // rushed into one wave: `docs/views.md:433-436` waives the no-rebuild promise for "a view
        // whose plan may already include later changes of the table (after a restart or while
        // catching up across several DDLs)", and six DDLs arriving together are exactly that
        // catch-up — then every view whose plan names `items` or `entities` may rebuild (what the
        // f1 run saw, and the engine is right to). Spacing them pins the promise the soak relies
        // on: the plans that read the widened column rebuild, everything else keeps its table.
        // (The event fires them as one burst; the soak's ledger — `LEDGER_READS`, and the widened
        // approvable set of `plan_stale_of` — is what covers that wave, asserted by
        // [`the_rebuild_rules_set_covers_what_the_engine_treats_as_affected`], not by this zero.)
        for s in BURST {
            // `DROP INDEX k_kind` is the one statement that may fail on a fresh schema (`entities`
            // starts without it) and the event tolerates that: `exec(...).await.ok()`.
            exec(&mut c, s).await.ok();
            wait_ready_tolerant(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            settle(&eng, &db, &specs).await?;
        }
        // A rebuild queued by the last statement can still land after its settle: re-read the
        // bars until they stop moving once more before holding them to the zeros below.
        let rows = settle(&eng, &db, &specs).await?;
        drop(c);
        let mut rebuilt = Vec::new();
        for v in &rows {
            let key = format!("{}@{}", v.name, v.target);
            let delta = v.whole_view_refreshes - base.get(&key).copied().unwrap_or(0);
            if delta > 0 {
                rebuilt.push(format!("{key} +{delta}"));
            }
            if v.name == "w_big_entities" {
                ensure!(
                    delta >= 1,
                    "{key}: its plan reads `entities`.`kind`, the widening must rebuild it"
                );
            } else {
                ensure!(
                    delta == 0,
                    "{key} did {delta} whole-view refresh(es) although its plan does not read \
                     `entities`.`kind` and no upstream table was swapped (docs/views.md:433-436); \
                     rebuilt: [{}]",
                    rebuilt.join(", ")
                );
            }
        }
        // And the widening is not merely tolerated: every view still equals its query.
        verify(&ctx, &mut eng, &db, &specs, "widened").await?;
        eng.stop().await
    })
}

/// The rule's side, the regression test of the f1 finding: everything the engine may legitimately
/// rebuild for this event must be inside what a `SourceDdl` event can approve. It was not — the
/// set came from the `occ` lines alone, so the parent of a cascading FK into a table the view reads
/// never entered it while `on_tables_changed` counts it (`src/engine.rs:3800-3807`, "parents of
/// cascading FKs into them"). `wc_refs_by_city` is exactly that view: its access record is
/// `occ entity_refs <schema>.entity_refs read (2) …` plus
/// `cascade <schema>.entities -> <schema>.entity_refs (0) on delete CASCADE …`, so when the engine
/// rebuilt it (the finding's run did, 6 s after the burst), no ledger entry could cover it and the
/// next check failed with `incremental view did a whole-view refresh: wc_refs_by_city@source = 1`.
#[test]
fn the_rebuild_rules_set_covers_what_the_engine_treats_as_affected() {
    scenario("soak_widen_rule_set", |ctx| async move {
        let (db, eng, specs) = setup(&ctx, "soak_widen_rule_set", "86400000").await?;
        let approvable = rule_set(&db, &specs, SEEDS).await?;
        let affected = engine_affected(&db, &specs, SEEDS).await?;
        let missing: Vec<&String> = affected.difference(&approvable).collect();
        ensure!(
            missing.is_empty(),
            "the engine treats {} view(s) as affected by a DDL of {SEEDS:?} that no SourceDdl event \
             can approve: {} — their plan's `occ` lines name no seed table, only a `cascade` line \
             names the seed as a cascading FK's parent (src/engine.rs:3800-3807 vs \
             tests/common/soakchaos.rs `plan_tables`): one legitimate whole refresh of them is an \
             unavoidable soak FAIL",
            missing.len(),
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        // Named, so a rule that quietly drops the cascade parents again fails here even before the
        // set comparison, and a schema that lost the cascading FK cannot make the test vacuous.
        ensure!(
            approvable.contains("wc_refs_by_city@source"),
            "`wc_refs_by_city` (plan: `entity_refs`, cascading from `entities`) is not in the set a \
             `SourceDdl` event of {SEEDS:?} may approve: {approvable:?} — the finding this file is \
             the regression test for"
        );
        eng.stop().await
    })
}

/// The third fix, without a server: `check_once` exempts a refresh beyond the ledger only when the
/// engine's own `last_whole_view_refresh.reason` explains it (`UNOWNED_REASONS`), and only for the
/// reasons no event of the run can own. The two the rule must keep failing are asserted here too,
/// so the exemption cannot be widened into "any rebuild at all" without this test saying so
/// (`docs/views.md:433-436` is the contract; a refresh no event owns and no documented reason
/// explains still FAILS).
#[test]
fn the_rule_exempts_only_reasons_the_engine_actually_reports() {
    let engine = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/engine.rs"))
        .expect("`src/engine.rs` is readable: the rule's exemption list is checked against it");
    for reason in UNOWNED_REASONS {
        assert!(
            engine.contains(&format!("\"{reason}\"")),
            "`{reason}` is exempted by the soak's F5 rule but is not a reason the engine reports \
             (`src/engine.rs`, the `built(…, reason)` of the whole-view refresh): the engine renamed \
             it and the rule would silently exempt nothing"
        );
    }
    for owned in [
        // A definition, target or source-schema change: the `SourceDdl` event owns these, and the
        // rule accepts them only inside that event's own wave ([`WAVE_REASONS`], and the ledger
        // test below), never on their own.
        "new version: definition, targets or source schema changed, or rebuild requested",
        // Impossible for a view whose `mode` is `incremental`.
        "full-mode view: the definition is not maintained incrementally",
    ] {
        assert!(
            engine.contains(&format!("\"{owned}\"")),
            "`{owned}` is no longer a reason the engine reports: the exemption list and this test \
             need to follow it"
        );
        assert!(
            !UNOWNED_REASONS.contains(&owned),
            "the soak's F5 rule exempts `{owned}` on its own, which an event owns or an \
             incremental view can never have: the rule is looser than docs/views.md:433-436"
        );
    }
    // The one reason an event does own is exempted only while that event's wave is open, and only
    // for a view the event owns: `WAVE_REASONS` holds the source-schema string and nothing else,
    // so widening it is this test's problem.
    assert_eq!(
        WAVE_REASONS,
        ["new version: definition, targets or source schema changed, or rebuild requested"],
        "the reason list an event can be credited with changed: it must stay the engine's \
         source-schema version bump, which docs/views.md:433 documents for ALTER TABLE of a source \
         table"
    );
}

/// The second half of the third fix, without a server: the ledger's wave window. The f1 class is
/// not only "the reason was never read" but "the reason was read too early": the event's own ledger
/// closed on bars of 0, a scheduled check ran beside the still-landing wave, and `w_chain_a@source
/// = 1` (`last reason: new version: …`) was held to a bar nobody had raised. [`Ledger::judge`]
/// credits such a refresh to the event that owns the view — and to nobody else, at no other time,
/// and for no other reason (D5 stays D5).
#[test]
fn the_wave_credits_only_the_event_that_owns_the_view() {
    const NEW_VERSION: &str =
        "new version: definition, targets or source schema changed, or rebuild requested";
    const FULL_MODE: &str = "full-mode view: the definition is not maintained incrementally";
    const OWNED: &str = "w_chain_a@source";
    const OTHER: &str = "wc_refs_by_city@source";
    let t = 1_700_000_000u64; // the burst
    let refresh = |reason: &str, at: u64| (reason.to_string(), at);

    // An event that accounted for the refresh: the bar holds, wave or no wave.
    let mut l = Ledger::default();
    l.approve(OWNED, 1);
    assert!(l
        .judge(OWNED, 1, Some(&refresh(NEW_VERSION, t + 5)), t + 5)
        .is_ok());
    assert!(l
        .judge(OWNED, 2, Some(&refresh(NEW_VERSION, t + 6)), t + 6)
        .is_err());

    // The f1 case: a bar of 0, the event in flight, the refresh of a view it owns.
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    let note = l
        .judge(OWNED, 1, Some(&refresh(NEW_VERSION, t + 40)), t + 41)
        .expect("the event owns this view and its wave is open");
    assert!(
        note.contains("wave"),
        "the report must say who the refresh was credited to, got {note}"
    );
    assert_eq!(
        l.bar(OWNED),
        1,
        "a refresh credited to the event is adopted into the ledger, so the next check does not \
         judge it again"
    );
    // The same refresh on a view the event's DDL does not touch: nobody owns it, so it FAILS.
    assert!(Ledger::default()
        .judge(OTHER, 1, Some(&refresh(NEW_VERSION, t + 41)), t + 41)
        .is_err());
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    assert!(
        l.judge(OTHER, 1, Some(&refresh(NEW_VERSION, t + 41)), t + 41)
            .is_err(),
        "a wave credits the views the event owns and no others"
    );
    // A refresh that predates the burst is not this event's, whoever owns the view.
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    assert!(l
        .judge(OWNED, 1, Some(&refresh(NEW_VERSION, t - 1)), t)
        .is_err());
    // The tail: the ledger said the wave was over at `t + 25`, and a refresh read within
    // `WAVE_TAIL_S` of that is still the event's (the race with the check that follows the close).
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    l.close_wave(t + 25, 20);
    assert!(l
        .judge(OWNED, 1, Some(&refresh(NEW_VERSION, t + 26)), t + 45)
        .is_ok());
    // Past the tail the same string is a failure again.
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    l.close_wave(t + 25, 20);
    assert!(
        l.judge(OWNED, 1, Some(&refresh(NEW_VERSION, t + 47)), t + 47)
            .is_err(),
        "past the tail of a closed wave nothing is credited: the rule is not \
         source-ddl-forever"
    );
    // A reason that is not the wave's is never credited, in the middle of the window or out.
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    assert!(l
        .judge(OWNED, 1, Some(&refresh(FULL_MODE, t + 1)), t + 1)
        .is_err());
    // A fresh engine resets the `/stats` counters: the bars and the wave both go with them.
    let mut l = Ledger::default();
    l.open_wave(HashSet::from([OWNED.to_string()]), t, t + 60);
    l.approve(OWNED, 7);
    l.clear();
    assert_eq!(l.bar(OWNED), 0);
    assert!(l
        .judge(OWNED, 1, Some(&refresh(NEW_VERSION, t + 1)), t + 1)
        .is_err());
}
