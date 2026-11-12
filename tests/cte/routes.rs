use super::*;

/// Cascading / SET NULL foreign keys in every shape the planner routes: parents found through
/// their own row (traced), units from the parent's image, and the shapes that must rebuild
/// (a NULL-side parent with a condition beyond the key, a self-referential key).
const CASCADE_DDL: &[&str] = &[
    "CREATE TABLE c_pars (id INT NOT NULL PRIMARY KEY, name VARCHAR(32) NULL, active TINYINT NOT NULL DEFAULT 1)",
    "CREATE TABLE c_kids (id INT NOT NULL PRIMARY KEY, par_id INT NULL, qty INT NOT NULL, KEY k_par (par_id),
     CONSTRAINT fk_c_kids_par FOREIGN KEY (par_id) REFERENCES c_pars (id) ON DELETE CASCADE ON UPDATE CASCADE)",
    "CREATE TABLE c_orders (id INT NOT NULL PRIMARY KEY, kid_id INT NULL, KEY k_kid (kid_id))",
    "CREATE TABLE sn_pars (id INT NOT NULL PRIMARY KEY, name VARCHAR(32) NULL, active TINYINT NOT NULL DEFAULT 1)",
    "CREATE TABLE sn_kids (id INT NOT NULL PRIMARY KEY, par_id INT NULL, qty INT NOT NULL, KEY k_par (par_id),
     CONSTRAINT fk_sn_kids_par FOREIGN KEY (par_id) REFERENCES sn_pars (id) ON DELETE SET NULL ON UPDATE SET NULL)",
    "CREATE TABLE gp (id INT NOT NULL PRIMARY KEY, name VARCHAR(32) NULL)",
    "CREATE TABLE gp_pars (id INT NOT NULL PRIMARY KEY, gp_id INT NULL, name VARCHAR(32) NULL, KEY k_gp (gp_id),
     CONSTRAINT fk_gp_pars_gp FOREIGN KEY (gp_id) REFERENCES gp (id) ON DELETE CASCADE ON UPDATE CASCADE)",
    "CREATE TABLE gp_kids (id INT NOT NULL PRIMARY KEY, par_id INT NULL, qty INT NOT NULL, KEY k_par (par_id),
     CONSTRAINT fk_gp_kids_par FOREIGN KEY (par_id) REFERENCES gp_pars (id) ON DELETE CASCADE ON UPDATE CASCADE)",
    "CREATE TABLE nodes (id INT NOT NULL PRIMARY KEY, parent_id INT NULL, label VARCHAR(32) NULL, KEY k_parent (parent_id),
     CONSTRAINT fk_nodes_parent FOREIGN KEY (parent_id) REFERENCES nodes (id) ON DELETE CASCADE ON UPDATE CASCADE)",
];

const CASCADE_SEED: &[&str] = &[
    "INSERT INTO c_pars VALUES (1,'a',1),(2,'b',0),(3,'c',1),(4,'d',0),(5,'e',1)",
    "INSERT INTO c_kids VALUES (10,1,5),(20,2,6),(30,3,7),(40,NULL,8),(50,4,9),(60,5,1),(70,1,2)",
    "INSERT INTO c_orders VALUES (100,10),(200,20),(300,30),(400,40),(500,NULL),(600,50)",
    "INSERT INTO sn_pars VALUES (1,'a',1),(2,'b',0),(3,'c',1),(4,'d',0)",
    "INSERT INTO sn_kids VALUES (10,1,5),(20,2,6),(30,3,7),(40,NULL,8),(50,4,9)",
    "INSERT INTO gp VALUES (1,'g1'),(2,'g2')",
    "INSERT INTO gp_pars VALUES (1,1,'a'),(2,1,'b'),(3,2,'c'),(4,NULL,'d')",
    "INSERT INTO gp_kids VALUES (10,1,5),(20,2,6),(30,3,7),(40,NULL,8),(50,4,9)",
    "INSERT INTO nodes VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'aa'),(5,4,'aaa'),(6,NULL,'root2'),(7,6,'x')",
];

/// Views whose cascades are all traced or unit-derived (`tracing`): no rebuild, ever.
fn cascade_traced_cases() -> Vec<(Case, &'static str)> {
    vec![
        // C3: semi-joins over the cascading child, correlated on the unit through the FK: the
        // parent's image gives the units.
        (
            case("casc_semi_exists", "incremental", "spj",
                 "SELECT p.id, p.name FROM c_pars p WHERE EXISTS (SELECT 1 FROM c_kids k WHERE k.par_id = p.id AND k.qty > 5)"),
            "p:unit,k:unit,^c_pars:unit",
        ),
        (
            case("casc_semi_in", "incremental", "spj",
                 "SELECT p.id, p.name FROM c_pars p WHERE p.id IN (SELECT k.par_id FROM c_kids k WHERE k.qty > 1)"),
            "p:unit,k:unit,^c_pars:unit",
        ),
        (
            case("casc_semi_setnull", "incremental", "spj",
                 "SELECT p.id, p.name FROM sn_pars p WHERE EXISTS (SELECT 1 FROM sn_kids k WHERE k.par_id = p.id)"),
            "p:unit,k:unit,^sn_pars:unit",
        ),
        // The parent on the NULL side, joined on exactly the key.
        (
            case("casc_left_exact", "incremental", "spj",
                 "SELECT k.id, k.qty, p.name FROM c_kids k LEFT JOIN c_pars p ON p.id = k.par_id"),
            "k:unit,p:lineage,^c_pars:traced",
        ),
        // Inner join with a condition beyond the key: a kid is materialized only with its parent.
        (
            case("casc_inner_cond", "incremental", "spj",
                 "SELECT k.id, k.qty, p.name FROM c_kids k JOIN c_pars p ON p.id = k.par_id AND p.active = 1"),
            "k:unit,p:lineage,^c_pars:traced",
        ),
        // The child on the NULL side of its parent, with a condition.
        (
            case("casc_parent_first", "incremental", "spj",
                 "SELECT p.id, p.name, k.id AS kid, k.qty FROM c_pars p LEFT JOIN c_kids k ON k.par_id = p.id AND k.qty > 2"),
            "p:unit,k:unit,^c_pars:unit",
        ),
        // The parent two LEFT JOINs down, joined on exactly the key.
        (
            case("casc_chain_null", "incremental", "spj",
                 "SELECT o.id, k.qty, p.name FROM c_orders o LEFT JOIN c_kids k ON k.id = o.kid_id LEFT JOIN c_pars p ON p.id = k.par_id"),
            "o:unit,k:lineage,p:lineage,^c_pars:traced",
        ),
        (
            case("casc_grouped", "incremental", "grouped",
                 "SELECT p.id, p.name, COUNT(k.id) AS n FROM c_pars p LEFT JOIN c_kids k ON k.par_id = p.id GROUP BY p.id, p.name"),
            "p:unit,k:unit,^c_pars:unit",
        ),
        // Two levels (grandparents → parents → kids), every level joined on its key.
        (
            case("chain_all", "incremental", "spj",
                 "SELECT k.id, p.name, g.name AS gname FROM gp_kids k LEFT JOIN gp_pars p ON p.id = k.par_id LEFT JOIN gp g ON g.id = p.gp_id"),
            "k:unit,p:lineage,g:lineage,^gp:traced,^gp_pars:traced",
        ),
        (
            case("chain_grp", "incremental", "grouped",
                 "SELECT p.gp_id, COUNT(k.id) AS n FROM gp_pars p LEFT JOIN gp_kids k ON k.par_id = p.id GROUP BY p.gp_id"),
            "p:unit,k:lookup,^gp_pars:traced,^gp:unit",
        ),
        // ON DELETE SET NULL: the kid stays, its key column changes without a binlog row.
        (
            case("sn_inner", "incremental", "spj",
                 "SELECT k.id, p.name FROM sn_kids k JOIN sn_pars p ON p.id = k.par_id"),
            "k:unit,p:lineage,^sn_pars:traced",
        ),
    ]
}

/// Views whose cascades cannot be traced from the parent's row (A1: a NULL-side parent with a
/// condition beyond the key leaves kids of failing parents without lineage; A3: a
/// self-referential key cascades to every descendant): the cascade map keeps the children and
/// replays what the storage engine changed, which must keep them equal to the oracle without a
/// rebuild.
fn cascade_map_cases() -> Vec<(Case, &'static str)> {
    vec![
        // C3: a semi-join over the cascading child not correlated on the unit: the units a
        // parent's delete / key update changes are not known, so the view is Full.
        (
            case("casc_semi_lookup", "full",
                 "EXISTS subquery over `c_kids`: the units a cascade from `c_pars` changes are not known",
                 "SELECT o.id FROM c_orders o WHERE EXISTS (SELECT 1 FROM c_kids k WHERE k.qty = o.kid_id - 4)"),
            "rebuild",
        ),
        (
            case("casc_semi_grouped", "full",
                 "EXISTS subquery over `c_kids`: the units a cascade from `c_pars` changes are not known",
                 "SELECT p.active, COUNT(*) AS n FROM c_pars p WHERE EXISTS (SELECT 1 FROM c_kids k WHERE k.par_id = p.id) GROUP BY p.active"),
            "rebuild",
        ),
        (
            case("a1_left", "incremental", "spj",
                 "SELECT k.id, k.qty, p.name FROM c_kids k LEFT JOIN c_pars p ON p.id = k.par_id AND p.active = 1"),
            "k:unit,p:lineage,^c_pars:map",
        ),
        (
            case("a1_chain", "incremental", "spj",
                 "SELECT o.id, k.qty, p.name FROM c_orders o LEFT JOIN c_kids k ON k.id = o.kid_id LEFT JOIN c_pars p ON p.id = k.par_id AND p.active = 1"),
            "o:unit,k:lineage,p:lineage,^c_pars:map",
        ),
        (
            case("a1_set_null", "incremental", "spj",
                 "SELECT k.id, k.par_id, p.name FROM sn_kids k LEFT JOIN sn_pars p ON p.id = k.par_id AND p.active = 1"),
            "k:unit,p:lineage,^sn_pars:map",
        ),
        (
            case("sn_flat", "incremental", "spj", "SELECT k.id, k.par_id, k.qty FROM sn_kids k"),
            "k:unit,^sn_pars:map",
        ),
        (
            case("self_flat", "incremental", "spj", "SELECT n.id, n.parent_id, n.label FROM nodes n"),
            "n:unit,^nodes:map",
        ),
        (
            case("self_join", "incremental", "spj",
                 "SELECT n.id, n.label, p.label AS plabel FROM nodes n LEFT JOIN nodes p ON p.id = n.parent_id"),
            "n:unit,p:lineage,^nodes:map,^nodes:map",
        ),
        (
            case("self_grp", "incremental", "grouped",
                 "SELECT n.parent_id, COUNT(*) AS n FROM nodes n GROUP BY n.parent_id"),
            "n:unit,^nodes:map",
        ),
    ]
}

fn cascade_steps() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "inactive parent deleted (kid 20 cascades away)",
            "DELETE FROM c_pars WHERE id = 2",
        ),
        (
            "active parent deleted (kids 10, 70 cascade away)",
            "DELETE FROM c_pars WHERE id = 1",
        ),
        (
            "inactive parent key moved (kid 50 follows)",
            "UPDATE c_pars SET id = 44 WHERE id = 4",
        ),
        (
            "kid moved to another parent",
            "UPDATE c_kids SET par_id = 3 WHERE id = 60",
        ),
        (
            "SET NULL parent deleted (kid 20 keeps its row, key NULL)",
            "DELETE FROM sn_pars WHERE id = 2",
        ),
        (
            "SET NULL parent key moved (kid 50 key NULL)",
            "UPDATE sn_pars SET id = 44 WHERE id = 4",
        ),
        (
            "grandparent deleted (parents 1, 2 and kids 10, 20 cascade away)",
            "DELETE FROM gp WHERE id = 1",
        ),
        (
            "grandparent key moved",
            "UPDATE gp SET id = 22 WHERE id = 2",
        ),
        (
            "node deleted (descendants 4, 5 cascade away)",
            "DELETE FROM nodes WHERE id = 2",
        ),
        (
            "root deleted (3 cascades away)",
            "DELETE FROM nodes WHERE id = 1",
        ),
        (
            // (InnoDB refuses ON UPDATE CASCADE on a self-referential key: no key move here.)
            "node re-parented",
            "UPDATE nodes SET parent_id = NULL WHERE id = 7",
        ),
    ]
}

async fn run_cascade(
    ctx: &Ctx,
    label: &str,
    cases: Vec<(Case, &'static str)>,
    mapped: bool,
    window_ms: &str,
) -> Result<()> {
    let db = ctx.new_db(&format!("cte_casc_{label}")).await?;
    let mut c = db.conn().await?;
    for s in CASCADE_DDL.iter().chain(CASCADE_SEED) {
        exec(&mut c, s).await?;
    }
    let mariadb = ctx.server().is_mariadb();
    let mut bad = String::new();
    let mut plain = Vec::new();
    for (case, tracing) in cases {
        let def = format!("CREATE VIEW {} AS {}", case.name, case.select);
        let plan = analyzer_plan(&db, mariadb, &def).await?;
        if plan.tracing() != *tracing {
            bad.push_str(&format!(
                "\n  {}: routes {}, expected {tracing}",
                case.name,
                plan.tracing()
            ));
        }
        plain.push(case);
    }
    if !bad.is_empty() {
        bail!("cascade routes differ:{bad}");
    }
    let (specs, expect) = prepare(&db, mariadb, plain).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    let before = eng.snapshot();
    // Full-mode views (C3) are rebuilt on every change by design; incremental ones never are.
    let incremental: Vec<&str> = specs
        .iter()
        .filter(|s| s.kind == Kind::Incremental)
        .map(|s| s.name.as_str())
        .collect();
    let whole0: Vec<u64> = incremental
        .iter()
        .map(|v| eng.view_stats(&db, v).map_or(0, |s| s.whole_view_refreshes))
        .collect();
    for (what, stmt) in cascade_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    let d = eng.snapshot().since(&before);
    for (v, w0) in incremental.iter().zip(&whole0) {
        let s = eng.stats_of(&db, v)?;
        anyhow::ensure!(
            s.whole_view_refreshes == *w0,
            "{label}: cascades rebuilt the incremental view {v}: {s:?} {}",
            fmt_snapshot(&d)
        );
    }
    anyhow::ensure!(
        incremental.len() < specs.len() || d.full_rebuilds == 0,
        "{label}: cascades rebuilt the views: {}",
        fmt_snapshot(&d)
    );
    anyhow::ensure!(
        !mapped || d.cascade_rows > 0,
        "{label}: no cascaded row was replayed from the cascade map: {}",
        fmt_snapshot(&d)
    );
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// A1 / A3: every cascade shape against its oracle, none with a rebuild: traced and unit-derived
/// routes from the parent's row; NULL-side parents with extra conditions, SET NULL keys and
/// self-referential keys through the cascade map (rows replayed).
#[test]
fn cte_cascades_match_their_oracles() {
    scenario("cte_cascade", |ctx| async move {
        run_cascade(&ctx, "traced_w0", cascade_traced_cases(), false, "0").await?;
        run_cascade(&ctx, "map_w0", cascade_map_cases(), true, "0").await?;
        run_cascade(&ctx, "map_w50", cascade_map_cases(), true, "50").await?;
        Ok(())
    });
}

// --- L1: rows chosen by ORDER BY / PARTITION BY columns that are not projected.
