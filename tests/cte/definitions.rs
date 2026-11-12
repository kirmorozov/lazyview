use super::*;

const NOT_IN_DDL: &[&str] = &[
    "CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, ref INT NULL)",
    "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, customer_id INT NULL, total INT NOT NULL)",
    "CREATE TABLE payments (id INT NOT NULL PRIMARY KEY, order_id INT NULL, amount INT NOT NULL)",
    "INSERT INTO customers VALUES (1, 2), (2, NULL), (3, 1), (4, 4), (5, NULL)",
    "INSERT INTO orders VALUES (20, 2, 450)",
    "INSERT INTO payments VALUES (200, 20, 450)",
];

fn not_in_cases() -> Vec<Case> {
    vec![
        case(
            "nin_single",
            "incremental",
            "spj",
            "SELECT c.id FROM customers c WHERE c.ref NOT IN (SELECT o.customer_id FROM orders o WHERE o.total > 400)",
        ),
        // The 3-table form (L4).
        case(
            "nin_join",
            "incremental",
            "spj",
            "SELECT c.id FROM customers c WHERE c.ref NOT IN
             (SELECT o.customer_id FROM orders o JOIN payments p ON p.order_id = o.id WHERE p.amount > 400)",
        ),
        case(
            "nin_grouped",
            "incremental",
            "grouped",
            "SELECT c.ref, COUNT(*) AS n FROM customers c
             WHERE c.id NOT IN (SELECT o.customer_id FROM orders o WHERE o.total > 400) GROUP BY c.ref",
        ),
        // One unit, the same refresh statement every time.
        case(
            "nin_global",
            "incremental",
            "global",
            "SELECT COUNT(*) AS n FROM customers c WHERE c.ref NOT IN (SELECT o.customer_id FROM orders o WHERE o.total > 400)",
        ),
        case(
            "nex_control",
            "incremental",
            "spj",
            "SELECT c.id FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.total > 400 AND o.customer_id = c.ref)",
        ),
    ]
}

/// The subqueries flip between empty and non-empty five times, then a NULL key enters and
/// leaves.
fn not_in_steps() -> Steps {
    let mut out = Vec::new();
    for round in 1..=5 {
        out.push((
            format!("round {round}: orders empty"),
            vec!["UPDATE orders SET total = 1 WHERE id = 20".to_string()],
        ));
        out.push((
            format!("round {round}: payments empty"),
            vec!["UPDATE payments SET amount = 1 WHERE id = 200".to_string()],
        ));
        out.push((
            format!("round {round}: orders non-empty"),
            vec!["UPDATE orders SET total = 401 WHERE id = 20".to_string()],
        ));
        out.push((
            format!("round {round}: payments non-empty"),
            vec!["UPDATE payments SET amount = 450 WHERE id = 200".to_string()],
        ));
    }
    out.push((
        "a NULL key enters".into(),
        vec!["INSERT INTO orders VALUES (21, NULL, 500)".to_string()],
    ));
    out.push((
        "the NULL key leaves".into(),
        vec!["DELETE FROM orders WHERE id = 21".to_string()],
    ));
    out
}

const SUITE_NOT_IN: Suite<'static> = Suite {
    tag: "nin",
    ddl: NOT_IN_DDL,
    cases: |_| Cases::Plain(not_in_cases()),
    steps: not_in_steps,
    plans: None,
    plan_budget: None,
};

/// Whether the server re-uses a stale NOT IN result in a re-executed prepared statement (the
/// MariaDB bug F5 works around), for the log: lazyview must be right either way.
async fn not_in_canary(ctx: &Ctx) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let db = ctx.new_db("cte_nin_canary").await?;
    let mut c = db.conn().await?;
    for s in [
        "CREATE TABLE t1 (id INT PRIMARY KEY, ref INT NULL)",
        "CREATE TABLE t2 (id INT PRIMARY KEY, k INT NULL, amount INT NULL)",
        "INSERT INTO t1 VALUES (1, 2), (2, NULL), (3, 1)",
        "INSERT INTO t2 VALUES (20, 2, 1)",
    ] {
        exec(&mut c, s).await?;
    }
    let sql =
        "SELECT id FROM t1 WHERE ref NOT IN (SELECT k FROM t2 WHERE amount > 400) ORDER BY id";
    let st = c.prep(renamed_sql(&c, sql)?.sql).await?;
    let _: Vec<i64> = c.exec(&st, ()).await?;
    exec(&mut c, "UPDATE t2 SET amount = 401").await?;
    let again: Vec<i64> = c.exec(&st, ()).await?;
    let fresh: Vec<i64> = typed_rows(&mut c, sql).await?;
    ctx.log(format!(
        "NOT IN canary: re-executed {again:?}, text {fresh:?}{}",
        if again != fresh {
            " (the server re-uses a stale NOT IN result)"
        } else {
            ""
        }
    ));
    c.disconnect().await.ok();
    Ok(())
}

/// F5: incremental NOT IN views stay equal to their oracles while the subquery flips between
/// empty and non-empty (their refresh statements carry the NULL-aware NOT EXISTS form).
#[test]
fn cte_not_in_survives_reexecution() {
    scenario("cte_nin", |ctx| async move {
        not_in_canary(&ctx).await?;
        run_suite(&ctx, &SUITE_NOT_IN, "w0", "0").await?;
        run_suite(&ctx, &SUITE_NOT_IN, "w50", "50").await?;
        Ok(())
    });
}

// --- L-06 / L-07: definitions that are not one SELECT fail once, with the user's words.

/// (name, definition, expected error part; None: must become ready).
const BAD_DEFINITIONS: &[(&str, &str, Option<&str>)] = &[
    (
        "lz_insert",
        "INSERT INTO lz_t VALUES (9)",
        Some("the definition must be a single SELECT statement (or CREATE VIEW name AS SELECT …); found `INSERT` … after AS"),
    ),
    (
        "lz_two",
        "SELECT id FROM lz_t; SELECT id FROM lz_t",
        Some("the definition contains 2 statements"),
    ),
    ("lz_empty", "", Some("the definition is empty after AS")),
    (
        "lz_var",
        "SELECT id, @@session.sql_mode AS m FROM lz_t",
        Some("a view cannot read user or system variables (`@@session.sql_mode`)"),
    ),
    ("lz_crlf", "SELECT id\r\nFROM lz_t\r\nWHERE id > 0;\r\n", None),
];

async fn run_bad_definitions(ctx: &Ctx) -> Result<()> {
    let db = ctx.new_db("cte_baddefs").await?;
    let mut c = db.conn().await?;
    exec(&mut c, "CREATE TABLE lz_t (id INT NOT NULL PRIMARY KEY)").await?;
    exec(&mut c, "INSERT INTO lz_t VALUES (1), (2)").await?;
    let ok = vec![case("lz_ok", "incremental", "spj", "SELECT id FROM lz_t")];
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), ok).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, "0").await?;
    let mut ids = Vec::new();
    for (name, def, _) in BAD_DEFINITIONS {
        exec(
            &mut c,
            &format!(
                "INSERT INTO _lazyview (name, definition) VALUES ({}, {})",
                q(name),
                q(def)
            ),
        )
        .await?;
        ids.push(scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64);
    }
    let mut problems = Vec::new();
    for ((name, _, want), id) in BAD_DEFINITIONS.iter().zip(&ids) {
        let state = if want.is_some() { "error" } else { "ready" };
        match wait_def_state(&mut c, &mut eng, *id, state, t_ready()).await {
            Ok(st) => {
                let msg = st.last_error.clone().unwrap_or_default();
                if let Some(w) = want {
                    if !msg.contains(w) {
                        problems.push(format!("{name}: error `{msg}` lacks `{w}`"));
                    }
                }
                ctx.log(format!("{name} → {state}: {msg}"));
            }
            Err(e) => problems.push(format!("{name}: {e:#}")),
        }
    }
    // An error is final: no rebuild loop (state stays `error`), no table.
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < until {
        for ((name, _, want), id) in BAD_DEFINITIONS.iter().zip(&ids) {
            if want.is_none() {
                continue;
            }
            let rows: Vec<StateRow> = view_states(&mut c)
                .await?
                .into_iter()
                .filter(|s| s.definition_id == Some(*id))
                .collect();
            if rows.iter().any(|s| s.state != "error") {
                problems.push(format!("{name}: state left `error`: {rows:?}"));
            }
            if table_exists(&mut c, name).await? {
                problems.push(format!("{name}: a table was created"));
            }
        }
        if !problems.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    eng.stop().await?;
    c.disconnect().await.ok();
    anyhow::ensure!(problems.is_empty(), "{}", problems.join("\n"));
    Ok(())
}

/// L-06 / L-07: a non-SELECT, two statements, an empty text or a variable fail at parse time
/// with a message in the user's terms and stay in `error` (no rebuild loop); CRLF text works.
#[test]
fn cte_bad_definitions_fail_once() {
    scenario("cte_baddefs", |ctx| async move {
        run_bad_definitions(&ctx).await
    });
}

// --- G1: a semi-join or scalar subquery joins its table only over key-exact equalities.

/// (the name the engine records, a definition inserted without a name).
const COMMENTED_DEFINITIONS: &[(&str, &str)] = &[
    (
        "g2_cmt",
        "-- annotated view\nCREATE VIEW g2_cmt AS SELECT id, num FROM g2_c",
    ),
    (
        "g2_blk",
        "/* block note */ CREATE VIEW g2_blk AS SELECT id FROM g2_c",
    ),
    (
        "g2_hash",
        "#hash note\nCREATE VIEW g2_hash AS SELECT num FROM g2_c",
    ),
    ("g2_ctl", "CREATE VIEW g2_ctl AS SELECT id FROM g2_c"),
    (
        "g2_bom",
        "\u{feff}CREATE VIEW g2_bom AS SELECT id, num FROM g2_c WHERE num > 1",
    ),
];

/// Every state row of definition `id` is `ready` under `name`.
async fn def_ready_as(c: &mut mysql_async::Conn, id: u64, name: &str) -> Result<bool> {
    let rows: Vec<StateRow> = view_states(c)
        .await?
        .into_iter()
        .filter(|s| s.definition_id == Some(id))
        .collect();
    Ok(!rows.is_empty() && rows.iter().all(|s| s.state == "ready" && s.name == name))
}

async fn run_commented_definitions(ctx: &Ctx) -> Result<()> {
    let db = ctx.new_db("cte_g2").await?;
    let mut c = db.conn().await?;
    exec(
        &mut c,
        "CREATE TABLE g2_c (id INT NOT NULL PRIMARY KEY, num INT NOT NULL)",
    )
    .await?;
    exec(&mut c, "INSERT INTO g2_c VALUES (1, 1), (2, 2), (3, 7)").await?;
    let ok = vec![case("g2_ok", "incremental", "spj", "SELECT id FROM g2_c")];
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), ok).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, "0").await?;
    let mut ids = Vec::new();
    for (_, def) in COMMENTED_DEFINITIONS {
        exec(
            &mut c,
            &format!("INSERT INTO _lazyview (definition) VALUES ({})", q(def)),
        )
        .await?;
        ids.push(scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64);
    }
    // G3: a bare SELECT without a name.
    exec(
        &mut c,
        "INSERT INTO _lazyview (definition) VALUES ('SELECT num, COUNT(*) AS n FROM g2_c GROUP BY num')",
    )
    .await?;
    let bare = scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64;
    for ((name, _), id) in COMMENTED_DEFINITIONS.iter().zip(&ids) {
        wait_def_state(&mut c, &mut eng, *id, "ready", t_ready()).await?;
        let recorded = scalar(
            &mut c,
            &format!("SELECT name FROM _lazyview WHERE id = {id}"),
        )
        .await?;
        anyhow::ensure!(
            recorded.as_deref() == Some(*name),
            "{name}: _lazyview.name is {recorded:?}"
        );
    }
    let st = wait_def_state(&mut c, &mut eng, bare, "error", t_ready()).await?;
    let msg = st.last_error.unwrap_or_default();
    anyhow::ensure!(
        msg.contains("the definition needs a name: set `_lazyview.name`, or write CREATE VIEW name AS SELECT"),
        "bare SELECT without a name: {msg}"
    );
    // A restart plans the definitions again, now with their recorded names.
    eng.stop().await?;
    let mut eng = ctx.start_engine(
        &db,
        &[
            ("LAZYVIEW_BATCH_WINDOW_MS", "0"),
            ("LAZYVIEW_FULL_REFRESH_MS", "0"),
        ],
    )?;
    exec(
        &mut c,
        "INSERT INTO _lazyview (definition) VALUES ('CREATE VIEW g2_after AS SELECT num FROM g2_c')",
    )
    .await?;
    let after = scalar_i64(&mut c, "SELECT LAST_INSERT_ID()").await? as u64;
    wait_def_state(&mut c, &mut eng, after, "ready", t_ready()).await?;
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < until {
        for ((name, _), id) in COMMENTED_DEFINITIONS.iter().zip(&ids) {
            if !def_ready_as(&mut c, *id, name).await? {
                let rows: Vec<StateRow> = view_states(&mut c)
                    .await?
                    .into_iter()
                    .filter(|s| s.definition_id == Some(*id))
                    .collect();
                bail!("{name} after the restart: {rows:?}");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// G2 / G3: definitions with a comment (or a byte-order mark) before CREATE VIEW stay ready
/// under their names across a restart; a bare SELECT without a name says it needs one.
#[test]
fn cte_commented_definitions_survive_restart() {
    scenario("cte_g2", |ctx| async move {
        run_commented_definitions(&ctx).await
    });
}

// --- G5: a JSON column in a composite unit (MySQL compares JSON by representation in a row IN
// list of two or more tuples, the engine's filter for several units).
