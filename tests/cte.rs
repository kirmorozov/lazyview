//! CTEs, derived tables and subqueries through the query analyzer (docs/design-analyzer.md):
//! one or more views per taxonomy class over the e-commerce schema, each checked for its
//! strategy (`_lazyview_state.mode` / `detail`, and the analyzer's own plan against the server's
//! catalog) and against an oracle VIEW with the same definition after a seeded random workload.
//!
//! Until the analyzer is wired into planning (`analyze::plan`), the engine is given the
//! analyzer's flattened form of each definition while the oracle keeps the definition as
//! written: that checks on every server that the rewrite returns the same rows and column names.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use common::workload::{names, run_batches, setup_ecommerce, Gen, Sizes, Workload};
use common::*;
use lazyview::analyze::{parse_definition, Mode, ViewPlan};
use lazyview::catalog::{Catalog, Column, TableInfo, TableRef};

struct Case {
    name: &'static str,
    select: &'static str,
    /// Expected `_lazyview_state.mode` / `detail`.
    mode: &'static str,
    detail: &'static str,
}

const fn case(
    name: &'static str,
    mode: &'static str,
    detail: &'static str,
    select: &'static str,
) -> Case {
    Case {
        name,
        select,
        mode,
        detail,
    }
}

fn cases() -> Vec<Case> {
    vec![
        // --- merge family: planned exactly like the inline form
        case(
            "cte_filter",
            "incremental",
            "spj",
            "WITH paid AS (SELECT id, customer_id, created_at, status FROM orders WHERE status IN ('paid','shipped'))
             SELECT id, customer_id, created_at FROM paid",
        ),
        case(
            "cte_join",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, name, tier FROM customers WHERE tier IS NOT NULL)
             SELECT o.id, o.status, c.name AS customer, c.tier FROM orders o JOIN c ON c.id = o.customer_id",
        ),
        // The same CTE text in another definition.
        case(
            "cte_join_twin",
            "incremental",
            "grouped",
            "WITH c AS (SELECT id, name, tier FROM customers WHERE tier IS NOT NULL)
             SELECT c.id, c.tier, COUNT(*) AS n FROM c JOIN orders o ON o.customer_id = c.id GROUP BY c.id, c.tier",
        ),
        // Referenced three times, joined to itself through different columns, NULL sides.
        case(
            "cte_multi_ref",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, referrer_id, region_id, name FROM customers)
             SELECT a.id, a.name, b.name AS referrer, b2.name AS referrer2, x.id AS neighbour
             FROM c a JOIN c b ON b.id = a.referrer_id
             LEFT JOIN c b2 ON b2.id = b.referrer_id
             LEFT JOIN c x ON x.region_id = a.region_id AND x.id = a.id + 1",
        ),
        // Column list, unaliased expressions over CTE columns (server-generated names).
        case(
            "cte_column_list",
            "incremental",
            "spj",
            "WITH t (oid, line, amount) AS (SELECT order_id, line_no, qty * price FROM order_items)
             SELECT oid, line, amount, amount * 2, ROUND(amount / 3, 2) FROM t WHERE amount > 50",
        ),
        case(
            "cte_chained",
            "incremental",
            "spj",
            "WITH o AS (SELECT id, customer_id FROM orders WHERE status <> 'cancelled'),
                  oc AS (SELECT o.id, c.name, c.region_id FROM o JOIN customers c ON c.id = o.customer_id)
             SELECT oc.id, oc.name, r.name AS region FROM oc LEFT JOIN regions r ON r.id = oc.region_id",
        ),
        // A WITH inside a derived table inside a CTE.
        case(
            "cte_nested_with",
            "incremental",
            "spj",
            "WITH x AS (SELECT d.oid, d.amount FROM
                          (WITH li AS (SELECT order_id AS oid, line_no, qty * price AS amount FROM order_items)
                           SELECT oid, line_no, amount FROM li WHERE amount > 10) d)
             SELECT oid, amount FROM x",
        ),
        // A CTE named like the table it reads.
        case(
            "cte_shadow",
            "incremental",
            "spj",
            "WITH orders AS (SELECT id, customer_id, status FROM orders WHERE status = 'new')
             SELECT id, customer_id FROM orders",
        ),
        // A2: functions with a value for NULL arguments (CHAR(NULL) = '', CHARSET(NULL) =
        // 'binary', COERCIBILITY(NULL) = 6) on the NULL side: guarded when the derived table
        // or scalar subquery is merged, NULL where the payment is missing.
        case(
            "derived_null_side_char",
            "incremental",
            "spj",
            "SELECT o.id, p.v FROM orders o LEFT JOIN (SELECT order_id, CHAR(65 + (amount > 10)) AS v FROM payments WHERE status = 'ok') p
             ON p.order_id = o.id",
        ),
        case(
            "derived_null_side_charset",
            "incremental",
            "spj",
            "SELECT o.id, p.v, p.c FROM orders o LEFT JOIN (SELECT order_id, CHARSET(method) AS v, COERCIBILITY(method) AS c FROM payments WHERE status = 'ok') p
             ON p.order_id = o.id",
        ),
        case(
            "scalar_null_side_char",
            "incremental",
            "spj",
            "SELECT o.id, (SELECT CHAR(65 + (p.amount > 10)) FROM payments p WHERE p.id = o.id) AS v FROM orders o",
        ),
        // A4: `*` over USING / NATURAL joins (shared columns coalesced by the server): Full
        // with a reason, never an empty or duplicated projection.
        case(
            "star_using_join",
            "full",
            "SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns",
            "SELECT * FROM orders JOIN customers USING (id)",
        ),
        case(
            "star_natural_join",
            "full",
            "SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns",
            "SELECT * FROM customers NATURAL JOIN regions",
        ),
        case(
            "star_right_using",
            "full",
            "SELECT * over a USING or NATURAL join (shared columns are coalesced): list the columns",
            "SELECT * FROM orders RIGHT JOIN customers USING (id)",
        ),
        // Single-table derived table on the NULL side; its constant column is NULL when missing.
        case(
            "derived_null_side",
            "incremental",
            "spj",
            "SELECT o.id, p.method, p.amount, p.has_payment
             FROM orders o LEFT JOIN (SELECT order_id, method, amount, 1 AS has_payment FROM payments WHERE status = 'ok') p
             ON p.order_id = o.id",
        ),
        case(
            "derived_preserved",
            "incremental",
            "spj",
            "SELECT x.id, x.name, r.name AS region
             FROM (SELECT id, name, region_id FROM customers WHERE credit > 100) x
             LEFT JOIN regions r ON r.id = x.region_id",
        ),
        // --- join syntax that only spells an inner / left join differently
        case(
            "join_right",
            "incremental",
            "spj",
            "SELECT o.id, o.status, c.name AS customer FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
        ),
        case(
            "join_straight",
            "incremental",
            "spj",
            "SELECT i.order_id, i.line_no, p.sku FROM order_items i STRAIGHT_JOIN products p ON p.id = i.product_id",
        ),
        case(
            "join_nested",
            "incremental",
            "spj",
            "SELECT o.id, i.line_no, c.name FROM (orders o JOIN order_items i ON i.order_id = o.id)
             JOIN customers c ON c.id = o.customer_id",
        ),
        // --- aggregated relations
        // Merged up: group-key predicates go to WHERE, the others to HAVING; NULL group keys.
        case(
            "cte_agg_alone",
            "incremental",
            "grouped",
            "WITH t AS (SELECT o.customer_id, COUNT(*) AS n, SUM(i.qty) AS q
                        FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.customer_id)
             SELECT customer_id, n, q FROM t WHERE n > 1 AND (customer_id IS NULL OR customer_id > 3)",
        ),
        case(
            "cte_agg_global",
            "incremental",
            "global",
            "WITH t AS (SELECT COUNT(*) AS n, SUM(amount) AS s, MAX(paid_at) AS last_paid FROM payments WHERE status = 'ok')
             SELECT n, s, last_paid FROM t",
        ),
        // Joined on the other table's key: the aggregation is pulled up through the join.
        case(
            "cte_agg_key_join",
            "incremental",
            "grouped",
            "WITH t AS (SELECT order_id, SUM(qty * price) AS amount, COUNT(*) AS n FROM order_items GROUP BY order_id)
             SELECT o.id, o.status, t.amount, t.n FROM orders o JOIN t ON t.order_id = o.id WHERE t.n > 1",
        ),
        // ... also through a chain of key joins.
        case(
            "cte_agg_key_chain",
            "incremental",
            "grouped",
            "WITH t AS (SELECT order_id, SUM(qty) AS units FROM order_items GROUP BY order_id)
             SELECT t.order_id, c.name, c.tier, t.units FROM t
             JOIN orders o ON o.id = t.order_id JOIN customers c ON c.id = o.customer_id",
        ),
        // A table without primary key through a CTE, grouped by nullable columns.
        case(
            "cte_events",
            "incremental",
            "grouped",
            "WITH e AS (SELECT customer_id, kind, amount FROM events_log WHERE amount IS NOT NULL)
             SELECT customer_id, kind, COUNT(*) AS n, SUM(amount) AS s FROM e GROUP BY customer_id, kind",
        ),
        // --- DISTINCT = GROUP BY over the output columns
        case(
            "distinct_top",
            "incremental",
            "grouped",
            "SELECT DISTINCT customer_id, status FROM orders",
        ),
        case(
            "distinct_cte",
            "incremental",
            "grouped",
            "WITH t AS (SELECT DISTINCT p.category, i.qty > 5 AS big FROM order_items i JOIN products p ON p.id = i.product_id)
             SELECT category, big FROM t",
        ),
        // NULL-extended rows feeding the DISTINCT tuple: the block planner's NULL-side rule.
        case(
            "distinct_left",
            "full",
            "group key on the NULL side of a LEFT JOIN (needs internal views)",
            "SELECT DISTINCT o.status, c.tier FROM orders o LEFT JOIN customers c ON c.id = o.customer_id",
        ),
        // --- semi- and anti-joins
        case(
            "in_unique",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o
             WHERE o.customer_id IN (SELECT id FROM customers WHERE tier IN ('gold','platinum'))",
        ),
        case(
            "exists_on_unit",
            "incremental",
            "spj",
            "SELECT o.id, o.created_at FROM orders o
             WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 3)",
        ),
        case(
            "not_exists_on_unit",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o
             WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.status = 'ok')",
        ),
        case(
            "in_on_unit",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders WHERE status = 'paid')",
        ),
        case(
            "cte_in_subquery",
            "incremental",
            "spj",
            "WITH vip AS (SELECT id FROM customers WHERE FIND_IN_SET('vip', flags) > 0)
             SELECT id, customer_id FROM orders WHERE customer_id IN (SELECT id FROM vip)",
        ),
        // Correlated on something else than the unit: the subquery's rows are looked up.
        case(
            "in_lookup",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o WHERE o.customer_id IN (SELECT referrer_id FROM customers WHERE tier = 'gold')",
        ),
        case(
            "not_exists_lookup",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.referrer_id)",
        ),
        case(
            "grouped_exists",
            "incremental",
            "grouped",
            "SELECT o.customer_id, COUNT(*) AS n FROM orders o
             WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.region_id IS NOT NULL)
             GROUP BY o.customer_id",
        ),
        // Scalar subqueries looking one row up by its key: LEFT JOINs.
        case(
            "scalar_lookup",
            "incremental",
            "spj",
            "SELECT o.id, o.status, (SELECT name FROM customers c WHERE c.id = o.customer_id) AS customer,
                    (SELECT COALESCE(tier, 'none') FROM customers c2 WHERE c2.id = o.customer_id) AS tier
             FROM orders o",
        ),
        case(
            "scalar_unaliased",
            "incremental",
            "spj",
            "SELECT i.order_id, i.line_no, (SELECT sku FROM products p WHERE p.id = i.product_id) FROM order_items i",
        ),
        case(
            "scalar_in_where",
            "incremental",
            "spj",
            "SELECT o.id, o.created_at FROM orders o WHERE (SELECT region_id FROM customers c WHERE c.id = o.customer_id) = 2",
        ),
        // --- block shapes the planner maintains as bounded recomputes / partitions
        case(
            "topn_products",
            "incremental",
            "global",
            "SELECT id, sku, price FROM products WHERE active = 1 ORDER BY price DESC, id LIMIT 5",
        ),
        case(
            "window_partitioned",
            "incremental",
            "grouped",
            "SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS rn,
                    COUNT(*) OVER (PARTITION BY customer_id) AS n
             FROM orders",
        ),
        case(
            "window_whole",
            "incremental",
            "global",
            "SELECT id, RANK() OVER (ORDER BY created_at DESC, id) AS r FROM orders WHERE status = 'paid'",
        ),
        case(
            "keyless_events",
            "incremental",
            "spj",
            "SELECT customer_id, kind, amount FROM events_log WHERE amount > 50",
        ),
        // The group key reduced to what determines it (o.id; c joined on its key).
        case(
            "group_reduced_left",
            "incremental",
            "grouped",
            "SELECT o.id, c.name, COUNT(i.line_no) AS n_lines
             FROM orders o LEFT JOIN customers c ON c.id = o.customer_id LEFT JOIN order_items i ON i.order_id = o.id
             GROUP BY o.id, c.name",
        ),
        case(
            "distinct_reduced",
            "incremental",
            "grouped",
            "SELECT DISTINCT c.id, c.name, c.tier FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = 'paid'",
        ),
        // Anti-join: NULL-extended rows kept by WHERE, the NULL side traced by lookups.
        case(
            "anti_join_is_null",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o LEFT JOIN payments p ON p.order_id = o.id AND p.status = 'ok' WHERE p.id IS NULL",
        ),
        // --- not maintained incrementally yet (each reason names the construct)
        case(
            "cte_agg_joined",
            "full",
            "GROUP BY in CTE `t` joined to other tables (needs internal views)",
            "WITH t AS (SELECT customer_id, SUM(amount) AS total FROM events_log GROUP BY customer_id)
             SELECT c.id, c.name, t.total FROM customers c JOIN t ON t.customer_id = c.region_id",
        ),
        case(
            "cte_union",
            "full",
            "UNION ALL in CTE `u` (needs branch plans)",
            "WITH u AS (SELECT id, 'o' AS src FROM orders WHERE status = 'paid'
                        UNION ALL SELECT id, 'p' FROM payments WHERE status = 'ok')
             SELECT id, src FROM u",
        ),
        case(
            "cte_window",
            "full",
            "window function in CTE `t` (needs internal views)",
            "WITH t AS (SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS rn FROM orders)
             SELECT id, customer_id FROM t WHERE rn = 1",
        ),
        case(
            "cte_recursive",
            "full",
            "recursive CTE `r`",
            "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5)
             SELECT o.id, r.n FROM orders o JOIN r ON r.n = o.id % 5 + 1",
        ),
        case(
            "cte_limit",
            "full",
            "ORDER BY … LIMIT in CTE `t` (needs internal views)",
            "WITH t AS (SELECT id, price FROM products ORDER BY price DESC, id LIMIT 5) SELECT id, price FROM t",
        ),
        case(
            "not_in",
            "incremental",
            "spj",
            "SELECT id FROM orders WHERE customer_id NOT IN (SELECT referrer_id FROM customers WHERE referrer_id IS NOT NULL)",
        ),
    ]
}

fn mode_detail(m: &Mode) -> (String, String) {
    match m {
        Mode::Spj => ("incremental".into(), "spj".into()),
        Mode::Grouped => ("incremental".into(), "grouped".into()),
        Mode::Global => ("incremental".into(), "global".into()),
        Mode::Full(r) => ("full".into(), r.clone()),
    }
}

/// Whether `analyze::plan` already goes through the analyzer (a merged CTE is not Full).
fn wired() -> bool {
    let t = TableInfo {
        schema: "db".into(),
        name: "t0".into(),
        columns: vec![Column::simple("id", "int")],
        pk: vec![0],
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    };
    let tables: HashMap<TableRef, Arc<TableInfo>> =
        [(("db".to_string(), "t0".to_string()), Arc::new(t))].into();
    let d = parse_definition("CREATE VIEW v AS WITH c AS (SELECT id FROM t0) SELECT id FROM c")
        .unwrap();
    lazyview::analyze::plan(&d, "db", &tables)
        .map(|p| !matches!(p.mode, Mode::Full(_)))
        .unwrap_or(false)
}

/// The analyzer's plan of a definition, against the server's catalog.
async fn analyzer_plan(db: &Db, mariadb: bool, definition: &str) -> Result<ViewPlan> {
    let d = parse_definition(definition)?;
    let mut c = db.conn().await?;
    let mut cat = Catalog::default();
    let mut tables = HashMap::new();
    // Like the engine: the tables read, then the parents of their cascading foreign keys.
    let mut queue: Vec<(TableRef, usize)> =
        lazyview::analyzer::referenced_tables(&d.query, &db.name)
            .into_iter()
            .map(|t| (t, 0))
            .collect();
    while let Some((t, depth)) = queue.pop() {
        if tables.contains_key(&t) || depth > 8 {
            continue;
        }
        if let Some(info) = cat.get(&mut c, &t.0, &t.1, mariadb).await? {
            queue.extend(info.cascades.iter().map(|f| (f.parent.clone(), depth + 1)));
            tables.insert(t, info);
        }
    }
    c.disconnect().await.ok();
    lazyview::analyzer::plan(&d, &db.name, &tables)
}

/// The flat SELECT of an incremental plan (no hidden columns).
fn flat_sql(p: &ViewPlan) -> String {
    let mut sql = format!("SELECT {} FROM {}", p.projection.join(", "), p.from_sql);
    if let Some(w) = &p.where_sql {
        sql.push_str(&format!(" WHERE {w}"));
    }
    if let Some(g) = &p.group_sql {
        sql.push_str(&format!(" GROUP BY {g}"));
    }
    if let Some(h) = &p.having_sql {
        sql.push_str(&format!(" HAVING {h}"));
    }
    sql
}

async fn states(c: &mut mysql_async::Conn) -> Result<HashMap<String, (String, String)>> {
    let rs = rows(
        c,
        "SELECT name, COALESCE(mode, ''), COALESCE(detail, '') FROM _lazyview_state",
    )
    .await?;
    Ok(rs
        .into_iter()
        .map(|r| {
            (
                r[0].clone().unwrap_or_default(),
                (
                    r[1].clone().unwrap_or_default(),
                    r[2].clone().unwrap_or_default(),
                ),
            )
        })
        .collect())
}

/// The view specs of `cases`: asserts the analyzer's strategy for each (against the server's
/// catalog) and returns, per view, the (mode, detail) the engine must report.
async fn prepare(
    db: &Db,
    mariadb: bool,
    cases: Vec<Case>,
) -> Result<(Vec<ViewSpec>, HashMap<String, (String, String)>)> {
    let wired = wired();
    let mut specs = Vec::new();
    let mut engine_expect = HashMap::new();
    let mut bad = String::new();
    for c in cases {
        let kind = if c.mode == "full" {
            Kind::Full
        } else {
            Kind::Incremental
        };
        let mut spec = ViewSpec::new(c.name, kind, c.select);
        let plan = analyzer_plan(db, mariadb, &spec.definition)
            .await
            .with_context(|| format!("analyzer plan of {}", c.name))?;
        let got = mode_detail(&plan.mode);
        if got != (c.mode.to_string(), c.detail.to_string()) {
            bad.push_str(&format!(
                "\n  {}: analyzer chose {got:?}, expected ({}, {})",
                c.name, c.mode, c.detail
            ));
        }
        let expect = if wired {
            (c.mode.to_string(), c.detail.to_string())
        } else if matches!(plan.mode, Mode::Full(_)) {
            // As written: the engine's current planner names its own reason.
            ("full".to_string(), String::new())
        } else {
            spec.definition = format!("CREATE VIEW {} AS {}", c.name, flat_sql(&plan));
            let semi = plan.aliases.iter().any(|a| a.pos.0 == usize::MAX);
            if semi {
                // The current planner does not take subqueries apart.
                ("full".to_string(), "subquery".to_string())
            } else {
                got
            }
        };
        engine_expect.insert(c.name.to_string(), expect);
        specs.push(spec);
    }
    if !bad.is_empty() {
        bail!("analyzer strategies differ:{bad}");
    }
    Ok((specs, engine_expect))
}

/// Starts an engine over `specs` and checks the strategies it reports.
async fn start_checked(
    ctx: &Ctx,
    db: &Db,
    specs: &[ViewSpec],
    expect: &HashMap<String, (String, String)>,
    window_ms: &str,
) -> Result<Engine> {
    if !wired() {
        ctx.log("analyzer not wired into planning yet: the engine materializes the flattened definitions");
    }
    let (eng, _) = start_with_views(
        ctx,
        db,
        specs,
        &[
            ("LAZYVIEW_BATCH_WINDOW_MS", window_ms),
            ("LAZYVIEW_FULL_REFRESH_MS", "0"),
        ],
    )
    .await?;
    let mut c = db.conn().await?;
    let st = states(&mut c).await?;
    c.disconnect().await.ok();
    let mut bad = String::new();
    for (name, (mode, detail)) in expect {
        let got = st.get(name).cloned().unwrap_or_default();
        if got.0 != *mode || (!detail.is_empty() && got.1 != *detail) {
            bad.push_str(&format!(
                "\n  {name}: engine reports {got:?}, expected ({mode}, {detail})"
            ));
        }
    }
    if !bad.is_empty() {
        bail!("engine strategies differ:{bad}");
    }
    Ok(eng)
}

async fn run(ctx: &Ctx, label: &str, seed: u64, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_{label}")).await?;
    let ids = setup_ecommerce(&db, seed, Sizes::scaled()).await?;
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    let mut c = db.conn().await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    let mut wl = Workload::new(seed, ids.clone());
    for phase in 1..=3 {
        let n = run_batches(&mut c, &mut wl, scaled(50))
            .await
            .with_context(|| format!("{label} phase {phase} workload"))?;
        verify(
            ctx,
            &mut eng,
            &db,
            &specs,
            &format!("{label}: phase {phase} ({n} txns)"),
        )
        .await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    ctx.log(format!(
        "{label}: ops {}; metrics {}",
        wl.stats_line(),
        fmt_snapshot(&eng.snapshot())
    ));
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

// --------------------------------------------------------------------------------- edge cases

const EDGE_DDL: &[&str] = &[
    // utf8mb4_unicode_ci: case- and accent-insensitive, PAD SPACE ('abc' = 'ÁBC' = 'abc ').
    "CREATE TABLE e_parent (
       id INT NOT NULL PRIMARY KEY,
       grp INT NULL,
       code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
       amount DECIMAL(10,3) NULL,
       at DATETIME(6) NULL,
       kind ENUM('b','a','c') NULL,
       flags BIT(8) NULL,
       big BIGINT UNSIGNED NULL,
       KEY k_grp (grp), KEY k_code (code)
     )",
    "CREATE TABLE e_child (
       id INT NOT NULL PRIMARY KEY,
       parent_id INT NULL,
       code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
       qty INT NULL,
       KEY k_parent (parent_id), KEY k_code (code)
     )",
    // No primary key: duplicate rows.
    "CREATE TABLE e_bag (a INT NULL, b VARCHAR(8) NULL)",
];

const CODES: &[&str] = &[
    "'abc'", "'ABC'", "'abc '", "'ábc'", "'x'", "'X '", "NULL", "'🙂'", "'y'",
];
const AMOUNTS: &[&str] = &[
    "NULL",
    "1.5",
    "1.500",
    "0.001",
    "-2.25",
    "10",
    "999999.999",
    "0",
];
const KINDS: &[&str] = &["NULL", "'a'", "'b'", "'c'"];
const FLAGS: &[&str] = &["NULL", "b'0'", "b'11111111'", "b'1010'"];
const BIGS: &[&str] = &["NULL", "0", "18446744073709551615", "42"];
const QTYS: &[&str] = &["NULL", "-1", "0", "1", "5", "7"];

fn edge_cases() -> Vec<Case> {
    vec![
        case(
            "edge_null_join",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, parent_id, qty FROM e_child)
             SELECT p.id, p.grp, c.id AS child, c.qty FROM e_parent p LEFT JOIN c ON c.parent_id = p.id",
        ),
        case(
            "edge_null_group",
            "incremental",
            "grouped",
            "WITH t AS (SELECT grp, COUNT(*) AS n, SUM(amount) AS s FROM e_parent GROUP BY grp)
             SELECT grp, n, s FROM t WHERE n >= 1",
        ),
        case(
            "edge_distinct_nulls",
            "incremental",
            "grouped",
            "SELECT DISTINCT grp, kind FROM e_parent",
        ),
        case(
            "edge_bag_distinct",
            "incremental",
            "grouped",
            "SELECT DISTINCT a, b FROM e_bag",
        ),
        case(
            "edge_bag_cte",
            "incremental",
            "grouped",
            "WITH x AS (SELECT a, b FROM e_bag WHERE a IS NOT NULL) SELECT a, COUNT(*) AS n, COUNT(b) AS nb FROM x GROUP BY a",
        ),
        // Join keys equal under the collation; both sides projected (no representative to pick).
        case(
            "edge_ci_join",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, code, qty FROM e_child)
             SELECT p.id, p.code AS pcode, c.id AS cid, c.code AS ccode, c.qty FROM e_parent p JOIN c ON c.code = p.code",
        ),
        // Grouping under the collation; outputs that do not depend on the group's representative.
        case(
            "edge_ci_group",
            "incremental",
            "grouped",
            "SELECT COUNT(*) AS n, MIN(HEX(code)) AS lo, MAX(HEX(code)) AS hi FROM e_parent GROUP BY code",
        ),
        // DECIMAL scale, DATETIME(6), ENUM, BIGINT UNSIGNED max as unit keys.
        case(
            "edge_types_units",
            "incremental",
            "grouped",
            "SELECT DISTINCT amount, at, kind, big FROM e_parent",
        ),
        case(
            "edge_bit_group",
            "incremental",
            "grouped",
            "SELECT flags, COUNT(*) AS n FROM e_parent GROUP BY flags",
        ),
        // Rows moving between groups, key swaps, delete + reinsert.
        case(
            "edge_key_moves",
            "incremental",
            "grouped",
            "WITH c AS (SELECT id, parent_id, qty FROM e_child WHERE qty IS NOT NULL)
             SELECT parent_id, SUM(qty) AS total, COUNT(*) AS n FROM c GROUP BY parent_id",
        ),
        case(
            "edge_semi_nulls",
            "incremental",
            "spj",
            "SELECT p.id, p.code FROM e_parent p WHERE p.id IN (SELECT parent_id FROM e_child WHERE qty > 0)",
        ),
        case(
            "edge_not_exists",
            "incremental",
            "spj",
            "SELECT p.id, p.grp FROM e_parent p WHERE NOT EXISTS (SELECT 1 FROM e_child c WHERE c.parent_id = p.id)",
        ),
        // A NULL in the subquery makes NOT IN false everywhere.
        case(
            "edge_not_in_nulls",
            "incremental",
            "spj",
            "SELECT p.id FROM e_parent p WHERE p.id NOT IN (SELECT parent_id FROM e_child)",
        ),
        case(
            "edge_having",
            "incremental",
            "grouped",
            "SELECT grp, SUM(amount) AS s, COUNT(*) AS n FROM e_parent GROUP BY grp HAVING SUM(amount) > 10",
        ),
        // One CTE referenced three times, through different columns.
        case(
            "edge_self_3x",
            "incremental",
            "spj",
            "WITH p AS (SELECT id, grp, code FROM e_parent)
             SELECT a.id, b.id AS b_id, c.id AS c_id FROM p a
             JOIN p b ON b.grp = a.grp AND b.id > a.id
             JOIN p c ON c.code = a.code AND c.id <> a.id",
        ),
        case(
            "edge_nested",
            "incremental",
            "spj",
            "WITH a AS (SELECT * FROM (WITH b AS (SELECT id, grp, amount FROM e_parent) SELECT id, grp FROM b WHERE amount > 0) d)
             SELECT id, grp FROM a",
        ),
        case(
            "edge_shadow",
            "incremental",
            "spj",
            "WITH e_child AS (SELECT * FROM e_child WHERE qty > 1) SELECT c.id, c.parent_id, c.code FROM e_child c",
        ),
        // NULL-extended rows feeding the DISTINCT tuple.
        case(
            "edge_left_distinct",
            "full",
            "group key on the NULL side of a LEFT JOIN (needs internal views)",
            "SELECT DISTINCT p.grp, c.qty FROM e_parent p LEFT JOIN e_child c ON c.parent_id = p.id",
        ),
        case(
            "edge_agg_key_join",
            "incremental",
            "grouped",
            "WITH t AS (SELECT parent_id, SUM(qty) AS q, COUNT(*) AS n FROM e_child GROUP BY parent_id)
             SELECT p.id, p.code, t.q, t.n FROM e_parent p JOIN t ON t.parent_id = p.id",
        ),
    ]
}

fn pick<'a>(g: &mut Gen, xs: &[&'a str]) -> &'a str {
    xs[g.range(0, xs.len() as i64 - 1) as usize]
}

fn edge_seed() -> Vec<String> {
    let mut g = Gen::new(7);
    let mut out = Vec::new();
    for id in 1..=24 {
        out.push(format!(
            "INSERT INTO e_parent VALUES ({id}, {}, {}, {}, '2024-01-{:02} 10:00:00.{:06}', {}, {}, {})",
            ["NULL", "1", "2", "3"][id % 4],
            CODES[id % CODES.len()],
            AMOUNTS[id % AMOUNTS.len()],
            1 + id % 5,
            id * 7,
            KINDS[id % KINDS.len()],
            FLAGS[id % FLAGS.len()],
            BIGS[id % BIGS.len()],
        ));
    }
    for id in 1..=60 {
        let parent = if id % 9 == 0 {
            "NULL".to_string()
        } else {
            g.range(1, 28).to_string()
        };
        out.push(format!(
            "INSERT INTO e_child VALUES ({id}, {parent}, {}, {})",
            pick(&mut g, CODES),
            pick(&mut g, QTYS)
        ));
    }
    out.push(
        "INSERT INTO e_bag VALUES (1,'a'),(1,'a'),(1,'a'),(1,NULL),(1,NULL),(NULL,'a'),(2,'b'),(NULL,NULL),(NULL,NULL)"
            .into(),
    );
    out
}

/// Scripted transactions, each a list of statements run in one transaction.
fn edge_steps() -> Vec<(&'static str, Vec<String>)> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let mut steps = vec![
        ("move between groups", s(&[
            "UPDATE e_child SET parent_id = 2 WHERE id = 1",
            "UPDATE e_child SET parent_id = NULL WHERE id = 2",
        ])),
        ("swap two primary keys", s(&[
            "UPDATE e_child SET id = -id WHERE id IN (5, 6)",
            "UPDATE e_child SET id = 6 WHERE id = -5",
            "UPDATE e_child SET id = 5 WHERE id = -6",
        ])),
        ("swap two join keys", s(&[
            "UPDATE e_child SET parent_id = CASE id WHEN 3 THEN 4 WHEN 4 THEN 3 END WHERE id IN (3, 4)",
        ])),
        ("delete and reinsert", s(&[
            "DELETE FROM e_child WHERE id = 7",
            "INSERT INTO e_child VALUES (7, 3, 'ABC', 9)",
        ])),
        ("parent key change", s(&["UPDATE e_parent SET id = 100 WHERE id = 1"])),
        ("NULL group keys", s(&[
            "UPDATE e_parent SET grp = NULL WHERE id IN (2, 3)",
            "UPDATE e_parent SET grp = 1 WHERE grp IS NULL AND id > 20",
        ])),
        ("duplicates come and go", s(&[
            "INSERT INTO e_bag VALUES (3,'c'),(3,'c')",
            "UPDATE e_bag SET b = 'a' WHERE b = 'b'",
            "DELETE FROM e_bag WHERE a IS NULL AND b IS NULL LIMIT 1",
        ])),
        ("HAVING crossed upwards", s(&["UPDATE e_parent SET amount = 50 WHERE id = 5"])),
        ("HAVING crossed downwards", s(&["UPDATE e_parent SET amount = 0 WHERE id = 5"])),
        ("group becomes empty", s(&["DELETE FROM e_parent WHERE grp = 2"])),
        ("group refilled", s(&[
            "INSERT INTO e_parent (id, grp, code, amount) VALUES (200, 2, 'abc', 3.000), (201, 2, 'ÁBC', 4.5)",
        ])),
        ("collation-equal values move", s(&[
            "UPDATE e_parent SET code = 'ABC' WHERE code = 'x'",
            "UPDATE e_child SET code = 'abc  ' WHERE id % 5 = 0",
        ])),
        ("every child at once", s(&["UPDATE e_child SET qty = COALESCE(qty, 0) + 1"])),
        ("type edges", s(&[
            "UPDATE e_parent SET at = at + INTERVAL 1 MICROSECOND WHERE id % 2 = 0",
            "UPDATE e_parent SET kind = 'c' WHERE kind = 'a'",
            "UPDATE e_parent SET big = 18446744073709551615 WHERE big = 42",
            "UPDATE e_parent SET flags = b'11110000' WHERE flags = b'0'",
            "UPDATE e_parent SET amount = 0.001 WHERE amount = 10",
        ])),
        ("children of one parent deleted", s(&["DELETE FROM e_child WHERE parent_id = 3"])),
    ];
    // The same row hammered 30 times (each its own transaction).
    for k in 0..30 {
        steps.push((
            "hammer",
            vec![format!(
                "UPDATE e_child SET qty = qty + {} WHERE id = 9",
                k % 3
            )],
        ));
    }
    steps
}

/// Random transactions over the small value domains (collisions, NULLs, duplicates).
fn edge_random(g: &mut Gen) -> Vec<String> {
    let mut stmts = Vec::new();
    for _ in 0..g.range(1, 4) {
        stmts.push(match g.range(0, 8) {
            0 => format!(
                "INSERT INTO e_child VALUES ({}, {}, {}, {}) ON DUPLICATE KEY UPDATE parent_id = VALUES(parent_id), qty = VALUES(qty)",
                g.range(1, 80),
                if g.chance(0.15) { "NULL".into() } else { g.range(1, 30).to_string() },
                pick(g, CODES),
                pick(g, QTYS)
            ),
            1 => format!("DELETE FROM e_child WHERE id = {}", g.range(1, 80)),
            2 => format!(
                "UPDATE e_child SET parent_id = {} WHERE id = {}",
                if g.chance(0.2) { "NULL".into() } else { g.range(1, 30).to_string() },
                g.range(1, 80)
            ),
            3 => format!(
                "UPDATE e_parent SET grp = {}, code = {} WHERE id = {}",
                ["NULL", "1", "2", "3"][g.range(0, 3) as usize],
                pick(g, CODES),
                g.range(1, 30)
            ),
            4 => format!(
                "INSERT IGNORE INTO e_parent (id, grp, code, amount, kind) VALUES ({}, {}, {}, {}, {})",
                g.range(1, 40),
                ["NULL", "1", "2", "3"][g.range(0, 3) as usize],
                pick(g, CODES),
                pick(g, AMOUNTS),
                pick(g, KINDS)
            ),
            5 => format!("DELETE FROM e_parent WHERE id = {}", g.range(1, 40)),
            6 => format!(
                "INSERT INTO e_bag VALUES ({}, {})",
                ["NULL", "1", "2", "3"][g.range(0, 3) as usize],
                ["NULL", "'a'", "'b'", "'c'"][g.range(0, 3) as usize]
            ),
            7 => format!(
                "DELETE FROM e_bag WHERE a {} LIMIT 1",
                ["IS NULL", "= 1", "= 2", "= 3"][g.range(0, 3) as usize]
            ),
            _ => format!(
                "UPDATE e_parent SET amount = {} WHERE id = {}",
                pick(g, AMOUNTS),
                g.range(1, 40)
            ),
        });
    }
    stmts
}

async fn txn(c: &mut mysql_async::Conn, stmts: &[String]) -> Result<()> {
    exec(c, "START TRANSACTION").await?;
    for s in stmts {
        exec(c, s).await?;
    }
    exec(c, "COMMIT").await
}

async fn run_edge(ctx: &Ctx, label: &str, seed: u64, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_edge_{label}")).await?;
    let mut c = db.conn().await?;
    for d in EDGE_DDL {
        exec(&mut c, d).await?;
    }
    for s in edge_seed() {
        exec(&mut c, &s).await?;
    }
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), edge_cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    let mut last = "";
    for (what, stmts) in edge_steps() {
        txn(&mut c, &stmts)
            .await
            .with_context(|| what.to_string())?;
        if what == "hammer" {
            last = what;
            continue;
        }
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
        last = what;
    }
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: after {last}"),
    )
    .await?;
    let mut g = Gen::new(seed);
    for round in 1..=3 {
        for _ in 0..scaled(60) {
            txn(&mut c, &edge_random(&mut g)).await?;
        }
        verify(
            ctx,
            &mut eng,
            &db,
            &specs,
            &format!("{label}: random round {round}"),
        )
        .await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// Every taxonomy class, a random workload in two window settings and two seeds.
#[test]
fn cte_views_match_their_oracles() {
    scenario("cte", |ctx| async move {
        for (k, window) in [(0, "0"), (1, "50")] {
            let label = format!("w{window}_{k}");
            let seed = ctx.sub_seed(&label);
            run(&ctx, &label, seed, window)
                .await
                .with_context(|| format!("round {label} (sub-seed {seed})"))?;
        }
        Ok(())
    });
}

/// NULLs, duplicates without a key, key moves and swaps, collation-equal values, type edges,
/// emptied groups, HAVING thresholds and hammering, through CTEs, DISTINCT and semi-joins.
#[test]
fn cte_edge_cases_match_their_oracles() {
    scenario("cte_edge", |ctx| async move {
        for (k, window) in [(0, "0"), (1, "50")] {
            let label = format!("w{window}_{k}");
            let seed = ctx.sub_seed(&label);
            run_edge(&ctx, &label, seed, window)
                .await
                .with_context(|| format!("round {label} (sub-seed {seed})"))?;
        }
        Ok(())
    });
}

// --- foreign-key cascades (A1 / A3): the rows a cascade changes never reach the binlog.

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

/// Views whose cascades cannot be traced (A1: a NULL-side parent with a condition beyond the
/// key leaves kids of failing parents without lineage; A3: a self-referential key cascades to
/// every descendant): routed to a rebuild, which must keep them equal to the oracle.
fn cascade_rebuild_cases() -> Vec<(Case, &'static str)> {
    vec![
        (
            case("a1_left", "incremental", "spj",
                 "SELECT k.id, k.qty, p.name FROM c_kids k LEFT JOIN c_pars p ON p.id = k.par_id AND p.active = 1"),
            "k:unit,p:lineage,^c_pars:rebuild",
        ),
        (
            case("a1_chain", "incremental", "spj",
                 "SELECT o.id, k.qty, p.name FROM c_orders o LEFT JOIN c_kids k ON k.id = o.kid_id LEFT JOIN c_pars p ON p.id = k.par_id AND p.active = 1"),
            "o:unit,k:lineage,p:lineage,^c_pars:rebuild",
        ),
        (
            case("a1_set_null", "incremental", "spj",
                 "SELECT k.id, k.par_id, p.name FROM sn_kids k LEFT JOIN sn_pars p ON p.id = k.par_id AND p.active = 1"),
            "k:unit,p:lineage,^sn_pars:rebuild",
        ),
        (
            case("sn_flat", "incremental", "spj", "SELECT k.id, k.par_id, k.qty FROM sn_kids k"),
            "k:unit,^sn_pars:rebuild",
        ),
        (
            case("self_flat", "incremental", "spj", "SELECT n.id, n.parent_id, n.label FROM nodes n"),
            "n:unit,^nodes:rebuild",
        ),
        (
            case("self_join", "incremental", "spj",
                 "SELECT n.id, n.label, p.label AS plabel FROM nodes n LEFT JOIN nodes p ON p.id = n.parent_id"),
            "n:unit,p:lineage,^nodes:rebuild,^nodes:rebuild",
        ),
        (
            case("self_grp", "incremental", "grouped",
                 "SELECT n.parent_id, COUNT(*) AS n FROM nodes n GROUP BY n.parent_id"),
            "n:unit,^nodes:rebuild",
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
    rebuilds: bool,
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
    for (what, stmt) in cascade_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    let d = eng.snapshot().since(&before);
    if rebuilds {
        anyhow::ensure!(
            d.full_rebuilds >= 1,
            "{label}: untraced cascades were expected to rebuild: {}",
            fmt_snapshot(&d)
        );
    } else {
        anyhow::ensure!(
            d.full_rebuilds == 0,
            "{label}: traced cascades rebuilt the views: {}",
            fmt_snapshot(&d)
        );
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// A1 / A3: every cascade shape against its oracle: traced and unit-derived routes without a
/// rebuild; NULL-side parents with extra conditions, SET NULL keys and self-referential keys
/// through the rebuild route.
#[test]
fn cte_cascades_match_their_oracles() {
    scenario("cte_cascade", |ctx| async move {
        run_cascade(&ctx, "traced_w0", cascade_traced_cases(), false, "0").await?;
        run_cascade(&ctx, "rebuild_w0", cascade_rebuild_cases(), true, "0").await?;
        run_cascade(&ctx, "rebuild_w50", cascade_rebuild_cases(), true, "50").await?;
        Ok(())
    });
}
