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
        // L4: IN / EXISTS / NOT EXISTS / NOT IN over a join (the workload's v_big_spenders):
        // the subquery's tables are traced by direct units where they carry the correlation to
        // the unit, else by lookups (both tables of one subquery together when both changed).
        case(
            "semi_in_join",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c
             WHERE c.id IN (SELECT o.customer_id FROM orders o JOIN payments p ON p.order_id = o.id WHERE p.amount > 400)",
        ),
        case(
            "semi_exists_join_grouped",
            "incremental",
            "grouped",
            "SELECT c.tier, COUNT(*) AS n FROM customers c
             WHERE EXISTS (SELECT 1 FROM orders o JOIN payments p ON p.order_id = o.id
                           WHERE o.customer_id = c.id AND p.status = 'ok' AND p.amount > 200)
             GROUP BY c.tier",
        ),
        case(
            "semi_not_exists_join",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c
             WHERE NOT EXISTS (SELECT 1 FROM orders o JOIN order_items i ON i.order_id = o.id WHERE o.customer_id = c.id AND i.qty > 3)",
        ),
        case(
            "semi_not_in_join",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o
             WHERE o.customer_id NOT IN (SELECT c.id FROM customers c JOIN regions r ON r.id = c.region_id WHERE r.tax_rate > 0.05)",
        ),
        // JSON group keys (MySQL: bound typed by the engine and compared by the stored value;
        // MariaDB: text).
        case(
            "json_group_key",
            "incremental",
            "grouped",
            "SELECT meta, COUNT(*) AS n, MAX(id) AS last FROM customers GROUP BY meta",
        ),
        // Its text (->>) is a plain unit on every flavor; the JSON value a function computes is
        // Full (MariaDB compares it as JSON, a unit filter binds text: groups refreshed empty).
        case(
            "json_text_key",
            "incremental",
            "grouped",
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(meta, '$.lang')) AS lang, COUNT(*) AS n FROM customers
             GROUP BY JSON_UNQUOTE(JSON_EXTRACT(meta, '$.lang'))",
        ),
        case(
            "json_extract_key",
            "full",
            "group key `JSON_EXTRACT(meta, '$.lang')` is a JSON value computed by a function (MariaDB compares it as JSON, its unit filter as text): group by the column, or by its text with ->> / JSON_UNQUOTE",
            "SELECT JSON_EXTRACT(meta, '$.lang') AS lang, COUNT(*) AS n FROM customers GROUP BY JSON_EXTRACT(meta, '$.lang')",
        ),
        // A6: a scalar subquery whose value reads only outer columns is NULL where no row
        // matches (guarded after the LEFT JOIN rewrite), in the projection and in WHERE.
        case(
            "scalar_outer_only",
            "incremental",
            "spj",
            "SELECT o.id, (SELECT o.note FROM payments p WHERE p.id = o.id * 10) AS v,
                    (SELECT CONCAT(o.status, 'x') FROM payments p WHERE p.id = o.id * 10) AS w FROM orders o",
        ),
        case(
            "scalar_outer_only_where",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o WHERE (SELECT o.note FROM payments p WHERE p.id = o.id * 10) IS NULL",
        ),
        // C1: a string literal with a newline before a semi-join (the marker replacement
        // must convert multi-line token locations).
        case(
            "semi_newline_literal",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o WHERE o.note <> 'a\nb'
             AND EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 1)",
        ),
        // A5: a CTE chain whose inlined form would have 64 table references is Full with the
        // budget in the reason (the server evaluates the statement as written).
        case(
            "cte_chain_64",
            "full",
            "inlining the WITH clause reads 64 table references in one SELECT (budget 32): CTE `t5` is referenced 2 times",
            "WITH t0 AS (SELECT id, customer_id, status FROM orders)
             , t1 AS (SELECT a.id, a.customer_id, a.status FROM t0 a JOIN t0 b ON b.id = a.id)
             , t2 AS (SELECT a.id, a.customer_id, a.status FROM t1 a JOIN t1 b ON b.id = a.id)
             , t3 AS (SELECT a.id, a.customer_id, a.status FROM t2 a JOIN t2 b ON b.id = a.id)
             , t4 AS (SELECT a.id, a.customer_id, a.status FROM t3 a JOIN t3 b ON b.id = a.id)
             , t5 AS (SELECT a.id, a.customer_id, a.status FROM t4 a JOIN t4 b ON b.id = a.id)
             , t6 AS (SELECT a.id, a.customer_id, a.status FROM t5 a JOIN t5 b ON b.id = a.id)
             SELECT id, status FROM t6",
        ),
        // A4: `*` over USING / NATURAL joins expanded as the server does (shared columns once,
        // from the preserved side, then the rest): incremental, same columns as the view.
        case(
            "star_using_join",
            "incremental",
            "spj",
            "SELECT * FROM orders JOIN customers USING (id)",
        ),
        case(
            "star_natural_join",
            "incremental",
            "spj",
            "SELECT * FROM customers NATURAL JOIN regions",
        ),
        case(
            "star_right_using",
            "incremental",
            "spj",
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
            "group key on the NULL side of a LEFT JOIN",
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
            "GROUP BY in CTE `t` joined to other tables",
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
            "window function in CTE `t`",
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
            "ORDER BY … LIMIT in CTE `t`",
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
        unique_keys: vec![],
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
            "group key on the NULL side of a LEFT JOIN",
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

/// Views whose cascades cannot be traced (A1: a NULL-side parent with a condition beyond the
/// key leaves kids of failing parents without lineage; A3: a self-referential key cascades to
/// every descendant): routed to a rebuild, which must keep them equal to the oracle.
fn cascade_rebuild_cases() -> Vec<(Case, &'static str)> {
    vec![
        // C3: a semi-join over the cascading child not correlated on the unit: the parent's
        // deletes / key updates rebuild the view, the rest stays incremental.
        (
            case("casc_semi_lookup", "incremental", "spj",
                 "SELECT o.id FROM c_orders o WHERE EXISTS (SELECT 1 FROM c_kids k WHERE k.qty = o.kid_id - 4)"),
            "o:unit,k:lookup,^c_pars:rebuild",
        ),
        (
            case("casc_semi_grouped", "incremental", "grouped",
                 "SELECT p.active, COUNT(*) AS n FROM c_pars p WHERE EXISTS (SELECT 1 FROM c_kids k WHERE k.par_id = p.id) GROUP BY p.active"),
            "p:unit,k:lookup,^c_pars:rebuild",
        ),
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

// --- L1: rows chosen by ORDER BY / PARTITION BY columns that are not projected.

const ORDER_DDL: &[&str] = &[
    "CREATE TABLE l_customers (id INT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
    "CREATE TABLE l_orders (id INT NOT NULL PRIMARY KEY, customer_id INT NULL, customer VARCHAR(50) NULL,
     total DECIMAL(10,2) NOT NULL, KEY k_cust (customer_id))",
    "INSERT INTO l_customers VALUES (1,'ann'),(2,'bob'),(3,'cat'),(4,'dan')",
    "INSERT INTO l_orders VALUES (1,1,'ann',10),(2,2,'bob',5),(3,1,'ann',7),(4,3,'cat',1),(5,4,'dan',3),(6,NULL,NULL,2)",
];

/// Top-N and window views over columns that appear only in ORDER BY / PARTITION BY.
fn order_cases() -> Vec<Case> {
    vec![
        case("r_ctl", "incremental", "global", "SELECT id FROM l_orders ORDER BY total DESC LIMIT 2"),
        case("r_expr", "incremental", "global", "SELECT id FROM l_orders ORDER BY total * 2 DESC LIMIT 2"),
        case("r_neg", "incremental", "global", "SELECT id FROM l_orders ORDER BY -total LIMIT 2"),
        case("r_func", "incremental", "global", "SELECT id FROM l_orders ORDER BY ABS(total) DESC, id LIMIT 2"),
        case("r_offset", "incremental", "global", "SELECT id FROM l_orders ORDER BY total DESC LIMIT 1 OFFSET 1"),
        case("r_topcust", "incremental", "global",
             "SELECT customer_id FROM l_orders GROUP BY customer_id ORDER BY SUM(total) DESC, customer_id LIMIT 2"),
        case("r_joined", "incremental", "global",
             "SELECT o.id FROM l_orders o JOIN l_customers c ON c.id = o.customer_id ORDER BY c.name DESC, o.id LIMIT 2"),
        case("r_alias", "incremental", "global", "SELECT id, total * 2 AS t FROM l_orders ORDER BY t DESC LIMIT 2"),
        // L5: through a merged derived table / CTE, and subqueries in the ORDER BY (their
        // tables recompute the view).
        case("r_derived", "incremental", "global",
             "SELECT d.id FROM (SELECT id, total FROM l_orders) d ORDER BY d.total DESC, d.id LIMIT 2"),
        case("r_cte", "incremental", "global",
             "WITH c AS (SELECT id, total, customer_id FROM l_orders) SELECT c.id FROM c ORDER BY c.total DESC, c.id LIMIT 2"),
        case("r_grp_derived", "incremental", "global",
             "SELECT d.customer_id, COUNT(*) AS n FROM (SELECT customer_id, total FROM l_orders) d
              GROUP BY d.customer_id ORDER BY SUM(d.total) DESC, d.customer_id LIMIT 2"),
        case("r_sub_corr", "incremental", "global",
             "SELECT c.id FROM l_customers c ORDER BY (SELECT MAX(o.total) FROM l_orders o WHERE o.customer_id = c.id) DESC, c.id LIMIT 2"),
        case("r_sub_scalar", "incremental", "global",
             "SELECT o.id FROM l_orders o ORDER BY (SELECT c.name FROM l_customers c WHERE c.id = o.customer_id), o.id LIMIT 3"),
        case("r_sub_uncorr", "incremental", "global",
             "SELECT o.id FROM l_orders o ORDER BY o.total >= (SELECT MAX(o2.total) FROM l_orders o2 WHERE o2.customer_id = 1), o.id LIMIT 3"),
        case("w_part", "incremental", "grouped",
             "SELECT id, SUM(total) OVER (PARTITION BY customer_id) AS s FROM l_orders"),
        case("w_order", "incremental", "global",
             "SELECT id, ROW_NUMBER() OVER (ORDER BY total DESC, id) AS rn FROM l_orders"),
        case("w_joined", "incremental", "global",
             "SELECT o.id, RANK() OVER (ORDER BY c.name, o.id) AS rk FROM l_orders o JOIN l_customers c ON c.id = o.customer_id"),
        // L2: named windows (used by two functions, refined by another window, without
        // PARTITION BY) stay in the refresh SELECT.
        case(
            "w_named",
            "incremental",
            "grouped",
            "SELECT id, customer_id, ROW_NUMBER() OVER w AS rn, SUM(total) OVER w AS run FROM l_orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)",
        ),
        case(
            "w_named_one",
            "incremental",
            "global",
            "SELECT id, ROW_NUMBER() OVER w AS rn FROM l_orders WINDOW w AS (ORDER BY total DESC, id)",
        ),
        case(
            "w_named_ref",
            "incremental",
            "grouped",
            "SELECT id, SUM(total) OVER (w ORDER BY id) AS run FROM l_orders WINDOW w AS (PARTITION BY customer_id)",
        ),
    ]
}

/// UPDATE-only mutations: nothing is inserted or deleted, so only the read-column filter
/// decides whether the view is recomputed.
fn order_steps() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "ORDER BY column raised",
            "UPDATE l_orders SET total = 100 WHERE id = 4",
        ),
        (
            "ORDER BY column lowered",
            "UPDATE l_orders SET total = 0 WHERE id = 1",
        ),
        (
            "unrelated column",
            "UPDATE l_orders SET customer = 'bob' WHERE id = 3",
        ),
        (
            "joined ORDER BY column",
            "UPDATE l_customers SET name = 'aaa' WHERE id = 3",
        ),
        (
            "joined ORDER BY column again",
            "UPDATE l_customers SET name = 'zzz' WHERE id = 1",
        ),
        (
            "PARTITION BY column",
            "UPDATE l_orders SET customer_id = 1 WHERE id = 2",
        ),
        (
            "ORDER BY column of several rows",
            "UPDATE l_orders SET total = total + 50 WHERE id IN (5, 6)",
        ),
    ]
}

async fn run_order(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_order_{label}")).await?;
    let mut c = db.conn().await?;
    for s in ORDER_DDL {
        exec(&mut c, s).await?;
    }
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), order_cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    for (what, stmt) in order_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// L1: an UPDATE of a column that appears only in ORDER BY (plain, expression, aggregate input,
/// OFFSET, joined alias) or in a window's PARTITION BY / ORDER BY reaches the recompute.
#[test]
fn cte_order_columns_match_their_oracles() {
    scenario("cte_order", |ctx| async move {
        run_order(&ctx, "w0", "0").await?;
        run_order(&ctx, "w50", "50").await?;
        Ok(())
    });
}

// --- server schema: UNIQUE NOT NULL keys and generated columns.

const SCHEMA_DDL: &[&str] = &[
    // No primary key: `code` and (a, b) identify rows; g is VIRTUAL, s STORED.
    "CREATE TABLE sk_items (code VARCHAR(10) NOT NULL, a INT NOT NULL, b INT NOT NULL, note VARCHAR(40) NULL,
     g INT GENERATED ALWAYS AS (a + b) VIRTUAL, s INT GENERATED ALWAYS AS (a * 2) STORED,
     UNIQUE KEY u_code (code), UNIQUE KEY u_ab (a, b), KEY k_a (a))",
    "CREATE TABLE sk_orders (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) NULL, qty INT NOT NULL)",
    "INSERT INTO sk_items (code, a, b, note) VALUES ('x1',1,10,'one'),('x2',2,20,NULL),('x3',3,30,'three'),('x4',4,40,'four')",
    "INSERT INTO sk_orders VALUES (1,'x1',5),(2,'x2',6),(3,'x1',7),(4,NULL,8),(5,'x9',9)",
];

fn schema_cases() -> Vec<(Case, &'static str)> {
    vec![
        // PK-less anchor with a unique key: units by the key, not by every referenced column.
        (
            case("sk_flat", "incremental", "spj", "SELECT note, a, g FROM sk_items WHERE s > 2"),
            "sk_items:unit",
        ),
        // A PK-less lookup alias traced by its unique key.
        (
            case("sk_join", "incremental", "spj",
                 "SELECT o.id, o.qty, i.note, i.g FROM sk_orders o LEFT JOIN sk_items i ON i.code = o.code"),
            "o:unit,i:lineage",
        ),
        // DISTINCT over a unique key and other columns reduces to the key.
        (
            case("sk_distinct", "incremental", "grouped", "SELECT DISTINCT code, note, s FROM sk_items"),
            "sk_items:unit",
        ),
        (
            case("sk_distinct_ab", "incremental", "grouped", "SELECT DISTINCT a, b, note, g FROM sk_items"),
            "sk_items:unit",
        ),
        // IN over a UNIQUE NOT NULL key (no PK): an inner join, the key traces it.
        (
            case("sk_semi_unique", "incremental", "spj",
                 "SELECT o.id, o.qty FROM sk_orders o WHERE o.code IN (SELECT code FROM sk_items WHERE a > 1)"),
            "o:unit,sk_items:lineage",
        ),
        // NOT IN over columns that are never NULL: NOT EXISTS (no all-units NULL trace).
        (
            case("sk_not_in", "incremental", "spj",
                 "SELECT i.code, i.a FROM sk_items i WHERE i.a NOT IN (SELECT o.qty FROM sk_orders o)"),
            "i:unit,o:lookup",
        ),
        // Grouping by a generated column: its base columns are read columns.
        (
            case("sk_by_gen", "incremental", "grouped", "SELECT g, COUNT(*) AS n, SUM(s) AS t FROM sk_items GROUP BY g"),
            "sk_items:lookup",
        ),
    ]
}

fn schema_steps() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "base column of a generated column",
            "UPDATE sk_items SET a = 11 WHERE code = 'x1'",
        ),
        (
            "other base column",
            "UPDATE sk_items SET b = 21 WHERE code = 'x2'",
        ),
        (
            "non-key column",
            "UPDATE sk_items SET note = 'two' WHERE code = 'x2'",
        ),
        (
            "unique key changed",
            "UPDATE sk_items SET code = 'x5' WHERE code = 'x3'",
        ),
        (
            "row inserted and one deleted",
            "INSERT INTO sk_items (code, a, b, note) VALUES ('x9', 9, 90, 'nine')",
        ),
        ("row deleted", "DELETE FROM sk_items WHERE code = 'x4'"),
        (
            "order re-pointed",
            "UPDATE sk_orders SET code = 'x5' WHERE id = 4",
        ),
        (
            "NOT IN key now present",
            "UPDATE sk_orders SET qty = 2 WHERE id = 1",
        ),
        (
            "NOT IN key gone",
            "UPDATE sk_orders SET qty = 50 WHERE id = 1",
        ),
    ]
}

async fn run_schema(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_schema_{label}")).await?;
    let mut c = db.conn().await?;
    for s in SCHEMA_DDL {
        exec(&mut c, s).await?;
    }
    let mariadb = ctx.server().is_mariadb();
    let mut bad = String::new();
    let mut plain = Vec::new();
    for (case, tracing) in schema_cases() {
        let def = format!("CREATE VIEW {} AS {}", case.name, case.select);
        let plan = analyzer_plan(&db, mariadb, &def).await?;
        if plan.tracing() != *tracing {
            bad.push_str(&format!(
                "\n  {}: routes {}, expected {tracing}",
                case.name,
                plan.tracing()
            ));
        }
        if case.name == "sk_flat" && plan.unit_exprs != vec!["`sk_items`.`code`"] {
            bad.push_str(&format!("\n  sk_flat: units {:?}", plan.unit_exprs));
        }
        if case.name == "sk_distinct" && plan.unit_exprs != vec!["code"] {
            bad.push_str(&format!("\n  sk_distinct: units {:?}", plan.unit_exprs));
        }
        if case.name == "sk_not_in"
            && !plan
                .where_sql
                .as_deref()
                .is_some_and(|w| w.contains("NOT EXISTS") && !w.contains("IS NULL"))
        {
            bad.push_str(&format!("\n  sk_not_in: WHERE {:?}", plan.where_sql));
        }
        if case.name == "sk_distinct_ab" && plan.unit_exprs != vec!["a", "b"] {
            bad.push_str(&format!("\n  sk_distinct_ab: units {:?}", plan.unit_exprs));
        }
        plain.push(case);
    }
    if !bad.is_empty() {
        bail!("schema plans differ:{bad}");
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
    for (what, stmt) in schema_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// Server schema: UNIQUE NOT NULL keys identify rows of PK-less tables (units, lineage, DISTINCT
/// reduction); generated columns follow their base columns (VIRTUAL ones have no binlog value).
#[test]
fn cte_schema_keys_match_their_oracles() {
    scenario("cte_schema", |ctx| async move {
        run_schema(&ctx, "w0", "0").await?;
        run_schema(&ctx, "w50", "50").await?;
        Ok(())
    });
}

// --- B1 / B2 / B3: VIRTUAL generated columns (no value in the row images).

const VIRTUAL_DDL: &[&str] = &[
    "CREATE TABLE g1 (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NULL,
     name VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, dt DATETIME NULL, dt6 DATETIME(6) NULL,
     doc JSON NULL,
     gi INT AS (a / 3) VIRTUAL, gd DECIMAL(6,1) AS (a / 3) VIRTUAL, gs INT AS (a / 3) STORED,
     gbin VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin AS (name) VIRTUAL,
     gdate DATE AS (dt) VIRTUAL, gdate6 DATE AS (dt6) VIRTUAL, gchain INT AS (gi * 10) VIRTUAL,
     gsum INT AS (a + IFNULL(b, 0)) VIRTUAL, gsc INT AS (gsum * 10) VIRTUAL,
     jname VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin AS (JSON_UNQUOTE(JSON_EXTRACT(doc, '$.name'))) VIRTUAL)",
    "CREATE TABLE r (id INT NOT NULL PRIMARY KEY, w INT NULL)",
    "INSERT INTO g1 (id, a, b, name, dt, dt6, doc) VALUES
     (1, 1, 2, 'Ann', '2026-01-01 23:59:59', '2026-01-01 23:59:59.700000', '{\"name\": \"x\"}'),
     (2, 2, NULL, 'ann', '2026-01-02 00:00:01', '2026-01-02 00:00:00.200000', '{\"name\": \"y\"}'),
     (3, 5, 1, 'Bob', NULL, NULL, NULL),
     (4, 3, 3, 'bob', '2026-01-01 00:00:00', '2026-01-01 12:00:00.500000', '{\"name\": \"x\"}')",
    "INSERT INTO r VALUES (10, 2), (20, 4), (30, 6), (40, 8), (50, NULL)",
];

fn virtual_cases() -> Vec<Case> {
    vec![
        // B1: the value converted to the declared type and collation, exactly as the server
        // reads the column.
        case("vt_proj_int", "incremental", "spj", "SELECT id, gi FROM g1"),
        case(
            "vt_where_int",
            "incremental",
            "spj",
            "SELECT id, a FROM g1 WHERE gi = 0",
        ),
        case(
            "vt_group_int",
            "incremental",
            "grouped",
            "SELECT gi, COUNT(*) AS n FROM g1 GROUP BY gi",
        ),
        case("vt_proj_dec", "incremental", "spj", "SELECT id, gd FROM g1"),
        case(
            "vt_group_bin",
            "incremental",
            "grouped",
            "SELECT gbin, COUNT(*) AS n FROM g1 GROUP BY gbin",
        ),
        case(
            "vt_distinct_bin",
            "incremental",
            "grouped",
            "SELECT DISTINCT gbin FROM g1",
        ),
        case(
            "vt_where_date",
            "incremental",
            "spj",
            "SELECT id FROM g1 WHERE gdate = '2026-01-01'",
        ),
        case(
            "vt_group_date",
            "incremental",
            "grouped",
            "SELECT gdate, COUNT(*) AS n FROM g1 GROUP BY gdate",
        ),
        // DATE from DATETIME(6): rounded on MySQL, truncated on MariaDB (each as its server).
        case(
            "vt_group_date6",
            "incremental",
            "grouped",
            "SELECT gdate6, COUNT(*) AS n FROM g1 GROUP BY gdate6",
        ),
        case(
            "vt_join_eq",
            "incremental",
            "spj",
            "SELECT r.id, g.id AS gid FROM r JOIN g1 g ON g.gi = r.w",
        ),
        case(
            "vt_chain_group",
            "incremental",
            "grouped",
            "SELECT gchain, COUNT(*) AS n FROM g1 GROUP BY gchain",
        ),
        case(
            "vt_topn",
            "incremental",
            "global",
            "SELECT id, gi FROM g1 ORDER BY gi DESC, id LIMIT 2",
        ),
        case(
            "vt_stored_ctl",
            "incremental",
            "spj",
            "SELECT id, gs FROM g1 WHERE gs >= 1",
        ),
        // Unaliased: named after its text, grouped by the same inlined expression.
        case(
            "vt_group_unaliased",
            "incremental",
            "grouped",
            "SELECT gi + 1, COUNT(*) AS n FROM g1 GROUP BY gi + 1",
        ),
        // B2: nested columns, window partitions and subqueries.
        case(
            "vl_chain_group",
            "incremental",
            "grouped",
            "SELECT gsc, COUNT(*) AS n FROM g1 GROUP BY gsc",
        ),
        case(
            "vl_window",
            "incremental",
            "grouped",
            "SELECT id, SUM(a) OVER (PARTITION BY gsum) AS s FROM g1",
        ),
        case(
            "vl_in_inner",
            "incremental",
            "spj",
            "SELECT r.id, r.w FROM r WHERE r.w IN (SELECT g.gsum FROM g1 g)",
        ),
        case(
            "vl_exists_inner",
            "incremental",
            "spj",
            "SELECT r.id, r.w FROM r WHERE EXISTS (SELECT 1 FROM g1 g WHERE g.gsum = r.w)",
        ),
        case(
            "vl_not_in_inner",
            "incremental",
            "spj",
            "SELECT r.id, r.w FROM r WHERE r.w NOT IN (SELECT g.gsum FROM g1 g)",
        ),
        case(
            "vl_group_sum",
            "incremental",
            "grouped",
            "SELECT gsum, COUNT(*) AS n FROM g1 GROUP BY gsum",
        ),
        // B3: a string literal in the expression (MySQL prints it escaped in the catalog).
        case(
            "vj_group",
            "incremental",
            "grouped",
            "SELECT jname, COUNT(*) AS n FROM g1 GROUP BY jname",
        ),
    ]
}

fn virtual_steps() -> Vec<(&'static str, &'static str)> {
    vec![
        ("base column of gi / gchain / gsum", "UPDATE g1 SET a = 3 WHERE id = 1"),
        ("collation-only change of gbin", "UPDATE g1 SET name = 'ANN' WHERE id = 2"),
        ("datetime of gdate", "UPDATE g1 SET dt = '2026-01-01 12:00:00' WHERE id = 3"),
        ("other base column of gsum", "UPDATE g1 SET b = 5 WHERE id = 2"),
        ("row deleted", "DELETE FROM g1 WHERE id = 4"),
        (
            "row inserted",
            "INSERT INTO g1 (id, a, b, name, dt, dt6, doc) VALUES
             (5, 4, 4, 'Cat', '2026-03-01 10:00:00', '2026-03-01 23:59:59.600000', '{\"name\": \"z\"}')",
        ),
        ("every row", "UPDATE g1 SET a = a + 1"),
        ("fraction of gdate6", "UPDATE g1 SET dt6 = '2026-01-01 00:00:00.400000' WHERE id = 1"),
        ("JSON of jname", "UPDATE g1 SET doc = '{\"name\": \"y\"}' WHERE id = 5"),
        ("outer row", "UPDATE r SET w = 3 WHERE id = 10"),
    ]
}

async fn run_virtual(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_virtual_{label}")).await?;
    let mut c = db.conn().await?;
    for s in VIRTUAL_DDL {
        exec(&mut c, s).await?;
    }
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), virtual_cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    for (what, stmt) in virtual_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// B1 / B2 / B3: VIRTUAL generated columns are inlined with their declared type and collation
/// (nested ones, window partitions and subqueries included), from the initial build on.
#[test]
fn cte_virtual_columns_match_their_oracles() {
    scenario("cte_virtual", |ctx| async move {
        run_virtual(&ctx, "w0", "0").await?;
        run_virtual(&ctx, "w50", "50").await?;
        Ok(())
    });
}

// --- F1 / F2 / F4: integer VIRTUAL columns at the edges of their range, nesting fan-out.

const VIRTUAL_INT_DDL: &[&str] = &[
    // Values as stored: the rows are written in a non-strict sql_mode (strict mode refuses them).
    "SET SESSION sql_mode = ''",
    "CREATE TABLE vu (id INT NOT NULL PRIMARY KEY, u BIGINT UNSIGNED NULL, cat INT NULL,
     gi INT AS (u) VIRTUAL, giu INT UNSIGNED AS (u) VIRTUAL, gb BIGINT AS (u) VIRTUAL, gsum INT AS (u + 0) VIRTUAL)",
    "INSERT INTO vu (id, u, cat) VALUES (1, 5, 1), (2, 18446744073709551615, 1), (3, 9223372036854775808, 2),
     (4, 2147483648, 2), (5, NULL, 3)",
    "CREATE TABLE vd (id INT NOT NULL PRIMARY KEY, dbl DOUBLE NULL,
     gr INT AS (ROUND(dbl, 1)) VIRTUAL, gn BIGINT AS (-dbl) VIRTUAL, gm INT AS (dbl * 2) VIRTUAL)",
    "INSERT INTO vd (id, dbl) VALUES (1, 2.5), (2, -3.5), (3, NULL)",
    // 8-way fan-out per level: h2 inlines 64 base-column references, h6 262144.
    "CREATE TABLE vg (id INT NOT NULL PRIMARY KEY, a BIGINT NULL, h0 BIGINT AS (a) VIRTUAL,
     h1 BIGINT AS (GREATEST(h0, h0, h0, h0, h0, h0, h0, h0)) VIRTUAL,
     h2 BIGINT AS (GREATEST(h1, h1, h1, h1, h1, h1, h1, h1)) VIRTUAL,
     h3 BIGINT AS (GREATEST(h2, h2, h2, h2, h2, h2, h2, h2)) VIRTUAL,
     h4 BIGINT AS (GREATEST(h3, h3, h3, h3, h3, h3, h3, h3)) VIRTUAL,
     h5 BIGINT AS (GREATEST(h4, h4, h4, h4, h4, h4, h4, h4)) VIRTUAL,
     h6 BIGINT AS (GREATEST(h5, h5, h5, h5, h5, h5, h5, h5)) VIRTUAL)",
    "INSERT INTO vg (id, a) VALUES (1, 10), (2, NULL), (3, -4)",
];

fn virtual_int_cases() -> Vec<Case> {
    vec![
        // F1: an unsigned value >= 2^63 clips like the column (CAST AS SIGNED would wrap).
        case("vu_proj", "incremental", "spj", "SELECT id, gi, giu, gb, gsum FROM vu"),
        case("vu_where", "incremental", "spj", "SELECT id FROM vu WHERE gi = 2147483647"),
        case("vu_group", "incremental", "grouped", "SELECT gi, COUNT(*) AS n FROM vu GROUP BY gi"),
        case(
            "vu_sum",
            "incremental",
            "grouped",
            "SELECT cat, SUM(gb) AS s, MIN(giu) AS m FROM vu GROUP BY cat",
        ),
        // F2: a DOUBLE function result out of BIGINT range clips (MySQL's CAST would raise 1690).
        case("vd_round", "incremental", "spj", "SELECT id, gr FROM vd"),
        case("vd_neg", "incremental", "spj", "SELECT id, gn FROM vd"),
        case("vd_group", "incremental", "grouped", "SELECT gm, COUNT(*) AS n FROM vd GROUP BY gm"),
        // F4: within the budget the nesting is inlined, past it Full naming the column (fast).
        case("vg_h2", "incremental", "spj", "SELECT id, h2 FROM vg"),
        case(
            "vg_h6",
            "full",
            "VIRTUAL generated column `vg`.`h6` expands to 262144 base-column references (budget 256): its nested VIRTUAL columns are copied per reference",
            "SELECT id, h6 FROM vg",
        ),
    ]
}

fn virtual_int_steps() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "u to 2^63 - 1",
            "UPDATE vu SET u = 9223372036854775807 WHERE id = 3",
        ),
        (
            "u past 2^63",
            "UPDATE vu SET u = 9223372036854775809 WHERE id = 3",
        ),
        (
            "u near 2^64 inserted",
            "INSERT INTO vu (id, u, cat) VALUES (6, 18446744073709551614, 3)",
        ),
        ("u max deleted", "DELETE FROM vu WHERE id = 2"),
        ("double 1e300", "INSERT INTO vd (id, dbl) VALUES (4, 1e300)"),
        ("double -1e19", "INSERT INTO vd (id, dbl) VALUES (5, -1e19)"),
        ("both deleted", "DELETE FROM vd WHERE id IN (4, 5)"),
        ("double 7.25", "INSERT INTO vd (id, dbl) VALUES (6, 7.25)"),
        ("nested base column", "UPDATE vg SET a = 7 WHERE id = 1"),
    ]
}

async fn run_virtual_int(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_vint_{label}")).await?;
    let mut c = db.conn().await?;
    for s in VIRTUAL_INT_DDL {
        exec(&mut c, s).await?;
    }
    let started = std::time::Instant::now();
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), virtual_int_cases()).await?;
    anyhow::ensure!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "planning took {:?}",
        started.elapsed()
    );
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    for (what, stmt) in virtual_int_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// F1 / F2 / F4: integer VIRTUAL columns over unsigned values >= 2^63 and out-of-range DOUBLE
/// function results clip like the column; a nested fan-out past the budget is Full, quickly.
#[test]
fn cte_virtual_integers_match_their_oracles() {
    scenario("cte_vint", |ctx| async move {
        run_virtual_int(&ctx, "w0", "0").await?;
        run_virtual_int(&ctx, "w50", "50").await?;
        Ok(())
    });
}

// --- F3: no alias capture between a semi-join subquery and merged outer aliases.

const CAPTURE_DDL: &[&str] = &[
    "CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, name VARCHAR(20) NOT NULL, ref INT NULL)",
    "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, customer_id INT NULL, total DECIMAL(10,2) NOT NULL)",
    "CREATE TABLE payments (id INT NOT NULL PRIMARY KEY, order_id INT NULL, amount DECIMAL(10,2) NOT NULL)",
    "INSERT INTO customers VALUES (1,'ann',2),(2,'bob',NULL),(3,'cy',1),(4,'dee',4)",
    "INSERT INTO orders VALUES (10,1,100),(11,1,700),(20,2,450),(30,3,50),(31,3,300),(40,4,900)",
    "INSERT INTO payments VALUES (100,10,100),(110,11,500),(200,20,450),(300,30,50),(310,31,300)",
];

/// (name, detail, template): `{D}` derived / CTE alias, `{I}` its inner alias, `{S}` / `{T}` the
/// subquery's aliases.
const CAPTURE_TEMPLATES: &[(&str, &str, &str)] = &[
    (
        "exists_join",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS
         (SELECT 1 FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.amount > 400)",
    ),
    (
        "not_exists_join",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE NOT EXISTS
         (SELECT 1 FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.amount > 400)",
    ),
    (
        "cte_exists_join",
        "spj",
        "WITH {D} AS (SELECT {I}.id, {I}.customer_id FROM orders {I}) SELECT {D}.id FROM {D} WHERE EXISTS
         (SELECT 1 FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.amount > 400)",
    ),
    (
        "exists_self",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.ref FROM customers {I}) {D} WHERE EXISTS
         (SELECT 1 FROM customers {S} WHERE {S}.ref = {D}.id)",
    ),
    (
        "exists_other",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS
         (SELECT 1 FROM payments {S} WHERE {S}.order_id = {D}.id AND {S}.amount > 400)",
    ),
    (
        "exists_other_grouped",
        "grouped",
        "SELECT {D}.customer_id, COUNT(*) AS n FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS
         (SELECT 1 FROM payments {S} WHERE {S}.order_id = {D}.id AND {S}.amount > 400) GROUP BY {D}.customer_id",
    ),
    (
        "in_join",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE {D}.customer_id IN
         (SELECT {S}.customer_id FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {T}.amount > 400)",
    ),
    (
        "nested_derived",
        "spj",
        "SELECT {D}.id FROM (SELECT e.id, e.customer_id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) e) {D}
         WHERE EXISTS (SELECT 1 FROM payments {S} WHERE {S}.order_id = {D}.id AND {S}.amount > 400)",
    ),
];

/// The reviewer's aliases (`d`, `o`, `o`, `p`), a control without collisions, and seeded random
/// renamings from a small pool (collisions are likely).
fn capture_cases() -> Vec<Case> {
    let pool = ["o", "d", "p", "s", "x", "O", "e2"];
    let mut seed: u64 = std::env::var("LAZYVIEW_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xf3f3);
    let mut pick = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        pool[(seed % pool.len() as u64) as usize]
    };
    let mut out = Vec::new();
    for (name, detail, t) in CAPTURE_TEMPLATES {
        let mut variants = vec![("d", "o", "o", "p"), ("x", "o", "s", "q")];
        while variants.len() < 5 {
            let v = (pick(), pick(), pick(), pick());
            if !v.2.eq_ignore_ascii_case(v.3) && !v.0.eq_ignore_ascii_case("e") {
                variants.push(v);
            }
        }
        for (k, (d, i, s, x)) in variants.into_iter().enumerate() {
            let select = t
                .replace("{D}", d)
                .replace("{I}", i)
                .replace("{S}", s)
                .replace("{T}", x);
            let name: &'static str = Box::leak(format!("cap_{name}_{k}").into_boxed_str());
            let select: &'static str = Box::leak(select.into_boxed_str());
            out.push(case(name, "incremental", detail, select));
        }
    }
    out
}

fn capture_steps() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "payment over the threshold",
            "UPDATE payments SET amount = 401 WHERE id = 310",
        ),
        (
            "payment under the threshold",
            "UPDATE payments SET amount = 1 WHERE id = 110",
        ),
        (
            "order moved",
            "UPDATE orders SET customer_id = 2 WHERE id = 31",
        ),
        ("payment deleted", "DELETE FROM payments WHERE id = 200"),
        (
            "payment added",
            "INSERT INTO payments VALUES (400, 40, 900)",
        ),
        (
            "referrer changed",
            "UPDATE customers SET ref = 3 WHERE id = 2",
        ),
    ]
}

async fn run_capture(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_capture_{label}")).await?;
    let mut c = db.conn().await?;
    for s in CAPTURE_DDL {
        exec(&mut c, s).await?;
    }
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), capture_cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    for (what, stmt) in capture_steps() {
        exec(&mut c, stmt).await.with_context(|| what.to_string())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// F3: correlated subqueries over a merged derived table / CTE keep their meaning whatever the
/// aliases (the reviewer's colliding ones, a control, seeded random renamings).
#[test]
fn cte_semi_joins_never_capture_aliases() {
    scenario("cte_capture", |ctx| async move {
        run_capture(&ctx, "w0", "0").await?;
        run_capture(&ctx, "w50", "50").await?;
        Ok(())
    });
}

// --- F5: NOT IN through re-executed refresh statements (MariaDB re-uses a stale NOT IN result
// of a prepared statement after the subquery flips between empty and non-empty).

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
fn not_in_steps() -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    for round in 1..=5 {
        out.push((
            format!("round {round}: orders empty"),
            "UPDATE orders SET total = 1 WHERE id = 20",
        ));
        out.push((
            format!("round {round}: payments empty"),
            "UPDATE payments SET amount = 1 WHERE id = 200",
        ));
        out.push((
            format!("round {round}: orders non-empty"),
            "UPDATE orders SET total = 401 WHERE id = 20",
        ));
        out.push((
            format!("round {round}: payments non-empty"),
            "UPDATE payments SET amount = 450 WHERE id = 200",
        ));
    }
    out.push((
        "a NULL key enters".into(),
        "INSERT INTO orders VALUES (21, NULL, 500)",
    ));
    out.push((
        "the NULL key leaves".into(),
        "DELETE FROM orders WHERE id = 21",
    ));
    out
}

async fn run_not_in(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_nin_{label}")).await?;
    let mut c = db.conn().await?;
    for s in NOT_IN_DDL {
        exec(&mut c, s).await?;
    }
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), not_in_cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    for (what, stmt) in not_in_steps() {
        exec(&mut c, stmt).await.with_context(|| what.clone())?;
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

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
    let st = c.prep(sql).await?;
    let _: Vec<i64> = c.exec(&st, ()).await?;
    exec(&mut c, "UPDATE t2 SET amount = 401").await?;
    let again: Vec<i64> = c.exec(&st, ()).await?;
    let fresh: Vec<i64> = c.query(sql).await?;
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
        run_not_in(&ctx, "w0", "0").await?;
        run_not_in(&ctx, "w50", "50").await?;
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

const KEY_EXACT_DDL: &[&str] = &[
    "CREATE TABLE c (id INT NOT NULL PRIMARY KEY, num INT NOT NULL, name VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, ref INT NULL)",
    "CREATE TABLE s_str (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "CREATE TABLE s_spk (code VARCHAR(10) NOT NULL PRIMARY KEY, v INT NULL)",
    "CREATE TABLE s_bin (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "CREATE TABLE s_int (id INT NOT NULL PRIMARY KEY, code INT NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "INSERT INTO c VALUES (1,1,'abc',NULL),(2,2,'xyz',5),(3,7,'ABD',7)",
    "INSERT INTO s_str VALUES (1,'1',10),(2,'01',20),(3,'abc',30),(4,'7',40)",
    "INSERT INTO s_spk VALUES ('1',10),('01',20),('7',40)",
    "INSERT INTO s_bin VALUES (1,'abc',1),(2,'ABC',2),(3,'xyz',3)",
    "INSERT INTO s_int VALUES (1,1,10),(2,7,70)",
    // G4: TIMESTAMP keys (the fall-back hour of a DST time zone; the test servers run in UTC).
    "CREATE TABLE c_t (id INT NOT NULL PRIMARY KEY, dt DATETIME NOT NULL, ts TIMESTAMP NULL)",
    "CREATE TABLE k_ts (id INT NOT NULL PRIMARY KEY, k TIMESTAMP NOT NULL, v INT NULL, UNIQUE KEY u_k (k))",
    "CREATE TABLE k_dt (id INT NOT NULL PRIMARY KEY, k DATETIME NOT NULL, v INT NULL, UNIQUE KEY u_k (k))",
    "INSERT INTO c_t VALUES (1,'2026-01-01 00:00:00','2026-01-01 00:00:00'),(2,'2026-01-02 10:00:00',NULL),(3,'2026-11-01 01:30:00','2026-11-01 01:30:00')",
    "INSERT INTO k_ts VALUES (1,'2026-01-01 00:00:00',1),(2,'2026-11-01 01:30:00',2)",
    "INSERT INTO k_dt VALUES (1,'2026-01-01 00:00:00',1),(2,'2026-01-02 10:00:00',2)",
];

/// The final verifier's repros, with the routes the analyzer must choose: a string key against
/// a number (`'1'`, `'01'` and `'1.0'` all equal 1), under an explicit weaker collation (G1) or
/// a TIMESTAMP on either side (G4) matches several key rows, so an inner join would repeat the
/// outer row: semi-join aliases (lookups). INT and DATETIME keys are the controls (joined).
fn key_exact_cases() -> Vec<(Case, &'static str)> {
    vec![
        (
            case(
                "dup_uq_in_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_str s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dup_uq_exists_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_str s WHERE s.code = c.num)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dup_pk_in_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_spk s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dup_uq_in_collate",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.name COLLATE utf8mb4_general_ci IN (SELECT b.code FROM s_bin b)",
            ),
            "c:unit,b:lookup",
        ),
        (
            case(
                "ctl_uq_in_int",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_int s)",
            ),
            "c:unit,s:lineage",
        ),
        (
            case(
                "ts_in_dt",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.dt IN (SELECT s.k FROM k_ts s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "ts_exists_dt",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE EXISTS (SELECT 1 FROM k_ts s WHERE s.k = c.dt)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "ts_in_ts",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.ts IN (SELECT s.k FROM k_ts s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dt_in_ts",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.ts IN (SELECT s.k FROM k_dt s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "ctl_dt_in_dt",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.dt IN (SELECT s.k FROM k_dt s)",
            ),
            "c:unit,s:lineage",
        ),
    ]
}

const KEY_EXACT_STEPS: &[(&str, &[&str])] = &[
    (
        "a third numeric spelling",
        &[
            "INSERT INTO s_str VALUES (5,'1.0',50)",
            "INSERT INTO s_spk VALUES ('1.0',50)",
        ],
    ),
    (
        "key changed by UPDATE",
        &[
            "UPDATE s_str SET code = '8' WHERE id = 2",
            "UPDATE s_bin SET code = 'abd' WHERE id = 2",
            "UPDATE k_ts SET k = '2026-01-02 10:00:00' WHERE id = 2",
            "UPDATE k_dt SET k = '2026-11-01 01:30:00' WHERE id = 1",
        ],
    ),
];

async fn run_key_exact(ctx: &Ctx, label: &str, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_keyx_{label}")).await?;
    let mut c = db.conn().await?;
    for s in KEY_EXACT_DDL {
        exec(&mut c, s).await?;
    }
    let mariadb = ctx.server().is_mariadb();
    let mut bad = String::new();
    let mut plain = Vec::new();
    for (case, tracing) in key_exact_cases() {
        let def = format!("CREATE VIEW {} AS {}", case.name, case.select);
        let plan = analyzer_plan(&db, mariadb, &def).await?;
        if plan.tracing() != tracing {
            bad.push_str(&format!(
                "\n  {}: routes {}, expected {tracing}",
                case.name,
                plan.tracing()
            ));
        }
        plain.push(case);
    }
    if !bad.is_empty() {
        bail!("key-exact plans differ:{bad}");
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
    for (what, stmts) in KEY_EXACT_STEPS {
        for s in *stmts {
            exec(&mut c, s).await.with_context(|| what.to_string())?;
        }
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// G1 / G4: semi-joins over string keys compared with numbers or under a weaker COLLATE, and
/// over TIMESTAMP keys or against TIMESTAMP columns, stay semi-joins (no repeated outer rows);
/// the INT and DATETIME keys still join.
#[test]
fn cte_key_exact_semi_joins_match_their_oracles() {
    scenario("cte_keyx", |ctx| async move {
        run_key_exact(&ctx, "w0", "0").await?;
        run_key_exact(&ctx, "w50", "50").await?;
        Ok(())
    });
}

// --- G2 / G3: comments before CREATE VIEW, a byte-order mark, a bare SELECT without a name.

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
