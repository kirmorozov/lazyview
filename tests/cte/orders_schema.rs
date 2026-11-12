use super::*;

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
fn order_steps() -> Steps {
    steps(&[
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
    ])
}

const SUITE_ORDER: Suite<'static> = Suite {
    tag: "order",
    ddl: ORDER_DDL,
    cases: |_| Cases::Plain(order_cases()),
    steps: order_steps,
    plans: None,
    plan_budget: None,
};

/// L1: an UPDATE of a column that appears only in ORDER BY (plain, expression, aggregate input,
/// OFFSET, joined alias) or in a window's PARTITION BY / ORDER BY reaches the recompute.
#[test]
fn cte_order_columns_match_their_oracles() {
    scenario("cte_order", |ctx| async move {
        run_suite(&ctx, &SUITE_ORDER, "w0", "0").await?;
        run_suite(&ctx, &SUITE_ORDER, "w50", "50").await?;
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

fn schema_steps() -> Steps {
    steps(&[
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
    ])
}

/// The schema suite's assertions on a case's plan: the units a PK-less table is identified by
/// (its UNIQUE NOT NULL keys) and the NULL-aware form of NOT IN. The complaint for a case whose
/// plan differs, `None` when the plan is right.
fn schema_plan_check(db: &Db, case: &Case, plan: &ViewPlan) -> Option<String> {
    let units = |want: &[&str]| {
        (!same_texts(db, &plan.unit_exprs, want))
            .then(|| format!("\n  {}: units {:?}", case.name, plan.unit_exprs))
    };
    match case.name {
        "sk_flat" => units(&["`sk_items`.`code`"]),
        "sk_distinct" => units(&["code"]),
        "sk_distinct_ab" => units(&["a", "b"]),
        "sk_not_in" => {
            let rewritten = plan
                .where_sql
                .as_deref()
                .is_some_and(|w| w.contains("NOT EXISTS") && !w.contains("IS NULL"));
            (!rewritten).then(|| format!("\n  {}: WHERE {:?}", case.name, plan.where_sql))
        }
        _ => None,
    }
}

const SUITE_SCHEMA: Suite<'static> = Suite {
    tag: "schema",
    ddl: SCHEMA_DDL,
    cases: |_| Cases::Traced(schema_cases()),
    steps: schema_steps,
    plans: Some(Plans {
        what: "schema plans",
        check: schema_plan_check,
    }),
    plan_budget: None,
};

/// Server schema: UNIQUE NOT NULL keys identify rows of PK-less tables (units, lineage, DISTINCT
/// reduction); generated columns follow their base columns (VIRTUAL ones have no binlog value).
#[test]
fn cte_schema_keys_match_their_oracles() {
    scenario("cte_schema", |ctx| async move {
        run_suite(&ctx, &SUITE_SCHEMA, "w0", "0").await?;
        run_suite(&ctx, &SUITE_SCHEMA, "w50", "50").await?;
        Ok(())
    });
}

// --- B1 / B2 / B3: VIRTUAL generated columns (no value in the row images).
