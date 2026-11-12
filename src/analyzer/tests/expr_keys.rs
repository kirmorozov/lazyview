use super::*;

/// The key of the first projection of a one-column SELECT.
fn key_of(sql: &str) -> String {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    let res = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
    match &res[0] {
        Statement::Query(q) => match q.body.as_ref() {
            SetExpr::Select(s) => match &s.projection[0] {
                SelectItem::UnnamedExpr(e) => super::normalize::expr_key(e),
                _ => panic!("first projection is not a bare expression"),
            },
            _ => panic!("not a plain SELECT"),
        },
        _ => panic!("not a query"),
    }
}

#[test]
fn expr_key_identifiers() {
    // A column written in either letter case, with or without quotes, is one key.
    assert_eq!(key_of("SELECT price FROM t"), key_of("SELECT PRICE FROM t"));
    assert_eq!(
        key_of("SELECT price FROM t"),
        key_of("SELECT `Price` FROM t")
    );
    // In a qualified column only the last part is keyed; the table part keeps its case.
    assert_eq!(
        key_of("SELECT t.price FROM t"),
        key_of("SELECT t.PRICE FROM t")
    );
    assert_ne!(
        key_of("SELECT t.price FROM t"),
        key_of("SELECT T.price FROM T")
    );
    // String literals keep their exact text.
    assert_ne!(key_of("SELECT 'A' FROM t"), key_of("SELECT 'a' FROM t"));
}

#[test]
fn expr_key_functions_and_order() {
    // Function names are case-insensitive; arguments are keyed like columns.
    assert_eq!(
        key_of("SELECT COUNT(x) FROM t"),
        key_of("SELECT count(X) FROM t")
    );
    // Operand order is part of the key.
    assert_ne!(key_of("SELECT a + b FROM t"), key_of("SELECT b + a FROM t"));
}

#[test]
fn aggregated_cte_merge_ignores_column_letter_case() {
    // RI-3: the aggregated-CTE merge finds the outer output among the inner
    // group keys by expr_key, so the case the column was written in outside
    // does not change the plan.
    let a = p("CREATE VIEW v AS WITH t AS (SELECT customer_id, SUM(total) s, COUNT(*) n FROM orders GROUP BY customer_id) SELECT CUSTOMER_ID, s FROM t WHERE customer_id > 3 AND n > 1");
    let b = p("CREATE VIEW v AS WITH t AS (SELECT customer_id, SUM(total) s, COUNT(*) n FROM orders GROUP BY customer_id) SELECT customer_id, s FROM t WHERE customer_id > 3 AND n > 1");
    assert_eq!(format!("{:?}", a.mode), format!("{:?}", b.mode));
}

#[test]
fn key_join_pull_up_ignores_column_letter_case() {
    // RI-4: the pull-up through the key join matches the join column against
    // the group keys by expr_key, so the case it was written in does not
    // change the plan.
    let a = p("CREATE VIEW v AS WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) SELECT c.name, t.s FROM customers c JOIN t ON t.CUSTOMER_ID = c.id");
    let b = p("CREATE VIEW v AS WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) SELECT c.name, t.s FROM customers c JOIN t ON t.customer_id = c.id");
    assert_eq!(format!("{:?}", a.mode), format!("{:?}", b.mode));
}

#[test]
fn distinct_over_group_by_ignores_column_letter_case() {
    // RI-2: DISTINCT over its own GROUP BY is checked by expr_key, so the case
    // a group column was written in does not change the plan.
    let a = p(
        "CREATE VIEW v AS SELECT DISTINCT customer_id, COUNT(*) FROM orders GROUP BY CUSTOMER_ID",
    );
    let b = p(
        "CREATE VIEW v AS SELECT DISTINCT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
    );
    assert_eq!(format!("{:?}", a.mode), format!("{:?}", b.mode));
}

#[test]
fn window_partition_ignores_column_letter_case() {
    // RI-1: window PARTITION BY columns shared by all windows are compared by
    // expr_key, so the case a partition column was written in does not change the plan.
    let a = p("CREATE VIEW v AS SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn, SUM(total) OVER w AS run FROM orders WINDOW w AS (PARTITION BY CUSTOMER_ID ORDER BY id)");
    let b = p("CREATE VIEW v AS SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn, SUM(total) OVER w AS run FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)");
    assert_eq!(format!("{:?}", a.mode), format!("{:?}", b.mode));
}
