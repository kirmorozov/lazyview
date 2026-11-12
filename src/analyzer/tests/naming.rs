use super::*;

/// C4: a top-N over a set operation names the ORDER BY … LIMIT and the operator.
#[test]
fn top_n_over_a_union_is_named() {
    assert_eq!(
        reason("CREATE VIEW v AS (SELECT id, total FROM orders) UNION ALL (SELECT id, price FROM order_items) ORDER BY total DESC LIMIT 5"),
        "ORDER BY … LIMIT over a UNION ALL"
    );
    assert_eq!(
        reason("CREATE VIEW v AS SELECT id FROM orders UNION SELECT id FROM customers ORDER BY id LIMIT 5"),
        "ORDER BY … LIMIT over a UNION"
    );
    assert_eq!(
        reason("CREATE VIEW v AS SELECT id FROM orders UNION SELECT id FROM customers"),
        "set operation (UNION/INTERSECT/EXCEPT)"
    );
}

/// C6: an aggregate scalar subquery names itself and its place instead of "subquery".
#[test]
fn aggregate_scalar_subqueries_are_named() {
    for (sql, why) in [
        (
            "SELECT c.id, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS n FROM customers c",
            "correlated aggregate subquery in the SELECT list",
        ),
        (
            "SELECT o.id, (SELECT SUM(i.price) FROM order_items i WHERE i.order_id = o.id AND i.qty > 1) AS paid FROM orders o",
            "correlated aggregate subquery in the SELECT list",
        ),
        (
            "SELECT id FROM orders o WHERE o.total > (SELECT AVG(total) FROM orders)",
            "uncorrelated aggregate subquery in the WHERE clause",
        ),
    ] {
        assert_eq!(reason(&format!("CREATE VIEW v AS {sql}")), why, "{sql}");
    }
}

/// L5: ORDER BY … LIMIT goes through derived-table / CTE merges like the projection (Global,
/// ORDER BY over the merged columns); subqueries in such an ORDER BY are traced (their tables
/// recompute the view) when the FROM and WHERE stay as written, else Full with the reason.
#[test]
fn order_by_through_merges_and_subqueries() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    for (sql, select) in [
        (
            "SELECT d.id FROM (SELECT id, total FROM orders) d ORDER BY d.total DESC LIMIT 2",
            "SELECT `orders`.id AS `id` FROM orders ORDER BY `orders`.total DESC LIMIT 2",
        ),
        (
            "WITH c AS (SELECT id, total, customer_id FROM orders) SELECT c.id FROM c ORDER BY c.total DESC LIMIT 2",
            "SELECT `orders`.id AS `id` FROM orders ORDER BY `orders`.total DESC LIMIT 2",
        ),
        (
            "SELECT d.customer_id, COUNT(*) AS n FROM (SELECT customer_id, total FROM orders) d \
             GROUP BY d.customer_id ORDER BY SUM(d.total) DESC LIMIT 2",
            "SELECT `orders`.customer_id AS `customer_id`, COUNT(*) AS n FROM orders GROUP BY `orders`.customer_id \
             ORDER BY SUM(`orders`.total) DESC LIMIT 2",
        ),
    ] {
        let p = v(sql);
        assert_eq!(p.mode, Mode::Global, "{sql}");
        assert_eq!(p.select_sql(&[], None), select, "{sql}");
        // The ORDER BY column is read (an UPDATE of it alone recomputes the view).
        let total = p.aliases[0].table.col_index("total").unwrap();
        assert!(p.aliases[0].referenced[total], "{sql}");
    }
    // Subqueries in the ORDER BY: their tables recompute the view on changes of the columns
    // they name; the view's own correlated columns are read.
    let c = v(
        "SELECT o.id FROM orders o ORDER BY \
               (SELECT MAX(i.price) FROM order_items i WHERE i.order_id = o.id) DESC, o.id LIMIT 2",
    );
    assert_eq!(c.mode, Mode::Global);
    assert_eq!(c.tracing(), "o:recompute,order_items:recompute");
    assert_eq!(c.aliases[1].referenced, vec![true, false, false, true]);
    let s = v("SELECT o.id FROM orders o ORDER BY \
               (SELECT c.name FROM customers c WHERE c.id = o.customer_id), o.id LIMIT 2");
    assert_eq!(s.tracing(), "o:recompute,customers:recompute");
    assert!(s.aliases[0].referenced[1], "o.customer_id is read");
    let u = v("SELECT o.id FROM orders o ORDER BY o.total = (SELECT MAX(i.price) FROM order_items i), o.id LIMIT 2");
    assert_eq!(u.tracing(), "o:recompute,order_items:recompute");
    let star = v("SELECT o.id FROM orders o ORDER BY (SELECT COUNT(*) FROM (SELECT * FROM order_items) x) LIMIT 2");
    assert_eq!(star.aliases[1].referenced, vec![true; 4]);
    // A value that changes without a source change decides the rows: nothing to trace.
    for (sql, f) in [
        ("SELECT id FROM orders ORDER BY RAND() LIMIT 2", "RAND"),
        ("SELECT id FROM orders ORDER BY id + 0 * UNIX_TIMESTAMP(), id LIMIT 2", "UNIX_TIMESTAMP"),
        (
            "SELECT o.id FROM orders o ORDER BY (SELECT MAX(i.price) FROM order_items i WHERE i.qty > RAND()) LIMIT 2",
            "RAND",
        ),
    ] {
        assert_eq!(
            reason(&format!("CREATE VIEW v AS {sql}")),
            format!("non-deterministic function {f} in the ORDER BY of a top-N view"),
            "{sql}"
        );
    }
    // Without LIMIT the ORDER BY does not decide the rows: not traced.
    let n = v("SELECT o.id FROM orders o ORDER BY (SELECT MAX(i.price) FROM order_items i WHERE i.order_id = o.id)");
    assert_eq!(n.tracing(), "o:unit");
    for (sql, why) in [
        (
            "SELECT d.id FROM (SELECT id, total FROM orders) d ORDER BY (SELECT MAX(i.price) FROM order_items i WHERE i.order_id = d.id) LIMIT 2",
            "subquery in the ORDER BY of a top-N view over derived table `d`",
        ),
        (
            "SELECT o.id FROM orders o WHERE o.customer_id IN (SELECT id FROM customers) \
             ORDER BY (SELECT MAX(i.price) FROM order_items i WHERE i.order_id = o.id), o.id LIMIT 2",
            "subquery in the ORDER BY of a top-N view whose other subqueries, DISTINCT or joins are rewritten",
        ),
    ] {
        assert_eq!(reason(&format!("CREATE VIEW v AS {sql}")), why, "{sql}");
    }
}

/// C1: a string literal containing a newline before a semi-join marker does not break the
/// marker replacement (token locations are converted with the literal's lines). C2: a
/// parenthesized join that is not a chain of inner joins names the construct.
#[test]
fn newlines_in_literals_and_nested_outer_joins() {
    let cat = wide_catalog();
    let d = parse_definition(
        "CREATE VIEW v AS SELECT o.id FROM orders o WHERE o.note <> 'a\nb' \
         AND EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 2)",
    )
    .unwrap();
    let p = crate::analyzer::plan(&d, "db", &cat).unwrap();
    assert_eq!(p.mode, Mode::Spj, "{}", p.mode);
    assert!(p.aliases.iter().any(|a| a.semi.is_some()));
    let d = parse_definition(
        "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
         LEFT JOIN (customers c JOIN regions r ON r.id = c.region_id) ON c.id = o.customer_id",
    )
    .unwrap();
    let p = crate::analyzer::plan(&d, "db", &cat).unwrap();
    assert_eq!(
        p.mode,
        Mode::Full(
            "parenthesized join (customers c JOIN regions r ON r.id = c.region_id) that cannot \
             be flattened into the FROM chain"
                .into()
        )
    );
    assert_eq!(p.tables.len(), 3);
}

/// A6: a scalar subquery whose value reads only outer columns (or constants) is guarded after
/// the LEFT JOIN rewrite (the NULL-extended row would otherwise keep the outer value); one
/// propagating NULL from the subquery's own column is not.
#[test]
fn outer_only_scalar_values_are_guarded() {
    let cat = wide_catalog();
    let select = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        let p = crate::analyzer::plan(&d, "db", &cat).unwrap();
        assert_eq!(p.mode, Mode::Spj, "{sql}: {}", p.mode);
        p.select_sql(&[0], None)
    };
    for sql in [
        "SELECT o.id, (SELECT o.note FROM regions r WHERE r.id = o.id * 10) AS v FROM orders o",
        "SELECT o.id, (SELECT o.id * 2 FROM regions r WHERE r.id = o.id * 10) AS v FROM orders o",
        "SELECT o.id, (SELECT CONCAT(o.note, 'x') FROM regions r WHERE r.id = o.id * 10) AS v FROM orders o",
        "SELECT o.id, (SELECT 1 FROM regions r WHERE r.id = o.id * 10) AS v FROM orders o",
    ] {
        let s = select(sql);
        assert!(s.contains("IF(`r`.`id` IS NULL, NULL,"), "{sql}\n{s}");
    }
    let s = select("SELECT o.id FROM orders o WHERE (SELECT o.note FROM regions r WHERE r.id = o.id * 10) IS NULL");
    assert!(
        s.contains("IF(`r`.`id` IS NULL, NULL, o.note) IS NULL"),
        "{s}"
    );
    let s = select(
        "SELECT o.id, (SELECT r.name FROM regions r WHERE r.id = o.id * 10) AS v FROM orders o",
    );
    assert!(!s.contains("IF("), "{s}");
}
