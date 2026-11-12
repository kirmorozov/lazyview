use super::*;

/// A5: the flat form of a definition is bounded before anything is built: a CTE chain whose
/// copies would exceed the budget is refused in linear time with a reason naming the CTE;
/// within the budget the copies are inlined and planned.
#[test]
fn cte_inlining_is_bounded() {
    let cat = wide_catalog();
    let plan = |n: usize| {
        let d = parse_definition(&cte_chain(n)).unwrap();
        let t = std::time::Instant::now();
        let p = crate::analyzer::plan(&d, "db", &cat).unwrap();
        (p, t.elapsed())
    };
    let (p, _) = plan(3);
    assert_eq!(p.mode, Mode::Spj, "{}", p.mode);
    let (p, dt) = plan(6);
    assert_eq!(
        p.mode,
        Mode::Full(
            "inlining the WITH clause reads 64 table references in one SELECT (budget 32): \
             CTE `t5` is referenced 2 times"
                .into()
        )
    );
    assert!(dt.as_millis() < 500, "{dt:?}");
    for n in [14, 18, 40] {
        let (p, dt) = plan(n);
        assert!(
            matches!(p.mode, Mode::Full(ref r) if r.starts_with("inlining the WITH clause reads")),
            "{}",
            p.mode
        );
        assert!(dt.as_millis() < 500, "n={n}: {dt:?}");
        assert_eq!(p.tables.len(), 1);
    }
}

/// L6: the budget applies to each join block on its own (a UNION ALL branch is planned alone):
/// wide unions of small branches plan, a plain wide join is the server's to refuse, and the
/// reason names the block that exceeds it and what it inlines.
#[test]
fn inlining_budget_is_per_join_block() {
    let opts = super::Options {
        branch_plans: true,
        ..Default::default()
    };
    let go = |sql: &str, opts: &super::Options| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        super::analyze(&d, "db", &wide_catalog(), opts).unwrap()
    };
    let union = |n: usize, branch: &str| vec![branch; n].join(" UNION ALL ");
    // 33 one-table branches, no WITH: 33 branch plans (without branch plans: the construct,
    // not the budget).
    let u33 = union(33, "SELECT id, customer_id FROM orders WHERE total > 1");
    let super::Outcome::Branches(b) = go(&u33, &opts) else {
        panic!("expected branch plans")
    };
    assert_eq!(b.plans.len(), 33);
    assert!(b.plans.iter().all(|p| p.mode == Mode::Spj));
    assert_eq!(
        reason(&format!("CREATE VIEW v AS {u33}")),
        "set operation (UNION/INTERSECT/EXCEPT)"
    );
    // 40 branches over one shared CTE: each branch inlines one copy.
    let u40 = format!(
        "WITH c AS (SELECT id, customer_id FROM orders WHERE status = 'paid') {}",
        union(40, "SELECT id, customer_id FROM c")
    );
    let super::Outcome::Branches(b) = go(&u40, &opts) else {
        panic!("expected branch plans")
    };
    assert_eq!(b.plans.len(), 40);
    // A branch that exceeds the budget is named.
    let d = parse_definition(&cte_chain(6)).unwrap();
    let chain_body = d.query_sql.clone();
    let wide = chain_body.replacen(
        "SELECT id, total FROM t6",
        "SELECT id, total FROM orders UNION ALL SELECT id, total FROM t6",
        1,
    );
    assert_eq!(
        reason(&format!("CREATE VIEW v AS {wide}")),
        "inlining the WITH clause reads 64 table references in UNION branch 2 (budget 32): \
         CTE `t5` is referenced 2 times"
    );
    // A 33-way join written out inlines nothing: planned as written.
    let mut join = "SELECT o0.id FROM orders o0".to_string();
    for k in 1..33 {
        join.push_str(&format!(" JOIN orders o{k} ON o{k}.id = o0.id"));
    }
    assert_eq!(p(&format!("CREATE VIEW v AS {join}")).mode, Mode::Spj);
    // Derived tables alone: named as such.
    let mut derived = "SELECT d0.id FROM (SELECT id FROM orders) d0".to_string();
    for k in 1..33 {
        derived.push_str(&format!(
            " JOIN (SELECT id FROM orders) d{k} ON d{k}.id = d0.id"
        ));
    }
    assert_eq!(
        reason(&format!("CREATE VIEW v AS {derived}")),
        "inlining derived tables reads 33 table references in one SELECT (budget 32)"
    );
}

/// A CTE or derived table that only filters / projects / joins is planned like its inline form.
#[test]
fn metamorphic_cte_equals_inline() {
    for (cte, inline) in [
        (
            "WITH big AS (SELECT id, total, status FROM orders WHERE total > 5) SELECT id, status FROM big",
            "SELECT id, status FROM orders WHERE total > 5",
        ),
        (
            "WITH c AS (SELECT id, name FROM customers WHERE region_id = 3) \
             SELECT o.id, c.name FROM orders o JOIN c ON c.id = o.customer_id",
            "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id AND c.region_id = 3",
        ),
        (
            "SELECT x.id, x.name FROM (SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id) x",
            "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id",
        ),
        (
            "WITH a AS (SELECT id, customer_id FROM orders), b AS (SELECT a.id, c.name, c.region_id FROM a JOIN customers c ON c.id = a.customer_id) \
             SELECT b.id, b.name, r.name AS region FROM b LEFT JOIN regions r ON r.id = b.region_id",
            "SELECT o.id, c.name, r.name AS region FROM orders o JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
        ),
        (
            "WITH t (k, n) AS (SELECT order_id, qty FROM order_items) SELECT o.id, t.n FROM orders o JOIN t ON t.k = o.id",
            "SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
        ),
        (
            "SELECT o.id, d.name FROM orders o LEFT JOIN (SELECT id, name FROM customers WHERE region_id = 1) d ON d.id = o.customer_id",
            "SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id AND c.region_id = 1",
        ),
        (
            "WITH t AS (SELECT customer_id, COUNT(*) AS n, SUM(total) AS s FROM orders GROUP BY customer_id) \
             SELECT customer_id, s FROM t WHERE customer_id > 3 AND n > 1",
            "SELECT customer_id, SUM(total) AS s FROM orders WHERE customer_id > 3 GROUP BY customer_id HAVING COUNT(*) > 1",
        ),
        (
            "WITH t AS (SELECT * FROM order_items ORDER BY qty) SELECT order_id, line FROM t",
            "SELECT order_id, line FROM order_items",
        ),
        (
            "SELECT id FROM orders WHERE customer_id IN (WITH vip AS (SELECT id FROM customers WHERE region_id = 1) SELECT id FROM vip)",
            "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.region_id = 1",
        ),
    ] {
        let a = p(&format!("CREATE VIEW v AS {cte}"));
        let b = p(&format!("CREATE VIEW v AS {inline}"));
        assert_eq!(signature(&a), signature(&b), "\n{cte}\n  vs\n{inline}");
    }
}
