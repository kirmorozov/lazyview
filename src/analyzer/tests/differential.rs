use super::*;
use crate::analyze;

/// The analyzer plans every statement of the block planner's test corpus identically, except
/// the listed improvements (never an incremental plan made worse).
#[test]
fn differential_against_the_block_planner() {
    let cat = wide_catalog();
    assert!(
        VIEW_CORPUS.len() > 60,
        "the view corpus holds only {}",
        VIEW_CORPUS.len()
    );
    let improved = improvements();
    let mut report = String::new();
    for sql in VIEW_CORPUS {
        let Ok(d) = parse_definition(sql) else {
            continue;
        };
        let old = analyze::plan_block(&d, "db", &cat);
        let new = analyzer_plan(&d, "db", &cat);
        let expected = improved.iter().find(|(s, _)| sql.contains(s));
        let same = match (&old, &new) {
            (Ok(o), Ok(n)) => format!("{o:?}") == format!("{n:?}"),
            (Err(_), Err(_)) => true,
            _ => false,
        };
        match expected {
            None if same => {}
            None => report.push_str(&format!(
                "\nDIFFERS: {sql}\n  old: {:?}\n  new: {:?}\n",
                old.as_ref().map(|p| &p.mode),
                new.as_ref().map(|p| &p.mode)
            )),
            Some((_, want)) => {
                if let Ok(o) = &old {
                    assert!(
                        matches!(o.mode, Mode::Full(_)),
                        "{sql}: an incremental plan must stay identical"
                    );
                }
                match (want, &new) {
                    (Ok(m), Ok(n)) if &n.mode == m => {}
                    (Err(part), Err(e)) if e.to_string().contains(part) => {}
                    _ => report.push_str(&format!(
                        "\nIMPROVEMENT MISMATCH: {sql}\n  want {want:?}\n  got {:?}\n",
                        new.as_ref().map(|p| &p.mode)
                    )),
                }
            }
        }
    }
    assert!(report.is_empty(), "{report}");
}

/// One statement per taxonomy class: the strategy and, for Full, the exact reason.
#[test]
fn taxonomy_strategies_and_reasons() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    let m = |sql: &str| v(sql).mode;
    let full = |sql: &str| match m(sql) {
        Mode::Full(r) => r,
        other => panic!("{sql}: expected Full, got {other:?}"),
    };
    // Merge family.
    assert_eq!(
        m("WITH t AS (SELECT id, status FROM orders WHERE total > 1) SELECT id FROM t"),
        Mode::Spj
    );
    assert_eq!(
        m("WITH t AS (SELECT id, customer_id FROM orders WHERE status = 'paid') \
           SELECT a.id, b.id AS other, c.id AS third FROM t a JOIN t b ON a.customer_id = b.customer_id \
           JOIN t c ON c.customer_id = b.customer_id"),
        Mode::Spj
    );
    let cols = v("WITH t (a, b) AS (SELECT id, total * 2 FROM orders) SELECT * FROM t");
    assert_eq!(
        cols.projection,
        ["`orders`.id AS `a`", "(`orders`.total * 2) AS `b`"]
    );
    assert_eq!(
        m("SELECT d.id FROM (WITH x AS (SELECT id, total FROM orders) SELECT id FROM x WHERE total > 0) d"),
        Mode::Spj
    );
    // A CTE named like a table reads the table in its own body.
    let sh = v("WITH orders AS (SELECT id, status FROM orders WHERE status = 'paid') SELECT id FROM orders");
    assert_eq!(sh.mode, Mode::Spj);
    assert_eq!(sh.where_sql.as_deref(), Some("(`orders`.status = 'paid')"));
    // NULL side: a single table merges (constant outputs guarded by the primary key) ...
    let ns = v("SELECT o.id, d.one, d.name FROM orders o LEFT JOIN (SELECT id, 1 AS one, name FROM customers) d ON d.id = o.customer_id");
    assert_eq!(ns.mode, Mode::Spj);
    assert_eq!(
        ns.projection[1],
        "IF(`customers`.`id` IS NULL, NULL, 1) AS `one`"
    );
    // ... several tables cannot be flattened.
    assert_eq!(
        full("SELECT o.id, d.name FROM orders o LEFT JOIN (SELECT c.id, r.name FROM customers c JOIN regions r ON r.id = c.region_id) d ON d.id = o.customer_id"),
        "derived table `d` joins several tables on the NULL side of a LEFT JOIN"
    );
    // Aggregated / DISTINCT relations: merged up when alone, isolated when joined.
    assert_eq!(
        m("WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) SELECT customer_id, s * 2 AS d FROM t WHERE s > 10"),
        Mode::Grouped
    );
    assert_eq!(
        m("WITH t AS (SELECT DISTINCT customer_id FROM orders) SELECT customer_id FROM t"),
        Mode::Grouped
    );
    assert_eq!(
        m("SELECT n FROM (SELECT COUNT(*) AS n FROM orders) d WHERE n > 0"),
        Mode::Global
    );
    // Joined on the other table's key: the aggregation is pulled up (see the dedicated test).
    assert_eq!(
        m(
            "WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) \
           SELECT c.name, t.s FROM customers c JOIN t ON t.customer_id = c.id"
        ),
        Mode::Grouped
    );
    assert_eq!(
        m("WITH t AS (SELECT DISTINCT customer_id FROM orders) SELECT c.name FROM customers c JOIN t ON t.customer_id = c.id"),
        Mode::Grouped
    );
    // Joined on anything else: kept apart (Full).
    assert_eq!(
        full(
            "WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) \
              SELECT c.name, t.s FROM customers c JOIN t ON t.customer_id = c.region_id"
        ),
        "GROUP BY in CTE `t` joined to other tables"
    );
    assert_eq!(
        full("WITH t AS (SELECT DISTINCT customer_id FROM orders) SELECT c.name FROM customers c JOIN t ON t.customer_id = c.region_id"),
        "DISTINCT in CTE `t` joined to other tables"
    );
    assert_eq!(
        full("WITH t AS (SELECT customer_id, COUNT(*) n FROM orders GROUP BY customer_id) SELECT n, COUNT(*) FROM t GROUP BY n"),
        "GROUP BY in CTE `t` under an aggregate, DISTINCT, window or LIMIT"
    );
    // Set operations, LIMIT, windows, recursion, constants, non-determinism.
    assert_eq!(
        full(
            "WITH u AS (SELECT id FROM orders UNION ALL SELECT id FROM customers) SELECT id FROM u"
        ),
        "UNION ALL in CTE `u` (needs branch plans)"
    );
    assert_eq!(
        full("SELECT x.id FROM (SELECT id FROM orders UNION SELECT id FROM customers) x"),
        "UNION in derived table `x`"
    );
    assert_eq!(
        full("WITH t AS (SELECT id FROM orders ORDER BY total DESC LIMIT 5) SELECT id FROM t"),
        "ORDER BY … LIMIT in CTE `t`"
    );
    assert_eq!(
        full("WITH t AS (SELECT id FROM orders LIMIT 5) SELECT id FROM t"),
        "LIMIT without ORDER BY in CTE `t`"
    );
    assert_eq!(
        full("WITH t AS (SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) rn FROM orders) SELECT id FROM t WHERE rn = 1"),
        "window function in CTE `t`"
    );
    assert_eq!(
        full("WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT o.id FROM orders o JOIN r ON r.n = o.id"),
        "recursive CTE `r`"
    );
    // An unreferenced CTE is never evaluated.
    assert_eq!(
        m("WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT id FROM orders"),
        Mode::Spj
    );
    assert_eq!(
        full("WITH k AS (SELECT 1 AS one) SELECT o.id FROM orders o JOIN k ON k.one = o.id"),
        "no FROM clause in CTE `k`"
    );
    assert_eq!(
        full("WITH t AS (SELECT id, NOW() AS ts FROM orders) SELECT id, ts FROM t"),
        "non-deterministic function NOW"
    );
    // Semi- and anti-joins.
    assert_eq!(m("SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region_id = 2)"), Mode::Spj);
    let ex = v("SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 5)");
    assert_eq!(ex.mode, Mode::Spj);
    assert_eq!(ex.aliases[1].direct_unit, Some(vec![0]));
    let nx = v("SELECT o.id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id)");
    assert_eq!(nx.mode, Mode::Spj);
    assert!(nx.where_sql.as_deref().unwrap().contains("NOT EXISTS"));
    let inu = v("SELECT c.id, c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders WHERE status = 'paid')");
    assert_eq!(inu.mode, Mode::Spj);
    assert_eq!(inu.aliases[1].direct_unit, Some(vec![1]));
    let g = v("SELECT customer_id, COUNT(*) FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.region_id = 1) GROUP BY customer_id");
    assert_eq!(g.mode, Mode::Grouped);
    // Not correlated on the unit: the subquery's rows are traced by forward lookups.
    assert_eq!(
        m("SELECT o.id FROM orders o WHERE o.customer_id IN (SELECT customer_id FROM orders WHERE total > 100)"),
        Mode::Spj
    );
    assert_eq!(
        m("SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)"),
        Mode::Spj
    );
    assert_eq!(
        m("SELECT COUNT(*) FROM orders o WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id)"),
        Mode::Global
    );
    assert_eq!(
        m("WITH vip AS (SELECT id FROM customers WHERE region_id = 1) SELECT id FROM orders WHERE customer_id IN (SELECT id FROM vip)"),
        Mode::Spj
    );
    // DISTINCT at the top.
    assert_eq!(
        m("SELECT DISTINCT customer_id, status FROM orders"),
        Mode::Grouped
    );
    assert_eq!(m("SELECT DISTINCT 1 FROM orders"), Mode::Global);
    assert_eq!(
        m("SELECT DISTINCT customer_id, COUNT(*) FROM orders GROUP BY customer_id"),
        Mode::Grouped
    );
    assert_eq!(m("SELECT ALL id FROM orders"), Mode::Spj);
    // A rewritten unaliased column whose name differs between server versions.
    assert_eq!(
        full("WITH t AS (SELECT id, total FROM orders) SELECT id, total /* c */ + 1 FROM t"),
        "unaliased column `total /* c */ + 1` needs an alias (its server-generated name differs between versions)"
    );
}

/// Aggregated CTEs joined to tables on their key: the aggregation is pulled up through the
/// key-preserving joins; any other join keeps the CTE apart.
#[test]
fn aggregation_pulled_up_through_key_joins() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    let t = "WITH t AS (SELECT order_id, SUM(qty * price) AS amount, COUNT(*) AS n FROM order_items GROUP BY order_id) ";
    let one = v(&format!(
        "{t}SELECT o.id, o.status, t.amount FROM orders o JOIN t ON t.order_id = o.id WHERE t.n > 1 AND o.status <> 'x'"
    ));
    assert_eq!(one.mode, Mode::Grouped);
    assert_eq!(
        one.select_sql(&[], None),
        "SELECT `o`.id AS `id`, `o`.status AS `status`, SUM(`order_items`.qty * `order_items`.price) AS `amount` \
         FROM orders o JOIN order_items ON (`order_items`.order_id = `o`.id) WHERE (`o`.status <> 'x') \
         GROUP BY `order_items`.order_id, `o`.`id`, `o`.`status` HAVING (COUNT(*) > 1)"
    );
    let inline = p(
        "CREATE VIEW v AS SELECT o.id, o.status, SUM(i.qty * i.price) AS amount FROM orders o \
                    JOIN order_items i ON i.order_id = o.id WHERE o.status <> 'x' \
                    GROUP BY i.order_id, o.id, o.status HAVING COUNT(*) > 1",
    );
    assert_eq!(signature(&one), signature(&inline));
    // Bound through another bound table.
    assert_eq!(
        v(&format!(
            "{t}SELECT t.order_id, c.name, t.amount FROM t JOIN orders o ON o.id = t.order_id JOIN customers c ON c.id = o.customer_id"
        ))
        .mode,
        Mode::Grouped
    );
    // Not the joined table's key: one group meets many rows.
    let r = reason(
        "CREATE VIEW v AS WITH t AS (SELECT customer_id, SUM(total) AS s FROM orders GROUP BY customer_id) \
         SELECT c.id, t.s FROM customers c JOIN t ON t.customer_id = c.region_id"
    );
    assert_eq!(r, "GROUP BY in CTE `t` joined to other tables");
    // NULL side, and a key compared across type families: not pulled up.
    assert!(matches!(
        v(&format!(
            "{t}SELECT o.id, t.amount FROM orders o LEFT JOIN t ON t.order_id = o.id"
        ))
        .mode,
        Mode::Full(_)
    ));
    assert!(matches!(
        v(
            "WITH t AS (SELECT status, COUNT(*) AS n FROM orders GROUP BY status) \
           SELECT c.id, t.n FROM customers c JOIN t ON t.status = c.id"
        )
        .mode,
        Mode::Full(_)
    ));
}
