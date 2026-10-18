//! Analyzer tests: differential against the block planner on its own test corpus, metamorphic
//! CTE-vs-inline checks, and one test per taxonomy class (strategy and exact reason).

use std::collections::HashMap;
use std::sync::Arc;

use super::plan as analyzer_plan;
use crate::analyze::{self, parse_definition, Mode, ViewPlan};
use crate::catalog::{Column, TableInfo, TableRef};

fn typed(name: &str, cols: Vec<Column>, pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
    let t = TableInfo {
        schema: "db".into(),
        name: name.into(),
        columns: cols,
        pk: pk.to_vec(),
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    };
    (("db".into(), name.into()), Arc::new(t))
}

fn table(name: &str, cols: &[(&str, &str)], pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
    typed(
        name,
        cols.iter().map(|(n, ty)| Column::simple(n, ty)).collect(),
        pk,
    )
}

/// The union of the catalogs of `analyze.rs`'s tests.
fn catalog() -> HashMap<TableRef, Arc<TableInfo>> {
    let opaque = |n: &str, ty: &str| Column::new(n, ty, ty, None, None, Some(16), true);
    let mut cat: HashMap<TableRef, Arc<TableInfo>> = [
        table(
            "orders",
            &[
                ("id", "int"),
                ("customer_id", "int"),
                ("status", "varchar"),
                ("total", "decimal"),
                ("note", "text"),
            ],
            &[0],
        ),
        table(
            "customers",
            &[("id", "int"), ("name", "varchar"), ("region_id", "int")],
            &[0],
        ),
        table("regions", &[("id", "int"), ("name", "varchar")], &[0]),
        table(
            "order_items",
            &[
                ("order_id", "int"),
                ("line", "int"),
                ("qty", "int"),
                ("price", "decimal"),
            ],
            &[0, 1],
        ),
        table("logs", &[("msg", "varchar")], &[]),
        typed(
            "codes",
            vec![
                Column::simple("code", "varchar"),
                Column::simple("v", "int"),
            ],
            &[0],
        ),
        typed(
            "refs",
            vec![
                Column::simple("id", "int"),
                Column::new(
                    "code",
                    "varchar",
                    "varchar(9)",
                    Some("latin1"),
                    Some("latin1_swedish_ci"),
                    Some(9),
                    false,
                ),
                Column::simple("code_int", "int"),
            ],
            &[0],
        ),
        typed(
            "docs",
            vec![
                Column::simple("id", "int"),
                Column::new("doc", "json", "json", None, None, None, false),
                Column::new("flags", "bit", "bit(8)", None, None, None, false),
            ],
            &[0],
        ),
        typed(
            "devices",
            vec![
                opaque("id", "uuid"),
                opaque("ip", "inet6"),
                Column::simple("name", "varchar"),
                opaque("pos", "point"),
            ],
            &[0],
        ),
        typed(
            "hits",
            vec![
                Column::simple("n", "int"),
                opaque("ip", "inet6"),
                opaque("v4", "inet4"),
            ],
            &[0],
        ),
        table("we`ird", &[("i`d", "int"), ("v", "int")], &[0]),
    ]
    .into_iter()
    .collect();
    let mut paid = (*cat[&("db".to_string(), "orders".to_string())]).clone();
    paid.name = "paid_orders".into();
    paid.pk.clear();
    paid.is_view = true;
    paid.view_bases = vec![("db".into(), "orders".into())];
    cat.insert(("db".into(), "paid_orders".into()), Arc::new(paid));
    cat
}

fn p(sql: &str) -> ViewPlan {
    let d = parse_definition(sql).unwrap();
    analyzer_plan(&d, "db", &catalog()).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
}

fn reason(sql: &str) -> String {
    match p(sql).mode {
        Mode::Full(r) => r,
        m => panic!("{sql}: expected Full, got {m:?}"),
    }
}

/// Every string literal of `analyze.rs`'s tests that holds a `CREATE VIEW` statement.
fn corpus() -> Vec<String> {
    let src = include_str!("../analyze.rs");
    let src = &src[src.find("#[cfg(test)]").expect("analyze.rs has tests")..];
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'/' && b.get(i + 1) == Some(&b'/') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b[i] == b'\'' {
            // A char literal ('x', '\'', '`') or a lifetime: skip it.
            let end = src[i + 1..].find('\'').map(|e| i + 1 + e);
            i = match end {
                Some(e) if e - i <= 3 || src[i + 1..e].starts_with('\\') => e + 1,
                _ => i + 1,
            };
            continue;
        }
        let raw = b[i] == b'r' && matches!(b.get(i + 1), Some(b'"' | b'#'));
        if raw {
            let hashes = b[i + 1..].iter().take_while(|&&c| c == b'#').count();
            let start = i + 2 + hashes;
            let close = format!("\"{}", "#".repeat(hashes));
            let end = start + src[start..].find(&close).expect("raw string ends");
            out.push(src[start..end].to_string());
            i = end + close.len();
            continue;
        }
        if b[i] == b'"' {
            let mut s = String::new();
            let mut j = i + 1;
            let chars: Vec<char> = src[j..].chars().take(4000).collect();
            let mut k = 0;
            while k < chars.len() {
                let c = chars[k];
                if c == '"' {
                    break;
                }
                if c == '\\' {
                    k += 1;
                    match chars[k] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        '"' => s.push('"'),
                        '\'' => s.push('\''),
                        '0' => s.push('\0'),
                        '\n' => {
                            while chars.get(k + 1).is_some_and(|c| c.is_whitespace()) {
                                k += 1;
                            }
                        }
                        other => {
                            s.push('\\');
                            s.push(other);
                        }
                    }
                } else {
                    s.push(c);
                }
                k += 1;
            }
            j += chars[..k].iter().map(|c| c.len_utf8()).sum::<usize>();
            out.push(s);
            i = j + 1;
            continue;
        }
        i += 1;
    }
    let mut v: Vec<String> = out
        .into_iter()
        .filter(|s| s.trim_start().starts_with("CREATE") && s.contains("VIEW"))
        .collect();
    v.dedup();
    v
}

/// Statements the analyzer plans differently from the block planner, with the expected new
/// outcome (`Ok(mode)` or `Err(message part)`). Everything else must be planned identically.
fn improvements() -> Vec<(&'static str, Result<Mode, &'static str>)> {
    vec![
        // DISTINCT = GROUP BY over the output columns.
        ("SELECT DISTINCT status FROM orders", Ok(Mode::Grouped)),
        ("SELECT DISTINCT * FROM orders", Ok(Mode::Grouped)),
        ("SELECT DISTINCT o.* FROM orders o", Ok(Mode::Grouped)),
        ("SELECT DISTINCT CONCAT(status, '\\\\n')", Ok(Mode::Grouped)),
        (
            "(a, b) AS SELECT DISTINCT * FROM regions",
            Ok(Mode::Grouped),
        ),
        (
            "SELECT DISTINCT r.*, c.id FROM regions r",
            Ok(Mode::Grouped),
        ),
        // DISTINCT over one aggregate row is a no-op.
        (
            "SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders",
            Ok(Mode::Global),
        ),
        // `*` over a USING join: the server coalesces the shared column (A4).
        (
            "(a, b, c, d) AS SELECT DISTINCT * FROM regions JOIN customers USING (id)",
            Ok(Mode::Full(crate::analyze::STAR_USING.into())),
        ),
        // The construct that really blocks it is named.
        (
            "SELECT DISTINCT id FROM orders NATURAL JOIN order_items",
            // No common columns: a cross join.
            Ok(Mode::Full(
                "table `order_items` contributes to every row (no join condition)".into(),
            )),
        ),
        // A leading RIGHT JOIN is a LEFT JOIN from the other side.
        (
            "SELECT o.id FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
            Ok(Mode::Spj),
        ),
        // IN over a primary key = inner join.
        (
            "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
            Ok(Mode::Spj),
        ),
        // A filter CTE is merged.
        (
            "WITH big AS (SELECT id FROM orders WHERE total > 5) SELECT id FROM big",
            Ok(Mode::Spj),
        ),
        // Top-N with a semi-join on the unit: one bounded recompute.
        (
            "WHERE c.id IN (SELECT customer_id FROM orders WHERE total > 5) ORDER BY region_id LIMIT 3",
            Ok(Mode::Global),
        ),
        // A group key on the NULL side: traced through the tuple-lineage view (internal views).
        (
            "SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id",
            Ok(Mode::Full(
                "group key on the NULL side of a LEFT JOIN (needs internal views)".into(),
            )),
        ),
        (
            "SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c",
            Ok(Mode::Full(
                "group key on the NULL side of a LEFT JOIN (needs internal views)".into(),
            )),
        ),
        // The server rejects the definition (2 columns for 1 name).
        (
            "CREATE VIEW v (a) AS SELECT * FROM (SELECT id, name FROM regions) d",
            Err("column list has 1 names but SELECT returns 2 columns"),
        ),
    ]
}

/// The analyzer plans every statement of the block planner's test corpus identically, except
/// the listed improvements (never an incremental plan made worse).
#[test]
fn differential_against_the_block_planner() {
    let cat = catalog();
    let corpus = corpus();
    assert!(
        corpus.len() > 60,
        "corpus extraction found only {}",
        corpus.len()
    );
    let improved = improvements();
    let mut report = String::new();
    for sql in &corpus {
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

/// What decides maintenance: mode, units, and per alias (table, columns used, how it is traced).
fn signature(v: &ViewPlan) -> String {
    let aliases: Vec<String> = v
        .aliases
        .iter()
        .map(|a| {
            format!(
                "{}:{:?}:{:?}:{}",
                a.table.name,
                a.referenced,
                a.membership,
                if a.direct_unit.is_some() {
                    "direct"
                } else {
                    "lookup"
                }
            )
        })
        .collect();
    format!(
        "{:?} units={} unique={} lineage={} cols={} aliases=[{}]",
        v.mode,
        v.unit_exprs.len(),
        v.unit_unique,
        v.lineage.len(),
        v.projection.len(),
        aliases.join(", ")
    )
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
    // Joined on anything else: kept apart (internal views pending).
    assert_eq!(
        full(
            "WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) \
              SELECT c.name, t.s FROM customers c JOIN t ON t.customer_id = c.region_id"
        ),
        "GROUP BY in CTE `t` joined to other tables (needs internal views)"
    );
    assert_eq!(
        full("WITH t AS (SELECT DISTINCT customer_id FROM orders) SELECT c.name FROM customers c JOIN t ON t.customer_id = c.region_id"),
        "DISTINCT in CTE `t` joined to other tables (needs internal views)"
    );
    assert_eq!(
        full("WITH t AS (SELECT customer_id, COUNT(*) n FROM orders GROUP BY customer_id) SELECT n, COUNT(*) FROM t GROUP BY n"),
        "GROUP BY in CTE `t` under an aggregate, DISTINCT, window or LIMIT (needs internal views)"
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
        "ORDER BY … LIMIT in CTE `t` (needs internal views)"
    );
    assert_eq!(
        full("WITH t AS (SELECT id FROM orders LIMIT 5) SELECT id FROM t"),
        "LIMIT without ORDER BY in CTE `t`"
    );
    assert_eq!(
        full("WITH t AS (SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) rn FROM orders) SELECT id FROM t WHERE rn = 1"),
        "window function in CTE `t` (needs internal views)"
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
    assert_eq!(
        r,
        "GROUP BY in CTE `t` joined to other tables (needs internal views)"
    );
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

fn outcome(sql: &str, cat: &HashMap<TableRef, Arc<TableInfo>>) -> super::Outcome {
    let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
    let opts = super::Options {
        internal_views: true,
        ..Default::default()
    };
    super::analyze(&d, "db", cat, &opts).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
}

/// The table the engine would create for an internal view (its columns, `pk` = its unit).
fn internal_table(
    iv: &super::InternalView,
    cols: &[(&str, &str)],
    pk: &[usize],
) -> (TableRef, Arc<TableInfo>) {
    table(&iv.name, cols, pk)
}

/// Aggregated CTEs joined on anything but a key become internal views; the parent is planned
/// over their tables once they exist, and identical subtrees share one view.
#[test]
fn internal_views_for_aggregates_joined_on_non_keys() {
    let sql = "WITH t AS (SELECT customer_id, SUM(total) AS s FROM orders GROUP BY customer_id) \
               SELECT c.id, t.s FROM customers c JOIN t ON t.customer_id = c.region_id";
    let mut cat = catalog();
    let super::Outcome::NeedsInternal(ivs) = outcome(sql, &cat) else {
        panic!("expected an internal view")
    };
    assert_eq!(ivs.len(), 1);
    let iv = &ivs[0];
    assert!(
        iv.name.starts_with("_lv_int_") && iv.name.len() == 24,
        "{}",
        iv.name
    );
    assert_eq!(
        iv.sql,
        format!(
            "CREATE VIEW `{}` AS SELECT `orders`.customer_id AS `customer_id`, SUM(`orders`.total) AS `s` \
             FROM orders GROUP BY `orders`.customer_id",
            iv.name
        )
    );
    assert_eq!(iv.purpose, "GROUP BY in CTE `t`");
    assert_eq!(iv.key, vec!["customer_id".to_string()]);
    // The same CTE in another definition: the same internal view.
    let other = "WITH t AS (SELECT customer_id, SUM(total) AS s FROM orders GROUP BY customer_id) \
                 SELECT r.id, t.s FROM regions r JOIN t ON t.customer_id = r.id + 1";
    let super::Outcome::NeedsInternal(again) = outcome(other, &cat) else {
        panic!("expected an internal view")
    };
    assert_eq!(again[0].name, iv.name);
    // The internal view itself plans as a grouped view despite its reserved name.
    let d = parse_definition(&iv.sql).unwrap();
    let opts = super::Options {
        internal: true,
        ..Default::default()
    };
    let super::Outcome::Plan(ip) = super::analyze(&d, "db", &cat, &opts).unwrap() else {
        panic!()
    };
    assert_eq!(
        (ip.mode.clone(), ip.name.as_str()),
        (Mode::Grouped, iv.name.as_str())
    );
    // Once its table exists, the parent is a chained view over it.
    let (k, t) = internal_table(iv, &[("customer_id", "int"), ("s", "decimal")], &[]);
    cat.insert(k, t);
    let super::Outcome::Plan(v) = outcome(sql, &cat) else {
        panic!("expected a plan")
    };
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.aliases[1].table.name, iv.name);
    assert!(
        v.from_sql.contains(&format!("`db`.`{}` AS `t`", iv.name)),
        "{}",
        v.from_sql
    );
    // Without internal views: Full, naming the construct.
    assert_eq!(
        reason(&format!("CREATE VIEW v AS {sql}")),
        "GROUP BY in CTE `t` joined to other tables (needs internal views)"
    );
}

/// A semi-join whose table maps to the units neither directly nor through a unique key: an
/// inner join with the internal view of its distinct keys.
#[test]
fn internal_views_for_untraceable_semi_joins() {
    // A cascading foreign key changes order_items rows without binlog rows of their own.
    let mut cat = catalog();
    let key = ("db".to_string(), "order_items".to_string());
    let mut items = (*cat[&key]).clone();
    items.cascades = vec![crate::catalog::ForeignKey {
        parent: ("db".into(), "orders".into()),
        child_cols: vec![0],
        parent_cols: vec!["id".into()],
        on_delete: crate::catalog::FkAction::Cascade,
        on_update: crate::catalog::FkAction::Cascade,
    }];
    cat.insert(key, Arc::new(items));
    let sql = "SELECT o.id, o.status FROM orders o WHERE o.customer_id IN (SELECT qty FROM order_items WHERE price > 1)";
    let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
    assert_eq!(
        analyzer_plan(&d, "db", &cat).unwrap().mode,
        Mode::Full("IN subquery over `order_items`, a child of a cascading foreign key".into())
    );
    let super::Outcome::NeedsInternal(ivs) = outcome(sql, &cat) else {
        panic!("expected an internal view")
    };
    assert_eq!(
        ivs[0].sql,
        format!(
            "CREATE VIEW `{}` AS SELECT `order_items`.`qty` AS `qty` FROM `db`.`order_items` AS `order_items` \
             WHERE (`order_items`.price > 1) GROUP BY `order_items`.`qty`",
            ivs[0].name
        )
    );
    assert_eq!(ivs[0].key, vec!["qty".to_string()]);
    let (k, t) = internal_table(&ivs[0], &[("qty", "int")], &[0]);
    cat.insert(k, t);
    let super::Outcome::Plan(v) = outcome(sql, &cat) else {
        panic!("expected a plan")
    };
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.aliases.len(), 2);
    assert!(
        v.where_sql
            .as_deref()
            .is_some_and(|w| w.contains("`order_items_keys`.`qty` = `o`.customer_id")),
        "{:?}",
        v.where_sql
    );
}

/// Subquery tables not correlated on the unit are looked up: the view's FROM and WHERE without
/// the subquery predicates, joined to their rows by the subquery's own conditions.
#[test]
fn semi_join_lookups() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    let a = v("SELECT o.id FROM orders o WHERE o.status <> 'x' AND o.customer_id IN (SELECT customer_id FROM orders WHERE total > 100)");
    let s = a.aliases.len() - 1;
    assert_eq!(a.aliases[s].direct_unit, None);
    assert_eq!(
        a.lookup_sql(s, "(R)"),
        "SELECT DISTINCT `o`.`id` FROM orders o WHERE ((o.status <> 'x') AND TRUE) AND (EXISTS (SELECT 1 FROM (R) AS `orders` \
         WHERE (`orders`.customer_id = o.customer_id) AND (`orders`.total > 100)))"
    );
    // Together with a table of the FROM clause: both substituted, parameters in statement order.
    let j = v(
        "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id \
               WHERE NOT EXISTS (SELECT 1 FROM regions r WHERE r.id = c.region_id)",
    );
    let (sql, order) = j
        .lookup_sql_multi(&[(2, "(R)".into()), (1, "(C)".into())])
        .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT `o`.`id` FROM orders o JOIN (C) AS `c` ON c.id = o.customer_id WHERE (TRUE) AND \
         (EXISTS (SELECT 1 FROM (R) AS `r` WHERE (`r`.id = c.region_id)))"
    );
    assert_eq!(order, vec![1, 0]);
    // NOT IN: a NULL key or a NULL operand relates every row.
    let n = v("SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)");
    assert!(
        n.lookup_sql(1, "(R)").contains(
            "`customers`.id = customer_id OR customer_id IS NULL OR `customers`.id IS NULL"
        ),
        "{}",
        n.lookup_sql(1, "(R)")
    );
    // Uncorrelated: every row is related (the predicate is one value for all rows).
    let u = v("SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM regions WHERE name = 'x')");
    assert!(
        u.lookup_sql(1, "(R)")
            .ends_with("(EXISTS (SELECT 1 FROM (R) AS `regions` WHERE (`regions`.name = 'x')))"),
        "{}",
        u.lookup_sql(1, "(R)")
    );
}

/// Window and top-N CTEs are isolated too: the parent filters or joins their table.
#[test]
fn internal_views_for_windows_and_top_n() {
    let mut cat = catalog();
    let win = "WITH t AS (SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn \
               FROM orders) SELECT id, customer_id FROM t WHERE rn = 1";
    let super::Outcome::NeedsInternal(ivs) = outcome(win, &cat) else {
        panic!("expected an internal view")
    };
    assert_eq!(ivs[0].purpose, "window function in CTE `t`");
    assert!(
        ivs[0].sql.contains(
            "ROW_NUMBER() OVER (PARTITION BY `orders`.customer_id ORDER BY `orders`.id) AS `rn`"
        ),
        "{}",
        ivs[0].sql
    );
    let top =
        "WITH top AS (SELECT id, total AS amount FROM orders ORDER BY amount DESC, id LIMIT 5) \
               SELECT c.name, t.amount FROM customers c JOIN top t ON t.id = c.id";
    let super::Outcome::NeedsInternal(ivs) = outcome(top, &cat) else {
        panic!("expected an internal view")
    };
    assert!(
        ivs[0]
            .sql
            .ends_with("FROM orders ORDER BY 2 DESC, 1 LIMIT 5"),
        "{}",
        ivs[0].sql
    );
    let (k, t) = internal_table(&ivs[0], &[("id", "int"), ("amount", "decimal")], &[]);
    cat.insert(k, t);
    let super::Outcome::Plan(v) = outcome(top, &cat) else {
        panic!("expected a plan")
    };
    assert_eq!(v.mode, Mode::Spj);
    // Without internal views: Full, naming the construct.
    assert_eq!(
        reason(&format!("CREATE VIEW v AS {win}")),
        "window function in CTE `t` (needs internal views)"
    );
}

/// Grouped views whose group key is reached through two lookup tables, or lies on the NULL side
/// of a LEFT JOIN, aggregate a tuple-lineage view of their join.
#[test]
fn tuple_lineage_views_for_untraceable_groups() {
    let mut cat = catalog();
    let sql = "SELECT c.region_id, SUM(i.qty) AS units, COUNT(*) AS n FROM order_items i \
               JOIN orders o ON o.id = i.order_id JOIN customers c ON c.id = o.customer_id GROUP BY c.region_id";
    let today = p(&format!("CREATE VIEW v AS {sql}"));
    assert_eq!(today.mode, Mode::Grouped);
    assert_eq!(
        today
            .aliases
            .iter()
            .filter(|a| a.direct_unit.is_none())
            .count(),
        2
    );
    let super::Outcome::NeedsInternal(ivs) = outcome(sql, &cat) else {
        panic!("expected the tuple-lineage view")
    };
    let j = &ivs[0];
    assert_eq!(
        j.sql,
        format!(
            "CREATE VIEW `{}` AS SELECT `c`.`region_id` AS `_lv_c0`, `i`.`qty` AS `_lv_c1` FROM order_items i \
             JOIN orders o ON `o`.id = `i`.order_id JOIN customers c ON `c`.id = `o`.customer_id",
            j.name
        )
    );
    let (k, t) = internal_table(j, &[("_lv_c0", "int"), ("_lv_c1", "int")], &[]);
    cat.insert(k, t);
    let super::Outcome::Plan(v) = outcome(sql, &cat) else {
        panic!("expected a plan")
    };
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.aliases.len(), 1);
    assert_eq!(
        v.aliases[0].direct_unit,
        Some(vec![0]),
        "J's rows carry the group key"
    );
    assert_eq!(
        v.select_sql(&[], None),
        format!(
            "SELECT `_lv_j`.`_lv_c0` AS `region_id`, SUM(`_lv_j`.`_lv_c1`) AS `units`, COUNT(*) AS `n` \
             FROM `db`.`{}` AS `_lv_j` GROUP BY `_lv_j`.`_lv_c0`",
            j.name
        )
    );
    // A group key on the NULL side; conditions on the NULL side are evaluated on J's rows.
    let sql =
        "SELECT c.region_id, COUNT(*) AS n FROM orders o LEFT JOIN customers c ON c.id = o.customer_id \
               WHERE o.status <> 'x' AND c.name <> 'y' GROUP BY c.region_id";
    let mut cat = catalog();
    let super::Outcome::NeedsInternal(ivs) = outcome(sql, &cat) else {
        panic!("expected the tuple-lineage view")
    };
    assert!(
        ivs[0].sql.ends_with("WHERE (`o`.status <> 'x')"),
        "{}",
        ivs[0].sql
    );
    let (k, t) = internal_table(&ivs[0], &[("_lv_c0", "int"), ("_lv_c1", "varchar")], &[]);
    cat.insert(k, t);
    let super::Outcome::Plan(v) = outcome(sql, &cat) else {
        panic!("expected a plan")
    };
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.where_sql.as_deref(), Some("(`_lv_j`.`_lv_c1` <> 'y')"));
}

/// Scalar subqueries that look one row up by its primary key are LEFT JOINs.
#[test]
fn scalar_lookups_unnested() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    let a = v("SELECT o.id, (SELECT name FROM customers c WHERE c.id = o.customer_id) AS cname FROM orders o");
    assert_eq!(a.mode, Mode::Spj);
    assert_eq!(
        a.from_sql,
        "orders o LEFT JOIN `db`.`customers` AS `c` ON (`c`.`id` = o.customer_id)"
    );
    assert_eq!(a.projection[1], "`c`.`name` AS `cname`");
    // Not NULL-propagating: NULL when no row matches, as the subquery would be.
    let g = v("SELECT o.id, (SELECT COALESCE(name, '?') FROM customers c WHERE c.id = o.customer_id) AS n FROM orders o");
    assert_eq!(
        g.projection[1],
        "IF(`c`.`id` IS NULL, NULL, COALESCE(`c`.`name`, '?')) AS `n`"
    );
    // In WHERE, and unaliased (named after its text, as by the server).
    let w = v(
        "SELECT o.id, (SELECT name FROM customers c WHERE c.id = o.customer_id) FROM orders o \
               WHERE (SELECT region_id FROM customers c WHERE c.id = o.customer_id) = 3",
    );
    assert_eq!(w.mode, Mode::Spj);
    assert_eq!(
        w.projection[1],
        "`c`.`name` AS `(SELECT name FROM customers c WHERE c.id = o.customer_id)`"
    );
    assert_eq!(w.aliases.len(), 3);
    // Not a key lookup, or an aggregate: still a subquery.
    for sql in [
        "SELECT o.id, (SELECT name FROM customers c WHERE c.region_id = o.customer_id) AS n FROM orders o",
        "SELECT o.id, (SELECT COUNT(*) FROM order_items i WHERE i.order_id = o.id) AS n FROM orders o",
    ] {
        assert_eq!(reason(&format!("CREATE VIEW v AS {sql}")), "subquery", "{sql}");
    }
}

/// Join syntax that only spells an inner or left join differently.
#[test]
fn join_syntax_normalized() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    let r = v("SELECT o.id, c.name FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id");
    assert_eq!(r.mode, Mode::Spj);
    assert_eq!(
        r.aliases[0].table.name, "orders",
        "the preserved side is the anchor"
    );
    assert_eq!(
        r.from_sql,
        "orders o LEFT JOIN customers c ON o.customer_id = c.id"
    );
    let s =
        v("SELECT o.id, c.name FROM orders o STRAIGHT_JOIN customers c ON c.id = o.customer_id");
    assert_eq!(s.mode, Mode::Spj);
    let n = v(
        "SELECT o.id, i.qty, c.name FROM (orders o JOIN order_items i ON i.order_id = o.id) \
               JOIN customers c ON c.id = o.customer_id",
    );
    assert_eq!(n.mode, Mode::Spj);
    assert_eq!(n.aliases.len(), 3);
    let u = v("SELECT r.id, r.name FROM regions r NATURAL JOIN customers c");
    assert_eq!(u.mode, Mode::Spj);
    assert_eq!(u.from_sql, "regions r JOIN customers c USING(`id`, `name`)");
    // A LEFT JOIN inside parentheses keeps its own NULL semantics: not flattened.
    assert_eq!(
        reason("CREATE VIEW v AS SELECT o.id FROM orders o JOIN (customers c LEFT JOIN regions r ON r.id = c.region_id) ON c.id = o.customer_id"),
        "unsupported FROM item"
    );
}

/// UNION ALL: one plan per branch (names from the first branch); any other set operation is
/// not split.
#[test]
fn union_all_branch_plans() {
    let opts = super::Options {
        branch_plans: true,
        ..Default::default()
    };
    let go = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        super::analyze(&d, "db", &catalog(), &opts).unwrap()
    };
    let super::Outcome::Branches(b) = go(
        "WITH paid AS (SELECT id, customer_id FROM orders WHERE status = 'paid') \
         SELECT id, customer_id AS who, 'o' AS src FROM paid \
         UNION ALL SELECT o.id, c.id, 'c' FROM orders o JOIN customers c ON c.id = o.customer_id \
         UNION ALL (SELECT customer_id, COUNT(*), 'g' FROM orders GROUP BY customer_id)",
    ) else {
        panic!("expected branch plans")
    };
    assert_eq!(b.plans.len(), 3);
    assert_eq!(
        b.plans.iter().map(|p| p.mode.clone()).collect::<Vec<_>>(),
        [Mode::Spj, Mode::Spj, Mode::Grouped]
    );
    assert_eq!(
        b.plans[1].projection,
        ["`o`.id AS `id`", "`c`.id AS `who`", "'c' AS `src`"]
    );
    assert!(b.probe_sql.contains("UNION ALL"), "{}", b.probe_sql);
    // Not a UNION ALL chain: planned as before.
    assert!(matches!(
        go("SELECT id FROM orders UNION SELECT id FROM customers"),
        super::Outcome::Plan(ViewPlan {
            mode: Mode::Full(_),
            ..
        })
    ));
}

#[test]
fn analyzer_errors_mirror_the_server() {
    let err = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &catalog()).unwrap_err().to_string()
    };
    assert_eq!(
        err("WITH t (a) AS (SELECT id, total FROM orders) SELECT a FROM t"),
        "CTE `t` has a column list of 1 names but its SELECT returns 2 columns"
    );
    assert_eq!(
        err("SELECT id FROM (SELECT id FROM orders) a JOIN customers c ON c.id = a.id"),
        "column `id` is ambiguous"
    );
    assert!(err("WITH t AS (SELECT id FROM nope) SELECT id FROM t").contains("does not exist"));
}

#[test]
fn referenced_tables_follow_cte_scopes() {
    let t = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        super::referenced_tables(&d.query, "db")
            .into_iter()
            .map(|t| t.1)
            .collect::<Vec<_>>()
    };
    // The body of a CTE named like a table reads the table.
    assert_eq!(
        t("WITH orders AS (SELECT * FROM orders) SELECT id FROM orders"),
        ["orders"]
    );
    // A CTE is visible only in its own query: the outer `x` is a table.
    assert_eq!(
        t("SELECT * FROM (WITH x AS (SELECT id FROM orders) SELECT id FROM x) a JOIN x ON x.id = a.id"),
        ["orders", "x"]
    );
    // Qualified names are never CTEs; recursive references are not tables.
    assert_eq!(
        t("WITH RECURSIVE r AS (SELECT id FROM regions UNION ALL SELECT id FROM r) SELECT * FROM r JOIN db.r ON 1"),
        ["r", "regions"]
    );
    assert_eq!(
        t("WITH a AS (SELECT id FROM orders), b AS (SELECT * FROM a) SELECT * FROM b WHERE id IN (SELECT id FROM customers)"),
        ["customers", "orders"]
    );
}

#[test]
#[ignore]
fn show_plans() {
    for sql in std::env::var("LV_SQL").unwrap_or_default().split(";;") {
        if sql.trim().is_empty() {
            continue;
        }
        let d = parse_definition(sql).unwrap();
        match analyzer_plan(&d, "db", &catalog()) {
            Ok(v) => {
                println!(
                    "== {sql}\n mode {:?}\n select: {}",
                    v.mode,
                    v.select_sql(&[], None)
                );
                println!(
                    " units {:?} in_proj {:?} unique {}",
                    v.unit_exprs, v.unit_in_projection, v.unit_unique
                );
                for (i, a) in v.aliases.iter().enumerate() {
                    println!(
                        "  alias {i} {} ({}) direct {:?} ref {:?} mem {:?}",
                        a.name, a.table.name, a.direct_unit, a.referenced, a.membership
                    );
                    if a.direct_unit.is_none() && a.pos.0 != usize::MAX {
                        println!("    lookup: {}", v.lookup_sql(i, "(LIT)"));
                    }
                }
                println!(" tables {:?}", v.tables);
            }
            Err(e) => println!("== {sql}\n ERROR {e:#}"),
        }
    }
}
