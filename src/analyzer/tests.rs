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
        // The construct that really blocks it is named.
        (
            "SELECT DISTINCT id FROM orders NATURAL JOIN order_items",
            Ok(Mode::Full("NATURAL join".into())),
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
        let old = analyze::plan(&d, "db", &cat);
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
    assert_eq!(
        full(
            "WITH t AS (SELECT customer_id, SUM(total) s FROM orders GROUP BY customer_id) \
              SELECT c.name, t.s FROM customers c JOIN t ON t.customer_id = c.id"
        ),
        "GROUP BY in CTE `t` joined to other tables"
    );
    assert_eq!(
        full("WITH t AS (SELECT DISTINCT customer_id FROM orders) SELECT c.name FROM customers c JOIN t ON t.customer_id = c.id"),
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
        "UNION ALL in CTE `u`"
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
    assert_eq!(
        full("SELECT o.id FROM orders o WHERE o.customer_id IN (SELECT customer_id FROM orders WHERE total > 100)"),
        "IN subquery over `orders` is not correlated on the view's unit key"
    );
    assert_eq!(
        full("SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)"),
        "subquery"
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
