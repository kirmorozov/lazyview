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
        unique_keys: vec![],
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
/// A chain of `n` CTEs, each joining the previous one to itself (2^n copies when inlined).
fn cte_chain(n: usize) -> String {
    let mut sql = "WITH t0 AS (SELECT id, customer_id, total FROM orders)".to_string();
    for k in 1..=n {
        sql.push_str(&format!(
            ", t{k} AS (SELECT a.id, a.customer_id, a.total + b.total AS total FROM t{} a JOIN t{} b ON b.id = a.id)",
            k - 1,
            k - 1
        ));
    }
    format!("CREATE VIEW v AS {sql} SELECT id, total FROM t{n}")
}

/// A5: the flat form of a definition is bounded before anything is built: a CTE chain whose
/// copies would exceed the budget is refused in linear time with a reason naming the CTE;
/// within the budget the copies are inlined and planned.
#[test]
fn cte_inlining_is_bounded() {
    let cat = catalog();
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

/// B1/B2: a VIRTUAL generated column is inlined everywhere as its expression converted to the
/// declared type exactly as the server reads it (nested ones through their own conversion), in
/// window partitions and units derived after the inlining and inside semi-join subqueries; a type
/// without an exact SQL conversion is Full with the column and type named.
#[test]
fn virtual_columns_convert_to_their_declared_type() {
    use crate::catalog::Generated;
    let col = |n: &str, dt: &str, ct: &str| {
        let text = matches!(dt, "varchar" | "char");
        Column::new(
            n,
            dt,
            ct,
            text.then_some("utf8mb4"),
            text.then_some("utf8mb4_general_ci"),
            None,
            false,
        )
    };
    let gen = |mut c: Column, expr: &str, deps: Vec<usize>| {
        c.generated = Some(Generated {
            expr: expr.into(),
            stored: false,
            deps: Some(deps),
        });
        c
    };
    let mut gbin = col("gbin", "varchar", "varchar(20)");
    gbin.collation = Some("utf8mb4_bin".into());
    let mut cat = catalog();
    for (k, t) in [
        typed(
            "g1",
            vec![
                col("id", "int", "int"),
                col("a", "int", "int"),
                col("b", "int", "int"),
                col("name", "varchar", "varchar(20)"),
                col("dt", "datetime", "datetime"),
                gen(col("gi", "int", "int"), "(`a` / 3)", vec![1]),
                gen(col("gd", "decimal", "decimal(6,1)"), "(`a` / 3)", vec![1]),
                gen(gbin, "`name`", vec![3]),
                gen(col("gdate", "date", "date"), "`dt`", vec![4]),
                gen(col("gchain", "int", "int"), "(`gi` * 10)", vec![5]),
                gen(col("gsum", "int", "int"), "(`a` + `b`)", vec![1, 2]),
                gen(col("gf", "float", "float"), "`a`", vec![1]),
                gen(col("gshort", "varchar", "varchar(3)"), "(`a` / 3)", vec![1]),
            ],
            &[0],
        ),
        typed(
            "r",
            vec![col("id", "int", "int"), col("w", "int", "int")],
            &[0],
        ),
    ] {
        cat.insert(k, t);
    }
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    let gi = "(CAST(LEAST(GREATEST(CAST((`g1`.`a` / 3) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED))";
    let v = plan("SELECT id, gi, gd FROM g1 WHERE gi = 0");
    assert_eq!(
        v.projection,
        [
            "id".to_string(),
            format!("{gi} AS `gi`"),
            "(CAST((`g1`.`a` / 3) AS DECIMAL(6,1))) AS `gd`".into()
        ]
    );
    assert_eq!(v.where_sql.as_deref(), Some(format!("{gi} = 0").as_str()));
    let v = plan("SELECT gbin, COUNT(*) AS n FROM g1 GROUP BY gbin");
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(
        v.unit_exprs,
        ["(LEFT(CONVERT((`g1`.`name`) USING utf8mb4), 20) COLLATE utf8mb4_bin)"]
    );
    let v = plan("SELECT id FROM g1 WHERE gdate = '2026-01-01'");
    assert_eq!(
        v.where_sql.as_deref(),
        Some("(CAST(CAST((`g1`.`dt`) AS DATETIME) AS DATE)) = '2026-01-01'")
    );
    // Nested: gchain reads gi through gi's conversion, the same text everywhere (MySQL's
    // ONLY_FULL_GROUP_BY compares the projection with the GROUP BY).
    let v = plan("SELECT gchain, COUNT(*) AS n FROM g1 GROUP BY gchain");
    let chain = format!("(CAST(LEAST(GREATEST(CAST(({gi} * 10) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED))");
    assert_eq!(v.unit_exprs, std::slice::from_ref(&chain));
    assert_eq!(v.projection[0], format!("{chain} AS `gchain`"));
    assert_eq!(v.group_sql.as_deref(), Some(chain.as_str()));
    // An unaliased expression keeps the server's name for it as an alias.
    let v = plan("SELECT gi + 1, COUNT(*) AS n FROM g1 GROUP BY gi + 1");
    assert_eq!(v.projection[0], format!("{gi} + 1 AS `gi + 1`"));
    assert_eq!(v.group_sql, Some(format!("{gi} + 1")));
    // Window partitions (and the units taken from them) after the inlining.
    let v = plan("SELECT id, SUM(a) OVER (PARTITION BY gsum) AS s FROM g1");
    assert_eq!(v.mode, Mode::Grouped);
    assert!(
        v.unit_exprs[0].contains("`g1`.`a` + `g1`.`b`") && !v.unit_exprs[0].contains("gsum"),
        "{:?}",
        v.unit_exprs
    );
    // Inside IN / EXISTS / NOT IN subqueries: the lookups evaluate the expression.
    for sql in [
        "SELECT r.id, r.w FROM r WHERE r.w IN (SELECT g.gsum FROM g1 g)",
        "SELECT r.id, r.w FROM r WHERE EXISTS (SELECT 1 FROM g1 g WHERE g.gsum = r.w)",
        "SELECT r.id, r.w FROM r WHERE r.w NOT IN (SELECT g.gsum FROM g1 g)",
    ] {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.tracing(), "r:unit,g:lookup", "{sql}");
        let look = v.lookup_sql(1, "(G)");
        assert!(
            !look.contains("gsum") && look.contains("`g`.`a` + `g`.`b`"),
            "{sql}: {look}"
        );
        assert!(!v.where_sql.as_deref().unwrap().contains("gsum"), "{sql}");
        assert_eq!(v.aliases[1].image_cols, vec![1, 2], "{sql}");
    }
    // No exact conversion: Full, naming the column and its type.
    for (sql, why) in [
        (
            "SELECT id, gf FROM g1",
            "VIRTUAL generated column `g1`.`gf` of type float: no exact SQL conversion of its expression (an exact number) to that type",
        ),
        (
            "SELECT gshort, COUNT(*) FROM g1 GROUP BY gshort",
            "VIRTUAL generated column `g1`.`gshort` of type varchar(3): no exact SQL conversion of its expression (an exact number) to that type",
        ),
        (
            "SELECT r.id FROM r WHERE r.w IN (SELECT g.gf FROM g1 g)",
            "IN subquery reading VIRTUAL generated column `g1`.`gf` of type float: no exact SQL conversion of its expression (an exact number) to that type",
        ),
        (
            "SELECT id FROM g1 WHERE gbin = 'x' COLLATE utf8mb4_general_ci",
            "VIRTUAL generated column `g1`.`gbin` next to an explicit COLLATE (its inlined form has an explicit collation)",
        ),
    ] {
        match plan(sql).mode {
            Mode::Full(r) => assert_eq!(r, why, "{sql}"),
            m => panic!("{sql}: expected Full, got {m:?}"),
        }
    }
}

/// F1 / F2: integer targets clip before the CAST (an unsigned value >= 2^63 would wrap, an
/// out-of-range DOUBLE function result raises ERROR 1690 on MySQL); F4: nested VIRTUAL columns
/// (copied per reference) are measured before they are built and refused past the budget.
#[test]
fn virtual_integers_clip_and_nesting_is_bounded() {
    use crate::catalog::Generated;
    let col = |n: &str, dt: &str, ct: &str| Column::new(n, dt, ct, None, None, None, false);
    let gen = |mut c: Column, expr: &str, deps: Vec<usize>| {
        c.generated = Some(Generated {
            expr: expr.into(),
            stored: false,
            deps: Some(deps),
        });
        c
    };
    let mut vg = vec![
        col("id", "int", "int"),
        col("a", "bigint", "bigint"),
        gen(col("h0", "bigint", "bigint"), "`a`", vec![1]),
    ];
    for k in 1..8 {
        let prev = format!("`h{}`", k - 1);
        vg.push(gen(
            col(&format!("h{k}"), "bigint", "bigint"),
            &format!("greatest({})", vec![prev; 8].join(",")),
            vec![k + 1],
        ));
    }
    let mut cat = catalog();
    for (k, t) in [
        typed(
            "vu",
            vec![
                col("id", "int", "int"),
                col("u", "bigint", "bigint unsigned"),
                col("dbl", "double", "double"),
                gen(col("gi", "int", "int"), "`u`", vec![1]),
                gen(col("gb", "bigint", "bigint unsigned"), "(`u` + 0)", vec![1]),
                gen(col("gr", "int", "int"), "round(`dbl`,1)", vec![2]),
                gen(col("gn", "bigint", "bigint"), "-(`dbl`)", vec![2]),
                gen(col("gbu", "bigint", "bigint unsigned"), "`dbl`", vec![2]),
            ],
            &[0],
        ),
        typed("vg", vg, &[0]),
    ] {
        cat.insert(k, t);
    }
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        let t = std::time::Instant::now();
        let p = analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
        assert!(t.elapsed().as_millis() < 1000, "{sql}: {:?}", t.elapsed());
        p
    };
    let v = plan("SELECT id, gi, gb, gr, gn FROM vu");
    assert_eq!(
        v.projection,
        [
            "id",
            "(CAST(LEAST(GREATEST(CAST((`vu`.`u`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED)) AS `gi`",
            "(CAST(LEAST(GREATEST(CAST((`vu`.`u` + 0) AS DECIMAL(65,0)), 0), 18446744073709551615) AS UNSIGNED)) AS `gb`",
            "(CAST(LEAST(GREATEST((round(`vu`.`dbl`, 1)), -2147483648), 2147483647) AS SIGNED)) AS `gr`",
            "(IF((-(`vu`.`dbl`)) >= 9.223372036854775807e18, 9223372036854775807, CAST(GREATEST((-(`vu`.`dbl`)), -9.223372036854775808e18) AS SIGNED))) AS `gn`",
        ]
    );
    assert_eq!(
        plan("SELECT id, gbu FROM vu").mode,
        Mode::Full("VIRTUAL generated column `vu`.`gbu` of type bigint unsigned: no exact SQL conversion of its expression (a floating-point number) to that type".into())
    );
    // The last guard: a refresh SELECT past 1 MiB is not planned incrementally.
    let big = format!(
        "CREATE VIEW v AS SELECT id, '{}' AS pad FROM orders",
        "x".repeat(1 << 20)
    );
    match p(&big).mode {
        Mode::Full(r) => assert!(
            r.starts_with("the planned SELECT is ") && r.contains("bytes (limit 1048576)"),
            "{r}"
        ),
        m => panic!("{m:?}"),
    }
    // 8-way fan-out per level: h2 is 64 references, h3 512.
    assert_eq!(plan("SELECT id, h2 FROM vg").mode, Mode::Spj);
    for k in [3, 6, 7] {
        let n = 8usize.pow(k);
        match plan(&format!("SELECT id, h{k} FROM vg")).mode {
            Mode::Full(r) => assert_eq!(
                r,
                format!("VIRTUAL generated column `vg`.`h{k}` expands to {n} base-column references (budget 256): its nested VIRTUAL columns are copied per reference")
            ),
            m => panic!("h{k}: {m:?}"),
        }
    }
}

/// F3: alias renamings that make a subquery's alias equal to an outer alias (after a derived
/// table / CTE merge, or after qualifying an unqualified outer column) never let the subquery
/// capture the outer expressions filled into it: its tables are renamed away from the block's.
#[test]
fn semi_join_subqueries_never_capture_outer_aliases() {
    let templates = [
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS \
         (SELECT 1 FROM orders {S} JOIN order_items {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.qty > 4)",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE NOT EXISTS \
         (SELECT 1 FROM orders {S} JOIN order_items {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.qty > 4)",
        "WITH {D} AS (SELECT {I}.id, {I}.customer_id FROM orders {I}) SELECT {D}.id FROM {D} WHERE EXISTS \
         (SELECT 1 FROM orders {S} JOIN order_items {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id)",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.region_id FROM customers {I}) {D} WHERE EXISTS \
         (SELECT 1 FROM customers {S} WHERE {S}.region_id = {D}.id)",
        "SELECT {D}.customer_id, COUNT(*) AS n FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS \
         (SELECT 1 FROM order_items {S} WHERE {S}.order_id = {D}.id AND {S}.qty > 4) GROUP BY {D}.customer_id",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE {D}.customer_id IN \
         (SELECT {S}.customer_id FROM orders {S} JOIN order_items {T} ON {T}.order_id = {S}.id WHERE {T}.qty > 4)",
        // An unqualified outer column, qualified with the outer alias by the analyzer.
        "SELECT {I}.id FROM orders {I} WHERE EXISTS (SELECT 1 FROM order_items {S} WHERE {S}.order_id = {I}.id AND {S}.price > total)",
    ];
    let pool = ["o", "d", "p", "s", "x", "i", "T", "O"];
    let mut seed: u64 = 0x005e_edf3;
    let mut pick = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        pool[(seed % pool.len() as u64) as usize]
    };
    let mut checked = 0;
    for t in templates {
        for round in 0..24 {
            let (d, i, s, x) = if round == 0 {
                ("d", "o", "o", "p")
            } else {
                (pick(), pick(), pick(), pick())
            };
            if s.eq_ignore_ascii_case(x)
                || (t.starts_with("SELECT {I}") && i.eq_ignore_ascii_case(s))
            {
                continue;
            }
            let sql = t
                .replace("{D}", d)
                .replace("{I}", i)
                .replace("{S}", s)
                .replace("{T}", x);
            let v = p(&format!("CREATE VIEW v AS {sql}"));
            assert!(v.is_incremental(), "{sql}: {:?}", v.mode);
            let from: Vec<String> = v
                .aliases
                .iter()
                .filter(|a| a.pos.0 != usize::MAX)
                .map(|a| a.name.to_lowercase())
                .collect();
            for a in v.aliases.iter().filter(|a| a.pos.0 == usize::MAX) {
                assert!(
                    !from.contains(&a.name.to_lowercase()),
                    "{sql}: {} captures",
                    a.name
                );
                let template = a.semi.as_deref().unwrap();
                assert!(
                    template.contains(&format!("AS {}", super::ast::qident(&a.name))),
                    "{sql}: {template}"
                );
            }
            checked += 1;
        }
    }
    assert!(checked > 100, "{checked}");
    // The reviewer's shape, rendered: the correlation compares the subquery's renamed `o` with
    // the merged outer `o`.
    let v = p("CREATE VIEW v AS SELECT d.id FROM (SELECT o.id, o.customer_id FROM orders o) d WHERE EXISTS \
               (SELECT 1 FROM orders o JOIN order_items p ON p.order_id = o.id WHERE o.customer_id = d.customer_id AND p.qty > 4)");
    assert!(
        v.where_sql
            .as_deref()
            .unwrap()
            .contains("`o__m1`.customer_id = `o`.customer_id AND"),
        "{:?}",
        v.where_sql
    );
}

/// F5: NOT IN semi-joins are rendered in the refresh SELECT as their exact NULL-aware NOT EXISTS
/// form (MariaDB re-uses a stale NOT IN result in re-executed prepared statements); the lookups
/// keep their NULL-relating conditions.
#[test]
fn not_in_is_rendered_as_not_exists() {
    let v = p(
        "CREATE VIEW v AS SELECT c.id FROM customers c WHERE c.region_id NOT IN \
               (SELECT o.customer_id FROM orders o WHERE o.total > 400)",
    );
    assert_eq!(v.mode, Mode::Spj);
    let w = v.where_sql.as_deref().unwrap();
    assert!(!w.contains("NOT IN"), "{w}");
    assert_eq!(
        w,
        "((NOT EXISTS (SELECT 1 FROM orders o WHERE (`o`.total > 400)) OR (c.region_id IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM orders o WHERE (`o`.total > 400) AND (`o`.customer_id = c.region_id)) \
         AND NOT EXISTS (SELECT 1 FROM orders o WHERE (`o`.total > 400) AND (`o`.customer_id IS NULL)))))"
    );
    // Over a join (L4), without a WHERE in the subquery.
    for sql in [
        "SELECT c.id FROM customers c WHERE c.id NOT IN \
         (SELECT o.customer_id FROM orders o JOIN order_items i ON i.order_id = o.id WHERE i.qty > 4)",
        "SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)",
    ] {
        let v = p(&format!("CREATE VIEW v AS {sql}"));
        let all = format!("{} {}", v.where_sql.as_deref().unwrap_or(""), v.from_sql);
        assert!(v.is_incremental(), "{sql}: {:?}", v.mode);
        assert!(!all.contains("NOT IN") && all.contains("IS NOT NULL AND NOT EXISTS"), "{sql}: {all}");
    }
    // A semi-join merged onto the NULL side of a LEFT JOIN (its marker in the ON) is named.
    assert_eq!(
        reason("CREATE VIEW v AS SELECT o.id, d.id AS c FROM orders o LEFT JOIN (SELECT id FROM customers WHERE region_id NOT IN \
                (SELECT r.id FROM regions r WHERE r.name = 'x')) d ON d.id = o.customer_id"),
        "IN subquery in a derived table or CTE on the NULL side of a LEFT JOIN"
    );
    // Not a filter context: Full with the reason.
    assert_eq!(
        reason("CREATE VIEW v AS SELECT o.id FROM orders o ORDER BY o.customer_id NOT IN (SELECT id FROM customers WHERE region_id = 1), o.id LIMIT 2"),
        "NOT IN subquery in the ORDER BY of a top-N view (MariaDB re-executes it with a stale result): write NOT EXISTS"
    );
}

/// Schema part 2: NOT IN over columns that are never NULL is NOT EXISTS (direct units, no
/// all-units NULL trace); a UNIQUE NOT NULL key makes a semi-join an inner join like the PK.
#[test]
fn not_null_columns_and_unique_keys_in_semi_joins() {
    let mut cat = catalog();
    let set = |cat: &mut HashMap<TableRef, Arc<TableInfo>>, t: &str, f: &dyn Fn(&mut TableInfo)| {
        let k = ("db".to_string(), t.to_string());
        let mut info = (*cat[&k]).clone();
        f(&mut info);
        cat.insert(k, Arc::new(info));
    };
    set(&mut cat, "customers", &|t| t.columns[0].nullable = false);
    let plan = |cat: &HashMap<TableRef, Arc<TableInfo>>, sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    let sql = "SELECT c.id FROM customers c WHERE c.id NOT IN (SELECT o.customer_id FROM orders o WHERE o.total > 5)";
    // orders.customer_id nullable: NOT IN (its exact NOT EXISTS form in the refresh), lookups.
    let v = plan(&cat, sql);
    assert_eq!(v.tracing(), "c:unit,o:lookup");
    assert!(
        v.lookup_sql(1, "(O)").contains("IS NULL"),
        "{}",
        v.lookup_sql(1, "(O)")
    );
    // NOT NULL on both sides: NOT EXISTS, direct units.
    set(&mut cat, "orders", &|t| t.columns[1].nullable = false);
    let v = plan(&cat, sql);
    assert_eq!(v.tracing(), "c:unit,o:unit");
    assert_eq!(
        v.where_sql.as_deref(),
        Some("(NOT EXISTS (SELECT 1 FROM orders o WHERE (`o`.total > 5) AND (`o`.customer_id = c.id)))")
    );
    // The outer column on the NULL side of a LEFT JOIN can be NULL: stays NOT IN.
    let v = plan(
        &cat,
        "SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id \
         WHERE c.id NOT IN (SELECT r.id FROM regions r)",
    );
    assert!(
        v.where_sql
            .as_deref()
            .unwrap()
            .contains("IS NOT NULL AND NOT EXISTS"),
        "{:?}",
        v.where_sql
    );
    // A UNIQUE NOT NULL key (no PK): IN over it is an inner join when the outer column compares
    // like the key (G1: same charset and collation).
    set(&mut cat, "codes", &|t| {
        t.pk.clear();
        t.columns[0].nullable = false;
        t.unique_keys = vec![vec![0]];
    });
    let v = plan(
        &cat,
        "SELECT cu.id FROM customers cu WHERE cu.name IN (SELECT c.code FROM codes c WHERE c.v > 1)",
    );
    assert!(
        v.aliases.iter().all(|a| a.pos.0 != usize::MAX),
        "{}",
        v.tracing()
    );
    assert!(v.from_sql.contains("codes"), "{}", v.from_sql);
    // A latin1 column against the utf8mb4 key: a semi-join (lookups).
    let v = plan(
        &cat,
        "SELECT r.id FROM refs r WHERE r.code IN (SELECT c.code FROM codes c WHERE c.v > 1)",
    );
    assert_eq!(v.tracing(), "r:unit,c:lookup");
    assert!(!v.from_sql.contains("codes"), "{}", v.from_sql);
}

/// G1: a semi-join becomes an inner join, and a scalar subquery a LEFT JOIN, only when the
/// equalities on the key compare like the key. A string key against a number (`'1'`, `'01'`,
/// `'1.0'` all equal 1) or under an explicit COLLATE matches several key rows: those stay
/// semi-joins (lookups, exact) or subqueries. An integer expression against an integer key, or
/// a column of the key's own collation, joins.
#[test]
fn semi_joins_need_key_exact_equalities() {
    let mut cat = catalog();
    let int = |n: &str| {
        let mut c = Column::simple(n, "int");
        c.nullable = false;
        c
    };
    let text = |n: &str, co: &str| {
        let mut c = Column::new(
            n,
            "varchar",
            "varchar(10)",
            Some("utf8mb4"),
            Some(co),
            Some(40),
            false,
        );
        c.nullable = false;
        c
    };
    let general = "utf8mb4_general_ci";
    for (name, cols, pk, unique) in [
        (
            "c",
            vec![
                int("id"),
                int("num"),
                text("name", general),
                Column::simple("ref", "int"),
            ],
            vec![0],
            vec![],
        ),
        (
            "s_str",
            vec![int("id"), text("code", general), Column::simple("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
        (
            "s_spk",
            vec![text("code", general), Column::simple("v", "int")],
            vec![0],
            vec![],
        ),
        (
            "s_bin",
            vec![
                int("id"),
                text("code", "utf8mb4_bin"),
                Column::simple("v", "int"),
            ],
            vec![0],
            vec![vec![1]],
        ),
        (
            "s_int",
            vec![int("id"), int("code"), Column::simple("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
    ] {
        let (k, t) = typed(name, cols, &pk);
        let mut t = (*t).clone();
        t.unique_keys = unique;
        cat.insert(k, Arc::new(t));
    }
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    // (definition, subquery table, joined)
    for (sql, table, joined) in [
        ("SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_str s)", "s_str", false),
        (
            "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_str s WHERE s.code = c.num)",
            "s_str",
            false,
        ),
        ("SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_spk s)", "s_spk", false),
        (
            "SELECT c.id FROM c WHERE c.name COLLATE utf8mb4_general_ci IN (SELECT b.code FROM s_bin b)",
            "s_bin",
            false,
        ),
        ("SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_int s)", "s_int", true),
        ("SELECT c.id FROM c WHERE c.num * 2 + 1 IN (SELECT s.code FROM s_int s)", "s_int", true),
        ("SELECT c.id FROM c WHERE c.name IN (SELECT s.code FROM s_str s)", "s_str", true),
        ("SELECT c.id FROM c WHERE c.name IN (SELECT s.code FROM s_spk s)", "s_spk", true),
    ] {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.from_sql.contains(table), joined, "{sql}: {}", v.from_sql);
        assert_eq!(
            v.aliases.iter().any(|a| a.semi.is_some()),
            !joined,
            "{sql}: {}",
            v.tracing()
        );
    }
    // Scalar subqueries: a LEFT JOIN only over a key-exact equality, else a subquery (Full).
    for (sql, joined) in [
        ("SELECT c.id, (SELECT s.v FROM s_str s WHERE s.code = c.num) AS v FROM c", false),
        ("SELECT c.id, (SELECT s.v FROM s_spk s WHERE s.code = c.num) AS v FROM c", false),
        (
            "SELECT c.id, (SELECT b.v FROM s_bin b WHERE b.code = c.name COLLATE utf8mb4_general_ci) AS v FROM c",
            false,
        ),
        ("SELECT c.id, (SELECT s.v FROM s_int s WHERE s.code = c.num) AS v FROM c", true),
        ("SELECT c.id, (SELECT s.v FROM s_spk s WHERE s.code = c.name) AS v FROM c", true),
    ] {
        let want = if joined {
            Mode::Spj
        } else {
            Mode::Full("subquery".into())
        };
        assert_eq!(plan(sql).mode, want, "{sql}");
    }
}

/// G1 / G4 (guard R4): the key-exact rule in other formulations: swapped sides and a one-element
/// row, composite keys with one non-exact part, integer widths and signedness, the subquery
/// reached through a CTE or derived table with renamed columns, scalar subqueries in the SELECT
/// list and in WHERE, and outer columns one and two joins away.
#[test]
fn key_exact_formulations() {
    let mut cat = catalog();
    let int = |n: &str, ty: &str| {
        let mut c = Column::new(n, ty, ty, None, None, None, false);
        c.nullable = false;
        c
    };
    let text = |n: &str| {
        let mut c = Column::new(
            n,
            "varchar",
            "varchar(10)",
            Some("utf8mb4"),
            Some("utf8mb4_general_ci"),
            Some(40),
            false,
        );
        c.nullable = false;
        c
    };
    for (name, cols, pk, unique) in [
        (
            "c",
            vec![int("id", "int"), int("num", "int"), text("name")],
            vec![0],
            vec![],
        ),
        (
            "d",
            vec![
                int("id", "int"),
                int("c_id", "int"),
                int("num", "int"),
                text("code"),
            ],
            vec![0],
            vec![],
        ),
        (
            "e",
            vec![int("id", "int"), int("d_id", "int"), int("num", "int")],
            vec![0],
            vec![],
        ),
        (
            "s_str",
            vec![int("id", "int"), text("code"), int("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
        (
            "s_int",
            vec![int("id", "int"), int("code", "int"), int("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
        (
            "s_big",
            vec![int("id", "int"), int("code", "bigint"), int("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
        (
            "s_uns",
            vec![
                int("id", "int"),
                Column::new("code", "int", "int unsigned", None, None, None, false),
                int("v", "int"),
            ],
            vec![0],
            vec![vec![1]],
        ),
        (
            "s_comp",
            vec![
                int("id", "int"),
                int("a", "int"),
                text("b"),
                int("v", "int"),
            ],
            vec![0],
            vec![vec![1, 2]],
        ),
    ] {
        let (k, t) = typed(name, cols, &pk);
        let mut t = (*t).clone();
        for c in &mut t.columns {
            c.nullable = false;
        }
        t.unique_keys = unique;
        cat.insert(k, Arc::new(t));
    }
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    // (definition, subquery table, joined)
    for (sql, table, joined) in [
        // (a) swapped sides, a one-element row.
        ("SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_str s WHERE c.num = s.code)", "s_str", false),
        ("SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_int s WHERE c.num = s.code)", "s_int", true),
        ("SELECT c.id FROM c WHERE (c.num) IN (SELECT s.code FROM s_str s)", "s_str", false),
        ("SELECT c.id FROM c WHERE (c.num) IN (SELECT (s.code) FROM s_int s)", "s_int", true),
        // (b) a composite key: every part must be exact.
        ("SELECT c.id FROM c WHERE (c.num, c.num) IN (SELECT s.a, s.b FROM s_comp s)", "s_comp", false),
        (
            "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_comp s WHERE s.a = c.num AND s.b = c.num)",
            "s_comp",
            false,
        ),
        ("SELECT c.id FROM c WHERE (c.num, c.name) IN (SELECT s.a, s.b FROM s_comp s)", "s_comp", true),
        (
            "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_comp s WHERE s.b = c.name AND s.a = c.num)",
            "s_comp",
            true,
        ),
        // (c) integer widths and signedness compare as integers.
        ("SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_big s)", "s_big", true),
        ("SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_uns s)", "s_uns", true),
        // (d) through a CTE / derived table with renamed columns, and inside a CTE.
        (
            "WITH t AS (SELECT c.id AS cid, c.num AS n FROM c) SELECT t.cid FROM t WHERE t.n IN (SELECT s.code FROM s_str s)",
            "s_str",
            false,
        ),
        (
            "WITH t AS (SELECT c.id AS cid, c.num AS n FROM c) SELECT t.cid FROM t WHERE t.n IN (SELECT s.code FROM s_int s)",
            "s_int",
            true,
        ),
        (
            "SELECT x.cid FROM (SELECT c.id AS cid, c.num AS n FROM c) x WHERE EXISTS (SELECT 1 FROM s_str s WHERE s.code = x.n)",
            "s_str",
            false,
        ),
        (
            "SELECT x.cid FROM (SELECT c.id AS cid, c.num AS n FROM c) x WHERE EXISTS (SELECT 1 FROM s_int s WHERE s.code = x.n)",
            "s_int",
            true,
        ),
        (
            "WITH t AS (SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_str s)) SELECT id FROM t",
            "s_str",
            false,
        ),
        (
            "WITH t AS (SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_int s)) SELECT id FROM t",
            "s_int",
            true,
        ),
        // (e) outer columns one and two joins away.
        (
            "SELECT c.id FROM c JOIN d ON d.c_id = c.id WHERE d.num IN (SELECT s.code FROM s_str s)",
            "s_str",
            false,
        ),
        (
            "SELECT c.id FROM c JOIN d ON d.c_id = c.id WHERE d.code IN (SELECT s.code FROM s_str s)",
            "s_str",
            true,
        ),
        (
            "SELECT c.id FROM c JOIN d ON d.c_id = c.id JOIN e ON e.d_id = d.id WHERE e.num IN (SELECT s.code FROM s_str s)",
            "s_str",
            false,
        ),
        (
            "SELECT c.id FROM c JOIN d ON d.c_id = c.id JOIN e ON e.d_id = d.id WHERE e.num IN (SELECT s.code FROM s_int s)",
            "s_int",
            true,
        ),
    ] {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.from_sql.contains(table), joined, "{sql}: {}", v.from_sql);
        assert_eq!(
            v.aliases.iter().any(|a| a.semi.is_some()),
            !joined,
            "{sql}: {}",
            v.tracing()
        );
    }
    // Scalar subqueries in the SELECT list and in WHERE: a LEFT JOIN only over exact keys.
    for (sql, joined) in [
        ("SELECT c.id, (SELECT s.v FROM s_str s WHERE c.num = s.code) AS v FROM c", false),
        ("SELECT c.id, (SELECT s.v FROM s_int s WHERE c.num = s.code) AS v FROM c", true),
        ("SELECT c.id FROM c WHERE (SELECT s.v FROM s_str s WHERE s.code = c.num) > 1", false),
        ("SELECT c.id FROM c WHERE (SELECT s.v FROM s_int s WHERE s.code = c.num) > 1", true),
        (
            "SELECT c.id, (SELECT s.v FROM s_comp s WHERE s.a = c.num AND s.b = c.num) AS v FROM c",
            false,
        ),
        (
            "SELECT c.id, (SELECT s.v FROM s_comp s WHERE s.a = c.num AND s.b = c.name) AS v FROM c",
            true,
        ),
        (
            "SELECT c.id, (SELECT s.v FROM s_big s WHERE s.code = d.num) AS v FROM c JOIN d ON d.c_id = c.id",
            true,
        ),
    ] {
        let m = plan(sql).mode;
        if joined {
            assert_eq!(m, Mode::Spj, "{sql}");
        } else {
            assert!(matches!(&m, Mode::Full(_)), "{sql}: {m:?}");
        }
    }
}

/// G4: TIMESTAMP equality is not injective under a time zone with DST (two instants of the
/// fall-back hour show one local time), so a TIMESTAMP on either side of a key equality never
/// turns a semi-join, a scalar subquery or an aggregated CTE into a join; DATE and DATETIME keys
/// against their own type still do.
#[test]
fn timestamp_keys_are_not_key_exact() {
    let mut cat = catalog();
    let col = |n: &str, ty: &str| {
        let mut c = Column::new(n, ty, ty, None, None, None, false);
        c.nullable = false;
        c
    };
    for (name, cols, pk, unique) in [
        (
            "c",
            vec![
                col("id", "int"),
                col("d", "date"),
                col("dt", "datetime"),
                col("ts", "timestamp"),
            ],
            vec![0],
            vec![],
        ),
        (
            "k_ts",
            vec![col("id", "int"), col("k", "timestamp"), col("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
        (
            "k_tspk",
            vec![col("k", "timestamp"), col("v", "int")],
            vec![0],
            vec![],
        ),
        (
            "k_dt",
            vec![col("id", "int"), col("k", "datetime"), col("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
        (
            "k_date",
            vec![col("id", "int"), col("k", "date"), col("v", "int")],
            vec![0],
            vec![vec![1]],
        ),
    ] {
        let (k, t) = typed(name, cols, &pk);
        let mut t = (*t).clone();
        t.unique_keys = unique;
        cat.insert(k, Arc::new(t));
    }
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    // (definition, subquery table, joined)
    for (sql, table, joined) in [
        (
            "SELECT c.id FROM c WHERE c.dt IN (SELECT s.k FROM k_ts s)",
            "k_ts",
            false,
        ),
        (
            "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM k_ts s WHERE s.k = c.dt)",
            "k_ts",
            false,
        ),
        (
            "SELECT c.id FROM c WHERE c.ts IN (SELECT s.k FROM k_ts s)",
            "k_ts",
            false,
        ),
        (
            "SELECT c.id FROM c WHERE c.ts IN (SELECT s.k FROM k_tspk s)",
            "k_tspk",
            false,
        ),
        (
            "SELECT c.id FROM c WHERE c.ts IN (SELECT s.k FROM k_dt s)",
            "k_dt",
            false,
        ),
        (
            "SELECT c.id FROM c WHERE c.dt IN (SELECT s.k FROM k_dt s)",
            "k_dt",
            true,
        ),
        (
            "SELECT c.id FROM c WHERE c.d IN (SELECT s.k FROM k_date s)",
            "k_date",
            true,
        ),
    ] {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.from_sql.contains(table), joined, "{sql}: {}", v.from_sql);
        assert_eq!(
            v.aliases.iter().any(|a| a.semi.is_some()),
            !joined,
            "{sql}: {}",
            v.tracing()
        );
    }
    for (sql, want) in [
        (
            "SELECT c.id, (SELECT s.v FROM k_ts s WHERE s.k = c.dt) AS v FROM c",
            Mode::Full("subquery".into()),
        ),
        (
            "SELECT c.id, (SELECT s.v FROM k_tspk s WHERE s.k = c.ts) AS v FROM c",
            Mode::Full("subquery".into()),
        ),
        (
            "SELECT c.id, (SELECT s.v FROM k_dt s WHERE s.k = c.ts) AS v FROM c",
            Mode::Full("subquery".into()),
        ),
        (
            "SELECT c.id, (SELECT s.v FROM k_dt s WHERE s.k = c.dt) AS v FROM c",
            Mode::Spj,
        ),
        // An aggregated CTE joined on a TIMESTAMP key is not pulled up; on a DATETIME key it is.
        (
            "WITH t AS (SELECT ts, COUNT(*) AS n FROM c GROUP BY ts) SELECT s.id, t.n FROM k_ts s JOIN t ON t.ts = s.k",
            Mode::Full("GROUP BY in CTE `t` joined to other tables".into()),
        ),
        (
            "WITH t AS (SELECT dt, COUNT(*) AS n FROM c GROUP BY dt) SELECT s.id, t.n FROM k_dt s JOIN t ON t.dt = s.k",
            Mode::Grouped,
        ),
    ] {
        assert_eq!(plan(sql).mode, want, "{sql}");
    }
}

/// G5: a JSON value in a composite unit is Full (MySQL compares JSON by representation in a row
/// IN list of two or more tuples, the engine's filter for several units). Decided on the value's
/// type, not on how it is written; a single JSON key, text and scalars over JSON, a unit the keys
/// reduce to columns without JSON, and MariaDB's JSON (LONGTEXT) are not affected. Also through
/// CTEs and derived tables with renamed columns, joins in every placement, and 1-3 hops.
#[test]
fn json_columns_in_composite_units() {
    let mut cat = catalog();
    let col = |n: &str, ty: &str| Column::new(n, ty, ty, None, None, None, false);
    let mut long = Column::new(
        "doc",
        "longtext",
        "longtext",
        Some("utf8mb4"),
        Some("utf8mb4_bin"),
        None,
        true,
    );
    long.nullable = true;
    for (name, cols, pk) in [
        (
            "docs",
            vec![col("id", "int"), col("doc", "json"), col("grp", "int")],
            vec![0],
        ),
        (
            "docs_m",
            vec![col("id", "int"), long, col("grp", "int")],
            vec![0],
        ),
        (
            "docs_ab",
            vec![
                col("a", "int"),
                col("b", "int"),
                col("doc", "json"),
                col("grp", "int"),
            ],
            vec![0, 1],
        ),
        (
            "items",
            vec![
                col("id", "int"),
                col("grp", "int"),
                col("doc_id", "int"),
                col("link_id", "int"),
            ],
            vec![0],
        ),
        (
            "links",
            vec![col("id", "int"), col("doc_id", "int"), col("mid_id", "int")],
            vec![0],
        ),
        (
            "mids",
            vec![col("id", "int"), col("doc_id", "int")],
            vec![0],
        ),
    ] {
        let (k, t) = typed(name, cols, &pk);
        cat.insert(k, t);
    }
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    let full = |sql: &str| {
        let m = plan(sql).mode;
        assert!(
            matches!(&m, Mode::Full(r) if r.starts_with("JSON value `")
                && r.ends_with("` in a composite group key: MySQL compares JSON by representation in multi-row IN lists")),
            "{sql}: {m:?}"
        );
    };
    let grouped = |sql: &str| assert_eq!(plan(sql).mode, Mode::Grouped, "{sql}");
    assert_eq!(
        plan("SELECT grp, doc, COUNT(*) AS n FROM docs GROUP BY grp, doc").mode,
        Mode::Full(
            "JSON value `doc` in a composite group key: MySQL compares JSON by representation in multi-row IN lists"
                .into()
        )
    );
    // JSON values next to another key: columns, choices among JSON values, JSON computed below
    // the top (DISTINCT and PARTITION BY units alike).
    for sql in [
        "SELECT DISTINCT grp, doc FROM docs",
        "SELECT id, SUM(grp) OVER (PARTITION BY grp, doc) AS s FROM docs",
        "SELECT grp, IFNULL(doc, '{}') AS k, COUNT(*) AS n FROM docs GROUP BY grp, k",
        "SELECT grp, CASE WHEN grp > 1 THEN doc END AS k, COUNT(*) AS n FROM docs GROUP BY grp, k",
        "SELECT grp, IFNULL(JSON_OBJECT('k', id), JSON_OBJECT()) AS k, COUNT(*) AS n FROM docs GROUP BY grp, k",
    ] {
        full(sql);
    }
    // JSON computed by CAST or CONVERT (both spellings), alone or next to another key: Full by
    // the computed-JSON rule.
    for sql in [
        "SELECT grp, CAST(JSON_UNQUOTE(doc) AS JSON) AS k, COUNT(*) AS n FROM docs GROUP BY grp, k",
        "SELECT grp, CONVERT(JSON_UNQUOTE(doc), JSON) AS k, COUNT(*) AS n FROM docs GROUP BY grp, k",
        "SELECT CONVERT(JSON_UNQUOTE(doc), JSON) AS k, COUNT(*) AS n FROM docs GROUP BY k",
    ] {
        let m = plan(sql).mode;
        assert!(
            matches!(&m, Mode::Full(r) if r.contains("is a JSON value computed by a function")),
            "{sql}: {m:?}"
        );
    }
    // Text and scalars over JSON next to another key, single JSON keys, MariaDB.
    for sql in [
        "SELECT doc, COUNT(*) AS n FROM docs GROUP BY doc",
        "SELECT DISTINCT doc FROM docs",
        "SELECT grp, JSON_UNQUOTE(JSON_EXTRACT(doc, '$.a')) AS a, COUNT(*) AS n FROM docs GROUP BY grp, a",
        "SELECT grp, doc ->> '$.a' AS a, COUNT(*) AS n FROM docs GROUP BY grp, a",
        "SELECT grp, LOWER(doc ->> '$.a') AS a, COUNT(*) AS n FROM docs GROUP BY grp, a",
        "SELECT grp, JSON_VALUE(doc, '$.a') AS a, COUNT(*) AS n FROM docs GROUP BY grp, a",
        "SELECT grp, JSON_LENGTH(doc) AS l, COUNT(*) AS n FROM docs GROUP BY grp, l",
        "SELECT grp, CAST(doc AS CHAR) AS t, COUNT(*) AS n FROM docs GROUP BY grp, t",
        "SELECT grp, doc, COUNT(*) AS n FROM docs_m GROUP BY grp, doc",
    ] {
        grouped(sql);
    }
    // The primary key determines doc: the unit is the key alone (renamed, by position, or not
    // selected); a composite key stays composite but holds no JSON.
    for sql in [
        "SELECT id, doc, COUNT(*) AS n FROM docs GROUP BY id, doc",
        "SELECT id AS k, doc, COUNT(*) AS n FROM docs GROUP BY k, doc",
        "SELECT id, doc, COUNT(*) AS n FROM docs GROUP BY 1, 2",
        "SELECT doc, COUNT(*) AS n FROM docs GROUP BY id, doc",
        "SELECT a, b, doc, COUNT(*) AS n FROM docs_ab GROUP BY a, b, doc",
        "SELECT DISTINCT id, doc FROM docs",
    ] {
        grouped(sql);
    }
    full("SELECT a, doc, COUNT(*) AS n FROM docs_ab GROUP BY a, doc");
    // Window units are the PARTITION BY list as written (the unit planner does not reduce them by
    // keys): a JSON column there is Full even next to the key (conservative, sound).
    for sql in [
        "SELECT id, SUM(grp) OVER (PARTITION BY id, doc) AS s FROM docs",
        "SELECT a, SUM(grp) OVER (PARTITION BY a, b, doc) AS s FROM docs_ab",
        "SELECT a, SUM(grp) OVER (PARTITION BY a, doc) AS s FROM docs_ab",
    ] {
        full(sql);
    }
    // Through a CTE and a derived table with renamed columns.
    for (sql, json) in [
        ("WITH x AS (SELECT grp AS g, doc AS j FROM docs) SELECT g, j, COUNT(*) AS n FROM x GROUP BY g, j", true),
        ("SELECT g, j, COUNT(*) AS n FROM (SELECT grp AS g, doc AS j FROM docs) x GROUP BY g, j", true),
        ("WITH x AS (SELECT grp AS g, doc AS j FROM docs_m) SELECT g, j, COUNT(*) AS n FROM x GROUP BY g, j", false),
        ("SELECT g, j, COUNT(*) AS n FROM (SELECT grp AS g, doc AS j FROM docs_m) x GROUP BY g, j", false),
        ("WITH x AS (SELECT id AS k, doc AS j FROM docs) SELECT k, j, COUNT(*) AS n FROM x GROUP BY k, j", false),
    ] {
        if json {
            full(sql)
        } else {
            grouped(sql)
        }
    }
    // A joined table's JSON column: joined on its key (JOIN … ON, comma + WHERE, either order,
    // LEFT JOIN), one to three hops away; the outer key determines it (Grouped), another column
    // does not (Full).
    let hops = [
        "items i JOIN docs d ON d.id = i.doc_id",
        "items i JOIN docs d ON i.doc_id = d.id",
        "items i, docs d WHERE d.id = i.doc_id",
        "items i, docs d WHERE i.doc_id = d.id",
        "docs d JOIN items i ON d.id = i.doc_id",
        "items i LEFT JOIN docs d ON d.id = i.doc_id",
        "items i JOIN links l ON l.id = i.link_id JOIN docs d ON d.id = l.doc_id",
        "items i JOIN links l ON l.id = i.link_id JOIN mids m ON m.id = l.mid_id JOIN docs d ON d.id = m.doc_id",
    ];
    for from in hops {
        let (from, cond) = match from.split_once(" WHERE ") {
            Some((f, w)) => (f.to_string(), format!(" WHERE {w}")),
            None => (from.to_string(), String::new()),
        };
        grouped(&format!(
            "SELECT i.id, d.doc, COUNT(*) AS n FROM {from}{cond} GROUP BY i.id, d.doc"
        ));
        let other =
            format!("SELECT i.grp, d.doc, COUNT(*) AS n FROM {from}{cond} GROUP BY i.grp, d.doc");
        if from.contains("LEFT JOIN") {
            // (The NULL-side group key is refused first.)
            assert_eq!(
                plan(&other).mode,
                Mode::Full("group key on the NULL side of a LEFT JOIN".into())
            );
        } else {
            full(&other);
        }
    }
    // A one-element `IN (y)` is not a key equality for the unit planner: the key does not reduce
    // the unit, so the JSON column stays in it and the view is Full (conservative, sound).
    for from in [
        "items i JOIN docs d ON d.id IN (i.doc_id)",
        "items i, docs d WHERE d.id IN (i.doc_id)",
    ] {
        full(&format!(
            "SELECT i.id, d.doc, COUNT(*) AS n FROM {from} GROUP BY i.id, d.doc"
        ));
    }
}

/// The VIEW_DEFINITION validator: the server's canonical text (MySQL and MariaDB printing)
/// reads the same tables and column count; a different reading is an error naming it.
#[test]
fn validate_against_the_canonical_definition() {
    let cat = catalog();
    let check = |sql: &str, canonical: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        super::validate(&d, canonical, "db", &cat)
    };
    for (sql, canonical) in [
        (
            "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id",
            "select `o`.`id` AS `id`,`c`.`name` AS `name` from (`db`.`orders` `o` join `db`.`customers` `c` on((`c`.`id` = `o`.`customer_id`)))",
        ),
        (
            "SELECT * FROM regions",
            "select `db`.`regions`.`id` AS `id`,`db`.`regions`.`name` AS `name` from `db`.`regions`",
        ),
        (
            "WITH t AS (SELECT id FROM orders WHERE total > 5) SELECT id FROM t",
            "with `t` as (select `db`.`orders`.`id` AS `id` from `db`.`orders` where (`db`.`orders`.`total` > 5)) select `t`.`id` AS `id` from `t`",
        ),
        (
            "SELECT d.id FROM (SELECT id FROM orders) d WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = d.id)",
            "select `d`.`id` AS `id` from (select `db`.`orders`.`id` AS `id` from `db`.`orders`) `d` where exists(select 1 from `db`.`customers` `c` where (`c`.`id` = `d`.`id`))",
        ),
    ] {
        assert!(check(sql, canonical).is_ok(), "{sql}: {:?}", check(sql, canonical));
    }
    let e = check(
        "SELECT id FROM orders",
        "select `db`.`customers`.`id` AS `id` from `db`.`customers`",
    )
    .unwrap_err()
    .to_string();
    assert!(
        e.contains("`db`.`customers`") && e.contains("`db`.`orders`"),
        "{e}"
    );
    let e = check(
        "SELECT * FROM regions",
        "select `db`.`regions`.`id` AS `id` from `db`.`regions`",
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("1 columns where lazyview reads 2"), "{e}");
    // Unreadable printing: not checked.
    assert!(check("SELECT id FROM orders", "select ??? garbage").is_ok());
}

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
        super::analyze(&d, "db", &catalog(), opts).unwrap()
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
    let cat = catalog();
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
    let cat = catalog();
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
        // `*` over a USING join: the shared column once, then the rest (A4); DISTINCT over it
        // is a GROUP BY like any other.
        (
            "(a, b, c, d) AS SELECT DISTINCT * FROM regions JOIN customers USING (id)",
            Ok(Mode::Grouped),
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
        // A group key on the NULL side: Full (the tuple-lineage view is an internal view).
        (
            "SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id",
            Ok(Mode::Full(
                "group key on the NULL side of a LEFT JOIN".into(),
            )),
        ),
        (
            "SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c",
            Ok(Mode::Full(
                "group key on the NULL side of a LEFT JOIN".into(),
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
        "GROUP BY in CTE `t` joined to other tables"
    );
}

/// C3: a semi-join over a child of a cascading foreign key stays incremental where the subquery
/// is correlated on the unit through the FK columns (the parent's deletes / key updates give the
/// units); otherwise the view is Full (cascaded child rows have no binlog rows).
#[test]
fn semi_joins_over_cascading_children() {
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
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap()
    };
    // Correlated on the unit through the FK column: the parent's image gives the units.
    let v = plan("SELECT o.id, o.status FROM orders o WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 5)");
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.tracing(), "o:unit,i:unit,^orders:unit");
    // Not on the unit: the units a parent's delete changes are not known.
    let v = plan("SELECT o.id, o.status FROM orders o WHERE o.customer_id IN (SELECT qty FROM order_items WHERE price > 1)");
    assert_eq!(
        v.mode,
        Mode::Full(
            "IN subquery over `order_items`: the units a cascade from `orders` changes are not known"
                .into()
        )
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

/// Subqueries over inner joins (L4: the workload's `v_big_spenders`): every table of the
/// subquery is a semi-join alias, traced by direct units where it carries the correlation to the
/// unit, else by lookups that substitute it among the subquery's other tables (several of them
/// together share one EXISTS). Outer joins, LIMIT, aggregates and derived tables inside the
/// subquery keep it a subquery, with a reason naming the construct.
#[test]
fn semi_joins_over_joined_subqueries() {
    let v = |sql: &str| p(&format!("CREATE VIEW v AS {sql}"));
    let a = v("SELECT c.id, c.name FROM customers c WHERE c.id IN \
               (SELECT o.customer_id FROM orders o JOIN order_items i ON i.order_id = o.id WHERE i.qty > 4)");
    assert_eq!(a.mode, Mode::Spj);
    assert_eq!(a.tracing(), "c:unit,o:unit,i:lookup");
    assert_eq!(a.aliases[1].direct_unit, Some(vec![1]));
    assert_eq!(
        a.where_sql.as_deref(),
        Some("(c.id IN (SELECT `o`.customer_id FROM `db`.`orders` AS `o`, `db`.`order_items` AS `i` \
              WHERE (`i`.order_id = `o`.id) AND (`i`.qty > 4)))")
    );
    assert_eq!(
        a.lookup_sql(2, "(I)"),
        "SELECT DISTINCT `c`.`id` FROM customers c WHERE (TRUE) AND (EXISTS (SELECT 1 FROM `db`.`orders` AS `o`, \
         (I) AS `i` WHERE (`o`.customer_id = c.id) AND (`i`.order_id = `o`.id) AND (`i`.qty > 4)))"
    );
    assert_eq!(a.aliases[2].image_cols, vec![0, 2]);
    // Neither table carries the unit: both are looked up, and a lookup of both at once
    // evaluates one subquery over both rows (an old row of one may only have joined an old row
    // of the other).
    let g = v("SELECT c.region_id, COUNT(*) AS n FROM customers c WHERE EXISTS \
               (SELECT 1 FROM orders o, order_items i WHERE i.order_id = o.id AND o.customer_id = c.id AND i.qty > 4) \
               GROUP BY c.region_id");
    assert_eq!(g.mode, Mode::Grouped);
    assert_eq!(g.tracing(), "c:unit,o:lookup,i:lookup");
    let (sql, order) = g
        .lookup_sql_multi(&[(2, "(I)".into()), (1, "(O)".into())])
        .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT c.region_id FROM customers c WHERE (TRUE) AND (EXISTS (SELECT 1 FROM (O) AS `o`, (I) AS `i` \
         WHERE (`i`.order_id = `o`.id) AND (`o`.customer_id = c.id) AND (`i`.qty > 4)))"
    );
    assert_eq!(order, vec![1, 0]);
    assert!(
        g.lookup_sql(1, "(O)")
            .contains("FROM (O) AS `o`, `db`.`order_items` AS `i` WHERE"),
        "{}",
        g.lookup_sql(1, "(O)")
    );
    // With a table of the view's FROM: its own substitution, the subquery's EXISTS.
    let (sql, order) = g
        .lookup_sql_multi(&[(1, "(O)".into()), (0, "(C)".into())])
        .unwrap();
    assert!(
        sql.starts_with("SELECT DISTINCT c.region_id FROM (C) AS `c` WHERE (TRUE) AND (EXISTS (SELECT 1 FROM (O) AS `o`, `db`.`order_items` AS `i` "),
        "{sql}"
    );
    assert_eq!(order, vec![1, 0]);
    // NOT EXISTS: the correlated table's changes are direct units too.
    let n = v("SELECT c.id FROM customers c WHERE NOT EXISTS \
               (SELECT 1 FROM orders o JOIN order_items i ON i.order_id = o.id WHERE o.customer_id = c.id)");
    assert_eq!(n.tracing(), "c:unit,o:unit,i:lookup");
    // NOT IN: never direct; a NULL key from any combination relates every row.
    let ni = v("SELECT c.id FROM customers c WHERE c.id NOT IN \
                (SELECT o.customer_id FROM orders o JOIN order_items i ON i.order_id = o.id)");
    assert_eq!(ni.tracing(), "c:unit,o:lookup,i:lookup");
    assert!(
        ni.lookup_sql(2, "(I)")
            .contains("(`o`.customer_id = c.id OR c.id IS NULL OR `o`.customer_id IS NULL)"),
        "{}",
        ni.lookup_sql(2, "(I)")
    );
    // USING inside the subquery becomes ON.
    let u = v("SELECT c.id FROM customers c WHERE c.region_id IN \
               (SELECT r.id FROM regions r JOIN customers c2 USING (id, name))");
    assert_eq!(u.mode, Mode::Spj);
    assert!(
        u.where_sql
            .as_deref()
            .is_some_and(|w| w.contains("WHERE ((`r`.id = `c2`.id) AND (`r`.name = `c2`.name))")),
        "{:?}",
        u.where_sql
    );
    // What keeps a subquery a subquery.
    for (sql, why) in [
        (
            "SELECT c.id FROM customers c WHERE c.id IN (SELECT o.customer_id FROM orders o LEFT JOIN order_items i ON i.order_id = o.id WHERE i.qty IS NULL)",
            "IN subquery with an outer join",
        ),
        (
            "SELECT id FROM orders WHERE NOT EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = orders.id LIMIT 1)",
            "NOT EXISTS subquery with LIMIT",
        ),
        (
            "SELECT c.id FROM customers c WHERE c.id IN (SELECT o.customer_id FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.customer_id HAVING SUM(i.qty) > 4)",
            "IN subquery with an aggregate",
        ),
        (
            "SELECT c.id FROM customers c WHERE c.id IN (SELECT d.customer_id FROM (SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id) d WHERE d.n > 2)",
            "IN subquery with an aggregate",
        ),
    ] {
        assert_eq!(reason(&format!("CREATE VIEW v AS {sql}")), why, "{sql}");
    }
    // A cascading child among the subquery's tables (no binlog rows for cascaded changes).
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
    let d = parse_definition(
        "CREATE VIEW v AS SELECT c.id FROM customers c WHERE c.id IN \
         (SELECT o.customer_id FROM orders o JOIN order_items i ON i.order_id = o.id)",
    )
    .unwrap();
    let v = analyzer_plan(&d, "db", &cat).unwrap();
    assert_eq!(
        v.mode,
        Mode::Full(
            "IN subquery over `order_items`: the units a cascade from `orders` changes are not known"
                .into()
        )
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
        "window function in CTE `t`"
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
    // Not a key lookup, or an aggregate (named, C6): still a subquery.
    for (sql, why) in [
        (
            "SELECT o.id, (SELECT name FROM customers c WHERE c.region_id = o.customer_id) AS n FROM orders o",
            "subquery",
        ),
        (
            "SELECT o.id, (SELECT COUNT(*) FROM order_items i WHERE i.order_id = o.id) AS n FROM orders o",
            "correlated aggregate subquery in the SELECT list",
        ),
    ] {
        assert_eq!(reason(&format!("CREATE VIEW v AS {sql}")), why, "{sql}");
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
        "parenthesized join (customers c LEFT JOIN regions r ON r.id = c.region_id) that cannot be flattened into the FROM chain"
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

/// Upserts by row identity (`ViewPlan::rows_identified`): tables of semi- and anti-join
/// subqueries add no rows of their own, so a select/join view is identified by its unit and the
/// lineage of its FROM tables; a FROM table without lineage or a first table without a row key
/// leaves the rows unidentified.
#[test]
fn semi_join_views_are_identified_by_their_from_tables() {
    for sql in [
        "CREATE VIEW v AS SELECT o.id, o.status FROM orders o \
         WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.customer_id)",
        "CREATE VIEW v AS SELECT o.id, o.status FROM orders o \
         WHERE NOT EXISTS (SELECT 1 FROM logs l WHERE l.msg = o.status)",
        "CREATE VIEW v AS SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id \
         WHERE o.id IN (SELECT i.order_id FROM order_items i WHERE i.qty > 1)",
    ] {
        let v = p(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert!(
            v.aliases.iter().any(|a| a.semi.is_some()),
            "{sql}: planned without a semi-join table"
        );
        assert!(v.rows_identified(), "{sql}");
    }
    for sql in [
        "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id \
         WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)",
        "CREATE VIEW v AS SELECT msg FROM logs",
    ] {
        assert!(!p(sql).rows_identified(), "{sql}");
    }
}

/// Catalog for the formulation tests below: `item` is a cascading child of `entity`, `pair_item`
/// one of `pair` through a composite key; `attr` / `attr_n` reference `entity` without a foreign
/// key (NOT NULL / nullable column).
fn formulation_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
    use crate::catalog::{FkAction, ForeignKey};
    let mut cat = catalog();
    let col = |n: &str, nullable: bool| {
        let mut c = Column::new(n, "int", "int", None, None, None, false);
        c.nullable = nullable;
        c
    };
    let fk = |parent: &str, child_cols: Vec<usize>, parent_cols: Vec<&str>| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols,
        parent_cols: parent_cols.into_iter().map(String::from).collect(),
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    for (name, cols, pk, cascades) in [
        (
            "entity",
            vec![col("id", false), col("v", false)],
            vec![0],
            vec![],
        ),
        (
            "attr",
            vec![col("id", false), col("ent_id", false), col("v", false)],
            vec![0],
            vec![],
        ),
        (
            "attr_n",
            vec![col("id", false), col("ent_id", true), col("v", false)],
            vec![0],
            vec![],
        ),
        (
            "link",
            vec![col("id", false), col("ent_id", false)],
            vec![0],
            vec![],
        ),
        (
            "link2",
            vec![col("id", false), col("link_id", false)],
            vec![0],
            vec![],
        ),
        (
            "link3",
            vec![col("id", false), col("link2_id", false)],
            vec![0],
            vec![],
        ),
        (
            "item",
            vec![col("id", false), col("ent_id", false), col("v", false)],
            vec![0],
            vec![fk("entity", vec![1], vec!["id"])],
        ),
        (
            "item_n",
            vec![col("id", false), col("ent_id", true), col("v", false)],
            vec![0],
            vec![fk("entity", vec![1], vec!["id"])],
        ),
        (
            "item_part",
            vec![col("id", false), col("item_id", false), col("v", false)],
            vec![0],
            vec![fk("item", vec![1], vec!["id"])],
        ),
        (
            "item_part_sub",
            vec![col("id", false), col("part_id", false), col("v", false)],
            vec![0],
            vec![fk("item_part", vec![1], vec!["id"])],
        ),
        (
            "pair",
            vec![col("a", false), col("b", false), col("v", false)],
            vec![0, 1],
            vec![],
        ),
        (
            "pair_item",
            vec![
                col("id", false),
                col("pa", false),
                col("pb", false),
                col("v", false),
            ],
            vec![0],
            vec![fk("pair", vec![1, 2], vec!["a", "b"])],
        ),
    ] {
        let (k, t) = typed(name, cols, &pk);
        let mut t = (*t).clone();
        t.cascades = cascades;
        cat.insert(k, Arc::new(t));
    }
    cat
}

/// Guard R4 on PR #3: `x NOT IN (SELECT k …)` with x and k never NULL is `NOT EXISTS (… AND k = x)`
/// (direct units) in every formulation where x is a NOT NULL column of the head or an inner-joined
/// table: written as NOT EXISTS, the outer column on an inner-joined table that is not first in
/// FROM, a comma join. A NULL side or a nullable k keeps the NULL-aware NOT IN form.
#[test]
fn not_in_over_never_null_columns_formulations() {
    let cat = formulation_catalog();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    let anti = |sql: &str, tracing: &str| {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.tracing(), tracing, "{sql}");
        let w = v.where_sql.unwrap_or_default();
        assert!(
            w.contains("NOT EXISTS") && !w.contains("IS NULL"),
            "{sql}: {w}"
        );
    };
    let not_in = |sql: &str, tracing: &str| {
        let v = plan(sql);
        assert_eq!(v.tracing(), tracing, "{sql}");
        let w = v.where_sql.unwrap_or_default();
        assert!(w.contains("IS NOT NULL AND NOT EXISTS"), "{sql}: {w}");
    };
    let sub = "(SELECT a.ent_id FROM attr a WHERE a.v > 5)";
    anti(
        &format!("SELECT e.id FROM entity e WHERE e.id NOT IN {sub}"),
        "e:unit,a:unit",
    );
    anti(
        "SELECT e.id FROM entity e WHERE NOT EXISTS (SELECT 1 FROM attr a WHERE a.v > 5 AND a.ent_id = e.id)",
        "e:unit,a:unit",
    );
    // The outer column on an inner-joined table that is not first in FROM, and a comma join (the
    // unit is the head's key: the subquery table is looked up).
    anti(
        &format!(
            "SELECT l.id FROM link l JOIN entity e ON e.id = l.ent_id WHERE e.id NOT IN {sub}"
        ),
        "l:unit,e:lineage,a:lookup",
    );
    anti(
        &format!("SELECT l.id FROM link l, entity e WHERE e.id = l.ent_id AND e.id NOT IN {sub}"),
        "l:unit,e:lineage,a:lookup",
    );
    // The outer column on a NULL side, a nullable k: NOT IN with its NULL branches.
    not_in(
        &format!(
            "SELECT l.id FROM link l LEFT JOIN entity e ON e.id = l.ent_id WHERE e.id NOT IN {sub}"
        ),
        "l:unit,e:lookup,a:lookup",
    );
    not_in(
        "SELECT e.id FROM entity e WHERE e.id NOT IN (SELECT a.ent_id FROM attr_n a WHERE a.v > 5)",
        "e:unit,a:lookup",
    );
    // Through a CTE / derived table with renamed columns, also nested: the derived output is the
    // inner NOT NULL column, the same NOT EXISTS form and route as the base.
    for sql in [
        format!("WITH t AS (SELECT e.id AS eid FROM entity e) SELECT t.eid FROM t WHERE t.eid NOT IN {sub}"),
        format!("SELECT x.eid FROM (SELECT e.id AS eid FROM entity e) x WHERE x.eid NOT IN {sub}"),
        format!(
            "WITH t AS (SELECT e.id AS eid FROM entity e), u AS (SELECT t.eid AS k FROM t) SELECT u.k FROM u WHERE u.k NOT IN {sub}"
        ),
    ] {
        anti(&sql, "e:unit,a:unit");
    }
    // A derived output that may be NULL, or is not a plain column, keeps NOT IN: a nullable inner
    // column, an inner LEFT JOIN's NULL side, the NULL side of a leading RIGHT JOIN (mirrored to a
    // LEFT JOIN), an expression output.
    for sql in [
        format!("SELECT x.k FROM (SELECT a.ent_id AS k FROM attr_n a) x WHERE x.k NOT IN {sub}"),
        format!(
            "SELECT x.lid FROM (SELECT l.id AS lid, e.id AS eid FROM link l LEFT JOIN entity e ON e.id = l.ent_id) x WHERE x.eid NOT IN {sub}"
        ),
        format!(
            "SELECT x.lid FROM (SELECT l.id AS lid FROM link l RIGHT JOIN entity e ON e.id = l.ent_id) x WHERE x.lid NOT IN {sub}"
        ),
        format!("SELECT x.eid FROM (SELECT e.id + 0 AS eid FROM entity e) x WHERE x.eid NOT IN {sub}"),
        format!("SELECT x.k FROM (SELECT COALESCE(a.ent_id, 0) AS k FROM attr_n a) x WHERE x.k NOT IN {sub}"),
    ] {
        let w = plan(&sql).where_sql.unwrap_or_default();
        assert!(w.contains("IS NOT NULL AND NOT EXISTS"), "{sql}: {w}");
    }
    // An aggregated derived block, and a RIGHT JOIN after the column's table, are Full anyway.
    assert_eq!(
        plan(&format!("SELECT x.eid FROM (SELECT e.id AS eid FROM entity e GROUP BY e.id) x WHERE x.eid NOT IN {sub}")).mode,
        Mode::Full("GROUP BY in derived table `x` under an aggregate, DISTINCT, window or LIMIT".into())
    );
    assert_eq!(
        plan(&format!("SELECT x.eid FROM (SELECT e.id AS eid FROM entity e JOIN link l ON l.ent_id = e.id RIGHT JOIN attr a ON a.ent_id = e.id) x WHERE x.eid NOT IN {sub}")).mode,
        Mode::Full("RIGHT/FULL/other join".into())
    );
}

/// Guard R4 on PR #3 (C3): a semi-join over a child of a cascading foreign key takes the parent's
/// cascade route when it is correlated on the view's unit through the whole key (`^parent:unit`),
/// in every formulation: swapped sides, IN, a CTE or derived table with renamed outputs, a
/// composite key, the parent first with other tables after it; otherwise the view is Full.
#[test]
fn cascading_child_semi_join_formulations() {
    let cat = formulation_catalog();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    for (sql, tracing) in [
        (
            "SELECT e.id FROM entity e WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id AND i.v > 5)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT e.id FROM entity e WHERE EXISTS (SELECT 1 FROM item i WHERE e.id = i.ent_id AND i.v > 5)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT e.id FROM entity e WHERE e.id IN (SELECT i.ent_id FROM item i WHERE i.v > 5)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "WITH t AS (SELECT e.id AS eid FROM entity e) SELECT t.eid FROM t WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = t.eid AND i.v > 5)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT x.eid FROM (SELECT e.id AS eid FROM entity e) x WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = x.eid AND i.v > 5)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT p.a, p.b FROM pair p WHERE EXISTS (SELECT 1 FROM pair_item q WHERE q.pa = p.a AND q.pb = p.b)",
            "p:unit,q:unit,^pair:unit",
        ),
        (
            "SELECT p.a, p.b FROM pair p WHERE EXISTS (SELECT 1 FROM pair_item q WHERE p.b = q.pb AND p.a = q.pa)",
            "p:unit,q:unit,^pair:unit",
        ),
        // The parent first, another table joined after it (JOIN or comma join).
        (
            "SELECT e.id FROM entity e JOIN link l ON l.ent_id = e.id WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "e:unit,l:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT e.id FROM entity e, link l WHERE l.ent_id = e.id AND EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "e:unit,l:unit,i:unit,^entity:unit",
        ),
        // The parent key not selected.
        (
            "SELECT e.v FROM entity e WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "e:unit,i:unit,^entity:unit",
        ),
        // Anti-joins: NOT EXISTS = NOT IN over never-NULL columns = its CTE / derived forms.
        (
            "SELECT e.id FROM entity e WHERE NOT EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT e.id FROM entity e WHERE e.id NOT IN (SELECT i.ent_id FROM item i)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "WITH t AS (SELECT e.id AS eid FROM entity e) SELECT t.eid FROM t WHERE t.eid NOT IN (SELECT i.ent_id FROM item i)",
            "e:unit,i:unit,^entity:unit",
        ),
        (
            "SELECT x.eid FROM (SELECT e.id AS eid FROM entity e) x WHERE x.eid NOT IN (SELECT i.ent_id FROM item i)",
            "e:unit,i:unit,^entity:unit",
        ),
    ] {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.tracing(), tracing, "{sql}");
    }
    // NOT IN over a nullable child key keeps its NULL branches (no direct units): the units a
    // cascade changes are not known.
    assert_eq!(
        plan("SELECT e.id FROM entity e WHERE e.id NOT IN (SELECT i.ent_id FROM item_n i)").mode,
        Mode::Full(
            "IN subquery over `item_n`: the units a cascade from `entity` changes are not known"
                .into()
        )
    );
    // A grouped view keyed on the parent key.
    let v = plan("SELECT e.id, COUNT(*) AS n FROM entity e WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id) GROUP BY e.id");
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.tracing(), "e:unit,i:unit,^entity:unit");
    // Not correlated on the unit through the whole key, so Full: part of a composite key, a group
    // key that is an expression of the parent key, a cascade that continues out of the subquery's
    // child (2 and 3 hops). (The parent after another table: known_limit_unit_follows_from_order.)
    for (sql, parent, child) in [
        (
            "SELECT p.a, p.b FROM pair p WHERE EXISTS (SELECT 1 FROM pair_item q WHERE q.pa = p.a)",
            "pair",
            "pair_item",
        ),
        // A group key that is an expression of the parent key gives no direct units.
        (
            "SELECT e.id + 0 AS k, COUNT(*) AS n FROM entity e WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id) GROUP BY e.id + 0",
            "entity",
            "item",
        ),
        // A cascade that continues out of the subquery's child (2 and 3 hops).
        (
            "SELECT e.id FROM entity e WHERE EXISTS (SELECT 1 FROM item i JOIN item_part q ON q.item_id = i.id WHERE i.ent_id = e.id)",
            "item",
            "item_part",
        ),
        (
            "SELECT e.id FROM entity e WHERE EXISTS (SELECT 1 FROM item i JOIN item_part q ON q.item_id = i.id JOIN item_part_sub r ON r.part_id = q.id WHERE i.ent_id = e.id)",
            "item",
            "item_part",
        ),
        (
            "SELECT i.id FROM item i WHERE EXISTS (SELECT 1 FROM item_part q WHERE q.item_id = i.id)",
            "entity",
            "item_part",
        ),
    ] {
        assert_eq!(
            plan(sql).mode,
            Mode::Full(format!(
                "EXISTS subquery over `{child}`: the units a cascade from `{parent}` changes are not known"
            )),
            "{sql}"
        );
    }
}

/// KNOWN LIMIT (pre-existing, conservative; tracked for the key-map rewrite): semi- and
/// anti-joins are extracted from WHERE conjuncts only, so the same predicate in an inner join's ON
/// stays a subquery and the view is Full, while its WHERE form is incremental. Lifting the limit
/// flips these assertions on purpose.
#[test]
fn known_limit_on_form_semi_join_full() {
    let cat = formulation_catalog();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    for (on, wh) in [
        (
            "SELECT l.id FROM link l JOIN entity e ON e.id = l.ent_id AND e.id NOT IN (SELECT a.ent_id FROM attr a WHERE a.v > 5)",
            "SELECT l.id FROM link l JOIN entity e ON e.id = l.ent_id WHERE e.id NOT IN (SELECT a.ent_id FROM attr a WHERE a.v > 5)",
        ),
        (
            "SELECT e.id FROM entity e JOIN link l ON l.ent_id = e.id AND EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "SELECT e.id FROM entity e JOIN link l ON l.ent_id = e.id WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
        ),
    ] {
        assert_eq!(plan(on).mode, Mode::Full("subquery".into()), "{on}");
        assert_eq!(plan(wh).mode, Mode::Spj, "{wh}");
    }
}

/// KNOWN LIMIT (pre-existing, conservative; tracked for the key-map rewrite): the unit of a
/// select/join view is the first table's key, so a semi-join over a cascading child correlated on
/// a parent that is not first in FROM is not on the unit and the view is Full, while the same view
/// with the parent first is incremental (`^parent:unit`). Lifting the limit flips these
/// assertions on purpose.
#[test]
fn known_limit_unit_follows_from_order() {
    let cat = formulation_catalog();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    let why =
        "EXISTS subquery over `item`: the units a cascade from `entity` changes are not known";
    for (parent_first, parent_later) in [
        (
            "SELECT e.id FROM entity e JOIN link l ON l.ent_id = e.id WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "SELECT e.id FROM link l JOIN entity e ON l.ent_id = e.id WHERE EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
        ),
        (
            "SELECT e.id FROM entity e, link l WHERE l.ent_id = e.id AND EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
            "SELECT e.id FROM link l, entity e WHERE l.ent_id = e.id AND EXISTS (SELECT 1 FROM item i WHERE i.ent_id = e.id)",
        ),
    ] {
        let v = plan(parent_first);
        assert_eq!(v.mode, Mode::Spj, "{parent_first}");
        assert_eq!(v.tracing(), "e:unit,l:unit,i:unit,^entity:unit", "{parent_first}");
        assert_eq!(plan(parent_later).mode, Mode::Full(why.into()), "{parent_later}");
    }
}

/// Guard R4 on 7g: never_null reads the column's own join kind, every later join of its FROM
/// entry, and (for a derived table / CTE) the derived item's position in the outer FROM and the
/// output it maps to. Each `x NOT IN (…)` formulation below gets the same NOT EXISTS form and route
/// as its written NOT EXISTS twin: the column's table followed by an inner, comma or LEFT join;
/// the cascade forms with another table after the parent; a derived table / CTE inner- or
/// comma-joined after another table; a multi-table inner block (the column on the head or on an
/// inner-joined table); the column's table or a derived table in the middle of a chain, two or
/// three joins from the unit; the NOT IN column not selected and not the first output.
#[test]
fn not_in_twins_across_join_placements() {
    let cat = formulation_catalog();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &cat).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
    };
    // (template with {P} for the predicate on x, x, subquery table, its key column, tracing)
    let attr = ("attr a WHERE a.v > 5", "a.ent_id");
    let item = ("item i WHERE TRUE", "i.ent_id");
    for (template, x, (from, key), tracing) in [
        ("SELECT e.id FROM entity e JOIN link l ON l.ent_id = e.id WHERE {P}", "e.id", attr, "e:unit,l:unit,a:unit"),
        ("SELECT e.id FROM entity e, link l WHERE l.ent_id = e.id AND {P}", "e.id", attr, "e:unit,l:unit,a:unit"),
        ("SELECT e.id FROM entity e LEFT JOIN link l ON l.ent_id = e.id WHERE {P}", "e.id", attr, "e:unit,l:unit,a:unit"),
        ("SELECT e.id FROM entity e JOIN link l ON l.ent_id = e.id WHERE {P}", "e.id", item, "e:unit,l:unit,i:unit,^entity:unit"),
        ("SELECT e.id FROM entity e, link l WHERE l.ent_id = e.id AND {P}", "e.id", item, "e:unit,l:unit,i:unit,^entity:unit"),
        ("SELECT l.id FROM link l JOIN (SELECT e.id AS eid FROM entity e) x ON x.eid = l.ent_id WHERE {P}", "x.eid", attr, "l:unit,e:lineage,a:lookup"),
        ("SELECT l.id FROM link l, (SELECT e.id AS eid FROM entity e) x WHERE x.eid = l.ent_id AND {P}", "x.eid", attr, "l:unit,e:lineage,a:lookup"),
        ("WITH t AS (SELECT e.id AS eid FROM entity e) SELECT l.id FROM link l JOIN t ON t.eid = l.ent_id WHERE {P}", "t.eid", attr, "l:unit,e:lineage,a:lookup"),
        ("SELECT x.lid FROM (SELECT l.id AS lid, e.id AS eid FROM link l JOIN entity e ON e.id = l.ent_id) x WHERE {P}", "x.eid", attr, "l:unit,e:lineage,a:lookup"),
        ("SELECT x.eid FROM (SELECT e.id AS eid, l.id AS lid FROM entity e JOIN link l ON l.ent_id = e.id) x WHERE {P}", "x.eid", attr, "e:unit,l:unit,a:unit"),
        // The column's table (or a derived table) in the middle of a chain: inner-joined and
        // followed by an inner, comma or LEFT join; and two or three joins away from the unit.
        (
            "SELECT l.id FROM link l JOIN entity e ON e.id = l.ent_id JOIN attr b ON b.ent_id = e.id WHERE {P}",
            "e.id",
            attr,
            "l:unit,e:lineage,b:lineage,a:lookup",
        ),
        (
            "SELECT l.id FROM link l JOIN entity e ON e.id = l.ent_id, attr b WHERE b.ent_id = e.id AND {P}",
            "e.id",
            attr,
            "l:unit,e:lineage,b:lineage,a:lookup",
        ),
        (
            "SELECT l.id FROM link l JOIN entity e ON e.id = l.ent_id LEFT JOIN attr b ON b.ent_id = e.id WHERE {P}",
            "e.id",
            attr,
            "l:unit,e:lineage,b:lineage,a:lookup",
        ),
        (
            "SELECT l.id FROM link l JOIN (SELECT e.id AS eid FROM entity e) x ON x.eid = l.ent_id JOIN attr b ON b.ent_id = x.eid WHERE {P}",
            "x.eid",
            attr,
            "l:unit,e:lineage,b:lineage,a:lookup",
        ),
        (
            "SELECT m.id FROM link2 m JOIN link l ON l.id = m.link_id JOIN entity e ON e.id = l.ent_id WHERE {P}",
            "e.id",
            attr,
            "m:unit,l:lineage,e:lineage,a:lookup",
        ),
        (
            "SELECT n.id FROM link3 n JOIN link2 m ON m.id = n.link2_id JOIN link l ON l.id = m.link_id JOIN entity e ON e.id = l.ent_id WHERE {P}",
            "e.id",
            attr,
            "n:unit,m:lineage,l:lineage,e:lineage,a:lookup",
        ),
        // The NOT IN column not selected and not the derived block's first output.
        ("SELECT e.v FROM entity e WHERE {P}", "e.id", attr, "e:unit,a:unit"),
        ("SELECT x.v FROM (SELECT e.v AS v, e.id AS eid FROM entity e) x WHERE {P}", "x.eid", attr, "e:unit,a:unit"),
        ("WITH t AS (SELECT e.v AS v, e.id AS eid FROM entity e) SELECT t.v FROM t WHERE {P}", "t.eid", attr, "e:unit,a:unit"),
    ] {
        let (table, cond) = from.split_once(" WHERE ").unwrap();
        let col = key.split_once('.').unwrap();
        let not_in = template.replace("{P}", &format!("{x} NOT IN (SELECT {key} FROM {table} WHERE {cond})"));
        let twin = template.replace(
            "{P}",
            &format!("NOT EXISTS (SELECT 1 FROM {table} WHERE {cond} AND {}.{} = {x})", col.0, col.1),
        );
        let (a, b) = (plan(&not_in), plan(&twin));
        assert_eq!(a.mode, Mode::Spj, "{not_in}");
        assert_eq!(b.mode, Mode::Spj, "{twin}");
        assert_eq!(a.tracing(), tracing, "{not_in}");
        assert_eq!(b.tracing(), tracing, "{twin}");
        let w = a.where_sql.unwrap_or_default();
        assert!(w.contains("NOT EXISTS") && !w.contains("IS NULL"), "{not_in}: {w}");
    }
    // A derived table on the NULL side of an outer LEFT JOIN keeps the NULL-aware NOT IN form.
    let sql = "SELECT l.id FROM link l LEFT JOIN (SELECT e.id AS eid FROM entity e) x ON x.eid = l.ent_id \
               WHERE x.eid NOT IN (SELECT a.ent_id FROM attr a WHERE a.v > 5)";
    let w = plan(sql).where_sql.unwrap_or_default();
    assert!(w.contains("IS NOT NULL AND NOT EXISTS"), "{sql}: {w}");
}
