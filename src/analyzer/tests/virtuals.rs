use super::*;

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
    let mut cat = wide_catalog();
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
    let mut cat = wide_catalog();
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
