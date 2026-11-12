use super::*;

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
    let mut cat = wide_catalog();
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
    let mut cat = wide_catalog();
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
    let mut cat = wide_catalog();
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
    let mut cat = wide_catalog();
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
