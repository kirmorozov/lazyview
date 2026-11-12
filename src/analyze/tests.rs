use super::*;
use crate::catalog::test_catalog::{catalog, table, typed, VIEW_CORPUS};
use crate::catalog::Column;

fn full_reason(sql: &str) -> String {
    match p(sql).mode {
        Mode::Full(r) => r,
        m => panic!("{sql}: expected Full, got {m:?}"),
    }
}

fn p(sql: &str) -> ViewPlan {
    let d = parse_definition(sql).unwrap();
    plan_block(&d, "db", &catalog()).unwrap()
}

/// The shared corpus is these tests' own statements, so they are checked here too: the
/// analyzer's differential test compares its plans against the block planner's, and a
/// statement that stopped reaching this planner would otherwise go unnoticed. Refusing one
/// is an outcome (some name tables this catalog has no entry for); panicking is not.
#[test]
fn every_statement_of_the_shared_corpus_is_planned() {
    let cat = catalog();
    for sql in VIEW_CORPUS {
        let Ok(d) = parse_definition(sql) else {
            continue;
        };
        if let Err(e) = plan_block(&d, "db", &cat) {
            assert!(!e.to_string().is_empty(), "{sql}: refused without a reason");
        }
    }
}

/// The flag that makes a lookup bind its strings without a `COLLATE` (see `engine`): the
/// definition's own `COLLATE`, wherever it sits.
#[test]
fn a_definition_with_an_explicit_collate_is_marked() {
    let cat: HashMap<_, _> = [
        table("c", &[("id", "int"), ("name", "varchar")], &[0]),
        table("s_bin", &[("code", "varchar")], &[0]),
    ]
    .into_iter()
    .collect();
    let marked = |cat: &HashMap<TableRef, Arc<TableInfo>>, sql: &str| {
        crate::analyzer::plan(
            &parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap(),
            "db",
            cat,
        )
        .unwrap()
        .has_explicit_collate
    };
    assert!(
        marked(&cat, "SELECT c.id FROM c WHERE c.name COLLATE utf8mb4_general_ci IN (SELECT b.code FROM s_bin b)"),
        "a COLLATE of its own in the WHERE"
    );
    assert!(
        !marked(
            &cat,
            "SELECT c.id FROM c WHERE c.name IN (SELECT b.code FROM s_bin b)"
        ),
        "the same view without it"
    );
    assert!(
        marked(
            &cat,
            "SELECT c.id FROM c ORDER BY c.name COLLATE utf8mb4_general_ci"
        ),
        "the ORDER BY counts as much as the WHERE"
    );
    assert!(
        marked(
            &cat,
            "SELECT c.id, b.code COLLATE utf8mb4_general_ci AS cd FROM c \
             JOIN s_bin b ON b.code COLLATE utf8mb4_general_ci = c.name"
        ),
        "a COLLATE in a JOIN ... ON"
    );
    assert!(
        marked(
            &cat,
            "SELECT c.id, c.name COLLATE utf8mb4_general_ci AS nm FROM c"
        ),
        "a COLLATE in the projection"
    );
    assert!(
        marked(
            &cat,
            "SELECT c.name COLLATE utf8mb4_general_ci AS nm, COUNT(*) AS n FROM c \
             GROUP BY c.name COLLATE utf8mb4_general_ci"
        ),
        "a COLLATE in the GROUP BY"
    );
    assert!(
        marked(
            &cat,
            "SELECT c.id FROM c GROUP BY c.id \
             HAVING MAX(c.name COLLATE utf8mb4_general_ci) = 'x'"
        ),
        "a COLLATE in the HAVING"
    );
    assert!(
        marked(
            &cat,
            "SELECT c.id, (SELECT b.code COLLATE utf8mb4_general_ci FROM s_bin b LIMIT 1) AS x FROM c"
        ),
        "a COLLATE in a standalone subquery"
    );
    assert!(
        marked(
            &cat,
            "WITH s AS (SELECT b.code COLLATE utf8mb4_general_ci AS cd FROM s_bin b) \
             SELECT t.cd FROM s AS t JOIN c ON c.name = t.cd"
        ),
        "a COLLATE in a view wrapped in a CTE / derived table with renamed aliases"
    );
    // The only COLLATE comes from an inlined VIRTUAL column: its inlined string form carries
    // an EXPLICIT collation the definition as written never shows (inline_virtual_columns).
    let mut gn = Column::new(
        "gn",
        "varchar",
        "varchar(64)",
        Some("utf8mb4"),
        Some("utf8mb4_bin"),
        None,
        true,
    );
    gn.generated = Some(crate::catalog::Generated {
        expr: "(CAST(`code` AS CHAR))".into(),
        stored: false,
        deps: Some(vec![0]),
    });
    let vcat: HashMap<TableRef, Arc<TableInfo>> = [(
        ("db".to_string(), "skus".to_string()),
        Arc::new(TableInfo {
            schema: "db".into(),
            name: "skus".into(),
            columns: vec![Column::simple("code", "varchar"), gn],
            pk: vec![0],
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        }),
    )]
    .into_iter()
    .collect();
    assert!(
        marked(&vcat, "SELECT gn FROM skus"),
        "the inlined form of a VIRTUAL column carries a COLLATE of its own"
    );
    assert!(
        !marked(&vcat, "SELECT code FROM skus"),
        "the same table read without the VIRTUAL column"
    );
}

/// Constant filters per FROM alias: `col = lit`, `lit = col`, `col <=> lit` and IN lists of
/// integer literals over integer columns, from the WHERE, an inner join's ON or the alias's
/// own LEFT JOIN condition, carried through equalities; nothing that converts, negates,
/// combines or reads other tables.
#[test]
fn const_filters_per_alias() {
    use std::collections::BTreeMap;

    let int = |n: &str, ct: &str| {
        Column::new(
            n,
            ct.split(' ').next().unwrap(),
            ct,
            None,
            None,
            None,
            false,
        )
    };
    let mut virt = int("g", "int");
    virt.generated = Some(crate::catalog::Generated {
        expr: "(`oid` + 1)".into(),
        stored: false,
        deps: Some(vec![1]),
    });
    let mut stored = int("gs", "int");
    stored.generated = Some(crate::catalog::Generated {
        expr: "(`oid` * 2)".into(),
        stored: true,
        deps: Some(vec![1]),
    });
    let cat: HashMap<_, _> = [
        typed("obj", vec![int("id", "int")], &[0]),
        typed(
            "obj_attr",
            vec![
                int("id", "int"),
                int("oid", "int unsigned"),
                int("aid", "smallint unsigned"),
                int("scope", "smallint"),
                int("big", "bigint unsigned"),
                Column::simple("code", "varchar"),
                Column::new(
                    "bin",
                    "varbinary",
                    "varbinary(8)",
                    None,
                    None,
                    Some(8),
                    false,
                ),
                Column::simple("price", "decimal"),
                Column::simple("ts", "datetime"),
                Column::simple("yr", "year"),
                Column::simple("flag", "bit"),
                Column::new(
                    "kind",
                    "enum",
                    "enum('a','b')",
                    Some("utf8mb4"),
                    Some("utf8mb4_general_ci"),
                    None,
                    false,
                ),
                virt,
                stored,
            ],
            &[0],
        ),
    ]
    .into_iter()
    .collect();
    // Per alias name: its filters as `column=values`, sorted.
    let filters = |sql: &str| -> BTreeMap<String, Vec<String>> {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        let v = crate::analyzer::plan(&d, "db", &cat).unwrap();
        assert!(
            v.is_incremental() || v.mode == Mode::Global,
            "{sql}: {:?}",
            v.mode
        );
        (0..v.aliases.len())
            .map(|a| {
                let t = &v.aliases[a].table;
                let mut f: Vec<String> = v
                    .filters_of(a)
                    .iter()
                    .map(|f| format!("{}={:?}", t.columns[f.col].name, f.values))
                    .collect();
                f.sort();
                (v.aliases[a].name.clone(), f)
            })
            .collect()
    };
    let none = |sql: &str| {
        let f = filters(sql);
        assert!(f.values().all(Vec::is_empty), "{sql}: {f:?}");
    };
    let map = |entries: &[(&str, &[&str])]| -> BTreeMap<String, Vec<String>> {
        entries
            .iter()
            .map(|(a, f)| (a.to_string(), f.iter().map(|s| s.to_string()).collect()))
            .collect()
    };
    let from = "FROM obj o JOIN obj_attr a ON a.oid = o.id";
    // Inner ON, LEFT JOIN ON, both operand orders, `<=>`, IN lists, the WHERE; a constant
    // reaches the columns equal to the filtered one (o.id -> a.oid, b.oid).
    assert_eq!(
        filters(&format!(
            "SELECT o.id, a.scope, b.scope AS s {from} AND a.aid = 7 AND 3 = a.scope \
             LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid IN (7, 9) AND b.scope <=> 4 \
             WHERE a.big = 18446744073709551615 AND o.id = -1 AND (b.scope = +4) AND a.gs = 4"
        )),
        map(&[
            ("o", &["id=[Int(-1)]"]),
            (
                "a",
                &[
                    "aid=[Int(7)]",
                    "big=[UInt(18446744073709551615)]",
                    "gs=[Int(4)]",
                    "oid=[Int(-1)]",
                    "scope=[Int(3)]",
                ]
            ),
            (
                "b",
                &["aid=[Int(7), Int(9)]", "oid=[Int(-1)]", "scope=[Int(4)]"]
            ),
        ])
    );
    // Grouped and global views alike; parentheses, signs and booleans are literals.
    assert_eq!(
        filters(&format!(
            "SELECT o.id, COUNT(*) AS n {from} AND ((a.aid) = (7)) AND a.scope = -(3) \
             WHERE a.big IN (TRUE, 2) GROUP BY o.id"
        ))["a"],
        ["aid=[Int(7)]", "big=[Int(1), Int(2)]", "scope=[Int(-3)]"]
    );
    assert_eq!(
        filters(&format!("SELECT COUNT(*) AS n {from} WHERE a.aid = 7"))["a"],
        ["aid=[Int(7)]"]
    );
    // Equivalent formulations give the same filters per alias: join order, which ON or the
    // WHERE holds a condition, comma joins, `=` vs a one-element IN vs swapped sides, the
    // view wrapped in a CTE or a derived table with other output names.
    let base = "SELECT o.id, a.big AS x, b.big AS y FROM obj o \
                JOIN obj_attr a ON a.oid = o.id AND a.aid = 7 \
                LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid IN (8) AND b.scope = 2 \
                WHERE a.scope = 2";
    let want = filters(base);
    assert_eq!(
        want,
        map(&[
            ("o", &[]),
            ("a", &["aid=[Int(7)]", "scope=[Int(2)]"]),
            ("b", &["aid=[Int(8)]", "scope=[Int(2)]"]),
        ])
    );
    for sql in [
        "SELECT o.id, a.big AS x, b.big AS y FROM obj_attr a \
         JOIN obj o ON o.id = a.oid AND 7 = a.aid AND a.scope IN (2) \
         LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid = 8 AND 2 = b.scope",
        "SELECT o.id, a.big AS x, b.big AS y FROM obj o, obj_attr a \
         LEFT JOIN obj_attr b ON b.oid = a.oid AND b.scope = 2 AND b.aid = 8 \
         WHERE a.oid = o.id AND a.scope = 2 AND a.aid = 7",
        "SELECT o.id, a.big AS x, b.big AS y FROM obj o \
         JOIN obj_attr a ON a.oid = o.id \
         LEFT JOIN obj_attr b ON b.oid = o.id AND b.aid IN (8) AND b.scope = 2 \
         WHERE a.scope = 2 AND a.aid IN (7)",
        &format!("WITH w AS ({base}) SELECT id AS ident, x AS xx, y FROM w"),
        &format!("SELECT d.id AS ident, d.x FROM ({base}) d"),
        "SELECT o.id, a.big AS x, b.big AS y FROM obj o \
         JOIN (SELECT * FROM obj_attr WHERE aid = 7 AND scope = 2) a ON a.oid = o.id \
         LEFT JOIN (SELECT * FROM obj_attr WHERE scope = 2 AND aid = 8) b ON b.oid = o.id",
    ] {
        // Per alias; only where the normalizer renamed a merged derived table's aliases,
        // the filter sets regardless of names.
        let got = filters(sql);
        if got.keys().eq(want.keys()) {
            assert_eq!(got, want, "{sql}");
        } else {
            let sets = |m: &BTreeMap<String, Vec<String>>| {
                let mut v: Vec<Vec<String>> = m.values().cloned().collect();
                v.sort();
                v
            };
            assert_eq!(sets(&got), sets(&want), "{sql}");
        }
    }
    // Constants carried through equalities of the alias's rows (inner ON / WHERE, its own
    // LEFT JOIN condition), not through another LEFT JOIN's.
    assert_eq!(
        filters(
            "SELECT o.id FROM obj o JOIN obj_attr a ON a.oid = o.id \
             JOIN obj_attr b ON b.oid = o.id AND b.aid = a.aid \
             LEFT JOIN obj_attr c ON c.oid = o.id AND c.scope = a.scope WHERE a.aid = 7 AND a.scope = 2"
        ),
        map(&[
            ("o", &[]),
            ("a", &["aid=[Int(7)]", "scope=[Int(2)]"]),
            ("b", &["aid=[Int(7)]"]),
            ("c", &["scope=[Int(2)]"]),
        ])
    );
    assert_eq!(
        filters("SELECT o.id FROM obj o JOIN obj_attr a ON a.oid = o.id AND o.id = 5"),
        map(&[("o", &["id=[Int(5)]"]), ("a", &["oid=[Int(5)]"])])
    );
    // Not filters: OR, NOT, IS NULL, functions, expressions, other columns or tables,
    // NULLs, non-integer literals, converting kinds.
    for cond in [
        "(a.aid = 7 OR a.aid = 9)",
        "NOT a.aid = 7",
        "a.aid NOT IN (7)",
        "a.aid <> 7",
        "a.aid IS NULL",
        "a.aid IS NOT NULL",
        "ABS(a.scope) = 1",
        "a.aid + 0 = 7",
        "a.aid = a.scope",
        "a.aid = o.id",
        "a.aid IN (7, NULL)",
        "a.aid <=> NULL",
        "a.aid = NULL",
        "a.aid = '7'",
        "a.aid = 7.0",
        "a.aid = 1e2",
        "a.aid = 0x61",
        "a.aid BETWEEN 1 AND 2",
        "a.aid > 6",
        "a.big = 99999999999999999999999",
        "a.code = 'x'",
        "a.bin = 'x'",
        "a.price = 5",
        "a.ts = 20200101",
        "a.yr = 2020",
        "a.flag = 1",
        "a.kind = 1",
        "a.g = 2",
    ] {
        none(&format!("SELECT o.id, a.scope {from} WHERE {cond}"));
        none(&format!("SELECT o.id, a.scope {from} AND {cond}"));
    }
    // A LEFT JOIN keeps its preserved rows whatever its condition says about them.
    none("SELECT o.id, a.scope FROM obj o LEFT JOIN obj_attr a ON a.oid = o.id AND o.id = 5");
    // A semi-join over a unique key becomes a join: its table's own conditions are its filters.
    assert_eq!(
        filters(&format!(
            "SELECT o.id, a.scope {from} WHERE a.aid IN (SELECT x.id FROM obj x WHERE x.id = 3)"
        ))["x"],
        ["id=[Int(3)]"]
    );
}

/// Partial direct units: an alias that is neither a unit nor a lineage alias but whose row
/// equals some unit expressions (through inner-join, WHERE or its own LEFT JOIN equalities,
/// same type family, exact kinds, any integer width or sign) gives that part of the unit, for
/// every formulation of the same query; select/join views keep lineage.
#[test]
fn partial_units_per_alias() {
    use std::collections::BTreeMap;

    let int = |n: &str, ct: &str| {
        Column::new(
            n,
            ct.split(' ').next().unwrap(),
            ct,
            None,
            None,
            None,
            false,
        )
    };
    let cat: HashMap<_, _> = [
        typed(
            "link",
            vec![
                int("gid", "int"),
                int("oid", "mediumint unsigned"),
                int("pos", "int"),
            ],
            &[0, 1],
        ),
        typed(
            "obj",
            vec![int("oid", "bigint"), Column::simple("label", "varchar")],
            &[0],
        ),
        typed(
            "obj_attr",
            vec![
                int("id", "int"),
                int("oid", "int unsigned"),
                int("aid", "smallint unsigned"),
                int("val", "int"),
            ],
            &[0],
        ),
        typed(
            "grp_attr",
            vec![
                int("id", "int"),
                int("gid", "smallint unsigned"),
                int("aid", "tinyint"),
                int("val", "int"),
            ],
            &[0],
        ),
        typed(
            "gen_attr",
            vec![int("id", "int"), int("src", "int unsigned"), {
                let mut g = int("oid", "int unsigned");
                g.generated = Some(crate::catalog::Generated {
                    expr: "`src`".into(),
                    stored: true,
                    deps: Some(vec![1]),
                });
                g
            }],
            &[0],
        ),
    ]
    .into_iter()
    .collect();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        crate::analyzer::plan(&d, "db", &cat).unwrap()
    };
    // Per alias name: unit / lineage / lookup / partial with the unit positions it gives.
    let routes = |sql: &str| -> BTreeMap<String, String> {
        let v = plan(sql);
        assert_eq!(v.mode, Mode::Grouped, "{sql}");
        (0..v.aliases.len())
            .map(|a| {
                let how = if v.aliases[a].direct_unit.is_some() {
                    "unit".to_string()
                } else if v.lineage_of(a).is_some() {
                    "lineage".to_string()
                } else if let Some((set, _)) = v.partial_of(a) {
                    format!("partial{:?}", v.partial.sets[set])
                } else {
                    "lookup".to_string()
                };
                (v.aliases[a].name.clone(), how)
            })
            .collect()
    };
    let map = |entries: &[(&str, &str)]| -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(a, h)| (a.to_string(), h.to_string()))
            .collect()
    };
    // Each alias gives the part of (group, object) its row carries: the object's attribute
    // and the object itself the second, the group's attribute (own LEFT JOIN condition) the
    // first; a table joined on a non-key value does not.
    let v = plan(
        "SELECT c.gid, o.oid, MAX(a.val) AS s, MAX(ga.val) AS t, MAX(x.val) AS x FROM link c \
         JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
         LEFT JOIN grp_attr ga ON ga.gid = c.gid AND ga.aid = 4 \
         LEFT JOIN obj_attr x ON x.val = a.val GROUP BY c.gid, o.oid",
    );
    assert_eq!(
        v.tracing(),
        "c:unit,o:partial,a:partial,ga:partial,x:lookup"
    );
    assert_eq!(v.partial.sets, vec![vec![1], vec![0]]);
    assert_eq!(v.partial_of(1), Some((0, &[0usize][..])));
    assert_eq!(v.partial_of(2), Some((0, &[1usize][..])));
    assert_eq!(v.partial_of(3), Some((1, &[1usize][..])));
    assert_eq!(v.partial_of(4), None);
    // The same routes for every formulation: join order, the carrying equality in the
    // WHERE or the ON, comma joins, the joins in a CTE or a derived table with other
    // output names, the group column not selected (a hidden unit column).
    let want = map(&[("c", "unit"), ("o", "partial[1]"), ("a", "partial[1]")]);
    for sql in [
        "SELECT c.gid, o.oid, MAX(a.val) AS s FROM link c JOIN obj o ON o.oid = c.oid \
         JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY c.gid, o.oid",
        "SELECT c.gid, o.oid, MAX(a.val) AS s FROM obj_attr a JOIN obj o ON o.oid = a.oid \
         JOIN link c ON c.oid = o.oid WHERE a.aid = 7 GROUP BY c.gid, o.oid",
        "SELECT c.gid, o.oid, MAX(a.val) AS s FROM link c JOIN obj o JOIN obj_attr a \
         ON a.aid = 7 WHERE o.oid = c.oid AND a.oid = o.oid GROUP BY c.gid, o.oid",
        "SELECT c.gid, o.oid, MAX(a.val) AS s FROM link c, obj o, obj_attr a \
         WHERE o.oid = c.oid AND a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, o.oid",
        "WITH w AS (SELECT c.gid AS g, o.oid AS ob, a.val AS v FROM link c \
         JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7) \
         SELECT g AS grp, ob AS object, MAX(v) AS s FROM w GROUP BY g, ob",
        "SELECT d.g, d.ob, MAX(d.v) AS s FROM (SELECT c.gid AS g, o.oid AS ob, a.val AS v \
         FROM link c JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid \
         WHERE a.aid = 7) d GROUP BY d.g, d.ob",
        "SELECT o.oid AS object, MAX(a.val) AS s FROM link c JOIN obj o ON o.oid = c.oid \
         JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 GROUP BY c.gid, o.oid",
    ] {
        assert_eq!(routes(sql), want, "{sql}");
    }
    // A generated column (STORED too) is not read from images: no partial part from it.
    assert_eq!(
        routes(
            "SELECT c.gid, o.oid, COUNT(*) AS n FROM link c JOIN obj o ON o.oid = c.oid \
             JOIN gen_attr g ON g.oid = o.oid GROUP BY c.gid, o.oid"
        ),
        map(&[("c", "unit"), ("o", "partial[1]"), ("g", "lookup")])
    );
    // An expression group key gives no partial position; the plain key next to it does.
    assert_eq!(
        routes(
            "SELECT c.gid + 0 AS g, o.oid, MAX(a.val) AS s, MAX(ga.val) AS t FROM link c \
             JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
             LEFT JOIN grp_attr ga ON ga.gid = c.gid GROUP BY c.gid + 0, o.oid"
        ),
        map(&[
            ("c", "partial[1]"),
            ("o", "partial[1]"),
            ("a", "partial[1]"),
            ("ga", "lookup"),
        ])
    );
    // Another type family does not carry the unit; a full match is a unit alias.
    assert_eq!(
        plan(
            "SELECT c.gid, o.oid, COUNT(*) AS n FROM link c JOIN obj o ON o.oid = c.oid \
             JOIN obj_attr a ON a.oid = o.label GROUP BY c.gid, o.oid"
        )
        .tracing(),
        "c:unit,o:partial,a:lookup"
    );
    let v = plan(
        "SELECT a.oid, a.aid, COUNT(*) AS n FROM obj_attr a \
         JOIN obj_attr b ON b.oid = a.oid AND b.aid = a.aid GROUP BY a.oid, a.aid",
    );
    assert_eq!(v.tracing(), "a:unit,b:unit");
    assert!(v.partial.sets.is_empty() && v.partial.aliases.is_empty());
    // Select/join: lineage stays; single-column units have no partial part.
    assert_eq!(
        plan(
            "SELECT c.gid, c.oid, a.val FROM link c JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7"
        )
        .tracing(),
        "c:unit,a:lineage"
    );
    assert_eq!(
        plan(
            "SELECT o.oid, MAX(a.val) AS v FROM obj o JOIN obj_attr a ON a.val = o.oid + 1 \
             GROUP BY o.oid"
        )
        .tracing(),
        "o:unit,a:lookup"
    );
}

/// Cascades the parent's row still bounds: a parent read through a partial alias traces
/// them; a child alias whose partial unit columns are FK columns gets partial keys from the
/// parent's key; a child alias pinned by constant filters on FK columns only needs the
/// parent rows that pass them; anything else, and chains through tables the view does not
/// read, still rebuild.
#[test]
fn cascades_derived_from_partial_ids_and_filters() {
    use crate::catalog::{FkAction, ForeignKey};
    let int = |n: &str, ct: &str| {
        Column::new(
            n,
            ct.split(' ').next().unwrap(),
            ct,
            None,
            None,
            None,
            false,
        )
    };
    let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols: vec![child_col],
        parent_cols: vec![parent_col.into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
        let mut i = (*t.1).clone();
        i.cascades = f;
        (t.0, Arc::new(i))
    };
    let cat: HashMap<_, _> = [
        typed("realm", vec![int("rid", "int unsigned")], &[0]),
        with_fk(
            typed(
                "kind",
                vec![int("kid", "smallint unsigned"), int("rid", "int unsigned")],
                &[0],
            ),
            vec![fk("realm", 1, "rid")],
        ),
        typed(
            "obj",
            vec![int("oid", "int unsigned"), int("w", "int")],
            &[0],
        ),
        typed(
            "link",
            vec![
                int("gid", "int"),
                int("oid", "int unsigned"),
                int("pos", "int"),
            ],
            &[0, 1],
        ),
        with_fk(
            typed(
                "obj_attr",
                vec![
                    int("id", "int"),
                    int("oid", "int unsigned"),
                    int("aid", "smallint unsigned"),
                    int("val", "int"),
                ],
                &[0],
            ),
            vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")],
        ),
    ]
    .into_iter()
    .collect();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        crate::analyzer::plan(&d, "db", &cat).unwrap()
    };
    let route = |v: &ViewPlan, parent: &str| -> CascadeRoute {
        v.cascades
            .iter()
            .find(|c| c.parent.name == parent)
            .unwrap_or_else(|| panic!("{parent}: {:?}", v.cascades))
            .clone()
    };
    // The parent read through a partial alias: traced. The value-key parent is bounded
    // by the aliases' filters; its own parent (a chain) still rebuilds.
    let v = plan(
        "SELECT c.gid, o.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
         JOIN obj o ON o.oid = c.oid JOIN obj_attr a ON a.oid = o.oid AND a.aid = 7 \
         LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid IN (8, 9) GROUP BY c.gid, o.oid",
    );
    assert!(route(&v, "obj").traced, "{:?}", v.cascades);
    let k = route(&v, "kind");
    assert!(k.derived && k.untraced.is_none(), "{k:?}");
    assert!(k.partial_from_parent.is_empty());
    let kid = k.parent.col_index("kid").unwrap();
    // One count per pinned value (grouped view), named by the plan.
    assert_eq!(
        k.pinned,
        vec![
            Pinned {
                parent_col: kid,
                values: vec![(Datum::Int(7), 0)]
            },
            Pinned {
                parent_col: kid,
                values: vec![(Datum::Int(8), 1), (Datum::Int(9), 2)]
            },
        ]
    );
    assert_eq!(
        v.presence
            .iter()
            .map(|p| p.expr.clone())
            .collect::<Vec<_>>(),
        [
            "COUNT(CASE WHEN `a`.`aid` = 7 THEN 1 END)",
            "COUNT(CASE WHEN `b`.`aid` = 8 THEN 1 END)",
            "COUNT(CASE WHEN `b`.`aid` = 9 THEN 1 END)"
        ]
    );
    assert_eq!(v.presence[1].marks(&Datum::Int(8)), "`_lv_c1` > 0");
    assert_eq!(v.presence[2].holds(&Datum::Int(9)), "`b`.`aid` = 9");
    assert!(v
        .select_sql(&[], None)
        .contains("COUNT(CASE WHEN `a`.`aid` = 7 THEN 1 END) AS `_lv_c0`"));
    assert!(!route(&v, "realm").derived && route(&v, "realm").untraced.is_some());
    assert!(v.tracing().contains("^obj:traced") && v.tracing().contains("^kind:derived"));
    // Every alias of the child pins the FK: the keys that can reach the view at all. The
    // chain through `kind` (a table the view does not read) has no bound.
    assert_eq!(
        k.reach,
        Some((kid, vec![Datum::Int(7), Datum::Int(8), Datum::Int(9)]))
    );
    assert_eq!(route(&v, "realm").reach, None);
    // The parent not read: the child alias's partial unit is its FK column, so the
    // parent's key gives partial keys.
    let v = plan(
        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c JOIN obj_attr a ON a.oid = c.oid \
         AND a.aid = 7 GROUP BY c.gid, c.oid",
    );
    let o = route(&v, "obj");
    assert!(o.derived, "{o:?}");
    assert_eq!(
        o.partial_from_parent,
        vec![(0, vec![o.parent.col_index("oid").unwrap()])]
    );
    assert_eq!(v.partial.sets, vec![vec![1]]);
    // An unpinned child alias without a partial unit on the FK: still a rebuild route.
    let v = plan(
        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c JOIN obj_attr a ON a.val = c.pos \
         GROUP BY c.gid, c.oid",
    );
    assert!(route(&v, "kind").untraced.is_some() && !route(&v, "kind").derived);
    assert!(route(&v, "obj").untraced.is_some() && !route(&v, "obj").derived);
    assert_eq!(route(&v, "kind").reach, None);
    // A HAVING over a NULL-side pinned alias: removing its rows could let a group the table
    // does not hold appear, so presence cannot find it: the route stays untraced, but only
    // the pinned key reaches the view, and the reason says so.
    let v = plan(
        "SELECT c.gid, o.oid, MAX(b.val) AS y FROM link c JOIN obj o ON o.oid = c.oid \
         LEFT JOIN obj_attr b ON b.oid = o.oid AND b.aid = 8 GROUP BY c.gid, o.oid \
         HAVING MAX(b.val) IS NULL",
    );
    let k = route(&v, "kind");
    assert!(k.untraced.is_some() && !k.derived);
    assert!(v.presence.is_empty());
    assert_eq!(k.reach, Some((kid, vec![Datum::Int(8)])));
    assert!(
        k.untraced
            .as_deref()
            .is_some_and(|r| r.ends_with(" (only `kind`.`kid` IN (8) reaches it)")),
        "{k:?}"
    );
    // One alias of the child pinned, another not: no bound.
    let v = plan(
        "SELECT c.gid, c.oid, MAX(a.val) AS x, MAX(b.val) AS y FROM link c \
         JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 LEFT JOIN obj_attr b ON b.oid = c.oid \
         GROUP BY c.gid, c.oid",
    );
    assert_eq!(route(&v, "kind").reach, None);
    // A pinned child whose own cascade continues into an alias not joined to it on that
    // FK: the rows a key outside the pins removes there may be read, so no bound; joined
    // on the FK, the bound holds.
    let v = plan(
        "SELECT a.id, a.val, k.kid FROM obj_attr a JOIN kind k ON k.kid = a.val AND k.rid = 3",
    );
    assert_eq!(route(&v, "realm").reach, None);
    let v = plan(
        "SELECT a.id, a.val, k.kid FROM obj_attr a JOIN kind k ON k.kid = a.aid AND k.rid = 3",
    );
    let r = route(&v, "realm");
    assert_eq!(r.reach, Some((0, vec![Datum::Int(3)])), "{r:?}");
    // A unit alias whose unit is not the FK: covered only by its filters on the FK.
    let v = plan("SELECT a.id, a.val FROM obj_attr a WHERE a.aid = 7");
    assert!(v.aliases[0].direct_unit.is_some());
    let k = route(&v, "kind");
    assert!(k.derived && k.pinned.len() == 1, "{k:?}");
    // Select/join: the FK value itself, selected by value.
    assert_eq!(v.presence.len(), 1);
    assert_eq!(v.presence[0].expr, "`a`.`aid`");
    assert_eq!(v.presence[0].marks(&Datum::Int(7)), "`_lv_c0` = 7");
    assert!(route(&v, "obj").untraced.is_some() && !route(&v, "obj").derived);
}

/// Derived cascade routes do not depend on how the query is written: the same routes, pins
/// and partial keys for permuted joins, the FK equality in the WHERE or the ON (comma joins
/// too), the pin as `=`, a one-element IN or with swapped sides, the view wrapped in a CTE or
/// a derived table with renamed outputs, and the target id not selected. A pin on a
/// composite FK is not followed (presence needs one FK column): untraced. The keys that can
/// reach the view (`reach`) are the same per formulation too, also on an untraced route.
#[test]
fn derived_cascades_per_formulation() {
    use crate::catalog::{FkAction, ForeignKey};
    let int = |n: &str, ct: &str| {
        Column::new(
            n,
            ct.split(' ').next().unwrap(),
            ct,
            None,
            None,
            None,
            false,
        )
    };
    let fk = |parent: &str, child_cols: Vec<usize>, parent_cols: &[&str]| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols,
        parent_cols: parent_cols.iter().map(|c| c.to_string()).collect(),
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
        let mut i = (*t.1).clone();
        i.cascades = f;
        (t.0, Arc::new(i))
    };
    let cat: HashMap<_, _> = [
        typed("kind", vec![int("kid", "smallint unsigned")], &[0]),
        typed("pair", vec![int("p1", "int"), int("p2", "int")], &[0, 1]),
        typed("obj", vec![int("oid", "int unsigned")], &[0]),
        typed(
            "link",
            vec![int("gid", "int"), int("oid", "int unsigned")],
            &[0, 1],
        ),
        with_fk(
            typed(
                "obj_attr",
                vec![
                    int("id", "int"),
                    int("oid", "int unsigned"),
                    int("aid", "smallint unsigned"),
                    int("p1", "int"),
                    int("p2", "int"),
                    int("val", "int"),
                ],
                &[0],
            ),
            vec![
                fk("obj", vec![1], &["oid"]),
                fk("kind", vec![2], &["kid"]),
                fk("pair", vec![3, 4], &["p1", "p2"]),
            ],
        ),
    ]
    .into_iter()
    .collect();
    // Per parent: how its cascade is followed (pins as their values, presence order apart).
    let routes = |sql: &str| -> BTreeMap<String, String> {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        let v = crate::analyzer::plan(&d, "db", &cat).unwrap();
        assert_eq!(v.mode, Mode::Grouped, "{sql}");
        v.cascades
            .iter()
            .map(|c| {
                let how = if c.unit_from_parent.is_some() {
                    "unit".to_string()
                } else if c.traced {
                    "traced".to_string()
                } else if c.derived {
                    let pins: Vec<Vec<Datum>> = c
                        .pinned
                        .iter()
                        .map(|p| p.values.iter().map(|(d, _)| d.clone()).collect())
                        .collect();
                    format!(
                        "derived partial={:?} pins={pins:?}",
                        c.partial_from_parent
                            .iter()
                            .map(|(s, _)| v.partial.sets[*s].clone())
                            .collect::<Vec<_>>()
                    )
                } else {
                    "rebuild".to_string()
                };
                let reach = c.reach.as_ref().map(|(_, v)| v);
                (c.parent.name.clone(), format!("{how} reach={reach:?}"))
            })
            .collect()
    };
    use std::collections::BTreeMap;
    let base = "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c \
                JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, c.oid";
    let want = routes(base);
    assert_eq!(want["obj"], "derived partial=[[1]] pins=[] reach=None");
    assert_eq!(
        want["kind"],
        "derived partial=[] pins=[[Int(7)]] reach=Some([Int(7)])"
    );
    assert_eq!(want["pair"], "rebuild reach=None");
    // Untraced (a HAVING over the NULL-side pinned alias) but bounded by the pin.
    let bounded = "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
                   ON b.oid = c.oid AND b.aid = 8 GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL";
    let want_bounded = routes(bounded);
    assert_eq!(want_bounded["kind"], "rebuild reach=Some([Int(8)])");
    for sql in [
        "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
         ON 8 = b.aid AND c.oid = b.oid GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
        "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
         ON b.oid = c.oid AND b.aid IN (8) GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
        "SELECT c.gid, c.oid, MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b \
         ON c.oid IN (b.oid) AND b.aid = 8 GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
        "SELECT MAX(b.val) AS y FROM link c LEFT JOIN obj_attr b ON b.oid = c.oid \
         AND b.aid = 8 GROUP BY c.gid, c.oid HAVING MAX(b.val) IS NULL",
        "SELECT d.g, d.ob, MAX(d.v) AS y FROM (SELECT c.gid AS g, c.oid AS ob, b.val AS v \
         FROM link c LEFT JOIN obj_attr b ON b.oid = c.oid AND b.aid = 8) d \
         GROUP BY d.g, d.ob HAVING MAX(d.v) IS NULL",
    ] {
        assert_eq!(routes(sql), want_bounded, "{sql}");
    }
    for sql in [
        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM obj_attr a JOIN link c ON c.oid = a.oid \
         WHERE a.aid = 7 GROUP BY c.gid, c.oid",
        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c, obj_attr a \
         WHERE a.oid = c.oid AND a.aid IN (7) GROUP BY c.gid, c.oid",
        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c JOIN obj_attr a \
         ON 7 = a.aid AND c.oid = a.oid GROUP BY c.gid, c.oid",
        "WITH w AS (SELECT c.gid AS g, c.oid AS ob, a.val AS v FROM link c \
         JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7) \
         SELECT g AS grp, ob AS object, MAX(v) AS x FROM w GROUP BY g, ob",
        "SELECT d.g, d.ob, MAX(d.v) AS x FROM (SELECT c.gid AS g, c.oid AS ob, a.val AS v \
         FROM link c JOIN obj_attr a ON a.oid = c.oid WHERE a.aid = 7) d GROUP BY d.g, d.ob",
        "SELECT MAX(a.val) AS x FROM link c JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 \
         GROUP BY c.gid, c.oid",
    ] {
        assert_eq!(routes(sql), want, "{sql}");
    }
}

/// Cascades no derivation follows keep their child rows in the cascade map: a child read
/// through a unit alias whose unit is not the FK, a child read through a lookup alias (kept
/// only where that alias's filters pass), and a level the view does not read (a chain). The
/// map stores the key, the FK and what the child's routes take values from, the same for
/// every equivalent formulation. A child without a key is not kept: its route stays a
/// rebuild route, and its reason says why.
#[test]
fn cascade_maps_keep_untraced_children() {
    use crate::catalog::{FkAction, ForeignKey};
    use std::collections::BTreeMap;
    let int = |n: &str, ct: &str| {
        Column::new(
            n,
            ct.split(' ').next().unwrap(),
            ct,
            None,
            None,
            None,
            false,
        )
    };
    let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols: vec![child_col],
        parent_cols: vec![parent_col.into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
        let mut i = (*t.1).clone();
        i.cascades = f;
        (t.0, Arc::new(i))
    };
    let cat: HashMap<_, _> = [
        typed("realm", vec![int("rid", "int unsigned")], &[0]),
        with_fk(
            typed(
                "kind",
                vec![int("kid", "smallint unsigned"), int("rid", "int unsigned")],
                &[0],
            ),
            vec![fk("realm", 1, "rid")],
        ),
        typed("grp", vec![int("gid", "int unsigned")], &[0]),
        typed("obj", vec![int("oid", "int unsigned")], &[0]),
        with_fk(
            typed(
                "link",
                vec![
                    int("gid", "int unsigned"),
                    int("oid", "int unsigned"),
                    int("pos", "int"),
                ],
                &[0, 1],
            ),
            vec![fk("obj", 1, "oid")],
        ),
        with_fk(
            typed(
                "obj_attr",
                vec![
                    int("id", "bigint unsigned"),
                    int("oid", "int unsigned"),
                    int("aid", "smallint unsigned"),
                    int("val", "int"),
                ],
                &[0],
            ),
            vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")],
        ),
        with_fk(
            typed(
                "note",
                vec![int("oid", "int unsigned"), int("w", "int")],
                &[],
            ),
            vec![fk("obj", 0, "oid")],
        ),
    ]
    .into_iter()
    .collect();
    let plan = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        crate::analyzer::plan(&d, "db", &cat).unwrap()
    };
    // Per route (parent -> child): the map as (key, stored columns, filters), or the reason.
    let maps = |sql: &str| -> BTreeMap<String, String> {
        plan(sql)
            .cascades
            .iter()
            .map(|c| {
                let what = match (&c.map, &c.untraced) {
                    (Some(m), _) => format!(
                        "map key={:?} cols={:?} filters={:?}",
                        m.key,
                        m.cols,
                        m.filters
                            .iter()
                            .map(|f| f
                                .iter()
                                .map(|f| (f.col, f.values.clone()))
                                .collect::<Vec<_>>())
                            .collect::<Vec<_>>()
                    ),
                    (None, Some(r)) => format!("rebuild: {r}"),
                    (None, None) => "followed".into(),
                };
                (format!("{}->{}", c.parent.name, c.child.1), what)
            })
            .collect()
    };
    let base = "SELECT g.gid, COUNT(*) AS n, MAX(a.val) AS x FROM grp g \
                JOIN link c ON c.gid = g.gid JOIN obj_attr a ON a.oid = c.oid AND a.aid = 7 \
                GROUP BY g.gid";
    let want = maps(base);
    // `c`'s unit is the group, not its FK: its rows are kept whole (key = FK + unit).
    assert_eq!(want["obj->link"], "map key=[0, 1] cols=[0, 1] filters=[[]]");
    // `a` is reached through `c` (a lookup): kept where its filter passes; stored: key, FK,
    // the filter column and its image (the join column).
    assert_eq!(
        want["obj->obj_attr"],
        "map key=[0] cols=[0, 1, 2] filters=[[(2, [Int(7)])]]"
    );
    // Pinned on the key FK, without HAVING: derived (patch 3), not mapped.
    assert_eq!(want["kind->obj_attr"], "followed");
    // A level the view does not read: every row, with the parent column of the next level.
    assert_eq!(want["realm->kind"], "map key=[0] cols=[0, 1] filters=[]");
    let v = plan(base);
    assert!(v.tracing().contains("^obj:map") && v.tracing().contains("^realm:map"));
    for sql in [
        "SELECT g.gid, COUNT(*) AS n, MAX(a.val) AS x FROM obj_attr a \
         JOIN link c ON c.oid = a.oid JOIN grp g ON g.gid = c.gid WHERE a.aid = 7 GROUP BY g.gid",
        "SELECT g.gid, COUNT(*) AS n, MAX(a.val) AS x FROM grp g, link c, obj_attr a \
         WHERE c.gid = g.gid AND a.oid = c.oid AND a.aid IN (7) GROUP BY g.gid",
        "WITH w AS (SELECT g.gid AS k, a.val AS v FROM grp g JOIN link c ON c.gid = g.gid \
         JOIN obj_attr a ON a.oid = c.oid AND 7 = a.aid) SELECT k AS grp_id, COUNT(*) AS n, \
         MAX(v) AS x FROM w GROUP BY k",
    ] {
        assert_eq!(maps(sql), want, "{sql}");
    }
    // A child without a key: not kept, named.
    let m = maps(
        "SELECT g.gid, COUNT(*) AS n, MAX(t.w) AS x FROM grp g JOIN link c ON c.gid = g.gid \
         LEFT JOIN note t ON t.oid = c.oid GROUP BY g.gid",
    );
    assert!(
        m["obj->note"].starts_with("rebuild: ")
            && m["obj->note"].ends_with(
                "(`note` has no primary or NOT NULL unique key: the cascade map cannot keep its rows)"
            ),
        "{m:?}"
    );
}

#[test]
fn parse_noise() {
    let d = parse_definition(
        "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `db`.`v1` (a, b) AS SELECT id, status FROM orders WITH CASCADED CHECK OPTION;",
    )
    .unwrap();
    assert_eq!(d.name, "v1");
    assert_eq!(d.schema.as_deref(), Some("db"));
    assert_eq!(d.columns, vec!["a", "b"]);
    assert_eq!(d.query_sql, "SELECT id, status FROM orders");
    assert!(parse_definition("SELECT 1").is_err());
    // L-06 / L-07: what is wrong, in the user's terms (a plan error is final: no retries).
    let err = |sql: &str| format!("{:#}", parse_definition(sql).unwrap_err());
    let one = "the definition must be a single SELECT statement (or CREATE VIEW name AS SELECT …)";
    assert_eq!(err(""), format!("the definition is empty: {one}"));
    assert_eq!(
        err("  -- a comment\n/* only */ "),
        format!("the definition is empty: {one}")
    );
    assert_eq!(
        err("CREATE VIEW `v` AS "),
        format!("the definition is empty after AS: {one}")
    );
    assert_eq!(
        err("CREATE VIEW `lz_insert` AS INSERT INTO orders VALUES (9,'z',1)"),
        format!("{one}; found `INSERT` … after AS")
    );
    for (sql, word) in [
        ("CREATE VIEW `v` AS UPDATE orders SET a = 1", "UPDATE"),
        ("CREATE VIEW `v` AS DELETE FROM orders", "DELETE"),
        ("CREATE VIEW `v` AS CREATE TABLE t (a INT)", "CREATE"),
        ("CREATE VIEW `v` AS DROP TABLE orders", "DROP"),
    ] {
        assert_eq!(
            err(sql),
            format!("{one}; found `{word}` … after AS"),
            "{sql}"
        );
    }
    assert_eq!(
        err("INSERT INTO orders VALUES (1)"),
        format!("{one}; found `INSERT` …")
    );
    // G3: a query without CREATE VIEW comes from a row without a name.
    for sql in [
        "SELECT num, COUNT(*) AS n FROM c GROUP BY num",
        "WITH t AS (SELECT 1 AS a) SELECT a FROM t",
        "(SELECT 1)",
    ] {
        assert_eq!(
            err(sql),
            "the definition needs a name: set `_lazyview.name`, or write CREATE VIEW name AS SELECT …",
            "{sql}"
        );
    }
    assert_eq!(
        err("CREATE TABLE t (a int)"),
        format!("{one}; found CREATE without VIEW")
    );
    assert_eq!(
        err("CREATE VIEW `v` AS SELECT id FROM orders; SELECT id FROM orders"),
        format!("the definition contains 2 statements: {one}")
    );
    assert_eq!(
        err("CREATE VIEW `v` AS SELECT 1 FROM t; SELECT 2 FROM t;\n SELECT 3 FROM t;"),
        format!("the definition contains 3 statements: {one}")
    );
    assert_eq!(
        err("CREATE VIEW v AS SELECT id, @@session.sql_mode AS m FROM t"),
        "a view cannot read user or system variables (`@@session.sql_mode`): the servers refuse them in views"
    );
    assert!(err("CREATE VIEW v AS SELECT id, @x AS x FROM t").contains("`@x`"));
    assert_eq!(
        err("CREATE VIEW v AS SELECT id FROM t LIMIT 2 ROWS EXAMINED 100"),
        "unexpected `ROWS` after the end of the SELECT statement"
    );
    // Accepted: CRLF line ends, a trailing semicolon (or two), comments, a string with `;`.
    for sql in [
        "CREATE VIEW v AS\r\nSELECT id\r\nFROM t\r\nWHERE id > 1;\r\n",
        "CREATE VIEW v AS SELECT id FROM t;;",
        "CREATE VIEW v AS SELECT id, 'a;b' AS s FROM t -- trailing; comment",
        "CREATE VIEW v AS SELECT `@x` FROM t",
    ] {
        assert!(parse_definition(sql).is_ok(), "{sql}");
    }
    assert!(parse_definition("CREATE TABLE t (a int)").is_err());
    assert!(parse_definition("CREATE VIEW v AS SELECT 1 garbage garbage").is_err());
}

/// Full-refresh views watch only the columns their statement names (E1a: an update of
/// `orders.note` must not rebuild a DISTINCT view over `orders.status`).
#[test]
fn full_views_read_columns() {
    let cols = |sql: &str| {
        let v = p(sql);
        assert!(matches!(v.mode, Mode::Full(_)), "{sql}: {:?}", v.mode);
        v.tables
            .iter()
            .zip(&v.table_columns)
            .map(|(t, c)| (t.1.clone(), c.clone()))
            .collect::<Vec<_>>()
    };
    let (t, f) = (true, false);
    assert_eq!(
        cols("CREATE VIEW v AS SELECT DISTINCT status FROM orders"),
        vec![("orders".into(), Some(vec![f, f, t, f, f]))]
    );
    // Subqueries, ORDER BY, USING and aliases all count; names match any watched table.
    assert_eq!(
        cols(
            "CREATE VIEW v AS SELECT c.name FROM customers c JOIN regions USING (id) \
             WHERE c.id IN (SELECT customer_id FROM orders WHERE total > 5) ORDER BY region_id LIMIT 3"
        ),
        vec![
            ("customers".into(), Some(vec![t, t, t])),
            ("orders".into(), Some(vec![t, t, f, t, f])),
            ("regions".into(), Some(vec![t, t])),
        ]
    );
    // Implicit column reads: any column counts.
    for sql in [
        "CREATE VIEW v AS SELECT DISTINCT * FROM orders",
        "CREATE VIEW v AS SELECT DISTINCT o.* FROM orders o",
        "CREATE VIEW v AS SELECT DISTINCT id FROM orders NATURAL JOIN order_items",
        "CREATE VIEW v AS SELECT id FROM orders UNION SELECT * FROM (SELECT id FROM regions) x",
    ] {
        assert!(cols(sql).iter().all(|(_, c)| c.is_none()), "{sql}");
    }
}

/// E6: select/join views trace lookup tables through hidden lineage key columns (their
/// primary key per materialized row); their images carry only that key.
#[test]
fn spj_lineage_keys() {
    let v = p(
        "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
         JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
    );
    assert_eq!(
        v.lineage,
        vec![
            LineageKey {
                alias: 1,
                cols: vec!["_lv_a1_pk0".into()],
                exprs: vec!["`c`.`id`".into()],
            },
            LineageKey {
                alias: 2,
                cols: vec!["_lv_a2_pk0".into()],
                exprs: vec!["`r`.`id`".into()],
            },
        ]
    );
    assert_eq!(
        (v.aliases[1].image_cols.clone(), v.lineage_width()),
        (vec![0], 2)
    );
    assert_eq!(v.tracing(), "o:unit,c:lineage,r:lineage");
    assert!(v.select_sql(&[], None).ends_with(
        "`c`.`id` AS `_lv_a1_pk0`, `r`.`id` AS `_lv_a2_pk0` FROM orders o \
         JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id"
    ));
    assert_eq!(
        v.key_lookup_sql("`c`.`id` IN (1, 2)"),
        "SELECT DISTINCT `o`.`id` FROM orders o JOIN customers c ON c.id = o.customer_id \
         LEFT JOIN regions r ON r.id = c.region_id WHERE `c`.`id` IN (1, 2)"
    );
    // Composite keys; a key that the query does not read still counts as read (a key-only
    // UPDATE changes the hidden columns).
    let v = p(
        "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.customer_id",
    );
    assert_eq!(v.lineage[0].cols, vec!["_lv_a1_pk0", "_lv_a1_pk1"]);
    assert_eq!(v.lineage[0].exprs, vec!["`i`.`order_id`", "`i`.`line`"]);
    assert_eq!(v.aliases[1].image_cols, vec![0, 1]);
    assert!(v.aliases[1].referenced[1], "order_items.line");
    // Without a primary key the whole row is the key.
    let v = p("CREATE VIEW v AS SELECT o.id, l.msg FROM orders o JOIN logs l ON l.msg = o.status");
    assert_eq!(v.lineage[0].exprs, vec!["`l`.`msg`"]);
    assert_eq!(v.aliases[1].image_cols, vec![0]);
    // Tables whose rows map to units directly, grouped views and single tables: none.
    for sql in [
        "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
        "CREATE VIEW v AS SELECT c.name, COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name",
        "CREATE VIEW v AS SELECT id, status FROM orders",
    ] {
        assert!(p(sql).lineage.is_empty(), "{sql}");
    }
}

/// Rows of a select/join view are identified by the unit plus the lineage keys only when
/// every other table carries its primary key as lineage (upserts by that identity).
#[test]
fn spj_rows_identified_by_unit_and_lineage() {
    for sql in [
        "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
         JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
        "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.customer_id",
        "CREATE VIEW v AS SELECT id, status FROM orders",
    ] {
        assert!(p(sql).rows_identified(), "{sql}");
    }
    for sql in [
        // order_items maps to units directly: no lineage, several rows per unit.
        "CREATE VIEW v AS SELECT o.id, i.qty FROM orders o JOIN order_items i ON i.order_id = o.id",
        // logs has no primary key: whole-row lineage, equal rows are duplicates.
        "CREATE VIEW v AS SELECT o.id, l.msg FROM orders o JOIN logs l ON l.msg = o.status",
        "CREATE VIEW v AS SELECT c.name, COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name",
        "CREATE VIEW v AS SELECT COUNT(*) AS n FROM orders",
        // A first table without a row key: equal rows share their unit.
        "CREATE VIEW v AS SELECT msg FROM logs",
    ] {
        assert!(!p(sql).rows_identified(), "{sql}");
    }
}

/// E1c: a cascading FK whose parent the view joins on the FK (through a lineage or
/// direct-unit alias) is traced by the parent's own rows; one whose parent the view does not
/// read (or joins otherwise) still rebuilds.
#[test]
fn cascades_traced_by_the_parent_alias() {
    use crate::catalog::{FkAction, ForeignKey};
    let fk = |parent: &str, child_cols: Vec<usize>| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols,
        parent_cols: vec!["id".into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    let mut cat = catalog();
    let with_fk = |t: (TableRef, Arc<TableInfo>), f: ForeignKey| {
        let mut i = (*t.1).clone();
        i.cascades = vec![f];
        (t.0, Arc::new(i))
    };
    cat.extend([
        table("grps", &[("id", "int"), ("name", "varchar")], &[0]),
        with_fk(
            table(
                "pars",
                &[("id", "int"), ("grp_id", "int"), ("label", "varchar")],
                &[0],
            ),
            fk("grps", vec![1]),
        ),
        with_fk(
            table(
                "kids",
                &[("par_id", "int"), ("seq", "int"), ("qty", "int")],
                &[0, 1],
            ),
            fk("pars", vec![0]),
        ),
    ]);
    let routes = |sql: &str| {
        let d = parse_definition(sql).unwrap();
        let v = plan_block(&d, "db", &cat).unwrap();
        let mut r: Vec<(String, bool)> = v
            .cascades
            .iter()
            .map(|c| (c.parent.name.clone(), c.traced))
            .collect();
        r.sort();
        r
    };
    assert_eq!(
        routes(
            "CREATE VIEW v AS SELECT k.par_id, k.seq, p.label, g.name FROM kids k \
             JOIN pars p ON p.id = k.par_id LEFT JOIN grps g ON g.id = p.grp_id"
        ),
        vec![("grps".into(), true), ("pars".into(), true)]
    );
    // The parent is not read, or not joined on the FK: rebuilt.
    assert_eq!(
        routes("CREATE VIEW v AS SELECT par_id, seq, qty FROM kids"),
        vec![("grps".into(), false), ("pars".into(), false)]
    );
    assert_eq!(
        routes(
            "CREATE VIEW v AS SELECT k.par_id, k.seq, p.label FROM kids k JOIN pars p ON p.grp_id = k.qty"
        ),
        vec![("grps".into(), false), ("pars".into(), false)]
    );
}

/// A1: a cascade is traced only where a child row's presence in a materialized row implies
/// the parent alias carries its parent. A parent on the NULL side of a LEFT JOIN with a
/// condition beyond the key equality (or joined through another alias) keeps child rows
/// whose parent fails it, NULL-extended: their cascading deletes would go unseen. The
/// condition must hold down every further cascade; cycles are never traced.
#[test]
fn cascades_traced_only_where_the_child_row_implies_its_parent() {
    use crate::catalog::{FkAction, ForeignKey};
    let fk = |parent: &str, child_cols: Vec<usize>| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols,
        parent_cols: vec!["id".into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    let mut cat = catalog();
    let with_fk = |t: (TableRef, Arc<TableInfo>), f: Vec<ForeignKey>| {
        let mut i = (*t.1).clone();
        i.cascades = f;
        (t.0, Arc::new(i))
    };
    cat.extend([
        table("grps", &[("id", "int"), ("name", "varchar")], &[0]),
        with_fk(
            table(
                "pars",
                &[
                    ("id", "int"),
                    ("grp_id", "int"),
                    ("active", "tinyint"),
                    ("name", "varchar"),
                ],
                &[0],
            ),
            vec![fk("grps", vec![1])],
        ),
        with_fk(
            table(
                "kids",
                &[("id", "int"), ("par_id", "int"), ("qty", "int")],
                &[0],
            ),
            vec![fk("pars", vec![1])],
        ),
        table("ords", &[("id", "int"), ("kid_id", "int")], &[0]),
        with_fk(
            table(
                "nodes",
                &[("id", "int"), ("parent_id", "int"), ("name", "varchar")],
                &[0],
            ),
            vec![fk("nodes", vec![1])],
        ),
        with_fk(
            table(
                "gkids",
                &[("id", "int"), ("kid_id", "int"), ("v", "int")],
                &[0],
            ),
            vec![fk("kids", vec![1])],
        ),
        with_fk(
            table("ma", &[("id", "int"), ("b_id", "int")], &[0]),
            vec![fk("mb", vec![1])],
        ),
        with_fk(
            table("mb", &[("id", "int"), ("a_id", "int")], &[0]),
            vec![fk("ma", vec![1])],
        ),
    ]);
    let routes = |sql: &str| {
        let d = parse_definition(sql).unwrap();
        let v = plan_block(&d, "db", &cat).unwrap();
        assert!(v.is_incremental(), "{sql}: {}", v.mode);
        let mut r: Vec<(String, String, bool, String)> = v
            .cascades
            .iter()
            .map(|c| {
                // Unit-derived routes need no tracing; the others are traced or say why not.
                let unit = c.unit_from_parent.is_some();
                assert_eq!(c.traced, !unit && c.untraced.is_none(), "{sql}: {c:?}");
                (
                    c.parent.name.clone(),
                    c.child.1.clone(),
                    c.traced || unit,
                    c.untraced.clone().unwrap_or_default(),
                )
            })
            .collect();
        r.sort();
        r
    };
    // Every route of a parent the view reads is traced (one it does not read rebuilds).
    let traced = |sql: &str| {
        let rs = routes(sql);
        let read: Vec<&str> = ["grps", "pars", "nodes"]
            .into_iter()
            .filter(|t| sql.contains(&format!(" {t} ")))
            .collect();
        assert!(!read.is_empty());
        for r in rs.iter().filter(|r| read.contains(&r.0.as_str())) {
            assert!(r.2, "{sql}: {r:?}");
        }
    };
    let untraced = |sql: &str, parent: &str, why: &str| {
        let rs = routes(sql);
        let r = rs.iter().find(|r| r.0 == parent).unwrap();
        assert!(!r.2 && r.3.contains(why), "{sql}: {rs:?}");
    };
    // The parent on the NULL side, joined on exactly the key: the constraint makes the
    // join match whenever the key is non-NULL (an inner-join condition elsewhere is fine).
    traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id");
    traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON k.par_id = p.id WHERE k.qty > 1");
    traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON (p.id = k.par_id)");
    // The parent on the NULL side with a condition beyond the key (A1): untraced.
    let a1 = "whose condition is not the foreign-key equality alone";
    untraced(
        "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1",
        "pars",
        a1,
    );
    untraced(
        "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND k.qty > 1",
        "pars",
        a1,
    );
    untraced(
        "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.id > 1",
        "pars",
        a1,
    );
    // The same through a NULL-side child (A1, second shape).
    untraced(
        "CREATE VIEW v AS SELECT o.id, k.qty, p.name FROM ords o LEFT JOIN kids k ON k.id = o.kid_id \
         LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1",
        "pars",
        a1,
    );
    traced(
        "CREATE VIEW v AS SELECT o.id, k.qty, p.name FROM ords o LEFT JOIN kids k ON k.id = o.kid_id \
         LEFT JOIN pars p ON p.id = k.par_id",
    );
    // Inner joins: a child row is materialized only with its parent, whatever else the
    // condition asks; so is a child on the NULL side of its parent.
    traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k JOIN pars p ON p.id = k.par_id AND p.active = 1");
    traced("CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k, pars p WHERE p.id = k.par_id AND p.active = 1");
    traced("CREATE VIEW v AS SELECT p.id, p.name, k.qty FROM pars p LEFT JOIN kids k ON k.par_id = p.id AND k.qty > 2");
    // A NULL-side parent whose condition goes through a third alias is not the key
    // equality (the constraint says nothing about `o.pid`).
    untraced(
        "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k JOIN ords o ON o.kid_id = k.id \
         LEFT JOIN pars p ON p.id = o.id",
        "pars",
        "`p.id` = `o.id` restricts `p` further",
    );
    // Two aliases of the parent: one joined on the key is enough.
    traced(
        "CREATE VIEW v AS SELECT k.id, p.name, q.name AS other FROM kids k LEFT JOIN pars p ON p.id = k.par_id \
         LEFT JOIN pars q ON q.id = k.qty",
    );
    // Chains: groups → parents → kids. Traced only when every level is.
    traced(
        "CREATE VIEW v AS SELECT k.id, p.name, g.name AS grp FROM kids k JOIN pars p ON p.id = k.par_id \
         LEFT JOIN grps g ON g.id = p.grp_id",
    );
    untraced(
        "CREATE VIEW v AS SELECT k.id, p.name, g.name AS grp FROM kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1 \
         LEFT JOIN grps g ON g.id = p.grp_id",
        "grps",
        "continues from `pars` into `kids`, which is not traced",
    );
    untraced(
        "CREATE VIEW v AS SELECT p.id, p.name, g.name AS grp FROM pars p LEFT JOIN grps g ON g.id = p.grp_id AND g.name <> ''",
        "grps",
        a1,
    );
    // A self-referential cascade reaches every descendant: never traced (the alias read
    // as the parent is itself a child nobody is joined to as its parent).
    untraced(
        "CREATE VIEW v AS SELECT c.id, c.name, p.name AS parent FROM nodes c JOIN nodes p ON p.id = c.parent_id",
        "nodes",
        "`p` is not joined to `p` on the foreign key",
    );
    // A unit route (the parent's image gives the units) covers the next level only where
    // its rows are joined to the child's: here grandkids follow their kid, there they do
    // not (a deleted kid's grandkids sit in other units).
    {
        let d = parse_definition(
            "CREATE VIEW v AS SELECT k.par_id, COUNT(*) AS n FROM kids k \
             JOIN gkids g ON g.kid_id = k.id AND g.v > 0 GROUP BY k.par_id",
        )
        .unwrap();
        let v = plan_block(&d, "db", &cat).unwrap();
        assert_eq!(
            v.tracing(),
            "k:unit,g:lookup,^kids:traced,^pars:unit,^grps:map"
        );
        let d = parse_definition(
            "CREATE VIEW v AS SELECT k.par_id, COUNT(*) AS n FROM kids k \
             JOIN gkids g ON g.v = k.qty GROUP BY k.par_id",
        )
        .unwrap();
        let v = plan_block(&d, "db", &cat).unwrap();
        // Untraced: the cascade map keeps the rows of every level.
        assert_eq!(v.tracing(), "k:unit,g:lookup,^kids:map,^pars:map,^grps:map");
        let r = v.cascades.iter().find(|c| c.parent.name == "pars").unwrap();
        assert!(
            r.untraced.as_deref().is_some_and(|w| {
                w.contains("continues from `kids` into `gkids`")
                    && w.contains("`k` is not joined to `g` on the foreign key")
            }),
            "{r:?}"
        );
    }
    // A3: a self-referential cascade reaches every descendant, also with the unit on the
    // key column (the parent's image gives one level): never unit-derived, never traced;
    // the cascade map follows it level by level (each kept row once).
    {
        let d = parse_definition(
            "CREATE VIEW v AS SELECT n.parent_id, COUNT(*) AS n FROM nodes n GROUP BY n.parent_id",
        )
        .unwrap();
        let v = plan_block(&d, "db", &cat).unwrap();
        assert_eq!(v.tracing(), "n:unit,^nodes:map");
        assert!(v.cascades[0]
            .untraced
            .as_deref()
            .is_some_and(|w| w.contains("part of a cycle")));
    }
    // Mutual cascades, each level joined on its key: a cycle, never traced.
    let rs = routes(
        "CREATE VIEW v AS SELECT a.id, b.id AS b FROM ma a JOIN mb b ON b.id = a.b_id AND a.id = b.a_id",
    );
    assert!(
        rs.iter().all(|r| !r.2 && r.3.contains("part of a cycle")),
        "{rs:?}"
    );
    // The parent is read but weakly (a WHERE conjunct keeps its NULL-extended rows): no
    // lineage, so nothing to trace through.
    untraced(
        "CREATE VIEW v AS SELECT k.id, k.qty, p.name FROM kids k LEFT JOIN pars p ON p.id = k.par_id \
         WHERE p.id IS NULL OR p.active = 1",
        "pars",
        "does not read `pars` through a unit, partial or lineage alias",
    );
}

/// A2: only strict builtins and operators propagate NULL; everything else (CHAR, CHARSET,
/// COERCIBILITY, NVL2, DECODE_ORACLE, aggregates, CASE, `<=>`, logical operators) may yield
/// a value for NULL arguments, so NULL-side expressions over them are guarded and
/// predicates over them keep NULL-extended rows.
#[test]
fn null_propagation_is_an_allow_list() {
    let e = |sql: &str| parse_sql(sql, |p| p.parse_expr()).unwrap();
    for sql in [
        "c.x",
        "c.x + 1",
        "-c.x",
        "c.x = 'a'",
        "UPPER(c.x)",
        "CONCAT(c.x, 'a')",
        "DATE_FORMAT(c.at, '%Y')",
        "CAST(c.x AS CHAR)",
        "ROUND(c.x / 3, 2)",
        "GREATEST(c.x, c.y)",
        "c.doc->'$.a'",
    ] {
        assert!(null_propagating(&e(sql)), "{sql}");
    }
    for sql in [
        "CHAR(c.x)",
        "CHARSET(c.x)",
        "COERCIBILITY(c.x)",
        "COLLATION(c.x)",
        "NVL2(c.x, 'a', 'b')",
        "DECODE_ORACLE(c.x, NULL, 'x', 'y')",
        "IFNULL(c.x, 0)",
        "COALESCE(c.x, 0)",
        "IF(c.x > 1, 1, 0)",
        "CASE WHEN c.x IS NULL THEN 1 END",
        "c.x <=> 1",
        "c.x IS NULL",
        "c.x > 1 AND c.y > 1",
        "c.x > 1 OR c.y > 1",
        "COUNT(c.x)",
        "IS_IPV4(c.x)",
        "INTERVAL_FN(c.x)",
        "QUOTE(c.x)",
        "JSON_ARRAY(c.x)",
        // A7: a NULL mode / locale / length argument selects a default.
        "WEEK(c.at, c.x)",
        "YEARWEEK(c.at, c.x)",
        "FORMAT(c.x, 2, c.l)",
        "SHA2(c.x, c.l)",
    ] {
        assert!(!null_propagating(&e(sql)), "{sql}");
    }
    // `||` is OR to the server and concatenation to the parser: refused as written.
    let err = parse_definition("CREATE VIEW v AS SELECT (a > 1) || 1 AS v FROM t")
        .unwrap_err()
        .to_string();
    assert!(err.contains("`||` is OR on the server"), "{err}");
    assert!(parse_definition("CREATE VIEW v AS SELECT a OR 1 AS v FROM t").is_ok());
}

/// A4: `SELECT *` over USING / NATURAL joins is expanded as the server does it: the shared
/// columns once, first, from the preserved side, then the remaining columns of the left
/// side, then of the right table, join by join; shapes the expansion does not cover are
/// Full with a reason naming the fix.
#[test]
fn star_over_using_joins_expands_like_the_server() {
    let cat: HashMap<TableRef, Arc<TableInfo>> = [
        table("t_a", &[("k", "int"), ("x", "int")], &[0]),
        table("t_b", &[("k", "int"), ("y", "int")], &[0]),
        table("t_c", &[("y", "int"), ("z", "int")], &[0]),
    ]
    .into();
    let plan = |sql: &str| {
        let d = parse_definition(sql).unwrap();
        crate::analyzer::plan(&d, "db", &cat).unwrap()
    };
    for (sql, cols) in [
        (
            "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k)",
            vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
        ),
        (
            "CREATE VIEW v AS SELECT * FROM t_a LEFT JOIN t_b USING (k)",
            vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
        ),
        (
            "CREATE VIEW v AS SELECT * FROM t_a NATURAL JOIN t_b",
            vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
        ),
        // RIGHT JOIN: the mirrored LEFT JOIN, the right table's columns first.
        (
            "CREATE VIEW v AS SELECT * FROM t_a RIGHT JOIN t_b USING (k)",
            vec!["`t_b`.`k`", "`t_b`.`y`", "`t_a`.`x`"],
        ),
        // A chain: the second join's shared columns move to the front.
        (
            "CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k) JOIN t_c USING (y)",
            vec!["`t_b`.`y`", "`t_a`.`k`", "`t_a`.`x`", "`t_c`.`z`"],
        ),
        (
            "CREATE VIEW v AS SELECT t_a.x AS ax, * FROM t_a JOIN t_b USING (k)",
            vec!["t_a.x AS ax", "`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"],
        ),
    ] {
        let p = plan(sql);
        assert_eq!(p.mode, Mode::Spj, "{sql}: {}", p.mode);
        assert_eq!(p.projection, cols, "{sql}");
    }
    // The block planner alone (the path without rewrites) expands the same way.
    let d = parse_definition("CREATE VIEW v AS SELECT * FROM t_a JOIN t_b USING (k)").unwrap();
    let p = plan_block(&d, "db", &cat).unwrap();
    assert_eq!(p.mode, Mode::Spj);
    assert_eq!(p.projection, vec!["`t_a`.`k`", "`t_a`.`x`", "`t_b`.`y`"]);
    // Not covered (a derived table in the chain): Full, naming the fix.
    let p = plan("CREATE VIEW v AS SELECT * FROM t_a JOIN (SELECT k, y FROM t_b) d USING (k)");
    assert_eq!(p.mode, Mode::Full(STAR_USING.into()));
    assert_eq!(p.tables.len(), 2);
    for sql in [
        "CREATE VIEW v AS SELECT t_a.*, y FROM t_a JOIN t_b USING (k)",
        "CREATE VIEW v AS SELECT k, x, y FROM t_a LEFT JOIN t_b USING (k)",
    ] {
        let d = parse_definition(sql).unwrap();
        let p = crate::analyzer::plan(&d, "db", &cat).unwrap();
        assert_eq!(p.mode, Mode::Spj, "{sql}: {}", p.mode);
    }
}

/// L1: a top-N view's rows are chosen by its ORDER BY, so columns that appear only there
/// (plain, in expressions, as aggregate inputs, of joined aliases) are read columns: an
/// UPDATE of such a column must reach the recompute.
#[test]
fn order_by_columns_of_top_n_views_are_referenced() {
    let col = |v: &ViewPlan, alias: &str, c: &str| -> bool {
        let a = v.aliases.iter().find(|a| a.name == alias).unwrap();
        a.referenced[a.table.col_index(c).unwrap()]
    };
    for sql in [
        "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 2",
        "CREATE VIEW v AS SELECT id FROM orders ORDER BY total * 2 DESC LIMIT 2",
        "CREATE VIEW v AS SELECT id FROM orders ORDER BY -total LIMIT 2",
        "CREATE VIEW v AS SELECT id FROM orders ORDER BY ABS(total) DESC, id LIMIT 2",
        "CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 1 OFFSET 1",
        "CREATE VIEW v AS SELECT customer_id FROM orders GROUP BY customer_id ORDER BY SUM(total) DESC LIMIT 2",
    ] {
        let v = p(sql);
        assert_eq!(v.mode, Mode::Global, "{sql}");
        assert!(col(&v, "orders", "total"), "{sql}");
        assert!(!col(&v, "orders", "note"), "{sql}");
    }
    let v = p(
        "CREATE VIEW v AS SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id \
         ORDER BY c.name DESC, o.id LIMIT 2",
    );
    assert!(col(&v, "c", "name") && !col(&v, "o", "total"));
    // An alias or ordinal of a projected expression adds nothing new.
    let v = p("CREATE VIEW v AS SELECT id, total * 2 AS t FROM orders ORDER BY t DESC LIMIT 2");
    assert!(col(&v, "orders", "total"));
    let v = p("CREATE VIEW v AS SELECT id, status FROM orders ORDER BY 2 DESC LIMIT 2");
    assert!(col(&v, "orders", "status") && !col(&v, "orders", "total"));
    // Windows: PARTITION BY / ORDER BY columns inside OVER are read columns too.
    let v = p(
        "CREATE VIEW v AS SELECT id, SUM(total) OVER (PARTITION BY customer_id ORDER BY status) AS s FROM orders",
    );
    assert!(col(&v, "orders", "customer_id") && col(&v, "orders", "status"));
}

/// L2: a named WINDOW clause stays part of the refresh SELECT (the projection's `OVER w`
/// needs it), for windows used by several functions and windows refining another one.
#[test]
fn named_windows_are_kept_in_the_select() {
    let v = p(
        "CREATE VIEW v AS SELECT id, customer_id, ROW_NUMBER() OVER w AS rn, SUM(total) OVER w AS run \
         FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)",
    );
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(
        v.window_sql.as_deref(),
        Some("WINDOW w AS (PARTITION BY customer_id ORDER BY id)")
    );
    let sql = v.select_sql(&[0], None);
    assert!(
        sql.ends_with(" FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY id)"),
        "{sql}"
    );
    let v = p(
        "CREATE VIEW v AS SELECT id, SUM(total) OVER (w ORDER BY id) AS run FROM orders \
         WINDOW w AS (PARTITION BY customer_id)",
    );
    assert_eq!(v.mode, Mode::Grouped);
    assert!(v
        .select_sql(&[0], None)
        .ends_with(" WINDOW w AS (PARTITION BY customer_id)"));
    let v = p("CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER w AS rn FROM orders WINDOW w AS (ORDER BY id)");
    assert_eq!(v.mode, Mode::Global);
    assert!(v
        .select_sql(&[], None)
        .ends_with(" WINDOW w AS (ORDER BY id)"));
    assert!(p("CREATE VIEW v AS SELECT id, status FROM orders")
        .window_sql
        .is_none());
}

/// L3: duplicate output names (two plain columns or aliases with the same name, `*` over
/// tables sharing names) and an empty projection are refused when planning, by name,
/// instead of failing at CREATE TABLE and retrying forever; a column list renames.
#[test]
fn duplicate_output_names_are_refused() {
    let cat: HashMap<TableRef, Arc<TableInfo>> = [
        table("t_a", &[("k", "int"), ("x", "int")], &[0]),
        table("t_c", &[("k", "int"), ("z", "int"), ("x", "int")], &[0]),
    ]
    .into();
    let err = |sql: &str| {
        let d = parse_definition(sql).unwrap();
        let e = crate::analyzer::plan(&d, "db", &cat)
            .unwrap_err()
            .to_string();
        assert_eq!(
            plan_block(&d, "db", &cat).unwrap_err().to_string(),
            e,
            "{sql}"
        );
        e
    };
    assert!(
        err("CREATE VIEW v AS SELECT a.k, c.k FROM t_a a JOIN t_c c ON c.k = a.k")
            .contains("duplicate column name `k`")
    );
    assert!(
        err("CREATE VIEW v AS SELECT a.k, c.z AS K FROM t_a a JOIN t_c c ON c.k = a.k")
            .contains("duplicate column name `K`")
    );
    assert!(
        err("CREATE VIEW v AS SELECT * FROM t_a a JOIN t_c c ON c.k = a.k")
            .contains("duplicate column name `k`")
    );
    for sql in [
        "CREATE VIEW v AS SELECT a.k, c.k AS ck FROM t_a a JOIN t_c c ON c.k = a.k",
        "CREATE VIEW v (k1, k2) AS SELECT a.k, c.k FROM t_a a JOIN t_c c ON c.k = a.k",
        "CREATE VIEW v AS SELECT * FROM t_a JOIN t_c USING (k, x)",
    ] {
        let d = parse_definition(sql).unwrap();
        assert!(crate::analyzer::plan(&d, "db", &cat).is_ok(), "{sql}");
    }
}

/// Server schema: UNIQUE NOT NULL keys identify rows like the primary key (unit of a
/// PK-less anchor, lineage, FD reduction of a DISTINCT, the parent key of a cascade), and
/// a generated column is read through its base columns (a VIRTUAL one never a key).
#[test]
fn unique_keys_and_generated_columns() {
    use crate::catalog::Generated;
    let mut cat = catalog();
    let mut t = TableInfo {
        schema: "db".into(),
        name: "skus".into(),
        columns: vec![
            Column::simple("code", "varchar"),
            Column::simple("a", "int"),
            Column::simple("b", "int"),
            Column::simple("g", "int"),
            Column::simple("s", "int"),
            Column::simple("note", "text"),
        ],
        pk: vec![],
        unique_keys: vec![vec![0], vec![1, 2]],
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    };
    for c in &mut t.columns {
        c.nullable = c.name != "code" && c.name != "a" && c.name != "b";
    }
    t.columns[3].generated = Some(Generated {
        expr: "(`a` + `b`)".into(),
        stored: false,
        deps: Some(vec![1, 2]),
    });
    t.columns[4].generated = Some(Generated {
        expr: "(`a` * 2)".into(),
        stored: true,
        deps: Some(vec![1]),
    });
    cat.insert(("db".into(), "skus".into()), Arc::new(t));
    let plan = |sql: &str, cat: &HashMap<TableRef, Arc<TableInfo>>| {
        let d = parse_definition(sql).unwrap();
        crate::analyzer::plan(&d, "db", cat).unwrap()
    };
    // PK-less anchor with a unique key: the key is the unit, not the referenced columns.
    let v = plan("CREATE VIEW v AS SELECT note, a FROM skus", &cat);
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.unit_exprs, vec!["`skus`.`code`"]);
    assert!(v.unit_unique);
    // Lineage of a joined PK-less table through its unique key.
    let v = plan(
        "CREATE VIEW v AS SELECT o.id, s.note FROM orders o JOIN skus s ON s.code = o.status",
        &cat,
    );
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.lineage_of(1).unwrap().cols, vec!["_lv_a1_pk0"]);
    assert_eq!(v.lineage_of(1).unwrap().exprs, vec!["`s`.`code`"]);
    // DISTINCT over a unique key and other columns reduces to the key.
    let v = plan(
        "CREATE VIEW v AS SELECT DISTINCT code, note, s FROM skus",
        &cat,
    );
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.unit_exprs, vec!["code"]);
    let v = plan(
        "CREATE VIEW v AS SELECT DISTINCT a, b, note FROM skus",
        &cat,
    );
    assert_eq!(v.unit_exprs, vec!["a", "b"]);
    // A VIRTUAL generated column is inlined as its expression converted to its type (no
    // image value exists, B1); a STORED one is an ordinary column that reads its base
    // column too.
    let v = plan("CREATE VIEW v AS SELECT g, s FROM skus WHERE g > 1", &cat);
    assert_eq!(
        v.projection,
        vec![
            "(CAST(LEAST(GREATEST(CAST((`skus`.`a` + `skus`.`b`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED)) AS `g`",
            "s"
        ]
    );
    assert_eq!(
        v.where_sql.as_deref(),
        Some("(CAST(LEAST(GREATEST(CAST((`skus`.`a` + `skus`.`b`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED)) > 1")
    );
    let a = &v.aliases[0];
    assert!(a.referenced[1] && a.referenced[2] && a.referenced[4] && !a.referenced[3]);
    assert!(a.membership[1] && a.membership[2]);
    let v = plan("CREATE VIEW v AS SELECT DISTINCT g FROM skus", &cat);
    assert_eq!(v.unit_exprs, vec!["(CAST(LEAST(GREATEST(CAST((`skus`.`a` + `skus`.`b`) AS DECIMAL(65,0)), -2147483648), 2147483647) AS SIGNED))"]);
    let mut keyless = (*cat[&("db".to_string(), "skus".to_string())]).clone();
    keyless.unique_keys.clear();
    cat.insert(("db".into(), "skus".into()), Arc::new(keyless));
    let v = plan("CREATE VIEW v AS SELECT g, s FROM skus", &cat);
    assert_eq!(v.unit_exprs, vec!["`skus`.`a`", "`skus`.`b`", "`skus`.`s`"]);
    let mut unreadable = (*cat[&("db".to_string(), "skus".to_string())]).clone();
    unreadable.columns[3].generated.as_mut().unwrap().deps = None;
    cat.insert(("db".into(), "skus".into()), Arc::new(unreadable));
    let v = plan("CREATE VIEW v AS SELECT g FROM skus", &cat);
    assert!(
        matches!(&v.mode, Mode::Full(r) if r.starts_with("VIRTUAL generated column `skus`.`g`")),
        "{}",
        v.mode
    );
}

/// E6: lookups that replace several tables by their row images at once, with parameters in
/// the order the derived tables appear.
#[test]
fn multi_alias_lookup_sql() {
    let v = p(
        "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region FROM orders o \
         JOIN customers c ON c.id = o.customer_id LEFT JOIN regions r ON r.id = c.region_id",
    );
    let (c, r) = (1, 2);
    assert!(v.aliases[c].direct_unit.is_none() && v.aliases[r].direct_unit.is_none());
    let (sql, order) = v
        .lookup_sql_multi(&[(r, "(R)".into()), (c, "(C)".into())])
        .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT `o`.`id` FROM orders o JOIN (C) AS `c` ON c.id = o.customer_id \
         JOIN (R) AS `r` ON r.id = c.region_id"
    );
    assert_eq!(order, vec![1, 0]);
    // One alias: the same statement as the single-table lookup.
    let (one, _) = v.lookup_sql_multi(&[(r, "(R)".into())]).unwrap();
    assert_eq!(one, v.lookup_sql(r, "(R)"));
}

#[test]
fn single_table_spj() {
    let v = p("CREATE VIEW v AS SELECT id, status FROM orders WHERE total > 10");
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.unit_exprs, vec!["`orders`.`id`"]);
    assert_eq!(v.unit_in_projection, vec![Some(0)]);
    assert!(v.unit_unique);
    let a = &v.aliases[0];
    assert_eq!(a.direct_unit, Some(vec![0]));
    assert_eq!(a.referenced, vec![true, false, true, true, false]);
    assert_eq!(a.membership, vec![true, false, false, true, false]);
}

#[test]
fn join_direct_units_via_equalities() {
    let v = p(
        "CREATE VIEW v AS SELECT o.id, c.name, r.name AS region, i.qty \
               FROM orders o JOIN customers c ON c.id = o.customer_id \
               LEFT JOIN regions r ON r.id = c.region_id \
               LEFT JOIN order_items i ON i.order_id = o.id",
    );
    assert_eq!(v.mode, Mode::Spj);
    assert!(!v.unit_unique);
    let by = |n: &str| v.aliases.iter().find(|a| a.name == n).unwrap();
    assert_eq!(by("o").direct_unit, Some(vec![0]));
    assert_eq!(
        by("i").direct_unit,
        Some(vec![0]),
        "items map to unit via i.order_id = o.id"
    );
    assert_eq!(by("c").direct_unit, None, "customers need a lookup");
    assert_eq!(by("r").direct_unit, None);
    assert_eq!(by("c").subst_from.0, "orders o JOIN ");
    let look = v.lookup_sql(1, "(SELECT ? AS `id`)");
    assert!(
        look.starts_with(
            "SELECT DISTINCT `o`.`id` FROM orders o JOIN (SELECT ? AS `id`) AS `c` ON"
        ),
        "{look}"
    );
}

#[test]
fn one_element_in_is_an_equality() {
    let eq = p("CREATE VIEW v AS SELECT o.id, c.name FROM orders o \
                JOIN customers c ON c.id = o.customer_id");
    let in_on = p("CREATE VIEW v AS SELECT o.id, c.name FROM orders o \
                   JOIN customers c ON c.id IN (o.customer_id)");
    let in_swapped = p("CREATE VIEW v AS SELECT o.id, c.name FROM orders o \
                        JOIN customers c ON o.customer_id IN (c.id)");
    let in_where = p(
        "CREATE VIEW v AS SELECT o.id, c.name FROM orders o, customers c \
                      WHERE c.id IN (o.customer_id)",
    );
    for (name, v) in [
        ("ON `=`", &eq),
        ("ON IN", &in_on),
        ("swapped", &in_swapped),
        ("WHERE", &in_where),
    ] {
        assert_eq!(v.tracing(), eq.tracing(), "{name} plans like `=`");
        assert_eq!(
            v.aliases[1].direct_unit, eq.aliases[1].direct_unit,
            "{name} keeps customers' direct unit"
        );
    }
    let grouped = p(
        "CREATE VIEW v AS SELECT o.id, c.name, COUNT(*) n FROM orders o \
                     JOIN customers c ON c.id IN (o.customer_id) GROUP BY o.id, c.name",
    );
    assert_eq!(grouped.unit_exprs, vec!["o.id"], "the key reduces the unit");
}

#[test]
fn parse_select_plans_like_create_view() {
    let sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id";
    let q = parse_select(sql, "q").unwrap();
    assert_eq!(q.hash, definition_hash(sql), "the hash is the definition's");
    let via = plan_block(&q, "db", &catalog()).unwrap();
    let cv = p(&format!("CREATE VIEW q AS {sql}"));
    assert_eq!(via.mode, cv.mode, "same mode");
    assert_eq!(via.tracing(), cv.tracing(), "same tracing");
    assert_eq!(via.unit_exprs, cv.unit_exprs, "same unit exprs");
    for (a, b) in via.aliases.iter().zip(&cv.aliases) {
        assert_eq!(a.direct_unit, b.direct_unit, "same direct unit");
    }
}

#[test]
fn parse_select_refuses_create_and_two_statements() {
    assert!(parse_select("CREATE VIEW x AS SELECT 1", "q").is_err());
    assert!(parse_select("SELECT 1; SELECT 2", "q").is_err());
}

#[test]
fn id_outputs_take_the_named_column() {
    let mut d = parse_select(
        "SELECT o.customer_id AS _key, o.id, o.status FROM orders o",
        "q",
    )
    .unwrap();
    d.id_outputs = vec!["_key".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.unit_exprs, vec!["o.customer_id"]);
    assert_eq!(v.unit_in_projection, vec![Some(0)]);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
    assert!(!v.unit_unique, "a chosen id is not known to be unique");
}

#[test]
fn id_outputs_through_joins() {
    let mut d = parse_select(
        "SELECT c.id AS _key, o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
        "q",
    )
    .unwrap();
    d.id_outputs = vec!["_key".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
    assert_eq!(v.aliases[1].direct_unit, Some(vec![0]));

    // The same with the two joins permuted:
    let mut d = parse_select(
        "SELECT c.id AS _key, o.id FROM customers c JOIN orders o ON c.id = o.customer_id",
        "q",
    )
    .unwrap();
    d.id_outputs = vec!["_key".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.aliases[0].direct_unit, Some(vec![0]));
    assert_eq!(v.aliases[1].direct_unit, Some(vec![1]));

    // ... and with the equality in WHERE:
    let mut d = parse_select(
        "SELECT c.id AS _key, o.id FROM orders o, customers c WHERE c.id = o.customer_id",
        "q",
    )
    .unwrap();
    d.id_outputs = vec!["_key".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
    assert_eq!(v.aliases[1].direct_unit, Some(vec![0]));
}

#[test]
fn id_outputs_grouped_need_group_keys() {
    let sql = "SELECT o.customer_id, o.status, COUNT(*) n \
               FROM orders o GROUP BY o.customer_id, o.status";
    let mut d = parse_select(sql, "q").unwrap();
    d.id_outputs = vec!["customer_id".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(
        v.unit_exprs,
        vec!["o.customer_id"],
        "a chosen id is not reduced"
    );

    for (out, msg) in [
        ("n", "is not a GROUP BY"),
        ("nope", "is not an output column"),
    ] {
        let mut d = parse_select(sql, "q").unwrap();
        d.id_outputs = vec![out.to_string()];
        let e = plan_block(&d, "db", &catalog()).unwrap_err().to_string();
        assert!(e.contains(msg), "{out}: {e}");
    }

    let mut d = parse_select("SELECT COUNT(*) n FROM orders", "q").unwrap();
    d.id_outputs = vec!["n".to_string()];
    let e = plan_block(&d, "db", &catalog()).unwrap_err().to_string();
    assert!(e.contains("single-row aggregate"), "{e}");
}

#[test]
fn id_outputs_survive_the_front_end() {
    let mut d = parse_select("SELECT o.customer_id AS _key, o.id FROM orders o", "q").unwrap();
    d.id_outputs = vec!["_key".to_string()];
    let inline = plan_block(&d, "db", &catalog()).unwrap();

    let mut d = parse_select(
        "WITH x AS (SELECT o.customer_id AS k, o.id FROM orders o) SELECT k AS _key, id FROM x",
        "q",
    )
    .unwrap();
    d.id_outputs = vec!["_key".to_string()];
    let v = crate::analyzer::plan(&d, "db", &catalog()).unwrap();
    assert_eq!(v.aliases[0].direct_unit, inline.aliases[0].direct_unit);
}

#[test]
fn id_outputs_view_column_list() {
    let mut d =
        parse_definition("CREATE VIEW v (k, idcol) AS SELECT o.customer_id, o.id FROM orders o")
            .unwrap();
    d.id_outputs = vec!["K".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.unit_exprs, vec!["o.customer_id"]);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
    assert!(!v.unit_unique);
}

#[test]
fn id_outputs_windowed_need_shared_partition_keys() {
    let sql = "SELECT o.customer_id, o.status, \
               ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.id) rn FROM orders o";
    let mut d = parse_select(sql, "q").unwrap();
    d.id_outputs = vec!["customer_id".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(
        v.unit_exprs,
        vec!["o.customer_id"],
        "the shared partition key, unreduced"
    );
    assert!(!v.unit_unique);

    let mut d = parse_select(sql, "q").unwrap();
    d.id_outputs = vec!["status".to_string()];
    let e = plan_block(&d, "db", &catalog()).unwrap_err().to_string();
    assert!(
        e.contains("is not a GROUP BY or shared PARTITION BY key"),
        "{e}"
    );
}

#[test]
fn id_outputs_composite() {
    let mut d = parse_select("SELECT o.customer_id, o.status FROM orders o", "q").unwrap();
    d.id_outputs = vec!["customer_id".to_string(), "status".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.unit_exprs, vec!["o.customer_id", "o.status"]);
    assert_eq!(v.unit_in_projection, vec![Some(0), Some(1)]);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1, 2]));
    assert!(!v.unit_unique, "a chosen id is not known to be unique");
}

#[test]
fn id_outputs_expression_keys() {
    // An aliased expression as group key: the key comparison is on the expression text, so
    // the output has to match it exactly, and the unit keeps it unreduced.
    let sql = "SELECT DATE(o.note) d, UPPER(o.status) u, o.id \
               FROM orders o GROUP BY DATE(o.note), o.id";
    let mut d = parse_select(sql, "q").unwrap();
    d.id_outputs = vec!["d".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(
        v.unit_exprs,
        vec!["DATE(o.note)"],
        "the expression key, unreduced"
    );
    assert!(!v.unit_unique);

    // An expression that is not one of the group keys:
    let mut d = parse_select(sql, "q").unwrap();
    d.id_outputs = vec!["u".to_string()];
    let e = plan_block(&d, "db", &catalog()).unwrap_err().to_string();
    assert!(
        e.contains("is not a GROUP BY or shared PARTITION BY key"),
        "{e}"
    );

    // The same expression as the shared PARTITION BY key of every window:
    let mut d = parse_select(
        "SELECT DATE(o.note) d, o.id, \
         ROW_NUMBER() OVER (PARTITION BY DATE(o.note), o.id ORDER BY o.id) rn FROM orders o",
        "q",
    )
    .unwrap();
    d.id_outputs = vec!["d".to_string()];
    let v = plan_block(&d, "db", &catalog()).unwrap();
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(
        v.unit_exprs,
        vec!["DATE(o.note)"],
        "the shared partition key, unreduced"
    );
    assert!(!v.unit_unique);
}

#[test]
fn reserved_tables_are_refused_as_sources() {
    assert!(set_reserved(&["zz_reserved_t"], &["zz_reserved_"]));
    let mut cat = catalog();
    for t in [
        table("zz_reserved_t", &[("id", "int")], &[0]),
        table("zz_reserved_x", &[("id", "int")], &[0]),
    ] {
        cat.insert(t.0, t.1);
    }
    for name in ["zz_reserved_t", "zz_reserved_x"] {
        let e = plan_block(
            &parse_definition(&format!("CREATE VIEW v AS SELECT id FROM {name}")).unwrap(),
            "db",
            &cat,
        )
        .unwrap_err();
        assert!(e.to_string().contains("reserved table"), "{name}: {e}");
    }
    plan_block(
        &parse_definition("CREATE VIEW v AS SELECT o.id FROM orders o").unwrap(),
        "db",
        &cat,
    )
    .unwrap();
}

#[test]
fn grouped() {
    let v = p(
        "CREATE VIEW v AS SELECT o.customer_id AS cust, COUNT(*) n, SUM(i.qty * i.price) amt \
               FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY cust HAVING n > 1",
    );
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.unit_exprs, vec!["o.customer_id"]);
    assert_eq!(v.unit_in_projection, vec![Some(0)]);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
    assert_eq!(v.aliases[1].direct_unit, None);
    assert!(v.aliases[1].membership[0], "join column is membership");
    assert!(!v.aliases[1].membership[2], "qty only aggregated");
    assert!(v.aliases[1].referenced[2]);
    let s = v.select_sql(&[], Some("o.customer_id IN (1)"));
    assert!(
        s.ends_with("WHERE o.customer_id IN (1) GROUP BY cust HAVING n > 1"),
        "{s}"
    );

    // The group key reduced to what determines it: customers' key, equal to orders' column.
    let v = p("CREATE VIEW v AS SELECT c.id, c.name, COUNT(*) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.id, c.name");
    assert_eq!(v.unit_exprs, vec!["c.id"]);
    assert_eq!(
        v.aliases[0].direct_unit,
        Some(vec![1]),
        "orders map to the unit directly"
    );
    let v = p("CREATE VIEW v AS SELECT DATE(o.note) d, COUNT(*) FROM orders o GROUP BY 1");
    assert_eq!(v.unit_exprs, vec!["DATE(o.note)"]);
    assert_eq!(v.aliases[0].direct_unit, None);
}

#[test]
fn global_and_full() {
    assert_eq!(
        p("CREATE VIEW v AS SELECT COUNT(*) FROM orders").mode,
        Mode::Global
    );
    for sql in [
        "CREATE VIEW v AS SELECT DISTINCT status FROM orders",
        "CREATE VIEW v AS SELECT id FROM orders LIMIT 5",
        "CREATE VIEW v AS SELECT id FROM orders UNION SELECT id FROM customers",
        "CREATE VIEW v AS SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
        "CREATE VIEW v AS SELECT id, NOW() FROM orders",
        "CREATE VIEW v AS SELECT o.id FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
    ] {
        let v = p(sql);
        assert!(matches!(v.mode, Mode::Full(_)), "{sql}: {:?}", v.mode);
        assert!(!v.tables.is_empty());
    }
    let v = p(
        "CREATE VIEW v AS SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
    );
    assert_eq!(v.tables.len(), 2);
}

#[test]
fn wildcards_and_columns() {
    let v = p("CREATE VIEW v (a, b, c) AS SELECT r.*, 1 FROM regions r");
    assert_eq!(v.projection, vec!["`r`.`id`", "`r`.`name`", "1"]);
    assert_eq!(
        v.output_names,
        Some(vec!["a".to_string(), "b".into(), "c".into()])
    );
    assert!(
        parse_definition("CREATE VIEW v (a) AS SELECT id, name FROM regions")
            .and_then(|d| plan_block(&d, "db", &catalog()))
            .is_err(),
        "column list length must match"
    );
    assert!(parse_definition("CREATE VIEW v AS SELECT * FROM nope")
        .and_then(|d| plan_block(&d, "db", &catalog()))
        .is_err());
    assert!(
        parse_definition("CREATE VIEW v AS SELECT bogus FROM orders")
            .and_then(|d| plan_block(&d, "db", &catalog()))
            .is_err()
    );
}

#[test]
fn composite_and_using() {
    let v = p("CREATE VIEW v AS SELECT i.order_id, i.line, o.status FROM order_items i JOIN orders o ON o.id = i.order_id");
    assert_eq!(v.unit_exprs.len(), 2);
    assert_eq!(
        v.aliases[1].direct_unit, None,
        "orders map to only part of the composite unit"
    );
    let v = p("CREATE VIEW v AS SELECT o.id, o.status FROM orders o JOIN customers c ON c.id = o.customer_id");
    assert_eq!(v.aliases[1].direct_unit, None);
    let v = p("CREATE VIEW v AS SELECT r.id, c.name FROM regions r JOIN customers c ON c.region_id = r.id");
    assert_eq!(v.aliases[1].direct_unit, Some(vec![2]));
}

#[test]
fn column_list_keeps_select_aliases_for_having() {
    // HAVING n refers to the SELECT alias `n`, not to the view column name.
    let v = p("CREATE VIEW v (cust, cnt) AS SELECT customer_id, COUNT(*) n FROM orders GROUP BY customer_id HAVING n > 1");
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.projection[1], "COUNT(*) AS n");
    assert!(v.select_sql(&[], None).ends_with("HAVING n > 1"));
    assert_eq!(v.output_names.as_ref().unwrap()[1], "cnt");
}

#[test]
fn order_by_kept_for_full_and_ctes_are_not_tables() {
    // Top-N: one unit, recomputed by the bounded query itself.
    let v = p("CREATE VIEW v AS SELECT id FROM orders ORDER BY total DESC LIMIT 3");
    assert_eq!(v.mode, Mode::Global);
    assert!(v.unit_exprs.is_empty() && v.lineage.is_empty());
    assert!(
        v.select_sql(&[], None)
            .ends_with("FROM orders ORDER BY total DESC LIMIT 3"),
        "{}",
        v.select_sql(&[], None)
    );
    assert_eq!(
        full_reason("CREATE VIEW v AS SELECT id FROM orders LIMIT 3"),
        "LIMIT without ORDER BY"
    );
    // ORDER BY without LIMIT is irrelevant for a table: dropped on the incremental path.
    let v = p("CREATE VIEW v AS SELECT id FROM orders ORDER BY total");
    assert_eq!(v.mode, Mode::Spj);
    assert!(!v.select_sql(&[], None).contains("ORDER BY"));
    let d = parse_definition(
        "CREATE VIEW v AS WITH big AS (SELECT id FROM orders WHERE total > 5) SELECT id FROM big",
    )
    .unwrap();
    assert_eq!(
        referenced_tables(&d.query, "db"),
        vec![("db".to_string(), "orders".to_string())]
    );
    assert!(matches!(
        plan_block(&d, "db", &catalog()).unwrap().mode,
        Mode::Full(_)
    ));
}

/// Window functions: the unit is the PARTITION BY all windows share (whole partitions are
/// recomputed); without one, the view is one unit.
#[test]
fn window_partitions_are_units() {
    let v = p("CREATE VIEW v AS SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn, \
               SUM(total) OVER w AS run FROM orders WINDOW w AS (PARTITION BY customer_id, status ORDER BY id)");
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.unit_exprs, vec!["customer_id"]);
    assert!(!v.unit_unique);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![1]));
    assert!(
        v.aliases[0].referenced[2],
        "status, used by the named window"
    );
    assert!(v
        .select_sql(&[], Some("customer_id IN (1)"))
        .contains("WHERE customer_id IN (1)"));
    assert_eq!(
        p("CREATE VIEW v AS SELECT id, RANK() OVER (ORDER BY total DESC) AS r FROM orders").mode,
        Mode::Global
    );
    assert_eq!(
        p("CREATE VIEW v AS SELECT customer_id, SUM(total), RANK() OVER (ORDER BY SUM(total)) FROM orders GROUP BY customer_id").mode,
        Mode::Global
    );
    // A partition key that the rest of the key determines is reduced by keys, like GROUP BY.
    assert_eq!(
        p("CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER (PARTITION BY id, status ORDER BY id) AS rn FROM orders")
            .unit_exprs,
        vec!["id"]
    );
    assert_eq!(
        p("CREATE VIEW v AS SELECT o.id, ROW_NUMBER() OVER (PARTITION BY o.id, c.name ORDER BY o.id) AS rn \
           FROM orders o JOIN customers c ON c.id = o.customer_id")
            .unit_exprs,
        vec!["o.id"]
    );
}

/// A first table without primary key: equal rows are one unit (its referenced columns).
#[test]
fn keyless_anchor_units() {
    let v = p("CREATE VIEW v AS SELECT msg, LENGTH(msg) AS n FROM logs WHERE msg <> ''");
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.unit_exprs, vec!["`logs`.`msg`"]);
    assert!(!v.unit_unique);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![0]));
}

#[test]
fn left_join_null_side() {
    // Group key from the NULL-supplying side: NULL-extended group is invisible to lookups.
    let r = full_reason("CREATE VIEW v AS SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id");
    assert!(r.contains("NULL side"), "{r}");
    let r = full_reason("CREATE VIEW v AS SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY n");
    assert!(r.contains("NULL side"), "{r}");
    // Anti-join (WHERE keeps NULL-extended rows): the NULL-side table's lookups leave those
    // conjuncts out and it is traced by lookups only (no lineage).
    let v = p("CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.id IS NULL AND o.total > 0");
    assert_eq!(v.mode, Mode::Spj);
    assert!(v.aliases[1].lookup_where && v.lineage.is_empty());
    assert_eq!(
        v.lookup_sql(1, "(R)"),
        "SELECT DISTINCT `o`.`id` FROM orders o JOIN (R) AS `c` ON c.id = o.customer_id WHERE (o.total > 0)"
    );
    assert!(v
        .select_sql(&[], None)
        .ends_with("WHERE c.id IS NULL AND o.total > 0"));
    for sql in [
        "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE IFNULL(c.name, 'x') = 'x'",
        "CREATE VIEW v AS SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name = 'a' OR o.total > 1",
    ] {
        let v = p(sql);
        assert_eq!(v.mode, Mode::Spj, "{sql}");
        assert_eq!(v.lookup_sql(1, "(R)"), "SELECT DISTINCT `o`.`id` FROM orders o JOIN (R) AS `c` ON c.id = o.customer_id");
    }
    // A NULL-side group key that the rest of the key determines is not part of the unit.
    let v = p("CREATE VIEW v AS SELECT o.id, c.name, COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY o.id, c.name");
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(v.unit_exprs, vec!["o.id"]);
    // Null-rejecting conditions stay incremental.
    let v = p("CREATE VIEW v AS SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.name LIKE 'a%' AND UPPER(c.name) <> 'B' AND c.region_id IN (1, 2)");
    assert_eq!(v.mode, Mode::Spj);
    // The substituted LEFT-joined table is joined INNER in its own lookup.
    let c = v.aliases.iter().position(|a| a.name == "c").unwrap();
    assert_eq!(v.aliases[c].subst_from.0, "orders o JOIN ");
    let look = v.lookup_sql(c, "(SELECT 1 AS `id`)");
    assert!(!look.contains("LEFT JOIN (SELECT"), "{look}");
    // HAVING is evaluated on refresh, not in lookups: fine on the NULL side.
    let v = p("CREATE VIEW v AS SELECT o.id, COUNT(i.line) n FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY o.id HAVING COUNT(i.line) = 0");
    assert_eq!(v.mode, Mode::Grouped);
}

#[test]
fn equalities_only_within_a_type_family() {
    let mut cat = catalog();
    let (k, t) = typed(
        "codes",
        vec![
            Column::simple("code", "varchar"),
            Column::simple("v", "int"),
        ],
        &[0],
    );
    cat.insert(k, t);
    let (k, t) = typed(
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
    );
    cat.insert(k, t);
    let pl = |sql: &str| plan_block(&parse_definition(sql).unwrap(), "db", &cat).unwrap();
    // int = varchar: implicit conversion, not transitive -> no direct unit.
    let v =
        pl("CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code_int = c.code");
    assert_eq!(v.aliases[1].direct_unit, None);
    // different collations -> no direct unit.
    let v = pl("CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code = c.code");
    assert_eq!(v.aliases[1].direct_unit, None);
    // same family -> direct.
    let v = pl("CREATE VIEW v AS SELECT c.code, r.id FROM codes c JOIN refs r ON r.code_int = c.v");
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.aliases[0].direct_unit, Some(vec![0]));
}

#[test]
fn unsupported_constructs_fall_back() {
    assert!(
        full_reason("CREATE VIEW v AS SELECT id FROM orders WHERE MATCH(note) AGAINST ('x')")
            .contains("MATCH")
    );
    assert!(
        full_reason("CREATE VIEW v AS SELECT o.id, c.name FROM orders o CROSS JOIN regions c")
            .contains("every row")
    );
    assert!(
        full_reason("CREATE VIEW v AS SELECT o.id, r.name FROM orders o JOIN regions r")
            .contains("every row")
    );
}

#[test]
fn json_and_bit_unit_keys() {
    let mut cat = catalog();
    let (k, t) = typed(
        "docs",
        vec![
            Column::simple("id", "int"),
            Column::new("doc", "json", "json", None, None, None, false),
            Column::new("flags", "bit", "bit(8)", None, None, None, false),
        ],
        &[0],
    );
    cat.insert(k, t);
    let pl = |sql: &str| {
        plan_block(&parse_definition(sql).unwrap(), "db", &cat)
            .unwrap()
            .mode
    };
    // A JSON column is a plain unit (the engine binds it typed, `CAST(? AS JSON)`).
    assert_eq!(
        pl("CREATE VIEW v AS SELECT doc, COUNT(*) FROM docs GROUP BY doc"),
        Mode::Grouped
    );
    // A JSON value computed by a function: MariaDB compares it as JSON, the filter as text.
    for (sql, key) in [
        (
            "CREATE VIEW v AS SELECT JSON_EXTRACT(doc, '$.a') k, COUNT(*) FROM docs GROUP BY k",
            "JSON_EXTRACT(doc, '$.a')",
        ),
        (
            "CREATE VIEW v AS SELECT doc->'$.a' k, COUNT(*) FROM docs GROUP BY k",
            "doc -> '$.a'",
        ),
    ] {
        assert_eq!(
            pl(sql),
            Mode::Full(format!("group key `{key}` is a JSON value computed by a function (MariaDB compares it as JSON, its unit filter as text): group by the column, or by its text with ->> / JSON_UNQUOTE")),
            "{sql}"
        );
    }
    // ->> yields text: fine.
    assert_eq!(
        pl("CREATE VIEW v AS SELECT doc->>'$.a' k, COUNT(*) FROM docs GROUP BY k"),
        Mode::Grouped
    );
    assert_eq!(
        pl("CREATE VIEW v AS SELECT flags, COUNT(*) FROM docs GROUP BY flags"),
        Mode::Grouped
    );
}

#[test]
fn sql_views_reserved_names_and_metadata() {
    let mut cat = catalog();
    let mut info = (*cat[&("db".to_string(), "orders".to_string())]).clone();
    info.name = "paid_orders".into();
    info.pk.clear();
    info.is_view = true;
    info.view_bases = vec![
        ("db".into(), "orders".into()),
        ("db".into(), "nested_v".into()),
    ];
    cat.insert(("db".into(), "paid_orders".into()), Arc::new(info));
    let mut nested = (*cat[&("db".to_string(), "regions".to_string())]).clone();
    nested.name = "nested_v".into();
    nested.is_view = true;
    nested.view_bases = vec![("db".into(), "regions".into())];
    cat.insert(("db".into(), "nested_v".into()), Arc::new(nested));
    let v = plan_block(&parse_definition("CREATE VIEW v AS SELECT p.id, c.name FROM paid_orders p JOIN customers c ON c.id = p.customer_id").unwrap(), "db", &cat).unwrap();
    assert!(
        matches!(&v.mode, Mode::Full(r) if r.contains("paid_orders")),
        "{:?}",
        v.mode
    );
    assert_eq!(
        v.tables,
        vec![
            ("db".to_string(), "customers".to_string()),
            ("db".into(), "orders".into()),
            ("db".into(), "regions".into())
        ]
    );
    for bad in [
        "CREATE VIEW x__lv_new AS SELECT id FROM orders",
        "CREATE VIEW x__LV_OLD AS SELECT id FROM orders",
        "CREATE VIEW _lv_probe AS SELECT id FROM orders",
        "CREATE VIEW _lazyview_state AS SELECT id FROM orders",
    ] {
        assert!(
            plan_block(&parse_definition(bad).unwrap(), "db", &cat).is_err(),
            "{bad}"
        );
    }
    let mut cat2 = catalog();
    let (k, t) = table("_lazyview_state", &[("name", "varchar")], &[0]);
    cat2.insert(k, t);
    let e = plan_block(
        &parse_definition("CREATE VIEW v AS SELECT name FROM _lazyview_state").unwrap(),
        "db",
        &cat2,
    )
    .unwrap_err();
    assert!(e.to_string().contains("metadata"), "{e}");
    assert_eq!(
        select_tables(
            "select `db`.`orders`.`id` AS `id` from `db`.`orders` join x on 1",
            "db"
        ),
        vec![
            ("db".to_string(), "orders".to_string()),
            ("db".into(), "x".into())
        ]
    );
    assert!(select_tables("not sql", "db").is_empty());
}

#[test]
fn function_calls_listed() {
    let d = parse_definition("CREATE VIEW v AS SELECT id, my_fn(total), app.calc(id) FROM orders WHERE UPPER(status) = 'X'").unwrap();
    let f = function_calls(&d.query);
    assert!(f.contains(&(None, "my_fn".to_string())));
    assert!(f.contains(&(Some("app".to_string()), "calc".to_string())));
    assert!(f.contains(&(None, "UPPER".to_string())));
}

#[test]
fn literal_escapes_and_quoted_identifiers_survive() {
    let v = p(
        r"CREATE VIEW v AS SELECT id, status FROM orders WHERE status = 'a\'b' OR status LIKE 'x\_y' OR note = '\\n'",
    );
    let w = v.where_sql.as_deref().unwrap();
    for lit in [r"'a\'b'", r"'x\_y'", r"'\\n'"] {
        assert!(w.contains(lit), "{lit} in {w}");
    }
    let v = p(r"CREATE VIEW v AS SELECT CONCAT(status, '\\n') k, COUNT(*) FROM orders GROUP BY k");
    assert_eq!(v.unit_exprs, vec![r"CONCAT(status, '\\n')"]);
    // Full rebuilds run the text as written.
    let d = parse_definition(
        "CREATE VIEW v AS\n  SELECT DISTINCT CONCAT(status, '\\\\n')  FROM orders WHERE note = 'it''s' -- tail\n WITH CHECK OPTION;",
    )
    .unwrap();
    assert_eq!(
        d.query_sql,
        "SELECT DISTINCT CONCAT(status, '\\\\n')  FROM orders WHERE note = 'it''s'"
    );
    // Doubled backticks: names resolve unquoted and render quoted again.
    let mut cat = catalog();
    let (k, t) = table("we`ird", &[("i`d", "int"), ("v", "int")], &[0]);
    cat.insert(k, t);
    let d = parse_definition(
        "CREATE VIEW `v``1` (`a``b`, c) AS SELECT `i``d`, w.v FROM `we``ird` w WHERE w.`i``d` > 1",
    )
    .unwrap();
    assert_eq!(d.name, "v`1");
    assert_eq!(d.columns, vec!["a`b", "c"]);
    assert_eq!(
        referenced_tables(&d.query, "db"),
        vec![("db".to_string(), "we`ird".to_string())]
    );
    let v = plan_block(&d, "db", &cat).unwrap();
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.unit_exprs, vec!["`w`.`i``d`"]);
    assert_eq!(v.aliases[0].referenced, vec![true, true]);
    assert_eq!(v.aliases[0].membership, vec![true, false]);
    assert_eq!(
        select_tables("select `x`.`i``d` from `db`.`we``ird` `x`", "db"),
        vec![("db".to_string(), "we`ird".to_string())]
    );
}

#[test]
fn opaque_key_columns_fall_back() {
    let mut cat = catalog();
    let opaque = |n: &str, ty: &str| Column::new(n, ty, ty, None, None, Some(16), true);
    let (k, t) = typed(
        "devices",
        vec![
            opaque("id", "uuid"),
            opaque("ip", "inet6"),
            Column::simple("name", "varchar"),
            opaque("pos", "point"),
        ],
        &[0],
    );
    cat.insert(k, t);
    let (k, t) = typed(
        "hits",
        vec![
            Column::simple("n", "int"),
            opaque("ip", "inet6"),
            opaque("v4", "inet4"),
        ],
        &[0],
    );
    cat.insert(k, t);
    let pl = |sql: &str| {
        plan_block(&parse_definition(sql).unwrap(), "db", &cat)
            .unwrap()
            .mode
    };
    for (sql, want) in [
        (
            "CREATE VIEW v AS SELECT id, name FROM devices",
            "UUID column `devices`.`id`",
        ),
        (
            "CREATE VIEW v AS SELECT ip, COUNT(*) FROM hits GROUP BY ip",
            "INET6 column `hits`.`ip`",
        ),
        (
            "CREATE VIEW v AS SELECT h.n, d.name FROM hits h JOIN devices d ON d.ip = h.ip",
            "INET6 column `hits`.`ip`",
        ),
        (
            "CREATE VIEW v AS SELECT n FROM hits WHERE v4 > '10.0.0.0'",
            "INET4 column `hits`.`v4`",
        ),
    ] {
        let m = pl(sql);
        assert!(
            matches!(&m, Mode::Full(r) if r.contains(want)),
            "{sql}: {m:?}"
        );
    }
    // Only projected (values come from queries), or no units at all: unaffected.
    assert_eq!(pl("CREATE VIEW v AS SELECT n, ip, v4 FROM hits"), Mode::Spj);
    assert_eq!(
        pl("CREATE VIEW v AS SELECT COUNT(*) FROM hits WHERE ip IS NOT NULL"),
        Mode::Global
    );
}

#[test]
fn schema_qualified_columns_use_the_alias() {
    let v = p(
        "CREATE VIEW v AS SELECT db.orders.id, db.customers.name, db.orders.total + 1 \
               FROM db.orders JOIN db.customers ON db.customers.id = db.orders.customer_id \
               WHERE db.customers.name <> 'x' AND db.orders.total > 0",
    );
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.unit_exprs, vec!["`orders`.`id`"]);
    assert_eq!(
        v.where_sql.as_deref(),
        Some("`customers`.name <> 'x' AND `orders`.total > 0")
    );
    assert_eq!(v.projection[..2], ["`orders`.id", "`customers`.name"]);
    // Valid as written in the full SELECT (it is not substituted).
    assert_eq!(v.projection[2], "db.orders.total + 1");
    let look = v.lookup_sql(1, "(SELECT ? AS `id`, ? AS `name`)");
    assert_eq!(
        look,
        "SELECT DISTINCT `orders`.`id` FROM db.orders JOIN (SELECT ? AS `id`, ? AS `name`) AS `customers` \
         ON `customers`.id = `orders`.customer_id WHERE `customers`.name <> 'x' AND `orders`.total > 0"
    );
    let look = v.lookup_sql(0, "(SELECT ? AS `id`)");
    assert!(!look.contains("db.orders."), "{look}");
    let v = p("CREATE VIEW v AS SELECT db.orders.customer_id c, COUNT(*) FROM orders GROUP BY db.orders.customer_id");
    assert_eq!(v.unit_exprs, vec!["`orders`.customer_id"]);
    assert_eq!(v.group_sql.as_deref(), Some("`orders`.customer_id"));
}

#[test]
fn column_list_for_full_rebuilds() {
    let v = p("CREATE VIEW v (st) AS SELECT DISTINCT status FROM orders");
    assert_eq!(
        v.full_select,
        "WITH `_lv_cols` (`st`) AS (SELECT DISTINCT status FROM orders) SELECT * FROM `_lv_cols`"
    );
    // Set operations take names from the first SELECT; ORDER BY keeps its own aliases.
    let v = p("CREATE VIEW v (x, y) AS SELECT id, status AS s FROM orders UNION ALL SELECT id, name FROM customers ORDER BY s LIMIT 2");
    assert_eq!(
        v.full_select,
        "WITH `_lv_cols` (`x`, `y`) AS (SELECT id, status AS s FROM orders UNION ALL \
         SELECT id, name FROM customers ORDER BY s LIMIT 2) SELECT * FROM `_lv_cols`"
    );
    let pl = |sql: &str| parse_definition(sql).and_then(|d| plan_block(&d, "db", &catalog()));
    for ok in [
        "CREATE VIEW v (a, b) AS SELECT DISTINCT * FROM regions",
        "CREATE VIEW v (a, b, c) AS SELECT DISTINCT r.*, c.id FROM regions r JOIN customers c ON c.region_id = r.id",
        // Not countable here (derived table, USING): the server checks the count.
        "CREATE VIEW v (a) AS SELECT * FROM (SELECT id, name FROM regions) d",
        "CREATE VIEW v (a, b, c, d) AS SELECT DISTINCT * FROM regions JOIN customers USING (id)",
    ] {
        let v = pl(ok).unwrap();
        assert!(matches!(v.mode, Mode::Full(_)), "{ok}");
        assert!(v.full_select.starts_with("WITH `_lv_cols` ("), "{ok}");
    }
    for bad in [
        "CREATE VIEW v (a) AS SELECT DISTINCT id, status FROM orders",
        "CREATE VIEW v (a) AS SELECT * FROM regions UNION SELECT id, name FROM customers",
        "CREATE VIEW v (a, b, c) AS SELECT DISTINCT * FROM regions",
    ] {
        let e = pl(bad).unwrap_err().to_string();
        assert!(e.contains("column list has"), "{bad}: {e}");
    }
    // Incremental plans keep their SELECT and are renamed positionally.
    let v = pl("CREATE VIEW v (a, b) AS SELECT id, name FROM regions").unwrap();
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(v.output_names, Some(vec!["a".to_string(), "b".into()]));
}

#[test]
fn unaliased_expressions_keep_their_source_text() {
    let v = p("CREATE VIEW v AS SELECT id, CONCAT(status,'-',note), CASE WHEN total > 1 THEN 'y' ELSE 'n' END, \
               id+customer_id, IF(total>1,'y','n'), `status`, o.note, (total), 'it''s', total AS t, \
               total /* c */ + 1, total\n  + 2, CONCAT(note, 'é—x'), \
               CONCAT(status, '-', note, '-', status, '-', note, '-', status, '-', note, '-', status) FROM orders o");
    assert_eq!(v.mode, Mode::Spj);
    assert_eq!(
        v.projection,
        vec![
            "id",
            "CONCAT(status,'-',note)",
            "CASE WHEN total > 1 THEN 'y' ELSE 'n' END",
            "id+customer_id",
            "IF(total>1,'y','n')",
            "`status`",
            "o.note",
            "(total)",
            "'it''s'",
            "total AS t",
            "total /* c */ + 1",
            "total\n  + 2",
            "CONCAT(note, 'é—x')",
            "CONCAT(status, '-', note, '-', status, '-', note, '-', status, '-', note, '-', status)",
        ]
    );
    let v = p("CREATE VIEW v AS SELECT SQL_NO_CACHE customer_id, COUNT( * ), count(*), SUM(total*2) FROM orders GROUP BY customer_id");
    assert_eq!(v.mode, Mode::Grouped);
    assert_eq!(
        v.projection,
        vec!["customer_id", "COUNT( * )", "count(*)", "SUM(total*2)"]
    );
    assert_eq!(v.unit_in_projection, vec![Some(0)]);
    let v = p("CREATE VIEW v AS SELECT r.*, LENGTH( name ) FROM regions r");
    assert_eq!(
        v.projection,
        vec!["`r`.`id`", "`r`.`name`", "LENGTH( name )"]
    );
    let v = p("CREATE VIEW v AS SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders");
    assert_eq!(
        v.full_select,
        "SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders"
    );
}

#[test]
fn executable_comments_are_refused() {
    let err = |sql: &str| format!("{:#}", parse_definition(sql).unwrap_err());
    for sql in [
        "CREATE VIEW v AS SELECT id /*! , 1 */ FROM items",
        "CREATE VIEW v AS SELECT id FROM items /*!50100 WHERE 1 */",
        "CREATE VIEW v AS SELECT id FROM items /*M! WHERE 1 */",
        "CREATE VIEW v AS SELECT id FROM items /*m!100500 WHERE 1 */",
        "CREATE VIEW v AS SELECT id FROM items WHERE note = '/*!x*/'",
    ] {
        let e = err(sql);
        assert!(e.contains("executable comment"), "{sql}: {e}");
    }
    for sql in [
        "CREATE VIEW v AS SELECT id /* a note */ FROM items",
        "CREATE VIEW v AS SELECT id FROM items -- a note",
        "CREATE VIEW v AS SELECT /*+ MAX_EXECUTION_TIME(1000) */ id FROM items",
    ] {
        assert!(parse_definition(sql).is_ok(), "{sql}");
    }
}
