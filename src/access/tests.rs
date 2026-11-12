use super::*;
use crate::analyze::{parse_definition, plan_block};
use crate::catalog::test_catalog::linked_catalog;
use crate::catalog::Column;
use std::sync::Arc;

fn plan(sql: &str) -> ViewPlan {
    let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
    plan_block(&d, "db", &linked_catalog()).unwrap()
}

#[test]
fn single_table_is_dedup() {
    let ap = AccessPlan::of(&plan("SELECT i.id FROM items i"));
    assert!(matches!(ap.route, Route::Dedup));
    assert_eq!(ap.route_name(), "dedup");
    assert!(ap.id.unique);
    let body = ap.body();
    assert!(body.contains("occ i db.items"), "{body}");
    assert!(body.contains("-> direct"), "{body}");
    assert!(body.contains(r"id 0 `i`.`id`"), "{body}");
}

#[test]
fn lineage_alias_is_viewplan() {
    // `k` hops to the id; `l` reaches only `k` (its own LEFT-join edge), so it keeps its
    // lineage and the view stays on the viewplan route.
    let ap = AccessPlan::of(&plan(
        "SELECT i.id, k.name, l.parent_id FROM items i LEFT JOIN kinds k ON k.id = i.kind_id \
         LEFT JOIN item_links l ON l.parent_id = k.id",
    ));
    assert!(matches!(ap.occurrences[1].path, Path::Hop(_)));
    assert!(matches!(ap.occurrences[2].path, Path::Lineage { .. }));
    match &ap.route {
        Route::ViewPlan { reason } => {
            assert!(reason.starts_with("`l` (`item_links`): "), "{reason}");
            assert!(
                reason.ends_with("lineage only: not joined to the id by equalities"),
                "{reason}"
            );
        }
        r => panic!("route: {r:?}"),
    }
}

#[test]
fn full_view_is_whole() {
    let ap = AccessPlan::of(&plan("SELECT DISTINCT kind_id FROM items"));
    match &ap.route {
        Route::Full { reason } => assert_eq!(reason, "DISTINCT"),
        r => panic!("route: {r:?}"),
    }
    assert_eq!(ap.occurrences.len(), 1);
    assert!(ap.occurrences.iter().all(|o| matches!(o.path, Path::Whole)));
    assert_eq!(ap.route_name(), "full");
}

#[test]
fn fingerprint_is_stable_and_sensitive() {
    let a = AccessPlan::of(&plan(
        "SELECT i.id, i.kind_id FROM items i WHERE i.kind_id = 3",
    ));
    let b = AccessPlan::of(&plan(
        "SELECT i.id, i.kind_id FROM items i WHERE i.kind_id = 3",
    ));
    assert_eq!(a.fingerprint, b.fingerprint);
    let c = AccessPlan::of(&plan(
        "SELECT i.id, i.kind_id FROM items i WHERE i.kind_id = 4",
    ));
    assert_ne!(a.fingerprint, c.fingerprint);
    assert_eq!(a.fingerprint.len(), 16);
    assert!(a.fingerprint.chars().all(|ch| ch.is_ascii_hexdigit()));
    // The header carries the same fingerprint.
    assert!(a.to_string().contains(&format!("fp {}", a.fingerprint)));
}

/// The dump agrees with what runs: each occurrence's path kind is the label
/// `tracing()` prints at the same index.
#[test]
fn paths_match_tracing() {
    let defs: [(&str, Mode); 10] = [
        ("SELECT i.id FROM items i", Mode::Spj),
        (
            "SELECT i.id, k.name FROM items i JOIN kinds k ON k.id = i.kind_id",
            Mode::Spj,
        ),
        (
            "SELECT i.id, v.key_id, MAX(v.val) n FROM items i JOIN item_values v \
             ON v.item_id = i.id AND v.key_id = i.kind_id GROUP BY i.id, v.key_id",
            Mode::Grouped,
        ),
        (
            "SELECT i.id, k.name, COUNT(*) n FROM items i JOIN kinds k \
             ON k.id = i.kind_id GROUP BY i.id, k.name",
            Mode::Grouped,
        ),
        (
            "SELECT i.id, k.name, MAX(o.name) n FROM items i JOIN kinds k ON k.id = i.kind_id \
             JOIN owners o ON o.id = i.owner_id GROUP BY i.id, k.name",
            Mode::Grouped,
        ),
        (
            "SELECT i.id FROM items i LEFT JOIN kinds k ON k.id = i.kind_id WHERE k.id IS NULL",
            Mode::Spj,
        ),
        ("SELECT COUNT(*) FROM items", Mode::Global),
        (
            "SELECT id, kind_id, ROW_NUMBER() OVER (PARTITION BY kind_id ORDER BY id) rn FROM items",
            Mode::Grouped,
        ),
        (
            "SELECT i.id, v.item_id, v.key_id, v.val FROM items i JOIN item_values v \
             ON v.item_id = i.id",
            Mode::Spj,
        ),
        ("SELECT DISTINCT kind_id FROM items", Mode::Full("DISTINCT".into())),
    ];
    for (def, want) in defs {
        let v = plan(def);
        assert_eq!(v.mode, want, "{def}");
        let ap = AccessPlan::of(&v);
        for (i, o) in ap.occurrences.iter().enumerate() {
            let tracing = v.tracing();
            let label = match v.mode {
                Mode::Full(_) | Mode::Global => "whole",
                _ => tracing
                    .split(',')
                    .nth(i)
                    .and_then(|seg| seg.rsplit_once(':').map(|(_, h)| h))
                    .unwrap_or_else(|| {
                        panic!("{def}: tracing {tracing} lacks a label for occurrence {i}")
                    }),
            };
            let kind = match &o.path {
                Path::Direct { .. } => "unit",
                Path::Partial { .. } => "partial",
                // A hop is a lineage for a lineage alias and a lookup for any other alias.
                Path::Hop(_) => {
                    if v.lineage_of(i).is_some() {
                        "lineage"
                    } else {
                        "lookup"
                    }
                }
                Path::Lineage { .. } => "lineage",
                Path::Untraced { .. } => "lookup",
                Path::Whole => "whole",
            };
            assert_eq!(kind, label, "{def}: occurrence {i} ({})", o.alias);
        }
    }
}

#[test]
fn one_hop_reaches_the_full_id() {
    // `l` joins to `i` by a non-unit column: its row image gives no id part, so the id is
    // reached through the link (`item_links` from `items`), one hop, the full id.
    let v =
        plan("SELECT i.id, l.child_id FROM items i JOIN item_links l ON l.parent_id = i.kind_id");
    let (aliases, links, positions) = chain(&v, 1).unwrap();
    assert_eq!(
        (aliases, links, positions),
        (vec![0], vec![(1, (0, 1))], vec![0])
    );
    let ap = AccessPlan::of(&v);
    match &ap.occurrences[1].path {
        Path::Hop(h) => {
            // One hop: `l` reaches `i`, the id's only alias; the full id is reached.
            assert_eq!(h.link_cols, vec![1]);
            assert_eq!(h.partner, vec![r#"`i`.`kind_id`"#.to_string()]);
            assert_eq!(h.positions, vec![0]);
            assert_eq!(h.aliases, vec![0]);
            assert!(
                h.select.starts_with("SELECT DISTINCT `i`.`kind_id`"),
                "{}",
                h.select
            );
            assert!(
                h.select.ends_with("FROM `db`.`items` AS `i` WHERE TRUE"),
                "{}",
                h.select
            );
            assert!(!h.select.contains("`item_links`"), "{}", h.select);
            assert_eq!(h.partner_cols, vec![(0, 1)]);
        }
        other => panic!("expected a hop, got {other:?}"),
    }
}

#[test]
fn two_hops_through_two_tables() {
    // The id is reached from `o` through two tables: `o` -> `item_links` -> `items`.
    let v = plan(
        "SELECT i.id, MAX(o.name) n FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
         JOIN owners o ON o.id = l.child_id GROUP BY i.id",
    );
    let ap = AccessPlan::of(&v);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    match &ap.occurrences[2].path {
        Path::Hop(h) => {
            assert_eq!(h.aliases, vec![0, 1]);
            assert_eq!(h.partner, vec![r#"`l`.`child_id`"#.to_string()]);
            assert_eq!(h.positions, vec![0]);
            assert!(
                !h.select.contains("`o`.`id` = `l`.`child_id`"),
                "{}",
                h.select
            );
            assert!(!h.select.contains("`owners`"), "{}", h.select);
            assert!(
                h.select.contains("`l`.`parent_id` = `i`.`kind_id`"),
                "{}",
                h.select
            );
        }
        other => panic!("expected a hop, got {other:?}"),
    }
}

/// A synthetic plan (the live planner either reduces or fully rejects the shape): the
/// chain from `l` reaches the `i.id` part but not the `o.id` part, which sits behind `o`'s
/// own LEFT-join edge.
fn partial_plan() -> ViewPlan {
    let alias = |i: usize, name: &str, t: Arc<TableInfo>| {
        let n = t.columns.len();
        crate::analyze::AliasPlan {
            name: name.into(),
            table: t,
            referenced: vec![true; n],
            membership: vec![true; n],
            direct_unit: None,
            image_cols: vec![],
            subst_from: (String::new(), String::new()),
            pos: (i, None),
            semi: None,
            lookup_where: false,
        }
    };
    let cat = linked_catalog();
    let i = cat[&("db".into(), "items".into())].clone();
    let l = cat[&("db".into(), "item_links".into())].clone();
    let o = cat[&("db".into(), "owners".into())].clone();
    ViewPlan {
        schema: "db".into(),
        name: "v".into(),
        hash: "h".into(),
        mode: Mode::Grouped,
        tables: vec![],
        table_columns: vec![],
        aliases: vec![alias(0, "i", i), alias(1, "l", l), alias(2, "o", o)],
        projection: vec![],
        from_sql: String::new(),
        from_items: vec![],
        where_sql: None,
        group_sql: None,
        having_sql: None,
        unit_exprs: vec!["`i`.`id`".into(), "`o`.`id`".into()],
        unit_in_projection: vec![Some(0), Some(1)],
        full_select: String::new(),
        unit_unique: false,
        lineage: vec![],
        cascades: vec![],
        output_names: None,
        order_limit_sql: None,
        window_sql: None,
        lookup_where_sql: None,
        alias_filters: vec![vec![], vec![], vec![]],
        partial: crate::analyze::PartialUnits {
            sets: vec![vec![0]],
            aliases: vec![Some((0, vec![0])), None, None],
        },
        presence: vec![],
        edges: vec![
            // `l` joins to `i` (usable for every occurrence).
            Edge {
                a: (1, 1),
                b: (0, 1),
                only_with: None,
            },
            // `o` is LEFT JOINed: its edge is usable for `o` only.
            Edge {
                a: (2, 0),
                b: (0, 2),
                only_with: Some(2),
            },
        ],
        unit_refs: vec![vec![(0, 0)], vec![(2, 0)]],
        has_explicit_collate: false,
    }
}

#[test]
fn hops_reach_partial_positions() {
    let v = partial_plan();
    // The chain of `l` reaches `i` only: one of the two id parts.
    let (aliases, links, positions) = chain(&v, 1).unwrap();
    assert_eq!(
        (aliases, links, positions),
        (vec![0], vec![(1, (0, 1))], vec![0])
    );
    let ap = AccessPlan::of(&v);
    match &ap.occurrences[1].path {
        Path::Hop(h) => {
            assert_eq!(h.positions, vec![0]);
            assert_eq!(h.aliases, vec![0]);
        }
        other => panic!("expected a hop, got {other:?}"),
    }
}

#[test]
fn hop_positions_need_a_partial_set() {
    // The chain of `l` reaches one of the two id parts and no partial id has that set:
    // executor (a) could not refresh it, so the hop is refused and the view keeps the
    // viewplan route.
    let mut v = partial_plan();
    v.partial = crate::analyze::PartialUnits::default();
    let ap = AccessPlan::of(&v);
    match &ap.occurrences[1].path {
        Path::Untraced { reason } => {
            assert!(reason.contains("which no partial id has"), "{reason}");
        }
        other => panic!("expected an untraced occurrence, got {other:?}"),
    }
    assert!(matches!(ap.route, Route::ViewPlan { .. }), "{ap}");
}

#[test]
fn hop_select_leaves_out_the_occurrence() {
    // The SELECT reads the chain's tables only: it never names the occurrence's own
    // `AS <alias>` and its partner columns parallel `partner`.
    let views = [
        "SELECT i.id, l.child_id FROM items i JOIN item_links l ON l.parent_id = i.kind_id",
        "SELECT i.id, MAX(o.name) n FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
         JOIN owners o ON o.id = l.child_id GROUP BY i.id",
        "SELECT i.id, MAX(o.name) n FROM items i JOIN item_values v ON v.item_id = i.id \
         JOIN item_links l ON l.child_id = v.key_id JOIN owners o ON o.id = l.parent_id \
         GROUP BY i.id",
        "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
         JOIN owners o ON o.id = i.owner_id",
    ];
    let mut hops = 0usize;
    for sql in views {
        let ap = AccessPlan::of(&plan(sql));
        for o in &ap.occurrences {
            if let Path::Hop(h) = &o.path {
                hops += 1;
                assert!(
                    !h.select.contains(&format!("AS `{}`", o.alias)),
                    "the SELECT of {} names the occurrence `{}`: {}",
                    sql,
                    o.alias,
                    h.select
                );
                assert_eq!(h.partner_cols.len(), h.partner.len(), "{:?}", h.select);
            }
        }
    }
    assert!(hops >= 4, "expected hops on these views, got {hops}");
}

#[test]
fn link_types() {
    // The hop cache keys answers by the partner's query values and looks them up by the
    // link's binlog values: the pair must decode equal datums, i.e. the same decoded kind
    // (unsigned included) and both integers or the same data type.
    assert!(same_link_type(
        &Column::simple("a", "int"),
        &Column::simple("b", "bigint")
    ));
    assert!(same_link_type(
        &Column::simple("a", "varchar"),
        &Column::simple("b", "varchar")
    ));
    assert!(!same_link_type(
        &Column::simple("a", "int"),
        &Column::simple("b", "decimal")
    ));
    assert!(!same_link_type(
        &Column::simple("a", "int"),
        &Column::simple("b", "varchar")
    ));
    // The binlog decodes a signed and an unsigned integer into different kinds.
    let unsigned = |n: &str| Column::new(n, "int", "int(11) unsigned", None, None, None, false);
    assert!(!same_link_type(&Column::simple("a", "int"), &unsigned("b")));
    assert!(same_link_type(&unsigned("a"), &unsigned("b")));
    // JSON, BLOB-family and spatial columns never key the hop cache.
    assert!(!same_link_type(
        &Column::simple("a", "json"),
        &Column::simple("b", "json")
    ));
    assert!(!same_link_type(
        &Column::simple("a", "blob"),
        &Column::simple("b", "blob")
    ));
    assert!(!same_link_type(
        &Column::simple("a", "point"),
        &Column::simple("b", "point")
    ));
}

/// Three aliases `i` (0), occurrence `l` (1) and `o` (2), with the given id parts and
/// edges (synthetic shapes the live planner reduces or rejects).
fn three_alias_plan(
    i: Arc<TableInfo>,
    l: Arc<TableInfo>,
    o: Arc<TableInfo>,
    unit_refs: Vec<Vec<(usize, usize)>>,
    edges: Vec<Edge>,
) -> ViewPlan {
    let alias = |i: usize, name: &str, t: Arc<TableInfo>| {
        let n = t.columns.len();
        crate::analyze::AliasPlan {
            name: name.into(),
            table: t,
            referenced: vec![true; n],
            membership: vec![true; n],
            direct_unit: None,
            image_cols: vec![],
            subst_from: (String::new(), String::new()),
            pos: (i, None),
            semi: None,
            lookup_where: false,
        }
    };
    ViewPlan {
        schema: "db".into(),
        name: "v".into(),
        hash: "h".into(),
        mode: Mode::Grouped,
        tables: vec![],
        table_columns: vec![],
        aliases: vec![alias(0, "i", i), alias(1, "l", l), alias(2, "o", o)],
        projection: vec![],
        from_sql: String::new(),
        from_items: vec![],
        where_sql: None,
        group_sql: None,
        having_sql: None,
        unit_exprs: vec!["`i`.`id`".to_string(); unit_refs.len()],
        unit_in_projection: (0..unit_refs.len()).map(Some).collect(),
        full_select: String::new(),
        unit_unique: false,
        lineage: vec![],
        cascades: vec![],
        output_names: None,
        order_limit_sql: None,
        window_sql: None,
        lookup_where_sql: None,
        alias_filters: vec![vec![], vec![], vec![]],
        partial: crate::analyze::PartialUnits::default(),
        presence: vec![],
        edges,
        unit_refs,
        has_explicit_collate: false,
    }
}

#[test]
fn search_keeps_two_links_on_one_id_column() {
    // Both edges sit on `l.child_id` (col 0) with different partners: the link dedup must
    // keep both; only a true duplicate of the pair would be dropped.
    let c = linked_catalog();
    let pick = |t: &str| c[&("db".into(), t.into())].clone();
    let p = three_alias_plan(
        pick("items"),
        pick("item_links"),
        pick("owners"),
        vec![vec![(0, 0)], vec![(2, 0)]],
        vec![
            Edge {
                a: (1, 0),
                b: (0, 0),
                only_with: None,
            },
            Edge {
                a: (1, 0),
                b: (2, 0),
                only_with: None,
            },
        ],
    );
    let (aliases, links, positions) = chain(&p, 1).unwrap();
    assert_eq!(aliases, vec![0, 2]);
    assert_eq!(positions, vec![0, 1]);
    assert_eq!(links, vec![(0, (0, 0)), (0, (2, 0))]);
}

#[test]
fn search_rejects_a_chain_that_covers_no_id_part() {
    // `l` reaches `i` (one link), but every id part also reads the unreachable `o`:
    // no position survives, so the chain cannot stand for the id.
    let c = linked_catalog();
    let pick = |t: &str| c[&("db".into(), t.into())].clone();
    let p = three_alias_plan(
        pick("items"),
        pick("item_links"),
        pick("owners"),
        vec![vec![(0, 0), (2, 0)], vec![(2, 1)]],
        vec![Edge {
            a: (1, 0),
            b: (0, 0),
            only_with: None,
        }],
    );
    assert_eq!(
        chain(&p, 1).unwrap_err(),
        "not joined to the id by equalities"
    );
}

#[test]
fn hop_rejects_a_link_compared_by_collation() {
    // `o.ts` (timestamp) is not an exact key: a link whose one side is inexact must fail
    // even when the other side is exact.
    let c = linked_catalog();
    let pick = |t: &str| c[&("db".into(), t.into())].clone();
    let o = Arc::new(TableInfo {
        schema: "db".into(),
        name: "owners".into(),
        columns: vec![
            Column::simple("id", "int"),
            Column::simple("ts", "timestamp"),
        ],
        pk: vec![0],
        unique_keys: vec![],
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    });
    let p = three_alias_plan(
        pick("items"),
        pick("item_links"),
        o,
        vec![vec![(0, 0)], vec![(2, 0)]],
        vec![
            Edge {
                a: (1, 0),
                b: (0, 0),
                only_with: None,
            },
            Edge {
                a: (1, 0),
                b: (2, 1),
                only_with: None,
            },
        ],
    );
    assert_eq!(hop(&p, 1).unwrap_err(), "link compared by collation");
}

#[test]
fn left_join_owns_its_on_equality() {
    // `l`'s only usable edge is its own LEFT-join ON: it still hops.
    let v = plan(
        "SELECT i.id, l.child_id FROM items i LEFT JOIN item_links l ON l.parent_id = i.kind_id",
    );
    let ap = AccessPlan::of(&v);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    match &ap.occurrences[1].path {
        Path::Hop(h) => assert_eq!(h.partner, vec![r#"`i`.`kind_id`"#.to_string()]),
        other => panic!("expected a hop, got {other:?}"),
    }
}

#[test]
fn semi_join_tables_do_not_hop() {
    // The semi table is not in the FROM: the analyzer keeps the negated, key-exact
    // semi-join as a semi alias (the positive, key-covered ones are joined in place).
    let d = parse_definition(
        "CREATE VIEW v AS SELECT i.id FROM items i WHERE i.id NOT IN (SELECT id FROM kinds k)",
    )
    .unwrap();
    let v = crate::analyze::plan(&d, "db", &linked_catalog()).unwrap();
    let ap = AccessPlan::of(&v);
    let semi = ap
        .occurrences
        .iter()
        .find(|o| o.table == ("db".to_string(), "kinds".to_string()))
        .unwrap();
    let want = format!("`{}` is a semi-join table", semi.alias);
    match &semi.path {
        Path::Untraced { reason } => assert_eq!(reason, &want),
        other => panic!("path: {other:?}"),
    }
    match &ap.route {
        Route::ViewPlan { reason } => assert!(reason.ends_with(&want), "{reason}"),
        r => panic!("route: {r:?}"),
    }
}

#[test]
fn collated_links_do_not_hop() {
    // A non-utf8-liked varchar is compared by collation: the binlog value cannot stand
    // for the SQL value, so the hop is refused.
    let latin = |n: &str| {
        crate::catalog::Column::new(
            n,
            "varchar",
            "varchar(10)",
            Some("latin1"),
            Some("latin1_swedish_ci"),
            None,
            false,
        )
    };
    let mk = |name: &str, cols: Vec<crate::catalog::Column>| {
        let t = crate::catalog::TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols,
            pk: vec![0],
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    };
    let tables = std::collections::HashMap::from([
        mk("entities", vec![latin("id"), latin("code")]),
        mk("links", vec![latin("ref"), latin("id")]),
    ]);
    let d = parse_definition(
        "CREATE VIEW v AS SELECT e.id FROM entities e JOIN links l ON l.ref = e.id",
    )
    .unwrap();
    let v = plan_block(&d, "db", &tables).unwrap();
    let ap = AccessPlan::of(&v);
    match &ap.occurrences[1].path {
        Path::Untraced { reason } => assert_eq!(reason, "link compared by collation"),
        other => panic!("path: {other:?}"),
    }
}

/// Equivalent formulations of the same view plan identical EXPLAIN bodies.
#[test]
fn equivalent_formulations_same_body() {
    let defs = [
        "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
         JOIN owners o ON o.id = i.owner_id",
        "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id IN (i.kind_id) \
         JOIN owners o ON o.id IN (i.owner_id)",
        "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON i.kind_id = l.parent_id \
         JOIN owners o ON i.owner_id = o.id",
        "SELECT i.id, l.child_id, o.name FROM items i, item_links l, owners o \
         WHERE l.parent_id = i.kind_id AND o.id = i.owner_id",
        "SELECT i.id, l.child_id, o.name FROM items i, item_links l, owners o \
         WHERE o.id = i.owner_id AND l.parent_id = i.kind_id",
    ];
    let mut bodies = Vec::new();
    for def in defs {
        let v = plan(def);
        assert!(matches!(AccessPlan::of(&v).route, Route::Dedup), "{def}");
        bodies.push(AccessPlan::of(&v).body());
    }
    for b in &bodies[1..] {
        assert_eq!(b, &bodies[0], "differing body:\n{b}\n---\n{}", bodies[0]);
    }
}

/// The hop of an alias does not depend on where the tables sit in the FROM / JOIN order:
/// link or partner tables first give the same paths, partner columns and id positions.
#[test]
fn permuted_from_orders_same_hops() {
    let defs = [
        "SELECT i.id, MAX(o.name) n, MAX(l.child_id) m FROM items i JOIN item_links l \
         ON l.parent_id = i.kind_id JOIN owners o ON o.id = i.owner_id GROUP BY i.id",
        "SELECT i.id, MAX(o.name) n, MAX(l.child_id) m FROM item_links l JOIN items i \
         ON i.kind_id = l.parent_id JOIN owners o ON o.id = i.owner_id GROUP BY i.id",
        "SELECT i.id, MAX(o.name) n, MAX(l.child_id) m FROM owners o JOIN items i \
         ON o.id = i.owner_id JOIN item_links l ON l.parent_id = i.kind_id GROUP BY i.id",
    ];
    for def in defs {
        let v = plan(def);
        let ap = AccessPlan::of(&v);
        assert!(matches!(ap.route, Route::Dedup), "{def}\n{ap}");
        let l = ap.occurrences.iter().find(|o| o.alias == "l").unwrap();
        let o = ap.occurrences.iter().find(|o| o.alias == "o").unwrap();
        match (&l.path, &o.path) {
            (Path::Hop(lh), Path::Hop(oh)) => {
                assert_eq!(lh.link_cols, vec![1], "{def}");
                assert_eq!(lh.partner, vec![r#"`i`.`kind_id`"#.to_string()], "{def}");
                assert_eq!(lh.positions, vec![0], "{def}");
                assert_eq!(oh.link_cols, vec![0], "{def}");
                assert_eq!(oh.partner, vec![r#"`i`.`owner_id`"#.to_string()], "{def}");
                assert_eq!(oh.positions, vec![0], "{def}");
            }
            other => panic!("{def}: {other:?}"),
        }
    }
}

/// A depth-3 chain: the id is reached from `o` through three tables
/// (`o` -> `item_links` -> `item_values` -> `items`), the union of the one shortest path.
#[test]
fn three_hops_depth_three_chain() {
    let v = plan(
        "SELECT i.id, MAX(o.name) n FROM items i JOIN item_values v ON v.item_id = i.id \
         JOIN item_links l ON l.child_id = v.key_id JOIN owners o ON o.id = l.parent_id \
         GROUP BY i.id",
    );
    let ap = AccessPlan::of(&v);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    let o = ap.occurrences.iter().find(|o| o.alias == "o").unwrap();
    match &o.path {
        Path::Hop(h) => {
            assert_eq!(h.aliases, vec![0, 1, 2], "{ap}");
            assert_eq!(h.link_cols, vec![0], "{ap}");
            assert_eq!(h.partner, vec![r#"`l`.`parent_id`"#.to_string()], "{ap}");
            assert_eq!(h.positions, vec![0], "{ap}");
            assert!(
                h.select.contains("`v`.`item_id` = `i`.`id`"),
                "{}",
                h.select
            );
            assert!(
                h.select.contains("`l`.`child_id` = `v`.`key_id`"),
                "{}",
                h.select
            );
            assert!(
                !h.select.contains("`o`.`id` = `l`.`parent_id`"),
                "{}",
                h.select
            );
            assert!(
                h.select.starts_with("SELECT DISTINCT `l`.`parent_id`"),
                "{}",
                h.select
            );
        }
        other => panic!("expected a hop, got {other:?}"),
    }
}

/// A view whose lookup aliases all hop is on the dedup route.
#[test]
fn all_hops_are_dedup() {
    let v = plan(
        "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
         JOIN owners o ON o.id = i.owner_id",
    );
    let ap = AccessPlan::of(&v);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    assert_eq!(ap.route_name(), "dedup");
    assert!(
        ap.occurrences[1..]
            .iter()
            .all(|o| matches!(o.path, Path::Hop(_))),
        "{ap}"
    );
}

/// The exact EXPLAIN text of an incremental view: one cascade traced directly (the units
/// straight from the parent's image) and one derived through a pinned presence column; the
/// header fingerprint is read back by `fingerprint_of`.
#[test]
fn explain_pins_cascade_traced_directly() {
    use crate::catalog::{FkAction, ForeignKey};
    let cat = linked_catalog();
    let mut i = (*cat[&("db".into(), "item_values".into())]).clone();
    let fk = |parent: &str, cols: Vec<usize>, pcols: &[&str]| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols: cols,
        parent_cols: pcols.iter().map(|c| c.to_string()).collect(),
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    i.cascades = vec![
        fk("item_links", vec![0, 1], &["child_id", "parent_id"]),
        fk("kinds", vec![1], &["id"]),
    ];
    let mut cat = cat;
    cat.insert(("db".into(), "item_values".into()), Arc::new(i));
    let d = parse_definition(
        "CREATE VIEW v AS SELECT v.item_id, v.key_id, v.val FROM item_values v WHERE v.key_id = 7",
    )
    .unwrap();
    let ap = AccessPlan::of(&plan_block(&d, "db", &cat).unwrap());
    let want = [
        format!("view db.v route dedup fp {}", ap.fingerprint),
        "id 0 `v`.`item_id` (output 0)".into(),
        "id 1 `v`.`key_id` (output 1)".into(),
        "occ v db.item_values read (0, 1, 2) filter 1 IN (7) -> direct id = (0, 1)".into(),
        "cascade db.item_links -> db.item_values (0, 1) on delete CASCADE on update CASCADE \
         via direct (0, 1)"
            .into(),
        "cascade db.kinds -> db.item_values (0) on delete CASCADE on update CASCADE \
         via derived partial () presence (_lv_c0) reach 0 IN (7)"
            .into(),
    ]
    .join("\n")
        + "\n";
    assert_eq!(ap.to_string(), want);
    assert_eq!(
        AccessPlan::fingerprint_of(&ap.to_string()),
        Some(ap.fingerprint.as_str())
    );
}

/// The exact EXPLAIN text of an incremental view whose cascading parent the view reads
/// through a lineage alias: the cascade is traced, the parent occurrence hops to the id.
#[test]
fn explain_pins_traced_cascade() {
    use crate::catalog::{FkAction, ForeignKey};
    let cat = linked_catalog();
    let mut i = (*cat[&("db".into(), "items".into())]).clone();
    i.cascades = vec![ForeignKey {
        parent: ("db".into(), "kinds".into()),
        child_cols: vec![1],
        parent_cols: vec!["id".into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    }];
    let mut cat = cat;
    cat.insert(("db".into(), "items".into()), Arc::new(i));
    let d = parse_definition(
        "CREATE VIEW v AS SELECT i.id, k.name FROM items i LEFT JOIN kinds k ON k.id = i.kind_id",
    )
    .unwrap();
    let ap = AccessPlan::of(&plan_block(&d, "db", &cat).unwrap());
    let want = [
        format!("view db.v route dedup fp {}", ap.fingerprint),
        "id 0 `i`.`id` (output 0)".into(),
        "occ i db.items read (0, 1) -> direct id = (0)".into(),
        "occ k db.kinds read (0, 1) -> hop 0 = `i`.`kind_id` -> id[0] (2 tables)".into(),
        "cascade db.kinds -> db.items (0) on delete CASCADE on update CASCADE via traced".into(),
    ]
    .join("\n")
        + "\n";
    assert_eq!(ap.to_string(), want);
    assert_eq!(
        AccessPlan::fingerprint_of(&ap.to_string()),
        Some(ap.fingerprint.as_str())
    );
}

/// The exact EXPLAIN text of a Full route: the reason in the header, one whole occurrence.
#[test]
fn explain_pins_full_route_reason() {
    let ap = AccessPlan::of(&plan("SELECT DISTINCT kind_id FROM items"));
    match &ap.route {
        Route::Full { reason } => assert_eq!(reason, "DISTINCT"),
        r => panic!("route: {r:?}"),
    }
    let want = [
        format!("view db.v route full (DISTINCT) fp {}", ap.fingerprint),
        "occ items db.items read (1) -> whole".into(),
    ]
    .join("\n")
        + "\n";
    assert_eq!(ap.to_string(), want);
    assert_eq!(
        AccessPlan::fingerprint_of(&ap.to_string()),
        Some(ap.fingerprint.as_str())
    );
}

/// A chosen output column is the id; the join's constant filter stays on the occurrence.
#[test]
fn plan_access_bare_select_outputs() {
    let p = plan_access(
        "SELECT i.id AS _key, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
         AND v.key_id = 99",
        IdSpec::Outputs {
            names: &["_key"],
            keys_marker: None,
        },
        "db",
        &linked_catalog(),
    )
    .unwrap();
    assert!(
        matches!(p.access.route, Route::Dedup),
        "{:?}",
        p.access.route
    );
    assert_eq!(p.access.id.parts[0].output, Some(0));
    let v = p
        .access
        .occurrences
        .iter()
        .find(|o| o.alias == "v")
        .unwrap();
    assert!(matches!(v.path, Path::Direct { .. }), "{:?}", v.path);
    assert_eq!(v.filters.len(), 1);
    assert_eq!(v.filters[0].col, 1, "key_id");
    assert_eq!(v.filters[0].values, vec![Datum::Int(99)]);
}

/// The marker conjunct is gone before planning: the plan is the marker-less one's.
#[test]
fn keys_marker_is_removed() {
    let base = "SELECT i.id AS _key, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
                AND v.key_id = 99";
    let id = |m: Option<&'static str>| IdSpec::Outputs {
        names: &["_key"],
        keys_marker: m,
    };
    let plain = plan_access(base, id(None), "db", &linked_catalog()).unwrap();
    let with = plan_access(
        &format!("{base} WHERE i.id IN (:keys)"),
        id(Some(":keys")),
        "db",
        &linked_catalog(),
    )
    .unwrap();
    assert_eq!(with.plan.where_sql, None);
    let body = |a: &AccessPlan| {
        a.to_string()
            .lines()
            .skip(1)
            .map(String::from)
            .collect::<Vec<_>>()
    };
    assert_eq!(body(&with.access), body(&plain.access));
}

/// A column equal to the id on every row may carry the marker.
#[test]
fn keys_marker_on_an_equal_column() {
    let p = plan_access(
        "SELECT i.id AS _key, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
         AND v.key_id = 99 WHERE v.item_id IN (:keys)",
        IdSpec::Outputs {
            names: &["_key"],
            keys_marker: Some(":keys"),
        },
        "db",
        &linked_catalog(),
    );
    assert!(p.is_ok(), "{:?}", p.err());
}

/// A column that is not an id column is refused; a missing marker is refused too.
#[test]
fn keys_marker_on_another_column_is_refused() {
    let e = plan_access(
        "SELECT i.id AS _key, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
         AND v.key_id = 99 WHERE i.kind_id IN (:keys)",
        IdSpec::Outputs {
            names: &["_key"],
            keys_marker: Some(":keys"),
        },
        "db",
        &linked_catalog(),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("not an id column"), "{e}");
    let e = plan_access(
        "SELECT i.id AS _key, v.val FROM items i JOIN item_values v ON v.item_id = i.id",
        IdSpec::Outputs {
            names: &["_key"],
            keys_marker: Some(":keys"),
        },
        "db",
        &linked_catalog(),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("not found"), "{e}");
}

/// The tables to load are the ones the FROM clause uses.
#[test]
fn access_tables_lists_what_to_load() {
    let t = access_tables(
        "SELECT i.id AS _key, k.name FROM items i JOIN kinds k ON k.id = i.kind_id",
        IdSpec::Outputs {
            names: &["_key"],
            keys_marker: None,
        },
        "db",
    )
    .unwrap();
    assert_eq!(
        t,
        vec![
            ("db".to_string(), "items".to_string()),
            ("db".to_string(), "kinds".to_string())
        ]
    );
}

/// A `CREATE VIEW` with `IdSpec::Unit` plans exactly as `AccessPlan::of` does.
#[test]
fn create_view_unit_matches_of() {
    let p = plan_access(
        "CREATE VIEW v AS SELECT i.id FROM items i",
        IdSpec::Unit,
        "db",
        &linked_catalog(),
    )
    .unwrap();
    let want = AccessPlan::of(&plan("SELECT i.id FROM items i"));
    assert_eq!(p.access, want);
    assert_eq!(p.access.fingerprint, want.fingerprint);
}
