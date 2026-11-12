use super::*;

#[test]
fn analyzer_errors_mirror_the_server() {
    let err = |sql: &str| {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        analyzer_plan(&d, "db", &wide_catalog())
            .unwrap_err()
            .to_string()
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
        match analyzer_plan(&d, "db", &wide_catalog()) {
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
