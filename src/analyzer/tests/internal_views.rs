use super::*;

/// Aggregated CTEs joined on anything but a key become internal views; the parent is planned
/// over their tables once they exist, and identical subtrees share one view.
#[test]
fn internal_views_for_aggregates_joined_on_non_keys() {
    let sql = "WITH t AS (SELECT customer_id, SUM(total) AS s FROM orders GROUP BY customer_id) \
               SELECT c.id, t.s FROM customers c JOIN t ON t.customer_id = c.region_id";
    let mut cat = wide_catalog();
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
    let (k, t) = table(&iv.name, &[("customer_id", "int"), ("s", "decimal")], &[]);
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
    let mut cat = wide_catalog();
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
    let mut cat = wide_catalog();
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
    let mut cat = wide_catalog();
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
    let (k, t) = table(&ivs[0].name, &[("id", "int"), ("amount", "decimal")], &[]);
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
    let mut cat = wide_catalog();
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
    let (k, t) = table(&j.name, &[("_lv_c0", "int"), ("_lv_c1", "int")], &[]);
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
    let mut cat = wide_catalog();
    let super::Outcome::NeedsInternal(ivs) = outcome(sql, &cat) else {
        panic!("expected the tuple-lineage view")
    };
    assert!(
        ivs[0].sql.ends_with("WHERE (`o`.status <> 'x')"),
        "{}",
        ivs[0].sql
    );
    let (k, t) = table(
        &ivs[0].name,
        &[("_lv_c0", "int"), ("_lv_c1", "varchar")],
        &[],
    );
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
        super::analyze(&d, "db", &wide_catalog(), &opts).unwrap()
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
