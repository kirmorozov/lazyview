use super::*;

/// G5: a JSON value in a composite unit is Full (MySQL compares JSON by representation in a row
/// IN list of two or more tuples, the engine's filter for several units). Decided on the value's
/// type, not on how it is written; a single JSON key, text and scalars over JSON, a unit the keys
/// reduce to columns without JSON, and MariaDB's JSON (LONGTEXT) are not affected. Also through
/// CTEs and derived tables with renamed columns, joins in every placement, and 1-3 hops.
#[test]
fn json_columns_in_composite_units() {
    let mut cat = wide_catalog();
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
    // A partition key that the rest of the key determines is reduced like a GROUP BY key (the
    // partitions are the same sets): a JSON column next to the key drops out of the unit, while
    // without the key's other column it is not determined and the view stays Full.
    for sql in [
        "SELECT id, SUM(grp) OVER (PARTITION BY id, doc) AS s FROM docs",
        "SELECT a, SUM(grp) OVER (PARTITION BY a, b, doc) AS s FROM docs_ab",
    ] {
        grouped(sql);
    }
    full("SELECT a, SUM(grp) OVER (PARTITION BY a, doc) AS s FROM docs_ab");
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
    // A one-element `IN (y)` is a key equality for the unit planner: the key reduces the unit.
    for from in [
        "items i JOIN docs d ON d.id IN (i.doc_id)",
        "items i, docs d WHERE d.id IN (i.doc_id)",
    ] {
        grouped(&format!(
            "SELECT i.id, d.doc, COUNT(*) AS n FROM {from} GROUP BY i.id, d.doc"
        ));
    }
}

/// The VIEW_DEFINITION validator: the server's canonical text (MySQL and MariaDB printing)
/// reads the same tables and column count; a different reading is an error naming it.
#[test]
fn validate_against_the_canonical_definition() {
    let cat = wide_catalog();
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
