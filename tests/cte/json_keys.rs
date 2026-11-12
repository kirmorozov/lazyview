use super::*;

const JSON_KEY_DDL: &[&str] = &[
    "CREATE TABLE docs (id INT NOT NULL PRIMARY KEY, doc JSON NULL, grp INT NULL)",
    r#"INSERT INTO docs VALUES (1,'{"a":1}',1),(2,'{"a": 1}',1),(3,'{"a":1.0}',2),(4,'{"b":2,"a":1}',1),(5,'{"a":1,"b":2}',2),(6,'null',1),(7,NULL,1),(8,'[1,2]',2),(9,'[1, 2]',1),(10,'{"n":{"x":[1,{"y":2}]}}',1),(11,'"str"',2),(12,'1',1),(13,'1.0',2),(14,'true',1),(15,'{"d":0.30000000000000004}',1),(16,'{"big":18446744073709551615}',1),(17,'{"neg":-0.0}',2),(18,'{"s":"\\u00e9"}',1),(19,'{"s":"é"}',2)"#,
];

/// The final verifier's JSON group-key cases; `j_grp2` (JSON in a composite key) is Full on the
/// MySQL family and a plain text unit on MariaDB (JSON is LONGTEXT there).
fn json_key_cases(mariadb: bool) -> Vec<Case> {
    vec![
        case(
            "j_grp",
            "incremental",
            "grouped",
            "SELECT doc, COUNT(*) AS n FROM docs GROUP BY doc",
        ),
        case(
            "j_distinct",
            "incremental",
            "grouped",
            "SELECT DISTINCT doc FROM docs",
        ),
        if mariadb {
            case(
                "j_grp2",
                "incremental",
                "grouped",
                "SELECT grp, doc, COUNT(*) AS n FROM docs GROUP BY grp, doc",
            )
        } else {
            case(
                "j_grp2",
                "full",
                "JSON value `doc` in a composite group key: MySQL compares JSON by representation in multi-row IN lists",
                "SELECT grp, doc, COUNT(*) AS n FROM docs GROUP BY grp, doc",
            )
        },
        // The same through a CTE with renamed columns.
        if mariadb {
            case(
                "j_cte2",
                "incremental",
                "grouped",
                "WITH x AS (SELECT grp AS g, doc AS j FROM docs) SELECT g, j, COUNT(*) AS n FROM x GROUP BY g, j",
            )
        } else {
            case(
                "j_cte2",
                "full",
                "JSON value `docs.doc` in a composite group key: MySQL compares JSON by representation in multi-row IN lists",
                "WITH x AS (SELECT grp AS g, doc AS j FROM docs) SELECT g, j, COUNT(*) AS n FROM x GROUP BY g, j",
            )
        },
        case(
            "j_sum",
            "incremental",
            "grouped",
            "SELECT doc, SUM(grp) AS s, MIN(id) AS m FROM docs GROUP BY doc",
        ),
        case(
            "j_ifnull",
            "incremental",
            "grouped",
            "SELECT IFNULL(doc, '{}') AS k, COUNT(*) AS n FROM docs GROUP BY k",
        ),
        case(
            "j_unquote",
            "incremental",
            "grouped",
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(doc, '$.a')) AS a, COUNT(*) AS n FROM docs GROUP BY a",
        ),
        case(
            "j_spj",
            "incremental",
            "spj",
            "SELECT id, doc FROM docs WHERE grp = 1",
        ),
        // A window unit is reduced by keys like GROUP BY: the key `id` determines `doc`, so
        // `PARTITION BY id, doc` is a plain key unit (the key alone) on both families.
        case(
            "j_win",
            "incremental",
            "grouped",
            "SELECT id, grp, SUM(grp) OVER (PARTITION BY id, doc) AS s FROM docs",
        ),
    ]
}

const JSON_KEY_STEPS: &[(&str, &[&str])] = &[
    (
        "move within a JSON-equal group, other spelling",
        &[r#"UPDATE docs SET doc = '{"a":1.0}' WHERE id = 1"#],
    ),
    (
        "delete a group's first row",
        &["DELETE FROM docs WHERE id = 2"],
    ),
    (
        "JSON null and SQL NULL swap",
        &[
            "UPDATE docs SET doc = 'null' WHERE id = 3",
            "UPDATE docs SET doc = NULL WHERE id = 6",
        ],
    ),
    (
        "group key unchanged, other column changes",
        &["UPDATE docs SET grp = 2 WHERE id = 4"],
    ),
    (
        "another spelling, keys swapped",
        &[
            r#"INSERT INTO docs VALUES (20, '{"a" : 1 , "b" : 2}', 1), (21, '{"b":2, "a":1.00}', 2)"#,
        ],
    ),
];

/// Why G5 exists, for the log: one-tuple vs two-tuple row IN over JSON-equal spellings (the
/// MySQL family finds 3 rows, then 1). lazyview must be right either way.
async fn json_row_in_canary(ctx: &Ctx) -> Result<()> {
    if ctx.server().is_mariadb() {
        // (No JSON type: JSON is LONGTEXT, CAST(… AS JSON) does not parse.)
        ctx.log("JSON row-IN canary: not applicable on MariaDB");
        return Ok(());
    }
    let db = ctx.new_db("cte_json_canary").await?;
    let mut c = db.conn().await?;
    for s in [
        "CREATE TABLE jd (id INT PRIMARY KEY, g INT, doc JSON)",
        r#"INSERT INTO jd VALUES (1, 2, '{"a": 1, "b": 2}'), (2, 2, '{"b":2, "a":1.00}'), (3, 2, '{"a":1,"b":2}')"#,
    ] {
        exec(&mut c, s).await?;
    }
    let one = scalar_i64(
        &mut c,
        r#"SELECT COUNT(*) FROM jd WHERE (g, doc) IN ((2, CAST('{"a": 1.0, "b": 2}' AS JSON)))"#,
    )
    .await?;
    let two = scalar_i64(
        &mut c,
        r#"SELECT COUNT(*) FROM jd WHERE (g, doc) IN ((2, CAST('{"a": 1.0, "b": 2}' AS JSON)), (3, CAST('{}' AS JSON)))"#,
    )
    .await?;
    ctx.log(format!(
        "JSON row-IN canary: one tuple {one} rows, two tuples {two} rows{}",
        if one != two {
            " (multi-row IN compares JSON by representation)"
        } else {
            ""
        }
    ));
    c.disconnect().await.ok();
    Ok(())
}

fn json_key_steps() -> Steps {
    step_groups(JSON_KEY_STEPS)
}

const SUITE_JSON_KEYS: Suite<'static> = Suite {
    tag: "jkey",
    ddl: JSON_KEY_DDL,
    cases: |mariadb| Cases::Plain(json_key_cases(mariadb)),
    steps: json_key_steps,
    plans: None,
    plan_budget: None,
};

/// G5: JSON group keys stay equal to their oracles; a JSON column in a composite key is Full on
/// the MySQL family (its refresh filter would compare JSON by representation).
#[test]
fn cte_json_group_keys_match_their_oracles() {
    scenario("cte_jkey", |ctx| async move {
        json_row_in_canary(&ctx).await?;
        run_suite(&ctx, &SUITE_JSON_KEYS, "w0", "0").await?;
        run_suite(&ctx, &SUITE_JSON_KEYS, "w50", "50").await?;
        Ok(())
    });
}
