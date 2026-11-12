use super::*;

const EDGE_DDL: &[&str] = &[
    // utf8mb4_unicode_ci: case- and accent-insensitive, PAD SPACE ('abc' = 'ÁBC' = 'abc ').
    "CREATE TABLE e_parent (
       id INT NOT NULL PRIMARY KEY,
       grp INT NULL,
       code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
       amount DECIMAL(10,3) NULL,
       at DATETIME(6) NULL,
       kind ENUM('b','a','c') NULL,
       flags BIT(8) NULL,
       big BIGINT UNSIGNED NULL,
       KEY k_grp (grp), KEY k_code (code)
     )",
    "CREATE TABLE e_child (
       id INT NOT NULL PRIMARY KEY,
       parent_id INT NULL,
       code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
       qty INT NULL,
       KEY k_parent (parent_id), KEY k_code (code)
     )",
    // No primary key: duplicate rows.
    "CREATE TABLE e_bag (a INT NULL, b VARCHAR(8) NULL)",
];

const CODES: &[&str] = &[
    "'abc'", "'ABC'", "'abc '", "'ábc'", "'x'", "'X '", "NULL", "'🙂'", "'y'",
];
const AMOUNTS: &[&str] = &[
    "NULL",
    "1.5",
    "1.500",
    "0.001",
    "-2.25",
    "10",
    "999999.999",
    "0",
];
const KINDS: &[&str] = &["NULL", "'a'", "'b'", "'c'"];
const FLAGS: &[&str] = &["NULL", "b'0'", "b'11111111'", "b'1010'"];
const BIGS: &[&str] = &["NULL", "0", "18446744073709551615", "42"];
const QTYS: &[&str] = &["NULL", "-1", "0", "1", "5", "7"];

fn edge_cases() -> Vec<Case> {
    vec![
        case(
            "edge_null_join",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, parent_id, qty FROM e_child)
             SELECT p.id, p.grp, c.id AS child, c.qty FROM e_parent p LEFT JOIN c ON c.parent_id = p.id",
        ),
        case(
            "edge_null_group",
            "incremental",
            "grouped",
            "WITH t AS (SELECT grp, COUNT(*) AS n, SUM(amount) AS s FROM e_parent GROUP BY grp)
             SELECT grp, n, s FROM t WHERE n >= 1",
        ),
        case(
            "edge_distinct_nulls",
            "incremental",
            "grouped",
            "SELECT DISTINCT grp, kind FROM e_parent",
        ),
        case(
            "edge_bag_distinct",
            "incremental",
            "grouped",
            "SELECT DISTINCT a, b FROM e_bag",
        ),
        case(
            "edge_bag_cte",
            "incremental",
            "grouped",
            "WITH x AS (SELECT a, b FROM e_bag WHERE a IS NOT NULL) SELECT a, COUNT(*) AS n, COUNT(b) AS nb FROM x GROUP BY a",
        ),
        // Join keys equal under the collation; both sides projected (no representative to pick).
        case(
            "edge_ci_join",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, code, qty FROM e_child)
             SELECT p.id, p.code AS pcode, c.id AS cid, c.code AS ccode, c.qty FROM e_parent p JOIN c ON c.code = p.code",
        ),
        // Grouping under the collation; outputs that do not depend on the group's representative.
        case(
            "edge_ci_group",
            "incremental",
            "grouped",
            "SELECT COUNT(*) AS n, MIN(HEX(code)) AS lo, MAX(HEX(code)) AS hi FROM e_parent GROUP BY code",
        ),
        // DECIMAL scale, DATETIME(6), ENUM, BIGINT UNSIGNED max as unit keys.
        case(
            "edge_types_units",
            "incremental",
            "grouped",
            "SELECT DISTINCT amount, at, kind, big FROM e_parent",
        ),
        case(
            "edge_bit_group",
            "incremental",
            "grouped",
            "SELECT flags, COUNT(*) AS n FROM e_parent GROUP BY flags",
        ),
        // Rows moving between groups, key swaps, delete + reinsert.
        case(
            "edge_key_moves",
            "incremental",
            "grouped",
            "WITH c AS (SELECT id, parent_id, qty FROM e_child WHERE qty IS NOT NULL)
             SELECT parent_id, SUM(qty) AS total, COUNT(*) AS n FROM c GROUP BY parent_id",
        ),
        case(
            "edge_semi_nulls",
            "incremental",
            "spj",
            "SELECT p.id, p.code FROM e_parent p WHERE p.id IN (SELECT parent_id FROM e_child WHERE qty > 0)",
        ),
        case(
            "edge_not_exists",
            "incremental",
            "spj",
            "SELECT p.id, p.grp FROM e_parent p WHERE NOT EXISTS (SELECT 1 FROM e_child c WHERE c.parent_id = p.id)",
        ),
        // A NULL in the subquery makes NOT IN false everywhere.
        case(
            "edge_not_in_nulls",
            "incremental",
            "spj",
            "SELECT p.id FROM e_parent p WHERE p.id NOT IN (SELECT parent_id FROM e_child)",
        ),
        case(
            "edge_having",
            "incremental",
            "grouped",
            "SELECT grp, SUM(amount) AS s, COUNT(*) AS n FROM e_parent GROUP BY grp HAVING SUM(amount) > 10",
        ),
        // One CTE referenced three times, through different columns.
        case(
            "edge_self_3x",
            "incremental",
            "spj",
            "WITH p AS (SELECT id, grp, code FROM e_parent)
             SELECT a.id, b.id AS b_id, c.id AS c_id FROM p a
             JOIN p b ON b.grp = a.grp AND b.id > a.id
             JOIN p c ON c.code = a.code AND c.id <> a.id",
        ),
        case(
            "edge_nested",
            "incremental",
            "spj",
            "WITH a AS (SELECT * FROM (WITH b AS (SELECT id, grp, amount FROM e_parent) SELECT id, grp FROM b WHERE amount > 0) d)
             SELECT id, grp FROM a",
        ),
        case(
            "edge_shadow",
            "incremental",
            "spj",
            "WITH e_child AS (SELECT * FROM e_child WHERE qty > 1) SELECT c.id, c.parent_id, c.code FROM e_child c",
        ),
        // NULL-extended rows feeding the DISTINCT tuple.
        case(
            "edge_left_distinct",
            "full",
            "group key on the NULL side of a LEFT JOIN",
            "SELECT DISTINCT p.grp, c.qty FROM e_parent p LEFT JOIN e_child c ON c.parent_id = p.id",
        ),
        case(
            "edge_agg_key_join",
            "incremental",
            "grouped",
            "WITH t AS (SELECT parent_id, SUM(qty) AS q, COUNT(*) AS n FROM e_child GROUP BY parent_id)
             SELECT p.id, p.code, t.q, t.n FROM e_parent p JOIN t ON t.parent_id = p.id",
        ),
    ]
}

fn pick<'a>(g: &mut Gen, xs: &[&'a str]) -> &'a str {
    xs[g.range(0, xs.len() as i64 - 1) as usize]
}

fn edge_seed() -> Vec<String> {
    let mut g = Gen::new(7);
    let mut out = Vec::new();
    for id in 1..=24 {
        out.push(format!(
            "INSERT INTO e_parent VALUES ({id}, {}, {}, {}, '2024-01-{:02} 10:00:00.{:06}', {}, {}, {})",
            ["NULL", "1", "2", "3"][id % 4],
            CODES[id % CODES.len()],
            AMOUNTS[id % AMOUNTS.len()],
            1 + id % 5,
            id * 7,
            KINDS[id % KINDS.len()],
            FLAGS[id % FLAGS.len()],
            BIGS[id % BIGS.len()],
        ));
    }
    for id in 1..=60 {
        let parent = if id % 9 == 0 {
            "NULL".to_string()
        } else {
            g.range(1, 28).to_string()
        };
        out.push(format!(
            "INSERT INTO e_child VALUES ({id}, {parent}, {}, {})",
            pick(&mut g, CODES),
            pick(&mut g, QTYS)
        ));
    }
    out.push(
        "INSERT INTO e_bag VALUES (1,'a'),(1,'a'),(1,'a'),(1,NULL),(1,NULL),(NULL,'a'),(2,'b'),(NULL,NULL),(NULL,NULL)"
            .into(),
    );
    out
}

/// Scripted transactions, each a list of statements run in one transaction.
fn edge_steps() -> Vec<(&'static str, Vec<String>)> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let mut steps = vec![
        ("move between groups", s(&[
            "UPDATE e_child SET parent_id = 2 WHERE id = 1",
            "UPDATE e_child SET parent_id = NULL WHERE id = 2",
        ])),
        ("swap two primary keys", s(&[
            "UPDATE e_child SET id = -id WHERE id IN (5, 6)",
            "UPDATE e_child SET id = 6 WHERE id = -5",
            "UPDATE e_child SET id = 5 WHERE id = -6",
        ])),
        ("swap two join keys", s(&[
            "UPDATE e_child SET parent_id = CASE id WHEN 3 THEN 4 WHEN 4 THEN 3 END WHERE id IN (3, 4)",
        ])),
        ("delete and reinsert", s(&[
            "DELETE FROM e_child WHERE id = 7",
            "INSERT INTO e_child VALUES (7, 3, 'ABC', 9)",
        ])),
        ("parent key change", s(&["UPDATE e_parent SET id = 100 WHERE id = 1"])),
        ("NULL group keys", s(&[
            "UPDATE e_parent SET grp = NULL WHERE id IN (2, 3)",
            "UPDATE e_parent SET grp = 1 WHERE grp IS NULL AND id > 20",
        ])),
        ("duplicates come and go", s(&[
            "INSERT INTO e_bag VALUES (3,'c'),(3,'c')",
            "UPDATE e_bag SET b = 'a' WHERE b = 'b'",
            "DELETE FROM e_bag WHERE a IS NULL AND b IS NULL LIMIT 1",
        ])),
        ("HAVING crossed upwards", s(&["UPDATE e_parent SET amount = 50 WHERE id = 5"])),
        ("HAVING crossed downwards", s(&["UPDATE e_parent SET amount = 0 WHERE id = 5"])),
        ("group becomes empty", s(&["DELETE FROM e_parent WHERE grp = 2"])),
        ("group refilled", s(&[
            "INSERT INTO e_parent (id, grp, code, amount) VALUES (200, 2, 'abc', 3.000), (201, 2, 'ÁBC', 4.5)",
        ])),
        ("collation-equal values move", s(&[
            "UPDATE e_parent SET code = 'ABC' WHERE code = 'x'",
            "UPDATE e_child SET code = 'abc  ' WHERE id % 5 = 0",
        ])),
        ("every child at once", s(&["UPDATE e_child SET qty = COALESCE(qty, 0) + 1"])),
        ("type edges", s(&[
            "UPDATE e_parent SET at = at + INTERVAL 1 MICROSECOND WHERE id % 2 = 0",
            "UPDATE e_parent SET kind = 'c' WHERE kind = 'a'",
            "UPDATE e_parent SET big = 18446744073709551615 WHERE big = 42",
            "UPDATE e_parent SET flags = b'11110000' WHERE flags = b'0'",
            "UPDATE e_parent SET amount = 0.001 WHERE amount = 10",
        ])),
        ("children of one parent deleted", s(&["DELETE FROM e_child WHERE parent_id = 3"])),
    ];
    // The same row hammered 30 times (each its own transaction).
    for k in 0..30 {
        steps.push((
            "hammer",
            vec![format!(
                "UPDATE e_child SET qty = qty + {} WHERE id = 9",
                k % 3
            )],
        ));
    }
    steps
}

/// Random transactions over the small value domains (collisions, NULLs, duplicates).
fn edge_random(g: &mut Gen) -> Vec<String> {
    let mut stmts = Vec::new();
    for _ in 0..g.range(1, 4) {
        stmts.push(match g.range(0, 8) {
            0 => format!(
                "INSERT INTO e_child VALUES ({}, {}, {}, {}) ON DUPLICATE KEY UPDATE parent_id = VALUES(parent_id), qty = VALUES(qty)",
                g.range(1, 80),
                if g.chance(0.15) { "NULL".into() } else { g.range(1, 30).to_string() },
                pick(g, CODES),
                pick(g, QTYS)
            ),
            1 => format!("DELETE FROM e_child WHERE id = {}", g.range(1, 80)),
            2 => format!(
                "UPDATE e_child SET parent_id = {} WHERE id = {}",
                if g.chance(0.2) { "NULL".into() } else { g.range(1, 30).to_string() },
                g.range(1, 80)
            ),
            3 => format!(
                "UPDATE e_parent SET grp = {}, code = {} WHERE id = {}",
                ["NULL", "1", "2", "3"][g.range(0, 3) as usize],
                pick(g, CODES),
                g.range(1, 30)
            ),
            4 => format!(
                "INSERT IGNORE INTO e_parent (id, grp, code, amount, kind) VALUES ({}, {}, {}, {}, {})",
                g.range(1, 40),
                ["NULL", "1", "2", "3"][g.range(0, 3) as usize],
                pick(g, CODES),
                pick(g, AMOUNTS),
                pick(g, KINDS)
            ),
            5 => format!("DELETE FROM e_parent WHERE id = {}", g.range(1, 40)),
            6 => format!(
                "INSERT INTO e_bag VALUES ({}, {})",
                ["NULL", "1", "2", "3"][g.range(0, 3) as usize],
                ["NULL", "'a'", "'b'", "'c'"][g.range(0, 3) as usize]
            ),
            7 => format!(
                "DELETE FROM e_bag WHERE a {} LIMIT 1",
                ["IS NULL", "= 1", "= 2", "= 3"][g.range(0, 3) as usize]
            ),
            _ => format!(
                "UPDATE e_parent SET amount = {} WHERE id = {}",
                pick(g, AMOUNTS),
                g.range(1, 40)
            ),
        });
    }
    stmts
}

async fn run_edge(ctx: &Ctx, label: &str, seed: u64, window_ms: &str) -> Result<()> {
    let db = ctx.new_db(&format!("cte_edge_{label}")).await?;
    let mut c = db.conn().await?;
    for d in EDGE_DDL {
        exec(&mut c, d).await?;
    }
    for s in edge_seed() {
        exec(&mut c, &s).await?;
    }
    let (specs, expect) = prepare(&db, ctx.server().is_mariadb(), edge_cases()).await?;
    let mut eng = start_checked(ctx, &db, &specs, &expect, window_ms).await?;
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: initial build"),
    )
    .await?;
    let mut last = "";
    for (what, stmts) in edge_steps() {
        txn(&mut c, &stmts, false)
            .await
            .with_context(|| what.to_string())?;
        if what == "hammer" {
            last = what;
            continue;
        }
        verify(ctx, &mut eng, &db, &specs, &format!("{label}: {what}")).await?;
        last = what;
    }
    verify(
        ctx,
        &mut eng,
        &db,
        &specs,
        &format!("{label}: after {last}"),
    )
    .await?;
    let mut g = Gen::new(seed);
    for round in 1..=3 {
        for _ in 0..scaled(60) {
            txn(&mut c, &edge_random(&mut g), false).await?;
        }
        verify(
            ctx,
            &mut eng,
            &db,
            &specs,
            &format!("{label}: random round {round}"),
        )
        .await?;
    }
    assert_ready(&mut c, &names(&specs)).await?;
    eng.stop().await?;
    c.disconnect().await.ok();
    Ok(())
}

/// Every taxonomy class, a random workload in two window settings and two seeds.
#[test]
fn cte_views_match_their_oracles() {
    scenario("cte", |ctx| async move {
        for (k, window) in [(0, "0"), (1, "50")] {
            let label = format!("w{window}_{k}");
            let seed = ctx.sub_seed(&label);
            run(&ctx, &label, seed, window)
                .await
                .with_context(|| format!("round {label} (sub-seed {seed})"))?;
        }
        Ok(())
    });
}

/// NULLs, duplicates without a key, key moves and swaps, collation-equal values, type edges,
/// emptied groups, HAVING thresholds and hammering, through CTEs, DISTINCT and semi-joins.
#[test]
fn cte_edge_cases_match_their_oracles() {
    scenario("cte_edge", |ctx| async move {
        for (k, window) in [(0, "0"), (1, "50")] {
            let label = format!("w{window}_{k}");
            let seed = ctx.sub_seed(&label);
            run_edge(&ctx, &label, seed, window)
                .await
                .with_context(|| format!("round {label} (sub-seed {seed})"))?;
        }
        Ok(())
    });
}

// --- foreign-key cascades (A1 / A3): the rows a cascade changes never reach the binlog.
