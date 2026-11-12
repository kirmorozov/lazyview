use super::*;

const VIRTUAL_DDL: &[&str] = &[
    "CREATE TABLE g1 (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NULL,
     name VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, dt DATETIME NULL, dt6 DATETIME(6) NULL,
     doc JSON NULL,
     gi INT AS (a / 3) VIRTUAL, gd DECIMAL(6,1) AS (a / 3) VIRTUAL, gs INT AS (a / 3) STORED,
     gbin VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin AS (name) VIRTUAL,
     gdate DATE AS (dt) VIRTUAL, gdate6 DATE AS (dt6) VIRTUAL, gchain INT AS (gi * 10) VIRTUAL,
     gsum INT AS (a + IFNULL(b, 0)) VIRTUAL, gsc INT AS (gsum * 10) VIRTUAL,
     jname VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin AS (JSON_UNQUOTE(JSON_EXTRACT(doc, '$.name'))) VIRTUAL)",
    "CREATE TABLE r (id INT NOT NULL PRIMARY KEY, w INT NULL)",
    "INSERT INTO g1 (id, a, b, name, dt, dt6, doc) VALUES
     (1, 1, 2, 'Ann', '2026-01-01 23:59:59', '2026-01-01 23:59:59.700000', '{\"name\": \"x\"}'),
     (2, 2, NULL, 'ann', '2026-01-02 00:00:01', '2026-01-02 00:00:00.200000', '{\"name\": \"y\"}'),
     (3, 5, 1, 'Bob', NULL, NULL, NULL),
     (4, 3, 3, 'bob', '2026-01-01 00:00:00', '2026-01-01 12:00:00.500000', '{\"name\": \"x\"}')",
    "INSERT INTO r VALUES (10, 2), (20, 4), (30, 6), (40, 8), (50, NULL)",
];

fn virtual_cases() -> Vec<Case> {
    vec![
        // B1: the value converted to the declared type and collation, exactly as the server
        // reads the column.
        case("vt_proj_int", "incremental", "spj", "SELECT id, gi FROM g1"),
        case(
            "vt_where_int",
            "incremental",
            "spj",
            "SELECT id, a FROM g1 WHERE gi = 0",
        ),
        case(
            "vt_group_int",
            "incremental",
            "grouped",
            "SELECT gi, COUNT(*) AS n FROM g1 GROUP BY gi",
        ),
        case("vt_proj_dec", "incremental", "spj", "SELECT id, gd FROM g1"),
        case(
            "vt_group_bin",
            "incremental",
            "grouped",
            "SELECT gbin, COUNT(*) AS n FROM g1 GROUP BY gbin",
        ),
        case(
            "vt_distinct_bin",
            "incremental",
            "grouped",
            "SELECT DISTINCT gbin FROM g1",
        ),
        case(
            "vt_where_date",
            "incremental",
            "spj",
            "SELECT id FROM g1 WHERE gdate = '2026-01-01'",
        ),
        case(
            "vt_group_date",
            "incremental",
            "grouped",
            "SELECT gdate, COUNT(*) AS n FROM g1 GROUP BY gdate",
        ),
        // DATE from DATETIME(6): rounded on MySQL, truncated on MariaDB (each as its server).
        case(
            "vt_group_date6",
            "incremental",
            "grouped",
            "SELECT gdate6, COUNT(*) AS n FROM g1 GROUP BY gdate6",
        ),
        case(
            "vt_join_eq",
            "incremental",
            "spj",
            "SELECT r.id, g.id AS gid FROM r JOIN g1 g ON g.gi = r.w",
        ),
        case(
            "vt_chain_group",
            "incremental",
            "grouped",
            "SELECT gchain, COUNT(*) AS n FROM g1 GROUP BY gchain",
        ),
        case(
            "vt_topn",
            "incremental",
            "global",
            "SELECT id, gi FROM g1 ORDER BY gi DESC, id LIMIT 2",
        ),
        case(
            "vt_stored_ctl",
            "incremental",
            "spj",
            "SELECT id, gs FROM g1 WHERE gs >= 1",
        ),
        // Unaliased: named after its text, grouped by the same inlined expression.
        case(
            "vt_group_unaliased",
            "incremental",
            "grouped",
            "SELECT gi + 1, COUNT(*) AS n FROM g1 GROUP BY gi + 1",
        ),
        // B2: nested columns, window partitions and subqueries.
        case(
            "vl_chain_group",
            "incremental",
            "grouped",
            "SELECT gsc, COUNT(*) AS n FROM g1 GROUP BY gsc",
        ),
        case(
            "vl_window",
            "incremental",
            "grouped",
            "SELECT id, SUM(a) OVER (PARTITION BY gsum) AS s FROM g1",
        ),
        case(
            "vl_in_inner",
            "incremental",
            "spj",
            "SELECT r.id, r.w FROM r WHERE r.w IN (SELECT g.gsum FROM g1 g)",
        ),
        case(
            "vl_exists_inner",
            "incremental",
            "spj",
            "SELECT r.id, r.w FROM r WHERE EXISTS (SELECT 1 FROM g1 g WHERE g.gsum = r.w)",
        ),
        case(
            "vl_not_in_inner",
            "incremental",
            "spj",
            "SELECT r.id, r.w FROM r WHERE r.w NOT IN (SELECT g.gsum FROM g1 g)",
        ),
        case(
            "vl_group_sum",
            "incremental",
            "grouped",
            "SELECT gsum, COUNT(*) AS n FROM g1 GROUP BY gsum",
        ),
        // B3: a string literal in the expression (MySQL prints it escaped in the catalog).
        case(
            "vj_group",
            "incremental",
            "grouped",
            "SELECT jname, COUNT(*) AS n FROM g1 GROUP BY jname",
        ),
    ]
}

fn virtual_steps() -> Steps {
    steps(&[
        ("base column of gi / gchain / gsum", "UPDATE g1 SET a = 3 WHERE id = 1"),
        ("collation-only change of gbin", "UPDATE g1 SET name = 'ANN' WHERE id = 2"),
        ("datetime of gdate", "UPDATE g1 SET dt = '2026-01-01 12:00:00' WHERE id = 3"),
        ("other base column of gsum", "UPDATE g1 SET b = 5 WHERE id = 2"),
        ("row deleted", "DELETE FROM g1 WHERE id = 4"),
        (
            "row inserted",
            "INSERT INTO g1 (id, a, b, name, dt, dt6, doc) VALUES
             (5, 4, 4, 'Cat', '2026-03-01 10:00:00', '2026-03-01 23:59:59.600000', '{\"name\": \"z\"}')",
        ),
        ("every row", "UPDATE g1 SET a = a + 1"),
        ("fraction of gdate6", "UPDATE g1 SET dt6 = '2026-01-01 00:00:00.400000' WHERE id = 1"),
        ("JSON of jname", "UPDATE g1 SET doc = '{\"name\": \"y\"}' WHERE id = 5"),
        ("outer row", "UPDATE r SET w = 3 WHERE id = 10"),
    ])
}

const SUITE_VIRTUAL: Suite<'static> = Suite {
    tag: "virtual",
    ddl: VIRTUAL_DDL,
    cases: |_| Cases::Plain(virtual_cases()),
    steps: virtual_steps,
    plans: None,
    plan_budget: None,
};

/// B1 / B2 / B3: VIRTUAL generated columns are inlined with their declared type and collation
/// (nested ones, window partitions and subqueries included), from the initial build on.
#[test]
fn cte_virtual_columns_match_their_oracles() {
    scenario("cte_virtual", |ctx| async move {
        run_suite(&ctx, &SUITE_VIRTUAL, "w0", "0").await?;
        run_suite(&ctx, &SUITE_VIRTUAL, "w50", "50").await?;
        Ok(())
    });
}

// --- F1 / F2 / F4: integer VIRTUAL columns at the edges of their range, nesting fan-out.

const VIRTUAL_INT_DDL: &[&str] = &[
    // Values as stored: the rows are written in a non-strict sql_mode (strict mode refuses them).
    "SET SESSION sql_mode = ''",
    "CREATE TABLE vu (id INT NOT NULL PRIMARY KEY, u BIGINT UNSIGNED NULL, cat INT NULL,
     gi INT AS (u) VIRTUAL, giu INT UNSIGNED AS (u) VIRTUAL, gb BIGINT AS (u) VIRTUAL, gsum INT AS (u + 0) VIRTUAL)",
    "INSERT INTO vu (id, u, cat) VALUES (1, 5, 1), (2, 18446744073709551615, 1), (3, 9223372036854775808, 2),
     (4, 2147483648, 2), (5, NULL, 3)",
    "CREATE TABLE vd (id INT NOT NULL PRIMARY KEY, dbl DOUBLE NULL,
     gr INT AS (ROUND(dbl, 1)) VIRTUAL, gn BIGINT AS (-dbl) VIRTUAL, gm INT AS (dbl * 2) VIRTUAL)",
    "INSERT INTO vd (id, dbl) VALUES (1, 2.5), (2, -3.5), (3, NULL)",
    // 8-way fan-out per level: h2 inlines 64 base-column references, h6 262144.
    "CREATE TABLE vg (id INT NOT NULL PRIMARY KEY, a BIGINT NULL, h0 BIGINT AS (a) VIRTUAL,
     h1 BIGINT AS (GREATEST(h0, h0, h0, h0, h0, h0, h0, h0)) VIRTUAL,
     h2 BIGINT AS (GREATEST(h1, h1, h1, h1, h1, h1, h1, h1)) VIRTUAL,
     h3 BIGINT AS (GREATEST(h2, h2, h2, h2, h2, h2, h2, h2)) VIRTUAL,
     h4 BIGINT AS (GREATEST(h3, h3, h3, h3, h3, h3, h3, h3)) VIRTUAL,
     h5 BIGINT AS (GREATEST(h4, h4, h4, h4, h4, h4, h4, h4)) VIRTUAL,
     h6 BIGINT AS (GREATEST(h5, h5, h5, h5, h5, h5, h5, h5)) VIRTUAL)",
    "INSERT INTO vg (id, a) VALUES (1, 10), (2, NULL), (3, -4)",
];

fn virtual_int_cases() -> Vec<Case> {
    vec![
        // F1: an unsigned value >= 2^63 clips like the column (CAST AS SIGNED would wrap).
        case("vu_proj", "incremental", "spj", "SELECT id, gi, giu, gb, gsum FROM vu"),
        case("vu_where", "incremental", "spj", "SELECT id FROM vu WHERE gi = 2147483647"),
        case("vu_group", "incremental", "grouped", "SELECT gi, COUNT(*) AS n FROM vu GROUP BY gi"),
        case(
            "vu_sum",
            "incremental",
            "grouped",
            "SELECT cat, SUM(gb) AS s, MIN(giu) AS m FROM vu GROUP BY cat",
        ),
        // F2: a DOUBLE function result out of BIGINT range clips (MySQL's CAST would raise 1690).
        case("vd_round", "incremental", "spj", "SELECT id, gr FROM vd"),
        case("vd_neg", "incremental", "spj", "SELECT id, gn FROM vd"),
        case("vd_group", "incremental", "grouped", "SELECT gm, COUNT(*) AS n FROM vd GROUP BY gm"),
        // F4: within the budget the nesting is inlined, past it Full naming the column (fast).
        case("vg_h2", "incremental", "spj", "SELECT id, h2 FROM vg"),
        case(
            "vg_h6",
            "full",
            "VIRTUAL generated column `vg`.`h6` expands to 262144 base-column references (budget 256): its nested VIRTUAL columns are copied per reference",
            "SELECT id, h6 FROM vg",
        ),
    ]
}

fn virtual_int_steps() -> Steps {
    steps(&[
        (
            "u to 2^63 - 1",
            "UPDATE vu SET u = 9223372036854775807 WHERE id = 3",
        ),
        (
            "u past 2^63",
            "UPDATE vu SET u = 9223372036854775809 WHERE id = 3",
        ),
        (
            "u near 2^64 inserted",
            "INSERT INTO vu (id, u, cat) VALUES (6, 18446744073709551614, 3)",
        ),
        ("u max deleted", "DELETE FROM vu WHERE id = 2"),
        ("double 1e300", "INSERT INTO vd (id, dbl) VALUES (4, 1e300)"),
        ("double -1e19", "INSERT INTO vd (id, dbl) VALUES (5, -1e19)"),
        ("both deleted", "DELETE FROM vd WHERE id IN (4, 5)"),
        ("double 7.25", "INSERT INTO vd (id, dbl) VALUES (6, 7.25)"),
        ("nested base column", "UPDATE vg SET a = 7 WHERE id = 1"),
    ])
}

const SUITE_VINT: Suite<'static> = Suite {
    tag: "vint",
    ddl: VIRTUAL_INT_DDL,
    cases: |_| Cases::Plain(virtual_int_cases()),
    steps: virtual_int_steps,
    plans: None,
    plan_budget: Some(Duration::from_secs(10)),
};

/// F1 / F2 / F4: integer VIRTUAL columns over unsigned values >= 2^63 and out-of-range DOUBLE
/// function results clip like the column; a nested fan-out past the budget is Full, quickly.
#[test]
fn cte_virtual_integers_match_their_oracles() {
    scenario("cte_vint", |ctx| async move {
        run_suite(&ctx, &SUITE_VINT, "w0", "0").await?;
        run_suite(&ctx, &SUITE_VINT, "w50", "50").await?;
        Ok(())
    });
}

// --- F3: no alias capture between a semi-join subquery and merged outer aliases.
