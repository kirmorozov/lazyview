use super::*;

const CAPTURE_DDL: &[&str] = &[
    "CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, name VARCHAR(20) NOT NULL, ref INT NULL)",
    "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, customer_id INT NULL, total DECIMAL(10,2) NOT NULL)",
    "CREATE TABLE payments (id INT NOT NULL PRIMARY KEY, order_id INT NULL, amount DECIMAL(10,2) NOT NULL)",
    "INSERT INTO customers VALUES (1,'ann',2),(2,'bob',NULL),(3,'cy',1),(4,'dee',4)",
    "INSERT INTO orders VALUES (10,1,100),(11,1,700),(20,2,450),(30,3,50),(31,3,300),(40,4,900)",
    "INSERT INTO payments VALUES (100,10,100),(110,11,500),(200,20,450),(300,30,50),(310,31,300)",
];

/// (name, detail, template): `{D}` derived / CTE alias, `{I}` its inner alias, `{S}` / `{T}` the
/// subquery's aliases.
const CAPTURE_TEMPLATES: &[(&str, &str, &str)] = &[
    (
        "exists_join",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS
         (SELECT 1 FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.amount > 400)",
    ),
    (
        "not_exists_join",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE NOT EXISTS
         (SELECT 1 FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.amount > 400)",
    ),
    (
        "cte_exists_join",
        "spj",
        "WITH {D} AS (SELECT {I}.id, {I}.customer_id FROM orders {I}) SELECT {D}.id FROM {D} WHERE EXISTS
         (SELECT 1 FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {S}.customer_id = {D}.customer_id AND {T}.amount > 400)",
    ),
    (
        "exists_self",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.ref FROM customers {I}) {D} WHERE EXISTS
         (SELECT 1 FROM customers {S} WHERE {S}.ref = {D}.id)",
    ),
    (
        "exists_other",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS
         (SELECT 1 FROM payments {S} WHERE {S}.order_id = {D}.id AND {S}.amount > 400)",
    ),
    (
        "exists_other_grouped",
        "grouped",
        "SELECT {D}.customer_id, COUNT(*) AS n FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE EXISTS
         (SELECT 1 FROM payments {S} WHERE {S}.order_id = {D}.id AND {S}.amount > 400) GROUP BY {D}.customer_id",
    ),
    (
        "in_join",
        "spj",
        "SELECT {D}.id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) {D} WHERE {D}.customer_id IN
         (SELECT {S}.customer_id FROM orders {S} JOIN payments {T} ON {T}.order_id = {S}.id WHERE {T}.amount > 400)",
    ),
    (
        "nested_derived",
        "spj",
        "SELECT {D}.id FROM (SELECT e.id, e.customer_id FROM (SELECT {I}.id, {I}.customer_id FROM orders {I}) e) {D}
         WHERE EXISTS (SELECT 1 FROM payments {S} WHERE {S}.order_id = {D}.id AND {S}.amount > 400)",
    ),
];

/// The reviewer's aliases (`d`, `o`, `o`, `p`), a control without collisions, and seeded random
/// renamings from a small pool (collisions are likely).
fn capture_cases() -> Vec<Case> {
    let pool = ["o", "d", "p", "s", "x", "O", "e2"];
    let mut seed: u64 = std::env::var("LAZYVIEW_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xf3f3);
    let mut pick = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        pool[(seed % pool.len() as u64) as usize]
    };
    let mut out = Vec::new();
    for (name, detail, t) in CAPTURE_TEMPLATES {
        let mut variants = vec![("d", "o", "o", "p"), ("x", "o", "s", "q")];
        while variants.len() < 5 {
            let v = (pick(), pick(), pick(), pick());
            if !v.2.eq_ignore_ascii_case(v.3) && !v.0.eq_ignore_ascii_case("e") {
                variants.push(v);
            }
        }
        for (k, (d, i, s, x)) in variants.into_iter().enumerate() {
            let select = t
                .replace("{D}", d)
                .replace("{I}", i)
                .replace("{S}", s)
                .replace("{T}", x);
            let name: &'static str = Box::leak(format!("cap_{name}_{k}").into_boxed_str());
            let select: &'static str = Box::leak(select.into_boxed_str());
            out.push(case(name, "incremental", detail, select));
        }
    }
    out
}

fn capture_steps() -> Steps {
    steps(&[
        (
            "payment over the threshold",
            "UPDATE payments SET amount = 401 WHERE id = 310",
        ),
        (
            "payment under the threshold",
            "UPDATE payments SET amount = 1 WHERE id = 110",
        ),
        (
            "order moved",
            "UPDATE orders SET customer_id = 2 WHERE id = 31",
        ),
        ("payment deleted", "DELETE FROM payments WHERE id = 200"),
        (
            "payment added",
            "INSERT INTO payments VALUES (400, 40, 900)",
        ),
        (
            "referrer changed",
            "UPDATE customers SET ref = 3 WHERE id = 2",
        ),
    ])
}

const SUITE_CAPTURE: Suite<'static> = Suite {
    tag: "capture",
    ddl: CAPTURE_DDL,
    cases: |_| Cases::Plain(capture_cases()),
    steps: capture_steps,
    plans: None,
    plan_budget: None,
};

/// F3: correlated subqueries over a merged derived table / CTE keep their meaning whatever the
/// aliases (the reviewer's colliding ones, a control, seeded random renamings).
#[test]
fn cte_semi_joins_never_capture_aliases() {
    scenario("cte_capture", |ctx| async move {
        run_suite(&ctx, &SUITE_CAPTURE, "w0", "0").await?;
        run_suite(&ctx, &SUITE_CAPTURE, "w50", "50").await?;
        Ok(())
    });
}

// --- F5: NOT IN through re-executed refresh statements (MariaDB re-uses a stale NOT IN result
// of a prepared statement after the subquery flips between empty and non-empty).

const KEY_EXACT_DDL: &[&str] = &[
    "CREATE TABLE c (id INT NOT NULL PRIMARY KEY, num INT NOT NULL, name VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, ref INT NULL)",
    "CREATE TABLE s_str (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "CREATE TABLE s_spk (code VARCHAR(10) NOT NULL PRIMARY KEY, v INT NULL)",
    "CREATE TABLE s_bin (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "CREATE TABLE s_int (id INT NOT NULL PRIMARY KEY, code INT NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "INSERT INTO c VALUES (1,1,'abc',NULL),(2,2,'xyz',5),(3,7,'ABD',7)",
    "INSERT INTO s_str VALUES (1,'1',10),(2,'01',20),(3,'abc',30),(4,'7',40)",
    "INSERT INTO s_spk VALUES ('1',10),('01',20),('7',40)",
    "INSERT INTO s_bin VALUES (1,'abc',1),(2,'ABC',2),(3,'xyz',3)",
    "INSERT INTO s_int VALUES (1,1,10),(2,7,70)",
    // G4: TIMESTAMP keys (the fall-back hour of a DST time zone; the test servers run in UTC).
    "CREATE TABLE c_t (id INT NOT NULL PRIMARY KEY, dt DATETIME NOT NULL, ts TIMESTAMP NULL)",
    "CREATE TABLE k_ts (id INT NOT NULL PRIMARY KEY, k TIMESTAMP NOT NULL, v INT NULL, UNIQUE KEY u_k (k))",
    "CREATE TABLE k_dt (id INT NOT NULL PRIMARY KEY, k DATETIME NOT NULL, v INT NULL, UNIQUE KEY u_k (k))",
    "INSERT INTO c_t VALUES (1,'2026-01-01 00:00:00','2026-01-01 00:00:00'),(2,'2026-01-02 10:00:00',NULL),(3,'2026-11-01 01:30:00','2026-11-01 01:30:00')",
    "INSERT INTO k_ts VALUES (1,'2026-01-01 00:00:00',1),(2,'2026-11-01 01:30:00',2)",
    "INSERT INTO k_dt VALUES (1,'2026-01-01 00:00:00',1),(2,'2026-01-02 10:00:00',2)",
    // Other formulations (guard R4): composite keys, integer widths, an outer column one join away.
    "CREATE TABLE s_comp (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, b VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, v INT NULL, UNIQUE KEY u_ab (a, b))",
    "CREATE TABLE s_big (id INT NOT NULL PRIMARY KEY, code BIGINT NOT NULL, v INT NULL, UNIQUE KEY u_code (code))",
    "CREATE TABLE c_link (id INT NOT NULL PRIMARY KEY, c_id INT NOT NULL, num INT NOT NULL)",
    "INSERT INTO s_comp VALUES (1,1,'1',1),(2,1,'01',2),(3,7,'7',3),(4,2,'xyz',4),(5,3,'abc',5)",
    "INSERT INTO s_big VALUES (1,1,1),(2,7,2),(3,9007199254740993,3)",
    "INSERT INTO c_link VALUES (1,1,1),(2,2,7),(3,3,2)",
];

/// The final verifier's repros, with the routes the analyzer must choose: a string key against
/// a number (`'1'`, `'01'` and `'1.0'` all equal 1), under an explicit weaker collation (G1) or
/// a TIMESTAMP on either side (G4) matches several key rows, so an inner join would repeat the
/// outer row: semi-join aliases (lookups). INT and DATETIME keys are the controls (joined).
fn key_exact_cases() -> Vec<(Case, &'static str)> {
    vec![
        (
            case(
                "dup_uq_in_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_str s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dup_uq_exists_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_str s WHERE s.code = c.num)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dup_pk_in_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_spk s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dup_uq_in_collate",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.name COLLATE utf8mb4_general_ci IN (SELECT b.code FROM s_bin b)",
            ),
            "c:unit,b:lookup",
        ),
        (
            case(
                "ctl_uq_in_int",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_int s)",
            ),
            "c:unit,s:lineage",
        ),
        (
            case(
                "ts_in_dt",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.dt IN (SELECT s.k FROM k_ts s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "ts_exists_dt",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE EXISTS (SELECT 1 FROM k_ts s WHERE s.k = c.dt)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "ts_in_ts",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.ts IN (SELECT s.k FROM k_ts s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "dt_in_ts",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.ts IN (SELECT s.k FROM k_dt s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "ctl_dt_in_dt",
                "incremental",
                "spj",
                "SELECT c.id FROM c_t c WHERE c.dt IN (SELECT s.k FROM k_dt s)",
            ),
            "c:unit,s:lineage",
        ),
        // Swapped sides; a composite key with one non-exact part, and an exact one; INT vs
        // BIGINT; an outer column one join away.
        (
            case(
                "swap_exists_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM s_str s WHERE c.num = s.code)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "comp_mixed",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE (c.num, c.num) IN (SELECT s.a, s.b FROM s_comp s)",
            ),
            "c:unit,s:lookup",
        ),
        (
            case(
                "comp_exact",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE (c.num, c.name) IN (SELECT s.a, s.b FROM s_comp s)",
            ),
            "c:unit,s:lineage",
        ),
        (
            case(
                "big_in_int",
                "incremental",
                "spj",
                "SELECT c.id FROM c WHERE c.num IN (SELECT s.code FROM s_big s)",
            ),
            "c:unit,s:lineage",
        ),
        (
            case(
                "hop_numstr",
                "incremental",
                "spj",
                "SELECT c.id FROM c JOIN c_link l ON l.c_id = c.id WHERE l.num IN (SELECT s.code FROM s_str s)",
            ),
            "c:unit,l:unit,s:lookup",
        ),
    ]
}

const KEY_EXACT_STEPS: &[(&str, &[&str])] = &[
    (
        "a third numeric spelling",
        &[
            "INSERT INTO s_str VALUES (5,'1.0',50)",
            "INSERT INTO s_spk VALUES ('1.0',50)",
        ],
    ),
    (
        "key changed by UPDATE",
        &[
            "UPDATE s_str SET code = '8' WHERE id = 2",
            "UPDATE s_bin SET code = 'abd' WHERE id = 2",
            "UPDATE k_ts SET k = '2026-01-02 10:00:00' WHERE id = 2",
            "UPDATE k_dt SET k = '2026-11-01 01:30:00' WHERE id = 1",
            "UPDATE s_comp SET b = '2' WHERE id = 4",
            "UPDATE c_link SET num = 1 WHERE id = 3",
        ],
    ),
];

fn key_exact_steps() -> Steps {
    step_groups(KEY_EXACT_STEPS)
}

const SUITE_KEY_EXACT: Suite<'static> = Suite {
    tag: "keyx",
    ddl: KEY_EXACT_DDL,
    cases: |_| Cases::Traced(key_exact_cases()),
    steps: key_exact_steps,
    plans: Some(Plans {
        what: "key-exact plans",
        // the routes are the whole check here, and `run_suite` compares them.
        check: |_, _, _| None,
    }),
    plan_budget: None,
};

/// G1 / G4: semi-joins over string keys compared with numbers or under a weaker COLLATE, and
/// over TIMESTAMP keys or against TIMESTAMP columns, stay semi-joins (no repeated outer rows);
/// the INT and DATETIME keys still join.
#[test]
fn cte_key_exact_semi_joins_match_their_oracles() {
    scenario("cte_keyx", |ctx| async move {
        run_suite(&ctx, &SUITE_KEY_EXACT, "w0", "0").await?;
        run_suite(&ctx, &SUITE_KEY_EXACT, "w50", "50").await?;
        Ok(())
    });
}

// --- G2 / G3: comments before CREATE VIEW, a byte-order mark, a bare SELECT without a name.
