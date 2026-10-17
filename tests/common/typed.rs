//! Type-torture schema: every interesting MySQL type through filters, group keys and join keys,
//! with boundary values (unsigned max, signed min, negative decimals, fractional seconds, emoji,
//! binary bytes, JSON incl. JSON null, case-insensitive collation keys) and NULLs.

use super::oracle::{Kind, ViewSpec};
use super::q;
use super::workload::Gen;

pub fn ddl() -> Vec<String> {
    vec![
        "CREATE TABLE t_types (
           id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
           u64 BIGINT UNSIGNED NULL,
           i64 BIGINT NULL,
           i32 INT NULL,
           u8 TINYINT UNSIGNED NULL,
           i8 TINYINT NULL,
           m24 MEDIUMINT NULL,
           d DECIMAL(20,6) NULL,
           dbig DECIMAL(65,30) NULL,
           f DOUBLE NULL,
           e ENUM('a','b','c','ö') NULL,
           s SET('x','y','z') NULL,
           dt DATETIME(6) NULL,
           ts TIMESTAMP(6) NULL DEFAULT NULL,
           dte DATE NULL,
           tm TIME(3) NULL,
           yr YEAR NULL,
           j JSON NULL,
           ci VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
           cs VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
           emoji VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
           ch CHAR(5) NULL,
           bin VARBINARY(16) NULL,
           blb BLOB NULL,
           txt TEXT NULL,
           bt BIT(8) NULL,
           KEY k_ci (ci),
           KEY k_u64 (u64)
         )"
        .into(),
        "CREATE TABLE t_ref (
           id INT NOT NULL PRIMARY KEY,
           u64 BIGINT UNSIGNED NULL,
           d DECIMAL(20,6) NULL,
           ci VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
           dt DATETIME(6) NULL,
           e ENUM('a','b','c','ö') NULL,
           bin VARBINARY(16) NULL,
           label VARCHAR(40) NOT NULL,
           KEY k_u64 (u64),
           KEY k_ci (ci)
         )"
        .into(),
        "CREATE TABLE t_ckey (
           k1 VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
           k2 DATETIME(6) NOT NULL,
           k3 BIGINT UNSIGNED NOT NULL,
           v INT NULL,
           PRIMARY KEY (k1, k2, k3)
         )"
        .into(),
    ]
}

pub const U64: &[&str] = &["NULL", "0", "1", "42", "9223372036854775808", "18446744073709551614", "18446744073709551615"];
pub const I64: &[&str] = &["NULL", "-9223372036854775808", "9223372036854775807", "-1", "0", "7"];
pub const I32: &[&str] = &["NULL", "-2147483648", "2147483647", "-5", "5"];
pub const U8: &[&str] = &["NULL", "0", "255", "128"];
pub const I8: &[&str] = &["NULL", "-128", "127", "-1"];
pub const M24: &[&str] = &["NULL", "-8388608", "8388607", "0"];
pub const DEC: &[&str] = &["NULL", "-99999999999999.999999", "99999999999999.999999", "0.000001", "-0.000001", "0", "12.5"];
pub const DBIG: &[&str] = &[
    "NULL",
    "1.000000000000000000000000000001",
    "-12345678901234567890123456789012345.123456789012345678901234567890",
    "0",
];
pub const DBL: &[&str] = &["NULL", "0", "-1.5", "3.25", "1e300", "-2.5e-300"];
pub const ENUMS: &[&str] = &["NULL", "'a'", "'b'", "'c'", "'ö'"];
pub const SETS: &[&str] = &["NULL", "''", "'x'", "'x,y'", "'x,y,z'", "'z'"];
pub const DT: &[&str] = &[
    "NULL",
    "'1000-01-01 00:00:00.000000'",
    "'9999-12-31 23:59:59.999999'",
    "'2021-06-01 12:00:00.500000'",
    "'2021-06-01 12:00:00.500001'",
    "'2021-06-01 12:00:00'",
];
pub const TS: &[&str] = &[
    "NULL",
    "'1971-01-01 00:00:00.000001'",
    "'2038-01-18 00:00:00.999999'",
    "'2021-06-01 12:00:00.500000'",
    "'2021-06-01 12:00:00.499999'",
];
pub const DATE: &[&str] = &["NULL", "'1000-01-01'", "'9999-12-31'", "'2024-02-29'"];
pub const TIME: &[&str] = &["NULL", "'-838:59:59.000'", "'838:59:59.000'", "'00:00:00.001'", "'-00:00:01.500'"];
pub const YEAR: &[&str] = &["NULL", "1901", "2155", "2000"];
pub const JSONS: &[&str] = &[
    "NULL",
    r#"'{"k": 1}'"#,
    r#"'{"k": "1"}'"#,
    "'[1, 2, 3]'",
    r#"'"str"'"#,
    "'null'",
    r#"'{"k": 1.5, "nested": {"a": [true, false, null]}}'"#,
    r#"'{"emoji": "😀"}'"#,
];
/// Case-insensitive group/join keys: ASCII only, so `UPPER()`/`LOWER()` of any group member is
/// the same (accent-insensitive equalities would make the displayed key nondeterministic).
pub const CI: &[&str] = &["NULL", "'abc'", "'ABC'", "'Abc'", "'xyz'", "'XYZ'", "''", "'mixed Case'", "'MIXED CASE'"];
pub const CS: &[&str] = &["NULL", "'abc'", "'ABC'", "'résumé'", "'RESUME'", "'a b'"];
pub const EMOJI: &[&str] = &[
    "NULL",
    "'😀'",
    "'😀😀'",
    "'👍🏽'",
    "'🇩🇪'",
    "'你好'",
    "'Ω'",
    "'plain'",
    r"'with \' quote'",
    r"'back\\slash'",
];
pub const CHARS: &[&str] = &["NULL", "'a'", "'ab'", "'abcde'", "''"];
pub const BIN: &[&str] = &["NULL", "X''", "X'00'", "X'00FF'", "X'FF'", "X'DEADBEEF'", "'abc'"];
pub const BLOB: &[&str] = &["NULL", "X'00010203'", "X''", "'text in blob'"];
pub const BITS: &[&str] = &["NULL", "b'0'", "b'11111111'", "b'1010'"];

fn txt(g: &mut Gen) -> String {
    match g.range(0, 3) {
        0 => "NULL".into(),
        1 => "''".into(),
        2 => q(&"long text ".repeat(30)),
        _ => q("multi\nline"),
    }
}

pub const COLS: &str = "id, u64, i64, i32, u8, i8, m24, d, dbig, f, e, s, dt, ts, dte, tm, yr, j, ci, cs, emoji, ch, bin, blb, txt, bt";

/// A full random `t_types` row as a VALUES tuple.
pub fn types_row(g: &mut Gen, id: u64) -> String {
    let vals = [
        id.to_string(),
        g.pick(U64).to_string(),
        g.pick(I64).to_string(),
        g.pick(I32).to_string(),
        g.pick(U8).to_string(),
        g.pick(I8).to_string(),
        g.pick(M24).to_string(),
        g.pick(DEC).to_string(),
        g.pick(DBIG).to_string(),
        g.pick(DBL).to_string(),
        g.pick(ENUMS).to_string(),
        g.pick(SETS).to_string(),
        g.pick(DT).to_string(),
        g.pick(TS).to_string(),
        g.pick(DATE).to_string(),
        g.pick(TIME).to_string(),
        g.pick(YEAR).to_string(),
        g.pick(JSONS).to_string(),
        g.pick(CI).to_string(),
        g.pick(CS).to_string(),
        g.pick(EMOJI).to_string(),
        g.pick(CHARS).to_string(),
        g.pick(BIN).to_string(),
        g.pick(BLOB).to_string(),
        txt(g),
        g.pick(BITS).to_string(),
    ];
    format!("({})", vals.join(", "))
}

fn ref_row(g: &mut Gen, id: i64) -> String {
    format!(
        "({id}, {}, {}, {}, {}, {}, {}, 'r{id}')",
        g.pick(U64),
        g.pick(DEC),
        g.pick(CI),
        g.pick(DT),
        g.pick(ENUMS),
        g.pick(BIN)
    )
}

/// Non-NULL value from a domain.
fn nn<'a>(g: &mut Gen, xs: &'a [&'a str]) -> &'a str {
    loop {
        let v = *g.pick(xs);
        if v != "NULL" {
            return v;
        }
    }
}

fn ckey_row(g: &mut Gen) -> String {
    let k1 = *g.pick(&["'abc'", "'ABC'", "'def'", "'DEF'", "'résumé'", "'😀'"]);
    format!("({k1}, {}, {}, {})", nn(g, DT), nn(g, U64), g.pick(I32))
}

/// Seed rows: `n` t_types rows, n/2 t_ref rows, n/2 t_ckey rows.
pub fn seed(g: &mut Gen, n: u64) -> Vec<String> {
    let rows: Vec<String> = (1..=n).map(|id| types_row(g, id)).collect();
    let refs: Vec<String> = (1..=(n as i64 / 2).max(4)).map(|id| ref_row(g, id)).collect();
    let ck: Vec<String> = (0..(n / 2).max(4)).map(|_| ckey_row(g)).collect();
    vec![
        format!("INSERT INTO t_types ({COLS}) VALUES {}", rows.join(", ")),
        format!("INSERT INTO t_ref (id, u64, d, ci, dt, e, bin, label) VALUES {}", refs.join(", ")),
        format!("INSERT INTO t_ckey (k1, k2, k3, v) VALUES {} ON DUPLICATE KEY UPDATE v = VALUES(v)", ck.join(", ")),
    ]
}

/// One random change (a few statements meant to run in one transaction).
pub fn random_change(g: &mut Gen) -> Vec<String> {
    let id = g.range(1, 90);
    let id2 = g.range(1, 90);
    let col_set: &[(&str, &[&str])] = &[
        ("u64", U64),
        ("i64", I64),
        ("i32", I32),
        ("u8", U8),
        ("i8", I8),
        ("m24", M24),
        ("d", DEC),
        ("dbig", DBIG),
        ("f", DBL),
        ("e", ENUMS),
        ("s", SETS),
        ("dt", DT),
        ("ts", TS),
        ("dte", DATE),
        ("tm", TIME),
        ("yr", YEAR),
        ("j", JSONS),
        ("ci", CI),
        ("cs", CS),
        ("emoji", EMOJI),
        ("ch", CHARS),
        ("bin", BIN),
        ("blb", BLOB),
        ("bt", BITS),
    ];
    match g.range(0, 11) {
        0 | 1 | 2 => {
            let (c1, d1) = *g.pick(col_set);
            let (c2, d2) = *g.pick(col_set);
            vec![format!("UPDATE t_types SET {c1} = {}, {c2} = {} WHERE id = {id}", g.pick(d1), g.pick(d2))]
        }
        3 => {
            let row = types_row(g, id as u64);
            vec![format!("REPLACE INTO t_types ({COLS}) VALUES {row}")]
        }
        4 => {
            let row = types_row(g, id as u64);
            vec![format!("INSERT INTO t_types ({COLS}) VALUES {row} ON DUPLICATE KEY UPDATE ci = VALUES(ci), u64 = VALUES(u64), d = VALUES(d)")]
        }
        5 => vec![format!("DELETE FROM t_types WHERE id = {id}")],
        6 => vec![format!("UPDATE IGNORE t_types SET id = {} WHERE id = {id}", id2 + 100)],
        7 => {
            // predicate on typed columns, possibly hitting several rows (CI match, unsigned max)
            let (c, dom) = *g.pick(&[("ci", CI), ("u64", U64), ("e", ENUMS), ("bin", BIN), ("dt", DT)][..]);
            let v = nn(g, dom);
            let (c2, d2) = *g.pick(col_set);
            vec![format!("UPDATE t_types SET {c2} = {} WHERE {c} = {v} ORDER BY id LIMIT 3", g.pick(d2))]
        }
        8 => {
            let rid = g.range(1, 40);
            let (c, dom) = *g.pick(&[("u64", U64), ("d", DEC), ("ci", CI), ("dt", DT), ("e", ENUMS), ("bin", BIN)][..]);
            if g.chance(0.2) {
                vec![format!("DELETE FROM t_ref WHERE id = {rid}")]
            } else if g.chance(0.3) {
                let row = ref_row(g, rid);
                vec![format!("REPLACE INTO t_ref (id, u64, d, ci, dt, e, bin, label) VALUES {row}")]
            } else {
                vec![format!("UPDATE t_ref SET {c} = {} WHERE id = {rid}", g.pick(dom))]
            }
        }
        9 => {
            let row = ckey_row(g);
            vec![format!("INSERT INTO t_ckey (k1, k2, k3, v) VALUES {row} ON DUPLICATE KEY UPDATE v = VALUES(v)")]
        }
        10 => {
            let k1 = *g.pick(&["'abc'", "'def'", "'résumé'"]);
            match g.range(0, 3) {
                // PK changes that only change case (same key under the collation)
                0 => vec![format!("UPDATE t_ckey SET k1 = UPPER(k1) WHERE k1 = {k1}")],
                1 => vec![format!("UPDATE t_ckey SET k1 = LOWER(k1) WHERE k1 = {k1}")],
                2 => vec![format!("UPDATE IGNORE t_ckey SET k2 = {} WHERE k1 = {k1} ORDER BY k2 LIMIT 1", nn(g, DT))],
                _ => vec![format!("DELETE FROM t_ckey WHERE k1 = {k1} ORDER BY k2 LIMIT 1")],
            }
        }
        _ => {
            let id = g.range(100, 200) as u64;
            let row = types_row(g, id);
            vec![format!("INSERT IGNORE INTO t_types ({COLS}) VALUES {row}")]
        }
    }
}

pub fn views() -> Vec<ViewSpec> {
    use Kind::*;
    vec![
        ViewSpec::new(
            "vt_all",
            Incremental,
            "SELECT id, u64, i64, i32, u8, i8, d, f, e, s, dt, ts, j, ci, cs, emoji, ch, bin FROM t_types",
        ),
        ViewSpec::new("vt_extra", Incremental, "SELECT id, m24, dbig, dte, tm, yr, blb, txt, bt FROM t_types"),
        ViewSpec::new(
            "vt_filter",
            Incremental,
            "SELECT id, u64, d, e, s, ts FROM t_types
             WHERE u64 > 9223372036854775807 OR d < 0 OR e = 'b' OR FIND_IN_SET('y', s) > 0
                OR ts >= '2021-06-01 12:00:00.500000' OR JSON_EXTRACT(j, '$.k') = 1 OR emoji = '😀'",
        ),
        ViewSpec::new(
            "vt_by_enum",
            Incremental,
            "SELECT e, COUNT(*) AS n, SUM(d) AS sd, MIN(dt) AS min_dt, MAX(ts) AS max_ts FROM t_types GROUP BY e",
        ),
        ViewSpec::new("vt_by_set", Incremental, "SELECT s, COUNT(*) AS n, MAX(u64) AS max_u64 FROM t_types GROUP BY s"),
        ViewSpec::new(
            "vt_by_ci",
            Incremental,
            "SELECT UPPER(ci) AS k, COUNT(*) AS n, SUM(i32) AS si FROM t_types GROUP BY ci",
        ),
        ViewSpec::new("vt_by_lower", Incremental, "SELECT LOWER(ci) AS k, COUNT(*) AS n FROM t_types GROUP BY LOWER(ci)"),
        ViewSpec::new("vt_by_dt", Incremental, "SELECT dt, COUNT(*) AS n, SUM(i64) AS s FROM t_types GROUP BY dt"),
        ViewSpec::new("vt_by_ts", Incremental, "SELECT ts, COUNT(*) AS n FROM t_types GROUP BY ts"),
        ViewSpec::new(
            "vt_by_dec",
            Incremental,
            "SELECT d, COUNT(*) AS n, MIN(i8) AS min_i8, MAX(i8) AS max_i8 FROM t_types GROUP BY d",
        ),
        ViewSpec::new("vt_by_u64", Incremental, "SELECT u64, COUNT(*) AS n, SUM(u8) AS su8 FROM t_types GROUP BY u64"),
        ViewSpec::new("vt_by_emoji", Incremental, "SELECT emoji, COUNT(*) AS n FROM t_types GROUP BY emoji"),
        ViewSpec::new("vt_by_bin", Incremental, "SELECT bin, COUNT(*) AS n FROM t_types GROUP BY bin"),
        ViewSpec::new("vt_by_neg", Incremental, "SELECT i64, i8, COUNT(*) AS n FROM t_types GROUP BY i64, i8"),
        ViewSpec::new(
            "vt_by_json",
            Incremental,
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(j, '$.k')) AS jk, COUNT(*) AS n FROM t_types GROUP BY jk",
        ),
        ViewSpec::new(
            "vt_join_u64",
            Incremental,
            "SELECT t.id, t.u64, r.id AS rid, r.label FROM t_types t JOIN t_ref r ON r.u64 = t.u64",
        ),
        ViewSpec::new(
            "vt_join_ci",
            Incremental,
            "SELECT t.id, t.ci, r.id AS rid, r.ci AS rci FROM t_types t JOIN t_ref r ON r.ci = t.ci",
        ),
        ViewSpec::new(
            "vt_join_dec_dt",
            Incremental,
            "SELECT t.id, r.id AS rid, t.d, t.dt FROM t_types t JOIN t_ref r ON r.d = t.d AND r.dt = t.dt",
        ),
        ViewSpec::new(
            "vt_left_enum_bin",
            Incremental,
            "SELECT r.id, r.label, t.id AS tid FROM t_ref r LEFT JOIN t_types t ON t.e = r.e AND t.bin = r.bin",
        ),
        ViewSpec::new(
            "vt_ref_groups",
            Incremental,
            "SELECT r.label, COUNT(*) AS n, SUM(t.d) AS sd, MAX(t.u64) AS mu FROM t_types t JOIN t_ref r ON r.ci = t.ci GROUP BY r.label",
        ),
        ViewSpec::new("vt_ckey", Incremental, "SELECT k1, k2, k3, v FROM t_ckey WHERE v IS NULL OR v >= 0"),
        ViewSpec::new(
            "vt_ckey_group",
            Incremental,
            "SELECT k2, COUNT(*) AS n, MAX(k3) AS mk3, SUM(v) AS sv FROM t_ckey GROUP BY k2",
        ),
    ]
}
