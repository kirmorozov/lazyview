//! Upserts by row key: views with at most one row per unit (PRIMARY KEY on the unit, or the
//! unique index `_lv_row` when the unit can be NULL or is hidden) and select/join views whose
//! rows the unit and the lineage keys identify (`_lv_row` over both) write changed rows with
//! `INSERT ... ON DUPLICATE KEY UPDATE` and delete only rows that vanished; views without such
//! a key keep rewriting changed units. Edge cases: NULLs in keys (never conflict: deleted and
//! inserted again), collation-equal keys with other bytes (case, trailing spaces), DECIMAL /
//! JSON / TIME values and keys, rows moving between units, units growing and shrinking, and
//! statements split by rows and bytes. Every step ends with the oracle matching. Each test runs
//! on every flavor in `LAZYVIEW_TEST_DSNS`.

mod common;

use anyhow::{ensure, Result};
use common::*;
use mysql_async::Conn;

const DDL: &[&str] = &[
    "CREATE TABLE cats (id INT PRIMARY KEY, \
     code VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, \
     label VARCHAR(50) NULL)",
    "CREATE TABLE items (id INT PRIMARY KEY, cat_id INT NULL, \
     grp VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, \
     tag VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, \
     amount DECIMAL(12,4) NULL, doc JSON NULL, tm TIME(3) NULL, note VARCHAR(100) NULL, \
     KEY (cat_id), KEY (grp), KEY (tag))",
];

fn specs() -> Vec<ViewSpec> {
    vec![
        // One row per unit, nullable key: `_lv_row` over the unit (the NULL group is deleted and
        // inserted again); TIME and DECIMAL aggregates.
        ViewSpec::new(
            "u_by_grp",
            Kind::Incremental,
            "SELECT grp, COUNT(*) AS n, SUM(amount) AS total, MAX(tm) AS max_tm, MIN(note) AS first_note \
             FROM items GROUP BY grp",
        ),
        // The planner reduces the unit to the primary key (code depends on it): PRIMARY KEY (id).
        ViewSpec::new(
            "u_by_cat",
            Kind::Incremental,
            "SELECT c.id, c.code, COUNT(i.id) AS n, SUM(i.amount) AS total \
             FROM cats c LEFT JOIN items i ON i.cat_id = c.id GROUP BY c.id, c.code",
        ),
        // A NOT NULL string unit: the PRIMARY KEY; a code that changes case or loses a trailing
        // space is a new key with other bytes (the codes of these rows are distinct under the
        // collation).
        ViewSpec::new(
            "u_by_code",
            Kind::Incremental,
            "SELECT code, COUNT(*) AS n, MAX(id) AS last_id FROM cats WHERE id < 13 GROUP BY code",
        ),
        // Hidden, nullable unit: `_lv_row` over an invisible column.
        ViewSpec::new(
            "u_hidden_unit",
            Kind::Incremental,
            "SELECT COUNT(*) AS n, SUM(amount) AS total FROM items GROUP BY cat_id",
        ),
        // TIME and DECIMAL units.
        ViewSpec::new(
            "u_by_tm",
            Kind::Incremental,
            "SELECT tm, COUNT(*) AS n FROM items GROUP BY tm",
        ),
        ViewSpec::new(
            "u_by_amount",
            Kind::Incremental,
            "SELECT amount, COUNT(*) AS n, MAX(id) AS last_id FROM items GROUP BY amount",
        ),
        // Single table: the PRIMARY KEY; rows leaving the filter vanish.
        ViewSpec::new(
            "u_items_pos",
            Kind::Incremental,
            "SELECT id, grp, amount, doc, tm FROM items WHERE amount > 0",
        ),
        // Select/join with LEFT JOIN: (item, cat lineage), NULL lineage while unmatched.
        ViewSpec::new(
            "u_items",
            Kind::Incremental,
            "SELECT i.id, i.note, i.amount, i.doc, i.tm, c.code, c.label \
             FROM items i LEFT JOIN cats c ON c.id = i.cat_id",
        ),
        // Several rows per unit (items joined by a non-key, collation-insensitive column):
        // (cat, item lineage); units grow, shrink and exchange rows.
        ViewSpec::new(
            "u_cat_items",
            Kind::Incremental,
            "SELECT c.id AS cat, c.label, i.id AS item, i.amount, i.doc, i.tm \
             FROM cats c JOIN items i ON i.tag = c.code",
        ),
        // Items map to units straight from their images (no lineage): rewritten per unit.
        ViewSpec::new(
            "u_cat_lines",
            Kind::Incremental,
            "SELECT c.id, c.code, i.id AS item_id, i.amount, i.note \
             FROM cats c JOIN items i ON i.cat_id = c.id",
        ),
    ]
}

/// Columns of the index `index` of table `t` (in key order) and whether it is unique; None if
/// the table has no such index.
async fn index_cols(c: &mut Conn, t: &str, index: &str) -> Result<Option<(Vec<String>, bool)>> {
    let r = rows(
        c,
        &format!(
            "SELECT COLUMN_NAME, NON_UNIQUE FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} AND INDEX_NAME = {} ORDER BY SEQ_IN_INDEX",
            q(t),
            q(index)
        ),
    )
    .await?;
    if r.is_empty() {
        return Ok(None);
    }
    let unique = r.iter().all(|x| x[1].as_deref() == Some("0"));
    Ok(Some((
        r.into_iter()
            .map(|x| x[0].clone().unwrap_or_default())
            .collect(),
        unique,
    )))
}

fn seed() -> Vec<String> {
    let mut v = Vec::new();
    let cats: Vec<String> = (1..=12)
        .map(|c| {
            // Codes equal under the collation but stored with other bytes: tags match both.
            let code = match c {
                3 => "K3".to_string(),
                5 => "k5 ".to_string(),
                _ => format!("k{c}"),
            };
            format!("({c}, {}, 'label {c}')", q(&code))
        })
        .collect();
    v.push(format!("INSERT INTO cats VALUES {}", cats.join(", ")));
    let items: Vec<String> = (1..=240)
        .map(|i| {
            let cat = if i % 10 == 0 {
                "NULL".to_string()
            } else {
                (i % 12 + 1).to_string()
            };
            let grp = if i % 9 == 0 {
                "NULL".to_string()
            } else {
                q(&format!("k{}", i % 8 + 1))
            };
            let tag = match i % 7 {
                0 => "'k3'".to_string(),
                1 => "'K5'".to_string(),
                2 => "'k5'".to_string(),
                3 => "NULL".to_string(),
                k => q(&format!("k{}", k + 4)),
            };
            let amount = format!("{}.{:04}", (i * 37) % 500 - 100, (i * 7919) % 10_000);
            let doc = match i % 4 {
                0 => format!(r#"'{{"a": {i}, "b": [1, 2.50]}}'"#),
                1 => "'[10.00, 20.5]'".to_string(),
                2 => "NULL".to_string(),
                _ => r#"'{"z": "x", "a": 1.5}'"#.to_string(),
            };
            let tm = match i % 6 {
                0 => "'-838:59:59.000'".to_string(),
                1 => "'00:00:00.500'".to_string(),
                2 => "NULL".to_string(),
                k => format!(
                    "'{}:{:02}:{:02}.{:03}'",
                    k * 25,
                    i % 60,
                    (i * 7) % 60,
                    i % 1000
                ),
            };
            format!("({i}, {cat}, {grp}, {tag}, {amount}, {doc}, {tm}, 'note {i}')")
        })
        .collect();
    for ch in items.chunks(80) {
        v.push(format!(
            "INSERT INTO items (id, cat_id, grp, tag, amount, doc, tm, note) VALUES {}",
            ch.join(", ")
        ));
    }
    v
}

fn steps() -> Vec<(&'static str, Vec<String>)> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    vec![
        (
            "one value of a multi-row unit",
            s(&["UPDATE items SET amount = amount + 1 WHERE id = 7"]),
        ),
        (
            "keys equal under the collation, other bytes",
            s(&[
                "UPDATE cats SET code = UPPER(code) WHERE id IN (2, 4)",
                "UPDATE cats SET code = 'k3' WHERE id = 3",
                "UPDATE cats SET code = 'k5' WHERE id = 5",
                "UPDATE items SET grp = UPPER(grp) WHERE grp = 'k2' COLLATE utf8mb4_bin",
                "UPDATE items SET grp = CONCAT(grp, ' ') WHERE grp = 'k4' COLLATE utf8mb4_bin",
                "UPDATE items SET tag = UPPER(tag) WHERE id % 5 = 0",
            ]),
        ),
        (
            "rows move between units",
            s(&[
                "UPDATE items SET grp = 'k6' WHERE id % 7 = 0",
                "UPDATE items SET cat_id = (cat_id % 5) + 1 WHERE id % 3 = 0",
                "UPDATE items SET cat_id = NULL WHERE id % 11 = 0",
                "UPDATE items SET tag = 'k1' WHERE id % 13 = 0",
                "UPDATE items SET amount = -amount WHERE id % 19 = 0",
            ]),
        ),
        (
            "units grow and shrink",
            vec![
                format!(
                    "INSERT INTO items (id, cat_id, grp, tag, amount, note) VALUES {}",
                    (1000..1040)
                        .map(|i| format!("({i}, 2, 'k2', 'k2', {i}.5, 'new {i}')"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                "DELETE FROM items WHERE tag = 'k7'".into(),
                "DELETE FROM items WHERE id % 17 = 0".into(),
            ],
        ),
        (
            "NULLs in keys",
            s(&[
                "UPDATE items SET grp = NULL WHERE id % 5 = 0",
                "UPDATE items SET note = CONCAT(note, '!') WHERE grp IS NULL",
                "UPDATE items SET cat_id = 1 WHERE cat_id IS NULL AND id % 2 = 0",
                "DELETE FROM cats WHERE id = 4",
                "UPDATE items SET tm = NULL, amount = NULL WHERE id % 23 = 0",
            ]),
        ),
        (
            "DECIMAL, JSON and TIME values",
            s(&[
                r#"UPDATE items SET doc = '{"b": [1, 2.50], "a": 1.50}', amount = -0.0001, tm = '-838:59:59' WHERE id = 10"#,
                "UPDATE items SET tm = '00:00:00.5', amount = 0.0000 WHERE id = 11",
                r#"UPDATE items SET doc = '{"z":"x","a":1.50}' WHERE id % 4 = 3"#,
                // (A DECIMAL inside a stored JSON value cannot be written back by any client:
                // docs/views.md. A DOUBLE can.)
                "UPDATE items SET doc = JSON_OBJECT('x', CAST(amount AS DOUBLE)) WHERE id % 9 = 1",
                "UPDATE items SET tm = '838:59:58.999', amount = 99999999.9999 WHERE id = 12",
            ]),
        ),
        (
            "one large transaction",
            vec![
                "START TRANSACTION".to_string(),
                format!(
                    "INSERT INTO items (id, cat_id, grp, tag, amount, doc, note) VALUES {}",
                    (2000..3500)
                        .map(|i| format!(
                            "({i}, {}, 'k{}', 'k{}', {}.25, '[{i}]', REPEAT('x', {}))",
                            i % 12 + 1,
                            i % 8 + 1,
                            i % 12 + 1,
                            i % 300,
                            i % 90
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                "UPDATE items SET amount = amount + 1 WHERE id >= 2000 AND id % 2 = 0".into(),
                "DELETE FROM items WHERE id BETWEEN 2100 AND 2200".into(),
                "INSERT INTO cats VALUES (13, 'k13', 'label 13'), (14, 'K1', 'label 14')".into(),
                "COMMIT".into(),
            ],
        ),
        (
            "back to the start",
            s(&[
                "DELETE FROM items WHERE id >= 1000",
                "UPDATE cats SET label = CONCAT(label, ' v2')",
            ]),
        ),
    ]
}

/// Every edge case, with statements split by rows and bytes (7 rows, 2 KiB) and small unit
/// chunks; then the same again after a restart that resumes the tables.
#[test]
fn upsert_edge_cases() {
    scenario("upsert_edge", |ctx| async move {
        let db = ctx.new_db("upsert_edge").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &DDL.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await?;
        exec_all(&mut c, &seed()).await?;
        let specs = specs();
        let ov = [
            ("LAZYVIEW_QUERY_CHUNK", "13"),
            ("LAZYVIEW_WRITE_BATCH_ROWS", "7"),
            ("LAZYVIEW_WRITE_BATCH_BYTES", "2048"),
        ];
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &ov).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        for st in view_states(&mut c).await? {
            ensure!(
                st.mode.as_deref() == Some("incremental"),
                "view {} is maintained as {:?}, expected incremental",
                st.name,
                st.mode
            );
        }
        // The unique indexes rows are upserted by.
        let unique = |cols: &[&str]| Some((cols.iter().map(|s| s.to_string()).collect(), true));
        for (t, index, expected) in [
            ("u_by_grp", "_lv_row", unique(&["grp"])),
            ("u_by_cat", "PRIMARY", unique(&["id"])),
            ("u_by_code", "PRIMARY", unique(&["code"])),
            ("u_hidden_unit", "_lv_row", unique(&["_lv_u0"])),
            ("u_by_tm", "_lv_row", unique(&["tm"])),
            ("u_by_amount", "_lv_row", unique(&["amount"])),
            ("u_items_pos", "PRIMARY", unique(&["id"])),
            ("u_items", "_lv_row", unique(&["id", "_lv_a1_pk0"])),
            ("u_cat_items", "_lv_row", unique(&["cat", "_lv_a1_pk0"])),
            ("u_cat_lines", "_lv_row", None),
        ] {
            let got = index_cols(&mut c, t, index).await?;
            ensure!(
                got == expected,
                "{t}.{index}: {got:?}, expected {expected:?}"
            );
        }
        ensure!(
            index_cols(&mut c, "u_cat_lines", "_lv_unit")
                .await?
                .is_some(),
            "u_cat_lines keeps its unit index"
        );
        let mut upserted = 0;
        for (what, stmts) in steps() {
            let before = eng.snapshot();
            exec_all(&mut c, &stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            let d = eng.snapshot().since(&before);
            ctx.log(format!("{what}: {}", fmt_snapshot(&d)));
            upserted += d.mv_rows_upserted;
        }
        ensure!(upserted > 0, "no row was upserted");
        // Restart: the tables are resumed (their indexes match) and maintained as before.
        eng.stop().await?;
        let mut eng = ctx.start_engine(&db, &ov)?;
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        wait_ready(&mut c, &mut eng, &names, t_ready()).await?;
        for (what, stmts) in steps().into_iter().take(3) {
            exec_all(&mut c, &stmts).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after restart: {what}"),
            )
            .await?;
        }
        eng.stop().await?;
        c.disconnect().await.ok();
        Ok(())
    });
}

/// Only rows that differ are written: one changed row of a multi-row unit is one upserted row
/// (no DELETE), a vanished row one deleted row, and an unchanged unit nothing.
#[test]
fn upsert_writes_only_changed_rows() {
    scenario("upsert_rows", |ctx| async move {
        let db = ctx.new_db("upsert_rows").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &DDL.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await?;
        exec_all(&mut c, &seed()).await?;
        let specs: Vec<ViewSpec> = specs()
            .into_iter()
            .filter(|s| s.name == "u_cat_items" || s.name == "u_by_grp")
            .collect();
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &[]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        // Item 11: tag 'k8' (a unit of cat 8 with several rows), grp 'k4' (one group row).
        let k8 = scalar_i64(&mut c, "SELECT COUNT(*) FROM items WHERE tag = 'k8'").await?;
        ensure!(k8 > 3, "cat k8 has {k8} items");
        for (what, sql, upserts, deletes) in [
            (
                "one row of a multi-row unit changes",
                "UPDATE items SET amount = amount + 1 WHERE id = 11",
                2,
                0,
            ),
            (
                "a value no view reads",
                "UPDATE items SET cat_id = 3 WHERE id = 11",
                0,
                0,
            ),
            (
                "a row leaves its unit",
                "UPDATE items SET tag = 'k99' WHERE id = 11",
                0,
                1,
            ),
            (
                "it comes back",
                "UPDATE items SET tag = 'k8' WHERE id = 11",
                1,
                0,
            ),
            (
                "a group row vanishes",
                "UPDATE items SET grp = 'k9' WHERE grp = 'k2' COLLATE utf8mb4_bin",
                1,
                1,
            ),
        ] {
            let before = eng.snapshot();
            exec(&mut c, sql).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            let d = eng.snapshot().since(&before);
            ctx.log(format!("{what}: {}", fmt_snapshot(&d)));
            ensure!(
                d.mv_rows_upserted == upserts
                    && d.mv_rows_inserted == upserts
                    && d.mv_rows_deleted == deletes,
                "{what}: upserted {} / inserted {} / deleted {}, expected {upserts} / {upserts} / {deletes}",
                d.mv_rows_upserted,
                d.mv_rows_inserted,
                d.mv_rows_deleted
            );
        }
        eng.stop().await?;
        c.disconnect().await.ok();
        Ok(())
    });
}

/// A table built before upserts (no `_lv_row`) is not resumed: it is rebuilt with the index,
/// and the view stays correct. Tables with the index are resumed.
#[test]
fn upsert_resume_rebuilds_tables_without_row_index() {
    scenario("upsert_resume", |ctx| async move {
        let db = ctx.new_db("upsert_resume").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &DDL.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await?;
        exec_all(&mut c, &seed()).await?;
        let specs: Vec<ViewSpec> = specs()
            .into_iter()
            .filter(|s| s.name == "u_items" || s.name == "u_by_cat")
            .collect();
        let (mut eng, _) = start_with_views(&ctx, &db, &specs, &[]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        eng.stop().await?;
        exec(&mut c, "ALTER TABLE u_items DROP INDEX `_lv_row`").await?;
        exec(
            &mut c,
            "UPDATE items SET amount = amount + 1 WHERE id % 3 = 0",
        )
        .await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &["u_items", "u_by_cat"], t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after restart").await?;
        ensure!(
            index_cols(&mut c, "u_items", "_lv_row").await?.is_some(),
            "u_items was resumed without its unique index"
        );
        exec(&mut c, "UPDATE items SET cat_id = NULL WHERE id % 4 = 0").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the rebuild").await?;
        eng.stop().await?;
        c.disconnect().await.ok();
        Ok(())
    });
}
