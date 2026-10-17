//! Types: every interesting type through filters, group keys and join keys (incl. before-image
//! substitution on deletes / key changes): NULLs, unsigned max, signed min, negative decimals,
//! DATETIME/TIMESTAMP with fractional seconds, JSON (incl. JSON null), ENUM/SET, emoji, binary
//! bytes, case-insensitive collation keys ('abc' vs 'ABC'), huge unsigned / negative PK units.

mod common;

use anyhow::{Context, Result};
use common::typed;
use common::workload::{names, Gen};
use common::*;

async fn setup_typed(
    ctx: &Ctx,
    tag: &str,
    seed_rows: u64,
    specs: &[ViewSpec],
) -> Result<(Db, Gen)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    for s in typed::ddl() {
        exec(&mut c, &s).await?;
    }
    let mut g = Gen::new(ctx.sub_seed(tag));
    if seed_rows > 0 {
        for s in typed::seed(&mut g, seed_rows) {
            exec(&mut c, &s).await?;
        }
    }
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, specs).await?;
    c.disconnect().await.ok();
    Ok((db, g))
}

async fn txn(c: &mut mysql_async::Conn, stmts: &[String], rollback: bool) -> Result<()> {
    exec(c, "START TRANSACTION").await?;
    exec_all(c, stmts).await?;
    exec(c, if rollback { "ROLLBACK" } else { "COMMIT" }).await
}

#[test]
fn types_all_types_roundtrip() {
    scenario("types_all", |ctx| async move {
        let specs = typed::views();
        for (k, (window, chunk)) in [("0", "2"), ("300", "1000")].into_iter().enumerate() {
            let tag = format!("types_all_{k}");
            let (db, mut g) = setup_typed(&ctx, &tag, 60, &specs).await?;
            let mut c = db.conn().await?;
            let mut eng = ctx.start_engine(
                &db,
                &[
                    ("LAZYVIEW_BATCH_WINDOW_MS", window),
                    ("LAZYVIEW_QUERY_CHUNK", chunk),
                ],
            )?;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            verify(&ctx, &mut eng, &db, &specs, &format!("{tag}: initial")).await?;

            // Random changes.
            for phase in 1..=2 {
                for _ in 0..scaled(80) {
                    let stmts = typed::random_change(&mut g);
                    let rb = g.chance(0.08);
                    txn(&mut c, &stmts, rb)
                        .await
                        .with_context(|| format!("{stmts:?}"))?;
                }
                verify(
                    &ctx,
                    &mut eng,
                    &db,
                    &specs,
                    &format!("{tag}: random phase {phase}"),
                )
                .await?;
            }

            // Walk one row through every value of every domain (NULL ↔ value, boundaries).
            let domains: &[(&str, &[&str])] = &[
                ("u64", typed::U64),
                ("i64", typed::I64),
                ("i32", typed::I32),
                ("u8", typed::U8),
                ("i8", typed::I8),
                ("m24", typed::M24),
                ("d", typed::DEC),
                ("dbig", typed::DBIG),
                ("f", typed::DBL),
                ("e", typed::ENUMS),
                ("s", typed::SETS),
                ("dt", typed::DT),
                ("ts", typed::TS),
                ("dte", typed::DATE),
                ("tm", typed::TIME),
                ("yr", typed::YEAR),
                ("j", typed::JSONS),
                ("ci", typed::CI),
                ("cs", typed::CS),
                ("emoji", typed::EMOJI),
                ("ch", typed::CHARS),
                ("bin", typed::BIN),
                ("blb", typed::BLOB),
                ("bt", typed::BITS),
            ];
            let row = typed::types_row(&mut g, 5);
            exec(
                &mut c,
                &format!("REPLACE INTO t_types ({}) VALUES {row}", typed::COLS),
            )
            .await?;
            for (col, dom) in domains {
                for v in dom.iter().chain(std::iter::once(&"NULL")) {
                    exec(
                        &mut c,
                        &format!("UPDATE t_types SET {col} = {v} WHERE id = 5"),
                    )
                    .await?;
                }
            }
            verify(&ctx, &mut eng, &db, &specs, &format!("{tag}: value walk")).await?;

            // Huge unsigned and negative primary keys as units; boundary rows deleted by value.
            let big_ids = [
                "18446744073709551615",
                "18446744073709551614",
                "9223372036854775808",
                "9223372036854775807",
                "0",
            ];
            for id in big_ids {
                let row = typed::types_row(&mut g, 1).replacen("(1,", &format!("({id},"), 1);
                exec(
                    &mut c,
                    &format!("REPLACE INTO t_types ({}) VALUES {row}", typed::COLS),
                )
                .await?;
            }
            exec(&mut c, "UPDATE t_types SET u64 = 18446744073709551615, ci = 'ABC' WHERE id >= 9223372036854775807").await?;
            for id in ["-2147483648", "-1", "2147483647"] {
                exec(&mut c, &format!("REPLACE INTO t_ref (id, u64, d, ci, dt, e, bin, label) VALUES ({id}, 18446744073709551615, -0.000001, 'abc', '2021-06-01 12:00:00.500000', 'ö', X'00', 'neg{id}')")).await?;
            }
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("{tag}: huge / negative keys"),
            )
            .await?;
            txn(
                &mut c,
                &[
                    "UPDATE t_types SET id = 18446744073709551613 WHERE id = 18446744073709551615"
                        .into(),
                    "DELETE FROM t_types WHERE id = 9223372036854775808".into(),
                    "UPDATE t_ref SET id = -2147483647 WHERE id = -2147483648".into(),
                    "DELETE FROM t_types WHERE u64 = 18446744073709551615 AND id < 100".into(),
                    "DELETE FROM t_types WHERE d = -99999999999999.999999".into(),
                    "DELETE FROM t_types WHERE dt = '2021-06-01 12:00:00.500001'".into(),
                    "DELETE FROM t_types WHERE ts = '2021-06-01 12:00:00.499999'".into(),
                    "DELETE FROM t_types WHERE emoji = '👍🏽'".into(),
                    "DELETE FROM t_types WHERE bin = X'00FF'".into(),
                    "DELETE FROM t_ref WHERE ci = 'ABC'".into(),
                ],
                false,
            )
            .await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("{tag}: deletes by typed values"),
            )
            .await?;
            assert_ready(&mut c, &names(&specs)).await?;
            eng.stop().await?;
        }
        Ok(())
    });
}

#[test]
fn types_case_insensitive_keys_step_by_step() {
    scenario("types_ci", |ctx| async move {
        let keep = [
            "vt_all",
            "vt_by_ci",
            "vt_by_lower",
            "vt_join_ci",
            "vt_ref_groups",
            "vt_ckey",
            "vt_ckey_group",
        ];
        let specs: Vec<ViewSpec> = typed::views()
            .into_iter()
            .filter(|v| keep.contains(&v.name.as_str()))
            .collect();
        let (db, _g) = setup_typed(&ctx, "types_ci", 0, &specs).await?;
        let mut c = db.conn().await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_BATCH_WINDOW_MS", "0")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "insert case variants",
                &[
                    "INSERT INTO t_types (id, ci, i32) VALUES (1, 'abc', 1), (2, 'ABC', 2), (3, 'Abc', 3), (4, 'xyz', 4)",
                    "INSERT INTO t_ref (id, ci, label) VALUES (1, 'ABC', 'r1'), (2, 'xyz', 'r2')",
                ],
            ),
            ("case-only change of a group / join key", &["UPDATE t_types SET ci = 'ABC' WHERE id = 1"]),
            ("delete one member of a case-insensitive group", &["DELETE FROM t_types WHERE id = 2"]),
            ("all to lower case", &["UPDATE t_types SET ci = LOWER(ci)"]),
            ("join partner changes case only", &["UPDATE t_ref SET ci = 'abc' WHERE id = 1"]),
            ("join partner moves to another key", &["UPDATE t_ref SET ci = 'XYZ' WHERE id = 1"]),
            ("group emptied", &["DELETE FROM t_types WHERE ci = 'ABC'"]),
            (
                "composite PK with CI key",
                &[
                    "INSERT INTO t_ckey (k1, k2, k3, v) VALUES ('abc', '2021-06-01 12:00:00.500000', 18446744073709551615, 1), ('def', '2021-06-01 12:00:00.500001', 0, 2)",
                ],
            ),
            ("PK changes case only", &["UPDATE t_ckey SET k1 = UPPER(k1) WHERE k1 = 'abc'"]),
            ("PK fractional second changes", &["UPDATE t_ckey SET k2 = '2021-06-01 12:00:00.500002' WHERE k1 = 'ABC'"]),
            ("ODKU on case-variant PK", &["INSERT INTO t_ckey (k1, k2, k3, v) VALUES ('abc', '2021-06-01 12:00:00.500002', 18446744073709551615, 9) ON DUPLICATE KEY UPDATE v = VALUES(v)"]),
            ("delete by case-variant PK", &["DELETE FROM t_ckey WHERE k1 = 'Abc'"]),
        ];
        for (what, stmts) in steps {
            let owned: Vec<String> = stmts.iter().map(|s| s.to_string()).collect();
            txn(&mut c, &owned, false).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await?;
        Ok(())
    });
}
