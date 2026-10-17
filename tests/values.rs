//! How values travel from the source through unit filters, lookups and MV writes: BIT values,
//! unit expressions that are boolean operators, TIME and TIMESTAMP keys (fractions, negative,
//! over 24 h, DST fall-back hour), long unit keys, zero dates, ENUM/SET in numeric context and
//! negative TIME(1)/TIME(2) binlog values. Each test runs on every flavor in
//! `LAZYVIEW_TEST_DSNS`.

mod common;

use anyhow::{bail, ensure, Context, Result};
use common::*;
use lazyview::accumulate::UnitChunk;
use lazyview::datum::{Datum, Key};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

async fn setup(
    ctx: &Ctx,
    tag: &str,
    ddl: &[&str],
    specs: &[ViewSpec],
    ov: &[(&str, &str)],
) -> Result<(Db, Engine)> {
    let db = ctx.new_db(tag).await?;
    let mut c = db.conn().await?;
    run(&mut c, ddl).await?;
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, specs).await?;
    let mut eng = ctx.start_engine(&db, ov)?;
    wait_ready(&mut c, &mut eng, &names(specs), t_ready()).await?;
    verify(ctx, &mut eng, &db, specs, "initial").await?;
    Ok((db, eng))
}

fn names(specs: &[ViewSpec]) -> Vec<&str> {
    specs.iter().map(|s| s.name.as_str()).collect()
}

async fn run(c: &mut Conn, stmts: &[&str]) -> Result<()> {
    for s in stmts {
        exec(c, s).await?;
    }
    Ok(())
}

/// The views expected to be incremental are maintained per unit (the paths under test), not
/// rebuilt in full.
async fn ensure_incremental(c: &mut Conn, specs: &[ViewSpec]) -> Result<()> {
    for s in specs.iter().filter(|s| s.kind == Kind::Incremental) {
        for st in view_state(c, &s.name).await? {
            ensure!(
                st.mode.as_deref() == Some("incremental"),
                "view {} is maintained as {:?}, expected incremental",
                s.name,
                st.mode
            );
        }
    }
    Ok(())
}

/// BIT values through refreshes: written as unsigned integers. Bound as strings, the server
/// converts them through the connection's character set when it (re)prepares the cached INSERT
/// (after a swap, on a parameter type change): MySQL / Percona stored `b'11111111'` as 0 or
/// failed with ERROR 3988 (reproduced by `reg_bit_column`). Also BIT(64) extremes, BIT group
/// keys (planned Full) and BIT join keys.
#[test]
fn values_bit_columns() {
    scenario("val_bit", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "v_bits",
                Kind::Incremental,
                "SELECT id, g, blb, txt, b8, b1, b64 FROM bits",
            ),
            // (Planned Full: BIT unit key.)
            ViewSpec::new(
                "v_bits_by_b8",
                Kind::Full,
                "SELECT b8, COUNT(*) AS n, SUM(g) AS sg FROM bits GROUP BY b8",
            ),
            ViewSpec::new(
                "v_bits_join",
                Kind::Incremental,
                "SELECT o.id, o.b8 AS ob, k.label FROM bits o JOIN bit_labels k ON k.b = o.b8",
            ),
        ];
        let ddl = [
            "CREATE TABLE bits (id BIGINT UNSIGNED PRIMARY KEY, g INT NULL, blb BLOB NULL, txt TEXT NULL, b8 BIT(8) NULL, b1 BIT(1) NULL, b64 BIT(64) NULL)",
            "CREATE TABLE bit_labels (id INT PRIMARY KEY, b BIT(8) NULL, label VARCHAR(10) NOT NULL)",
            "INSERT INTO bits VALUES (1, 1, X'00', 'x', b'11111111', b'1', 0xFFFFFFFFFFFFFFFF), (2, NULL, NULL, '', b'0', b'0', 0), \
             (3, 3, '', NULL, NULL, NULL, NULL), (4, 4, X'FF', 'multi\nline', b'10000000', b'1', 0x8000000000000000)",
            "INSERT INTO bit_labels VALUES (1, b'11111111', 'all'), (2, b'10000000', 'high'), (3, b'0', 'zero')",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "val_bit",
            &ddl,
            &specs,
            &[
                ("LAZYVIEW_QUERY_CHUNK", "2"),
                ("LAZYVIEW_BATCH_WINDOW_MS", "0"),
            ],
        )
        .await?;
        ensure_incremental(&mut db.conn().await?, &specs).await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            ("other column changes, BIT unchanged", &["UPDATE bits SET g = 7 WHERE id = 1"]),
            ("again, same statement", &["UPDATE bits SET g = 8 WHERE id = 1"]),
            (
                "REPLACE / PK change with BIT values",
                &[
                    "REPLACE INTO bits VALUES (1, 9, NULL, 'r', b'11111111', b'1', 0xFFFFFFFFFFFFFFFF)",
                    "UPDATE bits SET id = 77 WHERE id = 4",
                ],
            ),
            (
                "other columns change, BIT join partner changes",
                &[
                    "UPDATE bits SET g = COALESCE(g, 0) + 1",
                    "UPDATE bit_labels SET b = b'11111111' WHERE id = 3",
                ],
            ),
            (
                "insert / delete",
                &[
                    "INSERT INTO bits VALUES (5, 5, NULL, 't', b'01111111', b'0', 0x7FFFFFFFFFFFFFFF)",
                    "DELETE FROM bits WHERE id = 2",
                    "UPDATE bits SET b8 = b'10000000' WHERE id = 1",
                ],
            ),
        ];
        for (what, stmts) in steps {
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// Unit expressions are operands of the unit filter: unparenthesized, `NOT f IN (0,1)` is
/// `NOT (f IN (0,1))` and `a BETWEEN 2 AND 3 IN (0,1)` is `a BETWEEN 2 AND (3 IN (0,1))`, so
/// refreshes of views grouped by such expressions read the wrong rows.
#[test]
fn values_unit_expressions_are_operands() {
    scenario("val_unit_ops", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "v_not",
                Kind::Incremental,
                "SELECT NOT f AS nf, COUNT(*) AS n, SUM(v) AS sv FROM flags GROUP BY NOT f",
            ),
            ViewSpec::new(
                "v_and",
                Kind::Incremental,
                "SELECT a > 1 AND b < 5 AS k, COUNT(*) AS n, SUM(v) AS sv FROM flags GROUP BY a > 1 AND b < 5",
            ),
            ViewSpec::new(
                "v_or",
                Kind::Incremental,
                "SELECT a = 1 OR b = 2 AS k, COUNT(*) AS n FROM flags GROUP BY a = 1 OR b = 2",
            ),
            ViewSpec::new(
                "v_between",
                Kind::Incremental,
                "SELECT a BETWEEN 2 AND 3 AS k, COUNT(*) AS n, MAX(v) AS mv FROM flags GROUP BY a BETWEEN 2 AND 3",
            ),
        ];
        let ddl = [
            "CREATE TABLE flags (id INT PRIMARY KEY, a INT NULL, b INT NULL, f TINYINT NULL, v INT NULL)",
            "INSERT INTO flags VALUES (1, 1, 1, 0, 10), (2, 2, 6, 1, 20), (3, 3, 4, NULL, 30), (4, NULL, 2, 1, 40), (5, 5, 5, 0, 50)",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "val_unit_ops",
            &ddl,
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        ensure_incremental(&mut c, &specs).await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "rows change groups",
                &[
                    "UPDATE flags SET f = 1 - f WHERE id IN (1, 2)",
                    "UPDATE flags SET a = 4 WHERE id = 3",
                ],
            ),
            (
                "insert, NULL groups",
                &[
                    "UPDATE flags SET b = 1 WHERE id = 5",
                    "INSERT INTO flags VALUES (6, 2, 2, NULL, 60)",
                ],
            ),
            (
                "delete, to NULL",
                &[
                    "DELETE FROM flags WHERE id = 2",
                    "UPDATE flags SET f = NULL, a = NULL WHERE id = 1",
                ],
            ),
        ];
        for (what, stmts) in steps {
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// Rows of `table` matched by the unit filter for `keys` over `exprs` (ids, sorted).
async fn filter_matches(
    c: &mut Conn,
    table: &str,
    exprs: &[String],
    keys: Vec<Key>,
) -> Result<Vec<i64>> {
    let (f, params) = UnitChunk::Keys(keys).filter(exprs);
    let sql = format!("SELECT id FROM {table} WHERE {f} ORDER BY id");
    let ids: Vec<i64> = c
        .exec(sql.as_str(), params)
        .await
        .with_context(|| sql.clone())?;
    Ok(ids)
}

/// TIME keys with a fraction: on MySQL / Percona `t IN (?, ?)` with two or more TIME(fsp > 0)
/// parameters matched no row, so refreshes of views grouped by such a column lost the groups.
#[test]
fn values_time_unit_keys() {
    scenario("val_time_keys", |ctx| async move {
        let db = ctx.new_db("val_time_keys").await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "CREATE TABLE tk (id INT PRIMARY KEY, t3 TIME(3) NULL, t6 TIME(6) NULL, t0 TIME NULL, v INT NULL)",
                "INSERT INTO tk VALUES (1, '12:00:00.500', '12:00:00.500001', '12:00:00', 1), \
                 (2, '-00:00:01.500', '-00:00:01.000001', '-00:00:01', 2), \
                 (3, '100:00:00.250', '838:59:59.000000', '100:00:00', 3), \
                 (4, '-838:59:59.000', '-838:59:59.000000', '-838:59:59', 4), \
                 (5, '00:00:00.001', '00:00:00.000000', '00:00:00', 5), \
                 (6, NULL, NULL, NULL, 6)",
            ],
        )
        .await?;
        // The filter for the keys as read back from the server (binary protocol, as the engine
        // reads them: TIME values) matches exactly their rows.
        for col in ["t3", "t6", "t0"] {
            let rows: Vec<Row> = c
                .exec(format!("SELECT id, {col} FROM tk ORDER BY id"), ())
                .await?;
            let mut all: Vec<(i64, Datum)> = Vec::new();
            for r in rows {
                let mut v = r.unwrap();
                let d = Datum::from_value(v.pop().unwrap());
                let id: i64 = mysql_async::from_value(v.pop().unwrap());
                all.push((id, d));
            }
            let keys: Vec<Key> = all.iter().map(|(_, d)| Key::from([d.clone()])).collect();
            let ids = filter_matches(&mut c, "tk", &[col.to_string()], keys).await?;
            ensure!(
                ids == vec![1, 2, 3, 4, 5, 6],
                "{col}: unit filter for all keys matched rows {ids:?}"
            );
            let some: Vec<Key> = all[1..4]
                .iter()
                .map(|(_, d)| Key::from([d.clone()]))
                .collect();
            let ids = filter_matches(&mut c, "tk", &[col.to_string()], some).await?;
            ensure!(
                ids == vec![2, 3, 4],
                "{col}: keys 2..4 matched rows {ids:?}"
            );
            let pairs: Vec<Key> = all
                .iter()
                .map(|(id, d)| Key::from([Datum::Int(*id), d.clone()]))
                .collect();
            let ids =
                filter_matches(&mut c, "tk", &["id".to_string(), col.to_string()], pairs).await?;
            ensure!(
                ids == vec![1, 2, 3, 4, 5, 6],
                "{col}: (id, {col}) keys matched rows {ids:?}"
            );
        }

        // End to end: several TIME groups in one refresh chunk.
        let specs = vec![
            ViewSpec::new(
                "v_by_t3",
                Kind::Incremental,
                "SELECT t3, COUNT(*) AS n, SUM(v) AS sv FROM tk GROUP BY t3",
            ),
            ViewSpec::new(
                "v_by_t6",
                Kind::Incremental,
                "SELECT t6, MAX(v) AS mv FROM tk GROUP BY t6",
            ),
        ];
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(
            &db,
            &[
                ("LAZYVIEW_BATCH_WINDOW_MS", "300"),
                ("LAZYVIEW_QUERY_CHUNK", "1000"),
            ],
        )?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        ensure_incremental(&mut c, &specs).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "values of several groups change",
                &["UPDATE tk SET v = v + 10"],
            ),
            (
                "rows move between groups",
                &[
                    "UPDATE tk SET t3 = '-00:00:01.500', t6 = '-00:00:01.000001' WHERE id IN (1, 5)",
                    "UPDATE tk SET t3 = '100:00:00.250', t6 = '838:59:59.000000' WHERE id = 6",
                ],
            ),
            (
                "groups emptied and created",
                &[
                    "DELETE FROM tk WHERE id IN (3, 6)",
                    "INSERT INTO tk VALUES (7, '-12:34:56.789', '-12:34:56.789012', '-12:34:56', 7), (8, '25:00:00.001', '25:00:00.000001', '25:00:00', 8)",
                ],
            ),
        ];
        for (what, stmts) in steps {
            exec(&mut c, "START TRANSACTION").await?;
            run(&mut c, stmts).await?;
            exec(&mut c, "COMMIT").await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// Unit keys too long for an InnoDB index (3072 bytes): a PRIMARY KEY on a VARCHAR(1000)
/// utf8mb4 group key, or a KEY with four TEXT prefixes, failed and the view never built.
#[test]
fn values_long_unit_keys() {
    scenario("val_long_keys", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "v_by_title",
                Kind::Incremental,
                "SELECT title, COUNT(*) AS c, SUM(n) AS s FROM docs GROUP BY title",
            ),
            ViewSpec::new(
                "v_by_texts",
                Kind::Incremental,
                "SELECT t1, t2, t3, t4, COUNT(*) AS c FROM docs GROUP BY t1, t2, t3, t4",
            ),
            ViewSpec::new("v_lk", Kind::Incremental, "SELECT k, v FROM lk"),
        ];
        let ddl = [
            "CREATE TABLE docs (id INT PRIMARY KEY, title VARCHAR(1000) CHARACTER SET utf8mb4 NOT NULL, \
             t1 TEXT NULL, t2 TEXT NULL, t3 TEXT NULL, t4 TEXT NULL, n INT NULL)",
            "CREATE TABLE lk (k VARCHAR(768) CHARACTER SET utf8mb4 NOT NULL PRIMARY KEY, v INT NULL)",
            "INSERT INTO docs VALUES (1, REPEAT('é', 1000), REPEAT('a', 300), 'b', 'c', 'd', 1), \
             (2, REPEAT('é', 999), REPEAT('a', 300), 'b', 'c', 'd', 2), (3, 'short', NULL, NULL, NULL, NULL, 3), \
             (4, CONCAT(REPEAT('é', 999), 'x'), REPEAT('a', 299), 'b', 'c', 'e', 4)",
            "INSERT INTO lk VALUES (REPEAT('ü', 768), 1), ('a', 2), (CONCAT(REPEAT('ü', 767), 'x'), 3)",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "val_long_keys",
            &ddl,
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        ensure_incremental(&mut c, &specs).await?;
        let indexed = |t: &str| {
            format!(
                "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{t}'"
            )
        };
        ensure!(
            scalar_i64(
                &mut c,
                &format!("{} AND INDEX_NAME = 'PRIMARY'", indexed("v_lk"))
            )
            .await?
                == 1,
            "v_lk keeps its PRIMARY KEY (3072 bytes fit)"
        );
        for t in ["v_by_title", "v_by_texts"] {
            ensure!(
                scalar_i64(&mut c, &indexed(t)).await? > 0,
                "{t} has a (prefix) unit index"
            );
        }
        let steps: &[(&str, &[&str])] = &[
            (
                "long keys change",
                &[
                    "UPDATE docs SET n = n + 10",
                    "UPDATE docs SET title = REPEAT('é', 1000), t4 = 'd' WHERE id = 4",
                    "UPDATE lk SET v = v + 1",
                ],
            ),
            (
                "insert / delete",
                &[
                    "INSERT INTO docs VALUES (5, REPEAT('é', 998), REPEAT('a', 300), 'b', 'c', 'd', 5)",
                    "DELETE FROM docs WHERE id = 1",
                    "DELETE FROM lk WHERE k = 'a'",
                    "UPDATE lk SET k = REPEAT('ü', 700) WHERE v = 4",
                ],
            ),
        ];
        for (what, stmts) in steps {
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// Zero dates in the source ('0000-00-00', '2020-00-00'): under the default strict sql_mode the
/// MV writes failed (MySQL / Percona), and lookups bound them as NULL.
///
/// lazyview's sessions allow zero dates (no STRICT_*, NO_ZERO_DATE, NO_ZERO_IN_DATE), so its
/// views see dates as stored, like MariaDB in any mode. MySQL in strict mode instead groups
/// '2020-00-00' with '0000-00-00' while still filtering it exactly, which no per-unit refresh
/// can follow: the grouped view is compared with its query in such a session, not the oracle.
#[test]
fn values_zero_dates() {
    scenario("val_zero_dates", |ctx| async move {
        let specs = vec![
            ViewSpec::new("v_zd", Kind::Incremental, "SELECT id, d, dt, g FROM zd"),
            ViewSpec::new(
                "v_zd_by_d",
                Kind::Incremental,
                "SELECT d, COUNT(*) AS n, MAX(dt) AS mdt FROM zd GROUP BY d",
            ),
            ViewSpec::new(
                "v_zd_join",
                Kind::Incremental,
                "SELECT z.id, r.label FROM zd z JOIN zref r ON r.d = z.d",
            ),
        ];
        let db = ctx.new_db("val_zero_dates").await?;
        let mut c = db.conn().await?;
        run(
            &mut c,
            &[
                "SET SESSION sql_mode = ''",
                "CREATE TABLE zd (id INT PRIMARY KEY, d DATE NULL, dt DATETIME NULL, g INT NULL)",
                "CREATE TABLE zref (id INT PRIMARY KEY, d DATE NULL, label VARCHAR(10) NOT NULL)",
                "INSERT INTO zd VALUES (1, '0000-00-00', '0000-00-00 00:00:00', 1), (2, '2020-00-00', '2020-01-00 10:00:00', 1), \
                 (3, '2021-05-06', '2021-05-06 07:08:09', 2), (4, NULL, NULL, 2)",
                "INSERT INTO zref VALUES (1, '0000-00-00', 'zero'), (2, '2021-05-06', 'real'), (3, '2020-00-00', 'part')",
            ],
        )
        .await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_BATCH_WINDOW_MS", "0")])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        ensure_incremental(&mut c, &specs).await?;
        let oracle_checked = [specs[0].clone(), specs[2].clone()];
        // (`c` is in a session without strict / zero-date flags.)
        let grouped = [(
            "SELECT d, n, mdt FROM v_zd_by_d ORDER BY d".to_string(),
            "SELECT d, COUNT(*), MAX(dt) FROM zd GROUP BY d ORDER BY d".to_string(),
        )];
        verify(&ctx, &mut eng, &db, &oracle_checked, "initial").await?;
        ensure_same(&mut c, &grouped, "initial").await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "rows move to zero dates",
                &[
                    "UPDATE zd SET d = '0000-00-00', dt = '2021-00-00 00:00:00' WHERE id = 3",
                    "INSERT INTO zd VALUES (5, '0000-00-00', '2020-00-00 00:00:00', 3)",
                ],
            ),
            (
                "join partners on zero dates change",
                &[
                    "UPDATE zref SET label = 'z2' WHERE id = 1",
                    "UPDATE zref SET d = '0000-00-00' WHERE id = 2",
                    "UPDATE zref SET d = '2021-05-06' WHERE id = 3",
                ],
            ),
            (
                "zero-date rows deleted",
                &[
                    "DELETE FROM zd WHERE id = 1",
                    "UPDATE zd SET d = NULL WHERE id = 2",
                ],
            ),
        ];
        for (what, stmts) in steps {
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &oracle_checked, what).await?;
            ensure_same(&mut c, &grouped, what).await?;
        }
        eng.stop().await
    });
}

/// `(sql, label)` pairs whose results must be equal (TIMESTAMPs compared as epoch seconds).
async fn ensure_same(c: &mut Conn, checks: &[(String, String)], what: &str) -> Result<()> {
    for (actual, expected) in checks {
        let a = rows(c, actual).await?;
        let e = rows(c, expected).await?;
        if a != e {
            bail!("{what}: {actual}\n  MV:     {a:?}\n  source: {e:?} ({expected})");
        }
    }
    Ok(())
}

/// TIMESTAMPs in a DST fall-back hour: read as local datetimes and written back in the same
/// zone, the second 02:30 of 2021-10-31 in Europe/Berlin became the first one (an hour off).
/// Needs time zone tables; runs with the server's global time zone set to Europe/Berlin.
///
/// Columns must keep their exact instants; the oracle checks the group keys, which MySQL
/// compares by local time (both 02:30 in one group) and MariaDB by instant, and the exact
/// checks cover the columns (and MariaDB's group keys). A key in that hour is one local time
/// for two instants: an index lookup finds only the first, a scan both. (No MIN / MAX view:
/// over such instants the servers return local times, differently from one plan to the next.)
#[test]
fn values_timestamp_dst_fall_back() {
    scenario("val_ts_dst", |ctx| async move {
        let db = ctx.new_db("val_ts_dst").await?;
        let mut admin = db.conn().await?;
        if scalar(
            &mut admin,
            "SELECT CONVERT_TZ('2021-10-31 02:30:00', 'Europe/Berlin', '+00:00')",
        )
        .await?
        .is_none()
        {
            ctx.note("SKIP: no time zone tables (load them with mysql_tzinfo_to_sql)");
            return Ok(());
        }
        // SET GLOBAL time_zone affects every connection on the server, so a suite running in
        // parallel against the same servers would see shifted DATE()s. Opt-in: CI sets it (it
        // runs one test binary at a time).
        if std::env::var("LAZYVIEW_TEST_GLOBAL_TZ")
            .map(|v| v != "1")
            .unwrap_or(true)
        {
            ctx.note("SKIP: changes GLOBAL time_zone; set LAZYVIEW_TEST_GLOBAL_TZ=1 to run it");
            return Ok(());
        }
        let old = scalar(&mut admin, "SELECT @@GLOBAL.time_zone")
            .await?
            .unwrap_or_else(|| "SYSTEM".into());
        exec(&mut admin, "SET GLOBAL time_zone = 'Europe/Berlin'").await?;
        let r = dst_scenario(&ctx, &db).await;
        let restored = exec(&mut admin, &format!("SET GLOBAL time_zone = {}", q(&old))).await;
        r.and(restored)
    });
}

async fn dst_scenario(ctx: &Ctx, db: &Db) -> Result<()> {
    let specs = vec![
        ViewSpec::new("v_ev", Kind::Incremental, "SELECT id, g, ts, ts0 FROM ev"),
        ViewSpec::new(
            "v_ev_ts",
            Kind::Incremental,
            "SELECT ts, COUNT(*) AS n, SUM(g) AS sg FROM ev GROUP BY ts",
        ),
    ];
    let mut checks: Vec<(String, String)> = vec![(
        "SELECT id, g, UNIX_TIMESTAMP(ts), UNIX_TIMESTAMP(ts0) FROM v_ev ORDER BY id".into(),
        "SELECT id, g, UNIX_TIMESTAMP(ts), UNIX_TIMESTAMP(ts0) FROM ev ORDER BY id".into(),
    )];
    if ctx.servers[0].is_mariadb() {
        checks.push((
            "SELECT UNIX_TIMESTAMP(ts), n, sg FROM v_ev_ts ORDER BY 1".into(),
            "SELECT UNIX_TIMESTAMP(ts), COUNT(*), SUM(g) FROM ev GROUP BY ts ORDER BY 1".into(),
        ));
    }
    let mut c = db.conn().await?;
    // Instants are written in UTC: 00:30 and 01:30 UTC are both 02:30 in Berlin.
    run(
        &mut c,
        &[
            "SET SESSION time_zone = '+00:00'",
            "CREATE TABLE ev (id INT PRIMARY KEY, g INT NULL, ts TIMESTAMP(6) NULL DEFAULT NULL, ts0 TIMESTAMP NULL DEFAULT NULL)",
            "INSERT INTO ev VALUES (1, 1, '2021-10-31 00:30:00.500000', '2021-10-31 00:30:00'), \
             (2, 1, '2021-10-31 01:30:00.250000', '2021-10-31 01:30:00'), \
             (3, 2, '2021-10-31 02:30:00', '2021-10-31 01:59:59'), (4, 2, '2021-06-01 12:00:00', NULL)",
        ],
    )
    .await?;
    ensure_lazyview_table(&mut c).await?;
    add_views(&mut c, &specs).await?;
    let mut eng = ctx.start_engine(db, &[("LAZYVIEW_BATCH_WINDOW_MS", "0")])?;
    wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
    ensure_incremental(&mut c, &specs).await?;
    verify(ctx, &mut eng, db, &specs, "initial").await?;
    ensure_same(&mut c, &checks, "initial").await?;
    let steps: &[(&str, &[&str])] = &[
        (
            "rows in the repeated hour change group, new one inserted",
            &[
                "UPDATE ev SET g = 3 WHERE id = 2",
                "INSERT INTO ev VALUES (5, 3, '2021-10-31 01:00:00.000001', '2021-10-31 00:59:59')",
            ],
        ),
        (
            "timestamps move inside the repeated hour",
            &[
                "UPDATE ev SET ts = '2021-10-31 01:45:00' WHERE id = 1",
                "UPDATE ev SET ts = '2021-10-31 00:30:00.500000' WHERE id = 3",
                "DELETE FROM ev WHERE id = 4",
            ],
        ),
    ];
    for (what, stmts) in steps {
        run(&mut c, stmts).await?;
        verify(ctx, &mut eng, db, &specs, what).await?;
        ensure_same(&mut c, &checks, what).await?;
    }
    eng.stop().await
}

/// A case-only change of a join partner after earlier case changes of the other side (the
/// `types_case_insensitive_keys_step_by_step` sequence for the join view).
#[test]
fn values_case_variant_join_partner() {
    scenario("val_ci_join", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "v_join_ci",
                Kind::Incremental,
                "SELECT t.id, t.ci, r.id AS rid, r.ci AS rci FROM tci t JOIN rci r ON r.ci = t.ci",
            ),
            ViewSpec::new(
                "v_ref_groups",
                Kind::Incremental,
                "SELECT r.label, COUNT(*) AS n, SUM(t.i) AS si FROM tci t JOIN rci r ON r.ci = t.ci GROUP BY r.label",
            ),
        ];
        let ddl = [
            "CREATE TABLE tci (id BIGINT UNSIGNED PRIMARY KEY, ci VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL, i INT NULL, KEY (ci))",
            "CREATE TABLE rci (id INT PRIMARY KEY, ci VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL, label VARCHAR(40) NOT NULL, KEY (ci))",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "val_ci_join",
            &ddl,
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "insert case variants",
                &[
                    "INSERT INTO tci VALUES (1, 'abc', 1), (2, 'ABC', 2), (3, 'Abc', 3), (4, 'xyz', 4)",
                    "INSERT INTO rci VALUES (1, 'ABC', 'r1'), (2, 'xyz', 'r2')",
                ],
            ),
            ("case-only change", &["UPDATE tci SET ci = 'ABC' WHERE id = 1"]),
            ("delete a case variant", &["DELETE FROM tci WHERE id = 2"]),
            ("all to lower case", &["UPDATE tci SET ci = LOWER(ci)"]),
            (
                "join partner changes case only",
                &["UPDATE rci SET ci = 'abc' WHERE id = 1"],
            ),
            (
                "join partner moves to another key",
                &["UPDATE rci SET ci = 'XYZ' WHERE id = 1"],
            ),
        ];
        for (what, stmts) in steps {
            exec(&mut c, "START TRANSACTION").await?;
            run(&mut c, stmts).await?;
            exec(&mut c, "COMMIT").await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// ENUM / SET columns in numeric context (`e + 0`, `e > 1`, `s & 4`): lookups bind row images
/// as label strings, for which those predicates evaluate differently than for the column.
#[test]
fn values_enum_set_numeric_context() {
    scenario("val_enum_num", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "v_enum_plus",
                Kind::Full,
                "SELECT o.id, p.e FROM o JOIN p ON p.id = o.pid WHERE p.e + 0 >= 2",
            ),
            ViewSpec::new(
                "v_enum_gt",
                Kind::Full,
                "SELECT o.id, p.e FROM o JOIN p ON p.id = o.pid WHERE p.e > 1",
            ),
            ViewSpec::new(
                "v_set_bits",
                Kind::Full,
                "SELECT o.id, p.s FROM o JOIN p ON p.id = o.pid WHERE p.s & 4",
            ),
            ViewSpec::new(
                "v_enum_label",
                Kind::Incremental,
                "SELECT o.id, p.e FROM o JOIN p ON p.id = o.pid WHERE p.e = 'b'",
            ),
        ];
        let ddl = [
            "CREATE TABLE p (id INT PRIMARY KEY, e ENUM('a','b','c') NULL, s SET('x','y','z') NULL)",
            "CREATE TABLE o (id INT PRIMARY KEY, pid INT NULL, KEY (pid))",
            "INSERT INTO p VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x,z')",
            "INSERT INTO o VALUES (10, 1), (11, 2), (12, 3), (13, 1)",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "val_enum_num",
            &ddl,
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "ENUM / SET values change",
                &[
                    "UPDATE p SET e = 'c' WHERE id = 1",
                    "UPDATE p SET s = 'z' WHERE id = 2",
                ],
            ),
            (
                "values change back",
                &[
                    "UPDATE p SET e = 'a', s = 'x' WHERE id = 3",
                    "UPDATE p SET e = 'b' WHERE id = 2",
                ],
            ),
        ];
        for (what, stmts) in steps {
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// Negative TIME(1) / TIME(2) values with a fraction: mysql_common 0.37 decodes them wrongly
/// (an overflow panic in debug builds, garbage in release builds), here as unit keys taken
/// straight from the binlog row images.
#[test]
fn values_negative_short_fraction_time() {
    scenario("val_time_fsp12", |ctx| async move {
        let specs = vec![
            ViewSpec::new("v_tq", Kind::Incremental, "SELECT t1, t2, v FROM tq"),
            ViewSpec::new(
                "v_tq_by_t2",
                Kind::Incremental,
                "SELECT t2, COUNT(*) AS n, SUM(v) AS sv FROM tq GROUP BY t2",
            ),
        ];
        let ddl = [
            "CREATE TABLE tq (t1 TIME(1) NOT NULL PRIMARY KEY, t2 TIME(2) NULL, v INT NULL)",
            "INSERT INTO tq VALUES ('-00:00:01.5', '-00:00:01.25', 1), ('-100:00:00.1', '-838:59:58.99', 2), \
             ('12:00:00.5', '12:00:00.75', 3), ('-838:59:59.0', '-700:00:00.01', 4)",
        ];
        let (db, mut eng) = setup(
            &ctx,
            "val_time_fsp12",
            &ddl,
            &specs,
            &[("LAZYVIEW_BATCH_WINDOW_MS", "0")],
        )
        .await?;
        let mut c = db.conn().await?;
        ensure_incremental(&mut c, &specs).await?;
        let steps: &[(&str, &[&str])] = &[
            ("every row changes", &["UPDATE tq SET v = v + 10"]),
            (
                "group keys change",
                &[
                    "UPDATE tq SET t2 = '-600:00:00.5' WHERE v = 12",
                    "UPDATE tq SET t2 = '-00:00:01.25' WHERE v = 13",
                ],
            ),
            (
                "insert / delete / key change",
                &[
                    "INSERT INTO tq VALUES ('-00:00:00.1', '-00:00:00.01', 5)",
                    "DELETE FROM tq WHERE t1 = '-100:00:00.1'",
                    "UPDATE tq SET t1 = '-800:00:00.9' WHERE t1 = '-838:59:59.0'",
                ],
            ),
        ];
        for (what, stmts) in steps {
            run(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}
