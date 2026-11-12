use super::*;

/// JSON unit keys (MySQL/Percona) must be matched as JSON values: the server groups JSON-equal
/// values (`{"a": 1}` with `{"a": 1.0}`) but compares a JSON column with a plain string
/// parameter as text, so the refresh filters (source read, MV read and the DELETE) never found a
/// dirty unit's rows and the MV went stale. Drives `mv::apply` directly with the filters the
/// engine uses: the planner still runs JSON group keys in full mode, so no engine path reaches
/// them yet. MariaDB (JSON is LONGTEXT) runs the plain path.
#[test]
fn reg_json_unit_filters_match_json_equal_groups() {
    scenario("reg_json_units", |ctx| async move {
        use lazyview::accumulate::UnitKind;
        use lazyview::datum::Datum;
        use lazyview::mv;
        use lazyview::Metrics;

        let db = ctx.new_db("reg_json_units").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE jt (id INT PRIMARY KEY, g INT NULL, doc JSON NULL)",
                "CREATE TABLE mv_j (doc JSON NULL, n BIGINT NOT NULL)",
                "CREATE TABLE mv_gj (g INT NULL, doc JSON NULL, n BIGINT NOT NULL)",
                r#"INSERT INTO jt VALUES (1, 1, '{"a": 1}'), (2, 1, '{"a": 1.0}'), (3, 2, '{"b": [1, 2]}'), (4, 2, NULL), (5, NULL, '{"a": 1}')"#,
            ],
        )
        .await?;
        let is_json = |l: &Layout| l.unit_kinds().contains(&UnitKind::Json);
        let l_j = probe(&mut c, &db.name, "mv_j", 1).await?;
        let l_gj = probe(&mut c, &db.name, "mv_gj", 2).await?;
        ensure!(
            is_json(&l_j) == !ctx.servers[0].is_mariadb(),
            "JSON unit kind: {:?} (MariaDB stores JSON as LONGTEXT)",
            l_j.unit_kinds()
        );
        let metrics = Metrics::default();
        let w = mv::write_opts(
            &mut c,
            lazyview::target::Kind::Mysql,
            1000,
            mv::DEFAULT_WRITE_BATCH_BYTES,
        )
        .await?;
        let j = |s: &str| Datum::Bytes(s.as_bytes().to_vec());
        let sel_j = "SELECT doc, COUNT(*) AS n FROM jt WHERE {F} GROUP BY doc";
        let orc_j = "SELECT doc, COUNT(*) AS n FROM jt GROUP BY doc";
        let on_j = "o.doc <=> m.doc";
        let sel_gj = "SELECT g, doc, COUNT(*) AS n FROM jt WHERE {F} GROUP BY g, doc";
        let orc_gj = "SELECT g, doc, COUNT(*) AS n FROM jt GROUP BY g, doc";
        let on_gj = "o.g <=> m.g AND o.doc <=> m.doc";
        // Dirty keys as the engine sees them: on MySQL the binlog decoder renders the binary JSON
        // (serde_json: no spaces, floats with a fraction) and the server compares JSON values; on
        // MariaDB the text is stored as written and compared as text, so every text is its own
        // group and the images are the exact texts.
        let maria = ctx.servers[0].is_mariadb();
        let key = |mysql: &str, mariadb: &str| j(if maria { mariadb } else { mysql });
        // Initial fill of the single-column view (the whole view).
        let r = rows(&mut c, &l_j, sel_j, &["doc"], None).await?;
        mv::apply(
            &mut c,
            &db.name,
            &name_for(&db, "mv_j"),
            &l_j,
            None,
            &r,
            &w,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_j", orc_j, on_j, "initial fill").await?;
        // Row 3 moves to the {"a": 1} group (a new text group on MariaDB).
        exec(&mut c, r#"UPDATE jt SET doc = '{"a": 1.00}' WHERE id = 3"#).await?;
        let chunk = UnitChunk::Keys(vec![
            Box::new([key(r#"{"b":[1,2]}"#, r#"{"b": [1, 2]}"#)]),
            Box::new([key(r#"{"a":1.0}"#, r#"{"a": 1.00}"#)]),
        ]);
        let r = rows(&mut c, &l_j, sel_j, &["doc"], Some(&chunk)).await?;
        ensure!(
            r.len() == 1,
            "source read of the two dirty JSON units found {} groups, expected 1",
            r.len()
        );
        mv::apply(
            &mut c,
            &db.name,
            &name_for(&db, "mv_j"),
            &l_j,
            Some(&chunk),
            &r,
            &w,
            &metrics,
        )
        .await?;
        check(
            &mut c,
            "mv_j",
            orc_j,
            on_j,
            "update into a JSON-equal group",
        )
        .await?;
        // The {"a": 1} group disappears: the DELETE must find the MV row(s).
        exec(&mut c, "DELETE FROM jt WHERE id IN (1, 2, 3, 5)").await?;
        let chunk = UnitChunk::Keys(if maria {
            vec![
                Box::new([j(r#"{"a": 1}"#)]),
                Box::new([j(r#"{"a": 1.0}"#)]),
                Box::new([j(r#"{"a": 1.00}"#)]),
            ]
        } else {
            vec![Box::new([j(r#"{"a":1}"#)])]
        });
        let r = rows(&mut c, &l_j, sel_j, &["doc"], Some(&chunk)).await?;
        ensure!(
            r.is_empty(),
            "deleted JSON unit still has {} source groups",
            r.len()
        );
        mv::apply(
            &mut c,
            &db.name,
            &name_for(&db, "mv_j"),
            &l_j,
            Some(&chunk),
            &r,
            &w,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_j", orc_j, on_j, "delete of a JSON group").await?;
        // Composite units with NULLs: plain tuples and the null-safe branch bind per position.
        let r = rows(&mut c, &l_gj, sel_gj, &["g", "doc"], None).await?;
        mv::apply(
            &mut c,
            &db.name,
            &name_for(&db, "mv_gj"),
            &l_gj,
            None,
            &r,
            &w,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_gj", orc_gj, on_gj, "initial fill (composite)").await?;
        exec_all(
            &mut c,
            &[
                r#"INSERT INTO jt VALUES (6, 1, '{"a": 1.0}'), (7, NULL, '{"a":1}'), (8, 3, NULL)"#,
                "UPDATE jt SET g = 3 WHERE id = 4",
            ],
        )
        .await?;
        let chunk = UnitChunk::Keys(vec![
            Box::new([Datum::Int(1), key(r#"{"a":1.0}"#, r#"{"a": 1.0}"#)]),
            Box::new([Datum::Int(1), key(r#"{"a":1}"#, r#"{"a": 1.0}"#)]),
            Box::new([Datum::Null, key(r#"{"a":1.0}"#, r#"{"a":1}"#)]),
            Box::new([Datum::Int(2), Datum::Null]),
            Box::new([Datum::Int(3), Datum::Null]),
        ]);
        let r = rows(&mut c, &l_gj, sel_gj, &["g", "doc"], Some(&chunk)).await?;
        ensure!(
            r.len() == 3,
            "source read of the composite JSON units found {} groups, expected 3",
            r.len()
        );
        mv::apply(
            &mut c,
            &db.name,
            &name_for(&db, "mv_gj"),
            &l_gj,
            Some(&chunk),
            &r,
            &w,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_gj", orc_gj, on_gj, "composite units with NULLs").await?;
        Ok(())
    });
}

/// A view grouped by a JSON column stays equal to its oracle through inserts, updates and
/// deletes of JSON-equal and NULL values. The planner runs it in full mode today ("JSON group
/// key"); once it is maintained incrementally (typed unit filters, see
/// `reg_json_unit_filters_match_json_equal_groups`), no change may fall back to a rebuild.
#[test]
fn reg_json_group_key_view() {
    scenario("reg_json_view", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_docs",
            Kind::Full,
            "SELECT doc, COUNT(*) AS n FROM jdocs GROUP BY doc",
        )];
        let (db, mut eng) = setup(
            &ctx,
            "reg_json_view",
            &[
                "CREATE TABLE jdocs (id INT PRIMARY KEY, doc JSON NULL)",
                r#"INSERT INTO jdocs VALUES (1, '{"a": 1}'), (2, '{"b": [1, 2]}'), (3, NULL)"#,
            ],
            &specs,
            &[],
        )
        .await?;
        let mut c = db.conn().await?;
        let mode: Option<String> = typed_first(
            &mut c,
            "SELECT `mode` FROM _lazyview_state WHERE name = 'r_docs'",
        )
        .await?;
        let incremental = mode.as_deref() == Some("incremental");
        for (what, stmt) in [
            (
                "JSON-equal inserts",
                r#"INSERT INTO jdocs VALUES (4, '{"a": 1.0}'), (5, '{"a": 1.00}'), (6, '{"b": [1.0, 2]}')"#,
            ),
            (
                "update into a JSON-equal group",
                r#"UPDATE jdocs SET doc = '{"b": [1, 2.0]}' WHERE id = 1"#,
            ),
            ("update to NULL", "UPDATE jdocs SET doc = NULL WHERE id = 4"),
            ("delete", "DELETE FROM jdocs WHERE id IN (2, 5, 6)"),
        ] {
            let before = eng.snapshot();
            exec(&mut c, stmt).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                !incremental || d.full_rebuilds == 0,
                "{what}: an incremental JSON group key view fell back to a rebuild: {}",
                fmt_snapshot(&d)
            );
        }
        eng.stop().await
    });
}

/// A JSON unit key in a layout that also reads TIMESTAMP columns as epochs: the DELETE of a
/// changed unit runs in the `+00:00` session with the keys from the rows and bound JSON keys as
/// plain strings, which a MySQL JSON column never equals, so the unit's old row stayed next to
/// the new one. Drives `mv::apply` directly (the planner runs JSON group keys in full mode).
#[test]
fn reg_json_unit_delete_with_epoch_timestamps() {
    scenario("reg_json_epoch", |ctx| async move {
        use lazyview::datum::Datum;
        use lazyview::mv;
        use lazyview::Metrics;

        let db = ctx.new_db("reg_json_epoch").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE jt (id INT PRIMARY KEY, doc JSON NULL, ts TIMESTAMP(6) NULL)",
                "CREATE TABLE mv_jt (doc JSON NULL, mx TIMESTAMP(6) NULL, n BIGINT NOT NULL)",
                r#"INSERT INTO jt VALUES (1, '{"a": 1}', '2024-01-01 10:00:00.5'), (2, '{"a": 1.0}', '2024-01-02 10:00:00'), (3, '[1, 2]', '2024-01-03 10:00:00'), (4, NULL, NULL)"#,
            ],
        )
        .await?;
        let mut l = probe(&mut c, &db.name, "mv_jt", 1).await?;
        l.epoch = vec![(1, "MAX(ts)".to_string())];
        let sel = "SELECT UNIX_TIMESTAMP(MAX(ts)) AS `_lv_ts0`, doc, MAX(ts) AS mx, COUNT(*) AS n \
                   FROM jt WHERE {F} GROUP BY doc";
        let orc = "SELECT doc, MAX(ts) AS mx, COUNT(*) AS n FROM jt GROUP BY doc";
        let on = "o.doc <=> m.doc AND o.mx <=> m.mx";
        let metrics = Metrics::default();
        let w = mv::write_opts(
            &mut c,
            lazyview::target::Kind::Mysql,
            1000,
            mv::DEFAULT_WRITE_BATCH_BYTES,
        )
        .await?;
        let r = rows(&mut c, &l, sel, &["doc"], None).await?;
        mv::apply(
            &mut c,
            &db.name,
            &name_for(&db, "mv_jt"),
            &l,
            None,
            &r,
            &w,
            &metrics,
        )
        .await?;
        check(&mut c, "mv_jt", orc, on, "initial fill").await?;
        // Dirty keys as the binlog decoder renders them on MySQL; the text as stored on MariaDB.
        let key = |mysql: &str, mariadb: &str| {
            let k = if ctx.servers[0].is_mariadb() {
                mariadb
            } else {
                mysql
            };
            Datum::Bytes(k.as_bytes().to_vec())
        };
        for (what, stmt, keys) in [
            (
                "newer timestamp in a JSON group",
                "UPDATE jt SET ts = '2024-02-01 10:00:00.25' WHERE id = 2",
                vec![key(r#"{"a":1.0}"#, r#"{"a": 1.0}"#)],
            ),
            (
                "row moves between JSON groups",
                r#"UPDATE jt SET doc = '[1, 2.0]' WHERE id = 1"#,
                vec![key(r#"{"a":1}"#, r#"{"a": 1}"#), key("[1,2.0]", "[1, 2.0]")],
            ),
            (
                "NULL group gets a timestamp",
                "UPDATE jt SET ts = '2024-03-01 00:00:00' WHERE id = 4",
                vec![Datum::Null],
            ),
        ] {
            exec(&mut c, stmt).await?;
            let chunk = UnitChunk::Keys(keys.into_iter().map(|k| Box::new([k]) as _).collect());
            let r = rows(&mut c, &l, sel, &["doc"], Some(&chunk)).await?;
            mv::apply(
                &mut c,
                &db.name,
                &name_for(&db, "mv_jt"),
                &l,
                Some(&chunk),
                &r,
                &w,
                &metrics,
            )
            .await?;
            check(&mut c, "mv_jt", orc, on, what).await?;
        }
        Ok(())
    });
}

/// DECIMALs inside JSON values a view computes (`JSON_ARRAYAGG(amount)`, `JSON_OBJECT(..,
/// SUM(amount))`, `CAST(MIN(amount) AS JSON)`): MySQL stores every number of a JSON text that
/// is not an integer as a DOUBLE, so the MV holds `[10.0, 20.5]` where the view returns
/// `[10.00, 20.50]` (no client can write a DECIMAL into JSON; docs/views.md "JSON values"). The
/// MV must hold exactly what the server stores for the view's JSON text, and refreshes that
/// re-read such units must not rewrite them: the diff compared the texts and rewrote every
/// re-read unit on every refresh. MariaDB (JSON is LONGTEXT) keeps the text as computed.
#[test]
fn reg_json_decimal_values_converge() {
    scenario("reg_json_dec", |ctx| async move {
        let maria = ctx.servers[0].is_mariadb();
        let select = format!(
            "SELECT g, JSON_ARRAYAGG(amount) AS amts, \
             JSON_OBJECT('sum', SUM(amount), 'big', MAX(big)) AS tot, {} AS mn, COUNT(*) AS n \
             FROM amounts WHERE flag > 0 GROUP BY g",
            if maria {
                "JSON_ARRAY(MIN(amount))"
            } else {
                "CAST(MIN(amount) AS JSON)"
            }
        );
        let specs = vec![ViewSpec::new("r_json_dec", Kind::Incremental, &select)];
        let db = ctx.new_db("reg_json_dec").await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE amounts (id INT PRIMARY KEY, g INT NOT NULL, amount DECIMAL(10,2) NOT NULL, \
                 big DECIMAL(30,10) NOT NULL, flag INT NOT NULL)",
                "INSERT INTO amounts VALUES (1, 1, 10.00, 12345678901234567890.1234567890, 1), \
                 (2, 1, 20.50, 0.0000000001, 1), (3, 2, 3.25, 1.5, 1), (4, 2, 0.10, -99999999999999999999.9999999999, 1), \
                 (5, 3, 100.00, 0, 1)",
            ],
        )
        .await?;
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &specs).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let mode: Option<String> = typed_first(
            &mut c,
            "SELECT `mode` FROM _lazyview_state WHERE name = 'r_json_dec'",
        )
        .await?;
        ensure!(
            mode.as_deref() == Some("incremental"),
            "r_json_dec is not incremental: {mode:?}"
        );
        // The view's rows with every JSON value as the server stores it (MySQL: re-parsed from
        // its text; MariaDB: the text), compared as strings with the MV's.
        type Rows = Vec<(i64, String, String, String, i64)>;
        let stored = |e: &str| {
            if maria {
                format!("CAST({e} AS CHAR)")
            } else {
                format!("CAST(CAST(CAST({e} AS CHAR) AS JSON) AS CHAR)")
            }
        };
        let expected_sql = format!(
            "SELECT g, {}, {}, {}, n FROM ({select}) o ORDER BY g",
            stored("amts"),
            stored("tot"),
            stored("mn")
        );
        let computed_sql = format!(
            "SELECT g, CAST(amts AS CHAR), CAST(tot AS CHAR), CAST(mn AS CHAR), n FROM ({select}) o ORDER BY g"
        );
        let mv_sql = "SELECT g, CAST(amts AS CHAR), CAST(tot AS CHAR), CAST(mn AS CHAR), n \
                      FROM r_json_dec ORDER BY g";
        let mut sc = db.conn().await?;
        let mut touches = 0;
        for (what, stmt) in [
            ("initial", None),
            (
                "insert",
                Some("INSERT INTO amounts VALUES (6, 1, 7.10, 0.5, 1), (7, 4, 0.05, 1234.5678900000, 1)"),
            ),
            (
                "update",
                Some("UPDATE amounts SET amount = 3.30, big = 2.25 WHERE id = 3"),
            ),
            ("delete", Some("DELETE FROM amounts WHERE id IN (4, 7)")),
        ] {
            if let Some(stmt) = stmt {
                exec(&mut c, stmt).await?;
            }
            eng.wait_caught_up(&mut sc, t_catchup()).await?;
            let expected: Rows = typed_rows(&mut c, expected_sql.as_str()).await?;
            let actual: Rows = typed_rows(&mut c, mv_sql).await?;
            ensure!(
                actual == expected,
                "{what}: r_json_dec\n  {actual:?}\nis not the view's rows as stored\n  {expected:?}"
            );
            let computed: Rows = typed_rows(&mut c, computed_sql.as_str()).await?;
            ensure!(
                maria || computed != expected,
                "{what}: the view's JSON text is stored unchanged ({computed:?}): the server keeps \
                 DECIMALs in JSON text now; docs/views.md \"JSON values\" is out of date"
            );
            // Refreshes that re-read every unit (the WHERE column changes, the rows do not) must
            // leave the MV alone, every time.
            for _ in 0..2 {
                touches += 1;
                let before = eng.snapshot();
                exec(&mut c, "UPDATE amounts SET flag = flag + 1").await?;
                eng.wait_caught_up(&mut sc, t_catchup()).await?;
                let d = eng.snapshot().since(&before);
                ensure!(
                    d.units_refreshed >= expected.len() as u64
                        && d.units_changed == 0
                        && d.mv_rows_deleted == 0
                        && d.mv_rows_inserted == 0,
                    "{what}, refresh {touches} of unchanged units: expected {} units re-read and none \
                     written: {}",
                    expected.len(),
                    fmt_snapshot(&d)
                );
                let actual: Rows = typed_rows(&mut c, mv_sql).await?;
                ensure!(
                    actual == expected,
                    "{what}, refresh {touches}: r_json_dec changed\n  {actual:?}\n  {expected:?}"
                );
            }
        }
        eng.stop().await
    });
}
