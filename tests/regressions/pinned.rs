use super::*;

/// Key / value sources: one value table joined once per (key, scope), each alias pinned by
/// constant equalities. Columns of one table use different integer types. Seeded so that every
/// value row an alias reads exists where needed: (11, 3), (11, 5), (12, 3), (15, 3), (40, 3) for
/// every object, the others for some.
const PIN_DDL: &[&str] = &[
    "CREATE TABLE obj (oid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)",
    "CREATE TABLE obj_link (gid MEDIUMINT UNSIGNED NOT NULL, oid INT UNSIGNED NOT NULL, \
     pos INT NOT NULL, PRIMARY KEY (gid, oid))",
    "CREATE TABLE obj_attr (vid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
     oid INT UNSIGNED NOT NULL, aid MEDIUMINT UNSIGNED NOT NULL, scope TINYINT NOT NULL, \
     val INT NULL, UNIQUE KEY obj_aid_scope (oid, aid, scope))",
];

fn pin_seed() -> Vec<String> {
    let mut obj = Vec::new();
    let mut link = Vec::new();
    let mut attr = Vec::new();
    for o in 1..=24u32 {
        obj.push(format!("({o}, 'o{o}')"));
        link.push(format!("({}, {o}, {})", 1 + o % 3, o * 10));
        if o % 2 == 0 {
            link.push(format!("(4, {o}, {o})"));
        }
        let mut val = |a: u32, s: u32, v: &str| attr.push(format!("({o}, {a}, {s}, {v})"));
        val(11, 3, &(1 + o % 2).to_string());
        val(11, 5, if o % 3 == 0 { "NULL" } else { "1" });
        val(12, 3, &(1 + o % 4).to_string());
        if o % 4 == 0 {
            val(12, 5, "4");
        }
        if o % 2 == 1 {
            val(13, 3, &(o * 3).to_string());
        }
        if o % 5 == 0 {
            val(14, 3, &(o * 7).to_string());
        }
        val(15, 3, &(o % 7).to_string());
        if o % 2 == 0 {
            val(16, 3, "1");
        }
        val(40, 3, "0");
    }
    vec![
        format!("INSERT INTO obj VALUES {}", obj.join(", ")),
        format!("INSERT INTO obj_link VALUES {}", link.join(", ")),
        format!(
            "INSERT INTO obj_attr (oid, aid, scope, val) VALUES {}",
            attr.join(", ")
        ),
    ]
}

/// The joins of the pinned views: seven aliases of `obj_attr`, pinned in their own ON (also as
/// `literal = col`, `<=>` and an IN list) or, for `s0`, in the WHERE.
const PIN_JOINS: &str = "JOIN obj_attr s0 ON s0.oid = o.oid \
     LEFT JOIN obj_attr s1 ON s1.oid = o.oid AND s1.aid = 11 AND s1.scope = 5 \
     JOIN obj_attr v0 ON v0.oid = o.oid AND v0.aid = 12 AND v0.scope = 3 \
     LEFT JOIN obj_attr v1 ON v1.oid = o.oid AND 12 = v1.aid AND v1.scope <=> 5 \
     LEFT JOIN obj_attr p0 ON p0.oid = o.oid AND p0.aid IN (13, 14) AND p0.scope = 3 \
     LEFT JOIN obj_attr w0 ON w0.oid = o.oid AND w0.aid = 15 AND w0.scope = 3 \
     LEFT JOIN obj_attr n0 ON n0.oid = o.oid AND n0.aid = 16 AND n0.scope = 3 \
     WHERE s0.aid = 11 AND s0.scope = 3";

/// The grouped select list over `PIN_JOINS` (grouped by link group and object).
const PIN_GROUPED: &str = "SELECT c.gid, o.oid, MAX(c.pos) AS pos, COUNT(*) AS n, \
     COALESCE(MAX(s1.val), MAX(s0.val)) AS a1, COALESCE(MAX(v1.val), MAX(v0.val)) AS a2, \
     MAX(p0.val) AS a3, MAX(w0.val) AS a4, MAX(n0.val) AS a5";

fn pin_views() -> Vec<ViewSpec> {
    vec![
        // Grouped by (link group, object): every alias of `obj_attr` is resolved by lookups.
        ViewSpec::new(
            "r_pin_grouped",
            Kind::Incremental,
            &format!(
                "{PIN_GROUPED} FROM obj_link c JOIN obj o ON o.oid = c.oid {PIN_JOINS} \
                 GROUP BY c.gid, o.oid"
            ),
        ),
        // The same view with the joins permuted and the pinning conditions moved between the
        // ON clauses and the WHERE: the same filters per alias.
        ViewSpec::new(
            "r_pin_grouped_perm",
            Kind::Incremental,
            &format!(
                "{PIN_GROUPED} FROM obj_attr v0 JOIN obj_attr s0 ON s0.oid = v0.oid AND s0.aid IN (11) \
                 JOIN obj o ON o.oid = s0.oid JOIN obj_link c ON c.oid = o.oid \
                 LEFT JOIN obj_attr n0 ON n0.oid = o.oid AND n0.scope = 3 AND n0.aid = 16 \
                 LEFT JOIN obj_attr w0 ON w0.oid = o.oid AND 15 = w0.aid AND w0.scope = 3 \
                 LEFT JOIN obj_attr p0 ON p0.oid = o.oid AND p0.scope = 3 AND p0.aid IN (14, 13) \
                 LEFT JOIN obj_attr v1 ON v1.oid = o.oid AND v1.aid = 12 AND v1.scope = 5 \
                 LEFT JOIN obj_attr s1 ON s1.oid = o.oid AND s1.scope = 5 AND s1.aid = 11 \
                 WHERE 3 = s0.scope AND v0.scope = 3 AND v0.aid = 12 \
                 GROUP BY c.gid, o.oid"
            ),
        ),
        // Select/join over the links: the aliases are traced by lineage.
        ViewSpec::new(
            "r_pin_rows",
            Kind::Incremental,
            &format!(
                "SELECT c.gid, c.oid, c.pos, o.label, s0.val AS a1, s1.val AS b1, v0.val AS a2, \
                 v1.val AS b2, p0.aid AS k3, p0.val AS a3, w0.val AS a4, n0.val AS a5 \
                 FROM obj_link c JOIN obj o ON o.oid = c.oid {PIN_JOINS}"
            ),
        ),
        // The same select/join view wrapped in a CTE, with other output names.
        ViewSpec::new(
            "r_pin_rows_cte",
            Kind::Incremental,
            &format!(
                "WITH r AS (SELECT c.gid, c.oid, c.pos, o.label, s0.val AS a1, v0.val AS a2, \
                 p0.val AS a3, n0.val AS a5 FROM obj_link c JOIN obj o ON o.oid = c.oid {PIN_JOINS}) \
                 SELECT gid AS grp, oid AS obj_id, pos, label, a1 AS x1, a2 AS x2, a3, a5 FROM r"
            ),
        ),
        // Grouped by object: the aliases give units straight from the row images.
        ViewSpec::new(
            "r_pin_obj",
            Kind::Incremental,
            &format!(
                "SELECT o.oid, MAX(s0.val) AS a1, MAX(s1.val) AS b1, MAX(v0.val) AS a2, \
                 MAX(v1.val) AS b2, SUM(p0.val) AS a3, MAX(w0.val) AS a4, MAX(n0.val) AS a5 \
                 FROM obj o {PIN_JOINS} GROUP BY o.oid"
            ),
        ),
    ]
}

/// A transaction that changes several values of one object updates several rows of the same
/// value table, and every row change was recorded for every alias of the table, whatever its
/// key / scope constants. Each such transaction then gave images to all seven lookup aliases,
/// more than the lookups combine (`lookback::MAX_TABLES`), and the grouped views were rebuilt
/// instead of refreshed, over and over under ordinary writes. A row an alias's constant filters
/// reject is never that alias's row, so concurrent single-object transactions stay incremental;
/// rows also move into and out of the filters (key and scope changes).
#[test]
fn reg_pinned_aliases_see_only_their_rows() {
    scenario("reg_pinned", |ctx| async move {
        let specs = pin_views();
        let mut ddl: Vec<String> = PIN_DDL.iter().map(|s| s.to_string()).collect();
        ddl.extend(pin_seed());
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let (db, mut eng) = setup(&ctx, "reg_pinned", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        // Concurrent single-object transactions: keys 11 and 12 (scope 3), 11 (scope 5) and a
        // key no view reads, one object per transaction, each writer its own objects.
        let mut writers = Vec::new();
        for w in 0..4u32 {
            let dsn = db.dsn.clone();
            writers.push(tokio::spawn(async move {
                let mut c = connect(&dsn).await?;
                for i in 0..40u32 {
                    let o = 1 + w + 4 * (i % 6);
                    let set = |a: u32, s: u32, v: u32| {
                        format!(
                            "UPDATE obj_attr SET val = {v} WHERE oid = {o} \
                             AND aid = {a} AND scope = {s}"
                        )
                    };
                    let stmts = [
                        "START TRANSACTION".to_string(),
                        set(11, 3, 1 + (i + w) % 2),
                        set(12, 3, 1 + (i * 3 + w) % 4),
                        set(11, 5, 1 + i % 2),
                        set(40, 3, i),
                        "COMMIT".to_string(),
                    ];
                    for s in &stmts {
                        exec(&mut c, s).await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }));
        }
        for w in writers {
            w.await.context("writer panicked")??;
        }
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "concurrent single-object transactions",
        )
        .await?;
        let txs = eng.snapshot().since(&before);
        ctx.log(format!("after 160 transactions: {}", fmt_snapshot(&txs)));
        // Rows moving into and out of the aliases' filters.
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "key moves from one alias to another (15 -> 16)",
                &["UPDATE obj_attr SET aid = 16 WHERE oid = 1 AND aid = 15 AND scope = 3"],
            ),
            (
                "key moves out of every alias (16 -> 41)",
                &["UPDATE obj_attr SET aid = 41 WHERE oid = 1 AND aid = 16 AND scope = 3"],
            ),
            (
                "key moves back into an alias (41 -> 15), value changed with it",
                &["UPDATE obj_attr SET aid = 15, val = 42 WHERE oid = 1 AND aid = 41"],
            ),
            (
                "scope moves out of an alias (12: 5 -> 6), another row's value changes in it",
                &[
                    "UPDATE obj_attr SET scope = 6 WHERE oid = 4 AND aid = 12 AND scope = 5",
                    "UPDATE obj_attr SET scope = 5, val = 3 WHERE oid = 8 AND aid = 12 AND scope = 5",
                ],
            ),
            (
                "scope moves back in",
                &["UPDATE obj_attr SET scope = 5 WHERE oid = 4 AND aid = 12 AND scope = 6"],
            ),
            (
                "key moves within an IN list (13 -> 14)",
                &["UPDATE obj_attr SET aid = 14 WHERE oid = 3 AND aid = 13"],
            ),
            (
                "the inner-joined alias's row moves out (object leaves) and back",
                &[
                    "START TRANSACTION",
                    "UPDATE obj_attr SET aid = 10 WHERE oid = 6 AND aid = 11 AND scope = 3",
                    "UPDATE obj_attr SET scope = 7 WHERE oid = 7 AND aid = 11 AND scope = 3",
                    "COMMIT",
                ],
            ),
            (
                "they come back in one transaction with other changes",
                &[
                    "START TRANSACTION",
                    "UPDATE obj_attr SET aid = 11 WHERE oid = 6 AND aid = 10",
                    "UPDATE obj_attr SET scope = 3 WHERE oid = 7 AND aid = 11 AND scope = 7",
                    "UPDATE obj_attr SET val = 5 WHERE oid = 7 AND aid = 12 AND scope = 3",
                    "COMMIT",
                ],
            ),
            (
                "unread keys inserted, changed, deleted",
                &[
                    "INSERT INTO obj_attr (oid, aid, scope, val) VALUES (5, 50, 3, 1), (5, 11, 6, 1)",
                    "UPDATE obj_attr SET val = 2 WHERE oid = 5 AND aid IN (50, 40)",
                    "DELETE FROM obj_attr WHERE oid = 5 AND (aid = 50 OR scope = 6)",
                ],
            ),
            (
                "read rows deleted, re-inserted and re-keyed",
                &[
                    "DELETE FROM obj_attr WHERE oid = 9 AND aid IN (11, 15)",
                    "INSERT INTO obj_attr (oid, aid, scope, val) VALUES (9, 11, 3, 2), (9, 16, 3, 7)",
                    "UPDATE obj_attr SET vid = vid + 100000 WHERE oid = 10 AND aid = 12 AND scope = 3",
                ],
            ),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        let d = eng.snapshot().since(&before);
        ctx.log(format!("whole run: {}", fmt_snapshot(&d)));
        ensure_pruned(&d, 10)?;
        ensure!(
            d.full_rebuilds == 0,
            "single-object transactions rebuilt the views ({} rebuilds): {}",
            d.full_rebuilds,
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// Constant filters compare the decoded binlog value with the literal: at the ends of every
/// integer range (signed and unsigned, 8 to 64 bits) a row moving into or out of an alias's
/// filter must be seen by exactly that alias, also through a NULL and a STORED generated column.
#[test]
fn reg_const_filters_at_integer_extremes() {
    scenario("reg_const_int", |ctx| async move {
        let ddl = [
            "CREATE TABLE ks (id INT PRIMARY KEY)",
            "CREATE TABLE kv (id INT PRIMARY KEY, ts TINYINT NULL, tu TINYINT UNSIGNED NULL, \
             ms MEDIUMINT NULL, mu MEDIUMINT UNSIGNED NULL, bs BIGINT NULL, bu BIGINT UNSIGNED NULL, \
             g INT AS (ts + 1) STORED, v INT NOT NULL)",
            "INSERT INTO ks VALUES (1), (2), (3)",
            "INSERT INTO kv (id, ts, tu, ms, mu, bs, bu, v) VALUES (1, 0, 0, 0, 0, 0, 0, 10), \
             (2, -128, 255, -8388608, 16777215, -9223372036854775808, 18446744073709551615, 20), \
             (3, NULL, NULL, NULL, NULL, NULL, NULL, 30)",
        ];
        let specs = vec![ViewSpec::new(
            "r_int_ends",
            Kind::Incremental,
            "SELECT k.id, a.v AS a, b.v AS b, c.v AS c, d.v AS d, f.v AS f, h.v AS h, j.v AS j \
             FROM ks k \
             LEFT JOIN kv a ON a.id = k.id AND a.ts = -128 \
             LEFT JOIN kv b ON b.id = k.id AND b.tu = 255 \
             LEFT JOIN kv c ON c.id = k.id AND c.ms IN (-8388608, 8388607) \
             LEFT JOIN kv d ON d.id = k.id AND d.mu = 16777215 \
             LEFT JOIN kv f ON f.id = k.id AND f.bs <=> -9223372036854775808 \
             LEFT JOIN kv h ON h.id = k.id AND h.bu = 18446744073709551615 \
             LEFT JOIN kv j ON j.id = k.id AND j.g = -127",
        )];
        let (db, mut eng) = setup(&ctx, "reg_const_int", &ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &str)] = &[
            (
                "row 1 moves into every filter",
                "UPDATE kv SET ts = -128, tu = 255, ms = 8388607, mu = 16777215, \
                 bs = -9223372036854775808, bu = 18446744073709551615, v = 11 WHERE id = 1",
            ),
            (
                "row 2 moves out by one",
                "UPDATE kv SET ts = -127, tu = 254, ms = -8388607, mu = 16777214, \
                 bs = -9223372036854775807, bu = 18446744073709551614, v = 21 WHERE id = 2",
            ),
            (
                "row 3 moves in from NULL",
                "UPDATE kv SET ts = -128, tu = 255, ms = -8388608, mu = 16777215, \
                 bs = -9223372036854775808, bu = 18446744073709551615, v = 31 WHERE id = 3",
            ),
            (
                "row 1 moves out to NULL",
                "UPDATE kv SET ts = NULL, tu = NULL, ms = NULL, mu = NULL, bs = NULL, bu = NULL, \
                 v = 12 WHERE id = 1",
            ),
            (
                "row 2 moves to the other end",
                "UPDATE kv SET ts = 127, tu = 0, ms = 8388607, mu = 0, bs = 9223372036854775807, \
                 bu = 0, v = 22 WHERE id = 2",
            ),
        ];
        for (what, stmt) in steps {
            exec_all(&mut c, &[stmt]).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        eng.stop().await
    });
}

/// Partial direct units: in a view grouped by (link group, object), an object value alias
/// gives the object part of the unit and a group value alias the group part, through the join
/// equalities. Their changes refresh every unit with that part, with no lookup query and no part
/// in the lookup combinations: concurrent object and group transactions (which also touch more
/// aliases together than the lookups combine) stay incremental without one lookup. A permuted
/// formulation of the view runs alongside.
#[test]
fn reg_partial_units() {
    scenario("reg_partial", |ctx| async move {
        let mut ddl: Vec<String> = PIN_DDL.iter().map(|s| s.to_string()).collect();
        ddl.extend(pin_seed());
        ddl.push(
            "CREATE TABLE grp (gid SMALLINT UNSIGNED PRIMARY KEY, name VARCHAR(20) NOT NULL)"
                .into(),
        );
        ddl.push(
            "CREATE TABLE grp_attr (vid INT AUTO_INCREMENT PRIMARY KEY, \
             gid BIGINT NOT NULL, aid TINYINT UNSIGNED NOT NULL, \
             scope SMALLINT NOT NULL, val INT NULL, \
             UNIQUE KEY grp_aid_scope (gid, aid, scope))"
                .into(),
        );
        ddl.push("INSERT INTO grp VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')".into());
        let mut gi = Vec::new();
        for g in 1..=4 {
            for (a, s) in [(21, 3), (21, 5), (22, 3), (40, 3)] {
                gi.push(format!("({g}, {a}, {s}, {})", (g + a) % 2));
            }
        }
        ddl.push(format!(
            "INSERT INTO grp_attr (gid, aid, scope, val) VALUES {}",
            gi.join(", ")
        ));
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let select = "SELECT c.gid, o.oid, MAX(c.pos) AS pos, COUNT(*) AS n, \
             COALESCE(MAX(s1.val), MAX(s0.val)) AS a1, MAX(v0.val) AS a2, MAX(w0.val) AS a4, \
             COALESCE(MAX(ka1.val), MAX(ka0.val)) AS g1, MAX(kn0.val) AS g2, MAX(k.name) AS gname";
        let specs = vec![
            ViewSpec::new(
                "r_partial",
                Kind::Incremental,
                &format!(
                    "{select} FROM obj_link c JOIN obj o ON o.oid = c.oid \
                     JOIN grp k ON k.gid = c.gid \
                     JOIN obj_attr s0 ON s0.oid = o.oid AND s0.aid = 11 AND s0.scope = 3 \
                     LEFT JOIN obj_attr s1 ON s1.oid = o.oid AND s1.aid = 11 AND s1.scope = 5 \
                     JOIN obj_attr v0 ON v0.oid = c.oid AND v0.aid = 12 AND v0.scope = 3 \
                     LEFT JOIN obj_attr w0 ON w0.oid = o.oid AND w0.aid = 15 AND w0.scope = 3 \
                     JOIN grp_attr ka0 ON ka0.gid = k.gid AND ka0.aid = 21 AND ka0.scope = 3 \
                     LEFT JOIN grp_attr ka1 ON ka1.gid = k.gid AND ka1.aid = 21 AND ka1.scope = 5 \
                     LEFT JOIN grp_attr kn0 ON kn0.gid = c.gid AND kn0.aid = 22 AND kn0.scope = 3 \
                     GROUP BY c.gid, o.oid"
                ),
            ),
            // Permuted joins, carrying equalities and constants in the WHERE, the group key
            // not selected under its own name.
            ViewSpec::new(
                "r_partial_perm",
                Kind::Incremental,
                "SELECT c.gid AS grp_key, o.oid AS obj_key, MAX(c.pos) AS pos, COUNT(*) AS n, \
                 COALESCE(MAX(s1.val), MAX(s0.val)) AS a1, MAX(v0.val) AS a2, MAX(w0.val) AS a4, \
                 COALESCE(MAX(ka1.val), MAX(ka0.val)) AS g1, MAX(kn0.val) AS g2, MAX(k.name) AS gname \
                 FROM obj_attr v0, grp_attr ka0 JOIN grp k ON k.gid = ka0.gid \
                 JOIN obj_link c ON c.gid = k.gid JOIN obj o ON o.oid = c.oid \
                 JOIN obj_attr s0 ON s0.oid = o.oid \
                 LEFT JOIN grp_attr kn0 ON kn0.gid = c.gid AND kn0.aid = 22 AND kn0.scope = 3 \
                 LEFT JOIN obj_attr w0 ON w0.oid = o.oid AND 15 = w0.aid AND w0.scope = 3 \
                 LEFT JOIN grp_attr ka1 ON ka1.gid = k.gid AND ka1.scope = 5 AND ka1.aid = 21 \
                 LEFT JOIN obj_attr s1 ON s1.oid = o.oid AND s1.aid IN (11) AND s1.scope = 5 \
                 WHERE v0.oid = c.oid AND ka0.aid = 21 AND ka0.scope = 3 \
                 AND s0.aid = 11 AND s0.scope = 3 AND v0.aid = 12 AND v0.scope = 3 \
                 GROUP BY c.gid, o.oid",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_partial", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        for s in &specs {
            let mode: Option<String> = typed_exec_first(
                &mut sc,
                "SELECT `mode` FROM _lazyview_state WHERE name = ?",
                (s.name.as_str(),),
            )
            .await?;
            ensure!(
                mode.as_deref() == Some("incremental"),
                "{} is not maintained incrementally: {mode:?}",
                s.name
            );
        }
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        let mut writers = Vec::new();
        for w in 0..4u32 {
            let dsn = db.dsn.clone();
            writers.push(tokio::spawn(async move {
                let mut c = connect(&dsn).await?;
                for i in 0..40u32 {
                    let (table, key, id, attrs): (&str, &str, u32, &[(u32, u32)]) = if i % 3 == 2 {
                        ("grp_attr", "gid", 1 + (w + i) % 4, &[(21, 3), (22, 3), (21, 5), (40, 3)])
                    } else {
                        ("obj_attr", "oid", 1 + w + 4 * (i % 6), &[(11, 3), (12, 3), (11, 5), (15, 3), (40, 3)])
                    };
                    let mut stmts = vec!["START TRANSACTION".to_string()];
                    for (k, (a, s)) in attrs.iter().enumerate() {
                        stmts.push(format!(
                            "UPDATE {table} SET val = {} WHERE {key} = {id} AND aid = {a} AND scope = {s}",
                            (i + w + k as u32) % 3
                        ));
                    }
                    stmts.push("COMMIT".into());
                    for s in &stmts {
                        exec(&mut c, s).await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }));
        }
        for w in writers {
            w.await.context("writer panicked")??;
        }
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "concurrent object and group transactions",
        )
        .await?;
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "a group value moves between aliases and scopes",
                &[
                    "UPDATE grp_attr SET aid = 22 WHERE gid = 2 AND aid = 21 AND scope = 5",
                    "UPDATE grp_attr SET scope = 5 WHERE gid = 3 AND aid = 22 AND scope = 3",
                ],
            ),
            (
                "an object value moves to another object, a group value to another group",
                &[
                    "UPDATE obj_attr SET oid = 25 WHERE oid = 2 AND aid = 15 AND scope = 3",
                    "UPDATE grp_attr SET gid = 9 WHERE gid = 4 AND aid = 21 AND scope = 3",
                ],
            ),
            (
                "they move back, with a link and a group change",
                &[
                    "START TRANSACTION",
                    "UPDATE obj_attr SET oid = 2 WHERE oid = 25 AND aid = 15",
                    "UPDATE grp_attr SET gid = 4 WHERE gid = 9",
                    "UPDATE obj_link SET gid = 3 WHERE gid = 2 AND oid = 4",
                    "UPDATE grp SET name = 'bb' WHERE gid = 2",
                    "COMMIT",
                ],
            ),
            (
                "rows deleted and inserted",
                &[
                    "DELETE FROM grp_attr WHERE gid = 1 AND aid = 22",
                    "DELETE FROM obj_attr WHERE oid = 5 AND aid = 11 AND scope = 5",
                    "INSERT INTO grp_attr (gid, aid, scope, val) VALUES (1, 22, 3, 7)",
                    "INSERT INTO obj_link VALUES (1, 7, 3)",
                ],
            ),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        let d = eng.snapshot().since(&before);
        ctx.log(format!("whole run: {}", fmt_snapshot(&d)));
        ensure_pruned(&d, 1)?;
        ensure!(
            d.full_rebuilds == 0 && d.lookup_queries == 0,
            "partial units: {} rebuilds, {} lookup queries: {}",
            d.full_rebuilds,
            d.lookup_queries,
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// Cascading-FK parents whose row gives the changed units: a parent read through a partial
/// alias traces its cascades (its own change refreshes every group holding it); a child alias
/// whose partial unit is the FK gets partial keys from the parent's key (ON DELETE CASCADE and
/// SET NULL); a child alias pinned by constant filters on the FK ignores other parent rows, and
/// a pinned parent row finds its units through a presence column of the materialized table; a
/// route the plan cannot derive (a HAVING over a NULL-side pinned alias) still ignores parent
/// keys outside the pins, and follows its pinned key through the cascade map
/// (or the binlog, where the server logs the cascaded rows). Parent deletes and
/// key updates (the children's rows change without binlog events) leave the views exact without
/// a rebuild.
#[test]
fn reg_cascades_derived_from_the_parent() {
    scenario("reg_casc_derived", |ctx| async move {
        let mut ddl: Vec<String> = vec![
            "CREATE TABLE kind (kid SMALLINT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE obj (oid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE obj_link (gid MEDIUMINT UNSIGNED NOT NULL, oid INT UNSIGNED NOT NULL, \
             pos INT NOT NULL, PRIMARY KEY (gid, oid))"
                .into(),
            "CREATE TABLE obj_attr (vid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
             oid INT UNSIGNED NOT NULL, aid SMALLINT UNSIGNED NOT NULL, scope TINYINT NOT NULL, \
             val INT NULL, UNIQUE KEY obj_aid_scope (oid, aid, scope), \
             FOREIGN KEY (oid) REFERENCES obj (oid) ON DELETE CASCADE ON UPDATE CASCADE, \
             FOREIGN KEY (aid) REFERENCES kind (kid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
            "CREATE TABLE obj_tag (tid INT PRIMARY KEY, oid INT UNSIGNED NULL, tag VARCHAR(10) NOT NULL, \
             FOREIGN KEY (oid) REFERENCES obj (oid) ON DELETE SET NULL ON UPDATE CASCADE)"
                .into(),
            "INSERT INTO kind VALUES (11, 'a'), (12, 'b'), (40, 'c'), (50, 'd')".into(),
        ];
        let (mut objs, mut links, mut attrs) = (Vec::new(), Vec::new(), Vec::new());
        for o in 1..=12u32 {
            objs.push(format!("({o}, 'o{o}')"));
            links.push(format!("({}, {o}, {})", 1 + o % 3, o * 10));
            for a in [11, 12, 40, 50] {
                attrs.push(format!("({o}, {a}, 3, {})", o + a));
            }
        }
        ddl.push(format!("INSERT INTO obj VALUES {}", objs.join(", ")));
        ddl.push(format!("INSERT INTO obj_link VALUES {}", links.join(", ")));
        ddl.push(format!(
            "INSERT INTO obj_attr (oid, aid, scope, val) VALUES {}",
            attrs.join(", ")
        ));
        ddl.push(
            "INSERT INTO obj_tag VALUES (1, 3, 'x'), (2, 3, 'y'), (3, 4, 'z'), (4, 5, 'w'), (5, NULL, 'v')"
                .into(),
        );
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let specs = vec![
            // Grouped by (group, object), the object read through a partial alias.
            ViewSpec::new(
                "r_casc_grouped",
                Kind::Incremental,
                "SELECT c.gid, o.oid, MAX(c.pos) AS pos, MAX(s.val) AS a1, MAX(t.val) AS a2 \
                 FROM obj_link c JOIN obj o ON o.oid = c.oid \
                 JOIN obj_attr s ON s.oid = o.oid AND s.aid = 11 AND s.scope = 3 \
                 LEFT JOIN obj_attr t ON t.oid = o.oid AND t.aid = 12 AND t.scope = 3 \
                 GROUP BY c.gid, o.oid",
            ),
            // The object not read: the value alias's partial unit is the FK.
            ViewSpec::new(
                "r_casc_noparent",
                Kind::Incremental,
                "SELECT c.gid, c.oid, MAX(c.pos) AS pos, MAX(s.val) AS a1 FROM obj_link c \
                 LEFT JOIN obj_attr s ON s.oid = c.oid AND s.aid = 11 GROUP BY c.gid, c.oid",
            ),
            // Tags whose object is SET NULL by a delete: partial keys from the parent's key.
            ViewSpec::new(
                "r_casc_tags",
                Kind::Incremental,
                "SELECT c.gid, c.oid, COUNT(g.tid) AS tags FROM obj_link c \
                 LEFT JOIN obj_tag g ON g.oid = c.oid GROUP BY c.gid, c.oid",
            ),
            // Select/join anchored on the value table: the object by lineage, the key table by
            // the anchor's pins.
            ViewSpec::new(
                "r_casc_rows",
                Kind::Incremental,
                "SELECT a.vid, a.val, o.label FROM obj_attr a JOIN obj o ON o.oid = a.oid \
                 WHERE a.aid IN (11, 12)",
            ),
            // A HAVING over the NULL-side pinned alias: no presence column, but only its own
            // key can reach the view.
            ViewSpec::new(
                "r_casc_having",
                Kind::Incremental,
                "SELECT c.gid, c.oid, MAX(t.val) AS a3 FROM obj_link c \
                 LEFT JOIN obj_attr t ON t.oid = c.oid AND t.aid = 50 AND t.scope = 3 \
                 GROUP BY c.gid, c.oid HAVING MAX(t.val) IS NULL OR MAX(t.val) > 55",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_casc_derived", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let before = eng.snapshot();
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "a key row no alias reads changes its key, then is deleted (its values follow, \
                 then cascade away)",
                &[
                    "UPDATE kind SET kid = 41 WHERE kid = 40",
                    "DELETE FROM kind WHERE kid = 41",
                ],
            ),
            (
                "objects deleted (values cascade away, tags lose them)",
                &["DELETE FROM obj WHERE oid IN (5, 9)"],
            ),
            (
                "an object's key changes (its values and tags follow by cascade)",
                &["UPDATE obj SET oid = 103 WHERE oid = 3"],
            ),
            (
                "the object comes back under its old key with new values",
                &[
                    "UPDATE obj SET oid = 3 WHERE oid = 103",
                    "UPDATE obj_attr SET val = 77 WHERE oid = 3 AND aid = 11",
                ],
            ),
            (
                "a key row an alias is pinned to changes its key, then is deleted",
                &[
                    "UPDATE kind SET kid = 13 WHERE kid = 12",
                    "DELETE FROM kind WHERE kid = 13",
                ],
            ),
            (
                "an object deleted and its key re-inserted in one transaction",
                &[
                    "START TRANSACTION",
                    "DELETE FROM obj WHERE oid = 7",
                    "INSERT INTO obj VALUES (7, 'again')",
                    "INSERT INTO obj_attr (oid, aid, scope, val) VALUES (7, 11, 3, 5)",
                    "COMMIT",
                ],
            ),
            (
                "a key row an alias is pinned to is deleted",
                &["DELETE FROM kind WHERE kid = 11"],
            ),
        ];
        for (what, stmts) in steps {
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        let d = eng.snapshot().since(&before);
        ctx.log(format!(
            "parent deletes and key updates: {}",
            fmt_snapshot(&d)
        ));
        ensure!(
            d.full_rebuilds == 0,
            "cascades whose units the parent's row gives rebuilt the views: {}",
            fmt_snapshot(&d)
        );
        // The key the HAVING view pins still reaches it: no derivation there, its rows come
        // back from the cascade map.
        let before = eng.snapshot();
        exec_all(&mut c, &["DELETE FROM kind WHERE kid = 50"]).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "the key row the HAVING view pins is deleted",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        // Where the server logs the cascaded child rows (before the parent's row event), they reach
        // the view through the binlog and leave the cache empty under the parent key: nothing to
        // replay from the map.
        let logged = cascades_logged(&mut c).await?;
        ensure!(
            d.full_rebuilds == 0 && (logged || d.cascade_rows > 0),
            "a pinned key of a route the plan cannot derive was not followed through the cascade map: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// Cascades no derivation follows, through the cascade map: the child rows the storage engine
/// deletes or re-keys without binlog events are replayed from the map through every route of
/// the child table. Views keyed by group read the children through link hops: one hop (values
/// pinned to one key, read through the link), two (boxes), three (box items, a cascade of a
/// cascade), a SET NULL child, a select/join view, and equivalent formulations of the one-hop
/// view. Steps: deletes through 1 / 2 / 3 hops, one transaction deleting values, links and their
/// object, a key update (children follow), a link moving between groups, a delete and re-insert
/// of a key in one transaction, rows inserted after the build then cascaded (in a later and in
/// the same window), values moved out of the pinned key before their object goes, a key moved and
/// the new key deleted in one transaction (children found under the key they moved to), a key
/// deleted, re-inserted with new children and deleted again. Every step against the oracle,
/// without a rebuild, refreshing only the groups it touches.
#[test]
fn reg_cascades_through_the_map() {
    scenario("reg_casc_map", |ctx| async move {
        let mut ddl: Vec<String> = vec![
            "CREATE TABLE grp (gid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE obj (oid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE obj_link (gid INT UNSIGNED NOT NULL, oid INT UNSIGNED NOT NULL, \
             pos INT NOT NULL, PRIMARY KEY (gid, oid), \
             FOREIGN KEY (oid) REFERENCES obj (oid) ON DELETE CASCADE ON UPDATE CASCADE, \
             FOREIGN KEY (gid) REFERENCES grp (gid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
            "CREATE TABLE obj_attr (vid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
             oid INT UNSIGNED NOT NULL, aid SMALLINT UNSIGNED NOT NULL, val INT NULL, \
             UNIQUE KEY obj_aid (oid, aid), \
             FOREIGN KEY (oid) REFERENCES obj (oid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
            "CREATE TABLE obj_part (pid INT PRIMARY KEY, oid INT UNSIGNED NULL, qty INT NOT NULL, \
             FOREIGN KEY (oid) REFERENCES obj (oid) ON DELETE SET NULL ON UPDATE CASCADE)"
                .into(),
            "CREATE TABLE obj_box (bid INT PRIMARY KEY, oid INT UNSIGNED NOT NULL, \
             FOREIGN KEY (oid) REFERENCES obj (oid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
            "CREATE TABLE box_item (iid INT PRIMARY KEY, bid INT NOT NULL, w INT NOT NULL, \
             FOREIGN KEY (bid) REFERENCES obj_box (bid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
        ];
        let groups: Vec<String> = (1..=8u32).map(|g| format!("({g}, 'g{g}')")).collect();
        ddl.push(format!("INSERT INTO grp VALUES {}", groups.join(", ")));
        let (mut objs, mut links, mut attrs, mut parts, mut boxes, mut items) = (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        for o in 1..=60u32 {
            objs.push(format!("({o}, 'o{o}')"));
            links.push(format!("({}, {o}, {})", 1 + o % 8, o * 10));
            if o % 3 == 0 {
                links.push(format!("({}, {o}, {})", 1 + (o + 3) % 8, o * 10 + 1));
            }
            for a in [11, 12] {
                attrs.push(format!("({o}, {a}, {})", (o * a) % 17));
            }
            parts.push(format!("({o}, {o}, {})", o % 5 + 1));
            boxes.push(format!("({}, {o})", 100 + o));
            for k in 0..2 {
                items.push(format!(
                    "({}, {}, {})",
                    1000 + 2 * o + k,
                    100 + o,
                    o % 7 + k
                ));
            }
        }
        ddl.push(format!("INSERT INTO obj VALUES {}", objs.join(", ")));
        ddl.push(format!("INSERT INTO obj_link VALUES {}", links.join(", ")));
        ddl.push(format!(
            "INSERT INTO obj_attr (oid, aid, val) VALUES {}",
            attrs.join(", ")
        ));
        ddl.push(format!("INSERT INTO obj_part VALUES {}", parts.join(", ")));
        ddl.push(format!("INSERT INTO obj_box VALUES {}", boxes.join(", ")));
        ddl.push(format!("INSERT INTO box_item VALUES {}", items.join(", ")));
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let specs = vec![
            // One hop: the values of one key, through the link.
            ViewSpec::new(
                "r_map_hop1",
                Kind::Incremental,
                "SELECT c.gid, COUNT(*) AS n, SUM(a.val) AS s FROM obj_link c \
                 JOIN obj_attr a ON a.oid = c.oid AND a.aid = 11 GROUP BY c.gid",
            ),
            // ... written another way: permuted, the pin in the WHERE.
            ViewSpec::new(
                "r_map_hop1_permuted",
                Kind::Incremental,
                "SELECT c.gid AS grp_id, SUM(a.val) AS total, COUNT(*) AS cnt FROM obj_attr a \
                 JOIN obj_link c ON c.oid = a.oid WHERE a.aid IN (11) GROUP BY c.gid",
            ),
            // ... comma join, swapped equality.
            ViewSpec::new(
                "r_map_hop1_comma",
                Kind::Incremental,
                "SELECT c.gid, COUNT(*) AS n, SUM(a.val) AS s FROM obj_link c, obj_attr a \
                 WHERE c.oid = a.oid AND 11 = a.aid GROUP BY c.gid",
            ),
            // ... wrapped in a CTE with renamed outputs.
            ViewSpec::new(
                "r_map_hop1_cte",
                Kind::Incremental,
                "WITH w AS (SELECT c.gid AS g, a.val AS v FROM obj_link c \
                 JOIN obj_attr a ON a.oid = c.oid AND a.aid = 11) \
                 SELECT g AS group_key, COUNT(*) AS n, SUM(v) AS s FROM w GROUP BY g",
            ),
            // Two hops (boxes) and three (their items: a cascade of a cascade).
            ViewSpec::new(
                "r_map_hop2",
                Kind::Incremental,
                "SELECT c.gid, COUNT(*) AS n, SUM(b.bid) AS sb FROM obj_link c \
                 JOIN obj_box b ON b.oid = c.oid GROUP BY c.gid",
            ),
            ViewSpec::new(
                "r_map_hop3",
                Kind::Incremental,
                "SELECT c.gid, COUNT(*) AS n, SUM(i.w) AS w FROM obj_link c \
                 JOIN obj_box b ON b.oid = c.oid JOIN box_item i ON i.bid = b.bid GROUP BY c.gid",
            ),
            // A SET NULL child.
            ViewSpec::new(
                "r_map_setnull",
                Kind::Incremental,
                "SELECT c.gid, COUNT(p.pid) AS np, SUM(p.qty) AS q FROM obj_link c \
                 LEFT JOIN obj_part p ON p.oid = c.oid GROUP BY c.gid",
            ),
            // Select/join, keyed by the value row.
            ViewSpec::new(
                "r_map_rows",
                Kind::Incremental,
                "SELECT a.vid, a.val, c.gid, c.pos FROM obj_attr a JOIN obj_link c \
                 ON c.oid = a.oid WHERE a.aid IN (11, 12)",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_casc_map", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let start = eng.snapshot();
        let mut c = db.conn().await?;
        let steps: &[(&str, &[&str])] = &[
            (
                "an object deleted: links, values, boxes and box items cascade away, parts lose it",
                &["DELETE FROM obj WHERE oid = 3"],
            ),
            (
                "one transaction deletes an object's values and links, then the object",
                &[
                    "START TRANSACTION",
                    "DELETE FROM obj_attr WHERE oid = 4",
                    "DELETE FROM obj_link WHERE oid = 4",
                    "DELETE FROM obj WHERE oid = 4",
                    "COMMIT",
                ],
            ),
            (
                "a box deleted: its items cascade away",
                &["DELETE FROM obj_box WHERE oid = 5"],
            ),
            (
                "an object's key changes: its rows follow (parts too)",
                &["UPDATE obj SET oid = 107 WHERE oid = 7"],
            ),
            (
                "a link moves an object to another group",
                &["UPDATE obj_link SET gid = 8 WHERE oid = 8 AND gid = 1"],
            ),
            (
                "an object deleted and its key re-inserted with new rows in one transaction",
                &[
                    "START TRANSACTION",
                    "DELETE FROM obj WHERE oid = 9",
                    "INSERT INTO obj VALUES (9, 'again')",
                    "INSERT INTO obj_link VALUES (1, 9, 90)",
                    "INSERT INTO obj_attr (oid, aid, val) VALUES (9, 11, 5)",
                    "INSERT INTO obj_box VALUES (900, 9)",
                    "INSERT INTO box_item VALUES (9000, 900, 4)",
                    "COMMIT",
                ],
            ),
            (
                "rows inserted after the build",
                &[
                    "INSERT INTO obj VALUES (61, 'new')",
                    "INSERT INTO obj_link VALUES (2, 61, 610)",
                    "INSERT INTO obj_attr (oid, aid, val) VALUES (61, 11, 7)",
                    "INSERT INTO obj_box VALUES (161, 61)",
                    "INSERT INTO box_item VALUES (1161, 161, 3)",
                ],
            ),
            (
                "... their object deleted",
                &["DELETE FROM obj WHERE oid = 61"],
            ),
            (
                "rows inserted and their object deleted in the same window",
                &[
                    "INSERT INTO obj VALUES (62, 'brief')",
                    "INSERT INTO obj_link VALUES (3, 62, 620)",
                    "INSERT INTO obj_attr (oid, aid, val) VALUES (62, 11, 9)",
                    "DELETE FROM obj WHERE oid = 62",
                ],
            ),
            (
                "values moved out of the pinned key, then their object deleted",
                &[
                    "UPDATE obj_attr SET aid = 13 WHERE oid = 10 AND aid = 11",
                    "DELETE FROM obj WHERE oid = 10",
                ],
            ),
            (
                "an object's key changes and the new key is deleted, in one transaction",
                &[
                    "START TRANSACTION",
                    "UPDATE obj SET oid = 120 WHERE oid = 12",
                    "DELETE FROM obj WHERE oid = 120",
                    "COMMIT",
                ],
            ),
            (
                "an object deleted, its key re-inserted with new rows and deleted again",
                &[
                    "START TRANSACTION",
                    "DELETE FROM obj WHERE oid = 13",
                    "INSERT INTO obj VALUES (13, 'twice')",
                    "INSERT INTO obj_link VALUES (4, 13, 130)",
                    "INSERT INTO obj_attr (oid, aid, val) VALUES (13, 11, 3)",
                    "COMMIT",
                    "DELETE FROM obj WHERE oid = 13",
                ],
            ),
            (
                "a box's key changes and the new key is deleted: its items follow, then go",
                &[
                    "START TRANSACTION",
                    "UPDATE obj_box SET bid = 2014 WHERE oid = 14",
                    "DELETE FROM obj_box WHERE bid = 2014",
                    "COMMIT",
                ],
            ),
            (
                "a group deleted: its links cascade away (the unit's own key)",
                &["DELETE FROM grp WHERE gid = 3"],
            ),
        ];
        let mut cascade_replayed = 0u64;
        for (what, stmts) in steps {
            let before = eng.snapshot();
            let vbefore: Vec<ViewStats> = specs
                .iter()
                .map(|sp| eng.stats_of(&db, &sp.name))
                .collect::<Result<_>>()?;
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            for (sp, s0) in specs.iter().zip(&vbefore) {
                let s = eng.stats_of(&db, &sp.name)?;
                cascade_replayed += s.cascade_rows - s0.cascade_rows;
                ensure!(
                    s.balanced() && s.whole_view_refreshes == s0.whole_view_refreshes,
                    "{what}: view {} not balanced or refreshed whole: {s:?}",
                    sp.name
                );
            }
            let d = eng.snapshot().since(&before);
            // A step touches at most three groups and a few value rows per view.
            ensure!(
                d.full_rebuilds == 0 && d.units_refreshed <= 8 * specs.len() as u64,
                "{what}: more than the touched groups refreshed: {}",
                fmt_snapshot(&d)
            );
        }
        ensure!(
            cascade_replayed > 0,
            "cascades through the map replayed no row in the per-view stats"
        );
        let d = eng.snapshot().since(&start);
        ctx.log(format!("cascades through the map: {}", fmt_snapshot(&d)));
        ensure!(
            d.full_rebuilds == 0 && d.cascade_rows > 0,
            "cascades through the map rebuilt the views, or replayed no row: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// Restart gap: with a cached route, a parent deleted while the engine is stopped is
/// replayed from before the startup seed, and the cache counts it as a gap. The children the
/// delete took with it cannot be named from the cache, the plan or the binlog, so the gap marks
/// the whole view: at most one whole-view refresh per view per window, and no full rebuild.
/// Outside a gap window such a replay never refreshes the whole view.
#[test]
fn reg_cascade_restart_gap() {
    scenario("reg_casc_gap", |ctx| async move {
        let mut ddl: Vec<String> = vec![
            "CREATE TABLE grp (gid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE ent (eid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE ent_link (gid INT UNSIGNED NOT NULL, eid INT UNSIGNED NOT NULL, \
             pos INT NOT NULL, PRIMARY KEY (gid, eid), \
             FOREIGN KEY (eid) REFERENCES ent (eid) ON DELETE CASCADE ON UPDATE CASCADE, \
             FOREIGN KEY (gid) REFERENCES grp (gid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
            "CREATE TABLE ent_val (vid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
             eid INT UNSIGNED NOT NULL, aid SMALLINT UNSIGNED NOT NULL, val INT NULL, \
             UNIQUE KEY ent_aid (eid, aid), \
             FOREIGN KEY (eid) REFERENCES ent (eid) ON DELETE CASCADE ON UPDATE CASCADE)"
                .into(),
        ];
        let groups: Vec<String> = (1..=8u32).map(|g| format!("({g}, 'g{g}')")).collect();
        ddl.push(format!("INSERT INTO grp VALUES {}", groups.join(", ")));
        ddl.push("INSERT INTO ent VALUES (5, 'e5'), (6, 'e6')".into());
        ddl.push("INSERT INTO ent_link VALUES (1, 5, 50), (1, 6, 60), (3, 5, 51)".into());
        ddl.push("INSERT INTO ent_val (eid, aid, val) VALUES (5, 11, 3), (6, 11, 7)".into());
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let specs = vec![ViewSpec::new(
            "r_gap_hop1",
            Kind::Incremental,
            "SELECT l.gid, COUNT(*) AS n, SUM(v.val) AS s FROM ent_link l \
             JOIN ent_val v ON v.eid = l.eid AND v.aid = 11 GROUP BY l.gid",
        )];
        let (db, mut eng) = setup(&ctx, "reg_casc_gap", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        eng.stop().await?;
        // `ent` is the parent of the cached routes (`^ent:map` into ent_link and ent_val); a `grp`
        // delete is a unit route (the view groups by `l.gid`) and never reaches the cache.
        exec_all(&mut sc, &["DELETE FROM ent WHERE eid = 5"]).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut sc, &mut eng, &names(&specs), t_ready()).await?;
        let f0 = eng.snapshot();
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        // /stats is published at the end of a flush; after the resume the only event is the
        // skipped childless delete, so wait for the periodic flush (position save, 5 s).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let s = loop {
            if let Some(s) = eng.view_stats(&db, "r_gap_hop1") {
                if s.cascade_gaps >= 1 {
                    break s;
                }
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "the pre-seed delete was never counted in /stats within 15 s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        };
        let d = eng.snapshot().since(&f0);
        ensure!(
            s.cascade_gaps >= 1 && s.whole_view_refreshes <= 1 && d.full_rebuilds == 0,
            "the pre-seed delete was not counted, or refreshed the whole view more than once / \
             rebuilt: {s:?} {}",
            fmt_snapshot(&d)
        );
        // The window's whole-view refresh is what healed the groups the delete left stale (they
        // cannot be named from the cache): the writes under each of them only show that a later
        // change is still absorbed incrementally, with no whole-view refresh outside the window.
        let in_window = s.whole_view_refreshes;
        exec_all(
            &mut sc,
            &[
                "UPDATE ent_val SET val = 8 WHERE eid = 6 AND aid = 11",
                "INSERT INTO ent_link VALUES (3, 6, 61)",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "the stale groups after a write under each",
        )
        .await?;
        let s = eng.stats_of(&db, "r_gap_hop1")?;
        let d = eng.snapshot().since(&f0);
        ensure!(
            s.cascade_gaps >= 1 && s.whole_view_refreshes == in_window && d.full_rebuilds == 0,
            "the write under the refreshed id was not absorbed incrementally, or cost another \
             whole-view refresh: {s:?} {}",
            fmt_snapshot(&d)
        );
        ctx.log(format!("restart gap: {s:?} {}", fmt_snapshot(&d)));
        eng.stop().await
    });
}

/// Restart gap, a CASCADE key update (issue #167): the parent's key changes while the engine is
/// stopped, so the resume replays it from before the startup seed and the cache counts a gap. The
/// storage engine moved the children to the new key, so the lookup under the old one hits nothing:
/// the units under them are refreshed from the cache's new side instead. The view is oracle-exact
/// at once, with no whole-view refresh and no rebuild.
///
/// A MAP route: `entities` is the cascade's parent and no alias of it (an alias of the parent
/// would trace the change itself, `^entities:traced`, and be immune), and its key gives no unit
/// (the group is `l.gid`, which only a child row of `links` carries). The FK value is a view
/// column, so a key update is visible: the groups of the moved rows change and only the cache
/// knows those rows changed (an engine cascade writes no binlog rows).
#[test]
fn reg_cascade_window_parent_update() {
    scenario("reg_casc_win_upd", |ctx| async move {
        let mut ddl: Vec<String> = vec![
            "CREATE TABLE items (gid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE entities (eid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)"
                .into(),
            "CREATE TABLE links (gid INT UNSIGNED NOT NULL, eid INT UNSIGNED NOT NULL, \
             pos INT NOT NULL, PRIMARY KEY (gid, eid), \
             FOREIGN KEY (eid) REFERENCES entities (eid) ON DELETE CASCADE ON UPDATE CASCADE, \
             FOREIGN KEY (gid) REFERENCES items (gid))"
                .into(),
        ];
        ddl.push("INSERT INTO items VALUES (1, 'i1'), (2, 'i2'), (3, 'i3')".into());
        ddl.push("INSERT INTO entities VALUES (5, 'e5'), (6, 'e6')".into());
        ddl.push("INSERT INTO links VALUES (1, 5, 10), (2, 5, 20), (3, 6, 30)".into());
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let specs = vec![ViewSpec::new(
            "r_win_upd",
            Kind::Incremental,
            "SELECT l.gid, COUNT(*) AS n, SUM(l.pos) AS s, MAX(l.eid) AS k FROM links l \
             GROUP BY l.gid",
        )];
        let (db, mut eng) = setup(&ctx, "reg_casc_win_upd", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        // The groups of the rows that will move are (1, 5) and (2, 5); the third one stays.
        eng.stop().await?;
        // Both the parent's key update and the ON UPDATE CASCADE that moved links 1 and 2 sit
        // inside the replay window: the startup seed read them already under eid 9.
        exec_all(&mut sc, &["UPDATE entities SET eid = 9 WHERE eid = 5"]).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut sc, &mut eng, &names(&specs), t_ready()).await?;
        let f0 = eng.snapshot();
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        // /stats is published at the end of a flush; after the resume nothing else writes, so
        // wait for the periodic flush (position save, 5 s) to publish the gap of the replay.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let _s = loop {
            if let Some(s) = eng.view_stats(&db, "r_win_upd") {
                if s.cascade_gaps >= 1 {
                    break s;
                }
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "the replayed key update was never counted as a gap in /stats within 15 s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        };
        // Nothing was written under the moved groups since the resume: they are exact at once.
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "the groups the replayed key update moved",
        )
        .await?;
        let s = eng.stats_of(&db, "r_win_upd")?;
        let d = eng.snapshot().since(&f0);
        ensure!(
            s.cascade_gaps >= 1 && s.whole_view_refreshes == 0 && d.full_rebuilds == 0,
            "the replayed key update was not counted as a gap, or refreshed the whole view / \
             rebuilt: {s:?} {}",
            fmt_snapshot(&d)
        );
        ctx.log(format!("gap-window key update: {s:?} {}", fmt_snapshot(&d)));
        eng.stop().await
    });
}

/// Restart gap, a CASCADE delete (issue #167): the parent is deleted while the engine is stopped,
/// so the resume replays it from before the startup seed and the cache counts a gap. The storage
/// engine deleted the children with it, so the lookup under the parent's key hits nothing and no
/// other key holds them: their units cannot be named from the cache, the plan or the binlog. The
/// whole view is refreshed once for the window (`mark_all`, a refresh and not a rebuild), so the
/// row of the group the deleted parent was the only member of is gone at once.
///
/// A MAP route, as in [`reg_cascade_window_parent_update`]: `entities` is the cascade's parent, is
/// not an alias of the view (an alias of it would trace the delete itself and be immune), and its
/// key gives no unit (the group is `l.gid`, which only a child row of `links` carries); the FK
/// value is not a view column, so only the cache knows the row is gone.
#[test]
fn reg_cascade_window_parent_delete() {
    scenario("reg_casc_win_del", |ctx| async move {
        let mut ddl: Vec<String> = vec![
            "CREATE TABLE items (gid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)".into(),
            "CREATE TABLE entities (eid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)"
                .into(),
            "CREATE TABLE links (gid INT UNSIGNED NOT NULL, eid INT UNSIGNED NOT NULL, \
             pos INT NOT NULL, PRIMARY KEY (gid, eid), \
             FOREIGN KEY (eid) REFERENCES entities (eid) ON DELETE CASCADE ON UPDATE CASCADE, \
             FOREIGN KEY (gid) REFERENCES items (gid))"
                .into(),
        ];
        ddl.push("INSERT INTO items VALUES (1, 'i1'), (2, 'i2'), (3, 'i3')".into());
        ddl.push("INSERT INTO entities VALUES (5, 'e5'), (6, 'e6')".into());
        ddl.push("INSERT INTO links VALUES (1, 5, 10), (2, 5, 20), (3, 6, 30)".into());
        let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
        let specs = vec![ViewSpec::new(
            "r_win_del",
            Kind::Incremental,
            "SELECT l.gid, COUNT(*) AS n, SUM(l.pos) AS s FROM links l GROUP BY l.gid",
        )];
        let (db, mut eng) = setup(&ctx, "reg_casc_win_del", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        // Group 3 has one row, under entity 6: deleting that parent empties the group.
        eng.stop().await?;
        // Both the parent delete and the ON DELETE CASCADE that removed links (3, 6) sit inside
        // the replay window: the startup seed read the row already gone.
        exec_all(&mut sc, &["DELETE FROM entities WHERE eid = 6"]).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut sc, &mut eng, &names(&specs), t_ready()).await?;
        let f0 = eng.snapshot();
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        // /stats is published at the end of a flush, and the whole-view refresh of the window is
        // applied by that same flush; nothing else writes after the resume, so wait for the
        // periodic flush (position save, 5 s) to publish both the gap and the refresh.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let _s = loop {
            if let Some(s) = eng.view_stats(&db, "r_win_del") {
                if s.cascade_gaps >= 1 {
                    break s;
                }
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "the replayed parent delete was never counted as a gap in /stats within 15 s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        };
        // Nothing was written since the resume: the group the deleted parent was the only member
        // of is gone at once, and the other two are exact.
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "the group the replayed parent delete emptied",
        )
        .await?;
        let s = eng.stats_of(&db, "r_win_del")?;
        let d = eng.snapshot().since(&f0);
        ensure!(
            s.cascade_gaps >= 1 && s.whole_view_refreshes <= 1 && d.full_rebuilds == 0,
            "the replayed parent delete was not counted as a gap, or refreshed the whole view \
             more than once, or rebuilt: {s:?} {}",
            fmt_snapshot(&d)
        );
        ctx.log(format!(
            "gap-window parent delete: {s:?} {}",
            fmt_snapshot(&d)
        ));
        eng.stop().await
    });
}

/// Per-key delete cases (lazywf §12 / §9.4): a delete through 1 / 2 / 3 hops, an item and
/// its links in one transaction, an explicit ON DELETE CASCADE, a SET NULL, a key move, and
/// a delete + re-insert in one window. Each case is oracle-exact, refreshes no whole view,
/// and the per-view `entities_updated` delta equals the case's known affected keys.
#[test]
fn reg_cascade_deletes_per_key() {
    scenario("reg_casc_del_key", |ctx| async move {
        let ddl: Vec<&str> = vec![
            "CREATE TABLE item (iid INT UNSIGNED PRIMARY KEY, label VARCHAR(20) NOT NULL)",
            "CREATE TABLE item_link (child_id INT UNSIGNED NOT NULL, parent_id INT UNSIGNED NOT NULL, \
             PRIMARY KEY (parent_id, child_id), \
             FOREIGN KEY (parent_id) REFERENCES item (iid) ON DELETE CASCADE ON UPDATE CASCADE, \
             FOREIGN KEY (child_id) REFERENCES item (iid) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE item_value (vid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
             item_id INT UNSIGNED NOT NULL, key_id SMALLINT UNSIGNED NOT NULL, scope_id TINYINT NOT NULL, \
             val INT NULL, UNIQUE KEY item_key_scope (item_id, key_id, scope_id), \
             FOREIGN KEY (item_id) REFERENCES item (iid) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE item_child (mid INT PRIMARY KEY, item_id INT UNSIGNED NOT NULL, \
             FOREIGN KEY (item_id) REFERENCES item (iid) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE child_item (cid INT PRIMARY KEY, mid INT NOT NULL, w INT NOT NULL, \
             FOREIGN KEY (mid) REFERENCES item_child (mid) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE item_soft (sid INT PRIMARY KEY, item_id INT UNSIGNED NULL, qty INT NOT NULL, \
             FOREIGN KEY (item_id) REFERENCES item (iid) ON DELETE SET NULL ON UPDATE CASCADE)",
            "INSERT INTO item VALUES (1,'g1'),(5,'g5'),(9,'g9'),(2,'a'),(3,'b'),(4,'c'),(6,'d'),\
             (7,'e'),(8,'f'),(10,'h'),(11,'i'),(12,'j')",
            "INSERT INTO item_link VALUES (2,1),(3,1),(4,1),(6,5),(7,5),(8,5),(10,9),(11,9),(12,9)",
            "INSERT INTO item_value (item_id, key_id, scope_id, val) VALUES \
             (2,99,3,20),(3,99,3,30),(4,99,3,40),(6,99,3,60),(7,99,3,70),(8,99,3,80),\
             (10,99,3,100),(11,99,3,110),(12,99,3,120),(2,50,3,2),(6,50,3,6)",
            "INSERT INTO item_child VALUES \
             (102,2),(103,3),(104,4),(106,6),(107,7),(108,8),(110,10),(111,11),(112,12)",
            "INSERT INTO child_item VALUES \
             (1002,102,2),(1003,103,3),(1004,104,4),(1006,106,6),(1007,107,7),\
             (1008,108,8),(1010,110,10),(1011,111,11),(1012,112,12)",
            "INSERT INTO item_soft VALUES \
             (202,2,21),(203,3,31),(204,4,41),(206,6,61),(207,7,71),(208,8,81),\
             (210,10,101),(211,11,111),(212,12,121)",
        ];
        let specs = vec![
            ViewSpec::new(
                "r_del_hop1",
                Kind::Incremental,
                "SELECT l.parent_id AS g, COUNT(*) AS n, \
             SUM(v.val) AS s FROM item_link l JOIN item_value v ON v.item_id = l.child_id \
             AND v.key_id = 99 AND v.scope_id = 3 GROUP BY l.parent_id",
            ),
            ViewSpec::new(
                "r_del_hop2",
                Kind::Incremental,
                "SELECT l.parent_id AS g, COUNT(*) AS n \
             FROM item_link l JOIN item_child c ON c.item_id = l.child_id GROUP BY l.parent_id",
            ),
            ViewSpec::new(
                "r_del_hop3",
                Kind::Incremental,
                "SELECT l.parent_id AS g, COUNT(*) AS n, \
             SUM(ci.w) AS sw FROM item_link l JOIN item_child c ON c.item_id = l.child_id \
             JOIN child_item ci ON ci.mid = c.mid GROUP BY l.parent_id",
            ),
            ViewSpec::new(
                "r_del_soft",
                Kind::Incremental,
                "SELECT l.parent_id AS g, COUNT(s.sid) AS ns, \
             SUM(s.qty) AS q FROM item_link l LEFT JOIN item_soft s ON s.item_id = l.child_id \
             GROUP BY l.parent_id",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_casc_del_key", &ddl, &specs, &[]).await?;
        let mut sc = db.conn().await?;
        eng.wait_caught_up(&mut sc, t_catchup()).await?;
        let start = eng.snapshot();
        let mut c = db.conn().await?;
        // A deleted item also marks its own id: `item_link.parent_id` cascades from `item` and is
        // the views' unit, so the parent's key is marked from its row (a unit route; no rows
        // under it). Each delete case gives 2: the group plus the deleted item.
        let steps: &[(&str, &[&str], u64)] = &[
            (
                "a delete through 1 / 2 / 3 hops (item 2 under g1)",
                &["DELETE FROM item WHERE iid = 2"],
                2,
            ),
            (
                "an item and its links in one transaction (item 3 under g1)",
                &[
                    "START TRANSACTION",
                    "DELETE FROM item_value WHERE item_id = 3",
                    "DELETE FROM item_child WHERE item_id = 3",
                    "DELETE FROM item WHERE iid = 3",
                    "COMMIT",
                ],
                2,
            ),
            (
                "ON DELETE CASCADE (item 6 under g5)",
                &["DELETE FROM item WHERE iid = 6"],
                2,
            ),
            (
                "SET NULL (item 10 under g9)",
                &["DELETE FROM item WHERE iid = 10"],
                2,
            ),
            (
                "a key move (item 7 from g5 to g9)",
                &["UPDATE item_link SET parent_id = 9 WHERE parent_id = 5 AND child_id = 7"],
                2,
            ),
            (
                "delete + re-insert in one window (item 8 under g5)",
                &[
                    "START TRANSACTION",
                    "DELETE FROM item WHERE iid = 8",
                    "INSERT INTO item VALUES (8, 'again')",
                    "INSERT INTO item_link VALUES (8, 5)",
                    "INSERT INTO item_value (item_id, key_id, scope_id, val) VALUES (8, 99, 3, 88)",
                    "INSERT INTO item_child VALUES (108, 8)",
                    "INSERT INTO child_item VALUES (1008, 108, 9)",
                    "INSERT INTO item_soft VALUES (2080, 8, 88)",
                    "COMMIT",
                ],
                2,
            ),
        ];
        for (what, stmts, want) in steps {
            let vbefore: Vec<ViewStats> = specs
                .iter()
                .map(|sp| eng.stats_of(&db, &sp.name))
                .collect::<Result<_>>()?;
            exec_all(&mut c, stmts).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
            for (sp, s0) in specs.iter().zip(&vbefore) {
                let s = eng.stats_of(&db, &sp.name)?;
                ensure!(
                    s.entities_updated - s0.entities_updated == *want,
                    "{what}: view {} entities_updated delta != {want}: {s:?}",
                    sp.name
                );
                ensure!(
                    s.whole_view_refreshes == 0,
                    "{what}: view {} refreshed whole: {s:?}",
                    sp.name
                );
            }
            let d = eng.snapshot().since(&start);
            ensure!(
                d.full_rebuilds == 0,
                "{what}: a full rebuild: {}",
                fmt_snapshot(&d)
            );
        }
        ctx.log(format!(
            "per-key deletes: {}",
            fmt_snapshot(&eng.snapshot().since(&start))
        ));
        eng.stop().await
    });
}
