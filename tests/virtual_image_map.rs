//! C-01 (issue #7): a row image that leaves generated columns out must still be read by
//! catalog column. The binlog reader reconciles one image map per (table, image layout) at
//! the TABLE_MAP boundary and `accumulate` reads every value through it, so a table whose
//! images omit a column keeps its incremental plan instead of being read by position.
//!
//! R4 covers the routing decision end to end over the JOIN and cascade shapes (the neutral
//! `entities` / `links` / `items` fixtures) with a VIRTUAL column before a real one: the
//! prefix-reading join stays on the core, the one reading the STORED column past the
//! VIRTUAL one keeps the viewplan executor, and both maintain exactly (the same churn,
//! the same oracle).
//!
//! Measured layouts (PR-1, `tmp/out/lvs-virtual/pr1/facts.md`): MariaDB >= 11.4 logs
//! UPDATE/DELETE images without generated columns (STORED included) and INSERT images as the
//! full catalog; the MySQL family logs the full catalog in every image. `TABLE_MAP` always
//! declares the full catalog width, so the reader tells the layouts apart by the decoded
//! image width.
//!
//! Refusal is unchanged until PR-4 (`source::plan_refusal`): a plan reading a REAL column
//! positioned after a VIRTUAL one is refused on MariaDB >= 11.4, with a reason naming #7. The
//! two shapes the images do carry are maintained here: STORED columns (which the server writes
//! into the images) and the VIRTUAL column itself (the planner treats it as an expression over
//! real columns).

mod common;

use anyhow::{ensure, Result};
use common::*;

/// Whether this server leaves VIRTUAL generated columns out of the row images (issue #7):
/// MariaDB >= 11.4. The same version gate as `source::plan_refusal` (which refuses a plan
/// reading a real column after a VIRTUAL one there) and `source::image_layouts` (which offers
/// the three layouts the reader reconciles).
async fn images_leave_out_virtuals(ctx: &Ctx, c: &mut mysql_async::Conn) -> Result<bool> {
    if !ctx.server().is_mariadb() {
        return Ok(false);
    }
    Ok(server_version(c).await? >= (11, 4))
}

/// A table whose real columns sit after generated ones: the STORED `s` and the real `extra`
/// follow the VIRTUAL `g`, so a layout that omits generated columns shifts them.
const GEN_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, \
                       g INT AS (a + 1) VIRTUAL, s INT AS (a + 2) STORED, extra INT NULL)";

/// The views over it that the planner accepts on every flavor: they read `id` and `a` (both
/// before the VIRTUAL column) and `s` (STORED, so reading it is not refused anywhere).
fn gen_views() -> Vec<ViewSpec> {
    vec![
        ViewSpec::new("v_sel", Kind::Incremental, "SELECT id, a, s FROM t"),
        ViewSpec::new(
            "v_by_s",
            Kind::Incremental,
            "SELECT s, COUNT(*) AS n FROM t GROUP BY s",
        ),
    ]
}

/// Writes that reach every image layout: INSERT (the full catalog), an UPDATE of the base of
/// the generated columns (a base-only image on MariaDB >= 11.4, where the STORED `s` the view
/// groups by is then in no image at all), an UPDATE of a column no view reads, and a DELETE.
async fn churn(c: &mut mysql_async::Conn, from: i64) -> Result<()> {
    exec(
        c,
        &format!(
            "INSERT INTO t (id, a, extra) VALUES ({}, {}, {}), ({}, {}, {}), ({}, {}, NULL)",
            from,
            from + 10,
            from,
            from + 1,
            from + 20,
            from + 1,
            from + 2,
            from + 30
        ),
    )
    .await?;
    exec(c, "UPDATE t SET a = a + 7 WHERE id % 3 = 1").await?;
    exec(c, "UPDATE t SET extra = extra + 1000 WHERE id % 4 = 2").await?;
    exec(c, "UPDATE t SET a = 1 WHERE a > 100").await?;
    exec(c, "DELETE FROM t WHERE id % 7 = 3").await?;
    Ok(())
}

/// An ALTER followed by writes to the columns around the moved ones, then a full check of
/// every view: the layouts of this table change with the ALTER, so the next images of each
/// width must be reconciled against the new catalog before anything is read by position.
async fn alter_and_check(
    ctx: &Ctx,
    eng: &mut Engine,
    db: &Db,
    c: &mut mysql_async::Conn,
    specs: &[ViewSpec],
    step: &(String, String),
    next_id: i64,
) -> Result<()> {
    let (what, ddl) = (step.0.as_str(), step.1.as_str());
    exec(c, ddl).await?;
    exec(c, "UPDATE t SET a = a + 1 WHERE id % 2 = 1").await?;
    exec(
        c,
        &format!("INSERT INTO t (id, a, extra) VALUES ({next_id}, {next_id}, {next_id})"),
    )
    .await?;
    exec(c, "DELETE FROM t WHERE id % 11 = 5").await?;
    verify(ctx, eng, db, specs, what).await
}

/// The acceptance case of PR-2: a view grouping by a STORED generated column that sits after
/// a VIRTUAL one is maintained incrementally and exactly on every flavor. On MariaDB >= 11.4
/// the images that carry the change do not carry `s` at all: the map says so, and the plan
/// marks what it cannot see instead of reading its neighbour's value.
#[test]
fn stored_generated_after_a_virtual_maintains_exactly() {
    scenario("vim_stored_after_virtual", |ctx| async move {
        let db = ctx.new_db("vim_stored").await?;
        let mut c = db.conn().await?;
        exec(&mut c, GEN_DDL).await?;
        exec(
            &mut c,
            "INSERT INTO t (id, a, extra) VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30), (4, 5, 40)",
        )
        .await?;
        let specs = gen_views();
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        for round in 0..4 {
            churn(&mut c, 100 + round * 20).await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after churn round {round}"),
            )
            .await?;
        }
        // Only the VIRTUAL column's base moves: `g` is in no image of any MariaDB >= 11.4
        // layout, and the STORED `s` the view groups by moves with it.
        exec(&mut c, "UPDATE t SET a = a + 1 WHERE id % 2 = 0").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after moving the virtual's base",
        )
        .await?;
        // Two rows whose `s` becomes equal: the groups merge.
        exec(&mut c, "UPDATE t SET a = 11 WHERE id % 5 = 1").await?;
        verify(&ctx, &mut eng, &db, &specs, "after merging groups").await?;
        // One transaction: its images arrive together.
        txn(
            &mut c,
            &[
                "INSERT INTO t (id, a, extra) VALUES (900, 900, 1)".to_string(),
                "UPDATE t SET a = a + 3 WHERE id = 900".to_string(),
                "DELETE FROM t WHERE id = 4".to_string(),
            ],
            false,
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "after a transaction").await?;
        eng.stop().await?;
        Ok(())
    });
}

/// The frozen half of #7 (until PR-4): where the server omits VIRTUAL columns from the
/// images, a view reading a REAL column positioned after one is refused and the state's
/// reason names the issue. Elsewhere the same view is maintained and verified: the map must
/// not have quietly widened what the planner accepts.
#[test]
fn real_column_after_a_virtual_is_refused_where_images_omit_virtuals() {
    scenario("vim_refusal", |ctx| async move {
        let db = ctx.new_db("vim_refusal").await?;
        let mut c = db.conn().await?;
        let virtuals_out = images_leave_out_virtuals(&ctx, &mut c).await?;
        exec(
            &mut c,
            "CREATE TABLE p (id INT PRIMARY KEY, a INT NULL, g INT AS (a + 1) VIRTUAL, \
             c INT NULL)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO p (id, a, c) VALUES (1, 1, 10), (2, NULL, 20), (3, 3, NULL)",
        )
        .await?;
        let spec = ViewSpec::new("v_p", Kind::Incremental, "SELECT id, g, c FROM p");
        if !virtuals_out {
            let specs = vec![spec];
            let (mut eng, _ids) = start_with_views(&ctx, &db, &specs, &[]).await?;
            verify(&ctx, &mut eng, &db, &specs, "initial").await?;
            exec(
                &mut c,
                "UPDATE p SET c = c + 1, a = COALESCE(a, 0) + 1 WHERE id % 2 = 1",
            )
            .await?;
            exec(&mut c, "INSERT INTO p (id, a, c) VALUES (4, 4, 40)").await?;
            exec(&mut c, "DELETE FROM p WHERE id = 2").await?;
            verify(&ctx, &mut eng, &db, &specs, "after writes").await?;
            eng.stop().await?;
            return Ok(());
        }
        // The refusal must survive PR-2 untouched.
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &[spec]).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let row = wait_state(&mut c, &mut eng, "v_p", "error", t_ready()).await?;
        let why = row.last_error.unwrap_or_default();
        ensure!(
            why.contains("#7"),
            "the refusal does not name issue #7: {why}"
        );
        exec(&mut c, "UPDATE p SET c = COALESCE(c, 0) + 1 WHERE id = 1").await?;
        // The refused view stays refused while its table keeps changing.
        exec(&mut c, "INSERT INTO p (id, a, c) VALUES (7, 7, 70)").await?;
        let row = wait_state(&mut c, &mut eng, "v_p", "error", t_ready()).await?;
        ensure!(
            row.last_error.unwrap_or_default().contains("#7"),
            "the view stopped being refused after writes to its table"
        );
        eng.stop().await?;
        Ok(())
    });
}

/// A VIRTUAL column added, moved, turned STORED, turned VIRTUAL and dropped while a view over
/// the same table is live: every ALTER changes the widths the server logs, so each layout has
/// to be reconciled against the catalog again (and the stale one dropped) rather than applied
/// to the new table shape. The MySQL family also refuses to convert a VIRTUAL column to
/// STORED in place (errno 3106); MariaDB converts it by dropping and re-adding the column.
#[test]
fn alter_a_virtual_column_while_the_view_is_live() {
    scenario("vim_alter", |ctx| async move {
        let db = ctx.new_db("vim_alter").await?;
        let mut c = db.conn().await?;
        let mariadb = ctx.server().is_mariadb();
        exec(
            &mut c,
            "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, extra INT NULL)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO t (id, a, extra) VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)",
        )
        .await?;
        // Reads only `a`, which stays before every generated column: the plan is accepted on
        // every flavor through every ALTER below.
        let specs = vec![ViewSpec::new(
            "v_a",
            Kind::Incremental,
            "SELECT a, COUNT(*) AS n FROM t GROUP BY a",
        )];
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;

        let steps: Vec<(String, String)> = [
            (
                "a VIRTUAL column added after a",
                "ALTER TABLE t ADD COLUMN g INT AS (a + 1) VIRTUAL AFTER a",
            ),
            (
                "a STORED column added after the VIRTUAL one",
                "ALTER TABLE t ADD COLUMN s INT AS (a + 100) STORED AFTER g",
            ),
            (
                "the real column moved after both generated ones",
                "ALTER TABLE t MODIFY COLUMN extra INT NULL AFTER s",
            ),
            ("the STORED column dropped", "ALTER TABLE t DROP COLUMN s"),
            (
                "the VIRTUAL column turned STORED",
                "ALTER TABLE t DROP COLUMN g, ADD COLUMN g INT AS (a + 5) STORED AFTER a",
            ),
            (
                "the STORED column turned VIRTUAL",
                "ALTER TABLE t DROP COLUMN g, ADD COLUMN g INT AS (a + 7) VIRTUAL AFTER a",
            ),
            ("the VIRTUAL column dropped", "ALTER TABLE t DROP COLUMN g"),
        ]
        .into_iter()
        .map(|(what, ddl)| (what.to_string(), ddl.to_string()))
        .collect();
        for (i, step) in steps.iter().enumerate() {
            alter_and_check(&ctx, &mut eng, &db, &mut c, &specs, step, 100 + i as i64).await?;
        }
        if !mariadb {
            // The in-place conversion the MySQL family refuses: #7 does not change that.
            exec(
                &mut c,
                "ALTER TABLE t ADD COLUMN g INT AS (a + 3) VIRTUAL AFTER a",
            )
            .await?;
            let e = exec(
                &mut c,
                "ALTER TABLE t MODIFY COLUMN g INT AS (a + 3) STORED",
            )
            .await
            .expect_err("MySQL refuses to make a VIRTUAL column STORED");
            ensure!(
                server_code(&e) == Some(3106),
                "expected errno 3106, got {e:?}"
            );
            exec(&mut c, "ALTER TABLE t DROP COLUMN g").await?;
            verify(&ctx, &mut eng, &db, &specs, "after the refused conversion").await?;
        }
        eng.stop().await?;
        Ok(())
    });
}

/// The rows of a prepared XA transaction reach the view through `xa_committed`, which knows
/// only table names: each group of rows resolves the image map of its own table, and the
/// plain table beside it keeps the identity path.
#[test]
fn xa_rows_of_a_table_with_generated_columns_use_their_image_map() {
    scenario("vim_xa", |ctx| async move {
        let db = ctx.new_db("vim_xa").await?;
        let mut c = db.conn().await?;
        exec(&mut c, GEN_DDL).await?;
        exec(
            &mut c,
            "CREATE TABLE u (id INT PRIMARY KEY, a INT NOT NULL, note VARCHAR(10) NULL)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO t (id, a, extra) VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO u (id, a, note) VALUES (1, 1, 'x'), (2, 8, 'y'), (3, 3, 'z')",
        )
        .await?;
        let specs = vec![
            ViewSpec::new("v_sel", Kind::Incremental, "SELECT id, a, s FROM t"),
            ViewSpec::new(
                "v_join",
                Kind::Incremental,
                "SELECT t.id AS tid, t.s AS ts, u.id AS uid, u.note FROM t JOIN u ON u.a = t.a",
            ),
        ];
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let mut xc = db.conn().await?;

        // Prepared and unresolved: invisible. The branch changes `t` (whose images may omit
        // the columns the view reads) and `u` (identity).
        exec_all(
            &mut xc,
            &[
                "XA START 'vim1'".to_string(),
                "INSERT INTO t (id, a, extra) VALUES (501, 51, 51)".to_string(),
                "UPDATE t SET a = a + 40 WHERE id = 2".to_string(),
                "INSERT INTO u (id, a, note) VALUES (501, 51, 'xa')".to_string(),
                "UPDATE u SET note = 'xa' WHERE id = 1".to_string(),
                "XA END 'vim1'".to_string(),
            ],
        )
        .await?;
        exec(&mut xc, "XA PREPARE 'vim1'").await?;
        exec(&mut c, "UPDATE t SET extra = extra + 1 WHERE id = 1").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA prepared (uncommitted)").await?;
        exec(&mut xc, "XA COMMIT 'vim1'").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA COMMIT").await?;

        // A rolled-back branch never appears.
        exec_all(
            &mut xc,
            &[
                "XA START 'vim2'".to_string(),
                "UPDATE t SET a = a + 60 WHERE id = 501".to_string(),
                "DELETE FROM u WHERE id = 2".to_string(),
                "XA END 'vim2'".to_string(),
            ],
        )
        .await?;
        exec(&mut xc, "XA PREPARE 'vim2'").await?;
        verify(&ctx, &mut eng, &db, &specs, "second XA prepared").await?;
        exec(&mut xc, "XA ROLLBACK 'vim2'").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA ROLLBACK").await?;

        // One phase.
        exec_all(
            &mut xc,
            &[
                "XA START 'vim3'".to_string(),
                "INSERT INTO t (id, a, extra) VALUES (502, 52, 52)".to_string(),
                "UPDATE t SET a = a + 1 WHERE id = 1".to_string(),
                "XA END 'vim3'".to_string(),
            ],
        )
        .await?;
        exec(&mut xc, "XA COMMIT 'vim3' ONE PHASE").await?;
        verify(&ctx, &mut eng, &db, &specs, "XA COMMIT ONE PHASE").await?;
        eng.stop().await?;
        Ok(())
    });
}

/// R4: the routing decision over the JOIN and cascade shapes, with a VIRTUAL column
/// before a real one. The join that reads only the prefix every layout keeps stays on
/// the core (the core's positional read is exact there); the join that reads the STORED
/// column past the VIRTUAL one keeps the viewplan executor (`Dedup::layout_refusal`);
/// the grouped view over the FK child runs on the executor through the cascade map.
/// All three take the same churn — every image layout, and a cascade delete of the
/// child the binlog never logs — and every one stays oracle-equal on every flavor.
#[test]
fn join_and_cascade_shapes_are_maintained_on_both_routes() {
    scenario("vim_shapes", |ctx| async move {
        let db = ctx.new_db("vim_shapes").await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, \
             score INT NOT NULL, g INT AS (score + 1) VIRTUAL, s INT AS (score + 2) STORED, \
             extra INT NULL)",
        )
        .await?;
        exec(
            &mut c,
            "CREATE TABLE links (id INT PRIMARY KEY, entity_id INT NOT NULL, w INT NULL)",
        )
        .await?;
        exec(
            &mut c,
            "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, qty INT NOT NULL, \
             g INT AS (qty + 1) VIRTUAL, s INT AS (qty + 2) STORED, extra INT NULL, \
             FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE \
             ON UPDATE CASCADE)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO entities (id, name, score, extra) VALUES \
             (1,'a',1,10),(2,'b',2,20),(3,'c',3,30),(4,'d',4,NULL)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO links (id, entity_id, w) VALUES \
             (1,1,5),(2,1,6),(3,2,NULL),(4,3,7),(5,4,8)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO items (id, entity_id, qty, extra) VALUES \
             (1,1,1,1),(2,1,2,2),(3,2,1,3),(4,3,3,NULL),(5,4,2,5)",
        )
        .await?;
        let specs = vec![
            // Reads `id`, `name` (before the VIRTUAL) and the plain partner: a prefix
            // join, which the core may serve by position.
            ViewSpec::new(
                "v_join_prefix",
                Kind::Incremental,
                "SELECT e.id, e.name, l.w FROM entities e JOIN links l ON l.entity_id = e.id",
            ),
            // Reads the STORED `s` positioned after the VIRTUAL `g`: the core indexes an
            // image by position and cannot see it in a base-only image — the executor.
            ViewSpec::new(
                "v_join_stored",
                Kind::Incremental,
                "SELECT e.id, e.s, l.w FROM entities e JOIN links l ON l.entity_id = e.id",
            ),
            // The cascade shape: only the child is read, the parent is the cascade's own
            // business; its kept rows go through the map and a parent's cascade delete
            // reaches the view through the cascade route.
            ViewSpec::new(
                "v_cascade",
                Kind::Incremental,
                "SELECT i.qty, COUNT(*) AS n, SUM(i.qty * 2) AS d FROM items i GROUP BY i.qty",
            ),
        ];
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let p0 = eng.stats_of(&db, "v_join_prefix")?;
        let s0 = eng.stats_of(&db, "v_join_stored")?;
        for round in 0..3 {
            let b = 100 + round * 20;
            exec(
                &mut c,
                &format!(
                    "INSERT INTO entities (id, name, score, extra) VALUES ({b}, 'e{b}', {b}, {b})"
                ),
            )
            .await?;
            exec(
                &mut c,
                &format!(
                    "INSERT INTO links (id, entity_id, w) VALUES ({b}, {b}, {})",
                    b % 5
                ),
            )
            .await?;
            exec(
                &mut c,
                &format!(
                    "INSERT INTO items (id, entity_id, qty, extra) VALUES ({b}, {}, {}, NULL)",
                    1 + b % 4,
                    b % 3
                ),
            )
            .await?;
            // The VIRTUAL's and STORED's base moves: on MariaDB >= 11.4 the image that
            // carries the change carries neither of them.
            exec(
                &mut c,
                "UPDATE entities SET score = score + 1 WHERE id % 3 = 1",
            )
            .await?;
            exec(
                &mut c,
                "UPDATE entities SET extra = extra + 100 WHERE id % 4 = 2",
            )
            .await?;
            exec(&mut c, "UPDATE items SET qty = qty + 1 WHERE id % 2 = 1").await?;
            exec(&mut c, "DELETE FROM items WHERE id % 7 = 3").await?;
            // The cascade delete: the child rows leave without a binlog row event.
            exec(&mut c, "DELETE FROM entities WHERE id % 11 = 4").await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after churn round {round}"),
            )
            .await?;
        }
        // The two routes did differ: the prefix join fed the core's counters, the join
        // reading past the VIRTUAL column never touched them (its plan is refused there).
        let p1 = eng.stats_of(&db, "v_join_prefix")?;
        let s1 = eng.stats_of(&db, "v_join_stored")?;
        ctx.log(format!(
            "routes: prefix ids_marked +{}, stored ids_marked +{}",
            p1.ids_marked.saturating_sub(p0.ids_marked),
            s1.ids_marked.saturating_sub(s0.ids_marked)
        ));
        ensure!(
            p1.ids_marked > p0.ids_marked,
            "the prefix join never fed the core: it is not on the route its plan earned"
        );
        ensure!(
            s1.ids_marked == s0.ids_marked,
            "the join reading STORED past VIRTUAL fed the core {} marks: layout_refusal \
             must keep it on the viewplan executor",
            s1.ids_marked.saturating_sub(s0.ids_marked)
        );
        eng.stop().await?;
        Ok(())
    });
}

/// R1: a view whose cascade map keeps a STORED generated key is refused at PLAN time on
/// a server that logs generated columns out of its images (`Dedup::map_refusal`, the
/// reason names #7 and the kept column): the map reads its kept rows back from the
/// child's UPDATE/DELETE images, which carry no slot for that column, so no kept row
/// could ever be trusted — a per-row fallback would decide that permanent condition
/// again with every row change. On a server whose images are the full catalog the same
/// view is built and maintained exactly.
#[test]
fn cascade_map_keeping_a_stored_generated_key_is_refused_where_images_omit_generated() {
    scenario("vim_map_refusal", |ctx| async move {
        let db = ctx.new_db("vim_map_refusal").await?;
        let mut c = db.conn().await?;
        let omit_generated = images_leave_out_virtuals(&ctx, &mut c).await?;
        exec(
            &mut c,
            "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
        )
        .await?;
        // The child's PK is STORED generated (`pk_src` carries its base) — it is also the
        // map key of the untraced cascade route the grouped view over the child builds.
        exec(
            &mut c,
            "CREATE TABLE parts (pk_src INT NOT NULL, pk INT AS (pk_src) STORED NOT NULL, \
             eid INT NOT NULL, qty INT NOT NULL, PRIMARY KEY (pk), KEY (eid), \
             FOREIGN KEY (eid) REFERENCES entities (id) ON DELETE CASCADE ON UPDATE CASCADE)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO entities (id, name) VALUES (1,'a'),(2,'b'),(3,'c')",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO parts (pk_src, eid, qty) VALUES \
             (10,1,1),(11,1,2),(12,2,1),(13,3,3),(14,3,2)",
        )
        .await?;
        let spec = ViewSpec::new(
            "v_parts",
            Kind::Incremental,
            "SELECT qty, COUNT(*) AS n FROM parts GROUP BY qty",
        );
        if !omit_generated {
            let specs = vec![spec];
            let (mut eng, _ids) =
                start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
            verify(&ctx, &mut eng, &db, &specs, "initial").await?;
            exec(&mut c, "UPDATE parts SET qty = qty + 1 WHERE pk % 2 = 0").await?;
            exec(
                &mut c,
                "INSERT INTO parts (pk_src, eid, qty) VALUES (90, 2, 4)",
            )
            .await?;
            exec(&mut c, "DELETE FROM parts WHERE pk = 11").await?;
            exec(&mut c, "DELETE FROM entities WHERE id = 3").await?;
            verify(&ctx, &mut eng, &db, &specs, "after writes").await?;
            eng.stop().await?;
            return Ok(());
        }
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &[spec]).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let row = wait_state(&mut c, &mut eng, "v_parts", "error", t_ready()).await?;
        let why = row.last_error.unwrap_or_default();
        ensure!(
            why.contains("#7") && why.contains("parts") && why.contains("pk"),
            "the refusal names issue #7, the child table and the kept column: {why}"
        );
        exec(&mut c, "UPDATE parts SET qty = qty + 1 WHERE pk = 10").await?;
        exec(
            &mut c,
            "INSERT INTO parts (pk_src, eid, qty) VALUES (91, 1, 5)",
        )
        .await?;
        let row = wait_state(&mut c, &mut eng, "v_parts", "error", t_ready()).await?;
        ensure!(
            row.last_error.unwrap_or_default().contains("#7"),
            "the view stopped being refused while its table keeps changing"
        );
        eng.stop().await?;
        Ok(())
    });
}
