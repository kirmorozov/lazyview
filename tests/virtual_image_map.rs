//! C-01 (issue #7): a row image that leaves generated columns out must still be read by
//! catalog column. The binlog reader reconciles one image map per (table, image layout) at
//! the TABLE_MAP boundary and `accumulate` reads every value through it, so a table whose
//! images omit a column keeps its incremental plan instead of being read by position.
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
