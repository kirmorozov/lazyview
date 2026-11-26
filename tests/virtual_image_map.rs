//! C-01 (issue #7): a row image that leaves generated columns out must still be read by
//! catalog column. The binlog reader reconciles one image map per (table, image layout) at
//! the TABLE_MAP boundary and `accumulate` reads every value through it, so a table whose
//! images omit a column keeps its incremental plan instead of being read by position.
//!
//! R4 covers the routing decision end to end over the JOIN and cascade shapes (the neutral
//! `entities` / `links` / `items` fixtures) with a VIRTUAL column before a real one. The route
//! of each plan is read from the record the engine keeps of it — `Dedup::sync` logs a plan it
//! held back from the core, by name and with its reason, and says nothing about a plan the core
//! took — plus the engine's cascade cache replaying kept child rows for a parent's delete
//! (`cascade_rows`), the read that goes through the image map on a server that logs generated
//! columns out of its images. The counters (`ids_marked`, `images_absorbed`) are logged, not
//! asserted: they are the engine's own tally and accrue on both routes, so they say a view was
//! fed, never where. All four views take the same churn and stay equal to their oracle on every
//! flavor.
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

/// R4: the routing decision over the JOIN and cascade shapes, with a VIRTUAL and a STORED
/// column before a real one on both sides of the join. Five views, five plans, one churn, and
/// each plan is checked against the record the engine itself keeps of the route it got
/// (`Dedup::sync` logs a plan it held back from the core, by name and with the gate's own
/// reason, and says nothing about a plan the core took):
///
/// * the join reading only the prefix every layout preserves stays on the **core**: the gate has
///   no reason to touch it. Proof: no refusal line for it at all, and it was fed through the
///   churn; at unit level the same SQL is `Dedup::routed` with a `None` `core_refusal` and a
///   `None` `layout_refusal` (`exec::tests::layout_refusal_covers_join_and_cascade_shapes`).
/// * the join reading the parent's STORED column past the parent's VIRTUAL one, and the
///   parent-only view doing the same, keep the **executor only where the images omit generated
///   columns** (the layout gate), and stay on the core where the image is the full catalog.
///   Proof: the layout gate's line for each, present iff the flavor omits them — with both fed
///   through the churn either way. These two are also the functional proof of the map: on a
///   base-only image a positional read gives the neighbour's value (the panic this PR exists
///   for) and their answers equal the oracle.
/// * the view joining the parent to the child of the `ON DELETE CASCADE` FK keeps the **viewplan
///   executor** on every flavor. Proof: `core_refusal`'s line names the cascade and the
///   executor; at unit level the same SQL is `Dedup::routed` with the layout gate silent and
///   `core_refusal` naming that cascade, so this refusal — not a layout, not the access route —
///   is the only thing that moved it off the core.
/// * the child-only grouped view takes the **mapped cascade** route: the engine logs it `via map
///   key`, tracing the parent `^entities:map`, which is what makes
///   `accumulate::feed_map_with_image` the reader of its kept child rows — the rows a parent's
///   cascade delete answers from, with no binlog row event behind them. Proof: those two lines.
///   `cascade_rows` is logged, not asserted: measured on all four flavors it does not move for a
///   parent's delete here while the view stays oracle-equal, so it measures a path the marks
///   already healed rather than whether the map was read (reported for #167/PR-3).
///
/// All five take the same churn — every image layout, and a cascade delete of the child the
/// binlog never logs — and stay equal to their oracle on every flavor. The counters
/// (`ids_marked`, `images_absorbed`, `cascade_rows`, `cascade_gaps`) are logged, never asserted:
/// they are the engine's own tally and accrue on both routes, so a number moving says a view was
/// fed, never where.
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
            // Reads the STORED `s` positioned after the VIRTUAL `g` — on the PARENT side, but
            // read as a column like any other, so the layout gate owns it: the executor where
            // the images omit generated columns, the core where they are the catalog.
            ViewSpec::new(
                "v_join_stored",
                Kind::Incremental,
                "SELECT e.id, e.s, l.w FROM entities e JOIN links l ON l.entity_id = e.id",
            ),
            // The parent alone, reading its STORED `s` past its VIRTUAL `g`: `entities` is a
            // cascade parent (nothing of its own is cascaded into it), so this is the one plan
            // of the four whose route the layout gate decides — the executor where the images
            // omit generated columns, the core where they are the catalog.
            ViewSpec::new(
                "v_parent_stored",
                Kind::Incremental,
                "SELECT e.id, e.score, e.s FROM entities e",
            ),
            // The cascade shape the core holds: the view joins the parent to the child of the
            // ON DELETE CASCADE FK, so a parent's delete removes child rows the binlog never
            // logs. `Dedup::core_refusal` keeps every cascade plan on the viewplan executor until
            // #167's replay-window fixes land, so this plan — which has no cascade map (the
            // engine logs its FK `via direct`) — is the one whose route only that refusal
            // decides. (That it routes no map is why the first version of this test, a
            // child-only view, could not show anything here: it is a different shape.)
            ViewSpec::new(
                "v_cascade",
                Kind::Incremental,
                "SELECT e.id AS eid, i.id AS iid, i.qty FROM entities e \
                 JOIN items i ON i.entity_id = e.id",
            ),
            // The MAPPED cascade shape: only the child is read, so the plan follows the FK
            // through the cascade map — the engine logs it `via map key` and traces the parent
            // `^entities:map` — and its kept child rows are read back out of the child's images
            // by `accumulate::feed_map_with_image`. This is the view whose kept rows the image
            // map serves on a server that logs generated columns out of its images.
            ViewSpec::new(
                "v_child_map",
                Kind::Incremental,
                "SELECT i.qty, COUNT(*) AS n, SUM(i.qty * 2) AS d FROM items i GROUP BY i.qty",
            ),
        ];
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        let p0 = eng.stats_of(&db, "v_join_prefix")?;
        let s0 = eng.stats_of(&db, "v_join_stored")?;
        let f0 = eng.stats_of(&db, "v_parent_stored")?;
        let k0 = eng.stats_of(&db, "v_cascade")?;
        let m0 = eng.stats_of(&db, "v_child_map")?;
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
        // The routes, read from the record the engine itself keeps of them. `ids_marked` is
        // logged but deliberately NOT the discriminator: the tally is the engine's (`stats.rs`)
        // and accrues on both routes, with the core's own counters merely added on top for a
        // view that has a core target — a number moving says the view was fed, never where.
        // What says where is `Dedup::sync`'s refusal: a plan held back from the core is logged
        // once, by name, with its reason (`src/engine.rs`, "the dedup core refused the view's
        // plan, it keeps the viewplan executor"); a plan the core takes is not logged. That the
        // unlogged plans here really are core plans is pinned where the same decision is made,
        // at unit level (`exec::tests::layout_refusal_covers_join_and_cascade_shapes`: the same
        // SQL asserted `Dedup::routed` with a `None` `core_refusal`/`layout_refusal`).
        let omit_generated = images_leave_out_virtuals(&ctx, &mut c).await?;
        let p1 = eng.stats_of(&db, "v_join_prefix")?;
        let s1 = eng.stats_of(&db, "v_join_stored")?;
        let f1 = eng.stats_of(&db, "v_parent_stored")?;
        let k1 = eng.stats_of(&db, "v_cascade")?;
        let m1 = eng.stats_of(&db, "v_child_map")?;
        ctx.log(format!(
            "fed: marks prefix +{}, stored +{}, parent +{}, cascade +{}, child_map +{}; \
             cascade_rows +{} cascade_gaps +{}; child_map cascade_rows +{} cascade_gaps +{} \
             (images omit generated: {omit_generated})",
            p1.ids_marked.saturating_sub(p0.ids_marked),
            s1.ids_marked.saturating_sub(s0.ids_marked),
            f1.ids_marked.saturating_sub(f0.ids_marked),
            k1.ids_marked.saturating_sub(k0.ids_marked),
            m1.ids_marked.saturating_sub(m0.ids_marked),
            k1.cascade_rows.saturating_sub(k0.cascade_rows),
            k1.cascade_gaps.saturating_sub(k0.cascade_gaps),
            m1.cascade_rows.saturating_sub(m0.cascade_rows),
            m1.cascade_gaps.saturating_sub(m0.cascade_gaps),
        ));
        for (view, marks, absorbed) in [
            (
                "v_join_prefix",
                p1.ids_marked.saturating_sub(p0.ids_marked),
                0,
            ),
            (
                "v_join_stored",
                s1.ids_marked.saturating_sub(s0.ids_marked),
                s1.ids_absorbed.saturating_sub(s0.ids_absorbed),
            ),
            (
                "v_cascade",
                k1.ids_marked.saturating_sub(k0.ids_marked),
                k1.cascade_rows.saturating_sub(k0.cascade_rows),
            ),
            (
                "v_parent_stored",
                f1.ids_marked.saturating_sub(f0.ids_marked),
                0,
            ),
            (
                "v_child_map",
                m1.ids_marked.saturating_sub(m0.ids_marked),
                m1.cascade_rows.saturating_sub(m0.cascade_rows),
            ),
        ] {
            ensure!(
                marks + absorbed > 0,
                "`{view}` was fed nothing through the churn: the shape is not being exercised"
            );
        }
        let log = std::fs::read_to_string(&eng.log_path).unwrap_or_default();
        // `Dedup::sync` logs a plan it held back from the core once, by name, with the gate's own
        // reason; a plan the core took is never logged. `why` is that reason's text (empty = any
        // gate), so each assertion reads the decision it claims to read: the layout gate ("logs
        // its generated columns out of the row images") or the cascade hold ("cascade `entities`
        // -> `items`", which both `core_refusal` wordings for a cascade carry).
        let log = log.as_str();
        let refused = |view: &str, why: &str| {
            log.lines().any(|l| {
                l.contains("keeps the viewplan executor") && l.contains(view) && l.contains(why)
            })
        };
        const LAYOUT_WHY: &str = "logs its generated columns out of the row images";
        // The prefix join stays on the core: nothing it reads sits at a position a layout moves,
        // so the gate has no reason to touch it (unit-pinned: the same SQL is `Dedup::routed`
        // with a `None` `core_refusal` and a `None` `layout_refusal`).
        ensure!(
            !refused("v_join_prefix", ""),
            "a join reading only what every layout preserves was pushed off the core: the layout \
             gate is over-refusing (engine log {})",
            eng.log_path.display()
        );
        // The join reading the PARENT's STORED column past its VIRTUAL one reads that column
        // like any other, so the layout gate moves it to the mapped executor exactly where the
        // images omit generated columns — and leaves it on the core where they do not. Refusing
        // it everywhere would cost a route for nothing; reading it on the core where the images
        // are base-only would read the neighbour's value.
        ensure!(
            refused("v_join_stored", LAYOUT_WHY) == omit_generated,
            "the join reading the parent's STORED column past its VIRTUAL one {} on this server \
             (images omit generated columns: {omit_generated}); the engine logged the layout \
             refusal for it: {} (log {})",
            if omit_generated {
                "should have kept the viewplan executor, which reads the image through the map"
            } else {
                "should have stayed on the core, whose positional read is exact there"
            },
            refused("v_join_stored", LAYOUT_WHY),
            eng.log_path.display()
        );
        // The cascade view: the core refuses every cascade plan until #167, so this is the
        // shape whose kept child rows — what a parent's cascade delete answers from, with no
        // binlog row event behind it — are read by the mapped feed.
        ensure!(
            refused("v_cascade", "cascade `entities` -> `items`"),
            "the view joining the parent to the cascading child did not keep the viewplan \
             executor, so the mapped cascade feed went untested here (engine log {})",
            eng.log_path.display()
        );
        // The mapped cascade route, from the engine's own record of the plan: it follows this
        // FK through the cascade map (`via map key`, the parent traced `^entities:map`), which
        // is what makes `accumulate::feed_map_with_image` the reader of the child's kept rows —
        // the rows a parent's cascade delete answers from, with no binlog row event behind them.
        // `cascade_rows` is logged above and deliberately NOT asserted: measured on all four
        // flavors (tmp/out/vim-cont-1124/suite-vim3-*), a parent's delete on this fixture
        // replays no kept row through `expand` on any of them while the view stays equal to its
        // oracle, so the counter measures a path the marks already healed, not whether the map
        // was read. What the map is trusted for — reading a kept row where the image is not the
        // catalog — is pinned at unit level (`accumulate::tests::value_at_reads_through_the_
        // image_map`, the fail-before of this PR) and by the two layout-routed views below:
        // on a base-only image a wrong map makes those two answers wrong, and they are not.
        ensure!(
            log.lines()
                .any(|l| l.contains("follows cascades through the cascade map")
                    && l.contains("v_child_map")
                    && l.contains("`entities` -> `items`"))
                && log.contains("via map key"),
            "the child-only view did not take the mapped cascade route, so the cascade cache — \
             the reader the image map serves — went untested here (engine log {})",
            eng.log_path.display()
        );
        // The layout gate's own decision, on the one plan not already kept off the core by a
        // cascade: the parent reading its STORED column past its VIRTUAL one. It keeps the
        // executor only where the images omit generated columns, and the core where they are the
        // full catalog.
        ensure!(
            refused("v_parent_stored", LAYOUT_WHY) == omit_generated,
            "the parent view reading its STORED column past its VIRTUAL one {} on this server \
             (images omit generated columns: {omit_generated}); the engine logged the layout \
             refusal: {} (log {})",
            if omit_generated {
                "should have kept the viewplan executor, which reads the image through the map"
            } else {
                "should have stayed on the core, whose positional read is exact there"
            },
            refused("v_parent_stored", LAYOUT_WHY),
            eng.log_path.display()
        );
        eng.stop().await?;
        Ok(())
    });
}

/// R4 / R1: the same join read on the CHILD's side, where the core's positional read does
/// decide the route. The child keeps `extra` positioned after its generated `g` and `s`, so on
/// a server that logs generated columns out of its images no layout has a slot in front of it
/// and `Dedup::layout_refusal` refuses the plan where it is built (the parent-side read of the
/// test above is a read by key and stays on the core). The refusal is a plan-time `_lazyview_
/// state` error naming #7, the child table and the column — not a row-by-row decision. Where
/// the image is the full catalog the same view is built and maintained exactly.
#[test]
fn join_reading_a_child_column_past_its_generated_columns_is_refused_where_images_omit_generated() {
    scenario("vim_join_refusal", |ctx| async move {
        let db = ctx.new_db("vim_join_refusal").await?;
        let mut c = db.conn().await?;
        let omit_generated = images_leave_out_virtuals(&ctx, &mut c).await?;
        exec(
            &mut c,
            "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
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
            "INSERT INTO entities (id, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO items (id, entity_id, qty, extra) VALUES \
             (1,1,1,1),(2,1,2,2),(3,2,1,3),(4,3,3,NULL),(5,4,2,5)",
        )
        .await?;
        let spec = ViewSpec::new(
            "v_items",
            Kind::Incremental,
            "SELECT i.id AS iid, i.entity_id, i.extra, e.id AS eid, e.name FROM items i \
             JOIN entities e ON i.entity_id = e.id",
        );
        if !omit_generated {
            let specs = vec![spec];
            let (mut eng, _ids) =
                start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
            verify(&ctx, &mut eng, &db, &specs, "initial").await?;
            exec(&mut c, "UPDATE items SET qty = qty + 1 WHERE id % 2 = 0").await?;
            exec(
                &mut c,
                "UPDATE items SET extra = extra + 10 WHERE id % 3 = 1",
            )
            .await?;
            exec(
                &mut c,
                "INSERT INTO items (id, entity_id, qty, extra) VALUES (90, 2, 4, 4)",
            )
            .await?;
            exec(&mut c, "DELETE FROM items WHERE id = 11").await?;
            exec(&mut c, "DELETE FROM entities WHERE id = 3").await?;
            verify(&ctx, &mut eng, &db, &specs, "after writes").await?;
            eng.stop().await?;
            return Ok(());
        }
        ensure_lazyview_table(&mut c).await?;
        add_views(&mut c, &[spec]).await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let row = wait_state(&mut c, &mut eng, "v_items", "error", t_ready()).await?;
        let why = row.last_error.unwrap_or_default();
        ensure!(
            why.contains("#7") && why.contains("items") && why.contains("`extra`"),
            "the refusal names issue #7, the child table and the column read past the \
             generated ones: {why}"
        );
        exec(&mut c, "UPDATE items SET extra = extra + 1 WHERE id = 10").await?;
        exec(
            &mut c,
            "INSERT INTO items (id, entity_id, qty, extra) VALUES (91, 1, 5, 5)",
        )
        .await?;
        let row = wait_state(&mut c, &mut eng, "v_items", "error", t_ready()).await?;
        ensure!(
            row.last_error.unwrap_or_default().contains("#7"),
            "the view stopped being refused while its table keeps changing"
        );
        eng.stop().await?;
        Ok(())
    });
}

/// R1 / R4: the shape a cascade map is built for, keyed by a STORED generated column this
/// server keeps out of its images — a grouped view over the child of a cascading FK. Measured
/// on mariadb114 and mysql84 the compiler puts this view on the `source` target and no map
/// reads the generated column, so `Dedup::map_refusal` has no plan to refuse here (the case
/// that would key a map by such a column is `source::aggregate_no_group`, which has no call
/// site yet). What the flavors must therefore guarantee is that the route they do take reads
/// the generated column honestly: the view is built and stays equal to its oracle while
/// writes move the column's base and while a parent's cascade delete removes child rows the
/// binlog never logs. The refusal for a map that could not keep such a row is pinned where
/// the shape exists, in `src/exec.rs`
/// (`map_refusal_refuses_a_cascade_map_keeping_a_generated_key_or_fk`).
#[test]
fn cascade_grouped_by_a_stored_generated_column_is_maintained_where_images_omit_generated() {
    scenario("vim_map_generated_group", |ctx| async move {
        let db = ctx.new_db("vim_map_generated_group").await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "CREATE TABLE entities (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)",
        )
        .await?;
        // The grouped view is over the child of a cascading FK and groups by the STORED
        // generated `s`: no image layout of a server that omits generated columns has a slot
        // for it, so whichever route serves it must not read one by position.
        exec(
            &mut c,
            "CREATE TABLE parts (id INT PRIMARY KEY, eid INT NOT NULL, qty INT NOT NULL, \
             s INT AS (qty * 2) STORED, KEY (eid), KEY (s), \
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
            "INSERT INTO parts (id, eid, qty) VALUES (10,1,1),(11,1,2),(12,2,1),(13,3,3),(14,3,2)",
        )
        .await?;
        let specs = vec![ViewSpec::new(
            "v_parts",
            Kind::Incremental,
            "SELECT s, COUNT(*) AS n FROM parts GROUP BY s",
        )];
        let (mut eng, _ids) =
            start_with_views(&ctx, &db, &specs, &[("LAZYVIEW_BATCH_WINDOW_MS", "100")]).await?;
        verify(&ctx, &mut eng, &db, &specs, "initial").await?;
        // The generated column's base moves, so every image layout has to answer for the
        // group it belongs to; a parent delete cascades into rows the binlog never logs.
        exec(&mut c, "UPDATE parts SET qty = qty + 1 WHERE id % 2 = 0").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "the STORED column's base moved",
        )
        .await?;
        exec(&mut c, "INSERT INTO parts (id, eid, qty) VALUES (90, 2, 4)").await?;
        exec(&mut c, "DELETE FROM parts WHERE id = 11").await?;
        verify(&ctx, &mut eng, &db, &specs, "child writes").await?;
        exec(&mut c, "DELETE FROM entities WHERE id = 3").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "a cascade delete of the parent",
        )
        .await?;
        let s = eng.stats_of(&db, "v_parts")?;
        ctx.log(format!(
            "v_parts: ids_marked={}, ids_absorbed={}, images_absorbed={}, whole_view_refreshes={}",
            s.ids_marked, s.ids_absorbed, s.images_absorbed, s.whole_view_refreshes
        ));
        eng.stop().await?;
        Ok(())
    });
}
