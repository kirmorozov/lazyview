//! Small deterministic reproductions of engine bugs found by the randomized suites (each runs on
//! every flavor in `LAZYVIEW_TEST_DSNS`, takes seconds, and names the bug it pins down).

#[path = "regressions/availability.rs"]
mod availability;
#[path = "regressions/builds.rs"]
mod builds;
#[path = "regressions/catchup.rs"]
mod catchup;
mod common;
#[path = "regressions/full_mode.rs"]
mod full_mode;
#[path = "regressions/joins.rs"]
mod joins;
#[path = "regressions/json.rs"]
mod json;
#[path = "regressions/lifecycle.rs"]
mod lifecycle;
#[path = "regressions/pinned.rs"]
mod pinned;
#[path = "regressions/state.rs"]
mod state;

use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use common::workload::names;
use common::*;
use lazyview::accumulate::UnitChunk;
use lazyview::mv::{Layout, MvCol};
use mysql_async::Value;

/// Tiny schema used by most reproductions.
const DDL: &[&str] = &[
    "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(30) NOT NULL, note VARCHAR(30) NULL)",
    "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NULL, status VARCHAR(10) NOT NULL)",
    "CREATE TABLE products (id INT PRIMARY KEY, sku VARCHAR(10) NOT NULL, price DECIMAL(10,2) NOT NULL, active TINYINT NOT NULL DEFAULT 1)",
    "INSERT INTO customers VALUES (1, 'ann', NULL), (2, 'bob', NULL), (3, 'cid', NULL)",
    "INSERT INTO orders VALUES (10, 1, 'new'), (11, 1, 'paid'), (12, 2, 'new'), (13, 3, 'paid')",
    "INSERT INTO products VALUES (1, 'a', 5.00, 1), (2, 'b', 50.00, 1), (3, 'c', 20.00, 1), (4, 'd', 99.00, 0), (5, 'e', 70.00, 1), (6, 'f', 1.00, 1)",
];

/// [`setup_views`] with the tiny schema [`DDL`] first, then `extra_ddl`.
async fn setup(
    ctx: &Ctx,
    tag: &str,
    extra_ddl: &[&str],
    specs: &[ViewSpec],
    ov: &[(&str, &str)],
) -> Result<(Db, Engine)> {
    let ddl: Vec<&str> = DDL.iter().chain(extra_ddl).copied().collect();
    setup_views(ctx, tag, &ddl, specs, ov).await
}

fn join_view() -> ViewSpec {
    ViewSpec::new(
        "r_order_customer",
        Kind::Incremental,
        "SELECT o.id, o.status, c.name FROM orders o JOIN customers c ON c.id = o.customer_id",
    )
}

/// The MV layout as `mv::probe` builds it: column types from information_schema (`json`
/// on MySQL/Percona, `longtext` on MariaDB), the first `units` columns as the unit.
async fn probe(
    c: &mut mysql_async::Conn,
    schema: &str,
    table: &str,
    units: usize,
) -> Result<Layout> {
    // (As the server names them: `mv::apply` writes with these names.)
    let rows: Vec<(String, String, String, String)> = typed_exec_as_sent(
        c,
        "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(IS_NULLABLE AS CHAR) \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    .await?;
    let cols: Vec<MvCol> = rows
        .into_iter()
        .map(|(name, column_type, data_type, nullable)| MvCol {
            src_name: name.clone(),
            name,
            column_type,
            data_type: data_type.to_ascii_lowercase(),
            charset: None,
            collation: None,
            nullable: nullable == "YES",
        })
        .collect();
    Ok(Layout {
        n_visible: cols.len(),
        hidden: Vec::new(),
        unit_pos: (0..units).collect(),
        primary: false,
        lineage: Vec::new(),
        epoch: Vec::new(),
        // JSON units: no row identity (written per unit).
        identity: Vec::new(),
        partial: Vec::new(),
        cols,
        // These tables are only ever built on a MySQL target: the MySQL family's key budget.
        key_budget: 3072,
    })
}

/// Rows of the view's query for the chunk's units (what `mv::read_source` does).
async fn rows(
    c: &mut mysql_async::Conn,
    layout: &Layout,
    select: &str,
    unit_exprs: &[&str],
    chunk: Option<&UnitChunk>,
) -> Result<Vec<Vec<Value>>> {
    let exprs: Vec<String> = unit_exprs.iter().map(|e| e.to_string()).collect();
    let (f, p) = match chunk {
        Some(ch) => ch.filter_typed(&exprs, &layout.unit_kinds()),
        None => ("TRUE".into(), Vec::new()),
    };
    let sql = select.replace("{F}", &f);
    let rows: Vec<mysql_async::Row> = typed_exec(c, sql, p).await?;
    Ok(rows
        .into_iter()
        .map(|r| layout.normalize_source(r.unwrap()))
        .collect())
}

/// The MV must hold exactly the oracle's groups (JSON-equal keys, equal counts).
async fn check(
    c: &mut mysql_async::Conn,
    mv: &str,
    oracle: &str,
    on: &str,
    what: &str,
) -> Result<()> {
    let (a, b, both): (i64, i64, i64) = typed_first(
        c,
        format!(
            "SELECT (SELECT COUNT(*) FROM {mv}), (SELECT COUNT(*) FROM ({oracle}) o), \
             (SELECT COUNT(*) FROM {mv} m JOIN ({oracle}) o ON {on} AND o.n = m.n)"
        ),
    )
    .await?
    .context("counts")?;
    ensure!(
        a == b && b == both,
        "{what}: {mv} has {a} rows, the oracle {b}, {both} agree"
    );
    Ok(())
}

/// A child of a cascading foreign key whose units the parent cannot give (the parent on the NULL
/// side of a LEFT JOIN with a condition beyond the foreign-key equality) but with a primary key
/// and an FK compared by equality: the cascade map follows it. The engine logs the mapped route
/// (not the rebuild warning, which is only for routes the map cannot keep), and the view stays
/// correct through the cascades without a rebuild.
#[test]
fn reg_mapped_cascade_is_logged() {
    scenario("reg_fk_untraced", |ctx| async move {
        let ddl = [
            "CREATE TABLE pars (id INT PRIMARY KEY, label VARCHAR(20) NOT NULL)",
            "CREATE TABLE kids (par_id INT NOT NULL, seq INT NOT NULL, qty INT NOT NULL, PRIMARY KEY (par_id, seq),
             FOREIGN KEY (par_id) REFERENCES pars (id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "INSERT INTO pars VALUES (10, 'a'), (11, 'zz'), (12, 'c')",
            "INSERT INTO kids VALUES (10, 1, 5), (10, 2, 6), (11, 1, 7), (12, 1, 8)",
        ];
        let specs = vec![ViewSpec::new(
            "r_kids_left",
            Kind::Incremental,
            "SELECT k.par_id, k.seq, k.qty, p.label FROM kids k \
             LEFT JOIN pars p ON p.id = k.par_id AND p.label <> 'zz'",
        )];
        let (db, mut eng) = setup(&ctx, "reg_fk_untraced", &ddl, &specs, &[]).await?;
        let log = std::fs::read_to_string(&eng.log_path)?;
        ensure!(
            !log.contains("reads a child of a cascading foreign key"),
            "a cascade the map follows still warns about a rebuild"
        );
        let line = log
            .lines()
            .find(|l| l.contains("follows cascades through the cascade map"))
            .context("no cascade-map line in the engine log")?;
        ensure!(
            line.contains("view=r_kids_left") && line.contains("`pars` -> `kids`"),
            "the cascade-map line does not name the route: {line}"
        );
        let before = eng.snapshot();
        let mut c = db.conn().await?;
        for (what, stmt) in [
            (
                "parent deleted (children cascade)",
                "DELETE FROM pars WHERE id = 10",
            ),
            ("parent key moved", "UPDATE pars SET id = 20 WHERE id = 12"),
        ] {
            exec(&mut c, stmt).await?;
            verify(&ctx, &mut eng, &db, &specs, what).await?;
        }
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds == 0,
            "the mapped cascades rebuilt the view: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await
    });
}

/// A view whose targets include an unreachable instance must still be built and maintained on
/// the reachable ones (v_orders_left with targets 'b,down' never appeared on b).
#[test]
fn reg_multi_target_with_one_down() {
    scenario_multi("reg_target_down", 1, |ctx| async move {
        let src = ctx.new_db("reg_tdown_src").await?;
        let tb = ctx.new_db_on(1, "reg_tdown_b").await?;
        let mut c = src.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        ensure_lazyview_table(&mut c).await?;
        let spec = join_view().with_targets("b,down");
        add_views(&mut c, std::slice::from_ref(&spec)).await?;
        let env = target_env(&[
            ("b", &tb.dsn),
            ("down", "mysql://lv:lv@127.0.0.1:1/nowhere"),
        ]);
        let mut eng = ctx.start_engine(&src, &ov_refs(&env))?;
        let tmap: std::collections::HashMap<String, Db> = [("b".to_string(), tb.clone())].into();
        let healthy = vec![spec.clone().with_targets("b")];
        let limit = Duration::from_secs(90);
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            limit,
            "built on b although 'down' is unreachable",
        )
        .await?;
        exec_all(&mut c, &["UPDATE customers SET name = 'zed' WHERE id = 3"]).await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &src,
            &tmap,
            &healthy,
            limit,
            "maintained on b",
        )
        .await?;
        if eng.exited().is_some() {
            bail!("engine exited");
        }
        Ok(())
    });
}

/// Writes to `entities` (unless `links_only`) and `links` until `stop`: inserts into both,
/// flips the status of existing entities. Ids come from `seq`.
async fn churn(
    mut c: mysql_async::Conn,
    seq: std::sync::Arc<std::sync::atomic::AtomicI64>,
    links_only: bool,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    loop {
        let i = seq.fetch_add(1, Relaxed);
        if !links_only {
            exec(
                &mut c,
                &format!(
                    "INSERT INTO entities (id, grp, status) VALUES ({i}, {}, 'new')",
                    i % 3
                ),
            )
            .await?;
            exec(
                &mut c,
                &format!(
                    "UPDATE entities SET status = IF(status = 'new', 'paid', 'new') WHERE id = {}",
                    10 + i % 4
                ),
            )
            .await?;
        }
        let tag = ["a", "b", "c"][(i % 3) as usize];
        exec(
            &mut c,
            &format!(
                "INSERT INTO links (id, ref_id, tag) VALUES ({i}, {}, '{tag}')",
                10 + i % 4
            ),
        )
        .await?;
        if stop.load(Relaxed) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Fails unless the constant filters pruned at least `per_row` alias routes per changed row on
/// average: without them (an engine built without the filters, e.g. a stale test binary) the
/// rebuild checks that follow would fail with a misleading message.
fn ensure_pruned(d: &lazyview::metrics::Snapshot, per_row: u64) -> Result<()> {
    ensure!(
        d.rows_skipped >= per_row * d.rows_seen,
        "the constant filters pruned only {} alias routes for {} changed rows (expected at least {per_row} \
         per row): the engine under test does not apply them (a stale binary?)",
        d.rows_skipped,
        d.rows_seen
    );
    Ok(())
}
