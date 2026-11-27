//! The unit cursor of the *target* and the vanished-unit delete (R-02, PR C): `mv::target_units`
//! streams the units the materialized table holds from one server cursor, and `mv::vanished` asks
//! the source once per chunk which of them it no longer answers for, so the whole-view refresh of a
//! unit-based view can delete the units that vanished from the source (#167's gap windows) without
//! ever holding the view. These check what the two owe the chunk path they feed: the target cursor's
//! keys are the keys `read_old` answers rows with, and the delete touches the units the source lacks
//! and nothing else. MySQL family targets, as the cursor's `Conn` is (lv-super's ruling 7): a
//! PostgreSQL target is read by its own client, and its cursor comes with the sweep's PostgreSQL leg.

mod common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};
use common::*;
use lazyview::accumulate::UnitChunk;
use lazyview::analyze::ViewPlan;
use lazyview::catalog::Catalog;
use lazyview::datum::{Datum, Key};
use lazyview::mv;
use lazyview::target::Kind;

/// Parent and child: the parent is the view's unit, and a chunk of units is what a refresh reads.
const DDL: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL)",
    "CREATE TABLE links (id INT PRIMARY KEY, entity_id INT NOT NULL, v INT NOT NULL, \
     CONSTRAINT fk_links_entity FOREIGN KEY (entity_id) REFERENCES entities(id))",
];
const JOIN: &str = "SELECT l.entity_id AS unit, COUNT(*) AS n, SUM(l.v) AS total \
                   FROM links l JOIN entities e ON e.id = l.entity_id GROUP BY l.entity_id";

/// No parameters, of the one type `typed_exec` will accept (a bare `Vec::new()` is ambiguous).
const NO_PARAMS: Vec<mysql_async::Value> = Vec::new();

/// The analyzer's plan of a view definition, against the live schema (as `lifecycle.rs` builds
/// it; the definition is named, as `analyze::parse_definition` wants it).
async fn plan_of(db: &Db, name: &str, select: &str) -> Result<ViewPlan> {
    let d = lazyview::analyze::parse_definition(&format!("CREATE VIEW {name} AS {select}"))?;
    let mut c = db.conn().await?;
    let mut cat = Catalog::default();
    let mut tables: HashMap<lazyview::catalog::TableRef, Arc<lazyview::catalog::TableInfo>> =
        HashMap::new();
    let mut queue: Vec<(lazyview::catalog::TableRef, usize)> =
        lazyview::analyzer::referenced_tables(&d.query, &db.name)
            .into_iter()
            .map(|t| (t, 0))
            .collect();
    let mariadb = db.flavor.contains("mariadb");
    while let Some((t, depth)) = queue.pop() {
        if tables.contains_key(&t) || depth > 8 {
            continue;
        }
        if let Some(info) = cat.get(&mut c, &t.0, &t.1, mariadb).await? {
            queue.extend(info.cascades.iter().map(|f| (f.parent.clone(), depth + 1)));
            tables.insert(t, info);
        }
    }
    c.disconnect().await.ok();
    lazyview::analyzer::plan(&d, &db.name, &tables).context("analyzer plan")
}

/// `units` parents of `per` child rows each, with ids from 1.
async fn seed(c: &mut mysql_async::Conn, units: usize, per: usize) -> Result<()> {
    let mut vals: Vec<String> = Vec::new();
    for id in 1..=units {
        vals.push(format!("({id}, {})", id % 7));
    }
    exec(
        c,
        &format!("INSERT INTO entities VALUES {}", vals.join(",")),
    )
    .await?;
    vals.clear();
    let mut n = 0usize;
    for id in 1..=units * per {
        vals.push(format!("({id}, {}, {id})", (id - 1) / per + 1));
        n += 1;
        if n.is_multiple_of(500) || n == units * per {
            exec(c, &format!("INSERT INTO links VALUES {}", vals.join(","))).await?;
            vals.clear();
        }
    }
    Ok(())
}

/// The target's write limits, from the server itself (the same call the engine makes).
async fn write_opts(c: &mut mysql_async::Conn) -> Result<mv::WriteOpts> {
    let v: Vec<(String, u64)> = typed_exec(
        c,
        "SELECT @@version, @@max_allowed_packet".to_string(),
        NO_PARAMS,
    )
    .await?;
    let (version, packet) = v.into_iter().next().context("no server version")?;
    Ok(mv::WriteOpts::for_server(
        Kind::Mysql,
        &version,
        packet,
        500,
        4 << 20,
    ))
}

/// The materialized table of `plan`, filled with the view's current rows. A test's view is small,
/// so one read fills it; the production path these are the blocks of never reads it that way.
async fn build_target(
    db: &Db,
    plan: &ViewPlan,
    layout: &mv::Layout,
    table: &str,
    m: &lazyview::Metrics,
) -> Result<mv::WriteOpts> {
    let mut c = db.conn().await?;
    let w = write_opts(&mut c).await?;
    exec(&mut c, &format!("DROP TABLE IF EXISTS `{table}`")).await?;
    exec(&mut c, &layout.create_sql(Kind::Mysql, &db.name, table)).await?;
    let rows = mv::read_source(&mut c, plan, layout, None, m, false).await?;
    let new = mv::prepare(layout, rows);
    mv::apply_prepared(&mut c, &db.name, table, layout, None, &new, &w, m).await?;
    Ok(w)
}

/// The rows of `table` whose unit is one of `ids`, as the test asks the server.
async fn target_rows_where(
    c: &mut mysql_async::Conn,
    table: &str,
    unit: &str,
    ids: &[i64],
) -> Result<usize> {
    let n: Vec<(i64,)> = typed_exec(
        c,
        format!(
            "SELECT COUNT(*) FROM `{table}` WHERE {unit} IN ({})",
            ids.iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        NO_PARAMS,
    )
    .await?;
    Ok(n.first().map(|r| r.0 as usize).unwrap_or(0))
}

/// The units of the test's grouped target, as bytes in the test's own order (the server's order is
/// the collation's, which is the point of the second test).
async fn units_of(c: &mut mysql_async::Conn, table: &str, col: &str) -> Result<Vec<String>> {
    let mut v: Vec<String> = rows(c, &format!("SELECT `{col}` FROM `{table}`"))
        .await?
        .into_iter()
        .map(|r| r[0].clone().unwrap_or_default())
        .collect();
    v.sort();
    Ok(v)
}

/// The rows of `table` whose unit is exactly `want`, byte for byte: the collation would match a
/// variant of it too, which is what these tests are about.
async fn exact(c: &mut mysql_async::Conn, table: &str, col: &str, want: &str) -> Result<usize> {
    let n: Vec<(i64,)> = typed_exec(
        c,
        format!("SELECT COUNT(*) FROM `{table}` WHERE BINARY `{col}` = BINARY '{want}'"),
        NO_PARAMS,
    )
    .await?;
    Ok(n.first().map(|r| r.0 as usize).unwrap_or(0))
}

/// The target's own units, streamed: every unit the view holds once, in the unit's order, in
/// chunks of the caller's size — and a chunk of them is a chunk `read_old` answers rows for, which
/// is what lets the vanished pass delete by it.
#[test]
fn target_units_streams_the_view_s_own_units_in_chunks() {
    scenario("mv_target_units_chunks", |ctx| async move {
        let db = ctx.new_db("mv_tu_chunks").await?;
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        let (units, per) = (300usize, 3usize);
        seed(&mut c, units, per).await?;
        let plan = plan_of(&db, "mv_tu_view", JOIN).await?;
        let layout = mv::probe(&mut c, &db.name, &plan).await?;
        assert_eq!(layout.unit_pos.len(), 1, "the parent is the unit");
        let table = "mv_tu_target";
        let m = lazyview::Metrics::default();
        build_target(&db, &plan, &layout, table, &m).await?;
        let mv_before = m.mv_queries.load(Ordering::Relaxed);
        drop(c);

        // (As the source cursor is, this one is the connection's open result set: it streams on a
        // connection of its own while the chunks are read and written on the others.)
        let mut cur = db.conn().await?;
        let mut s = mv::target_units(&mut cur, &db.name, table, &plan, &layout, 64, &m).await?;
        assert_eq!(
            m.mv_queries.load(Ordering::Relaxed),
            mv_before + 1,
            "one target query starts the pass, whatever the view's size"
        );
        let mut chunks: Vec<UnitChunk> = Vec::new();
        let mut seen: Vec<i64> = Vec::new();
        while let Some(ch) = s.next_chunk().await? {
            assert!(!ch.is_empty() && ch.len() <= 64, "chunk of {}", ch.len());
            let UnitChunk::Ints(v) = &ch else {
                panic!("an int unit comes back as Ints: {ch:?}");
            };
            assert!(
                v.windows(2).all(|w| w[0] < w[1]),
                "sorted, no repeats: {v:?}"
            );
            seen.extend(v.iter().copied());
            chunks.push(ch);
        }
        assert!(s.is_exhausted(), "the table has no more units");
        assert_eq!(s.units(), seen.len(), "every unit counted once");
        assert_eq!(
            chunks.len(),
            seen.len().div_ceil(64),
            "chunks of at most 64"
        );

        // Completeness: the units the table holds, no more and no less.
        let mut o = db.conn().await?;
        let unit = layout.unit_cols()[0].clone();
        let want: Vec<i64> = rows(
            &mut o,
            &format!("SELECT DISTINCT {unit} FROM `{table}` ORDER BY 1"),
        )
        .await?
        .into_iter()
        .map(|r| r[0].clone().unwrap().parse().unwrap())
        .collect();
        assert_eq!(
            seen, want,
            "the units the cursor saw are the units of the table"
        );
        assert_eq!(seen.len(), units, "every unit of the view is in the table");

        // The contract with the chunk path: a chunk this cursor handed out is a chunk `read_old`
        // reads the rows of — its keys are the keys the target filters by.
        let ch = chunks.first().expect("a first chunk");
        let ids: Vec<i64> = match ch {
            UnitChunk::Ints(v) => v.clone(),
            _ => unreachable!("an int unit comes back as Ints"),
        };
        let want = target_rows_where(&mut o, table, &unit, &ids).await?;
        assert!(want > 0, "the chunk's units have rows");
        let got = mv::read_old(&mut o, &db.name, table, &layout, Kind::Mysql, Some(ch), &m)
            .await?
            .len();
        assert_eq!(got, want, "the cursor's keys find the rows of their units");

        // A pass that stops early closes its cursor, so the connection answers again.
        let mut cur2 = db.conn().await?;
        let mut part = mv::target_units(&mut cur2, &db.name, table, &plan, &layout, 8, &m).await?;
        let first = part.next_chunk().await?.context("a first chunk")?;
        assert_eq!(first.len(), 8, "the caller's chunk size");
        assert!(!part.is_exhausted(), "the table has units left");
        part.finish().await?;
        assert_eq!(scalar(&mut cur2, "SELECT 1").await?.as_deref(), Some("1"));

        // A view whose rows are not a set of units has no target list either (see `streamable`):
        // the GLOBAL view has no unit to walk, and nothing is spent finding that out.
        let gplan = plan_of(&db, "mv_tu_all", "SELECT COUNT(*) AS n FROM links").await?;
        let glayout = mv::probe(&mut cur2, &db.name, &gplan).await?;
        let before = m.mv_queries.load(Ordering::Relaxed);
        let e = mv::target_units(&mut cur2, &db.name, table, &gplan, &glayout, 64, &m).await;
        assert!(e.is_err(), "no unit list to walk: {}", gplan.tracing());
        assert_eq!(
            m.mv_queries.load(Ordering::Relaxed),
            before,
            "no query spent"
        );
        Ok(())
    });
}

/// The vanished-unit delete: of a chunk of the target's units, only the ones the source has no row
/// for any more are deleted. The units the source answers for keep their rows — a unit it still has
/// but spells differently (a case variant under this case-insensitive collation) included — and the
/// read that decides costs one statement per chunk (lv-super's ruling 2).
#[test]
fn vanished_deletes_only_the_units_the_source_has_lost() {
    scenario("mv_target_units_vanished", |ctx| async move {
        let db = ctx.new_db("mv_tu_vanished").await?;
        let mut c = db.conn().await?;
        // The acceptance's neutral fixture: a text unit of the server's default, case-insensitive
        // collation, so the source's spelling of a unit and the view's need not be the same bytes.
        exec(
            &mut c,
            "CREATE TABLE entities (id INT PRIMARY KEY, name_ci VARCHAR(32) NOT NULL)",
        )
        .await?;
        exec(
            &mut c,
            "INSERT INTO entities VALUES (1,'Alpha'),(2,'Beta'),(3,'Gamma'),(4,'Delta'),\
             (5,'Alpha'),(6,'Beta'),(7,'Gamma'),(8,'Delta')",
        )
        .await?;
        let plan = plan_of(
            &db,
            "mv_tu_ci",
            "SELECT name_ci AS name, COUNT(*) AS n FROM entities GROUP BY name_ci",
        )
        .await?;
        let layout = mv::probe(&mut c, &db.name, &plan).await?;
        assert_eq!(layout.unit_pos.len(), 1, "the grouped column is the unit");
        let table = "mv_tu_ci_target";
        let m = lazyview::Metrics::default();
        let w = build_target(&db, &plan, &layout, table, &m).await?;
        assert_eq!(
            units_of(&mut c, table, "name").await?,
            vec!["Alpha", "Beta", "Delta", "Gamma"],
            "the view holds the four units, one row each"
        );

        // The source loses Gamma entirely, and re-spells Alpha in another case.
        exec(&mut c, "DELETE FROM entities WHERE id IN (3, 7)").await?;
        exec(&mut c, "UPDATE entities SET name_ci='alpha' WHERE id = 1").await?;
        exec(&mut c, "UPDATE entities SET name_ci='aLpHa' WHERE id = 5").await?;
        drop(c);

        let keys = |v: &[&str]| -> Vec<Key> {
            v.iter()
                .map(|s| Key::from([Datum::Bytes(s.as_bytes().to_vec())]))
                .collect()
        };
        let mut src = db.conn().await?;
        let mut tgt = db.conn().await?;
        let mut o = db.conn().await?;

        // A chunk the equality fast path settles: every answer is one of the chunk's own keys, so
        // a key with no answer has no source rows — and only those are handed out for deletion.
        let before = m.source_queries.load(Ordering::Relaxed);
        let ch = UnitChunk::Keys(keys(&["Beta", "Delta"]));
        let v = mv::vanished(&mut src, &plan, &layout, &ch, &m, false).await?;
        assert_eq!(v, mv::Vanished::Keys(Vec::new()), "both are still there");
        assert_eq!(
            m.source_queries.load(Ordering::Relaxed),
            before + 1,
            "one existence read decides a chunk, and it reads keys only"
        );
        exec(&mut o, "DELETE FROM entities WHERE name_ci = 'Delta'").await?;
        let ch = UnitChunk::Keys(keys(&["Beta", "Delta"]));
        let v = mv::vanished(&mut src, &plan, &layout, &ch, &m, false).await?;
        assert_eq!(
            v,
            mv::Vanished::Keys(vec![Key::from([Datum::Bytes(b"Delta".to_vec())])]),
            "the source is silent about Delta alone"
        );
        // The delete is the ordinary chunk path with nothing on the new side.
        let del = UnitChunk::Keys(vec![Key::from([Datum::Bytes(b"Delta".to_vec())])]);
        let empty = mv::prepare(&layout, Vec::new());
        let a = mv::apply_prepared(
            &mut tgt,
            &db.name,
            table,
            &layout,
            Some(&del),
            &empty,
            &w,
            &m,
        )
        .await?;
        assert_eq!(a.deleted, 1, "the Delta row goes");
        assert_eq!(a.inserted, 0, "a delete inserts nothing");
        assert_eq!(a.units_changed, 1, "one unit, the vanished one");
        assert_eq!(
            units_of(&mut o, table, "name").await?,
            vec!["Alpha", "Beta", "Gamma"],
            "Gamma was already gone in the earlier state; nothing else moved"
        );

        // The chunk with the case variant in it: the source answers for `Alpha` as `alpha`, and
        // only the server can tell those are the same unit. The pass says so instead of guessing —
        // a wrong guess here would delete a live unit's rows for good.
        let ch = UnitChunk::Keys(keys(&["Alpha", "Beta", "Gamma"]));
        let v = mv::vanished(&mut src, &plan, &layout, &ch, &m, false).await?;
        assert_eq!(
            v,
            mv::Vanished::Ambiguous,
            "an answer that is not the chunk's own spelling settles nothing"
        );
        // The caller's fallback for such a chunk: the ordinary chunk refresh, which reaches the same
        // end state with the server doing the comparison — the live unit keeps its rows (rewritten
        // to the source's own text) and the vanished one loses them in the diff.
        let rows = mv::read_source(&mut src, &plan, &layout, Some(&ch), &m, false).await?;
        let new = mv::prepare(&layout, rows);
        let a =
            mv::apply_prepared(&mut tgt, &db.name, table, &layout, Some(&ch), &new, &w, &m).await?;
        assert!(
            a.deleted > 0 && a.inserted > 0,
            "rewritten, not deleted: {a:?}"
        );
        assert_eq!(
            exact(&mut o, table, "name", "Alpha").await?,
            0,
            "the stale spelling is gone"
        );
        assert_eq!(
            exact(&mut o, table, "name", "alpha").await?
                + exact(&mut o, table, "name", "aLpHa").await?,
            1,
            "the unit the source still has is there in the source's own spelling"
        );
        assert_eq!(exact(&mut o, table, "name", "Beta").await?, 1, "untouched");
        assert!(
            units_of(&mut o, table, "name")
                .await?
                .iter()
                .all(|u| u.to_lowercase() != "gamma"),
            "the unit the source lost is not in the view"
        );
        Ok(())
    });
}

/// #331: parallel chunk writes to one target never deadlock each other. Every connection upserts
/// the same units with its own values, so each round's diff changes every row; the statements
/// of two connections meet on the same keys. A write transaction holding its row locks to the
/// COMMIT while another asks for them in a different order is `ERROR 1213`; the writes are
/// autocommit with sorted keys, so none of them ever fails (no retry here: `apply_prepared` has none).
#[test]
fn parallel_chunk_writes_to_the_same_rows_never_deadlock() {
    scenario("mv_parallel_writes_331", |ctx| async move {
        let db = ctx.new_db("mv_pw_331").await?;
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        let units = 400usize;
        seed(&mut c, units, 2).await?;
        let plan = plan_of(&db, "mv_pw_view", JOIN).await?;
        let layout = mv::probe(&mut c, &db.name, &plan).await?;
        let table = "mv_pw_target";
        let m = lazyview::Metrics::default();
        let w = build_target(&db, &plan, &layout, table, &m).await?;
        let base = mv::read_source(&mut c, &plan, &layout, None, &m, false).await?;
        assert_eq!(base.len(), units);
        let base_final = base.clone();
        let (writers, rounds) = (6usize, 25usize);
        let mut tasks = Vec::new();
        for t in 0..writers {
            let (db_name, layout) = (db.name.clone(), layout.clone());
            let mut conn = db.conn().await?;
            let base = base.clone();
            tasks.push(tokio::spawn(async move {
                let m = lazyview::Metrics::default();
                for r in 0..rounds {
                    // Column 1 is COUNT(*): a value of this writer and round, so the old rows
                    // (some other writer's) always differ.
                    let rows: Vec<Vec<mysql_async::Value>> = base
                        .iter()
                        .cloned()
                        .map(|mut row| {
                            row[1] = mysql_async::Value::Int((1 + t * rounds + r) as i64);
                            row
                        })
                        .collect();
                    let new = mv::prepare(&layout, rows);
                    mv::apply_prepared(&mut conn, &db_name, table, &layout, None, &new, &w, &m)
                        .await
                        .map_err(|e| anyhow::anyhow!("writer {t} round {r}: {e:#}"))?;
                }
                anyhow::Ok(())
            }));
        }
        for t in tasks {
            t.await??;
        }
        // Converged: the writers are done, one more pass with the source's own rows brings the
        // view to the source exactly, and a second pass finds nothing left to write.
        let new = mv::prepare(&layout, base_final);
        mv::apply_prepared(&mut c, &db.name, table, &layout, None, &new, &w, &m).await?;
        let again =
            mv::apply_prepared(&mut c, &db.name, table, &layout, None, &new, &w, &m).await?;
        assert_eq!(
            again.units_changed, 0,
            "the view equals the source: {again:?}"
        );
        assert_eq!(again.unchanged, units as u64);
        Ok(())
    });
}

/// #331: a chunk is written as separate autocommit statements, so a crash can land between them.
/// The resume position is saved only after the whole chunk, so the chunk is replayed: this is
/// the state a crash after the delete of the chunk's vanished units (and before its upserts) leaves,
/// and the replay must bring the view to the source exactly, once and for good.
#[test]
fn a_chunk_cut_between_its_statements_converges_on_replay() {
    scenario("mv_cut_chunk_331", |ctx| async move {
        let db = ctx.new_db("mv_cc_331").await?;
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        seed(&mut c, 50, 2).await?;
        let plan = plan_of(&db, "mv_cc_view", JOIN).await?;
        let layout = mv::probe(&mut c, &db.name, &plan).await?;
        let table = "mv_cc_target";
        let m = lazyview::Metrics::default();
        let w = build_target(&db, &plan, &layout, table, &m).await?;
        let unit = layout.unit_cols()[0].clone();
        // The source moves on: unit 3 loses its rows, unit 4's values change, unit 60 appears.
        exec(&mut c, "DELETE FROM links WHERE entity_id = 3").await?;
        exec(&mut c, "DELETE FROM entities WHERE id = 3").await?;
        exec(&mut c, "UPDATE links SET v = v + 1000 WHERE entity_id = 4").await?;
        exec(&mut c, "INSERT INTO entities VALUES (60, 1)").await?;
        exec(&mut c, "INSERT INTO links VALUES (1000, 60, 7)").await?;
        // The crash: the delete of unit 3's row went through, nothing else of the chunk did.
        exec(&mut c, &format!("DELETE FROM `{table}` WHERE {unit} = 3")).await?;
        // The replay: read the source, diff against what the target holds now, write.
        for pass in 0..2 {
            let rows = mv::read_source(&mut c, &plan, &layout, None, &m, false).await?;
            let new = mv::prepare(&layout, rows);
            let a =
                mv::apply_prepared(&mut c, &db.name, table, &layout, None, &new, &w, &m).await?;
            if pass == 1 {
                assert_eq!(a.units_changed, 0, "converged after one replay: {a:?}");
            }
        }
        let got = rows(
            &mut c,
            &format!("SELECT {unit}, COUNT(*) FROM `{table}` GROUP BY {unit} ORDER BY 1"),
        )
        .await?;
        assert_eq!(got.len(), 50, "units 1-2, 4-50 and 60");
        Ok(())
    });
}
