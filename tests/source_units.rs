//! The unit cursor of a whole-view refresh (R-02): `mv::source_units` streams the source's units
//! from one server cursor, so refreshing a whole view walks the per-unit chunk path and never
//! holds the view in memory. These check what the cursor owes the chunk path it feeds: chunk
//! sizes, every unit of the view exactly once, in the order, and keys the chunk filter finds rows
//! with. MySQL family only (lv-super's ruling 7): a PostgreSQL source streams through its own
//! client, and there is no keyset-paging fallback here.

mod common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};
use common::*;
use lazyview::accumulate::UnitChunk;
use lazyview::analyze::ViewPlan;
use lazyview::catalog::Catalog;
use lazyview::mv;

/// Parent and child: the parent is the view's unit, and a chunk of units is what a refresh reads.
const DDL: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL)",
    "CREATE TABLE links (id INT PRIMARY KEY, entity_id INT NOT NULL, v INT NOT NULL, \
     CONSTRAINT fk_links_entity FOREIGN KEY (entity_id) REFERENCES entities(id))",
];
const FROM: &str = "links l JOIN entities e ON e.id = l.entity_id";
const JOIN: &str =
    "SELECT l.id, l.entity_id, e.grp FROM links l JOIN entities e ON e.id = l.entity_id";

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

/// The cursor hands out every unit of the view once, in the unit's order, in chunks of the
/// caller's size, and its keys are the keys the chunk path filters by.
#[test]
fn source_units_streams_every_unit_once_in_chunks() {
    scenario("mv_source_units_chunks", |ctx| async move {
        let db = ctx.new_db("mv_su_chunks").await?;
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        let (units, per) = (300usize, 3usize);
        seed(&mut c, units, per).await?;
        let plan = plan_of(&db, "mv_units_view", JOIN).await?;
        let layout = mv::probe(&mut c, &db.name, &plan).await?;
        assert_eq!(
            layout.unit_pos.len(),
            1,
            "the parent is the unit: {:?}",
            plan.unit_exprs
        );
        drop(c);

        // (The cursor is the connection's open result set: it streams on a connection of its own.)
        let m = lazyview::Metrics::default();
        let mut cur = db.conn().await?;
        let mut s = mv::source_units(&mut cur, &plan, &layout, 64, &m).await?;
        assert_eq!(
            m.source_queries.load(Ordering::Relaxed),
            1,
            "one source query starts a sweep, whatever the view's size"
        );
        let mut chunks: Vec<UnitChunk> = Vec::new();
        let mut seen: Vec<i64> = Vec::new();
        while let Some(ch) = s.next_chunk().await? {
            assert!(!ch.is_empty() && ch.len() <= 64, "chunk of {}", ch.len());
            // A single integer unit takes the fast path every chunk filter already uses, sorted
            // and deduplicated as `keys_chunk` builds it.
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
        assert!(s.is_exhausted(), "the source has no more units");
        assert!(
            s.next_chunk().await?.is_none(),
            "an exhausted cursor stays exhausted"
        );
        assert_eq!(s.units(), seen.len(), "every unit counted once");
        assert_eq!(
            chunks.len(),
            seen.len().div_ceil(64),
            "chunks of at most 64"
        );

        // Completeness: the units of a full read of the view, no more and no less. (The expected
        // list is the plan's own unit expressions: which column is the unit is the analyzer's
        // answer, and the cursor owes it the same list a whole read gives.)
        let mut o = db.conn().await?;
        let want: Vec<i64> = rows(
            &mut o,
            &format!(
                "SELECT DISTINCT {} FROM {FROM} ORDER BY 1",
                plan.unit_exprs.join(", ")
            ),
        )
        .await?
        .into_iter()
        .map(|r| r[0].clone().unwrap().parse().unwrap())
        .collect();
        assert_eq!(
            seen, want,
            "the units the cursor saw are the units of the view"
        );
        assert!(seen.len() > 64, "more units than one chunk");

        // The contract with the chunk path: a chunk the cursor handed out filters the source to
        // exactly its units' rows (its keys are the keys `read_source` binds).
        let ch = chunks.first().expect("a first chunk");
        let (f, p) = ch.filter(&plan.unit_exprs);
        let cnt: Vec<(i64,)> =
            typed_exec(&mut o, format!("SELECT COUNT(*) FROM {FROM} WHERE {f}"), p).await?;
        let want_rows = cnt.first().map(|r| r.0 as usize).unwrap_or(0);
        assert!(want_rows > 0, "the chunk's units have rows to read");
        let got = mv::read_source(&mut o, &plan, &layout, Some(ch), &m, false)
            .await?
            .len();
        assert_eq!(
            got, want_rows,
            "the cursor's keys find the rows of their units"
        );

        // A sweep that stops early closes its cursor, so the connection answers again: a stream
        // left open would hold the connection with it.
        let mut cur2 = db.conn().await?;
        let mut part = mv::source_units(&mut cur2, &plan, &layout, 8, &m).await?;
        let first = part.next_chunk().await?.context("a first chunk")?;
        assert_eq!(first.len(), 8, "the caller's chunk size");
        assert!(!part.is_exhausted(), "the source has rows left");
        part.finish().await?;
        assert_eq!(scalar(&mut cur2, "SELECT 1").await?.as_deref(), Some("1"));
        Ok(())
    });
}

/// A view whose rows are not a set of units has no unit list to stream: the cursor says so before
/// it spends a query, and the whole refresh that view gets stays the single read it has today.
#[test]
fn source_units_refuses_a_view_without_a_unit_list() {
    scenario("mv_source_units_refused", |ctx| async move {
        let db = ctx.new_db("mv_su_refused").await?;
        let mut c = db.conn().await?;
        for s in DDL {
            exec(&mut c, s).await?;
        }
        seed(&mut c, 5, 2).await?;
        let m = lazyview::Metrics::default();
        // The GLOBAL view: one row, no unit to list.
        let plan = plan_of(&db, "mv_units_all", "SELECT COUNT(*) AS n FROM links").await?;
        let layout = mv::probe(&mut c, &db.name, &plan).await?;
        let mut cur = db.conn().await?;
        let e = mv::source_units(&mut cur, &plan, &layout, 64, &m).await;
        assert!(e.is_err(), "no unit list: {}", plan.tracing());
        assert_eq!(
            m.source_queries.load(Ordering::Relaxed),
            0,
            "no query spent"
        );
        assert_eq!(scalar(&mut cur, "SELECT 1").await?.as_deref(), Some("1"));
        Ok(())
    });
}
