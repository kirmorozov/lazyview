//! The workload `bench_throughput` measures: the neutral tables the plan names (`entities`,
//! `items`, `links`), the five view shapes, a seeded write-plan generator, and the comparison that
//! keeps the harness from holding a whole view in memory.
//!
//! Everything here is test-side: no engine code runs per binlog event for it, it allocates nothing
//! on any hot path, and the copy set (`src/{analyze,analyzer,access,binlog,catalog,datum,intset,
//! fence,cascade,dedup,source,dialect}`) is untouched.
//!
//! The tables are the same three for every shape, so a rate difference between two rows of the
//! table is the view shape and not the schema. `items` and `links` are FK children of `entities`
//! with `ON DELETE CASCADE`, which is how the cascade shape makes the *server* feed the views.

use anyhow::{ensure, Result};

use super::engineproc::Rng;
use super::oracle::{Kind, ViewSpec};
use super::{exec_all, ident, rows, Conn};

/// One measured view group. One shape per engine run: the engine's counters are process-global
/// (the precedent is `tests/efficiency.rs:1-3`), so a shape measured beside another shape of the
/// same instance would report the other's rows.
pub struct Shape {
    pub name: &'static str,
    /// What it measures, for the printed line.
    pub what: &'static str,
    /// Definitions inserted before the engine starts.
    pub base: Vec<ViewSpec>,
    /// Definitions inserted once `base` are `ready`: they read an MV table (`Kind::Chained`).
    pub chained: Vec<ViewSpec>,
    /// Every view of the shape must be maintained incrementally, so `full_rebuilds` may not move
    /// (owner rule: an incremental view never falls back to a rebuild; a refresh is allowed).
    /// False only for the set-operation shape, whose `UNION ALL` view is a full-mode view today
    /// (`src/analyze/build.rs`) and whose whole refresh is the number that shape measures.
    pub incremental: bool,
    /// The write plan deletes parents, so the server's FK cascade reaches the views.
    pub cascade_deletes: bool,
}

/// The five shapes of the plan's section 1, over `entities` / `items` / `links`.
///
/// `targets` is the leg's target name (`None` = every view on the source). A chained view's
/// upstream lives on the source too, the way `tests/targets.rs:597-606` does it, because the
/// engine plans a chained view against the upstream MV on the source.
pub fn shapes(targets: Option<&str>) -> Vec<Shape> {
    let on = |v: ViewSpec| match targets {
        Some(t) => v.with_targets(t),
        None => v,
    };
    let upstream = |v: ViewSpec| match targets {
        Some(t) => v.with_targets(&format!("source,{t}")),
        None => v,
    };
    vec![
        Shape {
            name: "simple",
            what: "filter and projection of one table",
            base: vec![on(ViewSpec::new(
                "b_simple",
                Kind::Incremental,
                "SELECT id, cat, price, qty FROM items WHERE price > 10",
            ))],
            chained: Vec::new(),
            incremental: true,
            cascade_deletes: false,
        },
        Shape {
            name: "join",
            what: "select-project-join over all three tables",
            base: vec![on(ViewSpec::new(
                "b_join",
                Kind::Incremental,
                "SELECT e.id AS eid, e.region AS region, i.id AS iid, i.cat AS cat, i.price AS price, \
                 l.kind AS kind FROM entities e JOIN links l ON l.entity_id = e.id \
                 JOIN items i ON i.id = l.item_id",
            ))],
            chained: Vec::new(),
            incremental: true,
            cascade_deletes: false,
        },
        Shape {
            name: "aggregate",
            what: "GROUP BY plus a global aggregate",
            base: vec![
                on(ViewSpec::new(
                    "b_by_cat",
                    Kind::Incremental,
                    "SELECT cat, COUNT(*) AS n, SUM(qty) AS qty_sum, SUM(price) AS price_sum \
                     FROM items GROUP BY cat",
                )),
                on(ViewSpec::new(
                    "b_all",
                    Kind::Incremental,
                    "SELECT COUNT(*) AS n, SUM(qty) AS qty_sum, MAX(price) AS price_max FROM items",
                )),
            ],
            chained: Vec::new(),
            incremental: true,
            cascade_deletes: false,
        },
        Shape {
            name: "cascade",
            what: "chained view over an MV, fed by FK cascade deletes",
            base: vec![upstream(ViewSpec::new(
                "b_base",
                Kind::Incremental,
                "SELECT id, cat, price, qty FROM items WHERE qty > 100",
            ))],
            chained: vec![on(ViewSpec::new(
                "b_chained",
                Kind::Chained,
                "SELECT cat, COUNT(*) AS n, SUM(qty) AS qty_sum FROM b_base GROUP BY cat",
            ))],
            incremental: true,
            cascade_deletes: true,
        },
        Shape {
            name: "setop",
            what: "UNION ALL (a full-mode view: whole refresh per round)",
            base: vec![on(ViewSpec::new(
                "b_union",
                Kind::Full,
                "SELECT id, cat, price FROM items WHERE cat IN ('c0','c1') \
                 UNION ALL SELECT id, cat, price FROM items WHERE cat IN ('c2','c3')",
            ))],
            chained: Vec::new(),
            incremental: false,
            cascade_deletes: false,
        },
    ]
}

/// The schema: three tables, the second and third FK children of the first with `ON DELETE
/// CASCADE` (what the cascade shape drives), all with the neutral names the owner's rule asks for.
pub fn ddl() -> Vec<String> {
    vec![
        "CREATE TABLE entities (id BIGINT PRIMARY KEY, name VARCHAR(24) NOT NULL, \
         region VARCHAR(6) NOT NULL, score INT NOT NULL DEFAULT 0)"
            .to_string(),
        "CREATE TABLE items (id BIGINT PRIMARY KEY, entity_id BIGINT NOT NULL, cat VARCHAR(4) NOT NULL, \
         price DECIMAL(10,2) NOT NULL, qty INT NOT NULL, note VARCHAR(24) NULL, \
         CONSTRAINT fk_items_entity FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE)"
            .to_string(),
        "CREATE TABLE links (id BIGINT PRIMARY KEY, entity_id BIGINT NOT NULL, item_id BIGINT NOT NULL, \
         kind VARCHAR(6) NOT NULL, \
         CONSTRAINT fk_links_entity FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE, \
         CONSTRAINT fk_links_item FOREIGN KEY (item_id) REFERENCES items (id) ON DELETE CASCADE)"
            .to_string(),
    ]
}

pub async fn create_tables(c: &mut Conn) -> Result<()> {
    exec_all(c, &ddl()).await
}

/// A writer's whole window: the statements to run, in order, and the row operations they touch
/// (the printed budget). Deterministic in `(seed, phase, writer)`, so two same-seed runs write the
/// same rows — the comparability control the plan's section 1 asks for. `phase` is the warm-up (0)
/// or the window number: it moves the id block, so a later window writes new rows instead of
/// colliding with what an earlier one inserted.
pub struct WritePlan {
    pub stmts: Vec<String>,
    pub row_ops: usize,
}

/// Each `(phase, writer)` pair owns one `BLOCK` of ids: `items` at its start, `links` 30 M in,
/// `entities` 60 M in. Two writers and two windows therefore never collide, and no id is reused
/// after a delete.
const BLOCK: i64 = 100_000_000;
const CATS: [&str; 8] = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"];
const REGIONS: [&str; 6] = ["r1", "r2", "r3", "r4", "r5", "r6"];
const KINDS: [&str; 4] = ["k1", "k2", "k3", "k4"];

pub fn write_plan(
    seed: u64,
    phase: usize,
    writer: usize,
    rows: usize,
    cascade_deletes: bool,
) -> WritePlan {
    let mut rng =
        Rng::new(seed ^ (((phase * 8 + writer) as u64) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let base = ((phase * 8 + writer) as i64) * BLOCK;
    let (mut next_item, mut next_link, mut next_entity) =
        (base + 1, base + 30_000_001, base + 60_000_001);
    let mut stmts: Vec<String> = Vec::new();
    let mut row_ops = 0usize;
    let mut live_entities: Vec<i64> = Vec::new();
    let mut live_items: Vec<i64> = Vec::new();

    // Seed block: parents first, then the children that reference them (the FKs require it).
    let seed_rows = rows / 3;
    for _ in 0..(seed_rows / 25).max(1) {
        let ids: Vec<i64> = (0..25)
            .map(|_| {
                let id = next_entity;
                next_entity += 1;
                id
            })
            .collect();
        live_entities.extend_from_slice(&ids);
        stmts.push(insert(
            "entities",
            &["id", "name", "region", "score"],
            &ids.iter()
                .map(|id| {
                    format!(
                        "({id}, 'e{id}', '{}', {})",
                        REGIONS[rng.below(REGIONS.len())],
                        rng.below(1000)
                    )
                })
                .collect::<Vec<_>>(),
        ));
        row_ops += ids.len();
    }
    for _ in 0..(seed_rows / 25).max(1) {
        let ids: Vec<i64> = (0..25)
            .map(|_| {
                let id = next_item;
                next_item += 1;
                id
            })
            .collect();
        live_items.extend_from_slice(&ids);
        stmts.push(insert(
            "items",
            &["id", "entity_id", "cat", "price", "qty", "note"],
            &ids.iter()
                .map(|id| {
                    let e = live_entities[rng.below(live_entities.len())];
                    format!(
                        "({id}, {e}, '{}', {}, {}, 'n{id}')",
                        CATS[rng.below(CATS.len())],
                        money(&mut rng),
                        rng.below(500)
                    )
                })
                .collect::<Vec<_>>(),
        ));
        row_ops += ids.len();
    }
    for _ in 0..(seed_rows / 50).max(1) {
        let ids: Vec<i64> = (0..50)
            .map(|_| {
                let id = next_link;
                next_link += 1;
                id
            })
            .collect();
        stmts.push(insert(
            "links",
            &["id", "entity_id", "item_id", "kind"],
            &ids.iter()
                .map(|id| {
                    let i = live_items[rng.below(live_items.len())];
                    let e = live_entities[rng.below(live_entities.len())];
                    format!("({id}, {e}, {i}, '{}')", KINDS[rng.below(KINDS.len())])
                })
                .collect::<Vec<_>>(),
        ));
        row_ops += ids.len();
    }

    // Steady block: more inserts (the growth a live system has), updates of referenced and of
    // unreferenced columns, and deletes; the cascade shape also deletes parents.
    let mut left = rows.saturating_sub(row_ops);
    let mut turn = 0u32;
    while left > 0 {
        turn += 1;
        left = match turn % 10 {
            // two batches of new items and the links that point at them
            0..=3 => {
                let n = left.min(25);
                let ids: Vec<i64> = (0..n)
                    .map(|_| {
                        let id = next_item;
                        next_item += 1;
                        id
                    })
                    .collect();
                live_items.extend_from_slice(&ids);
                stmts.push(insert(
                    "items",
                    &["id", "entity_id", "cat", "price", "qty", "note"],
                    &ids.iter()
                        .map(|id| {
                            let e = live_entities[rng.below(live_entities.len())];
                            format!(
                                "({id}, {e}, '{}', {}, {}, 'n{id}')",
                                CATS[rng.below(CATS.len())],
                                money(&mut rng),
                                rng.below(500)
                            )
                        })
                        .collect::<Vec<_>>(),
                ));
                left - n
            }
            4 | 5 => {
                let n = left.min(25);
                let ids: Vec<i64> = (0..n)
                    .map(|_| {
                        let id = next_link;
                        next_link += 1;
                        id
                    })
                    .collect();
                stmts.push(insert(
                    "links",
                    &["id", "entity_id", "item_id", "kind"],
                    &ids.iter()
                        .map(|id| {
                            let i = live_items[rng.below(live_items.len())];
                            let e = live_entities[rng.below(live_entities.len())];
                            format!("({id}, {e}, {i}, '{}')", KINDS[rng.below(KINDS.len())])
                        })
                        .collect::<Vec<_>>(),
                ));
                left - n
            }
            // a column every view of every shape reads, and one no view reads
            6 | 7 => {
                let ids = pick(&mut rng, &live_items, 50);
                let col = if turn.is_multiple_of(2) {
                    "qty"
                } else {
                    "note"
                };
                stmts.push(format!(
                    "UPDATE items SET {col} = {} WHERE id IN ({})",
                    if col == "qty" {
                        (rng.below(50) as i64).to_string()
                    } else {
                        format!("'u{}'", rng.below(1000))
                    },
                    ids_csv(&ids)
                ));
                left - ids.len()
            }
            8 => {
                let ids = pick(&mut rng, &live_items, 25);
                stmts.push(format!(
                    "UPDATE items SET price = {} WHERE id IN ({})",
                    money(&mut rng),
                    ids_csv(&ids)
                ));
                left - ids.len()
            }
            _ => {
                // deletes: rows go away, and with them the view rows that referenced them
                let ids = pick(&mut rng, &live_items, 20);
                stmts.push(format!("DELETE FROM items WHERE id IN ({})", ids_csv(&ids)));
                live_items.retain(|i| !ids.contains(i));
                if cascade_deletes && !live_entities.is_empty() {
                    let es = pick(&mut rng, &live_entities, 5);
                    stmts.push(format!(
                        "DELETE FROM entities WHERE id IN ({})",
                        ids_csv(&es)
                    ));
                    live_entities.retain(|e| !es.contains(e));
                    row_ops += es.len();
                }
                left - ids.len()
            }
        };
        if live_items.is_empty() || live_entities.is_empty() {
            break; // nothing left to update or point at
        }
    }
    assert!(
        !stmts.is_empty(),
        "write plan for writer {writer} produced no statements"
    );
    WritePlan { stmts, row_ops }
}

/// `1,2,3` for an `IN (…)` list.
fn ids_csv(ids: &[i64]) -> String {
    ids.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn pick(rng: &mut Rng, from: &[i64], n: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(n.min(from.len()));
    for _ in 0..n.min(from.len()) {
        out.push(from[rng.below(from.len())]);
    }
    out
}

/// `0.<2 decimals>` in the range the shapes' predicates split on (`price > 10` and `cat IN (…)`).
fn money(rng: &mut Rng) -> String {
    format!("{}.{}", rng.below(200), rng.below(100))
}

fn insert(table: &str, cols: &[&str], values: &[String]) -> String {
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        ident(table),
        cols.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", "),
        values.join(", ")
    )
}

// ---------------------------------------------------------------------------------------------
// The comparison, done by the server
// ---------------------------------------------------------------------------------------------

/// The oracle for one view: a plain SQL view of the same SELECT on the source. `tests/common/
/// oracle.rs` does the same for the row-by-row suites; the bench needs only the two aggregates
/// below, so it creates the view itself and never reads a row of the view.
pub fn oracle_name(name: &str) -> String {
    format!("_bench_oracle_{name}")
}

pub async fn create_oracle(c: &mut Conn, spec: &ViewSpec) -> Result<()> {
    exec_all(
        c,
        &[format!(
            "CREATE VIEW {} AS {}",
            ident(&oracle_name(&spec.name)),
            spec.select
        )],
    )
    .await
}

/// Visible columns of a table or view of this schema, in declaration order.
pub async fn columns(c: &mut Conn, schema: &str, table: &str) -> Result<Vec<String>> {
    ensure!(
        schema
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            && table
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "unusable schema or table name: {schema}.{table}"
    );
    let rs = rows(
        c,
        &format!(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' \
             AND table_name = '{table}' AND column_name NOT LIKE '\\_lv\\_%' \
             ORDER BY ordinal_position"
        ),
    )
    .await?;
    Ok(rs
        .into_iter()
        .map(|r| r[0].clone().unwrap_or_default())
        .collect())
}

/// The two aggregates of one shape row, read out of what the server answered. Moved here verbatim
/// from `shape_of` so the row can be fed to it by a unit test; the way it reads a row it cannot
/// parse is the subject of #338, which the test beside this module shows is wrong.
fn shape_row(_table: &str, rs: &[Vec<Option<String>>]) -> Result<(i64, u64)> {
    let r = rs.first().and_then(|r| r.first()).cloned().flatten();
    let n: i64 = r.as_deref().unwrap_or("0").parse().unwrap_or(0);
    let h: u64 = rs
        .first()
        .and_then(|r| r.get(1).cloned().flatten())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    Ok((n, h))
}

/// `COUNT(*)` plus an order-independent checksum of the same columns, both computed inside the
/// server: exactly one row crosses the wire, so the harness never holds a view in memory.
async fn shape_of(c: &mut Conn, table: &str, cols: &[String]) -> Result<(i64, u64)> {
    let expr = cols
        .iter()
        .map(|col| format!("IFNULL(CAST({} AS CHAR), '~')", ident(col)))
        .collect::<Vec<_>>()
        .join(", ");
    let rs = rows(
        c,
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS('|', {expr}))), 0) FROM {}",
            ident(table)
        ),
    )
    .await?;
    shape_row(table, &rs)
}

/// One view of the source leg equals its oracle: same row count, same checksum. Called after every
/// measured window (the owner's rule that the views stay exact through anything the test does).
pub async fn compare_with_oracle(c: &mut Conn, schema: &str, spec: &ViewSpec) -> Result<()> {
    let cols = columns(c, schema, &oracle_name(&spec.name)).await?;
    ensure!(!cols.is_empty(), "oracle of {} has no columns", spec.name);
    let want = shape_of(c, &oracle_name(&spec.name), &cols).await?;
    let got = shape_of(c, &spec.name, &cols).await?;
    ensure!(
        want == got,
        "{}: the materialized view is not exact: rows {:?} vs oracle {:?} ({} columns compared)",
        spec.name,
        got,
        want,
        cols.len()
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The aggregate row, unit-proved (#338): a row the harness cannot read fails, it never measures 0
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{oracle_name, shape_row};

    /// The one answer `(0, 0)` is legitimate for: an empty table, where `COUNT(*)` really is 0 and
    /// the checksum of no rows really is 0. This case has to stay a measurement — the fix #338 asks
    /// for is about rows the harness cannot read, not about zeros it does read.
    #[test]
    fn a_real_empty_table_measures_zero() {
        assert_eq!(
            shape_row("b_simple", &[vec![Some("0".into()), Some("0".into())]]).unwrap(),
            (0, 0),
        );
        assert_eq!(
            shape_row("b_simple", &[vec![Some("6000".into()), Some("3735928559".into())]]).unwrap(),
            (6_000, 3_735_928_559),
        );
    }

    /// #338: no row at all used to come back as `(0, 0)` — the pair of an exact empty view — so the
    /// window this row belongs to was recorded as a view that equals its oracle. It must be an error
    /// naming the view and the value that is missing.
    #[test]
    fn no_row_at_all_fails_the_comparison() {
        let e = shape_row("b_simple", &[]).unwrap_err().to_string();
        assert!(e.contains("b_simple"), "the view is not named: {e}");
        assert!(e.contains("COUNT(*)"), "the missing value is not named: {e}");
    }

    /// The driver hands `NULL` to the harness as `None`, so a `NULL` count is the same unreadable
    /// row in a different shape; it used to be read as "zero rows".
    #[test]
    fn a_null_count_fails_the_comparison() {
        let e = shape_row("b_agg", &[vec![None, Some("42".into())]])
            .unwrap_err()
            .to_string();
        assert!(e.contains("b_agg") && e.contains("COUNT(*)"), "{e}");
        assert!(!e.contains("checksum"), "the wrong value is named: {e}");
    }

    /// …and so is a `NULL` checksum, which used to be read as the checksum of an empty view.
    #[test]
    fn a_null_checksum_fails_the_comparison() {
        let e = shape_row("b_agg", &[vec![Some("12".into()), None]])
            .unwrap_err()
            .to_string();
        assert!(e.contains("b_agg") && e.contains("checksum"), "{e}");
        assert!(!e.contains("COUNT(*)"), "the wrong value is named: {e}");
    }

    /// A row of the right shape whose values are not numbers — a driver that hands back text it
    /// should not have, a proxy answering for the server — is not a measurement either, and the row
    /// it was read out of has to appear in the message.
    #[test]
    fn unreadable_values_fail_the_comparison() {
        for (count, checksum) in [("twelve", "42"), ("", "42"), ("12", "0x2a"), ("12", "-1")] {
            let row = vec![Some(count.to_string()), Some(checksum.to_string())];
            let e = shape_row("b_join", &[row]).unwrap_err().to_string();
            assert!(
                e.contains("b_join"),
                "{count:?} + {checksum:?}: the view is not named: {e}"
            );
        }
    }

    /// A row that stopped after the count must fail on its own: reading only the count would let
    /// `(12, 0)` meet the oracle's `(12, 0)` and call a view exact on a checksum nobody read.
    #[test]
    fn a_row_short_of_a_checksum_fails_the_comparison() {
        let e = shape_row("b_cascade", &[vec![Some("12".into())]])
            .unwrap_err()
            .to_string();
        assert!(e.contains("b_cascade") && e.contains("checksum"), "{e}");
    }

    /// The pass the issue names as the danger: `compare_with_oracle` reads both sides through
    /// `shape_of`, so both aggregate rows unreadable meant `(0, 0)` on the view and `(0, 0)` on the
    /// oracle, `want == got`, and a green window that measured nothing. Each side fails by itself
    /// now, so the comparison is never reached.
    #[test]
    fn two_unreadable_sides_do_not_agree() {
        let view = shape_row("b_cascade", &[]);
        let oracle = shape_row(&oracle_name("b_cascade"), &[]);
        assert!(
            view.is_err() && oracle.is_err(),
            "both sides measured something: view {view:?}, oracle {oracle:?}"
        );
    }
}
