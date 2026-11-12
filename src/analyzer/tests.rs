//! Analyzer tests: differential against the block planner on the shared view corpus, metamorphic
//! CTE-vs-inline checks, and one test per taxonomy class (strategy and exact reason).

use std::collections::HashMap;
use std::sync::Arc;

use super::plan as analyzer_plan;
use crate::analyze::{parse_definition, Mode, ViewPlan};
use crate::analyzer::{analyze, ast, normalize, referenced_tables, validate, Options, Outcome};
use crate::catalog::test_catalog::{table, typed, wide_catalog, VIEW_CORPUS};
use crate::catalog::{Column, TableInfo, TableRef};

mod catalog_formulations;
mod cte_inlining;
mod differential;
mod expr_keys;
mod internal_views;
mod json_units;
mod naming;
mod semi_joins;
mod virtuals;

fn p(sql: &str) -> ViewPlan {
    let d = parse_definition(sql).unwrap();
    analyzer_plan(&d, "db", &wide_catalog()).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
}

fn reason(sql: &str) -> String {
    match p(sql).mode {
        Mode::Full(r) => r,
        m => panic!("{sql}: expected Full, got {m:?}"),
    }
}

/// Statements the analyzer plans differently from the block planner, with the expected new
/// outcome (`Ok(mode)` or `Err(message part)`). Everything else must be planned identically.
/// A chain of `n` CTEs, each joining the previous one to itself (2^n copies when inlined).
fn cte_chain(n: usize) -> String {
    let mut sql = "WITH t0 AS (SELECT id, customer_id, total FROM orders)".to_string();
    for k in 1..=n {
        sql.push_str(&format!(
            ", t{k} AS (SELECT a.id, a.customer_id, a.total + b.total AS total FROM t{} a JOIN t{} b ON b.id = a.id)",
            k - 1,
            k - 1
        ));
    }
    format!("CREATE VIEW v AS {sql} SELECT id, total FROM t{n}")
}

fn improvements() -> Vec<(&'static str, Result<Mode, &'static str>)> {
    vec![
        // DISTINCT = GROUP BY over the output columns.
        ("SELECT DISTINCT status FROM orders", Ok(Mode::Grouped)),
        ("SELECT DISTINCT * FROM orders", Ok(Mode::Grouped)),
        ("SELECT DISTINCT o.* FROM orders o", Ok(Mode::Grouped)),
        ("SELECT DISTINCT CONCAT(status, '\\\\n')", Ok(Mode::Grouped)),
        (
            "(a, b) AS SELECT DISTINCT * FROM regions",
            Ok(Mode::Grouped),
        ),
        (
            "SELECT DISTINCT r.*, c.id FROM regions r",
            Ok(Mode::Grouped),
        ),
        // DISTINCT over one aggregate row is a no-op.
        (
            "SELECT DISTINCT  COUNT( * ) , 'a' 'b' FROM orders",
            Ok(Mode::Global),
        ),
        // `*` over a USING join: the shared column once, then the rest (A4); DISTINCT over it
        // is a GROUP BY like any other.
        (
            "(a, b, c, d) AS SELECT DISTINCT * FROM regions JOIN customers USING (id)",
            Ok(Mode::Grouped),
        ),
        // The construct that really blocks it is named.
        (
            "SELECT DISTINCT id FROM orders NATURAL JOIN order_items",
            // No common columns: a cross join.
            Ok(Mode::Full(
                "table `order_items` contributes to every row (no join condition)".into(),
            )),
        ),
        // A leading RIGHT JOIN is a LEFT JOIN from the other side.
        (
            "SELECT o.id FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
            Ok(Mode::Spj),
        ),
        // IN over a primary key = inner join.
        (
            "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)",
            Ok(Mode::Spj),
        ),
        // A filter CTE is merged.
        (
            "WITH big AS (SELECT id FROM orders WHERE total > 5) SELECT id FROM big",
            Ok(Mode::Spj),
        ),
        // Top-N with a semi-join on the unit: one bounded recompute.
        (
            "WHERE c.id IN (SELECT customer_id FROM orders WHERE total > 5) ORDER BY region_id LIMIT 3",
            Ok(Mode::Global),
        ),
        // A group key on the NULL side: Full (the tuple-lineage view is an internal view).
        (
            "SELECT i.order_id, COUNT(*) FROM orders o LEFT JOIN order_items i ON i.order_id = o.id GROUP BY i.order_id",
            Ok(Mode::Full(
                "group key on the NULL side of a LEFT JOIN".into(),
            )),
        ),
        (
            "SELECT COALESCE(c.name, '?') n, COUNT(*) FROM orders o LEFT JOIN customers c",
            Ok(Mode::Full(
                "group key on the NULL side of a LEFT JOIN".into(),
            )),
        ),
        // The server rejects the definition (2 columns for 1 name).
        (
            "CREATE VIEW v (a) AS SELECT * FROM (SELECT id, name FROM regions) d",
            Err("column list has 1 names but SELECT returns 2 columns"),
        ),
    ]
}

/// What decides maintenance: mode, units, and per alias (table, columns used, how it is traced).
fn signature(v: &ViewPlan) -> String {
    let aliases: Vec<String> = v
        .aliases
        .iter()
        .map(|a| {
            format!(
                "{}:{:?}:{:?}:{}",
                a.table.name,
                a.referenced,
                a.membership,
                if a.direct_unit.is_some() {
                    "direct"
                } else {
                    "lookup"
                }
            )
        })
        .collect();
    format!(
        "{:?} units={} unique={} lineage={} cols={} aliases=[{}]",
        v.mode,
        v.unit_exprs.len(),
        v.unit_unique,
        v.lineage.len(),
        v.projection.len(),
        aliases.join(", ")
    )
}

fn outcome(sql: &str, cat: &HashMap<TableRef, Arc<TableInfo>>) -> super::Outcome {
    let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
    let opts = super::Options {
        internal_views: true,
        ..Default::default()
    };
    super::analyze(&d, "db", cat, &opts).unwrap_or_else(|e| panic!("{sql}: {e:#}"))
}

/// The table the engine would create for an internal view (its columns, `pk` = its unit).
fn internal_table(
    iv: &super::InternalView,
    cols: &[(&str, &str)],
    pk: &[usize],
) -> (TableRef, Arc<TableInfo>) {
    table(&iv.name, cols, pk)
}

/// Catalog for the formulation tests below: `item` is a cascading child of `entity`, `pair_item`
/// one of `pair` through a composite key; `attr` / `attr_n` reference `entity` without a foreign
/// key (NOT NULL / nullable column).
fn formulation_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
    use crate::catalog::{FkAction, ForeignKey};
    let mut cat = wide_catalog();
    let col = |n: &str, nullable: bool| {
        let mut c = Column::new(n, "int", "int", None, None, None, false);
        c.nullable = nullable;
        c
    };
    let fk = |parent: &str, child_cols: Vec<usize>, parent_cols: Vec<&str>| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols,
        parent_cols: parent_cols.into_iter().map(String::from).collect(),
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    for (name, cols, pk, cascades) in [
        (
            "entity",
            vec![col("id", false), col("v", false)],
            vec![0],
            vec![],
        ),
        (
            "attr",
            vec![col("id", false), col("ent_id", false), col("v", false)],
            vec![0],
            vec![],
        ),
        (
            "attr_n",
            vec![col("id", false), col("ent_id", true), col("v", false)],
            vec![0],
            vec![],
        ),
        (
            "link",
            vec![col("id", false), col("ent_id", false)],
            vec![0],
            vec![],
        ),
        (
            "link2",
            vec![col("id", false), col("link_id", false)],
            vec![0],
            vec![],
        ),
        (
            "link3",
            vec![col("id", false), col("link2_id", false)],
            vec![0],
            vec![],
        ),
        (
            "item",
            vec![col("id", false), col("ent_id", false), col("v", false)],
            vec![0],
            vec![fk("entity", vec![1], vec!["id"])],
        ),
        (
            "item_n",
            vec![col("id", false), col("ent_id", true), col("v", false)],
            vec![0],
            vec![fk("entity", vec![1], vec!["id"])],
        ),
        (
            "item_part",
            vec![col("id", false), col("item_id", false), col("v", false)],
            vec![0],
            vec![fk("item", vec![1], vec!["id"])],
        ),
        (
            "item_part_sub",
            vec![col("id", false), col("part_id", false), col("v", false)],
            vec![0],
            vec![fk("item_part", vec![1], vec!["id"])],
        ),
        (
            "pair",
            vec![col("a", false), col("b", false), col("v", false)],
            vec![0, 1],
            vec![],
        ),
        (
            "pair_item",
            vec![
                col("id", false),
                col("pa", false),
                col("pb", false),
                col("v", false),
            ],
            vec![0],
            vec![fk("pair", vec![1, 2], vec!["a", "b"])],
        ),
    ] {
        let (k, t) = typed(name, cols, &pk);
        let mut t = (*t).clone();
        t.cascades = cascades;
        cat.insert(k, Arc::new(t));
    }
    cat
}

#[path = "tests_rename.rs"]
mod rename;
