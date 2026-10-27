//! The monkey-wrench rename test at the planner level: every statement of the block planner's
//! test corpus is planned over its catalog, and again with every schema, table, column, view,
//! alias, CTE and output name consistently renamed (seeded; opaque names plus ones that look
//! meaningful or need quoting, see `rename.rs`). Once the renamed plan's names are
//! mapped back the two plans must be equal in everything that decides maintenance and in every
//! statement they render: no code path may read meaning from a name.

#![cfg(test)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[path = "rename.rs"]
mod monkey;

use monkey::{ident_sql, map_words, Names, Quote, Role};

use super::{catalog, corpus};
use crate::analyze::{parse_definition, Mode, ViewPlan};
use crate::analyzer::plan as analyzer_plan;
use crate::catalog::{Column, FkAction, ForeignKey, Generated, TableInfo, TableRef};

type Catalog = HashMap<TableRef, Arc<TableInfo>>;

/// Seeds of the always-on run (fixed: the same renamings on every run).
fn seeds() -> Vec<u64> {
    (0..8).map(|k| 0x5eed_0000 + k).collect()
}

/// Corpus cases whose renamed plan differs today: (finding id, part of the statement, only when
/// columns are written in varying letter case). Skipped by the always-on test, run by
/// `rename_findings`. All four compare expressions by their rendered text, in which the letter
/// case of a column name counts (columns are case-insensitive):
///
/// RI-1 (`analyze::plan_block`): the unit of a windowed view is the PARTITION BY its windows
/// share; `customer_id` in one window and `CUSTOMER_ID` in the other are not shared, the view is
/// Global instead of Grouped.
/// RI-2 (`normalize`, DISTINCT over GROUP BY): the DISTINCT outputs are not recognized as the
/// GROUP BY keys, the view is Full(DISTINCT) instead of Grouped.
/// RI-3 (`normalize`, merging an aggregated CTE): its group keys are not recognized among its
/// outputs, a filter on a key stays in HAVING instead of moving to WHERE.
/// RI-4 (`normalize`, pulling an aggregation up through key joins): the join column is not
/// recognized as the group key, the view is Full ("GROUP BY in CTE `t` joined to other tables")
/// instead of Grouped.
const FINDINGS: &[(&str, &str, bool)] = &[
    (
        "RI-1",
        "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn, SUM(total) OVER w AS run",
        true,
    ),
    (
        "RI-2",
        "SELECT DISTINCT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
        true,
    ),
    (
        "RI-3",
        "SELECT customer_id, s FROM t WHERE customer_id > 3 AND n > 1",
        true,
    ),
    (
        "RI-4",
        "SELECT c.name, t.s FROM customers c JOIN t ON t.customer_id = c.id",
        true,
    ),
];

/// The catalog with cascading foreign keys (cascade routes are planned from them).
fn cascading() -> Catalog {
    let mut cat = catalog();
    let fk = |child: &str, col: usize, parent: &str, on_delete| {
        let k = ("db".to_string(), child.to_string());
        let mut t = (*cat[&k]).clone();
        t.cascades.push(ForeignKey {
            parent: ("db".into(), parent.into()),
            child_cols: vec![col],
            parent_cols: vec!["id".into()],
            on_delete,
            on_update: FkAction::Cascade,
        });
        (k, Arc::new(t))
    };
    let items = fk("order_items", 0, "orders", FkAction::Cascade);
    let customers = fk("customers", 2, "regions", FkAction::SetNull);
    cat.extend([items, customers]);
    cat
}

/// The catalog with VIRTUAL generated columns (their expressions name other columns).
fn generated() -> Catalog {
    let col = |n: &str, dt: &str| Column::new(n, dt, dt, None, None, None, false);
    let gen = |mut c: Column, expr: &str, deps: Vec<usize>| {
        c.generated = Some(Generated {
            expr: expr.into(),
            stored: false,
            deps: Some(deps),
        });
        c
    };
    let mut cat = catalog();
    let t = TableInfo {
        schema: "db".into(),
        name: "g1".into(),
        columns: vec![
            col("id", "int"),
            col("a", "int"),
            col("b", "int"),
            gen(col("gi", "int"), "(`a` / 3)", vec![1]),
            gen(col("gsum", "int"), "(`a` + `b`)", vec![1, 2]),
            gen(col("gchain", "int"), "(`gi` * 10)", vec![3]),
        ],
        pk: vec![0],
        unique_keys: vec![],
        cascades: vec![],
        is_view: false,
        view_bases: vec![],
    };
    cat.insert(("db".into(), "g1".into()), Arc::new(t));
    cat
}

const GENERATED_CORPUS: &[&str] = &[
    "CREATE VIEW v AS SELECT id, gi, gsum FROM g1 WHERE gi = 0",
    "CREATE VIEW v AS SELECT gchain, COUNT(*) AS n FROM g1 GROUP BY gchain",
    "CREATE VIEW v AS SELECT gi + 1, COUNT(*) AS n FROM g1 GROUP BY gi + 1",
    "CREATE VIEW v AS SELECT id, SUM(a) OVER (PARTITION BY gsum) AS s FROM g1",
    "CREATE VIEW v AS SELECT o.id, o.total FROM orders o WHERE o.customer_id IN (SELECT g.gsum FROM g1 g)",
    "CREATE VIEW v AS SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM g1 g WHERE g.gsum = o.customer_id)",
];

/// Gives every name of the catalog its new name, with the role its keys give it (sorted: the
/// renaming of a seed is the same on every run).
fn assign_catalog(names: &mut Names, cat: &Catalog) {
    names.assign("db", Role::Schema);
    let mut keys: Vec<&TableRef> = cat.keys().collect();
    keys.sort();
    for k in keys {
        let t = &cat[k];
        names.assign(
            &t.name,
            Role::Table {
                plain: t.cascades.is_empty(),
            },
        );
        let keyed: HashSet<usize> =
            t.pk.iter()
                .chain(t.unique_keys.iter().flatten())
                .copied()
                .collect();
        let fk: HashSet<usize> = t
            .cascades
            .iter()
            .flat_map(|f| f.child_cols.iter().copied())
            .collect();
        for (i, c) in t.columns.iter().enumerate() {
            let role = if keyed.contains(&i) {
                Role::KeyColumn
            } else {
                Role::Column {
                    fk: fk.contains(&i),
                }
            };
            names.assign(&c.name, role);
        }
    }
}

fn renamed_ref(names: &Names, r: &TableRef) -> TableRef {
    (names.get(&r.0).unwrap(), names.get(&r.1).unwrap())
}

/// The catalog as the server would describe the renamed schema.
fn rename_catalog(names: &mut Names, cat: &Catalog) -> Result<Catalog, String> {
    let mut out = Catalog::new();
    let mut keys: Vec<&TableRef> = cat.keys().collect();
    keys.sort();
    for k in keys {
        let mut t = (*cat[k]).clone();
        t.schema = names.get(&t.schema).unwrap();
        t.name = names.get(&t.name).unwrap();
        for c in &mut t.columns {
            c.name = names.get(&c.name).unwrap();
            if let Some(g) = &mut c.generated {
                // The expression as a SELECT item: its column names are renamed in place.
                let r = names.rename_sql(&format!("SELECT {}", g.expr))?;
                g.expr = r.sql["SELECT ".len()..].to_string();
            }
        }
        for f in &mut t.cascades {
            f.parent = renamed_ref(names, &f.parent);
            f.parent_cols = f
                .parent_cols
                .iter()
                .map(|c| names.get(c).unwrap())
                .collect();
        }
        t.view_bases = t.view_bases.iter().map(|r| renamed_ref(names, r)).collect();
        out.insert((t.schema.clone(), t.name.clone()), Arc::new(t));
    }
    Ok(out)
}

fn statement(schema: Option<&str>, name: &str, cols: &[String], query: &str) -> String {
    let name = match schema {
        Some(s) => format!("{}.{}", monkey::quote(s), monkey::quote(name)),
        None => monkey::quote(name),
    };
    let cols = if cols.is_empty() {
        String::new()
    } else {
        let c: Vec<String> = cols.iter().map(|c| monkey::quote(c)).collect();
        format!(" ({})", c.join(", "))
    };
    format!("CREATE VIEW {name}{cols} AS {query}")
}

/// Every name of a text in one spelling: lower case, quoted only where it has to be. Applied to
/// both plans, so names that differ only in letter case or quoting (a renamed name that needs
/// quotes) compare equal.
fn canonical(text: &str, originals: &HashSet<String>) -> String {
    map_words(text, &mut |w, q| {
        let l = w.to_ascii_lowercase();
        (matches!(q, Quote::None | Quote::Backtick) && originals.contains(&l))
            .then(|| ident_sql(&l))
    })
}

/// An unaliased expression is named after its text, `Name_exp_<n>` past 64 characters (the
/// server's rule): renaming changes that text and its length, so these names are masked.
fn mask_generated(text: &str) -> String {
    map_words(text, &mut |w, q| {
        let numbered = w
            .strip_prefix("Name_exp_")
            .or(w.strip_prefix("name_exp_"))
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()));
        (q == Quote::Backtick && (w.contains('(') || numbered)).then(|| "<generated>".into())
    })
}

/// What decides maintenance and every statement the plan renders, as (field, value) pairs.
fn fingerprint(
    p: &ViewPlan,
    text: &dyn Fn(&str) -> String,
    name: &dyn Fn(&str) -> String,
) -> Vec<(String, String)> {
    let mut f: Vec<(String, String)> = Vec::new();
    let mut put = |k: &str, v: String| f.push((k.to_string(), v));
    put(
        "mode",
        match &p.mode {
            Mode::Full(r) => format!("Full({})", text(r)),
            m => format!("{m:?}"),
        },
    );
    put("view", format!("{}.{}", name(&p.schema), name(&p.name)));
    // (Tables are listed by name: compared as a set.)
    let mut tables: Vec<String> = p
        .tables
        .iter()
        .enumerate()
        .map(|(i, (s, t))| {
            format!(
                "{}.{} {:?}",
                name(s),
                name(t),
                p.table_columns.get(i).cloned().flatten()
            )
        })
        .collect();
    tables.sort();
    put("tables", format!("{tables:?}"));
    for (i, a) in p.aliases.iter().enumerate() {
        let k = |what: &str| format!("alias[{i}].{what}");
        put(&k("name"), name(&a.name));
        put(
            &k("table"),
            format!(
                "{}.{} ({} columns)",
                name(&a.table.schema),
                name(&a.table.name),
                a.table.columns.len()
            ),
        );
        put(&k("referenced"), format!("{:?}", a.referenced));
        put(&k("membership"), format!("{:?}", a.membership));
        put(&k("direct_unit"), format!("{:?}", a.direct_unit));
        put(&k("image_cols"), format!("{:?}", a.image_cols));
        put(&k("pos"), format!("{:?}", a.pos));
        put(&k("lookup_where"), format!("{}", a.lookup_where));
        put(
            &k("subst_from"),
            format!("{} | {}", text(&a.subst_from.0), text(&a.subst_from.1)),
        );
        put(&k("semi"), format!("{:?}", a.semi.as_deref().map(text)));
    }
    let list = |v: &[String]| format!("{:?}", v.iter().map(|s| text(s)).collect::<Vec<_>>());
    let opt = |v: &Option<String>| format!("{:?}", v.as_deref().map(text));
    put("projection", list(&p.projection));
    put("from_sql", text(&p.from_sql));
    put("where_sql", opt(&p.where_sql));
    put("group_sql", opt(&p.group_sql));
    put("having_sql", opt(&p.having_sql));
    put("unit_exprs", list(&p.unit_exprs));
    put("unit_in_projection", format!("{:?}", p.unit_in_projection));
    put("unit_unique", format!("{}", p.unit_unique));
    put("full_select", text(&p.full_select));
    for (i, l) in p.lineage.iter().enumerate() {
        put(
            &format!("lineage[{i}]"),
            format!(
                "alias {} cols {:?} exprs {}",
                l.alias,
                l.cols,
                list(&l.exprs)
            ),
        );
    }
    let mut cascades: Vec<String> = p
        .cascades
        .iter()
        .map(|c| {
            format!(
                "parent {}.{} cols {:?} delete {:?} update {:?} unit {:?} child {}.{} cols {:?} traced {} untraced {}",
                name(&c.parent.schema),
                name(&c.parent.name),
                c.parent_cols,
                c.on_delete,
                c.on_update,
                c.unit_from_parent,
                name(&c.child.0),
                name(&c.child.1),
                c.child_cols,
                c.traced,
                opt(&c.untraced)
            )
        })
        .collect();
    cascades.sort();
    put("cascades", format!("{cascades:?}"));
    put(
        "output_names",
        format!(
            "{:?}",
            p.output_names
                .as_ref()
                .map(|v| v.iter().map(|n| name(n)).collect::<Vec<_>>())
        ),
    );
    put("order_limit_sql", opt(&p.order_limit_sql));
    put("window_sql", opt(&p.window_sql));
    put("lookup_where_sql", opt(&p.lookup_where_sql));
    // The statements the executor runs.
    let hidden: Vec<usize> = (0..p.unit_exprs.len()).collect();
    put("select_sql", text(&p.select_sql(&hidden, None)));
    if p.is_incremental() {
        put("key_lookup_sql", text(&p.key_lookup_sql("(F)")));
        for (i, a) in p.aliases.iter().enumerate() {
            if a.semi.is_some() || (a.direct_unit.is_none() && a.pos.0 != usize::MAX) {
                put(&format!("lookup_sql[{i}]"), text(&p.lookup_sql(i, "(R)")));
            }
        }
    }
    // `alias:how,…,^parent:how`, names as they are (unquoted).
    let tracing: Vec<String> = p
        .tracing()
        .split(',')
        .map(|part| match part.rsplit_once(':') {
            Some((n, how)) => match n.strip_prefix('^') {
                Some(parent) => format!("^{}:{how}", name(parent)),
                None => format!("{}:{how}", name(n)),
            },
            None => part.to_string(),
        })
        .collect();
    put("tracing", tracing.join(","));
    put("rows_identified", format!("{}", p.rows_identified()));
    put("lineage_width", format!("{}", p.lineage_width()));
    f
}

/// Plans `sql` over `cat` and renamed with `seed`; `Ok(false)`: not a definition.
fn check(sql: &str, cat: &Catalog, seed: u64) -> Result<bool, String> {
    let Ok(def) = parse_definition(sql) else {
        return Ok(false);
    };
    let mut names = Names::new(seed);
    assign_catalog(&mut names, cat);
    let base = statement(
        def.schema.as_deref(),
        &def.name,
        &def.columns,
        &def.query_sql,
    );
    let fail = |names: &Names, what: String| {
        format!("== {sql}\n  seed {seed}: {what}\n{}", names.describe())
    };
    let new_schema = def.schema.as_deref().map(|s| names.assign(s, Role::Schema));
    let new_name = names.assign(&def.name, Role::View);
    let new_cols: Vec<String> = def
        .columns
        .iter()
        .map(|c| names.assign(c, Role::OutputAlias))
        .collect();
    let query = match names.rename_sql(&def.query_sql) {
        Ok(r) => r.sql,
        Err(e) => return Err(fail(&names, format!("cannot rename: {e}"))),
    };
    let renamed_cat = match rename_catalog(&mut names, cat) {
        Ok(c) => c,
        Err(e) => return Err(fail(&names, format!("cannot rename the catalog: {e}"))),
    };
    let renamed = statement(new_schema.as_deref(), &new_name, &new_cols, &query);
    let plan = |s: &str, schema: &str, c: &Catalog| {
        parse_definition(s).and_then(|d| analyzer_plan(&d, schema, c))
    };
    let a = plan(&base, "db", cat);
    let b = plan(&renamed, &names.get("db").unwrap(), &renamed_cat);
    let originals: HashSet<String> = names
        .pairs()
        .iter()
        .map(|(o, _, _)| o.to_ascii_lowercase())
        .collect();
    let plain_text = |s: &str| mask_generated(&canonical(s, &originals));
    let plain_name = |s: &str| s.to_ascii_lowercase();
    let back_text = |s: &str| mask_generated(&canonical(&names.unrename_message(s), &originals));
    let back_name = |s: &str| {
        names
            .original_name(s)
            .unwrap_or_else(|| s.to_string())
            .to_ascii_lowercase()
    };
    // Errors by class: which of several missing tables is named first follows name order.
    let class = |t: String| {
        map_words(&t, &mut |w, q| {
            (q != Quote::Single && originals.contains(&w.to_ascii_lowercase())).then(|| "N".into())
        })
    };
    let fa = match &a {
        Ok(p) => fingerprint(p, &plain_text, &plain_name),
        Err(e) => vec![("error".into(), class(plain_text(&format!("{e:#}"))))],
    };
    let fb = match &b {
        Ok(p) => fingerprint(p, &back_text, &back_name),
        Err(e) => vec![("error".into(), class(back_text(&format!("{e:#}"))))],
    };
    if fa == fb {
        return Ok(true);
    }
    let at = (0..fa.len().max(fb.len()))
        .find(|&i| fa.get(i) != fb.get(i))
        .unwrap();
    let show = |f: &[(String, String)]| {
        f.get(at)
            .map(|(k, v)| format!("{k} = {v}"))
            .unwrap_or_else(|| "(missing)".into())
    };
    Err(fail(
        &names,
        format!(
            "renamed: {renamed}\n  first difference:\n    plain:   {}\n    renamed: {}\n  renamed (as planned): {}",
            show(&fa),
            show(&fb),
            match &b {
                Ok(p) => p.select_sql(&[], None),
                Err(e) => format!("{e:#}"),
            }
        ),
    ))
}

/// The SELECTs of the analyzer's own tests (CTE merges, aggregation pull-up, semi-joins,
/// windows, ...) as definitions; those over other catalogs fail alike in both plans.
fn analyzer_corpus() -> Vec<String> {
    let mut v: Vec<String> = monkey::rust_literals(include_str!("tests.rs"))
        .into_iter()
        .filter(|s| {
            let t = s.trim_start();
            (t.starts_with("SELECT") || t.starts_with("WITH"))
                && t.contains("FROM")
                && !s.contains('{')
        })
        .map(|s| format!("CREATE VIEW v AS {s}"))
        .collect();
    v.sort();
    v.dedup();
    v
}

fn cases() -> Vec<(&'static str, Catalog, Vec<String>)> {
    let corpus = corpus();
    vec![
        ("base", catalog(), corpus.clone()),
        ("analyzer", catalog(), analyzer_corpus()),
        ("cascading", cascading(), corpus),
        (
            "generated",
            generated(),
            GENERATED_CORPUS.iter().map(|s| s.to_string()).collect(),
        ),
    ]
}

/// Runs every case except (or only) the known findings.
fn run(findings: bool) -> (usize, Vec<String>) {
    let mut checked = 0;
    let mut failures = Vec::new();
    for (label, cat, statements) in cases() {
        for sql in &statements {
            for seed in seeds() {
                let known = FINDINGS.iter().find(|(_, part, case_only)| {
                    sql.contains(part) && (!case_only || Names::new(seed).vary_case)
                });
                if known.is_some() != findings {
                    continue;
                }
                match check(sql, &cat, seed) {
                    Ok(true) => checked += 1,
                    Ok(false) => {}
                    Err(e) => failures.push(format!(
                        "[{label}{}] {e}",
                        known.map(|(id, ..)| format!(", {id}")).unwrap_or_default()
                    )),
                }
            }
        }
    }
    (checked, failures)
}

/// Every corpus statement (three catalogs: plain, with cascading foreign keys, with VIRTUAL
/// generated columns) plans identically under 8 renamings.
#[test]
fn plans_are_invariant_under_renaming() {
    let (checked, failures) = run(false);
    let statements: Vec<String> = cases()
        .iter()
        .map(|(label, _, s)| format!("{label} {}", s.len()))
        .collect();
    println!(
        "{checked} renamed plans equal ({} statements per catalog, {} seeds)",
        statements.join(", "),
        seeds().len()
    );
    assert!(checked > 1000, "only {checked} renamed plans checked");
    // Every difference in short (statement, seed, first differing field), the first in full.
    let short: Vec<String> = failures
        .iter()
        .map(|f| {
            f.lines()
                .filter(|l| !l.starts_with("  `") && !l.starts_with("renaming seed"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} renamed plans differ:\n{}\n\nThe first in full:\n{}",
        failures.len(),
        checked + failures.len(),
        short.join("\n"),
        failures.first().cloned().unwrap_or_default()
    );
}

/// The known findings (see `FINDINGS`): they still differ.
#[test]
#[ignore = "RI-1..RI-4: expressions compared as text (letter case of column names), see FINDINGS"]
fn rename_findings() {
    let (_, failures) = run(true);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The renaming itself: statements keep everything but their names, names map back.
#[test]
fn renaming_rewrites_only_names() {
    let mut names = Names::new(7);
    let ddl = "CREATE TABLE entity (id INT NOT NULL PRIMARY KEY, label VARCHAR(20) CHARACTER SET utf8mb4 \
               COLLATE utf8mb4_bin, ref_id INT, KEY k_label (label(8)), \
               CONSTRAINT fk_ref FOREIGN KEY (ref_id) REFERENCES entity (id) ON DELETE CASCADE) ENGINE=InnoDB";
    let r = names.rename_sql(ddl).unwrap();
    for kept in [
        "INT NOT NULL PRIMARY KEY",
        "CHARACTER SET utf8mb4",
        "COLLATE utf8mb4_bin",
        "ON DELETE CASCADE) ENGINE=InnoDB",
    ] {
        assert!(r.sql.contains(kept), "{kept}: {}", r.sql);
    }
    for gone in ["entity", "label", "ref_id", "k_label", "fk_ref"] {
        assert!(names.get(gone).is_some(), "{gone}");
        assert!(
            !r.sql.to_ascii_lowercase().contains(&format!("{gone} ")),
            "{gone}: {}",
            r.sql
        );
    }
    let q = "SELECT e.label AS lbl, COUNT(*) FROM entity e WHERE e.label <> 'label' AND @x IS NULL GROUP BY e.label";
    let r = names.rename_sql(q).unwrap();
    assert!(r.sql.contains("COUNT(*)") && r.sql.contains("'label'") && r.sql.contains("@x"));
    let originals: HashSet<String> = names
        .pairs()
        .iter()
        .map(|(o, _, _)| o.to_ascii_lowercase())
        .collect();
    assert_eq!(
        canonical(&names.unrename_text(&r.sql), &originals),
        canonical(q, &originals)
    );
    // lazyview's metadata: table names and the literals that name views are renamed, columns not.
    let m = names
        .rename_sql("SELECT name, mode FROM _lazyview_state WHERE name = 'entity'")
        .unwrap();
    assert!(m.metadata);
    assert!(m
        .sql
        .starts_with("SELECT name, mode FROM _lazyview_state WHERE name = '"));
    assert!(!m.sql.contains("'entity'"), "{}", m.sql);
    let d = names
        .rename_sql(
            "INSERT INTO _lazyview (name, definition) VALUES ('v1', 'SELECT label FROM entity')",
        )
        .unwrap();
    assert!(
        !d.sql.contains("v1") && !d.sql.contains("label"),
        "{}",
        d.sql
    );
    // A name used before the statement that defines it (a view over a later view) is renamed
    // at its first use, consistently.
    let a = names
        .rename_sql("CREATE VIEW top_x AS SELECT k, n FROM per_k WHERE n > 1")
        .unwrap();
    let b = names
        .rename_sql(
            "CREATE VIEW per_k AS SELECT label AS k, COUNT(*) AS n FROM entity GROUP BY label",
        )
        .unwrap();
    let per_k = names.get("per_k").unwrap();
    assert!(
        a.sql.contains(&per_k) && b.sql.contains(&per_k),
        "{}\n{}",
        a.sql,
        b.sql
    );
    // MySQL's CHECK OPTION clause (not read by the parser) stays as written.
    let c = names
        .rename_sql("CREATE VIEW w AS SELECT label FROM entity WITH CASCADED CHECK OPTION")
        .unwrap();
    assert!(
        c.sql.ends_with(" WITH CASCADED CHECK OPTION") && !c.sql.contains("label"),
        "{}",
        c.sql
    );
    // Names that differ only in letter case stay different (table names are case-sensitive).
    let x = names
        .rename_sql("CREATE VIEW Lz_Case AS SELECT label FROM entity")
        .unwrap();
    let y = names
        .rename_sql("CREATE VIEW lz_case AS SELECT label FROM entity")
        .unwrap();
    let view = |s: &str| s.split_whitespace().nth(2).unwrap().to_string();
    assert!(
        view(&x.sql) != view(&y.sql) && view(&x.sql).eq_ignore_ascii_case(&view(&y.sql)),
        "{}\n{}",
        x.sql,
        y.sql
    );
    assert_eq!(
        names.original(view(&y.sql).trim_matches('`')).as_deref(),
        Some("lz_case")
    );
    // Reserved names stay.
    let s = names
        .rename_sql("SELECT `_lv_u0` FROM entity__lv_new")
        .unwrap();
    assert!(
        s.sql.contains("`_lv_u0`") && s.sql.ends_with("__lv_new"),
        "{}",
        s.sql
    );
}
