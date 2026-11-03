//! Whether a re-planned view reads exactly what its live plan reads, by column identity rather
//! than position (see `Engine::swap_in_place`). A source DDL that leaves those reads alone (an
//! added index, an unrelated column, a column moved, an online schema change's table swap) then
//! needs no rebuild: only the column positions the binlog rows are decoded with change.
//!
//! Every plan and catalog struct is destructured without `..`: a field added to one of them does
//! not compile here until it is compared, so two plans that differ in it never look the same.

use std::collections::BTreeSet;

use crate::analyze::{AliasPlan, CascadeRoute, ConstFilter, Mode, PartialUnits, Pinned, ViewPlan};
use crate::catalog::{Column, ForeignKey, Generated, TableInfo};

/// Old column position → new one, by name (`None`: the column is gone).
struct Ords(Vec<Option<usize>>);

impl Ords {
    fn new(old: &TableInfo, new: &TableInfo) -> Ords {
        Ords(
            old.columns
                .iter()
                .map(|c| new.columns.iter().position(|n| n.name == c.name))
                .collect(),
        )
    }

    fn one(&self, t: &TableInfo, i: usize) -> Result<usize, String> {
        self.0.get(i).copied().flatten().ok_or_else(|| {
            let name = t.columns.get(i).map_or("?", |c| c.name.as_str());
            format!("column `{}`.`{name}` is gone", t.name)
        })
    }

    fn list(&self, t: &TableInfo, v: &[usize]) -> Result<Vec<usize>, String> {
        v.iter().map(|&i| self.one(t, i)).collect()
    }

    /// Per-column flags in the new positions.
    fn flags(&self, t: &TableInfo, v: &[bool], len: usize) -> Result<Vec<bool>, String> {
        let mut out = vec![false; len];
        for (i, _) in v.iter().enumerate().filter(|(_, &f)| f) {
            out[self.one(t, i)?] = true;
        }
        Ok(out)
    }
}

fn eq<T: PartialEq + ?Sized>(what: &str, a: &T, b: &T) -> Result<(), String> {
    if a == b {
        Ok(())
    } else {
        Err(format!("{what} differs"))
    }
}

/// `Ok` when `new` reads the same columns (names, types, collations, nullability, generation),
/// uses the same keys and foreign keys, and renders the same SQL as `old`, whatever the column
/// positions; else what differs first.
pub fn same_reads(old: &ViewPlan, new: &ViewPlan) -> Result<(), String> {
    let ViewPlan {
        schema,
        name,
        hash,
        mode,
        tables,
        table_columns,
        aliases,
        projection,
        from_sql,
        from_items,
        where_sql,
        group_sql,
        having_sql,
        unit_exprs,
        unit_in_projection,
        full_select,
        unit_unique,
        lineage,
        cascades,
        output_names,
        order_limit_sql,
        window_sql,
        lookup_where_sql,
        alias_filters,
        partial,
        presence,
    } = old;
    eq("schema", schema, &new.schema)?;
    eq("name", name, &new.name)?;
    eq("definition", hash, &new.hash)?;
    eq("mode", mode, &new.mode)?;
    eq("tables", tables, &new.tables)?;
    eq("projection", projection, &new.projection)?;
    eq("FROM", from_sql, &new.from_sql)?;
    eq("FROM", from_items, &new.from_items)?;
    eq("WHERE", where_sql, &new.where_sql)?;
    eq("GROUP BY", group_sql, &new.group_sql)?;
    eq("HAVING", having_sql, &new.having_sql)?;
    eq("units", unit_exprs, &new.unit_exprs)?;
    eq("units", unit_in_projection, &new.unit_in_projection)?;
    eq("full rebuild statement", full_select, &new.full_select)?;
    eq("unit uniqueness", unit_unique, &new.unit_unique)?;
    eq("lineage", lineage, &new.lineage)?;
    eq("output names", output_names, &new.output_names)?;
    eq("ORDER BY / LIMIT", order_limit_sql, &new.order_limit_sql)?;
    eq("WINDOW", window_sql, &new.window_sql)?;
    eq("lookup WHERE", lookup_where_sql, &new.lookup_where_sql)?;
    eq("aliases", &aliases.len(), &new.aliases.len())?;
    let ords = aliases
        .iter()
        .zip(&new.aliases)
        .enumerate()
        .map(|(ai, (a, b))| same_alias(a, b, alias_filters.get(ai), partial, ai))
        .collect::<Result<Vec<Ords>, String>>()?;
    eq(
        "constant filters",
        &alias_filters.len(),
        &new.alias_filters.len(),
    )?;
    for (ai, (fa, fb)) in alias_filters.iter().zip(&new.alias_filters).enumerate() {
        let to = |f: &ConstFilter| -> Result<ConstFilter, String> {
            let ConstFilter { col, values } = f;
            let col = match (ords.get(ai), aliases.get(ai)) {
                (Some(m), Some(a)) => m.one(&a.table, *col)?,
                _ => *col,
            };
            Ok(ConstFilter {
                col,
                values: values.clone(),
            })
        };
        let fa = fa.iter().map(to).collect::<Result<Vec<_>, _>>()?;
        eq("constant filters", &fa, fb)?;
    }
    same_partial(partial, &new.partial, aliases, &ords)?;
    // Tables of full-refresh plans have no alias to map positions with: those must be equal.
    eq(
        "read columns",
        &table_columns.len(),
        &new.table_columns.len(),
    )?;
    for (ti, (ca, cb)) in table_columns.iter().zip(&new.table_columns).enumerate() {
        let alias = tables.get(ti).and_then(|t| {
            aliases
                .iter()
                .position(|a| (&a.table.schema, &a.table.name) == (&t.0, &t.1))
        });
        match (ca, cb, alias) {
            (Some(ca), Some(cb), Some(ai)) => {
                let ca = ords[ai].flags(&aliases[ai].table, ca, cb.len())?;
                eq("read columns", &ca, cb)?;
            }
            (Some(_), Some(_), None) => eq("read columns", ca, cb)?,
            // Only full-refresh plans route by these (`None`: any column, e.g. `SELECT *`).
            (None, None, _) if !matches!(mode, Mode::Full(_)) => {}
            _ => return Err("the view may read any column".into()),
        }
    }
    eq("cascades", &cascades.len(), &new.cascades.len())?;
    // The materialized presence columns are not rebuilt in place: they must be identical.
    eq("presence", presence, &new.presence)?;
    for (ca, cb) in cascades.iter().zip(&new.cascades) {
        same_cascade(ca, cb, aliases, &new.aliases)?;
    }
    Ok(())
}

/// `new` as it goes live in place of `old` (see [`same_reads`]): the image columns of each alias
/// keep `old`'s order, so images buffered or remembered under `old` stay valid.
pub fn adopt(old: &ViewPlan, new: &ViewPlan) -> ViewPlan {
    let mut out = new.clone();
    for (a, b) in old.aliases.iter().zip(out.aliases.iter_mut()) {
        let m = Ords::new(&a.table, &b.table);
        if let Ok(cols) = m.list(&a.table, &a.image_cols) {
            b.image_cols = cols;
        }
    }
    out
}

fn same_alias(
    a: &AliasPlan,
    b: &AliasPlan,
    filters: Option<&Vec<ConstFilter>>,
    partial: &PartialUnits,
    ai: usize,
) -> Result<Ords, String> {
    let AliasPlan {
        name,
        table,
        referenced,
        membership,
        direct_unit,
        image_cols,
        subst_from,
        pos,
        semi,
        lookup_where,
    } = a;
    eq("alias", name, &b.name)?;
    eq("alias", subst_from, &b.subst_from)?;
    eq("alias", pos, &b.pos)?;
    eq("semi-join", semi, &b.semi)?;
    eq("lookup WHERE", lookup_where, &b.lookup_where)?;
    let m = Ords::new(table, &b.table);
    let n = b.table.columns.len();
    let what = |w: &str| format!("{w} of `{name}`");
    eq(
        &what("referenced columns"),
        &m.flags(table, referenced, n)?,
        &b.referenced,
    )?;
    eq(
        &what("membership columns"),
        &m.flags(table, membership, n)?,
        &b.membership,
    )?;
    let du = direct_unit.as_ref().map(|d| m.list(table, d)).transpose()?;
    eq(&what("unit columns"), &du, &b.direct_unit)?;
    let mut ia = m.list(table, image_cols)?;
    let mut ib = b.image_cols.clone();
    ia.sort_unstable();
    ib.sort_unstable();
    eq(&what("image columns"), &ia, &ib)?;
    // Every column some part of the plan reads by position.
    let mut used: BTreeSet<usize> = referenced
        .iter()
        .zip(membership)
        .enumerate()
        .filter(|(_, (r, m))| **r || **m)
        .map(|(i, _)| i)
        .collect();
    used.extend(direct_unit.iter().flatten());
    used.extend(image_cols);
    used.extend(filters.into_iter().flatten().map(|f| f.col));
    if let Some(Some((_, cols))) = partial.aliases.get(ai) {
        used.extend(cols);
    }
    same_table(table, &b.table, &m, used)?;
    Ok(m)
}

/// The table's identity, keys and foreign keys, and every column in `used` (with the base
/// columns of generated ones), compared through `m`.
fn same_table(
    old: &TableInfo,
    new: &TableInfo,
    m: &Ords,
    mut used: BTreeSet<usize>,
) -> Result<(), String> {
    let TableInfo {
        schema,
        name,
        columns,
        pk,
        unique_keys,
        cascades,
        is_view,
        view_bases,
    } = old;
    eq("table", &(schema, name), &(&new.schema, &new.name))?;
    let what = |w: &str| format!("{w} of `{name}`");
    eq(&what("kind"), is_view, &new.is_view)?;
    eq(&what("view tables"), view_bases, &new.view_bases)?;
    eq(&what("primary key"), &m.list(old, pk)?, &new.pk)?;
    let uk = unique_keys
        .iter()
        .map(|k| m.list(old, k))
        .collect::<Result<Vec<_>, _>>()?;
    eq(&what("unique keys"), &uk, &new.unique_keys)?;
    eq(&what("foreign keys"), &cascades.len(), &new.cascades.len())?;
    for (fa, fb) in cascades.iter().zip(&new.cascades) {
        let ForeignKey {
            parent,
            child_cols,
            parent_cols,
            on_delete,
            on_update,
        } = fa;
        eq(&what("foreign keys"), parent, &fb.parent)?;
        eq(
            &what("foreign keys"),
            &m.list(old, child_cols)?,
            &fb.child_cols,
        )?;
        eq(&what("foreign keys"), parent_cols, &fb.parent_cols)?;
        eq(
            &what("foreign keys"),
            &(on_delete, on_update),
            &(&fb.on_delete, &fb.on_update),
        )?;
        used.extend(child_cols);
    }
    used.extend(pk);
    used.extend(unique_keys.iter().flatten());
    let deps: Vec<usize> = used
        .iter()
        .filter_map(|&i| columns.get(i)?.generated.as_ref()?.deps.clone())
        .flatten()
        .collect();
    used.extend(deps);
    for i in used {
        let (Some(a), j) = (columns.get(i), m.one(old, i)?) else {
            return Err(what("columns"));
        };
        same_column(a, &new.columns[j], old, m)
            .map_err(|w| format!("column `{name}`.`{}`: {w}", a.name))?;
    }
    Ok(())
}

fn same_column(a: &Column, b: &Column, t: &TableInfo, m: &Ords) -> Result<(), String> {
    let Column {
        name,
        kind,
        param,
        exact,
        invisible,
        data_type,
        column_type,
        charset,
        collation,
        octet_len,
        nullable,
        generated,
    } = a;
    eq("name", name, &b.name)?;
    eq(
        "type",
        &(data_type, column_type),
        &(&b.data_type, &b.column_type),
    )?;
    eq("type", kind, &b.kind)?;
    eq(
        "type",
        &(param, exact, octet_len),
        &(&b.param, &b.exact, &b.octet_len),
    )?;
    eq(
        "collation",
        &(charset, collation),
        &(&b.charset, &b.collation),
    )?;
    eq("nullability", nullable, &b.nullable)?;
    eq("visibility", invisible, &b.invisible)?;
    match (generated, &b.generated) {
        (None, None) => Ok(()),
        (Some(ga), Some(gb)) => {
            let Generated { expr, stored, deps } = ga;
            eq("generation", &(expr, stored), &(&gb.expr, &gb.stored))?;
            let deps = deps.as_ref().map(|d| m.list(t, d)).transpose()?;
            eq("generation", &deps, &gb.deps)
        }
        _ => Err("generation differs".into()),
    }
}

fn same_partial(
    a: &PartialUnits,
    b: &PartialUnits,
    aliases: &[AliasPlan],
    ords: &[Ords],
) -> Result<(), String> {
    let PartialUnits { sets, aliases: per } = a;
    eq("partial units", sets, &b.sets)?;
    eq("partial units", &per.len(), &b.aliases.len())?;
    for (ai, (pa, pb)) in per.iter().zip(&b.aliases).enumerate() {
        let pa = match (pa, ords.get(ai), aliases.get(ai)) {
            (Some((set, cols)), Some(m), Some(al)) => Some((*set, m.list(&al.table, cols)?)),
            (pa, _, _) => pa.clone(),
        };
        eq("partial units", &pa, pb)?;
    }
    Ok(())
}

fn same_cascade(
    a: &CascadeRoute,
    b: &CascadeRoute,
    old_aliases: &[AliasPlan],
    new_aliases: &[AliasPlan],
) -> Result<(), String> {
    let CascadeRoute {
        parent,
        parent_cols,
        on_delete,
        on_update,
        unit_from_parent,
        child,
        child_cols,
        traced,
        untraced,
        derived,
        partial_from_parent,
        pinned,
        reach,
    } = a;
    let pm = Ords::new(parent, &b.parent);
    let mut used: BTreeSet<usize> = parent_cols.iter().copied().collect();
    used.extend(unit_from_parent.iter().flatten());
    same_table(parent, &b.parent, &pm, used)?;
    eq("cascade", &pm.list(parent, parent_cols)?, &b.parent_cols)?;
    let ufp = unit_from_parent
        .as_ref()
        .map(|u| pm.list(parent, u))
        .transpose()?;
    eq("cascade", &ufp, &b.unit_from_parent)?;
    eq("cascade", derived, &b.derived)?;
    // (The set positions are into `plan.partial.sets`, compared in `same_partial`; the
    // presence positions in `pinned` are into `plan.presence`, compared in `same_reads`.)
    let pfp = partial_from_parent
        .iter()
        .map(|(set, cols)| Ok((*set, pm.list(parent, cols)?)))
        .collect::<Result<Vec<_>, String>>()?;
    eq("cascade", &pfp, &b.partial_from_parent)?;
    let pins = pinned
        .iter()
        .map(|p| {
            let Pinned { parent_col, values } = p;
            Ok(Pinned {
                parent_col: pm.one(parent, *parent_col)?,
                values: values.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    eq("cascade", &pins, &b.pinned)?;
    let reach = reach
        .as_ref()
        .map(|(pc, vals)| Ok::<_, String>((pm.one(parent, *pc)?, vals.clone())))
        .transpose()?;
    eq("cascade", &reach, &b.reach)?;
    eq(
        "cascade",
        &(on_delete, on_update),
        &(&b.on_delete, &b.on_update),
    )?;
    eq("cascade", child, &b.child)?;
    eq("cascade", &(traced, untraced), &(&b.traced, &b.untraced))?;
    // The child's columns map through an alias of the child (the view reads it).
    let of = |al: &[AliasPlan]| {
        al.iter()
            .find(|x| (&x.table.schema, &x.table.name) == (&child.0, &child.1))
            .map(|x| x.table.clone())
    };
    let cc = match (of(old_aliases), of(new_aliases)) {
        (Some(ot), Some(nt)) => Ords::new(&ot, &nt).list(&ot, child_cols)?,
        _ => child_cols.clone(),
    };
    eq("cascade", &cc, &b.child_cols)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{adopt, same_reads};
    use crate::analyze::{parse_definition, plan, ViewPlan};
    use crate::catalog::{Column, TableInfo, TableRef};

    type Catalog = HashMap<TableRef, Arc<TableInfo>>;

    fn table(name: &str, cols: Vec<Column>, pk: &[&str]) -> (TableRef, Arc<TableInfo>) {
        let pk = pk
            .iter()
            .map(|k| cols.iter().position(|c| c.name == *k).unwrap())
            .collect();
        let t = TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols,
            pk,
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    }

    fn cols(spec: &[(&str, &str)]) -> Vec<Column> {
        spec.iter().map(|(n, t)| Column::simple(n, t)).collect()
    }

    /// `items (id, grp, status, note)` and `links (id, item_id, tag)`, with `edit` applied to the
    /// column lists first.
    fn catalog(edit: impl Fn(&str, &mut Vec<Column>)) -> Catalog {
        let mut items = cols(&[
            ("id", "int"),
            ("grp", "int"),
            ("status", "varchar"),
            ("note", "text"),
        ]);
        let mut links = cols(&[("id", "int"), ("item_id", "int"), ("tag", "varchar")]);
        edit("items", &mut items);
        edit("links", &mut links);
        [
            table("items", items, &["id"]),
            table("links", links, &["id"]),
        ]
        .into_iter()
        .collect()
    }

    fn p(sql: &str, c: &Catalog) -> ViewPlan {
        let def = format!("CREATE VIEW v AS {sql}");
        plan(&parse_definition(&def).unwrap(), "db", c).unwrap()
    }

    const VIEWS: [&str; 3] = [
        "SELECT grp, COUNT(*) AS n FROM items GROUP BY grp",
        "SELECT i.id, i.status, l.tag FROM items i JOIN links l ON l.item_id = i.id",
        "SELECT l.tag, COUNT(*) AS n FROM links l JOIN items i ON i.id = l.item_id \
         WHERE i.status = 'new' GROUP BY l.tag",
    ];

    fn check(edit: impl Fn(&str, &mut Vec<Column>), same: bool) {
        let old = catalog(|_, _| {});
        let new = catalog(edit);
        for sql in VIEWS {
            let r = same_reads(&p(sql, &old), &p(sql, &new));
            assert_eq!(r.is_ok(), same, "{sql}: {r:?}");
        }
    }

    #[test]
    fn same_reads_ignore_positions_and_unread_columns() {
        check(|_, _| {}, true);
        // A column added in front of the read ones, one at the end, one dropped that no view reads.
        check(
            |t, c| c.insert(1, Column::simple(&format!("{t}_x"), "int")),
            true,
        );
        check(|_, c| c.push(Column::simple("later", "datetime")), true);
        check(
            |t, c| {
                if t == "items" {
                    c.retain(|c| c.name != "note");
                }
            },
            true,
        );
        // Read columns moved (MODIFY … AFTER), and an unread one retyped.
        check(|_, c| c.reverse(), true);
        check(
            |t, c| {
                if t == "items" {
                    c[3] = Column::simple("note", "mediumtext");
                }
            },
            true,
        );
    }

    #[test]
    fn same_reads_catch_changed_reads() {
        // A read column retyped, recollated, made NOT NULL.
        check(
            |t, c| {
                if t == "items" {
                    c[1] = Column::simple("grp", "bigint");
                }
                if t == "links" {
                    c[2] = Column::simple("tag", "text");
                }
            },
            false,
        );
        check(
            |t, c| {
                if t == "items" {
                    c[1].nullable = !c[1].nullable;
                    c[2].collation = Some("utf8mb4_bin".into());
                }
                if t == "links" {
                    c[1].nullable = !c[1].nullable;
                    c[2].collation = Some("utf8mb4_bin".into());
                }
            },
            false,
        );
    }

    #[test]
    fn same_reads_catch_changed_keys() {
        let old = catalog(|_, _| {});
        let mut new = old.clone();
        let mut items = (*new[&("db".to_string(), "items".to_string())]).clone();
        items.pk = vec![1];
        new.insert(("db".into(), "items".into()), Arc::new(items));
        let sql = VIEWS[1];
        let r = same_reads(&p(sql, &old), &p(sql, &new));
        assert!(r.is_err(), "{sql}: {r:?}");
    }

    /// The plan put in place keeps the old image column order (images buffered under the old
    /// plan stay valid) but the new positions.
    #[test]
    fn adopted_plan_keeps_image_order() {
        let old_c = catalog(|_, _| {});
        let new_c = catalog(|_, c| c.reverse());
        for sql in VIEWS {
            let (old, new) = (p(sql, &old_c), p(sql, &new_c));
            let put = adopt(&old, &new);
            same_reads(&old, &put).unwrap();
            for (a, b) in old.aliases.iter().zip(&put.aliases) {
                let names = |x: &crate::analyze::AliasPlan| -> Vec<String> {
                    x.image_cols
                        .iter()
                        .map(|&i| x.table.columns[i].name.clone())
                        .collect()
                };
                assert_eq!(names(a), names(b), "{sql}");
            }
        }
    }
}
