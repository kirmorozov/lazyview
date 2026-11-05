//! The analyzer's public, per-source-table output; plain data; the copy set.
//! One projection per view definition ([`AccessPlan::of`]).

use std::collections::{BTreeMap, BTreeSet};

use crate::analyze::{definition_hash, int_sql, quote_ident, ConstFilter, Edge, Mode, ViewPlan};
use crate::catalog::{FkAction, TableRef};
use crate::datum::Datum;

/// The view's unit, by expression.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetId {
    /// The unit expressions (`ViewPlan.unit_exprs`).
    pub parts: Vec<IdPart>,
    /// Whether one unit maps to at most one materialized row.
    pub unique: bool,
}

/// One unit expression.
#[derive(Debug, Clone, PartialEq)]
pub struct IdPart {
    pub expr: String,
    /// The projection index that already yields it; `None`: the hidden column `_lv_u<i>`.
    pub output: Option<usize>,
}

/// The view's maintenance route: static, by traceability.
#[derive(Debug, Clone, PartialEq)]
pub enum Route {
    /// Every occurrence (and cascade) has a clear derivation: ids come straight from binlog
    /// images.
    Dedup,
    /// Maintained by the view plan (lineage / lookup images); the reason names the first
    /// occurrence or cascade that cannot be derived.
    ViewPlan { reason: String },
    /// A single-row aggregate: recomputed on any change.
    Recompute,
    /// Not maintained incrementally; the planner's reason.
    Full { reason: String },
}

/// One source-table occurrence of the view.
#[derive(Debug, Clone, PartialEq)]
pub struct Occurrence {
    /// `(schema, name)` of the source table.
    pub table: TableRef,
    /// The name the query uses for it (Full: the table name).
    pub alias: String,
    /// The columns it reads (`None`: any column).
    pub read_columns: Option<Vec<bool>>,
    /// Its constant filters: integers, NULL never passes.
    pub filters: Vec<ConstFilter>,
    /// How its changed rows reach the view's units.
    pub path: Path,
}

/// How a changed row of the occurrence's table maps to the view's units.
#[derive(Debug, Clone, PartialEq)]
pub enum Path {
    /// The row image carries every unit position; `cols`: the alias's columns equal to them.
    Direct { cols: Vec<usize> },
    /// The row image carries some unit positions (`partial_of`'s set and columns).
    Partial {
        set: usize,
        positions: Vec<usize>,
        cols: Vec<usize>,
    },
    /// The units come through the materialized key columns (`cols`).
    Lineage { cols: Vec<String> },
    /// The id through link tables.
    Hop(Hop),
    /// No clear derivation; `reason`.
    Untraced { reason: String },
    /// One unit (the `Recompute` route) or the whole view (the `Full` route).
    Whole,
}

/// The id through the occurrence's link tables: its columns on the chain's edges, the matching
/// chain columns as `alias`.`col`, the id positions the chain reaches (all: the full id), the
/// chain's aliases (not including the occurrence) and the parameterized SELECT mapping the
/// partner values to the id values; the key filter on the partners is appended by the executor.
#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub link_cols: Vec<usize>,
    pub partner: Vec<String>,
    pub positions: Vec<usize>,
    pub aliases: Vec<usize>,
    pub select: String,
}

/// One cascading foreign key whose parent deletes / key updates the view must handle.
#[derive(Debug, Clone, PartialEq)]
pub struct CascadeAccess {
    /// `(schema, name)` of the parent table.
    pub parent: TableRef,
    /// Ordinals of the referenced parent columns.
    pub parent_cols: Vec<usize>,
    /// `(schema, name)` of the child table.
    pub child: TableRef,
    /// The child's FK columns (constraint order, matching `parent_cols`).
    pub child_cols: Vec<usize>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    pub via: Via,
    /// The parent column and the values that pin every child alias.
    pub reach: Option<(usize, Vec<Datum>)>,
}

/// How the parent's change reaches the child's rows.
#[derive(Debug, Clone, PartialEq)]
pub enum Via {
    /// The parent columns give each unit value.
    Direct { parent_cols: Vec<usize> },
    /// Through the parent's own occurrence.
    Traced,
    /// Partial unit keys from the parent and, per pinned value, a presence column.
    Derived {
        partial: Vec<(usize, Vec<usize>)>,
        presence: Vec<String>,
    },
    /// Through the in-memory cascade cache (the child's `key` over its `cols`).
    Map { key: Vec<usize>, cols: Vec<usize> },
    /// Unknown; `reason`: the view rebuilds on the parent's deletes / key updates.
    Untraced { reason: String },
}

/// The analyzer's public output for one view definition.
#[derive(Debug, Clone, PartialEq)]
pub struct AccessPlan {
    /// `ViewPlan.schema`, `ViewPlan.name`.
    pub view: (String, String),
    /// `ViewPlan.hash` (the definition hash).
    pub hash: String,
    /// The view's unit, by expression.
    pub id: TargetId,
    pub route: Route,
    /// Spj/Grouped/Global: one per `ViewPlan.aliases[i]` (same index);
    /// Full: one per `ViewPlan.tables[i]` (same index).
    pub occurrences: Vec<Occurrence>,
    /// One per `ViewPlan.cascades[i]` (same index).
    pub cascades: Vec<CascadeAccess>,
    /// `definition_hash` of the EXPLAIN text without its header: 16 hex.
    pub fingerprint: String,
}

/// One occurrence per source-table use of the view.
fn occurrences(p: &ViewPlan) -> Vec<Occurrence> {
    let alias_occurrence = |i: usize, a: &crate::analyze::AliasPlan| Occurrence {
        table: (a.table.schema.clone(), a.table.name.clone()),
        alias: a.name.clone(),
        read_columns: Some(a.referenced.clone()),
        filters: p.filters_of(i).to_vec(),
        path: Path::Whole,
    };
    match &p.mode {
        Mode::Full(_) => p
            .tables
            .iter()
            .zip(&p.table_columns)
            .map(|(t, cols)| Occurrence {
                table: t.clone(),
                alias: t.1.clone(),
                read_columns: cols.clone(),
                filters: Vec::new(),
                path: Path::Whole,
            })
            .collect(),
        Mode::Global => p
            .aliases
            .iter()
            .enumerate()
            .map(|(i, a)| alias_occurrence(i, a))
            .collect(),
        _ => p
            .aliases
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let path = if let Some(cols) = &a.direct_unit {
                    Path::Direct { cols: cols.clone() }
                } else if let Some((set, cols)) = p.partial_of(i) {
                    Path::Partial {
                        set,
                        positions: p.partial.sets[set].clone(),
                        cols: cols.to_vec(),
                    }
                } else {
                    // Lineage and lookup aliases try the id through link tables; a refusal keeps
                    // the alias's lineage (when it has one) or is untraced with the reason.
                    match (hop(p, i), p.lineage_of(i)) {
                        (Ok(h), _) => Path::Hop(h),
                        (Err(_), Some(k)) => Path::Lineage {
                            cols: k.cols.clone(),
                        },
                        (Err(r), None) => Path::Untraced { reason: r },
                    }
                };
                let mut o = alias_occurrence(i, a);
                o.path = path;
                o
            })
            .collect(),
    }
}

/// The chain found for an occurrence: the chain's aliases (ascending, without the occurrence),
/// its link pairs (its column -> the partner's alias column), the id positions the chain
/// reaches and the edge indexes of the chain's shortest paths.
struct Chain {
    aliases: Vec<usize>,
    links: Vec<(usize, (usize, usize))>,
    positions: Vec<usize>,
    edges: Vec<usize>,
}

/// The hop rule for an occurrence that is neither Direct nor Partial: a breadth-first search
/// from it over the usable edges (the equalities valid on every view row that involves the
/// occurrence: the WHERE / inner ON, plus its own LEFT JOIN ON), in alias-index order, reaches
/// the aliases the id parts read. The chain is the union of the shortest paths, without the
/// occurrence; the positions are the id parts whose refs all lie in the chain. Each refusal
/// is the `Err` reason.
fn search(p: &ViewPlan, y: usize) -> Result<Chain, String> {
    let a = &p.aliases[y];
    if a.semi.is_some() {
        return Err(format!("`{}` is a semi-join table", a.name));
    }
    if a.pos.0 == usize::MAX {
        return Err(format!("`{}` is an ORDER BY subquery table", a.name));
    }
    let id_refs = &p.unit_refs;
    if id_refs
        .iter()
        .any(|refs| refs.iter().any(|(al, _)| *al == y))
    {
        return Err(format!("the id reads `{}`", a.name));
    }
    let targets: BTreeSet<usize> = id_refs.iter().flatten().map(|(al, _)| *al).collect();
    if targets.is_empty() {
        return Err("not joined to the id by equalities".into());
    }
    let usable: Vec<(usize, &Edge)> = p
        .edges
        .iter()
        .enumerate()
        .filter(|(_, e)| e.only_with.is_none_or(|j| j == y))
        .collect();
    let mut adj: Vec<Vec<(usize, usize)>> = vec![Vec::new(); p.aliases.len()];
    for &(ei, e) in &usable {
        adj[e.a.0].push((e.b.0, ei));
        adj[e.b.0].push((e.a.0, ei));
    }
    let mut visited: BTreeSet<usize> = BTreeSet::from([y]);
    let mut parent: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
    let mut frontier = vec![y];
    loop {
        if targets.iter().all(|t| visited.contains(t)) {
            break;
        }
        let mut next: Vec<usize> = Vec::new();
        for &al in &frontier {
            for &(nb, ei) in &adj[al] {
                if visited.insert(nb) {
                    parent.insert(nb, (al, ei));
                    next.push(nb);
                }
            }
        }
        next.sort_unstable();
        next.dedup();
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    let mut sset: BTreeSet<usize> = BTreeSet::new();
    let mut edges: Vec<usize> = Vec::new();
    for &t in &targets {
        if !visited.contains(&t) {
            continue;
        }
        let mut al = t;
        while al != y {
            sset.insert(al);
            let (prev, ei) = parent[&al];
            edges.push(ei);
            al = prev;
        }
    }
    edges.sort_unstable();
    edges.dedup();
    let positions: Vec<usize> = (0..id_refs.len())
        .filter(|&i| id_refs[i].iter().all(|(al, _)| sset.contains(al)))
        .collect();
    let mut links: Vec<(usize, (usize, usize))> = Vec::new();
    for &ei in &edges {
        let e = &p.edges[ei];
        let (yc, pc) = if e.a.0 == y {
            (e.a.1, e.b)
        } else if e.b.0 == y {
            (e.b.1, e.a)
        } else {
            continue;
        };
        if !links.iter().any(|(c, q)| *c == yc && *q == pc) {
            links.push((yc, pc));
        }
    }
    links.sort_unstable();
    if links.is_empty() || positions.is_empty() {
        return Err("not joined to the id by equalities".into());
    }
    Ok(Chain {
        aliases: sset.into_iter().collect(),
        links,
        positions,
        edges,
    })
}

/// The chain of the occurrence: its aliases (without the occurrence), its link pairs
/// (its column -> the partner's alias column) and the id positions the chain reaches.
/// (The triple projection of [`search`]; the hop SQL and the EXPLAIN use [`hop`] instead.)
#[cfg(test)]
fn chain(
    p: &ViewPlan,
    y: usize,
) -> Result<(Vec<usize>, Vec<(usize, (usize, usize))>, Vec<usize>), String> {
    let c = search(p, y)?;
    Ok((c.aliases, c.links, c.positions))
}

/// The id of the occurrence through its link tables: the hop SQL maps the partner values to
/// the id values and drops the view's other conditions (a superset of the ids, sound).
fn hop(p: &ViewPlan, y: usize) -> Result<Hop, String> {
    let c = search(p, y)?;
    // Both sides of every edge of the chain carry binlog values that compare as their SQL
    // values do; a collation-compared link would make the step-6 cache inexact.
    for &ei in &c.edges {
        let e = &p.edges[ei];
        if !p.aliases[e.a.0].table.columns[e.a.1].exact_key()
            || !p.aliases[e.b.0].table.columns[e.b.1].exact_key()
        {
            return Err("link compared by collation".into());
        }
    }
    let qcol = |al: usize, col: usize| {
        format!(
            "{}.{}",
            quote_ident(&p.aliases[al].name),
            quote_ident(&p.aliases[al].table.columns[col].name)
        )
    };
    let partner: Vec<String> = c
        .links
        .iter()
        .map(|&(_, (al, col))| qcol(al, col))
        .collect();
    let mut sel = partner.clone();
    sel.extend(c.positions.iter().map(|&i| p.unit_exprs[i].clone()));
    let from: Vec<String> = std::iter::once(y)
        .chain(c.aliases.iter().copied())
        .map(|al| {
            let a = &p.aliases[al];
            format!(
                "{}.{} AS {}",
                quote_ident(&a.table.schema),
                quote_ident(&a.table.name),
                quote_ident(&a.name)
            )
        })
        .collect();
    let mut where_parts: Vec<String> = c
        .edges
        .iter()
        .map(|&ei| {
            let e = &p.edges[ei];
            format!("{} = {}", qcol(e.a.0, e.a.1), qcol(e.b.0, e.b.1))
        })
        .collect();
    for &al in &c.aliases {
        for f in p.filters_of(al) {
            where_parts.push(format!(
                "{} IN ({})",
                qcol(al, f.col),
                f.values.iter().map(int_sql).collect::<Vec<_>>().join(", ")
            ));
        }
    }
    Ok(Hop {
        link_cols: c.links.iter().map(|&(col, _)| col).collect(),
        partner,
        positions: c.positions,
        aliases: c.aliases,
        select: format!(
            "SELECT DISTINCT {} FROM {} WHERE {}",
            sel.join(", "),
            from.join(", "),
            where_parts.join(" AND ")
        ),
    })
}

/// One access per cascading foreign key of the plan, in `ViewPlan.cascades` order; the
/// `via` order is `ViewPlan::tracing`'s.
fn cascades(p: &ViewPlan) -> Vec<CascadeAccess> {
    p.cascades
        .iter()
        .map(|c| {
            let via = if let Some(cols) = &c.unit_from_parent {
                Via::Direct {
                    parent_cols: cols.clone(),
                }
            } else if c.traced {
                Via::Traced
            } else if c.derived {
                let presence = c
                    .pinned
                    .iter()
                    .flat_map(|pin| pin.values.iter().map(|(_, k)| p.presence[*k].name.clone()))
                    .collect();
                Via::Derived {
                    partial: c.partial_from_parent.clone(),
                    presence,
                }
            } else if let Some(m) = &c.map {
                Via::Map {
                    key: m.key.clone(),
                    cols: m.cols.clone(),
                }
            } else {
                Via::Untraced {
                    reason: c.untraced.clone().unwrap_or_else(|| {
                        format!(
                            "`{}` cascades into `{}`.`{}`",
                            c.parent.name, c.child.0, c.child.1
                        )
                    }),
                }
            };
            CascadeAccess {
                parent: (c.parent.schema.clone(), c.parent.name.clone()),
                parent_cols: c.parent_cols.clone(),
                child: c.child.clone(),
                child_cols: c.child_cols.clone(),
                on_delete: c.on_delete,
                on_update: c.on_update,
                via,
                reach: c.reach.clone(),
            }
        })
        .collect()
}

impl AccessPlan {
    /// The public access plan of `p`: its unit, route, occurrences and cascades, and the
    /// fingerprint of its EXPLAIN text (without the header).
    pub fn of(p: &ViewPlan) -> AccessPlan {
        let id = TargetId {
            parts: p
                .unit_exprs
                .iter()
                .zip(&p.unit_in_projection)
                .map(|(e, o)| IdPart {
                    expr: e.clone(),
                    output: *o,
                })
                .collect(),
            unique: p.unit_unique,
        };
        let occurrences = occurrences(p);
        let cascades = cascades(p);
        let route = if let Mode::Full(reason) = &p.mode {
            Route::Full {
                reason: reason.clone(),
            }
        } else if matches!(p.mode, Mode::Global) {
            Route::Recompute
        } else {
            match occurrences
                .iter()
                .enumerate()
                .find(|(_, o)| matches!(o.path, Path::Lineage { .. } | Path::Untraced { .. }))
            {
                Some((i, o)) => Route::ViewPlan {
                    reason: format!(
                        "{} ({}): {}",
                        quote_ident(&o.alias),
                        quote_ident(&o.table.1),
                        match &o.path {
                            // The hop was refused for this occurrence; the refusal is the why.
                            Path::Lineage { .. } =>
                                format!("lineage only: {}", hop(p, i).err().unwrap_or_default()),
                            Path::Untraced { reason } => reason.clone(),
                            _ => unreachable!(),
                        }
                    ),
                },
                None => match cascades
                    .iter()
                    .find(|c| matches!(c.via, Via::Untraced { .. }))
                {
                    Some(c) => Route::ViewPlan {
                        reason: format!(
                            "cascade {} -> {}: {}",
                            quote_ident(&c.parent.1),
                            quote_ident(&c.child.1),
                            match &c.via {
                                Via::Untraced { reason } => reason.clone(),
                                _ => unreachable!(),
                            }
                        ),
                    },
                    None => Route::Dedup,
                },
            }
        };
        let plan = AccessPlan {
            view: (p.schema.clone(), p.name.clone()),
            hash: p.hash.clone(),
            id,
            route,
            occurrences,
            cascades,
            fingerprint: String::new(),
        };
        let fingerprint = definition_hash(&plan.body());
        AccessPlan {
            fingerprint,
            ..plan
        }
    }

    /// The fingerprint token of an EXPLAIN text: after ` fp ` on the header line.
    pub fn fingerprint_of(text: &str) -> Option<&str> {
        text.lines().next()?.rsplit_once(" fp ").map(|(_, t)| t)
    }

    /// The route, for logs: `dedup` | `viewplan` | `recompute` | `full`.
    pub fn route_name(&self) -> &'static str {
        match self.route {
            Route::Dedup => "dedup",
            Route::ViewPlan { .. } => "viewplan",
            Route::Recompute => "recompute",
            Route::Full { .. } => "full",
        }
    }

    /// The EXPLAIN text without its header: one line per id part, occurrence and cascade.
    fn body(&self) -> String {
        let mut s = String::new();
        for (i, part) in self.id.parts.iter().enumerate() {
            s.push_str(&match part.output {
                Some(o) => format!("id {i} {} (output {o})\n", part.expr),
                None => format!("id {i} {} (hidden _lv_u{i})\n", part.expr),
            });
        }
        for o in &self.occurrences {
            let read = match &o.read_columns {
                None => "*".to_string(),
                Some(cols) => format!(
                    "({})",
                    cols.iter()
                        .enumerate()
                        .filter_map(|(c, r)| r.then_some(c.to_string()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            };
            let mut line = format!("occ {} {} read {read}", o.alias, tref(&o.table));
            if !o.filters.is_empty() {
                line.push_str(&format!(
                    " filter {}",
                    o.filters
                        .iter()
                        .map(|f| {
                            format!(
                                "{} IN ({})",
                                f.col,
                                f.values.iter().map(int_sql).collect::<Vec<_>>().join(", ")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ));
            }
            line.push_str(&format!(" -> {}", path_str(&o.path)));
            s.push_str(&line);
            s.push('\n');
        }
        for c in &self.cascades {
            let mut line = format!(
                "cascade {} -> {} ({}) on delete {} on update {} via {}",
                tref(&c.parent),
                tref(&c.child),
                ordinals(&c.parent_cols),
                fk(c.on_delete),
                fk(c.on_update),
                via_str(&c.via)
            );
            if let Some((col, vals)) = &c.reach {
                line.push_str(&format!(
                    " reach {col} IN ({})",
                    vals.iter().map(int_sql).collect::<Vec<_>>().join(", ")
                ));
            }
            s.push_str(&line);
            s.push('\n');
        }
        s
    }
}

/// `schema.name` of a table reference.
fn tref(t: &TableRef) -> String {
    format!("{}.{}", t.0, t.1)
}

/// 0-based column ordinals, as `0, 1, …`.
fn ordinals(cols: &[usize]) -> String {
    cols.iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn path_str(p: &Path) -> String {
    match p {
        Path::Whole => "whole".into(),
        Path::Direct { cols } => format!("direct id = ({})", ordinals(cols)),
        Path::Partial {
            positions, cols, ..
        } => {
            format!("partial id[{}] = ({})", ordinals(positions), ordinals(cols))
        }
        Path::Hop(h) => format!(
            "hop {} = {} -> id[{}] ({} tables)",
            ordinals(&h.link_cols),
            h.partner.join(", "),
            ordinals(&h.positions),
            1 + h.aliases.len()
        ),
        Path::Lineage { cols } => format!("lineage ({})", cols.join(", ")),
        Path::Untraced { reason } => format!("untraced ({reason})"),
    }
}

fn via_str(v: &Via) -> String {
    match v {
        Via::Direct { parent_cols } => format!("direct ({})", ordinals(parent_cols)),
        Via::Traced => "traced".into(),
        Via::Derived { partial, presence } => {
            let mut s = format!(
                "derived partial ({})",
                partial
                    .iter()
                    .map(|(set, cols)| format!("id[{set}] = ({})", ordinals(cols)))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            if !presence.is_empty() {
                s.push_str(&format!(" presence ({})", presence.join(", ")));
            }
            s
        }
        Via::Map { key, .. } => format!("map key ({})", ordinals(key)),
        Via::Untraced { reason } => format!("untraced ({reason})"),
    }
}

fn fk(a: FkAction) -> &'static str {
    match a {
        FkAction::Cascade => "CASCADE",
        FkAction::Restrict => "RESTRICT",
        FkAction::SetNull => "SET NULL",
    }
}

impl std::fmt::Display for AccessPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head = match &self.route {
            Route::ViewPlan { reason } | Route::Full { reason } => format!(
                "view {} route {} ({reason}) fp {}",
                tref(&self.view),
                self.route_name(),
                self.fingerprint
            ),
            _ => format!(
                "view {} route {} fp {}",
                tref(&self.view),
                self.route_name(),
                self.fingerprint
            ),
        };
        write!(f, "{head}\n{}", self.body())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::{parse_definition, plan_block};
    use crate::catalog::{Column, TableInfo};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn table(name: &str, cols: &[(&str, &str)], pk: &[usize]) -> (TableRef, Arc<TableInfo>) {
        let t = TableInfo {
            schema: "db".into(),
            name: name.into(),
            columns: cols.iter().map(|(n, ty)| Column::simple(n, ty)).collect(),
            pk: pk.to_vec(),
            unique_keys: vec![],
            cascades: vec![],
            is_view: false,
            view_bases: vec![],
        };
        (("db".into(), name.into()), Arc::new(t))
    }

    fn catalog() -> HashMap<TableRef, Arc<TableInfo>> {
        [
            table(
                "items",
                &[("id", "int"), ("kind_id", "int"), ("owner_id", "int")],
                &[0],
            ),
            table("kinds", &[("id", "int"), ("name", "varchar")], &[0]),
            table(
                "item_values",
                &[("item_id", "int"), ("key_id", "int"), ("val", "int")],
                &[0, 1],
            ),
            table("owners", &[("id", "int"), ("name", "varchar")], &[0]),
            table(
                "item_links",
                &[("child_id", "int"), ("parent_id", "int")],
                &[0, 1],
            ),
        ]
        .into_iter()
        .collect()
    }

    fn plan(sql: &str) -> ViewPlan {
        let d = parse_definition(&format!("CREATE VIEW v AS {sql}")).unwrap();
        plan_block(&d, "db", &catalog()).unwrap()
    }

    #[test]
    fn single_table_is_dedup() {
        let ap = AccessPlan::of(&plan("SELECT i.id FROM items i"));
        assert!(matches!(ap.route, Route::Dedup));
        assert_eq!(ap.route_name(), "dedup");
        assert!(ap.id.unique);
        let body = ap.body();
        assert!(body.contains("occ i db.items"), "{body}");
        assert!(body.contains("-> direct"), "{body}");
        assert!(body.contains(r"id 0 `i`.`id`"), "{body}");
    }

    #[test]
    fn lineage_alias_is_viewplan() {
        // `k` hops to the id; `l` reaches only `k` (its own LEFT-join edge), so it keeps its
        // lineage and the view stays on the viewplan route.
        let ap = AccessPlan::of(&plan(
            "SELECT i.id, k.name, l.parent_id FROM items i LEFT JOIN kinds k ON k.id = i.kind_id \
             LEFT JOIN item_links l ON l.parent_id = k.id",
        ));
        assert!(matches!(ap.occurrences[1].path, Path::Hop(_)));
        assert!(matches!(ap.occurrences[2].path, Path::Lineage { .. }));
        match &ap.route {
            Route::ViewPlan { reason } => {
                assert!(reason.starts_with("`l` (`item_links`): "), "{reason}");
                assert!(
                    reason.ends_with("lineage only: not joined to the id by equalities"),
                    "{reason}"
                );
            }
            r => panic!("route: {r:?}"),
        }
    }

    #[test]
    fn full_view_is_whole() {
        let ap = AccessPlan::of(&plan("SELECT DISTINCT kind_id FROM items"));
        match &ap.route {
            Route::Full { reason } => assert_eq!(reason, "DISTINCT"),
            r => panic!("route: {r:?}"),
        }
        assert_eq!(ap.occurrences.len(), 1);
        assert!(ap.occurrences.iter().all(|o| matches!(o.path, Path::Whole)));
        assert_eq!(ap.route_name(), "full");
    }

    #[test]
    fn fingerprint_is_stable_and_sensitive() {
        let a = AccessPlan::of(&plan(
            "SELECT i.id, i.kind_id FROM items i WHERE i.kind_id = 3",
        ));
        let b = AccessPlan::of(&plan(
            "SELECT i.id, i.kind_id FROM items i WHERE i.kind_id = 3",
        ));
        assert_eq!(a.fingerprint, b.fingerprint);
        let c = AccessPlan::of(&plan(
            "SELECT i.id, i.kind_id FROM items i WHERE i.kind_id = 4",
        ));
        assert_ne!(a.fingerprint, c.fingerprint);
        assert_eq!(a.fingerprint.len(), 16);
        assert!(a.fingerprint.chars().all(|ch| ch.is_ascii_hexdigit()));
        // The header carries the same fingerprint.
        assert!(a.to_string().contains(&format!("fp {}", a.fingerprint)));
    }

    /// The dump agrees with what runs: each occurrence's path kind is the label
    /// `tracing()` prints at the same index.
    #[test]
    fn paths_match_tracing() {
        let defs: [(&str, Mode); 10] = [
            ("SELECT i.id FROM items i", Mode::Spj),
            (
                "SELECT i.id, k.name FROM items i JOIN kinds k ON k.id = i.kind_id",
                Mode::Spj,
            ),
            (
                "SELECT i.id, v.key_id, MAX(v.val) n FROM items i JOIN item_values v \
                 ON v.item_id = i.id AND v.key_id = i.kind_id GROUP BY i.id, v.key_id",
                Mode::Grouped,
            ),
            (
                "SELECT i.id, k.name, COUNT(*) n FROM items i JOIN kinds k \
                 ON k.id = i.kind_id GROUP BY i.id, k.name",
                Mode::Grouped,
            ),
            (
                "SELECT i.id, k.name, MAX(o.name) n FROM items i JOIN kinds k ON k.id = i.kind_id \
                 JOIN owners o ON o.id = i.owner_id GROUP BY i.id, k.name",
                Mode::Grouped,
            ),
            (
                "SELECT i.id FROM items i LEFT JOIN kinds k ON k.id = i.kind_id WHERE k.id IS NULL",
                Mode::Spj,
            ),
            ("SELECT COUNT(*) FROM items", Mode::Global),
            (
                "SELECT id, kind_id, ROW_NUMBER() OVER (PARTITION BY kind_id ORDER BY id) rn FROM items",
                Mode::Grouped,
            ),
            (
                "SELECT i.id, v.item_id, v.key_id, v.val FROM items i JOIN item_values v \
                 ON v.item_id = i.id",
                Mode::Spj,
            ),
            ("SELECT DISTINCT kind_id FROM items", Mode::Full("DISTINCT".into())),
        ];
        for (def, want) in defs {
            let v = plan(def);
            assert_eq!(v.mode, want, "{def}");
            let ap = AccessPlan::of(&v);
            for (i, o) in ap.occurrences.iter().enumerate() {
                let tracing = v.tracing();
                let label = match v.mode {
                    Mode::Full(_) | Mode::Global => "whole",
                    _ => tracing
                        .split(',')
                        .nth(i)
                        .and_then(|seg| seg.rsplit_once(':').map(|(_, h)| h))
                        .unwrap_or_else(|| {
                            panic!("{def}: tracing {tracing} lacks a label for occurrence {i}")
                        }),
                };
                let kind = match &o.path {
                    Path::Direct { .. } => "unit",
                    Path::Partial { .. } => "partial",
                    // A hop is a lineage for a lineage alias and a lookup for any other alias.
                    Path::Hop(_) => {
                        if v.lineage_of(i).is_some() {
                            "lineage"
                        } else {
                            "lookup"
                        }
                    }
                    Path::Lineage { .. } => "lineage",
                    Path::Untraced { .. } => "lookup",
                    Path::Whole => "whole",
                };
                assert_eq!(kind, label, "{def}: occurrence {i} ({})", o.alias);
            }
        }
    }

    #[test]
    fn one_hop_reaches_the_full_id() {
        // `l` joins to `i` by a non-unit column: its row image gives no id part, so the id is
        // reached through the link (`item_links` from `items`), one hop, the full id.
        let v = plan(
            "SELECT i.id, l.child_id FROM items i JOIN item_links l ON l.parent_id = i.kind_id",
        );
        let (aliases, links, positions) = chain(&v, 1).unwrap();
        assert_eq!(
            (aliases, links, positions),
            (vec![0], vec![(1, (0, 1))], vec![0])
        );
        let ap = AccessPlan::of(&v);
        match &ap.occurrences[1].path {
            Path::Hop(h) => {
                // One hop: `l` reaches `i`, the id's only alias; the full id is reached.
                assert_eq!(h.link_cols, vec![1]);
                assert_eq!(h.partner, vec![r#"`i`.`kind_id`"#.to_string()]);
                assert_eq!(h.positions, vec![0]);
                assert_eq!(h.aliases, vec![0]);
                assert!(
                    h.select.starts_with("SELECT DISTINCT `i`.`kind_id`"),
                    "{}",
                    h.select
                );
                assert!(
                    h.select.ends_with("WHERE `l`.`parent_id` = `i`.`kind_id`"),
                    "{}",
                    h.select
                );
                assert!(
                    h.select.contains("AS `l`, `db`.`items` AS `i`"),
                    "{}",
                    h.select
                );
            }
            other => panic!("expected a hop, got {other:?}"),
        }
    }

    #[test]
    fn two_hops_through_two_tables() {
        // The id is reached from `o` through two tables: `o` -> `item_links` -> `items`.
        let v = plan(
            "SELECT i.id, MAX(o.name) n FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
             JOIN owners o ON o.id = l.child_id GROUP BY i.id",
        );
        let ap = AccessPlan::of(&v);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        match &ap.occurrences[2].path {
            Path::Hop(h) => {
                assert_eq!(h.aliases, vec![0, 1]);
                assert_eq!(h.partner, vec![r#"`l`.`child_id`"#.to_string()]);
                assert_eq!(h.positions, vec![0]);
                assert!(
                    h.select.contains("`o`.`id` = `l`.`child_id`"),
                    "{}",
                    h.select
                );
                assert!(
                    h.select.contains("`l`.`parent_id` = `i`.`kind_id`"),
                    "{}",
                    h.select
                );
            }
            other => panic!("expected a hop, got {other:?}"),
        }
    }

    /// A synthetic plan (the live planner either reduces or fully rejects the shape): the
    /// chain from `l` reaches the `i.id` part but not the `o.id` part, which sits behind `o`'s
    /// own LEFT-join edge.
    fn partial_plan() -> ViewPlan {
        let alias = |i: usize, name: &str, t: Arc<TableInfo>| {
            let n = t.columns.len();
            crate::analyze::AliasPlan {
                name: name.into(),
                table: t,
                referenced: vec![true; n],
                membership: vec![true; n],
                direct_unit: None,
                image_cols: vec![],
                subst_from: (String::new(), String::new()),
                pos: (i, None),
                semi: None,
                lookup_where: false,
            }
        };
        let cat = catalog();
        let i = cat[&("db".into(), "items".into())].clone();
        let l = cat[&("db".into(), "item_links".into())].clone();
        let o = cat[&("db".into(), "owners".into())].clone();
        ViewPlan {
            schema: "db".into(),
            name: "v".into(),
            hash: "h".into(),
            mode: Mode::Grouped,
            tables: vec![],
            table_columns: vec![],
            aliases: vec![alias(0, "i", i), alias(1, "l", l), alias(2, "o", o)],
            projection: vec![],
            from_sql: String::new(),
            from_items: vec![],
            where_sql: None,
            group_sql: None,
            having_sql: None,
            unit_exprs: vec!["`i`.`id`".into(), "`o`.`id`".into()],
            unit_in_projection: vec![Some(0), Some(1)],
            full_select: String::new(),
            unit_unique: false,
            lineage: vec![],
            cascades: vec![],
            output_names: None,
            order_limit_sql: None,
            window_sql: None,
            lookup_where_sql: None,
            alias_filters: vec![vec![], vec![], vec![]],
            partial: crate::analyze::PartialUnits::default(),
            presence: vec![],
            edges: vec![
                // `l` joins to `i` (usable for every occurrence).
                Edge {
                    a: (1, 1),
                    b: (0, 1),
                    only_with: None,
                },
                // `o` is LEFT JOINed: its edge is usable for `o` only.
                Edge {
                    a: (2, 0),
                    b: (0, 2),
                    only_with: Some(2),
                },
            ],
            unit_refs: vec![vec![(0, 0)], vec![(2, 0)]],
        }
    }

    #[test]
    fn hops_reach_partial_positions() {
        let v = partial_plan();
        // The chain of `l` reaches `i` only: one of the two id parts.
        let (aliases, links, positions) = chain(&v, 1).unwrap();
        assert_eq!(
            (aliases, links, positions),
            (vec![0], vec![(1, (0, 1))], vec![0])
        );
        let ap = AccessPlan::of(&v);
        match &ap.occurrences[1].path {
            Path::Hop(h) => {
                assert_eq!(h.positions, vec![0]);
                assert_eq!(h.aliases, vec![0]);
            }
            other => panic!("expected a hop, got {other:?}"),
        }
    }

    #[test]
    fn left_join_owns_its_on_equality() {
        // `l`'s only usable edge is its own LEFT-join ON: it still hops.
        let v = plan("SELECT i.id, l.child_id FROM items i LEFT JOIN item_links l ON l.parent_id = i.kind_id");
        let ap = AccessPlan::of(&v);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        match &ap.occurrences[1].path {
            Path::Hop(h) => assert_eq!(h.partner, vec![r#"`i`.`kind_id`"#.to_string()]),
            other => panic!("expected a hop, got {other:?}"),
        }
    }

    #[test]
    fn semi_join_tables_do_not_hop() {
        // The semi table is not in the FROM: the analyzer keeps the negated, key-exact
        // semi-join as a semi alias (the positive, key-covered ones are joined in place).
        let d = parse_definition(
            "CREATE VIEW v AS SELECT i.id FROM items i WHERE i.id NOT IN (SELECT id FROM kinds k)",
        )
        .unwrap();
        let v = crate::analyze::plan(&d, "db", &catalog()).unwrap();
        let ap = AccessPlan::of(&v);
        let semi = ap
            .occurrences
            .iter()
            .find(|o| o.table == ("db".to_string(), "kinds".to_string()))
            .unwrap();
        let want = format!("`{}` is a semi-join table", semi.alias);
        match &semi.path {
            Path::Untraced { reason } => assert_eq!(reason, &want),
            other => panic!("path: {other:?}"),
        }
        match &ap.route {
            Route::ViewPlan { reason } => assert!(reason.ends_with(&want), "{reason}"),
            r => panic!("route: {r:?}"),
        }
    }

    #[test]
    fn collated_links_do_not_hop() {
        // A non-utf8-liked varchar is compared by collation: the binlog value cannot stand
        // for the SQL value, so the hop is refused.
        let latin = |n: &str| {
            crate::catalog::Column::new(
                n,
                "varchar",
                "varchar(10)",
                Some("latin1"),
                Some("latin1_swedish_ci"),
                None,
                false,
            )
        };
        let mk = |name: &str, cols: Vec<crate::catalog::Column>| {
            let t = crate::catalog::TableInfo {
                schema: "db".into(),
                name: name.into(),
                columns: cols,
                pk: vec![0],
                unique_keys: vec![],
                cascades: vec![],
                is_view: false,
                view_bases: vec![],
            };
            (("db".into(), name.into()), Arc::new(t))
        };
        let tables = std::collections::HashMap::from([
            mk("entities", vec![latin("id"), latin("code")]),
            mk("links", vec![latin("ref"), latin("id")]),
        ]);
        let d = parse_definition(
            "CREATE VIEW v AS SELECT e.id FROM entities e JOIN links l ON l.ref = e.id",
        )
        .unwrap();
        let v = plan_block(&d, "db", &tables).unwrap();
        let ap = AccessPlan::of(&v);
        match &ap.occurrences[1].path {
            Path::Untraced { reason } => assert_eq!(reason, "link compared by collation"),
            other => panic!("path: {other:?}"),
        }
    }

    /// Equivalent formulations of the same view plan identical EXPLAIN bodies.
    #[test]
    fn equivalent_formulations_same_body() {
        let defs = [
            "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
             JOIN owners o ON o.id = i.owner_id",
            "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id IN (i.kind_id) \
             JOIN owners o ON o.id IN (i.owner_id)",
            "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON i.kind_id = l.parent_id \
             JOIN owners o ON i.owner_id = o.id",
            "SELECT i.id, l.child_id, o.name FROM items i, item_links l, owners o \
             WHERE l.parent_id = i.kind_id AND o.id = i.owner_id",
            "SELECT i.id, l.child_id, o.name FROM items i, item_links l, owners o \
             WHERE o.id = i.owner_id AND l.parent_id = i.kind_id",
        ];
        let mut bodies = Vec::new();
        for def in defs {
            let v = plan(def);
            assert!(matches!(AccessPlan::of(&v).route, Route::Dedup), "{def}");
            bodies.push(AccessPlan::of(&v).body());
        }
        for b in &bodies[1..] {
            assert_eq!(b, &bodies[0], "differing body:\n{b}\n---\n{}", bodies[0]);
        }
    }

    /// The hop of an alias does not depend on where the tables sit in the FROM / JOIN order:
    /// link or partner tables first give the same paths, partner columns and id positions.
    #[test]
    fn permuted_from_orders_same_hops() {
        let defs = [
            "SELECT i.id, MAX(o.name) n, MAX(l.child_id) m FROM items i JOIN item_links l \
             ON l.parent_id = i.kind_id JOIN owners o ON o.id = i.owner_id GROUP BY i.id",
            "SELECT i.id, MAX(o.name) n, MAX(l.child_id) m FROM item_links l JOIN items i \
             ON i.kind_id = l.parent_id JOIN owners o ON o.id = i.owner_id GROUP BY i.id",
            "SELECT i.id, MAX(o.name) n, MAX(l.child_id) m FROM owners o JOIN items i \
             ON o.id = i.owner_id JOIN item_links l ON l.parent_id = i.kind_id GROUP BY i.id",
        ];
        for def in defs {
            let v = plan(def);
            let ap = AccessPlan::of(&v);
            assert!(matches!(ap.route, Route::Dedup), "{def}\n{ap}");
            let l = ap.occurrences.iter().find(|o| o.alias == "l").unwrap();
            let o = ap.occurrences.iter().find(|o| o.alias == "o").unwrap();
            match (&l.path, &o.path) {
                (Path::Hop(lh), Path::Hop(oh)) => {
                    assert_eq!(lh.link_cols, vec![1], "{def}");
                    assert_eq!(lh.partner, vec![r#"`i`.`kind_id`"#.to_string()], "{def}");
                    assert_eq!(lh.positions, vec![0], "{def}");
                    assert_eq!(oh.link_cols, vec![0], "{def}");
                    assert_eq!(oh.partner, vec![r#"`i`.`owner_id`"#.to_string()], "{def}");
                    assert_eq!(oh.positions, vec![0], "{def}");
                }
                other => panic!("{def}: {other:?}"),
            }
        }
    }

    /// A depth-3 chain: the id is reached from `o` through three tables
    /// (`o` -> `item_links` -> `item_values` -> `items`), the union of the one shortest path.
    #[test]
    fn three_hops_depth_three_chain() {
        let v = plan(
            "SELECT i.id, MAX(o.name) n FROM items i JOIN item_values v ON v.item_id = i.id \
             JOIN item_links l ON l.child_id = v.key_id JOIN owners o ON o.id = l.parent_id \
             GROUP BY i.id",
        );
        let ap = AccessPlan::of(&v);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        let o = ap.occurrences.iter().find(|o| o.alias == "o").unwrap();
        match &o.path {
            Path::Hop(h) => {
                assert_eq!(h.aliases, vec![0, 1, 2], "{ap}");
                assert_eq!(h.link_cols, vec![0], "{ap}");
                assert_eq!(h.partner, vec![r#"`l`.`parent_id`"#.to_string()], "{ap}");
                assert_eq!(h.positions, vec![0], "{ap}");
                assert!(
                    h.select.contains("`v`.`item_id` = `i`.`id`"),
                    "{}",
                    h.select
                );
                assert!(
                    h.select.contains("`l`.`child_id` = `v`.`key_id`"),
                    "{}",
                    h.select
                );
                assert!(
                    h.select.contains("`o`.`id` = `l`.`parent_id`"),
                    "{}",
                    h.select
                );
                assert!(
                    h.select.starts_with("SELECT DISTINCT `l`.`parent_id`"),
                    "{}",
                    h.select
                );
            }
            other => panic!("expected a hop, got {other:?}"),
        }
    }

    /// A view whose lookup aliases all hop is on the dedup route.
    #[test]
    fn all_hops_are_dedup() {
        let v = plan(
            "SELECT i.id, l.child_id, o.name FROM items i JOIN item_links l ON l.parent_id = i.kind_id \
             JOIN owners o ON o.id = i.owner_id",
        );
        let ap = AccessPlan::of(&v);
        assert!(matches!(ap.route, Route::Dedup), "{ap}");
        assert_eq!(ap.route_name(), "dedup");
        assert!(
            ap.occurrences[1..]
                .iter()
                .all(|o| matches!(o.path, Path::Hop(_))),
            "{ap}"
        );
    }
}
