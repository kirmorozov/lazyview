//! The analyzer's public, per-source-table output; plain data; the copy set.
//! One projection per view definition ([`AccessPlan::of`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use sqlparser::ast::{BinaryOperator, Expr, SetExpr};

use crate::analyze::{
    definition_hash, int_sql, is_create_statement, parse_definition, parse_select, quote_ident,
    referenced_tables, ConstFilter, Edge, Mode, ParsedDef, ViewPlan,
};
use crate::catalog::{ColKind, Column, FkAction, TableInfo, TableRef};
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

/// The SELECT reads the chain's tables only, not the occurrence's: the executor appends
/// `AND (<partner>) IN (<link values from the occurrence's images>)`, so a link row deleted or
/// moved in the same window still finds the ids its old value reached (the chain rows exist
/// unless their own, traced, events removed them).
#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub link_cols: Vec<usize>,
    pub partner: Vec<String>,
    /// The partner columns as (alias, column) ordinals, in `link_cols` order. The executor
    /// appends `AND (<partner>) IN (<link values>)`, single- or multi-column, built and
    /// checked against the server rules in 6R/6T.
    pub partner_cols: Vec<(usize, usize)>,
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

/// What the caller calls the target id: the view's unit, or output columns of the SELECT.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IdSpec<'a> {
    Unit,
    /// The named output columns; with `keys_marker`, the WHERE conjunct `<expr> IN (<marker>)` is
    /// removed before planning and `<expr>` must be an id column.
    Outputs {
        names: &'a [&'a str],
        keys_marker: Option<&'a str>,
    },
}

/// A planned statement: the comprehensive plan and its access plan.
#[derive(Debug, Clone)]
pub struct Planned {
    pub plan: ViewPlan,
    pub access: AccessPlan,
}

/// `sql` (a bare SELECT, or a CREATE VIEW) as a definition with `id`'s outputs, and the expression of
/// the removed keys-marker conjunct.
fn parsed(sql: &str, id: IdSpec<'_>) -> anyhow::Result<(ParsedDef, Option<Expr>)> {
    let mut def = if is_create_statement(sql) {
        parse_definition(sql)?
    } else {
        parse_select(sql, "select")?
    };
    let IdSpec::Outputs { names, keys_marker } = id else {
        return Ok((def, None));
    };
    def.id_outputs = names.iter().map(|s| s.to_string()).collect();
    let Some(m) = keys_marker else {
        return Ok((def, None));
    };
    let SetExpr::Select(sel) = &mut *def.query.body else {
        anyhow::bail!("the keys marker needs a plain SELECT");
    };
    // The WHERE's top-level `AND` conjuncts, left to right.
    let mut conjuncts: Vec<Expr> = Vec::new();
    let mut pending: Vec<Expr> = sel.selection.take().into_iter().collect();
    while let Some(e) = pending.pop() {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                pending.push(*right);
                pending.push(*left);
            }
            other => conjuncts.push(other),
        }
    }
    let at = conjuncts.iter().position(|c| {
        matches!(c, Expr::InList { list, negated: false, .. }
            if list.len() == 1 && list[0].to_string() == m)
    });
    let Some(at) = at else {
        anyhow::bail!("keys marker `{m}` not found as `<expr> IN ({m})` in the WHERE");
    };
    let marker = conjuncts.remove(at);
    // The checked expression is the marker's left side, `<expr>` of `<expr> IN (<m>)`.
    let Expr::InList { expr, .. } = marker else {
        anyhow::bail!("keys marker `{m}` not found as `<expr> IN ({m})` in the WHERE");
    };
    let marker = *expr;
    sel.selection = conjuncts.into_iter().reduce(|acc, e| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOperator::And,
        right: Box::new(e),
    });
    def.query_sql = def.query.to_string();
    Ok((def, Some(marker)))
}

/// Whether the marker expression `e` is a column equal to an id part on every row of `p`: its
/// alias's direct unit or partial columns hold it (names compared as identifiers).
fn marks_the_id(p: &ViewPlan, e: &Expr) -> bool {
    let (alias, col) = match e {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            (Some(parts[0].value.clone()), parts[1].value.clone())
        }
        Expr::Identifier(i) => (None, i.value.clone()),
        _ => return false,
    };
    let mut found: Option<(usize, usize)> = None;
    for (ai, a) in p.aliases.iter().enumerate() {
        if alias
            .as_ref()
            .is_some_and(|al| !al.eq_ignore_ascii_case(&a.name))
        {
            continue;
        }
        for (c, cd) in a.table.columns.iter().enumerate() {
            if !cd.name.eq_ignore_ascii_case(&col) {
                continue;
            }
            if found.is_some() {
                return false;
            }
            found = Some((ai, c));
        }
    }
    let Some((ai, c)) = found else { return false };
    p.aliases[ai]
        .direct_unit
        .as_ref()
        .is_some_and(|u| u.contains(&c))
        || p.partial_of(ai).is_some_and(|(_, cols)| cols.contains(&c))
}

/// Plans `sql` (a bare SELECT or a CREATE VIEW) with the target id `id`; `tables` must hold every
/// table [`access_tables`] lists. Every failure is an `Err`.
pub fn plan_access(
    sql: &str,
    id: IdSpec<'_>,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> anyhow::Result<Planned> {
    let (def, marker) = parsed(sql, id)?;
    let plan = crate::analyzer::plan(&def, default_schema, tables)?;
    let access = AccessPlan::of(&plan);
    let bad = marker
        .as_ref()
        .filter(|e| !matches!(plan.mode, Mode::Full(_)) && !marks_the_id(&plan, e));
    if let Some(e) = bad {
        anyhow::bail!("the keys marker filters `{e}`, which is not an id column");
    }
    Ok(Planned { plan, access })
}

/// The tables `plan_access` needs metadata for.
pub fn access_tables(
    sql: &str,
    id: IdSpec<'_>,
    default_schema: &str,
) -> anyhow::Result<Vec<TableRef>> {
    Ok(referenced_tables(&parsed(sql, id)?.0.query, default_schema))
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

/// The SELECT reads the chain's tables only, not the occurrence's: the executor appends
/// `AND (<partner>) IN (<link values from the occurrence's images>)`, so a link row deleted or
/// moved in the same window still finds the ids its old value reached (the chain rows exist
/// unless their own, traced, events removed them).
fn hop(p: &ViewPlan, y: usize) -> Result<Hop, String> {
    let c = search(p, y)?;
    // Executor (a) refreshes the full id or the ids of one partial set (`ViewPlan::partial`).
    if c.positions.len() != p.unit_exprs.len() && !p.partial.sets.contains(&c.positions) {
        return Err(format!(
            "the hop reaches id[{}], which no partial id has",
            ordinals(&c.positions)
        ));
    }
    // Both sides of every edge of the chain carry binlog values that compare as their SQL
    // values do; a collation-compared link would make the step-6 cache inexact.
    for &ei in &c.edges {
        let e = &p.edges[ei];
        if !p.aliases[e.a.0].table.columns[e.a.1].exact_key()
            || !p.aliases[e.b.0].table.columns[e.b.1].exact_key()
        {
            return Err("link compared by collation".into());
        }
        if (e.a.0 == y || e.b.0 == y)
            && !same_link_type(
                &p.aliases[e.a.0].table.columns[e.a.1],
                &p.aliases[e.b.0].table.columns[e.b.1],
            )
        {
            return Err("link columns of different types".into());
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
    let from: Vec<String> = c
        .aliases
        .iter()
        .copied()
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
        .filter(|&&ei| p.edges[ei].a.0 != y && p.edges[ei].b.0 != y)
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
        partner_cols: c.links.iter().map(|&(_, pc)| pc).collect(),
        positions: c.positions,
        aliases: c.aliases,
        select: format!(
            "SELECT DISTINCT {} FROM {} WHERE {}",
            sel.join(", "),
            from.join(", "),
            if where_parts.is_empty() {
                "TRUE".to_string()
            } else {
                where_parts.join(" AND ")
            }
        ),
    })
}

/// Whether a link column and its partner decode to equal datums for equal SQL values (the hop
/// cache keys answers by the partner's query values and looks them up by the link's binlog
/// values): the same decoded kind (the binlog decoder's [`ColKind`], `unsigned` included),
/// never JSON, the BLOB family or the spatial / opaque types.
fn same_link_type(a: &Column, b: &Column) -> bool {
    const INTS: [&str; 6] = [
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
    ];
    // The decoded kind decides: a signed and an unsigned integer are different kinds, so an
    // `int` vs `int unsigned` pair is not one type class.
    if a.kind != b.kind {
        return false;
    }
    // JSON, BLOB-family and spatial / opaque columns never key the hop cache: their binlog and
    // query representations differ (or cannot be bound back at all), so such a link keeps the
    // viewplan route and the JSON key-part rule cannot arise.
    if matches!(a.kind, ColKind::Json | ColKind::Binary | ColKind::Opaque) {
        return false;
    }
    let int = |c: &Column| match &c.kind {
        ColKind::Plain => INTS.iter().any(|t| c.data_type.eq_ignore_ascii_case(t)),
        ColKind::Unsigned(_) => true,
        _ => false,
    };
    (int(a) && int(b)) || a.data_type.eq_ignore_ascii_case(&b.data_type)
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
mod tests;
