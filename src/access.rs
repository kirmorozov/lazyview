//! The analyzer's public, per-source-table output; plain data; the copy set.
//! One projection per view definition ([`AccessPlan::of`]).

use crate::analyze::{definition_hash, int_sql, quote_ident, ConstFilter, Mode, ViewPlan};
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
    /// No clear derivation; `reason`.
    Untraced { reason: String },
    /// One unit (the `Recompute` route) or the whole view (the `Full` route).
    Whole,
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
                } else if let Some(k) = p.lineage_of(i) {
                    Path::Lineage {
                        cols: k.cols.clone(),
                    }
                } else {
                    Path::Untraced {
                        reason: "no hop derivation yet".into(),
                    }
                };
                let mut o = alias_occurrence(i, a);
                o.path = path;
                o
            })
            .collect(),
    }
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
                .find(|o| matches!(o.path, Path::Lineage { .. } | Path::Untraced { .. }))
            {
                Some(o) => Route::ViewPlan {
                    reason: format!(
                        "{} ({}): {}",
                        quote_ident(&o.alias),
                        quote_ident(&o.table.1),
                        match &o.path {
                            Path::Lineage { .. } => "lineage only: no hop derivation yet".into(),
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
        let ap = AccessPlan::of(&plan(
            "SELECT i.id, k.name FROM items i JOIN kinds k ON k.id = i.kind_id",
        ));
        match &ap.route {
            Route::ViewPlan { reason } => {
                assert!(reason.starts_with("`k` (`kinds`): "), "{reason}");
            }
            r => panic!("route: {r:?}"),
        }
        assert!(matches!(ap.occurrences[1].path, Path::Lineage { .. }));
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
                    Path::Lineage { .. } => "lineage",
                    Path::Untraced { .. } => "lookup",
                    Path::Whole => "whole",
                };
                assert_eq!(kind, label, "{def}: occurrence {i} ({})", o.alias);
            }
        }
    }
}
