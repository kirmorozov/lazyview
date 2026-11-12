use super::*;
use super::{columns::*, expr::*, parse::*, plan::*, routes::*, vocab::*};

/// Plans a definition: the query analyzer resolves CTEs, derived tables and subqueries and
/// normalizes them into flat blocks it plans with [`plan_block`] (see `crate::analyzer`).
/// `tables` must contain metadata for every table [`referenced_tables`] lists.
pub fn plan(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<ViewPlan> {
    crate::analyzer::plan(def, default_schema, tables)
}

/// The block planner: plans one flat block (no CTE references or derived tables left).
/// `tables` must contain metadata for every table the query reads.
pub fn plan_block(
    def: &ParsedDef,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<ViewPlan> {
    let schema = def
        .schema
        .clone()
        .unwrap_or_else(|| default_schema.to_string());
    let lname = def.name.to_ascii_lowercase();
    if lname.ends_with("__lv_new")
        || lname.ends_with("__lv_old")
        || lname.starts_with("_lv_")
        || METADATA_TABLES.contains(&lname.as_str())
    {
        bail!(
            "view name `{}` is reserved by lazyview (names starting with `_lv_`, ending with `__lv_new`/`__lv_old`, or lazyview's metadata tables)",
            def.name
        );
    }
    let all_tables = referenced_tables(&def.query, default_schema);
    for t in &all_tables {
        if METADATA_TABLES.iter().any(|m| m.eq_ignore_ascii_case(&t.1)) {
            bail!("lazyview metadata table `{}` cannot be a view source", t.1);
        }
        if reserved(&t.1) {
            bail!("reserved table `{}` cannot be a view source", t.1);
        }
        if !tables.contains_key(t) {
            bail!("table `{}`.`{}` does not exist", t.0, t.1);
        }
    }
    let output_names = (!def.columns.is_empty()).then(|| def.columns.clone());
    // The definition as written; the incremental plans below add what the inlined VIRTUAL columns
    // carry. Never a reason to reject, only to bind string parameters the other way.
    let has_explicit_collate = has_collate(&def.query);
    // Full rebuilds run the statement exactly as written (ORDER BY matters with LIMIT).
    let full_select = full_select_sql(def, default_schema, tables)?;
    let full_with = |reason: &str, watched: Vec<TableRef>, columns| ViewPlan {
        schema: schema.clone(),
        name: def.name.clone(),
        hash: def.hash.clone(),
        mode: Mode::Full(reason.to_string()),
        cascades: cascade_routes(
            &watched
                .iter()
                .filter_map(|t| tables.get(t).map(|i| (i.clone(), None)))
                .collect::<Vec<_>>(),
            tables,
        ),
        tables: watched,
        table_columns: columns,
        aliases: Vec::new(),
        projection: Vec::new(),
        from_sql: String::new(),
        from_items: Vec::new(),
        where_sql: None,
        group_sql: None,
        having_sql: None,
        unit_exprs: Vec::new(),
        unit_in_projection: Vec::new(),
        full_select: full_select.clone(),
        order_limit_sql: None,
        window_sql: None,
        lookup_where_sql: None,
        alias_filters: Vec::new(),
        partial: PartialUnits::default(),
        presence: Vec::new(),
        unit_unique: false,
        lineage: Vec::new(),
        output_names: output_names.clone(),
        edges: Vec::new(),
        unit_refs: Vec::new(),
        has_explicit_collate,
    };
    let full = |reason: &str| {
        let columns = read_columns(&def.query, &all_tables, tables);
        full_with(reason, all_tables.clone(), columns)
    };

    // A SQL view has no binlog rows of its own: watch its base tables, rebuild on change.
    if let Some(v) = all_tables.iter().find(|t| tables[*t].is_view) {
        // Which base columns the SQL view reads is not known here: any change counts.
        let bases = expand_views(&all_tables, tables);
        let columns = vec![None; bases.len()];
        return Ok(full_with(
            &format!("reads SQL view `{}`", v.1),
            bases,
            columns,
        ));
    }

    let mut query = def.query.clone();
    let mut order_by = query.order_by.take();
    if query.with.is_some() {
        return Ok(full("WITH clause"));
    }
    // ORDER BY … LIMIT n: the first n rows are one unit, recomputed with one bounded query per
    // batch. Without ORDER BY the rows are not a function of the data: nothing to converge to.
    let top_n_limit = match (&order_by, query.limit_clause.take(), query.fetch.is_some()) {
        (_, None, false) => None,
        (Some(_), Some(l), false) => Some(l),
        _ => return Ok(full("LIMIT without ORDER BY")),
    };
    let select: &mut Select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        // C4: name what blocks a top-N over a set operation (refreshed in full: tracing it
        // would need an internal view of the union, shelved).
        SetExpr::SetOperation {
            op, set_quantifier, ..
        } if top_n_limit.is_some() => {
            let all = matches!(
                set_quantifier,
                sqlparser::ast::SetQuantifier::All | sqlparser::ast::SetQuantifier::AllByName
            );
            let op = match op {
                sqlparser::ast::SetOperator::Union if all => "UNION ALL",
                sqlparser::ast::SetOperator::Union => "UNION",
                sqlparser::ast::SetOperator::Intersect => "INTERSECT",
                _ => "EXCEPT",
            };
            return Ok(full(&format!("ORDER BY … LIMIT over a {op}")));
        }
        _ => return Ok(full("set operation (UNION/INTERSECT/EXCEPT)")),
    };
    if select.distinct.is_some() {
        return Ok(full("DISTINCT"));
    }
    if select.from.is_empty() {
        return Ok(full("no FROM clause"));
    }
    // `*` over USING / NATURAL joins lists the shared columns once (A4).
    let expanded_star = expand_star_over_using(select, default_schema, tables)?;
    let star_items: Vec<Option<&str>> = if expanded_star {
        // Expanded items have no source text of their own.
        Vec::new()
    } else {
        def.item_sql.iter().map(|s| s.as_deref()).collect()
    };

    // FROM: tables and joins we know how to maintain.
    let mut aliases: Vec<(String, Arc<TableInfo>)> = Vec::new();
    let mut alias_pos: Vec<(usize, Option<usize>)> = Vec::new();
    // (alias index of the joined table, is_left_join, constraint)
    let mut joins: Vec<(usize, bool, JoinConstraint)> = Vec::new();
    let mut unsupported = None;
    let add_factor = |f: &TableFactor,
                      pos: (usize, Option<usize>),
                      aliases: &mut Vec<(String, Arc<TableInfo>)>,
                      alias_pos: &mut Vec<(usize, Option<usize>)>|
     -> Option<String> {
        match f {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let t = object_table_ref(name, default_schema)?;
                let info = tables.get(&t)?.clone();
                let n = alias
                    .as_ref()
                    .map(|a| ident_name(&a.name))
                    .unwrap_or_else(|| t.1.clone());
                aliases.push((n, info));
                alias_pos.push(pos);
                None
            }
            TableFactor::Derived { .. } => Some("derived table in FROM".into()),
            _ => Some("unsupported FROM item".into()),
        }
    };
    for (ti, twj) in select.from.iter().enumerate() {
        if let Some(r) = add_factor(&twj.relation, (ti, None), &mut aliases, &mut alias_pos) {
            unsupported.get_or_insert(r);
        }
        for (ji, j) in twj.joins.iter().enumerate() {
            let (left, c) = match &j.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    (false, c.clone())
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (true, c.clone()),
                _ => {
                    unsupported.get_or_insert("RIGHT/FULL/other join".into());
                    continue;
                }
            };
            if matches!(c, JoinConstraint::Natural) {
                unsupported.get_or_insert("NATURAL join".into());
            }
            if let Some(r) = add_factor(&j.relation, (ti, Some(ji)), &mut aliases, &mut alias_pos) {
                unsupported.get_or_insert(r);
            } else {
                joins.push((aliases.len() - 1, left, c));
            }
        }
    }
    if select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)))
        && joins
            .iter()
            .any(|(_, _, c)| matches!(c, JoinConstraint::Using(_) | JoinConstraint::Natural))
    {
        return Ok(full(STAR_USING));
    }
    if let Some(r) = unsupported {
        return Ok(full(&r));
    }

    // Expand wildcards so the projection order is explicit.
    let mut projection = Vec::new();
    // Source text of each projection item (none for wildcard expansions).
    let mut item_sql: Vec<Option<&str>> = Vec::new();
    for (ii, item) in std::mem::take(&mut select.projection)
        .into_iter()
        .enumerate()
    {
        match item {
            SelectItem::Wildcard(_) => {
                for (n, t) in &aliases {
                    for c in t.columns.iter().filter(|c| !c.invisible) {
                        projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                            qident(n),
                            qident(&c.name),
                        ])));
                        item_sql.push(None);
                    }
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(on), _) => {
                let q = match on.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => ident_name(i),
                    _ => bail!("bad qualified wildcard"),
                };
                let (n, t) = aliases
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&q))
                    .ok_or_else(|| anyhow!("unknown table {q}.*"))?;
                for c in t.columns.iter().filter(|c| !c.invisible) {
                    projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                        qident(n),
                        qident(&c.name),
                    ])));
                    item_sql.push(None);
                }
            }
            SelectItem::QualifiedWildcard(..) => bail!("unsupported wildcard"),
            other => {
                projection.push(other);
                item_sql.push(star_items.get(ii).copied().flatten());
            }
        }
    }
    if projection.is_empty() {
        bail!("the SELECT has no columns");
    }
    // Output names the server will give (aliases and plain columns; other expressions get
    // server-generated names that `names.rs` keeps unique): a duplicate fails CREATE TABLE,
    // so it fails here, by name (L3). A column list renames everything positionally.
    if def.columns.is_empty() {
        let mut seen: Vec<String> = Vec::new();
        for item in &projection {
            let name = match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(ident_name(alias)),
                SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(ident_name(id)),
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    parts.last().map(ident_name)
                }
                _ => None,
            };
            if let Some(n) = name {
                if seen.iter().any(|s| s.eq_ignore_ascii_case(&n)) {
                    bail!(
                        "duplicate column name `{n}` in the SELECT (the server refuses such a view): alias one of them"
                    );
                }
                seen.push(n);
            }
        }
    }
    if !def.columns.is_empty() && def.columns.len() != projection.len() {
        bail!(
            "view column list has {} names but SELECT returns {} columns",
            def.columns.len(),
            projection.len()
        );
    }
    select.projection = projection;

    let scope = Scope { aliases: &aliases };
    strip_column_schemas(select, &scope);
    // VIRTUAL generated columns have no value in the row images (lookups substitute literal
    // rows): every reference becomes the column's own expression over its base columns, before
    // anything (window partitions, units) is derived from the expressions.
    // An unaliased expression over a VIRTUAL column is named after its text by the server; once
    // inlined it carries that name as an alias (a GROUP BY sees the same inlined text, which
    // MySQL's ONLY_FULL_GROUP_BY compares).
    for (item, src) in select.projection.iter_mut().zip(item_sql.iter_mut()) {
        let SelectItem::UnnamedExpr(e) = item else {
            continue;
        };
        if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            continue;
        }
        let mut reads_virtual = false;
        let _ = visit_expressions(&*e, |x: &Expr| {
            if let Some((a, c)) = scope.resolve(x) {
                if scope.aliases[a].1.columns[c]
                    .generated
                    .as_ref()
                    .is_some_and(|g| !g.stored)
                {
                    reads_virtual = true;
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        });
        if !reads_virtual {
            continue;
        }
        match src.filter(|t| t.chars().count() <= 64 && !t.ends_with(' ') && !t.contains('\n')) {
            Some(t) => {
                *item = SelectItem::ExprWithAlias {
                    expr: e.clone(),
                    alias: qident(t),
                };
                *src = None;
            }
            None => {
                return Ok(full(&format!(
                    "unaliased column `{e}` reads a VIRTUAL generated column: give it an alias"
                )))
            }
        }
    }
    let top_n_order = if top_n_limit.is_some() {
        order_by.as_mut()
    } else {
        None
    };
    if let Some(reason) = inline_virtual_columns(select, top_n_order, &scope)? {
        return Ok(full(&reason));
    }
    // Top-N rows are chosen by the ORDER BY: its columns (also of joined aliases, also as
    // aggregate inputs) are read columns, whether or not they are projected (L1).
    let order_usage = order_by.clone();
    let top_n = top_n_limit.map(|l| format!("{}{l}", order_by.as_ref().unwrap()));
    let (reason, has_agg, windows) = scan_exprs(select);
    if let Some(r) = reason {
        return Ok(full(&r));
    }
    // The ORDER BY of a top-N view decides which rows it holds: a value that changes without a
    // source change (RAND(), NOW(), …; also inside its subqueries) leaves nothing to trace.
    if let (Some(o), Some(_)) = (&order_by, &top_n) {
        let mut why = None;
        let _ = visit_expressions(o, |e: &Expr| {
            // (Not a filter context: no exact NOT EXISTS form, and MariaDB re-uses a stale NOT
            // IN result when the refresh statement is re-executed, F5.)
            if matches!(e, Expr::InSubquery { negated: true, .. }) {
                why = Some(
                    "NOT IN subquery in the ORDER BY of a top-N view (MariaDB re-executes it with a stale result): write NOT EXISTS"
                        .to_string(),
                );
                return ControlFlow::Break(());
            }
            if let Expr::Function(f) = e {
                let n = function_name(f);
                if f.over.is_none() && NONDETERMINISTIC.contains(&n.as_str()) {
                    why = Some(format!(
                        "non-deterministic function {n} in the ORDER BY of a top-N view"
                    ));
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        });
        if let Some(r) = why {
            return Ok(full(&r));
        }
    }
    // A window's values depend on its whole partition: the unit is the PARTITION BY expressions
    // every window shares (filtering on them keeps whole partitions); without any, one unit.
    let windowed = !windows.is_empty();
    let partition: Vec<Expr> = match windows.split_first() {
        Some((first, rest)) => first
            .iter()
            .filter(|e| {
                rest.iter()
                    .all(|w| w.iter().any(|x| expr_key(x) == expr_key(e)))
            })
            .cloned()
            .collect(),
        None => Vec::new(),
    };

    let n_alias = aliases.len();
    let referenced: Vec<Vec<bool>> = aliases
        .iter()
        .map(|(_, t)| vec![false; t.columns.len()])
        .collect();
    let membership = referenced.clone();

    let proj_exprs: Vec<(&Expr, Option<String>)> = select
        .projection
        .iter()
        .map(|i| match i {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(ident_name(alias))),
            _ => unreachable!(),
        })
        .collect();

    // Column usage. Unresolvable identifiers in GROUP BY / HAVING may be select aliases.
    let mut usage = Usage {
        scope: &scope,
        select_aliases: proj_exprs.iter().filter_map(|(_, a)| a.clone()).collect(),
        referenced,
        membership,
        unresolved: None,
    };
    usage.mark(&select.projection, false, false);
    usage.mark(&select.selection, true, false);
    for (_, _, c) in &joins {
        if let JoinConstraint::On(e) = c {
            usage.mark(e, true, false);
        }
    }
    usage.mark(&select.having, false, true);
    usage.mark(&select.named_window, false, false);
    // (ORDER BY without LIMIT does not decide which rows the view holds.)
    if let (Some(o), Some(_)) = (&order_usage, &top_n) {
        if let OrderByKind::Expressions(es) = &o.kind {
            for e in es {
                if has_subquery(&e.expr) {
                    // In a subquery a name resolves in its own FROM first: only the view's
                    // columns it names are marked (a name of both marks one column too many,
                    // never too few); its tables are traced below.
                    usage.mark_known(&e.expr);
                } else {
                    usage.mark(&e.expr, false, true);
                }
            }
        }
    }
    for e in &partition {
        usage.mark(e, true, false);
    }

    // USING (col): the column of the joined table and the first earlier table having it.
    let mut using_pairs: Vec<(usize, ColRef, ColRef)> = Vec::new();
    for (ja, _, c) in &joins {
        if let JoinConstraint::Using(cols) = c {
            for on in cols {
                let col = match on.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => ident_name(i),
                    _ => bail!("bad USING column"),
                };
                let rc = aliases[*ja]
                    .1
                    .col_index(&col)
                    .ok_or_else(|| anyhow!("USING column {col} not found"))?;
                let (la, lc) = (0..*ja)
                    .find_map(|a| aliases[a].1.col_index(&col).map(|c| (a, c)))
                    .ok_or_else(|| anyhow!("USING column {col} not found"))?;
                for (a, c) in [(*ja, rc), (la, lc)] {
                    usage.referenced[a][c] = true;
                    usage.membership[a][c] = true;
                }
                using_pairs.push((*ja, (*ja, rc), (la, lc)));
            }
        }
    }

    // GROUP BY: resolve ordinals and select aliases to the underlying expressions.
    let group_exprs: Vec<Expr> = match &select.group_by {
        GroupByExpr::Expressions(es, mods) if mods.is_empty() => es.clone(),
        GroupByExpr::Expressions(..) => return Ok(full("GROUP BY modifiers (ROLLUP)")),
        GroupByExpr::All(_) => return Ok(full("GROUP BY ALL")),
    };
    let mut resolved_groups = Vec::new();
    for g in &group_exprs {
        let r = match g {
            Expr::Value(v) if matches!(v.value, Value::Number(..)) => {
                let n: usize = v.value.to_string().parse().context("GROUP BY ordinal")?;
                proj_exprs
                    .get(n.wrapping_sub(1))
                    .map(|(e, _)| (*e).clone())
                    .ok_or_else(|| anyhow!("GROUP BY {n} out of range"))?
            }
            Expr::Identifier(id) if scope.resolve(g).is_none() => proj_exprs
                .iter()
                .find(|(_, a)| {
                    a.as_ref()
                        .map(|a| a.eq_ignore_ascii_case(&ident_name(id)))
                        .unwrap_or(false)
                })
                .map(|(e, _)| (*e).clone())
                .ok_or_else(|| anyhow!("cannot resolve GROUP BY {id}"))?,
            other => other.clone(),
        };
        resolved_groups.push(r);
    }
    for g in &resolved_groups {
        usage.mark(g, true, false);
    }
    if let Some(u) = usage.unresolved {
        bail!("cannot resolve column {u}");
    }
    let Usage {
        mut referenced,
        mut membership,
        ..
    } = usage;

    let mode = if top_n.is_some()
        || windowed && (partition.is_empty() || has_agg || !resolved_groups.is_empty())
    {
        Mode::Global
    } else if windowed || !resolved_groups.is_empty() {
        Mode::Grouped
    } else if has_agg {
        Mode::Global
    } else {
        Mode::Spj
    };

    // Units.
    // The caller's id (`ParsedDef::id_outputs`) replaces the unit; a single-row aggregate has none.
    let chosen = if def.id_outputs.is_empty() {
        None
    } else if matches!(mode, Mode::Global) {
        bail!("a single-row aggregate has no id outputs");
    } else {
        let keys = match mode {
            Mode::Grouped if windowed => Some(partition.as_slice()),
            Mode::Grouped => Some(resolved_groups.as_slice()),
            _ => None,
        };
        Some(chosen_id(def, &proj_exprs, keys, &scope)?)
    };
    let unit_refs: Vec<Expr> = match mode {
        _ if chosen.is_some() => {
            let c = chosen.clone().unwrap();
            for e in &c {
                for (a, col) in col_refs(e, &scope) {
                    referenced[a][col] = true;
                    membership[a][col] = true;
                }
            }
            c
        }
        Mode::Spj => {
            let (n, t) = &aliases[0];
            // Without a row key (primary key or a UNIQUE NOT NULL key), equal rows are one
            // unit: the referenced columns whose binlog values compare exactly with their SQL
            // values.
            let key: Vec<usize> = match t.row_key() {
                Some(k) => k.to_vec(),
                None => (0..t.columns.len())
                    .filter(|&c| referenced[0][c] && t.columns[c].exact_key())
                    .collect(),
            };
            if key.is_empty() {
                return Ok(full(&format!(
                    "first table `{}` has no primary key",
                    t.name
                )));
            }
            key.iter()
                .map(|&c| {
                    referenced[0][c] = true;
                    membership[0][c] = true;
                    Expr::CompoundIdentifier(vec![qident(n), qident(&t.columns[c].name)])
                })
                .collect()
        }
        Mode::Grouped if windowed => partition.clone(),
        Mode::Grouped => resolved_groups.clone(),
        _ => Vec::new(),
    };
    // Unit keys and lookup images of opaque types would be compared as different values.
    if matches!(mode, Mode::Spj | Mode::Grouped) {
        for (a, (_, t)) in aliases.iter().enumerate() {
            if let Some(c) = (0..t.columns.len())
                .find(|&c| membership[a][c] && t.columns[c].kind == ColKind::Opaque)
            {
                return Ok(full(&format!(
                    "{} column `{}`.`{}` decides which rows match, but its binlog value cannot be compared with its SQL value",
                    t.columns[c].data_type.to_ascii_uppercase(),
                    t.name,
                    t.columns[c].name
                )));
            }
        }
    }
    // Equality classes valid for rows that involve a given alias.
    let mut base_edges = Vec::new();
    if let Some(w) = &select.selection {
        base_edges.extend(equalities(w, &scope));
    }
    let mut own_edges: Vec<Vec<(ColRef, ColRef)>> = vec![Vec::new(); n_alias];
    for (ja, left, c) in &joins {
        let eqs = match c {
            JoinConstraint::On(e) => equalities(e, &scope),
            _ => Vec::new(),
        };
        if *left {
            // Valid only where the joined table is non-null and only if it participates.
            own_edges[*ja].extend(eqs.into_iter().filter(|(x, y)| x.0 == *ja || y.0 == *ja));
        } else {
            base_edges.extend(eqs);
        }
    }
    for (ja, l, r) in &using_pairs {
        let left_join = joins.iter().any(|(a, left, _)| a == ja && *left);
        if left_join {
            own_edges[*ja].push((*l, *r));
        } else {
            base_edges.push((*l, *r));
        }
    }

    // A group or partition key that the rest of the key determines adds nothing to the unit (the
    // partitions are the same sets).
    let unit_refs = if mode == Mode::Grouped && chosen.is_none() {
        reduce_unit(unit_refs, &scope, &base_edges, &own_edges)
    } else {
        unit_refs
    };
    // The plan's equality edges (the WHERE / inner ON, plus each LEFT-joined alias's own ON)
    // and the column refs of the reduced units: the hop paths of `crate::access` walk them.
    let edges: Vec<Edge> = base_edges
        .iter()
        .map(|&(a, b)| Edge {
            a,
            b,
            only_with: None,
        })
        .chain(own_edges.iter().enumerate().flat_map(|(ja, es)| {
            es.iter().map(move |&(a, b)| Edge {
                a,
                b,
                only_with: Some(ja),
            })
        }))
        .collect();
    let unit_refs_cols: Vec<Vec<ColRef>> = unit_refs.iter().map(|e| col_refs(e, &scope)).collect();
    // Units and WHERE are evaluated on literal row images in lookups; that is only sound when the
    // NULL-extended rows of LEFT JOINs cannot change them.
    let null_side: Vec<usize> = joins
        .iter()
        .filter(|(_, left, _)| *left)
        .map(|(a, _, _)| *a)
        .collect();
    let on_null_side = |e: &Expr| {
        col_refs(e, &scope)
            .iter()
            .any(|(a, _)| null_side.contains(a))
    };
    if unit_refs.iter().any(&on_null_side) {
        return Ok(full("group key on the NULL side of a LEFT JOIN"));
    }
    // A WHERE conjunct that keeps NULL-extended rows (`c.id IS NULL`, COALESCE, OR) cannot be
    // evaluated on a substituted row of its NULL-side table: lookups of that table leave it out
    // (a superset of the affected units) and the refresh evaluates it. Such a table is traced
    // by lookups for both images: rows it stops hiding were never materialized (no lineage).
    let mut weak: Vec<usize> = Vec::new();
    let mut lookup_where: Option<Vec<String>> = None;
    if let Some(w) = &select.selection {
        let mut cs = Vec::new();
        conjuncts(w, &mut cs);
        if cs.iter().any(|c| on_null_side(c) && !null_rejecting(c)) {
            let mut kept = Vec::new();
            for c in cs {
                if on_null_side(c) && !null_rejecting(c) {
                    for (a, _) in col_refs(c, &scope) {
                        if null_side.contains(&a) && !weak.contains(&a) {
                            weak.push(a);
                        }
                    }
                } else {
                    kept.push(format!("({c})"));
                }
            }
            lookup_where = Some(kept);
        }
    }
    // JSON unit keys: a JSON column is a plain unit (the engine binds it as `CAST(? AS JSON)`,
    // typed by the materialized column, on MySQL; MariaDB JSON is text). A JSON value computed by
    // a function is not: MariaDB compares it as JSON (`JSON_EXTRACT(d, '$.a') = '"x"'` is false
    // for "x") while its unit filter binds the text, so its groups would be refreshed empty.
    if let Some(u) = unit_refs.iter().find(|u| json_computed(u)) {
        return Ok(full(&format!(
            "group key `{u}` is a JSON value computed by a function (MariaDB compares it as JSON, its unit filter as text): group by the column, or by its text with ->> / JSON_UNQUOTE"
        )));
    }
    // A JSON value in a composite unit (G5): MySQL compares JSON by value in `doc = ?` and in a
    // one-row `(g, doc) IN ((?, ?))`, but by representation in a row IN list of two or more
    // tuples, the engine's filter for several units, so a refresh would miss JSON-equal rows.
    // (MariaDB JSON is LONGTEXT: its columns are not of type json.)
    if unit_refs.len() > 1 {
        let json_col = |x: &Expr| {
            scope
                .resolve(x)
                .is_some_and(|(a, c)| aliases[a].1.columns[c].data_type == "json")
        };
        if let Some(u) = unit_refs.iter().find(|u| json_valued(u, &json_col)) {
            return Ok(full(&format!(
                "JSON value `{}` in a composite group key: MySQL compares JSON by representation in multi-row IN lists",
                u.to_string().replace('`', "")
            )));
        }
    }
    // (BIT unit keys: binlog values become unsigned integers and are bound as such; both sides
    // of every comparison see the same number.)
    let unit_cols: Vec<Option<ColRef>> = unit_refs.iter().map(|e| scope.resolve(e)).collect();
    let unit_exprs: Vec<String> = unit_refs.iter().map(|e| e.to_string()).collect();
    let unit_in_projection: Vec<Option<usize>> = unit_refs
        .iter()
        .zip(&unit_cols)
        .map(|(u, uc)| {
            proj_exprs.iter().position(|(e, _)| match uc {
                Some(uc) => scope.resolve(e) == Some(*uc),
                None => expr_key(e) == expr_key(u),
            })
        })
        .collect();

    let from_sql = select
        .from
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mut alias_plans = Vec::new();
    for (ai, (n, t)) in aliases.iter().enumerate() {
        let direct_unit = if unit_cols.is_empty() || unit_cols.iter().any(|u| u.is_none()) {
            None
        } else {
            let mut cls = Classes::new();
            for (x, y) in base_edges.iter().chain(&own_edges[ai]) {
                cls.union(*x, *y);
            }
            unit_cols
                .iter()
                .map(|u| {
                    let u = u.unwrap();
                    if !aliases[u.0].1.columns[u.1].exact_key() {
                        return None;
                    }
                    if u.0 == ai {
                        return Some(u.1);
                    }
                    let ru = cls.find(u);
                    (0..t.columns.len())
                        .find(|&c| t.columns[c].exact_key() && cls.find((ai, c)) == ru)
                })
                .collect::<Option<Vec<usize>>>()
        };
        let image_cols: Vec<usize> = (0..t.columns.len())
            .filter(|&c| membership[ai][c])
            .collect();
        let subst_from = render_substituted_from(&select.from, alias_pos[ai], n)?;
        alias_plans.push(AliasPlan {
            name: n.clone(),
            table: t.clone(),
            referenced: referenced[ai].clone(),
            membership: membership[ai].clone(),
            direct_unit,
            image_cols,
            subst_from,
            pos: alias_pos[ai],
            semi: None,
            lookup_where: weak.contains(&ai),
        });
    }

    if let Some(a) = alias_plans
        .iter()
        .find(|a| a.image_cols.is_empty() && a.direct_unit.is_none())
    {
        if matches!(mode, Mode::Spj | Mode::Grouped) {
            return Ok(full(&format!(
                "table `{}` contributes to every row (no join condition)",
                a.table.name
            )));
        }
    }
    // Lineage (select/join views): a lookup-resolved table is traced to the materialized rows
    // its rows are part of through hidden key columns: its primary key or, without one, the
    // whole row (equal duplicates belong to the same materialized rows). Its images carry only
    // that key, and an UPDATE of a key column must refresh them (the hidden columns change).
    // Only keys whose binlog values compare exactly with their SQL values qualify; others keep
    // looking up images joined with the current state.
    let mut lineage = Vec::new();
    if mode == Mode::Spj {
        for (ai, a) in alias_plans.iter_mut().enumerate() {
            let key: Vec<usize> = match a.table.row_key() {
                Some(k) => k.to_vec(),
                None => (0..a.table.columns.len()).collect(),
            };
            if a.direct_unit.is_some()
                || a.lookup_where
                || key.is_empty()
                || !key.iter().all(|&c| a.table.columns[c].exact_key())
            {
                continue;
            }
            for &c in &key {
                a.referenced[c] = true;
            }
            lineage.push(LineageKey {
                alias: ai,
                cols: (0..key.len()).map(|j| format!("_lv_a{ai}_pk{j}")).collect(),
                exprs: key
                    .iter()
                    .map(|&c| {
                        Expr::CompoundIdentifier(vec![
                            qident(&a.name),
                            qident(&a.table.columns[c].name),
                        ])
                        .to_string()
                    })
                    .collect(),
            });
            a.image_cols = key;
        }
    }
    let alias_filters = const_filters(select, &joins, &scope, &base_edges, &own_edges);
    let partial = partial_units(
        &alias_plans,
        &lineage,
        &unit_cols,
        &scope,
        &base_edges,
        &own_edges,
    );
    // A chosen id is the caller's: not reduced, and not known to be unique.
    let unit_unique = chosen.is_none()
        && match mode {
            Mode::Spj => n_alias == 1 && aliases[0].1.row_key().is_some(),
            Mode::Grouped => !windowed,
            _ => false,
        };
    // Tables read by subqueries in the ORDER BY of a top-N view: a change of a column they name
    // recomputes it, like a change of its own tables (cascades into them too).
    let order_tables = match (&order_usage, &top_n) {
        (Some(o), Some(_)) => order_subquery_tables(o, default_schema, tables),
        _ => Vec::new(),
    };
    let mut presence: Vec<Presence> = Vec::new();
    let mut cascades = cascade_routes(
        &alias_plans
            .iter()
            .map(|a| (a.table.clone(), a.direct_unit.clone()))
            .chain(order_tables.iter().map(|(t, _)| (t.clone(), None)))
            .collect::<Vec<_>>(),
        tables,
    );
    // Cascades the parent's own rows trace: the view reads the parent through an alias whose
    // changed rows map to units directly or by lineage, joined on the whole FK (= the parent's
    // primary key) to every alias of the child so that a child row's presence implies that
    // alias carries its parent (`cascade_joined`). The child rows a cascade changes are exactly
    // those with the changed parent row, so every materialized row holding one is found through
    // it. A cascade that continues into the child's own children is found the same way only if
    // every further level is joined like that too.
    let table_ref = |t: &TableInfo| (t.schema.clone(), t.name.clone());
    let joined: Vec<Result<(), String>> = cascades
        .iter()
        .map(|c| {
            let mut keyed = c.parent_cols.clone();
            keyed.sort_unstable();
            let whole_key = c.parent.keys().any(|k| {
                let mut k = k.to_vec();
                k.sort_unstable();
                k == keyed
            });
            if !whole_key {
                return Err(format!(
                    "the foreign key of `{}` does not reference a whole key of `{}`",
                    c.child.1, c.parent.name
                ));
            }
            let parent = table_ref(&c.parent);
            let parents: Vec<usize> = (0..n_alias)
                .filter(|&x| {
                    table_ref(&alias_plans[x].table) == parent
                        && (alias_plans[x].direct_unit.is_some()
                            || lineage.iter().any(|k| k.alias == x)
                            || partial.aliases.get(x).is_some_and(Option::is_some))
                })
                .collect();
            let children: Vec<usize> = (0..n_alias)
                .filter(|&y| table_ref(&alias_plans[y].table) == c.child)
                .collect();
            if children.is_empty() {
                return Err(format!("the view does not read `{}`", c.child.1));
            }
            if parents.is_empty() {
                return Err(format!(
                    "the view does not read `{}` through a unit, partial or lineage alias",
                    c.parent.name
                ));
            }
            let mut why = String::new();
            for &y in &children {
                let mut ok = false;
                for &x in &parents {
                    match cascade_joined(
                        x,
                        y,
                        &c.parent_cols,
                        &c.child_cols,
                        &scope,
                        &base_edges,
                        &own_edges,
                        &null_side,
                        &joins,
                        &using_pairs,
                    ) {
                        Ok(()) => {
                            ok = true;
                            break;
                        }
                        Err(r) => why = r,
                    }
                }
                if !ok {
                    return Err(why);
                }
            }
            Ok(())
        })
        .collect();
    // Where the cascade continues (the child's own cascading children, recursively), the rows
    // it changes there are found through this route only if every further level is joined like
    // that: `traced` needs the structural condition here and down the chain, and a unit route
    // (units from the parent's image) needs it down the chain as well. A cycle (a
    // self-referential key, mutual cascades) reaches rows no image gives: never traced.
    fn continues_ok(
        i: usize,
        cascades: &[CascadeRoute],
        joined: &[Result<(), String>],
        path: &mut Vec<usize>,
    ) -> Result<(), (usize, String)> {
        path.push(i);
        let child = &cascades[i].child;
        let r = (0..cascades.len())
            .filter(|&j| cascades[j].parent.schema == child.0 && cascades[j].parent.name == child.1)
            .try_for_each(|j| {
                if path.contains(&j) {
                    return Err((
                        j,
                        format!(
                            "the cascade from `{}` into `{}` is part of a cycle",
                            cascades[j].parent.name, cascades[j].child.1
                        ),
                    ));
                }
                joined[j].clone().map_err(|r| (j, r))?;
                continues_ok(j, cascades, joined, path)
            });
        path.pop();
        r
    }
    let mut continuing = Vec::with_capacity(cascades.len());
    for i in 0..cascades.len() {
        let continues = continues_ok(i, &cascades, &joined, &mut Vec::new()).map_err(|(j, r)| {
            format!(
                "the cascade continues from `{}` into `{}`, which is not traced: {r}",
                cascades[j].parent.name, cascades[j].child.1
            )
        });
        let c = &mut cascades[i];
        if c.unit_from_parent.is_some() {
            // The parent's image gives this level's units; the next level may lie elsewhere.
            if let Err(r) = &continues {
                c.unit_from_parent = None;
                c.untraced = Some(r.clone());
            }
        } else {
            match joined[i].clone().and(continues.clone()) {
                Ok(()) => c.traced = true,
                Err(r) => c.untraced = Some(r),
            }
        }
        if c.untraced.is_some() && continues.is_ok() && matches!(mode, Mode::Spj | Mode::Grouped) {
            cascade_derivations(
                c,
                &PinContext {
                    aliases: &alias_plans,
                    partial: &partial,
                    filters: &alias_filters,
                    grouped: mode == Mode::Grouped && !resolved_groups.is_empty() && !windowed,
                    having: select.having.is_some(),
                    null_side: &null_side,
                },
                &mut presence,
            );
        }
        continuing.push(continues.is_ok());
    }
    for (t, cols) in order_tables {
        alias_plans.push(AliasPlan {
            name: t.name.clone(),
            table: t,
            referenced: cols.clone(),
            membership: cols,
            direct_unit: None,
            image_cols: Vec::new(),
            // Not in FROM: never looked up (one unit, recomputed).
            subst_from: (String::new(), String::new()),
            pos: (usize::MAX, None),
            semi: None,
            lookup_where: false,
        });
    }
    // (After every alias is known: the ORDER BY tables above have no filters.)
    for (c, &continues) in cascades.iter_mut().zip(&continuing) {
        cascade_reach(c, &alias_plans, &alias_filters, continues);
    }
    if matches!(mode, Mode::Spj | Mode::Grouped) {
        cascade_maps(&mut cascades, &alias_plans, &alias_filters, &partial);
    }
    let window_sql = if select.named_window.is_empty() {
        None
    } else {
        Some(format!(
            "WINDOW {}",
            select
                .named_window
                .iter()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    };
    // The statement as the lookups see it: a VIRTUAL column's inlined form carries a `COLLATE` of
    // its own too, so this only adds to what the definition as written already had.
    let has_explicit_collate = has_explicit_collate
        || has_collate(&*select)
        || order_usage.as_ref().is_some_and(has_collate);
    Ok(ViewPlan {
        schema,
        name: def.name.clone(),
        hash: def.hash.clone(),
        mode,
        table_columns: vec![None; all_tables.len()],
        tables: all_tables.clone(),
        projection: select
            .projection
            .iter()
            .zip(&item_sql)
            .map(|(p, src)| match (p, src) {
                // Unaliased expressions as written: the server names the column after the text
                // (or `Name_exp_<n>`, or a unique variant of a duplicate), as for the real view.
                (SelectItem::UnnamedExpr(e), Some(src))
                    if !matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) =>
                {
                    src.to_string()
                }
                _ => p.to_string(),
            })
            .collect(),
        from_sql,
        from_items: select.from.clone(),
        where_sql: select.selection.as_ref().map(|e| e.to_string()),
        group_sql: if group_exprs.is_empty() {
            None
        } else {
            Some(
                group_exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        },
        having_sql: select.having.as_ref().map(|e| e.to_string()),
        unit_exprs,
        unit_in_projection,
        full_select: query.to_string(),
        order_limit_sql: top_n,
        window_sql,
        lookup_where_sql: lookup_where
            .map(|w| w.join(" AND "))
            .filter(|w| !w.is_empty()),
        alias_filters,
        partial,
        presence,
        unit_unique,
        lineage,
        cascades,
        aliases: alias_plans,
        output_names,
        edges,
        unit_refs: unit_refs_cols,
        has_explicit_collate,
    })
}

/// Expands an unqualified `*` over USING / NATURAL joins the way the servers do: the join's
/// shared columns once, first, then the remaining columns of the left side, then those of the
/// right table; a RIGHT JOIN is the mirrored LEFT JOIN (shared columns and the rest from the
/// right table first); applied join by join along the chain. Only for a FROM of one chain of base tables with
/// inner / LEFT / RIGHT joins; anything else is left to the caller's fallback. Whether `*` was
/// expanded.
pub(crate) fn expand_star_over_using(
    select: &mut Select,
    default_schema: &str,
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Result<bool> {
    if !select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)))
        || select.from.len() != 1
        || !select.from[0].joins.iter().any(|j| {
            matches!(
                join_constraint(&j.join_operator),
                Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
            )
        })
    {
        return Ok(false);
    }
    let twj = &select.from[0];
    let factor = |f: &TableFactor| -> Option<(String, Arc<TableInfo>)> {
        match f {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let t = object_table_ref(name, default_schema)?;
                let info = tables.get(&t)?.clone();
                let n = alias
                    .as_ref()
                    .map(|a| ident_name(&a.name))
                    .unwrap_or_else(|| t.1.clone());
                Some((n, info))
            }
            _ => None,
        }
    };
    let Some((alias0, t0)) = factor(&twj.relation) else {
        return Ok(false);
    };
    // (alias, column name) in the server's `*` order so far.
    let mut cols: Vec<(String, String)> = t0
        .columns
        .iter()
        .filter(|c| !c.invisible)
        .map(|c| (alias0.clone(), c.name.clone()))
        .collect();
    for j in &twj.joins {
        let Some((alias, t)) = factor(&j.relation) else {
            return Ok(false);
        };
        let right_preserved = match &j.join_operator {
            JoinOperator::Join(_) | JoinOperator::Inner(_) | JoinOperator::CrossJoin(_) => false,
            JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => false,
            JoinOperator::Right(_) | JoinOperator::RightOuter(_) => true,
            _ => return Ok(false),
        };
        let visible: Vec<String> = t
            .columns
            .iter()
            .filter(|c| !c.invisible)
            .map(|c| c.name.clone())
            .collect();
        let keys: Vec<String> = match join_constraint(&j.join_operator) {
            Some(JoinConstraint::Using(names)) => names
                .iter()
                .map(|n| match n.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => Ok(ident_name(i)),
                    _ => bail!("bad USING column"),
                })
                .collect::<Result<_>>()?,
            Some(JoinConstraint::Natural) => cols
                .iter()
                .map(|(_, c)| c.clone())
                .filter(|c| visible.iter().any(|v| v.eq_ignore_ascii_case(c)))
                .collect(),
            _ => Vec::new(),
        };
        let mut next: Vec<(String, String)> = Vec::new();
        for k in &keys {
            let left = cols
                .iter()
                .find(|(_, c)| c.eq_ignore_ascii_case(k))
                .ok_or_else(|| anyhow!("USING column {k} not found"))?;
            let right = visible
                .iter()
                .find(|c| c.eq_ignore_ascii_case(k))
                .ok_or_else(|| anyhow!("USING column {k} not found"))?;
            next.push(if right_preserved {
                (alias.clone(), right.clone())
            } else {
                left.clone()
            });
        }
        let is_key = |c: &str| keys.iter().any(|k| k.eq_ignore_ascii_case(c));
        let left_rest = cols.iter().filter(|(_, c)| !is_key(c)).cloned();
        let right_rest = visible
            .iter()
            .filter(|c| !is_key(c))
            .map(|c| (alias.clone(), c.clone()));
        // A RIGHT JOIN expands as the mirrored LEFT JOIN: the right table's columns first.
        if right_preserved {
            next.extend(right_rest);
            next.extend(left_rest);
        } else {
            next.extend(left_rest);
            next.extend(right_rest);
        }
        cols = next;
    }
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard(_) => {
                for (a, c) in &cols {
                    projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                        qident(a),
                        qident(c),
                    ])));
                }
            }
            other => projection.push(other),
        }
    }
    select.projection = projection;
    Ok(true)
}

fn render_substituted_from(
    from: &[TableWithJoins],
    pos: (usize, Option<usize>),
    alias: &str,
) -> Result<(String, String)> {
    let mut from = from.to_vec();
    substitute_factor(&mut from, pos, alias, SUBST_MARKER);
    let s = from
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let (a, b) = s
        .split_once(SUBST_MARKER)
        .ok_or_else(|| anyhow!("internal: substitution marker missing"))?;
    Ok((a.to_string(), b.to_string()))
}

/// Replaces the table at `pos` by a placeholder table `marker` (keeping the alias). Only rows
/// that actually match the substituted rows are affected by them, so a LEFT JOIN of that table
/// becomes an inner join (the refresh of the units recomputes any NULL extension).
pub(crate) fn substitute_factor(
    from: &mut [TableWithJoins],
    pos: (usize, Option<usize>),
    alias: &str,
    marker: &str,
) {
    let marker = TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(marker))]),
        alias: Some(TableAlias {
            explicit: true,
            name: qident(alias),
            columns: vec![],
            at: None,
        }),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    };
    match pos {
        (t, None) => from[t].relation = marker,
        (t, Some(j)) => {
            let join = &mut from[t].joins[j];
            join.relation = marker;
            if let JoinOperator::Left(c) | JoinOperator::LeftOuter(c) = &join.join_operator {
                join.join_operator = JoinOperator::Join(c.clone());
            }
        }
    }
}
