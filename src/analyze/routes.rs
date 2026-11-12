use super::*;
use super::{expr::*, plan::*, vocab::*};

/// What [`cascade_derivations`] reads of the plan.
pub(crate) struct PinContext<'a> {
    pub(crate) aliases: &'a [AliasPlan],
    pub(crate) partial: &'a PartialUnits,
    pub(crate) filters: &'a [Vec<ConstFilter>],
    /// A GROUP BY view (presence counts per group); otherwise a select/join view.
    pub(crate) grouped: bool,
    pub(crate) having: bool,
    pub(crate) null_side: &'a [usize],
}

/// For a cascade the parent's row neither gives units for nor traces through its own alias:
/// whether it still gives the units the cascade can change, alias by alias of the child (an
/// alias with a unit route of its own needs nothing here), so that the route never rebuilds:
///
/// * an alias whose partial unit columns are all FK columns gets partial keys from the
///   parent's key (`partial_from_parent`);
/// * an alias pinned by its constant filters to some values of a single-column FK can only
///   hold child rows with those values: a parent key among them changes exactly the units
///   holding such rows, which a hidden presence column marks in the materialized table
///   ([`Presence`]); any other key reaches nothing. Exact only where those rows cannot keep a
///   unit out of the table: a grouped view without HAVING (or with the alias inner-joined and
///   pinned to one value: removing its rows removes the whole group), a select/join view
///   whose WHERE keeps no NULL-extended rows of the alias.
///
/// When every alias is covered the route is `derived`; otherwise it stays untraced.
/// (Only where the cascade continues into no untraced level: the caller checks.)
pub(crate) fn cascade_derivations(
    c: &mut CascadeRoute,
    ctx: &PinContext,
    presence: &mut Vec<Presence>,
) {
    let to_parent = |col: usize| -> Option<usize> {
        let j = c.child_cols.iter().position(|&x| x == col)?;
        let pc = c.parent_cols[j];
        filter_or_key_column(&c.parent.columns[pc]).then_some(pc)
    };
    let (mut parts, mut pins, mut added) = (Vec::new(), Vec::new(), Vec::new());
    for (ai, a) in ctx.aliases.iter().enumerate() {
        if (a.table.schema.as_str(), a.table.name.as_str())
            != (c.child.0.as_str(), c.child.1.as_str())
        {
            continue;
        }
        // A unit alias whose unit columns are all FK columns has a unit route of its own.
        if a.direct_unit.as_ref().is_some_and(|cols| {
            cols.iter()
                .all(|&col| a.table.columns[col].exact_key() && to_parent(col).is_some())
        }) {
            continue;
        }
        let part = ctx
            .partial
            .aliases
            .get(ai)
            .and_then(Option::as_ref)
            .and_then(|(set, cols)| {
                let pcols: Option<Vec<usize>> = cols.iter().map(|&col| to_parent(col)).collect();
                Some((*set, pcols?))
            });
        if let Some(p) = part {
            parts.push(p);
            continue;
        }
        let ([fk], [pc]) = (c.child_cols.as_slice(), c.parent_cols.as_slice()) else {
            return;
        };
        let Some(f) = ctx
            .filters
            .get(ai)
            .into_iter()
            .flatten()
            .find(|f| f.col == *fk)
        else {
            return;
        };
        let exact = if ctx.grouped {
            !ctx.having || (!ctx.null_side.contains(&ai) && f.values.len() == 1)
        } else {
            !a.lookup_where
        };
        if !exact || !filter_column(&c.parent.columns[*pc]) {
            return;
        }
        let fk_sql = format!(
            "{}.{}",
            quote_ident(&a.name),
            quote_ident(&a.table.columns[*fk].name)
        );
        let mut values: Vec<(Datum, usize)> = Vec::new();
        for (j, v) in f.values.iter().enumerate() {
            // Grouped: one count per value; select/join: the FK value itself, once.
            if !ctx.grouped && j > 0 {
                values.push((v.clone(), values[0].1));
                continue;
            }
            let k = presence.len() + added.len();
            added.push(Presence {
                name: format!("_lv_c{k}"),
                expr: if ctx.grouped {
                    format!("COUNT(CASE WHEN {fk_sql} = {} THEN 1 END)", int_sql(v))
                } else {
                    fk_sql.clone()
                },
                fk: fk_sql.clone(),
                counts: ctx.grouped.then(|| v.clone()),
            });
            values.push((v.clone(), k));
        }
        pins.push(Pinned {
            parent_col: *pc,
            values,
        });
    }
    if parts.is_empty() && pins.is_empty() {
        return;
    }
    presence.extend(added);
    c.derived = true;
    c.untraced = None;
    c.partial_from_parent = parts;
    c.pinned = pins;
}

/// Where every alias of a cascade's child pins its single-column FK with constant filters, the
/// parent column and the values they allow ([`CascadeRoute::reach`]): child rows with another
/// FK value are no alias's rows, before the cascade (old key) and after it (new key, or NULL),
/// and a cascade continuing out of them (`continues`: joined to an alias of this child) reaches
/// no materialized row either. The route's reason names the keys that still reach the view.
pub(crate) fn cascade_reach(
    c: &mut CascadeRoute,
    aliases: &[AliasPlan],
    filters: &[Vec<ConstFilter>],
    continues: bool,
) {
    let ([fk], [pc]) = (c.child_cols.as_slice(), c.parent_cols.as_slice()) else {
        return;
    };
    let (parent, mut child) = (&c.parent.columns[*pc], None);
    let mut values: Vec<Datum> = Vec::new();
    for (ai, a) in aliases.iter().enumerate() {
        if (a.table.schema.as_str(), a.table.name.as_str())
            != (c.child.0.as_str(), c.child.1.as_str())
        {
            continue;
        }
        let Some(f) = filters.get(ai).into_iter().flatten().find(|f| f.col == *fk) else {
            return;
        };
        child = Some(&a.table.columns[*fk]);
        for v in &f.values {
            if !values.contains(v) {
                values.push(v.clone());
            }
        }
    }
    // Both sides decode to the same integer kind (FK columns share their sign).
    let unsigned = |col: &crate::catalog::Column| matches!(col.kind, ColKind::Unsigned(_));
    let Some(child) = child else { return };
    if !continues || !filter_column(parent) || unsigned(parent) != unsigned(child) {
        return;
    }
    if let Some(r) = &mut c.untraced {
        let list: Vec<String> = values.iter().map(int_sql).collect();
        r.push_str(&format!(
            " (only `{}`.`{}` IN ({}) reaches it)",
            c.parent.name,
            parent.name,
            list.join(", ")
        ));
    }
    c.reach = Some((*pc, values));
}

/// The cascade map of every route that stays untraced (see [`CascadeRoute::map`]): what each
/// child table's rows must keep so that replaying them through the child's routes reads only
/// stored columns. A child without a key cannot be kept: its route stays a rebuild route, and
/// its reason says so.
pub(crate) fn cascade_maps(
    cascades: &mut [CascadeRoute],
    aliases: &[AliasPlan],
    filters: &[Vec<ConstFilter>],
    partial: &PartialUnits,
) {
    let same = |t: &TableInfo, r: &TableRef| t.schema == r.0 && t.name == r.1;
    let untraced: Vec<usize> = (0..cascades.len())
        .filter(|&i| {
            let c = &cascades[i];
            c.untraced.is_some() && !c.traced && !c.derived && c.unit_from_parent.is_none()
        })
        .collect();
    for &i in &untraced {
        let child = cascades[i].child_table.clone();
        let child_ref = (child.schema.clone(), child.name.clone());
        let why = match child.row_key() {
            None => Some("has no primary or NOT NULL unique key".to_string()),
            Some(_) => {
                let fk = &cascades[i].child_cols;
                if fk.iter().any(|&k| !filter_or_key_column(&child.columns[k])) {
                    Some("has a foreign key compared by collation or type conversion".to_string())
                } else {
                    None
                }
            }
        };
        if let Some(why) = why {
            if let Some(r) = &mut cascades[i].untraced {
                r.push_str(&format!(
                    " (`{}` {why}: the cascade map cannot keep its rows)",
                    child.name
                ));
            }
            continue;
        }
        let key = child.row_key().expect("checked").to_vec();
        let mut cols: BTreeSet<usize> = key.iter().copied().collect();
        let mut alias_filters = Vec::new();
        for (ai, a) in aliases.iter().enumerate() {
            if !same(&a.table, &child_ref) {
                continue;
            }
            let mut read: Vec<usize> = a.image_cols.clone();
            read.extend(a.direct_unit.iter().flatten());
            if let Some(Some((_, pc))) = partial.aliases.get(ai) {
                read.extend(pc);
            }
            let f = filters.get(ai).cloned().unwrap_or_default();
            read.extend(f.iter().map(|f| f.col));
            cols.extend(read);
            alias_filters.push(f);
        }
        for c in cascades.iter() {
            if same(&c.parent, &child_ref) {
                cols.extend(&c.parent_cols);
            }
            if c.child == child_ref {
                cols.extend(&c.child_cols);
            }
        }
        cascades[i].map = Some(MapPlan {
            key,
            cols: cols.into_iter().collect(),
            filters: alias_filters,
        });
    }
}

/// Whether every materialized row in which alias `y` (the child of a cascading FK) holds a row
/// also holds, in alias `x` (the parent), that row's parent: then the parent's own row finds the
/// rows a cascade changes. `Err` names why not.
///
/// Equalities valid whenever `y`'s row is present are the WHERE / inner-join ones (true in every
/// row, both sides non-NULL) and those of `y`'s own LEFT JOIN condition; `x`'s own LEFT JOIN
/// condition is not among them (`kids k LEFT JOIN pars p ON p.id = k.par_id AND p.active = 1`
/// keeps a kid whose parent is inactive, NULL-extended). A NULL-side parent is found anyway
/// when its join condition is exactly the foreign-key equality: the constraint guarantees the
/// parent row exists, so the condition matches it whenever the child's key is non-NULL (a child
/// with a NULL key is not cascaded into).
#[allow(clippy::too_many_arguments)]
pub(crate) fn cascade_joined(
    x: usize,
    y: usize,
    parent_cols: &[usize],
    child_cols: &[usize],
    scope: &Scope,
    base_edges: &[(ColRef, ColRef)],
    own_edges: &[Vec<(ColRef, ColRef)>],
    null_side: &[usize],
    joins: &[(usize, bool, JoinConstraint)],
    using_pairs: &[(usize, ColRef, ColRef)],
) -> Result<(), String> {
    let (xn, yn) = (&scope.aliases[x].0, &scope.aliases[y].0);
    let mut cls = Classes::new();
    for (a, b) in base_edges.iter().chain(&own_edges[y]) {
        cls.union(*a, *b);
    }
    let pairs = || child_cols.iter().copied().zip(parent_cols.iter().copied());
    if pairs().all(|(cc, pc)| cls.find((y, cc)) == cls.find((x, pc))) {
        return Ok(());
    }
    if !null_side.contains(&x) {
        return Err(format!("`{xn}` is not joined to `{yn}` on the foreign key"));
    }
    // The parent on the NULL side: its own condition must be the key equality and nothing else.
    let mut eqs: Vec<(ColRef, ColRef)> = Vec::new();
    match joins.iter().find(|(a, _, _)| *a == x).map(|(_, _, c)| c) {
        Some(JoinConstraint::On(e)) => {
            let mut cs = Vec::new();
            conjuncts(e, &mut cs);
            for c in cs {
                let eq = match c {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        right,
                    } => scope
                        .resolve(left)
                        .zip(scope.resolve(right))
                        .filter(|(a, b)| scope.same_family(*a, *b)),
                    Expr::InList {
                        expr: left,
                        list,
                        negated: false,
                    } if list.len() == 1 => scope
                        .resolve(left)
                        .zip(scope.resolve(&list[0]))
                        .filter(|(a, b)| scope.same_family(*a, *b)),
                    _ => None,
                };
                match eq {
                    Some(eq) => eqs.push(eq),
                    None => {
                        return Err(format!(
                            "`{xn}` is on the NULL side of a LEFT JOIN whose condition is not \
                             the foreign-key equality alone: `{c}` leaves a `{yn}` row whose \
                             parent fails it without its parent"
                        ))
                    }
                }
            }
        }
        Some(JoinConstraint::Using(_)) => {
            eqs.extend(
                using_pairs
                    .iter()
                    .filter(|(a, _, _)| *a == x)
                    .map(|(_, l, r)| (*l, *r)),
            );
        }
        _ => return Err(format!("`{xn}` is not joined to `{yn}` on the foreign key")),
    }
    let mut covered = vec![false; child_cols.len()];
    for (a, b) in &eqs {
        let other = if a.0 == x { b } else { a };
        let j = pairs().position(|(cc, pc)| {
            (*a == (x, pc) || *b == (x, pc)) && cls.find((y, cc)) == cls.find(*other)
        });
        match j {
            Some(j) => covered[j] = true,
            None => {
                return Err(format!(
                    "`{xn}` is on the NULL side of a LEFT JOIN whose condition is not the \
                     foreign-key equality alone: `{}` = `{}` restricts `{xn}` further",
                    scope.name(*a),
                    scope.name(*b)
                ))
            }
        }
    }
    if covered.iter().all(|&c| c) {
        Ok(())
    } else {
        Err(format!(
            "`{xn}` is not joined to `{yn}` on the whole foreign key"
        ))
    }
}

/// Cascading-FK parents (and, conservatively, their ancestors) of the given child tables.
pub(crate) fn cascade_routes(
    children: &[(Arc<TableInfo>, Option<Vec<usize>>)],
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<CascadeRoute> {
    let mut out = Vec::new();
    let mut stack: Vec<(Arc<TableInfo>, Option<Vec<usize>>, usize)> = children
        .iter()
        .map(|(t, d)| (t.clone(), d.clone(), 0))
        .collect();
    let mut seen: BTreeSet<(TableRef, TableRef)> = BTreeSet::new();
    // Tables the view reads get their own routes (checked down the chain); the walk continues
    // past them only through parents the view does not read.
    let read: BTreeSet<TableRef> = children
        .iter()
        .map(|(t, _)| (t.schema.clone(), t.name.clone()))
        .collect();
    while let Some((child, direct, depth)) = stack.pop() {
        for fk in &child.cascades {
            let Some(parent) = tables.get(&fk.parent) else {
                continue;
            };
            if !seen.insert((
                (child.schema.clone(), child.name.clone()),
                fk.parent.clone(),
            )) && direct.is_none()
            {
                continue;
            }
            let parent_cols: Option<Vec<usize>> =
                fk.parent_cols.iter().map(|n| parent.col_index(n)).collect();
            let Some(parent_cols) = parent_cols else {
                continue;
            };
            // Units follow from the parent image only if every unit column of the child is an
            // FK column with an exact representation on both sides.
            let unit_from_parent = direct.as_ref().and_then(|cols| {
                cols.iter()
                    .map(|c| {
                        let j = fk.child_cols.iter().position(|x| x == c)?;
                        let pc = parent_cols[j];
                        (child.columns[*c].exact_key() && parent.columns[pc].exact_key())
                            .then_some(pc)
                    })
                    .collect::<Option<Vec<usize>>>()
            });
            out.push(CascadeRoute {
                parent: parent.clone(),
                parent_cols,
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                unit_from_parent,
                child: (child.schema.clone(), child.name.clone()),
                child_cols: fk.child_cols.clone(),
                traced: false,
                untraced: None,
                derived: false,
                partial_from_parent: Vec::new(),
                pinned: Vec::new(),
                reach: None,
                child_table: child.clone(),
                map: None,
            });
            if depth < 8 && !read.contains(&fk.parent) {
                // A cascade into the parent cascades further into the child: unknown units.
                stack.push((parent.clone(), None, depth + 1));
            }
        }
    }
    out
}

/// Columns of each table in `watched` that `query` may read, matched by name: every identifier
/// anywhere in the statement (columns, aliases, `USING` lists, ...) that names a column of the
/// table counts, so this is a superset. `None` (any column) where the statement reads columns
/// implicitly: `*`, NATURAL joins, FROM items other than tables and subqueries.
pub(crate) fn read_columns(
    query: &Query,
    watched: &[TableRef],
    tables: &HashMap<TableRef, Arc<TableInfo>>,
) -> Vec<Option<Vec<bool>>> {
    #[derive(Default)]
    struct Names {
        names: HashSet<String>,
        any: bool,
    }
    impl Names {
        fn joins(&mut self, joins: &[Join]) {
            for j in joins {
                if !matches!(
                    join_constraint(&j.join_operator),
                    Some(JoinConstraint::On(_) | JoinConstraint::Using(_) | JoinConstraint::None)
                ) {
                    self.any = true;
                }
            }
        }
    }
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_ident(&mut self, i: &Ident) -> ControlFlow<()> {
            self.names.insert(ident_name(i).to_lowercase());
            ControlFlow::Continue(())
        }
        fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
            if s.projection.iter().any(|p| {
                matches!(
                    p,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                )
            }) {
                self.any = true;
            }
            for t in &s.from {
                self.joins(&t.joins);
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, f: &TableFactor) -> ControlFlow<()> {
            match f {
                TableFactor::Table { args: None, .. } | TableFactor::Derived { .. } => {}
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => self.joins(&table_with_joins.joins),
                _ => self.any = true,
            }
            ControlFlow::Continue(())
        }
    }
    let mut n = Names::default();
    let _ = query.visit(&mut n);
    watched
        .iter()
        .map(|t| {
            let info = tables.get(t).filter(|_| !n.any)?;
            Some(
                info.columns
                    .iter()
                    .map(|c| n.names.contains(&c.name.to_lowercase()))
                    .collect(),
            )
        })
        .collect()
}

/// The `JoinConstraint` of a join operator, `None` for the operators that carry none. One owner,
/// also read by `crate::analyzer` (its `ast` module re-exports it).
pub(crate) fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::AsOf { constraint: c, .. } => Some(c),
        _ => None,
    }
}
