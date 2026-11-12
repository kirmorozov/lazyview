use super::*;
use super::{build::*, vocab::*};

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Spj => f.write_str("incremental (select/join)"),
            Mode::Grouped => f.write_str("incremental (grouped)"),
            Mode::Global => f.write_str("incremental (single-row aggregate)"),
            Mode::Full(r) => write!(f, "full rebuild ({r})"),
        }
    }
}

/// Classification of a view.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Select/project/join; unit = anchor (first table) primary key.
    Spj,
    /// GROUP BY; unit = group expressions.
    Grouped,
    /// Aggregates without GROUP BY: a single row recomputed on any change.
    Global,
    /// Unsupported for incremental maintenance; rebuilt on any change.
    Full(String),
}

/// Per table occurrence (alias) in the FROM clause.
#[derive(Debug, Clone)]
pub struct AliasPlan {
    /// Name the query uses to qualify this table's columns.
    pub name: String,
    pub table: Arc<TableInfo>,
    /// Columns used anywhere in the query.
    pub referenced: Vec<bool>,
    /// Columns that decide which unit a row contributes to (ON / WHERE / GROUP BY).
    pub membership: Vec<bool>,
    /// For each unit expression, the column of this table that equals it on every row this table
    /// contributes to. When set, units come straight from binlog images (no lookup query).
    pub direct_unit: Option<Vec<usize>>,
    /// Columns carried in literal row images for lookups (membership columns, ordinal order).
    pub image_cols: Vec<usize>,
    /// FROM clause with this alias replaced by `SUBST_MARKER`, split around it.
    pub subst_from: (String, String),
    /// Position in the FROM clause: (table index, join index).
    pub pos: (usize, Option<usize>),
    /// The table of a semi-/anti-join subquery (not in FROM): `EXISTS (SELECT 1 FROM <rows> AS
    /// alias WHERE …)` relating its rows to the view's rows, with `SUBST_MARKER` where the rows
    /// go (and [`semi_table_marker`]s for the subquery's other tables). Its lookups add this to
    /// the view's FROM and [`ViewPlan::lookup_where_sql`].
    pub semi: Option<String>,
    /// Lookups of this alias use [`ViewPlan::lookup_where_sql`] (semi-join tables, and NULL-side
    /// tables whose WHERE conjuncts keep NULL-extended rows).
    pub lookup_where: bool,
}

/// A constant filter of an alias (see `const_filters`): a row of its table is that alias's row
/// in some row of the query only if column `col` holds one of `values`, as decoded from the
/// binlog (`Datum::from_binlog`).
#[derive(Debug, Clone, PartialEq)]
pub struct ConstFilter {
    pub col: usize,
    pub values: Vec<Datum>,
}

impl ConstFilter {
    /// Whether `row`, a binlog image of `table`, can be a row of the alias pinned by `filters`:
    /// a few comparisons of decoded integers, no allocation. A row that is not a full image,
    /// or lacks the value, passes.
    pub fn passes(
        filters: &[ConstFilter],
        row: &mysql_async::binlog::row::BinlogRow,
        table: &TableInfo,
    ) -> bool {
        filters.is_empty()
            || row.len() != table.columns.len()
            || filters.iter().all(|f| match row.as_ref(f.col) {
                None => true,
                v => f
                    .values
                    .contains(&Datum::from_binlog(v, &table.columns[f.col])),
            })
    }
}

/// A child alias of a cascade pinned by its constant filters to some values of the FK (see
/// [`CascadeRoute::pinned`]): the parent column and, per value, its presence column.
#[derive(Debug, Clone, PartialEq)]
pub struct Pinned {
    pub parent_col: usize,
    pub values: Vec<(Datum, usize)>,
}

/// A hidden materialized column telling which units hold rows of a pinned cascade child alias
/// (see `cascade_derivations`): a grouped view counts the alias's rows with one FK value per
/// group (`counts`), a select/join view carries the alias's FK value. The units a cascade from
/// a parent key changes are the materialized units it marks for that key ([`Self::marks`]),
/// plus, for a key update into a pinned value, the units holding such rows now
/// ([`Self::holds`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Presence {
    /// Materialized column name, `_lv_c<k>`.
    pub name: String,
    /// The SELECT expression computing it.
    pub expr: String,
    /// The alias's FK column, as an expression of the view's FROM clause.
    pub fk: String,
    /// Grouped views: the FK value it counts.
    pub counts: Option<Datum>,
}

impl Presence {
    /// Predicate on the materialized table: the units holding the alias's rows with FK `value`.
    pub fn marks(&self, value: &Datum) -> String {
        match self.counts {
            Some(_) => format!("{} > 0", quote_ident(&self.name)),
            None => format!("{} = {}", quote_ident(&self.name), int_sql(value)),
        }
    }

    /// Predicate on the sources: the rows of the alias with FK `value`.
    pub fn holds(&self, value: &Datum) -> String {
        format!("{} = {}", self.fk, int_sql(value))
    }
}

/// An integer datum (constant filters hold only integers) as an SQL literal.
pub(crate) fn int_sql(d: &Datum) -> String {
    match d {
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        other => unreachable!("non-integer filter value {other:?}"),
    }
}

/// Partial direct units (see `partial_units`): aliases whose rows give part of the unit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PartialUnits {
    /// The distinct sets of unit positions (ascending) some alias gives.
    pub sets: Vec<Vec<usize>>,
    /// Per alias of the FROM clause (aliases past the end have none): the set it gives and, per
    /// position of the set, its column equal to that unit expression.
    pub aliases: Vec<Option<(usize, Vec<usize>)>>,
}

/// Lineage of a lookup-resolved alias (select/join views): hidden, indexed materialized columns
/// carrying the primary key of that alias's row in each materialized row. A changed row of the
/// table maps to the materialized rows it is part of by an index lookup on the materialized
/// table, never by joining its old image with the current state of the other tables (which may
/// have changed too, in a transaction not read from the binlog yet).
#[derive(Debug, Clone, PartialEq)]
pub struct LineageKey {
    pub alias: usize,
    /// Materialized column names, `_lv_a<alias>_pk<j>`, one per primary-key column.
    pub cols: Vec<String>,
    /// The primary-key columns as source expressions (`c`.`id`).
    pub exprs: Vec<String>,
}

/// A parent table whose deletes/updates change rows of a table the view reads through a
/// cascading foreign key (such changes never reach the binlog).
#[derive(Debug, Clone)]
pub struct CascadeRoute {
    pub parent: Arc<TableInfo>,
    /// Ordinals of the referenced parent columns.
    pub parent_cols: Vec<usize>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    /// Parent column giving each unit value (the child's unit columns are the FK columns).
    /// `None`: affected units are unknown, the view is rebuilt (unless `traced`).
    pub unit_from_parent: Option<Vec<usize>>,
    /// The child table and its FK column ordinals (constraint order, matching `parent_cols`).
    pub child: TableRef,
    pub child_cols: Vec<usize>,
    /// The view reads the parent through a lineage or direct-unit alias that every alias of the
    /// child is joined to on the whole FK, in a shape where a child row's presence in a
    /// materialized row implies that alias carries its parent (`cascade_joined`), and the same
    /// holds down every further cascade out of the child: the parent's own rows (routed to that
    /// alias) find every materialized row the cascade changes, so the parent needs no route of
    /// its own.
    pub traced: bool,
    /// Why the route is neither unit-derived, traced nor `derived` (the view rebuilds on the
    /// parent's deletes / key updates).
    pub untraced: Option<String>,
    /// The parent's row gives the units the cascade can change, alias by alias of the child
    /// (`cascade_derivations`), never a rebuild: `partial_from_parent` gives partial unit keys
    /// (set of `ViewPlan::partial`, the parent column for each position); each entry of
    /// `pinned` is an alias the constant filters pin to some values of the (single-column) FK:
    /// a parent key among them names the materialized units holding that alias's rows through
    /// a presence column (`ViewPlan::presence`), any other key reaches nothing.
    pub derived: bool,
    pub partial_from_parent: Vec<(usize, Vec<usize>)>,
    pub pinned: Vec<Pinned>,
    /// Every alias of the child pins its (single-column) FK with constant filters: the parent
    /// column and the values they allow. A parent key outside them is the FK of no alias's
    /// row, before or after the change, so the cascade changes nothing the view reads,
    /// whatever the route (`cascade_reach`).
    pub reach: Option<(usize, Vec<Datum>)>,
    /// The child table itself (its columns, keys and cascades).
    pub child_table: Arc<TableInfo>,
    /// An untraced route of a select/join or grouped view keeps the child's rows in the
    /// in-memory cascade cache (`crate::cascade`): a parent change replays them as the child row
    /// changes the storage engine made, through every route of the child table
    /// (`cascade_maps`). `None`: traced, unit, derived, another mode, or a child without a key
    /// (named in `untraced`).
    pub map: Option<MapPlan>,
}

/// What the cascade map keeps of a child's rows (see [`CascadeRoute::map`]).
#[derive(Debug, Clone, PartialEq)]
pub struct MapPlan {
    /// The child's key columns: its primary key, else its shortest NOT NULL unique key.
    pub key: Vec<usize>,
    /// The stored columns (ascending): every column a route of the child table takes values
    /// from (its aliases' images, units, partial units and filters; the parent columns of the
    /// cascades out of it; the FK columns of the cascades into it) and the key. A replayed
    /// update differs from its old row in FK columns only, so what else the aliases read
    /// (`AliasPlan::referenced`) compares equal on both sides without being stored.
    pub cols: Vec<usize>,
    /// Per alias of the child: its constant filters. A row is kept when it passes those of some
    /// alias (rows of no alias never reach the view). No alias (a level the view does not
    /// read): every row is kept.
    pub filters: Vec<Vec<ConstFilter>>,
}

impl MapPlan {
    /// Whether the map keeps `row` (a binlog image of the child).
    pub fn keeps(&self, row: &mysql_async::binlog::row::BinlogRow, table: &TableInfo) -> bool {
        self.filters.is_empty()
            || self
                .filters
                .iter()
                .any(|f| ConstFilter::passes(f, row, table))
    }
}

/// An equality between two alias columns, valid on every view row when `only_with` is `None`
/// (a WHERE conjunct or an inner-join ON) and, when `Some(j)`, only on the rows where the
/// alias `j` participates (its own LEFT-join ON). The hop paths of `crate::access` walk them.
#[derive(Debug, Clone)]
pub struct Edge {
    pub a: (usize, usize),
    pub b: (usize, usize),
    pub only_with: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ViewPlan {
    pub schema: String,
    pub name: String,
    pub hash: String,
    pub mode: Mode,
    pub tables: Vec<TableRef>,
    /// Full-refresh plans: per entry of `tables`, the columns the statement may read (`None`:
    /// any). An UPDATE that changes none of them cannot change the view.
    pub table_columns: Vec<Option<Vec<bool>>>,
    pub aliases: Vec<AliasPlan>,
    /// Rendered projection items (with `AS` aliases where given).
    pub projection: Vec<String>,
    pub from_sql: String,
    /// The FROM clause (incremental plans), for lookups that replace several tables at once.
    pub from_items: Vec<TableWithJoins>,
    pub where_sql: Option<String>,
    pub group_sql: Option<String>,
    pub having_sql: Option<String>,
    /// Unit expressions over base tables (empty for Global / Full).
    pub unit_exprs: Vec<String>,
    /// Projection index that already yields each unit expression, if any.
    pub unit_in_projection: Vec<Option<usize>>,
    /// Statement used for full rebuilds of `Full` views.
    pub full_select: String,
    /// Whether one unit maps to at most one materialized row.
    pub unit_unique: bool,
    /// Lineage keys of the lookup-resolved aliases that have a primary key (select/join views).
    /// Their images carry only the primary key (`image_cols`).
    pub lineage: Vec<LineageKey>,
    pub cascades: Vec<CascadeRoute>,
    /// Column names from `CREATE VIEW v (a, b, ...)`, applied positionally to the materialized
    /// table (the SELECT itself keeps its own aliases so GROUP BY / HAVING resolve as written).
    pub output_names: Option<Vec<String>>,
    /// Top-N views (single-unit, recomputed with one bounded query per batch): the
    /// `ORDER BY … LIMIT n` that ends their SELECT.
    pub order_limit_sql: Option<String>,
    /// Named windows (`WINDOW w AS (…)`), rendered after HAVING: the projection's `OVER w`
    /// references need them (L2).
    pub window_sql: Option<String>,
    /// WHERE of the lookups of the aliases marked `lookup_where`: `where_sql` without the
    /// subquery predicates (evaluated on the substituted rows instead) and without conjuncts that
    /// keep NULL-extended rows.
    pub lookup_where_sql: Option<String>,
    /// Per alias of the FROM clause: its constant filters (aliases past the end have none).
    pub alias_filters: Vec<Vec<ConstFilter>>,
    /// Aliases traced by partial units instead of lookups.
    pub partial: PartialUnits,
    /// Hidden presence columns of pinned cascade children (after the lineage columns).
    pub presence: Vec<Presence>,
    /// The equality edges between alias columns ([`Edge`]).
    pub edges: Vec<Edge>,
    /// Per reduced unit expression: the alias columns it reads.
    pub unit_refs: Vec<Vec<(usize, usize)>>,
    /// Whether the definition spells a `COLLATE` of its own anywhere ([`has_collate`], the
    /// statement as written and as the lookups see it). The lookup of an incremental view then
    /// binds its string parameters without one: a `COLLATE` clause is EXPLICIT, and the view's own
    /// would meet the parameter's in an error ("Illegal mix of collations", 1267). See the lookup
    /// in `engine`.
    pub has_explicit_collate: bool,
}

impl ViewPlan {
    pub fn is_incremental(&self) -> bool {
        matches!(self.mode, Mode::Spj | Mode::Grouped)
    }

    /// SELECT producing materialized rows (projection, hidden unit columns, then the lineage
    /// columns), optionally restricted by `filter` (a predicate over `unit_exprs`).
    pub fn select_sql(&self, hidden_units: &[usize], filter: Option<&str>) -> String {
        if let Mode::Full(_) = self.mode {
            return self.full_select.clone();
        }
        let mut cols = self.projection.clone();
        for &u in hidden_units {
            cols.push(format!("{} AS `_lv_u{u}`", self.unit_exprs[u]));
        }
        for k in &self.lineage {
            for (e, c) in k.exprs.iter().zip(&k.cols) {
                cols.push(format!("{e} AS {}", quote_ident(c)));
            }
        }
        for p in &self.presence {
            cols.push(format!("{} AS {}", p.expr, quote_ident(&p.name)));
        }
        let mut sql = format!("SELECT {} FROM {}", cols.join(", "), self.from_sql);
        push_where(&mut sql, self.where_sql.as_deref(), filter);
        if let Some(g) = &self.group_sql {
            sql.push_str(" GROUP BY ");
            sql.push_str(g);
        }
        if let Some(h) = &self.having_sql {
            sql.push_str(" HAVING ");
            sql.push_str(h);
        }
        if let Some(w) = &self.window_sql {
            sql.push(' ');
            sql.push_str(w);
        }
        if let Some(o) = &self.order_limit_sql {
            sql.push(' ');
            sql.push_str(o);
        }
        sql
    }

    /// How a changed row of each table maps to the materialized rows it affects, for logs:
    /// `alias:unit` (straight from the row image), `alias:lineage` (old rows by the hidden key
    /// columns, new rows by key), `alias:partial` (part of the unit from the row image, every unit
    /// matching it refreshed), `alias:lookup` (images joined with the current state) or, for
    /// full-refresh views, `rebuild`; parents of cascading foreign keys as `^parent:unit|rebuild`.
    pub fn tracing(&self) -> String {
        if !self.is_incremental() && !matches!(self.mode, Mode::Global) {
            return "rebuild".into();
        }
        let mut parts: Vec<String> = self
            .aliases
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let how = if matches!(self.mode, Mode::Global) {
                    "recompute"
                } else if a.direct_unit.is_some() {
                    "unit"
                } else if self.lineage_of(i).is_some() {
                    "lineage"
                } else if self.partial_of(i).is_some() {
                    "partial"
                } else {
                    "lookup"
                };
                format!("{}:{how}", a.name)
            })
            .collect();
        parts.extend(self.cascades.iter().map(|c| {
            let how = if c.unit_from_parent.is_some() {
                "unit"
            } else if c.traced {
                "traced"
            } else if c.derived {
                "derived"
            } else if c.map.is_some() {
                "map"
            } else {
                "rebuild"
            };
            format!("^{}:{how}", c.parent.name)
        }));
        parts.join(",")
    }

    /// Number of lineage columns (they follow the hidden unit columns).
    pub fn lineage_width(&self) -> usize {
        self.lineage.iter().map(|k| k.cols.len()).sum()
    }

    /// The lineage key of `alias`, if it has one.
    pub fn lineage_of(&self, alias: usize) -> Option<&LineageKey> {
        self.lineage.iter().find(|k| k.alias == alias)
    }

    /// Whether the unit and the lineage keys together identify each materialized row of a
    /// select/join view: the first table has a row key (its primary key or a UNIQUE NOT NULL
    /// key, which is then the unit) and every other table of the FROM clause carries its row
    /// key as a lineage key. A row of such a view is one combination of rows of these tables
    /// (with NULLs for the unmatched side of a LEFT JOIN), so no two rows agree on all of them.
    /// Tables of semi- and anti-join subqueries and of ORDER BY subqueries contribute no rows
    /// of their own. A first table without a row key (equal rows share their unit), a table
    /// mapped to units straight from its images (no lineage columns), one without a row key
    /// (lineage by the whole row: duplicates) and any other table without lineage leave the
    /// rows unidentified. Grouped views and single-table views are identified by the unit
    /// alone (`unit_unique`).
    pub fn rows_identified(&self) -> bool {
        let keyed = |a: usize| self.aliases[a].table.row_key().map(<[usize]>::len);
        self.mode == Mode::Spj
            && !self.unit_exprs.is_empty()
            && !self.aliases.is_empty()
            && keyed(0).is_some()
            && (1..self.aliases.len()).all(|a| {
                let ap = &self.aliases[a];
                ap.semi.is_some()
                    || ap.pos.0 == usize::MAX
                    || keyed(a)
                        .is_some_and(|n| self.lineage_of(a).is_some_and(|k| k.cols.len() == n))
            })
    }

    /// The constant filters of `alias`: a row failing one is never that alias's row.
    pub fn filters_of(&self, alias: usize) -> &[ConstFilter] {
        self.alias_filters.get(alias).map_or(&[], Vec::as_slice)
    }

    /// The partial unit of `alias`: the index of its set in `partial.sets` and its column for
    /// each position of the set.
    pub fn partial_of(&self, alias: usize) -> Option<(usize, &[usize])> {
        let (set, cols) = self.partial.aliases.get(alias)?.as_ref()?;
        Some((*set, cols))
    }

    /// `SELECT DISTINCT <units> FROM ... WHERE ... AND <filter>`: the units of the current rows
    /// that `filter` (over a lineage alias's primary key) selects.
    pub fn key_lookup_sql(&self, filter: &str) -> String {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            self.unit_exprs.join(", "),
            self.from_sql
        );
        push_where(&mut sql, self.where_sql.as_deref(), Some(filter));
        sql
    }

    /// The semi-join template of `alias` with its rows `own` and, for the subquery's other
    /// tables, the rows `others` gives (`None`: the table itself).
    fn semi_exists(
        &self,
        alias: usize,
        own: &str,
        mut others: impl FnMut(usize) -> Option<String>,
    ) -> String {
        let mut exists = self.aliases[alias].semi.clone().unwrap_or_default();
        for (j, a) in self.aliases.iter().enumerate() {
            let marker = semi_table_marker(j);
            if exists.contains(&marker) {
                let rows = others(j).unwrap_or_else(|| {
                    format!(
                        "{}.{}",
                        quote_ident(&a.table.schema),
                        quote_ident(&a.table.name)
                    )
                });
                exists = exists.replacen(&marker, &rows, 1);
            }
        }
        exists.replacen(SUBST_MARKER, own, 1)
    }

    /// `SELECT DISTINCT <units> FROM <from with alias replaced by derived rows> WHERE ...`
    pub fn lookup_sql(&self, alias: usize, derived: &str) -> String {
        let a = &self.aliases[alias];
        if a.semi.is_some() {
            let mut sql = format!(
                "SELECT DISTINCT {} FROM {}",
                self.unit_exprs.join(", "),
                self.from_sql
            );
            let exists = self.semi_exists(alias, derived, |_| None);
            push_where(&mut sql, self.lookup_where_sql.as_deref(), Some(&exists));
            return sql;
        }
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}{}{}",
            self.unit_exprs.join(", "),
            a.subst_from.0,
            derived,
            a.subst_from.1
        );
        let where_sql = if a.lookup_where {
            self.lookup_where_sql.as_deref()
        } else {
            self.where_sql.as_deref()
        };
        push_where(&mut sql, where_sql, None);
        sql
    }

    /// As [`Self::lookup_sql`] with several aliases replaced by derived rows at once
    /// (`(alias, derived table)`). Also returns the order (indexes into `subst`) in which the
    /// derived tables appear in the statement, which is the order of their parameters.
    pub fn lookup_sql_multi(&self, subst: &[(usize, String)]) -> Result<(String, Vec<usize>)> {
        let mut from = self.from_items.clone();
        let marker = |k: usize| format!("__lv_subst_{k}__");
        // Tables of the FROM clause are replaced in place; semi-join tables join as EXISTS, one
        // per subquery: tables of one subquery substituted together share it.
        let mut exists = Vec::new();
        let mut done = vec![false; subst.len()];
        for (k, (a, _)) in subst.iter().enumerate() {
            let ap = &self.aliases[*a];
            match &ap.semi {
                Some(_) if done[k] => {}
                Some(_) => exists.push(self.semi_exists(*a, &marker(k), |j| {
                    let kj = subst.iter().position(|(b, _)| *b == j)?;
                    done[kj] = true;
                    Some(marker(kj))
                })),
                None => substitute_factor(&mut from, ap.pos, &ap.name, &marker(k)),
            }
        }
        let from_sql = from
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {from_sql}",
            self.unit_exprs.join(", ")
        );
        let weak = subst.iter().any(|(a, _)| self.aliases[*a].lookup_where);
        let where_sql = if weak || !exists.is_empty() {
            self.lookup_where_sql.as_deref()
        } else {
            self.where_sql.as_deref()
        };
        let filter = (!exists.is_empty()).then(|| exists.join(" AND "));
        push_where(&mut sql, where_sql, filter.as_deref());
        let mut order: Vec<(usize, usize)> = Vec::new();
        for k in 0..subst.len() {
            let at = sql
                .find(&marker(k))
                .ok_or_else(|| anyhow!("internal: substitution marker missing"))?;
            order.push((at, k));
        }
        order.sort_unstable();
        for (k, (_, derived)) in subst.iter().enumerate() {
            sql = sql.replacen(&marker(k), derived, 1);
        }
        Ok((sql, order.into_iter().map(|(_, k)| k).collect()))
    }
}

fn push_where(sql: &mut String, where_sql: Option<&str>, filter: Option<&str>) {
    match (where_sql, filter) {
        (Some(w), Some(f)) => {
            sql.push_str(&format!(" WHERE ({w}) AND ({f})"));
        }
        (Some(w), None) => sql.push_str(&format!(" WHERE {w}")),
        (None, Some(f)) => sql.push_str(&format!(" WHERE {f}")),
        (None, None) => {}
    }
}
