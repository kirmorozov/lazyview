//! The analyzer's logical plan: query blocks over resolved FROM items.
//!
//! A [`Block`] is one SELECT in the canonical pipeline `FROM (joins) → WHERE → GROUP BY → HAVING →
//! window → DISTINCT → ORDER BY/LIMIT → projection`, kept as a sqlparser [`Select`] that the
//! normalizer rewrites in place. Its FROM items are base tables or derived relations (CTE
//! references, derived tables); expressions stay sqlparser [`Expr`]s whose column references are
//! resolved against the items (see `build.rs`).

use std::sync::Arc;

use sqlparser::ast::{Expr, Query, Select, SelectItem};

use super::names::Name;
use crate::catalog::{TableInfo, TableRef};

/// A relation: a block, or something the normalizer cannot look into.
#[derive(Debug, Clone)]
pub(crate) enum Rel {
    Block(Box<Block>),
    /// A set operation, recursive CTE, VALUES, LATERAL derived table, ... `why` names it.
    Opaque {
        why: String,
    },
}

/// Where a FROM item comes from (for reasons and internal views).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Origin {
    Table,
    /// A reference to the CTE of that name.
    Cte(String),
    /// A derived table `(SELECT …) AS alias`.
    Derived,
}

impl Origin {
    /// `CTE \`t\`` / `derived table \`d\``, for reasons.
    pub(crate) fn describe(&self, alias: &str) -> String {
        match self {
            Origin::Cte(n) => format!("CTE `{n}`"),
            Origin::Derived => format!("derived table `{alias}`"),
            Origin::Table => format!("table `{alias}`"),
        }
    }
}

/// Position of a FROM factor: (index in FROM, join index within it).
pub(crate) type Pos = (usize, Option<usize>);

#[derive(Debug, Clone)]
pub(crate) struct Item {
    /// The name that qualifies the item's columns in its block.
    pub alias: String,
    pub pos: Pos,
    pub origin: Origin,
    pub source: Source,
}

#[derive(Debug, Clone)]
pub(crate) enum Source {
    Table(TableRef, Arc<TableInfo>),
    Derived(Rel),
}

impl Item {
    /// Column names (all of them, invisible ones included for tables), when known.
    pub(crate) fn columns(&self) -> Option<Vec<String>> {
        match &self.source {
            Source::Table(_, t) => Some(t.columns.iter().map(|c| c.name.clone()).collect()),
            Source::Derived(Rel::Block(b)) => {
                Some(b.outputs.iter().map(|o| o.name.text.clone()).collect())
            }
            Source::Derived(Rel::Opaque { .. }) => None,
        }
    }

    pub(crate) fn table(&self) -> Option<&Arc<TableInfo>> {
        match &self.source {
            Source::Table(_, t) => Some(t),
            _ => None,
        }
    }
}

/// One output column of a block.
#[derive(Debug, Clone)]
pub(crate) struct Output {
    pub name: Name,
    /// The value over the block's FROM items.
    pub expr: Expr,
    /// The projection item as written, while it is unchanged (`None` once rewritten or for
    /// wildcard expansions), with its source text.
    pub original: Option<(SelectItem, Option<String>)>,
}

/// A semi-/anti-join conjunct taken out of WHERE (`[NOT] EXISTS (…)`, `x IN (…)`) and replaced
/// there by `marker`, a call `__lv_semi_<n>__(<outer expressions>)` whose arguments are the
/// outer-scope expressions the subquery uses (rewritten together with the block).
#[derive(Debug, Clone)]
pub(crate) struct Semi {
    pub id: usize,
    pub negated: bool,
    /// The subquery's tables: one, or several joined by inner joins (their ON conditions are
    /// conjuncts of `join_conditions` / `lookup_conditions`).
    pub tables: Vec<SemiTable>,
    /// The predicate with outer expressions replaced by placeholders `__lv_outer_<n>__`.
    pub predicate: Expr,
    /// Conditions equivalent to the predicate when it joins at most one row of the table
    /// (correlation equalities, the subquery's WHERE and ON conditions), with the same
    /// placeholders.
    pub join_conditions: Vec<Expr>,
    /// The conditions relating rows of the tables to the outer row for lookups: the join
    /// conditions, except that `x NOT IN (…)` also relates every row when `x` or the key is NULL.
    pub lookup_conditions: Vec<Expr>,
    /// `x NOT IN (SELECT k …)` (never traced by direct units: a NULL key affects every row).
    pub not_in: bool,
    /// (index in `tables`, table column, index of the outer expression it equals).
    pub correlation: Vec<(usize, usize, usize)>,
}

/// A table of a semi-join subquery.
#[derive(Debug, Clone)]
pub(crate) struct SemiTable {
    pub table: TableRef,
    pub info: Arc<TableInfo>,
    /// The alias the subquery uses for it.
    pub alias: String,
    /// Columns of the table the subquery uses.
    pub referenced: Vec<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct Block {
    pub select: Select,
    pub items: Vec<Item>,
    pub outputs: Vec<Output>,
    /// The enclosing query has ORDER BY (dropped unless there is a LIMIT) / LIMIT or FETCH.
    pub ordered: bool,
    pub limit: bool,
    /// That ORDER BY / LIMIT, to render the block as an internal view.
    pub tail: Option<Box<Query>>,
    pub semis: Vec<Semi>,
    /// Why IN / EXISTS subqueries of the WHERE stayed subqueries (precise fallback reasons).
    pub semi_blockers: Vec<String>,
    /// Whether column references are all qualified with their item's alias.
    pub qualified: bool,
    /// DISTINCT was turned into GROUP BY (reasons still name DISTINCT).
    pub distinct_was: bool,
    /// Whether the SELECT differs from the source.
    pub rewritten: bool,
}
