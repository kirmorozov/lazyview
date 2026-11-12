//! Turns a `CREATE VIEW` statement into a [`ViewPlan`]: what to watch, how a changed row maps to
//! the materialized rows it affects ("units"), and pre-rendered SQL for every refresh query.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, BinaryOperator, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator, NamedWindowExpr,
    ObjectName, ObjectNamePart, OrderBy, OrderByKind, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableAlias, TableFactor, TableWithJoins,
    UnaryOperator, Value, Visit, Visitor, WindowType,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Word};

use crate::analyzer::expr_key;
use crate::catalog::{ColKind, FkAction, TableInfo, TableRef, TypeFamily};
use crate::datum::Datum;

mod build;
mod columns;
mod expr;
mod parse;
mod plan;
mod routes;
mod vocab;

pub(crate) use self::build::expand_star_over_using;
pub use self::build::{plan, plan_block};
pub(crate) use self::columns::virtual_expansion;
pub(crate) use self::expr::null_propagating;
use self::parse::tokenize;
pub use self::parse::{
    definition_hash, function_calls, is_create_statement, parse_definition, parse_select,
    referenced_tables, select_tables, ParsedDef,
};
pub(crate) use self::plan::int_sql;
pub use self::plan::{
    AliasPlan, CascadeRoute, ConstFilter, Edge, LineageKey, MapPlan, Mode, PartialUnits, Pinned,
    Presence, ViewPlan,
};
pub(crate) use self::routes::{cascade_routes, join_constraint};
pub(crate) use self::vocab::{ident_name, qident, semi_table_marker, STAR_USING, SUBST_MARKER};
pub use self::vocab::{quote_ident, set_reserved};

/// Parses all of `sql` with `f`; None if it fails or leaves tokens over.
pub(crate) fn parse_sql<T>(
    sql: &str,
    f: impl FnOnce(&mut Parser) -> Result<T, ParserError>,
) -> Option<T> {
    let mut p = Parser::new(&MySqlDialect {}).with_tokens_with_locations(tokenize(sql).ok()?);
    let v = f(&mut p).ok()?;
    (p.peek_token().token == Token::EOF).then_some(v)
}

#[cfg(test)]
mod tests;
