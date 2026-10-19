//! Lexical scopes of WITH clauses (MySQL / MariaDB rules), and the tables a statement reads.
//!
//! * A WITH clause opens a scope for its CTEs: a CTE body sees the CTEs defined *before* it in the
//!   same clause and those of enclosing queries, never itself — so in
//!   `WITH orders AS (SELECT … FROM orders)` the body reads the base table. With `RECURSIVE`, a body
//!   may reference itself (or a later CTE): such a CTE is recursive.
//! * Only single-part names can refer to CTEs (`db.t` is always a table); the innermost
//!   definition wins.
//! * Derived tables and subqueries see the CTEs of every enclosing query.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::rc::Rc;

use sqlparser::ast::{
    visit_relations, JoinConstraint, ObjectName, Query, Select, SetExpr, TableFactor,
    TableWithJoins, With,
};

use super::ast::{exprs_of, join_constraint, object_table_ref, single_name, subqueries};
use super::names::ident_name;
use crate::catalog::TableRef;

#[derive(Clone, Default)]
pub(crate) struct Scope<'q>(Option<Rc<Node<'q>>>);

struct Node<'q> {
    cte: Cte<'q>,
    parent: Scope<'q>,
}

#[derive(Clone)]
pub(crate) struct Cte<'q> {
    pub name: String,
    /// `WITH t (a, b) AS …`
    pub columns: Vec<String>,
    pub query: &'q Query,
    /// The scope its body is resolved in.
    pub body: Scope<'q>,
    /// `WITH RECURSIVE` and the body references this or a later CTE of the clause.
    pub recursive: bool,
}

impl<'q> Scope<'q> {
    /// The scope inside (the main body of) a query with this WITH clause.
    pub(crate) fn with_clause(&self, with: &'q With) -> Scope<'q> {
        let names: Vec<String> = with
            .cte_tables
            .iter()
            .map(|c| ident_name(&c.alias.name))
            .collect();
        let mut s = self.clone();
        for (i, c) in with.cte_tables.iter().enumerate() {
            let cte = Cte {
                name: names[i].clone(),
                columns: c
                    .alias
                    .columns
                    .iter()
                    .map(|c| ident_name(&c.name))
                    .collect(),
                query: &c.query,
                body: s.clone(),
                recursive: with.recursive && reads_any(&c.query, &names[i..]),
            };
            s = Scope(Some(Rc::new(Node { cte, parent: s })));
        }
        s
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<&Cte<'q>> {
        let mut cur = &self.0;
        while let Some(n) = cur {
            if n.cte.name.eq_ignore_ascii_case(name) {
                return Some(&n.cte);
            }
            cur = &n.parent.0;
        }
        None
    }

    /// The CTE a FROM name refers to, if any.
    pub(crate) fn cte_for(&self, name: &ObjectName) -> Option<&Cte<'q>> {
        single_name(name).and_then(|n| self.lookup(&n))
    }
}

/// Whether `q` names one of `names` as a single-part table anywhere (syntactically).
fn reads_any(q: &Query, names: &[String]) -> bool {
    let mut hit = false;
    let _ = visit_relations(q, |r: &ObjectName| {
        if single_name(r).is_some_and(|n| names.iter().any(|x| x.eq_ignore_ascii_case(&n))) {
            hit = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    hit
}

/// Every base table (or view) a statement reads, with CTE names resolved lexically: a CTE name
/// shadows a table only where the CTE is visible. Bodies of unreferenced CTEs count too (a
/// superset, like the server's own validation of the statement).
pub fn referenced_tables(query: &Query, default_schema: &str) -> Vec<TableRef> {
    let mut out = BTreeSet::new();
    walk_query(query, &Scope::default(), default_schema, &mut out);
    out.into_iter().collect()
}

fn walk_query(q: &Query, scope: &Scope, schema: &str, out: &mut BTreeSet<TableRef>) {
    let inner = match &q.with {
        Some(w) => {
            let all = scope.with_clause(w);
            for c in &w.cte_tables {
                // A recursive body sees the whole clause; a plain one only what precedes it.
                let body = match all.lookup(&ident_name(&c.alias.name)) {
                    Some(cte) if !w.recursive => cte.body.clone(),
                    _ => all.clone(),
                };
                walk_query(&c.query, &body, schema, out);
            }
            all
        }
        None => scope.clone(),
    };
    walk_set(&q.body, &inner, schema, out);
    if let Some(o) = &q.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(es) = &o.kind {
            for e in es {
                for sq in subqueries(&e.expr) {
                    walk_query(&sq, &inner, schema, out);
                }
            }
        }
    }
}

fn walk_set(s: &SetExpr, scope: &Scope, schema: &str, out: &mut BTreeSet<TableRef>) {
    match s {
        SetExpr::Select(sel) => walk_select(sel, scope, schema, out),
        SetExpr::Query(q) => walk_query(q, scope, schema, out),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set(left, scope, schema, out);
            walk_set(right, scope, schema, out);
        }
        SetExpr::Values(v) => {
            for row in &v.rows {
                for e in row.iter() {
                    for sq in subqueries(e) {
                        walk_query(&sq, scope, schema, out);
                    }
                }
            }
        }
        SetExpr::Table(t) => {
            if let Some(name) = &t.table_name {
                let tref = (
                    t.schema_name.clone().unwrap_or_else(|| schema.to_string()),
                    name.clone(),
                );
                if !(t.schema_name.is_none() && scope.lookup(name).is_some()) {
                    out.insert(tref);
                }
            }
        }
        _ => {}
    }
}

fn walk_select(s: &Select, scope: &Scope, schema: &str, out: &mut BTreeSet<TableRef>) {
    walk_from(&s.from, scope, schema, out);
    for e in exprs_of(s) {
        for sq in subqueries(e) {
            walk_query(&sq, scope, schema, out);
        }
    }
}

fn walk_from(from: &[TableWithJoins], scope: &Scope, schema: &str, out: &mut BTreeSet<TableRef>) {
    for twj in from {
        walk_factor(&twj.relation, scope, schema, out);
        for j in &twj.joins {
            walk_factor(&j.relation, scope, schema, out);
            if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                for sq in subqueries(e) {
                    walk_query(&sq, scope, schema, out);
                }
            }
        }
    }
}

fn walk_factor(f: &TableFactor, scope: &Scope, schema: &str, out: &mut BTreeSet<TableRef>) {
    match f {
        TableFactor::Table { name, .. } => {
            if scope.cte_for(name).is_none() {
                if let Some(t) = object_table_ref(name, schema) {
                    out.insert(t);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => walk_query(subquery, scope, schema, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => walk_from(std::slice::from_ref(table_with_joins), scope, schema, out),
        _ => {}
    }
}

/// The largest join block of the statement once every CTE and derived table is inlined (each
/// reference of a CTE copies its body): a SELECT with what is merged into it (derived tables,
/// CTE copies, subqueries), measured per branch of a set operation (each UNION ALL branch is
/// planned on its own). Computed without building anything, in time linear in the statement
/// (bodies are measured once, sizes saturate), so a chain of CTEs that each reference the previous
/// one twice (2^n copies) is refused before any work is done (A5). Only blocks that inline
/// something count: a join written out in full is the server's to accept or refuse.
pub(crate) struct InlinedSize {
    /// Base-table references of the largest block that inlines a CTE or derived table.
    pub refs: usize,
    /// That block inlines CTEs / derived tables.
    pub ctes: bool,
    pub derived: bool,
    /// The top-level set operation's branch (1-based) holding that block.
    pub branch: Option<usize>,
    /// (CTE name, number of references), for the reason text.
    pub worst: Option<(String, usize)>,
}

/// Size of a relation: its flat references, what its block inlines, and the largest inlining
/// block among its separately planned parts (set-operation branches).
#[derive(Clone, Copy, Default)]
struct Measure {
    refs: usize,
    ctes: bool,
    derived: bool,
    /// (references, ctes, derived) of the largest inlining block nested below as its own block.
    worst: Option<(usize, bool, bool)>,
}

impl Measure {
    fn add(&mut self, m: Measure) {
        self.refs = self.refs.saturating_add(m.refs);
        self.ctes |= m.ctes;
        self.derived |= m.derived;
        self.worst = max_block(self.worst, m.worst);
    }

    /// The largest inlining block, this one included.
    fn largest(&self) -> Option<(usize, bool, bool)> {
        let own = (self.ctes || self.derived).then_some((self.refs, self.ctes, self.derived));
        max_block(self.worst, own)
    }
}

fn max_block(
    a: Option<(usize, bool, bool)>,
    b: Option<(usize, bool, bool)>,
) -> Option<(usize, bool, bool)> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if y.0 > x.0 { y } else { x }),
        (x, None) => x,
        (None, y) => y,
    }
}

pub(crate) fn inlined_size(root: &Query, scope: &Scope) -> InlinedSize {
    struct Count {
        memo: HashMap<*const Query, Measure>,
        /// Per CTE name: references and inlined size of one copy.
        times: HashMap<String, (usize, usize)>,
    }
    fn query(q: &Query, scope: &Scope, c: &mut Count) -> Measure {
        let inner = match &q.with {
            Some(w) => scope.with_clause(w),
            None => scope.clone(),
        };
        let mut n = set(&q.body, &inner, c);
        if let Some(o) = &q.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(es) = &o.kind {
                for e in es {
                    for sq in subqueries(&e.expr) {
                        n.add(query(&sq, &inner, c));
                    }
                }
            }
        }
        n
    }
    fn set(s: &SetExpr, scope: &Scope, c: &mut Count) -> Measure {
        match s {
            SetExpr::Select(sel) => {
                let mut n = from(&sel.from, scope, c);
                for e in exprs_of(sel) {
                    for sq in subqueries(e) {
                        n.add(query(&sq, scope, c));
                    }
                }
                n
            }
            SetExpr::Query(q) => query(q, scope, c),
            // Each branch is a block of its own; as a FROM item the operation is (at most) its
            // largest branch.
            SetExpr::SetOperation { left, right, .. } => {
                let (l, r) = (set(left, scope, c), set(right, scope, c));
                Measure {
                    refs: l.refs.max(r.refs),
                    ctes: false,
                    derived: false,
                    worst: max_block(l.largest(), r.largest()),
                }
            }
            _ => Measure {
                refs: 1,
                ..Measure::default()
            },
        }
    }
    fn from(from: &[TableWithJoins], scope: &Scope, c: &mut Count) -> Measure {
        let mut n = Measure::default();
        for twj in from {
            n.add(factor(&twj.relation, scope, c));
            for j in &twj.joins {
                n.add(factor(&j.relation, scope, c));
                if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                    for sq in subqueries(e) {
                        n.add(query(&sq, scope, c));
                    }
                }
            }
        }
        n
    }
    fn factor(f: &TableFactor, scope: &Scope, c: &mut Count) -> Measure {
        let one = Measure {
            refs: 1,
            ..Measure::default()
        };
        match f {
            TableFactor::Table { name, .. } => match scope.cte_for(name) {
                Some(cte) => {
                    let key = cte.query as *const Query;
                    let n = match c.memo.get(&key) {
                        Some(&n) => n,
                        None => {
                            let (q, body) = (cte.query, cte.body.clone());
                            let n = query(q, &body, c);
                            c.memo.insert(key, n);
                            n
                        }
                    };
                    let e = c.times.entry(cte.name.clone()).or_insert((0, n.refs));
                    e.0 += 1;
                    Measure { ctes: true, ..n }
                }
                None => one,
            },
            TableFactor::Derived { subquery, .. } => Measure {
                derived: true,
                ..query(subquery, scope, c)
            },
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => from(std::slice::from_ref(table_with_joins), scope, c),
            _ => one,
        }
    }
    let mut c = Count {
        memo: HashMap::new(),
        times: HashMap::new(),
    };
    // The top-level set operation's branches, each measured on its own.
    fn leaves<'q>(s: &'q SetExpr, out: &mut Vec<&'q SetExpr>) {
        match s {
            SetExpr::SetOperation { left, right, .. } => {
                leaves(left, out);
                leaves(right, out);
            }
            other => out.push(other),
        }
    }
    let inner = match &root.with {
        Some(w) => scope.with_clause(w),
        None => scope.clone(),
    };
    let mut parts = Vec::new();
    leaves(&root.body, &mut parts);
    let mut largest: Option<(usize, bool, bool)> = None;
    let mut branch = None;
    for (i, part) in parts.iter().enumerate() {
        let mut m = set(part, &inner, &mut c);
        if parts.len() == 1 {
            if let Some(o) = &root.order_by {
                if let sqlparser::ast::OrderByKind::Expressions(es) = &o.kind {
                    for e in es {
                        for sq in subqueries(&e.expr) {
                            m.add(query(&sq, &inner, &mut c));
                        }
                    }
                }
            }
        }
        let l = m.largest();
        if l.is_some_and(|l| largest.is_none_or(|x| l.0 > x.0)) {
            largest = l;
            branch = (parts.len() > 1).then_some(i + 1);
        }
    }
    let (refs, ctes, derived) = largest.unwrap_or_default();
    // The multiply-referenced CTE whose copies take the most room.
    let worst = c
        .times
        .into_iter()
        .filter(|(_, (n, _))| *n > 1)
        .max_by_key(|(name, (n, size))| (n.saturating_mul(*size), std::cmp::Reverse(name.clone())))
        .map(|(name, (n, _))| (name, n));
    InlinedSize {
        refs,
        ctes,
        derived,
        branch,
        worst,
    }
}
