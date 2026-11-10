//! PostgreSQL as a target: the statements of a `postgres://` target DSN. Every function here is
//! a stub until the PR that lands its real form (T1-T5); none is reachable before
//! [`crate::target::Kind::from_dsn`] accepts a PostgreSQL scheme.

use crate::mv::Layout;

/// `INSERT ... ON CONFLICT (row key) DO UPDATE` — PR T4 replaces this stub with the real
/// statement. Unreachable today: a PostgreSQL target DSN is rejected at [`Kind::from_dsn`].
///
/// [`Kind::from_dsn`]: crate::target::Kind::from_dsn
pub(crate) fn insert_sql(
    _schema: &str,
    _table: &str,
    _layout: &Layout,
    _n: usize,
    _key: Option<&[usize]>,
) -> String {
    unreachable!("PostgreSQL targets are rejected by Kind::from_dsn")
}
