//! The server-family seam shared by a source and a target: one discriminator and the
//! answers planning time asks it about. Dispatch happens once (per plan, per connect),
//! never per row.
//!
//! Copy set: lazy-wire-fire copies `src/dialect/mod.rs` and `src/dialect/mysql.rs` verbatim,
//! as a pair (`mysql` is this module's only child). These two files reference only each other,
//! std and external crates — no host code — so the quoting answers the copy set calls (the
//! forwarders in `crate::analyze::vocab`) compile standalone. The host layer adds everything
//! lazyview-only: the DSN rules and the cross-family quoting dispatch live on the target's
//! `Kind`, a re-export of [`Family`] (see `src/target/mod.rs`).

pub mod mysql;

/// The family of one server, source or target. The target module (`src/target/mod.rs`)
/// re-exports this enum as its `Kind` and owns the host-side answers about it (the DSN rules,
/// the statement and quoting dispatch); a source's family comes from its DSN scheme, never
/// from `crate::binlog::Flavor` (the MySQL-family product identity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// MySQL, MariaDB, Percona: the statement shapes of lazyview's mv layer today.
    Mysql,
    /// PostgreSQL, from a `postgres://` DSN (`src/target/postgres.rs`).
    Postgres,
}
