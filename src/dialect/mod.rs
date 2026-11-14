//! The server-family seam shared by a source and a target: one discriminator and the
//! answers planning time asks it about. Dispatch happens once (per plan, per connect),
//! never per row.
//!
//! The answers so far are the identifier quoting, the function vocabularies, the [`scoping`] knobs,
//! the parameter and coercion renderings (the placeholder, how a JSON value binds, how a `?` takes
//! a character set), the parse profile (how a definition's text is tokenized and handed to the
//! query parser), the source-read renderings (how a TIMESTAMP epoch is read and read back) and the
//! session renderings (the statements a session is set up with and the `sql_mode` they carry): all
//! of them [`mysql`]'s, since MySQL family is the only one with answers here. A new family adds
//! its own answers next to these rather than branching on a server name at the call site.
//!
//! Copy set: lazy-wire-fire copies `src/dialect/mod.rs` and `src/dialect/mysql.rs` verbatim,
//! as a pair (`mysql` is this module's only child). These two files reference only each other,
//! std and external crates — no host code — so the quoting answers the copy set calls (the
//! forwarders in `crate::analyze::vocab`) compile standalone. The host layer adds everything
//! lazyview-only: the DSN rules and the cross-family quoting dispatch live on the target's
//! `Kind`, a re-export of [`Family`] (see `src/target/mod.rs`).

use anyhow::anyhow;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::TokenWithSpan;

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

/// Tokens of a definition with locations on `family`'s parse profile, refusal of the executable
/// comments included ([`mysql::tokenize_view`]).
///
/// The PostgreSQL arm **refuses**: lead decision D2 keeps the PostgreSQL parse profile unwritten
/// until `pgsrc-parse-profile` gives it sqlparser's `PostgreSqlDialect`; it never returns the
/// MySQL profile as a stand-in, so a PostgreSQL definition cannot be silently planned on MySQL
/// rules. Until then the analyzer has no PostgreSQL definitions (a `postgres://` source is
/// refused at connect), and its entry points pass [`Family::Mysql`].
pub(crate) fn tokenize_view(family: Family, sql: &str) -> anyhow::Result<Vec<TokenWithSpan>> {
    match family {
        Family::Mysql => mysql::tokenize_view(sql),
        Family::Postgres => Err(anyhow!("PostgreSQL sources are not supported yet")),
    }
}

/// A query parser over tokens with locations on `family`'s parse profile
/// ([`mysql::new_parser`]); the PostgreSQL arm refuses for the same reason as [`tokenize_view`].
pub(crate) fn new_parser(
    family: Family,
    tokens: Vec<TokenWithSpan>,
) -> anyhow::Result<Parser<'static>> {
    match family {
        Family::Mysql => Ok(mysql::new_parser(tokens)),
        Family::Postgres => Err(anyhow!("PostgreSQL sources are not supported yet")),
    }
}

/// Which statement and scoping extensions a family's server accepts, as one bundle the analyzer
/// asks the family about instead of assuming MySQL's. Read once per plan, never per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scoping {
    /// `GROUP BY` may name a select-list alias: the MySQL family allows it (its own extension over
    /// standard SQL) and the analyzer follows the server today (`src/analyze/build.rs` resolves such
    /// a name, `src/analyzer/normalize.rs` rewrites it), so the MySQL family answers `true`.
    pub group_by_output_alias: bool,
    /// A non-aggregated select list is legal when its columns depend on the grouped table's primary key.
    pub functional_dependency_on_pk: bool,
    /// `LATERAL` derived tables exist.
    pub lateral: bool,
    /// `SELECT DISTINCT ON (…)` exists.
    pub distinct_on: bool,
    /// The family has an ordered string aggregate (`string_agg`; MySQL's is `GROUP_CONCAT`).
    pub string_agg: bool,
    /// A unit key may be built over a `jsonb` column.
    pub jsonb_units: bool,
}

impl Scoping {
    /// The MySQL family's answers: only the output-alias rule above is on.
    pub const MYSQL: Scoping = Scoping {
        group_by_output_alias: true,
        functional_dependency_on_pk: false,
        lateral: false,
        distinct_on: false,
        string_agg: false,
        jsonb_units: false,
    };
}

/// The scoping answers of one family, read once per plan. PostgreSQL's own answers arrive with the
/// analyzer's PostgreSQL PRs (#57 S4c/S4d), which is where these flags become live; until then
/// nothing but a MySQL-family plan reads them.
pub fn scoping(family: Family) -> Scoping {
    match family {
        // Shared until S4c/S4d give PostgreSQL its own answers: no PostgreSQL plan reads these yet.
        Family::Mysql | Family::Postgres => Scoping::MYSQL,
    }
}
