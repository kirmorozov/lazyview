//! The server-family seam shared by a source and a target: one discriminator and the
//! answers planning time asks it about. Dispatch happens once (per plan, per connect),
//! never per row.
//!
//! The answers so far are the identifier quoting, the function vocabularies, the [`scoping`] knobs,
//! the parameter and coercion renderings (the placeholder, how a JSON value binds, how a `?` takes
//! a character set), the parse profile (how a definition's text is tokenized and handed to the
//! query parser), the source-read renderings (how a TIMESTAMP epoch is read and read back), the
//! session renderings (the statements a session is set up with and the `sql_mode` they carry), the
//! row-value normalizations (the BIT and TIME canonical forms, the JSON canonical text) and the
//! identifier case rule. The MySQL family has all of them; [`postgres`] has its parse profile and
//! its case rule, and its other answers arrive with `pgsrc-fn-vocab` and `pgsrc-group-scope`. A new
//! family adds its own answers next to these rather than branching on a server name at the call
//! site.
//!
//! Copy set: lazy-wire-fire copies `src/dialect/mod.rs`, `src/dialect/mysql.rs` and
//! `src/dialect/postgres.rs` verbatim, as one set (the two family files are this module's only
//! children). These files reference only each other, std, external crates and [`crate::datum`]
//! (another copy-set file, whose `time_param` owns the TIME parameter form the row normalizations
//! ask for) — no host code — so the quoting answers the copy set calls (the forwarders in
//! `crate::analyze::vocab`) compile standalone. The host layer adds everything lazyview-only: the
//! DSN rules and the cross-family quoting dispatch live on the target's `Kind`, a re-export of
//! [`Family`] (see `src/target/mod.rs`).

use sqlparser::parser::Parser;
use sqlparser::tokenizer::TokenWithSpan;

pub mod mysql;
pub mod postgres;

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

/// Tokens of a definition with locations on `family`'s parse profile. Each family refuses what its
/// own server would read differently from the plan: the MySQL family its executable comments
/// ([`mysql::tokenize_view`], whose content the server runs), PostgreSQL the constructs only it
/// has that lazyview does not accept yet ([`postgres::tokenize_view`]). A definition is never
/// tokenized on the other family's profile.
///
/// The analyzer's entry points pass [`Family::Mysql`] today: a `postgres://` source is refused at
/// connect until a PostgreSQL reader starts one (phase S5), so the analyzer has no PostgreSQL
/// definitions yet.
pub(crate) fn tokenize_view(family: Family, sql: &str) -> anyhow::Result<Vec<TokenWithSpan>> {
    match family {
        Family::Mysql => mysql::tokenize_view(sql),
        Family::Postgres => postgres::tokenize_view(sql),
    }
}

/// A query parser over tokens with locations on `family`'s parse profile ([`mysql::new_parser`],
/// [`postgres::new_parser`]: sqlparser's `MySqlDialect` and `PostgreSqlDialect`).
pub(crate) fn new_parser(
    family: Family,
    tokens: Vec<TokenWithSpan>,
) -> anyhow::Result<Parser<'static>> {
    match family {
        Family::Mysql => Ok(mysql::new_parser(tokens)),
        Family::Postgres => Ok(postgres::new_parser(tokens)),
    }
}

/// The key form of one identifier, for the plan-time comparisons that must not see a rename
/// (`crate::analyzer::expr_key`): the value and the quote style to render it with. The MySQL
/// family answers case-insensitively and quoting says nothing about a name, so its key folds the
/// name to lower and drops the quotes — except in the parts before the column of a qualified
/// name, whose case its servers do keep. PostgreSQL folds an unquoted name as its resolver does
/// and keeps a double-quoted one exactly, so `"Foo"` stays another column than `Foo`
/// ([`postgres::ident_key`]). `qualified` is true for the parts before the last one.
pub(crate) fn ident_key(
    family: Family,
    value: &str,
    quote_style: Option<char>,
    qualified: bool,
) -> (String, Option<char>) {
    match family {
        Family::Mysql if qualified => (value.to_string(), None),
        Family::Mysql => (value.to_lowercase(), None),
        Family::Postgres => postgres::ident_key(value, quote_style),
    }
}

/// The name `family`'s server gives the column an identifier stands for, from the name
/// `crate::analyze::ident_name` read off the AST: as it was written for the MySQL family, folded
/// to lower for an unquoted PostgreSQL one ([`postgres::fold_name`]).
pub(crate) fn fold_name(family: Family, name: String, quoted: bool) -> String {
    match family {
        Family::Mysql => name,
        Family::Postgres => postgres::fold_name(name, quoted),
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
