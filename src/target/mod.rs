//! How lazyview talks to one target instance: the family discriminator and the facts a
//! connection's statements are built from. Dispatched once per target connection: every
//! target-facing function in `crate::mv` matches on [`Kind`] at its top and calls one
//! function of the family's module. No trait objects, nothing on a per-row path.

use anyhow::{bail, Result};

pub mod mysql;

/// The family of one target instance. A source is always MySQL-family (`crate::config::Flavor`
/// is the source's product; this is the target's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// MySQL, MariaDB, Percona: the statements of [`crate::mv`] today.
    Mysql,
    /// PostgreSQL, from a `postgres://` target DSN (`src/target/postgres.rs`).
    Postgres,
}

impl Kind {
    /// The family a target DSN names. Only the scheme decides; the family module parses the
    /// rest. `postgres://` and `postgresql://` are a PostgreSQL target; `mysql://` and
    /// `mariadb://` a MySQL-family one; anything else is an error. A *source* DSN is never a
    /// PostgreSQL one (`LAZYVIEW_DSN` rejects it in `crate::config`).
    pub fn from_dsn(dsn: &str) -> Result<Kind> {
        let scheme = dsn
            .split_once("://")
            .map(|(s, _)| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        match scheme.as_str() {
            "mysql" | "mariadb" => Ok(Kind::Mysql),
            // Until src/target/postgres.rs lands, a PostgreSQL target cannot be reached.
            "postgres" | "postgresql" => {
                bail!("PostgreSQL targets are not supported yet (postgres:// DSN)")
            }
            _ => bail!(
                "invalid target DSN: scheme `{scheme}` is not mysql://, mariadb:// or postgres://"
            ),
        }
    }
}
