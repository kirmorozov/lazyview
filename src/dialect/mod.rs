//! The server-family seam shared by a source and a target: one discriminator and the
//! answers planning time asks it about. Dispatch happens once (per plan, per connect),
//! never per row.

use anyhow::{bail, Result};

/// The family of one server, source or target: the same discriminator [`crate::target::Kind`]
/// carries; a source's family comes from its DSN scheme, never from `crate::binlog::Flavor`
/// (the MySQL-family product identity).
pub use crate::target::Kind as Family;

impl Family {
    /// The family a *source* DSN (`LAZYVIEW_DSN`) names, mirroring [`Family::from_dsn`]:
    /// only the scheme decides. `postgres://` and `postgresql://` name a PostgreSQL
    /// source, which is not supported yet — one message, the `LAZYVIEW_DSN` rejection in
    /// `crate::config`; the enabling PR returns [`Family::Postgres`] instead. Any other
    /// scheme (or a bare one) is answered `Mysql`: the DSN itself stays validated by the
    /// MySQL-only parser [`crate::binlog::replica_opts`], as today.
    pub fn from_source_dsn(dsn: &str) -> Result<Family> {
        let scheme = dsn
            .split_once("://")
            .map(|(s, _)| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        match scheme.as_str() {
            "postgres" | "postgresql" => {
                bail!("PostgreSQL sources are not supported yet (postgres:// LAZYVIEW_DSN)")
            }
            _ => Ok(Family::Mysql),
        }
    }
}
