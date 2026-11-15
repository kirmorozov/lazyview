//! The PostgreSQL source (issue #57): the `pg_catalog` loader first; the slot, the stream,
//! the preflight and the fence join it in the later PRs of the S-series.
//!
//! This module is not part of the copy set: it reaches PostgreSQL through `tokio_postgres`
//! and calls the target's type map (source plan §0.2) — imports the copy-set files may never
//! make, so the copy set does not include it.

pub(crate) mod catalog;

#[cfg(test)]
mod tests;
