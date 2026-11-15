//! The PostgreSQL source's host side (issue #57): the `pg_catalog` loader first; the slot, the
//! stream, the preflight and the fence join it in the later PRs of the S-series.
//!
//! This is host code and is NOT part of the copy set (decision D5): it reaches PostgreSQL through
//! `tokio_postgres` and calls the target's type map and `crate::mv` (source plan §0.2) — imports
//! the copy-set files may never make, so no embedder copies it. It lives here rather than under
//! `src/source/` for exactly that reason, and `src/lib.rs` declares it.

pub(crate) mod catalog;

#[cfg(test)]
mod tests;
