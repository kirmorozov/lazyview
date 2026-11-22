//! The Percona side of the source seam.
//!
//! Percona Server runs the MySQL server's code, so this file owns no behaviour of its own: every
//! answer below is a forwarder into [`crate::source::mysql`], where the MySQL-family answer lives
//! (lead decision Q2). A Percona-only difference is then a body here that stops forwarding, which
//! is what makes "call into `mysql.rs`, never copy it" checkable from the layout alone. The
//! generic side reaches this file from [`crate::binlog::connect`]'s preflight and from
//! [`crate::source::Flavor`]'s product detection; `Flavor::Percona` shares every other MySQL arm.
//!
//! Copy set: `std` + `anyhow` + `mysql_async` only, no `crate::` beyond the copy set, no
//! environment variables (a knob is a parameter the caller passes), no logging.

/// The MySQL family's preflight check on `binlog_row_value_options`: PARTIAL_JSON row images
/// carry a diff instead of the values, so they are rejected. The probe is the network edge and
/// stays in [`crate::binlog::connect`]; the check and its message are
/// [`crate::source::mysql::preflight`], which Percona reaches through this file.
pub(crate) use crate::source::mysql::preflight;

/// The read endpoint (PR G): Percona takes every MySQL-family answer — the timed
/// `WAIT_FOR_EXECUTED_GTID_SET` (`gtid_wait_timeout`), the pre-flight (whose Galera arm is what
/// PXC runs) and the read-session init — through this file (lead decision Q2).
pub(crate) use crate::source::mysql::{
    gtid_wait_timeout as gtid_wait, init_read_session, read_preflight,
};

/// Whether a server whose `VERSION()` looks like MySQL's is in fact Percona: its `VERSION()`
/// carries no product name (`8.4.11-11`), so `@@GLOBAL.version_comment`
/// (`Percona Server (GPL), Release 11, Revision …`) is what names it. Reading that variable is
/// [`crate::binlog::connect`]'s job (the network edge); the product name is this flavor's own.
pub(crate) fn is_this(version_comment: &str) -> bool {
    version_comment.to_ascii_lowercase().contains("percona")
}

/// Percona Server's answer to `Flavor::snapshot_position_is_watermark`: MySQL's, `false` — and
/// this is the flavor the question was asked for, because unlike Oracle Community it *does* report
/// a pair. Measured on Percona Server 8.4 with GTIDs off (`tmp/out/lvs-skips/h3/probe2/`): with a
/// commit in flight, `Binlog_snapshot_position` had moved from 1014 to 1338 — past the in-flight
/// update's events — while that snapshot transaction still read the row's OLD value. So the pair
/// is the binlog write position, not a committed-transaction watermark, and the engine must not
/// wait on it: Percona's fence is the GTID wait, then the commit-order row it reaches through
/// [`crate::source::mysql`].
pub(crate) use crate::source::mysql::snapshot_position_is_watermark;
