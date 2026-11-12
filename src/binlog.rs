//! Binlog replication client: positions, server checks and a decoded event stream.
//!
//! # Connecting
//!
//! Plain `mysql_async::Opts::from_url(dsn)` works against every supported server (verified on
//! MySQL 8.4 / 26.7, Percona 8.4 / 9.7, MariaDB 10.11 / 13.0 and the 13.2 preview; earlier also
//! on the now EOL MySQL 8.0, Percona 8.0 and on MariaDB 11.4), including accounts using
//! `caching_sha2_password` (the MySQL 8.x default) **without TLS**: when the server asks for full
//! authentication over an insecure channel, mysql_async fetches the server's RSA public key and
//! sends the password RSA-encrypted (needs only the default
//! `caching_sha2_password_auto_generate_rsa_keys=ON` on the server). For `localhost` /
//! `127.0.0.1` mysql_async moves the connection to the server's unix socket (`prefer_socket`,
//! on by default), which also counts as secure; `?prefer_socket=false` keeps TCP. TLS is
//! optional: `?require_ssl=true&verify_ca=false` works with the servers' auto-generated
//! self-signed certificates (rustls with the `ring` provider); MariaDB 10.11 has no TLS unless
//! certificates are configured (`have_ssl=DISABLED`). See [`replica_opts`].
//!
//! # Positions
//!
//! Every event header carries `log_pos`, the offset *after* the event in the current file. The
//! reader tracks the file from ROTATE events (including the artificial one the server sends at
//! stream start, which on MariaDB carries a CRC32 that has to be stripped by hand) and the
//! offset from `log_pos`. Artificial events (fake ROTATE, the format description re-sent when
//! starting mid-file) do not move the position.
//!
//! * MySQL `binlog_transaction_compression=ON`: mysql_async yields the `TRANSACTION_PAYLOAD`
//!   event and then the decompressed inner events, whose `log_pos` is 0. They are not part of
//!   the file layout, so the position stays at the end of the payload event and the inner XID
//!   becomes a `Commit` that resumes right after the payload.
//! * MariaDB >= 11.4 (`binlog_legacy_event_pos=OFF`): events inside a transaction have
//!   `log_pos = 0`; only the commit event has a real one. The position advances by event size
//!   there, but because the server omits ANNOTATE_ROWS events to GTID-capable replicas (holes
//!   before TABLE_MAP events) it is only trusted until the first TABLE_MAP; see
//!   `Tracker::boundary_here`.
//! * MariaDB omits ANNOTATE_ROWS events (holes in the offsets) on every version; `log_pos` of
//!   the following event is still exact.
//! * MariaDB >= 12.3 splits a rows event larger than `binlog_row_event_fragment_threshold`
//!   (default 1 GiB) into `Partial_rows` events; they are reassembled into the rows event.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::StreamExt;
use mysql_async::binlog::events::{
    Event, EventData, FormatDescriptionEvent, GtidEvent, QueryEvent, TableMapEvent,
    TransactionPayloadEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::{BinlogVersion, EventType};
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Opts, OptsBuilder, Row, Value};

mod connect;
mod probe;
mod query;
mod stream;

pub use self::connect::{replica_opts, server_info, set_product, Flavor, Position, ServerInfo};
pub use self::probe::{
    binlog_exists, binlog_filters, current_position, is_access_denied, is_duplicate_server_id,
    is_missing_binlog, schema_filtered, MissingBinlog,
};
pub use self::stream::{BinlogReader, Item, RowsKind};

#[cfg(test)]
use self::connect::{product, requirement_problems, split_ca_params, unmet_requirements};
#[cfg(test)]
use self::probe::{status_statements, with_hint};
#[cfg(test)]
use self::query::{
    classify_query, format_uuid, mariadb_gtid, mysql_gtid, xa_prepare_event, QueryKind,
};
#[cfg(test)]
use self::stream::{crc32, ty, Tracker, ARTIFICIAL_FLAG, DUMMY_TABLE_ID};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ca_tests;
